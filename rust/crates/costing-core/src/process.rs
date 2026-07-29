use thiserror::Error;

use crate::error::{CostingError, ErrorStage};
use crate::fact::build_fact_bundle;
use crate::model::{MonthRange, RawWorkbook, StageTimings, WorkbookPayload};
use crate::normalize::normalize_workbook;
use crate::pipeline::PipelineRules;
use crate::presentation::build_workbook_payload;
use crate::split::split_detail_and_qty;
use crate::timing::measure;

#[derive(Debug)]
pub struct ProcessedWorkbook {
    pub payload: WorkbookPayload,
    pub stage_timings: StageTimings,
}

#[derive(Debug, Error)]
#[error("{source}")]
pub struct ProcessFailure {
    stage: ErrorStage,
    #[source]
    source: CostingError,
}

impl ProcessFailure {
    pub fn stage(&self) -> ErrorStage {
        self.stage
    }

    pub fn into_error(self) -> CostingError {
        self.source
    }
}

pub fn process_workbook(
    raw: RawWorkbook,
    rules: &PipelineRules,
    month_range: Option<MonthRange>,
    mut stage_timings: StageTimings,
) -> Result<ProcessedWorkbook, ProcessFailure> {
    let month_filter_requested = month_range.is_some();
    let normalized = measure(&mut stage_timings, "normalize", || {
        normalize_workbook(raw, rules, month_range)
    })
    .map_err(|source| ProcessFailure {
        stage: ErrorStage::Normalize,
        source,
    })?;
    let month_filter_empty_result = month_filter_requested && normalized.is_empty();
    let split = measure(&mut stage_timings, "split", || {
        split_detail_and_qty(normalized)
    })
    .map_err(|source| ProcessFailure {
        stage: ErrorStage::Split,
        source,
    })?;
    let bundle = measure(&mut stage_timings, "fact", || {
        build_fact_bundle(split, rules)
    })
    .map_err(|source| ProcessFailure {
        stage: ErrorStage::BuildFact,
        source,
    })?;
    let payload_timings = stage_timings.clone();
    let payload = measure(&mut stage_timings, "presentation", || {
        build_workbook_payload(bundle, rules, payload_timings, month_filter_empty_result)
    })
    .map_err(|source| ProcessFailure {
        stage: ErrorStage::BuildPresentation,
        source,
    })?;

    Ok(ProcessedWorkbook {
        payload,
        stage_timings,
    })
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;
    use crate::model::CellValue;
    use crate::pipeline::PipelineName;

    #[test]
    fn process_workbook_hides_the_complete_in_memory_pipeline() {
        let columns = [
            "年期",
            "产品编码",
            "产品名称",
            "工单编号",
            "工单行号",
            "本期完工数量",
            "本期完工金额",
            "成本项目名称",
        ];
        let raw = RawWorkbook {
            sheet_name: "成本计算单".to_string(),
            header_rows: [
                vec![String::new(); columns.len()],
                columns.iter().map(|value| (*value).to_string()).collect(),
            ],
            rows: vec![vec![
                CellValue::Text("2025年01期".to_string().into()),
                CellValue::Text("P1".to_string().into()),
                CellValue::Text("产品".to_string().into()),
                CellValue::Text("WO-1".to_string().into()),
                CellValue::Text("1".to_string().into()),
                CellValue::Decimal(Decimal::ONE),
                CellValue::Decimal(Decimal::TEN),
                CellValue::Blank,
            ]],
        };

        let processed = process_workbook(
            raw,
            &PipelineRules::for_name(PipelineName::Gb),
            None,
            StageTimings::default(),
        )
        .unwrap();

        assert_eq!(
            processed
                .payload
                .sheet_models
                .iter()
                .map(|sheet| sheet.sheet_name.as_str())
                .collect::<Vec<_>>(),
            [
                "成本计算单总表",
                "成本计算单数量聚合维度",
                "成本分析工单维度",
            ]
        );
        assert_eq!(
            processed
                .stage_timings
                .stages
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["fact", "normalize", "presentation", "split"]
        );
    }
}

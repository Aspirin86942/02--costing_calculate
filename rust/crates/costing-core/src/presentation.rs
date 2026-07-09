use std::collections::BTreeMap;

use crate::anomaly::build_work_order_anomaly_sheet;
use crate::error::CostingError;
use crate::fact::build_qty_sheet_rows;
use crate::model::{CellValue, FactBundle, SheetModel, StageTimings, TableRow, WorkbookPayload};
use crate::pipeline::PipelineConfig;
use crate::quality::build_quality_metrics;

const PRODUCT_DIMENSION_SHEET: &str = "成本分析产品维度";

pub fn build_workbook_payload(
    bundle: FactBundle,
    config: &PipelineConfig,
    timings: StageTimings,
) -> Result<WorkbookPayload, CostingError> {
    let detail_sheet = build_flat_sheet("成本计算单总表", bundle.detail_fact.clone());
    let qty_sheet = build_flat_sheet(
        "成本计算单数量聚合维度",
        build_qty_sheet_rows(&bundle, config),
    );
    let work_order_sheet = build_work_order_anomaly_sheet(&bundle, config);
    let sheets = vec![detail_sheet, qty_sheet, work_order_sheet];
    ensure_no_product_dimension(&sheets)?;

    Ok(WorkbookPayload {
        sheet_models: sheets,
        quality_metrics: build_quality_metrics(&bundle),
        error_log_count: bundle.error_issues.len(),
        stage_timings: timings,
    })
}

fn build_flat_sheet(sheet_name: &str, rows: Vec<TableRow>) -> SheetModel {
    let columns = rows
        .first()
        .map(|row| row.values.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    let sheet_rows = rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .map(|column| row.values.get(column).cloned().unwrap_or(CellValue::Blank))
                .collect()
        })
        .collect();
    SheetModel {
        sheet_name: sheet_name.to_string(),
        column_types: build_column_types(&columns),
        number_formats: build_number_formats(&columns),
        columns,
        rows: sheet_rows,
        freeze_panes: Some("A2".to_string()),
        auto_filter: true,
        fixed_width: Some(15.0),
    }
}

fn build_column_types(columns: &[String]) -> BTreeMap<String, String> {
    columns
        .iter()
        .map(|column| {
            let column_type = if column.contains("单位成本") || column.contains("单位完工")
            {
                "price"
            } else if column.contains("数量") {
                "qty"
            } else if column.contains("金额") || column.contains("成本") {
                "amount"
            } else {
                "text"
            };
            (column.clone(), column_type.to_string())
        })
        .collect()
}

fn build_number_formats(columns: &[String]) -> BTreeMap<String, String> {
    build_column_types(columns)
        .into_iter()
        .filter_map(|(column, column_type)| {
            if matches!(column_type.as_str(), "amount" | "price" | "qty") {
                Some((column, "#,##0.00".to_string()))
            } else {
                None
            }
        })
        .collect()
}

fn ensure_no_product_dimension(sheets: &[SheetModel]) -> Result<(), CostingError> {
    if sheets
        .iter()
        .any(|sheet| sheet.sheet_name == PRODUCT_DIMENSION_SHEET)
    {
        return Err(CostingError::invalid_input(format!(
            "{PRODUCT_DIMENSION_SHEET} 不属于 Rust 默认 workbook 契约"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rust_decimal::Decimal;

    use crate::model::{CellValue, ErrorIssue, FactBundle, StageTimings, TableRow};
    use crate::pipeline::{PipelineConfig, PipelineName};

    use super::*;

    fn row(values: &[(&str, CellValue)]) -> TableRow {
        TableRow {
            values: values
                .iter()
                .map(|(key, value)| ((*key).to_string(), value.clone()))
                .collect::<BTreeMap<_, _>>(),
        }
    }

    fn bundle() -> FactBundle {
        FactBundle {
            detail_fact: vec![row(&[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("本期完工金额", CellValue::Decimal(Decimal::new(100, 0))),
            ])],
            qty_fact: vec![row(&[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("产品编码", CellValue::Text("P1".to_string())),
                ("产品名称", CellValue::Text("产品".to_string())),
                ("工单编号", CellValue::Text("WO1".to_string())),
                ("工单行号", CellValue::Text("1".to_string())),
                ("completed_qty", CellValue::Decimal(Decimal::new(10, 0))),
                (
                    "completed_amount_total",
                    CellValue::Decimal(Decimal::new(100, 0)),
                ),
                ("dm_amount", CellValue::Decimal(Decimal::new(100, 0))),
            ])],
            work_order_fact: vec![row(&[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("产品编码", CellValue::Text("P1".to_string())),
                ("产品名称", CellValue::Text("产品".to_string())),
                ("工单编号", CellValue::Text("WO1".to_string())),
                ("工单行号", CellValue::Text("1".to_string())),
                ("单据类型", CellValue::Text("汇报入库-普通生产".to_string())),
                ("completed_qty", CellValue::Decimal(Decimal::new(10, 0))),
                (
                    "completed_amount_total",
                    CellValue::Decimal(Decimal::new(100, 0)),
                ),
                ("dm_amount", CellValue::Decimal(Decimal::new(100, 0))),
            ])],
            error_issues: vec![ErrorIssue {
                row_id: "row-1".to_string(),
                issue_type: "MISSING_AMOUNT".to_string(),
                field_name: "本期完工金额".to_string(),
                reason: "missing".to_string(),
                action: "filled zero".to_string(),
            }],
        }
    }

    #[test]
    fn payload_has_exactly_three_default_sheets_without_product_dimension() {
        let payload = build_workbook_payload(
            bundle(),
            &PipelineConfig::for_name(PipelineName::Gb),
            StageTimings::default(),
        )
        .unwrap();
        let names = payload
            .sheet_models
            .iter()
            .map(|sheet| sheet.sheet_name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec![
                "成本计算单总表",
                "成本计算单数量聚合维度",
                "成本分析工单维度"
            ]
        );
        assert!(!names.contains(&"成本分析产品维度"));
    }

    #[test]
    fn payload_carries_quality_errors_and_timings() {
        let mut timings = StageTimings::default();
        timings.insert("reader_rows", 1.0);

        let payload = build_workbook_payload(
            bundle(),
            &PipelineConfig::for_name(PipelineName::Gb),
            timings,
        )
        .unwrap();

        assert_eq!(payload.error_log_count, 1);
        assert_eq!(payload.quality_metrics.len(), 2);
        assert_eq!(payload.stage_timings.stages.get("reader_rows"), Some(&1.0));
    }
}

use std::collections::BTreeMap;

use crate::anomaly::build_work_order_anomaly_sheet;
use crate::error::CostingError;
use crate::fact::{build_qty_sheet_rows, qty_sheet_columns};
use crate::model::{CellValue, FactBundle, SheetModel, StageTimings, TableRow, WorkbookPayload};
use crate::pipeline::PipelineConfig;
use crate::quality::build_quality_metrics;
use crate::sheet_contract::detail_sheet_columns;

const PRODUCT_DIMENSION_SHEET: &str = "成本分析产品维度";
const DETAIL_TWO_DECIMAL_COLUMNS: &[&str] = &["本期完工单位成本", "本期完工金额"];
const QTY_TWO_DECIMAL_COLUMNS: &[&str] = &[
    "本期完工单位成本",
    "本期完工金额",
    "本期完工直接材料合计完工金额",
    "本期完工直接人工合计完工金额",
    "本期完工制造费用合计完工金额",
    "本期完工制造费用_其他合计完工金额",
    "本期完工制造费用_人工合计完工金额",
    "本期完工制造费用_机物料及低耗合计完工金额",
    "本期完工制造费用_折旧合计完工金额",
    "本期完工制造费用_水电费合计完工金额",
    "本期完工委外加工费合计完工金额",
    "直接材料单位完工金额",
    "直接人工单位完工金额",
    "制造费用单位完工金额",
    "制造费用_其他单位完工成本",
    "制造费用_人工单位完工成本",
    "制造费用_机物料及低耗单位完工成本",
    "制造费用_折旧单位完工成本",
    "制造费用_水电费单位完工成本",
    "委外加工费单位完工成本",
];

pub fn build_workbook_payload(
    bundle: FactBundle,
    config: &PipelineConfig,
    timings: StageTimings,
    month_filter_empty_result: bool,
) -> Result<WorkbookPayload, CostingError> {
    let quality_metrics = build_quality_metrics(&bundle, month_filter_empty_result);
    let work_order_sheet = build_work_order_anomaly_sheet(&bundle, config);
    let detail_columns = detail_sheet_columns(&bundle.detail_columns);
    let qty_columns = qty_sheet_columns(&bundle.qty_columns, config);

    let FactBundle {
        detail_fact,
        qty_fact,
        error_issues,
        ..
    } = bundle;
    let detail_sheet = build_flat_sheet(
        "成本计算单总表",
        detail_columns,
        detail_fact,
        detail_number_format_columns,
    );
    let qty_rows = build_qty_sheet_rows(qty_fact, config);
    let qty_sheet = build_flat_sheet(
        "成本计算单数量聚合维度",
        qty_columns,
        qty_rows,
        qty_number_format_columns,
    );
    let sheets = vec![detail_sheet, qty_sheet, work_order_sheet];
    ensure_no_product_dimension(&sheets)?;
    let error_log_count = error_issues.len();

    Ok(WorkbookPayload {
        sheet_models: sheets,
        quality_metrics,
        error_log_count,
        error_log: error_issues,
        stage_timings: timings,
    })
}

fn build_flat_sheet(
    sheet_name: &str,
    columns: Vec<String>,
    rows: Vec<TableRow>,
    number_format_columns: fn(&[String]) -> Vec<String>,
) -> SheetModel {
    let sheet_rows = rows
        .into_iter()
        .map(|mut row| {
            columns
                .iter()
                .map(|column| row.values.remove(column).unwrap_or(CellValue::Blank))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    SheetModel {
        sheet_name: sheet_name.to_string(),
        column_types: build_column_types(&columns),
        number_formats: build_number_formats(&number_format_columns(&columns)),
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
        .map(|column| (column.clone(), "text".to_string()))
        .collect()
}

fn build_number_formats(columns: &[String]) -> BTreeMap<String, String> {
    columns
        .iter()
        .map(|column| (column.clone(), "#,##0.00".to_string()))
        .collect()
}

fn detail_number_format_columns(columns: &[String]) -> Vec<String> {
    columns
        .iter()
        .filter(|column| DETAIL_TWO_DECIMAL_COLUMNS.contains(&column.as_str()))
        .cloned()
        .collect()
}

fn qty_number_format_columns(columns: &[String]) -> Vec<String> {
    columns
        .iter()
        .filter(|column| {
            QTY_TWO_DECIMAL_COLUMNS.contains(&column.as_str())
                || ((column.starts_with("本期完工") && column.ends_with("合计完工金额"))
                    || column.ends_with("单位完工成本"))
        })
        .cloned()
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
            detail_columns: vec![
                "月份".to_string(),
                "成本中心名称".to_string(),
                "成本项目名称".to_string(),
                "本期完工金额".to_string(),
            ],
            detail_fact: vec![row(&[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("成本中心名称", CellValue::Text("成型车间".to_string())),
                ("成本项目名称", CellValue::Text("直接材料".to_string())),
                ("本期完工金额", CellValue::Decimal(Decimal::new(100, 0))),
            ])],
            qty_columns: vec![
                "月份".to_string(),
                "成本中心名称".to_string(),
                "成本项目名称".to_string(),
                "本期完工数量".to_string(),
                "本期完工金额".to_string(),
            ],
            qty_input_row_count: 1,
            filtered_invalid_qty_count: 0,
            filtered_missing_total_amount_count: 0,
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
                original_value: String::new(),
                reason: "missing".to_string(),
                action: "filled zero".to_string(),
                retryable: false,
            }],
        }
    }

    fn empty_bundle_with_schema() -> FactBundle {
        FactBundle {
            detail_columns: vec![
                "月份".to_string(),
                "成本中心名称".to_string(),
                "成本项目名称".to_string(),
                "本期完工金额".to_string(),
            ],
            detail_fact: Vec::new(),
            qty_columns: vec![
                "月份".to_string(),
                "成本中心名称".to_string(),
                "本期完工数量".to_string(),
                "本期完工金额".to_string(),
            ],
            qty_input_row_count: 0,
            filtered_invalid_qty_count: 0,
            filtered_missing_total_amount_count: 0,
            qty_fact: Vec::new(),
            work_order_fact: Vec::new(),
            error_issues: Vec::new(),
        }
    }

    fn bundle_with_internal_schema_columns() -> FactBundle {
        let columns = vec![
            "年期".to_string(),
            "月份".to_string(),
            "成本中心名称".to_string(),
            "产品编码".to_string(),
            "产品名称".to_string(),
            "工单编号".to_string(),
            "工单行号".to_string(),
            "供应商编码".to_string(),
            "成本项目名称".to_string(),
            "子项物料编码".to_string(),
            "Filled_成本项目".to_string(),
            "本期完工数量".to_string(),
            "本期完工金额".to_string(),
        ];
        FactBundle {
            detail_columns: columns.clone(),
            detail_fact: Vec::new(),
            qty_columns: columns,
            qty_input_row_count: 0,
            filtered_invalid_qty_count: 0,
            filtered_missing_total_amount_count: 0,
            qty_fact: Vec::new(),
            work_order_fact: Vec::new(),
            error_issues: Vec::new(),
        }
    }

    #[test]
    fn payload_has_exactly_three_default_sheets_without_product_dimension() {
        let payload = build_workbook_payload(
            bundle(),
            &PipelineConfig::for_name(PipelineName::Gb),
            StageTimings::default(),
            false,
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
    fn payload_preserves_error_log_and_sheet_cells_when_consuming_bundle() {
        let source = bundle();
        let expected_error_log = source.error_issues.clone();
        let expected_detail_rows = source.detail_fact.len();

        let payload = build_workbook_payload(
            source,
            &PipelineConfig::for_name(PipelineName::Gb),
            StageTimings::default(),
            false,
        )
        .unwrap();

        assert_eq!(payload.error_log, expected_error_log);
        assert_eq!(payload.error_log_count, expected_error_log.len());
        assert_eq!(payload.sheet_models[0].rows.len(), expected_detail_rows);
    }

    #[test]
    fn payload_carries_quality_errors_and_timings() {
        let mut timings = StageTimings::default();
        timings.insert("reader_rows", 1.0);

        let payload = build_workbook_payload(
            bundle(),
            &PipelineConfig::for_name(PipelineName::Gb),
            timings,
            false,
        )
        .unwrap();

        assert_eq!(payload.error_log_count, 1);
        assert_eq!(payload.error_log.len(), 1);
        assert_eq!(payload.error_log[0].original_value, "");
        assert!(payload
            .quality_metrics
            .iter()
            .any(|metric| metric.metric == "可参与分析占比"));
        assert_eq!(payload.stage_timings.stages.get("reader_rows"), Some(&1.0));
    }

    #[test]
    fn empty_flat_sheets_keep_source_schema() {
        let payload = build_workbook_payload(
            empty_bundle_with_schema(),
            &PipelineConfig::for_name(PipelineName::Gb),
            StageTimings::default(),
            false,
        )
        .unwrap();

        let detail = &payload.sheet_models[0];
        assert_eq!(
            detail.columns,
            vec!["月份", "成本中心名称", "成本项目名称", "本期完工金额"]
        );
        assert!(detail.rows.is_empty());

        let qty = &payload.sheet_models[1];
        assert_eq!(
            &qty.columns[..4],
            ["月份", "成本中心名称", "本期完工数量", "本期完工金额"]
        );
        assert!(qty
            .columns
            .contains(&"本期完工直接材料合计完工金额".to_string()));
        assert!(qty.rows.is_empty());
    }

    #[test]
    fn flat_sheet_metadata_matches_default_python_contract() {
        let payload = build_workbook_payload(
            bundle(),
            &PipelineConfig::for_name(PipelineName::Gb),
            StageTimings::default(),
            false,
        )
        .unwrap();
        let detail = &payload.sheet_models[0];
        let qty = &payload.sheet_models[1];

        assert_eq!(detail.column_types["成本中心名称"], "text");
        assert_eq!(detail.column_types["成本项目名称"], "text");
        assert_eq!(detail.column_types["本期完工金额"], "text");
        assert!(!detail.number_formats.contains_key("成本中心名称"));
        assert!(!detail.number_formats.contains_key("成本项目名称"));
        assert_eq!(detail.number_formats["本期完工金额"], "#,##0.00");

        assert_eq!(qty.column_types["成本中心名称"], "text");
        assert_eq!(qty.column_types["本期完工数量"], "text");
        assert!(!qty.number_formats.contains_key("成本中心名称"));
        assert_eq!(qty.number_formats["本期完工金额"], "#,##0.00");
        assert_eq!(
            qty.number_formats["本期完工直接材料合计完工金额"],
            "#,##0.00"
        );
    }

    #[test]
    fn flat_sheets_do_not_expose_internal_or_cross_sheet_columns() {
        let payload = build_workbook_payload(
            bundle_with_internal_schema_columns(),
            &PipelineConfig::for_name(PipelineName::Gb),
            StageTimings::default(),
            false,
        )
        .unwrap();

        let detail = &payload.sheet_models[0];
        assert_eq!(
            detail.columns,
            vec![
                "年期",
                "月份",
                "成本中心名称",
                "产品编码",
                "产品名称",
                "工单编号",
                "工单行号",
                "供应商编码",
                "成本项目名称",
                "子项物料编码",
                "本期完工数量",
                "本期完工金额",
            ]
        );

        let qty = &payload.sheet_models[1];
        assert_eq!(
            qty.columns,
            vec![
                "年期",
                "月份",
                "成本中心名称",
                "产品编码",
                "产品名称",
                "工单编号",
                "工单行号",
                "本期完工数量",
                "本期完工金额",
                "本期完工直接材料合计完工金额",
                "本期完工直接人工合计完工金额",
                "本期完工制造费用合计完工金额",
                "本期完工制造费用_其他合计完工金额",
                "本期完工制造费用_人工合计完工金额",
                "本期完工制造费用_机物料及低耗合计完工金额",
                "本期完工制造费用_折旧合计完工金额",
                "本期完工制造费用_水电费合计完工金额",
                "本期完工委外加工费合计完工金额",
                "直接材料单位完工金额",
                "直接人工单位完工金额",
                "制造费用单位完工金额",
                "制造费用_其他单位完工成本",
                "制造费用_人工单位完工成本",
                "制造费用_机物料及低耗单位完工成本",
                "制造费用_折旧单位完工成本",
                "制造费用_水电费单位完工成本",
                "委外加工费单位完工成本",
                "制造费用明细项合计是否等于制造费用合计",
                "直接材料+直接人工+制造费用+委外加工费是否等于总完工成本",
                "数据校验状态",
                "异常原因说明",
            ]
        );
    }
}

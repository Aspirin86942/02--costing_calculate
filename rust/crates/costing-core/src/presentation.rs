use std::collections::BTreeMap;

use crate::anomaly::build_work_order_anomaly_sheet;
use crate::error::CostingError;
use crate::fact::qty_sheet_columns;
use crate::model::{CellValue, FactBundle, QtyFactRow, SheetModel, StageTimings, WorkbookPayload};
use crate::pipeline::PipelineConfig;
use crate::quality::build_quality_metrics;
use crate::table::{ColumnId, ColumnSchema, IndexedRow, ProjectionPlan};

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
    let quality_metrics = build_quality_metrics(&bundle, month_filter_empty_result)?;
    let work_order_sheet = build_work_order_anomaly_sheet(&bundle, config)?;
    let detail_columns = column_names(&bundle.schema, &bundle.detail_display_columns)?;
    let qty_base_columns = column_names(&bundle.schema, &bundle.qty_display_columns)?;
    let qty_columns = qty_sheet_columns(&qty_base_columns, config);

    let FactBundle {
        schema,
        detail_display_columns,
        detail_rows,
        qty_display_columns,
        qty_rows,
        error_issues,
        ..
    } = bundle;
    let detail_sheet = build_flat_sheet(
        "成本计算单总表",
        &schema,
        &detail_display_columns,
        detail_columns,
        detail_rows,
        detail_number_format_columns,
    )?;
    let qty_sheet = build_typed_qty_sheet(
        "成本计算单数量聚合维度",
        &schema,
        &qty_display_columns,
        qty_base_columns.len(),
        qty_columns,
        qty_rows,
        config,
        qty_number_format_columns,
    )?;
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
    schema: &ColumnSchema,
    display_columns: &[ColumnId],
    columns: Vec<String>,
    rows: Vec<IndexedRow>,
    number_format_columns: fn(&[String]) -> Vec<String>,
) -> Result<SheetModel, CostingError> {
    let plan = ProjectionPlan::new(schema, display_columns)?;
    let sheet_rows = rows
        .into_iter()
        .map(|row| plan.project_row(row))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SheetModel {
        sheet_name: sheet_name.to_string(),
        column_types: build_column_types(&columns),
        number_formats: build_number_formats(&number_format_columns(&columns)),
        columns,
        rows: sheet_rows,
        freeze_panes: Some("A2".to_string()),
        auto_filter: true,
        fixed_width: Some(15.0),
    })
}

fn build_typed_qty_sheet(
    sheet_name: &str,
    schema: &ColumnSchema,
    display_columns: &[ColumnId],
    base_column_count: usize,
    columns: Vec<String>,
    rows: Vec<QtyFactRow>,
    config: &PipelineConfig,
    number_format_columns: fn(&[String]) -> Vec<String>,
) -> Result<SheetModel, CostingError> {
    let plan = ProjectionPlan::new(schema, display_columns)?;
    let mut sheet_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut derived = Vec::with_capacity(columns.len() - base_column_count);
        append_typed_qty_cells(&mut derived, &row, config);
        let mut cells = plan.project_row(row.source)?;
        cells.extend(derived);
        sheet_rows.push(cells);
    }
    Ok(SheetModel {
        sheet_name: sheet_name.to_string(),
        column_types: build_column_types(&columns),
        number_formats: build_number_formats(&number_format_columns(&columns)),
        columns,
        rows: sheet_rows,
        freeze_panes: Some("A2".to_string()),
        auto_filter: true,
        fixed_width: Some(15.0),
    })
}

fn append_typed_qty_cells(cells: &mut Vec<CellValue>, row: &QtyFactRow, config: &PipelineConfig) {
    let amounts = &row.amounts;
    for amount in [
        amounts.direct_material,
        amounts.direct_labor,
        amounts.manufacturing_overhead,
        amounts.moh_other,
        amounts.moh_labor,
        amounts.moh_consumables,
        amounts.moh_depreciation,
        amounts.moh_utilities,
    ] {
        cells.push(CellValue::Decimal(amount));
    }
    for index in 0..config.standalone_cost_items.len() {
        cells.push(CellValue::Decimal(amounts.standalone_amount(index)));
    }

    for amount in [
        amounts.direct_material,
        amounts.direct_labor,
        amounts.manufacturing_overhead,
        amounts.moh_other,
        amounts.moh_labor,
        amounts.moh_consumables,
        amounts.moh_depreciation,
        amounts.moh_utilities,
    ] {
        cells.push(decimal_or_blank(safe_divide(amount, row.completed_qty)));
    }
    for index in 0..config.standalone_cost_items.len() {
        cells.push(decimal_or_blank(safe_divide(
            amounts.standalone_amount(index),
            row.completed_qty,
        )));
    }

    cells.push(CellValue::Text(yes_no(row.moh_matches).to_string()));
    cells.push(CellValue::Text(yes_no(row.total_matches).to_string()));
    cells.push(CellValue::Text(
        if row.check_reason.is_empty() {
            "通过"
        } else {
            "需复核"
        }
        .to_string(),
    ));
    cells.push(CellValue::Text(row.check_reason.clone()));
}

fn safe_divide(
    numerator: rust_decimal::Decimal,
    denominator: rust_decimal::Decimal,
) -> Option<rust_decimal::Decimal> {
    if denominator == rust_decimal::Decimal::ZERO {
        None
    } else {
        numerator.checked_div(denominator)
    }
}

fn decimal_or_blank(value: Option<rust_decimal::Decimal>) -> CellValue {
    value.map(CellValue::Decimal).unwrap_or(CellValue::Blank)
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "是"
    } else {
        "否"
    }
}

fn column_names(
    schema: &ColumnSchema,
    display_columns: &[ColumnId],
) -> Result<Vec<String>, CostingError> {
    display_columns
        .iter()
        .map(|id| schema.name(*id).map(str::to_string))
        .collect()
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

    use crate::model::{CellValue, CostAmounts, ErrorIssue, FactBundle, QtyFactRow, StageTimings};
    use crate::pipeline::{PipelineConfig, PipelineName};
    use crate::sheet_contract::{detail_sheet_columns, qty_sheet_base_columns};
    use crate::table::IndexedTable;

    use super::*;

    type NamedTestRow = BTreeMap<String, CellValue>;

    fn row(values: &[(&str, CellValue)]) -> NamedTestRow {
        values
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect()
    }

    fn test_bundle(
        columns: Vec<String>,
        detail: Vec<NamedTestRow>,
        qty: Vec<NamedTestRow>,
        work_order: Vec<NamedTestRow>,
        error_issues: Vec<ErrorIssue>,
    ) -> FactBundle {
        let detail_len = detail.len();
        let qty_len = qty.len();
        let unique_work_order_count = work_order.len().min(qty_len);
        let merged_qty = qty
            .iter()
            .enumerate()
            .map(|(index, row)| {
                let mut merged = row.clone();
                if let Some(extra) = work_order.get(index) {
                    merged.extend(extra.clone());
                }
                merged
            })
            .collect::<Vec<_>>();
        let named_rows = detail
            .iter()
            .chain(&merged_qty)
            .cloned()
            .collect::<Vec<_>>();
        let positional = named_rows
            .iter()
            .map(|named| {
                columns
                    .iter()
                    .map(|column| named.get(column).cloned().unwrap_or(CellValue::Blank))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let table = IndexedTable::from_raw(columns.clone(), positional).unwrap();
        let (schema, _, mut indexed_rows) = table.into_parts();
        let qty_sources = indexed_rows.split_off(detail_len);
        let qty_rows = qty_sources
            .into_iter()
            .zip(&merged_qty)
            .map(|(source, named)| QtyFactRow {
                source,
                work_order_key: test_text(named, "工单编号"),
                completed_qty: test_decimal(named, "completed_qty"),
                completed_total: test_decimal(named, "completed_amount_total"),
                amounts: CostAmounts {
                    direct_material: test_decimal(named, "dm_amount"),
                    direct_labor: test_decimal(named, "dl_amount"),
                    manufacturing_overhead: test_decimal(named, "moh_amount"),
                    moh_other: test_decimal(named, "moh_other_amount"),
                    moh_labor: test_decimal(named, "moh_labor_amount"),
                    moh_consumables: test_decimal(named, "moh_consumables_amount"),
                    moh_depreciation: test_decimal(named, "moh_depreciation_amount"),
                    moh_utilities: test_decimal(named, "moh_utilities_amount"),
                    standalone: vec![
                        test_decimal(named, "outsource_amount"),
                        test_decimal(named, "software_amount"),
                    ],
                },
                moh_matches: test_bool(named, "moh_matches", true),
                total_matches: test_bool(named, "total_matches", true),
                check_reason: test_text(named, "check_reason"),
            })
            .collect();
        let detail_names = detail_sheet_columns(&columns);
        let qty_names = qty_sheet_base_columns(&columns);
        FactBundle {
            detail_display_columns: schema.display_order_for(&detail_names).unwrap(),
            qty_display_columns: schema.display_order_for(&qty_names).unwrap(),
            schema,
            detail_rows: indexed_rows,
            qty_rows,
            unique_work_order_indices: (0..unique_work_order_count).collect(),
            qty_input_row_count: qty_len,
            filtered_invalid_qty_count: 0,
            filtered_missing_total_amount_count: 0,
            duplicate_work_order_row_count: 0,
            error_issues,
        }
    }

    fn test_decimal(row: &NamedTestRow, key: &str) -> Decimal {
        match row.get(key) {
            Some(CellValue::Decimal(value)) => *value,
            Some(CellValue::Text(value)) => value.parse().unwrap_or(Decimal::ZERO),
            _ => Decimal::ZERO,
        }
    }

    fn test_text(row: &NamedTestRow, key: &str) -> String {
        match row.get(key) {
            Some(CellValue::Text(value)) | Some(CellValue::DateLike(value)) => value.clone(),
            Some(CellValue::Decimal(value)) => value.normalize().to_string(),
            _ => String::new(),
        }
    }

    fn test_bool(row: &NamedTestRow, key: &str, default: bool) -> bool {
        match row.get(key) {
            Some(CellValue::Text(value)) => value == "true" || value == "是",
            _ => default,
        }
    }

    fn bundle() -> FactBundle {
        test_bundle(
            vec![
                "月份".to_string(),
                "成本中心名称".to_string(),
                "成本项目名称".to_string(),
                "产品编码".to_string(),
                "产品名称".to_string(),
                "工单编号".to_string(),
                "工单行号".to_string(),
                "本期完工金额".to_string(),
                "本期完工数量".to_string(),
            ],
            vec![row(&[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("成本中心名称", CellValue::Text("成型车间".to_string())),
                ("成本项目名称", CellValue::Text("直接材料".to_string())),
                ("本期完工金额", CellValue::Decimal(Decimal::new(100, 0))),
            ])],
            vec![row(&[
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
            vec![row(&[
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
            vec![ErrorIssue {
                row_id: "row-1".to_string(),
                issue_type: "MISSING_AMOUNT".to_string(),
                field_name: "本期完工金额".to_string(),
                original_value: String::new(),
                reason: "missing".to_string(),
                action: "filled zero".to_string(),
                retryable: false,
            }],
        )
    }

    fn empty_bundle_with_schema() -> FactBundle {
        test_bundle(
            vec![
                "月份".to_string(),
                "成本中心名称".to_string(),
                "成本项目名称".to_string(),
                "本期完工数量".to_string(),
                "本期完工金额".to_string(),
            ],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
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
        test_bundle(columns, Vec::new(), Vec::new(), Vec::new(), Vec::new())
    }

    fn typed_projection_bundle(check_reason: &str) -> FactBundle {
        let columns = [
            "月份",
            "产品编码",
            "产品名称",
            "工单编号",
            "工单行号",
            "本期完工数量",
            "本期完工金额",
            "单据类型",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        let typed_values = [
            (
                "completed_amount_total",
                CellValue::Decimal(Decimal::new(99, 0)),
            ),
            ("dm_amount", CellValue::Decimal(Decimal::new(11, 0))),
            ("dl_amount", CellValue::Decimal(Decimal::new(12, 0))),
            ("moh_amount", CellValue::Decimal(Decimal::new(13, 0))),
            ("moh_other_amount", CellValue::Decimal(Decimal::new(1, 0))),
            ("moh_labor_amount", CellValue::Decimal(Decimal::new(2, 0))),
            (
                "moh_consumables_amount",
                CellValue::Decimal(Decimal::new(3, 0)),
            ),
            (
                "moh_depreciation_amount",
                CellValue::Decimal(Decimal::new(4, 0)),
            ),
            ("moh_utilities_amount", CellValue::Decimal(Decimal::ZERO)),
            ("outsource_amount", CellValue::Decimal(Decimal::new(6, 0))),
            ("software_amount", CellValue::Decimal(Decimal::new(7, 0))),
            ("moh_matches", CellValue::Text("否".to_string())),
            ("total_matches", CellValue::Text("否".to_string())),
            ("check_reason", CellValue::Text(check_reason.to_string())),
        ];
        let make_qty = |order: &str, source_qty: &str, completed_qty: Decimal| {
            let mut values = row(&[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("产品编码", CellValue::Text("P1".to_string())),
                ("产品名称", CellValue::Text("产品".to_string())),
                ("工单编号", CellValue::Text(order.to_string())),
                ("工单行号", CellValue::Text("1".to_string())),
                ("本期完工数量", CellValue::Text(source_qty.to_string())),
                ("本期完工金额", CellValue::Text("99.00".to_string())),
                ("单据类型", CellValue::Text("汇报入库-普通生产".to_string())),
                ("completed_qty", CellValue::Decimal(completed_qty)),
            ]);
            values.extend(
                typed_values
                    .iter()
                    .cloned()
                    .map(|(key, value)| (key.to_string(), value)),
            );
            values
        };
        test_bundle(
            columns,
            Vec::new(),
            vec![
                make_qty("WO1", "2.00", Decimal::new(2, 0)),
                make_qty("WO2", "0.00", Decimal::ZERO),
            ],
            Vec::new(),
            Vec::new(),
        )
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
        let expected_detail_rows = source.detail_row_count();

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
    fn qty_sheet_projects_typed_amounts_without_overlay_map() {
        for (pipeline, reason, standalone_amounts, standalone_units) in [
            (
                PipelineName::Gb,
                "制造费用明细与合计不一致;直接材料+直接人工+制造费用+委外加工费与总完工成本不一致",
                vec!["本期完工委外加工费合计完工金额"],
                vec!["委外加工费单位完工成本"],
            ),
            (
                PipelineName::Sk,
                "制造费用明细与合计不一致;直接材料+直接人工+制造费用+委外加工费+软件费用与总完工成本不一致",
                vec![
                    "本期完工委外加工费合计完工金额",
                    "本期完工软件费用合计完工金额",
                ],
                vec!["委外加工费单位完工成本", "软件费用单位完工成本"],
            ),
        ] {
            let config = PipelineConfig::for_name(pipeline);
            let payload = build_workbook_payload(
                typed_projection_bundle(reason),
                &config,
                StageTimings::default(),
                false,
            )
            .unwrap();
            let sheet = &payload.sheet_models[1];
            let amount_start = sheet
                .columns
                .iter()
                .position(|column| column == "本期完工直接材料合计完工金额")
                .unwrap();
            let mut expected_suffix = vec![
                "本期完工直接材料合计完工金额",
                "本期完工直接人工合计完工金额",
                "本期完工制造费用合计完工金额",
                "本期完工制造费用_其他合计完工金额",
                "本期完工制造费用_人工合计完工金额",
                "本期完工制造费用_机物料及低耗合计完工金额",
                "本期完工制造费用_折旧合计完工金额",
                "本期完工制造费用_水电费合计完工金额",
            ];
            expected_suffix.extend(standalone_amounts.iter().copied());
            expected_suffix.extend([
                "直接材料单位完工金额",
                "直接人工单位完工金额",
                "制造费用单位完工金额",
                "制造费用_其他单位完工成本",
                "制造费用_人工单位完工成本",
                "制造费用_机物料及低耗单位完工成本",
                "制造费用_折旧单位完工成本",
                "制造费用_水电费单位完工成本",
            ]);
            expected_suffix.extend(standalone_units.iter().copied());
            let total_match_column = match pipeline {
                PipelineName::Gb => "直接材料+直接人工+制造费用+委外加工费是否等于总完工成本",
                PipelineName::Sk => {
                    "直接材料+直接人工+制造费用+委外加工费+软件费用是否等于总完工成本"
                }
            };
            expected_suffix.extend([
                "制造费用明细项合计是否等于制造费用合计",
                total_match_column,
                "数据校验状态",
                "异常原因说明",
            ]);
            assert_eq!(
                sheet.columns[amount_start..],
                expected_suffix
                    .iter()
                    .map(|value| (*value).to_string())
                    .collect::<Vec<_>>()
            );

            let mut expected_cells = vec![
                CellValue::Decimal(Decimal::new(11, 0)),
                CellValue::Decimal(Decimal::new(12, 0)),
                CellValue::Decimal(Decimal::new(13, 0)),
                CellValue::Decimal(Decimal::new(1, 0)),
                CellValue::Decimal(Decimal::new(2, 0)),
                CellValue::Decimal(Decimal::new(3, 0)),
                CellValue::Decimal(Decimal::new(4, 0)),
                CellValue::Decimal(Decimal::ZERO),
                CellValue::Decimal(Decimal::new(6, 0)),
            ];
            if pipeline == PipelineName::Sk {
                expected_cells.push(CellValue::Decimal(Decimal::new(7, 0)));
            }
            expected_cells.extend([
                CellValue::Decimal(Decimal::new(55, 1)),
                CellValue::Decimal(Decimal::new(6, 0)),
                CellValue::Decimal(Decimal::new(65, 1)),
                CellValue::Decimal(Decimal::new(5, 1)),
                CellValue::Decimal(Decimal::new(1, 0)),
                CellValue::Decimal(Decimal::new(15, 1)),
                CellValue::Decimal(Decimal::new(2, 0)),
                CellValue::Decimal(Decimal::ZERO),
                CellValue::Decimal(Decimal::new(3, 0)),
            ]);
            if pipeline == PipelineName::Sk {
                expected_cells.push(CellValue::Decimal(Decimal::new(35, 1)));
            }
            expected_cells.extend([
                CellValue::Text("否".to_string()),
                CellValue::Text("否".to_string()),
                CellValue::Text("需复核".to_string()),
                CellValue::Text(reason.to_string()),
            ]);
            assert_eq!(sheet.rows[0][amount_start..], expected_cells);
            assert!(sheet
                .rows
                .iter()
                .all(|cells| cells.len() == sheet.columns.len()));

            let source_qty = sheet
                .columns
                .iter()
                .position(|column| column == "本期完工数量")
                .unwrap();
            let source_total = sheet
                .columns
                .iter()
                .position(|column| column == "本期完工金额")
                .unwrap();
            assert_eq!(sheet.rows[0][source_qty], CellValue::Text("2.00".to_string()));
            assert_eq!(sheet.rows[0][source_total], CellValue::Text("99.00".to_string()));
            let amount_count = 8 + config.standalone_cost_items.len();
            let unit_start = amount_start + amount_count;
            assert!(sheet.rows[1][unit_start..unit_start + amount_count]
                .iter()
                .all(|value| *value == CellValue::Blank));
            assert_eq!(
                sheet.number_formats["本期完工直接材料合计完工金额"],
                "#,##0.00"
            );
            assert_eq!(sheet.number_formats[standalone_units[0]], "#,##0.00");
        }
    }

    #[test]
    fn qty_sheet_uses_blank_when_typed_unit_cost_overflows() {
        let mut source = typed_projection_bundle("");
        source.qty_rows.truncate(1);
        source.qty_rows[0].completed_qty = Decimal::new(1, 28);
        source.qty_rows[0].amounts.direct_material = Decimal::MAX;
        let payload = build_workbook_payload(
            source,
            &PipelineConfig::for_name(PipelineName::Gb),
            StageTimings::default(),
            false,
        )
        .unwrap();
        let sheet = &payload.sheet_models[1];
        let unit_cost = sheet
            .columns
            .iter()
            .position(|column| column == "直接材料单位完工金额")
            .unwrap();

        assert_eq!(sheet.rows[0][unit_cost], CellValue::Blank);
    }

    #[test]
    fn work_order_sheet_borrows_qty_fact_by_unique_indices() {
        let mut source = bundle();
        source.unique_work_order_indices = vec![0];
        let config = PipelineConfig {
            product_order: &[],
            ..PipelineConfig::for_name(PipelineName::Gb)
        };

        let payload =
            build_workbook_payload(source, &config, StageTimings::default(), false).unwrap();

        assert_eq!(payload.sheet_models[2].rows.len(), 1);
    }

    #[test]
    fn presentation_preserves_three_sheet_order_after_fact_model_change() {
        let payload = build_workbook_payload(
            bundle(),
            &PipelineConfig::for_name(PipelineName::Gb),
            StageTimings::default(),
            false,
        )
        .unwrap();

        assert_eq!(
            payload
                .sheet_models
                .iter()
                .map(|sheet| sheet.sheet_name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "成本计算单总表",
                "成本计算单数量聚合维度",
                "成本分析工单维度",
            ]
        );
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
            vec![
                "月份",
                "成本中心名称",
                "成本项目名称",
                "本期完工数量",
                "本期完工金额"
            ]
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

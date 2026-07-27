use std::collections::BTreeMap;
use std::str::FromStr;

use rust_decimal::Decimal;

use crate::model::{CellValue, CostAmounts, ErrorIssue, FactBundle, QtyFactRow};
use crate::pipeline::{PipelineName, PipelineRules};
use crate::table::IndexedTable;

use super::*;

const TEST_PRODUCT_ORDER: &[(&str, &str)] = &[("P1", "产品"), ("P-NEAR-MAD", "近零MAD产品")];
type NamedTestRow = BTreeMap<String, CellValue>;

fn test_config(name: PipelineName) -> PipelineRules {
    PipelineRules {
        product_order: crate::pipeline::owned_product_order(TEST_PRODUCT_ORDER),
        ..PipelineRules::for_name(name)
    }
}

fn row(
    order_no: &str,
    unit_cost: i64,
    doc_type: &str,
    extra: &[(&str, CellValue)],
) -> NamedTestRow {
    let mut values = BTreeMap::from([
        (
            "月份".to_string(),
            CellValue::Text("2025年01期".to_string()),
        ),
        ("产品编码".to_string(), CellValue::Text("P1".to_string())),
        ("产品名称".to_string(), CellValue::Text("产品".to_string())),
        (
            "工单编号".to_string(),
            CellValue::Text(order_no.to_string()),
        ),
        ("工单行号".to_string(), CellValue::Text("1".to_string())),
        (
            "单据类型".to_string(),
            CellValue::Text(doc_type.to_string()),
        ),
        (
            "completed_qty".to_string(),
            CellValue::Decimal(Decimal::new(1, 0)),
        ),
        (
            "completed_amount_total".to_string(),
            CellValue::Decimal(Decimal::new(unit_cost, 0)),
        ),
        (
            "dm_amount".to_string(),
            CellValue::Decimal(Decimal::new(unit_cost, 0)),
        ),
        ("dl_amount".to_string(), CellValue::Decimal(Decimal::ZERO)),
        ("moh_amount".to_string(), CellValue::Decimal(Decimal::ZERO)),
    ]);
    for (key, value) in extra {
        values.insert((*key).to_string(), value.clone());
    }
    values
}

fn bundle(rows: Vec<NamedTestRow>) -> FactBundle {
    let mut columns = Vec::new();
    for column in rows.iter().flat_map(BTreeMap::keys) {
        if !columns.contains(column) {
            columns.push(column.clone());
        }
    }
    let positional = rows
        .iter()
        .cloned()
        .map(|mut named| {
            columns
                .iter()
                .map(|column| named.remove(column).unwrap_or(CellValue::Blank))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let table = IndexedTable::from_raw(columns, positional).unwrap();
    let (schema, display, sources) = table.into_parts();
    let qty_rows = sources
        .into_iter()
        .zip(&rows)
        .map(|(source, named)| QtyFactRow {
            source,
            work_order_key: text_value(named, "工单编号"),
            completed_qty: decimal_value(named, "completed_qty"),
            completed_total: decimal_value(named, "completed_amount_total"),
            amounts: CostAmounts {
                direct_material: decimal_value(named, "dm_amount"),
                direct_labor: decimal_value(named, "dl_amount"),
                manufacturing_overhead: decimal_value(named, "moh_amount"),
                moh_other: decimal_value(named, "moh_other_amount"),
                moh_labor: decimal_value(named, "moh_labor_amount"),
                moh_consumables: decimal_value(named, "moh_consumables_amount"),
                moh_depreciation: decimal_value(named, "moh_depreciation_amount"),
                moh_utilities: decimal_value(named, "moh_utilities_amount"),
                standalone: vec![
                    decimal_value(named, "outsource_amount"),
                    decimal_value(named, "software_amount"),
                ],
            },
            moh_matches: true,
            total_matches: true,
            check_reason: String::new(),
        })
        .collect::<Vec<_>>();
    let unique_work_order_indices = (0..qty_rows.len()).collect();
    FactBundle {
        schema,
        detail_display_columns: Vec::new(),
        detail_rows: vec![],
        qty_display_columns: display,
        qty_rows,
        unique_work_order_indices,
        qty_input_row_count: rows.len(),
        filtered_invalid_qty_count: 0,
        filtered_missing_total_amount_count: 0,
        duplicate_work_order_row_count: 0,
        error_issues: Vec::<ErrorIssue>::new(),
    }
}

fn decimal_value(row: &NamedTestRow, key: &str) -> Decimal {
    match row.get(key) {
        Some(CellValue::Decimal(value)) => *value,
        Some(CellValue::Text(value)) => value.parse().unwrap_or(Decimal::ZERO),
        _ => Decimal::ZERO,
    }
}

fn text_value(row: &NamedTestRow, key: &str) -> String {
    match row.get(key) {
        Some(CellValue::Text(value)) | Some(CellValue::DateLike(value)) => value.clone(),
        Some(CellValue::Decimal(value)) => value.normalize().to_string(),
        _ => String::new(),
    }
}

fn decimal_row(
    order_no: &str,
    product_code: &str,
    product_name: &str,
    qty: &str,
    unit_cost: &str,
    doc_type: &str,
) -> NamedTestRow {
    let qty = Decimal::from_str(qty).unwrap();
    let unit_cost = Decimal::from_str(unit_cost).unwrap();
    let total_amount = qty * unit_cost;
    let mut values = row(order_no, 1, doc_type, &[]);
    values.insert(
        "产品编码".to_string(),
        CellValue::Text(product_code.to_string()),
    );
    values.insert(
        "产品名称".to_string(),
        CellValue::Text(product_name.to_string()),
    );
    values.insert("completed_qty".to_string(), CellValue::Decimal(qty));
    values.insert(
        "completed_amount_total".to_string(),
        CellValue::Decimal(total_amount),
    );
    values.insert("dm_amount".to_string(), CellValue::Decimal(total_amount));
    values
}

fn column_index(sheet: &SheetModel, column: &str) -> usize {
    sheet
        .columns
        .iter()
        .position(|value| value == column)
        .unwrap()
}

#[test]
fn work_order_sheet_contains_required_audit_columns() {
    let sheet = build_work_order_anomaly_sheet(
        &bundle(vec![row("WO1", 100, "汇报入库-普通生产", &[])]),
        &test_config(PipelineName::Gb),
    )
    .unwrap();

    assert_eq!(sheet.sheet_name, "成本分析工单维度");
    assert!(sheet.columns.contains(&"异常等级".to_string()));
    assert!(sheet.columns.contains(&"异常主要来源".to_string()));
    assert!(sheet.columns.contains(&"异常明细解释".to_string()));
    assert!(sheet.columns.contains(&"复核原因".to_string()));
    assert_eq!(sheet.freeze_panes, Some("A2".to_string()));
    assert!(sheet.auto_filter);
    assert_eq!(sheet.fixed_width, Some(15.0));
    assert_eq!(sheet.column_types["成本中心"], "text");
    assert!(!sheet.number_formats.contains_key("成本中心"));
}

#[test]
fn analysis_sheet_filters_exact_product_pairs_and_keeps_whitelist_order() {
    const PRODUCT_ORDER: &[(&str, &str)] = &[("P2", "产品二"), ("P1", "产品一")];
    let config = PipelineRules {
        product_order: crate::pipeline::owned_product_order(PRODUCT_ORDER),
        ..PipelineRules::for_name(PipelineName::Gb)
    };
    let rows = vec![
        row(
            "WO-P1",
            100,
            "汇报入库-普通生产",
            &[
                ("产品编码", CellValue::Text("P1".to_string())),
                ("产品名称", CellValue::Text("产品一".to_string())),
            ],
        ),
        row(
            "WO-WRONG-NAME",
            100,
            "汇报入库-普通生产",
            &[
                ("产品编码", CellValue::Text("P1".to_string())),
                ("产品名称", CellValue::Text("名称不匹配".to_string())),
            ],
        ),
        row(
            "WO-P2",
            100,
            "汇报入库-普通生产",
            &[
                ("产品编码", CellValue::Text("P2".to_string())),
                ("产品名称", CellValue::Text("产品二".to_string())),
            ],
        ),
        row(
            "WO-NOT-LISTED",
            100,
            "汇报入库-普通生产",
            &[
                ("产品编码", CellValue::Text("P3".to_string())),
                ("产品名称", CellValue::Text("产品三".to_string())),
            ],
        ),
    ];

    let sheet = build_work_order_anomaly_sheet(&bundle(rows), &config).unwrap();
    let product_code_idx = column_index(&sheet, "产品编码");

    assert_eq!(sheet.rows.len(), 2);
    assert_eq!(
        sheet.rows[0][product_code_idx],
        CellValue::Text("P2".to_string())
    );
    assert_eq!(
        sheet.rows[1][product_code_idx],
        CellValue::Text("P1".to_string())
    );
}

#[test]
fn analysis_sheet_sorts_each_product_by_month_order_and_numeric_order_line() {
    let rows = vec![
        row(
            "WO-B",
            100,
            "汇报入库-普通生产",
            &[
                ("月份", CellValue::Text("2025年02期".to_string())),
                ("工单行号", CellValue::Text("1".to_string())),
            ],
        ),
        row(
            "WO-A",
            100,
            "汇报入库-普通生产",
            &[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("工单行号", CellValue::Text("10".to_string())),
            ],
        ),
        row(
            "WO-A",
            100,
            "汇报入库-普通生产",
            &[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("工单行号", CellValue::Text("2".to_string())),
            ],
        ),
    ];

    let sheet =
        build_work_order_anomaly_sheet(&bundle(rows), &test_config(PipelineName::Gb)).unwrap();
    let month_index = column_index(&sheet, "月份");
    let order_index = column_index(&sheet, "工单编号");
    let order_line_index = column_index(&sheet, "工单行");

    assert_eq!(
        sheet
            .rows
            .iter()
            .map(|row| (
                row[month_index].clone(),
                row[order_index].clone(),
                row[order_line_index].clone(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                CellValue::Text("2025年01期".to_string()),
                CellValue::Text("WO-A".to_string()),
                CellValue::Text("2".to_string()),
            ),
            (
                CellValue::Text("2025年01期".to_string()),
                CellValue::Text("WO-A".to_string()),
                CellValue::Text("10".to_string()),
            ),
            (
                CellValue::Text("2025年02期".to_string()),
                CellValue::Text("WO-B".to_string()),
                CellValue::Text("1".to_string()),
            ),
        ]
    );
}

#[test]
fn grades_attention_and_suspicious_by_product_scope() {
    let sheet = build_work_order_anomaly_sheet(
        &bundle(vec![
            row("WO100", 100, "汇报入库-普通生产", &[]),
            row("WO101", 101, "汇报入库-普通生产", &[]),
            row("WO102", 102, "汇报入库-普通生产", &[]),
            row("WO103", 103, "汇报入库-普通生产", &[]),
            row("WO106", 106, "汇报入库-普通生产", &[]),
            row("WO115", 115, "汇报入库-普通生产", &[]),
            row("WO130", 130, "汇报入库-普通生产", &[]),
        ]),
        &test_config(PipelineName::Gb),
    )
    .unwrap();
    let level_idx = column_index(&sheet, "异常等级");
    let source_idx = column_index(&sheet, "异常主要来源");
    let detail_idx = column_index(&sheet, "异常明细解释");

    assert_eq!(
        sheet.rows[5][level_idx],
        CellValue::Text("关注".to_string())
    );
    assert_eq!(
        sheet.rows[6][level_idx],
        CellValue::Text("高度可疑".to_string())
    );
    assert_eq!(
        sheet.rows[6][source_idx],
        CellValue::Text("总成本异常".to_string())
    );
    let CellValue::Text(detail) = &sheet.rows[6][detail_idx] else {
        panic!("detail explanation should be text");
    };
    assert!(detail.contains("总成本:"));
    assert!(detail.contains("当前值=130.00"));
    assert!(detail.contains("基准值=103.00"));
    assert!(detail.contains("log偏离="));
    assert!(detail.contains("相对偏离="));
    assert!(detail.contains("score="));
}

#[test]
fn unknown_doc_type_is_not_analyzable() {
    let sheet = build_work_order_anomaly_sheet(
        &bundle(vec![row("WO1", 100, "其他入库", &[])]),
        &test_config(PipelineName::Gb),
    )
    .unwrap();
    let can_analyze_idx = column_index(&sheet, "是否可参与分析");
    let level_idx = column_index(&sheet, "异常等级");
    let reason_idx = column_index(&sheet, "复核原因");

    assert_eq!(
        sheet.rows[0][can_analyze_idx],
        CellValue::Text("否".to_string())
    );
    assert_eq!(sheet.rows[0][level_idx], CellValue::Text(String::new()));
    let CellValue::Text(reason) = &sheet.rows[0][reason_idx] else {
        panic!("reason should be text");
    };
    assert_eq!(
        reason,
        "单据类型未归类，不参与正常生产/返工生产异常池;直接人工单位完工成本小于等于0或为空;制造费用单位完工成本小于等于0或为空;制造费用_其他单位完工成本小于等于0或为空;制造费用_人工单位完工成本小于等于0或为空;制造费用_机物料及低耗单位完工成本小于等于0或为空;制造费用_折旧单位完工成本小于等于0或为空;制造费用_水电费单位完工成本小于等于0或为空"
    );
}

#[test]
fn sk_standalone_software_columns_are_visible_without_anomaly_flags() {
    let sheet = build_work_order_anomaly_sheet(
        &bundle(vec![row(
            "WO1",
            100,
            "汇报入库-普通生产",
            &[("software_amount", CellValue::Decimal(Decimal::new(5, 0)))],
        )]),
        &test_config(PipelineName::Sk),
    )
    .unwrap();

    assert!(sheet.columns.contains(&"软件费用合计完工金额".to_string()));
    assert!(sheet.columns.contains(&"软件费用单位完工成本".to_string()));
    assert!(!sheet.columns.contains(&"软件费用异常标记".to_string()));
    assert!(!sheet
        .columns
        .contains(&"Modified Z-score_软件费用".to_string()));
}

#[test]
fn work_order_sheet_maps_typed_amounts_and_standalone_by_unique_indices() {
    let mut source = bundle(vec![
        row("WO-SKIP", 1, "汇报入库-普通生产", &[]),
        row(
            "WO-KEEP",
            90,
            "汇报入库-普通生产",
            &[
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
                (
                    "moh_utilities_amount",
                    CellValue::Decimal(Decimal::new(5, 0)),
                ),
                ("outsource_amount", CellValue::Decimal(Decimal::new(6, 0))),
                ("software_amount", CellValue::Decimal(Decimal::new(7, 0))),
            ],
        ),
    ]);
    source.unique_work_order_indices = vec![1];
    let config = PipelineRules {
        product_order: vec![],
        ..PipelineRules::for_name(PipelineName::Sk)
    };

    let sheet = build_work_order_anomaly_sheet(&source, &config).unwrap();

    assert_eq!(sheet.rows.len(), 1);
    let expected = [
        ("工单编号", CellValue::Text("WO-KEEP".to_string())),
        ("总完工成本", CellValue::Decimal(Decimal::new(90, 0))),
        (
            "直接材料合计完工金额",
            CellValue::Decimal(Decimal::new(11, 0)),
        ),
        (
            "直接人工合计完工金额",
            CellValue::Decimal(Decimal::new(12, 0)),
        ),
        (
            "制造费用合计完工金额",
            CellValue::Decimal(Decimal::new(13, 0)),
        ),
        (
            "制造费用_其他合计完工金额",
            CellValue::Decimal(Decimal::new(1, 0)),
        ),
        (
            "制造费用_人工合计完工金额",
            CellValue::Decimal(Decimal::new(2, 0)),
        ),
        (
            "制造费用_机物料及低耗合计完工金额",
            CellValue::Decimal(Decimal::new(3, 0)),
        ),
        (
            "制造费用_折旧合计完工金额",
            CellValue::Decimal(Decimal::new(4, 0)),
        ),
        (
            "制造费用_水电费合计完工金额",
            CellValue::Decimal(Decimal::new(5, 0)),
        ),
        (
            "委外加工费合计完工金额",
            CellValue::Decimal(Decimal::new(6, 0)),
        ),
        (
            "软件费用合计完工金额",
            CellValue::Decimal(Decimal::new(7, 0)),
        ),
        ("总单位完工成本", CellValue::Decimal(Decimal::new(90, 0))),
        (
            "直接材料单位完工成本",
            CellValue::Decimal(Decimal::new(11, 0)),
        ),
        (
            "直接人工单位完工成本",
            CellValue::Decimal(Decimal::new(12, 0)),
        ),
        (
            "制造费用单位完工成本",
            CellValue::Decimal(Decimal::new(13, 0)),
        ),
        (
            "制造费用_其他单位完工成本",
            CellValue::Decimal(Decimal::new(1, 0)),
        ),
        (
            "制造费用_人工单位完工成本",
            CellValue::Decimal(Decimal::new(2, 0)),
        ),
        (
            "制造费用_机物料及低耗单位完工成本",
            CellValue::Decimal(Decimal::new(3, 0)),
        ),
        (
            "制造费用_折旧单位完工成本",
            CellValue::Decimal(Decimal::new(4, 0)),
        ),
        (
            "制造费用_水电费单位完工成本",
            CellValue::Decimal(Decimal::new(5, 0)),
        ),
        (
            "委外加工费单位完工成本",
            CellValue::Decimal(Decimal::new(6, 0)),
        ),
        (
            "软件费用单位完工成本",
            CellValue::Decimal(Decimal::new(7, 0)),
        ),
    ];
    for (column, value) in expected {
        assert_eq!(
            sheet.rows[0][column_index(&sheet, column)],
            value,
            "{column}"
        );
    }
}

#[test]
fn scores_normal_and_rework_in_separate_pools() {
    let sheet = build_work_order_anomaly_sheet(
        &bundle(vec![
            row("WO-N1", 100, "汇报入库-普通生产", &[]),
            row("WO-N2", 105, "汇报入库-普通生产", &[]),
            row("WO-N3", 500, "汇报入库-普通生产", &[]),
            row("WO-R1", 200, "汇报入库-返工生产", &[]),
            row("WO-R2", 210, "汇报入库-返工生产", &[]),
            row("WO-R3", 500, "汇报入库-返工生产", &[]),
        ]),
        &test_config(PipelineName::Gb),
    )
    .unwrap();
    let level_idx = column_index(&sheet, "异常等级");
    let scope_idx = column_index(&sheet, "生产类型");

    assert_eq!(
        sheet.rows[2][level_idx],
        CellValue::Text("高度可疑".to_string())
    );
    assert_eq!(
        sheet.rows[5][level_idx],
        CellValue::Text("高度可疑".to_string())
    );
    assert_eq!(
        sheet.rows[5][scope_idx],
        CellValue::Text("返工生产".to_string())
    );
}

#[test]
fn equal_decimal_scores_mark_multiple_non_total_sources() {
    let config = test_config(PipelineName::Gb);
    let source = row("WO-TIE", 100, "汇报入库-普通生产", &[]);
    let bundle = bundle(vec![source]);
    let mut anomaly_row = build_anomaly_row(
        bundle.work_order_rows().next().unwrap(),
        &bundle.schema,
        &config,
    )
    .unwrap();
    for metric_key in ["dm_unit_cost", "dl_unit_cost"] {
        anomaly_row.audits.insert(
            metric_key,
            MetricAudit {
                flag: "关注".to_string(),
                score: Some(Decimal::new(30, 1)),
                ..MetricAudit::default()
            },
        );
    }

    finalize_row_anomaly(&mut anomaly_row);

    assert_eq!(anomaly_row.anomaly_level, "关注");
    assert_eq!(anomaly_row.anomaly_source, "多项同时异常");
}

#[test]
fn near_zero_mad_uses_minimum_dispersion() {
    let sheet = build_work_order_anomaly_sheet(
        &bundle(vec![
            decimal_row(
                "WO-R-CENTER-1",
                "P-NEAR-MAD",
                "近零MAD产品",
                "100",
                "100.0000000",
                "汇报入库-返工生产",
            ),
            decimal_row(
                "WO-R-CENTER-2",
                "P-NEAR-MAD",
                "近零MAD产品",
                "710",
                "100.0000008",
                "汇报入库-返工生产",
            ),
            decimal_row(
                "WO-R-CLOSE",
                "P-NEAR-MAD",
                "近零MAD产品",
                "100",
                "100.01",
                "汇报入库-返工生产",
            ),
            decimal_row(
                "WO-R-FAR",
                "P-NEAR-MAD",
                "近零MAD产品",
                "100",
                "120",
                "汇报入库-返工生产",
            ),
            decimal_row(
                "WO-R-EXTREME",
                "P-NEAR-MAD",
                "近零MAD产品",
                "100",
                "180",
                "汇报入库-返工生产",
            ),
        ]),
        &test_config(PipelineName::Gb),
    )
    .unwrap();
    let level_idx = column_index(&sheet, "异常等级");
    let detail_idx = column_index(&sheet, "异常明细解释");

    assert_eq!(
        sheet.rows[2][level_idx],
        CellValue::Text("正常".to_string())
    );
    assert_eq!(sheet.rows[2][detail_idx], CellValue::Text(String::new()));
    assert_eq!(
        sheet.rows[3][level_idx],
        CellValue::Text("高度可疑".to_string())
    );
    assert_eq!(
        sheet.rows[4][level_idx],
        CellValue::Text("高度可疑".to_string())
    );
    let CellValue::Text(detail) = &sheet.rows[3][detail_idx] else {
        panic!("detail explanation should be text");
    };
    assert!(detail.contains("score="));
}

#[test]
fn foreign_schema_row_error_is_propagated() {
    let named = row("WO1", 100, "汇报入库-普通生产", &[]);
    let mut bundle = bundle(vec![named.clone()]);
    let columns = named.keys().cloned().collect::<Vec<_>>();
    let cells = columns
        .iter()
        .map(|column| named.get(column).cloned().unwrap())
        .collect::<Vec<_>>();
    let foreign = IndexedTable::from_raw(columns, vec![cells]).unwrap();
    let (_, _, mut rows) = foreign.into_parts();
    bundle.qty_rows[0].source = rows.pop().unwrap();

    let error =
        build_work_order_anomaly_sheet(&bundle, &test_config(PipelineName::Gb)).unwrap_err();

    assert_eq!(error.code(), crate::error::ErrorCode::InternalError);
}

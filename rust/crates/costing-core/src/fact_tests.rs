use std::collections::BTreeMap;

use crate::anomaly::build_work_order_anomaly_sheet;
use crate::error::ErrorCode;
use crate::model::{CellValue, SplitResult};
use crate::pipeline::{PipelineName, PipelineRules};
use crate::table::IndexedTable;

use super::*;

type NamedTestRow = BTreeMap<String, CellValue>;

fn row(values: &[(&str, CellValue)]) -> NamedTestRow {
    values
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

fn detail_row(cost_item: &str, amount: CellValue) -> NamedTestRow {
    row(&[
        ("月份", CellValue::Text("2025年01期".to_string())),
        ("产品编码", CellValue::Text("P1".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text("WO1".to_string())),
        ("工单行号", CellValue::Text("1".to_string())),
        ("成本项目名称", CellValue::Text(cost_item.to_string())),
        ("本期完工金额", amount),
    ])
}

fn qty_row(completed_qty: Decimal, completed_total: Decimal) -> NamedTestRow {
    row(&[
        ("月份", CellValue::Text("2025年01期".to_string())),
        ("产品编码", CellValue::Text("P1".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text("WO1".to_string())),
        ("工单行号", CellValue::Text("1".to_string())),
        ("本期完工数量", CellValue::Decimal(completed_qty)),
        ("本期完工金额", CellValue::Decimal(completed_total)),
    ])
}

fn detail_row_for(work_order: &str, cost_item: &str, amount: CellValue) -> NamedTestRow {
    let mut row = detail_row(cost_item, amount);
    row.insert(
        "工单编号".to_string(),
        CellValue::Text(work_order.to_string()),
    );
    row
}

fn qty_row_for(work_order: &str, completed_qty: Decimal, completed_total: Decimal) -> NamedTestRow {
    let mut row = qty_row(completed_qty, completed_total);
    row.insert(
        "工单编号".to_string(),
        CellValue::Text(work_order.to_string()),
    );
    row
}

fn split_result(detail_rows: Vec<NamedTestRow>, qty_rows: Vec<NamedTestRow>) -> SplitResult {
    let mut columns = [
        "月份",
        "产品编码",
        "产品名称",
        "工单编号",
        "工单行号",
        "成本项目名称",
        "本期完工数量",
        "本期完工金额",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<Vec<_>>();
    for column in detail_rows.iter().chain(&qty_rows).flat_map(BTreeMap::keys) {
        if !columns.contains(column) {
            columns.push(column.clone());
        }
    }
    split_result_with_columns(columns, detail_rows, qty_rows)
}

fn split_result_with_columns(
    columns: Vec<String>,
    detail_rows: Vec<NamedTestRow>,
    qty_rows: Vec<NamedTestRow>,
) -> SplitResult {
    let detail_len = detail_rows.len();
    let positional = detail_rows
        .into_iter()
        .chain(qty_rows)
        .map(|mut named| {
            columns
                .iter()
                .map(|column| named.remove(column).unwrap_or(CellValue::Blank))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let table = IndexedTable::from_raw(columns.clone(), positional).unwrap();
    let (schema, _, mut rows) = table.into_parts();
    let qty_rows = rows.split_off(detail_len);
    let detail_display_columns = schema.display_order_for(&columns).unwrap();
    SplitResult {
        qty_display_columns: detail_display_columns.clone(),
        schema,
        detail_display_columns,
        detail_rows: rows,
        qty_rows,
    }
}

fn source_value<'a>(
    schema: &ColumnSchema,
    row: &'a QtyFactRow,
    column: &str,
) -> Option<&'a CellValue> {
    schema
        .optional(column)
        .map(|id| row.source.get(id).unwrap())
}

fn bundle_columns(bundle: &FactBundle) -> Vec<String> {
    bundle
        .qty_display_columns
        .iter()
        .map(|id| bundle.schema.name(*id).unwrap().to_string())
        .collect()
}

#[test]
fn classifies_direct_material_and_direct_labor_without_dynamic_labels() {
    assert_eq!(
        classify_cost_item("直接材料", &["委外加工费"]),
        CostClassification::DirectMaterial
    );
    assert_eq!(
        classify_cost_item("直接人工", &["委外加工费"]),
        CostClassification::DirectLabor
    );
}

#[test]
fn manufacturing_component_updates_total_and_matching_component() {
    let classification = classify_cost_item("制造费用_折旧", &["委外加工费", "软件费用"]);
    assert_eq!(
        classification,
        CostClassification::ManufacturingOverhead(Some(MohComponent::Depreciation)),
    );

    let mut amounts = CostAmounts::new(2);
    amounts.add(classification, Decimal::new(1250, 2));

    assert_eq!(amounts.manufacturing_overhead, Decimal::new(1250, 2));
    assert_eq!(amounts.moh_depreciation, Decimal::new(1250, 2));
    assert_eq!(amounts.moh_component_sum(), Decimal::new(1250, 2));
}

#[test]
fn unknown_manufacturing_component_updates_only_moh_total() {
    let classification = classify_cost_item("制造费用_未分类", &["委外加工费"]);
    assert_eq!(
        classification,
        CostClassification::ManufacturingOverhead(None)
    );

    let mut amounts = CostAmounts::new(1);
    amounts.add(classification, Decimal::new(9, 0));

    assert_eq!(amounts.manufacturing_overhead, Decimal::new(9, 0));
    assert_eq!(amounts.moh_component_sum(), Decimal::ZERO);
    assert_eq!(amounts.standalone_amount(0), Decimal::ZERO);
}

#[test]
fn standalone_cost_uses_pipeline_configuration_index() {
    let items = ["软件费用", "委外加工费"];
    let classification = classify_cost_item("委外加工费", &items);
    assert_eq!(classification, CostClassification::Standalone(1));

    let mut amounts = CostAmounts::new(items.len());
    amounts.add(classification, Decimal::new(35, 1));

    assert_eq!(amounts.standalone_amount(0), Decimal::ZERO);
    assert_eq!(amounts.standalone_amount(1), Decimal::new(35, 1));
}

#[test]
fn sk_standalone_cost_order_keeps_outsource_before_software() {
    let items = ["委外加工费", "软件费用"];
    assert_eq!(
        classify_cost_item("委外加工费", &items),
        CostClassification::Standalone(0),
    );
    assert_eq!(
        classify_cost_item("软件费用", &items),
        CostClassification::Standalone(1),
    );
}

#[test]
fn unmapped_non_blank_cost_item_still_emits_the_same_issue() {
    let bundle = build_fact_bundle(
        split_result(vec![detail_row("未映射费用", CellValue::Blank)], vec![]),
        &PipelineRules::for_name(PipelineName::Gb),
    )
    .unwrap();

    assert_eq!(
        bundle.error_issues,
        vec![ErrorIssue {
            row_id: "2025年01期|P1|WO1|1".to_string(),
            issue_type: "UNMAPPED_COST_ITEM".to_string(),
            field_name: "成本项目名称".to_string(),
            original_value: "未映射费用".to_string(),
            reason: "成本项目未映射到直接材料/直接人工/制造费用".to_string(),
            action: "该行已从分析数据中排除".to_string(),
            retryable: false,
        }],
    );
}

#[test]
fn missing_amount_issue_payload_remains_exact() {
    let bundle = build_fact_bundle(
        split_result(vec![detail_row("直接材料", CellValue::Blank)], vec![]),
        &PipelineRules::for_name(PipelineName::Gb),
    )
    .unwrap();

    assert_eq!(
        bundle.error_issues,
        vec![ErrorIssue {
            row_id: "2025年01期|P1|WO1|1".to_string(),
            issue_type: "MISSING_AMOUNT".to_string(),
            field_name: "本期完工金额".to_string(),
            original_value: String::new(),
            reason: "成本明细金额为空，已按 0 参与汇总".to_string(),
            action: "金额置为 0 后继续计算".to_string(),
            retryable: false,
        }],
    );
}

#[test]
fn typed_amounts_keep_missing_amount_issue_before_qty_issues() {
    let detail = vec![
        detail_row("直接材料", CellValue::Blank),
        detail_row("直接材料", CellValue::Decimal(Decimal::ONE)),
        detail_row("直接人工", CellValue::Decimal(Decimal::ONE)),
        detail_row("制造费用_其他", CellValue::Decimal(Decimal::ONE)),
        detail_row("制造费用-人工", CellValue::Decimal(Decimal::ONE)),
        detail_row("制造费用_机物料及低耗", CellValue::Decimal(Decimal::ONE)),
        detail_row("制造费用_折旧", CellValue::Decimal(Decimal::ONE)),
        detail_row("制造费用_水电费", CellValue::Decimal(Decimal::ONE)),
    ];
    let qty = vec![
        qty_row(Decimal::ONE, Decimal::new(7, 0)),
        qty_row(Decimal::new(2, 0), Decimal::new(8, 0)),
    ];

    let bundle = build_fact_bundle(
        split_result(detail, qty),
        &PipelineRules::for_name(PipelineName::Gb),
    )
    .unwrap();

    assert_eq!(
        bundle
            .error_issues
            .iter()
            .map(|issue| issue.issue_type.as_str())
            .collect::<Vec<_>>(),
        vec![
            "MISSING_AMOUNT",
            "DUPLICATE_WORK_ORDER_KEY",
            "DUPLICATE_WORK_ORDER_KEY",
            "TOTAL_COST_MISMATCH",
        ]
    );
}

#[test]
fn qty_without_detail_uses_zero_for_every_typed_amount() {
    let qty = vec![row(&[
        ("月份", CellValue::Text("2025年01期".to_string())),
        ("产品编码", CellValue::Text("P1".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text("WO1".to_string())),
        ("工单行号", CellValue::Text("1".to_string())),
        ("本期完工数量", CellValue::Decimal(Decimal::ONE)),
        ("本期完工金额", CellValue::Decimal(Decimal::ZERO)),
    ])];
    let config = PipelineRules::for_name(PipelineName::Sk);

    let bundle = build_fact_bundle(split_result(vec![], qty), &config).unwrap();
    let amounts = &bundle.qty_rows[0].amounts;

    assert_eq!(amounts.direct_material, Decimal::ZERO);
    assert_eq!(amounts.direct_labor, Decimal::ZERO);
    assert_eq!(amounts.manufacturing_overhead, Decimal::ZERO);
    assert_eq!(amounts.moh_other, Decimal::ZERO);
    assert_eq!(amounts.moh_labor, Decimal::ZERO);
    assert_eq!(amounts.moh_consumables, Decimal::ZERO);
    assert_eq!(amounts.moh_depreciation, Decimal::ZERO);
    assert_eq!(amounts.moh_utilities, Decimal::ZERO);
    assert_eq!(amounts.standalone, vec![Decimal::ZERO, Decimal::ZERO]);
}

#[test]
fn gb_typed_fact_includes_outsource_total_match() {
    let detail = vec![
        row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("成本项目名称", CellValue::Text("直接材料".to_string())),
            ("本期完工金额", CellValue::Decimal(Decimal::new(100, 0))),
        ]),
        row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("成本项目名称", CellValue::Text("委外加工费".to_string())),
            ("本期完工金额", CellValue::Decimal(Decimal::new(5, 0))),
        ]),
    ];
    let qty = vec![row(&[
        ("月份", CellValue::Text("2025年01期".to_string())),
        ("产品编码", CellValue::Text("P1".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text("WO1".to_string())),
        ("工单行号", CellValue::Text("1".to_string())),
        ("本期完工数量", CellValue::Decimal(Decimal::new(10, 0))),
        ("本期完工金额", CellValue::Decimal(Decimal::new(105, 0))),
    ])];
    let config = PipelineRules::for_name(PipelineName::Gb);
    let bundle = build_fact_bundle(split_result(detail, qty), &config).unwrap();
    assert_eq!(
        bundle.qty_rows[0].amounts.standalone_amount(0),
        Decimal::new(5, 0)
    );
    assert!(bundle.qty_rows[0].total_matches);
}

#[test]
fn work_order_keys_trim_text_and_normalize_integer_suffixes_before_joining() {
    let detail = vec![row(&[
        ("月份", CellValue::Text(" 2025年01期 ".to_string())),
        ("产品编码", CellValue::Text(" P1 ".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text(" WO1 ".to_string())),
        ("工单行号", CellValue::Text(" 1.0 ".to_string())),
        ("成本项目名称", CellValue::Text("直接材料".to_string())),
        ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
    ])];
    let qty = vec![row(&[
        ("月份", CellValue::Text("2025年01期".to_string())),
        ("产品编码", CellValue::Text("P1".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text("WO1".to_string())),
        ("工单行号", CellValue::Text("1".to_string())),
        ("本期完工数量", CellValue::Decimal(Decimal::new(2, 0))),
        ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
    ])];

    let bundle = build_fact_bundle(
        split_result(detail, qty),
        &PipelineRules::for_name(PipelineName::Gb),
    )
    .unwrap();

    assert_eq!(
        bundle.qty_rows[0].amounts.direct_material,
        Decimal::new(10, 0)
    );
    assert_eq!(bundle.qty_rows[0].work_order_key, "2025年01期|P1|WO1|1");
    assert!(!bundle
        .error_issues
        .iter()
        .any(|issue| issue.issue_type == "TOTAL_COST_MISMATCH"));
}

#[test]
fn overflowing_unit_costs_are_omitted_without_panicking() {
    const PRODUCT_ORDER: &[(&str, &str)] = &[("P1", "产品")];
    let amount = Decimal::new(9_999_999_999, 0);
    let tiny_qty = Decimal::new(1, 28);
    let detail = vec![row(&[
        ("月份", CellValue::Text("2025年01期".to_string())),
        ("产品编码", CellValue::Text("P1".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text("WO1".to_string())),
        ("工单行号", CellValue::Text("1".to_string())),
        ("成本项目名称", CellValue::Text("直接材料".to_string())),
        ("本期完工金额", CellValue::Decimal(amount)),
    ])];
    let qty = vec![row(&[
        ("月份", CellValue::Text("2025年01期".to_string())),
        ("产品编码", CellValue::Text("P1".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text("WO1".to_string())),
        ("工单行号", CellValue::Text("1".to_string())),
        ("单据类型", CellValue::Text("汇报入库-普通生产".to_string())),
        ("本期完工数量", CellValue::Decimal(tiny_qty)),
        ("本期完工金额", CellValue::Decimal(amount)),
    ])];
    let config = PipelineRules {
        product_order: crate::pipeline::owned_product_order(PRODUCT_ORDER),
        ..PipelineRules::for_name(PipelineName::Gb)
    };

    let bundle = build_fact_bundle(split_result(detail, qty), &config).unwrap();
    let anomaly_sheet = build_work_order_anomaly_sheet(&bundle, &config).unwrap();
    let total_unit_index = anomaly_sheet
        .columns
        .iter()
        .position(|column| column == "总单位完工成本")
        .unwrap();

    assert_eq!(safe_divide(amount, tiny_qty), None);
    assert_eq!(anomaly_sheet.rows[0][total_unit_index], CellValue::Blank);
    assert!(!bundle.error_issues.iter().any(|issue| {
        issue.issue_type == "NON_POSITIVE_UNIT_COST"
            && matches!(
                issue.field_name.as_str(),
                "总单位完工成本" | "直接材料单位完工成本"
            )
    }));
}

#[test]
fn sk_typed_fact_includes_software_fee() {
    let detail = vec![row(&[
        ("月份", CellValue::Text("2025年01期".to_string())),
        ("产品编码", CellValue::Text("P1".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text("WO1".to_string())),
        ("工单行号", CellValue::Text("1".to_string())),
        ("成本项目名称", CellValue::Text("软件费用".to_string())),
        ("本期完工金额", CellValue::Decimal(Decimal::new(7, 0))),
    ])];
    let qty = vec![row(&[
        ("月份", CellValue::Text("2025年01期".to_string())),
        ("产品编码", CellValue::Text("P1".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text("WO1".to_string())),
        ("工单行号", CellValue::Text("1".to_string())),
        ("本期完工数量", CellValue::Decimal(Decimal::new(1, 0))),
        ("本期完工金额", CellValue::Decimal(Decimal::new(7, 0))),
    ])];
    let config = PipelineRules::for_name(PipelineName::Sk);
    let bundle = build_fact_bundle(split_result(detail, qty), &config).unwrap();
    assert_eq!(
        bundle.qty_rows[0].amounts.standalone_amount(1),
        Decimal::new(7, 0)
    );
}

#[test]
fn fact_bundle_records_missing_amount_and_duplicate_work_order_key() {
    let detail = vec![row(&[
        ("月份", CellValue::Text("2025年01期".to_string())),
        ("产品编码", CellValue::Text("P1".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text("WO1".to_string())),
        ("工单行号", CellValue::Text("1".to_string())),
        ("成本项目名称", CellValue::Text("直接材料".to_string())),
        ("本期完工金额", CellValue::Blank),
    ])];
    let qty = vec![
        row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("本期完工数量", CellValue::Decimal(Decimal::new(1, 0))),
            ("本期完工金额", CellValue::Decimal(Decimal::new(0, 0))),
        ]),
        row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("本期完工数量", CellValue::Decimal(Decimal::new(2, 0))),
            ("本期完工金额", CellValue::Decimal(Decimal::new(0, 0))),
        ]),
    ];

    let bundle = build_fact_bundle(
        split_result(detail, qty),
        &PipelineRules::for_name(PipelineName::Gb),
    )
    .unwrap();

    assert!(bundle
        .error_issues
        .iter()
        .any(|issue| issue.issue_type == "MISSING_AMOUNT"));
    assert!(bundle
        .error_issues
        .iter()
        .any(|issue| issue.issue_type == "DUPLICATE_WORK_ORDER_KEY"));
}

#[test]
fn fact_bundle_records_non_positive_unit_cost_audit_issues() {
    let detail = vec![row(&[
        ("月份", CellValue::Text("2025年01期".to_string())),
        ("产品编码", CellValue::Text("P1".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text("WO1".to_string())),
        ("工单行号", CellValue::Text("1".to_string())),
        ("成本项目名称", CellValue::Text("直接材料".to_string())),
        ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
    ])];
    let qty = vec![row(&[
        ("月份", CellValue::Text("2025年01期".to_string())),
        ("产品编码", CellValue::Text("P1".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text("WO1".to_string())),
        ("工单行号", CellValue::Text("1".to_string())),
        ("本期完工数量", CellValue::Decimal(Decimal::new(2, 0))),
        ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
    ])];

    let bundle = build_fact_bundle(
        split_result(detail, qty),
        &PipelineRules::for_name(PipelineName::Gb),
    )
    .unwrap();

    let issues = bundle
        .error_issues
        .iter()
        .filter(|issue| issue.issue_type == "NON_POSITIVE_UNIT_COST")
        .collect::<Vec<_>>();
    assert_eq!(issues.len(), 7);
    assert_eq!(
        issues
            .iter()
            .map(|issue| issue.field_name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "直接人工单位完工成本",
            "制造费用单位完工成本",
            "制造费用_其他单位完工成本",
            "制造费用_人工单位完工成本",
            "制造费用_机物料及低耗单位完工成本",
            "制造费用_折旧单位完工成本",
            "制造费用_水电费单位完工成本",
        ]
    );
    assert!(issues.iter().any(|issue| {
        issue.field_name == "直接人工单位完工成本"
            && issue.row_id == "2025年01期|P1|WO1|1"
            && issue.original_value == "0"
            && issue.reason == "单位成本小于等于 0，不参与 log 与 Modified Z-score"
            && issue.action == "保留在异常分析页并标记复核原因"
    }));
    assert!(!issues
        .iter()
        .any(|issue| issue.field_name == "委外加工费单位完工成本"));
}

#[test]
fn typed_fact_marks_moh_and_total_mismatch() {
    let detail = vec![
        row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("成本项目名称", CellValue::Text("制造费用".to_string())),
            ("本期完工金额", CellValue::Decimal(Decimal::new(20, 0))),
        ]),
        row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("成本项目名称", CellValue::Text("制造费用-人工".to_string())),
            ("本期完工金额", CellValue::Decimal(Decimal::new(5, 0))),
        ]),
    ];
    let qty = vec![row(&[
        ("月份", CellValue::Text("2025年01期".to_string())),
        ("产品编码", CellValue::Text("P1".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text("WO1".to_string())),
        ("工单行号", CellValue::Text("1".to_string())),
        ("本期完工数量", CellValue::Decimal(Decimal::new(1, 0))),
        ("本期完工金额", CellValue::Decimal(Decimal::new(99, 0))),
    ])];
    let config = PipelineRules::for_name(PipelineName::Gb);
    let bundle = build_fact_bundle(split_result(detail, qty), &config).unwrap();
    let columns = qty_sheet_columns(&bundle_columns(&bundle), &config);

    assert!(columns.contains(&"制造费用_人工单位完工成本".to_string()));
    assert_eq!(
        safe_divide(
            bundle.qty_rows[0].amounts.moh_labor,
            bundle.qty_rows[0].completed_qty
        ),
        Some(Decimal::new(5, 0))
    );
    assert!(!bundle.qty_rows[0].moh_matches);
    assert!(!bundle.qty_rows[0].total_matches);
    assert_eq!(
        bundle.qty_rows[0].check_reason,
        "制造费用明细与合计不一致;直接材料+直接人工+制造费用+委外加工费与总完工成本不一致"
    );
    assert_eq!(
        bundle
            .error_issues
            .iter()
            .take(2)
            .map(|issue| issue.issue_type.as_str())
            .collect::<Vec<_>>(),
        vec!["MOH_BREAKDOWN_MISMATCH", "TOTAL_COST_MISMATCH"]
    );
}

#[test]
fn filters_invalid_qty_rows_before_fact_output() {
    let detail = vec![row(&[
        ("月份", CellValue::Text("2025年01期".to_string())),
        ("产品编码", CellValue::Text("P1".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text("WO1".to_string())),
        ("工单行号", CellValue::Text("1".to_string())),
        ("成本项目名称", CellValue::Text("直接材料".to_string())),
        ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
    ])];
    let qty = vec![
        row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("本期完工数量", CellValue::Decimal(Decimal::new(2, 0))),
            ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
        ]),
        row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO-ZERO".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("本期完工数量", CellValue::Decimal(Decimal::new(0, 0))),
            ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
        ]),
        row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO-MISSING".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("本期完工数量", CellValue::Decimal(Decimal::new(1, 0))),
            ("本期完工金额", CellValue::Blank),
        ]),
    ];
    let config = PipelineRules::for_name(PipelineName::Gb);

    let bundle = build_fact_bundle(split_result(detail, qty), &config).unwrap();
    assert_eq!(bundle.qty_rows.len(), 1);
    assert_eq!(bundle.work_order_row_count(), 1);
    assert_eq!(
        source_value(&bundle.schema, &bundle.qty_rows[0], "工单编号"),
        Some(&CellValue::Text("WO1".to_string()))
    );
}

#[test]
fn gb_software_fee_is_unmapped_not_standalone() {
    let detail = vec![row(&[
        ("月份", CellValue::Text("2025年01期".to_string())),
        ("产品编码", CellValue::Text("P1".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text("WO1".to_string())),
        ("工单行号", CellValue::Text("1".to_string())),
        ("成本项目名称", CellValue::Text("软件费用".to_string())),
        ("本期完工金额", CellValue::Decimal(Decimal::new(7, 0))),
    ])];
    let qty = vec![row(&[
        ("月份", CellValue::Text("2025年01期".to_string())),
        ("产品编码", CellValue::Text("P1".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text("WO1".to_string())),
        ("工单行号", CellValue::Text("1".to_string())),
        ("本期完工数量", CellValue::Decimal(Decimal::new(1, 0))),
        ("本期完工金额", CellValue::Decimal(Decimal::new(0, 0))),
    ])];
    let config = PipelineRules::for_name(PipelineName::Gb);

    let bundle = build_fact_bundle(split_result(detail, qty), &config).unwrap();

    assert!(bundle
        .error_issues
        .iter()
        .any(|issue| issue.issue_type == "UNMAPPED_COST_ITEM"));
    assert_eq!(bundle.qty_rows[0].amounts.standalone.len(), 1);
    assert_eq!(
        bundle.qty_rows[0].amounts.standalone_amount(0),
        Decimal::ZERO
    );
}

#[test]
fn qty_fact_keeps_duplicates_but_unique_indices_keep_first() {
    let detail = vec![row(&[
        ("月份", CellValue::Text("2025年01期".to_string())),
        ("产品编码", CellValue::Text("P1".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text("WO1".to_string())),
        ("工单行号", CellValue::Text("1".to_string())),
        ("成本项目名称", CellValue::Text("直接材料".to_string())),
        ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
    ])];
    let qty = vec![
        row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("本期完工数量", CellValue::Decimal(Decimal::new(1, 0))),
            ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
        ]),
        row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("本期完工数量", CellValue::Decimal(Decimal::new(2, 0))),
            ("本期完工金额", CellValue::Decimal(Decimal::new(20, 0))),
        ]),
    ];

    let bundle = build_fact_bundle(
        split_result(detail, qty),
        &PipelineRules::for_name(PipelineName::Gb),
    )
    .unwrap();

    assert_eq!(bundle.qty_rows.len(), 2);
    assert_eq!(bundle.work_order_row_count(), 1);
    assert_eq!(
        bundle
            .error_issues
            .iter()
            .filter(|issue| issue.issue_type == "DUPLICATE_WORK_ORDER_KEY")
            .count(),
        2
    );
    assert_eq!(
        bundle.work_order_rows().next().unwrap().completed_qty,
        Decimal::new(1, 0)
    );
    assert!(bundle
        .error_issues
        .iter()
        .any(|issue| issue.issue_type == "DUPLICATE_WORK_ORDER_KEY"));
}

#[test]
fn missing_required_columns_returns_error() {
    let detail = vec![row(&[
        ("月份", CellValue::Text("2025年01期".to_string())),
        ("产品编码", CellValue::Text("P1".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text("WO1".to_string())),
        ("工单行号", CellValue::Text("1".to_string())),
        ("成本项目名称", CellValue::Text("直接材料".to_string())),
    ])];
    let qty = vec![row(&[
        ("月份", CellValue::Text("2025年01期".to_string())),
        ("产品编码", CellValue::Text("P1".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text("WO1".to_string())),
        ("工单行号", CellValue::Text("1".to_string())),
        ("本期完工数量", CellValue::Decimal(Decimal::new(1, 0))),
        ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
    ])];

    let error = build_fact_bundle(
        split_result_with_columns(
            [
                "月份",
                "产品编码",
                "产品名称",
                "工单编号",
                "工单行号",
                "成本项目名称",
                "本期完工数量",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            detail,
            qty,
        ),
        &PipelineRules::for_name(PipelineName::Gb),
    )
    .unwrap_err();

    assert_eq!(error.code(), ErrorCode::InvalidInput);
    assert!(error.message().contains("本期完工金额"));
}

#[test]
fn empty_rows_still_validate_required_schema_columns() {
    let error = build_fact_bundle(
        split_result_with_columns(vec!["产品编码".to_string()], vec![], vec![]),
        &PipelineRules::for_name(PipelineName::Gb),
    )
    .unwrap_err();

    assert_eq!(error.code(), ErrorCode::InvalidInput);
    assert!(error.message().contains("缺少必要字段"));
    assert!(error.message().contains("本期完工金额"));
}

#[test]
fn fact_bundle_keeps_qty_filter_audit_counts() {
    let detail = vec![row(&[
        ("月份", CellValue::Text("2025年01期".to_string())),
        ("产品编码", CellValue::Text("P1".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text("WO1".to_string())),
        ("工单行号", CellValue::Text("1".to_string())),
        ("成本项目名称", CellValue::Text("直接材料".to_string())),
        ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
    ])];
    let qty = vec![
        row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("本期完工数量", CellValue::Decimal(Decimal::new(1, 0))),
            ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
        ]),
        row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO-ZERO".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("本期完工数量", CellValue::Decimal(Decimal::ZERO)),
            ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
        ]),
        row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO-MISSING".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("本期完工数量", CellValue::Decimal(Decimal::ONE)),
            ("本期完工金额", CellValue::Blank),
        ]),
    ];

    let bundle = build_fact_bundle(
        split_result(detail, qty),
        &PipelineRules::for_name(PipelineName::Gb),
    )
    .unwrap();

    assert_eq!(bundle.qty_input_row_count, 3);
    assert_eq!(bundle.filtered_invalid_qty_count, 1);
    assert_eq!(bundle.filtered_missing_total_amount_count, 1);
    assert_eq!(bundle.qty_rows.len(), 1);
}

#[test]
fn prepared_qty_row_caches_normalized_work_order_key() {
    let qty = vec![row(&[
        ("月份", CellValue::Text(" 2025年01期 ".to_string())),
        ("产品编码", CellValue::Text(" P1 ".to_string())),
        ("产品名称", CellValue::Text("产品".to_string())),
        ("工单编号", CellValue::Text(" WO1 ".to_string())),
        ("工单行号", CellValue::Text("1.0".to_string())),
        ("本期完工数量", CellValue::Decimal(Decimal::ONE)),
        ("本期完工金额", CellValue::Decimal(Decimal::ONE)),
    ])];

    let bundle = build_fact_bundle(
        split_result(Vec::new(), qty),
        &PipelineRules::for_name(PipelineName::Gb),
    )
    .unwrap();

    assert_eq!(bundle.qty_rows[0].work_order_key, "2025年01期|P1|WO1|1");
}

#[test]
fn qty_fact_keeps_all_valid_rows_in_input_order() {
    let qty = vec![
        qty_row_for("WO2", Decimal::new(2, 0), Decimal::new(20, 0)),
        qty_row_for("WO1", Decimal::ONE, Decimal::new(10, 0)),
        qty_row_for("WO3", Decimal::new(3, 0), Decimal::new(30, 0)),
    ];

    let bundle = build_fact_bundle(
        split_result(Vec::new(), qty),
        &PipelineRules::for_name(PipelineName::Gb),
    )
    .unwrap();

    assert_eq!(
        bundle
            .qty_rows
            .iter()
            .map(|row| row.work_order_key.as_str())
            .collect::<Vec<_>>(),
        vec![
            "2025年01期|P1|WO2|1",
            "2025年01期|P1|WO1|1",
            "2025年01期|P1|WO3|1",
        ]
    );
}

#[test]
fn three_duplicate_qty_rows_count_as_three_duplicate_rows() {
    let qty = vec![
        qty_row(Decimal::ONE, Decimal::ONE),
        qty_row(Decimal::new(2, 0), Decimal::new(2, 0)),
        qty_row(Decimal::new(3, 0), Decimal::new(3, 0)),
    ];

    let bundle = build_fact_bundle(
        split_result(Vec::new(), qty),
        &PipelineRules::for_name(PipelineName::Gb),
    )
    .unwrap();
    let duplicate_issues = bundle
        .error_issues
        .iter()
        .filter(|issue| issue.issue_type == "DUPLICATE_WORK_ORDER_KEY")
        .collect::<Vec<_>>();

    assert_eq!(bundle.duplicate_work_order_row_count, 3);
    assert_eq!(duplicate_issues.len(), 3);
    for issue in duplicate_issues {
        assert_eq!(issue.row_id, "2025年01期|P1|WO1|1");
        assert_eq!(issue.issue_type, "DUPLICATE_WORK_ORDER_KEY");
        assert_eq!(issue.field_name, "工单主键");
        assert_eq!(issue.original_value, "3");
        assert_eq!(issue.reason, "数量页存在重复工单主键");
        assert_eq!(issue.action, "数量页原样保留，异常分析按首条记录去重");
        assert!(!issue.retryable);
    }
}

#[test]
fn unique_work_order_indices_keep_only_the_first_occurrence() {
    let qty = vec![
        qty_row_for("WO1", Decimal::ONE, Decimal::ONE),
        qty_row_for("WO2", Decimal::new(2, 0), Decimal::new(2, 0)),
        qty_row_for("WO1", Decimal::new(3, 0), Decimal::new(3, 0)),
    ];

    let bundle = build_fact_bundle(
        split_result(Vec::new(), qty),
        &PipelineRules::for_name(PipelineName::Gb),
    )
    .unwrap();

    assert_eq!(bundle.unique_work_order_indices, vec![0, 1]);
    assert_eq!(
        bundle
            .work_order_rows()
            .map(|row| row.completed_qty)
            .collect::<Vec<_>>(),
        vec![Decimal::ONE, Decimal::new(2, 0)]
    );
}

#[test]
fn fact_issue_order_is_detail_then_duplicate_then_reconciliation_then_unit_cost() {
    let mut detail = vec![
        detail_row_for("WO-DUP", "直接材料", CellValue::Blank),
        detail_row_for("WO-DUP", "直接材料", CellValue::Decimal(Decimal::ONE)),
        detail_row_for("WO-DUP", "直接人工", CellValue::Decimal(Decimal::ONE)),
        detail_row_for("WO-DUP", "制造费用_其他", CellValue::Decimal(Decimal::ONE)),
        detail_row_for("WO-DUP", "制造费用-人工", CellValue::Decimal(Decimal::ONE)),
        detail_row_for(
            "WO-DUP",
            "制造费用_机物料及低耗",
            CellValue::Decimal(Decimal::ONE),
        ),
        detail_row_for("WO-DUP", "制造费用_折旧", CellValue::Decimal(Decimal::ONE)),
        detail_row_for(
            "WO-DUP",
            "制造费用_水电费",
            CellValue::Decimal(Decimal::ONE),
        ),
    ];
    detail.extend([
        detail_row_for("WO-BAD", "直接材料", CellValue::Decimal(Decimal::ONE)),
        detail_row_for("WO-BAD", "直接人工", CellValue::Decimal(Decimal::ONE)),
        detail_row_for("WO-BAD", "制造费用_其他", CellValue::Decimal(Decimal::ONE)),
        detail_row_for("WO-BAD", "制造费用-人工", CellValue::Decimal(Decimal::ONE)),
        detail_row_for(
            "WO-BAD",
            "制造费用_机物料及低耗",
            CellValue::Decimal(Decimal::ONE),
        ),
        detail_row_for("WO-BAD", "制造费用_折旧", CellValue::Decimal(Decimal::ONE)),
        detail_row_for(
            "WO-BAD",
            "制造费用_未分类",
            CellValue::Decimal(Decimal::ONE),
        ),
    ]);
    let qty = vec![
        qty_row_for("WO-DUP", Decimal::ONE, Decimal::new(7, 0)),
        qty_row_for("WO-DUP", Decimal::new(2, 0), Decimal::new(7, 0)),
        qty_row_for("WO-BAD", Decimal::ONE, Decimal::new(8, 0)),
    ];

    let bundle = build_fact_bundle(
        split_result(detail, qty),
        &PipelineRules::for_name(PipelineName::Gb),
    )
    .unwrap();

    assert_eq!(
        bundle
            .error_issues
            .iter()
            .map(|issue| issue.issue_type.as_str())
            .collect::<Vec<_>>(),
        vec![
            "MISSING_AMOUNT",
            "DUPLICATE_WORK_ORDER_KEY",
            "DUPLICATE_WORK_ORDER_KEY",
            "MOH_BREAKDOWN_MISMATCH",
            "TOTAL_COST_MISMATCH",
            "NON_POSITIVE_UNIT_COST",
        ]
    );
}

#[test]
fn gb_total_reconciliation_uses_outsource_as_standalone() {
    let bundle = build_fact_bundle(
        split_result(
            vec![
                detail_row("直接材料", CellValue::Decimal(Decimal::new(100, 0))),
                detail_row("委外加工费", CellValue::Decimal(Decimal::new(5, 0))),
            ],
            vec![qty_row(Decimal::new(10, 0), Decimal::new(105, 0))],
        ),
        &PipelineRules::for_name(PipelineName::Gb),
    )
    .unwrap();

    assert!(bundle.qty_rows[0].total_matches);
    assert_eq!(
        bundle.qty_rows[0].amounts.standalone_amount(0),
        Decimal::new(5, 0)
    );
}

#[test]
fn sk_total_reconciliation_uses_outsource_and_software_as_standalone() {
    let bundle = build_fact_bundle(
        split_result(
            vec![
                detail_row("直接材料", CellValue::Decimal(Decimal::new(100, 0))),
                detail_row("委外加工费", CellValue::Decimal(Decimal::new(5, 0))),
                detail_row("软件费用", CellValue::Decimal(Decimal::new(7, 0))),
            ],
            vec![qty_row(Decimal::new(10, 0), Decimal::new(112, 0))],
        ),
        &PipelineRules::for_name(PipelineName::Sk),
    )
    .unwrap();

    assert!(bundle.qty_rows[0].total_matches);
    assert_eq!(
        bundle.qty_rows[0].amounts.standalone,
        vec![Decimal::new(5, 0), Decimal::new(7, 0)]
    );
}

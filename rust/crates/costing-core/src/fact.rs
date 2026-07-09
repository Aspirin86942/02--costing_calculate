use std::collections::BTreeMap;

use rust_decimal::Decimal;

use crate::error::CostingError;
use crate::model::{CellValue, ErrorIssue, FactBundle, SplitResult, TableRow};
use crate::pipeline::PipelineConfig;

const ZERO: Decimal = Decimal::ZERO;
const DM_AMOUNT_KEY: &str = "dm_amount";
const DL_AMOUNT_KEY: &str = "dl_amount";
const MOH_AMOUNT_KEY: &str = "moh_amount";
const MOH_OTHER_AMOUNT_KEY: &str = "moh_other_amount";
const MOH_LABOR_AMOUNT_KEY: &str = "moh_labor_amount";
const MOH_CONSUMABLES_AMOUNT_KEY: &str = "moh_consumables_amount";
const MOH_DEPRECIATION_AMOUNT_KEY: &str = "moh_depreciation_amount";
const MOH_UTILITIES_AMOUNT_KEY: &str = "moh_utilities_amount";
const COMPLETED_QTY_KEY: &str = "completed_qty";
const COMPLETED_TOTAL_KEY: &str = "completed_amount_total";
const QTY_DM_AMOUNT: &str = "本期完工直接材料合计完工金额";
const QTY_DL_AMOUNT: &str = "本期完工直接人工合计完工金额";
const QTY_MOH_AMOUNT: &str = "本期完工制造费用合计完工金额";
const QTY_MOH_OTHER_AMOUNT: &str = "本期完工制造费用_其他合计完工金额";
const QTY_MOH_LABOR_AMOUNT: &str = "本期完工制造费用_人工合计完工金额";
const QTY_MOH_CONSUMABLES_AMOUNT: &str = "本期完工制造费用_机物料及低耗合计完工金额";
const QTY_MOH_DEPRECIATION_AMOUNT: &str = "本期完工制造费用_折旧合计完工金额";
const QTY_MOH_UTILITIES_AMOUNT: &str = "本期完工制造费用_水电费合计完工金额";
const QTY_DM_UNIT_COST: &str = "直接材料单位完工金额";
const QTY_DL_UNIT_COST: &str = "直接人工单位完工金额";
const QTY_MOH_UNIT_COST: &str = "制造费用单位完工金额";
const QTY_OUTSOURCE_UNIT_COST: &str = "委外加工费单位完工成本";
const QTY_SOFTWARE_UNIT_COST: &str = "软件费用单位完工成本";
const QTY_MOH_MATCH: &str = "制造费用明细项合计是否等于制造费用合计";
const QTY_CHECK_STATUS: &str = "数据校验状态";
const QTY_CHECK_REASON: &str = "异常原因说明";

pub fn build_fact_bundle(
    split: SplitResult,
    config: &PipelineConfig,
) -> Result<FactBundle, CostingError> {
    let mut amount_by_key: BTreeMap<String, BTreeMap<String, Decimal>> = BTreeMap::new();
    let mut qty_rows_by_key: BTreeMap<String, usize> = BTreeMap::new();
    let mut error_issues = Vec::new();

    for row in &split.detail_rows {
        let key = work_order_key(row);
        let cost_item = text(row, "成本项目名称");
        let amount = decimal(row, "本期完工金额");
        if amount.is_none() {
            error_issues.push(ErrorIssue {
                row_id: key.clone(),
                issue_type: "MISSING_AMOUNT".to_string(),
                field_name: "本期完工金额".to_string(),
                reason: "成本明细金额为空，已按 0 参与汇总".to_string(),
                action: "金额置为 0 后继续计算".to_string(),
            });
        }

        let bucket = bucket_name(&cost_item);
        *amount_by_key
            .entry(key)
            .or_default()
            .entry(bucket)
            .or_default() += amount.unwrap_or(ZERO);
    }

    for qty_row in &split.qty_rows {
        let key = work_order_key(qty_row);
        *qty_rows_by_key.entry(key).or_default() += 1;
    }

    for (key, count) in &qty_rows_by_key {
        if *count > 1 {
            error_issues.push(ErrorIssue {
                row_id: key.clone(),
                issue_type: "DUPLICATE_WORK_ORDER_KEY".to_string(),
                field_name: "工单主键".to_string(),
                reason: "数量页存在重复工单主键".to_string(),
                action: "数量页原样保留，异常分析按首条记录去重".to_string(),
            });
        }
    }

    let mut work_order_fact = Vec::new();
    for qty_row in &split.qty_rows {
        let key = work_order_key(qty_row);
        let amounts = amount_by_key.get(&key).cloned().unwrap_or_default();
        let completed_qty = decimal(qty_row, "本期完工数量").unwrap_or(ZERO);
        let completed_total = decimal(qty_row, "本期完工金额").unwrap_or(ZERO);
        let mut values = qty_row.values.clone();

        for (bucket, amount) in amounts {
            values.insert(bucket, CellValue::Decimal(amount));
        }
        values.insert(
            COMPLETED_QTY_KEY.to_string(),
            CellValue::Decimal(completed_qty),
        );
        values.insert(
            COMPLETED_TOTAL_KEY.to_string(),
            CellValue::Decimal(completed_total),
        );

        // 在 fact 阶段就产出审计问题，避免后续写表阶段再分叉业务口径。
        let moh_sum = moh_component_sum(&values);
        let moh_total = decimal_from_values(&values, MOH_AMOUNT_KEY);
        if moh_sum != moh_total {
            error_issues.push(ErrorIssue {
                row_id: key.clone(),
                issue_type: "MOH_BREAKDOWN_MISMATCH".to_string(),
                field_name: "制造费用".to_string(),
                reason: "制造费用明细项合计不等于制造费用合计".to_string(),
                action: "保留结果并标记需复核".to_string(),
            });
        }

        let derived_total = total_amount_from_values(&values, config);
        if derived_total != completed_total {
            error_issues.push(ErrorIssue {
                row_id: key,
                issue_type: "TOTAL_COST_MISMATCH".to_string(),
                field_name: "总完工成本".to_string(),
                reason: format!(
                    "{}不等于数量页总完工成本",
                    total_expression(config.standalone_cost_items)
                ),
                action: "保留结果并标记需复核".to_string(),
            });
        }

        work_order_fact.push(TableRow { values });
    }

    Ok(FactBundle {
        detail_fact: split.detail_rows,
        qty_fact: split.qty_rows,
        work_order_fact,
        error_issues,
    })
}

pub fn build_qty_sheet_rows(bundle: &FactBundle, config: &PipelineConfig) -> Vec<TableRow> {
    bundle
        .work_order_fact
        .iter()
        .map(|row| {
            let mut values = row.values.clone();
            let completed_qty = decimal_from_values(&values, COMPLETED_QTY_KEY);
            let dm = decimal_from_values(&values, DM_AMOUNT_KEY);
            let dl = decimal_from_values(&values, DL_AMOUNT_KEY);
            let moh = decimal_from_values(&values, MOH_AMOUNT_KEY);
            let moh_other = decimal_from_values(&values, MOH_OTHER_AMOUNT_KEY);
            let moh_labor = decimal_from_values(&values, MOH_LABOR_AMOUNT_KEY);
            let moh_consumables = decimal_from_values(&values, MOH_CONSUMABLES_AMOUNT_KEY);
            let moh_depreciation = decimal_from_values(&values, MOH_DEPRECIATION_AMOUNT_KEY);
            let moh_utilities = decimal_from_values(&values, MOH_UTILITIES_AMOUNT_KEY);

            values.insert(QTY_DM_AMOUNT.to_string(), CellValue::Decimal(dm));
            values.insert(QTY_DL_AMOUNT.to_string(), CellValue::Decimal(dl));
            values.insert(QTY_MOH_AMOUNT.to_string(), CellValue::Decimal(moh));
            values.insert(QTY_MOH_OTHER_AMOUNT.to_string(), CellValue::Decimal(moh_other));
            values.insert(QTY_MOH_LABOR_AMOUNT.to_string(), CellValue::Decimal(moh_labor));
            values.insert(
                QTY_MOH_CONSUMABLES_AMOUNT.to_string(),
                CellValue::Decimal(moh_consumables),
            );
            values.insert(
                QTY_MOH_DEPRECIATION_AMOUNT.to_string(),
                CellValue::Decimal(moh_depreciation),
            );
            values.insert(
                QTY_MOH_UTILITIES_AMOUNT.to_string(),
                CellValue::Decimal(moh_utilities),
            );
            values.insert(
                QTY_DM_UNIT_COST.to_string(),
                decimal_or_blank(safe_divide(dm, completed_qty)),
            );
            values.insert(
                QTY_DL_UNIT_COST.to_string(),
                decimal_or_blank(safe_divide(dl, completed_qty)),
            );
            values.insert(
                QTY_MOH_UNIT_COST.to_string(),
                decimal_or_blank(safe_divide(moh, completed_qty)),
            );

            let mut derived_total = dm + dl + moh;
            for item in config.standalone_cost_items {
                let key = standalone_key(item);
                let amount = decimal_from_values(&values, &key);
                values.insert(
                    format!("本期完工{item}合计完工金额"),
                    CellValue::Decimal(amount),
                );
                let unit_cost_column = standalone_unit_cost_column(item);
                values.insert(
                    unit_cost_column.to_string(),
                    decimal_or_blank(safe_divide(amount, completed_qty)),
                );
                derived_total += amount;
            }

            let moh_match = if moh_component_sum(&values) == moh {
                "是"
            } else {
                "否"
            };
            values.insert(
                QTY_MOH_MATCH.to_string(),
                CellValue::Text(moh_match.to_string()),
            );

            let completed_total = decimal_from_values(&values, COMPLETED_TOTAL_KEY);
            let total_match = if derived_total == completed_total {
                "是"
            } else {
                "否"
            };
            values.insert(
                total_match_column(config.standalone_cost_items),
                CellValue::Text(total_match.to_string()),
            );

            let reason = build_check_reason(moh_match, total_match, config.standalone_cost_items);
            values.insert(
                QTY_CHECK_STATUS.to_string(),
                CellValue::Text(if reason.is_empty() { "通过" } else { "需复核" }.to_string()),
            );
            values.insert(QTY_CHECK_REASON.to_string(), CellValue::Text(reason));
            TableRow { values }
        })
        .collect()
}

fn build_check_reason(moh_match: &str, total_match: &str, standalone_items: &[&str]) -> String {
    let total_mismatch_reason = format!("{}与总完工成本不一致", total_expression(standalone_items));
    match (moh_match, total_match) {
        ("否", "否") => format!("制造费用明细与合计不一致;{total_mismatch_reason}"),
        ("否", _) => "制造费用明细与合计不一致".to_string(),
        (_, "否") => total_mismatch_reason,
        _ => String::new(),
    }
}

fn total_expression(standalone_items: &[&str]) -> String {
    let mut parts = vec!["直接材料", "直接人工", "制造费用"];
    parts.extend(standalone_items.iter().copied());
    parts.join("+")
}

fn total_amount_from_values(values: &BTreeMap<String, CellValue>, config: &PipelineConfig) -> Decimal {
    let mut total = decimal_from_values(values, DM_AMOUNT_KEY)
        + decimal_from_values(values, DL_AMOUNT_KEY)
        + decimal_from_values(values, MOH_AMOUNT_KEY);
    for item in config.standalone_cost_items {
        total += decimal_from_values(values, &standalone_key(item));
    }
    total
}

fn moh_component_sum(values: &BTreeMap<String, CellValue>) -> Decimal {
    decimal_from_values(values, MOH_OTHER_AMOUNT_KEY)
        + decimal_from_values(values, MOH_LABOR_AMOUNT_KEY)
        + decimal_from_values(values, MOH_CONSUMABLES_AMOUNT_KEY)
        + decimal_from_values(values, MOH_DEPRECIATION_AMOUNT_KEY)
        + decimal_from_values(values, MOH_UTILITIES_AMOUNT_KEY)
}

fn work_order_key(row: &TableRow) -> String {
    ["月份", "年期", "产品编码", "工单编号", "工单行号"]
        .iter()
        .filter_map(|column| row.values.get(*column).map(cell_to_text))
        .collect::<Vec<_>>()
        .join("|")
}

fn bucket_name(cost_item: &str) -> String {
    match cost_item.trim() {
        "直接材料" => DM_AMOUNT_KEY.to_string(),
        "直接人工" => DL_AMOUNT_KEY.to_string(),
        "制造费用_其他" => MOH_OTHER_AMOUNT_KEY.to_string(),
        "制造费用-人工" => MOH_LABOR_AMOUNT_KEY.to_string(),
        "制造费用_机物料及低耗" => MOH_CONSUMABLES_AMOUNT_KEY.to_string(),
        "制造费用_折旧" => MOH_DEPRECIATION_AMOUNT_KEY.to_string(),
        "制造费用_水电费" => MOH_UTILITIES_AMOUNT_KEY.to_string(),
        value if value.starts_with("制造费用") => MOH_AMOUNT_KEY.to_string(),
        "委外加工费" => standalone_key("委外加工费"),
        "软件费用" => standalone_key("软件费用"),
        other => format!("unmapped:{other}"),
    }
}

fn standalone_key(item: &str) -> String {
    match item.trim() {
        "委外加工费" => "outsource_amount".to_string(),
        "软件费用" => "software_amount".to_string(),
        other => format!("standalone:{other}"),
    }
}

fn standalone_unit_cost_column(item: &str) -> &'static str {
    match item.trim() {
        "委外加工费" => QTY_OUTSOURCE_UNIT_COST,
        "软件费用" => QTY_SOFTWARE_UNIT_COST,
        _ => "独立成本项单位完工成本",
    }
}

fn total_match_column(items: &[&str]) -> String {
    format!("{}是否等于总完工成本", total_expression(items))
}

fn text(row: &TableRow, column: &str) -> String {
    row.values
        .get(column)
        .map(cell_to_text)
        .unwrap_or_default()
}

fn decimal(row: &TableRow, column: &str) -> Option<Decimal> {
    row.values.get(column).and_then(cell_to_decimal)
}

fn decimal_from_values(values: &BTreeMap<String, CellValue>, column: &str) -> Decimal {
    values.get(column).and_then(cell_to_decimal).unwrap_or(ZERO)
}

fn decimal_or_blank(value: Option<Decimal>) -> CellValue {
    value.map(CellValue::Decimal).unwrap_or(CellValue::Blank)
}

fn safe_divide(numerator: Decimal, denominator: Decimal) -> Option<Decimal> {
    if denominator == ZERO {
        None
    } else {
        Some(numerator / denominator)
    }
}

fn cell_to_text(value: &CellValue) -> String {
    match value {
        CellValue::Blank => String::new(),
        CellValue::Text(value) | CellValue::DateLike(value) => value.clone(),
        CellValue::Decimal(value) => value.normalize().to_string(),
    }
}

fn cell_to_decimal(value: &CellValue) -> Option<Decimal> {
    match value {
        CellValue::Decimal(value) => Some(*value),
        CellValue::Text(value) => value.trim().parse().ok(),
        CellValue::Blank | CellValue::DateLike(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::model::{CellValue, SplitResult, TableRow};
    use crate::pipeline::{PipelineConfig, PipelineName};

    use super::*;

    fn row(values: &[(&str, CellValue)]) -> TableRow {
        TableRow {
            values: values
                .iter()
                .map(|(key, value)| ((*key).to_string(), value.clone()))
                .collect(),
        }
    }

    #[test]
    fn gb_quantity_sheet_includes_outsource_total_match() {
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
        let config = PipelineConfig::for_name(PipelineName::Gb);
        let bundle = build_fact_bundle(
            SplitResult {
                detail_rows: detail,
                qty_rows: qty,
            },
            &config,
        )
        .unwrap();
        let sheet = build_qty_sheet_rows(&bundle, &config);
        assert_eq!(
            sheet[0].values["本期完工委外加工费合计完工金额"],
            CellValue::Decimal(Decimal::new(5, 0))
        );
        assert_eq!(
            sheet[0].values["直接材料+直接人工+制造费用+委外加工费是否等于总完工成本"],
            CellValue::Text("是".to_string())
        );
    }

    #[test]
    fn sk_quantity_sheet_includes_software_fee() {
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
        let config = PipelineConfig::for_name(PipelineName::Sk);
        let bundle = build_fact_bundle(
            SplitResult {
                detail_rows: detail,
                qty_rows: qty,
            },
            &config,
        )
        .unwrap();
        let sheet = build_qty_sheet_rows(&bundle, &config);
        assert_eq!(
            sheet[0].values["本期完工软件费用合计完工金额"],
            CellValue::Decimal(Decimal::new(7, 0))
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
            SplitResult {
                detail_rows: detail,
                qty_rows: qty,
            },
            &PipelineConfig::for_name(PipelineName::Gb),
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
    fn qty_sheet_marks_moh_and_total_mismatch() {
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
        let config = PipelineConfig::for_name(PipelineName::Gb);
        let bundle = build_fact_bundle(
            SplitResult {
                detail_rows: detail,
                qty_rows: qty,
            },
            &config,
        )
        .unwrap();
        let sheet = build_qty_sheet_rows(&bundle, &config);

        assert_eq!(
            sheet[0].values["制造费用明细项合计是否等于制造费用合计"],
            CellValue::Text("否".to_string())
        );
        assert_eq!(
            sheet[0].values["直接材料+直接人工+制造费用+委外加工费是否等于总完工成本"],
            CellValue::Text("否".to_string())
        );
        assert!(bundle
            .error_issues
            .iter()
            .any(|issue| issue.issue_type == "MOH_BREAKDOWN_MISMATCH"));
        assert!(bundle
            .error_issues
            .iter()
            .any(|issue| issue.issue_type == "TOTAL_COST_MISMATCH"));
    }
}

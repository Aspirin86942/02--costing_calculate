use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;

use crate::model::{CellValue, FactBundle, QualityMetric, TableRow};

pub fn build_quality_metrics(bundle: &FactBundle) -> Vec<QualityMetric> {
    vec![
        QualityMetric {
            category: "行数勾稽".to_string(),
            metric: "成本明细输入行数".to_string(),
            value: bundle.detail_fact.len().to_string(),
            description: "原始拆分后的成本明细行数".to_string(),
        },
        QualityMetric {
            category: "行数勾稽".to_string(),
            metric: "产品数量统计输入行数".to_string(),
            value: bundle.qty_input_row_count.to_string(),
            description: "拆分后的数量页原始行数".to_string(),
        },
        QualityMetric {
            category: "行数勾稽".to_string(),
            metric: "产品数量统计输出行数".to_string(),
            value: bundle.qty_fact.len().to_string(),
            description: "仅保留完工数量大于 0 且总完工成本非空的工单".to_string(),
        },
        QualityMetric {
            category: "行数勾稽".to_string(),
            metric: "因完工数量无效被过滤行数".to_string(),
            value: bundle.filtered_invalid_qty_count.to_string(),
            description: "过滤条件包含完工数量为空、等于 0 或小于 0".to_string(),
        },
        QualityMetric {
            category: "行数勾稽".to_string(),
            metric: "因总完工成本为空被过滤行数".to_string(),
            value: bundle.filtered_missing_total_amount_count.to_string(),
            description: "仅统计完工数量有效但总完工成本为空的工单".to_string(),
        },
        QualityMetric {
            category: "行数勾稽".to_string(),
            metric: "工单异常分析输出行数".to_string(),
            value: bundle.work_order_fact.len().to_string(),
            description: "去重后的工单级分析行数".to_string(),
        },
        QualityMetric {
            category: "空值率".to_string(),
            metric: "直接材料金额缺失率".to_string(),
            value: format_rate(null_rate(&bundle.qty_fact, "dm_amount")),
            description: "派生金额字段空值率".to_string(),
        },
        QualityMetric {
            category: "唯一性检查".to_string(),
            metric: "工单主键重复行数".to_string(),
            value: duplicate_work_order_row_count(&bundle.qty_fact).to_string(),
            description: "键：月份+产品编码+工单编号+工单行".to_string(),
        },
        QualityMetric {
            category: "范围检查".to_string(),
            metric: "完工数量小于等于0行数".to_string(),
            value: non_positive_qty_count(&bundle.qty_fact).to_string(),
            description: "保留后的数量事实不应存在非正完工数量".to_string(),
        },
        QualityMetric {
            category: "分析覆盖率".to_string(),
            metric: "可参与分析占比".to_string(),
            value: format_rate(analyzable_rate(&bundle.work_order_fact)),
            description: "按完工数量、总单位成本和单据类型归类估算可分析工单占比".to_string(),
        },
    ]
}

fn null_rate(rows: &[TableRow], column: &str) -> f64 {
    if rows.is_empty() {
        return 0.0;
    }
    let null_count = rows
        .iter()
        // fact 中未出现的派生金额 bucket 会在数量页按 0 写出，不应计为空值。
        .filter(|row| row.values.get(column).is_some_and(is_blank_like))
        .count();
    null_count as f64 / rows.len() as f64
}

fn duplicate_work_order_row_count(rows: &[TableRow]) -> usize {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for row in rows {
        *counts.entry(work_order_key(row)).or_default() += 1;
    }
    let duplicate_keys = counts
        .iter()
        .filter_map(|(key, count)| if *count > 1 { Some(key.clone()) } else { None })
        .collect::<BTreeSet<_>>();
    rows.iter()
        .filter(|row| duplicate_keys.contains(&work_order_key(row)))
        .count()
}

fn non_positive_qty_count(rows: &[TableRow]) -> usize {
    rows.iter()
        .filter(|row| {
            row.values
                .get("completed_qty")
                .and_then(cell_to_decimal)
                .map(|value| value <= Decimal::ZERO)
                .unwrap_or(true)
        })
        .count()
}

fn analyzable_rate(rows: &[TableRow]) -> f64 {
    if rows.is_empty() {
        return 0.0;
    }
    let analyzable = rows
        .iter()
        .filter(|row| {
            let qty = row
                .values
                .get("completed_qty")
                .and_then(cell_to_decimal)
                .unwrap_or(Decimal::ZERO);
            let total = row
                .values
                .get("completed_amount_total")
                .and_then(cell_to_decimal)
                .unwrap_or(Decimal::ZERO);
            qty > Decimal::ZERO && total > Decimal::ZERO && has_analyzable_doc_type(row)
        })
        .count();
    analyzable as f64 / rows.len() as f64
}

fn has_analyzable_doc_type(row: &TableRow) -> bool {
    matches!(
        text_any(row, &["doc_type", "单据类型"]).trim(),
        "汇报入库-普通生产" | "直接入库-普通生产" | "汇报入库-返工生产"
    )
}

fn format_rate(value: f64) -> String {
    format!("{:.2}%", value * 100.0)
}

fn work_order_key(row: &TableRow) -> String {
    let period = row
        .values
        .get("月份")
        .or_else(|| row.values.get("年期"))
        .map(cell_to_text)
        .unwrap_or_default();
    [
        period,
        text(row, "产品编码"),
        text(row, "工单编号"),
        text(row, "工单行号"),
    ]
    .join("|")
}

fn text(row: &TableRow, column: &str) -> String {
    row.values.get(column).map(cell_to_text).unwrap_or_default()
}

fn text_any(row: &TableRow, columns: &[&str]) -> String {
    columns
        .iter()
        .find_map(|column| row.values.get(*column).map(cell_to_text))
        .unwrap_or_default()
}

fn is_blank_like(value: &CellValue) -> bool {
    match value {
        CellValue::Blank => true,
        CellValue::Text(value) | CellValue::DateLike(value) => value.trim().is_empty(),
        CellValue::Decimal(_) => false,
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
    use std::collections::BTreeMap;

    use crate::model::{CellValue, FactBundle, TableRow};

    use super::*;

    #[test]
    fn quality_metrics_report_fact_row_counts() {
        let bundle = FactBundle {
            detail_columns: Vec::new(),
            detail_fact: vec![TableRow {
                values: BTreeMap::new(),
            }],
            qty_columns: Vec::new(),
            qty_input_row_count: 3,
            filtered_invalid_qty_count: 1,
            filtered_missing_total_amount_count: 0,
            qty_fact: vec![
                TableRow {
                    values: BTreeMap::from([
                        (
                            "月份".to_string(),
                            CellValue::Text("2025年01期".to_string()),
                        ),
                        ("产品编码".to_string(), CellValue::Text("P1".to_string())),
                        ("工单编号".to_string(), CellValue::Text("WO1".to_string())),
                        ("工单行号".to_string(), CellValue::Text("1".to_string())),
                        (
                            "completed_qty".to_string(),
                            CellValue::Decimal(Decimal::ONE),
                        ),
                        (
                            "completed_amount_total".to_string(),
                            CellValue::Decimal(Decimal::new(100, 0)),
                        ),
                    ]),
                },
                TableRow {
                    values: BTreeMap::from([
                        (
                            "月份".to_string(),
                            CellValue::Text("2025年01期".to_string()),
                        ),
                        ("产品编码".to_string(), CellValue::Text("P1".to_string())),
                        ("工单编号".to_string(), CellValue::Text("WO1".to_string())),
                        ("工单行号".to_string(), CellValue::Text("1".to_string())),
                        (
                            "completed_qty".to_string(),
                            CellValue::Decimal(Decimal::ONE),
                        ),
                        (
                            "completed_amount_total".to_string(),
                            CellValue::Decimal(Decimal::new(100, 0)),
                        ),
                    ]),
                },
            ],
            work_order_fact: vec![
                TableRow {
                    values: BTreeMap::from([
                        (
                            "completed_qty".to_string(),
                            CellValue::Decimal(Decimal::ONE),
                        ),
                        (
                            "completed_amount_total".to_string(),
                            CellValue::Decimal(Decimal::new(100, 0)),
                        ),
                        (
                            "单据类型".to_string(),
                            CellValue::Text("汇报入库-普通生产".to_string()),
                        ),
                    ]),
                },
                TableRow {
                    values: BTreeMap::from([
                        (
                            "completed_qty".to_string(),
                            CellValue::Decimal(Decimal::ONE),
                        ),
                        (
                            "completed_amount_total".to_string(),
                            CellValue::Decimal(Decimal::new(100, 0)),
                        ),
                        (
                            "单据类型".to_string(),
                            CellValue::Text("其他入库".to_string()),
                        ),
                    ]),
                },
            ],
            error_issues: Vec::new(),
        };

        let metrics = build_quality_metrics(&bundle);
        let metric_map = metrics
            .iter()
            .map(|metric| (metric.metric.as_str(), metric))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(metric_map["成本明细输入行数"].value, "1");
        assert_eq!(metric_map["产品数量统计输入行数"].value, "3");
        assert_eq!(metric_map["产品数量统计输出行数"].value, "2");
        assert_eq!(metric_map["因完工数量无效被过滤行数"].value, "1");
        assert_eq!(metric_map["因总完工成本为空被过滤行数"].value, "0");
        assert_eq!(metric_map["直接材料金额缺失率"].category, "空值率");
        assert_eq!(metric_map["直接材料金额缺失率"].value, "0.00%");
        assert_eq!(metric_map["工单主键重复行数"].category, "唯一性检查");
        assert_eq!(metric_map["可参与分析占比"].category, "分析覆盖率");
        assert_eq!(metric_map["可参与分析占比"].value, "50.00%");
    }
}

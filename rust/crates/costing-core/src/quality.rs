use rust_decimal::Decimal;

use crate::error::CostingError;
use crate::model::{CellValue, FactBundle, QtyFactRow, QualityMetric};
use crate::table::ColumnSchema;

pub fn build_quality_metrics(
    bundle: &FactBundle,
    month_filter_empty_result: bool,
) -> Result<Vec<QualityMetric>, CostingError> {
    let not_applicable_description = "月份过滤后无数据，指标不适用";
    let null_rate_value = if month_filter_empty_result {
        "N/A".to_string()
    } else {
        // 数量事实中的直接材料始终是 Decimal；缺失明细已在 fact 阶段按 0 聚合。
        format_rate(0.0)
    };
    let coverage_value = if month_filter_empty_result {
        "N/A".to_string()
    } else {
        format_rate(analyzable_rate(bundle)?)
    };

    Ok(vec![
        QualityMetric {
            category: "行数勾稽".to_string(),
            metric: "成本明细输入行数".to_string(),
            value: bundle.detail_row_count().to_string(),
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
            value: bundle.qty_row_count().to_string(),
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
            value: bundle.work_order_row_count().to_string(),
            description: "去重后的工单级分析行数".to_string(),
        },
        QualityMetric {
            category: "空值率".to_string(),
            metric: "直接材料金额缺失率".to_string(),
            value: null_rate_value,
            description: if month_filter_empty_result {
                not_applicable_description.to_string()
            } else {
                "派生金额字段空值率".to_string()
            },
        },
        QualityMetric {
            category: "唯一性检查".to_string(),
            metric: "工单主键重复行数".to_string(),
            value: bundle.duplicate_work_order_row_count.to_string(),
            description: "键：月份+产品编码+工单编号+工单行".to_string(),
        },
        QualityMetric {
            category: "范围检查".to_string(),
            metric: "完工数量小于等于0行数".to_string(),
            value: non_positive_qty_count(&bundle.qty_rows).to_string(),
            description: "保留后的数量事实不应存在非正完工数量".to_string(),
        },
        QualityMetric {
            category: "分析覆盖率".to_string(),
            metric: "可参与分析占比".to_string(),
            value: coverage_value,
            description: if month_filter_empty_result {
                not_applicable_description.to_string()
            } else {
                "按完工数量、总单位成本和单据类型归类估算可分析工单占比".to_string()
            },
        },
    ])
}

fn non_positive_qty_count(rows: &[QtyFactRow]) -> usize {
    rows.iter()
        .filter(|row| row.completed_qty <= Decimal::ZERO)
        .count()
}

fn analyzable_rate(bundle: &FactBundle) -> Result<f64, CostingError> {
    let total = bundle.work_order_row_count();
    if total == 0 {
        return Ok(0.0);
    }
    let mut analyzable = 0usize;
    for row in bundle.work_order_rows() {
        if row.completed_qty > Decimal::ZERO
            && row.completed_total > Decimal::ZERO
            && has_analyzable_doc_type(&bundle.schema, row)?
        {
            analyzable += 1;
        }
    }
    Ok(analyzable as f64 / total as f64)
}

fn has_analyzable_doc_type(schema: &ColumnSchema, row: &QtyFactRow) -> Result<bool, CostingError> {
    Ok(matches!(
        text_any(schema, row, &["doc_type", "单据类型"])?.trim(),
        "汇报入库-普通生产" | "直接入库-普通生产" | "汇报入库-返工生产"
    ))
}

fn format_rate(value: f64) -> String {
    format!("{:.2}%", value * 100.0)
}

fn text_any(
    schema: &ColumnSchema,
    row: &QtyFactRow,
    columns: &[&str],
) -> Result<String, CostingError> {
    for column in columns {
        if let Some(id) = schema.optional(column) {
            return Ok(cell_to_text(row.source.get(id)?));
        }
    }
    Ok(String::new())
}

fn cell_to_text(value: &CellValue) -> String {
    match value {
        CellValue::Blank => String::new(),
        CellValue::Text(value) | CellValue::DateLike(value) => value.clone(),
        CellValue::Decimal(value) => value.normalize().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::model::{CellValue, CostAmounts, FactBundle, QtyFactRow};
    use crate::table::IndexedTable;

    use super::*;

    fn qty_fact(
        source: crate::table::IndexedRow,
        key: &str,
        completed_qty: Decimal,
        completed_total: Decimal,
    ) -> QtyFactRow {
        QtyFactRow {
            source,
            work_order_key: key.to_string(),
            completed_qty,
            completed_total,
            amounts: CostAmounts::new(0),
            moh_matches: true,
            total_matches: true,
            check_reason: String::new(),
        }
    }

    #[test]
    fn quality_metrics_report_fact_row_counts() {
        let columns = vec![
            "月份".to_string(),
            "产品编码".to_string(),
            "工单编号".to_string(),
            "工单行号".to_string(),
            "completed_qty".to_string(),
            "completed_amount_total".to_string(),
            "单据类型".to_string(),
        ];
        let source_rows = vec![
            vec![CellValue::Blank; columns.len()],
            vec![
                CellValue::Text("2025年01期".to_string()),
                CellValue::Text("P1".to_string()),
                CellValue::Text("WO1".to_string()),
                CellValue::Text("1".to_string()),
                CellValue::Decimal(Decimal::ONE),
                CellValue::Decimal(Decimal::new(100, 0)),
                CellValue::Text("汇报入库-普通生产".to_string()),
            ],
            vec![
                CellValue::Text("2025年01期".to_string()),
                CellValue::Text("P1".to_string()),
                CellValue::Text("WO1".to_string()),
                CellValue::Text("1".to_string()),
                CellValue::Decimal(Decimal::ONE),
                CellValue::Decimal(Decimal::new(100, 0)),
                CellValue::Text("其他入库".to_string()),
            ],
        ];
        let table = IndexedTable::from_raw(columns, source_rows).unwrap();
        let (schema, display, mut rows) = table.into_parts();
        let detail_rows = vec![rows.remove(0)];
        let qty_rows = rows
            .into_iter()
            .enumerate()
            .map(|(index, source)| {
                qty_fact(
                    source,
                    &format!("key-{index}"),
                    Decimal::ONE,
                    Decimal::new(100, 0),
                )
            })
            .collect::<Vec<_>>();
        let bundle = FactBundle {
            schema,
            detail_display_columns: display.clone(),
            detail_rows,
            qty_display_columns: display,
            qty_input_row_count: 3,
            filtered_invalid_qty_count: 1,
            filtered_missing_total_amount_count: 0,
            qty_rows,
            unique_work_order_indices: vec![0, 1],
            duplicate_work_order_row_count: 2,
            error_issues: Vec::new(),
        };

        let metrics = build_quality_metrics(&bundle, false).unwrap();
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

    #[test]
    fn month_filter_empty_result_marks_rates_not_applicable() {
        let table = IndexedTable::from_raw(Vec::new(), Vec::new()).unwrap();
        let (schema, display, _) = table.into_parts();
        let bundle = FactBundle {
            schema,
            detail_display_columns: display.clone(),
            detail_rows: Vec::new(),
            qty_display_columns: display,
            qty_input_row_count: 0,
            filtered_invalid_qty_count: 0,
            filtered_missing_total_amount_count: 0,
            qty_rows: Vec::new(),
            unique_work_order_indices: Vec::new(),
            duplicate_work_order_row_count: 0,
            error_issues: Vec::new(),
        };

        let metrics = build_quality_metrics(&bundle, true).unwrap();
        let metric_map = metrics
            .iter()
            .map(|metric| (metric.metric.as_str(), metric))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(metric_map["直接材料金额缺失率"].value, "N/A");
        assert_eq!(metric_map["可参与分析占比"].value, "N/A");
        assert_eq!(
            metric_map["可参与分析占比"].description,
            "月份过滤后无数据，指标不适用"
        );
    }

    #[test]
    fn foreign_schema_row_error_is_propagated() {
        let table = IndexedTable::from_raw(vec!["单据类型".to_string()], vec![]).unwrap();
        let (schema, display, _) = table.into_parts();
        let foreign = IndexedTable::from_raw(
            vec!["单据类型".to_string()],
            vec![vec![CellValue::Text("汇报入库-普通生产".to_string())]],
        )
        .unwrap();
        let (_, _, mut foreign_rows) = foreign.into_parts();
        let bundle = FactBundle {
            schema,
            detail_display_columns: Vec::new(),
            detail_rows: Vec::new(),
            qty_display_columns: display,
            qty_input_row_count: 1,
            filtered_invalid_qty_count: 0,
            filtered_missing_total_amount_count: 0,
            qty_rows: vec![qty_fact(
                foreign_rows.pop().unwrap(),
                "key",
                Decimal::ONE,
                Decimal::ONE,
            )],
            unique_work_order_indices: vec![0],
            duplicate_work_order_row_count: 0,
            error_issues: Vec::new(),
        };

        let error = build_quality_metrics(&bundle, false).unwrap_err();

        assert_eq!(error.code(), crate::error::ErrorCode::InternalError);
    }

    #[test]
    fn quality_reuses_fact_duplicate_count_without_rebuilding_keys() {
        let table = IndexedTable::from_raw(Vec::new(), Vec::new()).unwrap();
        let (schema, display, _) = table.into_parts();
        let bundle = FactBundle {
            schema,
            detail_display_columns: display.clone(),
            detail_rows: Vec::new(),
            qty_display_columns: display,
            qty_rows: Vec::new(),
            unique_work_order_indices: Vec::new(),
            qty_input_row_count: 0,
            filtered_invalid_qty_count: 0,
            filtered_missing_total_amount_count: 0,
            duplicate_work_order_row_count: 7,
            error_issues: Vec::new(),
        };

        let metrics = build_quality_metrics(&bundle, false).unwrap();
        let duplicate = metrics
            .iter()
            .find(|metric| metric.metric == "工单主键重复行数")
            .unwrap();

        assert_eq!(duplicate.value, "7");
    }

    #[test]
    fn quality_uses_unique_indices_for_analysis_coverage() {
        let columns = vec!["单据类型".to_string()];
        let table = IndexedTable::from_raw(
            columns,
            vec![
                vec![CellValue::Text("汇报入库-普通生产".to_string())],
                vec![CellValue::Text("汇报入库-普通生产".to_string())],
                vec![CellValue::Text("汇报入库-普通生产".to_string())],
                vec![CellValue::Text("其他入库".to_string())],
                vec![CellValue::Text("汇报入库-普通生产".to_string())],
            ],
        )
        .unwrap();
        let (schema, display, rows) = table.into_parts();
        let qty_rows = rows
            .into_iter()
            .enumerate()
            .map(|(index, source)| {
                let (qty, total) = match index {
                    2 => (Decimal::ONE, Decimal::ZERO),
                    4 => (Decimal::ZERO, Decimal::ONE),
                    _ => (Decimal::ONE, Decimal::ONE),
                };
                qty_fact(source, &format!("key-{index}"), qty, total)
            })
            .collect();
        let bundle = FactBundle {
            schema,
            detail_display_columns: display.clone(),
            detail_rows: Vec::new(),
            qty_display_columns: display,
            qty_rows,
            // index 0 是重复键的非首条；覆盖率只看首次索引 1..4。
            unique_work_order_indices: vec![1, 2, 3, 4],
            qty_input_row_count: 5,
            filtered_invalid_qty_count: 0,
            filtered_missing_total_amount_count: 0,
            duplicate_work_order_row_count: 2,
            error_issues: Vec::new(),
        };

        let metrics = build_quality_metrics(&bundle, false).unwrap();
        let coverage = metrics
            .iter()
            .find(|metric| metric.metric == "可参与分析占比")
            .unwrap();

        assert_eq!(coverage.value, "25.00%");
    }

    #[test]
    fn quality_reports_zero_non_positive_qty_after_fact_filter() {
        let table = IndexedTable::from_raw(
            vec!["单据类型".to_string()],
            vec![vec![CellValue::Text("汇报入库-普通生产".to_string())]],
        )
        .unwrap();
        let (schema, display, mut rows) = table.into_parts();
        let qty_rows = vec![QtyFactRow {
            source: rows.pop().unwrap(),
            work_order_key: "key".to_string(),
            completed_qty: Decimal::ONE,
            completed_total: Decimal::ONE,
            amounts: CostAmounts::new(0),
            moh_matches: true,
            total_matches: true,
            check_reason: String::new(),
        }];
        let bundle = FactBundle {
            schema,
            detail_display_columns: display.clone(),
            detail_rows: Vec::new(),
            qty_display_columns: display,
            qty_rows,
            unique_work_order_indices: vec![0],
            qty_input_row_count: 1,
            filtered_invalid_qty_count: 0,
            filtered_missing_total_amount_count: 0,
            duplicate_work_order_row_count: 0,
            error_issues: Vec::new(),
        };

        let metrics = build_quality_metrics(&bundle, false).unwrap();
        let non_positive = metrics
            .iter()
            .find(|metric| metric.metric == "完工数量小于等于0行数")
            .unwrap();

        assert_eq!(non_positive.value, "0");
    }
}

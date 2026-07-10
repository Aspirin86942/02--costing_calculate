use rust_decimal::Decimal;

use crate::error::CostingError;
use crate::model::{CellValue, FactBundle, IndexedFactRow, QualityMetric};
use crate::table::ColumnSchema;

pub fn build_quality_metrics(
    bundle: &FactBundle,
    month_filter_empty_result: bool,
) -> Result<Vec<QualityMetric>, CostingError> {
    let not_applicable_description = "月份过滤后无数据，指标不适用";
    let null_rate_value = if month_filter_empty_result {
        "N/A".to_string()
    } else {
        format_rate(null_rate(&bundle.schema, &bundle.qty_rows, "dm_amount")?)
    };
    let coverage_value = if month_filter_empty_result {
        "N/A".to_string()
    } else {
        format_rate(analyzable_rate(&bundle.schema, &bundle.work_order_rows)?)
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
            value: non_positive_qty_count(&bundle.schema, &bundle.qty_rows)?.to_string(),
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

fn null_rate(
    schema: &ColumnSchema,
    rows: &[IndexedFactRow],
    column: &str,
) -> Result<f64, CostingError> {
    if rows.is_empty() {
        return Ok(0.0);
    }
    let mut null_count = 0usize;
    for row in rows {
        // fact 中未出现的派生金额 bucket 会在数量页按 0 写出，不应计为空值。
        if row.get_named(schema, column)?.is_some_and(is_blank_like) {
            null_count += 1;
        }
    }
    Ok(null_count as f64 / rows.len() as f64)
}

fn non_positive_qty_count(
    schema: &ColumnSchema,
    rows: &[IndexedFactRow],
) -> Result<usize, CostingError> {
    let mut count = 0usize;
    for row in rows {
        if row
            .get_named(schema, "completed_qty")?
            .and_then(cell_to_decimal)
            .map(|value| value <= Decimal::ZERO)
            .unwrap_or(true)
        {
            count += 1;
        }
    }
    Ok(count)
}

fn analyzable_rate(schema: &ColumnSchema, rows: &[IndexedFactRow]) -> Result<f64, CostingError> {
    if rows.is_empty() {
        return Ok(0.0);
    }
    let mut analyzable = 0usize;
    for row in rows {
        let qty = row
            .get_named(schema, "completed_qty")?
            .and_then(cell_to_decimal)
            .unwrap_or(Decimal::ZERO);
        let total = row
            .get_named(schema, "completed_amount_total")?
            .and_then(cell_to_decimal)
            .unwrap_or(Decimal::ZERO);
        if qty > Decimal::ZERO && total > Decimal::ZERO && has_analyzable_doc_type(schema, row)? {
            analyzable += 1;
        }
    }
    Ok(analyzable as f64 / rows.len() as f64)
}

fn has_analyzable_doc_type(
    schema: &ColumnSchema,
    row: &IndexedFactRow,
) -> Result<bool, CostingError> {
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
    row: &IndexedFactRow,
    columns: &[&str],
) -> Result<String, CostingError> {
    for column in columns {
        if let Some(value) = row.get_named(schema, column)? {
            return Ok(cell_to_text(value));
        }
    }
    Ok(String::new())
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

    use crate::model::{CellValue, FactBundle, IndexedFactRow};
    use crate::table::IndexedTable;

    use super::*;

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
            .iter()
            .cloned()
            .map(IndexedFactRow::new)
            .collect::<Vec<_>>();
        let work_order_rows = rows.into_iter().map(IndexedFactRow::new).collect();
        let bundle = FactBundle {
            schema,
            detail_display_columns: display.clone(),
            detail_rows,
            qty_display_columns: display,
            qty_input_row_count: 3,
            filtered_invalid_qty_count: 1,
            filtered_missing_total_amount_count: 0,
            qty_rows,
            work_order_rows,
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
            work_order_rows: Vec::new(),
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
        let table = IndexedTable::from_raw(vec!["completed_qty".to_string()], vec![]).unwrap();
        let (schema, display, _) = table.into_parts();
        let foreign = IndexedTable::from_raw(
            vec!["completed_qty".to_string()],
            vec![vec![CellValue::Decimal(Decimal::ONE)]],
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
            qty_rows: vec![IndexedFactRow::new(foreign_rows.pop().unwrap())],
            work_order_rows: Vec::new(),
            duplicate_work_order_row_count: 0,
            error_issues: Vec::new(),
        };

        let error = build_quality_metrics(&bundle, false).unwrap_err();

        assert_eq!(error.code(), crate::error::ErrorCode::InternalError);
    }
}

use crate::error::CostingError;
use crate::model::{CellValue, NormalizedCostFrame, SplitResult};
use crate::sheet_contract::{detail_sheet_columns, qty_sheet_base_columns};
use crate::table::{ColumnId, ColumnSchema, IndexedRow};

const CHILD_MATERIAL_COLUMN: &str = "子项物料编码";
const COST_ITEM_COLUMN: &str = "成本项目名称";
const FILLED_COST_ITEM_COLUMN: &str = "Filled_成本项目";
const ORDER_NUMBER_COLUMN: &str = "工单编号";
const DIRECT_MATERIAL_NAME: &str = "直接材料";

struct SplitColumns {
    child_material: Option<ColumnId>,
    cost_item: Option<ColumnId>,
    filled_cost_item: Option<ColumnId>,
    order_number: Option<ColumnId>,
}

impl SplitColumns {
    fn resolve(schema: &ColumnSchema) -> Self {
        Self {
            child_material: schema.optional(CHILD_MATERIAL_COLUMN),
            cost_item: schema.optional(COST_ITEM_COLUMN),
            filled_cost_item: schema.optional(FILLED_COST_ITEM_COLUMN),
            order_number: schema.optional(ORDER_NUMBER_COLUMN),
        }
    }
}

pub fn split_detail_and_qty(frame: NormalizedCostFrame) -> Result<SplitResult, CostingError> {
    let table = frame.into_table();
    let columns = SplitColumns::resolve(table.schema());
    let (schema, source_display_order, rows) = table.into_parts();
    let source_names = source_display_order
        .iter()
        .map(|id| schema.name(*id).map(str::to_string))
        .collect::<Result<Vec<_>, _>>()?;
    let detail_names = detail_sheet_columns(&source_names);
    let qty_names = qty_sheet_base_columns(&source_names);
    let detail_display_columns = schema.display_order_for(&detail_names)?;
    let qty_display_columns = schema.display_order_for(&qty_names)?;
    let mut detail_rows = Vec::new();
    let mut qty_rows = Vec::new();

    for mut row in rows {
        let material_text = row_text(&row, columns.child_material)?;
        let cost_item_text = row_text(&row, columns.cost_item)?;
        let has_material = material_text.is_some();
        let no_material = !has_material;
        let no_cost_item = cost_item_text.is_none();
        let expense_mask = no_material
            && cost_item_text
                .as_deref()
                .map(|value| value != DIRECT_MATERIAL_NAME)
                .unwrap_or(false);
        let has_order =
            columns.order_number.is_none() || row_text(&row, columns.order_number)?.is_some();

        if no_material && no_cost_item && has_order {
            qty_rows.push(row);
            continue;
        }

        if has_material || expense_mask {
            if let (Some(filled), Some(cost_item)) = (columns.filled_cost_item, columns.cost_item) {
                let filled_cost_item = row.take(filled)?;
                row.replace(cost_item, filled_cost_item)?;
            }
            detail_rows.push(row);
        }
    }

    Ok(SplitResult {
        schema,
        detail_display_columns,
        detail_rows,
        qty_display_columns,
        qty_rows,
    })
}

fn row_text(row: &IndexedRow, column: Option<ColumnId>) -> Result<Option<String>, CostingError> {
    column
        .map(|id| row.get(id).map(cell_text))
        .transpose()
        .map(Option::flatten)
}

fn cell_text(value: &CellValue) -> Option<String> {
    match value {
        CellValue::Blank => None,
        CellValue::Text(text) | CellValue::DateLike(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        CellValue::Decimal(value) => Some(value.normalize().to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::model::{CellValue, NormalizedCostFrame};
    use crate::table::IndexedTable;

    use super::*;

    #[test]
    fn splits_detail_and_qty_rows_by_python_masks() {
        let qty = BTreeMap::from([
            (
                "工单编号".to_string(),
                CellValue::Text("WO-QTY".to_string()),
            ),
            ("成本项目名称".to_string(), CellValue::Blank),
            ("子项物料编码".to_string(), CellValue::Blank),
        ]);
        let detail_from_material = BTreeMap::from([
            (
                "工单编号".to_string(),
                CellValue::Text("WO-MAT".to_string()),
            ),
            (
                "成本项目名称".to_string(),
                CellValue::Text("直接材料".to_string()),
            ),
            (
                "子项物料编码".to_string(),
                CellValue::Text("MAT-1".to_string()),
            ),
        ]);
        let detail_from_expense = BTreeMap::from([
            (
                "工单编号".to_string(),
                CellValue::Text("WO-EXP".to_string()),
            ),
            (
                "成本项目名称".to_string(),
                CellValue::Text("制造费用".to_string()),
            ),
            ("子项物料编码".to_string(), CellValue::Blank),
        ]);

        let result = split_detail_and_qty(indexed_frame(
            &[ORDER_NUMBER_COLUMN, COST_ITEM_COLUMN, CHILD_MATERIAL_COLUMN],
            vec![qty, detail_from_material, detail_from_expense],
        ))
        .unwrap();

        assert_eq!(result.qty_rows().len(), 1);
        assert_eq!(result.detail_rows().len(), 2);
    }

    #[test]
    fn detail_rows_use_filled_cost_item_when_present() {
        let detail = BTreeMap::from([
            ("工单编号".to_string(), CellValue::Text("WO-1".to_string())),
            ("成本项目名称".to_string(), CellValue::Blank),
            (
                "Filled_成本项目".to_string(),
                CellValue::Text("直接人工".to_string()),
            ),
            (
                "子项物料编码".to_string(),
                CellValue::Text("MAT-1".to_string()),
            ),
        ]);

        let result = split_detail_and_qty(indexed_frame(
            &[
                ORDER_NUMBER_COLUMN,
                COST_ITEM_COLUMN,
                FILLED_COST_ITEM_COLUMN,
                CHILD_MATERIAL_COLUMN,
            ],
            vec![detail],
        ))
        .unwrap();
        let cost_item = result.schema().require(COST_ITEM_COLUMN).unwrap();

        assert_eq!(
            result.detail_rows()[0].get(cost_item).unwrap(),
            &CellValue::Text("直接人工".to_string())
        );
    }

    #[test]
    fn split_columns_follow_python_sheet_contracts() {
        let columns = [
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
            "Filled_成本项目",
            "本期完工数量",
            "本期完工金额",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
        let qty = BTreeMap::from([
            (
                "工单编号".to_string(),
                CellValue::Text("WO-QTY".to_string()),
            ),
            ("成本项目名称".to_string(), CellValue::Blank),
            ("子项物料编码".to_string(), CellValue::Blank),
        ]);
        let detail = BTreeMap::from([
            (
                "工单编号".to_string(),
                CellValue::Text("WO-DETAIL".to_string()),
            ),
            (
                "成本项目名称".to_string(),
                CellValue::Text("直接材料".to_string()),
            ),
            (
                "子项物料编码".to_string(),
                CellValue::Text("MAT-1".to_string()),
            ),
            (
                "Filled_成本项目".to_string(),
                CellValue::Text("直接材料".to_string()),
            ),
        ]);

        let source_columns = columns.iter().map(String::as_str).collect::<Vec<_>>();
        let result =
            split_detail_and_qty(indexed_frame(&source_columns, vec![qty, detail])).unwrap();
        let detail_names = result
            .detail_display_columns
            .iter()
            .map(|id| result.schema.name(*id).unwrap())
            .collect::<Vec<_>>();
        let qty_names = result
            .qty_display_columns
            .iter()
            .map(|id| result.schema.name(*id).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            detail_names,
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
        assert_eq!(
            qty_names,
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
            ]
        );
    }

    type NamedTestRow = BTreeMap<String, CellValue>;

    fn indexed_frame(columns: &[&str], named_rows: Vec<NamedTestRow>) -> NormalizedCostFrame {
        let columns = columns
            .iter()
            .map(|column| (*column).to_string())
            .collect::<Vec<_>>();
        let positional = named_rows
            .into_iter()
            .map(|mut named| {
                columns
                    .iter()
                    .map(|column| named.remove(column).unwrap_or(CellValue::Blank))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        NormalizedCostFrame::new(IndexedTable::from_raw(columns, positional).unwrap(), vec![])
    }

    fn order_numbers(
        result_rows: &[crate::table::IndexedRow],
        result: &SplitResult,
    ) -> Vec<String> {
        let order = result.schema().require(ORDER_NUMBER_COLUMN).unwrap();
        result_rows
            .iter()
            .map(|row| cell_text(row.get(order).unwrap()).unwrap_or_default())
            .collect()
    }

    #[test]
    fn ignores_rows_that_match_neither_detail_nor_qty() {
        let frame = indexed_frame(
            &[ORDER_NUMBER_COLUMN, COST_ITEM_COLUMN, CHILD_MATERIAL_COLUMN],
            vec![BTreeMap::from([
                (ORDER_NUMBER_COLUMN.to_string(), CellValue::Blank),
                (COST_ITEM_COLUMN.to_string(), CellValue::Blank),
                (CHILD_MATERIAL_COLUMN.to_string(), CellValue::Blank),
            ])],
        );

        let result = split_detail_and_qty(frame).unwrap();

        assert!(result.detail_rows().is_empty());
        assert!(result.qty_rows().is_empty());
    }

    #[test]
    fn missing_order_column_still_allows_qty_row() {
        let frame = indexed_frame(
            &[COST_ITEM_COLUMN, CHILD_MATERIAL_COLUMN],
            vec![BTreeMap::new()],
        );

        let result = split_detail_and_qty(frame).unwrap();

        assert_eq!(result.qty_rows().len(), 1);
    }

    #[test]
    fn preserves_input_order_within_each_partition() {
        let row = |order: &str, cost_item: CellValue, material: CellValue| {
            BTreeMap::from([
                (
                    ORDER_NUMBER_COLUMN.to_string(),
                    CellValue::Text(order.to_string()),
                ),
                (COST_ITEM_COLUMN.to_string(), cost_item),
                (CHILD_MATERIAL_COLUMN.to_string(), material),
            ])
        };
        let frame = indexed_frame(
            &[ORDER_NUMBER_COLUMN, COST_ITEM_COLUMN, CHILD_MATERIAL_COLUMN],
            vec![
                row("Q-1", CellValue::Blank, CellValue::Blank),
                row(
                    "D-1",
                    CellValue::Text("直接材料".to_string()),
                    CellValue::Text("M-1".to_string()),
                ),
                row("Q-2", CellValue::Blank, CellValue::Blank),
                row(
                    "D-2",
                    CellValue::Text("制造费用".to_string()),
                    CellValue::Blank,
                ),
            ],
        );

        let result = split_detail_and_qty(frame).unwrap();

        assert_eq!(order_numbers(result.detail_rows(), &result), ["D-1", "D-2"]);
        assert_eq!(order_numbers(result.qty_rows(), &result), ["Q-1", "Q-2"]);
    }

    #[test]
    fn duplicate_source_columns_do_not_duplicate_contract_display_columns() {
        let table = IndexedTable::from_raw(
            vec![
                ORDER_NUMBER_COLUMN.to_string(),
                ORDER_NUMBER_COLUMN.to_string(),
                COST_ITEM_COLUMN.to_string(),
                CHILD_MATERIAL_COLUMN.to_string(),
            ],
            vec![vec![
                CellValue::Text("OLD".to_string()),
                CellValue::Text("WO-1".to_string()),
                CellValue::Blank,
                CellValue::Blank,
            ]],
        )
        .unwrap();
        let result = split_detail_and_qty(NormalizedCostFrame::new(table, vec![])).unwrap();
        let (_, _, _, qty_display, _) = result.into_parts();

        assert_eq!(qty_display.len(), 1);
    }
}

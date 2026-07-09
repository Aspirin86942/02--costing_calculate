use crate::error::CostingError;
use crate::model::{CellValue, NormalizedCostFrame, SplitResult, TableRow};

const CHILD_MATERIAL_COLUMN: &str = "子项物料编码";
const COST_ITEM_COLUMN: &str = "成本项目名称";
const FILLED_COST_ITEM_COLUMN: &str = "Filled_成本项目";
const ORDER_NUMBER_COLUMN: &str = "工单编号";
const DIRECT_MATERIAL_NAME: &str = "直接材料";

pub fn split_detail_and_qty(frame: NormalizedCostFrame) -> Result<SplitResult, CostingError> {
    let source_columns = frame.columns;
    let mut detail_rows = Vec::new();
    let mut qty_rows = Vec::new();

    for mut row in frame.rows {
        let material_text = row_text(&row, CHILD_MATERIAL_COLUMN);
        let cost_item_text = row_text(&row, COST_ITEM_COLUMN);
        let has_material = material_text.is_some();
        let no_material = !has_material;
        let no_cost_item = cost_item_text.is_none();
        let expense_mask = no_material
            && cost_item_text
                .as_deref()
                .map(|value| value != DIRECT_MATERIAL_NAME)
                .unwrap_or(false);
        let has_order = row_text(&row, ORDER_NUMBER_COLUMN).is_some()
            || !row.values.contains_key(ORDER_NUMBER_COLUMN);

        if no_material && no_cost_item && has_order {
            qty_rows.push(row);
            continue;
        }

        if has_material || expense_mask {
            if row.values.contains_key(FILLED_COST_ITEM_COLUMN)
                && row.values.contains_key(COST_ITEM_COLUMN)
            {
                if let Some(filled_cost_item) = row.values.get(FILLED_COST_ITEM_COLUMN).cloned() {
                    row.values
                        .insert(COST_ITEM_COLUMN.to_string(), filled_cost_item);
                }
            }
            detail_rows.push(row);
        }
    }

    Ok(SplitResult {
        detail_columns: source_columns.clone(),
        detail_rows,
        qty_columns: source_columns,
        qty_rows,
    })
}

fn row_text(row: &TableRow, column: &str) -> Option<String> {
    row.values.get(column).and_then(cell_text)
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

    use crate::model::{CellValue, NormalizedCostFrame, TableRow};

    use super::*;

    #[test]
    fn splits_detail_and_qty_rows_by_python_masks() {
        let qty = TableRow {
            values: BTreeMap::from([
                (
                    "工单编号".to_string(),
                    CellValue::Text("WO-QTY".to_string()),
                ),
                ("成本项目名称".to_string(), CellValue::Blank),
                ("子项物料编码".to_string(), CellValue::Blank),
            ]),
        };
        let detail_from_material = TableRow {
            values: BTreeMap::from([
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
            ]),
        };
        let detail_from_expense = TableRow {
            values: BTreeMap::from([
                (
                    "工单编号".to_string(),
                    CellValue::Text("WO-EXP".to_string()),
                ),
                (
                    "成本项目名称".to_string(),
                    CellValue::Text("制造费用".to_string()),
                ),
                ("子项物料编码".to_string(), CellValue::Blank),
            ]),
        };

        let result = split_detail_and_qty(NormalizedCostFrame {
            columns: vec![],
            rows: vec![qty, detail_from_material, detail_from_expense],
            key_columns: vec![],
        })
        .unwrap();

        assert_eq!(result.qty_rows.len(), 1);
        assert_eq!(result.detail_rows.len(), 2);
        assert!(result.detail_columns.is_empty());
        assert!(result.qty_columns.is_empty());
    }

    #[test]
    fn detail_rows_use_filled_cost_item_when_present() {
        let detail = TableRow {
            values: BTreeMap::from([
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
            ]),
        };

        let result = split_detail_and_qty(NormalizedCostFrame {
            columns: vec![],
            rows: vec![detail],
            key_columns: vec![],
        })
        .unwrap();

        assert_eq!(
            result.detail_rows[0].values["成本项目名称"],
            CellValue::Text("直接人工".to_string())
        );
    }
}

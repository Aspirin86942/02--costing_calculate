use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Serialize, Serializer};

use crate::error::CostingError;
use crate::model::CellValue;

static NEXT_SCHEMA_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SchemaId(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ColumnId {
    schema_id: SchemaId,
    slot: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct ColumnSchema {
    schema_id: SchemaId,
    names_by_id: Vec<String>,
    id_by_name: HashMap<String, ColumnId>,
}

impl ColumnSchema {
    fn new(names: Vec<String>) -> Result<Self, CostingError> {
        let raw_id = NEXT_SCHEMA_ID
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| CostingError::internal("SchemaId counter exhausted"))?;
        let schema_id = SchemaId(raw_id);
        let mut id_by_name = HashMap::with_capacity(names.len());
        for (slot, name) in names.iter().enumerate() {
            id_by_name.insert(name.clone(), ColumnId { schema_id, slot });
        }
        Ok(Self {
            schema_id,
            names_by_id: names,
            id_by_name,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.names_by_id.len()
    }

    pub(crate) fn require(&self, name: &str) -> Result<ColumnId, CostingError> {
        self.optional(name)
            .ok_or_else(|| CostingError::invalid_input(format!("缺少必要列: {name}")))
    }

    pub(crate) fn optional(&self, name: &str) -> Option<ColumnId> {
        self.id_by_name.get(name).copied()
    }

    pub(crate) fn name(&self, id: ColumnId) -> Result<&str, CostingError> {
        let slot = self.validate_id(id)?;
        Ok(&self.names_by_id[slot])
    }

    pub(crate) fn display_order_for(
        &self,
        required_names: &[String],
    ) -> Result<Vec<ColumnId>, CostingError> {
        let missing = required_names
            .iter()
            .filter(|name| !self.id_by_name.contains_key(name.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(CostingError::invalid_input(format!(
                "缺少必要列: {}",
                missing.join(", ")
            )));
        }
        Ok(required_names
            .iter()
            .map(|name| self.id_by_name[name])
            .collect())
    }

    fn validate_id(&self, id: ColumnId) -> Result<usize, CostingError> {
        if id.schema_id != self.schema_id || id.slot >= self.names_by_id.len() {
            return Err(CostingError::internal(
                "ColumnId does not belong to this schema",
            ));
        }
        Ok(id.slot)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct IndexedRow {
    schema_id: SchemaId,
    cells: Vec<CellValue>,
}

impl IndexedRow {
    pub(crate) fn get(&self, id: ColumnId) -> Result<&CellValue, CostingError> {
        let slot = self.validate_id(id)?;
        Ok(&self.cells[slot])
    }

    pub(crate) fn get_mut(&mut self, id: ColumnId) -> Result<&mut CellValue, CostingError> {
        let slot = self.validate_id(id)?;
        Ok(&mut self.cells[slot])
    }

    pub(crate) fn replace(
        &mut self,
        id: ColumnId,
        value: CellValue,
    ) -> Result<CellValue, CostingError> {
        Ok(std::mem::replace(self.get_mut(id)?, value))
    }

    pub(crate) fn take(&mut self, id: ColumnId) -> Result<CellValue, CostingError> {
        self.replace(id, CellValue::Blank)
    }

    fn validate_id(&self, id: ColumnId) -> Result<usize, CostingError> {
        if id.schema_id != self.schema_id || id.slot >= self.cells.len() {
            return Err(CostingError::internal(
                "ColumnId does not belong to this row",
            ));
        }
        Ok(id.slot)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct IndexedTable {
    schema: ColumnSchema,
    source_display_order: Vec<ColumnId>,
    rows: Vec<IndexedRow>,
}

impl IndexedTable {
    pub(crate) fn from_raw(
        source_names: Vec<String>,
        rows: Vec<Vec<CellValue>>,
    ) -> Result<Self, CostingError> {
        let schema = ColumnSchema::new(source_names.clone())?;
        // 重复列保留物理槽位；按名称访问和展示时保持“最后一列生效”的兼容语义。
        let source_display_order = schema.display_order_for(&source_names)?;
        let width = schema.len();
        let rows = rows
            .into_iter()
            .map(|mut cells| {
                cells.truncate(width);
                cells.resize(width, CellValue::Blank);
                IndexedRow {
                    schema_id: schema.schema_id,
                    cells,
                }
            })
            .collect();
        Ok(Self {
            schema,
            source_display_order,
            rows,
        })
    }

    pub(crate) fn schema(&self) -> &ColumnSchema {
        &self.schema
    }

    pub(crate) fn rows(&self) -> &[IndexedRow] {
        &self.rows
    }

    pub(crate) fn into_parts(self) -> (ColumnSchema, Vec<ColumnId>, Vec<IndexedRow>) {
        (self.schema, self.source_display_order, self.rows)
    }
}

impl PartialEq for IndexedTable {
    fn eq(&self, other: &Self) -> bool {
        let Ok(left_display_slots) = display_slots(&self.schema, &self.source_display_order) else {
            return false;
        };
        let Ok(right_display_slots) = display_slots(&other.schema, &other.source_display_order)
        else {
            return false;
        };
        self.schema.names_by_id == other.schema.names_by_id
            && left_display_slots == right_display_slots
            && self
                .rows
                .iter()
                .map(|row| &row.cells)
                .eq(other.rows.iter().map(|row| &row.cells))
    }
}

#[derive(Serialize)]
struct IndexedTableSnapshot<'a> {
    names_by_id: &'a [String],
    source_display_slots: Vec<usize>,
    rows: Vec<&'a [CellValue]>,
}

impl Serialize for IndexedTable {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let source_display_slots = display_slots(&self.schema, &self.source_display_order)
            .map_err(serde::ser::Error::custom)?;
        IndexedTableSnapshot {
            names_by_id: &self.schema.names_by_id,
            source_display_slots,
            rows: self.rows.iter().map(|row| row.cells.as_slice()).collect(),
        }
        .serialize(serializer)
    }
}

fn display_slots(
    schema: &ColumnSchema,
    display_order: &[ColumnId],
) -> Result<Vec<usize>, CostingError> {
    display_order
        .iter()
        .map(|id| schema.validate_id(*id))
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::error::ErrorCode;
    use crate::model::CellValue;

    use super::*;

    #[test]
    fn from_raw_pads_short_rows_with_blank() {
        let table = IndexedTable::from_raw(
            vec!["产品编码".to_string(), "产品名称".to_string()],
            vec![vec![CellValue::Text("A".to_string())]],
        )
        .unwrap();

        assert_eq!(
            table.rows()[0].cells,
            vec![CellValue::Text("A".to_string()), CellValue::Blank]
        );
    }

    #[test]
    fn from_raw_truncates_long_rows() {
        let table = IndexedTable::from_raw(
            vec!["产品编码".to_string()],
            vec![vec![
                CellValue::Text("A".to_string()),
                CellValue::Text("ignored".to_string()),
            ]],
        )
        .unwrap();

        assert_eq!(
            table.rows()[0].cells,
            vec![CellValue::Text("A".to_string())]
        );
    }

    #[test]
    fn duplicate_column_names_resolve_to_last_physical_slot() {
        let table = IndexedTable::from_raw(
            vec![
                "产品编码".to_string(),
                "产品名称".to_string(),
                "产品编码".to_string(),
            ],
            vec![vec![
                CellValue::Text("first".to_string()),
                CellValue::Text("name".to_string()),
                CellValue::Text("last".to_string()),
            ]],
        )
        .unwrap();
        let id = table.schema().require("产品编码").unwrap();

        assert_eq!(id.slot, 2);
        assert_eq!(
            table.rows()[0].get(id).unwrap(),
            &CellValue::Text("last".to_string())
        );
        assert_eq!(
            table
                .source_display_order
                .iter()
                .map(|id| id.slot)
                .collect::<Vec<_>>(),
            vec![2, 1, 2]
        );
        assert_eq!(table.rows()[0].cells.len(), 3);
    }

    #[test]
    fn foreign_schema_column_id_returns_internal_error_even_when_slot_exists() {
        let left = IndexedTable::from_raw(
            vec!["产品编码".to_string()],
            vec![vec![CellValue::Text("A".to_string())]],
        )
        .unwrap();
        let right = IndexedTable::from_raw(
            vec!["产品编码".to_string()],
            vec![vec![CellValue::Text("B".to_string())]],
        )
        .unwrap();
        let foreign = left.schema().require("产品编码").unwrap();

        let error = right.rows()[0].get(foreign).unwrap_err();

        assert_eq!(error.code(), ErrorCode::InternalError);
    }

    #[test]
    fn invalid_column_id_returns_internal_error_without_panicking() {
        let table = IndexedTable::from_raw(
            vec!["产品编码".to_string()],
            vec![vec![CellValue::Text("A".to_string())]],
        )
        .unwrap();
        let invalid = ColumnId {
            schema_id: table.schema.schema_id,
            slot: table.schema.len(),
        };

        let error = table.rows()[0].get(invalid).unwrap_err();

        assert_eq!(error.code(), ErrorCode::InternalError);
    }

    #[test]
    fn logically_equal_tables_ignore_schema_id_in_equality_and_serialization() {
        let left = IndexedTable::from_raw(
            vec!["产品编码".to_string()],
            vec![vec![CellValue::Text("A".to_string())]],
        )
        .unwrap();
        let right = IndexedTable::from_raw(
            vec!["产品编码".to_string()],
            vec![vec![CellValue::Text("A".to_string())]],
        )
        .unwrap();

        assert_ne!(left.schema.schema_id, right.schema.schema_id);
        assert_eq!(left, right);
        assert_eq!(
            serde_json::to_vec(&left).unwrap(),
            serde_json::to_vec(&right).unwrap()
        );
    }

    #[test]
    fn optional_missing_column_returns_none() {
        let table = IndexedTable::from_raw(vec!["产品编码".to_string()], vec![]).unwrap();

        assert_eq!(table.schema().optional("产品名称"), None);
    }

    #[test]
    fn display_order_reports_all_missing_columns_once() {
        let table = IndexedTable::from_raw(vec!["产品编码".to_string()], vec![]).unwrap();
        let requested = vec![
            "缺列一".to_string(),
            "产品编码".to_string(),
            "缺列二".to_string(),
        ];

        let error = table.schema().display_order_for(&requested).unwrap_err();

        assert_eq!(error.code(), ErrorCode::InvalidInput);
        assert_eq!(error.message(), "缺少必要列: 缺列一, 缺列二");
    }

    #[test]
    fn take_moves_value_and_leaves_blank() {
        let mut table = IndexedTable::from_raw(
            vec!["产品编码".to_string()],
            vec![vec![CellValue::Text("A".to_string())]],
        )
        .unwrap();
        let id = table.schema().require("产品编码").unwrap();

        let taken = table.rows[0].take(id).unwrap();

        assert_eq!(taken, CellValue::Text("A".to_string()));
        assert_eq!(table.rows[0].get(id).unwrap(), &CellValue::Blank);
    }
}

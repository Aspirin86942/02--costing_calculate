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

    fn append(&mut self, name: String) -> ColumnId {
        let id = ColumnId {
            schema_id: self.schema_id,
            slot: self.names_by_id.len(),
        };
        self.names_by_id.push(name.clone());
        self.id_by_name.insert(name, id);
        id
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

    fn validate_shape(
        &self,
        expected_schema_id: SchemaId,
        expected_width: usize,
    ) -> Result<(), CostingError> {
        if self.schema_id != expected_schema_id || self.cells.len() != expected_width {
            return Err(CostingError::internal(
                "IndexedRow shape does not match its table schema",
            ));
        }
        Ok(())
    }

    fn push_validated_cell(&mut self, value: CellValue) {
        // 只能由已完成全表 shape 校验的 IndexedTable 调用。
        self.cells.push(value);
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum DerivedColumnPosition<'a> {
    End,
    AfterFirstSourceName(&'a str),
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

    pub(crate) fn try_update_rows<F>(&mut self, mut update: F) -> Result<(), CostingError>
    where
        F: FnMut(&mut IndexedRow) -> Result<(), CostingError>,
    {
        for row in &mut self.rows {
            update(row)?;
        }
        Ok(())
    }

    pub(crate) fn try_retain_rows<F>(&mut self, mut predicate: F) -> Result<(), CostingError>
    where
        F: FnMut(&IndexedRow) -> Result<bool, CostingError>,
    {
        let keep = self
            .rows
            .iter()
            .map(&mut predicate)
            .collect::<Result<Vec<_>, _>>()?;
        let mut index = 0usize;
        self.rows.retain(|_| {
            let retain = keep[index];
            index += 1;
            retain
        });
        Ok(())
    }

    pub(crate) fn ensure_or_reuse_derived_column(
        &mut self,
        name: &str,
        display_position: DerivedColumnPosition<'_>,
        values: Vec<CellValue>,
    ) -> Result<ColumnId, CostingError> {
        if values.len() != self.rows.len() {
            return Err(CostingError::invalid_input(format!(
                "派生列 {name} 的值数量 {} 与行数 {} 不一致",
                values.len(),
                self.rows.len(),
            )));
        }
        for row in &self.rows {
            row.validate_shape(self.schema.schema_id, self.schema.len())?;
        }

        if let Some(id) = self.schema.optional(name) {
            for (row, value) in self.rows.iter_mut().zip(values) {
                row.replace(id, value)?;
            }
            return Ok(id);
        }

        let id = self.schema.append(name.to_string());
        for (row, value) in self.rows.iter_mut().zip(values) {
            row.push_validated_cell(value);
        }
        match display_position {
            DerivedColumnPosition::End => self.source_display_order.push(id),
            DerivedColumnPosition::AfterFirstSourceName(source_name) => {
                let insert_at = self
                    .source_display_order
                    .iter()
                    .position(|source_id| {
                        matches!(
                            self.schema.name(*source_id),
                            Ok(name) if name == source_name
                        )
                    })
                    .map_or(self.source_display_order.len(), |index| index + 1);
                self.source_display_order.insert(insert_at, id);
            }
        }
        Ok(id)
    }

    pub(crate) fn into_parts(self) -> (ColumnSchema, Vec<ColumnId>, Vec<IndexedRow>) {
        (self.schema, self.source_display_order, self.rows)
    }
}

#[derive(Debug, Clone, Copy)]
enum ProjectionMode {
    Clone,
    Take,
}

#[derive(Debug, Clone, Copy)]
struct ProjectionStep {
    id: ColumnId,
    mode: ProjectionMode,
}

#[derive(Debug, Clone)]
pub(crate) struct ProjectionPlan {
    expected_schema_id: SchemaId,
    expected_width: usize,
    steps: Vec<ProjectionStep>,
}

impl ProjectionPlan {
    pub(crate) fn new(
        schema: &ColumnSchema,
        display_columns: &[ColumnId],
    ) -> Result<Self, CostingError> {
        let mut last_positions = HashMap::new();
        for (index, id) in display_columns.iter().copied().enumerate() {
            schema.validate_id(id)?;
            last_positions.insert(id, index);
        }
        let steps = display_columns
            .iter()
            .copied()
            .enumerate()
            .map(|(index, id)| ProjectionStep {
                id,
                mode: if last_positions[&id] == index {
                    ProjectionMode::Take
                } else {
                    ProjectionMode::Clone
                },
            })
            .collect();
        Ok(Self {
            expected_schema_id: schema.schema_id,
            expected_width: schema.len(),
            steps,
        })
    }

    pub(crate) fn project_row(&self, mut row: IndexedRow) -> Result<Vec<CellValue>, CostingError> {
        row.validate_shape(self.expected_schema_id, self.expected_width)?;
        for step in &self.steps {
            row.validate_id(step.id)?;
        }
        self.steps
            .iter()
            .map(|step| match step.mode {
                ProjectionMode::Clone => Ok(row.get(step.id)?.clone()),
                ProjectionMode::Take => row.take(step.id),
            })
            .collect()
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

    #[test]
    fn adding_derived_column_preserves_existing_column_ids() {
        let mut table = IndexedTable::from_raw(
            vec!["产品编码".to_string(), "产品名称".to_string()],
            vec![vec![
                CellValue::Text("P1".to_string()),
                CellValue::Text("产品一".to_string()),
            ]],
        )
        .unwrap();
        let product_code = table.schema().require("产品编码").unwrap();
        let product_name = table.schema().require("产品名称").unwrap();

        let derived = table
            .ensure_or_reuse_derived_column(
                "月份",
                DerivedColumnPosition::End,
                vec![CellValue::Text("2026-07".to_string())],
            )
            .unwrap();

        assert_eq!(table.schema().require("产品编码").unwrap(), product_code);
        assert_eq!(table.schema().require("产品名称").unwrap(), product_name);
        assert_eq!(derived.slot, 2);
        assert_eq!(
            table.rows()[0].get(product_code).unwrap(),
            &CellValue::Text("P1".to_string())
        );
    }

    #[test]
    fn ensure_derived_column_updates_schema_rows_and_display_order_atomically() {
        let mut table = IndexedTable::from_raw(
            vec!["期间".to_string(), "产品编码".to_string()],
            vec![
                vec![
                    CellValue::Text("2026-07".to_string()),
                    CellValue::Text("P1".to_string()),
                ],
                vec![
                    CellValue::Text("2026-08".to_string()),
                    CellValue::Text("P2".to_string()),
                ],
            ],
        )
        .unwrap();

        let month = table
            .ensure_or_reuse_derived_column(
                "月份",
                DerivedColumnPosition::AfterFirstSourceName("期间"),
                vec![
                    CellValue::Text("2026-07".to_string()),
                    CellValue::Text("2026-08".to_string()),
                ],
            )
            .unwrap();

        assert_eq!(table.schema().len(), 3);
        assert_eq!(table.schema().require("月份").unwrap(), month);
        assert_eq!(
            table
                .source_display_order
                .iter()
                .map(|id| id.slot)
                .collect::<Vec<_>>(),
            vec![0, 2, 1]
        );
        assert_eq!(
            table
                .rows()
                .iter()
                .map(|row| row.cells.len())
                .collect::<Vec<_>>(),
            vec![3, 3]
        );
        assert_eq!(
            table.rows()[1].get(month).unwrap(),
            &CellValue::Text("2026-08".to_string())
        );
    }

    #[test]
    fn ensure_derived_column_rejects_wrong_value_count_without_mutation() {
        let mut table = IndexedTable::from_raw(
            vec!["产品编码".to_string()],
            vec![vec![CellValue::Text("P1".to_string())]],
        )
        .unwrap();
        let before = table.clone();

        let error = table
            .ensure_or_reuse_derived_column("月份", DerivedColumnPosition::End, vec![])
            .unwrap_err();

        assert_eq!(error.code(), ErrorCode::InvalidInput);
        assert_eq!(table, before);
    }

    #[test]
    fn ensure_derived_column_reuses_last_duplicate_without_moving_display_order() {
        let mut table = IndexedTable::from_raw(
            vec![
                "产品编码".to_string(),
                "月份".to_string(),
                "月份".to_string(),
            ],
            vec![vec![
                CellValue::Text("P1".to_string()),
                CellValue::Text("first".to_string()),
                CellValue::Text("last".to_string()),
            ]],
        )
        .unwrap();
        let existing = table.schema().require("月份").unwrap();
        let names_before = table.schema.names_by_id.clone();
        let display_before = display_slots(&table.schema, &table.source_display_order).unwrap();

        let reused = table
            .ensure_or_reuse_derived_column(
                "月份",
                DerivedColumnPosition::AfterFirstSourceName("产品编码"),
                vec![CellValue::Text("2026-07".to_string())],
            )
            .unwrap();

        assert_eq!(reused, existing);
        assert_eq!(table.schema.names_by_id, names_before);
        assert_eq!(
            display_slots(&table.schema, &table.source_display_order).unwrap(),
            display_before
        );
        assert_eq!(
            table.rows()[0].cells,
            vec![
                CellValue::Text("P1".to_string()),
                CellValue::Text("first".to_string()),
                CellValue::Text("2026-07".to_string()),
            ]
        );
    }

    #[test]
    fn ensure_derived_column_rejects_malformed_row_shape_without_mutation() {
        let mut table = IndexedTable::from_raw(
            vec!["产品编码".to_string(), "产品名称".to_string()],
            vec![
                vec![
                    CellValue::Text("P1".to_string()),
                    CellValue::Text("产品一".to_string()),
                ],
                vec![
                    CellValue::Text("P2".to_string()),
                    CellValue::Text("产品二".to_string()),
                ],
            ],
        )
        .unwrap();
        table.rows[1].cells.pop();
        let names_before = table.schema.names_by_id.clone();
        let display_before = display_slots(&table.schema, &table.source_display_order).unwrap();
        let cells_before = table
            .rows
            .iter()
            .map(|row| row.cells.clone())
            .collect::<Vec<_>>();
        let row_lengths_before = table
            .rows
            .iter()
            .map(|row| row.cells.len())
            .collect::<Vec<_>>();

        let error = table
            .ensure_or_reuse_derived_column(
                "月份",
                DerivedColumnPosition::End,
                vec![
                    CellValue::Text("2026-07".to_string()),
                    CellValue::Text("2026-08".to_string()),
                ],
            )
            .unwrap_err();

        assert_eq!(error.code(), ErrorCode::InternalError);
        assert_eq!(table.schema.names_by_id, names_before);
        assert_eq!(
            display_slots(&table.schema, &table.source_display_order).unwrap(),
            display_before
        );
        assert_eq!(
            table
                .rows
                .iter()
                .map(|row| row.cells.len())
                .collect::<Vec<_>>(),
            row_lengths_before
        );
        assert_eq!(
            table
                .rows
                .iter()
                .map(|row| row.cells.clone())
                .collect::<Vec<_>>(),
            cells_before
        );
    }

    #[test]
    fn try_update_rows_changes_cells_without_changing_row_shape() {
        let mut table = IndexedTable::from_raw(
            vec!["产品编码".to_string()],
            vec![
                vec![CellValue::Text("P1".to_string())],
                vec![CellValue::Text("P2".to_string())],
            ],
        )
        .unwrap();
        let product_code = table.schema().require("产品编码").unwrap();
        let row_lengths_before = table
            .rows()
            .iter()
            .map(|row| row.cells.len())
            .collect::<Vec<_>>();

        table
            .try_update_rows(|row| {
                row.replace(product_code, CellValue::Text("updated".to_string()))?;
                Ok(())
            })
            .unwrap();

        assert_eq!(
            table
                .rows()
                .iter()
                .map(|row| row.cells.len())
                .collect::<Vec<_>>(),
            row_lengths_before
        );
        assert_eq!(
            table.rows()[0].get(product_code).unwrap(),
            &CellValue::Text("updated".to_string())
        );
        assert_eq!(
            table.rows()[1].get(product_code).unwrap(),
            &CellValue::Text("updated".to_string())
        );
    }

    #[test]
    fn try_retain_rows_propagates_access_error_without_filtering() {
        let mut table = IndexedTable::from_raw(
            vec!["产品编码".to_string()],
            vec![
                vec![CellValue::Text("P1".to_string())],
                vec![CellValue::Text("P2".to_string())],
            ],
        )
        .unwrap();
        let foreign_table = IndexedTable::from_raw(vec!["产品编码".to_string()], vec![]).unwrap();
        let foreign_id = foreign_table.schema().require("产品编码").unwrap();
        let rows_before = table.rows.clone();

        let error = table
            .try_retain_rows(|row| row.get(foreign_id).map(|_| true))
            .unwrap_err();

        assert_eq!(error.code(), ErrorCode::InternalError);
        assert_eq!(
            table
                .rows
                .iter()
                .map(|row| row.cells.as_slice())
                .collect::<Vec<_>>(),
            rows_before
                .iter()
                .map(|row| row.cells.as_slice())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn projection_plan_clones_duplicate_ids_until_last_occurrence() {
        let table = IndexedTable::from_raw(
            vec!["产品编码".to_string()],
            vec![vec![CellValue::Text("P1".to_string())]],
        )
        .unwrap();
        let id = table.schema().require("产品编码").unwrap();
        let plan = ProjectionPlan::new(table.schema(), &[id, id]).unwrap();
        let (_, _, mut rows) = table.into_parts();

        let projected = plan.project_row(rows.pop().unwrap()).unwrap();

        assert_eq!(
            projected,
            vec![
                CellValue::Text("P1".to_string()),
                CellValue::Text("P1".to_string()),
            ]
        );
    }

    #[test]
    fn projection_plan_rejects_foreign_row_even_when_empty() {
        let table = IndexedTable::from_raw(vec!["产品编码".to_string()], vec![]).unwrap();
        let plan = ProjectionPlan::new(table.schema(), &[]).unwrap();
        let foreign_table = IndexedTable::from_raw(
            vec!["产品编码".to_string()],
            vec![vec![CellValue::Text("P1".to_string())]],
        )
        .unwrap();
        let (_, _, mut foreign_rows) = foreign_table.into_parts();

        let error = plan.project_row(foreign_rows.pop().unwrap()).unwrap_err();

        assert_eq!(error.code(), ErrorCode::InternalError);
    }

    #[test]
    fn projection_plan_rejects_malformed_row_shape_when_projecting_subset() {
        let mut table = IndexedTable::from_raw(
            vec!["产品编码".to_string(), "产品名称".to_string()],
            vec![vec![
                CellValue::Text("P1".to_string()),
                CellValue::Text("产品一".to_string()),
            ]],
        )
        .unwrap();
        let product_code = table.schema().require("产品编码").unwrap();
        let plan = ProjectionPlan::new(table.schema(), &[product_code]).unwrap();
        let mut malformed_row = table.rows.pop().unwrap();
        malformed_row.cells.pop();

        let error = plan.project_row(malformed_row).unwrap_err();

        assert_eq!(error.code(), ErrorCode::InternalError);
    }
}

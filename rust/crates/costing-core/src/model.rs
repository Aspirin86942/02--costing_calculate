use std::collections::BTreeMap;

use crate::error::CostingError;
use crate::error::ErrorCode;
use crate::table::{ColumnId, ColumnSchema, IndexedRow, IndexedTable};
use rust_decimal::Decimal;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value")]
pub enum CellValue {
    Blank,
    Text(String),
    Decimal(Decimal),
    DateLike(String),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SheetModel {
    pub sheet_name: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<CellValue>>,
    pub column_types: BTreeMap<String, String>,
    pub number_formats: BTreeMap<String, String>,
    pub freeze_panes: Option<String>,
    pub auto_filter: bool,
    pub fixed_width: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RawWorkbook {
    pub sheet_name: String,
    pub header_rows: [Vec<String>; 2],
    pub rows: Vec<Vec<CellValue>>,
}

#[derive(Debug)]
pub struct NormalizedCostFrame {
    pub(crate) table: IndexedTable,
    key_columns: Vec<String>,
}

impl NormalizedCostFrame {
    pub(crate) fn new(table: IndexedTable, key_columns: Vec<String>) -> Self {
        Self { table, key_columns }
    }

    pub fn is_empty(&self) -> bool {
        self.table.rows().is_empty()
    }

    pub fn row_count(&self) -> usize {
        self.table.rows().len()
    }

    pub fn key_columns(&self) -> &[String] {
        &self.key_columns
    }

    pub(crate) fn into_table(self) -> IndexedTable {
        self.table
    }
}

#[derive(Debug)]
pub struct SplitResult {
    pub(crate) schema: ColumnSchema,
    pub(crate) detail_display_columns: Vec<ColumnId>,
    pub(crate) detail_rows: Vec<IndexedRow>,
    pub(crate) qty_display_columns: Vec<ColumnId>,
    pub(crate) qty_rows: Vec<IndexedRow>,
}

impl SplitResult {
    pub(crate) fn schema(&self) -> &ColumnSchema {
        &self.schema
    }

    pub(crate) fn detail_rows(&self) -> &[IndexedRow] {
        &self.detail_rows
    }

    pub(crate) fn qty_rows(&self) -> &[IndexedRow] {
        &self.qty_rows
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        ColumnSchema,
        Vec<ColumnId>,
        Vec<IndexedRow>,
        Vec<ColumnId>,
        Vec<IndexedRow>,
    ) {
        (
            self.schema,
            self.detail_display_columns,
            self.detail_rows,
            self.qty_display_columns,
            self.qty_rows,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ErrorIssue {
    pub row_id: String,
    pub issue_type: String,
    pub field_name: String,
    pub original_value: String,
    pub reason: String,
    pub action: String,
    pub retryable: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexedFactRow {
    pub(crate) source: IndexedRow,
    pub(crate) derived_values: BTreeMap<String, CellValue>,
}

impl IndexedFactRow {
    pub(crate) fn new(source: IndexedRow) -> Self {
        Self {
            source,
            derived_values: BTreeMap::new(),
        }
    }

    pub(crate) fn get_named<'a>(
        &'a self,
        schema: &ColumnSchema,
        name: &str,
    ) -> Result<Option<&'a CellValue>, CostingError> {
        if let Some(value) = self.derived_values.get(name) {
            return Ok(Some(value));
        }
        schema
            .optional(name)
            .map(|id| self.source.get(id))
            .transpose()
    }

    pub(crate) fn insert_derived(
        &mut self,
        name: impl Into<String>,
        value: CellValue,
    ) -> Option<CellValue> {
        self.derived_values.insert(name.into(), value)
    }

    pub(crate) fn take_named(
        &mut self,
        schema: &ColumnSchema,
        name: &str,
    ) -> Result<Option<CellValue>, CostingError> {
        if let Some(value) = self.derived_values.remove(name) {
            return Ok(Some(value));
        }
        schema
            .optional(name)
            .map(|id| self.source.take(id))
            .transpose()
    }

    pub(crate) fn into_parts(self) -> (IndexedRow, BTreeMap<String, CellValue>) {
        (self.source, self.derived_values)
    }
}

#[derive(Debug)]
pub struct FactBundle {
    pub(crate) schema: ColumnSchema,
    pub(crate) detail_display_columns: Vec<ColumnId>,
    pub(crate) detail_rows: Vec<IndexedRow>,
    pub(crate) qty_display_columns: Vec<ColumnId>,
    pub(crate) qty_input_row_count: usize,
    pub(crate) filtered_invalid_qty_count: usize,
    pub(crate) filtered_missing_total_amount_count: usize,
    pub(crate) qty_rows: Vec<IndexedFactRow>,
    pub(crate) work_order_rows: Vec<IndexedFactRow>,
    pub(crate) duplicate_work_order_row_count: usize,
    pub(crate) error_issues: Vec<ErrorIssue>,
}

impl FactBundle {
    pub(crate) fn detail_row_count(&self) -> usize {
        self.detail_rows.len()
    }

    pub(crate) fn qty_row_count(&self) -> usize {
        self.qty_rows.len()
    }

    pub(crate) fn work_order_row_count(&self) -> usize {
        self.work_order_rows.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MonthRange {
    pub start: Option<String>,
    pub end: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReaderSnapshot {
    pub sheet_name: String,
    pub row_count: usize,
    pub column_count: usize,
    pub headers: Vec<String>,
    pub null_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct StageTimings {
    pub stages: BTreeMap<String, f64>,
}

impl StageTimings {
    pub fn insert(&mut self, stage: impl Into<String>, seconds: f64) {
        self.stages.insert(stage.into(), seconds);
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QualityMetric {
    pub category: String,
    pub metric: String,
    pub value: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WorkbookPayload {
    pub sheet_models: Vec<SheetModel>,
    pub quality_metrics: Vec<QualityMetric>,
    pub error_log_count: usize,
    pub error_log: Vec<ErrorIssue>,
    pub stage_timings: StageTimings,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RunSummary {
    pub status: String,
    pub pipeline: String,
    pub output_written: bool,
    pub workbook_path: Option<String>,
    pub sheet_count: usize,
    pub error_log_count: usize,
    pub issue_type_counts: BTreeMap<String, usize>,
    pub quality_metrics: Vec<QualityMetric>,
    pub run_counts: BTreeMap<String, usize>,
    pub stage_timings: StageTimings,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ErrorSummary {
    pub status: String,
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
}

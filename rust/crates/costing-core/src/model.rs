use std::collections::BTreeMap;

use crate::error::ErrorCode;
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

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TableRow {
    pub values: BTreeMap<String, CellValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct NormalizedCostFrame {
    pub columns: Vec<String>,
    pub rows: Vec<TableRow>,
    pub key_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SplitResult {
    pub detail_columns: Vec<String>,
    pub detail_rows: Vec<TableRow>,
    pub qty_columns: Vec<String>,
    pub qty_rows: Vec<TableRow>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ErrorIssue {
    pub row_id: String,
    pub issue_type: String,
    pub field_name: String,
    pub reason: String,
    pub action: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FactBundle {
    pub detail_columns: Vec<String>,
    pub detail_fact: Vec<TableRow>,
    pub qty_columns: Vec<String>,
    pub qty_fact: Vec<TableRow>,
    pub work_order_fact: Vec<TableRow>,
    pub error_issues: Vec<ErrorIssue>,
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
    pub stage_timings: StageTimings,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ErrorSummary {
    pub status: String,
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
}

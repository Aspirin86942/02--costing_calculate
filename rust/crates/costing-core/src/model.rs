use std::collections::BTreeMap;

use crate::error::{CostingError, ErrorCode, ErrorDetails};
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

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CostAmounts {
    pub(crate) direct_material: Decimal,
    pub(crate) direct_labor: Decimal,
    pub(crate) manufacturing_overhead: Decimal,
    pub(crate) moh_other: Decimal,
    pub(crate) moh_labor: Decimal,
    pub(crate) moh_consumables: Decimal,
    pub(crate) moh_depreciation: Decimal,
    pub(crate) moh_utilities: Decimal,
    // 独立成本项按 PipelineConfig 的固定顺序保存，避免每个工单重复持有 bucket 字符串。
    pub(crate) standalone: Vec<Decimal>,
}

impl CostAmounts {
    pub(crate) fn new(standalone_count: usize) -> Self {
        Self {
            direct_material: Decimal::ZERO,
            direct_labor: Decimal::ZERO,
            manufacturing_overhead: Decimal::ZERO,
            moh_other: Decimal::ZERO,
            moh_labor: Decimal::ZERO,
            moh_consumables: Decimal::ZERO,
            moh_depreciation: Decimal::ZERO,
            moh_utilities: Decimal::ZERO,
            standalone: vec![Decimal::ZERO; standalone_count],
        }
    }

    pub(crate) fn standalone_amount(&self, index: usize) -> Decimal {
        self.standalone.get(index).copied().unwrap_or(Decimal::ZERO)
    }

    pub(crate) fn moh_component_sum(&self) -> Decimal {
        self.moh_other
            + self.moh_labor
            + self.moh_consumables
            + self.moh_depreciation
            + self.moh_utilities
    }
}

#[derive(Debug)]
pub(crate) struct QtyFactRow {
    pub(crate) source: IndexedRow,
    pub(crate) work_order_key: String,
    pub(crate) completed_qty: Decimal,
    pub(crate) completed_total: Decimal,
    pub(crate) amounts: CostAmounts,
    pub(crate) moh_matches: bool,
    pub(crate) total_matches: bool,
    pub(crate) check_reason: String,
}

#[derive(Debug)]
pub struct FactBundle {
    pub(crate) schema: ColumnSchema,
    pub(crate) detail_display_columns: Vec<ColumnId>,
    pub(crate) detail_rows: Vec<IndexedRow>,
    pub(crate) qty_display_columns: Vec<ColumnId>,
    pub(crate) qty_rows: Vec<QtyFactRow>,
    pub(crate) unique_work_order_indices: Vec<usize>,
    pub(crate) qty_input_row_count: usize,
    pub(crate) filtered_invalid_qty_count: usize,
    pub(crate) filtered_missing_total_amount_count: usize,
    pub(crate) duplicate_work_order_row_count: usize,
    pub(crate) error_issues: Vec<ErrorIssue>,
}

impl FactBundle {
    pub(crate) fn work_order_rows(&self) -> impl Iterator<Item = &QtyFactRow> {
        self.unique_work_order_indices
            .iter()
            .map(|index| &self.qty_rows[*index])
    }

    pub(crate) fn detail_row_count(&self) -> usize {
        self.detail_rows.len()
    }

    pub(crate) fn qty_row_count(&self) -> usize {
        self.qty_rows.len()
    }

    pub(crate) fn work_order_row_count(&self) -> usize {
        self.unique_work_order_indices.len()
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
    pub request_id: Option<String>,
    pub details: Option<ErrorDetails>,
}

impl ErrorSummary {
    pub fn from_error(error: &CostingError) -> Self {
        let (request_id, details) = error
            .context()
            .map(|context| {
                (
                    Some(context.request_id.clone()),
                    Some(context.details.clone()),
                )
            })
            .unwrap_or((None, None));
        Self {
            status: "failed".to_string(),
            code: error.code(),
            message: error.message().to_string(),
            retryable: error.retryable(),
            request_id,
            details,
        }
    }
}

#[cfg(test)]
mod error_summary_tests {
    use std::path::PathBuf;

    use crate::error::{CostingError, ErrorCode, ErrorContext, ErrorStage, IoKindCode};

    use super::ErrorSummary;

    #[test]
    fn error_summary_serializes_context_and_flattened_io_metadata() {
        let contextual = CostingError::io_with_source(
            ErrorCode::OutputNotWritable,
            "write failed",
            std::io::Error::from_raw_os_error(112),
        )
        .with_context(ErrorContext::new(
            "costing-test-1",
            ErrorStage::SaveWorkbook,
            Some(PathBuf::from("output.xlsx")),
        ));

        let summary = ErrorSummary::from_error(&contextual);

        assert_eq!(summary.request_id.as_deref(), Some("costing-test-1"));
        let details = summary.details.as_ref().expect("structured details");
        assert_eq!(details.stage, ErrorStage::SaveWorkbook);
        assert_eq!(details.path, Some(PathBuf::from("output.xlsx")));
        assert_eq!(
            details.io_meta.as_ref().expect("I/O metadata").kind,
            IoKindCode::StorageFull
        );
        assert_eq!(
            details.io_meta.as_ref().expect("I/O metadata").raw_os_error,
            Some(112)
        );

        let json = serde_json::to_value(summary).expect("serialize error summary");
        assert_eq!(json["request_id"], "costing-test-1");
        assert_eq!(json["details"]["stage"], "SaveWorkbook");
        assert_eq!(json["details"]["path"], "output.xlsx");
        assert_eq!(json["details"]["io_kind"], "StorageFull");
        assert_eq!(json["details"]["raw_os_error"], 112);
        assert_eq!(json["details"]["final_output_valid"], false);
        assert!(json["details"]["partial_output_removed"].is_null());
        assert_eq!(json["details"]["cleanup_failures"], serde_json::json!([]));
        assert!(json["details"].get("io_meta").is_none());
    }

    #[test]
    fn error_stage_has_exact_approved_serialization_set() {
        let stages = [
            ErrorStage::ValidateCliRequest,
            ErrorStage::ResolveCliPaths,
            ErrorStage::IngestWorkbook,
            ErrorStage::Normalize,
            ErrorStage::Split,
            ErrorStage::BuildFact,
            ErrorStage::BuildPresentation,
            ErrorStage::PrepareOutputDirectory,
            ErrorStage::CheckDiskSpace,
            ErrorStage::CreateTempWorkspace,
            ErrorStage::PlanSheet,
            ErrorStage::InitializeLowMemoryTempWriter,
            ErrorStage::PopulateWorkbook,
            ErrorStage::CreateFinalOutput,
            ErrorStage::SaveWorkbook,
            ErrorStage::RemovePartialOutput,
            ErrorStage::CleanupTempWorkspace,
            ErrorStage::ReadOutputMetadata,
        ];
        let serialized = stages
            .iter()
            .map(|stage| serde_json::to_value(stage).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(stages.len(), 18);
        assert_eq!(
            serde_json::Value::Array(serialized),
            serde_json::json!([
                "ValidateCliRequest",
                "ResolveCliPaths",
                "IngestWorkbook",
                "Normalize",
                "Split",
                "BuildFact",
                "BuildPresentation",
                "PrepareOutputDirectory",
                "CheckDiskSpace",
                "CreateTempWorkspace",
                "PlanSheet",
                "InitializeLowMemoryTempWriter",
                "PopulateWorkbook",
                "CreateFinalOutput",
                "SaveWorkbook",
                "RemovePartialOutput",
                "CleanupTempWorkspace",
                "ReadOutputMetadata"
            ])
        );
    }
}

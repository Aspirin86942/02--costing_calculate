use std::io::Write;
use std::path::Path;
use std::time::Instant;

use costing_core::error::{CleanupFailureMeta, ErrorContext, ErrorStage, IoFailureMeta};
use costing_core::model::{CellValue, SheetModel, WorkbookPayload};
use rust_decimal::prelude::ToPrimitive;
use rust_xlsxwriter::{Color, Format, FormatAlign, FormatBorder, Workbook, Worksheet};

use crate::atomic_file::{AtomicFile, AtomicFileError, AtomicFileStage};
use crate::reader::CostingXlsxError;

const DEFAULT_SHEET_NAMES: [&str; 3] = [
    "成本计算单总表",
    "成本计算单数量聚合维度",
    "成本分析工单维度",
];

enum ColumnBehavior {
    Text,
    Numeric(Format),
}

#[cfg(feature = "low-memory")]
const LOW_MEMORY_CELL_SLOT_THRESHOLD: usize = 5_000_000;

#[cfg(feature = "low-memory")]
struct TempWorkspace {
    directory: tempfile::TempDir,
}

#[cfg(feature = "low-memory")]
impl TempWorkspace {
    fn create(parent: &Path, request_id: &str) -> std::io::Result<Self> {
        let sanitized = request_id
            .chars()
            .take(48)
            .map(|character| {
                if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                    character
                } else {
                    '_'
                }
            })
            .collect::<String>();
        let request_part = if sanitized.is_empty() {
            "request"
        } else {
            &sanitized
        };
        let directory = tempfile::Builder::new()
            .prefix(&format!(".costing-tmp-{request_part}-"))
            .tempdir_in(parent)?;
        Ok(Self { directory })
    }

    fn path(&self) -> &Path {
        self.directory.path()
    }

    fn close(self) -> std::io::Result<()> {
        self.directory.close()
    }
}

pub struct WriterContext {
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkbookWriteReport {
    pub writer_populate_seconds: f64,
    pub xlsx_save_seconds: f64,
    pub output_size_bytes: u64,
    pub low_memory_writer: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum WriterPrimaryError {
    #[error("{0}")]
    Io(#[source] std::io::Error),
    #[error("{0}")]
    Xlsx(#[source] CostingXlsxError),
    #[error("{0}")]
    Contract(String),
}

#[derive(Debug, thiserror::Error)]
#[error("{primary}")]
pub struct WriterError {
    pub context: ErrorContext,
    #[source]
    pub primary: WriterPrimaryError,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum WriterTestFault {
    #[default]
    None,
    FailAfterStagingWrite,
    CompeteBeforePublish,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct WriterTestControl {
    force_low_memory: bool,
    fault: WriterTestFault,
}

// WriterError 保留完整阶段、路径与清理上下文；为压缩 Err 大小而装箱会改变现有公共错误链。
#[allow(clippy::result_large_err)]
pub fn write_workbook(
    context: &WriterContext,
    path: &Path,
    payload: &WorkbookPayload,
) -> Result<WorkbookWriteReport, WriterError> {
    write_workbook_controlled(context, path, payload, WriterTestControl::default())
}

#[cfg(test)]
#[allow(clippy::result_large_err)]
fn write_workbook_with_test_control(
    context: &WriterContext,
    path: &Path,
    payload: &WorkbookPayload,
    control: WriterTestControl,
) -> Result<WorkbookWriteReport, WriterError> {
    write_workbook_controlled(context, path, payload, control)
}

#[allow(clippy::result_large_err)]
fn write_workbook_controlled(
    context: &WriterContext,
    path: &Path,
    payload: &WorkbookPayload,
    control: WriterTestControl,
) -> Result<WorkbookWriteReport, WriterError> {
    validate_default_sheet_contract(payload).map_err(|error| {
        writer_error(
            context,
            path,
            ErrorStage::PlanSheet,
            primary_from_xlsx_error(error),
        )
    })?;

    let mut sheet_modes = payload
        .sheet_models
        .iter()
        .map(|sheet| use_low_memory_for_shape(sheet.rows.len(), sheet.columns.len()))
        .collect::<Vec<_>>();
    #[cfg(feature = "low-memory")]
    if control.force_low_memory {
        sheet_modes.fill(true);
    }
    #[cfg(not(feature = "low-memory"))]
    debug_assert!(!control.force_low_memory);
    let needs_low_memory = sheet_modes.iter().any(|enabled| *enabled);

    #[cfg(feature = "low-memory")]
    let temp_workspace = if needs_low_memory {
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent).map_err(|error| {
            writer_error(
                context,
                path,
                ErrorStage::PrepareOutputDirectory,
                WriterPrimaryError::Io(error),
            )
        })?;
        Some(
            TempWorkspace::create(parent, &context.request_id).map_err(|error| {
                writer_error(
                    context,
                    parent,
                    ErrorStage::CreateTempWorkspace,
                    WriterPrimaryError::Io(error),
                )
            })?,
        )
    } else {
        None
    };

    #[cfg(not(feature = "low-memory"))]
    debug_assert!(!needs_low_memory);

    let mut workbook = Workbook::new();

    #[cfg(feature = "low-memory")]
    if let Some(workspace) = temp_workspace.as_ref() {
        if let Err(error) = workbook.set_compression_level(5) {
            drop(workbook);
            return finish_with_temp_cleanup(
                context,
                Err(writer_error(
                    context,
                    workspace.path(),
                    ErrorStage::InitializeLowMemoryTempWriter,
                    WriterPrimaryError::Xlsx(CostingXlsxError::Writer(error)),
                )),
                temp_workspace,
            );
        }
        if let Err(error) = workbook.set_tempdir(workspace.path()) {
            drop(workbook);
            return finish_with_temp_cleanup(
                context,
                Err(writer_error(
                    context,
                    workspace.path(),
                    ErrorStage::InitializeLowMemoryTempWriter,
                    WriterPrimaryError::Xlsx(CostingXlsxError::Writer(error)),
                )),
                temp_workspace,
            );
        }
    }

    let primary_result = (|| -> Result<WorkbookWriteReport, WriterError> {
        let writer_populate_started = Instant::now();
        for (sheet, use_low_memory) in payload.sheet_models.iter().zip(&sheet_modes) {
            let worksheet = add_worksheet_for_mode(&mut workbook, *use_low_memory);
            worksheet
                .set_name(&sheet.sheet_name)
                .map_err(CostingXlsxError::Writer)
                .map_err(|error| {
                    writer_error(
                        context,
                        path,
                        ErrorStage::PopulateWorkbook,
                        WriterPrimaryError::Xlsx(error),
                    )
                })?;

            let header_format = Format::new()
                .set_bold()
                .set_background_color(Color::RGB(0xD9E1F2))
                .set_border(FormatBorder::Thin)
                .set_align(FormatAlign::Center)
                .set_align(FormatAlign::VerticalCenter);
            let text_format = Format::new()
                .set_align(FormatAlign::Left)
                .set_align(FormatAlign::VerticalCenter);
            let column_behaviors = sheet
                .columns
                .iter()
                .map(|column| {
                    sheet
                        .number_formats
                        .get(column)
                        .map_or(ColumnBehavior::Text, |number_format| {
                            ColumnBehavior::Numeric(numeric_format(number_format))
                        })
                })
                .collect::<Vec<_>>();

            if *use_low_memory {
                configure_sheet_metadata(worksheet, sheet).map_err(|error| {
                    writer_error(
                        context,
                        path,
                        ErrorStage::PopulateWorkbook,
                        primary_from_xlsx_error(error),
                    )
                })?;
            }
            write_header_row(
                worksheet,
                &sheet.columns,
                &column_behaviors,
                sheet.fixed_width,
                &header_format,
                &text_format,
            )
            .map_err(|error| {
                writer_error(
                    context,
                    path,
                    ErrorStage::PopulateWorkbook,
                    primary_from_xlsx_error(error),
                )
            })?;
            write_data_rows(worksheet, &sheet.rows, &column_behaviors, &text_format).map_err(
                |error| {
                    writer_error(
                        context,
                        path,
                        ErrorStage::PopulateWorkbook,
                        primary_from_xlsx_error(error),
                    )
                },
            )?;
            if !*use_low_memory {
                configure_sheet_metadata(worksheet, sheet).map_err(|error| {
                    writer_error(
                        context,
                        path,
                        ErrorStage::PopulateWorkbook,
                        primary_from_xlsx_error(error),
                    )
                })?;
            }
        }
        let writer_populate_seconds = writer_populate_started.elapsed().as_secs_f64();

        let mut staged = AtomicFile::create(path, &context.request_id)
            .map_err(|error| atomic_file_error(context, path, error))?;
        if control.fault == WriterTestFault::FailAfterStagingWrite {
            if let Err(source) = staged.writer().write_all(b"partial workbook") {
                return Err(finish_staging_failure(
                    writer_error(
                        context,
                        path,
                        ErrorStage::SaveWorkbook,
                        WriterPrimaryError::Io(source),
                    ),
                    staged,
                ));
            }
            return Err(finish_staging_failure(
                writer_error(
                    context,
                    path,
                    ErrorStage::SaveWorkbook,
                    WriterPrimaryError::Io(std::io::Error::new(
                        std::io::ErrorKind::StorageFull,
                        "injected staging write failure",
                    )),
                ),
                staged,
            ));
        }

        let xlsx_save_started = Instant::now();
        let xlsx_save_seconds = match workbook.save_to_writer(staged.writer()) {
            Ok(()) => xlsx_save_started.elapsed().as_secs_f64(),
            Err(error) => {
                return Err(finish_staging_failure(
                    writer_error(
                        context,
                        path,
                        ErrorStage::SaveWorkbook,
                        WriterPrimaryError::Xlsx(CostingXlsxError::Writer(error)),
                    ),
                    staged,
                ))
            }
        };

        if control.fault == WriterTestFault::CompeteBeforePublish {
            if let Err(source) = std::fs::write(path, b"competing workbook") {
                return Err(finish_staging_failure(
                    writer_error(
                        context,
                        path,
                        ErrorStage::PublishWorkbook,
                        WriterPrimaryError::Io(source),
                    ),
                    staged,
                ));
            }
        }

        let published = staged
            .publish()
            .map_err(|error| atomic_file_error(context, path, error))?;
        let metadata = published.metadata().map_err(|source| {
            let mut error = writer_error(
                context,
                path,
                ErrorStage::ReadOutputMetadata,
                WriterPrimaryError::Io(source),
            );
            error.context.details.final_output_valid = true;
            error
        })?;
        if metadata.len() == 0 {
            let mut error = writer_error(
                context,
                path,
                ErrorStage::ReadOutputMetadata,
                WriterPrimaryError::Contract("written workbook is empty".to_string()),
            );
            error.context.details.final_output_valid = true;
            return Err(error);
        }
        Ok(WorkbookWriteReport {
            writer_populate_seconds,
            xlsx_save_seconds,
            output_size_bytes: metadata.len(),
            low_memory_writer: needs_low_memory,
        })
    })();

    drop(workbook);

    #[cfg(feature = "low-memory")]
    return finish_with_temp_cleanup(context, primary_result, temp_workspace);

    #[cfg(not(feature = "low-memory"))]
    primary_result
}

fn use_low_memory_for_shape(row_count: usize, column_count: usize) -> bool {
    #[cfg(feature = "low-memory")]
    {
        row_count > 0
            && column_count > 0
            && row_count.saturating_mul(column_count) >= LOW_MEMORY_CELL_SLOT_THRESHOLD
    }

    #[cfg(not(feature = "low-memory"))]
    {
        let _ = (row_count, column_count);
        false
    }
}

fn add_worksheet_for_mode(workbook: &mut Workbook, use_low_memory: bool) -> &mut Worksheet {
    #[cfg(feature = "low-memory")]
    if use_low_memory {
        return workbook.add_worksheet_with_low_memory();
    }

    let _ = use_low_memory;
    workbook.add_worksheet()
}

fn configure_sheet_metadata(
    worksheet: &mut Worksheet,
    sheet: &SheetModel,
) -> Result<(), CostingXlsxError> {
    if sheet.auto_filter && !sheet.columns.is_empty() {
        let last_row = sheet.rows.len() as u32;
        let last_col = (sheet.columns.len() - 1) as u16;
        worksheet
            .autofilter(0, 0, last_row, last_col)
            .map_err(CostingXlsxError::Writer)?;
    }
    if let Some(freeze_panes) = &sheet.freeze_panes {
        let (row, col) = parse_freeze_panes(freeze_panes)?;
        worksheet
            .set_freeze_panes(row, col)
            .map_err(CostingXlsxError::Writer)?;
    }
    Ok(())
}

#[cfg(feature = "low-memory")]
#[allow(clippy::result_large_err)]
fn finish_with_temp_cleanup(
    context: &WriterContext,
    primary_result: Result<WorkbookWriteReport, WriterError>,
    workspace: Option<TempWorkspace>,
) -> Result<WorkbookWriteReport, WriterError> {
    let Some(workspace) = workspace else {
        return primary_result;
    };
    let workspace_path = workspace.path().to_path_buf();
    match (primary_result, workspace.close()) {
        (Ok(report), Ok(())) => Ok(report),
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(cleanup_error)) => Err(merge_cleanup_failure(
            error,
            ErrorStage::CleanupTempWorkspace,
            workspace_path,
            cleanup_error,
        )),
        (Ok(_), Err(cleanup_error)) => {
            let mut error = writer_error(
                context,
                &workspace_path,
                ErrorStage::CleanupTempWorkspace,
                WriterPrimaryError::Io(cleanup_error),
            );
            error.context.details.final_output_valid = true;
            Err(error)
        }
    }
}

fn writer_error(
    context: &WriterContext,
    path: &Path,
    stage: ErrorStage,
    primary: WriterPrimaryError,
) -> WriterError {
    WriterError {
        context: ErrorContext::new(context.request_id.clone(), stage, Some(path.to_path_buf())),
        primary,
    }
}

fn primary_from_xlsx_error(error: CostingXlsxError) -> WriterPrimaryError {
    match error {
        CostingXlsxError::Message(message) => WriterPrimaryError::Contract(message),
        error => WriterPrimaryError::Xlsx(error),
    }
}

fn finish_staging_failure(mut error: WriterError, staged: AtomicFile) -> WriterError {
    let staging_path = staged.staging_path().to_path_buf();
    match staged.discard() {
        Ok(()) => {
            error.context.details.partial_output_removed = Some(true);
            error
        }
        Err(cleanup) => {
            error.context.details.partial_output_removed = Some(false);
            merge_cleanup_failure(
                error,
                ErrorStage::CleanupWorkbookTempFile,
                staging_path,
                cleanup.source,
            )
        }
    }
}

fn atomic_file_error(context: &WriterContext, path: &Path, error: AtomicFileError) -> WriterError {
    let AtomicFileError {
        stage: atomic_stage,
        staging_path,
        final_published,
        cleanup_error,
        source,
        ..
    } = error;
    let stage = match atomic_stage {
        AtomicFileStage::CheckTarget => ErrorStage::CreateFinalOutput,
        AtomicFileStage::PrepareParent => ErrorStage::PrepareOutputDirectory,
        AtomicFileStage::CreateStaging => ErrorStage::CreateWorkbookTempFile,
        AtomicFileStage::Flush | AtomicFileStage::Sync => ErrorStage::SyncWorkbookTempFile,
        AtomicFileStage::Publish => ErrorStage::PublishWorkbook,
        AtomicFileStage::Cleanup => ErrorStage::CleanupWorkbookTempFile,
    };
    let mut mapped = writer_error(context, path, stage, WriterPrimaryError::Io(source));
    mapped.context.details.final_output_valid = final_published;
    if let (Some(cleanup_path), Some(cleanup_error)) = (staging_path.clone(), cleanup_error) {
        mapped.context.details.partial_output_removed = Some(false);
        mapped = merge_cleanup_failure(
            mapped,
            ErrorStage::CleanupWorkbookTempFile,
            cleanup_path,
            cleanup_error,
        );
    } else if staging_path.is_some() {
        mapped.context.details.partial_output_removed = Some(true);
    }
    mapped
}

fn merge_cleanup_failure(
    mut error: WriterError,
    stage: ErrorStage,
    path: std::path::PathBuf,
    cleanup_error: std::io::Error,
) -> WriterError {
    error
        .context
        .details
        .cleanup_failures
        .push(CleanupFailureMeta {
            stage,
            path: Some(path),
            io_meta: IoFailureMeta::from(&cleanup_error),
            message: cleanup_error.to_string(),
        });
    error
}

fn validate_default_sheet_contract(payload: &WorkbookPayload) -> Result<(), CostingXlsxError> {
    let actual = payload
        .sheet_models
        .iter()
        .map(|sheet| sheet.sheet_name.as_str())
        .collect::<Vec<_>>();
    if actual.as_slice() == DEFAULT_SHEET_NAMES.as_slice() {
        return Ok(());
    }

    Err(CostingXlsxError::Message(format!(
        "Rust 默认 workbook 只允许按顺序输出: {}; 实际: {}",
        DEFAULT_SHEET_NAMES.join(", "),
        actual.join(", ")
    )))
}

fn write_header_row(
    worksheet: &mut Worksheet,
    columns: &[String],
    column_behaviors: &[ColumnBehavior],
    fixed_width: Option<f64>,
    header_format: &Format,
    text_format: &Format,
) -> Result<(), CostingXlsxError> {
    for (col_idx, column) in columns.iter().enumerate() {
        let col_idx = col_idx as u16;
        worksheet
            .write_string_with_format(0, col_idx, column, header_format)
            .map_err(CostingXlsxError::Writer)?;
        if let Some(width) = fixed_width {
            worksheet
                .set_column_width(col_idx, normalized_column_width(width))
                .map_err(CostingXlsxError::Writer)?;
        }
        let column_format = match &column_behaviors[col_idx as usize] {
            ColumnBehavior::Text => text_format,
            ColumnBehavior::Numeric(format) => format,
        };
        worksheet
            .set_column_format(col_idx, column_format)
            .map_err(CostingXlsxError::Writer)?;
    }
    Ok(())
}

fn write_data_rows(
    worksheet: &mut Worksheet,
    rows: &[Vec<CellValue>],
    column_behaviors: &[ColumnBehavior],
    text_format: &Format,
) -> Result<(), CostingXlsxError> {
    for (row_idx, row) in rows.iter().enumerate() {
        let excel_row = (row_idx + 1) as u32;
        for (col_idx, (value, behavior)) in row.iter().zip(column_behaviors).enumerate() {
            if matches!(value, CellValue::Blank) {
                continue;
            }
            let excel_col = col_idx as u16;
            match value {
                CellValue::Blank => {}
                CellValue::Decimal(value) => {
                    worksheet
                        .write_number(excel_row, excel_col, decimal_to_f64(value)?)
                        .map_err(CostingXlsxError::Writer)?;
                }
                CellValue::Text(value) | CellValue::DateLike(value) => {
                    match behavior {
                        ColumnBehavior::Text => worksheet
                            .write_string(excel_row, excel_col, value)
                            .map_err(CostingXlsxError::Writer)?,
                        ColumnBehavior::Numeric(_) => worksheet
                            .write_string_with_format(excel_row, excel_col, value, text_format)
                            .map_err(CostingXlsxError::Writer)?,
                    };
                }
            }
        }
    }
    Ok(())
}

fn numeric_format(number_format: &str) -> Format {
    Format::new()
        .set_num_format(number_format)
        .set_align(FormatAlign::Right)
        .set_align(FormatAlign::VerticalCenter)
}

fn normalized_column_width(width: f64) -> f64 {
    // 与 Python xlsxwriter 的固定 15 列宽换算一致，使 openpyxl/OOXML 语义值保持 15.0。
    if width == 15.0 {
        14.3
    } else {
        width
    }
}

fn decimal_to_f64(value: &rust_decimal::Decimal) -> Result<f64, CostingXlsxError> {
    value.to_f64().ok_or_else(|| {
        CostingXlsxError::Message(format!("decimal value cannot be written to xlsx: {value}"))
    })
}

fn parse_freeze_panes(token: &str) -> Result<(u32, u16), CostingXlsxError> {
    match token.trim().to_ascii_uppercase().as_str() {
        "A2" => Ok((1, 0)),
        other => Err(CostingXlsxError::Message(format!(
            "unsupported freeze panes token: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::error::Error;
    use std::io::ErrorKind;
    use std::process;
    use std::sync::{Arc, Barrier};
    use std::time::{SystemTime, UNIX_EPOCH};

    use calamine::{open_workbook_auto, Reader};
    use costing_core::error::{ErrorStage, IoKindCode};
    use costing_core::model::{
        CellValue, QualityMetric, SheetModel, StageTimings, WorkbookPayload,
    };
    use rust_decimal::Decimal;

    use super::*;

    fn writer_context() -> WriterContext {
        WriterContext {
            request_id: "writer-test-request".to_string(),
        }
    }

    fn writer_io_error(raw_os_error: i32) -> WriterError {
        WriterError {
            context: ErrorContext::new(
                "writer-test-request",
                ErrorStage::SaveWorkbook,
                Some(std::path::PathBuf::from("output.xlsx")),
            ),
            primary: WriterPrimaryError::Io(std::io::Error::from_raw_os_error(raw_os_error)),
        }
    }

    fn unique_temp_path(stem: &str) -> std::path::PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "costing-writer-{stem}-pid{}-{timestamp}.xlsx",
            process::id()
        ))
    }

    fn unique_temp_dir(stem: &str) -> std::path::PathBuf {
        let path = unique_temp_path(stem).with_extension("");
        std::fs::create_dir(&path).unwrap();
        path
    }

    fn assert_no_temporary_artifacts(parent: &Path) {
        let artifacts = std::fs::read_dir(parent)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| {
                name.starts_with(".costing-publish-") || name.starts_with(".costing-tmp-")
            })
            .collect::<Vec<_>>();
        assert_eq!(artifacts, Vec::<String>::new());
    }

    fn sheet(sheet_name: &str) -> SheetModel {
        SheetModel {
            sheet_name: sheet_name.to_string(),
            columns: vec!["月份".to_string(), "本期完工金额".to_string()],
            rows: vec![vec![
                CellValue::Text("2025年01期".to_string()),
                CellValue::Decimal(Decimal::new(125, 1)),
            ]],
            column_types: BTreeMap::from([
                ("月份".to_string(), "text".to_string()),
                ("本期完工金额".to_string(), "text".to_string()),
            ]),
            number_formats: BTreeMap::from([("本期完工金额".to_string(), "#,##0.00".to_string())]),
            freeze_panes: Some("A2".to_string()),
            auto_filter: true,
            fixed_width: Some(15.0),
        }
    }

    fn payload(sheet_models: Vec<SheetModel>) -> WorkbookPayload {
        WorkbookPayload {
            sheet_models,
            quality_metrics: vec![QualityMetric {
                category: "行数勾稽".to_string(),
                metric: "产品数量统计输出行数".to_string(),
                value: "1".to_string(),
                description: "test".to_string(),
            }],
            error_log_count: 0,
            error_log: Vec::new(),
            stage_timings: StageTimings::default(),
        }
    }

    #[cfg(feature = "low-memory")]
    #[test]
    fn low_memory_threshold_uses_saturating_cell_slots() {
        assert!(!use_low_memory_for_shape(0, LOW_MEMORY_CELL_SLOT_THRESHOLD));
        assert!(!use_low_memory_for_shape(
            1,
            LOW_MEMORY_CELL_SLOT_THRESHOLD - 1
        ));
        assert!(use_low_memory_for_shape(1, LOW_MEMORY_CELL_SLOT_THRESHOLD));
        assert!(use_low_memory_for_shape(usize::MAX, 2));
    }

    #[cfg(feature = "low-memory")]
    #[test]
    fn temp_workspace_is_created_and_removed_below_output_parent() {
        let parent = unique_temp_path("workspace-parent");
        std::fs::create_dir(&parent).unwrap();

        let workspace = TempWorkspace::create(&parent, "request/with:path").unwrap();
        let workspace_path = workspace.path().to_path_buf();

        assert_eq!(workspace_path.parent(), Some(parent.as_path()));
        assert!(workspace_path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with(".costing-tmp-request_with_path-"));
        workspace.close().unwrap();
        assert!(!workspace_path.exists());
        std::fs::remove_dir(parent).unwrap();
    }

    #[test]
    fn write_workbook_reports_populate_save_and_output_size() {
        let output = unique_temp_path("three-sheet");
        let payload = payload(vec![
            sheet("成本计算单总表"),
            sheet("成本计算单数量聚合维度"),
            sheet("成本分析工单维度"),
        ]);

        let report = write_workbook(&writer_context(), &output, &payload).unwrap();

        assert!(report.writer_populate_seconds.is_finite());
        assert!(report.writer_populate_seconds >= 0.0);
        assert!(report.xlsx_save_seconds.is_finite());
        assert!(report.xlsx_save_seconds >= 0.0);
        let output_size_bytes = std::fs::metadata(&output).unwrap().len();
        assert!(output_size_bytes > 0);
        assert_eq!(report.output_size_bytes, output_size_bytes);

        let workbook = open_workbook_auto(&output).unwrap();
        assert_eq!(
            workbook.sheet_names(),
            &[
                "成本计算单总表".to_string(),
                "成本计算单数量聚合维度".to_string(),
                "成本分析工单维度".to_string(),
            ]
        );
        let _ = std::fs::remove_file(output);
    }

    #[test]
    fn refuses_to_replace_existing_output() {
        let output = unique_temp_path("existing-output");
        let original = b"existing workbook bytes";
        std::fs::write(&output, original).unwrap();
        let payload = payload(vec![
            sheet("成本计算单总表"),
            sheet("成本计算单数量聚合维度"),
            sheet("成本分析工单维度"),
        ]);

        let error = write_workbook(&writer_context(), &output, &payload).unwrap_err();

        assert!(matches!(
            error.primary,
            WriterPrimaryError::Io(ref source) if source.kind() == ErrorKind::AlreadyExists
        ));
        assert_eq!(std::fs::read(&output).unwrap(), original);
        let _ = std::fs::remove_file(output);
    }

    #[test]
    fn concurrent_writers_allow_only_one_output() {
        let output = Arc::new(unique_temp_path("concurrent-output"));
        let barrier = Arc::new(Barrier::new(2));
        let handles = (0..2)
            .map(|_| {
                let output = Arc::clone(&output);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let payload = payload(vec![
                        sheet("成本计算单总表"),
                        sheet("成本计算单数量聚合维度"),
                        sheet("成本分析工单维度"),
                    ]);
                    barrier.wait();
                    match write_workbook(&writer_context(), &output, &payload) {
                        Ok(_) => "written",
                        Err(WriterError {
                            primary: WriterPrimaryError::Io(source),
                            ..
                        }) if source.kind() == ErrorKind::AlreadyExists => "exists",
                        Err(error) => panic!("unexpected writer error: {error}"),
                    }
                })
            })
            .collect::<Vec<_>>();

        let mut outcomes = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        outcomes.sort_unstable();

        assert_eq!(outcomes, ["exists", "written"]);
        let workbook = open_workbook_auto(output.as_ref()).unwrap();
        assert_eq!(workbook.sheet_names().len(), 3);
        let _ = std::fs::remove_file(output.as_ref());
    }

    #[cfg(feature = "low-memory")]
    #[test]
    fn staging_write_failure_never_exposes_final_output_in_either_writer_mode() {
        for force_low_memory in [false, true] {
            let parent = unique_temp_dir(if force_low_memory {
                "atomic-write-low"
            } else {
                "atomic-write-standard"
            });
            let output = parent.join("output.xlsx");
            let payload = payload(vec![
                sheet("成本计算单总表"),
                sheet("成本计算单数量聚合维度"),
                sheet("成本分析工单维度"),
            ]);
            let control = WriterTestControl {
                force_low_memory,
                fault: WriterTestFault::FailAfterStagingWrite,
            };

            let error =
                write_workbook_with_test_control(&writer_context(), &output, &payload, control)
                    .unwrap_err();

            assert_eq!(error.context.details.stage, ErrorStage::SaveWorkbook);
            assert!(!error.context.details.final_output_valid);
            assert!(!output.exists());
            assert_no_temporary_artifacts(&parent);
            std::fs::remove_dir(parent).unwrap();
        }
    }

    #[cfg(feature = "low-memory")]
    #[test]
    fn publish_race_preserves_competitor_in_either_writer_mode() {
        for force_low_memory in [false, true] {
            let parent = unique_temp_dir(if force_low_memory {
                "atomic-race-low"
            } else {
                "atomic-race-standard"
            });
            let output = parent.join("output.xlsx");
            let payload = payload(vec![
                sheet("成本计算单总表"),
                sheet("成本计算单数量聚合维度"),
                sheet("成本分析工单维度"),
            ]);
            let control = WriterTestControl {
                force_low_memory,
                fault: WriterTestFault::CompeteBeforePublish,
            };

            let error =
                write_workbook_with_test_control(&writer_context(), &output, &payload, control)
                    .unwrap_err();

            assert_eq!(error.context.details.stage, ErrorStage::PublishWorkbook);
            assert!(matches!(
                error.primary,
                WriterPrimaryError::Io(ref source) if source.kind() == ErrorKind::AlreadyExists
            ));
            assert!(!error.context.details.final_output_valid);
            assert_eq!(std::fs::read(&output).unwrap(), b"competing workbook");
            assert_no_temporary_artifacts(&parent);
            std::fs::remove_file(output).unwrap();
            std::fs::remove_dir(parent).unwrap();
        }
    }

    #[test]
    fn rejects_product_dimension_sheet() {
        let output = unique_temp_path("product-dimension");
        let payload = payload(vec![sheet("成本分析产品维度")]);

        let error = write_workbook(&writer_context(), &output, &payload).unwrap_err();

        assert!(error.to_string().contains("成本分析产品维度"));
        assert!(!output.exists());
    }

    #[test]
    fn rejects_extra_non_default_sheet() {
        let output = unique_temp_path("extra-sheet");
        let payload = payload(vec![
            sheet("成本计算单总表"),
            sheet("成本计算单数量聚合维度"),
            sheet("成本分析工单维度"),
            sheet("调试输出"),
        ]);

        let error = write_workbook(&writer_context(), &output, &payload).unwrap_err();

        assert!(error.to_string().contains("默认 workbook"));
        assert!(!output.exists());
    }

    #[test]
    fn cleanup_failure_does_not_replace_primary_error() {
        let output = unique_temp_path("cleanup-primary");
        let error = writer_io_error(112);

        let error = merge_cleanup_failure(
            error,
            ErrorStage::RemovePartialOutput,
            output.clone(),
            std::io::Error::new(ErrorKind::PermissionDenied, "cleanup denied"),
        );

        let WriterPrimaryError::Io(primary) = &error.primary else {
            panic!("expected original I/O primary error")
        };
        assert_eq!(primary.kind(), ErrorKind::StorageFull);
        assert_eq!(primary.raw_os_error(), Some(112));
        assert_eq!(
            error
                .source()
                .unwrap()
                .source()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .raw_os_error(),
            Some(112)
        );
        assert_eq!(error.context.details.cleanup_failures.len(), 1);
        let cleanup = &error.context.details.cleanup_failures[0];
        assert_eq!(cleanup.stage, ErrorStage::RemovePartialOutput);
        assert_eq!(cleanup.path.as_deref(), Some(output.as_path()));
        assert_eq!(cleanup.io_meta.kind, IoKindCode::PermissionDenied);
    }
}

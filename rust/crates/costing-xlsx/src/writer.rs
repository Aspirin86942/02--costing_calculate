use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use costing_core::error::{CleanupFailureMeta, ErrorContext, ErrorStage, IoFailureMeta};
use costing_core::model::{CellValue, WorkbookPayload};
use rust_decimal::prelude::ToPrimitive;
use rust_xlsxwriter::{Color, Format, FormatAlign, FormatBorder, Workbook, Worksheet};

use crate::reader::CostingXlsxError;

const DEFAULT_SHEET_NAMES: [&str; 3] = [
    "成本计算单总表",
    "成本计算单数量聚合维度",
    "成本分析工单维度",
];

pub struct WriterContext {
    pub request_id: String,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutputArtifactState {
    NotCreated,
    CreatedByCurrentRun,
    CompletedByCurrentRun,
}

pub fn write_workbook(
    context: &WriterContext,
    path: &Path,
    payload: &WorkbookPayload,
) -> Result<(), WriterError> {
    let artifact_state = OutputArtifactState::NotCreated;
    validate_default_sheet_contract(payload).map_err(|error| {
        writer_error(
            context,
            path,
            ErrorStage::PlanSheet,
            primary_from_xlsx_error(error),
        )
    })?;

    let mut workbook = Workbook::new();
    for sheet in &payload.sheet_models {
        let worksheet = workbook.add_worksheet();
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

        write_header_row(
            worksheet,
            &sheet.columns,
            &sheet.number_formats,
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
        write_data_rows(
            worksheet,
            &sheet.columns,
            &sheet.rows,
            &sheet.number_formats,
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

        if sheet.auto_filter && !sheet.columns.is_empty() {
            let last_row = sheet.rows.len() as u32;
            let last_col = (sheet.columns.len() - 1) as u16;
            worksheet
                .autofilter(0, 0, last_row, last_col)
                .map_err(CostingXlsxError::Writer)
                .map_err(|error| {
                    writer_error(
                        context,
                        path,
                        ErrorStage::PopulateWorkbook,
                        WriterPrimaryError::Xlsx(error),
                    )
                })?;
        }
        if let Some(freeze_panes) = &sheet.freeze_panes {
            let (row, col) = parse_freeze_panes(freeze_panes).map_err(|error| {
                writer_error(
                    context,
                    path,
                    ErrorStage::PopulateWorkbook,
                    primary_from_xlsx_error(error),
                )
            })?;
            worksheet
                .set_freeze_panes(row, col)
                .map_err(CostingXlsxError::Writer)
                .map_err(|error| {
                    writer_error(
                        context,
                        path,
                        ErrorStage::PopulateWorkbook,
                        WriterPrimaryError::Xlsx(error),
                    )
                })?;
        }
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            writer_error(
                context,
                path,
                ErrorStage::PrepareOutputDirectory,
                WriterPrimaryError::Io(error),
            )
        })?;
    }
    // 在真正写出时原子创建目标文件，避免前置 exists 检查与保存之间的并发覆盖竞态。
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| {
            finish_writer_failure(
                writer_error(
                    context,
                    path,
                    ErrorStage::CreateFinalOutput,
                    WriterPrimaryError::Io(error),
                ),
                artifact_state,
                path,
            )
        })?;
    let artifact_state = OutputArtifactState::CreatedByCurrentRun;

    if let Err(error) = workbook.save_to_writer(&mut file) {
        drop(file);
        return Err(finish_writer_failure(
            writer_error(
                context,
                path,
                ErrorStage::SaveWorkbook,
                WriterPrimaryError::Xlsx(CostingXlsxError::Writer(error)),
            ),
            artifact_state,
            path,
        ));
    }

    if let Err(error) = file.flush() {
        drop(file);
        return Err(finish_writer_failure(
            writer_error(
                context,
                path,
                ErrorStage::SaveWorkbook,
                WriterPrimaryError::Io(error),
            ),
            artifact_state,
            path,
        ));
    }
    drop(file);

    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) => {
            return Err(finish_writer_failure(
                writer_error(
                    context,
                    path,
                    ErrorStage::ReadOutputMetadata,
                    WriterPrimaryError::Io(error),
                ),
                artifact_state,
                path,
            ));
        }
    };
    if metadata.len() == 0 {
        return Err(finish_writer_failure(
            writer_error(
                context,
                path,
                ErrorStage::ReadOutputMetadata,
                WriterPrimaryError::Contract("written workbook is empty".to_string()),
            ),
            artifact_state,
            path,
        ));
    }
    let artifact_state = OutputArtifactState::CompletedByCurrentRun;
    debug_assert_eq!(artifact_state, OutputArtifactState::CompletedByCurrentRun);
    Ok(())
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

fn finish_writer_failure(
    mut error: WriterError,
    artifact_state: OutputArtifactState,
    path: &Path,
) -> WriterError {
    match artifact_state {
        OutputArtifactState::NotCreated => error,
        OutputArtifactState::CreatedByCurrentRun => match std::fs::remove_file(path) {
            Ok(()) => {
                error.context.details.partial_output_removed = Some(true);
                error
            }
            Err(cleanup_error) => {
                error.context.details.partial_output_removed = Some(false);
                merge_cleanup_failure(
                    error,
                    ErrorStage::RemovePartialOutput,
                    path.to_path_buf(),
                    cleanup_error,
                )
            }
        },
        OutputArtifactState::CompletedByCurrentRun => {
            error.context.details.final_output_valid = true;
            error
        }
    }
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
    number_formats: &std::collections::BTreeMap<String, String>,
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
        let column_format = number_formats
            .get(column)
            .map(|format| numeric_format(format))
            .unwrap_or_else(|| text_format.clone());
        worksheet
            .set_column_format(col_idx, &column_format)
            .map_err(CostingXlsxError::Writer)?;
    }
    Ok(())
}

fn write_data_rows(
    worksheet: &mut Worksheet,
    columns: &[String],
    rows: &[Vec<CellValue>],
    number_formats: &std::collections::BTreeMap<String, String>,
    text_format: &Format,
) -> Result<(), CostingXlsxError> {
    for (row_idx, row) in rows.iter().enumerate() {
        let excel_row = (row_idx + 1) as u32;
        for (col_idx, value) in row.iter().enumerate() {
            let Some(column_name) = columns.get(col_idx) else {
                continue;
            };
            let excel_col = col_idx as u16;
            let number_format = number_formats
                .get(column_name)
                .map(|format| numeric_format(format));
            match (value, number_format.as_ref()) {
                (CellValue::Blank, _) => {}
                (CellValue::Decimal(value), Some(format)) => {
                    worksheet
                        .write_number_with_format(
                            excel_row,
                            excel_col,
                            decimal_to_f64(value)?,
                            format,
                        )
                        .map_err(CostingXlsxError::Writer)?;
                }
                (CellValue::Decimal(value), None) => {
                    worksheet
                        .write_number(excel_row, excel_col, decimal_to_f64(value)?)
                        .map_err(CostingXlsxError::Writer)?;
                }
                (CellValue::Text(value) | CellValue::DateLike(value), _) => {
                    worksheet
                        .write_string_with_format(excel_row, excel_col, value, text_format)
                        .map_err(CostingXlsxError::Writer)?;
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

    #[test]
    fn writes_three_sheet_workbook() {
        let output = unique_temp_path("three-sheet");
        let payload = payload(vec![
            sheet("成本计算单总表"),
            sheet("成本计算单数量聚合维度"),
            sheet("成本分析工单维度"),
        ]);

        write_workbook(&writer_context(), &output, &payload).unwrap();

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
                        Ok(()) => "written",
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

    #[test]
    fn not_created_never_deletes_existing_path() {
        let output = unique_temp_path("not-created-existing");
        std::fs::write(&output, b"pre-existing").unwrap();

        let error = finish_writer_failure(
            writer_io_error(112),
            OutputArtifactState::NotCreated,
            &output,
        );

        assert_eq!(std::fs::read(&output).unwrap(), b"pre-existing");
        assert_eq!(error.context.details.partial_output_removed, None);
        let _ = std::fs::remove_file(output);
    }

    #[test]
    fn created_by_current_run_removes_partial_output() {
        let output = unique_temp_path("created-partial");
        std::fs::write(&output, b"partial").unwrap();

        let error = finish_writer_failure(
            writer_io_error(112),
            OutputArtifactState::CreatedByCurrentRun,
            &output,
        );

        assert!(!output.exists());
        assert_eq!(error.context.details.partial_output_removed, Some(true));
    }

    #[test]
    fn completed_output_is_not_deleted_by_secondary_cleanup_failure() {
        let output = unique_temp_path("completed-secondary-cleanup");
        std::fs::write(&output, b"complete workbook").unwrap();
        let error = merge_cleanup_failure(
            writer_io_error(112),
            ErrorStage::CleanupTempWorkspace,
            unique_temp_path("temp-workspace"),
            std::io::Error::new(ErrorKind::PermissionDenied, "temp cleanup denied"),
        );

        let error =
            finish_writer_failure(error, OutputArtifactState::CompletedByCurrentRun, &output);

        assert_eq!(std::fs::read(&output).unwrap(), b"complete workbook");
        assert!(error.context.details.final_output_valid);
        assert_eq!(error.context.details.partial_output_removed, None);
        assert_eq!(error.context.details.cleanup_failures.len(), 1);
        let _ = std::fs::remove_file(output);
    }
}

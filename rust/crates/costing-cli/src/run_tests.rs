use std::error::Error;
use std::path::PathBuf;
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

use costing_core::model::{ErrorSummary, MonthRange};
use costing_core::{ErrorCode, PipelineName, RunSummary};
use rust_xlsxwriter::{ExcelDateTime, Format, Workbook};

use super::*;
use crate::application::{RunManifestV1, RunOperation, RunRequest};

fn run(request: RunRequest) -> anyhow::Result<RunSummary> {
    let effective = crate::config::load_configuration(None, "costing-unit-test")
        .unwrap()
        .for_pipeline(request.pipeline)
        .unwrap();
    super::run(
        request,
        effective.rules,
        effective.input_pattern,
        "costing-unit-test".to_string(),
    )
}

fn args(input: &str) -> RunRequest {
    RunRequest {
        pipeline: PipelineName::Gb,
        input: Some(PathBuf::from(input)),
        output: Some(PathBuf::from("out.xlsx")),
        month_start: None,
        month_end: None,
        check_only: false,
        benchmark: false,
        summary_output: None,
        redact_paths: false,
        config: None,
        operation: RunOperation::Execute,
    }
}

#[test]
fn rejects_missing_input_file() {
    let error = validate_cli_request(&args("does-not-exist.xlsx")).unwrap_err();
    assert_eq!(error.code(), ErrorCode::FileNotFound);
    assert!(!error.retryable());
}

#[test]
fn rejects_non_xlsx_input() {
    let temp_dir = std::env::temp_dir();
    let path = unique_temp_path(&temp_dir, "not-xlsx", "txt");
    std::fs::write(&path, "not xlsx").unwrap();
    let error = validate_cli_request(&args(path.to_str().unwrap())).unwrap_err();
    assert_eq!(error.code(), ErrorCode::UnsupportedFileType);
    let _ = std::fs::remove_file(path);
}

#[test]
fn check_only_does_not_require_output_path() {
    let path = unique_temp_path(&std::env::temp_dir(), "check-only", "xlsx");
    std::fs::write(&path, "placeholder").unwrap();
    let request = RunRequest {
        pipeline: PipelineName::Gb,
        input: Some(path.clone()),
        output: None,
        month_start: None,
        month_end: None,
        check_only: true,
        benchmark: false,
        summary_output: None,
        redact_paths: false,
        config: None,

        operation: RunOperation::Execute,
    };
    assert!(validate_cli_request(&request).is_ok());
    let _ = std::fs::remove_file(path);
}

#[test]
fn resolves_default_input_and_output_paths() {
    let root = unique_temp_path(&std::env::temp_dir(), "auto-paths", "dir");
    let raw_dir = root.join("data/raw/gb");
    std::fs::create_dir_all(&raw_dir).unwrap();
    let input = raw_dir.join("gb-sample.xlsx");
    std::fs::write(&input, "placeholder").unwrap();
    let request = RunRequest {
        pipeline: PipelineName::Gb,
        input: None,
        output: None,
        month_start: None,
        month_end: None,
        check_only: false,
        benchmark: false,
        summary_output: None,
        redact_paths: false,
        config: None,

        operation: RunOperation::Execute,
    };

    let paths = resolve_cli_paths(&request, &root, None, "gb-*.xlsx").unwrap();

    assert_eq!(paths.input, input);
    assert_eq!(
        paths.output,
        Some(root.join("data/processed/gb/gb-sample_处理后.xlsx"))
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn default_output_path_includes_month_filter_suffix() {
    let root = unique_temp_path(&std::env::temp_dir(), "auto-month-output", "dir");
    let raw_dir = root.join("data/raw/sk");
    std::fs::create_dir_all(&raw_dir).unwrap();
    let input = raw_dir.join("sk-sample.xlsx");
    std::fs::write(&input, "placeholder").unwrap();
    let request = RunRequest {
        pipeline: PipelineName::Sk,
        input: None,
        output: None,
        month_start: Some("2026-01".to_string()),
        month_end: Some("2026-03".to_string()),
        check_only: false,
        benchmark: false,
        summary_output: None,
        redact_paths: false,
        config: None,

        operation: RunOperation::Execute,
    };
    let month_range = MonthRange {
        start: Some("2026-01".to_string()),
        end: Some("2026-03".to_string()),
    };

    let paths = resolve_cli_paths(&request, &root, Some(&month_range), "sk-*.xlsx").unwrap();

    assert_eq!(
        paths.output,
        Some(root.join("data/processed/sk/sk-sample_处理后_2026-01_2026-03.xlsx"))
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn month_output_suffix_supports_open_ranges() {
    let from_month = MonthRange {
        start: Some("2026-01".to_string()),
        end: None,
    };
    let to_month = MonthRange {
        start: None,
        end: Some("2026-03".to_string()),
    };

    assert_eq!(
        month_output_suffix(Some(&from_month)).as_deref(),
        Some("from_2026-01")
    );
    assert_eq!(
        month_output_suffix(Some(&to_month)).as_deref(),
        Some("to_2026-03")
    );
}

#[test]
fn requires_output_for_non_check_only_runs() {
    let path = unique_temp_path(&std::env::temp_dir(), "missing-output", "xlsx");
    std::fs::write(&path, "placeholder").unwrap();
    let request = RunRequest {
        pipeline: PipelineName::Gb,
        input: Some(path.clone()),
        output: None,
        month_start: None,
        month_end: None,
        check_only: false,
        benchmark: false,
        summary_output: None,
        redact_paths: false,
        config: None,

        operation: RunOperation::Execute,
    };
    let error = validate_cli_request(&request).unwrap_err();
    assert_eq!(error.code(), ErrorCode::InvalidInput);
    let _ = std::fs::remove_file(path);
}

#[test]
fn existing_output_fails_request_validation_without_changing_original() {
    let input = unique_temp_path(&std::env::temp_dir(), "existing-output-input", "xlsx");
    let output = unique_temp_path(&std::env::temp_dir(), "existing-output", "xlsx");
    std::fs::write(&input, "input").unwrap();
    std::fs::write(&output, "existing").unwrap();
    let request = RunRequest {
        output: Some(output.clone()),
        ..args(input.to_str().unwrap())
    };

    let error = validate_cli_request(&request).unwrap_err();

    assert_eq!(error.code(), ErrorCode::OutputExists);
    assert_eq!(error.path(), Some(output.as_path()));
    assert_eq!(std::fs::read_to_string(&output).unwrap(), "existing");
    let _ = std::fs::remove_file(input);
    let _ = std::fs::remove_file(output);
}

#[test]
fn maps_writer_output_race_to_output_exists_error_code() {
    let output = PathBuf::from("late-existing-output.xlsx");

    let error = map_xlsx_write_error(
        &output,
        WriterError {
            context: ErrorContext::new(
                "writer-race-request",
                ErrorStage::CreateFinalOutput,
                Some(output.clone()),
            ),
            low_memory_writer: false,
            primary: WriterPrimaryError::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "already exists",
            )),
        },
    );

    assert_eq!(error.code(), ErrorCode::OutputExists);
    assert!(!error.retryable());
    let io_error = error
        .source()
        .unwrap()
        .source()
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert_eq!(io_error.kind(), std::io::ErrorKind::AlreadyExists);
    let json = serde_json::to_value(ErrorSummary::from_error(&error)).unwrap();
    assert_eq!(json["details"]["io_kind"], "AlreadyExists");
}

#[test]
fn writer_io_error_reaches_cli_with_same_raw_os_error() {
    let output = PathBuf::from("storage-full-output.xlsx");
    // Windows ERROR_DISK_FULL and Unix ENOSPC use different native codes.
    let raw_os_error = if cfg!(windows) { 112 } else { 28 };
    let writer_error = WriterError {
        context: ErrorContext::new(
            "writer-owned-request",
            ErrorStage::SaveWorkbook,
            Some(output.clone()),
        ),
        low_memory_writer: false,
        primary: WriterPrimaryError::Xlsx(CostingXlsxError::Writer(XlsxError::IoError(
            std::io::Error::from_raw_os_error(raw_os_error),
        ))),
    };

    let error = map_xlsx_write_error(&output, writer_error);

    assert_eq!(error.code(), ErrorCode::OutputNotWritable);
    assert!(error.retryable());
    assert_eq!(error.context().unwrap().request_id, "writer-owned-request");
    let mut source = Some(&error as &(dyn Error + 'static));
    let mut original_io = None;
    while let Some(current) = source {
        if let Some(io_error) = current.downcast_ref::<std::io::Error>() {
            original_io = Some(io_error);
            break;
        }
        source = current.source();
    }
    let original_io = original_io.expect("original std::io::Error in source chain");
    assert_eq!(original_io.kind(), std::io::ErrorKind::StorageFull);
    assert_eq!(original_io.raw_os_error(), Some(raw_os_error));

    let json = serde_json::to_value(ErrorSummary::from_error(&error)).unwrap();
    assert_eq!(json["details"]["io_kind"], "StorageFull");
    assert_eq!(json["details"]["raw_os_error"], raw_os_error);
}

#[test]
fn post_publish_writer_failure_recovers_manifest_output_identity_and_mode() {
    let root = unique_temp_path(&std::env::temp_dir(), "post-publish-audit", "dir");
    std::fs::create_dir(&root).unwrap();
    let input = root.join("input.xlsx");
    let output = root.join("output.xlsx");
    std::fs::write(&input, b"input").unwrap();
    std::fs::write(&output, b"complete published workbook").unwrap();
    let request = RunRequest {
        input: Some(input),
        output: Some(output.clone()),
        summary_output: Some(root.join("summary.json")),
        ..args("unused.xlsx")
    };
    let mut audit = RunAudit::new(&request, true, root.clone());
    let mut context = ErrorContext::new(
        "post-publish-request",
        ErrorStage::ReadOutputMetadata,
        Some(output.clone()),
    );
    context.details.final_output_valid = true;
    let writer_error = WriterError {
        context,
        low_memory_writer: true,
        primary: WriterPrimaryError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "injected metadata failure",
        )),
    };

    let mapped = record_and_map_writer_error(&mut audit, &output, writer_error);
    let failure = ErrorSummary::from_error(&mapped);
    let manifest = audit.build_failure("post-publish-request", None, &failure, false);

    let RunManifestV1::Failed(manifest) = manifest else {
        panic!("expected failure manifest");
    };
    assert!(manifest.final_output_valid);
    assert!(manifest.execution.low_memory_writer);
    let final_output = manifest.final_output.expect("hashed valid output");
    assert_eq!(final_output.path, output.display().to_string());
    assert_eq!(
        final_output.sha256,
        sha256_bytes(b"complete published workbook")
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn rejects_input_and_output_that_resolve_to_same_file() {
    let input = unique_temp_path(&std::env::temp_dir(), "same-input-output", "xlsx");
    std::fs::write(&input, "input").unwrap();
    let request = RunRequest {
        output: Some(input.clone()),
        ..args(input.to_str().unwrap())
    };

    let error = validate_cli_request(&request).unwrap_err();

    assert_eq!(error.code(), ErrorCode::InvalidInput);
    let _ = std::fs::remove_file(input);
}

#[test]
fn run_omits_writer_breakdown_and_output_size_for_check_only() {
    let path = unique_temp_path(&std::env::temp_dir(), "run-reader", "xlsx");
    write_minimal_input_workbook(&path);

    let args = RunRequest {
        pipeline: PipelineName::Gb,
        input: Some(path.clone()),
        output: None,
        month_start: None,
        month_end: None,
        check_only: true,
        benchmark: false,
        summary_output: None,
        redact_paths: false,
        config: None,

        operation: RunOperation::Execute,
    };
    let summary = run(args).unwrap();

    assert_run_counts(&summary, 1, 0, 1, 1, 1, 10);
    assert_stage_timings(&summary, false, false);
    assert!(!summary.request_id.is_empty());
    assert_eq!(summary.output_size_bytes, None);
    assert_eq!(
        summary
            .stage_timings
            .stages
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["fact", "ingest", "normalize", "presentation", "split"]
    );
    assert_eq!(summary.sheet_count, 3);
    assert!(summary
        .quality_metrics
        .iter()
        .any(|metric| metric.metric == "可参与分析占比"));
    let serialized = serde_json::to_value(&summary).unwrap();
    assert!(serialized.get("error_log_preview").is_none());
    assert!(serialized.get("error_log_preview_truncated").is_none());
    assert!(serialized["output_size_bytes"].is_null());
    let serialized_text = serialized.to_string();
    for sensitive_field in ["row_id", "field_name", "original_value", "reason", "action"] {
        assert!(!serialized_text.contains(sensitive_field));
    }
    assert!(!summary.output_written);
    let _ = std::fs::remove_file(path);
}

#[test]
fn run_reports_request_id_writer_breakdown_and_output_size_for_normal_mode() {
    let input = unique_temp_path(&std::env::temp_dir(), "run-writes-input", "xlsx");
    let output = unique_temp_path(&std::env::temp_dir(), "run-writes-output", "xlsx");
    write_minimal_input_workbook(&input);

    let args = RunRequest {
        pipeline: PipelineName::Gb,
        input: Some(input.clone()),
        output: Some(output.clone()),
        month_start: None,
        month_end: None,
        check_only: false,
        benchmark: false,
        summary_output: None,
        redact_paths: false,
        config: None,

        operation: RunOperation::Execute,
    };
    let summary = run(args).unwrap();

    assert!(summary.output_written);
    assert!(!summary.request_id.is_empty());
    assert_eq!(summary.workbook_path, Some(output.display().to_string()));
    assert!(output.exists());
    assert_eq!(
        summary.output_size_bytes,
        Some(std::fs::metadata(&output).unwrap().len())
    );
    assert!(summary.output_size_bytes.unwrap() > 0);
    assert_eq!(
        summary
            .stage_timings
            .stages
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        [
            "export",
            "fact",
            "ingest",
            "normalize",
            "presentation",
            "split",
            "writer_populate",
            "xlsx_save",
        ]
    );
    assert_stage_timings(&summary, true, false);
    let _ = std::fs::remove_file(input);
    let _ = std::fs::remove_file(output);
}

#[test]
fn run_adds_total_timing_only_when_benchmark_is_enabled() {
    let path = unique_temp_path(&std::env::temp_dir(), "run-benchmark", "xlsx");
    write_minimal_input_workbook(&path);

    let summary = run(RunRequest {
        pipeline: PipelineName::Gb,
        input: Some(path.clone()),
        output: None,
        month_start: None,
        month_end: None,
        check_only: true,
        benchmark: true,
        summary_output: None,
        redact_paths: false,
        config: None,

        operation: RunOperation::Execute,
    })
    .unwrap();

    assert_stage_timings(&summary, false, true);
    let _ = std::fs::remove_file(path);
}

#[test]
fn run_returns_error_when_workbook_output_cannot_be_created() {
    let input = unique_temp_path(&std::env::temp_dir(), "run-output-error-input", "xlsx");
    let blocked_parent = unique_temp_path(&std::env::temp_dir(), "run-output-error-parent", "tmp");
    let output = blocked_parent.join("out.xlsx");
    write_minimal_input_workbook(&input);
    std::fs::write(&blocked_parent, "not a directory").unwrap();

    let args = RunRequest {
        pipeline: PipelineName::Gb,
        input: Some(input.clone()),
        output: Some(output.clone()),
        month_start: None,
        month_end: None,
        check_only: false,
        benchmark: false,
        summary_output: None,
        redact_paths: false,
        config: None,

        operation: RunOperation::Execute,
    };

    let error = run(args).unwrap_err().downcast::<CostingError>().unwrap();
    assert_eq!(error.code(), ErrorCode::OutputNotWritable);
    assert!(!output.exists());
    let _ = std::fs::remove_file(input);
    let _ = std::fs::remove_file(blocked_parent);
}

#[test]
fn run_rejects_non_strict_month_range() {
    let path = unique_temp_path(&std::env::temp_dir(), "invalid-month", "xlsx");
    std::fs::write(&path, "placeholder").unwrap();
    let args = RunRequest {
        pipeline: PipelineName::Gb,
        input: Some(path.clone()),
        output: None,
        month_start: Some("2025年01期".to_string()),
        month_end: None,
        check_only: true,
        benchmark: false,
        summary_output: None,
        redact_paths: false,
        config: None,

        operation: RunOperation::Execute,
    };

    let error = run(args).unwrap_err().downcast::<CostingError>().unwrap();
    assert_eq!(error.code(), ErrorCode::InvalidInput);
    let _ = std::fs::remove_file(path);
}

#[test]
fn run_filters_rows_inside_month_range_before_split_summary() {
    let path = unique_temp_path(&std::env::temp_dir(), "month-range", "xlsx");
    let mut workbook = Workbook::new();
    let sheet = workbook.add_worksheet();
    sheet.set_name("成本计算单").unwrap();
    sheet.write_string(0, 0, "年期").unwrap();
    sheet.write_string(0, 1, "产品编码").unwrap();
    sheet.write_string(0, 2, "产品名称").unwrap();
    sheet.write_string(0, 3, "工单编号").unwrap();
    sheet.write_string(0, 4, "工单行号").unwrap();
    sheet.write_string(0, 5, "本期完工数量").unwrap();
    sheet.write_string(0, 6, "本期完工金额").unwrap();
    sheet.write_string(0, 7, "成本项目名称").unwrap();
    sheet.write_string(1, 0, "").unwrap();
    sheet.write_string(1, 1, "").unwrap();
    sheet.write_string(1, 2, "").unwrap();
    sheet.write_string(1, 3, "").unwrap();
    sheet.write_string(1, 4, "").unwrap();
    sheet.write_string(1, 5, "").unwrap();
    sheet.write_string(1, 6, "").unwrap();
    sheet.write_string(1, 7, "").unwrap();
    sheet.write_string(2, 0, "2025年01期").unwrap();
    sheet.write_string(2, 1, "P1").unwrap();
    sheet.write_string(2, 2, "产品").unwrap();
    sheet.write_string(2, 3, "WO-1").unwrap();
    sheet.write_string(2, 4, "1").unwrap();
    sheet.write_number(2, 5, 1).unwrap();
    sheet.write_number(2, 6, 10).unwrap();
    sheet.write_string(2, 7, "").unwrap();
    sheet.write_string(3, 0, "2025年02期").unwrap();
    sheet.write_string(3, 1, "P2").unwrap();
    sheet.write_string(3, 2, "产品").unwrap();
    sheet.write_string(3, 3, "WO-2").unwrap();
    sheet.write_string(3, 4, "1").unwrap();
    sheet.write_number(3, 5, 1).unwrap();
    sheet.write_number(3, 6, 10).unwrap();
    sheet.write_string(3, 7, "").unwrap();
    workbook.save(&path).unwrap();

    let args = RunRequest {
        pipeline: PipelineName::Gb,
        input: Some(path.clone()),
        output: None,
        month_start: Some("2025-02".to_string()),
        month_end: Some("2025-02".to_string()),
        check_only: true,
        benchmark: false,
        summary_output: None,
        redact_paths: false,
        config: None,

        operation: RunOperation::Execute,
    };
    let summary = run(args).unwrap();

    assert_run_counts(&summary, 2, 0, 1, 1, 1, 10);
    let _ = std::fs::remove_file(path);
}

fn assert_run_counts(
    summary: &RunSummary,
    reader_rows: usize,
    detail_rows: usize,
    qty_rows: usize,
    work_order_rows: usize,
    qty_sheet_rows: usize,
    quality_metric_count: usize,
) {
    assert_eq!(summary.run_counts.len(), 6);
    assert_eq!(summary.run_counts.get("reader_rows"), Some(&reader_rows));
    assert_eq!(summary.run_counts.get("detail_rows"), Some(&detail_rows));
    assert_eq!(summary.run_counts.get("qty_rows"), Some(&qty_rows));
    assert_eq!(
        summary.run_counts.get("work_order_rows"),
        Some(&work_order_rows)
    );
    assert_eq!(
        summary.run_counts.get("qty_sheet_rows"),
        Some(&qty_sheet_rows)
    );
    assert_eq!(
        summary.run_counts.get("quality_metric_count"),
        Some(&quality_metric_count)
    );
}

fn assert_stage_timings(summary: &RunSummary, has_export: bool, has_total: bool) {
    let timings = &summary.stage_timings.stages;
    for stage in ["ingest", "normalize", "split", "fact", "presentation"] {
        assert!(timings.contains_key(stage), "missing timing for {stage}");
    }
    assert_eq!(timings.contains_key("export"), has_export);
    assert_eq!(timings.contains_key("total"), has_total);
    assert!(timings.keys().all(|stage| !stage.ends_with("_rows")));
    assert!(!timings.contains_key("quality_metric_count"));
    assert!(timings
        .values()
        .all(|seconds| seconds.is_finite() && *seconds >= 0.0));
}

fn unique_temp_path(base_dir: &std::path::Path, suffix: &str, ext: &str) -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    base_dir
        .join(format!(
            "costing-rust-{}-pid{}-{}",
            suffix,
            process::id(),
            now
        ))
        .with_extension(ext)
}

fn write_minimal_input_workbook(path: &std::path::Path) {
    let mut workbook = Workbook::new();
    let sheet = workbook.add_worksheet();
    sheet.set_name("成本计算单").unwrap();
    sheet.write_string(0, 0, "年期").unwrap();
    sheet.write_string(0, 1, "产品编码").unwrap();
    sheet.write_string(0, 2, "产品名称").unwrap();
    sheet.write_string(0, 3, "工单编号").unwrap();
    sheet.write_string(0, 4, "工单行号").unwrap();
    sheet.write_string(0, 5, "本期完工数量").unwrap();
    sheet.write_string(0, 6, "本期完工金额").unwrap();
    sheet.write_string(0, 7, "成本项目名称").unwrap();
    sheet.write_string(0, 8, "日期").unwrap();
    sheet.write_string(1, 0, "").unwrap();
    sheet.write_string(1, 1, "").unwrap();
    sheet.write_string(1, 2, "").unwrap();
    sheet.write_string(1, 3, "").unwrap();
    sheet.write_string(1, 4, "").unwrap();
    sheet.write_string(1, 5, "").unwrap();
    sheet.write_string(1, 6, "").unwrap();
    sheet.write_string(1, 7, "").unwrap();
    sheet.write_string(1, 8, "").unwrap();
    sheet.write_string(2, 0, "2025年01期").unwrap();
    sheet.write_string(2, 1, "P1").unwrap();
    sheet.write_string(2, 2, "产品").unwrap();
    sheet.write_string(2, 3, "WO-1").unwrap();
    sheet.write_string(2, 4, "1").unwrap();
    sheet.write_number(2, 5, 1).unwrap();
    sheet.write_number(2, 6, 10).unwrap();
    sheet.write_string(2, 7, "").unwrap();
    let date_format = Format::new().set_num_format("yyyy-mm-dd");
    sheet
        .write_datetime_with_format(
            2,
            8,
            ExcelDateTime::from_ymd(2025, 1, 2).unwrap(),
            &date_format,
        )
        .unwrap();
    workbook.save(path).unwrap();
}

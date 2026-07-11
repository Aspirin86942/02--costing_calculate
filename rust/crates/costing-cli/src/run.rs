use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use costing_core::error::{ErrorContext, ErrorStage};
use costing_core::fact::build_fact_bundle;
use costing_core::model::MonthRange;
use costing_core::normalize::{build_month_range, normalize_workbook};
use costing_core::presentation::build_workbook_payload;
use costing_core::split::split_detail_and_qty;
use costing_core::timing::measure;
use costing_core::{CostingError, ErrorCode, PipelineConfig, RunSummary, StageTimings};
use costing_xlsx::{
    reader::{read_raw_workbook, CostingXlsxError, XlsxError},
    writer::{write_workbook, WriterContext, WriterError, WriterPrimaryError},
};

use crate::args::CliArgs;

#[derive(Debug, PartialEq, Eq)]
struct ResolvedCliPaths {
    input: PathBuf,
    output: Option<PathBuf>,
}

pub fn run(mut args: CliArgs) -> anyhow::Result<RunSummary> {
    let request_id = new_request_id();
    let month_range = build_month_range(args.month_start.as_deref(), args.month_end.as_deref())
        .map_err(|error| {
            with_stage_context(error, &request_id, ErrorStage::ValidateCliRequest, None)
        })?;
    let base_dir = std::env::current_dir().map_err(|error| {
        let error = CostingError::io_with_source(
            ErrorCode::InvalidInput,
            format!("无法获取当前工作目录: {error}"),
            error,
        );
        with_stage_context(
            error,
            &request_id,
            ErrorStage::ResolveCliPaths,
            Some(PathBuf::from(".")),
        )
    })?;
    let resolve_path = args.input.clone().unwrap_or_else(|| {
        base_dir
            .join("data")
            .join("raw")
            .join(args.pipeline.as_str())
    });
    let paths = resolve_cli_paths(&args, &base_dir, month_range.as_ref()).map_err(|error| {
        with_stage_context(
            error,
            &request_id,
            ErrorStage::ResolveCliPaths,
            Some(resolve_path),
        )
    })?;
    args.input = Some(paths.input);
    args.output = paths.output;
    validate_cli_request(&args).map_err(|error| {
        with_stage_context(
            error,
            &request_id,
            ErrorStage::ValidateCliRequest,
            args.input.clone(),
        )
    })?;
    let month_filter_requested = month_range.is_some();
    let pipeline = PipelineConfig::for_name(args.pipeline);
    let mut timings = StageTimings::default();
    let input = args
        .input
        .as_ref()
        .expect("resolve_cli_paths always supplies an input path")
        .clone();
    let total_started = args.benchmark.then(Instant::now);

    let (raw, reader_rows) = measure(&mut timings, "ingest", || {
        let raw = read_raw_workbook(&input).map_err(|error| map_xlsx_read_error(&input, error))?;
        let reader_rows = raw.rows.len();
        Ok::<_, CostingError>((raw, reader_rows))
    })
    .map_err(|error| {
        with_stage_context(
            error,
            &request_id,
            ErrorStage::IngestWorkbook,
            Some(input.clone()),
        )
    })?;
    let normalized = measure(&mut timings, "normalize", || {
        normalize_workbook(raw, &pipeline, month_range)
    })
    .map_err(|error| {
        with_stage_context(
            error,
            &request_id,
            ErrorStage::Normalize,
            Some(input.clone()),
        )
    })?;
    let month_filter_empty_result = month_filter_requested && normalized.is_empty();
    let split =
        measure(&mut timings, "split", || split_detail_and_qty(normalized)).map_err(|error| {
            with_stage_context(error, &request_id, ErrorStage::Split, Some(input.clone()))
        })?;
    let bundle =
        measure(&mut timings, "fact", || build_fact_bundle(split, &pipeline)).map_err(|error| {
            with_stage_context(
                error,
                &request_id,
                ErrorStage::BuildFact,
                Some(input.clone()),
            )
        })?;
    let payload_timings = timings.clone();
    let payload = measure(&mut timings, "presentation", || {
        build_workbook_payload(
            bundle,
            &pipeline,
            payload_timings,
            month_filter_empty_result,
        )
    })
    .map_err(|error| {
        with_stage_context(
            error,
            &request_id,
            ErrorStage::BuildPresentation,
            Some(input.clone()),
        )
    })?;

    if let Some(started) = total_started {
        timings.insert("total", started.elapsed().as_secs_f64());
    }

    let detail_rows = required_quality_count(&payload.quality_metrics, "成本明细输入行数")
        .map_err(|error| {
            with_stage_context(
                error,
                &request_id,
                ErrorStage::BuildPresentation,
                Some(input.clone()),
            )
        })?;
    let qty_rows = required_quality_count(&payload.quality_metrics, "产品数量统计输出行数")
        .map_err(|error| {
            with_stage_context(
                error,
                &request_id,
                ErrorStage::BuildPresentation,
                Some(input.clone()),
            )
        })?;
    let work_order_rows = required_quality_count(&payload.quality_metrics, "工单异常分析输出行数")
        .map_err(|error| {
            with_stage_context(
                error,
                &request_id,
                ErrorStage::BuildPresentation,
                Some(input.clone()),
            )
        })?;
    let qty_sheet_rows = payload
        .sheet_models
        .iter()
        .find(|sheet| sheet.sheet_name == "成本计算单数量聚合维度")
        .ok_or_else(|| CostingError::Internal {
            code: ErrorCode::InternalError,
            message: "workbook payload is missing quantity sheet".to_string(),
        })
        .map_err(|error| {
            with_stage_context(
                error,
                &request_id,
                ErrorStage::BuildPresentation,
                Some(input.clone()),
            )
        })?
        .rows
        .len();
    let run_counts = BTreeMap::from([
        ("reader_rows".to_string(), reader_rows),
        ("detail_rows".to_string(), detail_rows),
        ("qty_rows".to_string(), qty_rows),
        ("qty_sheet_rows".to_string(), qty_sheet_rows),
        (
            "quality_metric_count".to_string(),
            payload.quality_metrics.len(),
        ),
        ("work_order_rows".to_string(), work_order_rows),
    ]);
    let workbook_path = args.output.as_ref().map(|path| path.display().to_string());
    let output_size_bytes = if !args.check_only {
        let output = args
            .output
            .as_ref()
            .expect("resolve_cli_paths supplies output for non check-only runs");
        let writer_context = WriterContext {
            request_id: request_id.clone(),
        };
        let report = measure(&mut timings, "export", || {
            write_workbook(&writer_context, output, &payload)
                .map_err(|error| map_xlsx_write_error(output, error))
        })
        .map_err(|error| {
            with_stage_context(
                error,
                &request_id,
                ErrorStage::SaveWorkbook,
                Some(output.clone()),
            )
        })?;
        timings.insert("writer_populate", report.writer_populate_seconds);
        timings.insert("xlsx_save", report.xlsx_save_seconds);
        Some(report.output_size_bytes)
    } else {
        None
    };
    let mut issue_type_counts = BTreeMap::new();
    for issue in &payload.error_log {
        *issue_type_counts
            .entry(issue.issue_type.clone())
            .or_default() += 1;
    }
    Ok(RunSummary {
        status: "succeeded".to_string(),
        request_id,
        pipeline: pipeline.name.as_str().to_string(),
        output_written: !args.check_only,
        output_size_bytes,
        workbook_path,
        sheet_count: payload.sheet_models.len(),
        error_log_count: payload.error_log_count,
        issue_type_counts,
        quality_metrics: payload.quality_metrics,
        run_counts,
        stage_timings: timings,
    })
}

fn with_stage_context(
    error: CostingError,
    request_id: &str,
    stage: ErrorStage,
    default_path: Option<PathBuf>,
) -> CostingError {
    let path = error.path().map(Path::to_path_buf).or(default_path);
    error.with_context(ErrorContext::new(request_id, stage, path))
}

fn new_request_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("costing-{}-{nanos}", std::process::id())
}

fn required_quality_count(
    quality_metrics: &[costing_core::model::QualityMetric],
    metric_name: &str,
) -> Result<usize, CostingError> {
    let mut matches = quality_metrics
        .iter()
        .filter(|metric| metric.metric == metric_name);
    let metric = matches.next().ok_or_else(|| CostingError::Internal {
        code: ErrorCode::InternalError,
        message: format!("workbook payload is missing quality metric: {metric_name}"),
    })?;
    if matches.next().is_some() {
        return Err(CostingError::Internal {
            code: ErrorCode::InternalError,
            message: format!("workbook payload has duplicate quality metric: {metric_name}"),
        });
    }
    metric
        .value
        .parse::<usize>()
        .map_err(|error| CostingError::Internal {
            code: ErrorCode::InternalError,
            message: format!(
                "workbook payload quality metric {metric_name} is not an integer: {}; {error}",
                metric.value,
            ),
        })
}

fn resolve_cli_paths(
    args: &CliArgs,
    base_dir: &Path,
    month_range: Option<&MonthRange>,
) -> Result<ResolvedCliPaths, CostingError> {
    let pipeline = args.pipeline.as_str();
    let input = match &args.input {
        Some(input) => input.clone(),
        None => discover_default_input(base_dir, pipeline)?,
    };
    let output = match (&args.output, args.check_only) {
        (Some(output), _) => Some(output.clone()),
        (None, true) => None,
        (None, false) => Some(default_output_path(
            base_dir,
            pipeline,
            &input,
            month_range,
        )?),
    };
    Ok(ResolvedCliPaths { input, output })
}

fn discover_default_input(base_dir: &Path, pipeline: &str) -> Result<PathBuf, CostingError> {
    let raw_dir = base_dir.join("data").join("raw").join(pipeline);
    let entries = std::fs::read_dir(&raw_dir).map_err(|error| {
        let code = if error.kind() == std::io::ErrorKind::NotFound {
            ErrorCode::FileNotFound
        } else {
            ErrorCode::FileNotReadable
        };
        CostingError::io(
            code,
            format!("无法读取默认输入目录 {}: {error}", raw_dir.display()),
            raw_dir.clone(),
        )
    })?;
    let expected_prefix = format!("{pipeline}-");
    let mut candidates = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            CostingError::io(
                ErrorCode::FileNotReadable,
                format!("读取默认输入目录项失败: {error}"),
                raw_dir.clone(),
            )
        })?;
        let path = entry.path();
        let file_name = entry.file_name();
        let normalized_name = file_name.to_string_lossy().to_ascii_lowercase();
        if !normalized_name.starts_with(&expected_prefix) || !normalized_name.ends_with(".xlsx") {
            continue;
        }
        let metadata = entry.metadata().map_err(|error| {
            CostingError::io(
                ErrorCode::FileNotReadable,
                format!("读取默认输入文件元数据失败 {}: {error}", path.display()),
                path.clone(),
            )
        })?;
        if !metadata.is_file() {
            continue;
        }
        candidates.push(path);
    }
    candidates.sort();
    match candidates.as_slice() {
        [input] => Ok(input.clone()),
        [] => Err(CostingError::io(
            ErrorCode::FileNotFound,
            format!(
                "未在默认输入目录 {} 找到 {pipeline}-*.xlsx",
                raw_dir.display()
            ),
            raw_dir,
        )),
        _ => {
            let candidate_text = candidates
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ");
            Err(CostingError::invalid_input(format!(
                "检测到多个 {pipeline} 输入文件，请使用 --input 明确指定: {candidate_text}"
            )))
        }
    }
}

fn default_output_path(
    base_dir: &Path,
    pipeline: &str,
    input: &Path,
    month_range: Option<&MonthRange>,
) -> Result<PathBuf, CostingError> {
    let stem = input
        .file_stem()
        .ok_or_else(|| CostingError::invalid_input("输入文件名缺少有效主文件名"))?;
    let mut file_name = stem.to_os_string();
    file_name.push("_处理后");
    if let Some(suffix) = month_output_suffix(month_range) {
        file_name.push("_");
        file_name.push(suffix);
    }
    file_name.push(".xlsx");
    Ok(base_dir
        .join("data")
        .join("processed")
        .join(pipeline)
        .join(file_name))
}

fn month_output_suffix(month_range: Option<&MonthRange>) -> Option<String> {
    let month_range = month_range?;
    match (&month_range.start, &month_range.end) {
        (Some(start), Some(end)) => Some(format!("{start}_{end}")),
        (Some(start), None) => Some(format!("from_{start}")),
        (None, Some(end)) => Some(format!("to_{end}")),
        (None, None) => None,
    }
}

pub fn validate_cli_request(args: &CliArgs) -> Result<(), CostingError> {
    let input = args
        .input
        .as_ref()
        .ok_or_else(|| CostingError::invalid_input("缺少输入文件路径"))?;
    if !input.exists() {
        return Err(CostingError::Io {
            code: ErrorCode::FileNotFound,
            message: format!("输入文件不存在: {}", input.display()),
            path: input.clone(),
            retryable: false,
        });
    }
    if !input.is_file() {
        return Err(CostingError::Io {
            code: ErrorCode::InvalidInput,
            message: format!("输入路径不是文件: {}", input.display()),
            path: input.clone(),
            retryable: false,
        });
    }
    if input
        .extension()
        .and_then(|value| value.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
        != Some("xlsx")
    {
        return Err(CostingError::Io {
            code: ErrorCode::UnsupportedFileType,
            message: "输入文件必须是 .xlsx 格式".to_string(),
            path: input.clone(),
            retryable: false,
        });
    }
    if !args.check_only && args.output.is_none() {
        return Err(CostingError::invalid_input(
            "非 check-only 运行必须提供 --output",
        ));
    }
    if !args.check_only {
        let output = args.output.as_ref().expect("checked output above");
        if paths_resolve_to_same_file(input, output) {
            return Err(CostingError::invalid_input(
                "输入文件与输出文件不能是同一文件",
            ));
        }
    }
    Ok(())
}

fn paths_resolve_to_same_file(input: &Path, output: &Path) -> bool {
    if !output.exists() {
        return false;
    }
    match (input.canonicalize(), output.canonicalize()) {
        (Ok(input), Ok(output)) => input == output,
        _ => input == output,
    }
}

fn map_xlsx_read_error(path: &Path, error: CostingXlsxError) -> CostingError {
    let code = match &error {
        CostingXlsxError::Calamine(_) => ErrorCode::FileNotReadable,
        CostingXlsxError::Message(_) => ErrorCode::InvalidInput,
        CostingXlsxError::Writer(_) => ErrorCode::InvalidInput,
    };
    CostingError::io(
        code,
        format!("读取 workbook 失败: {error}"),
        path.to_path_buf(),
    )
}

fn map_xlsx_write_error(path: &Path, error: WriterError) -> CostingError {
    let WriterError { context, primary } = error;
    let mapped = match primary {
        WriterPrimaryError::Io(source) => {
            let is_create_race = context.details.stage == ErrorStage::CreateFinalOutput
                && source.kind() == std::io::ErrorKind::AlreadyExists;
            let code = if is_create_race {
                ErrorCode::OutputExists
            } else {
                ErrorCode::OutputNotWritable
            };
            let message = if is_create_race {
                format!("输出 workbook 已存在: {}", path.display())
            } else {
                format!("写出 workbook 失败: {source}")
            };
            CostingError::io_with_source(code, message, source)
        }
        WriterPrimaryError::Xlsx(CostingXlsxError::Writer(XlsxError::IoError(source))) => {
            let message = format!("写出 workbook 失败: {source}");
            CostingError::io_with_source(ErrorCode::OutputNotWritable, message, source)
        }
        primary => CostingError::Writer {
            code: ErrorCode::OutputNotWritable,
            message: format!("写出 workbook 失败: {primary}"),
            retryable: false,
        },
    };
    mapped.with_context(context)
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::path::PathBuf;
    use std::process;
    use std::time::{SystemTime, UNIX_EPOCH};

    use costing_core::model::ErrorSummary;
    use costing_core::{ErrorCode, PipelineName, RunSummary};
    use rust_xlsxwriter::{ExcelDateTime, Format, Workbook};

    use super::*;
    use crate::args::CliArgs;

    fn args(input: &str) -> CliArgs {
        CliArgs {
            pipeline: PipelineName::Gb,
            input: Some(PathBuf::from(input)),
            output: Some(PathBuf::from("out.xlsx")),
            month_start: None,
            month_end: None,
            check_only: false,
            benchmark: false,
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
        let request = CliArgs {
            pipeline: PipelineName::Gb,
            input: Some(path.clone()),
            output: None,
            month_start: None,
            month_end: None,
            check_only: true,
            benchmark: false,
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
        let request = CliArgs {
            pipeline: PipelineName::Gb,
            input: None,
            output: None,
            month_start: None,
            month_end: None,
            check_only: false,
            benchmark: false,
        };

        let paths = resolve_cli_paths(&request, &root, None).unwrap();

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
        let request = CliArgs {
            pipeline: PipelineName::Sk,
            input: None,
            output: None,
            month_start: Some("2026-01".to_string()),
            month_end: Some("2026-03".to_string()),
            check_only: false,
            benchmark: false,
        };
        let month_range = MonthRange {
            start: Some("2026-01".to_string()),
            end: Some("2026-03".to_string()),
        };

        let paths = resolve_cli_paths(&request, &root, Some(&month_range)).unwrap();

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
        let request = CliArgs {
            pipeline: PipelineName::Gb,
            input: Some(path.clone()),
            output: None,
            month_start: None,
            month_end: None,
            check_only: false,
            benchmark: false,
        };
        let error = validate_cli_request(&request).unwrap_err();
        assert_eq!(error.code(), ErrorCode::InvalidInput);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn existing_output_is_deferred_to_writer_atomic_create() {
        let input = unique_temp_path(&std::env::temp_dir(), "existing-output-input", "xlsx");
        let output = unique_temp_path(&std::env::temp_dir(), "existing-output", "xlsx");
        std::fs::write(&input, "input").unwrap();
        std::fs::write(&output, "existing").unwrap();
        let request = CliArgs {
            output: Some(output.clone()),
            ..args(input.to_str().unwrap())
        };

        let result = validate_cli_request(&request);

        assert!(result.is_ok());
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
        let writer_error = WriterError {
            context: ErrorContext::new(
                "writer-owned-request",
                ErrorStage::SaveWorkbook,
                Some(output.clone()),
            ),
            primary: WriterPrimaryError::Xlsx(CostingXlsxError::Writer(XlsxError::IoError(
                std::io::Error::from_raw_os_error(112),
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
        assert_eq!(original_io.raw_os_error(), Some(112));

        let json = serde_json::to_value(ErrorSummary::from_error(&error)).unwrap();
        assert_eq!(json["details"]["io_kind"], "StorageFull");
        assert_eq!(json["details"]["raw_os_error"], 112);
    }

    #[test]
    fn rejects_input_and_output_that_resolve_to_same_file() {
        let input = unique_temp_path(&std::env::temp_dir(), "same-input-output", "xlsx");
        std::fs::write(&input, "input").unwrap();
        let request = CliArgs {
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

        let args = CliArgs {
            pipeline: PipelineName::Gb,
            input: Some(path.clone()),
            output: None,
            month_start: None,
            month_end: None,
            check_only: true,
            benchmark: false,
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

        let args = CliArgs {
            pipeline: PipelineName::Gb,
            input: Some(input.clone()),
            output: Some(output.clone()),
            month_start: None,
            month_end: None,
            check_only: false,
            benchmark: false,
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

        let summary = run(CliArgs {
            pipeline: PipelineName::Gb,
            input: Some(path.clone()),
            output: None,
            month_start: None,
            month_end: None,
            check_only: true,
            benchmark: true,
        })
        .unwrap();

        assert_stage_timings(&summary, false, true);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn run_returns_error_when_workbook_output_cannot_be_created() {
        let input = unique_temp_path(&std::env::temp_dir(), "run-output-error-input", "xlsx");
        let blocked_parent =
            unique_temp_path(&std::env::temp_dir(), "run-output-error-parent", "tmp");
        let output = blocked_parent.join("out.xlsx");
        write_minimal_input_workbook(&input);
        std::fs::write(&blocked_parent, "not a directory").unwrap();

        let args = CliArgs {
            pipeline: PipelineName::Gb,
            input: Some(input.clone()),
            output: Some(output.clone()),
            month_start: None,
            month_end: None,
            check_only: false,
            benchmark: false,
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
        let args = CliArgs {
            pipeline: PipelineName::Gb,
            input: Some(path.clone()),
            output: None,
            month_start: Some("2025年01期".to_string()),
            month_end: None,
            check_only: true,
            benchmark: false,
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

        let args = CliArgs {
            pipeline: PipelineName::Gb,
            input: Some(path.clone()),
            output: None,
            month_start: Some("2025-02".to_string()),
            month_end: Some("2025-02".to_string()),
            check_only: true,
            benchmark: false,
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
}

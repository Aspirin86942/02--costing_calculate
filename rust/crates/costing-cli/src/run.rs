use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use costing_core::error::{ErrorContext, ErrorStage};
use costing_core::{
    build_month_range, process_workbook, CostingError, ErrorCode, PipelineRules, RunSummary,
    StageTimings,
};
use costing_xlsx::{
    reader::{read_raw_workbook, read_raw_workbook_from_bytes, CostingXlsxError, XlsxError},
    writer::{write_workbook, WriterContext, WriterError, WriterPrimaryError},
};

use crate::application::manifest::{sha256_bytes, sha256_file, RunAudit};
use crate::application::RunRequest;

#[path = "run_paths.rs"]
mod paths;

#[cfg(test)]
use paths::{month_output_suffix, validate_cli_request};
use paths::{resolve_cli_paths, validate_cli_request_from};

#[cfg(test)]
pub(crate) fn run(
    args: RunRequest,
    pipeline: PipelineRules,
    input_pattern: String,
    request_id: String,
) -> anyhow::Result<RunSummary> {
    let cwd = std::env::current_dir().map_err(|source| {
        CostingError::io_with_source(
            ErrorCode::InvalidInput,
            format!("无法获取当前工作目录: {source}"),
            source,
        )
        .with_context(ErrorContext::new(
            &request_id,
            ErrorStage::ResolveCliPaths,
            None,
        ))
    })?;
    let mut audit = RunAudit::new(&args, false, cwd);
    run_with_audit(args, pipeline, input_pattern, request_id, &mut audit)
}

pub(crate) fn run_with_audit(
    mut args: RunRequest,
    pipeline: PipelineRules,
    input_pattern: String,
    request_id: String,
    audit: &mut RunAudit,
) -> anyhow::Result<RunSummary> {
    let month_range = build_month_range(args.month_start.as_deref(), args.month_end.as_deref())
        .map_err(|error| {
            with_stage_context(error, &request_id, ErrorStage::ValidateCliRequest, None)
        })?;
    let base_dir = audit.cwd().to_path_buf();
    let resolve_path = args.input.clone().unwrap_or_else(|| {
        base_dir
            .join("data")
            .join("raw")
            .join(args.pipeline.as_str())
    });
    let paths = resolve_cli_paths(&args, &base_dir, month_range.as_ref(), &input_pattern).map_err(
        |error| {
            with_stage_context(
                error,
                &request_id,
                ErrorStage::ResolveCliPaths,
                Some(resolve_path),
            )
        },
    )?;
    args.input = Some(paths.input);
    args.output = paths.output;
    audit.record_resolved_paths(
        args.input
            .as_deref()
            .expect("resolved paths always include input"),
        args.output.as_deref(),
    );
    validate_cli_request_from(&args, &base_dir).map_err(|error| {
        with_stage_context(
            error,
            &request_id,
            ErrorStage::ValidateCliRequest,
            args.input.clone(),
        )
    })?;
    let mut timings = StageTimings::default();
    let input = args
        .input
        .as_ref()
        .expect("resolve_cli_paths always supplies an input path")
        .clone();
    let total_started = args.benchmark.then(Instant::now);

    let capture_input_identity = audit.enabled();
    let (raw, reader_rows, input_identity) = measure(&mut timings, "ingest", || {
        let (raw, input_identity) = if capture_input_identity {
            let bytes = std::fs::read(&input).map_err(|source| {
                CostingError::io_with_source(
                    ErrorCode::FileNotReadable,
                    format!("读取 workbook 字节失败: {source}"),
                    source,
                )
            })?;
            let size_bytes = bytes.len() as u64;
            let sha256 = sha256_bytes(&bytes);
            let raw = read_raw_workbook_from_bytes(&bytes)
                .map_err(|error| map_xlsx_read_error(&input, error))?;
            (raw, Some((size_bytes, sha256)))
        } else {
            let raw =
                read_raw_workbook(&input).map_err(|error| map_xlsx_read_error(&input, error))?;
            (raw, None)
        };
        let reader_rows = raw.rows.len();
        Ok::<_, CostingError>((raw, reader_rows, input_identity))
    })
    .map_err(|error| {
        with_stage_context(
            error,
            &request_id,
            ErrorStage::IngestWorkbook,
            Some(input.clone()),
        )
    })?;
    audit.record_reader_identity(raw.sheet_name.clone(), reader_rows);
    if let Some((input_size_bytes, input_sha256)) = input_identity {
        audit.record_input(
            input_size_bytes,
            input_sha256,
            raw.sheet_name.clone(),
            reader_rows,
        );
    }
    let processed = process_workbook(raw, &pipeline, month_range, timings).map_err(|failure| {
        let stage = failure.stage();
        with_stage_context(
            failure.into_error(),
            &request_id,
            stage,
            Some(input.clone()),
        )
    })?;
    let payload = processed.payload;
    timings = processed.stage_timings;

    if let Some(started) = total_started {
        timings.insert("total", started.elapsed().as_secs_f64());
    }

    let quality_count = |metric_name: &str| {
        required_quality_count(&payload.quality_metrics, metric_name).map_err(|error| {
            with_stage_context(
                error,
                &request_id,
                ErrorStage::BuildPresentation,
                Some(input.clone()),
            )
        })
    };
    let detail_rows = quality_count("成本明细输入行数")?;
    let qty_rows = quality_count("产品数量统计输出行数")?;
    let work_order_rows = quality_count("工单异常分析输出行数")?;
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
    audit.record_sheet_names(
        payload
            .sheet_models
            .iter()
            .map(|sheet| sheet.sheet_name.clone())
            .collect(),
    );
    let workbook_path = args.output.as_ref().map(|path| path.display().to_string());
    let output_size_bytes = if !args.check_only {
        let output = args
            .output
            .as_ref()
            .expect("resolve_cli_paths supplies output for non check-only runs");
        let writer_context = WriterContext {
            request_id: request_id.clone(),
        };
        let report = match measure(&mut timings, "export", || {
            write_workbook(&writer_context, output, &payload).map_err(Box::new)
        }) {
            Ok(report) => report,
            Err(error) => {
                return Err(record_and_map_writer_error(audit, output, *error).into());
            }
        };
        timings.insert("writer_populate", report.writer_populate_seconds);
        timings.insert("xlsx_save", report.xlsx_save_seconds);
        audit.mark_output_published(report.output_size_bytes, report.low_memory_writer);
        if audit.enabled() {
            let (output_size_bytes, output_sha256) = sha256_file(output).map_err(|source| {
                let mut context =
                    ErrorContext::new(&request_id, ErrorStage::HashOutput, Some(output.clone()));
                context.details.final_output_valid = true;
                context.details.final_output =
                    Some(Box::new(costing_core::error::FinalOutputMeta {
                        final_output_path: output.clone(),
                        final_output_sha256: None,
                    }));
                CostingError::io_with_source(
                    ErrorCode::OutputNotWritable,
                    format!("计算输出 workbook SHA-256 失败: {source}"),
                    source,
                )
                .with_context(context)
            })?;
            audit.record_output(output_size_bytes, output_sha256, report.low_memory_writer);
        }
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

fn measure<T, E>(
    timings: &mut StageTimings,
    stage: &'static str,
    f: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    let started = Instant::now();
    let result = f();
    timings.insert(stage, started.elapsed().as_secs_f64());
    result
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

fn record_and_map_writer_error(
    audit: &mut RunAudit,
    path: &Path,
    error: WriterError,
) -> CostingError {
    let low_memory_writer = error.low_memory_writer;
    let final_output_valid = error.context.details.final_output_valid;
    audit.record_writer_failure(low_memory_writer, final_output_valid);
    if final_output_valid && audit.enabled() {
        match sha256_file(path) {
            Ok((size_bytes, sha256)) => {
                audit.record_output(size_bytes, sha256, low_memory_writer);
            }
            Err(source) => {
                audit.warn(format!(
                    "有效 workbook 已发布，但恢复输出 SHA-256 失败: {source}"
                ));
            }
        }
    }
    map_xlsx_write_error(path, error)
}

fn map_xlsx_write_error(path: &Path, error: WriterError) -> CostingError {
    let WriterError {
        context, primary, ..
    } = error;
    let mapped = match primary {
        WriterPrimaryError::Io(source) => {
            let is_create_race = matches!(
                context.details.stage,
                ErrorStage::CreateFinalOutput | ErrorStage::PublishWorkbook
            ) && source.kind() == std::io::ErrorKind::AlreadyExists;
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
#[path = "run_tests.rs"]
mod tests;

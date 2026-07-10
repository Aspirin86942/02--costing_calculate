use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

use costing_core::fact::build_fact_bundle;
use costing_core::normalize::{build_month_range, normalize_workbook};
use costing_core::presentation::build_workbook_payload;
use costing_core::split::split_detail_and_qty;
use costing_core::timing::measure;
use costing_core::{CostingError, ErrorCode, PipelineConfig, RunSummary, StageTimings};
use costing_xlsx::{
    reader::{read_raw_workbook, CostingXlsxError},
    snapshot::build_reader_snapshot,
    writer::write_workbook,
};

use crate::args::CliArgs;

const ERROR_LOG_PREVIEW_LIMIT: usize = 20;

pub fn run(args: CliArgs) -> anyhow::Result<RunSummary> {
    validate_cli_request(&args)?;
    let month_range = build_month_range(args.month_start.as_deref(), args.month_end.as_deref())?;
    let month_filter_requested = month_range.is_some();
    let pipeline = PipelineConfig::for_name(args.pipeline);
    let mut timings = StageTimings::default();
    let total_started = args.benchmark.then(Instant::now);
    let (raw, snapshot) = measure(&mut timings, "ingest", || {
        let raw = read_raw_workbook(&args.input)
            .map_err(|error| map_xlsx_read_error(&args.input, error))?;
        let snapshot = build_reader_snapshot(&raw);
        Ok::<_, CostingError>((raw, snapshot))
    })?;
    let normalized = measure(&mut timings, "normalize", || {
        Ok::<_, anyhow::Error>(normalize_workbook(raw, &pipeline, month_range)?)
    })?;
    let month_filter_empty_result = month_filter_requested && normalized.rows.is_empty();
    let split = measure(&mut timings, "split", || {
        Ok::<_, anyhow::Error>(split_detail_and_qty(normalized)?)
    })?;
    let bundle = measure(&mut timings, "fact", || {
        Ok::<_, anyhow::Error>(build_fact_bundle(split, &pipeline)?)
    })?;
    let mut run_counts = BTreeMap::from([
        ("reader_rows".to_string(), snapshot.row_count),
        ("detail_rows".to_string(), bundle.detail_fact.len()),
        ("qty_rows".to_string(), bundle.qty_fact.len()),
        ("work_order_rows".to_string(), bundle.work_order_fact.len()),
    ]);
    let payload_timings = timings.clone();
    let payload = measure(&mut timings, "presentation", || {
        build_workbook_payload(
            bundle,
            &pipeline,
            payload_timings,
            month_filter_empty_result,
        )
    })?;
    run_counts.insert(
        "qty_sheet_rows".to_string(),
        payload
            .sheet_models
            .iter()
            .find(|sheet| sheet.sheet_name == "成本计算单数量聚合维度")
            .expect("workbook payload must contain the quantity aggregation sheet")
            .rows
            .len(),
    );
    run_counts.insert(
        "quality_metric_count".to_string(),
        payload.quality_metrics.len(),
    );
    let workbook_path = args.output.as_ref().map(|path| path.display().to_string());
    if !args.check_only {
        let output = args
            .output
            .as_ref()
            .expect("validate_cli_request requires --output for non check-only runs");
        measure(&mut timings, "export", || {
            write_workbook(output, &payload).map_err(|error| map_xlsx_write_error(output, error))
        })?;
    }
    if let Some(started) = total_started {
        timings.insert("total", started.elapsed().as_secs_f64());
    }
    let mut issue_type_counts = BTreeMap::new();
    for issue in &payload.error_log {
        *issue_type_counts
            .entry(issue.issue_type.clone())
            .or_default() += 1;
    }
    let error_log_preview = payload
        .error_log
        .iter()
        .take(ERROR_LOG_PREVIEW_LIMIT)
        .cloned()
        .collect::<Vec<_>>();

    Ok(RunSummary {
        status: "succeeded".to_string(),
        pipeline: pipeline.name.as_str().to_string(),
        output_written: !args.check_only,
        workbook_path,
        sheet_count: payload.sheet_models.len(),
        error_log_count: payload.error_log_count,
        issue_type_counts,
        error_log_preview_truncated: payload.error_log.len() > ERROR_LOG_PREVIEW_LIMIT,
        error_log_preview,
        quality_metrics: payload.quality_metrics,
        run_counts,
        stage_timings: timings,
    })
}

pub fn validate_cli_request(args: &CliArgs) -> Result<(), CostingError> {
    if !args.input.exists() {
        return Err(CostingError::Io {
            code: ErrorCode::FileNotFound,
            message: format!("输入文件不存在: {}", args.input.display()),
            path: args.input.clone(),
            retryable: false,
        });
    }
    if !args.input.is_file() {
        return Err(CostingError::Io {
            code: ErrorCode::InvalidInput,
            message: format!("输入路径不是文件: {}", args.input.display()),
            path: args.input.clone(),
            retryable: false,
        });
    }
    if args
        .input
        .extension()
        .and_then(|value| value.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
        != Some("xlsx")
    {
        return Err(CostingError::Io {
            code: ErrorCode::UnsupportedFileType,
            message: "输入文件必须是 .xlsx 格式".to_string(),
            path: args.input.clone(),
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
        if paths_resolve_to_same_file(&args.input, output) {
            return Err(CostingError::invalid_input(
                "输入文件与输出文件不能是同一文件",
            ));
        }
        if output.exists() {
            return Err(CostingError::io(
                ErrorCode::OutputExists,
                format!("输出 workbook 已存在: {}", output.display()),
                output.clone(),
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
    let code = match error {
        CostingXlsxError::Calamine(_) => ErrorCode::FileNotReadable,
        CostingXlsxError::Message(_) => ErrorCode::InvalidInput,
    };
    CostingError::io(
        code,
        format!("读取 workbook 失败: {error}"),
        path.to_path_buf(),
    )
}

fn map_xlsx_write_error(path: &Path, error: CostingXlsxError) -> CostingError {
    CostingError::io(
        ErrorCode::OutputNotWritable,
        format!("写出 workbook 失败: {error}"),
        path.to_path_buf(),
    )
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::process;
    use std::time::{SystemTime, UNIX_EPOCH};

    use costing_core::{ErrorCode, PipelineName, RunSummary};
    use rust_xlsxwriter::{ExcelDateTime, Format, Workbook};

    use super::*;
    use crate::args::CliArgs;

    fn args(input: &str) -> CliArgs {
        CliArgs {
            pipeline: PipelineName::Gb,
            input: PathBuf::from(input),
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
            input: path.clone(),
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
    fn requires_output_for_non_check_only_runs() {
        let path = unique_temp_path(&std::env::temp_dir(), "missing-output", "xlsx");
        std::fs::write(&path, "placeholder").unwrap();
        let request = CliArgs {
            pipeline: PipelineName::Gb,
            input: path.clone(),
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
    fn rejects_existing_output_without_overwriting() {
        let input = unique_temp_path(&std::env::temp_dir(), "existing-output-input", "xlsx");
        let output = unique_temp_path(&std::env::temp_dir(), "existing-output", "xlsx");
        std::fs::write(&input, "input").unwrap();
        std::fs::write(&output, "existing").unwrap();
        let request = CliArgs {
            output: Some(output.clone()),
            ..args(input.to_str().unwrap())
        };

        let error = validate_cli_request(&request).unwrap_err();

        assert_eq!(error.code(), ErrorCode::OutputExists);
        assert_eq!(std::fs::read_to_string(&output).unwrap(), "existing");
        let _ = std::fs::remove_file(input);
        let _ = std::fs::remove_file(output);
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
    fn run_reports_actual_stage_timings_and_exact_run_counts() {
        let path = unique_temp_path(&std::env::temp_dir(), "run-reader", "xlsx");
        write_minimal_input_workbook(&path);

        let args = CliArgs {
            pipeline: PipelineName::Gb,
            input: path.clone(),
            output: None,
            month_start: None,
            month_end: None,
            check_only: true,
            benchmark: false,
        };
        let summary = run(args).unwrap();

        assert_run_counts(&summary, 1, 0, 1, 1, 1, 10);
        assert_stage_timings(&summary, false, false);
        assert_eq!(summary.sheet_count, 3);
        assert!(summary
            .quality_metrics
            .iter()
            .any(|metric| metric.metric == "可参与分析占比"));
        assert!(summary
            .error_log_preview
            .iter()
            .any(|issue| issue.issue_type == "NON_POSITIVE_UNIT_COST"));
        assert!(summary.error_log_preview.len() <= ERROR_LOG_PREVIEW_LIMIT);
        if summary.error_log_count > 0 {
            assert!(!summary.error_log_preview.is_empty());
            assert!(summary
                .error_log_preview
                .iter()
                .all(|issue| !issue.issue_type.is_empty() && !issue.field_name.is_empty()));
        }
        assert!(!summary.output_written);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn run_writes_workbook_for_non_check_only_runs() {
        let input = unique_temp_path(&std::env::temp_dir(), "run-writes-input", "xlsx");
        let output = unique_temp_path(&std::env::temp_dir(), "run-writes-output", "xlsx");
        write_minimal_input_workbook(&input);

        let args = CliArgs {
            pipeline: PipelineName::Gb,
            input: input.clone(),
            output: Some(output.clone()),
            month_start: None,
            month_end: None,
            check_only: false,
            benchmark: false,
        };
        let summary = run(args).unwrap();

        assert!(summary.output_written);
        assert_eq!(summary.workbook_path, Some(output.display().to_string()));
        assert!(output.exists());
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
            input: path.clone(),
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
            input: input.clone(),
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
            input: path.clone(),
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
            input: path.clone(),
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

use std::collections::BTreeMap;

use costing_core::fact::{build_fact_bundle, build_qty_sheet_rows};
use costing_core::normalize::{build_month_range, normalize_workbook};
use costing_core::presentation::build_workbook_payload;
use costing_core::quality::build_quality_metrics;
use costing_core::split::split_detail_and_qty;
use costing_core::{CostingError, ErrorCode, PipelineConfig, RunSummary, StageTimings};
use costing_xlsx::{
    reader::read_raw_workbook, snapshot::build_reader_snapshot, writer::write_workbook,
};

use crate::args::CliArgs;

const ERROR_LOG_PREVIEW_LIMIT: usize = 20;

pub fn run(args: CliArgs) -> anyhow::Result<RunSummary> {
    validate_cli_request(&args)?;
    let month_range = build_month_range(args.month_start.as_deref(), args.month_end.as_deref())?;
    let pipeline = PipelineConfig::for_name(args.pipeline);
    let raw = read_raw_workbook(&args.input)?;
    let snapshot = build_reader_snapshot(&raw);
    let normalized = normalize_workbook(raw, &pipeline, month_range)?;
    let split = split_detail_and_qty(normalized)?;
    let bundle = build_fact_bundle(split, &pipeline)?;
    let qty_sheet_rows = build_qty_sheet_rows(&bundle, &pipeline);
    let quality_metrics = build_quality_metrics(&bundle);
    let mut timings = StageTimings::default();
    timings.insert("ingest", 0.0);
    timings.insert("reader_rows", snapshot.row_count as f64);
    timings.insert("detail_rows", bundle.detail_fact.len() as f64);
    timings.insert("qty_rows", bundle.qty_fact.len() as f64);
    timings.insert("work_order_rows", bundle.work_order_fact.len() as f64);
    timings.insert("qty_sheet_rows", qty_sheet_rows.len() as f64);
    timings.insert("quality_metric_count", quality_metrics.len() as f64);
    let payload = build_workbook_payload(bundle, &pipeline, timings.clone())?;
    let workbook_path = args.output.as_ref().map(|path| path.display().to_string());
    if !args.check_only {
        let output = args
            .output
            .as_ref()
            .expect("validate_cli_request requires --output for non check-only runs");
        write_workbook(output, &payload)?;
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::process;
    use std::time::{SystemTime, UNIX_EPOCH};

    use costing_core::{ErrorCode, PipelineName};
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
    fn run_populates_reader_rows_from_input_workbook() {
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

        assert_eq!(summary.stage_timings.stages.get("reader_rows"), Some(&1.0));
        assert_eq!(summary.stage_timings.stages.get("detail_rows"), Some(&0.0));
        assert_eq!(summary.stage_timings.stages.get("qty_rows"), Some(&1.0));
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
        let _ = std::fs::remove_file(input);
        let _ = std::fs::remove_file(output);
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

        assert!(run(args).is_err());
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
        sheet.write_string(1, 0, "").unwrap();
        sheet.write_string(1, 1, "").unwrap();
        sheet.write_string(1, 2, "").unwrap();
        sheet.write_string(1, 3, "").unwrap();
        sheet.write_string(1, 4, "").unwrap();
        sheet.write_string(1, 5, "").unwrap();
        sheet.write_string(1, 6, "").unwrap();
        sheet.write_string(2, 0, "2025年01期").unwrap();
        sheet.write_string(2, 1, "P1").unwrap();
        sheet.write_string(2, 2, "产品").unwrap();
        sheet.write_string(2, 3, "WO-1").unwrap();
        sheet.write_string(2, 4, "1").unwrap();
        sheet.write_number(2, 5, 1).unwrap();
        sheet.write_number(2, 6, 10).unwrap();
        sheet.write_string(3, 0, "2025年02期").unwrap();
        sheet.write_string(3, 1, "P2").unwrap();
        sheet.write_string(3, 2, "产品").unwrap();
        sheet.write_string(3, 3, "WO-2").unwrap();
        sheet.write_string(3, 4, "1").unwrap();
        sheet.write_number(3, 5, 1).unwrap();
        sheet.write_number(3, 6, 10).unwrap();
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

        assert_eq!(summary.stage_timings.stages.get("reader_rows"), Some(&2.0));
        assert_eq!(summary.stage_timings.stages.get("qty_rows"), Some(&1.0));
        let _ = std::fs::remove_file(path);
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
        sheet.write_string(0, 7, "日期").unwrap();
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
        let date_format = Format::new().set_num_format("yyyy-mm-dd");
        sheet
            .write_datetime_with_format(
                2,
                7,
                ExcelDateTime::from_ymd(2025, 1, 2).unwrap(),
                &date_format,
            )
            .unwrap();
        workbook.save(path).unwrap();
    }
}

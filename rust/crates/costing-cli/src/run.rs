use costing_core::{CostingError, ErrorCode, PipelineConfig, RunSummary, StageTimings};

use crate::args::CliArgs;

pub fn run(args: CliArgs) -> anyhow::Result<RunSummary> {
    validate_cli_request(&args)?;
    let pipeline = PipelineConfig::for_name(args.pipeline);
    let output_written = !args.check_only;
    Ok(RunSummary {
        status: "succeeded".to_string(),
        pipeline: pipeline.name.as_str().to_string(),
        output_written,
        workbook_path: args.output.map(|path| path.display().to_string()),
        sheet_count: 0,
        error_log_count: 0,
        stage_timings: StageTimings::default(),
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

    fn unique_temp_path(base_dir: &std::path::Path, suffix: &str, ext: &str) -> PathBuf {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        base_dir.join(format!(
            "costing-rust-{}-pid{}-{}",
            suffix,
            process::id(),
            now
        ))
        .with_extension(ext)
    }
}

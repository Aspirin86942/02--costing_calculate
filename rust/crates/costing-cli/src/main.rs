use std::process::ExitCode;

use clap::{error::ErrorKind, Parser};
use costing_calculate::{
    application::{execute, RunOutcome, RunRequest},
    args::CliArgs,
    build_info::BuildInfo,
};
use costing_core::{model::ErrorSummary, ErrorCode};

fn main() -> ExitCode {
    if standalone_version_json_requested() {
        return emit_json(&BuildInfo::current());
    }
    let args = match CliArgs::try_parse() {
        Ok(args) => args,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
            print!("{error}");
            return ExitCode::SUCCESS;
        }
        Err(error) => {
            return emit_error(ErrorSummary {
                status: "failed".to_string(),
                code: ErrorCode::InvalidInput,
                message: error.to_string(),
                retryable: false,
                request_id: None,
                details: None,
            });
        }
    };
    if args.version_json {
        return emit_json(&BuildInfo::current());
    }
    match execute(RunRequest::from(args)) {
        RunOutcome::Succeeded(summary) => emit_json(&summary),
        RunOutcome::Failed(failure) => emit_error(failure),
    }
}

fn standalone_version_json_requested() -> bool {
    let mut arguments = std::env::args_os().skip(1);
    matches!(arguments.next(), Some(argument) if argument == "--version-json")
        && arguments.next().is_none()
}

fn emit_json(value: &impl serde::Serialize) -> ExitCode {
    println!(
        "{}",
        serde_json::to_string_pretty(value).expect("serialize successful output")
    );
    ExitCode::SUCCESS
}

fn emit_error(error_summary: ErrorSummary) -> ExitCode {
    eprintln!(
        "{}",
        serde_json::to_string_pretty(&error_summary).expect("serialize error summary")
    );
    ExitCode::FAILURE
}

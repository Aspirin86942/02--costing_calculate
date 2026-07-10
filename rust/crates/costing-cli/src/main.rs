mod args;
mod run;

use std::process::ExitCode;

use clap::{error::ErrorKind, Parser};
use costing_core::{model::ErrorSummary, CostingError, ErrorCode};

use args::CliArgs;

fn main() -> ExitCode {
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
            });
        }
    };
    match run::run(args) {
        Ok(summary) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&summary).expect("serialize run summary")
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            let error_summary = error
                .downcast_ref::<CostingError>()
                .map(|cause| ErrorSummary {
                    status: "failed".to_string(),
                    code: cause.code(),
                    message: cause.message().to_string(),
                    retryable: cause.retryable(),
                })
                .unwrap_or_else(|| ErrorSummary {
                    status: "failed".to_string(),
                    code: ErrorCode::InternalError,
                    message: error.to_string(),
                    retryable: false,
                });
            emit_error(error_summary)
        }
    }
}

fn emit_error(error_summary: ErrorSummary) -> ExitCode {
    eprintln!(
        "{}",
        serde_json::to_string_pretty(&error_summary).expect("serialize error summary")
    );
    ExitCode::FAILURE
}

use std::path::PathBuf;

use costing_core::PipelineName;

use crate::args::CliArgs;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOperation {
    Execute,
    ValidateConfig,
    PrintEffectiveConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRequest {
    pub pipeline: PipelineName,
    pub input: Option<PathBuf>,
    pub output: Option<PathBuf>,
    pub month_start: Option<String>,
    pub month_end: Option<String>,
    pub check_only: bool,
    pub benchmark: bool,
    pub config: Option<PathBuf>,
    pub operation: RunOperation,
}

impl From<CliArgs> for RunRequest {
    fn from(args: CliArgs) -> Self {
        debug_assert!(!args.version_json, "version-json is handled before execute");
        let operation = if args.validate_config {
            RunOperation::ValidateConfig
        } else if args.print_effective_config {
            RunOperation::PrintEffectiveConfig
        } else {
            RunOperation::Execute
        };
        Self {
            pipeline: args.pipeline,
            input: args.input,
            output: args.output,
            month_start: args.month_start,
            month_end: args.month_end,
            check_only: args.check_only,
            benchmark: args.benchmark,
            config: args.config,
            operation,
        }
    }
}

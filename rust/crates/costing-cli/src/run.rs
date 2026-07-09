use costing_core::{PipelineConfig, RunSummary, StageTimings};

use crate::args::CliArgs;

pub fn run(args: CliArgs) -> anyhow::Result<RunSummary> {
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

use std::path::PathBuf;

use clap::Parser;
use costing_core::PipelineName;

#[derive(Debug, Parser)]
#[command(name = "costing-calculate", about = "成本核算 ETL Rust CLI")]
pub struct CliArgs {
    pub pipeline: PipelineName,
    #[arg(long)]
    pub input: PathBuf,
    #[arg(long)]
    pub output: Option<PathBuf>,
    #[arg(long)]
    pub month_start: Option<String>,
    #[arg(long)]
    pub month_end: Option<String>,
    #[arg(long)]
    pub check_only: bool,
    #[arg(long)]
    pub benchmark: bool,
}

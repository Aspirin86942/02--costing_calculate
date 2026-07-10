use std::path::PathBuf;

use clap::Parser;
use costing_core::PipelineName;

#[derive(Debug, Parser)]
#[command(name = "costing-calculate", about = "成本核算 ETL Rust CLI")]
pub struct CliArgs {
    pub pipeline: PipelineName,
    #[arg(
        long,
        help = "输入 workbook；省略时自动查找 data/raw/<pipeline>/<pipeline>-*.xlsx"
    )]
    pub input: Option<PathBuf>,
    #[arg(
        long,
        help = "输出 workbook；非 check-only 省略时写入 data/processed/<pipeline>/*_处理后.xlsx"
    )]
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

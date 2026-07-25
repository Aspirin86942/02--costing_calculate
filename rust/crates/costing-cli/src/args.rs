use std::path::PathBuf;

use clap::Parser;
use costing_core::PipelineName;

#[derive(Debug, Parser)]
#[command(name = "costing-calculate", version, about = "成本核算 ETL Rust CLI")]
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
    #[arg(
        long,
        value_name = "PATH",
        conflicts_with_all = ["validate_config", "print_effective_config", "version_json"],
        help = "原子写出版本化 RunManifestV1；默认拒绝覆盖"
    )]
    pub summary_output: Option<PathBuf>,
    #[arg(
        long,
        conflicts_with = "version_json",
        help = "当前目录内路径显示为相对路径，目录外路径仅显示文件名"
    )]
    pub redact_paths: bool,
    #[arg(
        long,
        value_name = "PATH",
        help = "完整的 v1 TOML 配置；省略时使用内置默认配置"
    )]
    pub config: Option<PathBuf>,
    #[arg(
        long,
        conflicts_with = "print_effective_config",
        conflicts_with = "version_json",
        conflicts_with_all = [
            "input",
            "output",
            "month_start",
            "month_end",
            "check_only",
            "benchmark",
            "summary_output"
        ],
        help = "仅加载并严格校验配置，不读取 workbook"
    )]
    pub validate_config: bool,
    #[arg(
        long,
        conflicts_with = "validate_config",
        conflicts_with = "version_json",
        conflicts_with_all = [
            "input",
            "output",
            "month_start",
            "month_end",
            "check_only",
            "benchmark",
            "summary_output"
        ],
        help = "输出完整有效配置、字段来源和配置哈希，不运行 ETL"
    )]
    pub print_effective_config: bool,
    #[arg(
        long,
        conflicts_with_all = [
            "input",
            "output",
            "month_start",
            "month_end",
            "check_only",
            "benchmark",
            "summary_output",
            "redact_paths",
            "config",
            "validate_config",
            "print_effective_config"
        ],
        help = "输出确定性的构建与 schema 版本 JSON，不运行 ETL"
    )]
    pub version_json: bool,
}

# Full Rust Costing Rewrite Implementation Plan

> Compatibility note (2026-07-10): this is a historical implementation plan. Python oracle commands were updated from the retired conda environment to the current uv-managed `.venv` so validation steps remain runnable; implementation status belongs in current validation docs.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Rust CLI that fully replaces the current Python default costing ETL path for GB and SK, directly reads raw `.xlsx`, writes the current 3-sheet workbook, and validates against Python as a migration oracle.

**Architecture:** Add a `rust/` Cargo workspace with separate crates for CLI orchestration, core ETL/analytics, XLSX I/O, and oracle test support. Keep Python production code in place during implementation, but use it only as the oracle for contract, fixture, real-sample, and benchmark comparisons.

**Tech Stack:** Rust 2021, `clap`, `serde`, `serde_json`, `thiserror`, `anyhow`, `rust_decimal`, `chrono`, `calamine`, `rust_xlsxwriter`, `csv`; existing Python oracle commands use project `uv run` / `.venv`.

## Global Constraints

- Final route is one-shot full Rust rewrite, not a sidecar and not a Python shell.
- First version covers both `gb` and `sk`.
- Rust CLI is the final primary entrypoint.
- Rust must directly read raw Kingdee `.xlsx`; Python-generated CSV, Parquet, or manifest is not allowed as production input.
- Python remains only as a migration oracle until Rust is validated.
- Default workbook has exactly 3 sheets: `成本计算单总表`, `成本计算单数量聚合维度`, `成本分析工单维度`.
- `成本分析产品维度` must not be implemented in the Rust system and must be rejected by writer and validator code.
- Do not delete Python before Rust GB + SK validation passes and a separate retirement change is approved.
- Monetary, quantity, unit-cost, and score calculations must avoid binary float equality; use `rust_decimal::Decimal` for business values and explicit tolerances for validation.
- Keep unrelated untracked files out of every commit: `docs/2026-07-09-work-order-anomaly-architecture-fast-writer-spec.md`, `docs/superpowers/plans/2026-07-09-three-sheet-workbook-phase0.md`. Keep `uv.lock` unchanged unless dependency resolution changes.
- Use `cargo` commands for Rust checks and `uv run` for Python oracle/tests after `uv sync --extra dev`.
- Each task should be committed independently after its verification passes.

---

## File Structure

Create:

- `rust/Cargo.toml` - workspace members and shared dependency versions.
- `rust/rust-toolchain.toml` - stable Rust toolchain pin for repeatable local builds.
- `rust/crates/costing-cli/Cargo.toml` - binary crate dependencies.
- `rust/crates/costing-cli/src/main.rs` - CLI entrypoint and process exit mapping.
- `rust/crates/costing-cli/src/args.rs` - `clap` argument definitions.
- `rust/crates/costing-cli/src/run.rs` - orchestration from args to run summary.
- `rust/crates/costing-core/Cargo.toml` - core domain dependencies.
- `rust/crates/costing-core/src/lib.rs` - module exports.
- `rust/crates/costing-core/src/error.rs` - typed error codes and retryability.
- `rust/crates/costing-core/src/model.rs` - `CellValue`, rows, sheets, payloads, summaries.
- `rust/crates/costing-core/src/pipeline.rs` - GB/SK config, product order, standalone cost items.
- `rust/crates/costing-core/src/timing.rs` - stage timing helper.
- `rust/crates/costing-core/src/normalize.rs` - header flattening, cell normalization, month filtering, fill rules.
- `rust/crates/costing-core/src/split.rs` - detail and quantity split.
- `rust/crates/costing-core/src/fact.rs` - fact bundle and quantity aggregation.
- `rust/crates/costing-core/src/quality.rs` - quality metrics and error issue rows.
- `rust/crates/costing-core/src/scoring.rs` - weighted median/MAD and Modified Z-score grading.
- `rust/crates/costing-core/src/anomaly.rs` - work-order anomaly sheet.
- `rust/crates/costing-core/src/presentation.rs` - three `SheetModel`s and product-dimension rejection.
- `rust/crates/costing-xlsx/Cargo.toml` - XLSX I/O dependencies.
- `rust/crates/costing-xlsx/src/lib.rs` - module exports.
- `rust/crates/costing-xlsx/src/reader.rs` - raw workbook reader using `calamine`.
- `rust/crates/costing-xlsx/src/writer.rs` - 3-sheet writer using `rust_xlsxwriter`.
- `rust/crates/costing-xlsx/src/snapshot.rs` - reader/workbook semantic snapshots for oracle comparison.
- `rust/crates/costing-oracle-tests/Cargo.toml` - Rust-side helpers for integration tests.
- `rust/crates/costing-oracle-tests/src/lib.rs` - oracle comparison structs and helpers.
- `tests/rust_oracle/__init__.py` - Python package marker.
- `tests/rust_oracle/oracle_runner.py` - runs Python oracle and Rust CLI in pytest.
- `tests/rust_oracle/workbook_compare.py` - final workbook comparison with openpyxl normalization.
- `tests/test_full_rust_cli_oracle.py` - Python integration tests around Rust CLI.
- `docs/rust_rewrite_validation.md` - validation commands, fixture strategy, and manual Excel/WPS checklist.

Modify:

- `README.md` - add Rust CLI section only after the CLI reaches final validation in Task 12.
- `AGENTS.md` - update current business rule line from Python default entrypoint to Rust CLI only after Task 12.
- `tests/contracts/README.md` - document Rust oracle contract comparison after Task 10.

Do not modify in this plan:

- Existing Python production path behavior before final validation.
- Legacy product-dimension Python helper code before a separate retirement change.
- Existing sidecar spike files, except for reading them as reference.

---

### Task 1: Rust Workspace, Shared Types, and CLI Skeleton

**Files:**
- Create: `rust/Cargo.toml`
- Create: `rust/rust-toolchain.toml`
- Create: `rust/crates/costing-core/Cargo.toml`
- Create: `rust/crates/costing-core/src/lib.rs`
- Create: `rust/crates/costing-core/src/error.rs`
- Create: `rust/crates/costing-core/src/model.rs`
- Create: `rust/crates/costing-core/src/pipeline.rs`
- Create: `rust/crates/costing-core/src/timing.rs`
- Create: `rust/crates/costing-cli/Cargo.toml`
- Create: `rust/crates/costing-cli/src/main.rs`
- Create: `rust/crates/costing-cli/src/args.rs`
- Create: `rust/crates/costing-cli/src/run.rs`

**Interfaces:**
- Produces: `PipelineName`, `PipelineConfig`, `CostingError`, `ErrorCode`, `RunSummary`, `StageTimings`, `CliArgs`.
- Produces: binary command `costing-calculate <gb|sk> --input <xlsx> --output <xlsx> [--check-only] [--benchmark]`.
- Consumes: no earlier Rust code.

- [ ] **Step 1: Add the workspace manifest**

Create `rust/Cargo.toml`:

```toml
[workspace]
members = [
    "crates/costing-cli",
    "crates/costing-core",
]
resolver = "2"

[workspace.package]
edition = "2021"
version = "0.1.0"

[workspace.dependencies]
anyhow = "1"
chrono = { version = "0.4", default-features = false, features = ["clock", "std"] }
clap = { version = "4", features = ["derive"] }
rust_decimal = { version = "1", features = ["serde"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "1"
```

Create `rust/rust-toolchain.toml`:

```toml
[toolchain]
channel = "stable"
```

- [ ] **Step 2: Add core crate metadata**

Create `rust/crates/costing-core/Cargo.toml`:

```toml
[package]
name = "costing-core"
version.workspace = true
edition.workspace = true

[dependencies]
chrono.workspace = true
rust_decimal.workspace = true
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
```

- [ ] **Step 3: Add core exports**

Create `rust/crates/costing-core/src/lib.rs`:

```rust
pub mod error;
pub mod model;
pub mod pipeline;
pub mod timing;

pub use error::{CostingError, ErrorCode};
pub use model::{RunSummary, StageTimings};
pub use pipeline::{PipelineConfig, PipelineName};
```

- [ ] **Step 4: Add typed errors**

Create `rust/crates/costing-core/src/error.rs`:

```rust
use std::path::PathBuf;

use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    InvalidInput,
    FileNotFound,
    FileNotReadable,
    UnsupportedFileType,
    OutputExists,
    ReaderMismatch,
    EtlMismatch,
    AnalysisMismatch,
    WorkbookMismatch,
    PerformanceRegression,
    InternalError,
}

#[derive(Debug, Error)]
pub enum CostingError {
    #[error("{message}")]
    User {
        code: ErrorCode,
        message: String,
        retryable: bool,
    },
    #[error("{message}")]
    Io {
        code: ErrorCode,
        message: String,
        path: PathBuf,
        retryable: bool,
    },
    #[error("{message}")]
    Internal {
        code: ErrorCode,
        message: String,
    },
}

impl CostingError {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::User { code, .. } | Self::Io { code, .. } | Self::Internal { code, .. } => *code,
        }
    }

    pub fn retryable(&self) -> bool {
        match self {
            Self::User { retryable, .. } | Self::Io { retryable, .. } => *retryable,
            Self::Internal { .. } => false,
        }
    }

    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::User {
            code: ErrorCode::InvalidInput,
            message: message.into(),
            retryable: false,
        }
    }
}
```

- [ ] **Step 5: Add shared models and summary output**

Create `rust/crates/costing-core/src/model.rs`:

```rust
use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value")]
pub enum CellValue {
    Blank,
    Text(String),
    Decimal(Decimal),
    DateLike(String),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SheetModel {
    pub sheet_name: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<CellValue>>,
    pub column_types: BTreeMap<String, String>,
    pub number_formats: BTreeMap<String, String>,
    pub freeze_panes: Option<String>,
    pub auto_filter: bool,
    pub fixed_width: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct StageTimings {
    pub stages: BTreeMap<String, f64>,
}

impl StageTimings {
    pub fn insert(&mut self, stage: impl Into<String>, seconds: f64) {
        self.stages.insert(stage.into(), seconds);
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QualityMetric {
    pub category: String,
    pub metric: String,
    pub value: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WorkbookPayload {
    pub sheet_models: Vec<SheetModel>,
    pub quality_metrics: Vec<QualityMetric>,
    pub error_log_count: usize,
    pub stage_timings: StageTimings,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RunSummary {
    pub status: String,
    pub pipeline: String,
    pub output_written: bool,
    pub workbook_path: Option<String>,
    pub sheet_count: usize,
    pub error_log_count: usize,
    pub stage_timings: StageTimings,
}
```

- [ ] **Step 6: Add GB/SK pipeline config**

Create `rust/crates/costing-core/src/pipeline.rs`:

```rust
use std::str::FromStr;

use serde::Serialize;

use crate::error::CostingError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum PipelineName {
    Gb,
    Sk,
}

impl PipelineName {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Gb => "gb",
            Self::Sk => "sk",
        }
    }
}

impl FromStr for PipelineName {
    type Err = CostingError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "gb" => Ok(Self::Gb),
            "sk" => Ok(Self::Sk),
            other => Err(CostingError::invalid_input(format!("未知管线: {other}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PipelineConfig {
    pub name: PipelineName,
    pub product_order: &'static [(&'static str, &'static str)],
    pub standalone_cost_items: &'static [&'static str],
}

pub const GB_PRODUCT_ORDER: &[(&str, &str)] = &[
    ("GB_C.D.B0048AA", "BMS-400W驱动器"),
    ("GB_C.D.B0040AA", "BMS-750W驱动器"),
    ("GB_C.D.B0041AA", "BMS-1100W驱动器"),
    ("GB_C.D.B0042AA", "BMS-1700W驱动器"),
    ("GB_C.D.B0043AA", "BMS-2400W驱动器"),
    ("GB_C.D.B0044AA", "BMS-3900W驱动器"),
    ("GB_C.D.B0045AA", "BMS-5900W驱动器"),
    ("GB_C.D.B0046AA", "BMS-7500W驱动器"),
];

pub const SK_PRODUCT_ORDER: &[(&str, &str)] = &[
    ("DP.C.P0197AA", "动力线"),
    ("DP.C.P0201AA", "动力线"),
    ("DP.C.P0198AA", "动力线"),
    ("DP.C.P0199AA", "动力线"),
    ("DP.C.P0257AA", "动力线"),
    ("DP.C.P0200AA", "动力线"),
    ("DP.C.P0246AA", "动力抱闸线"),
    ("DP.C.P0252AA", "动力线"),
];

impl PipelineConfig {
    pub fn for_name(name: PipelineName) -> Self {
        match name {
            PipelineName::Gb => Self {
                name,
                product_order: GB_PRODUCT_ORDER,
                standalone_cost_items: &["委外加工费"],
            },
            PipelineName::Sk => Self {
                name,
                product_order: SK_PRODUCT_ORDER,
                standalone_cost_items: &["委外加工费", "软件费用"],
            },
        }
    }
}
```

- [ ] **Step 7: Add timing helper**

Create `rust/crates/costing-core/src/timing.rs`:

```rust
use std::time::Instant;

use crate::model::StageTimings;

pub fn measure<T, E>(
    timings: &mut StageTimings,
    stage: &'static str,
    f: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    let started = Instant::now();
    let result = f();
    timings.insert(stage, started.elapsed().as_secs_f64());
    result
}
```

- [ ] **Step 8: Add CLI crate and args**

Create `rust/crates/costing-cli/Cargo.toml`:

```toml
[package]
name = "costing-calculate"
version.workspace = true
edition.workspace = true

[dependencies]
anyhow.workspace = true
clap.workspace = true
costing-core = { path = "../costing-core" }
serde_json.workspace = true
```

Create `rust/crates/costing-cli/src/args.rs`:

```rust
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
```

Create `rust/crates/costing-cli/src/main.rs`:

```rust
mod args;
mod run;

use std::process::ExitCode;

use clap::Parser;

use args::CliArgs;

fn main() -> ExitCode {
    let args = CliArgs::parse();
    match run::run(args) {
        Ok(summary) => {
            println!("{}", serde_json::to_string_pretty(&summary).expect("serialize run summary"));
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{}", error);
            ExitCode::FAILURE
        }
    }
}
```

Create `rust/crates/costing-cli/src/run.rs`:

```rust
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
```

- [ ] **Step 9: Write CLI skeleton tests**

Append to `rust/crates/costing-core/src/pipeline.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gb_and_sk_configs_match_python_contract() {
        let gb = PipelineConfig::for_name(PipelineName::Gb);
        assert_eq!(gb.standalone_cost_items, ["委外加工费"]);
        assert_eq!(gb.product_order[0], ("GB_C.D.B0048AA", "BMS-400W驱动器"));

        let sk = PipelineConfig::for_name(PipelineName::Sk);
        assert_eq!(sk.standalone_cost_items, ["委外加工费", "软件费用"]);
        assert_eq!(sk.product_order[0], ("DP.C.P0197AA", "动力线"));
    }
}
```

- [ ] **Step 10: Run verification**

Run:

```powershell
cargo test --manifest-path rust/Cargo.toml
cargo run --manifest-path rust/Cargo.toml -p costing-calculate -- gb --input data/raw/gb/missing.xlsx --check-only
```

Expected:

- `cargo test` passes.
- CLI command exits `0` for the skeleton only because input validation is introduced in Task 2.
- JSON contains `"pipeline": "gb"` and `"output_written": false`.

- [ ] **Step 11: Commit**

```powershell
git add -- rust
git commit -m "feat(rust): scaffold costing cli workspace"
```

---

### Task 2: CLI Validation, Summary Shape, and Error Model

**Files:**
- Modify: `rust/crates/costing-cli/src/run.rs`
- Modify: `rust/crates/costing-core/src/error.rs`
- Modify: `rust/crates/costing-core/src/model.rs`
- Test: `rust/crates/costing-cli/src/run.rs`

**Interfaces:**
- Consumes: `CliArgs`, `PipelineConfig`, `CostingError`.
- Produces: `validate_cli_request(args: &CliArgs) -> Result<(), CostingError>`.
- Produces: stable error JSON fields `code`, `message`, `retryable`.

- [ ] **Step 1: Write failing validation tests**

Add to `rust/crates/costing-cli/src/run.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::path::PathBuf;

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
        let path = temp_dir.join("costing-rust-not-xlsx.txt");
        std::fs::write(&path, "not xlsx").unwrap();
        let error = validate_cli_request(&args(path.to_str().unwrap())).unwrap_err();
        assert_eq!(error.code(), ErrorCode::UnsupportedFileType);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn check_only_does_not_require_output_path() {
        let path = std::env::temp_dir().join("costing-rust-input.xlsx");
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
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```powershell
cargo test --manifest-path rust/Cargo.toml -p costing-calculate
```

Expected: FAIL because `validate_cli_request` is not defined.

- [ ] **Step 3: Implement validation**

Replace `rust/crates/costing-cli/src/run.rs` with:

```rust
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
    if args.input.extension().and_then(|value| value.to_str()).map(str::to_ascii_lowercase).as_deref() != Some("xlsx") {
        return Err(CostingError::Io {
            code: ErrorCode::UnsupportedFileType,
            message: "输入文件必须是 .xlsx 格式".to_string(),
            path: args.input.clone(),
            retryable: false,
        });
    }
    if !args.check_only && args.output.is_none() {
        return Err(CostingError::invalid_input("非 check-only 运行必须提供 --output"));
    }
    Ok(())
}
```

- [ ] **Step 4: Improve CLI error output**

Modify `rust/crates/costing-cli/src/main.rs`:

```rust
mod args;
mod run;

use std::process::ExitCode;

use clap::Parser;
use serde_json::json;

use args::CliArgs;

fn main() -> ExitCode {
    let args = CliArgs::parse();
    match run::run(args) {
        Ok(summary) => {
            println!("{}", serde_json::to_string_pretty(&summary).expect("serialize run summary"));
            ExitCode::SUCCESS
        }
        Err(error) => {
            let message = error.to_string();
            eprintln!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "status": "failed",
                    "message": message,
                    "retryable": false,
                }))
                .expect("serialize error")
            );
            ExitCode::FAILURE
        }
    }
}
```

- [ ] **Step 5: Run verification**

Run:

```powershell
cargo test --manifest-path rust/Cargo.toml
cargo run --manifest-path rust/Cargo.toml -p costing-calculate -- gb --input missing.xlsx --check-only
```

Expected:

- `cargo test` passes.
- CLI exits non-zero for `missing.xlsx`.
- stderr JSON contains `"status": "failed"` and `输入文件不存在`.

- [ ] **Step 6: Commit**

```powershell
git add -- rust
git commit -m "feat(rust): validate costing cli inputs"
```

---

### Task 3: XLSX Reader Crate and Reader Snapshot

**Files:**
- Modify: `rust/Cargo.toml`
- Create: `rust/crates/costing-xlsx/Cargo.toml`
- Create: `rust/crates/costing-xlsx/src/lib.rs`
- Create: `rust/crates/costing-xlsx/src/reader.rs`
- Create: `rust/crates/costing-xlsx/src/snapshot.rs`
- Modify: `rust/crates/costing-core/src/model.rs`
- Modify: `rust/crates/costing-cli/Cargo.toml`
- Modify: `rust/crates/costing-cli/src/run.rs`

**Interfaces:**
- Consumes: valid `.xlsx` path from CLI.
- Produces: `read_raw_workbook(path: &Path) -> Result<RawWorkbook, CostingXlsxError>`.
- Produces: `ReaderSnapshot { sheet_name, row_count, column_count, headers, null_counts }`.

- [ ] **Step 1: Add XLSX crate to workspace**

Modify `rust/Cargo.toml`:

```toml
[workspace]
members = [
    "crates/costing-cli",
    "crates/costing-core",
    "crates/costing-xlsx",
]
resolver = "2"

[workspace.package]
edition = "2021"
version = "0.1.0"

[workspace.dependencies]
anyhow = "1"
calamine = "0.26"
chrono = { version = "0.4", default-features = false, features = ["clock", "std"] }
clap = { version = "4", features = ["derive"] }
rust_decimal = { version = "1", features = ["serde"] }
rust_xlsxwriter = "0"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "1"
```

Create `rust/crates/costing-xlsx/Cargo.toml`:

```toml
[package]
name = "costing-xlsx"
version.workspace = true
edition.workspace = true

[dependencies]
calamine.workspace = true
costing-core = { path = "../costing-core" }
rust_decimal.workspace = true
rust_xlsxwriter.workspace = true
serde.workspace = true
thiserror.workspace = true

[dev-dependencies]
rust_xlsxwriter.workspace = true
```

- [ ] **Step 2: Add raw workbook models**

Append to `rust/crates/costing-core/src/model.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RawWorkbook {
    pub sheet_name: String,
    pub header_rows: [Vec<String>; 2],
    pub rows: Vec<Vec<CellValue>>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReaderSnapshot {
    pub sheet_name: String,
    pub row_count: usize,
    pub column_count: usize,
    pub headers: Vec<String>,
    pub null_counts: BTreeMap<String, usize>,
}
```

- [ ] **Step 3: Write failing reader test**

Create `rust/crates/costing-xlsx/src/lib.rs`:

```rust
pub mod reader;
pub mod snapshot;
```

Create `rust/crates/costing-xlsx/src/reader.rs` with tests first:

```rust
use std::path::Path;

use costing_core::model::{CellValue, RawWorkbook};

#[derive(Debug, thiserror::Error)]
pub enum CostingXlsxError {
    #[error("{0}")]
    Message(String),
}

pub fn read_raw_workbook(_path: &Path) -> Result<RawWorkbook, CostingXlsxError> {
    Err(CostingXlsxError::Message("reader not implemented".to_string()))
}

#[cfg(test)]
mod tests {
    use rust_xlsxwriter::Workbook;

    use super::*;

    #[test]
    fn reads_two_header_rows_and_data_values() {
        let path = std::env::temp_dir().join("costing-reader-two-headers.xlsx");
        let mut workbook = Workbook::new();
        let worksheet = workbook.add_worksheet();
        worksheet.set_name("成本计算单").unwrap();
        worksheet.write_string(0, 0, "年期").unwrap();
        worksheet.write_string(0, 1, "产品").unwrap();
        worksheet.write_string(1, 0, "").unwrap();
        worksheet.write_string(1, 1, "产品编码").unwrap();
        worksheet.write_string(2, 0, "2025年01期").unwrap();
        worksheet.write_string(2, 1, "GB_C.D.B0040AA").unwrap();
        workbook.save(&path).unwrap();

        let raw = read_raw_workbook(&path).unwrap();

        assert_eq!(raw.sheet_name, "成本计算单");
        assert_eq!(raw.header_rows[0][0], "年期");
        assert_eq!(raw.header_rows[1][1], "产品编码");
        assert_eq!(raw.rows[0][0], CellValue::Text("2025年01期".to_string()));
        assert_eq!(raw.rows[0][1], CellValue::Text("GB_C.D.B0040AA".to_string()));
        let _ = std::fs::remove_file(path);
    }
}
```

- [ ] **Step 4: Run test and verify it fails**

Run:

```powershell
cargo test --manifest-path rust/Cargo.toml -p costing-xlsx reader::tests::reads_two_header_rows_and_data_values
```

Expected: FAIL with `reader not implemented`.

- [ ] **Step 5: Implement calamine reader**

Replace `rust/crates/costing-xlsx/src/reader.rs`:

```rust
use std::path::Path;

use calamine::{open_workbook_auto, Data, Reader};
use costing_core::model::{CellValue, RawWorkbook};
use rust_decimal::Decimal;

#[derive(Debug, thiserror::Error)]
pub enum CostingXlsxError {
    #[error("{0}")]
    Message(String),
    #[error("xlsx read error: {0}")]
    Calamine(#[from] calamine::Error),
}

pub fn read_raw_workbook(path: &Path) -> Result<RawWorkbook, CostingXlsxError> {
    let mut workbook = open_workbook_auto(path)?;
    let sheet_name = workbook
        .sheet_names()
        .first()
        .cloned()
        .ok_or_else(|| CostingXlsxError::Message("workbook has no sheets".to_string()))?;
    let range = workbook.worksheet_range(&sheet_name)?;
    let rows: Vec<Vec<Data>> = range.rows().map(|row| row.to_vec()).collect();
    if rows.len() < 2 {
        return Err(CostingXlsxError::Message("workbook must contain two header rows".to_string()));
    }
    let max_width = rows.iter().map(Vec::len).max().unwrap_or(0);
    let header_rows = [
        normalize_header_row(rows.first().unwrap(), max_width),
        normalize_header_row(rows.get(1).unwrap(), max_width),
    ];
    let data_rows = rows
        .iter()
        .skip(2)
        .map(|row| normalize_data_row(row, max_width))
        .collect();
    Ok(RawWorkbook {
        sheet_name,
        header_rows,
        rows: data_rows,
    })
}

fn normalize_header_row(row: &[Data], width: usize) -> Vec<String> {
    (0..width)
        .map(|idx| match row.get(idx).unwrap_or(&Data::Empty) {
            Data::String(value) => value.trim().to_string(),
            Data::Float(value) => trim_numeric_string(*value),
            Data::Int(value) => value.to_string(),
            Data::Bool(value) => value.to_string(),
            Data::DateTime(value) => value.to_string(),
            Data::DateTimeIso(value) => value.trim().to_string(),
            Data::DurationIso(value) => value.trim().to_string(),
            Data::Error(value) => format!("{value:?}"),
            Data::Empty => String::new(),
        })
        .collect()
}

fn normalize_data_row(row: &[Data], width: usize) -> Vec<CellValue> {
    (0..width)
        .map(|idx| match row.get(idx).unwrap_or(&Data::Empty) {
            Data::Empty => CellValue::Blank,
            Data::String(value) => {
                let text = value.trim().to_string();
                if text.is_empty() { CellValue::Blank } else { CellValue::Text(text) }
            }
            Data::Float(value) => Decimal::from_f64_retain(*value)
                .map(CellValue::Decimal)
                .unwrap_or_else(|| CellValue::Text(value.to_string())),
            Data::Int(value) => CellValue::Decimal(Decimal::from(*value)),
            Data::Bool(value) => CellValue::Text(value.to_string()),
            Data::DateTime(value) => CellValue::DateLike(value.to_string()),
            Data::DateTimeIso(value) => CellValue::DateLike(value.clone()),
            Data::DurationIso(value) => CellValue::Text(value.clone()),
            Data::Error(value) => CellValue::Text(format!("{value:?}")),
        })
        .collect()
}

fn trim_numeric_string(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use rust_xlsxwriter::Workbook;

    use super::*;

    #[test]
    fn reads_two_header_rows_and_data_values() {
        let path = std::env::temp_dir().join("costing-reader-two-headers.xlsx");
        let mut workbook = Workbook::new();
        let worksheet = workbook.add_worksheet();
        worksheet.set_name("成本计算单").unwrap();
        worksheet.write_string(0, 0, "年期").unwrap();
        worksheet.write_string(0, 1, "产品").unwrap();
        worksheet.write_string(1, 0, "").unwrap();
        worksheet.write_string(1, 1, "产品编码").unwrap();
        worksheet.write_string(2, 0, "2025年01期").unwrap();
        worksheet.write_string(2, 1, "GB_C.D.B0040AA").unwrap();
        workbook.save(&path).unwrap();

        let raw = read_raw_workbook(&path).unwrap();

        assert_eq!(raw.sheet_name, "成本计算单");
        assert_eq!(raw.header_rows[0][0], "年期");
        assert_eq!(raw.header_rows[1][1], "产品编码");
        assert_eq!(raw.rows[0][0], CellValue::Text("2025年01期".to_string()));
        assert_eq!(raw.rows[0][1], CellValue::Text("GB_C.D.B0040AA".to_string()));
        let _ = std::fs::remove_file(path);
    }
}
```

- [ ] **Step 6: Add reader snapshot**

Create `rust/crates/costing-xlsx/src/snapshot.rs`:

```rust
use std::collections::BTreeMap;

use costing_core::model::{CellValue, RawWorkbook, ReaderSnapshot};

pub fn build_reader_snapshot(raw: &RawWorkbook) -> ReaderSnapshot {
    let headers = flatten_header_rows(&raw.header_rows);
    let mut null_counts = BTreeMap::new();
    for (idx, header) in headers.iter().enumerate() {
        let count = raw
            .rows
            .iter()
            .filter(|row| matches!(row.get(idx), None | Some(CellValue::Blank)))
            .count();
        null_counts.insert(header.clone(), count);
    }
    ReaderSnapshot {
        sheet_name: raw.sheet_name.clone(),
        row_count: raw.rows.len(),
        column_count: headers.len(),
        headers,
        null_counts,
    }
}

pub fn flatten_header_rows(header_rows: &[Vec<String>; 2]) -> Vec<String> {
    let width = header_rows[0].len().max(header_rows[1].len());
    (0..width)
        .map(|idx| {
            let top = header_rows[0].get(idx).map(String::as_str).unwrap_or("").trim();
            let bottom = header_rows[1].get(idx).map(String::as_str).unwrap_or("").trim();
            match (top.is_empty(), bottom.is_empty()) {
                (true, true) => format!("column_{idx}"),
                (false, true) => top.to_string(),
                (true, false) => bottom.to_string(),
                (false, false) if top == bottom => top.to_string(),
                (false, false) => bottom.to_string(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use costing_core::model::{CellValue, RawWorkbook};

    use super::*;

    #[test]
    fn snapshot_counts_blank_cells_by_flattened_header() {
        let raw = RawWorkbook {
            sheet_name: "成本计算单".to_string(),
            header_rows: [
                vec!["产品".to_string(), "金额".to_string()],
                vec!["产品编码".to_string(), "".to_string()],
            ],
            rows: vec![
                vec![CellValue::Text("A".to_string()), CellValue::Blank],
                vec![CellValue::Blank, CellValue::Decimal("10".parse().unwrap())],
            ],
        };
        let snapshot = build_reader_snapshot(&raw);
        assert_eq!(snapshot.headers, vec!["产品编码", "金额"]);
        assert_eq!(snapshot.null_counts["产品编码"], 1);
        assert_eq!(snapshot.null_counts["金额"], 1);
    }
}
```

- [ ] **Step 7: Wire reader into CLI check-only path**

Modify `rust/crates/costing-cli/Cargo.toml`:

```toml
[package]
name = "costing-calculate"
version.workspace = true
edition.workspace = true

[dependencies]
anyhow.workspace = true
clap.workspace = true
costing-core = { path = "../costing-core" }
costing-xlsx = { path = "../costing-xlsx" }
serde_json.workspace = true
```

Modify `rust/crates/costing-cli/src/run.rs` to call reader after validation:

```rust
use costing_core::{CostingError, ErrorCode, PipelineConfig, RunSummary, StageTimings};
use costing_xlsx::{reader::read_raw_workbook, snapshot::build_reader_snapshot};

use crate::args::CliArgs;

pub fn run(args: CliArgs) -> anyhow::Result<RunSummary> {
    validate_cli_request(&args)?;
    let pipeline = PipelineConfig::for_name(args.pipeline);
    let raw = read_raw_workbook(&args.input)?;
    let snapshot = build_reader_snapshot(&raw);
    let output_written = !args.check_only;
    Ok(RunSummary {
        status: "succeeded".to_string(),
        pipeline: pipeline.name.as_str().to_string(),
        output_written,
        workbook_path: args.output.map(|path| path.display().to_string()),
        sheet_count: 0,
        error_log_count: 0,
        stage_timings: {
            let mut timings = StageTimings::default();
            timings.insert("ingest", 0.0);
            timings.insert("reader_rows", snapshot.row_count as f64);
            timings
        },
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
    if args.input.extension().and_then(|value| value.to_str()).map(str::to_ascii_lowercase).as_deref() != Some("xlsx") {
        return Err(CostingError::Io {
            code: ErrorCode::UnsupportedFileType,
            message: "输入文件必须是 .xlsx 格式".to_string(),
            path: args.input.clone(),
            retryable: false,
        });
    }
    if !args.check_only && args.output.is_none() {
        return Err(CostingError::invalid_input("非 check-only 运行必须提供 --output"));
    }
    Ok(())
}
```

- [ ] **Step 8: Run verification**

Run:

```powershell
cargo test --manifest-path rust/Cargo.toml
```

Expected: all Rust tests pass.

Run one real local file if present:

```powershell
cargo run --manifest-path rust/Cargo.toml -p costing-calculate -- gb --input "data/raw/gb/<actual-gb-file>.xlsx" --check-only --benchmark
```

Expected:

- Exit code `0` if a real GB file exists.
- JSON contains `"reader_rows"` with a positive value.
- If no real file exists, record `BLOCKED_ENVIRONMENT: GB sample missing` in the task notes and continue only with generated reader fixtures.

- [ ] **Step 9: Commit**

```powershell
git add -- rust
git commit -m "feat(rust): read raw costing workbooks"
```

---

### Task 4: Header Normalization, Month Range, Fill Rules, and Split

**Files:**
- Create: `rust/crates/costing-core/src/normalize.rs`
- Create: `rust/crates/costing-core/src/split.rs`
- Modify: `rust/crates/costing-core/src/lib.rs`
- Modify: `rust/crates/costing-core/src/model.rs`
- Modify: `rust/crates/costing-cli/src/run.rs`

**Interfaces:**
- Consumes: `RawWorkbook`, `PipelineConfig`.
- Produces: `NormalizedCostFrame { columns, rows, key_columns }`.
- Produces: `SplitResult { detail_rows, qty_rows }`.
- Produces: `normalize_workbook(raw, config, month_range) -> Result<NormalizedCostFrame, CostingError>`.
- Produces: `split_detail_and_qty(normalized) -> Result<SplitResult, CostingError>`.

- [ ] **Step 1: Add models**

Append to `rust/crates/costing-core/src/model.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TableRow {
    pub values: BTreeMap<String, CellValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct NormalizedCostFrame {
    pub columns: Vec<String>,
    pub rows: Vec<TableRow>,
    pub key_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SplitResult {
    pub detail_rows: Vec<TableRow>,
    pub qty_rows: Vec<TableRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonthRange {
    pub start: Option<String>,
    pub end: Option<String>,
}
```

Modify `rust/crates/costing-core/src/lib.rs`:

```rust
pub mod error;
pub mod model;
pub mod normalize;
pub mod pipeline;
pub mod split;
pub mod timing;

pub use error::{CostingError, ErrorCode};
pub use model::{RunSummary, StageTimings};
pub use pipeline::{PipelineConfig, PipelineName};
```

- [ ] **Step 2: Write failing normalization tests**

Create `rust/crates/costing-core/src/normalize.rs`:

```rust
use crate::error::CostingError;
use crate::model::{CellValue, MonthRange, NormalizedCostFrame, RawWorkbook, TableRow};
use crate::pipeline::PipelineConfig;

pub fn normalize_workbook(
    _raw: RawWorkbook,
    _config: &PipelineConfig,
    _month_range: Option<MonthRange>,
) -> Result<NormalizedCostFrame, CostingError> {
    Err(CostingError::invalid_input("normalizer missing"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::{PipelineConfig, PipelineName};

    fn raw_with_vendor_rows() -> RawWorkbook {
        RawWorkbook {
            sheet_name: "成本计算单".to_string(),
            header_rows: [
                vec![
                    "年期".to_string(),
                    "成本中心名称".to_string(),
                    "产品编码".to_string(),
                    "工单编号".to_string(),
                    "供应商编码".to_string(),
                ],
                vec!["".to_string(), "".to_string(), "".to_string(), "".to_string(), "".to_string()],
            ],
            rows: vec![
                vec![
                    CellValue::Text("2025年01期".to_string()),
                    CellValue::Text("普通车间".to_string()),
                    CellValue::Text("P1".to_string()),
                    CellValue::Text("WO-1".to_string()),
                    CellValue::Text("V001".to_string()),
                ],
                vec![
                    CellValue::Blank,
                    CellValue::Text("集成车间".to_string()),
                    CellValue::Blank,
                    CellValue::Blank,
                    CellValue::Blank,
                ],
            ],
        }
    }

    #[test]
    fn forward_fill_skips_vendor_columns_for_integrated_workshop() {
        let config = PipelineConfig::for_name(PipelineName::Gb);
        let normalized = normalize_workbook(raw_with_vendor_rows(), &config, None).unwrap();
        assert_eq!(normalized.rows[1].values["产品编码"], CellValue::Text("P1".to_string()));
        assert_eq!(normalized.rows[1].values["供应商编码"], CellValue::Blank);
    }

    #[test]
    fn removes_total_rows() {
        let config = PipelineConfig::for_name(PipelineName::Gb);
        let mut raw = raw_with_vendor_rows();
        raw.rows.push(vec![
            CellValue::Text("合计".to_string()),
            CellValue::Text("普通车间".to_string()),
            CellValue::Blank,
            CellValue::Blank,
            CellValue::Blank,
        ]);
        let normalized = normalize_workbook(raw, &config, None).unwrap();
        assert_eq!(normalized.rows.len(), 2);
    }
}
```

- [ ] **Step 3: Run tests and verify they fail**

Run:

```powershell
cargo test --manifest-path rust/Cargo.toml -p costing-core normalize::tests
```

Expected: FAIL with `normalizer missing`.

- [ ] **Step 4: Implement normalization**

Replace `rust/crates/costing-core/src/normalize.rs`:

```rust
use std::collections::BTreeMap;

use crate::error::CostingError;
use crate::model::{CellValue, MonthRange, NormalizedCostFrame, RawWorkbook, TableRow};
use crate::pipeline::PipelineConfig;

const FILL_COLUMNS: &[&str] = &[
    "年期",
    "成本中心名称",
    "产品编码",
    "产品名称",
    "规格型号",
    "工单编号",
    "工单行号",
    "供应商编码",
    "供应商名称",
    "基本单位",
    "计划产量",
    "生产类型",
    "单据类型",
];

const VENDOR_COLUMNS: &[&str] = &["供应商编码", "供应商名称"];
const INTEGRATED_WORKSHOP_NAME: &str = "集成车间";

pub fn normalize_workbook(
    raw: RawWorkbook,
    _config: &PipelineConfig,
    month_range: Option<MonthRange>,
) -> Result<NormalizedCostFrame, CostingError> {
    let columns = flatten_headers(&raw.header_rows);
    let mut rows = rows_to_maps(&columns, raw.rows);
    rows.retain(|row| !is_total_row(row));
    forward_fill_with_rules(&mut rows);
    if let Some(range) = month_range {
        rows.retain(|row| month_in_range(row.values.get("年期").or_else(|| row.values.get("月份")), &range));
    }
    Ok(NormalizedCostFrame {
        columns,
        rows,
        key_columns: vec!["月份".to_string(), "产品编码".to_string()],
    })
}

pub fn flatten_headers(header_rows: &[Vec<String>; 2]) -> Vec<String> {
    let width = header_rows[0].len().max(header_rows[1].len());
    (0..width)
        .map(|idx| {
            let top = header_rows[0].get(idx).map(String::as_str).unwrap_or("").trim();
            let bottom = header_rows[1].get(idx).map(String::as_str).unwrap_or("").trim();
            match (top.is_empty(), bottom.is_empty()) {
                (true, true) => format!("column_{idx}"),
                (false, true) => top.to_string(),
                (true, false) => bottom.to_string(),
                (false, false) if top == bottom => top.to_string(),
                (false, false) => bottom.to_string(),
            }
        })
        .collect()
}

fn rows_to_maps(columns: &[String], rows: Vec<Vec<CellValue>>) -> Vec<TableRow> {
    rows.into_iter()
        .map(|row| {
            let values = columns
                .iter()
                .enumerate()
                .map(|(idx, column)| (column.clone(), row.get(idx).cloned().unwrap_or(CellValue::Blank)))
                .collect::<BTreeMap<_, _>>();
            TableRow { values }
        })
        .collect()
}

fn is_total_row(row: &TableRow) -> bool {
    ["年期", "月份", "成本中心名称"]
        .iter()
        .filter_map(|column| row.values.get(*column))
        .any(|value| cell_text(value).contains("合计"))
}

fn forward_fill_with_rules(rows: &mut [TableRow]) {
    let mut last_values: BTreeMap<String, CellValue> = BTreeMap::new();
    for row in rows {
        let cost_center = row.values.get("成本中心名称").map(cell_text).unwrap_or_default();
        for column in FILL_COLUMNS {
            let current = row.values.get(*column).cloned().unwrap_or(CellValue::Blank);
            if matches!(current, CellValue::Blank) {
                let is_vendor = VENDOR_COLUMNS.contains(column);
                if !(is_vendor && cost_center == INTEGRATED_WORKSHOP_NAME) {
                    if let Some(previous) = last_values.get(*column) {
                        row.values.insert((*column).to_string(), previous.clone());
                    }
                }
            } else {
                last_values.insert((*column).to_string(), current);
            }
        }
    }
}

fn month_in_range(value: Option<&CellValue>, range: &MonthRange) -> bool {
    let normalized = value.and_then(normalize_period);
    match normalized {
        None => false,
        Some(period) => {
            let after_start = range.start.as_ref().map(|start| &period >= start).unwrap_or(true);
            let before_end = range.end.as_ref().map(|end| &period <= end).unwrap_or(true);
            after_start && before_end
        }
    }
}

fn normalize_period(value: &CellValue) -> Option<String> {
    let text = cell_text(value);
    let digits: String = text.chars().filter(|ch| ch.is_ascii_digit()).collect();
    if digits.len() < 6 {
        return None;
    }
    let year = &digits[0..4];
    let month: u32 = digits[4..6].parse().ok()?;
    if !(1..=12).contains(&month) {
        return None;
    }
    Some(format!("{year}-{month:02}"))
}

fn cell_text(value: &CellValue) -> String {
    match value {
        CellValue::Blank => String::new(),
        CellValue::Text(value) | CellValue::DateLike(value) => value.trim().to_string(),
        CellValue::Decimal(value) => value.normalize().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::{PipelineConfig, PipelineName};

    fn raw_with_vendor_rows() -> RawWorkbook {
        RawWorkbook {
            sheet_name: "成本计算单".to_string(),
            header_rows: [
                vec![
                    "年期".to_string(),
                    "成本中心名称".to_string(),
                    "产品编码".to_string(),
                    "工单编号".to_string(),
                    "供应商编码".to_string(),
                ],
                vec!["".to_string(), "".to_string(), "".to_string(), "".to_string(), "".to_string()],
            ],
            rows: vec![
                vec![
                    CellValue::Text("2025年01期".to_string()),
                    CellValue::Text("普通车间".to_string()),
                    CellValue::Text("P1".to_string()),
                    CellValue::Text("WO-1".to_string()),
                    CellValue::Text("V001".to_string()),
                ],
                vec![
                    CellValue::Blank,
                    CellValue::Text("集成车间".to_string()),
                    CellValue::Blank,
                    CellValue::Blank,
                    CellValue::Blank,
                ],
            ],
        }
    }

    #[test]
    fn forward_fill_skips_vendor_columns_for_integrated_workshop() {
        let config = PipelineConfig::for_name(PipelineName::Gb);
        let normalized = normalize_workbook(raw_with_vendor_rows(), &config, None).unwrap();
        assert_eq!(normalized.rows[1].values["产品编码"], CellValue::Text("P1".to_string()));
        assert_eq!(normalized.rows[1].values["供应商编码"], CellValue::Blank);
    }

    #[test]
    fn removes_total_rows() {
        let config = PipelineConfig::for_name(PipelineName::Gb);
        let mut raw = raw_with_vendor_rows();
        raw.rows.push(vec![
            CellValue::Text("合计".to_string()),
            CellValue::Text("普通车间".to_string()),
            CellValue::Blank,
            CellValue::Blank,
            CellValue::Blank,
        ]);
        let normalized = normalize_workbook(raw, &config, None).unwrap();
        assert_eq!(normalized.rows.len(), 2);
    }
}
```

- [ ] **Step 5: Add split tests and implementation**

Create `rust/crates/costing-core/src/split.rs`:

```rust
use crate::error::CostingError;
use crate::model::{NormalizedCostFrame, SplitResult, TableRow};

const DETAIL_MARKER_COLUMNS: &[&str] = &["成本项目名称", "子项物料编码", "子项物料名称"];

pub fn split_detail_and_qty(frame: NormalizedCostFrame) -> Result<SplitResult, CostingError> {
    let mut detail_rows = Vec::new();
    let mut qty_rows = Vec::new();
    for row in frame.rows {
        if is_detail_row(&row) {
            detail_rows.push(row);
        } else {
            qty_rows.push(row);
        }
    }
    Ok(SplitResult { detail_rows, qty_rows })
}

fn is_detail_row(row: &TableRow) -> bool {
    DETAIL_MARKER_COLUMNS.iter().any(|column| {
        row.values
            .get(*column)
            .map(|value| !matches!(value, crate::model::CellValue::Blank))
            .unwrap_or(false)
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::model::{CellValue, NormalizedCostFrame, TableRow};

    use super::*;

    #[test]
    fn splits_detail_rows_by_cost_item_presence() {
        let detail = TableRow {
            values: BTreeMap::from([("成本项目名称".to_string(), CellValue::Text("直接材料".to_string()))]),
        };
        let qty = TableRow {
            values: BTreeMap::from([("成本项目名称".to_string(), CellValue::Blank)]),
        };
        let result = split_detail_and_qty(NormalizedCostFrame {
            columns: vec!["成本项目名称".to_string()],
            rows: vec![detail, qty],
            key_columns: vec![],
        })
        .unwrap();
        assert_eq!(result.detail_rows.len(), 1);
        assert_eq!(result.qty_rows.len(), 1);
    }
}
```

- [ ] **Step 6: Wire CLI through normalize and split in check-only mode**

Modify `rust/crates/costing-cli/src/run.rs` imports and run body:

```rust
use costing_core::normalize::normalize_workbook;
use costing_core::split::split_detail_and_qty;
use costing_core::{CostingError, ErrorCode, PipelineConfig, RunSummary, StageTimings};
use costing_xlsx::{reader::read_raw_workbook, snapshot::build_reader_snapshot};
```

Inside `run`, after reader:

```rust
let normalized = normalize_workbook(raw.clone(), &pipeline, None)?;
let split = split_detail_and_qty(normalized)?;
let mut timings = StageTimings::default();
timings.insert("ingest", 0.0);
timings.insert("reader_rows", snapshot.row_count as f64);
timings.insert("detail_rows", split.detail_rows.len() as f64);
timings.insert("qty_rows", split.qty_rows.len() as f64);
```

Use `timings` in `RunSummary`.

- [ ] **Step 7: Run verification**

Run:

```powershell
cargo test --manifest-path rust/Cargo.toml
```

Expected: all Rust tests pass.

Run:

```powershell
cargo run --manifest-path rust/Cargo.toml -p costing-calculate -- gb --input "data/raw/gb/<actual-gb-file>.xlsx" --check-only --benchmark
```

Expected if sample exists:

- Exit `0`.
- Summary contains positive `reader_rows`.
- Summary contains non-negative `detail_rows` and `qty_rows`.

- [ ] **Step 8: Commit**

```powershell
git add -- rust
git commit -m "feat(rust): normalize and split costing rows"
```

---

### Task 5: Fact Bundle and Quantity Aggregation

**Files:**
- Create: `rust/crates/costing-core/src/fact.rs`
- Create: `rust/crates/costing-core/src/quality.rs`
- Modify: `rust/crates/costing-core/src/lib.rs`
- Modify: `rust/crates/costing-core/src/model.rs`
- Modify: `rust/crates/costing-cli/src/run.rs`

**Interfaces:**
- Consumes: `SplitResult`, `PipelineConfig`.
- Produces: `FactBundle`.
- Produces: `build_fact_bundle(split: SplitResult, config: &PipelineConfig) -> Result<FactBundle, CostingError>`.
- Produces: `build_qty_sheet_rows(bundle: &FactBundle, config: &PipelineConfig) -> Vec<TableRow>`.
- Produces: error issue types `MISSING_AMOUNT`, `TOTAL_COST_MISMATCH`, `MOH_BREAKDOWN_MISMATCH`, `DUPLICATE_WORK_ORDER_KEY`.

- [ ] **Step 1: Add fact models**

Append to `rust/crates/costing-core/src/model.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ErrorIssue {
    pub row_id: String,
    pub issue_type: String,
    pub field_name: String,
    pub reason: String,
    pub action: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FactBundle {
    pub detail_fact: Vec<TableRow>,
    pub qty_fact: Vec<TableRow>,
    pub work_order_fact: Vec<TableRow>,
    pub error_issues: Vec<ErrorIssue>,
}
```

Modify `rust/crates/costing-core/src/lib.rs`:

```rust
pub mod error;
pub mod fact;
pub mod model;
pub mod normalize;
pub mod pipeline;
pub mod quality;
pub mod split;
pub mod timing;
```

- [ ] **Step 2: Write failing GB/SK fact tests**

Create `rust/crates/costing-core/src/fact.rs`:

```rust
use crate::error::CostingError;
use crate::model::{FactBundle, SplitResult, TableRow};
use crate::pipeline::PipelineConfig;

pub fn build_fact_bundle(_split: SplitResult, _config: &PipelineConfig) -> Result<FactBundle, CostingError> {
    Err(CostingError::invalid_input("fact builder missing"))
}

pub fn build_qty_sheet_rows(_bundle: &FactBundle, _config: &PipelineConfig) -> Vec<TableRow> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rust_decimal::Decimal;

    use crate::model::{CellValue, SplitResult, TableRow};
    use crate::pipeline::{PipelineConfig, PipelineName};

    use super::*;

    fn row(values: &[(&str, CellValue)]) -> TableRow {
        TableRow {
            values: values.iter().map(|(key, value)| ((*key).to_string(), value.clone())).collect(),
        }
    }

    #[test]
    fn gb_quantity_sheet_includes_outsource_total_match() {
        let detail = vec![
            row(&[("月份", CellValue::Text("2025年01期".to_string())), ("产品编码", CellValue::Text("P1".to_string())), ("产品名称", CellValue::Text("产品".to_string())), ("工单编号", CellValue::Text("WO1".to_string())), ("工单行号", CellValue::Text("1".to_string())), ("成本项目名称", CellValue::Text("直接材料".to_string())), ("本期完工金额", CellValue::Decimal(Decimal::new(100, 0)))]),
            row(&[("月份", CellValue::Text("2025年01期".to_string())), ("产品编码", CellValue::Text("P1".to_string())), ("产品名称", CellValue::Text("产品".to_string())), ("工单编号", CellValue::Text("WO1".to_string())), ("工单行号", CellValue::Text("1".to_string())), ("成本项目名称", CellValue::Text("委外加工费".to_string())), ("本期完工金额", CellValue::Decimal(Decimal::new(5, 0)))]),
        ];
        let qty = vec![row(&[("月份", CellValue::Text("2025年01期".to_string())), ("产品编码", CellValue::Text("P1".to_string())), ("产品名称", CellValue::Text("产品".to_string())), ("工单编号", CellValue::Text("WO1".to_string())), ("工单行号", CellValue::Text("1".to_string())), ("本期完工数量", CellValue::Decimal(Decimal::new(10, 0))), ("本期完工金额", CellValue::Decimal(Decimal::new(105, 0)))])];
        let config = PipelineConfig::for_name(PipelineName::Gb);
        let bundle = build_fact_bundle(SplitResult { detail_rows: detail, qty_rows: qty }, &config).unwrap();
        let sheet = build_qty_sheet_rows(&bundle, &config);
        assert_eq!(sheet[0].values["本期完工委外加工费合计完工金额"], CellValue::Decimal(Decimal::new(5, 0)));
        assert_eq!(sheet[0].values["直接材料+直接人工+制造费用+委外加工费是否等于总完工成本"], CellValue::Text("是".to_string()));
    }

    #[test]
    fn sk_quantity_sheet_includes_software_fee() {
        let detail = vec![row(&[("月份", CellValue::Text("2025年01期".to_string())), ("产品编码", CellValue::Text("P1".to_string())), ("产品名称", CellValue::Text("产品".to_string())), ("工单编号", CellValue::Text("WO1".to_string())), ("工单行号", CellValue::Text("1".to_string())), ("成本项目名称", CellValue::Text("软件费用".to_string())), ("本期完工金额", CellValue::Decimal(Decimal::new(7, 0)))])];
        let qty = vec![row(&[("月份", CellValue::Text("2025年01期".to_string())), ("产品编码", CellValue::Text("P1".to_string())), ("产品名称", CellValue::Text("产品".to_string())), ("工单编号", CellValue::Text("WO1".to_string())), ("工单行号", CellValue::Text("1".to_string())), ("本期完工数量", CellValue::Decimal(Decimal::new(1, 0))), ("本期完工金额", CellValue::Decimal(Decimal::new(7, 0)))])];
        let config = PipelineConfig::for_name(PipelineName::Sk);
        let bundle = build_fact_bundle(SplitResult { detail_rows: detail, qty_rows: qty }, &config).unwrap();
        let sheet = build_qty_sheet_rows(&bundle, &config);
        assert_eq!(sheet[0].values["本期完工软件费用合计完工金额"], CellValue::Decimal(Decimal::new(7, 0)));
    }
}
```

- [ ] **Step 3: Run tests and verify they fail**

Run:

```powershell
cargo test --manifest-path rust/Cargo.toml -p costing-core fact::tests
```

Expected: FAIL with `fact builder missing`.

- [ ] **Step 4: Implement minimal fact aggregation**

Replace the non-test top of `rust/crates/costing-core/src/fact.rs` with:

```rust
use std::collections::BTreeMap;

use rust_decimal::Decimal;

use crate::error::CostingError;
use crate::model::{CellValue, ErrorIssue, FactBundle, SplitResult, TableRow};
use crate::pipeline::PipelineConfig;

const ZERO: Decimal = Decimal::ZERO;

pub fn build_fact_bundle(split: SplitResult, _config: &PipelineConfig) -> Result<FactBundle, CostingError> {
    let mut amount_by_key: BTreeMap<String, BTreeMap<String, Decimal>> = BTreeMap::new();
    let mut error_issues = Vec::new();

    for row in &split.detail_rows {
        let key = work_order_key(row);
        let cost_item = text(row, "成本项目名称");
        let amount = decimal(row, "本期完工金额");
        if amount.is_none() {
            error_issues.push(ErrorIssue {
                row_id: key.clone(),
                issue_type: "MISSING_AMOUNT".to_string(),
                field_name: "本期完工金额".to_string(),
                reason: "成本明细金额为空，已按 0 参与汇总".to_string(),
                action: "金额置为 0 后继续计算".to_string(),
            });
        }
        let bucket = bucket_name(&cost_item);
        *amount_by_key.entry(key).or_default().entry(bucket).or_default() += amount.unwrap_or(ZERO);
    }

    let mut work_order_fact = Vec::new();
    for qty_row in &split.qty_rows {
        let key = work_order_key(qty_row);
        let amounts = amount_by_key.get(&key).cloned().unwrap_or_default();
        let completed_qty = decimal(qty_row, "本期完工数量").unwrap_or(ZERO);
        let completed_total = decimal(qty_row, "本期完工金额").unwrap_or(ZERO);
        let mut values = qty_row.values.clone();
        for (bucket, amount) in amounts {
            values.insert(bucket, CellValue::Decimal(amount));
        }
        values.insert("completed_qty".to_string(), CellValue::Decimal(completed_qty));
        values.insert("completed_amount_total".to_string(), CellValue::Decimal(completed_total));
        work_order_fact.push(TableRow { values });
    }

    Ok(FactBundle {
        detail_fact: split.detail_rows,
        qty_fact: split.qty_rows,
        work_order_fact,
        error_issues,
    })
}

pub fn build_qty_sheet_rows(bundle: &FactBundle, config: &PipelineConfig) -> Vec<TableRow> {
    bundle
        .work_order_fact
        .iter()
        .map(|row| {
            let mut values = row.values.clone();
            let dm = decimal_from_values(&values, "dm_amount");
            let dl = decimal_from_values(&values, "dl_amount");
            let moh = decimal_from_values(&values, "moh_amount");
            values.insert("本期完工直接材料合计完工金额".to_string(), CellValue::Decimal(dm));
            values.insert("本期完工直接人工合计完工金额".to_string(), CellValue::Decimal(dl));
            values.insert("本期完工制造费用合计完工金额".to_string(), CellValue::Decimal(moh));
            let mut total = dm + dl + moh;
            for item in config.standalone_cost_items {
                let key = standalone_key(item);
                let amount = decimal_from_values(&values, &key);
                values.insert(format!("本期完工{item}合计完工金额"), CellValue::Decimal(amount));
                total += amount;
            }
            let completed_total = decimal_from_values(&values, "completed_amount_total");
            let expression = total_match_column(config.standalone_cost_items);
            let status = if total == completed_total { "是" } else { "否" };
            values.insert(expression, CellValue::Text(status.to_string()));
            TableRow { values }
        })
        .collect()
}

fn work_order_key(row: &TableRow) -> String {
    ["月份", "年期", "产品编码", "工单编号", "工单行号"]
        .iter()
        .filter_map(|column| row.values.get(*column).map(cell_to_text))
        .collect::<Vec<_>>()
        .join("|")
}

fn bucket_name(cost_item: &str) -> String {
    match cost_item.trim() {
        "直接材料" => "dm_amount".to_string(),
        "直接人工" => "dl_amount".to_string(),
        value if value.starts_with("制造费用") => "moh_amount".to_string(),
        "委外加工费" => "outsource_amount".to_string(),
        "软件费用" => "software_amount".to_string(),
        other => format!("unmapped:{other}"),
    }
}

fn standalone_key(item: &str) -> String {
    match item {
        "委外加工费" => "outsource_amount".to_string(),
        "软件费用" => "software_amount".to_string(),
        other => format!("standalone:{other}"),
    }
}

fn total_match_column(items: &[&str]) -> String {
    let mut parts = vec!["直接材料", "直接人工", "制造费用"];
    parts.extend(items.iter().copied());
    format!("{}是否等于总完工成本", parts.join("+"))
}

fn text(row: &TableRow, column: &str) -> String {
    row.values.get(column).map(cell_to_text).unwrap_or_default()
}

fn decimal(row: &TableRow, column: &str) -> Option<Decimal> {
    row.values.get(column).and_then(cell_to_decimal)
}

fn decimal_from_values(values: &BTreeMap<String, CellValue>, column: &str) -> Decimal {
    values.get(column).and_then(cell_to_decimal).unwrap_or(ZERO)
}

fn cell_to_text(value: &CellValue) -> String {
    match value {
        CellValue::Blank => String::new(),
        CellValue::Text(value) | CellValue::DateLike(value) => value.clone(),
        CellValue::Decimal(value) => value.normalize().to_string(),
    }
}

fn cell_to_decimal(value: &CellValue) -> Option<Decimal> {
    match value {
        CellValue::Decimal(value) => Some(*value),
        CellValue::Text(value) => value.trim().parse().ok(),
        CellValue::Blank | CellValue::DateLike(_) => None,
    }
}
```

Keep the tests from Step 2 below this implementation.

- [ ] **Step 5: Add quality metric helper**

Create `rust/crates/costing-core/src/quality.rs`:

```rust
use crate::model::{FactBundle, QualityMetric};

pub fn build_quality_metrics(bundle: &FactBundle) -> Vec<QualityMetric> {
    vec![
        QualityMetric {
            category: "行数勾稽".to_string(),
            metric: "成本明细行数".to_string(),
            value: bundle.detail_fact.len().to_string(),
            description: "Rust detail fact rows".to_string(),
        },
        QualityMetric {
            category: "行数勾稽".to_string(),
            metric: "数量页行数".to_string(),
            value: bundle.qty_fact.len().to_string(),
            description: "Rust qty fact rows".to_string(),
        },
    ]
}
```

- [ ] **Step 6: Run verification**

Run:

```powershell
cargo test --manifest-path rust/Cargo.toml
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```powershell
git add -- rust
git commit -m "feat(rust): build quantity aggregation facts"
```

---

### Task 6: Modified Z-Score Scoring

**Files:**
- Create: `rust/crates/costing-core/src/scoring.rs`
- Modify: `rust/crates/costing-core/src/lib.rs`

**Interfaces:**
- Produces: `weighted_median(values: &[(Decimal, Decimal)]) -> Option<Decimal>`.
- Produces: `weighted_mad(values: &[(Decimal, Decimal)], median: Decimal) -> Option<Decimal>`.
- Produces: `grade_score(score: Option<Decimal>) -> &'static str`.

- [ ] **Step 1: Add scoring module export**

Modify `rust/crates/costing-core/src/lib.rs`:

```rust
pub mod error;
pub mod fact;
pub mod model;
pub mod normalize;
pub mod pipeline;
pub mod quality;
pub mod scoring;
pub mod split;
pub mod timing;
```

- [ ] **Step 2: Write scoring tests**

Create `rust/crates/costing-core/src/scoring.rs`:

```rust
use rust_decimal::Decimal;

pub fn weighted_median(_values: &[(Decimal, Decimal)]) -> Option<Decimal> {
    None
}

pub fn weighted_mad(_values: &[(Decimal, Decimal)], _median: Decimal) -> Option<Decimal> {
    None
}

pub fn grade_score(_score: Option<Decimal>) -> &'static str {
    "正常"
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    #[test]
    fn weighted_median_uses_weights() {
        let values = vec![
            (Decimal::new(1, 0), Decimal::new(1, 0)),
            (Decimal::new(10, 0), Decimal::new(10, 0)),
            (Decimal::new(100, 0), Decimal::new(1, 0)),
        ];
        assert_eq!(weighted_median(&values), Some(Decimal::new(10, 0)));
    }

    #[test]
    fn grade_score_matches_contract_thresholds() {
        assert_eq!(grade_score(None), "正常");
        assert_eq!(grade_score(Some(Decimal::new(25, 1))), "正常");
        assert_eq!(grade_score(Some(Decimal::new(26, 1))), "关注");
        assert_eq!(grade_score(Some(Decimal::new(35, 1))), "关注");
        assert_eq!(grade_score(Some(Decimal::new(36, 1))), "高度可疑");
        assert_eq!(grade_score(Some(Decimal::new(-36, 1))), "高度可疑");
    }
}
```

- [ ] **Step 3: Run tests and verify they fail**

Run:

```powershell
cargo test --manifest-path rust/Cargo.toml -p costing-core scoring::tests
```

Expected: FAIL because `weighted_median` returns `None` and threshold grading is incomplete.

- [ ] **Step 4: Implement scoring**

Replace `rust/crates/costing-core/src/scoring.rs`:

```rust
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

pub fn weighted_median(values: &[(Decimal, Decimal)]) -> Option<Decimal> {
    let mut valid: Vec<(Decimal, Decimal)> = values
        .iter()
        .copied()
        .filter(|(_, weight)| *weight > Decimal::ZERO)
        .collect();
    if valid.is_empty() {
        return None;
    }
    valid.sort_by(|left, right| left.0.cmp(&right.0));
    let total_weight: Decimal = valid.iter().map(|(_, weight)| *weight).sum();
    let midpoint = total_weight / Decimal::new(2, 0);
    let mut cumulative = Decimal::ZERO;
    for (value, weight) in valid {
        cumulative += weight;
        if cumulative >= midpoint {
            return Some(value);
        }
    }
    None
}

pub fn weighted_mad(values: &[(Decimal, Decimal)], median: Decimal) -> Option<Decimal> {
    let deviations: Vec<(Decimal, Decimal)> = values
        .iter()
        .map(|(value, weight)| ((*value - median).abs(), *weight))
        .collect();
    weighted_median(&deviations)
}

pub fn grade_score(score: Option<Decimal>) -> &'static str {
    let Some(score) = score else {
        return "正常";
    };
    let abs = score.abs();
    if abs <= Decimal::new(25, 1) {
        "正常"
    } else if abs <= Decimal::new(35, 1) {
        "关注"
    } else {
        "高度可疑"
    }
}

pub fn decimal_ln(value: Decimal) -> Option<f64> {
    value.to_f64().filter(|number| *number > 0.0).map(f64::ln)
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    #[test]
    fn weighted_median_uses_weights() {
        let values = vec![
            (Decimal::new(1, 0), Decimal::new(1, 0)),
            (Decimal::new(10, 0), Decimal::new(10, 0)),
            (Decimal::new(100, 0), Decimal::new(1, 0)),
        ];
        assert_eq!(weighted_median(&values), Some(Decimal::new(10, 0)));
    }

    #[test]
    fn grade_score_matches_contract_thresholds() {
        assert_eq!(grade_score(None), "正常");
        assert_eq!(grade_score(Some(Decimal::new(25, 1))), "正常");
        assert_eq!(grade_score(Some(Decimal::new(26, 1))), "关注");
        assert_eq!(grade_score(Some(Decimal::new(35, 1))), "关注");
        assert_eq!(grade_score(Some(Decimal::new(36, 1))), "高度可疑");
        assert_eq!(grade_score(Some(Decimal::new(-36, 1))), "高度可疑");
    }
}
```

- [ ] **Step 5: Run verification**

Run:

```powershell
cargo test --manifest-path rust/Cargo.toml
```

Expected: all Rust tests pass.

- [ ] **Step 6: Commit**

```powershell
git add -- rust
git commit -m "feat(rust): add anomaly scoring primitives"
```

---

### Task 7: Work-Order Anomaly Sheet

**Files:**
- Create: `rust/crates/costing-core/src/anomaly.rs`
- Modify: `rust/crates/costing-core/src/lib.rs`
- Modify: `rust/crates/costing-core/src/model.rs`

**Interfaces:**
- Consumes: `FactBundle`, `PipelineConfig`.
- Produces: `build_work_order_anomaly_sheet(bundle: &FactBundle, config: &PipelineConfig) -> SheetModel`.
- Produces visible columns including `异常等级`, `异常主要来源`, `异常明细解释`, `复核原因`.

- [ ] **Step 1: Add module export**

Modify `rust/crates/costing-core/src/lib.rs`:

```rust
pub mod anomaly;
pub mod error;
pub mod fact;
pub mod model;
pub mod normalize;
pub mod pipeline;
pub mod quality;
pub mod scoring;
pub mod split;
pub mod timing;
```

- [ ] **Step 2: Write failing anomaly test**

Create `rust/crates/costing-core/src/anomaly.rs`:

```rust
use crate::model::{FactBundle, SheetModel};
use crate::pipeline::PipelineConfig;

pub fn build_work_order_anomaly_sheet(_bundle: &FactBundle, _config: &PipelineConfig) -> SheetModel {
    SheetModel {
        sheet_name: "成本分析工单维度".to_string(),
        columns: Vec::new(),
        rows: Vec::new(),
        column_types: Default::default(),
        number_formats: Default::default(),
        freeze_panes: Some("A2".to_string()),
        auto_filter: true,
        fixed_width: Some(15.0),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rust_decimal::Decimal;

    use crate::model::{CellValue, ErrorIssue, FactBundle, TableRow};
    use crate::pipeline::{PipelineConfig, PipelineName};

    use super::*;

    #[test]
    fn work_order_sheet_contains_required_audit_columns() {
        let row = TableRow {
            values: BTreeMap::from([
                ("月份".to_string(), CellValue::Text("2025年01期".to_string())),
                ("产品编码".to_string(), CellValue::Text("P1".to_string())),
                ("产品名称".to_string(), CellValue::Text("产品".to_string())),
                ("工单编号".to_string(), CellValue::Text("WO1".to_string())),
                ("工单行号".to_string(), CellValue::Text("1".to_string())),
                ("completed_qty".to_string(), CellValue::Decimal(Decimal::new(10, 0))),
                ("completed_amount_total".to_string(), CellValue::Decimal(Decimal::new(100, 0))),
            ]),
        };
        let bundle = FactBundle {
            detail_fact: vec![],
            qty_fact: vec![],
            work_order_fact: vec![row],
            error_issues: Vec::<ErrorIssue>::new(),
        };
        let sheet = build_work_order_anomaly_sheet(&bundle, &PipelineConfig::for_name(PipelineName::Gb));
        assert_eq!(sheet.sheet_name, "成本分析工单维度");
        assert!(sheet.columns.contains(&"异常等级".to_string()));
        assert!(sheet.columns.contains(&"异常主要来源".to_string()));
        assert!(sheet.columns.contains(&"异常明细解释".to_string()));
        assert!(sheet.columns.contains(&"复核原因".to_string()));
    }
}
```

- [ ] **Step 3: Run test and verify it fails**

Run:

```powershell
cargo test --manifest-path rust/Cargo.toml -p costing-core anomaly::tests
```

Expected: FAIL because required columns are missing.

- [ ] **Step 4: Implement first visible anomaly sheet**

Replace the non-test top of `rust/crates/costing-core/src/anomaly.rs`:

```rust
use std::collections::BTreeMap;

use crate::model::{CellValue, FactBundle, SheetModel};
use crate::pipeline::PipelineConfig;

const WORK_ORDER_COLUMNS: &[&str] = &[
    "月份",
    "成本中心",
    "产品编码",
    "产品名称",
    "规格型号",
    "工单编号",
    "工单行",
    "生产类型",
    "基本单位",
    "本期完工数量",
    "总完工成本",
    "直接材料合计完工金额",
    "直接人工合计完工金额",
    "制造费用合计完工金额",
    "总单位完工成本",
    "直接材料单位完工成本",
    "直接人工单位完工成本",
    "制造费用单位完工成本",
    "是否可参与分析",
    "异常等级",
    "异常主要来源",
    "异常明细解释",
    "复核原因",
];

pub fn build_work_order_anomaly_sheet(bundle: &FactBundle, _config: &PipelineConfig) -> SheetModel {
    let columns = WORK_ORDER_COLUMNS.iter().map(|value| (*value).to_string()).collect::<Vec<_>>();
    let rows = bundle
        .work_order_fact
        .iter()
        .map(|row| {
            columns
                .iter()
                .map(|column| map_work_order_value(row, column))
                .collect::<Vec<_>>()
        })
        .collect();
    let mut column_types = BTreeMap::new();
    let mut number_formats = BTreeMap::new();
    for column in &columns {
        let metric_type = if column.contains("金额") || column.contains("成本") {
            "amount"
        } else if column == "本期完工数量" {
            "qty"
        } else {
            "text"
        };
        column_types.insert(column.clone(), metric_type.to_string());
        if metric_type == "amount" || metric_type == "qty" {
            number_formats.insert(column.clone(), "#,##0.00".to_string());
        }
    }
    SheetModel {
        sheet_name: "成本分析工单维度".to_string(),
        columns,
        rows,
        column_types,
        number_formats,
        freeze_panes: Some("A2".to_string()),
        auto_filter: true,
        fixed_width: Some(15.0),
    }
}

fn map_work_order_value(row: &crate::model::TableRow, column: &str) -> CellValue {
    match column {
        "月份" => row.values.get("月份").or_else(|| row.values.get("年期")).cloned().unwrap_or(CellValue::Blank),
        "产品编码" => row.values.get("产品编码").cloned().unwrap_or(CellValue::Blank),
        "产品名称" => row.values.get("产品名称").cloned().unwrap_or(CellValue::Blank),
        "工单编号" => row.values.get("工单编号").cloned().unwrap_or(CellValue::Blank),
        "工单行" => row.values.get("工单行号").cloned().unwrap_or(CellValue::Blank),
        "本期完工数量" => row.values.get("completed_qty").cloned().unwrap_or(CellValue::Blank),
        "总完工成本" => row.values.get("completed_amount_total").cloned().unwrap_or(CellValue::Blank),
        "是否可参与分析" => CellValue::Text("是".to_string()),
        "异常等级" => CellValue::Text("正常".to_string()),
        "异常主要来源" | "异常明细解释" | "复核原因" => CellValue::Text(String::new()),
        _ => row.values.get(column).cloned().unwrap_or(CellValue::Blank),
    }
}
```

Keep tests from Step 2.

- [ ] **Step 5: Add exact anomaly parity work item inside this task**

Extend tests to cover at least one `关注` and one `高度可疑` case after the basic sheet passes. Use Python `tests/test_weighted_zscore.py` and `src/analytics/anomaly.py` as the oracle. The Rust implementation must then calculate:

```rust
// Required behavior:
// - group by product_code + product_name + production_scope
// - score only positive unit costs and positive completed quantity
// - use thresholds: <=2.5 normal, <=3.5 attention, >3.5 suspicious
// - leave standalone cost items out of anomaly-level calculation
```

Run:

```powershell
uv run python -m pytest tests/test_weighted_zscore.py tests/test_pq_analysis_v3.py -q
cargo test --manifest-path rust/Cargo.toml -p costing-core anomaly::tests
```

Expected: both Python oracle tests and Rust anomaly tests pass.

- [ ] **Step 6: Commit**

```powershell
git add -- rust
git commit -m "feat(rust): build work order anomaly sheet"
```

---

### Task 8: Presentation Layer and Product-Dimension Rejection

**Files:**
- Create: `rust/crates/costing-core/src/presentation.rs`
- Modify: `rust/crates/costing-core/src/lib.rs`
- Modify: `rust/crates/costing-cli/src/run.rs`

**Interfaces:**
- Consumes: `FactBundle`, quantity sheet rows, work-order anomaly sheet, quality metrics.
- Produces: `build_workbook_payload(bundle, config, timings) -> Result<WorkbookPayload, CostingError>`.
- Produces exactly 3 sheet models in the required order.

- [ ] **Step 1: Export module**

Modify `rust/crates/costing-core/src/lib.rs`:

```rust
pub mod anomaly;
pub mod error;
pub mod fact;
pub mod model;
pub mod normalize;
pub mod pipeline;
pub mod presentation;
pub mod quality;
pub mod scoring;
pub mod split;
pub mod timing;
```

- [ ] **Step 2: Write failing presentation tests**

Create `rust/crates/costing-core/src/presentation.rs`:

```rust
use crate::error::CostingError;
use crate::model::{FactBundle, StageTimings, WorkbookPayload};
use crate::pipeline::PipelineConfig;

pub fn build_workbook_payload(
    _bundle: FactBundle,
    _config: &PipelineConfig,
    _timings: StageTimings,
) -> Result<WorkbookPayload, CostingError> {
    Err(CostingError::invalid_input("presentation missing"))
}

#[cfg(test)]
mod tests {
    use crate::model::{ErrorIssue, FactBundle, StageTimings};
    use crate::pipeline::{PipelineConfig, PipelineName};

    use super::*;

    #[test]
    fn payload_has_exactly_three_default_sheets_without_product_dimension() {
        let payload = build_workbook_payload(
            FactBundle {
                detail_fact: vec![],
                qty_fact: vec![],
                work_order_fact: vec![],
                error_issues: Vec::<ErrorIssue>::new(),
            },
            &PipelineConfig::for_name(PipelineName::Gb),
            StageTimings::default(),
        )
        .unwrap();
        let names = payload.sheet_models.iter().map(|sheet| sheet.sheet_name.as_str()).collect::<Vec<_>>();
        assert_eq!(names, vec!["成本计算单总表", "成本计算单数量聚合维度", "成本分析工单维度"]);
        assert!(!names.contains(&"成本分析产品维度"));
    }
}
```

- [ ] **Step 3: Run test and verify it fails**

Run:

```powershell
cargo test --manifest-path rust/Cargo.toml -p costing-core presentation::tests
```

Expected: FAIL with `presentation missing`.

- [ ] **Step 4: Implement workbook payload**

Replace `rust/crates/costing-core/src/presentation.rs`:

```rust
use std::collections::BTreeMap;

use crate::anomaly::build_work_order_anomaly_sheet;
use crate::error::CostingError;
use crate::fact::build_qty_sheet_rows;
use crate::model::{CellValue, FactBundle, SheetModel, StageTimings, WorkbookPayload};
use crate::pipeline::PipelineConfig;
use crate::quality::build_quality_metrics;

const PRODUCT_DIMENSION_SHEET: &str = "成本分析产品维度";

pub fn build_workbook_payload(
    bundle: FactBundle,
    config: &PipelineConfig,
    timings: StageTimings,
) -> Result<WorkbookPayload, CostingError> {
    let detail_sheet = build_flat_sheet("成本计算单总表", bundle.detail_fact.clone());
    let qty_sheet = build_flat_sheet("成本计算单数量聚合维度", build_qty_sheet_rows(&bundle, config));
    let work_order_sheet = build_work_order_anomaly_sheet(&bundle, config);
    let sheets = vec![detail_sheet, qty_sheet, work_order_sheet];
    ensure_no_product_dimension(&sheets)?;
    Ok(WorkbookPayload {
        sheet_models: sheets,
        quality_metrics: build_quality_metrics(&bundle),
        error_log_count: bundle.error_issues.len(),
        stage_timings: timings,
    })
}

fn build_flat_sheet(sheet_name: &str, rows: Vec<crate::model::TableRow>) -> SheetModel {
    let columns = rows
        .first()
        .map(|row| row.values.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    let sheet_rows = rows
        .iter()
        .map(|row| columns.iter().map(|column| row.values.get(column).cloned().unwrap_or(CellValue::Blank)).collect())
        .collect::<Vec<Vec<CellValue>>>();
    let mut column_types = BTreeMap::new();
    let mut number_formats = BTreeMap::new();
    for column in &columns {
        let is_number = column.contains("金额") || column.contains("成本") || column.contains("数量");
        column_types.insert(column.clone(), if is_number { "amount" } else { "text" }.to_string());
        if is_number {
            number_formats.insert(column.clone(), "#,##0.00".to_string());
        }
    }
    SheetModel {
        sheet_name: sheet_name.to_string(),
        columns,
        rows: sheet_rows,
        column_types,
        number_formats,
        freeze_panes: Some("A2".to_string()),
        auto_filter: true,
        fixed_width: Some(15.0),
    }
}

fn ensure_no_product_dimension(sheets: &[SheetModel]) -> Result<(), CostingError> {
    if sheets.iter().any(|sheet| sheet.sheet_name == PRODUCT_DIMENSION_SHEET) {
        return Err(CostingError::invalid_input(format!("{PRODUCT_DIMENSION_SHEET} 不属于 Rust 默认 workbook 契约")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::model::{ErrorIssue, FactBundle, StageTimings};
    use crate::pipeline::{PipelineConfig, PipelineName};

    use super::*;

    #[test]
    fn payload_has_exactly_three_default_sheets_without_product_dimension() {
        let payload = build_workbook_payload(
            FactBundle {
                detail_fact: vec![],
                qty_fact: vec![],
                work_order_fact: vec![],
                error_issues: Vec::<ErrorIssue>::new(),
            },
            &PipelineConfig::for_name(PipelineName::Gb),
            StageTimings::default(),
        )
        .unwrap();
        let names = payload.sheet_models.iter().map(|sheet| sheet.sheet_name.as_str()).collect::<Vec<_>>();
        assert_eq!(names, vec!["成本计算单总表", "成本计算单数量聚合维度", "成本分析工单维度"]);
        assert!(!names.contains(&"成本分析产品维度"));
    }
}
```

- [ ] **Step 5: Wire CLI through payload build**

Modify `rust/crates/costing-cli/src/run.rs` after `build_fact_bundle` is available:

```rust
use costing_core::fact::build_fact_bundle;
use costing_core::presentation::build_workbook_payload;
```

In `run`, after `split`:

```rust
let bundle = build_fact_bundle(split, &pipeline)?;
let payload = build_workbook_payload(bundle, &pipeline, timings)?;
```

Use `payload.sheet_models.len()` and `payload.error_log_count` in `RunSummary`.

- [ ] **Step 6: Run verification**

Run:

```powershell
cargo test --manifest-path rust/Cargo.toml
```

Expected: all Rust tests pass.

- [ ] **Step 7: Commit**

```powershell
git add -- rust
git commit -m "feat(rust): build three sheet payload"
```

---

### Task 9: Rust Workbook Writer

**Files:**
- Create: `rust/crates/costing-xlsx/src/writer.rs`
- Modify: `rust/crates/costing-xlsx/src/lib.rs`
- Modify: `rust/crates/costing-cli/src/run.rs`

**Interfaces:**
- Consumes: `WorkbookPayload`.
- Produces: `write_workbook(path: &Path, payload: &WorkbookPayload) -> Result<(), CostingXlsxError>`.
- Rejects `成本分析产品维度`.

- [ ] **Step 1: Export writer**

Modify `rust/crates/costing-xlsx/src/lib.rs`:

```rust
pub mod reader;
pub mod snapshot;
pub mod writer;
```

- [ ] **Step 2: Write writer tests**

Create `rust/crates/costing-xlsx/src/writer.rs`:

```rust
use std::path::Path;

use costing_core::model::WorkbookPayload;

use crate::reader::CostingXlsxError;

pub fn write_workbook(_path: &Path, _payload: &WorkbookPayload) -> Result<(), CostingXlsxError> {
    Err(CostingXlsxError::Message("writer missing".to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use costing_core::model::{CellValue, SheetModel, StageTimings, WorkbookPayload};
    use rust_decimal::Decimal;

    use super::*;

    #[test]
    fn writes_three_sheet_workbook() {
        let output = std::env::temp_dir().join("costing-rust-writer.xlsx");
        let sheet = SheetModel {
            sheet_name: "成本计算单总表".to_string(),
            columns: vec!["月份".to_string(), "金额".to_string()],
            rows: vec![vec![CellValue::Text("2025年01期".to_string()), CellValue::Decimal(Decimal::new(125, 1))]],
            column_types: BTreeMap::from([("月份".to_string(), "text".to_string()), ("金额".to_string(), "amount".to_string())]),
            number_formats: BTreeMap::from([("金额".to_string(), "#,##0.00".to_string())]),
            freeze_panes: Some("A2".to_string()),
            auto_filter: true,
            fixed_width: Some(15.0),
        };
        let payload = WorkbookPayload {
            sheet_models: vec![sheet],
            quality_metrics: vec![],
            error_log_count: 0,
            stage_timings: StageTimings::default(),
        };
        write_workbook(&output, &payload).unwrap();
        assert!(output.exists());
        let _ = std::fs::remove_file(output);
    }
}
```

- [ ] **Step 3: Run test and verify it fails**

Run:

```powershell
cargo test --manifest-path rust/Cargo.toml -p costing-xlsx writer::tests
```

Expected: FAIL with `writer missing`.

- [ ] **Step 4: Implement writer based on sidecar spike rules**

Replace `rust/crates/costing-xlsx/src/writer.rs`:

```rust
use std::path::Path;

use costing_core::model::{CellValue, WorkbookPayload};
use rust_xlsxwriter::{Format, Workbook};

use crate::reader::CostingXlsxError;

const PRODUCT_DIMENSION_SHEET: &str = "成本分析产品维度";

pub fn write_workbook(path: &Path, payload: &WorkbookPayload) -> Result<(), CostingXlsxError> {
    let mut workbook = Workbook::new();
    for sheet in &payload.sheet_models {
        if sheet.sheet_name == PRODUCT_DIMENSION_SHEET {
            return Err(CostingXlsxError::Message(format!("{PRODUCT_DIMENSION_SHEET} must not be written")));
        }
        let worksheet = workbook.add_worksheet();
        worksheet
            .set_name(&sheet.sheet_name)
            .map_err(|error| CostingXlsxError::Message(error.to_string()))?;
        let header_format = Format::new()
            .set_bold()
            .set_background_color("#D9E1F2")
            .set_border(rust_xlsxwriter::FormatBorder::Thin);
        let text_format = Format::new().set_align(rust_xlsxwriter::FormatAlign::Left);

        for (col_idx, column) in sheet.columns.iter().enumerate() {
            worksheet
                .write_string_with_format(0, col_idx as u16, column, &header_format)
                .map_err(|error| CostingXlsxError::Message(error.to_string()))?;
            if let Some(width) = sheet.fixed_width {
                worksheet
                    .set_column_width(col_idx as u16, width)
                    .map_err(|error| CostingXlsxError::Message(error.to_string()))?;
            }
        }

        for (row_idx, row) in sheet.rows.iter().enumerate() {
            for (col_idx, value) in row.iter().enumerate() {
                let excel_row = (row_idx + 1) as u32;
                let excel_col = col_idx as u16;
                let column_name = &sheet.columns[col_idx];
                let number_format = sheet.number_formats.get(column_name).map(|format| Format::new().set_num_format(format));
                match (value, number_format.as_ref()) {
                    (CellValue::Blank, _) => {}
                    (CellValue::Decimal(value), Some(format)) => worksheet
                        .write_number_with_format(excel_row, excel_col, value.to_string().parse::<f64>().unwrap_or(0.0), format)
                        .map_err(|error| CostingXlsxError::Message(error.to_string()))?,
                    (CellValue::Decimal(value), None) => worksheet
                        .write_number(excel_row, excel_col, value.to_string().parse::<f64>().unwrap_or(0.0))
                        .map_err(|error| CostingXlsxError::Message(error.to_string()))?,
                    (CellValue::Text(value) | CellValue::DateLike(value), _) => worksheet
                        .write_string_with_format(excel_row, excel_col, value, &text_format)
                        .map_err(|error| CostingXlsxError::Message(error.to_string()))?,
                }
            }
        }
        if sheet.auto_filter && !sheet.columns.is_empty() {
            let last_row = sheet.rows.len() as u32;
            let last_col = (sheet.columns.len() - 1) as u16;
            worksheet
                .autofilter(0, 0, last_row, last_col)
                .map_err(|error| CostingXlsxError::Message(error.to_string()))?;
        }
        if let Some(freeze_panes) = &sheet.freeze_panes {
            let (row, col) = parse_freeze_panes(freeze_panes)?;
            worksheet
                .set_freeze_panes(row, col)
                .map_err(|error| CostingXlsxError::Message(error.to_string()))?;
        }
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| CostingXlsxError::Message(error.to_string()))?;
    }
    workbook
        .save(path)
        .map_err(|error| CostingXlsxError::Message(error.to_string()))
}

fn parse_freeze_panes(token: &str) -> Result<(u32, u16), CostingXlsxError> {
    let trimmed = token.trim().to_ascii_uppercase();
    if trimmed == "A2" {
        return Ok((1, 0));
    }
    Err(CostingXlsxError::Message(format!("unsupported freeze panes token: {token}")))
}
```

Keep tests from Step 2.

- [ ] **Step 5: Wire writer into CLI**

Modify `rust/crates/costing-cli/src/run.rs`:

```rust
use costing_xlsx::writer::write_workbook;
```

After payload build:

```rust
if !args.check_only {
    let output = args.output.as_ref().expect("validated output path");
    write_workbook(output, &payload)?;
}
```

Set `output_written` from `!args.check_only`.

- [ ] **Step 6: Run verification**

Run:

```powershell
cargo test --manifest-path rust/Cargo.toml
cargo run --manifest-path rust/Cargo.toml -p costing-calculate -- gb --input "data/raw/gb/<actual-gb-file>.xlsx" --output ".pytest-tmp/rust-gb-output.xlsx" --benchmark
```

Expected if sample exists:

- Exit `0`.
- `.pytest-tmp/rust-gb-output.xlsx` exists.
- Output workbook contains no `成本分析产品维度`.

- [ ] **Step 7: Commit**

```powershell
git add -- rust
git commit -m "feat(rust): write three sheet workbook"
```

---

### Task 10: Python Oracle Harness and Workbook Validator

**Files:**
- Create: `tests/rust_oracle/__init__.py`
- Create: `tests/rust_oracle/oracle_runner.py`
- Create: `tests/rust_oracle/workbook_compare.py`
- Create: `tests/test_full_rust_cli_oracle.py`
- Modify: `tests/contracts/README.md`

**Interfaces:**
- Consumes: Rust CLI binary via `cargo run --manifest-path rust/Cargo.toml -p costing-calculate`.
- Consumes: Python oracle via current `main.py` or service path.
- Produces: pytest tests that compare final workbooks.

- [ ] **Step 1: Create package marker**

Create `tests/rust_oracle/__init__.py`:

```python
"""Rust rewrite oracle helpers."""
```

- [ ] **Step 2: Add workbook comparator**

Create `tests/rust_oracle/workbook_compare.py`:

```python
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from openpyxl import load_workbook

FORBIDDEN_SHEETS = {'成本分析产品维度'}
DECIMAL_TOLERANCE = Decimal('0.000001')


def compare_workbooks(expected_path: Path, actual_path: Path) -> dict[str, Any]:
    expected = load_workbook(expected_path, data_only=False)
    actual = load_workbook(actual_path, data_only=False)
    errors: list[str] = []

    if FORBIDDEN_SHEETS.intersection(actual.sheetnames):
        errors.append('actual workbook contains forbidden product dimension sheet')
    if expected.sheetnames != actual.sheetnames:
        errors.append(f'sheet names differ: expected={expected.sheetnames}, actual={actual.sheetnames}')

    for sheet_name in expected.sheetnames:
        if sheet_name not in actual.sheetnames:
            continue
        expected_ws = expected[sheet_name]
        actual_ws = actual[sheet_name]
        if expected_ws.max_row != actual_ws.max_row or expected_ws.max_column != actual_ws.max_column:
            errors.append(
                f'shape mismatch {sheet_name}: '
                f'expected={expected_ws.max_row}x{expected_ws.max_column}, '
                f'actual={actual_ws.max_row}x{actual_ws.max_column}'
            )
            continue
        if expected_ws.freeze_panes != actual_ws.freeze_panes:
            errors.append(f'freeze panes mismatch {sheet_name}')
        if expected_ws.auto_filter.ref != actual_ws.auto_filter.ref:
            errors.append(f'auto filter mismatch {sheet_name}')
        for row in range(1, expected_ws.max_row + 1):
            for col in range(1, expected_ws.max_column + 1):
                expected_value = expected_ws.cell(row, col).value
                actual_value = actual_ws.cell(row, col).value
                if not values_equal(expected_value, actual_value):
                    errors.append(
                        f'value mismatch {sheet_name}!{row},{col}: '
                        f'expected={expected_value!r}, actual={actual_value!r}'
                    )
                    if len(errors) >= 20:
                        return {'passed': False, 'errors': errors}
    return {'passed': not errors, 'errors': errors}


def values_equal(expected: object, actual: object) -> bool:
    if is_blank(expected) and is_blank(actual):
        return True
    expected_decimal = as_decimal(expected)
    actual_decimal = as_decimal(actual)
    if expected_decimal is not None and actual_decimal is not None:
        return abs(expected_decimal - actual_decimal) <= DECIMAL_TOLERANCE
    return expected == actual


def is_blank(value: object) -> bool:
    return value is None or value == ''


def as_decimal(value: object) -> Decimal | None:
    if value is None or value == '':
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
```

- [ ] **Step 3: Add oracle runner**

Create `tests/rust_oracle/oracle_runner.py`:

```python
from __future__ import annotations

import subprocess
from pathlib import Path


def run_python_oracle(pipeline: str, input_path: Path, output_path: Path) -> None:
    completed = subprocess.run(
        [
            'uv',
            'run',
            'python',
            'main.py',
            pipeline,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise AssertionError(f'python oracle failed\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}')
    if not output_path.exists():
        raise AssertionError(f'python oracle did not create expected workbook: {output_path}')


def run_rust_cli(pipeline: str, input_path: Path, output_path: Path) -> None:
    completed = subprocess.run(
        [
            'cargo',
            'run',
            '--quiet',
            '--manifest-path',
            'rust/Cargo.toml',
            '-p',
            'costing-calculate',
            '--',
            pipeline,
            '--input',
            str(input_path),
            '--output',
            str(output_path),
            '--benchmark',
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise AssertionError(f'rust cli failed\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}')
    if not output_path.exists():
        raise AssertionError(f'rust cli did not create expected workbook: {output_path}')
```

- [ ] **Step 4: Add pytest integration tests**

Create `tests/test_full_rust_cli_oracle.py`:

```python
from __future__ import annotations

from pathlib import Path

import pytest

from tests.rust_oracle.workbook_compare import compare_workbooks


def _first_sample(patterns: tuple[str, ...]) -> Path | None:
    for pattern in patterns:
        matches = sorted(Path('.').glob(pattern))
        if matches:
            return matches[0]
    return None


@pytest.mark.skipif(_first_sample(('data/raw/gb/*.xlsx',)) is None, reason='GB raw sample missing')
def test_rust_gb_workbook_matches_python_oracle(tmp_path: Path) -> None:
    input_path = _first_sample(('data/raw/gb/*.xlsx',))
    assert input_path is not None
    python_output = tmp_path / 'python-gb.xlsx'
    rust_output = tmp_path / 'rust-gb.xlsx'

    # Keep this test explicit while the oracle path is finalized: use the current service path
    # once it can accept direct input/output paths for isolated temp output.
    pytest.skip('Enable after oracle_runner can direct Python output path without touching data/processed')

    report = compare_workbooks(python_output, rust_output)
    assert report['passed'], report['errors']


@pytest.mark.skipif(_first_sample(('data/raw/sk/*.xlsx',)) is None, reason='SK raw sample missing')
def test_rust_sk_workbook_matches_python_oracle(tmp_path: Path) -> None:
    input_path = _first_sample(('data/raw/sk/*.xlsx',))
    assert input_path is not None
    python_output = tmp_path / 'python-sk.xlsx'
    rust_output = tmp_path / 'rust-sk.xlsx'

    pytest.skip('Enable after oracle_runner can direct Python output path without touching data/processed')

    report = compare_workbooks(python_output, rust_output)
    assert report['passed'], report['errors']
```

This file intentionally skips until `oracle_runner.py` can direct Python output to temp paths without writing production processed directories. The next step in this task removes the skip by using `src.services.costing_service.run_costing_request`.

- [ ] **Step 5: Replace pytest skips with service-based oracle execution**

Modify `tests/rust_oracle/oracle_runner.py` so `run_python_oracle` uses the service API:

```python
from __future__ import annotations

import subprocess
from pathlib import Path

from src.services.costing_service import CostingRunRequest, ServiceStatus, run_costing_request


def run_python_oracle(pipeline: str, input_path: Path, output_path: Path) -> None:
    output_dir = output_path.parent
    request = CostingRunRequest(
        pipeline=pipeline,
        input_path=input_path,
        output_dir=output_dir,
        benchmark=True,
        overwrite_confirmed=True,
    )
    result = run_costing_request(request)
    if result.status != ServiceStatus.SUCCEEDED:
        raise AssertionError(f'python oracle failed: {result.message} {result.technical_detail}')
    if result.workbook_path is None or not result.workbook_path.exists():
        raise AssertionError('python oracle did not create workbook')
    result.workbook_path.replace(output_path)


def run_rust_cli(pipeline: str, input_path: Path, output_path: Path) -> None:
    completed = subprocess.run(
        [
            'cargo',
            'run',
            '--quiet',
            '--manifest-path',
            'rust/Cargo.toml',
            '-p',
            'costing-calculate',
            '--',
            pipeline,
            '--input',
            str(input_path),
            '--output',
            str(output_path),
            '--benchmark',
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise AssertionError(f'rust cli failed\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}')
    if not output_path.exists():
        raise AssertionError(f'rust cli did not create expected workbook: {output_path}')
```

Modify `tests/test_full_rust_cli_oracle.py` to call the runner and remove skips inside test bodies:

```python
from __future__ import annotations

from pathlib import Path

import pytest

from tests.rust_oracle.oracle_runner import run_python_oracle, run_rust_cli
from tests.rust_oracle.workbook_compare import compare_workbooks


def _first_sample(patterns: tuple[str, ...]) -> Path | None:
    for pattern in patterns:
        matches = sorted(Path('.').glob(pattern))
        if matches:
            return matches[0]
    return None


@pytest.mark.skipif(_first_sample(('data/raw/gb/*.xlsx',)) is None, reason='GB raw sample missing')
def test_rust_gb_workbook_matches_python_oracle(tmp_path: Path) -> None:
    input_path = _first_sample(('data/raw/gb/*.xlsx',))
    assert input_path is not None
    python_output = tmp_path / 'python-gb.xlsx'
    rust_output = tmp_path / 'rust-gb.xlsx'

    run_python_oracle('gb', input_path, python_output)
    run_rust_cli('gb', input_path, rust_output)

    report = compare_workbooks(python_output, rust_output)
    assert report['passed'], report['errors']


@pytest.mark.skipif(_first_sample(('data/raw/sk/*.xlsx',)) is None, reason='SK raw sample missing')
def test_rust_sk_workbook_matches_python_oracle(tmp_path: Path) -> None:
    input_path = _first_sample(('data/raw/sk/*.xlsx',))
    assert input_path is not None
    python_output = tmp_path / 'python-sk.xlsx'
    rust_output = tmp_path / 'rust-sk.xlsx'

    run_python_oracle('sk', input_path, python_output)
    run_rust_cli('sk', input_path, rust_output)

    report = compare_workbooks(python_output, rust_output)
    assert report['passed'], report['errors']
```

- [ ] **Step 6: Run verification**

Run:

```powershell
uv run python -m pytest tests/test_full_rust_cli_oracle.py -q --basetemp .pytest-tmp/rust-oracle
```

Expected:

- If GB/SK raw samples exist, tests execute and expose Rust/Python differences.
- If a sample is missing, only that pipeline is skipped.
- A failure should include a concrete workbook diff, not only a non-zero return code.

- [ ] **Step 7: Commit**

```powershell
git add -- tests/rust_oracle tests/test_full_rust_cli_oracle.py tests/contracts/README.md
git commit -m "test(rust): compare rust cli against python oracle"
```

---

### Task 11: Full Contract Parity and Benchmark Verdict

**Files:**
- Create: `tests/rust_oracle/benchmark.py`
- Create: `tests/test_full_rust_cli_benchmark.py`
- Modify: `tests/test_full_rust_cli_oracle.py`
- Modify: Rust modules from Tasks 4-9 until oracle tests pass.

**Interfaces:**
- Consumes: Python oracle output and Rust CLI output.
- Produces: median benchmark summary for GB and SK.
- Produces verdict `VALIDATED`, `READER_MISMATCH`, `ETL_MISMATCH`, `ANALYSIS_MISMATCH`, `WORKBOOK_MISMATCH`, or `PERFORMANCE_REGRESSION`.

- [ ] **Step 1: Add benchmark helper**

Create `tests/rust_oracle/benchmark.py`:

```python
from __future__ import annotations

import statistics
import time
from dataclasses import dataclass
from pathlib import Path

from tests.rust_oracle.oracle_runner import run_python_oracle, run_rust_cli
from tests.rust_oracle.workbook_compare import compare_workbooks


@dataclass(frozen=True)
class BenchmarkResult:
    pipeline: str
    python_median_seconds: float
    rust_median_seconds: float
    validation_passed: bool
    verdict: str


def run_same_machine_benchmark(pipeline: str, input_path: Path, tmp_path: Path, repeats: int = 3) -> BenchmarkResult:
    python_seconds: list[float] = []
    rust_seconds: list[float] = []
    validation_passed = True

    for idx in range(repeats):
        python_output = tmp_path / f'python-{pipeline}-{idx}.xlsx'
        rust_output = tmp_path / f'rust-{pipeline}-{idx}.xlsx'

        start = time.perf_counter()
        run_python_oracle(pipeline, input_path, python_output)
        python_seconds.append(time.perf_counter() - start)

        start = time.perf_counter()
        run_rust_cli(pipeline, input_path, rust_output)
        rust_seconds.append(time.perf_counter() - start)

        report = compare_workbooks(python_output, rust_output)
        validation_passed = validation_passed and bool(report['passed'])

    python_median = statistics.median(python_seconds)
    rust_median = statistics.median(rust_seconds)
    verdict = classify_verdict(validation_passed, python_median, rust_median)
    return BenchmarkResult(pipeline, python_median, rust_median, validation_passed, verdict)


def classify_verdict(validation_passed: bool, python_median: float, rust_median: float) -> str:
    if not validation_passed:
        return 'WORKBOOK_MISMATCH'
    if rust_median > python_median:
        return 'PERFORMANCE_REGRESSION'
    return 'VALIDATED'
```

- [ ] **Step 2: Add benchmark tests**

Create `tests/test_full_rust_cli_benchmark.py`:

```python
from __future__ import annotations

from pathlib import Path

import pytest

from tests.rust_oracle.benchmark import classify_verdict, run_same_machine_benchmark


def _first_sample(pattern: str) -> Path | None:
    matches = sorted(Path('.').glob(pattern))
    return matches[0] if matches else None


def test_classify_verdict_requires_validation_and_no_regression() -> None:
    assert classify_verdict(True, 10.0, 9.0) == 'VALIDATED'
    assert classify_verdict(False, 10.0, 9.0) == 'WORKBOOK_MISMATCH'
    assert classify_verdict(True, 10.0, 10.1) == 'PERFORMANCE_REGRESSION'


@pytest.mark.skipif(_first_sample('data/raw/gb/*.xlsx') is None, reason='GB raw sample missing')
def test_gb_rust_benchmark_validated(tmp_path: Path) -> None:
    input_path = _first_sample('data/raw/gb/*.xlsx')
    assert input_path is not None
    result = run_same_machine_benchmark('gb', input_path, tmp_path, repeats=3)
    assert result.verdict == 'VALIDATED'


@pytest.mark.skipif(_first_sample('data/raw/sk/*.xlsx') is None, reason='SK raw sample missing')
def test_sk_rust_benchmark_validated(tmp_path: Path) -> None:
    input_path = _first_sample('data/raw/sk/*.xlsx')
    assert input_path is not None
    result = run_same_machine_benchmark('sk', input_path, tmp_path, repeats=3)
    assert result.verdict == 'VALIDATED'
```

- [ ] **Step 3: Run benchmark tests and collect mismatches**

Run:

```powershell
uv run python -m pytest tests/test_full_rust_cli_oracle.py tests/test_full_rust_cli_benchmark.py -q --basetemp .pytest-tmp/full-rust-oracle
```

Expected during first execution:

- The tests may fail with concrete diff messages.
- Classify each failure using the earliest failing layer:
  - reader snapshot mismatch -> fix `costing-xlsx/src/reader.rs`
  - normalized row mismatch -> fix `costing-core/src/normalize.rs`
  - fact/qty mismatch -> fix `costing-core/src/fact.rs`
  - anomaly mismatch -> fix `costing-core/src/anomaly.rs` or `scoring.rs`
  - workbook shape/style mismatch -> fix `costing-xlsx/src/writer.rs` or `presentation.rs`

- [ ] **Step 4: Iterate only against failing layer**

For each failure, run the smallest relevant command before re-running the full oracle:

```powershell
cargo test --manifest-path rust/Cargo.toml -p costing-core fact::tests
cargo test --manifest-path rust/Cargo.toml -p costing-core anomaly::tests
cargo test --manifest-path rust/Cargo.toml -p costing-xlsx
uv run python -m pytest tests/test_full_rust_cli_oracle.py::<failing-test-name> -q --basetemp .pytest-tmp/full-rust-single
```

Expected:

- The targeted failure passes before running the full suite.
- No fix should relax workbook comparison rules without an explicit spec update.

- [ ] **Step 5: Run full verification suite**

Run:

```powershell
cargo fmt --manifest-path rust/Cargo.toml --check
cargo test --manifest-path rust/Cargo.toml
uv run python -m pytest tests/test_full_rust_cli_oracle.py tests/test_full_rust_cli_benchmark.py -q --basetemp .pytest-tmp/full-rust-final
uv run python -m pytest tests -q --basetemp .pytest-tmp/python-regression
uv run python -m ruff check src tests
uv run python -m ruff format src tests --check
```

Expected:

- All Rust tests pass.
- GB and SK oracle tests must actually pass for final cutover evidence; a missing sample is `BLOCKED_ENVIRONMENT`, and a `skip` is not validation evidence.
- GB and SK benchmark verdicts are `VALIDATED` when raw samples exist.
- Existing Python regression suite remains green.

- [ ] **Step 6: Commit**

```powershell
git add -- rust tests/rust_oracle tests/test_full_rust_cli_oracle.py tests/test_full_rust_cli_benchmark.py
git commit -m "test(rust): validate full rust cli parity"
```

---

### Task 12: Documentation, Entrypoint Cutover Plan, and Python Retirement Ledger

**Files:**
- Modify: `README.md`
- Modify: `AGENTS.md`
- Modify: `tests/contracts/README.md`
- Create: `docs/rust_rewrite_validation.md`
- Create: `docs/python_retirement_after_rust.md`

**Interfaces:**
- Consumes: successful Task 11 validation evidence.
- Produces: reader-facing commands for Rust CLI.
- Produces: explicit Python retirement ledger; no Python code deletion in this task.

- [ ] **Step 1: Write validation document**

Create `docs/rust_rewrite_validation.md`:

```markdown
# Rust Rewrite Validation

## Status

Rust CLI is the validated replacement target for the default GB/SK costing ETL path after the full parity suite passes.

## Required Commands

```powershell
cargo fmt --manifest-path rust/Cargo.toml --check
cargo test --manifest-path rust/Cargo.toml
uv run python -m pytest tests/test_full_rust_cli_oracle.py tests/test_full_rust_cli_benchmark.py -q --basetemp .pytest-tmp/full-rust-final
uv run python -m pytest tests -q --basetemp .pytest-tmp/python-regression
uv run python -m ruff check src tests
uv run python -m ruff format src tests --check
```

## Manual Check

- Open one GB Rust output workbook in Excel or WPS.
- Open one SK Rust output workbook in Excel or WPS.
- Confirm the workbook contains exactly 3 sheets.
- Confirm `成本分析产品维度` is absent.
- Confirm filters and frozen panes are visible on all sheets.
```

- [ ] **Step 2: Write Python retirement ledger**

Create `docs/python_retirement_after_rust.md`:

```markdown
# Python Retirement After Rust Validation

This document lists Python code that may be removed only after Rust CLI is validated for GB and SK.

## Keep Until Rust Is Validated

- `main.py`
- `src/etl/`
- `src/analytics/`
- `src/excel/`
- `src/services/costing_service.py`
- `tests/contracts/`
- `tests/rust_oracle/`

## Product Dimension Retirement

Rust does not implement `成本分析产品维度`.

After Rust validation, remove the Python legacy product-dimension helpers in a separate change:

- `src/analytics/table_rendering.py` product anomaly section helpers
- `src/excel/product_anomaly_writer.py`
- Tests that only protect the retired product-dimension sheet

## Removal Rule

Do not delete any Python oracle code in the same commit that validates Rust. Deletion requires a separate review after Rust validation evidence is attached.
```

- [ ] **Step 3: Update README with Rust commands**

Modify the usage section in `README.md` to include:

```markdown
## Rust CLI

Rust CLI is the current primary entrypoint after full GB/SK validation:

```powershell
cargo run --manifest-path rust/Cargo.toml -p costing-calculate -- gb --input data/raw/gb/<file>.xlsx --output data/processed/gb/<file>_处理后.xlsx
cargo run --manifest-path rust/Cargo.toml -p costing-calculate -- sk --input data/raw/sk/<file>.xlsx --output data/processed/sk/<file>_处理后.xlsx
cargo run --manifest-path rust/Cargo.toml -p costing-calculate -- gb --input data/raw/gb/<file>.xlsx --check-only --benchmark
cargo run --manifest-path rust/Cargo.toml -p costing-calculate -- sk --input data/raw/sk/<file>.xlsx --check-only --benchmark
```

The default workbook remains exactly 3 sheets:

- `成本计算单总表`
- `成本计算单数量聚合维度`
- `成本分析工单维度`

`成本分析产品维度` is not part of the Rust system.
```

- [ ] **Step 4: Update AGENTS current business rule after validation**

Modify the current business rules in `AGENTS.md` so the workbook contract says:

```markdown
- 成本核算 Rust CLI 默认按顺序输出以下 3 张 Sheet：`成本计算单总表`、`成本计算单数量聚合维度`、`成本分析工单维度`。
- `成本分析产品维度` 不属于 Rust 新系统输出契约；Python legacy helper 只作为退场前历史代码存在。
```

- [ ] **Step 5: Update contracts README**

Append to `tests/contracts/README.md`:

```markdown
## Rust Oracle Parity

The Rust rewrite must pass the Python oracle comparison before it can replace the Python default path.

Required checks:

- `tests/test_full_rust_cli_oracle.py`
- `tests/test_full_rust_cli_benchmark.py`

The validator rejects any Rust workbook that contains `成本分析产品维度`.
```

- [ ] **Step 6: Run final verification**

Run:

```powershell
cargo fmt --manifest-path rust/Cargo.toml --check
cargo test --manifest-path rust/Cargo.toml
uv run python -m pytest tests/test_full_rust_cli_oracle.py tests/test_full_rust_cli_benchmark.py -q --basetemp .pytest-tmp/full-rust-docs
uv run python -m pytest tests -q --basetemp .pytest-tmp/python-regression-docs
uv run python -m ruff check src tests
uv run python -m ruff format src tests --check
```

Expected:

- Rust checks pass.
- Oracle parity remains green.
- Python regression remains green.
- Ruff checks pass.

- [ ] **Step 7: Commit**

```powershell
git add -- README.md AGENTS.md tests/contracts/README.md docs/rust_rewrite_validation.md docs/python_retirement_after_rust.md
git commit -m "docs(rust): document validated rust cli cutover"
```

---

## Self-Review Checklist

- Spec coverage:
  - Rust CLI primary entrypoint: Tasks 1, 2, 12.
  - GB + SK first version: Tasks 1, 5, 10, 11.
  - Direct `.xlsx` read: Task 3.
  - ETL normalize/fill/split: Task 4.
  - Quantity aggregation and standalone costs: Task 5.
  - Modified Z-score and work-order anomaly: Tasks 6, 7.
  - Three-sheet payload and product dimension rejection: Tasks 8, 9, 10.
  - Python oracle: Tasks 10, 11.
  - Benchmark verdict: Task 11.
  - Documentation and Python retirement separation: Task 12.
- Placeholder scan:
  - The plan must not contain unfinished marker words, vague implementation gates, or deferred detail slots.
  - Sample-path commands use `<actual-gb-file>` only where the executor must choose an existing local raw file; automated tests use glob discovery.
- Type consistency:
  - `CellValue`, `TableRow`, `RawWorkbook`, `SplitResult`, `FactBundle`, `SheetModel`, `WorkbookPayload`, and `RunSummary` are introduced before use.
  - `PipelineConfig::for_name`, `normalize_workbook`, `split_detail_and_qty`, `build_fact_bundle`, `build_workbook_payload`, and `write_workbook` are introduced before later tasks consume them.

## Execution Choice

Plan complete once this file is saved. Two execution options:

1. **Subagent-Driven (recommended)** - dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** - execute tasks in this session using executing-plans, batch execution with checkpoints.

# Rust 输出 Phase 0A–3 Writer 持续优化实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 冻结可复现的 Phase 0A 正常模式基线，增加低开销 writer 观测，优化 Standard writer 热路径，确定性选择压缩 feature，并用受控临时目录启用自适应 LowMemory，使 SK normal PWS 中位数不超过 2.0 GiB，同时守住 correctness、GB 防回退和输出大小门禁。

**Architecture:** Phase 0A 固定未启用候选 feature 的 reference EXE；Phase 0B 只增加 `WorkbookWriteReport`；Phase 1 通过 `SheetWritePlan/ColumnWritePlan` 把列名、BTreeMap 和 Format 构建移出逐格热路径；Phase 2 用四个独立 target-dir 测 A/B/C/D；Phase 3 才激活按 Sheet 槽位选择的 LowMemory、磁盘门禁和 `TempWorkspace`。每个性能结论都由 Phase 0H 同批 harness 生成，并可独立保留或回退。

**Tech Stack:** Rust 2021、patched `rust_xlsxwriter 0.96.0` 精确 Git revision、`tempfile`、`windows-sys`、Cargo feature、Python Phase 0H harness、MSVC/dumpbin、Windows 10/11 x64。

## Global Constraints

- 本计划只能在 Phase -1D 和 Phase 0H exit checklist 全通过后执行。
- Phase 0A reference 只包含 Phase -1D 依赖/错误安全改动；构建固定 `--no-default-features`，不得启用 `low-memory`、`zlib` 或 `zmij`。
- Phase 0A manifest 必须由用户人工确认后单独提交；确认前不得开始 Phase 0B。
- `total` 继续表示 ingest 到内存 payload，不含 export；不得改变既有含义。
- `writer_populate` 覆盖 Workbook/Sheet 创建、元数据、表头、数据写入和 LowMemory 临时 XML；`xlsx_save` 覆盖 `save_to_writer`；`export` 继续覆盖 writer 全调用。
- Phase 1 即使建立 mode planner，也必须强制所有 Sheet 为 Standard。
- Phase 2 feature 专属收益只按 SK normal mode；GB 只用于 correctness、wall/PWS 和 bytes 防回退。
- Phase 2 A/B/C/D 使用独立 target-dir；禁止覆盖后复制 EXE 或用 package 是否出现推断 feature 是否启用。
- Phase 3 只有 `row_count.saturating_mul(column_count) >= 5_000_000` 的 Sheet 使用 LowMemory；其余仍使用 Standard。
- LowMemory 不访问系统 `%TEMP%`，不自动降级 Standard，不自动重试。
- 任一候选 correctness 失败立即拒绝，不计算性能结论。
- 所有比率的分母必须是同批新跑 binary；Phase 0A 历史 manifest 只提供固定 bytes 和环境漂移校准。
- 性能代码提交与性能证据提交分开；证据不通过时用 `git revert` 保留审计链，不重采到“碰巧通过”。

---

## File Structure

### Production Rust files

- Modify: `rust/crates/costing-core/src/model.rs` — success `request_id`、`output_size_bytes`。
- Modify: `rust/crates/costing-xlsx/src/writer.rs` — report、planned write loop、mode-aware writer。
- Create: `rust/crates/costing-xlsx/src/write_plan.rs` — `SheetWritePlan/ColumnWritePlan` and pure selector。
- Create: `rust/crates/costing-xlsx/src/temp_workspace.rs` — controlled directory lifecycle。
- Create: `rust/crates/costing-xlsx/src/disk_space.rs` — Windows free-space query and boundary。
- Modify: `rust/crates/costing-xlsx/src/lib.rs` — private modules and public writer exports only。
- Modify: `rust/crates/costing-cli/src/run.rs` — writer report to runtime summary。
- Modify: `rust/crates/costing-cli/tests/cli_benchmark.rs`
- Modify: `rust/crates/costing-oracle-tests/tests/runtime_contract.rs`
- Modify as needed after Phase -1D: `rust/Cargo.toml`、`rust/Cargo.lock`、`rust/crates/costing-xlsx/Cargo.toml`、`rust/crates/costing-cli/Cargo.toml`。

### Benchmark/release support

- Modify: `tests/rust_oracle/benchmark_protocol.py`
- Modify: `tests/rust_oracle/phase0_harness.py`
- Modify: `tests/rust_oracle/test_phase0_harness.py`
- Modify: `tests/rust_oracle/evidence.py`
- Modify: `tests/rust_oracle/test_evidence.py`
- Create: `tests/rust_oracle/verify_pe_imports.ps1`
- Create: `tests/rust_oracle/run_clean_windows_smoke.ps1`
- Create: `tests/rust_oracle/test_windows_release_scripts.py`
- Generate through sanitizer: `docs/performance/baselines/2026-07-11-windows-x64-phase0a.json`
- Generate through sanitizer: `docs/performance/runs/phase0b/`、`phase1/`、`phase2/`、`phase3/` JSON evidence。

## Stable Writer Interfaces

Phase 0B finalizes this report:

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct WorkbookWriteReport {
    pub writer_populate_seconds: f64,
    pub xlsx_save_seconds: f64,
    pub output_size_bytes: u64,
}

pub fn write_workbook(
    context: &WriterContext,
    path: &Path,
    payload: &WorkbookPayload,
) -> Result<WorkbookWriteReport, WriterError>;
```

Internal signatures are fixed so raw I/O remains available to the Phase -1D adapter:

```rust
pub(crate) fn build_sheet_write_plan(
    sheet: &SheetModel,
    policy: SheetMemoryPolicy,
) -> Result<SheetWritePlan, WriterPrimaryError>;

impl TempWorkspace {
    pub(crate) fn create(output_parent: &Path, request_id: &str) -> std::io::Result<Self>;
    pub(crate) fn path(&self) -> &Path;
    pub(crate) fn close(self) -> std::io::Result<()>;
}

pub(crate) fn available_space_bytes(path: &Path) -> std::io::Result<u64>;
pub(crate) fn ensure_low_memory_space(
    available_bytes: u64,
    required_bytes: u64,
) -> Result<(), WriterPrimaryError>;
```

Task 8 extends `WriterPrimaryError` with a typed `InsufficientDiskSpace { available_bytes, required_bytes }` non-I/O variant. The writer boundary assigns `ErrorStage`；helpers do not stringify or construct CLI errors.

Phase 3 adds options without changing report/error semantics:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkbookWriteOptions {
    memory_policy: SheetMemoryPolicy,
}

impl WorkbookWriteOptions {
    pub fn standard_only() -> Self;
    pub fn for_compiled_features() -> Self;
}

pub fn write_workbook(
    context: &WriterContext,
    path: &Path,
    payload: &WorkbookPayload,
    options: &WorkbookWriteOptions,
) -> Result<WorkbookWriteReport, WriterError>;
```

Normal CLI summary must contain a nonempty `request_id` and non-null `output_size_bytes`, plus `writer_populate`、`xlsx_save`、`export` and existing `total`. Check-only also contains the run's nonempty `request_id`, but has `output_size_bytes=null` and no writer/export keys.

## Task 1: Build and Freeze the Phase 0A Reference

**Files:**
- Generate: `docs/performance/baselines/2026-07-11-windows-x64-phase0a.json`
- No production-code changes。

**Interfaces:**
- Consumes: Phase -1D/0H verified HEAD and exact fork revision。
- Produces: immutable reference EXE SHA and sanitized GB/SK calibration manifest。

- [ ] **Step 1: Assert a clean execution worktree and required input variables**

```powershell
if (git status --porcelain) { throw 'Phase 0A capture requires a clean execution worktree' }
if (-not $env:COSTING_GB_SAMPLE) { throw 'COSTING_GB_SAMPLE is required' }
if (-not $env:COSTING_SK_SAMPLE) { throw 'COSTING_SK_SAMPLE is required' }
if (-not (Test-Path -LiteralPath $env:COSTING_GB_SAMPLE -PathType Leaf)) { throw 'GB sample is unavailable' }
if (-not (Test-Path -LiteralPath $env:COSTING_SK_SAMPLE -PathType Leaf)) { throw 'SK sample is unavailable' }
```

- [ ] **Step 2: Build once in the fixed target directory and record its SHA**

```powershell
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/perf-builds/phase0a/reference --no-default-features
$Phase0AExe = (Resolve-Path 'rust/target/perf-builds/phase0a/reference/x86_64-pc-windows-msvc/release/costing-calculate.exe').Path
$Phase0ASha = (Get-FileHash -Algorithm SHA256 -LiteralPath $Phase0AExe).Hash.ToLowerInvariant()
if ($Phase0ASha -notmatch '^[0-9a-f]{64}$') { throw 'invalid Phase 0A EXE SHA' }
```

Do not rebuild this target after capture. A different binary SHA is a different reference and invalidates the manifest.

- [ ] **Step 3: Resolve the exact fork revision from Cargo metadata**

```powershell
$Metadata = cargo metadata --locked --manifest-path rust/Cargo.toml --format-version 1 | ConvertFrom-Json
$Xlsx = @($Metadata.packages | Where-Object name -eq 'rust_xlsxwriter')
if ($Xlsx.Count -ne 1) { throw 'expected exactly one rust_xlsxwriter package' }
$ForkRevision = [regex]::Match($Xlsx[0].source, '#([0-9a-f]{40})$').Groups[1].Value
if ($ForkRevision -notmatch '^[0-9a-f]{40}$') { throw 'Cargo metadata lacks the exact fork revision' }
```

- [ ] **Step 4: Capture 1 warm-up + 5 formal wall and PWS rounds per pipeline**

```powershell
uv run python -m tests.rust_oracle.phase0_harness phase0a `
  --gb-input "$env:COSTING_GB_SAMPLE" `
  --sk-input "$env:COSTING_SK_SAMPLE" `
  --reference-executable $Phase0AExe `
  --fork-revision $ForkRevision `
  --local-root rust/target/perf-local/phase0a `
  --output docs/performance/baselines/2026-07-11-windows-x64-phase0a.json
```

Expected: all ten formal wall runs and ten formal PWS runs succeed across GB/SK; each normal output passes runtime and OOXML oracle; all workbooks are deleted in `finally`. The sanitized manifest records raw values, medians, external output bytes, runtime/error counts, Sheet dimensions, machine specification aliases and hashes only.

- [ ] **Step 5: Scan and stop for explicit user confirmation**

```powershell
uv run python -m tests.rust_oracle.evidence scan --root docs/performance --staged
$BaselinePath = 'docs/performance/baselines/2026-07-11-windows-x64-phase0a.json'
Get-Content -Raw -Encoding UTF8 -LiteralPath $BaselinePath
(Get-FileHash -Algorithm SHA256 -LiteralPath $BaselinePath).Hash.ToLowerInvariant()
```

STOP. Present the complete sanitized manifest text, its SHA-256, Phase 0A EXE SHA and gate summary to the user. Ordinary `git diff` is intentionally not used because the new manifest is untracked. Do not stage, commit or begin Phase 0B until the user explicitly approves this generated evidence.

- [ ] **Step 6: After approval, commit only the immutable baseline**

```powershell
git add -- docs/performance/baselines/2026-07-11-windows-x64-phase0a.json
uv run python -m tests.rust_oracle.evidence scan --root docs/performance --staged
git diff --cached --name-only
git diff --cached --check
git commit -m "docs(perf): freeze phase0a windows baseline"
```

## Task 2: Add Phase 0B Writer Observability Only

**Files:**
- Modify: `rust/crates/costing-xlsx/src/writer.rs`
- Modify: `rust/crates/costing-core/src/model.rs`
- Modify: `rust/crates/costing-cli/src/run.rs`
- Modify: `rust/crates/costing-cli/tests/cli_benchmark.rs`
- Modify: `rust/crates/costing-oracle-tests/tests/runtime_contract.rs`
- Modify: `tests/rust_oracle/phase0_harness.py`
- Modify: `tests/rust_oracle/test_phase0_harness.py`

**Interfaces:**
- Produces: `WorkbookWriteReport` and success-summary fields。
- Must preserve: Standard writer strategy and workbook bytes semantics。

- [ ] **Step 1: Add RED writer report tests**

Add exact tests:

```text
writer::tests::write_workbook_reports_populate_save_and_output_size
run::tests::run_reports_request_id_writer_breakdown_and_output_size_for_normal_mode
run::tests::run_omits_writer_breakdown_and_output_size_for_check_only
cli_benchmark::normal_benchmark_reports_writer_breakdown_and_output_size
cli_benchmark::check_only_benchmark_omits_writer_breakdown_and_output_size
test_phase0a_runtime_schema_does_not_require_writer_breakdown
test_instrumented_runtime_schema_requires_writer_breakdown_and_output_size
```

Run:

```powershell
cargo test --locked --manifest-path rust/Cargo.toml -p costing-xlsx --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features writer::tests::write_workbook_reports_populate_save_and_output_size -- --exact
cargo test --locked --manifest-path rust/Cargo.toml -p costing-calculate --test cli_benchmark --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
```

Expected: RED because the report/summary fields do not exist.

- [ ] **Step 2: Implement precise timing boundaries**

Start `writer_populate` immediately before `Workbook::new()` and stop after all Sheet metadata/header/data calls succeed. Start `xlsx_save` immediately before `save_to_writer()` and stop after it succeeds. Flush and drop the final file, read nonzero metadata, then set `output_size_bytes`.

The CLI inserts report seconds into `RunSummary.stage_timings.stages` under exact keys `writer_populate` and `xlsx_save`; `export` stays the outer timer. Add `RunSummary.request_id: String` and `output_size_bytes: Option<u64>`；all successful runs occur after request creation, while check-only uses `None` only for output size.

- [ ] **Step 3: Verify no strategy change and commit code**

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --locked --manifest-path rust/Cargo.toml -p costing-xlsx --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
cargo test --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
cargo test --locked --manifest-path rust/Cargo.toml --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
uv run python -m pytest tests/rust_oracle/test_phase0_harness.py -q --basetemp .pytest-tmp/phase0b-schema
git add -- rust/crates/costing-xlsx/src/writer.rs rust/crates/costing-core/src/model.rs rust/crates/costing-cli/src/run.rs rust/crates/costing-cli/tests/cli_benchmark.rs rust/crates/costing-oracle-tests/tests/runtime_contract.rs tests/rust_oracle/phase0_harness.py tests/rust_oracle/test_phase0_harness.py
git diff --cached --name-only
git diff --cached --check
git commit -m "feat(xlsx): report writer phase timings"
```

- [ ] **Step 4: Build and measure Phase 0B against the same-batch Phase 0A reference**

```powershell
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/perf-builds/phase0b/instrumented --no-default-features
$Phase0AExe = 'rust/target/perf-builds/phase0a/reference/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$Phase0BExe = 'rust/target/perf-builds/phase0b/instrumented/x86_64-pc-windows-msvc/release/costing-calculate.exe'
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline gb --input "$env:COSTING_GB_SAMPLE" --reference-executable $Phase0AExe --candidate-executable $Phase0BExe --reference-label phase0a --candidate-label phase0b --comparison-profile phase0b-vs-phase0a --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/gb/.perf-runs --evidence-path docs/performance/runs/phase0b/gb.json
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline sk --input "$env:COSTING_SK_SAMPLE" --reference-executable $Phase0AExe --candidate-executable $Phase0BExe --reference-label phase0a --candidate-label phase0b --comparison-profile phase0b-vs-phase0a --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/sk/.perf-runs --evidence-path docs/performance/runs/phase0b/sk.json
```

Gate: both `candidate_wall / same_batch_phase0a_wall <= 1.02`, correctness passes, and any 3% critical result has mandatory N=10 evidence. If rejected, optimize only the observation implementation and repeat from a new code commit; do not enter Phase 1.

- [ ] **Step 5: Commit the sanitized Phase 0B evidence**

```powershell
git add -- docs/performance/runs/phase0b/gb.json docs/performance/runs/phase0b/sk.json
uv run python -m tests.rust_oracle.evidence scan --root docs/performance --staged
git diff --cached --check
git commit -m "docs(perf): validate phase0b observability"
```

## Task 3: Precompute the Standard Sheet and Column Write Plans

**Files:**
- Create: `rust/crates/costing-xlsx/src/write_plan.rs`
- Modify: `rust/crates/costing-xlsx/src/lib.rs`
- Modify: `rust/crates/costing-xlsx/src/writer.rs`

**Interfaces:**
- Produces: row-width validated `SheetWritePlan` and cached `ColumnWritePlan`。
- Phase 1 policy: `SheetMemoryPolicy::StandardOnly` only。

- [ ] **Step 1: Add RED planner and semantic tests**

Add exact tests:

```text
write_plan::tests::standard_plan_precomputes_one_entry_per_column
write_plan::tests::standard_plan_caches_numeric_and_text_formats
write_plan::tests::standard_plan_rejects_row_wider_than_columns
write_plan::tests::standard_plan_uses_saturating_cell_slots
writer::tests::blank_cell_is_skipped_before_hot_path_lookup
writer::tests::planned_writer_preserves_decimal_text_date_and_number_formats
writer::tests::phase1_writer_uses_standard_mode_for_every_sheet
```

The row-width validation test is required because blank fast-path must not silently accept an over-wide invalid row.

- [ ] **Step 2: Implement the minimum plan types**

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SheetWriteMode {
    Standard,
    #[cfg(feature = "low-memory")]
    LowMemory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SheetMemoryPolicy {
    StandardOnly,
}

pub(crate) struct ColumnWritePlan {
    pub(crate) excel_col: u16,
    pub(crate) text_format: Format,
    pub(crate) number_format: Option<Format>,
    pub(crate) width: Option<f64>,
}

pub(crate) struct SheetWritePlan {
    pub(crate) mode: SheetWriteMode,
    pub(crate) cell_slots: usize,
    pub(crate) columns: Vec<ColumnWritePlan>,
}
```

`build_sheet_write_plan()` validates every row width once, resolves Chinese column names and `number_formats` once per column, and constructs reusable Format values. In the data loop, check `CellValue::Blank` first, then fetch `column_plan`; no column-name/BTreeMap lookup and no new Format construction may remain in the nonblank cell hot path. Keep the Phase 0B Sheet-metadata call order unchanged for single-variable attribution；the metadata-before-row reorder belongs to Task 8 when LowMemory is activated. Continue using `add_worksheet()` for every Sheet.

- [ ] **Step 3: Verify and commit code**

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --locked --manifest-path rust/Cargo.toml -p costing-xlsx --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
cargo test --locked --manifest-path rust/Cargo.toml --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
git add -- rust/crates/costing-xlsx/src/write_plan.rs rust/crates/costing-xlsx/src/lib.rs rust/crates/costing-xlsx/src/writer.rs
git diff --cached --name-only
git diff --cached --check
git commit -m "perf(xlsx): precompute column write plans"
$Phase1CodeCommit = (git rev-parse HEAD).Trim()
```

- [ ] **Step 4: Run the two required Phase 1 same-batch comparisons**

```powershell
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/perf-builds/phase1/writer --no-default-features
$Phase0AExe = 'rust/target/perf-builds/phase0a/reference/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$Phase0BExe = 'rust/target/perf-builds/phase0b/instrumented/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$Phase1Exe = 'rust/target/perf-builds/phase1/writer/x86_64-pc-windows-msvc/release/costing-calculate.exe'
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline sk --input "$env:COSTING_SK_SAMPLE" --reference-executable $Phase0BExe --candidate-executable $Phase1Exe --reference-label phase0b --candidate-label phase1 --comparison-profile phase1-vs-phase0b --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/sk/.perf-runs --evidence-path docs/performance/runs/phase1/sk-internal.json
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline gb --input "$env:COSTING_GB_SAMPLE" --reference-executable $Phase0AExe --candidate-executable $Phase1Exe --reference-label phase0a --candidate-label phase1 --comparison-profile phase1-vs-phase0a --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/gb/.perf-runs --evidence-path docs/performance/runs/phase1/gb-external.json
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline sk --input "$env:COSTING_SK_SAMPLE" --reference-executable $Phase0AExe --candidate-executable $Phase1Exe --reference-label phase0a --candidate-label phase1 --comparison-profile phase1-vs-phase0a --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/sk/.perf-runs --evidence-path docs/performance/runs/phase1/sk-external.json
```

All gates must hold:

```text
SK writer_populate Phase1 / same-batch Phase0B <= 0.90
SK xlsx_save Phase1 / same-batch Phase0B <= 1.05
GB wall Phase1 / same-batch Phase0A <= 1.05
GB PWS Phase1 / same-batch Phase0A <= 1.05
SK PWS Phase1 / same-batch Phase0A <= 1.05
GB/SK bytes / approved Phase0A bytes <= 1.10
GB/SK correctness = pass
```

- [ ] **Step 5: Keep or revert deterministically**

If all gates pass:

```powershell
git add -- docs/performance/runs/phase1/sk-internal.json docs/performance/runs/phase1/gb-external.json docs/performance/runs/phase1/sk-external.json
uv run python -m tests.rust_oracle.evidence scan --root docs/performance --staged
git diff --cached --check
git commit -m "docs(perf): validate phase1 writer plan"
```

If any gate fails, use the same three-path command to commit sanitized rejected evidence with message `docs(perf): reject phase1 writer plan`, then run:

```powershell
$Phase1CodeCommit = (git log -1 --format=%H --fixed-strings --grep='perf(xlsx): precompute column write plans').Trim()
if ($Phase1CodeCommit -notmatch '^[0-9a-f]{40}$') { throw 'Phase 1 code commit is missing' }
git revert --no-edit $Phase1CodeCommit
```

Continue Phase 2 from Phase 0B code. Record the resulting A binary label in Phase 2 evidence as `phase1` or `phase0b`; never silently retain a rejected optimization.

## Task 4: Prove the Closed Cargo Feature Matrix

**Files:**
- Modify only if Phase -1D wiring differs: `rust/Cargo.toml`、`rust/Cargo.lock`、`rust/crates/costing-xlsx/Cargo.toml`、`rust/crates/costing-cli/Cargo.toml`
- Modify: `tests/rust_oracle/phase0_harness.py`
- Modify: `tests/rust_oracle/test_phase0_harness.py`

**Interfaces:**
- Produces: exact features `low-memory`、`zlib`、`zmij` at the binary package and sanitized Cargo feature graph evidence。

- [ ] **Step 1: Add RED closed-feature validation tests**

Add:

```text
test_feature_graph_requires_single_exact_fork_revision
test_zlib_label_requires_rust_xlsxwriter_zlib_edge
test_zmij_label_requires_rust_xlsxwriter_zmij_edge
test_zmij_transitive_package_without_writer_edge_is_rejected
test_low_memory_label_requires_constant_memory_edge
test_candidate_label_rejects_unapproved_feature_combination
test_phase2_decision_cli_applies_exact_a_b_c_d_table
test_phase2_decision_cli_rejects_missing_or_wrong_evidence_sha
```

- [ ] **Step 2: Reconcile manifests minimally**

The final manifest surface is exactly:

```toml
# costing-xlsx
[features]
default = []
low-memory = ["rust_xlsxwriter/constant_memory", "dep:tempfile", "dep:windows-sys"]
zlib = ["rust_xlsxwriter/zlib"]
zmij = ["rust_xlsxwriter/zmij"]

# costing-calculate
[features]
default = []
low-memory = ["costing-xlsx/low-memory"]
zlib = ["costing-xlsx/zlib"]
zmij = ["costing-xlsx/zmij"]
```

All `costing-xlsx` dependencies use `default-features = false` where applicable, and the CLI dev-dependency uses `rust_xlsxwriter.workspace = true`. If Phase -1D already produced this exact state, do not make a no-op manifest commit.

- [ ] **Step 3: Implement the feature-tree parser and closed evidence builder**

Add `parse_cargo_feature_tree(raw: str, candidate_label: ClosedBinaryLabel) -> CargoFeatureTreeEvidence`. It parses normalized package/revision/feature edges, requires the exact fork SHA, distinguishes the transitive `zmij` package from the `rust_xlsxwriter/zmij` feature edge, and validates the exact label-to-feature mapping. `EvidenceSanitizer.build_cargo_feature_tree()` reconstructs only the closed fields and local log SHA. Implement `decide-phase2` to consume only sanitized evidence hashes, apply the approved A/B/C/D table and write the fixed handoff schema without numeric CLI arguments.

```powershell
uv run python -m pytest tests/rust_oracle/test_phase0_harness.py tests/rust_oracle/test_evidence.py -q --basetemp .pytest-tmp/phase2-feature-parser -k "feature_graph or zlib_label or zmij or low_memory_label or candidate_label"
```

Expected: all RED tests from Step 1 are GREEN before any candidate evidence is captured.

- [ ] **Step 4: Build A/B/C/D into independent directories**

```powershell
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/perf-builds/phase2/A --no-default-features
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/perf-builds/phase2/B --no-default-features --features zlib
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/perf-builds/phase2/C --no-default-features --features zmij
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/perf-builds/phase2/D --no-default-features --features "zlib,zmij"
```

For each exact feature set, run `cargo tree --locked -e features` with the same target and options. Raw output remains local; `EvidenceSanitizer.build_cargo_feature_tree()` records only package names, exact fork revision, explicit writer feature edges, EXE SHA and local-log SHA.

- [ ] **Step 5: Verify and commit feature tooling**

```powershell
uv run python -m pytest tests/rust_oracle/test_phase0_harness.py -q --basetemp .pytest-tmp/phase2-features
cargo test --locked --manifest-path rust/Cargo.toml --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
git add -- tests/rust_oracle/phase0_harness.py tests/rust_oracle/test_phase0_harness.py tests/rust_oracle/evidence.py tests/rust_oracle/test_evidence.py
git diff --cached --name-only
git diff --cached --check
git commit -m "test(perf): validate writer feature matrix"
```

If manifests required real changes, stage their four exact paths in a separate `build(rust): close writer feature matrix` commit before the test tooling commit.

## Task 5: Add Reusable PE and Clean-Windows Smoke Gates

**Files:**
- Create: `tests/rust_oracle/verify_pe_imports.ps1`
- Create: `tests/rust_oracle/run_clean_windows_smoke.ps1`
- Create: `tests/rust_oracle/test_windows_release_scripts.py`
- Modify: `tests/rust_oracle/evidence.py`
- Modify: `tests/rust_oracle/test_evidence.py`

**Interfaces:**
- Produces: local raw PE/smoke logs and sanitized `PeImportsEvidence`/`SmokeSummaryEvidence`。
- Consumes: Phase 0H sanitized fixture and a single candidate EXE。

Use these closed PowerShell parameters:

```powershell
# verify_pe_imports.ps1
param(
    [string] $CandidateExecutable,
    [string] $Phase0AExecutable,
    [string] $LocalLogRoot,
    [string] $LocalResultPath
)

# run_clean_windows_smoke.ps1, executed on the clean host from outside the staged bundle
param(
    [ValidateSet('gb', 'sk')] [string] $Pipeline,
    [ValidateSet('Standard', 'LowMemory')] [string] $ExpectedWriterMode,
    [string] $CandidateExecutable,
    [string] $SanitizedInput,
    [string] $OutputRoot,
    [string] $LocalResultPath
)
```

Both result paths must resolve below an ignored local root and can never point into `docs/performance/`.

- [ ] **Step 1: Add RED script-contract tests**

Add:

```text
test_pe_parser_captures_normal_and_delay_import_basenames
test_pe_gate_rejects_zlib_libz_or_deflate_dll
test_pe_gate_rejects_new_unapproved_non_windows_import
test_smoke_bundle_contains_one_exe_fixture_and_no_dll
test_smoke_uses_nonexistent_temp_canary_outside_output
test_smoke_rejects_created_canary_or_temp_workspace_residue
test_smoke_requires_three_sheets_and_success_exit
test_low_memory_smoke_requires_a_sheet_at_or_above_five_million_slots
test_smoke_evidence_contains_only_aliases_hashes_and_allowlist_summary
```

- [ ] **Step 2: Implement PE comparison against Phase 0A**

`verify_pe_imports.ps1` runs `dumpbin /DEPENDENTS` and `dumpbin /IMPORTS`; if available, also runs `llvm-readobj --coff-imports`. Parse basenames case-insensitively. Reject `(zlib|libz|deflate).*\.dll`, project-private DLLs, and any new non-Windows/non-approved Microsoft runtime relative to Phase 0A. Raw commands/output remain local.

- [ ] **Step 3: Implement clean-Windows smoke entrypoint**

The staged bundle contains exactly candidate EXE and one generated sanitized raw-input workbook; it contains no DLL, Python, Cargo or Rust files. `ExpectedWriterMode=Standard` uses the small Phase 0H fixture. `LowMemory` uses a deterministic synthetic SK fixture with at least 100,000 valid quantity rows and asserts from output Sheet dimensions that one Sheet has at least 5,000,000 slots；it therefore exercises the production threshold without a CLI override. Inside a clean Windows 10/11 x64 host, set process-local TEMP/TMP/TMPDIR to an asserted-nonexistent canary outside the output root, run normal mode, parse allowlisted CLI JSON, verify three Sheet names, assert the canary was never created, assert no `.costing-tmp-*` remains, then delete output.

If no clean Windows host or Windows Sandbox is available, return `BLOCKED_CLEAN_WINDOWS_REQUIRED`; do not substitute the development machine.

For a candidate assigned to `$FeatureCandidateExe`, run local PE capture as:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/verify_pe_imports.ps1 -CandidateExecutable $FeatureCandidateExe -Phase0AExecutable $Phase0AExe -LocalLogRoot rust/target/perf-local/pe -LocalResultPath rust/target/perf-local/pe/result.json
```

On the clean host, provision the runner separately from the runtime bundle and run:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File C:\verification\run_clean_windows_smoke.ps1 -Pipeline sk -ExpectedWriterMode Standard -CandidateExecutable C:\smoke-bundle\costing-calculate.exe -SanitizedInput C:\smoke-bundle\sanitized-sk.xlsx -OutputRoot C:\smoke-output -LocalResultPath C:\verification-results\smoke.json
```

Copy only the raw-result SHA and the allowlisted parsed result back through `EvidenceSanitizer`; do not version the clean-host paths or raw file.

- [ ] **Step 4: Verify and commit**

```powershell
uv run python -m pytest tests/rust_oracle/test_windows_release_scripts.py tests/rust_oracle/test_evidence.py -q --basetemp .pytest-tmp/windows-release
git add -- tests/rust_oracle/verify_pe_imports.ps1 tests/rust_oracle/run_clean_windows_smoke.ps1 tests/rust_oracle/test_windows_release_scripts.py tests/rust_oracle/evidence.py tests/rust_oracle/test_evidence.py
git diff --cached --name-only
git diff --cached --check
git commit -m "test(release): add single exe windows gates"
```

## Task 6: Measure A/B/C/D and Apply the Deterministic Feature Decision

**Files:**
- Generate via sanitizer: `docs/performance/runs/phase2/*.json`
- No production-code changes。

**Interfaces:**
- Consumes: A/B/C/D exact EXE SHAs and Phase 0A reference。
- Produces: one closed selected set: `default`、`zlib`、`zmij` or `zlib-zmij`。

- [ ] **Step 1: Run B/A and C/A on SK normal mode**

Use `phase2-b-vs-a` and `phase2-c-vs-a` profiles. Each comparison runs wall/PWS with same-batch A as reference and validates `xlsx_save` or writer/export metrics from the same paired runtime samples.

Run the internal SK pairs exactly once before any decision:

```powershell
$Phase0AExe = 'rust/target/perf-builds/phase0a/reference/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$AExe = 'rust/target/perf-builds/phase2/A/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$BExe = 'rust/target/perf-builds/phase2/B/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$CExe = 'rust/target/perf-builds/phase2/C/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$DExe = 'rust/target/perf-builds/phase2/D/x86_64-pc-windows-msvc/release/costing-calculate.exe'
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline sk --input "$env:COSTING_SK_SAMPLE" --reference-executable $AExe --candidate-executable $BExe --reference-label phase2-a --candidate-label phase2-b --comparison-profile phase2-b-vs-a --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/sk/.perf-runs --evidence-path docs/performance/runs/phase2/b-vs-a-sk.json
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline sk --input "$env:COSTING_SK_SAMPLE" --reference-executable $AExe --candidate-executable $CExe --reference-label phase2-a --candidate-label phase2-c --comparison-profile phase2-c-vs-a --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/sk/.perf-runs --evidence-path docs/performance/runs/phase2/c-vs-a-sk.json
```

For B to pass:

```text
SK B xlsx_save / same-batch A xlsx_save <= 0.85
PE imports pass
clean Windows single EXE smoke pass
common correctness/wall/PWS/bytes gates pass against Phase 0A
```

For C to pass:

```text
SK C writer_populate / same-batch A writer_populate <= 0.97
or SK C export / same-batch A export <= 0.97
the metric used to pass wins in at least 4/5 paired rounds in every five-round group
common correctness/wall/PWS/bytes gates pass against Phase 0A
```

Use `phase2-selected-vs-phase0a` as the closed common-gate profile separately for each otherwise eligible B/C candidate; its evidence records the exact candidate label, so a failing common gate changes that candidate to “not passed”.

For each candidate that passes its internal feature threshold, run both common-gate commands by substituting only `$FeatureCandidateExe`, `$FeatureCandidateLabel`, and `$EvidenceStem` from the closed tuples `(BExe, phase2-b, b)` or `(CExe, phase2-c, c)`:

```powershell
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline gb --input "$env:COSTING_GB_SAMPLE" --reference-executable $Phase0AExe --candidate-executable $FeatureCandidateExe --reference-label phase0a --candidate-label $FeatureCandidateLabel --comparison-profile phase2-selected-vs-phase0a --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/gb/.perf-runs --evidence-path "docs/performance/runs/phase2/$EvidenceStem-vs-phase0a-gb.json"
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline sk --input "$env:COSTING_SK_SAMPLE" --reference-executable $Phase0AExe --candidate-executable $FeatureCandidateExe --reference-label phase0a --candidate-label $FeatureCandidateLabel --comparison-profile phase2-selected-vs-phase0a --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/sk/.perf-runs --evidence-path "docs/performance/runs/phase2/$EvidenceStem-vs-phase0a-sk.json"
```

The plan executor must assign from those two closed tuples; no other label or executable is accepted by the harness.

For B, also set `$FeatureCandidateExe=$BExe` and run the Task 5 PE command plus one clean-host `ExpectedWriterMode=Standard` smoke. Rebuild their allowlisted results through:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/verify_pe_imports.ps1 -CandidateExecutable $BExe -Phase0AExecutable $Phase0AExe -LocalLogRoot rust/target/perf-local/phase2/b-pe -LocalResultPath rust/target/perf-local/phase2/b-pe-raw.json
uv run python -m tests.rust_oracle.evidence pe-imports --local-result rust/target/perf-local/phase2/b-pe-raw.json --candidate-executable $BExe --output docs/performance/runs/phase2/b-pe.json
uv run python -m tests.rust_oracle.evidence smoke --standard-result rust/target/perf-local/phase2/b-smoke-returned.json --candidate-executable $BExe --output docs/performance/runs/phase2/b-smoke.json
```

- [ ] **Step 2: Evaluate D only when both B and C pass**

If B and C are not both passed, do not run D performance comparisons. If both pass, run:

```text
phase2-d-vs-c: SK D xlsx_save / same-batch C xlsx_save <= 0.85
phase2-d-vs-b: SK D writer_populate / same-batch B writer_populate <= 0.97
               or SK D export / same-batch B export <= 0.97
```

The zmij metric again requires at least 4/5 per five-round group. D also passes PE/clean-Windows/common Phase 0A gates.

When and only when B/C both passed, execute:

```powershell
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline sk --input "$env:COSTING_SK_SAMPLE" --reference-executable $CExe --candidate-executable $DExe --reference-label phase2-c --candidate-label phase2-d --comparison-profile phase2-d-vs-c --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/sk/.perf-runs --evidence-path docs/performance/runs/phase2/d-vs-c-sk.json
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline sk --input "$env:COSTING_SK_SAMPLE" --reference-executable $BExe --candidate-executable $DExe --reference-label phase2-b --candidate-label phase2-d --comparison-profile phase2-d-vs-b --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/sk/.perf-runs --evidence-path docs/performance/runs/phase2/d-vs-b-sk.json
```

If both internal deltas pass, set the closed common-gate tuple to `(DExe, phase2-d, d)` and run the same GB/SK common-gate commands from Step 1. Run PE and clean-Windows gates on `$DExe` before marking D passed.

The D sanitized paths are `docs/performance/runs/phase2/d-pe.json` and `docs/performance/runs/phase2/d-smoke.json`, generated by the same evidence subcommands with `$DExe` and D-specific local raw results.

- [ ] **Step 3: Apply the exact decision table**

```text
B fail, C fail                         -> A
B pass, C fail                         -> B
B fail, C pass                         -> C
B pass, C pass, D passes both deltas   -> D
B pass, C pass, D fails either delta   -> same-batch B/C decider
```

For the B/C decider, run `phase2-b-vs-c`. After mandatory expansion, compute `r_wall = B_wall_median / C_wall_median`; if `abs(r_wall - 1) > 0.03`, choose lower wall. Otherwise compute `r_pws = B_pws_median / C_pws_median`; if `abs(r_pws - 1) > 0.03`, choose lower PWS; otherwise choose C. A is fallback, not an automatic “smallest feature” winner.

The exact B/C decider command is:

```powershell
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline sk --input "$env:COSTING_SK_SAMPLE" --reference-executable $CExe --candidate-executable $BExe --reference-label phase2-c --candidate-label phase2-b --comparison-profile phase2-b-vs-c --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/sk/.perf-runs --evidence-path docs/performance/runs/phase2/b-vs-c-sk.json
```

Here `candidate/reference` is B/C, matching the decision formula.

- [ ] **Step 4: Commit sanitized evidence and selected-set manifest**

Generate the decision after all applicable evidence exists:

```powershell
uv run python -m tests.rust_oracle.phase0_harness decide-phase2 --evidence-root docs/performance/runs/phase2 --output docs/performance/runs/phase2/decision.json
```

The harness writes exact fields `selected_label`、`selected_feature_set`、`selected_exe_sha256`、`selected_code_commit`、`decision_verdict` and sorted `required_evidence_sha256`. It also records every evaluated edge, skipped D reason where applicable, required 4/5 counts and common-gate verdicts. It contains no fabricated median or expected verdict.

```powershell
git add -- docs/performance/runs/phase2
uv run python -m tests.rust_oracle.evidence scan --root docs/performance --staged
git diff --cached --check
git commit -m "docs(perf): select phase2 writer features"
```

## Task 7: Add the Pure Sheet-Mode Policy Without Activating LowMemory

**Files:**
- Modify: `rust/crates/costing-xlsx/src/write_plan.rs`

**Interfaces:**
- Produces: pure `SheetMemoryPolicy::Adaptive` and `select_sheet_mode()` only。
- Preserves: production writer continues to pass `StandardOnly`; no LowMemory worksheet can be created in this commit。

- [ ] **Step 1: Add RED pure selector tests**

Add exact tests:

```text
write_plan::tests::zero_slots_are_standard
write_plan::tests::slots_4_999_999_are_standard
write_plan::tests::slots_5_000_000_are_low_memory
write_plan::tests::slot_overflow_saturates_to_low_memory
write_plan::tests::empty_rows_or_columns_are_standard
```

Threshold tests call a pure function and must not allocate millions of cells.

- [ ] **Step 2: Implement mode selection and options**

```rust
pub(crate) enum SheetMemoryPolicy {
    StandardOnly,
    #[cfg(feature = "low-memory")]
    Adaptive { low_memory_cell_slot_threshold: usize },
}

pub(crate) fn select_sheet_mode(
    row_count: usize,
    column_count: usize,
    policy: SheetMemoryPolicy,
) -> (SheetWriteMode, usize) {
    let cell_slots = row_count.saturating_mul(column_count);
    // 空 Sheet 始终 Standard；正式阈值固定，不暴露 CLI 参数。
    if row_count == 0 || column_count == 0 {
        return (SheetWriteMode::Standard, cell_slots);
    }
    match policy {
        SheetMemoryPolicy::StandardOnly => (SheetWriteMode::Standard, cell_slots),
        #[cfg(feature = "low-memory")]
        SheetMemoryPolicy::Adaptive {
            low_memory_cell_slot_threshold,
        } if cell_slots >= low_memory_cell_slot_threshold => {
            (SheetWriteMode::LowMemory, cell_slots)
        }
        #[cfg(feature = "low-memory")]
        SheetMemoryPolicy::Adaptive { .. } => (SheetWriteMode::Standard, cell_slots),
    }
}
```

Do not add `WorkbookWriteOptions::for_compiled_features()` or an `add_worksheet_with_low_memory()` branch yet. The pure selector can return the planned LowMemory enum in tests, but the existing Phase 1 test `phase1_writer_uses_standard_mode_for_every_sheet` must continue passing until Task 8 has created and configured the controlled workspace.

- [ ] **Step 3: Verify the policy while production remains Standard and commit**

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --release --locked --manifest-path rust/Cargo.toml -p costing-xlsx --target x86_64-pc-windows-msvc --target-dir rust/target/test-low-memory --no-default-features --features low-memory
git add -- rust/crates/costing-xlsx/src/write_plan.rs
git diff --cached --name-only
git diff --cached --check
git commit -m "perf(xlsx): define adaptive sheet memory policy"
```

## Task 8: Add Controlled TempWorkspace and Disk-Space Gate

**Files:**
- Create: `rust/crates/costing-xlsx/src/temp_workspace.rs`
- Create: `rust/crates/costing-xlsx/src/disk_space.rs`
- Modify: `rust/crates/costing-xlsx/src/lib.rs`
- Modify: `rust/crates/costing-xlsx/src/write_plan.rs`
- Modify: `rust/crates/costing-xlsx/src/writer.rs`
- Modify: `rust/crates/costing-cli/src/run.rs`
- Modify if not already exact: `rust/Cargo.toml`、`rust/Cargo.lock`、`rust/crates/costing-xlsx/Cargo.toml`

**Interfaces:**
- Produces: `TempWorkspace::{create,path,close}`、`available_space_bytes/ensure_low_memory_space`、`WorkbookWriteOptions::for_compiled_features()` and the first production LowMemory branch。
- Preserves: Phase -1D `WriterError` source and output ownership state。

- [ ] **Step 1: Add RED workspace and space-boundary tests**

Add:

```text
temp_workspace::tests::workspace_is_created_below_output_parent
temp_workspace::tests::workspace_prefix_contains_sanitized_request_id
temp_workspace::tests::concurrent_workspaces_do_not_collide
temp_workspace::tests::explicit_close_removes_workspace
disk_space::tests::standard_only_does_not_query_or_require_one_gib
disk_space::tests::low_memory_rejects_one_byte_below_one_gib
disk_space::tests::low_memory_accepts_exactly_one_gib
writer::tests::standard_and_low_memory_preserve_sheet_semantics
writer::tests::low_memory_writer_uses_shared_strings
writer::tests::metadata_is_complete_before_first_low_memory_row
writer::tests::controlled_tempdir_is_set_before_low_memory_factory
```

- [ ] **Step 2: Implement Windows free-space and controlled workspace**

Use `GetDiskFreeSpaceExW` through `windows-sys` only; required free bytes are exactly `1_u64 << 30`. Query only after output parent exists and only if at least one Sheet plan is LowMemory.

Create `.costing-tmp-{request_id}-{random}` below the final output parent with `tempfile::Builder`; sanitize request-id path characters. Pass only `TempWorkspace::path()` to `Workbook::set_tempdir()`. Never call `std::env::temp_dir()` in project code.

Only after `set_tempdir()` succeeds, make `WorkbookWriteOptions::for_compiled_features()` select Adaptive and create large Sheets with `add_worksheet_with_low_memory()`；never use `add_worksheet_with_constant_memory()`. Set name, widths/formats, filter and freeze panes before header/data. This Task 8 commit is the first commit in which production can select LowMemory.

- [ ] **Step 3: Add RED failure-priority and cleanup tests**

Add:

```text
writer::tests::temp_workspace_is_removed_after_success
writer::tests::temp_workspace_is_removed_after_populate_failure
writer::tests::temp_workspace_is_removed_after_save_failure
writer::tests::cleanup_failure_does_not_replace_primary_error
writer::tests::valid_output_is_preserved_when_temp_cleanup_fails
writer::tests::disk_query_failure_keeps_original_io_source
writer::tests::insufficient_space_returns_stable_retryable_error
```

Use internal test-only injected probes/factories; do not fill a real disk or change global TEMP in-process.

- [ ] **Step 4: Implement the exact state flow**

The order is: validate path → prepare parent → plan sheets → optional disk check → optional workspace → populate → `create_new(true)` → save/flush → close file → nonzero metadata → drop workbook → explicit workspace close → report.

Map insufficient capacity to `INSUFFICIENT_DISK_SPACE`, retryable true. A sole workspace cleanup failure after a valid final output maps to `TEMP_CLEANUP_FAILED`, retryable false, `final_output_valid=true`, and preserves the output. A primary failure plus cleanup failure keeps the primary code/source and appends structured cleanup metadata.

- [ ] **Step 5: Verify both build modes and commit**

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --locked --manifest-path rust/Cargo.toml -p costing-xlsx --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
cargo test --release --locked --manifest-path rust/Cargo.toml -p costing-xlsx --target x86_64-pc-windows-msvc --target-dir rust/target/test-low-memory --no-default-features --features low-memory
cargo test --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/test-low-memory --no-default-features --features low-memory
git add -- rust/crates/costing-xlsx/src/temp_workspace.rs rust/crates/costing-xlsx/src/disk_space.rs rust/crates/costing-xlsx/src/lib.rs rust/crates/costing-xlsx/src/write_plan.rs rust/crates/costing-xlsx/src/writer.rs rust/crates/costing-cli/src/run.rs rust/Cargo.toml rust/Cargo.lock rust/crates/costing-xlsx/Cargo.toml
git diff --cached --name-only
git diff --cached --check
git commit -m "feat(xlsx): control low memory temp workspace"
```

Before staging, omit unchanged Cargo paths from the pathspec; cached paths must equal actual changes.

## Task 9: Revalidate Features Under LowMemory and Close Phase 3

**Files:**
- Generate via sanitizer: `docs/performance/runs/phase3/*.json`
- Modify: `tests/rust_oracle/phase0_harness.py`
- Modify: `tests/rust_oracle/test_phase0_harness.py`
- No production Rust changes unless a failed feature is removed from final build commands/docs later。

**Interfaces:**
- Consumes: Phase 2 selected set and Phase 3 LowMemory code。
- Produces: final Phase 3 feature set and verified EXE SHA。

- [ ] **Step 1: Add and implement the closed Phase 3 decision CLI**

Add `test_phase3_decision_requires_low_memory_revalidation_for_each_retained_feature`, `test_phase3_decision_drops_failed_feature_without_reusing_phase2_ratio`, `test_phase3_tentative_selection_uses_only_phase2_and_feature_delta_evidence`, and `test_phase3_decision_writes_closed_handoff_fields`. Implement `select-phase3-candidate` and `decide-phase3` with no numeric threshold arguments. The tentative command consumes Phase 2 decision plus applicable feature deltas and writes only to ignored local storage；the final command additionally requires GB/SK external, PE and two-mode clean-Windows evidence.

```powershell
uv run python -m pytest tests/rust_oracle/test_phase0_harness.py -q --basetemp .pytest-tmp/phase3-decision -k "phase3_decision"
git add -- tests/rust_oracle/phase0_harness.py tests/rust_oracle/test_phase0_harness.py
git diff --cached --check
git commit -m "test(perf): implement phase3 feature decision gate"
```

- [ ] **Step 2: Build the LowMemory candidate and required feature-off controls**

Use closed target labels:

```text
Phase 2 A -> low-memory-default
Phase 2 B -> low-memory-zlib and low-memory-default control
Phase 2 C -> low-memory-zmij and low-memory-default control
Phase 2 D -> low-memory-zlib-zmij, low-memory-zmij control, low-memory-zlib control
```

Every build uses release, locked, MSVC target, its own target-dir, `--no-default-features`, and an explicit feature list beginning with `low-memory`.

Build all closed controls so no later comparison depends on overwriting a target directory:

```powershell
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/perf-builds/phase3/low-memory-default --no-default-features --features low-memory
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/perf-builds/phase3/low-memory-zlib --no-default-features --features "low-memory,zlib"
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/perf-builds/phase3/low-memory-zmij --no-default-features --features "low-memory,zmij"
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/perf-builds/phase3/low-memory-zlib-zmij --no-default-features --features "low-memory,zlib,zmij"
```

- [ ] **Step 3: Revalidate each retained feature with same-batch SK normal evidence**

For zlib use `phase3-zlib-on-vs-off` and require `xlsx_save on/off <= 0.85`. For zmij use `phase3-zmij-on-vs-off` and require `writer_populate on/off <= 0.97` or `export on/off <= 0.97`, plus at least 4/5 paired wins in every five-round group for the metric used.

Use these exact SK pair mappings:

```text
Phase 2 B zlib: reference=low-memory-default, candidate=low-memory-zlib
Phase 2 C zmij: reference=low-memory-default, candidate=low-memory-zmij
Phase 2 D zlib: reference=low-memory-zmij, candidate=low-memory-zlib-zmij
Phase 2 D zmij: reference=low-memory-zlib, candidate=low-memory-zlib-zmij
```

For each applicable row, invoke the paired harness with profile `phase3-zlib-on-vs-off` or `phase3-zmij-on-vs-off`, the exact target paths above, pipeline `sk`, input `$env:COSTING_SK_SAMPLE`, and a distinct JSON below `docs/performance/runs/phase3/feature-deltas/`. The selected Phase 2 decision controls which rows are applicable; the harness rejects any other label pair.

The four closed commands are:

```powershell
$LmDefault = 'rust/target/perf-builds/phase3/low-memory-default/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$LmZlib = 'rust/target/perf-builds/phase3/low-memory-zlib/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$LmZmij = 'rust/target/perf-builds/phase3/low-memory-zmij/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$LmBoth = 'rust/target/perf-builds/phase3/low-memory-zlib-zmij/x86_64-pc-windows-msvc/release/costing-calculate.exe'
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline sk --input "$env:COSTING_SK_SAMPLE" --reference-executable $LmDefault --candidate-executable $LmZlib --reference-label low-memory-default --candidate-label low-memory-zlib --comparison-profile phase3-zlib-on-vs-off --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/sk/.perf-runs --evidence-path docs/performance/runs/phase3/feature-deltas/zlib-from-default.json
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline sk --input "$env:COSTING_SK_SAMPLE" --reference-executable $LmDefault --candidate-executable $LmZmij --reference-label low-memory-default --candidate-label low-memory-zmij --comparison-profile phase3-zmij-on-vs-off --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/sk/.perf-runs --evidence-path docs/performance/runs/phase3/feature-deltas/zmij-from-default.json
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline sk --input "$env:COSTING_SK_SAMPLE" --reference-executable $LmZmij --candidate-executable $LmBoth --reference-label low-memory-zmij --candidate-label low-memory-zlib-zmij --comparison-profile phase3-zlib-on-vs-off --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/sk/.perf-runs --evidence-path docs/performance/runs/phase3/feature-deltas/zlib-from-zmij.json
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline sk --input "$env:COSTING_SK_SAMPLE" --reference-executable $LmZlib --candidate-executable $LmBoth --reference-label low-memory-zlib --candidate-label low-memory-zlib-zmij --comparison-profile phase3-zmij-on-vs-off --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/sk/.perf-runs --evidence-path docs/performance/runs/phase3/feature-deltas/zmij-from-zlib.json
```

Run only the row(s) required by `phase2/decision.json`; a non-applicable output file must not exist.

Derive the tentative surviving feature set without hand selection:

```powershell
uv run python -m tests.rust_oracle.phase0_harness select-phase3-candidate --phase2-decision docs/performance/runs/phase2/decision.json --feature-evidence-root docs/performance/runs/phase3/feature-deltas --output rust/target/perf-local/phase3/tentative-selection.json
$Tentative = Get-Content -Raw -Encoding UTF8 rust/target/perf-local/phase3/tentative-selection.json | ConvertFrom-Json
$Phase3Label = $Tentative.selected_label
$Phase3Exe = "rust/target/perf-builds/phase3/$Phase3Label/x86_64-pc-windows-msvc/release/costing-calculate.exe"
```

If a feature fails, remove it from the selected set and rebuild; do not carry Phase 2 Standard-writer evidence forward.

- [ ] **Step 4: Run Phase 3 external gates against the fixed Phase 0A reference**

Use `phase3-vs-phase0a` for both pipelines. Require:

```text
SK PWS median <= 2,147,483,648 bytes
GB wall candidate / same-batch Phase0A <= 1.05
GB PWS candidate / same-batch Phase0A <= 1.05
SK/GB bytes candidate / approved Phase0A bytes <= 1.10
GB/SK correctness = pass
```

Also rerun PE imports and clean-Windows single EXE smoke for the exact final Phase 3 SHA. If unavailable, Phase 3 is `BLOCKED_CLEAN_WINDOWS_REQUIRED`.

Set `$Phase3Label` from the closed surviving set and derive `$Phase3Exe` only from `rust/target/perf-builds/phase3/$Phase3Label/x86_64-pc-windows-msvc/release/costing-calculate.exe`. Then run:

```powershell
$Phase0AExe = 'rust/target/perf-builds/phase0a/reference/x86_64-pc-windows-msvc/release/costing-calculate.exe'
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline gb --input "$env:COSTING_GB_SAMPLE" --reference-executable $Phase0AExe --candidate-executable $Phase3Exe --reference-label phase0a --candidate-label phase3 --comparison-profile phase3-vs-phase0a --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/gb/.perf-runs --evidence-path docs/performance/runs/phase3/gb-external.json
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline sk --input "$env:COSTING_SK_SAMPLE" --reference-executable $Phase0AExe --candidate-executable $Phase3Exe --reference-label phase0a --candidate-label phase3 --comparison-profile phase3-vs-phase0a --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/sk/.perf-runs --evidence-path docs/performance/runs/phase3/sk-external.json
```

Run PE capture locally, then run two separate clean-host bundles against the same `$Phase3Exe` SHA:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/verify_pe_imports.ps1 -CandidateExecutable $Phase3Exe -Phase0AExecutable $Phase0AExe -LocalLogRoot rust/target/perf-local/phase3/pe -LocalResultPath rust/target/perf-local/phase3/pe-raw.json
```

```powershell
# clean host, small fixture bundle
powershell -NoProfile -ExecutionPolicy Bypass -File C:\verification\run_clean_windows_smoke.ps1 -Pipeline sk -ExpectedWriterMode Standard -CandidateExecutable C:\smoke-bundle\costing-calculate.exe -SanitizedInput C:\smoke-bundle\sanitized-sk-small.xlsx -OutputRoot C:\smoke-output -LocalResultPath C:\verification-results\smoke-standard.json
# clean host, >=5,000,000-slot fixture bundle
powershell -NoProfile -ExecutionPolicy Bypass -File C:\verification\run_clean_windows_smoke.ps1 -Pipeline sk -ExpectedWriterMode LowMemory -CandidateExecutable C:\smoke-bundle\costing-calculate.exe -SanitizedInput C:\smoke-bundle\sanitized-sk-low-memory.xlsx -OutputRoot C:\smoke-output -LocalResultPath C:\verification-results\smoke-low-memory.json
```

The sanitizer combines both smoke results only if both bind the same EXE SHA and both TEMP canaries were never created.

```powershell
uv run python -m tests.rust_oracle.evidence pe-imports --local-result rust/target/perf-local/phase3/pe-raw.json --candidate-executable $Phase3Exe --output docs/performance/runs/phase3/pe.json
uv run python -m tests.rust_oracle.evidence smoke --standard-result rust/target/perf-local/phase3/smoke-standard-returned.json --low-memory-result rust/target/perf-local/phase3/smoke-low-memory-returned.json --candidate-executable $Phase3Exe --output docs/performance/runs/phase3/smoke.json
```

After exact PE and clean-Windows evidence is present, generate:

```powershell
uv run python -m tests.rust_oracle.phase0_harness decide-phase3 --phase2-decision docs/performance/runs/phase2/decision.json --evidence-root docs/performance/runs/phase3 --output docs/performance/runs/phase3/decision.json
```

The decision contains `selected_label`、`selected_feature_set`、`selected_exe_sha256`、`selected_code_commit`、`decision_verdict` and sorted `required_evidence_sha256`. The selected EXE SHA must equal `$Phase3Exe`.

- [ ] **Step 5: Commit only sanitized Phase 3 evidence**

```powershell
git add -- docs/performance/runs/phase3
uv run python -m tests.rust_oracle.evidence scan --root docs/performance --staged
git diff --cached --check
git commit -m "docs(perf): validate adaptive low memory writer"
```

Do not claim the `<=20.0s` final wall target yet; Phase 4 remains a mandatory A/B step even if Phase 3 already meets it.

## Pseudocode Draft

```rust
// 目标：在不改变 workbook 语义的前提下，先规划 Sheet，再安全选择 Standard/LowMemory 并返回可观测报告。
// 输入：request context、输出路径、完整 WorkbookPayload、编译 feature 对应的写出选项。
// 输出：成功时 WorkbookWriteReport；失败时保留原始 I/O source、output ownership 和 cleanup details。

fn write_workbook(
    context: &WriterContext,
    output: &Path,
    payload: &WorkbookPayload,
    options: &WorkbookWriteOptions,
) -> Result<WorkbookWriteReport, WriterError> {
    validate_output_path(output, context)?;
    let output_parent = prepare_output_parent(output, context)?;

    let plans = payload
        .sheet_models
        .iter()
        .map(|sheet| build_sheet_write_plan(sheet, options.memory_policy))
        .collect::<Result<Vec<_>, _>>()?;
    let needs_low_memory = plans.iter().any(|plan| plan.mode == SheetWriteMode::LowMemory);

    if needs_low_memory {
        // LowMemory 会产生临时 XML；空间不足时在创建任何最终文件前失败。
        ensure_low_memory_space(available_space_bytes(&output_parent)?, 1_u64 << 30)?;
    }

    let mut temp = if needs_low_memory {
        Some(TempWorkspace::create(&output_parent, &context.request_id)?)
    } else {
        None
    };
    let mut workbook = Workbook::new();
    let mut output_state = OutputArtifactState::NotCreated;
    let mut final_file: Option<File> = None;

    let primary_result = (|| -> Result<WorkbookWriteReport, WriterError> {
        if let Some(temp) = temp.as_ref() {
            workbook.set_tempdir(temp.path()).map_err(initialize_temp_writer_error)?;
        }

        let populate_started = Instant::now();
        for (sheet, plan) in payload.sheet_models.iter().zip(&plans) {
            let worksheet = match plan.mode {
                SheetWriteMode::Standard => workbook.add_worksheet(),
                SheetWriteMode::LowMemory => workbook.add_worksheet_with_low_memory(),
            };
            apply_all_sheet_metadata_before_rows(worksheet, sheet, plan)?;
            write_header_from_plan(worksheet, sheet, plan)?;
            write_nonblank_cells_from_plan(worksheet, sheet, plan)?;
        }
        let writer_populate_seconds = populate_started.elapsed().as_secs_f64();

        final_file = Some(create_new_final_output(output)?);
        output_state = OutputArtifactState::CreatedByCurrentRun;

        let save_started = Instant::now();
        workbook.save_to_writer(final_file.as_mut().expect("file was just created"))?;
        // xlsx_save 的批准边界在 save_to_writer 返回时结束；flush/metadata 只属于 export。
        let xlsx_save_seconds = save_started.elapsed().as_secs_f64();
        final_file.as_mut().expect("file exists").flush()?;
        drop(final_file.take());

        let output_size_bytes = read_nonzero_output_size(output)?;
        output_state = OutputArtifactState::CompletedByCurrentRun;
        Ok(WorkbookWriteReport {
            writer_populate_seconds,
            xlsx_save_seconds,
            output_size_bytes,
        })
    })();

    // Windows 清理前先释放最终文件和所有 worksheet/temp handles。
    drop(final_file.take());
    drop(workbook);
    let partial_cleanup = if primary_result.is_err()
        && output_state == OutputArtifactState::CreatedByCurrentRun
    {
        Some(remove_partial_output(output))
    } else {
        None
    };
    let temp_cleanup = temp.take().map(TempWorkspace::close);

    merge_primary_and_cleanup_results(
        primary_result,
        output_state,
        partial_cleanup,
        temp_cleanup,
    )
}
```

## Phase 0A–3 Exit Checklist

- [ ] Phase 0A manifest was generated by the harness, scanned, explicitly approved and committed once.
- [ ] Phase 0A EXE SHA remains unchanged and later batches rerun that exact binary.
- [ ] Phase 0B adds observation only and passes the 1.02 wall gate for GB/SK.
- [ ] Phase 1 is either retained with same-batch 10% populate gain or explicitly reverted.
- [ ] A/B/C/D selection follows the exact decision table and SK-only feature benefit rules.
- [ ] zlib candidates pass PE and clean-Windows single EXE gates.
- [ ] LowMemory is activated only at 5,000,000 or more cell slots and preserves shared strings/workbook semantics.
- [ ] Standard mode performs no disk-space/temp-workspace work.
- [ ] LowMemory uses only the output-parent workspace and all error/cleanup states remain structured.
- [ ] Retained features are revalidated under LowMemory with same-batch controls.
- [ ] Phase 3 meets PWS, GB regression, bytes and correctness gates; Phase 4 remains mandatory.

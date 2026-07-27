# Rust 成本管线性能优化实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在业务、审计、Workbook 和 CLI 契约完全一致的前提下，消除 Rust normalize/fact/presentation 的逐行字符串 Map、重复键构造和深复制，使 GB、SK 的 Rust release check-only 五轮中位数均不慢于对应 Python oracle。

**Architecture:** 先建立同边界的 test-only 性能门禁和 Windows 内存基线，再按顺序完成 Map 模型下的所有权去复制、`IndexedTable` 深模块迁移、类型化成本聚合和唯一工单索引。Phase 1–3 是强制范围；Phase 4 不预写猜测性实现，只有最终硬门禁失败且 profiler 指向单一热点时，才另开一份单热点小计划。

**Tech Stack:** Rust 2021、标准库 `HashMap/HashSet`、现有 `rust_decimal`、Calamine、rust_xlsxwriter、Python 3.11、pytest、Polars/Pandas oracle、PowerShell/WPR/WPA；不新增生产依赖。

**Approved Spec:** `docs/superpowers/specs/2026-07-10-rust-pipeline-performance-optimization-design.md`

## Global Constraints

- Correctness > Maintainability > Observability；任何性能收益都不能换取业务或审计差异。
- GB、SK 最终都必须满足 `Rust release median <= Python median`；每端预热 1 次、正式 5 个成对 round，奇偶轮交替先后顺序。
- Rust/Python `payload_total_seconds` 的统一边界是“即将进入 ingest”到“完整内存 `WorkbookPayload` 返回”；排除 Cargo build、进程启动、CLI/路径解析、export、run/issue summary 和结果序列化。
- Python 生产 `stage_timings` 不新增 `total`；Python 总时间只由 test-only helper 的 `perf_counter` 墙钟测量提供，禁止用阶段求和代替。
- `--check-only` 必须继续执行完整 analysis 和 presentation，只跳过 writer。
- Phase 1、Phase 2、Phase 3 均必须实施；Phase 4 只能处理 profiler 已证实的一个剩余热点，并在每次处理后重跑全部硬门禁。
- 金额、数量、单位成本继续使用 `rust_decimal::Decimal`，不得改为 `f64`。
- 不新增 Rust Polars、Arrow、IndexMap、字符串驻留池、自定义 allocator、benchmark framework、LTO 或 release profile 永久调参。
- 不改变三张 Sheet、Sheet/列顺序、格式、白名单顺序、Modified Z-score 规则、GB/SK 独立成本项和总成本勾稽口径。
- 不删除、抽样、去重或只计数 `ErrorIssue`；错误类型、字段、值、数量和顺序必须完全一致。
- `HashMap/HashSet` 只能用于 lookup/count/membership；Sheet 和错误顺序继续由输入向量和显式索引决定。
- `error_log_count == error_issues.len()` 且 `sum(issue_type_counts.values()) == error_log_count`。
- 不更新 `tests/contracts/baselines/` 来迁就性能重构。
- README/AGENTS 的正式 build/run/check-only 命令使用 `--release`；`cargo test`、`cargo fmt` 和 Python oracle 命令保持原样。
- 所有文本局部 patch 保持 UTF-8 和原换行风格；Windows 下不通过 PowerShell 管道写非 ASCII 内容。
- 每个任务只暂存列出的路径；不得覆盖、清理或顺带提交用户已有修改。
- 任一非目标 stage 的五轮 median 相对同一固定样本基线回退超过 5%，必须记录原因并再跑一组完整五轮；无解释或无法复现解释的回退不构成验收通过。
- 每个代码任务遵循 red-green-refactor，并在验证通过后独立 Conventional Commit。

---

## Starting Workspace State

截至计划编写时，`HEAD=b31f1b4`，工作区已有 7 个与“Rust CLI 自动输入/输出及安全写出”相关的未提交修改：

```text
AGENTS.md
README.md
rust/crates/costing-cli/src/args.rs
rust/crates/costing-cli/src/run.rs
rust/crates/costing-cli/tests/cli_errors.rs
rust/crates/costing-xlsx/src/reader.rs
rust/crates/costing-xlsx/src/writer.rs
```

这些修改属于用户此前已授权的工作，Task 1 必须先在当前工作区验证并单独提交。Task 1 完成前不要从旧 `HEAD` 创建 worktree，否则新 worktree 会漏掉这 7 个文件；Task 1 提交后，如选择隔离执行，再使用 `superpowers:using-git-worktrees` 从新提交创建工作树。

## File Structure

### Create

- `rust/crates/costing-core/src/table.rs` — `SchemaId`、`ColumnId`、`ColumnSchema`、`IndexedRow`、`IndexedTable`、派生列原子更新和消费式 projection plan。
- `rust/crates/costing-cli/tests/cli_benchmark.rs` — CLI 成功 benchmark JSON 契约，确认 check-only 无 export/workbook，普通模式独立报告 export。
- `tests/test_rust_check_only_benchmark.py` — 五轮 paired check-only 严格性能门禁；不得 skip。
- `tests/rust_oracle/test_benchmark.py` — paired 顺序、证据完整性、median 和 verdict 纯单元测试。
- `tests/rust_oracle/measure_peak_working_set.ps1` — Windows 直接 executable 的 baseline/current Peak Working Set 成对采样。
- `tests/rust_oracle/capture_cpu_profile.ps1` — 统一构建带符号 release profiling binary，并用 WPR 采集指定阶段的 GB/SK CPU trace。

### Modify

- `README.md` — 正式 Rust 命令统一为 release，并说明 dev profile 不能作为真实数据性能口径。
- `AGENTS.md` — 与 README 同步 release 命令，保留 test/fmt/Python 命令。
- `rust/crates/costing-cli/src/run.rs` — payload total 精确边界、run count accessor 和最终 pipeline 计数适配。
- `rust/crates/costing-core/src/lib.rs` — 注册私有 `table` 模块。
- `rust/crates/costing-core/src/error.rs` — 内部结构错误构造 helper。
- `rust/crates/costing-core/src/model.rs` — 移除逐行 Map 内部模型，保留外部 `WorkbookPayload`/`RunSummary` 序列化契约。
- `rust/crates/costing-core/src/normalize.rs` — `IndexedTable::from_raw`、列 ID 预解析、forward fill、月份/Filled 列复用和过滤。
- `rust/crates/costing-core/src/split.rs` — 使用 `ColumnId` 分类并按输入顺序移动 `IndexedRow`。
- `rust/crates/costing-core/src/fact.rs` — `CostAmounts`、分类 enum、prepared qty rows、键缓存、唯一索引和消费式数量页投影。
- `rust/crates/costing-core/src/anomaly.rs` — 借用源行，并在最终模型中通过 `QtyFactRow`/唯一索引读取。
- `rust/crates/costing-core/src/quality.rs` — 复用 fact 计数和唯一索引，不再重复构建工单键集合。
- `rust/crates/costing-core/src/presentation.rs` — 先借用分析再消费行，移动 error log 和最终单元格。
- `tests/rust_oracle/oracle_runner.py` — Rust/Python test-only check-only 内部计时与摘要解析。
- `tests/rust_oracle/benchmark.py` — paired round 数据结构、交错执行、五轮完整性和 verdict。
- `tests/rust_oracle/repo_paths.py` — 固定 GB/SK 性能样本的唯一性校验和强制解析。
- `tests/rust_oracle/test_oracle_runner.py` — total 解析、非有限值、Python 计时边界和 Rust 命令参数测试。
- `tests/test_full_rust_cli_benchmark.py` — 复用强制样本解析；样本缺失不得形成通过证据。

### Do Not Modify

- `rust/Cargo.toml`、任何 crate `Cargo.toml`、`uv.lock` — 本计划不增加依赖。
- `rust/crates/costing-xlsx/src/reader.rs` 的 ingest 算法 — 除 Task 1 已有安全写出相关差异外，本轮不优化 reader。
- `src/etl/pipeline.py`、`src/etl/runner.py` 的生产计时结构 — Python 同边界总计时只存在于 tests helper。
- `tests/contracts/baselines/` — 纯性能重构禁止刷新 baseline。
- 历史 `docs/superpowers/` spec/plan 和 `_archive/`。

---

### Task 1: Stabilize the Existing CLI Auto-Path and Safe-Output Patch

**Files:**
- Existing modified: `AGENTS.md`
- Existing modified: `README.md`
- Existing modified: `rust/crates/costing-cli/src/args.rs`
- Existing modified: `rust/crates/costing-cli/src/run.rs`
- Existing modified: `rust/crates/costing-cli/tests/cli_errors.rs`
- Existing modified: `rust/crates/costing-xlsx/src/reader.rs`
- Existing modified: `rust/crates/costing-xlsx/src/writer.rs`

**Interfaces:**
- Consumes: 当前工作区中已经写好的 `CliArgs.input: Option<PathBuf>`、`resolve_cli_paths`、默认输出路径、`CostingXlsxError::OutputExists` 和 `create_new(true)` 安全写出。
- Produces: 一个独立、已验证的 CLI 便利性提交；后续性能任务从该提交继续，不把既有用户修改混进性能提交。

本任务是“稳定并隔离既有改动”，不是重新实现功能，因此不制造假的 red；只验证当前 patch 的真实行为。

- [ ] **Step 1: Confirm the exact dirty-file boundary**

Run:

```powershell
git diff --name-only
```

Expected: 只出现本任务列出的 7 个路径；若多出路径，停止并保护额外用户修改，不扩大暂存范围。

- [ ] **Step 2: Review the current patch for accidental scope**

Run:

```powershell
git diff -- AGENTS.md README.md rust/crates/costing-cli/src/args.rs rust/crates/costing-cli/src/run.rs rust/crates/costing-cli/tests/cli_errors.rs rust/crates/costing-xlsx/src/reader.rs rust/crates/costing-xlsx/src/writer.rs
```

Expected: 差异只包含自动输入、自动输出、月后缀、拒绝覆盖、并发安全写出、错误映射、测试和对应文档。

- [ ] **Step 3: Run focused CLI/XLSX tests**

Run:

```powershell
cargo test --manifest-path rust/Cargo.toml -p costing-calculate
cargo test --manifest-path rust/Cargo.toml -p costing-xlsx
```

Expected: 两条命令均 exit 0；自动路径、0/1/多文件、已有输出和并发 writer 测试全部 PASS。

- [ ] **Step 4: Run workspace format and regression tests**

Run:

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --manifest-path rust/Cargo.toml
git diff --check
```

Expected: 全部 exit 0，没有格式、测试、空白或编码错误。

- [ ] **Step 5: Commit only the seven existing paths**

```powershell
git add -- AGENTS.md README.md rust/crates/costing-cli/src/args.rs rust/crates/costing-cli/src/run.rs rust/crates/costing-cli/tests/cli_errors.rs rust/crates/costing-xlsx/src/reader.rs rust/crates/costing-xlsx/src/writer.rs
git diff --cached --name-only
git commit -m "feat(cli): infer default workbook paths"
```

Expected: cached name list恰好为 7 个路径；commit 成功，其他用户文件不进入提交。

---

### Task 2: Align Release Documentation and the Rust Payload Timer Boundary

**Files:**
- Modify: `README.md`（Rust 使用、显式路径和 benchmark 命令）
- Modify: `AGENTS.md`（Build / Test / Dev Commands）
- Modify: `rust/crates/costing-cli/src/run.rs`（`run` 和 timing tests）
- Create: `rust/crates/costing-cli/tests/cli_benchmark.rs`

**Interfaces:**
- Consumes: Task 1 的自动路径 CLI；现有 `StageTimings::insert`、`measure`、`build_workbook_payload`。
- Produces: `RunSummary.stage_timings.stages["total"]: f64`，语义严格为 ingest 前到内存 payload 返回；`export` 始终独立。

- [ ] **Step 1: Add stable CLI benchmark characterization tests**

在新文件 `rust/crates/costing-cli/tests/cli_benchmark.rs` 写两个成功路径测试。测试只校验 JSON 契约，不写会随机器抖动的绝对或相对秒数断言：

```rust
#[test]
fn check_only_benchmark_reports_payload_total_without_export() {
    let input = unique_temp_path("check-only-input.xlsx");
    write_minimal_input_workbook(&input);

    let output = Command::new(locate_costing_binary())
        .args([
            "gb",
            "--input",
            input.to_str().unwrap(),
            "--check-only",
            "--benchmark",
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));

    let payload: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let stages = payload["stage_timings"]["stages"].as_object().unwrap();
    assert_eq!(payload["output_written"], false);
    assert!(payload["workbook_path"].is_null());
    assert!(stages["total"].as_f64().unwrap().is_finite());
    assert!(!stages.contains_key("export"));

    let _ = std::fs::remove_file(input);
}

#[test]
fn normal_benchmark_reports_export_separately() {
    let input = unique_temp_path("normal-input.xlsx");
    let workbook = unique_temp_path("normal-output.xlsx");
    write_minimal_input_workbook(&input);

    let output = Command::new(locate_costing_binary())
        .args([
            "gb",
            "--input",
            input.to_str().unwrap(),
            "--output",
            workbook.to_str().unwrap(),
            "--benchmark",
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));

    let payload: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let stages = payload["stage_timings"]["stages"].as_object().unwrap();
    assert_eq!(payload["output_written"], true);
    assert!(stages["total"].as_f64().unwrap().is_finite());
    assert!(stages["export"].as_f64().unwrap().is_finite());

    let _ = std::fs::remove_file(input);
    let _ = std::fs::remove_file(workbook);
}
```

同文件复用 `cli_errors.rs` 当前模式实现本地 `locate_costing_binary`、唯一临时路径和最小三表输入 workbook helper；不要把 test helper 提升为生产接口。

- [ ] **Step 2: Run the characterization tests before changing control flow**

Run:

```powershell
cargo test --manifest-path rust/Cargo.toml -p costing-calculate --test cli_benchmark
```

Expected: PASS。这里先固定外部结构；“stop 位于 presentation 后、export 前”由下一步的控制流位置、代码审查和 test-only 内部计时共同证明，不使用脆弱的普通单测时间比较。

- [ ] **Step 3: Move the timer stop and remove the run-only reader snapshot scan**

将 `run` 的核心顺序改为以下结构；ingest 中只携带后续无法恢复的 `reader_rows` O(1) 标量。fact/quality/run-count 汇总全部在计时停止后从已构建 payload 读取：

```rust
let mut timings = StageTimings::default();
let input = args
    .input
    .as_ref()
    .expect("resolve_cli_paths always supplies an input path");
let total_started = args.benchmark.then(Instant::now);

let (raw, reader_rows) = measure(&mut timings, "ingest", || {
    let raw = read_raw_workbook(input).map_err(|error| map_xlsx_read_error(input, error))?;
    let reader_rows = raw.rows.len();
    Ok::<_, CostingError>((raw, reader_rows))
})?;
let normalized = measure(&mut timings, "normalize", || {
    Ok::<_, anyhow::Error>(normalize_workbook(raw, &pipeline, month_range)?)
})?;
let month_filter_empty_result = month_filter_requested && normalized.rows.is_empty();
let split = measure(&mut timings, "split", || {
    Ok::<_, anyhow::Error>(split_detail_and_qty(normalized)?)
})?;
let bundle = measure(&mut timings, "fact", || {
    Ok::<_, anyhow::Error>(build_fact_bundle(split, &pipeline)?)
})?;
let payload_timings = timings.clone();
let payload = measure(&mut timings, "presentation", || {
    build_workbook_payload(
        bundle,
        &pipeline,
        payload_timings,
        month_filter_empty_result,
    )
})?;

if let Some(started) = total_started {
    timings.insert("total", started.elapsed().as_secs_f64());
}

let detail_rows = required_quality_count(&payload.quality_metrics, "成本明细输入行数")?;
let qty_rows = required_quality_count(&payload.quality_metrics, "产品数量统计输出行数")?;
let work_order_rows = required_quality_count(&payload.quality_metrics, "工单异常分析输出行数")?;
let qty_sheet_rows = payload
    .sheet_models
    .iter()
    .find(|sheet| sheet.sheet_name == "成本计算单数量聚合维度")
    .ok_or_else(|| CostingError::Internal {
        code: ErrorCode::InternalError,
        message: "workbook payload is missing quantity sheet".to_string(),
    })?
    .rows
    .len();
let mut run_counts = BTreeMap::from([
    ("reader_rows".to_string(), reader_rows),
    ("detail_rows".to_string(), detail_rows),
    ("qty_rows".to_string(), qty_rows),
    ("qty_sheet_rows".to_string(), qty_sheet_rows),
    ("quality_metric_count".to_string(), payload.quality_metrics.len()),
    ("work_order_rows".to_string(), work_order_rows),
]);
```

`required_quality_count` 使用现有类型并固定为：

```rust
fn required_quality_count(
    quality_metrics: &[costing_core::model::QualityMetric],
    metric_name: &str,
) -> Result<usize, CostingError> {
    let mut matches = quality_metrics
        .iter()
        .filter(|metric| metric.metric == metric_name);
    let metric = matches.next().ok_or_else(|| CostingError::Internal {
        code: ErrorCode::InternalError,
        message: format!("workbook payload is missing quality metric: {metric_name}"),
    })?;
    if matches.next().is_some() {
        return Err(CostingError::Internal {
            code: ErrorCode::InternalError,
            message: format!("workbook payload has duplicate quality metric: {metric_name}"),
        });
    }
    metric.value.parse::<usize>().map_err(|error| CostingError::Internal {
        code: ErrorCode::InternalError,
        message: format!(
            "workbook payload quality metric {metric_name} is not an integer: {}; {error}",
            metric.value,
        ),
    })
}
```

Task 2 不提前调用 Task 10 才新增的 `CostingError::internal` helper。timer stop 后才做名称查询、字符串解析、Sheet 查找、issue counts 和 `BTreeMap` 构建，满足“payload 返回即停表”。

同时从 CLI run path 移除 `build_reader_snapshot` import；`costing-xlsx::snapshot` 模块和其独立测试保留，不删除 reader snapshot 契约。

- [ ] **Step 4: Update formal Rust commands to release**

在 `README.md` 和 `AGENTS.md` 中：

```text
cargo build --release --manifest-path rust/Cargo.toml
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- gb
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- sk
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- gb --check-only --benchmark
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- sk --check-only --benchmark
```

显式 `--input/--output` 示例也加 `--release`。紧邻命令补充：dev profile 适合开发调试，不作为真实数据性能比较口径；`cargo test`、`cargo fmt` 和所有 `uv run` 命令不加 `--release`。

- [ ] **Step 5: Run focused and documentation checks**

Run:

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --manifest-path rust/Cargo.toml -p costing-calculate
cargo test --manifest-path rust/Cargo.toml -p costing-calculate --test cli_benchmark
git diff --check -- README.md AGENTS.md rust/crates/costing-cli/src/run.rs
```

Expected: 全部 exit 0；新增边界测试 PASS。

Run the command inventory:

```powershell
Select-String -Path README.md,AGENTS.md -Pattern 'cargo (build|run).*--release'
Select-String -Path README.md,AGENTS.md -Pattern 'cargo (test|fmt).*--release'
$missingRelease = Select-String -Path README.md,AGENTS.md -Pattern '^\s*(?:-\s*)?`?cargo (build|run)\b(?!.*--release)'
if ($missingRelease) { $missingRelease; throw 'formal cargo build/run command is missing --release' }
$unexpectedRelease = Select-String -Path README.md,AGENTS.md -Pattern '^\s*(?:-\s*)?`?cargo (test|fmt)\b.*--release'
if ($unexpectedRelease) { $unexpectedRelease; throw 'cargo test/fmt must not use --release' }
```

Expected: 第一条列出所有正式 build/run 示例；第二条无匹配；两个负向断言均 exit 0。任何残留正式 dev build/run 行都会让本步骤失败，而不是被正向搜索掩盖。

- [ ] **Step 6: Run an independent read-only documentation review**

实现 agent 完成修改后，交给 `doc_reviewer` 只读检查 `README.md` 与 `AGENTS.md`；reviewer 不直接修改文件。固定审查清单：

```text
自动输入/输出命令包含 --release
显式 --input/--output 命令包含 --release
GB/SK check-only/benchmark 命令包含 --release
build 命令包含 --release
dev profile 不作为真实数据性能证据
cargo test/cargo fmt 不含 --release
所有 uv/Python oracle 命令保持原样
未修改历史 spec/plan 命令
```

Expected: reviewer verdict `APPROVED` 且无 P1/P2。若有问题，由原实现 agent 定点修正，重跑 Step 5 后再次只读审查；不得让 reviewer 自己改文档。

- [ ] **Step 7: Commit**

```powershell
git add -- README.md AGENTS.md rust/crates/costing-cli/src/run.rs rust/crates/costing-cli/tests/cli_benchmark.rs
git diff --cached --check
git commit -m "perf(cli): align payload timing boundary"
```

---

### Task 3: Add Exact-Boundary Test-Only Python and Rust Check-Only Runners

**Files:**
- Modify: `tests/rust_oracle/oracle_runner.py`
- Modify: `tests/rust_oracle/test_oracle_runner.py`

**Interfaces:**
- Consumes: Python `_build_request`、test-only 调用 `costing_service._prepare_request/_build_etl`、`CostingEtlPipeline.build_workbook_payload`；Rust release executable JSON。
- Produces: `TimedPayloadRun`、`REQUIRED_RUST_PAYLOAD_STAGES`、`REQUIRED_RUST_RUN_COUNTS`、`run_python_check_only_payload`、`run_rust_cli_release_check_only`、`parse_rust_check_only_run`、`capture_rust_normal_benchmark_evidence`。两端统一暴露 `payload_total_seconds`，但不修改任何 Python 生产字段或 Rust `RunSummary` 结构。

在 `oracle_runner.py` 固定以下类型：

```python
REQUIRED_RUST_PAYLOAD_STAGES = (
    'ingest',
    'normalize',
    'split',
    'fact',
    'presentation',
    'total',
)
REQUIRED_RUST_RUN_COUNTS = (
    'reader_rows',
    'detail_rows',
    'qty_rows',
    'qty_sheet_rows',
    'quality_metric_count',
    'work_order_rows',
)


@dataclass(frozen=True)
class TimedPayloadRun:
    pipeline: str
    payload_total_seconds: float
    stage_timings: dict[str, float]
    runtime_summary: OracleRunSummary
    run_counts: dict[str, int] = field(default_factory=dict)
```

- [ ] **Step 1: Write parser and timing-boundary tests**

在 `tests/rust_oracle/test_oracle_runner.py` 新增至少以下测试：

```python
def test_parse_rust_check_only_run_uses_total_not_stage_sum() -> None:
    payload = {
        'status': 'succeeded',
        'pipeline': 'gb',
        'output_written': False,
        'workbook_path': None,
        'sheet_count': 3,
        'error_log_count': 0,
        'issue_type_counts': {},
        'quality_metrics': [],
        'run_counts': {
            'reader_rows': 1,
            'detail_rows': 1,
            'qty_rows': 1,
            'qty_sheet_rows': 1,
            'quality_metric_count': 0,
            'work_order_rows': 1,
        },
        'stage_timings': {
            'stages': {
                'ingest': 1.0,
                'normalize': 2.0,
                'split': 3.0,
                'fact': 4.0,
                'presentation': 5.0,
                'total': 99.0,
            }
        },
    }

    result = parse_rust_check_only_run(json.dumps(payload, ensure_ascii=False))

    assert result.payload_total_seconds == 99.0
    assert result.payload_total_seconds != sum(
        value for name, value in result.stage_timings.items() if name != 'total'
    )


@pytest.mark.parametrize('total', (None, float('nan'), float('inf'), -1.0))
def test_parse_rust_check_only_run_rejects_invalid_total(total: float | None) -> None:
    payload = valid_rust_check_only_payload()
    if total is None:
        del payload['stage_timings']['stages']['total']
    else:
        payload['stage_timings']['stages']['total'] = total

    with pytest.raises(AssertionError, match='total'):
        parse_rust_check_only_run(json.dumps(payload))
```

再增加：

- `test_parse_rust_check_only_run_rejects_export_stage`
- `test_parse_rust_check_only_run_requires_succeeded_status_and_three_sheets`
- `test_parse_rust_check_only_run_requires_output_written_false`
- `test_parse_rust_check_only_run_rejects_missing_or_non_finite_required_stage`
- `test_parse_rust_check_only_run_rejects_missing_or_non_integer_run_count`
- `test_run_rust_cli_release_check_only_omits_output_argument`
- `test_run_rust_cli_release_check_only_rejects_pipeline_mismatch`
- `test_run_python_check_only_payload_times_only_build_call`
- `test_run_python_check_only_payload_does_not_write_workbook`
- `test_capture_rust_normal_benchmark_evidence_requires_export_and_deletes_workbook`

Python 边界测试用 fake `perf_counter` 和 fake pipeline 记录事件，必须严格断言：

```python
assert events == [
    'prepare-request',
    'prepare-input',
    'build-etl',
    'reset-state',
    'timer-start',
    'build-payload',
    'timer-stop',
    'build-summary',
]
```

- [ ] **Step 2: Run the tests and verify red**

Run:

```powershell
uv run python -m pytest tests/rust_oracle/test_oracle_runner.py -q --basetemp .pytest-tmp/oracle-runner-timing
```

Expected: FAIL，因为 `TimedPayloadRun` 和三个 check-only helper 尚不存在。

- [ ] **Step 3: Refactor Rust JSON loading without changing the full runner**

将现有 `parse_rust_run_summary` 拆成可复用的纯 helper：

```python
def _load_rust_summary_payload(stdout: str) -> dict[str, Any]:
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise AssertionError(f'Rust CLI stdout is not valid JSON: {exc}\nSTDOUT:\n{stdout}') from exc
    if not isinstance(payload, dict):
        raise AssertionError(f'Rust CLI stdout JSON must be an object, got {type(payload).__name__}')
    return payload


def _oracle_summary_from_rust_payload(payload: dict[str, Any]) -> OracleRunSummary:
    return OracleRunSummary(
        error_log_count=_required_int(payload, 'error_log_count'),
        issue_type_counts=_issue_type_counts(payload),
        quality_metrics=_quality_metric_values(_required_list(payload, 'quality_metrics')),
    )


def parse_rust_run_summary(stdout: str) -> OracleRunSummary:
    return _oracle_summary_from_rust_payload(_load_rust_summary_payload(stdout))
```

现有 full-pipeline `run_rust_cli_release` 和 `OracleRunSummary` 行为保持不变。

- [ ] **Step 4: Implement the Python internal payload timer**

在测试 helper 中完成配置和输入校验后再启动表；payload 返回即停表，之后才构建摘要：

```python
def run_python_check_only_payload(pipeline: str, input_path: Path) -> TimedPayloadRun:
    try:
        pipeline_config = PIPELINES[pipeline]
    except KeyError as exc:
        raise AssertionError(f'unknown Python oracle pipeline: {pipeline!r}') from exc

    request = _build_request(
        config=pipeline_config,
        input_file=input_path,
        month_range=None,
        benchmark=True,
    )
    prepared, validation_error = costing_service._prepare_request(
        request,
        validate_output_dir=False,
    )
    if validation_error is not None or prepared is None:
        message = validation_error.message if validation_error is not None else 'missing prepared request'
        raise AssertionError(f'python check-only input validation failed: {message}')

    etl = costing_service._build_etl(request, prepared.month_range)
    etl._reset_last_run_state()

    started = time.perf_counter()
    payload = etl.pipeline.build_workbook_payload(
        input_path,
        standalone_cost_items=etl.standalone_cost_items,
        product_anomaly_scope_mode=etl.product_anomaly_scope_mode,
        month_range=etl.month_range,
        presentation_product_order=etl.product_order,
        artifacts_transform=etl._filter_analysis_artifacts_by_whitelist,
        progress_callback=None,
    )
    payload_total_seconds = time.perf_counter() - started
    if not math.isfinite(payload_total_seconds) or payload_total_seconds < 0:
        raise AssertionError(f'invalid Python payload total: {payload_total_seconds!r}')

    issue_type_counts: dict[str, int] = {}
    error_frame = payload.error_log_export
    if not error_frame.empty and 'issue_type' in error_frame.columns:
        issue_type_counts = {
            str(issue_type): int(count)
            for issue_type, count in error_frame['issue_type'].value_counts().items()
        }
    runtime_summary = OracleRunSummary(
        error_log_count=payload.error_log_count,
        issue_type_counts=issue_type_counts,
        quality_metrics=_quality_metric_values(payload.quality_metrics),
    )
    return TimedPayloadRun(
        pipeline=pipeline,
        payload_total_seconds=payload_total_seconds,
        stage_timings={name: float(value) for name, value in payload.stage_timings.items()},
        runtime_summary=runtime_summary,
    )
```

需要增加 `hashlib`、`math`、`time`、`field` 和 `from src.services import costing_service` imports；不得把这个 helper 移到 `src/`。

- [ ] **Step 5: Implement the Rust check-only runner and parser**

```python
def run_rust_cli_release_check_only(
    executable: Path,
    pipeline: str,
    input_path: Path,
) -> TimedPayloadRun:
    completed = subprocess.run(  # noqa: S603 - fixed local executable and arguments.
        [
            str(executable),
            pipeline,
            '--input',
            str(input_path.resolve()),
            '--check-only',
            '--benchmark',
        ],
        check=False,
        capture_output=True,
        cwd=repo_root(),
        encoding='utf-8',
        errors='replace',
    )
    if completed.returncode != 0:
        raise AssertionError(
            f'rust check-only failed\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}'
        )
    result = parse_rust_check_only_run(completed.stdout)
    if result.pipeline != pipeline:
        raise AssertionError(
            f'Rust check-only reported pipeline {result.pipeline!r}, expected {pipeline!r}'
        )
    return result


def parse_rust_check_only_run(stdout: str) -> TimedPayloadRun:
    payload = _load_rust_summary_payload(stdout)
    if payload.get('status') != 'succeeded':
        raise AssertionError("Rust check-only must report status='succeeded'")
    pipeline = payload.get('pipeline')
    if pipeline not in {'gb', 'sk'}:
        raise AssertionError(f'invalid Rust check-only pipeline: {pipeline!r}')
    if payload.get('sheet_count') != 3:
        raise AssertionError('Rust check-only must build exactly three in-memory sheets')
    if payload.get('output_written') is not False:
        raise AssertionError('Rust check-only must report output_written=false')
    if payload.get('workbook_path') is not None:
        raise AssertionError('Rust check-only must not report a workbook path')

    stage_timings = _parse_rust_stage_timings(payload, require_export=False)
    total = stage_timings['total']

    run_counts = _parse_rust_run_counts(payload)

    return TimedPayloadRun(
        pipeline=pipeline,
        payload_total_seconds=total,
        stage_timings=stage_timings,
        runtime_summary=_oracle_summary_from_rust_payload(payload),
        run_counts=run_counts,
    )
```

- [ ] **Step 6: Implement strict stage parsing and real normal-mode export evidence**

```python
def _parse_rust_stage_timings(
    payload: dict[str, Any],
    *,
    require_export: bool,
) -> dict[str, float]:
    timing_payload = payload.get('stage_timings')
    if not isinstance(timing_payload, dict) or not isinstance(timing_payload.get('stages'), dict):
        raise AssertionError("Rust field 'stage_timings.stages' must be an object")

    parsed: dict[str, float] = {}
    for name, raw_seconds in timing_payload['stages'].items():
        if not isinstance(name, str):
            raise AssertionError('Rust stage names must be strings')
        if isinstance(raw_seconds, bool) or not isinstance(raw_seconds, (int, float)):
            raise AssertionError(f'Rust stage {name!r} must be numeric')
        seconds = float(raw_seconds)
        if not math.isfinite(seconds) or seconds < 0:
            raise AssertionError(f'Rust stage {name!r} must be finite and non-negative')
        parsed[name] = seconds

    missing = [name for name in REQUIRED_RUST_PAYLOAD_STAGES if name not in parsed]
    if missing:
        raise AssertionError(f'Rust payload stages missing: {missing!r}')
    if require_export:
        if 'export' not in parsed:
            raise AssertionError('Rust normal benchmark must report export stage')
    elif 'export' in parsed:
        raise AssertionError('Rust check-only stage timings must not contain export')
    return parsed


def _parse_rust_run_counts(payload: dict[str, Any]) -> dict[str, int]:
    raw = payload.get('run_counts')
    if not isinstance(raw, dict):
        raise AssertionError("Rust field 'run_counts' must be an object")
    missing = [name for name in REQUIRED_RUST_RUN_COUNTS if name not in raw]
    if missing:
        raise AssertionError(f'Rust run counts missing: {missing!r}')
    parsed: dict[str, int] = {}
    for name, count in raw.items():
        if not isinstance(name, str) or isinstance(count, bool) or not isinstance(count, int):
            raise AssertionError('Rust run_counts must map strings to integers')
        if count < 0:
            raise AssertionError(f'Rust run count {name!r} must be non-negative')
        parsed[name] = count
    return parsed


def capture_rust_normal_benchmark_evidence(
    executable: Path,
    pipeline: str,
    input_path: Path,
    output_path: Path,
    evidence_path: Path,
) -> None:
    input_path = input_path.resolve()
    executable = executable.resolve()
    output_path = output_path.resolve()
    evidence_path = evidence_path.resolve()
    input_sha256 = _file_sha256(input_path)
    binary_sha256 = _file_sha256(executable)
    if output_path.exists():
        raise AssertionError(f'normal benchmark output already exists: {output_path}')
    completed = subprocess.run(  # noqa: S603 - fixed local executable and arguments.
        [
            str(executable),
            pipeline,
            '--input',
            str(input_path),
            '--output',
            str(output_path),
            '--benchmark',
        ],
        check=False,
        capture_output=True,
        cwd=repo_root(),
        encoding='utf-8',
        errors='replace',
    )
    try:
        if completed.returncode != 0:
            raise AssertionError(
                f'Rust normal benchmark failed\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}'
            )
        payload = _load_rust_summary_payload(completed.stdout)
        if payload.get('status') != 'succeeded' or payload.get('pipeline') != pipeline:
            raise AssertionError('Rust normal benchmark reported an invalid status or pipeline')
        if payload.get('output_written') is not True or payload.get('sheet_count') != 3:
            raise AssertionError('Rust normal benchmark must write one three-sheet workbook')
        if Path(str(payload.get('workbook_path'))).resolve() != output_path:
            raise AssertionError('Rust normal benchmark reported an unexpected workbook path')
        _parse_rust_stage_timings(payload, require_export=True)
        _parse_rust_run_counts(payload)
        summary = _oracle_summary_from_rust_payload(payload)
        if sum(summary.issue_type_counts.values()) != summary.error_log_count:
            raise AssertionError('Rust normal benchmark issue counts do not sum to error_log_count')
        if not output_path.is_file():
            raise AssertionError('Rust normal benchmark did not create its workbook')
        if _file_sha256(input_path) != input_sha256 or _file_sha256(executable) != binary_sha256:
            raise AssertionError('normal benchmark input or executable changed during capture')
        payload['input_sha256'] = input_sha256
        payload['rust_binary_sha256'] = binary_sha256
        payload['working_directory'] = str(repo_root())
        payload['command_arguments'] = [
            pipeline,
            '--input',
            str(input_path),
            '--output',
            str(output_path),
            '--benchmark',
        ]
        evidence_path.parent.mkdir(parents=True, exist_ok=True)
        evidence_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )
    finally:
        output_path.unlink(missing_ok=True)
```

`test_capture_rust_normal_benchmark_evidence_requires_export_and_deletes_workbook` 的 fake subprocess 在 `tmp_path` 创建 workbook 并返回含六个 payload stage + `export` 的 JSON；断言 evidence JSON 可按 UTF-8 读回、原 workbook 已删除。另一个参数化测试分别删除一个 required stage、写入 `NaN/inf/-1/bool/str`，断言 parser 指出具体 stage。

```python
def _file_sha256(path: Path) -> str:
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()
```

Task 3 因此增加 `hashlib` import。normal evidence 在原 RunSummary JSON 顶层补充 input/binary SHA、工作目录和实际命令参数，不改生产 CLI 输出结构。

- [ ] **Step 7: Run Python unit/lint checks and commit**

```powershell
uv run python -m pytest tests/rust_oracle/test_oracle_runner.py -q --basetemp .pytest-tmp/oracle-runner-timing
uv run python -m ruff check tests/rust_oracle/oracle_runner.py tests/rust_oracle/test_oracle_runner.py
uv run python -m ruff format tests/rust_oracle/oracle_runner.py tests/rust_oracle/test_oracle_runner.py --check
git diff --check -- tests/rust_oracle/oracle_runner.py tests/rust_oracle/test_oracle_runner.py
git add -- tests/rust_oracle/oracle_runner.py tests/rust_oracle/test_oracle_runner.py
git commit -m "test(oracle): measure check-only payload boundaries"
```

---

### Task 4: Add the Fixed Five-Round Paired Check-Only Benchmark

**Files:**
- Modify: `tests/rust_oracle/benchmark.py`
- Create: `tests/rust_oracle/test_benchmark.py`

**Interfaces:**
- Consumes: Task 3 `TimedPayloadRun` 和两个 runner；现有 `ValidationFailure`、`assert_runtime_contract_matches`。
- Produces: 固定 `CHECK_ONLY_WARMUPS=1`、`CHECK_ONLY_ROUNDS=5`、`RuntimeEvidence`、`CheckOnlyBenchmarkResult`、`run_check_only_payload_benchmark`、`classify_check_only_verdict`、`compare_non_target_stage_medians`、`assert_same_input_sha256` 和 `write_check_only_benchmark_result` UTF-8 JSON writer。

固定结果类型；正式函数不暴露可把 5 轮调小的参数：

```python
CHECK_ONLY_WARMUPS = 1
CHECK_ONLY_ROUNDS = 5


@dataclass(frozen=True)
class QualityMetricEvidence:
    category: str
    metric: str
    value: str


@dataclass(frozen=True)
class RuntimeEvidence:
    run_counts: dict[str, int]
    error_log_count: int
    issue_type_counts: dict[str, int]
    quality_metrics: tuple[QualityMetricEvidence, ...]


@dataclass(frozen=True)
class StageRegression:
    stage: str
    baseline_median_seconds: float
    current_median_seconds: float
    current_to_baseline_ratio: float


@dataclass(frozen=True)
class CheckOnlyBenchmarkResult:
    pipeline: str
    input_sha256: str
    rust_executable: str
    rust_binary_sha256: str
    git_head: str
    working_tree_diff_id: str
    working_directory: str
    command_arguments: tuple[str, ...]
    python_payload_total_seconds: tuple[float, ...]
    rust_payload_total_seconds: tuple[float, ...]
    python_median_seconds: float
    python_min_seconds: float
    python_max_seconds: float
    rust_median_seconds: float
    rust_min_seconds: float
    rust_max_seconds: float
    rust_stage_seconds: dict[str, tuple[float, ...]]
    rust_stage_median_seconds: dict[str, float]
    rust_runtime_evidence: RuntimeEvidence
    valid_pair_count: int
    validation_passed: bool
    verdict: str
    validation_failures: tuple[ValidationFailure, ...] = ()
```

- [ ] **Step 1: Write pure unit tests for order, statistics, and evidence**

在 `tests/rust_oracle/test_benchmark.py` 用 monkeypatch fake runner 新增：

- `test_check_only_benchmark_warms_each_runtime_once`
- `test_check_only_benchmark_alternates_five_paired_rounds`
- `test_check_only_verdict_rejects_four_complete_rounds`
- `test_check_only_verdict_rejects_runtime_mismatch`
- `test_check_only_result_reports_min_max_median_and_rust_stage_medians`
- `test_check_only_result_requires_five_values_for_every_required_rust_stage`
- `test_check_only_benchmark_rejects_runtime_evidence_drift_between_rounds`
- `test_runtime_evidence_requires_issue_counts_to_sum_to_error_count`
- `test_check_only_benchmark_rejects_input_hash_change`
- `test_check_only_benchmark_rejects_binary_or_working_tree_change`
- `test_compare_non_target_stage_medians_reports_more_than_five_percent`
- `test_assert_same_input_sha256_rejects_mixed_evidence`
- `test_check_only_result_writer_uses_utf8_json`

交错顺序的完整断言为：

```python
assert calls == [
    'warmup-rust',
    'warmup-python',
    'round-1-rust',
    'round-1-python',
    'round-2-python',
    'round-2-rust',
    'round-3-rust',
    'round-3-python',
    'round-4-python',
    'round-4-rust',
    'round-5-rust',
    'round-5-python',
]
```

证据不完整必须使用稳定 verdict：

```python
assert classify_check_only_verdict(
    rust_seconds=(1.0, 1.0, 1.0, 1.0),
    python_seconds=(2.0, 2.0, 2.0, 2.0),
    rust_stage_seconds={},
    valid_pair_count=4,
    validation_failures=(),
) == 'INCOMPLETE_EVIDENCE'
```

stage 完整性测试使用精确 fixture：

```python
five = (1.0,) * CHECK_ONLY_ROUNDS
stages = {name: five for name in REQUIRED_RUST_PAYLOAD_STAGES}
del stages['normalize']
assert classify_check_only_verdict(
    rust_seconds=five,
    python_seconds=(2.0,) * CHECK_ONLY_ROUNDS,
    rust_stage_seconds=stages,
    valid_pair_count=CHECK_ONLY_ROUNDS,
    validation_failures=(),
) == 'INCOMPLETE_EVIDENCE'
```

其余 benchmark 单测统一复用 `_timed_run(pipeline, total, stage_seconds, run_counts, summary)` fixture；每个测试必须显式传六个 required stage，只有被测“缺 stage”用例例外。

- [ ] **Step 2: Run tests and verify red**

```powershell
uv run python -m pytest tests/rust_oracle/test_benchmark.py -q --basetemp .pytest-tmp/check-only-benchmark-unit
```

Expected: FAIL，因为 check-only 结果类型和函数未定义。

- [ ] **Step 3: Implement fixed-round verdict and statistics**

`classify_check_only_verdict` 按以下固定优先级返回：

```python
def classify_check_only_verdict(
    *,
    rust_seconds: tuple[float, ...],
    python_seconds: tuple[float, ...],
    rust_stage_seconds: dict[str, tuple[float, ...]],
    valid_pair_count: int,
    validation_failures: tuple[ValidationFailure, ...],
) -> str:
    if len(rust_seconds) != CHECK_ONLY_ROUNDS or len(python_seconds) != CHECK_ONLY_ROUNDS:
        return 'INCOMPLETE_EVIDENCE'
    if valid_pair_count != CHECK_ONLY_ROUNDS:
        return 'INCOMPLETE_EVIDENCE'
    if set(rust_stage_seconds) != set(REQUIRED_RUST_PAYLOAD_STAGES):
        return 'INCOMPLETE_EVIDENCE'
    if any(len(values) != CHECK_ONLY_ROUNDS for values in rust_stage_seconds.values()):
        return 'INCOMPLETE_EVIDENCE'
    if any(failure.verdict == 'INCOMPLETE_EVIDENCE' for failure in validation_failures):
        return 'INCOMPLETE_EVIDENCE'
    if validation_failures:
        return validation_failures[0].verdict
    if statistics.median(rust_seconds) > statistics.median(python_seconds):
        return 'PERFORMANCE_REGRESSION'
    return 'VALIDATED'
```

主循环必须：

1. 运行前记录 input/binary SHA-256、Git HEAD、working-tree diff ID、工作目录和实际命令参数；
2. Rust/Python 各预热一次，不写入正式数组；
3. 5 个 round 按奇偶顺序调用；
4. 每对调用 `assert_runtime_contract_matches`，并要求五轮 Rust `RuntimeEvidence` 完全相同；
5. 运行后重新计算 input/binary SHA、Git/diff ID，任一变化即加入 `INCOMPLETE_EVIDENCE` failure；
6. 对六个 required Rust stage 各收集恰好 5 个值，保存原始数组后再计算 median；
7. 正式 JSON 持久化 run counts、error/issue counts、quality metrics、命令环境、min/max/median、原始数组、有效 pair 数和 verdict。

```python
def run_check_only_payload_benchmark(
    pipeline: str,
    input_path: Path,
    rust_executable: Path,
) -> CheckOnlyBenchmarkResult:
    input_path = input_path.resolve()
    rust_executable = rust_executable.resolve()
    input_sha256 = _sha256(input_path)
    binary_sha256 = _sha256(rust_executable)
    git_head, working_tree_diff_id = _git_evidence()
    command_arguments = (
        pipeline,
        '--input',
        str(input_path),
        '--check-only',
        '--benchmark',
    )

    run_rust_cli_release_check_only(rust_executable, pipeline, input_path)
    run_python_check_only_payload(pipeline, input_path)

    python_seconds: list[float] = []
    rust_seconds: list[float] = []
    rust_stage_values = {name: [] for name in REQUIRED_RUST_PAYLOAD_STAGES}
    validation_failures: list[ValidationFailure] = []
    valid_pair_count = 0
    first_rust_evidence: RuntimeEvidence | None = None

    for round_index in range(CHECK_ONLY_ROUNDS):
        if round_index % 2 == 0:
            rust_run = run_rust_cli_release_check_only(rust_executable, pipeline, input_path)
            python_run = run_python_check_only_payload(pipeline, input_path)
        else:
            python_run = run_python_check_only_payload(pipeline, input_path)
            rust_run = run_rust_cli_release_check_only(rust_executable, pipeline, input_path)

        rust_seconds.append(rust_run.payload_total_seconds)
        python_seconds.append(python_run.payload_total_seconds)
        for stage in REQUIRED_RUST_PAYLOAD_STAGES:
            rust_stage_values[stage].append(rust_run.stage_timings[stage])
        rust_evidence = _runtime_evidence(rust_run)
        try:
            assert_runtime_contract_matches(python_run.runtime_summary, rust_run.runtime_summary)
            if first_rust_evidence is not None and rust_evidence != first_rust_evidence:
                raise AssertionError('Rust runtime evidence changed between formal rounds')
        except AssertionError as exc:
            validation_failures.append(
                ValidationFailure('ETL_MISMATCH', f'round {round_index + 1}: {exc}')
            )
        else:
            valid_pair_count += 1
        if first_rust_evidence is None:
            first_rust_evidence = rust_evidence

    if _sha256(input_path) != input_sha256:
        validation_failures.append(
            ValidationFailure('INCOMPLETE_EVIDENCE', 'input SHA-256 changed during benchmark')
        )
    if _sha256(rust_executable) != binary_sha256:
        validation_failures.append(
            ValidationFailure('INCOMPLETE_EVIDENCE', 'Rust binary SHA-256 changed during benchmark')
        )
    if _git_evidence() != (git_head, working_tree_diff_id):
        validation_failures.append(
            ValidationFailure('INCOMPLETE_EVIDENCE', 'Git working state changed during benchmark')
        )

    rust_values = tuple(rust_seconds)
    python_values = tuple(python_seconds)
    failures = tuple(validation_failures)
    stage_values = {stage: tuple(values) for stage, values in rust_stage_values.items()}
    if first_rust_evidence is None:
        raise AssertionError('formal benchmark produced no Rust runtime evidence')
    verdict = classify_check_only_verdict(
        rust_seconds=rust_values,
        python_seconds=python_values,
        rust_stage_seconds=stage_values,
        valid_pair_count=valid_pair_count,
        validation_failures=failures,
    )
    return CheckOnlyBenchmarkResult(
        pipeline=pipeline,
        input_sha256=input_sha256,
        rust_executable=str(rust_executable),
        rust_binary_sha256=binary_sha256,
        git_head=git_head,
        working_tree_diff_id=working_tree_diff_id,
        working_directory=str(repo_root()),
        command_arguments=command_arguments,
        python_payload_total_seconds=python_values,
        rust_payload_total_seconds=rust_values,
        python_median_seconds=statistics.median(python_values),
        python_min_seconds=min(python_values),
        python_max_seconds=max(python_values),
        rust_median_seconds=statistics.median(rust_values),
        rust_min_seconds=min(rust_values),
        rust_max_seconds=max(rust_values),
        rust_stage_seconds=stage_values,
        rust_stage_median_seconds={
            stage: statistics.median(values) for stage, values in rust_stage_values.items()
        },
        rust_runtime_evidence=first_rust_evidence,
        valid_pair_count=valid_pair_count,
        validation_passed=not failures and valid_pair_count == CHECK_ONLY_ROUNDS,
        verdict=verdict,
        validation_failures=failures,
    )
```

证据 helper 固定为：

```python
def _runtime_evidence(run: TimedPayloadRun) -> RuntimeEvidence:
    summary = run.runtime_summary
    if sum(summary.issue_type_counts.values()) != summary.error_log_count:
        raise AssertionError('issue_type_counts must sum to error_log_count')
    quality_metrics = tuple(
        QualityMetricEvidence(category, metric, value)
        for (category, metric), value in sorted(summary.quality_metrics.items())
    )
    return RuntimeEvidence(
        run_counts=dict(sorted(run.run_counts.items())),
        error_log_count=summary.error_log_count,
        issue_type_counts=dict(sorted(summary.issue_type_counts.items())),
        quality_metrics=quality_metrics,
    )


def _git_evidence() -> tuple[str, str]:
    root = repo_root()
    head = _run_git(root, 'rev-parse', 'HEAD').strip()
    status = _run_git(root, 'status', '--porcelain=v1')
    diff = _run_git(root, 'diff', '--binary', 'HEAD', '--')
    diff_id = hashlib.sha256(f'{status}\n{diff}'.encode('utf-8')).hexdigest()
    return head, diff_id


def compare_non_target_stage_medians(
    baseline_path: Path,
    current_path: Path,
    *,
    target_stages: frozenset[str],
) -> tuple[StageRegression, ...]:
    baseline = json.loads(baseline_path.read_text(encoding='utf-8'))
    current = json.loads(current_path.read_text(encoding='utf-8'))
    if baseline['pipeline'] != current['pipeline'] or baseline['input_sha256'] != current['input_sha256']:
        raise AssertionError('stage comparison requires the same pipeline and input SHA-256')
    regressions: list[StageRegression] = []
    for stage in REQUIRED_RUST_PAYLOAD_STAGES:
        if stage == 'total' or stage in target_stages:
            continue
        before = float(baseline['rust_stage_median_seconds'][stage])
        after = float(current['rust_stage_median_seconds'][stage])
        ratio = after / before if before > 0 else (1.0 if after == 0 else math.inf)
        if ratio > 1.05:
            regressions.append(StageRegression(stage, before, after, ratio))
    return tuple(regressions)


def assert_same_input_sha256(evidence_paths: tuple[Path, ...]) -> str:
    if not evidence_paths:
        raise AssertionError('at least one evidence path is required')
    hashes: list[str] = []
    for path in evidence_paths:
        payload = json.loads(path.read_text(encoding='utf-8-sig'))
        value = payload.get('input_sha256')
        if not isinstance(value, str) or len(value) != 64:
            raise AssertionError(f'evidence has invalid input_sha256: {path}')
        hashes.append(value)
    if len(set(hashes)) != 1:
        raise AssertionError(f'performance evidence input SHA-256 mismatch: {hashes!r}')
    return hashes[0]
```

`_run_git` 先用 `shutil.which('git')` 解析绝对 executable，再使用 `subprocess.run([git, '-C', str(root), *args], check=False, capture_output=True, encoding='utf-8', errors='replace')`；该固定本地命令行标注 `# noqa: S603`，缺 executable 或非零退出抛 `AssertionError`。`_sha256` 使用 `hashlib.file_digest(stream, 'sha256').hexdigest()`。Task 4 相应增加 `dataclasses`、`hashlib`、`json`、`math`、`shutil`、`subprocess` imports，并从 `oracle_runner` 导入 `REQUIRED_RUST_PAYLOAD_STAGES`/`TimedPayloadRun`。

每阶段调用 `compare_non_target_stage_medians`：非空时必须重跑第二份完整五轮 current evidence，并写入对应的 ignored 文件：`stage-regression-review-phase1.json`、`stage-regression-review-phase2.json`、`stage-regression-review-phase3-typed.json`、`stage-regression-review-phase3.json` 或 `stage-regression-review-final.json`。

每份 review JSON 的必填键固定为 `phase`、`pipeline`、`first_evidence`、`second_evidence`、`first_regressions`、`second_regressions`、`profiler_evidence`、`explanation`、`data_auditor_verdict`；每个已出现的 regression 都写完整 `stage/baseline_median_seconds/current_median_seconds/current_to_baseline_ratio`。第二次已恢复到阈值内时 `second_regressions` 可为空，但仍需把解释明确标为测量噪声并经 data auditor 确认。没有第二份五轮、对应 profiler/噪声依据、具体解释或 data-auditor `APPROVED` 时，不得进入下一阶段/`STOP_SUCCESS`。

结果 writer 固定为：

```python
def write_check_only_benchmark_result(
    result: CheckOnlyBenchmarkResult,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(dataclasses.asdict(result), ensure_ascii=False, indent=2),
        encoding='utf-8',
    )
```

- [ ] **Step 4: Run unit/lint checks and commit**

```powershell
uv run python -m pytest tests/rust_oracle/test_benchmark.py -q --basetemp .pytest-tmp/check-only-benchmark-unit
uv run python -m ruff check tests/rust_oracle/benchmark.py tests/rust_oracle/test_benchmark.py
uv run python -m ruff format tests/rust_oracle/benchmark.py tests/rust_oracle/test_benchmark.py --check
git diff --check -- tests/rust_oracle/benchmark.py tests/rust_oracle/test_benchmark.py
git add -- tests/rust_oracle/benchmark.py tests/rust_oracle/test_benchmark.py
git commit -m "test(perf): add paired check-only benchmark"
```

---

### Task 5: Require GB and SK Performance Evidence Without Skips

**Files:**
- Modify: `tests/rust_oracle/repo_paths.py`
- Modify: `tests/rust_oracle/test_benchmark.py`
- Modify: `tests/test_full_rust_cli_benchmark.py`

**Interfaces:**
- Consumes: Task 4 benchmark、现有 `build_rust_cli_release` 和 full `run_same_machine_benchmark(repeats=3)`。
- Produces: `require_benchmark_sample(pipeline: str) -> Path`；现有 full gate 的样本缺失直接 fail。严格 check-only pytest 文件延后到最终验收任务创建，避免基线已知不达标时故意长期保持测试套件红灯。

- [ ] **Step 1: Write sample-resolution tests**

在纯单测中覆盖：

- `test_require_benchmark_sample_rejects_unknown_pipeline`
- `test_require_benchmark_sample_fails_when_sample_is_missing`
- `test_require_benchmark_sample_does_not_fallback_from_invalid_environment_path`
- `test_require_benchmark_sample_fails_when_multiple_samples_exist`
- `test_require_benchmark_sample_returns_the_only_absolute_xlsx_path`

环境变量与目录通过 monkeypatch 隔离；不得读取真实敏感样本。

缺样本测试的完整 fixture/断言：

```python
def test_require_benchmark_sample_fails_when_sample_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv('COSTING_GB_SAMPLE', raising=False)
    monkeypatch.setattr(repo_paths, 'repo_root', lambda: tmp_path)
    (tmp_path / 'data' / 'raw' / 'gb').mkdir(parents=True)

    with pytest.raises(AssertionError, match='requires exactly one sample'):
        require_benchmark_sample('gb')
```

其余四例复用同一 `tmp_path/repo_root` fixture：invalid env 明确不得回退；多样本创建两个 `gb-*.xlsx` 并断言 `found 2`；成功例创建唯一文件并断言返回 `resolve()` 后绝对路径。

- [ ] **Step 2: Run sample tests and verify red**

```powershell
uv run python -m pytest tests/rust_oracle/test_benchmark.py -q --basetemp .pytest-tmp/performance-samples
```

Expected: FAIL，因为 `require_benchmark_sample` 尚未定义。

- [ ] **Step 3: Implement strict sample resolution by reusing repo_paths.py**

```python
def require_benchmark_sample(pipeline: str) -> Path:
    env_names = {'gb': 'COSTING_GB_SAMPLE', 'sk': 'COSTING_SK_SAMPLE'}
    try:
        env_name = env_names[pipeline]
    except KeyError as exc:
        raise AssertionError(f'unsupported benchmark pipeline: {pipeline!r}') from exc

    configured = os.environ.get(env_name)
    if configured:
        path = Path(configured).expanduser().resolve()
        if not path.is_file() or path.suffix.lower() != '.xlsx':
            raise AssertionError(f'{env_name} must point to an existing .xlsx file: {path}')
        return path

    raw_dir = repo_root() / 'data' / 'raw' / pipeline
    candidates = sorted(
        path.resolve()
        for path in raw_dir.glob(f'{pipeline}-*.xlsx')
        if path.is_file()
    )
    if len(candidates) != 1:
        raise AssertionError(
            f'{pipeline} benchmark requires exactly one sample in {raw_dir}; '
            f'found {len(candidates)}. Set {env_name} explicitly.'
        )
    return candidates[0]
```

增加 `import os`；不新建重复的 sample utility 模块。

- [ ] **Step 4: Remove sample skipif from the existing full benchmark**

删除 `_sample_from_env`、`_first_sample`、两处 `@pytest.mark.skipif`，并删除随之未使用的 `os`、`repo_root` imports；`pytest` 仍被 `pytest.MonkeyPatch` 使用，必须保留。两个既有测试保留原名和三轮语义：

```python
def test_gb_rust_benchmark_validated(tmp_path: Path) -> None:
    result = run_same_machine_benchmark(
        'gb',
        require_benchmark_sample('gb'),
        tmp_path,
        repeats=3,
    )
    assert result.verdict == 'VALIDATED', result


def test_sk_rust_benchmark_validated(tmp_path: Path) -> None:
    result = run_same_machine_benchmark(
        'sk',
        require_benchmark_sample('sk'),
        tmp_path,
        repeats=3,
    )
    assert result.verdict == 'VALIDATED', result
```

- [ ] **Step 5: Verify full-benchmark collection completeness**

```powershell
uv run python -m pytest tests/test_full_rust_cli_benchmark.py --collect-only -q
```

Expected: 列出两个既有 pipeline benchmark。文件未收集时 pytest exit 5，不能继续验收。

- [ ] **Step 6: Run unit/lint checks and commit the sample gate**

先不在普通单元步骤执行长达数分钟的真实 benchmark；Task 7 基线会执行。此处运行：

```powershell
uv run python -m pytest tests/rust_oracle/test_benchmark.py tests/rust_oracle/test_oracle_runner.py -q --basetemp .pytest-tmp/perf-harness-unit
uv run python -m ruff check tests/rust_oracle tests/test_full_rust_cli_benchmark.py
uv run python -m ruff format tests/rust_oracle tests/test_full_rust_cli_benchmark.py --check
git diff --check -- tests/rust_oracle/repo_paths.py tests/rust_oracle/test_benchmark.py tests/test_full_rust_cli_benchmark.py
git add -- tests/rust_oracle/repo_paths.py tests/rust_oracle/test_benchmark.py tests/test_full_rust_cli_benchmark.py
git commit -m "test(perf): require gb and sk benchmark evidence"
```

---

### Task 6: Add a Reproducible Windows Peak Working Set Harness

**Files:**
- Create: `tests/rust_oracle/measure_peak_working_set.ps1`
- Create: `tests/rust_oracle/capture_cpu_profile.ps1`

**Interfaces:**
- Consumes: 普通 release executable、固定绝对 `.xlsx` 输入、仓库根路径。
- Produces: UTF-8 JSON，包含 input/binary SHA-256、Git HEAD、dirty diff 标识、5 个原始字节值、中位数、ratio 和 `BASELINE_RECORDED`/`VALIDATED`/`MEMORY_REGRESSION` verdict；另提供可重复的 WPR CPU trace 脚本。

- [ ] **Step 1: Write the script with fixed formal sampling constants**

正式参数只允许选择 pipeline 和路径，不暴露可把 warmup/round 降低的开关：

```powershell
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet('gb', 'sk')]
    [string] $Pipeline,

    [Parameter(Mandatory)]
    [string] $InputPath,

    [Parameter(Mandatory)]
    [string] $BaselineExecutable,

    [string] $CurrentExecutable = '',

    [Parameter(Mandatory)]
    [string] $ResultPath
)

$ErrorActionPreference = 'Stop'
$Warmups = 1
$Rounds = 5
$PollMilliseconds = 50
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
```

实现以下完整 helper；业务进程必须直接启动 executable，不得启动 Cargo：

```powershell
function Resolve-RequiredFile([string] $Path, [string] $Label) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "$Label does not exist or is not a file: $Path"
    }
    return (Resolve-Path -LiteralPath $Path).Path
}

function Get-Median([long[]] $Values) {
    if ($Values.Count -ne $Rounds) {
        throw "expected $Rounds values, got $($Values.Count)"
    }
    $sorted = @($Values | Sort-Object)
    return [long] $sorted[[int][Math]::Floor($sorted.Count / 2)]
}

function Get-TextSha256([string] $Value) {
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($Value)
        $hash = $sha.ComputeHash($bytes)
        return -join ($hash | ForEach-Object { $_.ToString('x2') })
    }
    finally {
        $sha.Dispose()
    }
}

function Invoke-PeakSample([string] $Executable, [string] $InputWorkbook) {
    if ($InputWorkbook.Contains('"')) {
        throw "input workbook path cannot contain a quote: $InputWorkbook"
    }
    $arguments = @(
        $Pipeline,
        '--input',
        ('"{0}"' -f $InputWorkbook),
        '--check-only',
        '--benchmark'
    )
    $process = Start-Process `
        -FilePath $Executable `
        -ArgumentList $arguments `
        -WorkingDirectory $RepoRoot `
        -WindowStyle Hidden `
        -PassThru
    $peakBytes = [long] 0
    while (-not $process.HasExited) {
        $process.Refresh()
        if ($process.PeakWorkingSet64 -gt $peakBytes) {
            $peakBytes = [long] $process.PeakWorkingSet64
        }
        Start-Sleep -Milliseconds $PollMilliseconds
    }
    $process.WaitForExit()
    $process.Refresh()
    if ($process.PeakWorkingSet64 -gt $peakBytes) {
        $peakBytes = [long] $process.PeakWorkingSet64
    }
    if ($process.ExitCode -ne 0) {
        throw "benchmark process failed with exit code $($process.ExitCode): $Executable"
    }
    return $peakBytes
}
```

主流程严格先预热再采 5 次；有 current 时奇偶轮交错：

```powershell
$input = Resolve-RequiredFile $InputPath 'input workbook'
$baseline = Resolve-RequiredFile $BaselineExecutable 'baseline executable'
$inputSha256 = (Get-FileHash -LiteralPath $input -Algorithm SHA256).Hash.ToLowerInvariant()
$baselineSha256 = (Get-FileHash -LiteralPath $baseline -Algorithm SHA256).Hash.ToLowerInvariant()
$current = if ($CurrentExecutable) {
    Resolve-RequiredFile $CurrentExecutable 'current executable'
}
else {
    $null
}
$currentSha256 = if ($null -ne $current) {
    (Get-FileHash -LiteralPath $current -Algorithm SHA256).Hash.ToLowerInvariant()
}
else {
    $null
}
$commandArguments = @(
    $Pipeline,
    '--input',
    $input,
    '--check-only',
    '--benchmark'
)

for ($index = 0; $index -lt $Warmups; $index++) {
    [void] (Invoke-PeakSample $baseline $input)
    if ($null -ne $current) {
        [void] (Invoke-PeakSample $current $input)
    }
}

$baselineValues = [System.Collections.Generic.List[long]]::new()
$currentValues = [System.Collections.Generic.List[long]]::new()
for ($round = 1; $round -le $Rounds; $round++) {
    if (($round % 2 -eq 1) -or ($null -eq $current)) {
        $baselineValues.Add((Invoke-PeakSample $baseline $input))
        if ($null -ne $current) {
            $currentValues.Add((Invoke-PeakSample $current $input))
        }
    }
    else {
        $currentValues.Add((Invoke-PeakSample $current $input))
        $baselineValues.Add((Invoke-PeakSample $baseline $input))
    }
}

$baselineMedian = Get-Median $baselineValues.ToArray()
if ($baselineMedian -le 0) {
    throw "baseline peak working set median must be positive, got $baselineMedian"
}
$currentMedian = if ($null -ne $current) { Get-Median $currentValues.ToArray() } else { $null }
$ratio = if ($null -ne $currentMedian) { [double] $currentMedian / [double] $baselineMedian } else { $null }
$verdict = if ($null -eq $currentMedian) {
    'BASELINE_RECORDED'
}
elseif ($ratio -le 1.05) {
    'VALIDATED'
}
else {
    'MEMORY_REGRESSION'
}

$inputSha256After = (Get-FileHash -LiteralPath $input -Algorithm SHA256).Hash.ToLowerInvariant()
if ($inputSha256After -ne $inputSha256) {
    throw "input workbook SHA-256 changed during sampling: $input"
}
if ((Get-FileHash -LiteralPath $baseline -Algorithm SHA256).Hash.ToLowerInvariant() -ne $baselineSha256) {
    throw "baseline executable SHA-256 changed during sampling: $baseline"
}
if (
    ($null -ne $current) -and
    ((Get-FileHash -LiteralPath $current -Algorithm SHA256).Hash.ToLowerInvariant() -ne $currentSha256)
) {
    throw "current executable SHA-256 changed during sampling: $current"
}
$workingTreeStatus = (& git -C $RepoRoot status --porcelain=v1 | Out-String).Trim()
$workingTreeDiff = (& git -C $RepoRoot diff --binary HEAD -- | Out-String).Trim()
$workingTreeState = $workingTreeStatus + "`n" + $workingTreeDiff
$result = [ordered]@{
    pipeline = $Pipeline
    input_path = $input
    input_sha256 = $inputSha256
    baseline_executable = $baseline
    baseline_sha256 = $baselineSha256
    current_executable = $current
    current_sha256 = $currentSha256
    git_head = (& git -C $RepoRoot rev-parse HEAD).Trim()
    working_tree_diff_id = Get-TextSha256 $workingTreeState
    working_directory = $RepoRoot
    command_arguments = $commandArguments
    warmups = $Warmups
    rounds = $Rounds
    poll_milliseconds = $PollMilliseconds
    baseline_peak_working_set_bytes = $baselineValues.ToArray()
    current_peak_working_set_bytes = $currentValues.ToArray()
    baseline_median_bytes = $baselineMedian
    current_median_bytes = $currentMedian
    current_to_baseline_ratio = $ratio
    verdict = $verdict
}

$resultParent = Split-Path -Parent $ResultPath
if ($resultParent) {
    [void] (New-Item -ItemType Directory -Force -Path $resultParent)
}
$resultJson = $result | ConvertTo-Json -Depth 6
$utf8NoBom = New-Object -TypeName System.Text.UTF8Encoding -ArgumentList $false
[System.IO.File]::WriteAllText($ResultPath, $resultJson, $utf8NoBom)
```

- [ ] **Step 2: Verify fail-fast behavior**

Run with a missing input:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/measure_peak_working_set.ps1 -Pipeline gb -InputPath rust/target/perf/missing.xlsx -BaselineExecutable rust/target/perf/missing.exe -ResultPath rust/target/perf/results/invalid.json
```

Expected: non-zero exit，错误指出 input workbook 缺失；不得生成 `invalid.json`。

- [ ] **Step 3: Run a same-binary smoke after building release**

```powershell
cargo build --release --manifest-path rust/Cargo.toml -p costing-calculate
$gbSample = (uv run python -c "from tests.rust_oracle.repo_paths import require_benchmark_sample; print(require_benchmark_sample('gb'))").Trim()
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/measure_peak_working_set.ps1 -Pipeline gb -InputPath $gbSample -BaselineExecutable rust/target/release/costing-calculate.exe -CurrentExecutable rust/target/release/costing-calculate.exe -ResultPath rust/target/perf/results/peak-working-set-smoke.json
```

必须使用 `require_benchmark_sample('gb')` 返回的绝对路径，不得硬编码样本名或用模糊匹配。Expected JSON:

```text
baseline_peak_working_set_bytes length = 5
current_peak_working_set_bytes length = 5
verdict = VALIDATED
```

- [ ] **Step 4: Add the reusable WPR CPU profile script**

```powershell
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet('gb', 'sk')]
    [string] $Pipeline,

    [Parameter(Mandatory)]
    [string] $InputPath,

    [Parameter(Mandatory)]
    [ValidatePattern('^[a-z0-9-]+$')]
    [string] $Label,

    [Parameter(Mandatory)]
    [string] $ResultPath
)

$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
if (-not (Test-Path -LiteralPath $InputPath -PathType Leaf)) {
    throw "input workbook does not exist: $InputPath"
}
$input = (Resolve-Path -LiteralPath $InputPath).Path
$result = if ([System.IO.Path]::IsPathRooted($ResultPath)) {
    [System.IO.Path]::GetFullPath($ResultPath)
}
else {
    [System.IO.Path]::GetFullPath((Join-Path $RepoRoot $ResultPath))
}
$target = Join-Path $RepoRoot "rust\target\perf\profile-$Label"
$oldTarget = $env:CARGO_TARGET_DIR
$oldDebug = $env:CARGO_PROFILE_RELEASE_DEBUG
$traceStarted = $false
try {
    if (-not (Get-Command wpr.exe -ErrorAction SilentlyContinue)) {
        throw 'wpr.exe is not available; install Windows Performance Toolkit or use the documented profiler fallback'
    }
    $env:CARGO_TARGET_DIR = $target
    $env:CARGO_PROFILE_RELEASE_DEBUG = 'true'
    & cargo build --release --manifest-path (Join-Path $RepoRoot 'rust\Cargo.toml') -p costing-calculate
    if ($LASTEXITCODE -ne 0) { throw "profiling build failed with exit code $LASTEXITCODE" }
    $executable = Join-Path $target 'release\costing-calculate.exe'
    if (-not (Test-Path -LiteralPath $executable -PathType Leaf)) {
        throw "profiling executable is missing: $executable"
    }
    [void] (New-Item -ItemType Directory -Force -Path (Split-Path -Parent $result))
    & wpr.exe -start CPU -filemode
    if ($LASTEXITCODE -ne 0) { throw "WPR start failed with exit code $LASTEXITCODE" }
    $traceStarted = $true
    & $executable $Pipeline --input $input --check-only --benchmark | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "profiling run failed with exit code $LASTEXITCODE" }
    & wpr.exe -stop $result
    if ($LASTEXITCODE -ne 0) { throw "WPR stop failed with exit code $LASTEXITCODE" }
    $traceStarted = $false
}
finally {
    if ($traceStarted) { & wpr.exe -cancel | Out-Null }
    if ($null -eq $oldTarget) { Remove-Item Env:CARGO_TARGET_DIR -ErrorAction SilentlyContinue }
    else { $env:CARGO_TARGET_DIR = $oldTarget }
    if ($null -eq $oldDebug) { Remove-Item Env:CARGO_PROFILE_RELEASE_DEBUG -ErrorAction SilentlyContinue }
    else { $env:CARGO_PROFILE_RELEASE_DEBUG = $oldDebug }
}
```

Fail-fast smoke：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/capture_cpu_profile.ps1 -Pipeline sk -InputPath rust/target/perf/missing.xlsx -Label invalid -ResultPath rust/target/perf/results/invalid.etl
```

Expected: 在检查 WPR/构建前因 input 缺失非零退出，不生成 `invalid.etl`。WPR 不可用时，执行者用 Visual Studio CPU Usage 对同一普通 release source revision 采样并在 evidence note 记录工具/version；不得跳过 profiler 证据。

- [ ] **Step 5: Commit only the harnesses**

```powershell
git diff --check -- tests/rust_oracle/measure_peak_working_set.ps1 tests/rust_oracle/capture_cpu_profile.ps1
git add -- tests/rust_oracle/measure_peak_working_set.ps1 tests/rust_oracle/capture_cpu_profile.ps1
git commit -m "test(perf): add windows performance harnesses"
```

`rust/target/perf/**` 全部是本地证据，不提交。

---

### Task 7: Freeze the Phase 0 Release Baseline and Profile It

**Files:**
- Generated only: `rust/target/perf/baseline/costing-calculate.exe`
- Generated only: `rust/target/perf/profile-phase0/`
- Generated only: `rust/target/perf/results/*.json`
- Generated only: `rust/target/perf/results/*.etl`

**Interfaces:**
- Consumes: Task 2–6 的精确计时和内存 harness、固定 GB/SK 样本。
- Produces: 不提交的 Phase 0 binary/hash/timing/memory/profile 证据；预计此时 check-only verdict 仍可能是 `PERFORMANCE_REGRESSION`，它是待优化基线，不得误报为通过。

- [ ] **Step 1: Build and freeze the ordinary release executable**

```powershell
cargo build --release --manifest-path rust/Cargo.toml -p costing-calculate
$metadata = cargo metadata --format-version 1 --no-deps --manifest-path rust/Cargo.toml | ConvertFrom-Json
$releaseExe = Join-Path $metadata.target_directory 'release/costing-calculate.exe'
New-Item -ItemType Directory -Force rust/target/perf/baseline | Out-Null
$baselineExe = (Join-Path (Resolve-Path 'rust/target/perf/baseline').Path 'costing-calculate.exe')
if (Test-Path -LiteralPath $baselineExe -PathType Leaf) {
    throw "frozen baseline already exists; verify and reuse it instead of overwriting: $baselineExe"
}
Copy-Item -LiteralPath $releaseExe -Destination $baselineExe
Get-FileHash -LiteralPath $baselineExe -Algorithm SHA256
```

Expected: baseline executable 存在且 SHA-256 已记录。若文件已存在，本步骤必须失败；先核对它是否来自同一 Phase 0 commit，绝不能用优化后的 binary 覆盖基线。

- [ ] **Step 2: Resolve and hash exactly one GB and one SK sample**

```powershell
uv run python -c "from tests.rust_oracle.repo_paths import require_benchmark_sample; print(require_benchmark_sample('gb')); print(require_benchmark_sample('sk'))"
```

对输出的两个绝对路径分别运行 `Get-FileHash -Algorithm SHA256`。0 个或多样本会失败，不能继续。

- [ ] **Step 3: Record baseline check-only results without demanding final success**

对 GB 和 SK 分别执行一行 Python 调用；该调用写 JSON，即使 verdict 为 `PERFORMANCE_REGRESSION` 也保留证据：

```powershell
uv run python -c "from pathlib import Path; from tests.rust_oracle.benchmark import run_check_only_payload_benchmark, write_check_only_benchmark_result; from tests.rust_oracle.repo_paths import require_benchmark_sample; exe=Path('rust/target/perf/baseline/costing-calculate.exe').resolve(); result=run_check_only_payload_benchmark('gb', require_benchmark_sample('gb'), exe); assert result.validation_passed and result.valid_pair_count == 5, result; write_check_only_benchmark_result(result, Path('rust/target/perf/results/check-only-baseline-gb.json')); print(result)"
uv run python -c "from pathlib import Path; from tests.rust_oracle.benchmark import run_check_only_payload_benchmark, write_check_only_benchmark_result; from tests.rust_oracle.repo_paths import require_benchmark_sample; exe=Path('rust/target/perf/baseline/costing-calculate.exe').resolve(); result=run_check_only_payload_benchmark('sk', require_benchmark_sample('sk'), exe); assert result.validation_passed and result.valid_pair_count == 5, result; write_check_only_benchmark_result(result, Path('rust/target/perf/results/check-only-baseline-sk.json')); print(result)"
```

Expected: 两份结果各有 5 个 Rust total、5 个 Python total、六个 required Rust stage 各 5 值、`valid_pair_count=5`，并在 `rust_runtime_evidence` 持久化相同的 run counts、error/issue counts、quality metrics，同时记录 Git/diff、工作目录和命令参数；`error_log_count == sum(issue_type_counts.values())`。此阶段不要求性能 verdict 为 `VALIDATED`。

- [ ] **Step 4: Record baseline Peak Working Set**

```powershell
$gbSample = (uv run python -c "from tests.rust_oracle.repo_paths import require_benchmark_sample; print(require_benchmark_sample('gb'))").Trim()
$skSample = (uv run python -c "from tests.rust_oracle.repo_paths import require_benchmark_sample; print(require_benchmark_sample('sk'))").Trim()
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/measure_peak_working_set.ps1 -Pipeline gb -InputPath $gbSample -BaselineExecutable rust/target/perf/baseline/costing-calculate.exe -ResultPath rust/target/perf/results/peak-working-set-baseline-gb.json
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/measure_peak_working_set.ps1 -Pipeline sk -InputPath $skSample -BaselineExecutable rust/target/perf/baseline/costing-calculate.exe -ResultPath rust/target/perf/results/peak-working-set-baseline-sk.json
$gbPws = Get-Content -LiteralPath rust/target/perf/results/peak-working-set-baseline-gb.json -Raw | ConvertFrom-Json
$skPws = Get-Content -LiteralPath rust/target/perf/results/peak-working-set-baseline-sk.json -Raw | ConvertFrom-Json
if ($gbPws.verdict -ne 'BASELINE_RECORDED' -or $gbPws.baseline_peak_working_set_bytes.Count -ne 5) { throw 'invalid GB PWS baseline evidence' }
if ($skPws.verdict -ne 'BASELINE_RECORDED' -or $skPws.baseline_peak_working_set_bytes.Count -ne 5) { throw 'invalid SK PWS baseline evidence' }
```

Expected: 两份结果均含 `working_directory`、五个 `command_arguments`、input/binary SHA、5 个原始字节值和 `BASELINE_RECORDED`。

- [ ] **Step 5: Capture real GB/SK normal-mode export stage evidence**

```powershell
uv run python -c "from pathlib import Path; from tests.rust_oracle.oracle_runner import capture_rust_normal_benchmark_evidence; from tests.rust_oracle.repo_paths import require_benchmark_sample; exe=Path('rust/target/perf/baseline/costing-calculate.exe'); capture_rust_normal_benchmark_evidence(exe, 'gb', require_benchmark_sample('gb'), Path('rust/target/perf/results/normal-baseline-gb.xlsx'), Path('rust/target/perf/results/normal-baseline-gb.json'))"
uv run python -c "from pathlib import Path; from tests.rust_oracle.oracle_runner import capture_rust_normal_benchmark_evidence; from tests.rust_oracle.repo_paths import require_benchmark_sample; exe=Path('rust/target/perf/baseline/costing-calculate.exe'); capture_rust_normal_benchmark_evidence(exe, 'sk', require_benchmark_sample('sk'), Path('rust/target/perf/results/normal-baseline-sk.xlsx'), Path('rust/target/perf/results/normal-baseline-sk.json'))"
```

Expected: 两份 JSON 均来自固定真实样本，含六个 required payload stage 和独立 `export`；`total` 不含 export。临时 `.xlsx` 已由 helper 验证后删除。两份 summary 还必须满足 `error_log_count == sum(issue_type_counts.values())`；helper 不满足即非零退出。

- [ ] **Step 6: Capture a symbolized SK CPU profile without changing Cargo.toml**

```powershell
$skSample = (uv run python -c "from tests.rust_oracle.repo_paths import require_benchmark_sample; print(require_benchmark_sample('sk'))").Trim()
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/capture_cpu_profile.ps1 -Pipeline sk -InputPath $skSample -Label phase0 -ResultPath rust/target/perf/results/phase0-sk-cpu.etl
```

Expected: `.etl` 可在 WPA 打开并能定位 `rows_to_maps`、`forward_fill_with_rules`、`build_fact_bundle`、`build_qty_sheet_rows`、`build_work_order_anomaly_sheet`、`build_quality_metrics`、`build_flat_sheet` 或其分配调用栈。若 WPR 无符号，使用 Visual Studio CPU Usage 对同一 profile executable 重采；仍不得修改 Cargo release profile 或新增依赖。

- [ ] **Step 7: Rebuild ordinary release after profiler and verify the frozen baseline is unchanged**

```powershell
cargo build --release --manifest-path rust/Cargo.toml -p costing-calculate
Get-FileHash -LiteralPath rust/target/perf/baseline/costing-calculate.exe -Algorithm SHA256
git status --short
```

Expected: baseline copy hash与 Step 1 相同；profile/baseline/results 都在 ignored target 下；无新源文件差异。本任务不提交 commit。

---

### Task 8: Borrow Anomaly Source Rows Instead of Cloning Them

**Files:**
- Modify: `rust/crates/costing-core/src/anomaly.rs`

**Interfaces:**
- Consumes: 当前 Map-backed `FactBundle.work_order_fact` 和所有既有 anomaly tests。
- Produces: `AnomalyRow<'a>` 借用 `&'a TableRow`；Sheet 值、排序、评分和解释文本完全不变。

本任务的优化由 Rust 所有权签名和代码审查证明，不增加不稳定的 clone 计数或时间断言。

- [ ] **Step 1: Run the current anomaly characterization suite**

```powershell
cargo test --manifest-path rust/Cargo.toml -p costing-core anomaly::tests
```

Expected before edit: PASS，固定当前 Sheet、评分、排序和白名单行为。

- [ ] **Step 2: Change only the ownership edges in AnomalyRow**

对现有实现做以下精确类型/表达式替换；除这些所有权边之外，不重写 `build_anomaly_row`，从而完整保留当前数字字段赋值：

```diff
-struct AnomalyRow {
+struct AnomalyRow<'a> {

-    source: TableRow,
+    source: &'a TableRow,

-fn build_anomaly_row(
-    row: &TableRow,
+fn build_anomaly_row<'a>(
+    row: &'a TableRow,
     config: &PipelineConfig,
-) -> AnomalyRow {
+) -> AnomalyRow<'a> {

-        source: row.clone(),
+        source: row,
```

这些是完整的允许替换集合：`numbers`、`production_scope`、`can_analyze`、`reasons`、`audits`、`anomaly_level`、`anomaly_source`、`detail_explanation` 字段及其赋值一行不改；若 diff 出现这些字段，停止并缩小 patch。

同时为以下签名补 `<'_>`/`<'a>`：

```rust
fn score_rows(rows: &mut [AnomalyRow<'_>]);
fn push_score_reason(row: &mut AnomalyRow<'_>, metric: Metric, reason: &str);
fn append_non_positive_reasons(rows: &mut [AnomalyRow<'_>], metric: Metric);
fn finalize_row_anomaly(row: &mut AnomalyRow<'_>);
fn build_detail_explanation(row: &AnomalyRow<'_>) -> String;
fn map_work_order_value(
    row: &AnomalyRow<'_>,
    column: &str,
    config: &PipelineConfig,
) -> CellValue;
fn group_key(row: &AnomalyRow<'_>) -> String;
fn positive_number(row: &AnomalyRow<'_>, key: &str) -> bool;
fn decimal_value(row: &AnomalyRow<'_>, key: &str) -> CellValue;
fn standalone_display_value(
    row: &AnomalyRow<'_>,
    column: &str,
    config: &PipelineConfig,
) -> CellValue;
```

- [ ] **Step 3: Prove the clone site is gone and behavior is green**

```powershell
Select-String -Path rust/crates/costing-core/src/anomaly.rs -Pattern 'source:\s*row\.clone\(\)'
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --manifest-path rust/Cargo.toml -p costing-core anomaly::tests
```

Expected: `Select-String` 无匹配；format/tests exit 0。

- [ ] **Step 4: Commit**

```powershell
git add -- rust/crates/costing-core/src/anomaly.rs
git diff --cached --check
git commit -m "perf(core): borrow anomaly source rows"
```

---

### Task 9: Consume Map-Backed Fact Rows in Presentation

**Files:**
- Modify: `rust/crates/costing-core/src/fact.rs`
- Modify: `rust/crates/costing-core/src/presentation.rs`

**Interfaces:**
- Consumes: Task 8 借用式 anomaly、当前 `FactBundle` 和固定唯一业务列清单。
- Produces: `build_qty_sheet_rows(rows: Vec<TableRow>, config: &PipelineConfig) -> Vec<TableRow>`、消费式 `build_flat_sheet`、直接移动 `detail_fact/error_issues` 的 payload；外部 `WorkbookPayload` 不变。

- [ ] **Step 1: Add presentation ownership characterization tests**

在 `presentation.rs` tests 新增：

```rust
#[test]
fn payload_preserves_error_log_and_sheet_cells_when_consuming_bundle() {
    let source = bundle();
    let expected_error_log = source.error_issues.clone();
    let expected_detail_rows = source.detail_fact.len();

    let payload = build_workbook_payload(
        source,
        &PipelineConfig::for_name(PipelineName::Gb),
        StageTimings::default(),
        false,
    )
    .unwrap();

    assert_eq!(payload.error_log, expected_error_log);
    assert_eq!(payload.error_log_count, expected_error_log.len());
    assert_eq!(payload.sheet_models[0].rows.len(), expected_detail_rows);
}
```

保留现有 `payload_has_exactly_three_default_sheets_without_product_dimension`、`payload_carries_quality_errors_and_timings`、`flat_sheets_do_not_expose_internal_or_cross_sheet_columns`。

- [ ] **Step 2: Run characterization tests**

```powershell
cargo test --manifest-path rust/Cargo.toml -p costing-core presentation::tests
```

Expected before ownership edit: PASS。它们固定行为；clone 消除由下一步签名和源码检查证明。

- [ ] **Step 3: Make the quantity builder consume rows**

只应用以下所有权 diff；两处之间现有的全部金额、单位成本和勾稽语句保持原位，不新增 helper、不改变顺序：

```diff
-pub fn build_qty_sheet_rows(bundle: &FactBundle, config: &PipelineConfig) -> Vec<TableRow> {
-    bundle
-        .qty_fact
-        .iter()
+pub fn build_qty_sheet_rows(rows: Vec<TableRow>, config: &PipelineConfig) -> Vec<TableRow> {
+    rows
+        .into_iter()
         .map(|row| {
-            let mut values = row.values.clone();
+            let mut values = row.values;
             let completed_qty = decimal_from_values(&values, COMPLETED_QTY_KEY);
```

函数尾部现有 `TableRow { values }` 和 `.collect()` 保持不变；不得修改计算口径或插入顺序。

- [ ] **Step 4: Make flat projection consume each Map row**

业务 contract 列是固定且唯一的，因此当前 Map phase 可直接移动：

```rust
let sheet_rows = rows
    .into_iter()
    .map(|mut row| {
        columns
            .iter()
            .map(|column| row.values.remove(column).unwrap_or(CellValue::Blank))
            .collect::<Vec<_>>()
    })
    .collect::<Vec<_>>();
```

只替换 `build_flat_sheet` 的行投影；列类型、number format、freeze panes、auto filter、fixed width 继续调用现有 helper，不重写格式规则。

- [ ] **Step 5: Reorder build_workbook_payload to borrow first, then move**

```rust
let quality_metrics = build_quality_metrics(&bundle, month_filter_empty_result);
let work_order_sheet = build_work_order_anomaly_sheet(&bundle, config);
let detail_columns = detail_sheet_columns(&bundle.detail_columns);
let qty_columns = qty_sheet_columns(&bundle.qty_columns, config);

let FactBundle {
    detail_fact,
    qty_fact,
    error_issues,
    ..
} = bundle;
let detail_sheet = build_flat_sheet(
    "成本计算单总表",
    detail_columns,
    detail_fact,
    detail_number_format_columns,
);
let qty_rows = build_qty_sheet_rows(qty_fact, config);
let qty_sheet = build_flat_sheet(
    "成本计算单数量聚合维度",
    qty_columns,
    qty_rows,
    qty_number_format_columns,
);
let sheets = vec![detail_sheet, qty_sheet, work_order_sheet];
ensure_no_product_dimension(&sheets)?;
let error_log_count = error_issues.len();

Ok(WorkbookPayload {
    sheet_models: sheets,
    quality_metrics,
    error_log_count,
    error_log: error_issues,
    stage_timings: timings,
})
```

- [ ] **Step 6: Run the Phase 1 correctness gate**

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --manifest-path rust/Cargo.toml
uv run python -m pytest tests/test_full_rust_cli_oracle.py -q --basetemp .pytest-tmp/phase1-oracle
git diff --check -- rust/crates/costing-core/src/fact.rs rust/crates/costing-core/src/presentation.rs
```

Expected: 全部通过；oracle summary 必须显示 GB、SK 均 passed，不能把 skipped 当通过。

- [ ] **Step 7: Measure and record the Phase 1 diagnostic gate**

```powershell
cargo build --release --manifest-path rust/Cargo.toml -p costing-calculate
New-Item -ItemType Directory -Force rust/target/perf/phase1 | Out-Null
Copy-Item -LiteralPath rust/target/release/costing-calculate.exe -Destination rust/target/perf/phase1/costing-calculate.exe -Force
$gbSample = (uv run python -c "from tests.rust_oracle.repo_paths import require_benchmark_sample; print(require_benchmark_sample('gb'))").Trim()
$skSample = (uv run python -c "from tests.rust_oracle.repo_paths import require_benchmark_sample; print(require_benchmark_sample('sk'))").Trim()
uv run python -c "from pathlib import Path; from tests.rust_oracle.benchmark import run_check_only_payload_benchmark, write_check_only_benchmark_result; from tests.rust_oracle.repo_paths import require_benchmark_sample; exe=Path('rust/target/perf/phase1/costing-calculate.exe'); r=run_check_only_payload_benchmark('gb', require_benchmark_sample('gb'), exe); assert r.validation_passed and r.valid_pair_count == 5, r; write_check_only_benchmark_result(r, Path('rust/target/perf/results/check-only-phase1-gb.json'))"
uv run python -c "from pathlib import Path; from tests.rust_oracle.benchmark import run_check_only_payload_benchmark, write_check_only_benchmark_result; from tests.rust_oracle.repo_paths import require_benchmark_sample; exe=Path('rust/target/perf/phase1/costing-calculate.exe'); r=run_check_only_payload_benchmark('sk', require_benchmark_sample('sk'), exe); assert r.validation_passed and r.valid_pair_count == 5, r; write_check_only_benchmark_result(r, Path('rust/target/perf/results/check-only-phase1-sk.json'))"
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/measure_peak_working_set.ps1 -Pipeline gb -InputPath $gbSample -BaselineExecutable rust/target/perf/baseline/costing-calculate.exe -CurrentExecutable rust/target/perf/phase1/costing-calculate.exe -ResultPath rust/target/perf/results/peak-working-set-phase1-gb.json
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/measure_peak_working_set.ps1 -Pipeline sk -InputPath $skSample -BaselineExecutable rust/target/perf/baseline/costing-calculate.exe -CurrentExecutable rust/target/perf/phase1/costing-calculate.exe -ResultPath rust/target/perf/results/peak-working-set-phase1-sk.json
uv run python -c "from pathlib import Path; from tests.rust_oracle.benchmark import compare_non_target_stage_medians; root=Path('rust/target/perf/results'); gb=compare_non_target_stage_medians(root/'check-only-baseline-gb.json',root/'check-only-phase1-gb.json',target_stages=frozenset({'presentation'})); sk=compare_non_target_stage_medians(root/'check-only-baseline-sk.json',root/'check-only-phase1-sk.json',target_stages=frozenset({'presentation'})); print({'gb':gb,'sk':sk}); assert not gb and not sk, (gb,sk)"
uv run python -c "import json; from pathlib import Path; root=Path('rust/target/perf/results'); load=lambda name: json.loads((root/name).read_text(encoding='utf-8')); pairs=[(p,load(f'check-only-baseline-{p}.json'),load(f'check-only-phase1-{p}.json')) for p in ('gb','sk')]; reports=[{'pipeline':p,'presentation_drop':1-c['rust_stage_median_seconds']['presentation']/b['rust_stage_median_seconds']['presentation'],'fact_ratio':c['rust_stage_median_seconds']['fact']/b['rust_stage_median_seconds']['fact']} for p,b,c in pairs]; print(reports); assert all(r['fact_ratio'] <= 1.02 for r in reports), reports"
```

Python 与 PWS 均通过同一 strict resolver/绝对路径执行，结果内 SHA 会再次证明。若最后一条因非目标 stage >5% 失败，重新执行两条 benchmark，输出改为 `check-only-phase1-rerun-{pipeline}.json`，再对 rerun 调同一 compare helper；保存两次结果、profile 和解释后交 data auditor 审查，未批准不得进入 Phase 2。

Expected diagnostics:

```text
presentation median: 目标下降 >= 20%
presentation median: 若下降 < 10%，保留正确所有权改造，但先重跑 profiler
fact median: 不得无解释回退 > 2%
Peak Working Set: 记录 5 对原始值；最终硬门禁为 ratio <= 1.05
```

Phase 1 未达到诊断目标不是回滚理由，但必须留下新 profile 后才能进入 Phase 2。

若 presentation 下降不足 10%，执行精确 profile 命令：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/capture_cpu_profile.ps1 -Pipeline sk -InputPath $skSample -Label phase1 -ResultPath rust/target/perf/results/phase1-sk-cpu.etl
```

- [ ] **Step 8: Prove targeted clone sites are absent and commit**

```powershell
Select-String -Path rust/crates/costing-core/src/presentation.rs -Pattern 'detail_fact\.clone|error_issues\.clone'
Select-String -Path rust/crates/costing-core/src/fact.rs -Pattern 'row\.values\.clone'
git add -- rust/crates/costing-core/src/fact.rs rust/crates/costing-core/src/presentation.rs
git diff --cached --check
git commit -m "perf(core): consume presentation fact rows"
```

Expected: 三个目标 clone pattern 均无匹配；commit 成功。

---

### Task 10: Add Schema-Aware Indexed Table Primitives

**Files:**
- Create: `rust/crates/costing-core/src/table.rs`
- Modify: `rust/crates/costing-core/src/lib.rs`
- Modify: `rust/crates/costing-core/src/error.rs`

**Interfaces:**
- Consumes: `CellValue`、`CostingError`、现有 serde 依赖。
- Produces: crate-private `SchemaId`、`ColumnId`、`ColumnSchema`、`IndexedRow`、基础 `IndexedTable::from_raw`；此任务暂不接入业务链。

- [ ] **Step 1: Write failing table identity and row-shape tests**

在新 `table.rs` tests 中先写：

- `from_raw_pads_short_rows_with_blank`
- `from_raw_truncates_long_rows`
- `duplicate_column_names_resolve_to_last_physical_slot`
- `foreign_schema_column_id_returns_internal_error_even_when_slot_exists`
- `invalid_column_id_returns_internal_error_without_panicking`
- `logically_equal_tables_ignore_schema_id_in_equality_and_serialization`
- `optional_missing_column_returns_none`
- `display_order_reports_all_missing_columns_once`
- `take_moves_value_and_leaves_blank`

核心跨 Schema 测试：

```rust
#[test]
fn foreign_schema_column_id_returns_internal_error_even_when_slot_exists() {
    let left = IndexedTable::from_raw(
        vec!["产品编码".to_string()],
        vec![vec![CellValue::Text("A".to_string())]],
    )
    .unwrap();
    let right = IndexedTable::from_raw(
        vec!["产品编码".to_string()],
        vec![vec![CellValue::Text("B".to_string())]],
    )
    .unwrap();
    let foreign = left.schema().require("产品编码").unwrap();

    let error = right.rows()[0].get(foreign).unwrap_err();

    assert_eq!(error.code(), ErrorCode::InternalError);
}
```

- [ ] **Step 2: Run tests and verify compile red**

```powershell
cargo test --manifest-path rust/Cargo.toml -p costing-core table::tests
```

Expected: FAIL，因为 `crate::table` 和类型尚不存在。

- [ ] **Step 3: Add the private module and internal error helper**

`lib.rs` 使用私有模块：

```rust
mod table;
```

`error.rs` 增加：

```rust
pub fn internal(message: impl Into<String>) -> Self {
    Self::Internal {
        code: ErrorCode::InternalError,
        message: message.into(),
    }
}
```

- [ ] **Step 4: Implement SchemaId, ColumnId, and ColumnSchema**

```rust
static NEXT_SCHEMA_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SchemaId(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ColumnId {
    schema_id: SchemaId,
    slot: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct ColumnSchema {
    schema_id: SchemaId,
    names_by_id: Vec<String>,
    id_by_name: HashMap<String, ColumnId>,
}

impl ColumnSchema {
    fn new(names: Vec<String>) -> Result<Self, CostingError> {
        let raw_id = NEXT_SCHEMA_ID
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| value.checked_add(1))
            .map_err(|_| CostingError::internal("SchemaId counter exhausted"))?;
        let schema_id = SchemaId(raw_id);
        let mut id_by_name = HashMap::with_capacity(names.len());
        for (slot, name) in names.iter().enumerate() {
            id_by_name.insert(name.clone(), ColumnId { schema_id, slot });
        }
        Ok(Self {
            schema_id,
            names_by_id: names,
            id_by_name,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.names_by_id.len()
    }

    pub(crate) fn require(&self, name: &str) -> Result<ColumnId, CostingError> {
        self.optional(name)
            .ok_or_else(|| CostingError::invalid_input(format!("缺少必要列: {name}")))
    }

    pub(crate) fn optional(&self, name: &str) -> Option<ColumnId> {
        self.id_by_name.get(name).copied()
    }

    pub(crate) fn name(&self, id: ColumnId) -> Result<&str, CostingError> {
        let slot = self.validate_id(id)?;
        Ok(&self.names_by_id[slot])
    }

    fn validate_id(&self, id: ColumnId) -> Result<usize, CostingError> {
        if id.schema_id != self.schema_id || id.slot >= self.names_by_id.len() {
            return Err(CostingError::internal("ColumnId does not belong to this schema"));
        }
        Ok(id.slot)
    }
}
```

`display_order_for` 必须先收集全部缺列再报一个 `INVALID_INPUT`，不能只报告第一个：

```rust
pub(crate) fn display_order_for(
    &self,
    required_names: &[String],
) -> Result<Vec<ColumnId>, CostingError> {
    let missing = required_names
        .iter()
        .filter(|name| !self.id_by_name.contains_key(name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(CostingError::invalid_input(format!(
            "缺少必要列: {}",
            missing.join(", ")
        )));
    }
    Ok(required_names
        .iter()
        .map(|name| self.id_by_name[name])
        .collect())
}
```

- [ ] **Step 5: Implement IndexedRow and from_raw without cloning cells**

```rust
#[derive(Debug, Clone)]
pub(crate) struct IndexedRow {
    schema_id: SchemaId,
    cells: Vec<CellValue>,
}

impl IndexedRow {
    pub(crate) fn get(&self, id: ColumnId) -> Result<&CellValue, CostingError> {
        let slot = self.validate_id(id)?;
        Ok(&self.cells[slot])
    }

    pub(crate) fn get_mut(&mut self, id: ColumnId) -> Result<&mut CellValue, CostingError> {
        let slot = self.validate_id(id)?;
        Ok(&mut self.cells[slot])
    }

    pub(crate) fn replace(
        &mut self,
        id: ColumnId,
        value: CellValue,
    ) -> Result<CellValue, CostingError> {
        Ok(std::mem::replace(self.get_mut(id)?, value))
    }

    pub(crate) fn take(&mut self, id: ColumnId) -> Result<CellValue, CostingError> {
        self.replace(id, CellValue::Blank)
    }

    fn validate_id(&self, id: ColumnId) -> Result<usize, CostingError> {
        if id.schema_id != self.schema_id || id.slot >= self.cells.len() {
            return Err(CostingError::internal("ColumnId does not belong to this row"));
        }
        Ok(id.slot)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct IndexedTable {
    schema: ColumnSchema,
    source_display_order: Vec<ColumnId>,
    rows: Vec<IndexedRow>,
}

impl IndexedTable {
    pub(crate) fn from_raw(
        source_names: Vec<String>,
        rows: Vec<Vec<CellValue>>,
    ) -> Result<Self, CostingError> {
        let schema = ColumnSchema::new(source_names.clone())?;
        // 重复列的物理槽位仍保留；兼容展示顺序按名称查询，均指向最后同名槽位。
        let source_display_order = schema.display_order_for(&source_names)?;
        let width = schema.len();
        let rows = rows
            .into_iter()
            .map(|mut cells| {
                cells.truncate(width);
                cells.resize(width, CellValue::Blank);
                IndexedRow {
                    schema_id: schema.schema_id,
                    cells,
                }
            })
            .collect();
        Ok(Self {
            schema,
            source_display_order,
            rows,
        })
    }

    pub(crate) fn schema(&self) -> &ColumnSchema {
        &self.schema
    }

    pub(crate) fn rows(&self) -> &[IndexedRow] {
        &self.rows
    }

    pub(crate) fn into_parts(self) -> (ColumnSchema, Vec<ColumnId>, Vec<IndexedRow>) {
        (self.schema, self.source_display_order, self.rows)
    }
}
```

- [ ] **Step 6: Implement logical equality/serialization without SchemaId leakage**

```rust
impl PartialEq for IndexedTable {
    fn eq(&self, other: &Self) -> bool {
        self.schema.names_by_id == other.schema.names_by_id
            && display_slots(&self.source_display_order)
                == display_slots(&other.source_display_order)
            && self.rows.iter().map(|row| &row.cells).eq(
                other.rows.iter().map(|row| &row.cells),
            )
    }
}

#[derive(Serialize)]
struct IndexedTableSnapshot<'a> {
    names_by_id: &'a [String],
    source_display_slots: Vec<usize>,
    rows: Vec<&'a [CellValue]>,
}

impl Serialize for IndexedTable {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        IndexedTableSnapshot {
            names_by_id: &self.schema.names_by_id,
            source_display_slots: display_slots(&self.source_display_order),
            rows: self.rows.iter().map(|row| row.cells.as_slice()).collect(),
        }
        .serialize(serializer)
    }
}
```

`display_slots` 只提取经当前 schema 验证过的 `ColumnId.slot`。`SchemaId`/`ColumnId` 不 derive `Serialize`；两个独立构造但逻辑相同的 table 必须相等且序列化字节相同。

- [ ] **Step 7: Run and commit the isolated primitive module**

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --manifest-path rust/Cargo.toml -p costing-core table::tests
git diff --check -- rust/crates/costing-core/src/table.rs rust/crates/costing-core/src/lib.rs rust/crates/costing-core/src/error.rs
git add -- rust/crates/costing-core/src/table.rs rust/crates/costing-core/src/lib.rs rust/crates/costing-core/src/error.rs
git commit -m "refactor(core): add indexed table primitives"
```

此时业务链尚未引用 table 类型，短暂 `dead_code` warning 可以接受；不要为消警把模块公开。

---

### Task 11: Add Atomic Table Updates and a Duplicate-Safe Projection Plan

**Files:**
- Modify: `rust/crates/costing-core/src/table.rs`

**Interfaces:**
- Consumes: Task 10 schema/row/table primitives。
- Produces: `DerivedColumnPosition`、fallible row update/retain、原子派生列复用/追加、`ProjectionPlan`。

- [ ] **Step 1: Write failing atomicity and projection tests**

新增：

- `adding_derived_column_preserves_existing_column_ids`
- `ensure_derived_column_updates_schema_rows_and_display_order_atomically`
- `ensure_derived_column_rejects_wrong_value_count_without_mutation`
- `ensure_derived_column_reuses_last_duplicate_without_moving_display_order`
- `ensure_derived_column_rejects_malformed_row_shape_without_mutation`
- `try_update_rows_changes_cells_without_changing_row_shape`
- `try_retain_rows_propagates_access_error_without_filtering`
- `projection_plan_clones_duplicate_ids_until_last_occurrence`

关键 projection 测试：

```rust
#[test]
fn projection_plan_clones_duplicate_ids_until_last_occurrence() {
    let table = IndexedTable::from_raw(
        vec!["产品编码".to_string()],
        vec![vec![CellValue::Text("P1".to_string())]],
    )
    .unwrap();
    let id = table.schema().require("产品编码").unwrap();
    let plan = ProjectionPlan::new(table.schema(), &[id, id]).unwrap();
    let (_, _, mut rows) = table.into_parts();

    let projected = plan.project_row(rows.pop().unwrap()).unwrap();

    assert_eq!(
        projected,
        vec![
            CellValue::Text("P1".to_string()),
            CellValue::Text("P1".to_string()),
        ]
    );
}
```

- [ ] **Step 2: Run tests and verify red**

```powershell
cargo test --manifest-path rust/Cargo.toml -p costing-core table::tests
```

Expected: 新方法和类型未定义导致 FAIL。

- [ ] **Step 3: Implement fallible update/retain and derived-column positions**

```rust
#[derive(Debug, Clone, Copy)]
pub(crate) enum DerivedColumnPosition<'a> {
    End,
    AfterFirstSourceName(&'a str),
}

pub(crate) fn try_update_rows<F>(&mut self, mut update: F) -> Result<(), CostingError>
where
    F: FnMut(&mut IndexedRow) -> Result<(), CostingError>,
{
    for row in &mut self.rows {
        update(row)?;
    }
    Ok(())
}

pub(crate) fn try_retain_rows<F>(&mut self, mut predicate: F) -> Result<(), CostingError>
where
    F: FnMut(&IndexedRow) -> Result<bool, CostingError>,
{
    let keep = self
        .rows
        .iter()
        .map(&mut predicate)
        .collect::<Result<Vec<_>, _>>()?;
    let mut index = 0usize;
    self.rows.retain(|_| {
        let retain = keep[index];
        index += 1;
        retain
    });
    Ok(())
}
```

先计算完整 keep mask，predicate 失败时原 rows 不变。

`ensure_or_reuse_derived_column` 必须使用以下原子顺序；`ColumnSchema::append`、`IndexedRow::push_validated_cell` 和 display order 修改保持为 table 模块私有实现：

```rust
impl ColumnSchema {
    fn append(&mut self, name: String) -> ColumnId {
        let id = ColumnId {
            schema_id: self.schema_id,
            slot: self.names_by_id.len(),
        };
        self.names_by_id.push(name.clone());
        self.id_by_name.insert(name, id);
        id
    }
}

impl IndexedRow {
    fn validate_shape(
        &self,
        expected_schema_id: SchemaId,
        expected_width: usize,
    ) -> Result<(), CostingError> {
        if self.schema_id != expected_schema_id || self.cells.len() != expected_width {
            return Err(CostingError::internal(
                "IndexedRow shape does not match its table schema",
            ));
        }
        Ok(())
    }

    fn push_validated_cell(&mut self, value: CellValue) {
        // 只能由已完成全表 shape 校验的 IndexedTable 调用。
        self.cells.push(value);
    }
}

pub(crate) fn ensure_or_reuse_derived_column(
    &mut self,
    name: &str,
    display_position: DerivedColumnPosition<'_>,
    values: Vec<CellValue>,
) -> Result<ColumnId, CostingError> {
    if values.len() != self.rows.len() {
        return Err(CostingError::invalid_input(format!(
            "派生列 {name} 的值数量 {} 与行数 {} 不一致",
            values.len(),
            self.rows.len(),
        )));
    }
    for row in &self.rows {
        row.validate_shape(self.schema.schema_id, self.schema.len())?;
    }

    if let Some(id) = self.schema.optional(name) {
        for (row, value) in self.rows.iter_mut().zip(values) {
            row.replace(id, value)?;
        }
        return Ok(id);
    }

    let id = self.schema.append(name.to_string());
    for (row, value) in self.rows.iter_mut().zip(values) {
        row.push_validated_cell(value);
    }
    match display_position {
        DerivedColumnPosition::End => self.source_display_order.push(id),
        DerivedColumnPosition::AfterFirstSourceName(source_name) => {
            let insert_at = self
                .source_display_order
                .iter()
                .position(|source_id| {
                    matches!(
                        self.schema.name(*source_id),
                        Ok(name) if name == source_name
                    )
                })
                .map_or(self.source_display_order.len(), |index| index + 1);
            self.source_display_order.insert(insert_at, id);
        }
    }
    Ok(id)
}
```

`ensure_derived_column_rejects_malformed_row_shape_without_mutation` 在 table 内部测试中先 `pop` 一行的最后 cell，记录 schema names、display slots 和各行长度，再调用扩列；断言返回 `ErrorCode::InternalError`，且这些记录全部未变化。该测试只利用同模块私有字段制造不变量破坏，生产 API 不能制造这种状态。

所有会失败的检查都发生在 mutation 前。新列始终物理 append；`AfterFirstSourceName` 只调整 display ID 顺序。已有同名列覆盖“最后一列生效”的槽位且不移动 display order。

- [ ] **Step 4: Implement ProjectionPlan with prevalidation**

```rust
#[derive(Debug, Clone, Copy)]
enum ProjectionMode {
    Clone,
    Take,
}

#[derive(Debug, Clone, Copy)]
struct ProjectionStep {
    id: ColumnId,
    mode: ProjectionMode,
}

#[derive(Debug, Clone)]
pub(crate) struct ProjectionPlan {
    steps: Vec<ProjectionStep>,
}

impl ProjectionPlan {
    pub(crate) fn new(
        schema: &ColumnSchema,
        display_columns: &[ColumnId],
    ) -> Result<Self, CostingError> {
        let mut last_positions = HashMap::new();
        for (index, id) in display_columns.iter().copied().enumerate() {
            schema.validate_id(id)?;
            last_positions.insert(id, index);
        }
        let steps = display_columns
            .iter()
            .copied()
            .enumerate()
            .map(|(index, id)| ProjectionStep {
                id,
                mode: if last_positions[&id] == index {
                    ProjectionMode::Take
                } else {
                    ProjectionMode::Clone
                },
            })
            .collect();
        Ok(Self { steps })
    }

    pub(crate) fn project_row(
        &self,
        mut row: IndexedRow,
    ) -> Result<Vec<CellValue>, CostingError> {
        for step in &self.steps {
            row.validate_id(step.id)?;
        }
        self.steps
            .iter()
            .map(|step| match step.mode {
                ProjectionMode::Clone => Ok(row.get(step.id)?.clone()),
                ProjectionMode::Take => row.take(step.id),
            })
            .collect()
    }
}
```

- [ ] **Step 5: Run and commit**

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --manifest-path rust/Cargo.toml -p costing-core table::tests
git diff --check -- rust/crates/costing-core/src/table.rs
git add -- rust/crates/costing-core/src/table.rs
git commit -m "refactor(core): add atomic indexed table updates"
```

---

### Task 12: Migrate Normalize/Split and All Consumers to Indexed Rows Atomically

**Files:**
- Modify: `rust/crates/costing-core/src/model.rs`
- Modify: `rust/crates/costing-core/src/normalize.rs`
- Modify: `rust/crates/costing-core/src/split.rs`
- Modify: `rust/crates/costing-core/src/fact.rs`
- Modify: `rust/crates/costing-core/src/anomaly.rs`
- Modify: `rust/crates/costing-core/src/quality.rs`
- Modify: `rust/crates/costing-core/src/presentation.rs`
- Modify: `rust/crates/costing-cli/src/run.rs`

**Interfaces:**
- Consumes: Task 10–11 table module、Task 9 ownership flow、现有固定 `detail_sheet_columns/qty_sheet_base_columns` 名称契约。
- Produces: `RawWorkbook.rows → IndexedTable → NormalizedCostFrame → SplitResult` 全链路索引化；临时 `IndexedFactRow` 只保存 qty 派生值小 overlay，使 Phase 2 可独立编译和测量。Task 14 必须删除该 overlay。

这一任务是最小可编译的纵向原子提交。只改 `model + normalize + split` 会同时破坏 fact、anomaly、quality、presentation、CLI 和 fixture；禁止通过保留全量旧 Map 管线来制造“较小提交”。

- [ ] **Step 1: Add physical-column and partition regression tests**

在 normalize 新增：

- `fills_cost_item_from_previous_non_blank_row`
- `alias_collision_uses_last_physical_column`
- `existing_month_column_reuses_last_slot_without_moving_display_position`
- `missing_period_column_does_not_add_or_overwrite_month`
- `duplicate_period_column_inserts_month_after_first_name_and_reads_last_slot`
- `existing_filled_cost_item_reuses_last_slot_without_appending`

在 split 新增：

- `ignores_rows_that_match_neither_detail_nor_qty`
- `missing_order_column_still_allows_qty_row`
- `preserves_input_order_within_each_partition`
- `duplicate_source_columns_do_not_duplicate_contract_display_columns`

在 fact 新增临时 seam 测试：

- `indexed_fact_row_reads_derived_value_before_same_named_source_value`
- `indexed_fact_row_keeps_source_schema_unchanged_when_derived_values_are_inserted`
- `indexed_fact_row_takes_derived_value_without_rebuilding_source_map`

normalize 物理槽位测试统一复用以下真实 `RawWorkbook` helper：

```rust
fn raw_table(columns: &[&str], rows: Vec<Vec<CellValue>>) -> RawWorkbook {
    RawWorkbook {
        sheet_name: "成本计算单".to_string(),
        header_rows: [
            vec![String::new(); columns.len()],
            columns.iter().map(|name| (*name).to_string()).collect(),
        ],
        rows,
    }
}
```

代表性完整断言：

```rust
#[test]
fn duplicate_period_column_inserts_month_after_first_name_and_reads_last_slot() {
    let frame = normalize_workbook(
        raw_table(
            &["年期", "年期", "成本中心名称"],
            vec![vec![
                CellValue::Text("2025年01期".to_string()),
                CellValue::Text("2025年02期".to_string()),
                CellValue::Text("车间".to_string()),
            ]],
        ),
        &PipelineConfig::for_name(PipelineName::Gb),
        None,
    )
    .unwrap();
    let (schema, display, rows) = frame.into_table().into_parts();
    let display_names = display
        .iter()
        .map(|id| schema.name(*id).unwrap())
        .collect::<Vec<_>>();
    let month = schema.require("月份").unwrap();

    assert_eq!(display_names[..3], ["年期", "月份", "年期"]);
    assert_eq!(
        rows[0].get(month).unwrap(),
        &CellValue::Text("2025年02期".to_string()),
    );
    assert_eq!(schema.len(), 5); // 两个年期 + 成本中心 + 月份 + Filled_成本项目
}
```

split 的四个新增测试复用本任务 Step 8 的 `indexed_rows` fixture；分别用 `工单编号` 值断言 detail/qty 向量的精确输入顺序，不比较 HashMap/Set iteration。

- [ ] **Step 2: Run the new tests and verify model red**

```powershell
cargo test --manifest-path rust/Cargo.toml -p costing-core normalize::tests
cargo test --manifest-path rust/Cargo.toml -p costing-core split::tests
cargo test --manifest-path rust/Cargo.toml -p costing-core fact::tests
```

Expected: 新测试不能用当前 `TableRow.values` 表达重复物理槽位和 Schema ID，至少一组编译/断言 FAIL。

- [ ] **Step 3: Replace internal Map models with opaque indexed models**

`RawWorkbook`、`CellValue`、`ErrorIssue`、`WorkbookPayload`、`RunSummary` 保持现有 public serde 结构。替换中间模型：

```rust
#[derive(Debug)]
pub struct NormalizedCostFrame {
    pub(crate) table: IndexedTable,
    key_columns: Vec<String>,
}

impl NormalizedCostFrame {
    pub(crate) fn new(table: IndexedTable, key_columns: Vec<String>) -> Self {
        Self { table, key_columns }
    }

    pub fn is_empty(&self) -> bool {
        self.table.rows().is_empty()
    }

    pub fn row_count(&self) -> usize {
        self.table.rows().len()
    }

    pub fn key_columns(&self) -> &[String] {
        &self.key_columns
    }

    pub(crate) fn into_table(self) -> IndexedTable {
        self.table
    }
}

pub struct SplitResult {
    pub(crate) schema: ColumnSchema,
    pub(crate) detail_display_columns: Vec<ColumnId>,
    pub(crate) detail_rows: Vec<IndexedRow>,
    pub(crate) qty_display_columns: Vec<ColumnId>,
    pub(crate) qty_rows: Vec<IndexedRow>,
}

impl SplitResult {
    pub(crate) fn schema(&self) -> &ColumnSchema {
        &self.schema
    }

    pub(crate) fn detail_rows(&self) -> &[IndexedRow] {
        &self.detail_rows
    }

    pub(crate) fn qty_rows(&self) -> &[IndexedRow] {
        &self.qty_rows
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        ColumnSchema,
        Vec<ColumnId>,
        Vec<IndexedRow>,
        Vec<ColumnId>,
        Vec<IndexedRow>,
    ) {
        (
            self.schema,
            self.detail_display_columns,
            self.detail_rows,
            self.qty_display_columns,
            self.qty_rows,
        )
    }
}
```

删除旧 `TableRow`。中间模型不需要外部序列化时移除其 `Serialize` derive；不得把 `SchemaId` 暴露进 JSON。

- [ ] **Step 4: Implement the temporary small overlay, not a second source Map**

```rust
#[derive(Debug, Clone)]
pub(crate) struct IndexedFactRow {
    source: IndexedRow,
    derived_values: BTreeMap<String, CellValue>,
}

impl IndexedFactRow {
    pub(crate) fn new(source: IndexedRow) -> Self {
        Self {
            source,
            derived_values: BTreeMap::new(),
        }
    }

    pub(crate) fn get_named<'a>(
        &'a self,
        schema: &ColumnSchema,
        name: &str,
    ) -> Result<Option<&'a CellValue>, CostingError> {
        if let Some(value) = self.derived_values.get(name) {
            return Ok(Some(value));
        }
        schema
            .optional(name)
            .map(|id| self.source.get(id))
            .transpose()
    }

    pub(crate) fn insert_derived(
        &mut self,
        name: impl Into<String>,
        value: CellValue,
    ) -> Option<CellValue> {
        self.derived_values.insert(name.into(), value)
    }

    pub(crate) fn take_named(
        &mut self,
        schema: &ColumnSchema,
        name: &str,
    ) -> Result<Option<CellValue>, CostingError> {
        if let Some(value) = self.derived_values.remove(name) {
            return Ok(Some(value));
        }
        schema
            .optional(name)
            .map(|id| self.source.take(id))
            .transpose()
    }

    pub(crate) fn into_parts(self) -> (IndexedRow, BTreeMap<String, CellValue>) {
        (self.source, self.derived_values)
    }
}
```

Phase 2 的临时 `FactBundle` 使用同一 source schema，并提供 CLI 稳定计数方法；Task 14 将 `work_order_rows` 副本替换为唯一索引，但方法名不再改变：

```rust
#[derive(Debug)]
pub struct FactBundle {
    pub(crate) schema: ColumnSchema,
    pub(crate) detail_display_columns: Vec<ColumnId>,
    pub(crate) detail_rows: Vec<IndexedRow>,
    pub(crate) qty_display_columns: Vec<ColumnId>,
    pub(crate) qty_input_row_count: usize,
    pub(crate) filtered_invalid_qty_count: usize,
    pub(crate) filtered_missing_total_amount_count: usize,
    pub(crate) qty_rows: Vec<IndexedFactRow>,
    pub(crate) work_order_rows: Vec<IndexedFactRow>,
    pub(crate) duplicate_work_order_row_count: usize,
    pub(crate) error_issues: Vec<ErrorIssue>,
}

impl FactBundle {
    pub(crate) fn detail_row_count(&self) -> usize {
        self.detail_rows.len()
    }

    pub(crate) fn qty_row_count(&self) -> usize {
        self.qty_rows.len()
    }

    pub(crate) fn work_order_row_count(&self) -> usize {
        self.work_order_rows.len()
    }
}
```

读取优先级必须是 derived overlay → source schema，等价于旧 `values.insert(derived_key, ...)` 的覆盖语义。禁止：

- 把十余个派生列追加到共享 source schema，导致所有 detail 行扩 Blank；
- 把 `IndexedRow` 转回完整 `BTreeMap<String, CellValue>`；
- 为 qty 另造第二个 source schema。

- [ ] **Step 5: Rewrite normalize around pre-resolved IDs**

固定 resolver：

```rust
#[derive(Debug, Clone, Copy)]
struct ResolvedFillColumn {
    id: ColumnId,
    is_vendor: bool,
}

struct NormalizeColumns {
    period: Option<ColumnId>,
    month: Option<ColumnId>,
    cost_center: Option<ColumnId>,
    cost_item: Option<ColumnId>,
    total_row_columns: [Option<ColumnId>; 3],
    fill_columns: Vec<ResolvedFillColumn>,
}

impl NormalizeColumns {
    fn resolve(schema: &ColumnSchema) -> Self {
        Self {
            period: schema.optional(PERIOD_COLUMN),
            month: schema.optional(MONTH_COLUMN),
            cost_center: schema.optional(COST_CENTER_COLUMN),
            cost_item: schema.optional(COST_ITEM_COLUMN),
            total_row_columns: [
                schema.optional(PERIOD_COLUMN),
                schema.optional(MONTH_COLUMN),
                schema.optional(COST_CENTER_COLUMN),
            ],
            fill_columns: FILL_COLUMNS
                .iter()
                .filter_map(|name| {
                    schema.optional(name).map(|id| ResolvedFillColumn {
                        id,
                        is_vendor: VENDOR_COLUMNS.contains(name),
                    })
                })
                .collect(),
        }
    }
}
```

`normalize_workbook` 顺序必须保持：

```rust
let normalized_range = month_range.map(normalize_month_range).transpose()?;
let mut source_names = flatten_headers(&raw.header_rows);
normalize_key_column_names(&mut source_names);
let mut table = IndexedTable::from_raw(source_names, raw.rows)?;
let columns = NormalizeColumns::resolve(table.schema());

table.try_retain_rows(|row| Ok(!is_total_row(row, &columns)?))?;
forward_fill_with_rules(&mut table, &columns)?;

let month_id = if let Some(period_id) = columns.period {
    let values = derive_month_values(table.rows(), period_id)?;
    Some(table.ensure_or_reuse_derived_column(
        MONTH_COLUMN,
        DerivedColumnPosition::AfterFirstSourceName(PERIOD_COLUMN),
        values,
    )?)
} else {
    table.schema().optional(MONTH_COLUMN)
};

let filled_values = derive_filled_cost_item_values(table.rows(), columns.cost_item)?;
table.ensure_or_reuse_derived_column(
    FILLED_COST_ITEM_COLUMN,
    DerivedColumnPosition::End,
    filled_values,
)?;

if let Some(range) = normalized_range.as_ref() {
    table.try_retain_rows(|row| month_in_range(row, month_id, columns.period, range))?;
}

Ok(NormalizedCostFrame::new(
    table,
    KEY_COLUMNS.iter().map(|name| (*name).to_string()).collect(),
))
```

`forward_fill_with_rules` 只保存与 `fill_columns` 等长的短 `Vec<Option<CellValue>>`；按 FILL_COLUMNS 原顺序更新，集成车间供应商列既不填也不成为后续 seed。

- [ ] **Step 6: Rewrite split without changing masks or row order**

```rust
struct SplitColumns {
    child_material: Option<ColumnId>,
    cost_item: Option<ColumnId>,
    filled_cost_item: Option<ColumnId>,
    order_number: Option<ColumnId>,
}

let table = frame.into_table();
let columns = SplitColumns::resolve(table.schema());
let (schema, source_display_order, rows) = table.into_parts();
let source_names = source_display_order
    .iter()
    .map(|id| schema.name(*id).map(str::to_string))
    .collect::<Result<Vec<_>, _>>()?;
let detail_names = detail_sheet_columns(&source_names);
let qty_names = qty_sheet_base_columns(&source_names);
let detail_display_columns = schema.display_order_for(&detail_names)?;
let qty_display_columns = schema.display_order_for(&qty_names)?;
```

逐行分类继续使用当前 mask；缺少整个工单编号列时 `has_order=true`，列存在但该行为空时 `has_order=false`。detail 行若同时有 Filled/成本项目 ID，则用 `take(filled_id)` 后 `replace(cost_item_id, value)`，不 clone 整行。

- [ ] **Step 7: Adapt fact and every downstream consumer in the same working change**

保持 Phase 3 尚未实施的动态金额 bucket 和错误顺序，但所有源字段通过预解析 ID 或 `IndexedFactRow::get_named` 访问：

```rust
#[derive(Debug, Clone, Copy)]
struct WorkOrderColumns {
    // 当前语义是“月份列存在就用月份，否则用年期”，不会因单元格 Blank 再回退。
    month_or_period: Option<ColumnId>,
    product_code: ColumnId,
    work_order_number: ColumnId,
    work_order_line: ColumnId,
}

#[derive(Debug, Clone, Copy)]
struct DetailFactColumns {
    key: WorkOrderColumns,
    cost_item: ColumnId,
    completed_amount: ColumnId,
}

#[derive(Debug, Clone, Copy)]
struct QtyFactColumns {
    key: WorkOrderColumns,
    completed_qty: ColumnId,
    completed_amount: ColumnId,
}
```

`DetailFactColumns::resolve` / `QtyFactColumns::resolve` 先按当前 `REQUIRED_DETAIL_COLUMNS` / `REQUIRED_QTY_COLUMNS` 收集全部缺名，并继续用小型 `BTreeSet<&str>` 排序/去重，分别保留以 `成本明细缺少必要字段:`、`产品数量统计缺少必要字段:` 开头的字段顺序和文本；确认后才调用 `schema.require` 绑定热循环 ID。`work_order_key` 接收 `&WorkOrderColumns`，不再按字符串查四次。

```text
fact:
  SplitResult schema/rows -> dynamic amount_by_key unchanged
  qty source IndexedRow -> IndexedFactRow
  dm_amount/completed_qty 等只写 derived_values

anomaly:
  source field -> schema + IndexedFactRow source
  derived metric -> get_named (overlay first)
  build_work_order_anomaly_sheet -> Result<SheetModel, CostingError>

quality:
  null/analyzable/duplicate checks -> schema + get_named
  build_quality_metrics -> Result<Vec<QualityMetric>, CostingError>

presentation:
  detail rows -> ProjectionPlan(detail_display_columns)
  qty base cells -> ProjectionPlan(qty_display_columns)
  qty derived cells -> take_named，按当前 qty_sheet_columns 后缀顺序追加
  quality/anomaly calls 均使用 ? 传播 foreign/invalid ColumnId
```

从 Task 12 起，所有 `IndexedRow::get` / `IndexedFactRow::get_named` 的结构错误都返回到 `build_workbook_payload`；禁止 `unwrap_or(Blank)`、`continue` 或把 schema mismatch 当成不满足 quality predicate。既有 quality/anomaly tests 改为 `.unwrap()` 取得成功结果，并新增 foreign-ID 错误传播断言。

CLI 的 `normalized.rows.is_empty()` 改为 `normalized.is_empty()`；Task 2 已从 payload quality metrics 构建 run counts，不再跨 crate 访问 bundle 内部字段。三个 crate-private row-count methods 保留给 core quality/tests 使用。

- [ ] **Step 8: Migrate fixtures so each table has one shared SchemaId**

删除测试中所有 `TableRow` 的直接构造。测试 helper 先返回 named fixture，随后一次建立共享表：

```rust
type NamedTestRow = BTreeMap<String, CellValue>;

fn indexed_rows(
    columns: &[String],
    named_rows: Vec<NamedTestRow>,
) -> (ColumnSchema, Vec<IndexedRow>) {
    let positional = named_rows
        .into_iter()
        .map(|mut named| {
            columns
                .iter()
                .map(|column| named.remove(column).unwrap_or(CellValue::Blank))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let table = IndexedTable::from_raw(columns.to_vec(), positional).unwrap();
    let (schema, _, rows) = table.into_parts();
    (schema, rows)
}
```

fact/anomaly/quality/presentation fixtures 必须用同一次构造产生 schema 和全部 rows，不能每行单独创建 SchemaId。原有业务测试名保留。

- [ ] **Step 9: Run the full Phase 2 correctness gate**

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --manifest-path rust/Cargo.toml -p costing-core table::tests
cargo test --manifest-path rust/Cargo.toml -p costing-core normalize::tests
cargo test --manifest-path rust/Cargo.toml -p costing-core split::tests
cargo test --manifest-path rust/Cargo.toml -p costing-core fact::tests
cargo test --manifest-path rust/Cargo.toml -p costing-core anomaly::tests
cargo test --manifest-path rust/Cargo.toml -p costing-core quality::tests
cargo test --manifest-path rust/Cargo.toml -p costing-core presentation::tests
cargo test --manifest-path rust/Cargo.toml
uv run python -m pytest tests/test_full_rust_cli_oracle.py -q --basetemp .pytest-tmp/phase2-oracle
git diff --check
```

Expected: 全部通过；GB/SK oracle 两例均 passed、0 skipped；不更新 baseline。

- [ ] **Step 10: Measure the Phase 2 diagnostic gate**

```powershell
cargo build --release --manifest-path rust/Cargo.toml -p costing-calculate
New-Item -ItemType Directory -Force rust/target/perf/phase2 | Out-Null
Copy-Item -LiteralPath rust/target/release/costing-calculate.exe -Destination rust/target/perf/phase2/costing-calculate.exe -Force
$gbSample = (uv run python -c "from tests.rust_oracle.repo_paths import require_benchmark_sample; print(require_benchmark_sample('gb'))").Trim()
$skSample = (uv run python -c "from tests.rust_oracle.repo_paths import require_benchmark_sample; print(require_benchmark_sample('sk'))").Trim()
uv run python -c "from pathlib import Path; from tests.rust_oracle.benchmark import run_check_only_payload_benchmark, write_check_only_benchmark_result; from tests.rust_oracle.repo_paths import require_benchmark_sample; exe=Path('rust/target/perf/phase2/costing-calculate.exe'); r=run_check_only_payload_benchmark('gb', require_benchmark_sample('gb'), exe); assert r.validation_passed and r.valid_pair_count == 5, r; write_check_only_benchmark_result(r, Path('rust/target/perf/results/check-only-phase2-gb.json'))"
uv run python -c "from pathlib import Path; from tests.rust_oracle.benchmark import run_check_only_payload_benchmark, write_check_only_benchmark_result; from tests.rust_oracle.repo_paths import require_benchmark_sample; exe=Path('rust/target/perf/phase2/costing-calculate.exe'); r=run_check_only_payload_benchmark('sk', require_benchmark_sample('sk'), exe); assert r.validation_passed and r.valid_pair_count == 5, r; write_check_only_benchmark_result(r, Path('rust/target/perf/results/check-only-phase2-sk.json'))"
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/measure_peak_working_set.ps1 -Pipeline gb -InputPath $gbSample -BaselineExecutable rust/target/perf/baseline/costing-calculate.exe -CurrentExecutable rust/target/perf/phase2/costing-calculate.exe -ResultPath rust/target/perf/results/peak-working-set-phase2-gb.json
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/measure_peak_working_set.ps1 -Pipeline sk -InputPath $skSample -BaselineExecutable rust/target/perf/baseline/costing-calculate.exe -CurrentExecutable rust/target/perf/phase2/costing-calculate.exe -ResultPath rust/target/perf/results/peak-working-set-phase2-sk.json
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/capture_cpu_profile.ps1 -Pipeline sk -InputPath $skSample -Label phase2 -ResultPath rust/target/perf/results/phase2-sk-cpu.etl
uv run python -c "from pathlib import Path; from tests.rust_oracle.benchmark import compare_non_target_stage_medians; root=Path('rust/target/perf/results'); results=[compare_non_target_stage_medians(root/f'check-only-baseline-{p}.json',root/f'check-only-phase2-{p}.json',target_stages=frozenset({'normalize','split'})) for p in ('gb','sk')]; print(results); assert not any(results), results"
uv run python -c "import json; from pathlib import Path; root=Path('rust/target/perf/results'); load=lambda name: json.loads((root/name).read_text(encoding='utf-8')); pairs=[(p,load(f'check-only-baseline-{p}.json'),load(f'check-only-phase2-{p}.json')) for p in ('gb','sk')]; reports=[{'pipeline':p,'normalize_drop':1-c['rust_stage_median_seconds']['normalize']/b['rust_stage_median_seconds']['normalize'],'split_ratio':c['rust_stage_median_seconds']['split']/b['rust_stage_median_seconds']['split']} for p,b,c in pairs]; print(reports)"
```

```text
normalize median: 目标下降 >= 50%
normalize median: 若下降 < 30%，必须重新检查 period 格式化、forward fill 和行规范化 profile
split: 不得有无解释显著回退
contracts: 必须全部一致
```

Phase 2 是批准的必做范围；诊断不足时保留正确迁移并带着 profile 证据进入 Phase 3，不回退到逐行 Map。

非目标 stage compare 失败时，按 Phase 1 的同一协议生成 `check-only-phase2-rerun-{pipeline}.json`、第二组 compare 和 data-auditor 审查记录；不能只看 normalize 改善就忽略其他 stage 回退。

- [ ] **Step 11: Commit the vertical migration atomically**

```powershell
git add -- rust/crates/costing-core/src/model.rs rust/crates/costing-core/src/normalize.rs rust/crates/costing-core/src/split.rs rust/crates/costing-core/src/fact.rs rust/crates/costing-core/src/anomaly.rs rust/crates/costing-core/src/quality.rs rust/crates/costing-core/src/presentation.rs rust/crates/costing-cli/src/run.rs
git diff --cached --check
git commit -m "refactor(core): index normalize and split rows"
```

Commit 后用 `rg -n "TableRow|rows_to_maps" rust/crates/costing-core/src`；Expected: 生产链不再引用旧 `TableRow/rows_to_maps`，仅文档文字或无关历史名可出现。

---

### Task 13: Replace Dynamic Cost Buckets with Typed CostAmounts

**Files:**
- Modify: `rust/crates/costing-core/src/model.rs`
- Modify: `rust/crates/costing-core/src/fact.rs`

**Interfaces:**
- Consumes: Task 12 的 `IndexedRow`/`IndexedFactRow`、现有 `PipelineConfig.standalone_cost_items` 顺序、现有成本分类和错误顺序。
- Produces: 紧凑 `CostAmounts`、`CostClassification`、`MohComponent`；聚合容器固定为 `HashMap<String, CostAmounts>`。Task 12 的临时 derived overlay 继续存在，但只由类型化金额一次性写入，Task 14 再整体删除。

这一任务只替换“金额如何聚合”，不同时改数量行扫描和唯一工单模型，以便独立定位 Phase 3 中每项改动的正确性与收益。

- [ ] **Step 1: Add failing classification and aggregation tests**

在 `fact.rs` tests 新增：

- `classifies_direct_material_and_direct_labor_without_allocating_bucket_names`
- `manufacturing_component_updates_total_and_matching_component`
- `unknown_manufacturing_component_updates_only_moh_total`
- `standalone_cost_uses_pipeline_configuration_index`
- `sk_standalone_cost_order_keeps_outsource_before_software`
- `unmapped_non_blank_cost_item_still_emits_the_same_issue`
- `missing_amount_issue_payload_remains_exact`
- `typed_amounts_keep_missing_amount_issue_before_qty_issues`

关键断言：

```rust
#[test]
fn manufacturing_component_updates_total_and_matching_component() {
    let classification = classify_cost_item(
        "制造费用_折旧",
        &["委外加工费", "软件费用"],
    );
    assert_eq!(
        classification,
        CostClassification::ManufacturingOverhead(Some(MohComponent::Depreciation)),
    );

    let mut amounts = CostAmounts::new(2);
    amounts.add(classification, Decimal::new(1250, 2));

    assert_eq!(amounts.manufacturing_overhead, Decimal::new(1250, 2));
    assert_eq!(amounts.moh_depreciation, Decimal::new(1250, 2));
    assert_eq!(amounts.moh_component_sum(), Decimal::new(1250, 2));
}

#[test]
fn sk_standalone_cost_order_keeps_outsource_before_software() {
    let items = ["委外加工费", "软件费用"];
    assert_eq!(
        classify_cost_item("委外加工费", &items),
        CostClassification::Standalone(0),
    );
    assert_eq!(
        classify_cost_item("软件费用", &items),
        CostClassification::Standalone(1),
    );
}
```

`typed_amounts_keep_missing_amount_issue_before_qty_issues` 用一个缺金额 detail、两个重复 qty、一个总额不勾稽样本断言完整 issue type 顺序，不只断言计数。

`missing_amount_issue_payload_remains_exact` 使用具备完整 qty schema 但 qty rows 为空的 fixture，使结果只有一条 issue，并硬编码比较整个结构：

```rust
assert_eq!(
    bundle.error_issues,
    vec![ErrorIssue {
        row_id: "2025年01期|P1|WO1|1".to_string(),
        issue_type: "MISSING_AMOUNT".to_string(),
        field_name: "本期完工金额".to_string(),
        original_value: String::new(),
        reason: "成本明细金额为空，已按 0 参与汇总".to_string(),
        action: "金额置为 0 后继续计算".to_string(),
        retryable: false,
    }],
);
```

- [ ] **Step 2: Run focused tests and verify red**

```powershell
cargo test --manifest-path rust/Cargo.toml -p costing-core fact::tests
```

Expected: `CostAmounts`、`CostClassification` 和 `classify_cost_item` 未定义导致 FAIL。

- [ ] **Step 3: Add the compact amount model**

在 `model.rs` 定义数据，不把字符串 bucket 或 pipeline 名称保存到每个工单：

```rust
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CostAmounts {
    pub(crate) direct_material: Decimal,
    pub(crate) direct_labor: Decimal,
    pub(crate) manufacturing_overhead: Decimal,
    pub(crate) moh_other: Decimal,
    pub(crate) moh_labor: Decimal,
    pub(crate) moh_consumables: Decimal,
    pub(crate) moh_depreciation: Decimal,
    pub(crate) moh_utilities: Decimal,
    pub(crate) standalone: Vec<Decimal>,
}

impl CostAmounts {
    pub(crate) fn new(standalone_count: usize) -> Self {
        Self {
            direct_material: Decimal::ZERO,
            direct_labor: Decimal::ZERO,
            manufacturing_overhead: Decimal::ZERO,
            moh_other: Decimal::ZERO,
            moh_labor: Decimal::ZERO,
            moh_consumables: Decimal::ZERO,
            moh_depreciation: Decimal::ZERO,
            moh_utilities: Decimal::ZERO,
            standalone: vec![Decimal::ZERO; standalone_count],
        }
    }

    pub(crate) fn standalone_amount(&self, index: usize) -> Decimal {
        self.standalone.get(index).copied().unwrap_or(Decimal::ZERO)
    }

    pub(crate) fn moh_component_sum(&self) -> Decimal {
        self.moh_other
            + self.moh_labor
            + self.moh_consumables
            + self.moh_depreciation
            + self.moh_utilities
    }
}
```

不为“金额 bucket 是否曾出现”增加 bitset：当前事实输出对缺少 bucket 的语义就是按 `0` 写出，`MISSING_AMOUNT` 由 detail 原值单独审计；增加 presence 状态不会改变任何已批准输出，只会扩大模型。

- [ ] **Step 4: Replace string-producing classification with enums**

在 `fact.rs` 定义：

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MohComponent {
    Other,
    Labor,
    Consumables,
    Depreciation,
    Utilities,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CostClassification {
    DirectMaterial,
    DirectLabor,
    ManufacturingOverhead(Option<MohComponent>),
    Standalone(usize),
    Unmapped,
}

fn classify_cost_item(
    cost_item: &str,
    standalone_items: &[&str],
) -> CostClassification {
    let normalized = cost_item.trim();
    match normalized {
        "直接材料" => CostClassification::DirectMaterial,
        "直接人工" => CostClassification::DirectLabor,
        value if value.starts_with("制造费用") => {
            let component = match value {
                "制造费用_其他" => Some(MohComponent::Other),
                "制造费用-人工" => Some(MohComponent::Labor),
                "制造费用_机物料及低耗" => Some(MohComponent::Consumables),
                "制造费用_折旧" => Some(MohComponent::Depreciation),
                "制造费用_水电费" => Some(MohComponent::Utilities),
                _ => None,
            };
            CostClassification::ManufacturingOverhead(component)
        }
        value => standalone_items
            .iter()
            .position(|item| item.trim() == value)
            .map(CostClassification::Standalone)
            .unwrap_or(CostClassification::Unmapped),
    }
}

impl CostAmounts {
    fn add(&mut self, classification: CostClassification, amount: Decimal) {
        match classification {
            CostClassification::DirectMaterial => self.direct_material += amount,
            CostClassification::DirectLabor => self.direct_labor += amount,
            CostClassification::ManufacturingOverhead(component) => {
                self.manufacturing_overhead += amount;
                match component {
                    Some(MohComponent::Other) => self.moh_other += amount,
                    Some(MohComponent::Labor) => self.moh_labor += amount,
                    Some(MohComponent::Consumables) => self.moh_consumables += amount,
                    Some(MohComponent::Depreciation) => self.moh_depreciation += amount,
                    Some(MohComponent::Utilities) => self.moh_utilities += amount,
                    None => {}
                }
            }
            CostClassification::Standalone(index) => {
                self.standalone[index] += amount;
            }
            CostClassification::Unmapped => {}
        }
    }
}
```

`Standalone(index)` 只能来自当前 slice 的 `position`，因此 `self.standalone[index]` 有边界保证；测试覆盖 GB 一项和 SK 两项。制造费用总额与细项保持“一行同时进入总额和一个可识别细项”的双口径。

- [ ] **Step 5: Use HashMap<String, CostAmounts> without changing traversal order**

金额聚合改为：

```rust
let standalone_count = config.standalone_cost_items.len();
let mut amounts_by_key: HashMap<String, CostAmounts> = HashMap::new();
let detail_columns = DetailFactColumns::resolve(split.schema())?;

for row in split.detail_rows() {
    let key = work_order_key(row, &detail_columns.key)?;
    let cost_item = text_by_id(row, detail_columns.cost_item)?;
    let amount = decimal_by_id(row, detail_columns.completed_amount)?;
    let classification = classify_cost_item(&cost_item, config.standalone_cost_items);

    if classification == CostClassification::Unmapped {
        if !cost_item.trim().is_empty() {
            error_issues.push(unmapped_cost_issue(key, cost_item));
        }
        continue;
    }
    if amount.is_none() {
        error_issues.push(missing_amount_issue(
            key.clone(),
            original_text_by_id(row, detail_columns.completed_amount)?,
        ));
    }
    amounts_by_key
        .entry(key)
        .or_insert_with(|| CostAmounts::new(standalone_count))
        .add(classification, amount.unwrap_or(Decimal::ZERO));
}
```

`HashMap` 只按 key 查询，禁止遍历它生成 Sheet、issue 或 quality 输出。detail 输入向量仍决定 `UNMAPPED_COST_ITEM`/`MISSING_AMOUNT` 顺序。

- [ ] **Step 6: Bridge typed amounts into the temporary Phase 2 overlay**

在 Task 14 删除 overlay 前，使用一个固定 helper 写入当前派生键：

```rust
fn write_amount_overlay(
    row: &mut IndexedFactRow,
    amounts: &CostAmounts,
    config: &PipelineConfig,
) {
    row.insert_derived(DM_AMOUNT_KEY, CellValue::Decimal(amounts.direct_material));
    row.insert_derived(DL_AMOUNT_KEY, CellValue::Decimal(amounts.direct_labor));
    row.insert_derived(MOH_AMOUNT_KEY, CellValue::Decimal(amounts.manufacturing_overhead));
    row.insert_derived(MOH_OTHER_AMOUNT_KEY, CellValue::Decimal(amounts.moh_other));
    row.insert_derived(MOH_LABOR_AMOUNT_KEY, CellValue::Decimal(amounts.moh_labor));
    row.insert_derived(
        MOH_CONSUMABLES_AMOUNT_KEY,
        CellValue::Decimal(amounts.moh_consumables),
    );
    row.insert_derived(
        MOH_DEPRECIATION_AMOUNT_KEY,
        CellValue::Decimal(amounts.moh_depreciation),
    );
    row.insert_derived(MOH_UTILITIES_AMOUNT_KEY, CellValue::Decimal(amounts.moh_utilities));
    for (index, item) in config.standalone_cost_items.iter().enumerate() {
        row.insert_derived(
            standalone_key(item),
            CellValue::Decimal(amounts.standalone_amount(index)),
        );
    }
}
```

现有 `standalone_key` 只为这最多两个派生槽生成名称；明细热循环不再生成 bucket `String`/`Vec<String>`。删除旧 `bucket_names`、`moh_component_key`、两层 `BTreeMap` 聚合，以及金额 lookup 后克隆整个内层 Map 的路径。

- [ ] **Step 7: Run correctness, oracle, and Phase 3 aggregation diagnostics**

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --manifest-path rust/Cargo.toml -p costing-core fact::tests
cargo test --manifest-path rust/Cargo.toml
uv run python -m pytest tests/test_full_rust_cli_oracle.py -q --basetemp .pytest-tmp/typed-amount-oracle
git diff --check
```

Expected: 全部通过，GB/SK oracle 均 passed、0 skipped；所有 issue type/count/order characterization 不变。随后执行：

```powershell
cargo build --release --manifest-path rust/Cargo.toml -p costing-calculate
New-Item -ItemType Directory -Force rust/target/perf/phase3-typed | Out-Null
Copy-Item -LiteralPath rust/target/release/costing-calculate.exe -Destination rust/target/perf/phase3-typed/costing-calculate.exe -Force
$gbSample = (uv run python -c "from tests.rust_oracle.repo_paths import require_benchmark_sample; print(require_benchmark_sample('gb'))").Trim()
$skSample = (uv run python -c "from tests.rust_oracle.repo_paths import require_benchmark_sample; print(require_benchmark_sample('sk'))").Trim()
uv run python -c "from pathlib import Path; from tests.rust_oracle.benchmark import run_check_only_payload_benchmark, write_check_only_benchmark_result; from tests.rust_oracle.repo_paths import require_benchmark_sample; exe=Path('rust/target/perf/phase3-typed/costing-calculate.exe'); r=run_check_only_payload_benchmark('gb', require_benchmark_sample('gb'), exe); assert r.validation_passed and r.valid_pair_count == 5, r; write_check_only_benchmark_result(r, Path('rust/target/perf/results/check-only-phase3-typed-gb.json'))"
uv run python -c "from pathlib import Path; from tests.rust_oracle.benchmark import run_check_only_payload_benchmark, write_check_only_benchmark_result; from tests.rust_oracle.repo_paths import require_benchmark_sample; exe=Path('rust/target/perf/phase3-typed/costing-calculate.exe'); r=run_check_only_payload_benchmark('sk', require_benchmark_sample('sk'), exe); assert r.validation_passed and r.valid_pair_count == 5, r; write_check_only_benchmark_result(r, Path('rust/target/perf/results/check-only-phase3-typed-sk.json'))"
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/measure_peak_working_set.ps1 -Pipeline gb -InputPath $gbSample -BaselineExecutable rust/target/perf/baseline/costing-calculate.exe -CurrentExecutable rust/target/perf/phase3-typed/costing-calculate.exe -ResultPath rust/target/perf/results/peak-working-set-phase3-typed-gb.json
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/measure_peak_working_set.ps1 -Pipeline sk -InputPath $skSample -BaselineExecutable rust/target/perf/baseline/costing-calculate.exe -CurrentExecutable rust/target/perf/phase3-typed/costing-calculate.exe -ResultPath rust/target/perf/results/peak-working-set-phase3-typed-sk.json
uv run python -c "from pathlib import Path; from tests.rust_oracle.benchmark import compare_non_target_stage_medians; root=Path('rust/target/perf/results'); results=[compare_non_target_stage_medians(root/f'check-only-baseline-{p}.json',root/f'check-only-phase3-typed-{p}.json',target_stages=frozenset({'fact'})) for p in ('gb','sk')]; print(results); assert not any(results), results"
```

此中间诊断不替代 Task 15 最终门禁。非目标回退按前述 rerun/data-auditor 协议处理；若 fact 改善不足 10%，先执行以下命令再进入 Task 14：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/capture_cpu_profile.ps1 -Pipeline sk -InputPath $skSample -Label phase3-typed -ResultPath rust/target/perf/results/phase3-typed-sk-cpu.etl
```

- [ ] **Step 8: Commit typed aggregation**

```powershell
git add -- rust/crates/costing-core/src/model.rs rust/crates/costing-core/src/fact.rs
git diff --cached --check
git commit -m "perf(core): type cost amount aggregation"
```

---

### Task 14: Cache Qty Keys, Remove Row Copies, and Reuse Fact Audit Results

**Files:**
- Modify: `rust/crates/costing-core/src/model.rs`
- Modify: `rust/crates/costing-core/src/fact.rs`
- Modify: `rust/crates/costing-core/src/anomaly.rs`
- Modify: `rust/crates/costing-core/src/quality.rs`
- Modify: `rust/crates/costing-core/src/presentation.rs`
- Modify: `rust/crates/costing-cli/src/run.rs`

**Interfaces:**
- Consumes: Task 13 `CostAmounts`、Task 12 indexed source rows、现有 issue/quality/anomaly/Sheet contracts。
- Produces: `PreparedQtyRow`、最终 `QtyFactRow`、缓存的规范化工单键、`unique_work_order_indices`、fact 预计算的重复/过滤/勾稽结果；删除 `IndexedFactRow` overlay 和完整 `work_order_rows` 副本。

- [ ] **Step 1: Write failing tests for final row ownership and exact audit order**

在现有模块 tests 中新增：

`fact.rs`：

- `prepared_qty_row_caches_normalized_work_order_key`
- `qty_fact_keeps_all_valid_rows_in_input_order`
- `three_duplicate_qty_rows_count_as_three_duplicate_rows`
- `unique_work_order_indices_keep_only_the_first_occurrence`
- `fact_issue_order_is_detail_then_duplicate_then_reconciliation_then_unit_cost`
- `gb_total_reconciliation_uses_outsource_as_standalone`
- `sk_total_reconciliation_uses_outsource_and_software_as_standalone`

`quality.rs`：

- `quality_reuses_fact_duplicate_count_without_rebuilding_keys`
- `quality_uses_unique_indices_for_analysis_coverage`
- `quality_reports_zero_non_positive_qty_after_fact_filter`

`presentation.rs` / `anomaly.rs`：

- `qty_sheet_projects_typed_amounts_without_overlay_map`
- `work_order_sheet_borrows_qty_fact_by_unique_indices`
- `presentation_preserves_three_sheet_order_after_fact_model_change`

完整 issue 顺序断言固定为：

```rust
assert_eq!(
    issue_types(&bundle.error_issues),
    vec![
        "MISSING_AMOUNT",
        "DUPLICATE_WORK_ORDER_KEY",
        "DUPLICATE_WORK_ORDER_KEY",
        "MOH_BREAKDOWN_MISMATCH",
        "TOTAL_COST_MISMATCH",
        "NON_POSITIVE_UNIT_COST",
    ],
);
```

fixture 必须构造出恰好这一序列；不能排序后比较。三重重复另行断言三个重复行各生成一条 issue，`duplicate_work_order_row_count == 3`，唯一索引只指向第一行。

三重重复的三条 `ErrorIssue` 还必须逐字段断言：相同规范化 `row_id`、`issue_type=DUPLICATE_WORK_ORDER_KEY`、`field_name=工单主键`、`original_value=3`、当前 reason/action 文本和 `retryable=false`；不能只比较 type/count。

- [ ] **Step 2: Run focused tests and verify red**

```powershell
cargo test --manifest-path rust/Cargo.toml -p costing-core fact::tests
cargo test --manifest-path rust/Cargo.toml -p costing-core quality::tests
cargo test --manifest-path rust/Cargo.toml -p costing-core presentation::tests
```

Expected: 最终结构和方法尚不存在导致 FAIL。

- [ ] **Step 3: Introduce PreparedQtyRow and the final typed fact row**

`PreparedQtyRow` 只在 `fact.rs` 构建期间存在：

```rust
struct PreparedQtyRow {
    source: IndexedRow,
    work_order_key: String,
    completed_qty: Decimal,
    completed_total: Decimal,
}
```

在 `model.rs` 定义最终事实行：

```rust
#[derive(Debug)]
pub(crate) struct QtyFactRow {
    pub(crate) source: IndexedRow,
    pub(crate) work_order_key: String,
    pub(crate) completed_qty: Decimal,
    pub(crate) completed_total: Decimal,
    pub(crate) amounts: CostAmounts,
    pub(crate) moh_matches: bool,
    pub(crate) total_matches: bool,
    pub(crate) check_reason: String,
}
```

`check_reason` 必须复用当前中文拼接函数生成，不能把展示层重新变成第二套勾稽规则。`CostAmounts` 仍用 `Decimal`；不引入 float 或新的 numeric type。

- [ ] **Step 4: Replace FactBundle work-order copies with stable indices**

最终 bundle：

```rust
#[derive(Debug)]
pub struct FactBundle {
    pub(crate) schema: ColumnSchema,
    pub(crate) detail_display_columns: Vec<ColumnId>,
    pub(crate) detail_rows: Vec<IndexedRow>,
    pub(crate) qty_display_columns: Vec<ColumnId>,
    pub(crate) qty_rows: Vec<QtyFactRow>,
    pub(crate) unique_work_order_indices: Vec<usize>,
    pub(crate) qty_input_row_count: usize,
    pub(crate) filtered_invalid_qty_count: usize,
    pub(crate) filtered_missing_total_amount_count: usize,
    pub(crate) duplicate_work_order_row_count: usize,
    pub(crate) error_issues: Vec<ErrorIssue>,
}

impl FactBundle {
    pub(crate) fn work_order_rows(&self) -> impl Iterator<Item = &QtyFactRow> {
        self.unique_work_order_indices
            .iter()
            .map(|index| &self.qty_rows[*index])
    }

    pub(crate) fn detail_row_count(&self) -> usize {
        self.detail_rows.len()
    }

    pub(crate) fn qty_row_count(&self) -> usize {
        self.qty_rows.len()
    }

    pub(crate) fn work_order_row_count(&self) -> usize {
        self.unique_work_order_indices.len()
    }
}
```

删除 `IndexedFactRow`、`derived_values`、overlay-first lookup、完整 `work_order_rows` 向量以及对应临时 tests。`unique_work_order_indices` 只追加每个 key 第一次出现时的 `qty_rows` index。

- [ ] **Step 5: Build facts in fixed passes and preserve issue order**

主流程必须按以下顺序执行；`HashMap`/`HashSet` 只查询，所有输出均按输入向量遍历：

```rust
// 1. detail 输入序：聚合类型化金额，同时追加 UNMAPPED/MISSING issue。
let detail_columns = DetailFactColumns::resolve(split.schema())?;
let qty_columns = QtyFactColumns::resolve(split.schema())?;
let (
    schema,
    detail_display_columns,
    detail_rows,
    qty_display_columns,
    qty_source_rows,
) = split.into_parts();
let mut error_issues = Vec::new();
let amounts_by_key = aggregate_detail_rows_in_input_order(
    &detail_rows,
    &schema,
    &detail_columns,
    config,
    &mut error_issues,
)?;

// 2. qty 输入序：一次读取字段、过滤、生成并缓存 key，同时统计 key 次数。
let qty_input_row_count = qty_source_rows.len();
let mut prepared_rows = Vec::with_capacity(qty_input_row_count);
let mut qty_key_counts: HashMap<String, usize> = HashMap::new();
let mut filtered_invalid_qty_count = 0usize;
let mut filtered_missing_total_amount_count = 0usize;
for source in qty_source_rows {
    let (completed_qty, completed_total) = match (
        decimal_by_id(&source, qty_columns.completed_qty)?,
        decimal_by_id(&source, qty_columns.completed_amount)?,
    ) {
        (Some(qty), Some(total)) if qty > Decimal::ZERO => (qty, total),
        (Some(qty), None) if qty > Decimal::ZERO => {
            filtered_missing_total_amount_count += 1;
            continue;
        }
        _ => {
            filtered_invalid_qty_count += 1;
            continue;
        }
    };
    let work_order_key = work_order_key(&source, &qty_columns.key)?;
    *qty_key_counts.entry(work_order_key.clone()).or_default() += 1;
    prepared_rows.push(PreparedQtyRow {
        source,
        work_order_key,
        completed_qty,
        completed_total,
    });
}

// 3. prepared 输入序：为重复 key 的每一行追加一条 issue，并统计重复行数。
let mut duplicate_work_order_row_count = 0usize;
for row in &prepared_rows {
    let count = qty_key_counts[&row.work_order_key];
    if count > 1 {
        duplicate_work_order_row_count += 1;
        error_issues.push(duplicate_work_order_issue(&row.work_order_key, count));
    }
}

// 4. consume prepared 输入序：每行先 MOH 后 total issue，再 push fact/首次唯一索引。
let mut qty_rows = Vec::with_capacity(prepared_rows.len());
let mut unique_work_order_indices = Vec::new();
let mut seen_work_orders = HashSet::new();
for prepared in prepared_rows {
    let amounts = amounts_by_key
        .get(&prepared.work_order_key)
        .cloned()
        .unwrap_or_else(|| CostAmounts::new(config.standalone_cost_items.len()));
    let audit = calculate_reconciliation(&amounts, prepared.completed_total, config);
    append_reconciliation_issues_in_current_order(
        &mut error_issues,
        &prepared.work_order_key,
        &amounts,
        prepared.completed_total,
        &audit,
        config,
    );
    let index = qty_rows.len();
    let is_first = seen_work_orders.insert(prepared.work_order_key.clone());
    qty_rows.push(build_qty_fact_row(prepared, amounts, audit));
    if is_first {
        unique_work_order_indices.push(index);
    }
}

// 5. unique 首次出现顺序 + NON_POSITIVE_UNIT_COST_METRICS 固定顺序。
append_non_positive_unit_cost_issues(
    &qty_rows,
    &unique_work_order_indices,
    &mut error_issues,
);
```

这一分支不使用 `Option::is_none_or` 或 `expect`，因此不提升 MSRV，也不会因输入数据触发 panic。`qty_input_row_count` 在消费向量前保存；最终构造 bundle 时复用这里绑定的 schema/display/detail 字段。

- [ ] **Step 6: Make reconciliation and unit-cost functions typed**

将以下逻辑从 string-key Map 改为 `CostAmounts` 字段访问，文本、比较和顺序不变：

```rust
struct ReconciliationAudit {
    moh_component_sum: Decimal,
    derived_total: Decimal,
    moh_matches: bool,
    total_matches: bool,
    check_reason: String,
}

fn calculate_reconciliation(
    amounts: &CostAmounts,
    completed_total: Decimal,
    config: &PipelineConfig,
) -> ReconciliationAudit {
    let moh_component_sum = amounts.moh_component_sum();
    let derived_total = amounts.direct_material
        + amounts.direct_labor
        + amounts.manufacturing_overhead
        + amounts.standalone.iter().copied().sum::<Decimal>();
    let moh_matches = moh_component_sum == amounts.manufacturing_overhead;
    let total_matches = derived_total == completed_total;
    ReconciliationAudit {
        moh_component_sum,
        derived_total,
        moh_matches,
        total_matches,
        check_reason: build_check_reason(moh_matches, total_matches, config),
    }
}
```

`append_non_positive_unit_cost_issues` 遍历 `unique_work_order_indices`，并用固定数组把 typed amount accessor 与现有中文 field name 一一对应。独立成本项继续不进入 Modified Z-score 指标数组，也不因独立身份产生 issue。

- [ ] **Step 7: Reuse fact metadata in quality instead of rescanning keys**

`build_quality_metrics` 改为：

- detail/qty/work-order 行数读取 bundle methods；
- 过滤计数读取 bundle fields；
- 重复行数直接读取 `duplicate_work_order_row_count`；
- 分析覆盖率只遍历 `work_order_rows()`，精确保留当前 `completed_qty > 0 && completed_total > 0 && doc_type 可归类` 三个谓词；不通过除法重写为“正总单位成本”；
- 非正完工数量对已过滤 `qty_rows` 仍返回 `0`，并由测试证明；
- 月份过滤空结果仍返回两个 `N/A`，描述不变；
- 直接材料派生金额始终是 typed `Decimal`，非空 qty facts 的缺失率保持 `0.00%`。

删除 quality 中 `duplicate_work_order_row_count(rows)`、重复 `work_order_key` Map/Set 和从 string-key overlay 取金额的路径。

- [ ] **Step 8: Adapt anomaly and presentation to typed rows and one-way ownership**

`anomaly.rs`：

- 输入改为 `bundle.work_order_rows()`；
- `AnomalyRow<'a>.source` 改借用 `&'a QtyFactRow`；
- source 业务字段从 `source.source` + schema ID 读取；
- 完工数量、总额、三大类、制造费用细项和独立成本项直接读 typed 字段；
- 分组、排序、Modified Z-score、白名单顺序和解释文本不变。

`presentation.rs` 严格先借用后消费：

```rust
pub fn build_workbook_payload(
    bundle: FactBundle,
    config: &PipelineConfig,
    stage_timings: StageTimings,
    month_filter_empty_result: bool,
) -> Result<WorkbookPayload, CostingError> {
    let quality_metrics = build_quality_metrics(&bundle, month_filter_empty_result)?;
    let work_order_sheet = build_work_order_anomaly_sheet(&bundle, config)?;

    let FactBundle {
        schema,
        detail_display_columns,
        detail_rows,
        qty_display_columns,
        qty_rows,
        error_issues,
        ..
    } = bundle;

    let detail_sheet = build_indexed_sheet(
        "成本计算单总表",
        &schema,
        detail_display_columns,
        detail_rows,
    )?;
    let qty_sheet = build_typed_qty_sheet(
        &schema,
        qty_display_columns,
        qty_rows,
        config,
    )?;
    let error_log_count = error_issues.len();
    Ok(WorkbookPayload {
        sheet_models: vec![detail_sheet, qty_sheet, work_order_sheet],
        quality_metrics,
        error_log_count,
        error_log: error_issues,
        stage_timings,
    })
}
```

`build_typed_qty_sheet` 为 source base IDs 建一次 `ProjectionPlan`，逐行消费 `QtyFactRow.source`，再按现有 `qty_sheet_columns` 后缀固定顺序把 `CostAmounts`、单位成本、勾稽状态和 `check_reason` 直接 push 到 `Vec<CellValue>`。不得重新构造 `BTreeMap<String, CellValue>`。

CLI 不读取 bundle；它继续在 payload timer 停止后从 quality metrics 构建 run counts。core quality/tests 使用 `detail_row_count()`、`qty_row_count()`、`work_order_row_count()`，不得重算 key。

- [ ] **Step 9: Prove the legacy copies and dynamic lookups are gone**

```powershell
rg -n "IndexedFactRow|derived_values|work_order_rows:\s*Vec|bucket_names|BTreeMap<String, BTreeMap<String, Decimal>>|work_order_fact|amount_by_key.*cloned" rust/crates/costing-core/src rust/crates/costing-cli/src
```

Expected: 生产代码无匹配；测试名/注释若包含迁移术语应改为最终领域名，不留下临时兼容层。

再检查 `work_order_key(` 调用：

```powershell
rg -n "work_order_key\(" rust/crates/costing-core/src/fact.rs rust/crates/costing-core/src/quality.rs rust/crates/costing-core/src/anomaly.rs
```

Expected: production 只在 detail 聚合每行一次、qty prepare 每行一次；quality/anomaly 无重算调用。

- [ ] **Step 10: Run the complete Phase 3 correctness gate**

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --manifest-path rust/Cargo.toml -p costing-core fact::tests
cargo test --manifest-path rust/Cargo.toml -p costing-core anomaly::tests
cargo test --manifest-path rust/Cargo.toml -p costing-core quality::tests
cargo test --manifest-path rust/Cargo.toml -p costing-core presentation::tests
cargo test --manifest-path rust/Cargo.toml
uv run python -m pytest tests/test_full_rust_cli_oracle.py -q --basetemp .pytest-tmp/phase3-oracle
uv run python -m pytest tests/contracts -q --basetemp .pytest-tmp/phase3-contracts
git diff --check
```

Expected: 全部通过；GB/SK oracle 分别 passed、0 skipped；baseline 不更新；Sheet 顺序、cell 语义、runtime summary、issue 类型/数量/顺序和 quality metrics 均一致。

- [ ] **Step 11: Measure and profile the final mandatory phase**

```powershell
cargo build --release --manifest-path rust/Cargo.toml -p costing-calculate
New-Item -ItemType Directory -Force rust/target/perf/phase3 | Out-Null
Copy-Item -LiteralPath rust/target/release/costing-calculate.exe -Destination rust/target/perf/phase3/costing-calculate.exe -Force
$gbSample = (uv run python -c "from tests.rust_oracle.repo_paths import require_benchmark_sample; print(require_benchmark_sample('gb'))").Trim()
$skSample = (uv run python -c "from tests.rust_oracle.repo_paths import require_benchmark_sample; print(require_benchmark_sample('sk'))").Trim()
uv run python -c "from pathlib import Path; from tests.rust_oracle.benchmark import run_check_only_payload_benchmark, write_check_only_benchmark_result; from tests.rust_oracle.repo_paths import require_benchmark_sample; exe=Path('rust/target/perf/phase3/costing-calculate.exe'); r=run_check_only_payload_benchmark('gb', require_benchmark_sample('gb'), exe); assert r.validation_passed and r.valid_pair_count == 5, r; write_check_only_benchmark_result(r, Path('rust/target/perf/results/check-only-phase3-gb.json'))"
uv run python -c "from pathlib import Path; from tests.rust_oracle.benchmark import run_check_only_payload_benchmark, write_check_only_benchmark_result; from tests.rust_oracle.repo_paths import require_benchmark_sample; exe=Path('rust/target/perf/phase3/costing-calculate.exe'); r=run_check_only_payload_benchmark('sk', require_benchmark_sample('sk'), exe); assert r.validation_passed and r.valid_pair_count == 5, r; write_check_only_benchmark_result(r, Path('rust/target/perf/results/check-only-phase3-sk.json'))"
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/measure_peak_working_set.ps1 -Pipeline gb -InputPath $gbSample -BaselineExecutable rust/target/perf/baseline/costing-calculate.exe -CurrentExecutable rust/target/perf/phase3/costing-calculate.exe -ResultPath rust/target/perf/results/peak-working-set-phase3-gb.json
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/measure_peak_working_set.ps1 -Pipeline sk -InputPath $skSample -BaselineExecutable rust/target/perf/baseline/costing-calculate.exe -CurrentExecutable rust/target/perf/phase3/costing-calculate.exe -ResultPath rust/target/perf/results/peak-working-set-phase3-sk.json
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/capture_cpu_profile.ps1 -Pipeline sk -InputPath $skSample -Label phase3 -ResultPath rust/target/perf/results/phase3-sk-cpu.etl
uv run python -c "from pathlib import Path; from tests.rust_oracle.benchmark import compare_non_target_stage_medians; root=Path('rust/target/perf/results'); results=[compare_non_target_stage_medians(root/f'check-only-baseline-{p}.json',root/f'check-only-phase3-{p}.json',target_stages=frozenset({'fact','presentation'})) for p in ('gb','sk')]; print(results); assert not any(results), results"
uv run python -c "import json; from pathlib import Path; root=Path('rust/target/perf/results'); load=lambda name: json.loads((root/name).read_text(encoding='utf-8')); pairs=[(p,load(f'check-only-phase2-{p}.json'),load(f'check-only-phase3-{p}.json')) for p in ('gb','sk')]; reports=[{'pipeline':p,'fact_drop_from_phase2':1-c['rust_stage_median_seconds']['fact']/b['rust_stage_median_seconds']['fact']} for p,b,c in pairs]; print(reports)"
```

每个 JSON 固定保存五个 Rust/Python total、六组各五个 Rust stage、runtime evidence 和环境/hash；PWS 各五对；ETL 来自同一 source revision 的带符号 release profiling binary。

Phase 3 诊断目标：fact median 至少下降 20%；不足 10% 时 profile 工单键、`ErrorIssue` 分配和 Decimal 运算。无论是否达到该诊断目标，都以 Task 15 的全局硬门禁决定 Phase 4；不得为了单阶段百分比破坏契约。

- [ ] **Step 12: Commit the final fact model**

```powershell
git add -- rust/crates/costing-core/src/model.rs rust/crates/costing-core/src/fact.rs rust/crates/costing-core/src/anomaly.rs rust/crates/costing-core/src/quality.rs rust/crates/costing-core/src/presentation.rs rust/crates/costing-cli/src/run.rs
git diff --cached --check
git commit -m "perf(core): cache typed work order facts"
```

---

### Task 15: Enforce the Final GB/SK Gates and Stop or Enter One-Hotspot Phase 4

**Files:**
- Create: `tests/test_rust_check_only_benchmark.py`
- Generated only: `rust/target/perf/final/costing-calculate.exe`
- Generated only: `rust/target/perf/results/check-only-final-*.json`
- Generated only: `rust/target/perf/results/peak-working-set-final-*.json`
- Conditional create only when required: `docs/superpowers/plans/2026-07-10-rust-pipeline-performance-phase4-hotspot.md`

**Interfaces:**
- Consumes: Task 4 fixed five-round harness、Task 5 strict samples、Task 6 memory harness、Phase 0 baseline executable、Task 14 final Rust pipeline。
- Produces: 两个不可 skip 的 pytest cases、GB/SK 各五轮有效 check-only 证据、现有 full-pipeline `VALIDATED` 证据、GB/SK Peak Working Set `VALIDATED` 证据，以及明确的 stop/Phase 4 verdict。

- [ ] **Step 1: Add the non-skippable final check-only tests**

```python
from __future__ import annotations

from pathlib import Path

import pytest

from tests.rust_oracle.benchmark import (
    CHECK_ONLY_ROUNDS,
    run_check_only_payload_benchmark,
    write_check_only_benchmark_result,
)
from tests.rust_oracle.oracle_runner import (
    REQUIRED_RUST_PAYLOAD_STAGES,
    build_rust_cli_release,
)
from tests.rust_oracle.repo_paths import repo_root, require_benchmark_sample


@pytest.fixture(scope='module')
def rust_release_executable() -> Path:
    return build_rust_cli_release()


@pytest.mark.parametrize('pipeline', ('gb', 'sk'))
def test_rust_check_only_is_not_slower_than_python(
    pipeline: str,
    rust_release_executable: Path,
) -> None:
    result = run_check_only_payload_benchmark(
        pipeline,
        require_benchmark_sample(pipeline),
        rust_release_executable,
    )
    result_path = (
        repo_root()
        / 'rust'
        / 'target'
        / 'perf'
        / 'results'
        / f'check-only-final-{pipeline}.json'
    )
    write_check_only_benchmark_result(result, result_path)

    assert len(result.rust_payload_total_seconds) == CHECK_ONLY_ROUNDS, result
    assert len(result.python_payload_total_seconds) == CHECK_ONLY_ROUNDS, result
    assert set(result.rust_stage_seconds) == set(REQUIRED_RUST_PAYLOAD_STAGES), result
    assert all(len(values) == CHECK_ONLY_ROUNDS for values in result.rust_stage_seconds.values()), result
    assert result.valid_pair_count == CHECK_ONLY_ROUNDS, result
    assert result.validation_passed, result
    assert (
        sum(result.rust_runtime_evidence.issue_type_counts.values())
        == result.rust_runtime_evidence.error_log_count
    ), result
    assert result.rust_median_seconds <= result.python_median_seconds, result
    assert result.verdict == 'VALIDATED', result
```

没有 `skipif`、`pytest.skip`、sample fallback 或可调小 rounds 参数。样本缺失/重复/无效直接 FAIL。

- [ ] **Step 2: Prove both cases are collected before running the long gate**

```powershell
uv run python -m pytest tests/test_rust_check_only_benchmark.py --collect-only -q
```

Expected: 精确收集 `gb`、`sk` 两例；pytest exit 0。0 例、1 例、skipped 或 exit 5 都不构成证据。

- [ ] **Step 3: Run the strict check-only gate and classify failures**

```powershell
uv run python -m pytest tests/test_rust_check_only_benchmark.py -q --basetemp .pytest-tmp/rust-check-only-benchmark
```

分类规则：

- `ETL_MISMATCH`、issue/quality 差异：正确性阻断，回到对应 Phase 1–3 修改；不能进入 Phase 4。
- `INCOMPLETE_EVIDENCE`、样本/hash/round 问题：修复证据采集或重新运行；不能宣称通过。
- `PERFORMANCE_REGRESSION` 且 5 对契约都有效：进入 Step 7 的 profiler 决策。
- 两个 `VALIDATED`：继续运行 full pipeline 和内存门禁。

- [ ] **Step 4: Run the existing full-pipeline workbook benchmark**

```powershell
uv run python -m pytest tests/test_full_rust_cli_benchmark.py -q --basetemp .pytest-tmp/full-rust-benchmark
```

Expected: GB、SK 两例均 `VALIDATED`，没有 skipped。这个 harness 继续使用既有 `repeats=3`；不得改成五轮或放宽 verdict 来适配 check-only harness。

- [ ] **Step 5: Run the final Peak Working Set comparison**

```powershell
cargo build --release --manifest-path rust/Cargo.toml -p costing-calculate
New-Item -ItemType Directory -Force rust/target/perf/final | Out-Null
Copy-Item -LiteralPath rust/target/release/costing-calculate.exe -Destination rust/target/perf/final/costing-calculate.exe -Force
$gbSample = (uv run python -c "from tests.rust_oracle.repo_paths import require_benchmark_sample; print(require_benchmark_sample('gb'))").Trim()
$skSample = (uv run python -c "from tests.rust_oracle.repo_paths import require_benchmark_sample; print(require_benchmark_sample('sk'))").Trim()
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/measure_peak_working_set.ps1 -Pipeline gb -InputPath $gbSample -BaselineExecutable rust/target/perf/baseline/costing-calculate.exe -CurrentExecutable rust/target/perf/final/costing-calculate.exe -ResultPath rust/target/perf/results/peak-working-set-final-gb.json
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/measure_peak_working_set.ps1 -Pipeline sk -InputPath $skSample -BaselineExecutable rust/target/perf/baseline/costing-calculate.exe -CurrentExecutable rust/target/perf/final/costing-calculate.exe -ResultPath rust/target/perf/results/peak-working-set-final-sk.json
$gbFinalPws = Get-Content -LiteralPath rust/target/perf/results/peak-working-set-final-gb.json -Raw | ConvertFrom-Json
$skFinalPws = Get-Content -LiteralPath rust/target/perf/results/peak-working-set-final-sk.json -Raw | ConvertFrom-Json
if ($gbFinalPws.verdict -ne 'VALIDATED' -or $gbFinalPws.current_peak_working_set_bytes.Count -ne 5) { throw 'GB PWS final gate failed' }
if ($skFinalPws.verdict -ne 'VALIDATED' -or $skFinalPws.current_peak_working_set_bytes.Count -ne 5) { throw 'SK PWS final gate failed' }
uv run python -c "from pathlib import Path; from tests.rust_oracle.benchmark import assert_same_input_sha256; root=Path('rust/target/perf/results'); names=lambda p: tuple(root / name.format(p=p) for name in ('check-only-baseline-{p}.json','check-only-final-{p}.json','peak-working-set-baseline-{p}.json','peak-working-set-final-{p}.json','normal-baseline-{p}.json')); [assert_same_input_sha256(names(p)) for p in ('gb','sk')]"
uv run python -c "import json; from pathlib import Path; root=Path('rust/target/perf/results'); load=lambda name: json.loads((root/name).read_text(encoding='utf-8-sig')); rows=[(p,load(f'check-only-baseline-{p}.json'),load(f'normal-baseline-{p}.json'),load(f'peak-working-set-baseline-{p}.json'),load(f'check-only-final-{p}.json'),load(f'peak-working-set-final-{p}.json')) for p in ('gb','sk')]; assert all(len({b['rust_binary_sha256'],n['rust_binary_sha256'],pb['baseline_sha256']}) == 1 and len({f['rust_binary_sha256'],pf['current_sha256']}) == 1 for p,b,n,pb,f,pf in rows), rows"
uv run python -c "from pathlib import Path; from tests.rust_oracle.benchmark import compare_non_target_stage_medians; root=Path('rust/target/perf/results'); results=[compare_non_target_stage_medians(root/f'check-only-baseline-{p}.json',root/f'check-only-final-{p}.json',target_stages=frozenset({'normalize','split','fact','presentation'})) for p in ('gb','sk')]; print(results); assert not any(results), results"
```

Expected: 两份 JSON 都有 baseline/current 各 5 个原始值，`current_to_baseline_ratio <= 1.05`，`verdict = VALIDATED`。后续命令自动比较同一 pipeline 的 baseline/final check-only、baseline/final PWS 和 normal export evidence 五个 input SHA，并分别证明三份 baseline binary hash 相同、两份 final binary hash 相同；任何不同均非零退出并整组重采。

- [ ] **Step 6: Run the full correctness, lint, formatting, and documentation gate**

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --manifest-path rust/Cargo.toml
uv run python -m pytest tests -q --basetemp .pytest-tmp/python-regression
uv run python -m ruff check src tests
uv run python -m ruff format src tests --check
rg -n "cargo (build|run).*costing-calculate|cargo run.*--check-only" README.md AGENTS.md
$missingRelease = Select-String -Path README.md,AGENTS.md -Pattern '^\s*(?:-\s*)?`?cargo (build|run)\b(?!.*--release)'
if ($missingRelease) { $missingRelease; throw 'formal cargo build/run command is missing --release' }
$unexpectedRelease = Select-String -Path README.md,AGENTS.md -Pattern '^\s*(?:-\s*)?`?cargo (test|fmt)\b.*--release'
if ($unexpectedRelease) { $unexpectedRelease; throw 'cargo test/fmt must not use --release' }
git diff --check
```

Expected:

- Rust 全 workspace green；
- Python 全回归 green，性能文件收集且没有 skipped；
- Ruff green；
- README/AGENTS 的正式 build/run/check-only 示例都含 `--release`，test/fmt 命令不加 release；
- 文档说明 dev profile 不作为真实数据性能证据；
- 无 baseline 更新、无新生产依赖、无 `rust/target/perf/**` staged 文件。

- [ ] **Step 7: Run the final independent documentation review**

再次交给未参与 README/AGENTS 修改的 `doc_reviewer` 按 Task 2 Step 6 同一清单只读复审，并额外核对实际 `git diff --name-only b31f1b4..HEAD -- README.md AGENTS.md docs/superpowers` 没有在批准点后意外改写 spec。Expected: `APPROVED`；问题由实现 agent 修复并重跑 Step 6/7。

- [ ] **Step 8: Apply the explicit stop/Phase 4 decision gate**

只有以下全部成立才 `STOP_SUCCESS`：

```text
GB check-only verdict = VALIDATED, valid_pair_count = 5
SK check-only verdict = VALIDATED, valid_pair_count = 5
GB/SK each have exactly 5 values for all 6 required Rust payload stages
GB/SK runtime evidence is stable and error_log_count = sum(issue_type_counts)
GB/SK real normal-mode evidence contains an independent finite export stage
GB full-pipeline verdict = VALIDATED
SK full-pipeline verdict = VALIDATED
GB Peak Working Set verdict = VALIDATED, ratio <= 1.05
SK Peak Working Set verdict = VALIDATED, ratio <= 1.05
Rust/Python/contracts/lint/docs checks = PASS
pytest skipped = 0 for all required performance cases
all cross-file input SHA and same-role binary SHA checks = PASS
independent doc_reviewer verdict = APPROVED
all non-target stage regressions <= 5% or explained and confirmed by a second five-round run
```

如果仅性能或内存硬门禁失败：

1. 先重跑同一硬门禁，确认失败可重复；时间失败用 `capture_cpu_profile.ps1`/WPA CPU sampling，只有 Peak Working Set 失败则用 WPA Memory/Heap、Visual Studio Memory Usage 或能给出 allocation/retained-size 调用栈的等价 profiler；两者都失败时分别采 CPU 与内存证据；
2. 只能从与失败类型对应的 profile 中选择一个有直接证据的最大剩余热点；CPU hotspot 不能替代内存 allocation/retention 证据，反之亦然。没有对应证据时返回 `INCOMPLETE_EVIDENCE` 并重新采样；
3. 创建 `docs/superpowers/plans/2026-07-10-rust-pipeline-performance-phase4-hotspot.md`，并在标题中写明实测符号/调用栈，记录采样证据、唯一修改点、TDD、契约门禁、回滚条件和伪代码；
4. 每份 Phase 4 小计划只允许一个 hotspot，不预先加入 ErrorIssue 紧凑化、并行化、LTO、streaming writer 或 check-only 分流；
5. 实施一个小 commit 后返回 Task 15 Step 3–8，完整重跑时间、full pipeline、内存、正确性和文档门禁；
6. 达标立即停止；仍未达标必须重新 profile，不能连续凭猜测叠加第二项优化。

严禁：放宽 `Rust median <= Python median`、把 missing/skipped 当通过、改变统一 timer 边界、减少 presentation 工作量、更新 oracle baseline、删减 error log 或把内存阈值改大。

- [ ] **Step 9: Commit the final performance gate only after it passes**

```powershell
git add -- tests/test_rust_check_only_benchmark.py
git diff --cached --check
git diff --cached --name-only
git commit -m "test(perf): enforce rust performance gates"
```

Expected staged path 只有 `tests/test_rust_check_only_benchmark.py`。若发生 Phase 4，其计划和对应单热点源码必须使用独立 commit，不混入此 gate commit。

---

## 伪代码草案

### 目标

完成 Phase 1–3 的所有必做优化，在业务和审计契约完全一致的前提下，使 GB、SK 的 Rust release check-only 五轮中位数均不慢于 Python，并保持 Peak Working Set 不超过基线 105%；只有 profiler 证明仍有必要时才进入一次一个热点的 Phase 4。

### 输入

- `gb_sample` / `sk_sample`：`require_benchmark_sample` 严格解析的唯一真实 `.xlsx`；全程校验 SHA-256。
- `baseline_executable`：Phase 0 冻结的普通 Rust release executable。
- `current_executable`：当前阶段普通 Rust release executable。
- `python_oracle`：现有 Python payload 构建路径，仅由 test-only helper 统一边界计时。
- `pipeline_config`：GB/SK 白名单、独立成本项和业务口径。
- `profiler`：WPR/WPA、Visual Studio CPU Usage 或等价 sampling profiler，只用于定位热点。

### 输出

- 成功：三张 Sheet、runtime summary、quality、error log 语义完全一致；GB/SK check-only 与 full-pipeline verdict 均为 `VALIDATED`；两条内存 ratio 均 `<= 1.05`。
- 可重采：计时噪声、样本/hash 变化、round 不完整或 profiler 证据不足；不把它们记为通过。
- 失败：契约回归、错误顺序变化、样本缺失、pytest skipped、性能回退或内存超限；返回明确层级，不静默降级。
- 条件结果：Phase 1–3 后仅性能/内存未达标时，生成并执行一份单热点 Phase 4 小计划，再回到同一硬门禁。

### 顶层实施与停止流程

```text
# [伪代码草案]
# 为什么先冻结基线：后续 executable 和内存必须始终与同一优化前版本比较
stabilize_existing_cli_patch()
align_release_docs_and_payload_timer()
install_test_only_benchmark_and_memory_harnesses()
baseline = freeze_release_baseline(gb_sample, sk_sample, warmup=1, rounds=5)
profile = profile_release(baseline, sk_sample)

# Phase 1–3 是批准后的必做项，不因中间阶段看似达标而提前停止
apply_phase1_ownership_moves()
assert_all_contracts_exact()
measure_phase("phase1")

apply_phase2_indexed_table_vertical_migration()
assert_all_contracts_exact()
measure_phase("phase2")

apply_phase3_typed_amounts_cached_keys_and_unique_indices()
assert_all_contracts_exact()
measure_phase("phase3")

while True:
    gate = run_final_gates(
        pipelines=["gb", "sk"],
        check_only_pairs=5,
        full_pipeline_repeats=3,
        peak_working_set_pairs=5,
    )
    if gate.contract_failure:
        return error_result(gate.failure_layer)  # 正确性问题不能当性能问题处理
    if gate.incomplete_evidence:
        refresh_samples_or_rerun_same_gate()
        continue
    if gate.all_validated and gate.peak_ratio_max <= 1.05:
        return success_result(gate)

    profile = profile_release(current_executable, gate.slowest_pipeline)
    hotspot = require_single_measured_hotspot(profile)
    phase4_plan = write_one_hotspot_plan(hotspot, contracts=gate.contracts)
    implement_one_hotspot(phase4_plan)
    assert_all_contracts_exact()
    # 继续循环，不能跳过任一最终门禁，也不能同时猜第二个热点
```

### Normalize / split 索引化

```rust
// [伪代码草案]
// 输入：RawWorkbook + PipelineConfig + 可选 MonthRange
// 输出：共享一个 ColumnSchema 的 SplitResult；非法列 ID/月份返回 CostingError
fn normalize_and_split(raw: RawWorkbook, config: &PipelineConfig) -> Result<SplitResult, CostingError> {
    let mut table = IndexedTable::from_raw(flatten_headers(&raw.header_rows), raw.rows)?;
    let columns = NormalizeColumns::resolve(table.schema());

    // 为什么先生成完整 mask：predicate 出错时不能留下半过滤表
    table.try_retain_rows(|row| Ok(!is_total_row(row, &columns)?))?;
    forward_fill_with_rules(&mut table, &columns)?;
    derive_or_reuse_month(&mut table, &columns)?;
    derive_or_reuse_filled_cost_item(&mut table, &columns)?;
    apply_month_filter_atomically(&mut table, &columns)?;

    // 为什么只转换 contract 名称：物理重复列保留，但业务 Sheet 固定为唯一列清单
    split_detail_and_qty(NormalizedCostFrame::new(table, fixed_key_columns()))
}
```

### Fact 构建与错误顺序

```rust
// [伪代码草案]
// 输入：SplitResult + PipelineConfig
// 输出：FactBundle；错误严格按 detail、duplicate、reconciliation、unit-cost 顺序
fn build_fact_bundle(split: SplitResult, config: &PipelineConfig) -> Result<FactBundle, CostingError> {
    let mut errors = Vec::new();
    let amounts_by_key = aggregate_detail_in_input_order(&split, config, &mut errors)?;

    let (prepared, key_counts, filter_counts) = prepare_qty_once(split, config)?;

    for row in &prepared {
        if key_counts[&row.work_order_key] > 1 {
            errors.push(duplicate_issue(row)); // 每个重复输入行都保留 issue
        }
    }

    let mut qty_rows = Vec::with_capacity(prepared.len());
    let mut unique_indices = Vec::new();
    let mut seen = HashSet::new();
    for row in prepared {
        let amounts = amounts_by_key
            .get(&row.work_order_key)
            .cloned()
            .unwrap_or_else(|| CostAmounts::new(config.standalone_cost_items.len()));
        let audit = calculate_reconciliation(&amounts, row.completed_total, config);
        append_moh_then_total_issues(&mut errors, &row, &amounts, &audit, config);

        let index = qty_rows.len();
        let first = seen.insert(row.work_order_key.clone());
        qty_rows.push(QtyFactRow::from_prepared(row, amounts, audit));
        if first {
            unique_indices.push(index); // 重复工单只借用首次出现行，不复制整行
        }
    }
    append_non_positive_issues_in_metric_order(&qty_rows, &unique_indices, &mut errors);
    Ok(FactBundle::new(qty_rows, unique_indices, filter_counts, errors))
}
```

### Presentation 所有权流

```rust
// [伪代码草案]
// 输入：拥有所有事实数据的 FactBundle
// 输出：拥有三张 Sheet 和 error log 的 WorkbookPayload
fn present(bundle: FactBundle, config: &PipelineConfig) -> Result<WorkbookPayload, CostingError> {
    // 为什么先借用：quality/anomaly 需要观察事实；完成后才能移动大向量
    let quality = build_quality_metrics(&bundle)?;
    let anomaly_sheet = build_anomaly_sheet(bundle.work_order_rows(), config)?;

    let parts = bundle.into_parts();
    let detail_sheet = project_indexed_rows_by_take(parts.detail_rows, parts.detail_columns)?;
    let qty_sheet = project_typed_qty_rows_by_take(parts.qty_rows, parts.qty_columns, config)?;

    Ok(WorkbookPayload {
        sheet_models: vec![detail_sheet, qty_sheet, anomaly_sheet],
        quality_metrics: quality,
        error_log_count: parts.error_issues.len(),
        error_log: parts.error_issues,
        stage_timings: parts.stage_timings,
    })
}
```

### 风险点 / 边界条件

- 重复源列：物理槽位全部保留；名称查询最后一列生效；业务 contract 列唯一且顺序固定。
- 派生列复用：已有 `月份`/`Filled_成本项目` 覆盖最后同名槽位，不移动 display order；新增列只物理 append。
- schema 安全：foreign/invalid `ColumnId` 返回 `INTERNAL_ERROR`，不 panic、不读错槽位。
- 原子性：派生列 count/schema/row shape 和 retain predicate 全部先验证，再 mutation。
- 金额：全程 `Decimal`；缺失 detail 金额按 0 聚合并保留 `MISSING_AMOUNT`。
- 独立成本项：GB 仅委外、SK 委外+软件；不归制造费用、不进入异常评分，但进入总成本勾稽。
- 重复工单：qty Sheet 保留全部有效行；分析只取首次出现 index；每个重复输入行保留 issue。
- 错误顺序：禁止 HashMap iteration、并行不稳定 merge、排序、抽样、去重或只保留计数。
- check-only：继续完整构建 presentation，只排除 export/结果序列化；不得另开偷减工作量的快路径。
- 性能证据：release only；GB/SK 各 5 个有效成对 round；missing/skipped/uncollected 一律失败。
- 内存证据：baseline/current 必须是直接 executable、同 sample SHA、各 5 个成对 round；ratio 上限 1.05。
- 工作区：每次只 stage 任务列出的路径；保留用户已有修改；生成物全部留在 ignored `rust/target/perf/`。

---

## Plan Self-Review Checklist

- [x] Approved spec 的 Phase 0、Phase 1、Phase 2、Phase 3 和 conditional Phase 4 均有唯一对应任务。
- [x] Rust/Python `payload_total_seconds` 使用同一边界；没有把 Python stage sum 当 wall-clock。
- [x] GB/SK check-only 各固定 5 对，full pipeline 保持现有 3 次，Peak Working Set 各固定 5 对。
- [x] 样本缺失、重复、invalid、pytest skipped/未收集均明确失败。
- [x] 六个 required Rust stage 各保存五值；runtime/run counts/error/quality/真实 export 证据均持久化。
- [x] PWS 保存命令参数/工作目录，baseline/final input 与同角色 binary SHA 有自动交叉断言。
- [x] 所有新类型都有最终定义和删除点；`IndexedFactRow` 只存在 Task 12–13，Task 14 原子删除。
- [x] `SchemaId` 不进入逻辑 equality/serialization；foreign ID 有 error test。
- [x] 重复列最后生效、物理槽位保留、display contract 唯一三者没有冲突。
- [x] `HashMap` 仅 lookup，输出和 issue 顺序由输入 `Vec`/固定 metric 数组决定。
- [x] `work_order_rows` 完整副本最终由 `unique_work_order_indices` 替代，首次出现语义有测试。
- [x] Phase 1–3 都有 Rust tests、oracle/contracts、release diagnostics 和独立 commit。
- [x] Phase 4 没有猜测性预实现，只允许与失败类型匹配的 profiler 证明一个热点。
- [x] README/AGENTS release 示例和 dev-profile 警告纳入正/负向验证，并有独立 doc review；test/fmt 命令保持 dev。
- [x] 未增加生产依赖、LTO、Polars/Arrow/interner、streaming writer 或 check-only 分流。
- [x] 每个非平凡函数的输入、输出、错误和关键边界已在 task 或伪代码中明确。
- [x] 所有 commit 命令都使用显式 pathspec，避免误提交无关脏工作区。

---

## Execution Handoff

实施必须从 Task 1 顺序执行到 Task 15；Task 10–12 的 IndexedTable 迁移存在类型依赖，不允许拆成并行旧/新生产管线。可并行的仅是只读 review、独立测试审查和 profiler 结果解读，不能让多个实现 agent 同时修改 `model.rs`/`fact.rs`/`presentation.rs`。

推荐每完成一个 commit 就进行一次只读 Rust review；Phase 1–3 各自完成后再追加 data auditor 契约审查。Task 2 文档修改后及 Task 15 最终验收前必须由未参与修改的 `doc_reviewer` 只读审查，reviewer 不直接改文档。任一 review 发现 correctness、文档或审计顺序问题，先由实现 agent 修复并重跑该阶段 gate，再进入下一任务。

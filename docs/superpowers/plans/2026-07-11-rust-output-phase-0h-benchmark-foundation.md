# Rust 输出 Phase 0H 基准、Oracle 与证据安全实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在采集任何正式 Phase 0A 性能基线前，建立可执行、fail-closed 的 Rust reference/candidate normal-mode wall/PWS 同批协议、逐单元格 OOXML workbook oracle 和全类型版本化证据脱敏边界。

**Architecture:** Python 负责闭合 benchmark 协议、runtime/workbook 校验、扩样裁决和证据生成；PowerShell 只负责 Windows 子进程及 `PeakWorkingSet64` 采样，并把原始结果写入 ignored 本地目录。所有版本化 artifact 都由 `EvidenceSanitizer` 从 typed input 重建；任何运行、校验、清理或敏感扫描失败都不会留下可提交证据。

**Tech Stack:** Python 3.11、pytest、PowerShell 7/Windows PowerShell、`subprocess`、`zipfile`、`xml.etree.ElementTree`、`decimal.Decimal`、现有 `python-calamine`/`xlsxwriter` 测试依赖、Rust release EXE。

## Global Constraints

- 本阶段只改 `tests/rust_oracle/` 和 `docs/performance/README.md`；不改 `src/` 或 `rust/crates/*/src/`。
- 不恢复已移除的 `scripts/` 目录；所有入口固定为 `python -m tests.rust_oracle.phase0_harness` 或现有 PowerShell 文件。
- normal wall 和 normal PWS 均比较固定 Rust reference 与 Rust candidate；Python 只生成 oracle，不作为性能分母。
- 初始组固定 global round 1–5；任一 time/PWS 指标进入 3% 临界区时，wall 和 PWS 都强制追加 global round 6–10。
- global round 1/3/5/7/9 固定 reference → candidate；2/4/6/8/10 固定 candidate → reference。
- 追加组不得重置本地奇偶；不得依据首组“暂时通过”或“暂时失败”选择是否扩样。
- 所有性能比必须在同一 `batch_id` 内满足 pipeline、input SHA、binary SHA、Git HEAD/repository-state SHA、`N` 和轮次顺序完全一致。
- reference 失败使整批无效；candidate 失败明确返回 `CANDIDATE_FAILED`；缺轮、重复轮、SHA 漂移和清理失败全部 fail closed。
- `PeakWorkingSet64` 是 Windows 内核维护的进程峰值；50 ms 只用于刷新进程对象，不把它描述为瞬时 RSS 采样。
- workbook 数值从 worksheet XML 的 `<v>` 直接解析为 `Decimal`；不得经过 `float`，不得保留统一 epsilon。
- mismatch 只允许保存 Sheet、坐标、错误种类和存储类型；禁止保存真实 expected/actual 单元格值。
- 原始 stdout/stderr、真实命令、绝对路径、ERP basename、机器用户名/主机名和 workbook 只能进入 ignored 本地目录。
- Phase -1D Task 6 的 `evidence.py/test_evidence.py`、dependency manifest 和 revision consistency gate 全通过是 Task 1 的阻塞前置；缺少时立即停止。
- formal phase 的第一批要求 execution worktree clean。后续 pipeline/edge 批次只允许前序 `EvidenceSanitizer` create-new 生成的同阶段 `docs/performance` JSON；每个文件必须重新过 schema/敏感扫描并进入 repository-state SHA。任何代码、配置、测试、普通文档、staged change 或未知 untracked path 都拒绝。
- Phase 0H 实现并测试稳定 `capture_phase0a()` API/CLI，但只用脱敏 raw-input fixture 做 smoke；后续 writer 子计划仅调用冻结入口采正式 Phase 0A，不重复实现。

---

## File Structure

### Create

- `tests/rust_oracle/benchmark_protocol.py` — 闭合 profile、轮次计划、same-batch 校验、扩样与 verdict。
- `tests/rust_oracle/phase0_harness.py` — normal wall/PWS 编排、CLI、finally cleanup。
- `tests/rust_oracle/sanitized_fixture.py` — 不含 ERP 数据、可被 GB/SK CLI 读取并生成三张输出 Sheet 的单 Sheet raw-input fixture。
- `tests/rust_oracle/test_benchmark_protocol.py`
- `tests/rust_oracle/test_phase0_harness.py`
- `tests/rust_oracle/test_peak_working_set.py`
- `docs/performance/README.md` — raw/versioned evidence 边界和操作顺序。

### Extend

- `tests/rust_oracle/evidence.py` — Phase -1D 已创建；扩展为所有 artifact kind 的 `EvidenceSanitizer` 和 scan CLI。
- `tests/rust_oracle/test_evidence.py`
- `tests/rust_oracle/measure_peak_working_set.ps1`
- `tests/rust_oracle/workbook_compare.py`
- `tests/rust_oracle/oracle_runner.py`
- `tests/rust_oracle/benchmark.py`
- `tests/rust_oracle/test_workbook_compare.py`
- `tests/rust_oracle/test_oracle_runner.py`
- `tests/rust_oracle/test_benchmark.py`
- `tests/test_rust_check_only_benchmark.py`
- `tests/test_full_rust_cli_benchmark.py`
- `tests/test_full_rust_cli_oracle.py`

### Do not modify

- `tests/contracts/baselines/` — 高层业务 baseline 不承载 XML oracle。
- `data/raw/`、任何真实 ERP workbook、Rust 生产代码和 Python legacy/oracle 生产实现。

## Stable Interfaces

`benchmark_protocol.py` 的闭合接口：

```python
PipelineName = Literal['gb', 'sk']
BinaryRole = Literal['reference', 'candidate']
MetricName = Literal['wall', 'pws']

class RuntimeSchema(StrEnum):
    BASE = 'base'
    INSTRUMENTED = 'instrumented'
    READER_INSTRUMENTED = 'reader-instrumented'

class ClosedBinaryLabel(StrEnum):
    PHASE0A = 'phase0a'
    PHASE0B = 'phase0b'
    PHASE1 = 'phase1'
    PHASE2_A = 'phase2-a'
    PHASE2_B = 'phase2-b'
    PHASE2_C = 'phase2-c'
    PHASE2_D = 'phase2-d'
    PHASE3 = 'phase3'
    LOW_MEMORY_DEFAULT = 'low-memory-default'
    LOW_MEMORY_ZLIB = 'low-memory-zlib'
    LOW_MEMORY_ZMIJ = 'low-memory-zmij'
    LOW_MEMORY_ZLIB_ZMIJ = 'low-memory-zlib-zmij'
    PHASE4 = 'phase4'
    PHASE5 = 'phase5'

class ComparisonProfile(StrEnum):
    PHASE0B_VS_PHASE0A = 'phase0b-vs-phase0a'
    PHASE1_VS_PHASE0B = 'phase1-vs-phase0b'
    PHASE1_VS_PHASE0A = 'phase1-vs-phase0a'
    PHASE2_B_VS_A = 'phase2-b-vs-a'
    PHASE2_C_VS_A = 'phase2-c-vs-a'
    PHASE2_D_VS_C = 'phase2-d-vs-c'
    PHASE2_D_VS_B = 'phase2-d-vs-b'
    PHASE2_B_VS_C = 'phase2-b-vs-c'
    PHASE2_SELECTED_VS_PHASE0A = 'phase2-selected-vs-phase0a'
    PHASE3_VS_PHASE0A = 'phase3-vs-phase0a'
    PHASE3_ZLIB_ON_VS_OFF = 'phase3-zlib-on-vs-off'
    PHASE3_ZMIJ_ON_VS_OFF = 'phase3-zmij-on-vs-off'
    PHASE4_VS_PHASE3 = 'phase4-vs-phase3'
    PHASE4_VS_PHASE0A = 'phase4-vs-phase0a'
    PHASE5_VS_PHASE0A = 'phase5-vs-phase0a'

class HarnessVerdict(StrEnum):
    VALIDATED = 'VALIDATED'
    CANDIDATE_FAILED = 'CANDIDATE_FAILED'
    REFERENCE_FAILED = 'REFERENCE_FAILED'
    CORRECTNESS_FAILED = 'CORRECTNESS_FAILED'
    INCOMPLETE_EVIDENCE = 'INCOMPLETE_EVIDENCE'
    ENVIRONMENT_DRIFT = 'ENVIRONMENT_DRIFT'
    INCONCLUSIVE = 'INCONCLUSIVE'
    CLEANUP_FAILED = 'CLEANUP_FAILED'
    SENSITIVE_EVIDENCE = 'SENSITIVE_EVIDENCE'

class AttemptState(StrEnum):
    CREATED = 'CREATED'
    FIRST_GROUP_COMPLETE = 'FIRST_GROUP_COMPLETE'
    EXPANDED_GROUP_COMPLETE = 'EXPANDED_GROUP_COMPLETE'
    CLEANUP_COMPLETE = 'CLEANUP_COMPLETE'
    EVIDENCE_COMMITTED = 'EVIDENCE_COMMITTED'
    FAILED = 'FAILED'

@dataclass(frozen=True)
class MachineEvidence:
    windows_build: str
    architecture: Literal['x86_64']
    cpu_model: str
    logical_cpu_count: int
    physical_memory_bytes: int
    system_drive_media_type: Literal['SSD', 'HDD', 'UNKNOWN']
    system_drive_size_bytes: int
    fingerprint_sha256: str

@dataclass(frozen=True)
class RuntimeEvidence:
    pipeline: PipelineName
    output_written: bool
    request_id_present: bool
    sheet_count: int
    error_log_count: int
    issue_type_counts: tuple[tuple[str, int], ...]
    quality_metrics: tuple[tuple[str, str, str], ...]
    run_counts: tuple[tuple[str, int], ...]
    stage_timings: tuple[tuple[str, Decimal], ...]
    output_size_bytes: int | None
    sheet_dimensions: tuple[str, ...]
    reader_snapshot_sha256: str

@dataclass(frozen=True)
class NormalRunEvidence:
    external_wall_seconds: Decimal
    peak_working_set_bytes: int | None
    runtime: RuntimeEvidence
    workbook_oracle_sha256: str

@dataclass(frozen=True)
class RoundPlan:
    global_round: int
    order: tuple[BinaryRole, BinaryRole]

@dataclass(frozen=True)
class MetricSample:
    role: BinaryRole
    global_round: int
    metric_value: Decimal
    exit_code: int
    input_sha256: str
    binary_sha256: str
    git_head: str
    repository_state_sha256: str
    machine_fingerprint_sha256: str
    local_unversioned_log_sha256: str
    normal_run: NormalRunEvidence

@dataclass(frozen=True)
class PairedRound:
    plan: RoundPlan
    reference: MetricSample
    candidate: MetricSample

@dataclass(frozen=True)
class MetricGroup:
    batch_id: str
    pipeline: PipelineName
    metric: MetricName
    global_round_start: Literal[1, 6]
    rounds: tuple[PairedRound, ...]

@dataclass(frozen=True)
class CalibrationRound:
    global_round: int
    reference: MetricSample

@dataclass(frozen=True)
class CalibrationGroup:
    batch_id: str
    pipeline: PipelineName
    metric: MetricName
    warmup_succeeded: bool
    rounds: tuple[CalibrationRound, ...]

@dataclass(frozen=True)
class BatchAttempt:
    comparison_key: str
    batch_id: str
    attempt_number: int
    state: AttemptState
    previous_attempt_head_sha256: str | None
    first_group_sha256: str | None
    expanded_group_sha256: str | None
    ledger_head_sha256: str
    attempt_directory: Path

@dataclass(frozen=True)
class PairedBenchmarkResult:
    wall: MetricGroup | None
    pws: MetricGroup | None
    attempt: BatchAttempt
    verdict: HarnessVerdict

@dataclass(frozen=True)
class Phase0AManifest:
    reference_exe_sha256: str
    fork_revision: str
    git_head: str
    machine: MachineEvidence
    gb_wall: CalibrationGroup
    gb_pws: CalibrationGroup
    sk_wall: CalibrationGroup
    sk_pws: CalibrationGroup

build_round_plan(*, global_round_start: Literal[1, 6], round_count: Literal[5]) -> tuple[RoundPlan, ...]
validate_metric_group(group: MetricGroup) -> None
validate_calibration_group(group: CalibrationGroup) -> None
merge_metric_groups(first: MetricGroup, second: MetricGroup) -> MetricGroup
requires_mandatory_expansion(*, measured: Decimal, limit: Decimal) -> bool
groups_have_conflicting_direction(first: MetricGroup, second: MetricGroup) -> bool
assert_same_batch_ratio(group: MetricGroup) -> None
assert_same_benchmark_batch(wall: MetricGroup, pws: MetricGroup) -> None
assert_environment_not_drifted(current: MetricGroup, phase0a: Phase0AManifest) -> None
```

`requires_mandatory_expansion()` uses `Decimal(str(raw_value))` and exact `Decimal('0.03')`. `build_round_plan()` 每次只创建五轮；`global_round_start=1` 或 `6` 决定全局奇偶。合并后 `MetricGroup` 必须恰好有 10 个连续 global rounds。

`phase0_harness.py` 的稳定入口：

```python
@dataclass(frozen=True)
class PairedBenchmarkRequest:
    pipeline: PipelineName
    input_path: Path
    reference_executable: Path
    candidate_executable: Path
    reference_label: ClosedBinaryLabel
    candidate_label: ClosedBinaryLabel
    comparison_profile: ComparisonProfile
    phase0a_manifest: Path
    local_root: Path
    evidence_path: Path
    attempt_ledger_root: Path

@dataclass(frozen=True)
class MetricGroupRequest:
    benchmark: PairedBenchmarkRequest
    batch_id: str
    metric: MetricName
    plans: tuple[RoundPlan, ...]
    attempt_directory: Path

@dataclass(frozen=True)
class Phase0HSmokeRequest:
    pipeline: PipelineName
    reference_executable: Path
    candidate_executable: Path
    local_root: Path

@dataclass(frozen=True)
class Phase0HSmokeResult:
    batch_id: str
    fixture_sha256: str
    verdict: HarnessVerdict

@dataclass(frozen=True)
class Phase0ARequest:
    gb_input_path: Path
    sk_input_path: Path
    reference_executable: Path
    fork_revision: str
    local_root: Path
    output_path: Path

run_normal_wall_group(request: MetricGroupRequest) -> MetricGroup
run_pws_group(request: MetricGroupRequest) -> MetricGroup
run_paired_normal_batch(request: PairedBenchmarkRequest) -> PairedBenchmarkResult
run_phase0h_smoke(request: Phase0HSmokeRequest) -> Phase0HSmokeResult
capture_phase0a(request: Phase0ARequest) -> Phase0AManifest
main(argv: list[str] | None = None) -> int
```

`MetricGroupRequest` is the only group-level form；`batch_id` and plans are never also passed as free function arguments.

### Closed profile gate table

Every profile first requires runtime/workbook correctness, fixed input/binary SHA, clean Git state, finite positive samples, and candidate bytes no more than `1.10` times approved Phase 0A bytes. Additional gates are exact:

| Profile | Pipeline | Reference schema | Candidate schema | Same-batch performance gate |
|---|---|---|---|---|
| `phase0b-vs-phase0a` | GB/SK | BASE | INSTRUMENTED | wall `<=1.02` |
| `phase1-vs-phase0b` | SK | INSTRUMENTED | INSTRUMENTED | writer_populate `<=0.90`; xlsx_save `<=1.05` |
| `phase1-vs-phase0a` | GB | BASE | INSTRUMENTED | wall/PWS `<=1.05` |
| `phase1-vs-phase0a` | SK | BASE | INSTRUMENTED | PWS `<=1.05` |
| `phase2-b-vs-a` | SK | INSTRUMENTED | INSTRUMENTED | xlsx_save `<=0.85` |
| `phase2-c-vs-a` | SK | INSTRUMENTED | INSTRUMENTED | writer_populate or export `<=0.97`; used metric wins at least 4/5 per group |
| `phase2-d-vs-c` | SK | INSTRUMENTED | INSTRUMENTED | xlsx_save `<=0.85` |
| `phase2-d-vs-b` | SK | INSTRUMENTED | INSTRUMENTED | writer_populate or export `<=0.97`; used metric wins at least 4/5 per group |
| `phase2-b-vs-c` | SK | INSTRUMENTED | INSTRUMENTED | approved wall, then PWS, then C tie-break |
| `phase2-selected-vs-phase0a` | GB | BASE | INSTRUMENTED | wall/PWS `<=1.05` |
| `phase2-selected-vs-phase0a` | SK | BASE | INSTRUMENTED | correctness/bytes; feature gain comes only from internal profile |
| `phase3-zlib-on-vs-off` | SK | INSTRUMENTED | INSTRUMENTED | xlsx_save `<=0.85` |
| `phase3-zmij-on-vs-off` | SK | INSTRUMENTED | INSTRUMENTED | writer_populate or export `<=0.97`; used metric wins at least 4/5 per group |
| `phase3-vs-phase0a` | GB | BASE | INSTRUMENTED | wall/PWS `<=1.05` |
| `phase3-vs-phase0a` | SK | BASE | INSTRUMENTED | absolute PWS `<=2,147,483,648` |
| `phase4-vs-phase3` | SK | INSTRUMENTED | READER_INSTRUMENTED | ingest or PWS `<=0.90`; wall `<=1.00` |
| `phase4-vs-phase3` | GB | INSTRUMENTED | READER_INSTRUMENTED | ingest/PWS `<=1.05` |
| `phase4-vs-phase0a` | GB | BASE | READER_INSTRUMENTED | wall/PWS `<=1.05` |
| `phase4-vs-phase0a` | SK | BASE | READER_INSTRUMENTED | correctness/bytes |
| `phase5-vs-phase0a` | GB | BASE | READER_INSTRUMENTED | wall/PWS `<=1.05` |
| `phase5-vs-phase0a` | SK | BASE | READER_INSTRUMENTED | wall `<=20.0s`; PWS `<=2,147,483,648` |

`RuntimeSchema.BASE` requires `ingest/normalize/split/fact/presentation/total/export` and forbids writer substage keys. `INSTRUMENTED` additionally requires `writer_populate/xlsx_save` and non-null external output size. `READER_INSTRUMENTED` adds exact reader snapshot/row-count validation; Phase 4's metric is still the existing `ingest` stage.

### Append-only attempt ledger

The formal `paired` CLI owns first group, expansion decision and second group. It does not expose `batch_id`, global-round start, group size or numeric thresholds. It derives `comparison_key` from profile, pipeline, input SHA, reference/candidate SHA, Git HEAD, machine fingerprint and repository-state SHA, then creates an append-only attempt below `rust/target/perf-local/batches/` with create-new semantics.

- Round, local result and group files cannot be overwritten; each record includes the previous record SHA.
- The first group SHA is committed before expansion is evaluated; the second group references that exact SHA.
- An interrupted attempt may resume only missing samples and cannot rerun an existing `(metric, global_round, role)`.
- A new attempt with the same candidate SHA is allowed only after `ENVIRONMENT_DRIFT` or `REFERENCE_FAILED`, links the prior ledger head, and uses closed reason `ENVIRONMENT_RECOVERED`.
- Candidate/correctness/gate failure or `INCONCLUSIVE` cannot be retried with the same candidate SHA.
- Evidence contains attempt count, all prior safe verdicts, first/second group SHAs and final ledger head.

`assert_same_benchmark_batch(wall, pws)` validates equal batch ID, pipeline, input SHA, reference/candidate SHA, machine fingerprint, formal `N` and global rounds. `assert_environment_not_drifted()` rejects a machine fingerprint change or `abs(current_reference_median / phase0a_reference_median - 1) > Decimal('0.10')`; exactly 10% is allowed.

所有 output workbook 位于：

```text
data/processed/{pipeline}/.perf-runs/{batch_id}/{metric}/{binary_sha256}/{global_round}/{role}.xlsx
```

花括号表示由 typed request 生成的运行期字段，不是调用者可传的任意路径片段。

## Task 1: Implement the Global AB/BA and Same-Batch Protocol

**Files:**
- Create: `tests/rust_oracle/benchmark_protocol.py`
- Create: `tests/rust_oracle/test_benchmark_protocol.py`

**Interfaces:**
- Produces: closed profiles/labels/verdicts、`RoundPlan`、`MetricGroup`、mandatory expansion、same-batch validator。
- Does not consume: workbook、子进程或真实输入。

- [ ] **Step 1: Write RED tests for global ordering and fail-closed evidence**

Add these exact tests:

```text
test_round_plan_uses_global_reference_candidate_order_for_rounds_one_to_ten
test_append_group_starts_at_global_round_six
test_validate_group_rejects_missing_round
test_validate_group_rejects_duplicate_round
test_validate_group_rejects_unbalanced_order
test_validate_group_rejects_binary_sha_change
test_validate_group_rejects_input_or_git_drift
test_same_batch_ratio_rejects_different_batch_id_n_or_round_order
test_wall_and_pws_must_share_batch_id
test_wall_and_pws_must_share_n_and_global_rounds
test_wall_and_pws_must_share_input_and_binary_hashes
test_wall_and_pws_must_share_machine_fingerprint
test_calibration_group_requires_five_reference_only_rounds
```

Run:

```powershell
uv run python -m pytest tests/rust_oracle/test_benchmark_protocol.py -q --basetemp .pytest-tmp/phase0h-protocol
```

Expected: collection/import FAIL because the protocol module does not exist.

- [ ] **Step 2: Write RED tests for mandatory expansion and inconclusive groups**

Add:

```text
test_mandatory_expansion_is_false_outside_three_percent_boundary
test_mandatory_expansion_includes_exact_lower_and_upper_boundaries
test_mandatory_expansion_applies_when_first_group_temporarily_passes
test_mandatory_expansion_applies_when_first_group_temporarily_fails
test_conflicting_five_round_groups_are_inconclusive
test_non_conflicting_groups_merge_to_global_rounds_one_through_ten
test_phase1_profile_uses_writer_populate_and_xlsx_save_from_same_samples
test_zmij_profiles_require_four_of_five_wins_in_each_group
test_phase4_profile_uses_ingest_and_pws_from_same_batch
test_output_bytes_uses_approved_phase0a_value
```

The test values are synthetic `Decimal` strings；use limit `Decimal('1.0')` with measured values `Decimal('0.9699')`, `Decimal('0.97')`, `Decimal('1.03')`, and `Decimal('1.0301')` to cover the exact formula `abs(measured / limit - 1) <= Decimal('0.03')` without binary-float boundary drift.

Add environment tests:

```text
test_environment_drift_rejects_changed_machine_fingerprint
test_environment_drift_rejects_reference_median_over_ten_percent
test_environment_drift_accepts_exactly_ten_percent
```

- [ ] **Step 3: Implement only the pure protocol**

Implement the stable interfaces and the complete profile table above. Put every numeric gate in a closed `COMPARISON_LIMITS` mapping keyed by `ComparisonProfile`; the CLI must not accept arbitrary limits. Validate finite positive performance samples before computing medians. Implement calibration validation separately from paired validation.

- [ ] **Step 4: Verify and commit**

```powershell
uv run python -m pytest tests/rust_oracle/test_benchmark_protocol.py -q --basetemp .pytest-tmp/phase0h-protocol
uv run python -m ruff check tests/rust_oracle/benchmark_protocol.py tests/rust_oracle/test_benchmark_protocol.py
uv run python -m ruff format tests/rust_oracle/benchmark_protocol.py tests/rust_oracle/test_benchmark_protocol.py --check
git add -- tests/rust_oracle/benchmark_protocol.py tests/rust_oracle/test_benchmark_protocol.py
git diff --cached --name-only
git diff --cached --check
git commit -m "test(perf): add paired benchmark protocol"
```

Expected: all tests pass; cached paths are exactly the two listed files.

## Task 2: Add the Normal Wall Runner and Runtime-Schema Boundary

**Files:**
- Create: `tests/rust_oracle/phase0_harness.py`
- Create: `tests/rust_oracle/test_phase0_harness.py`
- Modify: `tests/rust_oracle/oracle_runner.py`
- Modify: `tests/rust_oracle/benchmark.py`
- Modify: `tests/rust_oracle/test_oracle_runner.py`
- Modify: `tests/rust_oracle/test_benchmark.py`

**Interfaces:**
- Consumes: Task 1 protocol。
- Produces: `NormalRunEvidence`、profile-aware runtime parser、normal wall group、append-only attempt ledger and unique batch/metric output paths。

- [ ] **Step 1: Add RED runtime-schema tests**

Add these exact tests:

```text
test_base_runtime_schema_does_not_require_writer_breakdown
test_instrumented_runtime_schema_requires_writer_populate_xlsx_save_and_output_size
test_reader_instrumented_schema_requires_ingest_and_reader_contract
test_runtime_parser_rejects_non_finite_or_negative_timings
test_runtime_parser_rejects_unexpected_workbook_path_in_check_only
```

Run:

```powershell
uv run python -m pytest tests/rust_oracle/test_oracle_runner.py -q --basetemp .pytest-tmp/phase0h-runtime
```

Expected: FAIL until `parse_runtime_payload(payload, schema=...)` exists.

- [ ] **Step 2: Add RED normal-runner tests**

Add:

```text
test_normal_wall_group_omits_check_only_and_uses_unique_outputs
test_normal_wall_group_runs_global_ab_ba_order
test_candidate_nonzero_rejects_candidate
test_reference_nonzero_invalidates_whole_batch
test_wall_group_rejects_input_reference_candidate_or_git_drift
test_wall_group_deletes_workbooks_on_success
test_wall_group_deletes_workbooks_after_process_or_oracle_failure
test_cleanup_failure_prevents_versionable_evidence
test_first_formal_batch_requires_clean_worktree
test_formal_batch_rejects_non_evidence_worktree_change
test_later_batch_accepts_only_create_new_sanitized_prior_evidence
test_prior_evidence_content_change_invalidates_repository_state
test_batch_id_is_derived_and_cannot_be_supplied
test_second_round_one_to_five_attempt_is_rejected
test_existing_round_record_cannot_be_overwritten
test_expanded_group_requires_original_first_group_sha
test_pws_only_resample_is_rejected
test_interrupted_attempt_resumes_only_missing_samples
test_failed_candidate_sha_cannot_be_retried
test_environment_recovery_attempt_links_previous_ledger_head
```

Use fake executables/subprocess adapters and tiny sanitized workbooks; no real ERP or release build belongs in unit tests.

- [ ] **Step 3: Implement typed capture and finally cleanup**

Replace `capture_rust_normal_benchmark_evidence()` with `run_rust_normal_captured()`. The new function returns typed data only and never accepts a versioned evidence path. Keep raw stdout/stderr in `rust/target/perf-local/` and retain only their SHA-256 in typed results.

In `benchmark.py`, rename the raw check-only writer to `write_local_check_only_result()` and reject destinations outside canonical, non-reparse-point children of `rust/target/` or `data/processed/`.

Implement the append-only attempt state machine described above with create-new files and a SHA-256 hash chain. Formal entry refuses a dirty worktree. The wall group records the planned output before process launch; after each process it immediately validates runtime and workbook, then deletes that one workbook before starting the next role. The outer `finally` retries cleanup from the full path ledger without overwriting the primary verdict.

- [ ] **Step 4: Verify and commit**

```powershell
uv run python -m pytest tests/rust_oracle/test_phase0_harness.py tests/rust_oracle/test_oracle_runner.py tests/rust_oracle/test_benchmark.py -q --basetemp .pytest-tmp/phase0h-wall
uv run python -m ruff check tests/rust_oracle
uv run python -m ruff format tests/rust_oracle --check
git add -- tests/rust_oracle/phase0_harness.py tests/rust_oracle/test_phase0_harness.py tests/rust_oracle/oracle_runner.py tests/rust_oracle/benchmark.py tests/rust_oracle/test_oracle_runner.py tests/rust_oracle/test_benchmark.py
git diff --cached --name-only
git diff --cached --check
git commit -m "test(perf): add normal wall benchmark runner"
```

## Task 3: Extend Peak Working Set to Normal N=5/10

**Files:**
- Modify: `tests/rust_oracle/measure_peak_working_set.ps1`
- Create: `tests/rust_oracle/test_peak_working_set.py`
- Modify: `tests/rust_oracle/phase0_harness.py`
- Modify: `tests/rust_oracle/test_phase0_harness.py`

**Interfaces:**
- Consumes: `RoundPlan` and the same batch identity as wall runs。
- Produces: one raw local PWS sample per PowerShell invocation using `Process.PeakWorkingSet64`; Python assembles the `MetricGroup` and never writes raw results to `docs/performance/`。

- [ ] **Step 1: Add RED command-contract tests**

The Python tests inspect commands and parse synthetic PowerShell output. Add:

```text
test_pws_normal_command_does_not_include_check_only
test_pws_check_only_command_includes_check_only
test_pws_single_sample_accepts_only_one_global_round_and_role
test_python_pws_group_preserves_rounds_six_to_ten_global_order
test_pws_local_result_must_be_under_ignored_root
test_pws_script_parses_with_powershell
test_pws_single_sample_quotes_space_and_chinese_paths
test_pws_single_sample_smoke_reports_positive_peak
```

Run:

```powershell
uv run python -m pytest tests/rust_oracle/test_peak_working_set.py -q --basetemp .pytest-tmp/phase0h-pws
```

Expected: FAIL because the current script hard-codes five check-only rounds.

- [ ] **Step 2: Refactor PowerShell to one process sample per invocation**

Use this closed parameter surface:

```powershell
param(
    [ValidateSet('Normal', 'CheckOnly')] [string] $Mode,
    [ValidateSet('gb', 'sk')] [string] $Pipeline,
    [string] $InputPath,
    [string] $Executable,
    [ValidateSet('reference', 'candidate')] [string] $Role,
    [ValidatePattern('^[0-9a-f]{64}$')] [string] $BatchId,
    [ValidateRange(1, 10)] [int] $GlobalRound,
    [string] $OutputPath,
    [string] $LocalLogRoot,
    [string] $LocalResultPath
)
```

The formal Python CLI derives `BatchId/GlobalRound/Role/OutputPath`; users cannot pass them through `paired`. PowerShell measures exactly one child process. Normal mode passes its one output and omits `--check-only`.

PowerShell starts the stopwatch and child, refreshes every 50 ms, reads final `PeakWorkingSet64`, waits for exit, hashes local logs and writes one create-new local result. After it returns, Python immediately parses runtime, runs the hardened workbook oracle, removes that workbook in an inner `finally`, and only then starts the next role. A PowerShell failure is safe because Python already registered the exact output path in its cleanup ledger.

- [ ] **Step 3: Parse the script and run one sanitized child smoke**

```powershell
powershell -NoProfile -Command "[void][scriptblock]::Create((Get-Content -Raw -Encoding UTF8 'tests/rust_oracle/measure_peak_working_set.ps1'))"
uv run python -m pytest tests/rust_oracle/test_peak_working_set.py -q --basetemp .pytest-tmp/phase0h-pws-smoke -k "script or single_sample"
```

Expected: PowerShell syntax succeeds; the sanitized fake child exits 0 and reports positive `PeakWorkingSet64` even when its paths contain spaces and Chinese characters.

- [ ] **Step 4: Add fail-closed PWS parse and cleanup tests**

Add:

```text
test_pws_group_rejects_reference_nonzero
test_pws_group_rejects_candidate_nonzero
test_pws_group_rejects_missing_duplicate_or_unbalanced_rounds
test_pws_group_rejects_sha_or_git_drift
test_pws_normal_outputs_are_unique_and_removed
test_pws_cleanup_failure_deletes_batch_evidence
test_paired_batch_expands_wall_and_pws_together
test_wall_and_pws_groups_share_attempt_batch_and_n
```

- [ ] **Step 5: Verify and commit**

```powershell
uv run python -m pytest tests/rust_oracle/test_peak_working_set.py tests/rust_oracle/test_phase0_harness.py -q --basetemp .pytest-tmp/phase0h-pws
git add -- tests/rust_oracle/measure_peak_working_set.ps1 tests/rust_oracle/test_peak_working_set.py tests/rust_oracle/phase0_harness.py tests/rust_oracle/test_phase0_harness.py
git diff --cached --name-only
git diff --cached --check
git commit -m "test(perf): measure normal peak working set"
```

## Task 4: Harden the Workbook Oracle at OOXML Level

**Files:**
- Modify: `tests/rust_oracle/workbook_compare.py`
- Modify: `tests/rust_oracle/test_workbook_compare.py`
- Modify: `tests/rust_oracle/benchmark.py`
- Modify: `tests/test_full_rust_cli_oracle.py`
- Modify: `tests/test_full_rust_cli_benchmark.py`

**Interfaces:**
- Produces: `WorkbookComparisonReport` and value-free `WorkbookMismatch`。
- Preserves: Sheet order/dimensions、freeze panes、filter、column width/number format、header/conditional format existing checks。
- Fixes the public call as `compare_workbooks(expected_path: Path, actual_path: Path, *, pipeline: PipelineName) -> WorkbookComparisonReport`; every caller must pass `gb` or `sk` explicitly。

- [ ] **Step 1: Add RED Decimal and storage-type tests**

Add:

```text
test_decimal_lexical_difference_of_one_e_minus_seven_is_rejected
test_equivalent_decimal_lexemes_and_signed_zero_are_equal
test_numeric_string_is_not_equal_to_numeric_cell
test_decimal_column_total_mismatch_is_rejected
test_grouped_work_order_total_mismatch_is_rejected
```

Generate tiny `.xlsx` fixtures with existing `xlsxwriter`, then patch only the test ZIP XML where a lexical edge is required. Do not add a package dependency.

- [ ] **Step 2: Add RED coordinate-style and shared-string tests**

Add:

```text
test_swapped_data_row_styles_are_rejected_by_coordinate
test_explicit_blank_and_column_inherited_style_remain_equivalent
test_shared_strings_relationship_type_mismatch_is_rejected
test_shared_strings_relationship_target_mismatch_is_rejected
test_shared_strings_content_type_missing_is_rejected
test_shared_strings_part_missing_is_rejected
test_shared_string_index_out_of_range_is_rejected
test_inline_string_replacing_shared_string_is_rejected
test_workbook_mismatch_never_contains_real_cell_values
```

- [ ] **Step 3: Implement streaming XML comparison**

Use these internal types:

```python
StorageType = Literal['blank', 'n', 's', 'inlineStr', 'str', 'b', 'e', 'd']

@dataclass(frozen=True)
class WorkbookMismatch:
    sheet: str
    coordinate: str | None
    mismatch_kind: str
    expected_storage_type: StorageType | None = None
    actual_storage_type: StorageType | None = None

@dataclass(frozen=True)
class XmlCell:
    coordinate: str
    storage_type: StorageType
    lexical_value: str | None
    resolved_text: str | None
    style_id: int
```

Delete `DECIMAL_TOLERANCE`. Parse numeric `<v>` with `Decimal`; normalize signed zero and equivalent Decimal exponents without converting through float. Merge two sorted XML cell streams by coordinate. Effective style is explicit cell style first, otherwise column inherited style. Validate sharedStrings relationship type/target, `[Content_Types].xml`, part existence, index range and each string cell `t` value.

Column and grouped reconciliations use the approved business columns and work-order keys already present in workbook headers; absent required headers are oracle failures, not skipped checks.

The grouped keys are closed and exact:

```python
GROUP_KEYS = {
    '成本计算单数量聚合维度': ('月份', '产品编码', '工单编号', '工单行号'),
    '成本分析工单维度': ('月份', '产品编码', '工单编号', '工单行'),
}
```

Every numeric cell is compared exactly. Additional column/group sums use these closed policies:

```text
成本计算单总表:
  本期完工单位成本, 本期完工金额

成本计算单数量聚合维度:
  本期完工数量, 本期完工金额,
  本期完工直接材料合计完工金额, 本期完工直接人工合计完工金额,
  本期完工制造费用合计完工金额, 本期完工制造费用_其他合计完工金额,
  本期完工制造费用_人工合计完工金额,
  本期完工制造费用_机物料及低耗合计完工金额,
  本期完工制造费用_折旧合计完工金额, 本期完工制造费用_水电费合计完工金额,
  本期完工委外加工费合计完工金额,
  直接材料单位完工金额, 直接人工单位完工金额, 制造费用单位完工金额,
  制造费用_其他单位完工成本, 制造费用_人工单位完工成本,
  制造费用_机物料及低耗单位完工成本, 制造费用_折旧单位完工成本,
  制造费用_水电费单位完工成本, 委外加工费单位完工成本

成本分析工单维度:
  本期完工数量, 总完工成本,
  直接材料/直接人工/制造费用及五个制造费用细项的合计完工金额,
  委外加工费合计完工金额, 总单位完工成本,
  直接材料/直接人工/制造费用及五个制造费用细项的单位完工成本,
  委外加工费单位完工成本

SK only additions:
  数量聚合页的本期完工软件费用合计完工金额、软件费用单位完工成本；
  工单分析页的软件费用合计完工金额、软件费用单位完工成本
```

The implementation stores the expanded exact tuple for each `(pipeline, sheet)`；it does not select columns with a free-form substring rule. Missing required headers or an unexpected numeric business header fails the oracle.

- [ ] **Step 4: Adapt callers to structured mismatches**

Change `classify_validation_errors()` to classify by `mismatch_kind` and Sheet, not by parsing strings that contain cell values. Update full CLI tests to assert `report.passed` and only print safe mismatch metadata.

- [ ] **Step 5: Verify and commit**

```powershell
uv run python -m pytest tests/rust_oracle/test_workbook_compare.py -q --basetemp .pytest-tmp/workbook-oracle
uv run python -m pytest tests/test_full_rust_cli_benchmark.py -q --basetemp .pytest-tmp/workbook-callers -k "classify or runtime_mismatch"
uv run python -m pytest tests/test_full_rust_cli_oracle.py tests/test_full_rust_cli_benchmark.py --collect-only -q
uv run python -m ruff check tests/rust_oracle tests/test_full_rust_cli_oracle.py tests/test_full_rust_cli_benchmark.py
uv run python -m ruff format tests/rust_oracle tests/test_full_rust_cli_oracle.py tests/test_full_rust_cli_benchmark.py --check
git add -- tests/rust_oracle/workbook_compare.py tests/rust_oracle/test_workbook_compare.py tests/rust_oracle/benchmark.py tests/test_full_rust_cli_oracle.py tests/test_full_rust_cli_benchmark.py
git diff --cached --name-only
git diff --cached --check
git commit -m "test(oracle): harden workbook package comparison"
```

## Task 5: Extend EvidenceSanitizer to Every Versioned Artifact

**Files:**
- Modify: `tests/rust_oracle/evidence.py`
- Modify: `tests/rust_oracle/test_evidence.py`
- Modify: `tests/rust_oracle/oracle_runner.py`
- Modify: `tests/rust_oracle/benchmark.py`
- Modify: `tests/rust_oracle/measure_peak_working_set.ps1`
- Modify: `tests/rust_oracle/test_oracle_runner.py`
- Modify: `tests/rust_oracle/test_benchmark.py`
- Modify: `tests/test_rust_check_only_benchmark.py`

**Interfaces:**
- Extends: Phase -1D `DependencyEvidence` without breaking its CLI。
- Produces: typed builders for benchmark, command transcript, smoke, PE imports, fork provenance, Cargo feature tree and text report。

The seven typed inputs are closed:

```text
BenchmarkManifestEvidence:
  schema_version, profile, pipeline, input_alias, input_sha256,
  reference_label/reference_exe_sha256, candidate_label/candidate_exe_sha256,
  machine fields/fingerprint, attempt_count/prior_safe_verdicts/ledger_head_sha256,
  first_group_sha256/expanded_group_sha256, global round/order/value tuples,
  medians/ratios, runtime counts, Sheet dimensions, output bytes,
  mismatch kind/Sheet/coordinate tuples, local log SHA tuple, verdict

CommandTranscriptEvidence:
  closed command_id, tuple of literal tokens or approved aliases, tool name,
  sanitized tool version, exit_code, local log SHA, verdict

SmokeSummaryEvidence:
  candidate EXE SHA, fixture SHA, pipeline, exit_code, approved Sheet tuple,
  temp_canary_created, temp_residue_count, missing_dll, local log SHA, verdict

PeImportsEvidence:
  candidate/baseline EXE SHA, closed tool tuple, normal import basenames,
  delay import basenames, local log SHA, verdict

ForkProvenanceEvidence:
  official/fork URLs, tag, upstream base, crates checksum, fork revision,
  exact allowed diff path tuple/diff SHA, no-PR result, local log SHA, verdict

CargoFeatureTreeEvidence:
  candidate label, candidate EXE SHA, fork revision,
  normalized package/revision/feature-edge tuples, local log SHA, verdict

TextReportEvidence:
  closed report kind/title, tuple of closed check id + verdict + evidence SHA,
  overall verdict
```

No input has a free-form message, command, path, title, package, DLL or check field.

- [ ] **Step 1: Add RED allowlist and canary tests**

Add:

```text
test_success_manifest_contains_only_aliases_hashes_counts_and_finite_numbers
test_each_allowed_string_field_rejects_unknown_canary
test_mismatch_artifact_omits_expected_and_actual_values
test_nonzero_stdout_stderr_canary_is_not_copied_to_manifest
test_command_template_rejects_real_paths_and_arguments
test_all_artifact_kinds_use_typed_sanitizer_builders
test_scan_tree_rejects_drive_unc_users_username_hostname_and_erp_basename
test_scan_tree_rejects_expected_actual_stdout_and_stderr_markers
test_scan_staged_checks_all_staged_evidence_files
test_local_path_rejects_parent_traversal
test_local_path_rejects_junction_to_versioned_directory
test_local_path_rejects_case_normalized_escape
test_local_path_rejects_input_output_evidence_collision
```

- [ ] **Step 2: Implement dedicated typed builders**

Use a closed `EvidenceKind` and a dedicated builder for each kind. Allowed path aliases are exactly `$REPO_ROOT`, `$GB_INPUT`, `$SK_INPUT`, `$REFERENCE_EXE`, `$CANDIDATE_EXE`, `$ROUND_OUTPUT`, and `$FORK_CHECKOUT`. Do not expose a generic `sanitize(dict)` and do not call `dataclasses.asdict()`.

`validate_local_destination()` rejects `..`, normalizes Windows case/long paths, resolves the nearest existing parent, rejects every reparse-point component, and verifies canonical containment below the exact ignored roots. It also rejects equality/collision among input, output, raw log and evidence destinations.

`write_batch()` must be called only after the harness cleanup state is `CLEANUP_COMPLETE`. It writes a temporary staging tree, scans staging, scans the complete existing `docs/performance/`, scans every staged evidence file, then atomically moves the batch. If any operation fails, remove staging and every artifact moved by this batch. Refuse to overwrite a Phase 0A baseline.

- [ ] **Step 3: Add RED rollback and old-writer closure tests**

Add:

```text
test_write_batch_removes_staging_on_sensitive_scan_failure
test_write_batch_removes_this_batch_outputs_on_post_write_failure
test_cleanup_failure_leaves_no_versionable_artifact
test_phase0a_manifest_cannot_be_overwritten
test_old_normal_capture_cannot_write_raw_cli_payload
test_check_only_raw_result_can_only_be_written_under_ignored_root
```

Remove or privatize every old function that writes raw CLI payload to an arbitrary path. PowerShell raw JSON remains local and cannot target `docs/performance/`.

- [ ] **Step 4: Verify and commit**

```powershell
uv run python -m pytest tests/rust_oracle/test_evidence.py tests/rust_oracle/test_oracle_runner.py tests/rust_oracle/test_benchmark.py tests/test_rust_check_only_benchmark.py -q --basetemp .pytest-tmp/evidence-sanitizer
uv run python -m ruff check tests
uv run python -m ruff format tests --check
git add -- tests/rust_oracle/evidence.py tests/rust_oracle/test_evidence.py tests/rust_oracle/oracle_runner.py tests/rust_oracle/benchmark.py tests/rust_oracle/measure_peak_working_set.ps1 tests/rust_oracle/test_oracle_runner.py tests/rust_oracle/test_benchmark.py tests/test_rust_check_only_benchmark.py
git diff --cached --name-only
git diff --cached --check
git commit -m "test(evidence): add fail-closed artifact sanitizer"
```

## Task 6: Add a Sanitized Phase 0H Smoke and Phase 0A Capture Command

**Files:**
- Create: `tests/rust_oracle/sanitized_fixture.py`
- Create: `docs/performance/README.md`
- Modify: `tests/rust_oracle/phase0_harness.py`
- Modify: `tests/rust_oracle/test_phase0_harness.py`

**Interfaces:**
- Produces: `paired`、`smoke` and `phase0a` CLI subcommands。
- Does not produce: approved Phase 0A business manifest during this task。

- [ ] **Step 1: Add RED fixture and orchestration tests**

Add:

```text
test_sanitized_fixture_contains_no_erp_or_host_canary
test_low_memory_fixture_produces_at_least_five_million_output_slots
test_phase0h_smoke_runs_normal_wall_and_normal_pws
test_phase0h_smoke_cleans_every_workbook
test_paired_batch_expands_wall_and_pws_together
test_phase0a_manifest_uses_external_output_size_for_base_reference
test_phase0a_manifest_contains_gb_sk_wall_pws_runtime_and_sheet_dimensions
test_phase0a_manifest_contains_no_paths_filenames_hostname_or_raw_logs
test_phase0a_capture_refuses_existing_manifest
test_paired_cli_exposes_no_batch_round_or_threshold_argument
test_paired_cli_uses_closed_labels_profiles_and_exit_codes
test_sanitized_raw_fixture_runs_both_python_and_rust_to_three_sheets
```

- [ ] **Step 2: Implement the CLI with closed labels and profiles**

The `paired` subcommand accepts exactly `--pipeline`、`--input`、`--reference-executable`、`--candidate-executable`、`--reference-label`、`--candidate-label`、`--comparison-profile`、`--phase0a-manifest`、`--local-root` and `--evidence-path`. The append-only ledger root is fixed internally at `rust/target/perf-local/batches`. It exposes no batch id, attempt number, round, sample count or threshold. Exit codes are closed: 0 validated; 2 candidate/correctness/gate/inconclusive; 3 reference/environment/incomplete; 4 cleanup/sensitive evidence; 5 CLI usage.

`sanitized_fixture.py` exposes `build_raw_fixture(path: Path, pipeline: PipelineName, size: Literal['small', 'low-memory']) -> None`. Each fixture has one raw input Sheet, two metadata rows, two header rows and synthetic data rows. Exact columns are `年期`、`成本中心名称`、`产品编码`、`产品名称`、`规格型号`、`工单编号`、`工单行号`、`基本单位`、`成本项目名称`、`本期完工数量`、`本期完工单位成本`、`本期完工金额`. It uses synthetic product/order values, detail rows for direct material/labor/manufacturing overhead/outsourcing, positive quantity rows, and an SK-only software-fee row. The product is outside the business whitelist, so the third output Sheet is valid with headers and zero data rows. `small` is used by Phase 0H. `low-memory` deterministically creates at least 100,000 valid SK quantity rows and its smoke validator requires an output Sheet with at least 5,000,000 slots. Both Python oracle and Rust CLI must generate the approved three Sheet names.

The `smoke` subcommand creates this raw fixture below `rust/target/perf-local/phase0h-smoke`, runs the same reference EXE as both roles for five normal wall and five normal PWS rounds, compares every workbook, exercises the sanitizer, and deletes every fixture/output workbook in `finally`.

The `phase0a` subcommand accepts exactly `--gb-input`、`--sk-input`、`--reference-executable`、`--fork-revision`、`--local-root` and `--output`. It validates the explicit paths against the caller-provided `COSTING_GB_SAMPLE/COSTING_SK_SAMPLE` environment values before reading. It records separate reference-only `CalibrationGroup` values for wall/PWS, generates a sanitized manifest and refuses an existing destination. User confirmation belongs to the next subplan.

- [ ] **Step 3: Document raw/versioned boundaries and run smoke**

Build the Phase 0A reference after Phase -1D:

```powershell
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/perf-builds/phase0a/reference --no-default-features
$Phase0AExe = 'rust/target/perf-builds/phase0a/reference/x86_64-pc-windows-msvc/release/costing-calculate.exe'
uv run python -m tests.rust_oracle.phase0_harness smoke --pipeline gb --reference-executable $Phase0AExe --candidate-executable $Phase0AExe --local-root rust/target/perf-local/phase0h-smoke
```

Expected: `VALIDATED`; no workbook remains below the smoke root and no versioned Phase 0A business manifest is created.

- [ ] **Step 4: Run the full Phase 0H gate**

```powershell
uv run python -m pytest tests/rust_oracle -q --basetemp .pytest-tmp/rust-oracle
uv run python -m pytest tests/contracts tests/architecture -q --basetemp .pytest-tmp/contracts
uv run python -m ruff check src tests
uv run python -m ruff format src tests --check
git diff --check
```

Expected: all pass.

- [ ] **Step 5: Commit only the smoke/orchestration files**

```powershell
git add -- tests/rust_oracle/sanitized_fixture.py tests/rust_oracle/phase0_harness.py tests/rust_oracle/test_phase0_harness.py docs/performance/README.md
git diff --cached --name-only
git diff --cached --check
git commit -m "test(perf): close phase0h benchmark foundation"
```

## Pseudocode Draft

```python
# 目标：同一批内编排 wall/PWS，强制临界扩样，并保证失败后没有真实 workbook 或可提交证据残留
# 输入：固定输入、reference/candidate EXE、闭合 comparison profile、已批准 Phase 0A manifest
# 输出：VALIDATED / fail-closed verdict，以及仅含别名、hash、计数和指标的 sanitized evidence

def run_paired_normal_batch(request: PairedBenchmarkRequest) -> PairedBenchmarkResult:
    initial_state = capture_input_binary_and_git_state(request)
    require_closed_repository_state(initial_state, request.evidence_path.parent)
    attempt = open_or_resume_append_only_attempt(request, initial_state)
    batch_id = attempt.batch_id
    cleanup_ledger = plan_all_possible_local_artifacts(batch_id, global_rounds=range(1, 11))
    comparison: PairedBenchmarkResult | None = None

    try:
        first_plans = build_round_plan(global_round_start=1, round_count=5)
        wall_first = run_normal_wall_group(MetricGroupRequest(request, batch_id, 'wall', first_plans, attempt.attempt_directory))
        pws_first = run_pws_group(MetricGroupRequest(request, batch_id, 'pws', first_plans, attempt.attempt_directory))
        validate_metric_group(wall_first)
        validate_metric_group(pws_first)
        assert_same_benchmark_batch(wall_first, pws_first)
        first_group_sha = commit_first_group_to_ledger(attempt, wall_first, pws_first)
        assert_state_unchanged(initial_state)

        # 任一时间或 PWS 指标进入临界区，两套证据一起扩样，禁止选择性重采。
        if any_metric_requires_expansion(wall_first, pws_first, request.comparison_profile):
            second_plans = build_round_plan(global_round_start=6, round_count=5)
            wall_second = run_normal_wall_group(MetricGroupRequest(request, batch_id, 'wall', second_plans, attempt.attempt_directory))
            pws_second = run_pws_group(MetricGroupRequest(request, batch_id, 'pws', second_plans, attempt.attempt_directory))
            assert_expanded_group_links_first_sha(attempt, first_group_sha)
            if groups_have_conflicting_direction(wall_first, wall_second):
                comparison = fail_closed_result(HarnessVerdict.INCONCLUSIVE, attempt)
            if groups_have_conflicting_direction(pws_first, pws_second):
                comparison = fail_closed_result(HarnessVerdict.INCONCLUSIVE, attempt)
            if comparison is None:
                wall = merge_metric_groups(wall_first, wall_second)
                pws = merge_metric_groups(pws_first, pws_second)
        else:
            wall, pws = wall_first, pws_first

        if comparison is None:
            assert_same_batch_ratio(wall)
            assert_same_batch_ratio(pws)
            assert_same_benchmark_batch(wall, pws)
            assert_environment_not_drifted(wall, load_phase0a_manifest(request.phase0a_manifest))
            assert_environment_not_drifted(pws, load_phase0a_manifest(request.phase0a_manifest))
            assert_state_unchanged(initial_state)
            comparison = evaluate_profile(request.comparison_profile, wall, pws, attempt)
    except HarnessFailure as error:
        comparison = failure_result_from(error, attempt)
    finally:
        cleanup_error = remove_every_local_artifact(cleanup_ledger)
        if cleanup_error is not None:
            mark_attempt_failed(attempt, HarnessVerdict.CLEANUP_FAILED)
            raise HarnessFailure(HarnessVerdict.CLEANUP_FAILED) from cleanup_error

    # 真实 workbook/oracle 已清理并记账完成后，才允许创建可提交 artifact。
    mark_cleanup_complete(attempt)
    sanitized = EvidenceSanitizer.closed_policy().build_benchmark_manifest(comparison)
    EvidenceSanitizer.closed_policy().write_batch(
        destination_root=request.evidence_path.parent,
        artifacts=(sanitized,),
        scan_staged=True,
    )
    mark_evidence_committed(attempt)
    return comparison
```

## Phase 0H Exit Checklist

- [ ] Pure protocol tests prove global rounds 1–10, mandatory expansion and `INCONCLUSIVE`.
- [ ] normal wall and normal PWS both compare fixed Rust reference/candidate with the same batch identity.
- [ ] append-only ledger prevents overwriting, one-metric resampling and retrying a failed candidate SHA.
- [ ] wall/PWS share `N`、global rounds、machine fingerprint and binary/input hashes；Phase 0A drift over 10% is rejected.
- [ ] reference/candidate/input/Git SHA drift, missing rounds and nonzero exits fail closed.
- [ ] workbook values use XML Decimal semantics; coordinate styles and sharedStrings package rules are enforced.
- [ ] mismatch artifacts cannot contain real values.
- [ ] all versioned artifact kinds pass dedicated typed sanitizer builders and staged-tree scan.
- [ ] success and every injected failure path remove all generated workbooks.
- [ ] sanitized fixture normal smoke passes.
- [ ] the fixture is a legal single-Sheet raw input for both pipelines, not a three-Sheet output-shaped workbook.
- [ ] no formal Phase 0A business manifest has yet been approved or committed.

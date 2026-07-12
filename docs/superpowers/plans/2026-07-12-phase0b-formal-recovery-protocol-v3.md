# Phase 0B Formal Recovery Protocol v3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在保持 sealed protocol v2 完全只读、禁止选择性重采和不复用 v2 性能样本的前提下，实现 protocol/schema v3，完成 fresh GB/SK Phase 0B 正式证据，并恢复原总计划 Phase 1→5。

**Architecture:** 把恢复拆成四层闭合模块：`benchmark_protocol.py` 只负责版本化身份和纯判定，`phase0_harness.py` 负责 legacy snapshot、append-only v3 ledger、durable sample start 和正式状态机，`evidence.py` 负责 schema 1/2/3 multi-read + v3 single-write，文档/正式任务只消费已经 review 的接口。GB 通过固定授权绑定 sealed v2 tree；SK 通过 `UpstreamGateProvenance` 绑定已提交 GB artifact、marker 和 evidence-only commit。

**Tech Stack:** Python 3.11、pytest、openpyxl/OOXML oracle、PowerShell、Git、Windows `Process.PeakWorkingSet64`、现有 Rust release EXE；本计划不增加依赖、不修改生产 Rust、不创建 PR。

## Global Constraints

- 设计来源固定为 `docs/superpowers/specs/2026-07-12-phase0b-formal-recovery-protocol-v3-design.md`；不得重新解释门槛或恢复资格。
- 上位规格固定为 `docs/superpowers/specs/2026-07-11-rust-output-ingest-continuous-performance-design.md`。
- Phase 0B GB/SK instrumented wall / same-batch Phase 0A reference wall 必须分别 `<=1.02`。
- Phase 0B PWS 没有 direct closed gate，但必须 fresh N=5/10、same-batch、environment-valid，并参与临界扩样和 direction diagnostic。
- GB/SK output bytes / approved Phase 0A bytes 必须 `<=1.10`。
- correctness、runtime、dimensions、workbook oracle 和全部正式轮次必须通过。
- `N=5`；只有存在 closed limit 的 active gate（Phase 0B 为 wall `1.02` 和 output bytes `1.10`）进入 `abs(measured / limit - 1) <=0.03` 时才触发扩样；一旦触发，wall/PWS 都补 global round 6–10。PWS 无 limit，不能单独触发或否决。
- odd global round 为 reference→candidate；even global round 为 candidate→reference。
- v1/v2 只读；v3 是唯一新写协议和 evidence schema。
- v2 metric、median、direction、output observation 和 runtime timing 不得进入 v3 verdict/evidence。
- 每个正式子进程前必须 durable append `sample-started`；started-without-sample 永不重采。
- `EVIDENCE_COMMITTED` 是成功封存；失败 terminal 是失败封存；二者都禁止新的采样 attempt。
- `CLEANUP_FAILED` 只允许零子进程 cleanup-only successor。
- prepared/committed publication recovery 只允许 exact artifact/marker dirty-path allowlist，零子进程。
- sealed v2 comparison tree 固定为 inventory `134`、tree SHA `8e961515bcac3afad271bb75eac9e439fdb18d1e8ba07b0fef7e210838796ccb`、journal head `ae10e9d441ecebee9ba6cfb93a799f14a9085c75560103fedc9df6ff56b92c85`。
- 固定 Phase 0A reference EXE SHA：`f75f7ee17cc222765537f6bbe02f90e76cd041c55c8990b0261788e6fa63db56`。
- 固定 Phase 0B candidate EXE SHA：`d06470e4e7c9e6dc8f54efc9d26d996d3cbbbddec04cb7dffef6e6869802b629`。
- 固定 Phase 0A manifest SHA：`17faa1f08a0601fac52186ab64a4ebb4519312a259c59b7d64e69c748bab56df`。
- 固定 v1 terminal SHA：`d42940dfc48f208834efa103de6a08663e75d1ee09dd6804d4e2416ad90af71f`。
- 固定 v2 terminal SHA：`f515c305518093e9aa0ac90fa0b82520874fcd7006db16946b45921fd9b2a57b`。
- 不重建固定 EXE；正式 Task 9/10 不执行 `cargo build` 或 `cargo run`。
- 所有代码任务使用 red-green-refactor；每个任务独立 Conventional Commit。
- 每次只暂存当前任务的 exact pathspec；禁止 `git add -A`。
- 所有 PowerShell 验证/提交脚本首行必须启用 `Set-StrictMode -Version Latest`、`$ErrorActionPreference='Stop'` 和 `$PSNativeCommandUseErrorActionPreference=$true`；每个 native command 后仍显式检查 `$LASTEXITCODE` 并立即 `throw`。
- 每次 `git add` 前 index 必须为空；暂存后必须把 `git diff --cached --name-only` 与当前任务声明的 exact path set 做无序相等比较；commit 后必须检查退出码并断言 `git status --porcelain=v1 --untracked-files=all` 为空。
- 正式 GB/SK 只能在 Task 1–8 全部通过、review 无 P0/P1、worktree clean 后执行。
- 主工作区用户所有的 `rust/crates/costing-core/src/model.rs` 差异不得修改、暂存或提交。

---

## Starting State

执行 worktree：

```text
D:\python_program\02--costing_calculate\.worktrees\rust-output-ingest-performance
branch = perf/rust-output-ingest-continuous
approved v3 spec commit = dbe26ee
```

正式 v2 GB comparison：

```text
comparison_key = 09d6bb93ab04dda277e97f19dc8a270be91f2f8898a42f25d1d5bd745bdf0fd7
attempt = attempt-0001
terminal = INCOMPLETE_EVIDENCE
versioned evidence count = 0
formal SK = not run
```

实现前必须验证执行 worktree clean，主工作区仍只含用户 `model.rs` 修改。任何其他差异先停止并查明所有权。

### Mandatory PowerShell and commit gate

本计划中的每个 PowerShell code block 都必须在同一脚本顶部执行以下序言；任何 native command 非零立即终止，不能依赖“最后一条命令”的状态：

```powershell
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

function Invoke-NativeChecked {
  param([scriptblock]$Command, [string]$Failure)
  & $Command
  if ($LASTEXITCODE -ne 0) { throw "$Failure (exit=$LASTEXITCODE)" }
}

function Assert-ExactStagedPaths {
  param([string[]]$Expected)
  $Actual = @(git diff --cached --name-only)
  if ($LASTEXITCODE -ne 0) { throw 'cannot read staged paths' }
  $Difference = @(Compare-Object ($Actual | Sort-Object) ($Expected | Sort-Object))
  if ($Difference.Count -ne 0) { $Actual; throw 'staged paths differ from exact task set' }
}

function Assert-CleanRepository {
  $Status = @(git status --porcelain=v1 --untracked-files=all)
  if ($LASTEXITCODE -ne 0) { throw 'cannot read repository status' }
  if ($Status.Count -ne 0) { $Status; throw 'repository is not clean' }
}
```

每个 commit step 先执行 `Assert-ExactStagedPaths @()`，再 `git add -- <exact paths>`，随后对任务声明的 `$Expected` 执行 `Assert-ExactStagedPaths $Expected`、`git diff --cached --check`、`git commit`，两条 native command 都检查退出码，最后执行 `Assert-CleanRepository`。Task 9/10 的 evidence commit 也使用同一门禁，且 `$Expected` 必须由 typed artifact 和其确定性 marker basename 构造，不能只按文件数量判断。

## File Structure

### Modify

- `tests/rust_oracle/benchmark_protocol.py`：版本化 comparison identity 和 provenance 类型。
- `tests/rust_oracle/phase0_harness.py`：legacy snapshot、授权、v3 ledger、runner、publication 和 GB→SK gate。
- `tests/rust_oracle/evidence.py`：schema 1/2/3 reader、v3 builder/publisher、provenance sanitizer。
- `tests/rust_oracle/test_benchmark_protocol.py`：身份纯函数测试。
- `tests/rust_oracle/test_phase0_harness.py`：parent/ledger/runner/publication/SK gate 测试。
- `tests/rust_oracle/test_evidence.py`：schema/sanitizer/marker 测试。
- `docs/performance/README.md`：实现完成后把当前运行契约更新为 v3，并保留 v2 只读事实。
- `docs/superpowers/specs/2026-07-11-rust-output-ingest-continuous-performance-design.md`：只同步当前状态和 v3 链接。
- `docs/superpowers/plans/2026-07-11-rust-output-phase-0a-3-writer-optimization.md`：Phase 0B handoff 指向 v3 evidence。

### Create only after formal success

- `docs/performance/runs/phase0b-v3/benchmark-v3-<gb-key-prefix>.json`
- GB batch marker
- `docs/performance/runs/phase0b-v3/benchmark-v3-<sk-key-prefix>.json`
- SK batch marker

### Never modify

- `docs/performance/baselines/2026-07-11-windows-x64-phase0a.json`
- `docs/performance/dependencies/2026-07-11-rust-xlsxwriter-0.96.0.json`
- `rust/target/perf-local/batches/09d6bb93ab04dda277e97f19dc8a270be91f2f8898a42f25d1d5bd745bdf0fd7/**`
- `rust/crates/**`
- 主工作区用户差异

## Stable Interfaces

Task 1–7 必须使用以下名称；后续任务不得自行改名：

```python
LEGACY_PAIRED_PROTOCOL_VERSION: Final = 2
PAIRED_PROTOCOL_VERSION: Final = 3
CURRENT_BENCHMARK_SCHEMA_VERSION: Final = 3

class RecoveryReason(StrEnum):
    MISSING_FORMAL_SHEET_DIMENSIONS = 'MISSING_FORMAL_SHEET_DIMENSIONS'

@dataclass(frozen=True)
class RecoveryProvenance:
    parent_protocol_version: Literal[2]
    parent_comparison_key: str
    parent_attempt: Literal[1]
    parent_terminal_sha256: str
    parent_comparison_tree_sha256: str
    parent_journal_head_sha256: str
    parent_inventory_entry_count: Literal[134]
    reason: RecoveryReason

@dataclass(frozen=True)
class UpstreamGateProvenance:
    pipeline: Literal['gb']
    protocol_version: Literal[3]
    schema_version: Literal[3]
    comparison_key: str
    artifact_basename: str
    artifact_sha256: str
    marker_basename: str
    marker_sha256: str
    validated_commit_sha: str
```

- `derive_v2_comparison_key(*, pipeline: PipelineName, comparison_profile: ComparisonProfile, reference_label: ClosedBinaryLabel, candidate_label: ClosedBinaryLabel, input_sha256: str, reference_sha256: str, candidate_sha256: str) -> str`
- `derive_v3_comparison_key(*, pipeline: PipelineName, comparison_profile: ComparisonProfile, reference_label: ClosedBinaryLabel, candidate_label: ClosedBinaryLabel, phase0a_manifest_sha256: str, input_sha256: str, reference_sha256: str, candidate_sha256: str, recovery_provenance: RecoveryProvenance | None, upstream_gate_provenance: UpstreamGateProvenance | None) -> str`

`phase0_harness.py` 新接口：

```python
@dataclass(frozen=True)
class StaticComparisonInputs:
    pipeline: PipelineName
    comparison_profile: ComparisonProfile
    reference_label: ClosedBinaryLabel
    candidate_label: ClosedBinaryLabel
    phase0a_manifest_sha256: str
    input_sha256: str
    reference_sha256: str
    candidate_sha256: str


@dataclass(frozen=True)
class FormalV3Identity:
    comparison_key: str
    batch_id: str
    evidence_basename: str
    recovery_provenance: RecoveryProvenance | None
    upstream_gate_provenance: UpstreamGateProvenance | None


@dataclass(frozen=True)
class ComparisonTreeDigest:
    sha256: str
    journal_head_sha256: str
    entry_count: int


@dataclass(frozen=True)
class FormalV3StateInspection:
    identity: FormalV3Identity
    state: Literal['NEW', 'CLEANUP_COMPLETE', 'EVIDENCE_PREPARED', 'EVIDENCE_COMMITTED', 'FAILED_TERMINAL', 'INVALID']
    child_process_allowed: bool
    sample_started_count: int
    sample_record_count: int
    artifact_basename: str
    marker_basename: str | None

@dataclass(frozen=True)
class ApprovedRecoveryParent:
    pipeline: Literal['gb']
    comparison_profile: Literal[ComparisonProfile.PHASE0B_VS_PHASE0A]
    reference_label: Literal[ClosedBinaryLabel.PHASE0A]
    candidate_label: Literal[ClosedBinaryLabel.PHASE0B]
    input_sha256: str
    reference_sha256: str
    candidate_sha256: str
    parent_protocol_version: Literal[2]
    parent_comparison_key: str
    parent_attempt: Literal[1]
    parent_terminal_sha256: str
    parent_comparison_tree_sha256: str
    parent_journal_head_sha256: str
    parent_inventory_entry_count: Literal[134]
    reason: Literal[RecoveryReason.MISSING_FORMAL_SHEET_DIMENSIONS]
```

- `comparison_tree_digest(path: Path) -> ComparisonTreeDigest`
- `parse_and_validate_ledger_snapshot(directory: Path, *, expected_protocol_version: int) -> AppendOnlyAttemptLedger`
- `authorize_v3_recovery(static: StaticComparisonInputs) -> RecoveryProvenance`
- `derive_v3_batch_id(*, comparison_key: str, profile: ComparisonProfile, pipeline: PipelineName, phase0a_manifest_sha256: str, identity: BenchmarkIdentity, recovery_provenance: RecoveryProvenance | None, upstream_gate_provenance: UpstreamGateProvenance | None) -> str`
- `derive_committed_gb_v3_provenance(static: StaticComparisonInputs) -> UpstreamGateProvenance`
- `derive_expected_formal_v3_identity(request: PairedBenchmarkRequest) -> FormalV3Identity`（纯读取，不创建 comparison/attempt/evidence）
- `inspect_expected_formal_v3_state(request: PairedBenchmarkRequest) -> FormalV3StateInspection`（纯读取；fresh 只允许 `NEW/child_process_allowed=True`，publication recovery 只允许三个零进程状态）

`AppendOnlyAttemptLedger` 新接口：

- `open_v3_for_resume(directory: Path, identity: BenchmarkIdentity) -> AppendOnlyAttemptLedger`
- `record_sample_started(*, batch_id: str, metric: MetricName, global_round: int, role: BinaryRole, order: tuple[BinaryRole, BinaryRole], input_sha256: str, binary_sha256: str, planned_output_record_sha256: str) -> str`
- `record_sample(metric: MetricName, global_round: int, role: BinaryRole, payload: dict[str, object], *, sample_started_record_sha256: str) -> str`
- `prepare_evidence(*, artifact_basename: str, artifact_sha256: str, artifact_content: str, marker_basename: str, marker_sha256: str) -> str`
- `mark_evidence_committed(*, artifact_sha256: str, marker_sha256: str) -> str`

### Closed test fixture contracts

下列 helper 只存在于 `test_phase0_harness.py` / `test_evidence.py`，不得读取或修改真实 fixed parent。实现时先按这里的签名写 helper，再写使用它的测试：

- `_write_synthetic_v2_recovery_parent(root: Path, *, semantic_override: Mapping[str, object] | None = None) -> SyntheticV2Parent`：在 `root/batches/<v2-key>/attempt-0001` 写完整 metadata、20 个 sample、first-group、cleanup、terminal 和 comparison journal；所有 canonical bytes/hash-chain/checkpoint/terminal/journal 均重新计算。默认 sample 精确合法且 `sheet_dimensions=()`；`semantic_override` 只在重新封链前修改指定 sample 业务字段，用于证明 exact parser 而非 hash-chain 先拒绝。
- `SyntheticV2Parent` 精确字段为 `comparison: Path`、`approved: ApprovedRecoveryParent`、`static: StaticComparisonInputs`；`approved` 从该 synthetic tree 的 terminal/tree/journal/count 计算，不引用真实固定值。
- `_mutate_recovery_parent(parent: SyntheticV2Parent, mutation: Literal['journal','attempt-0002','unknown-file']) -> None`：只做结构/bytes 漂移且故意不重封链；`_tree_bytes(path: Path) -> dict[str, bytes]` 返回 relative POSIX file path 到原始 bytes 的有序映射。
- `_mutate_v2_sample_semantics(root: Path, mutation: Literal['missing-dimensions','none-dimensions','list-dimensions','partial-dimensions','identity-drift','oracle-mismatch']) -> SyntheticV2Parent`：调用 synthetic builder 在写盘前注入一个且仅一个语义非法字段，再完整重算 record/checkpoint/terminal/journal/tree；测试必须先证明 `parse_and_validate_ledger_snapshot()` 的 hash-chain 有效，再断言 recovery authorization 因 v2 sample semantics 失败。
- `_v3_ledger(root: Path, *, pipeline: PipelineName='gb') -> AppendOnlyAttemptLedger`：用固定合法 `BenchmarkIdentity`、comparison key、batch ID、manifest SHA 和 GB recovery provenance 创建 isolated schema/protocol v3 attempt；`_v3_group_request()` 从该 ledger 构造 rounds 1–5，不调用真实 EXE。
- `_v3_batch_id_inputs_with_one_mutation(mutation) -> tuple[dict[str, object], dict[str, object]]`：返回两个完整 `derive_v3_batch_id()` kwargs，只改变 closed mutation 指定的 parent tree/journal 或 GB artifact/marker/commit；inventory 的唯一合法值是 `134`，单独测试其他值在派生前被拒绝；`_invalid_v3_batch_payloads()` 逐个生成 extra key、字符串化整数和 bool-as-int，供 strict parser 测试。
- `_record_started_without_sample(ledger, *, metric, round_number, role) -> str`：先写匹配 planned-output，再写 `sample-started`，不写 sample，返回 started record SHA。
- `_append_v3_record_for_test(ledger, *, record_kind: Literal['planned-output','sample-started','sample'], batch_id: str) -> None`：按合法前置链调用对应 production method，只把目标 record 的 batch ID 改为入参；用于逐类证明跨批 record 在 append 前被拒绝。
- `_install_authorized_parent_with_valid_but_extreme_metrics(root, *, wall: str, pws: int) -> SyntheticV2Parent`：创建 hash-chain/语义均合法的 synthetic parent，仅 metric 值极端；`_install_closed_profile_gate_spy(monkeypatch) -> GateInputs` 包装 production closed-profile evaluator，保存其 fresh wall/PWS object identity 后调用原实现，证明 parent metric 只被 eligibility 解析而未进入 v3 verdict/evidence。
- `_benchmark_manifest_v3(*, pipeline: PipelineName='gb', recovery: RecoveryProvenanceEvidence | None=None, upstream: UpstreamGateProvenanceEvidence | None=None, comparison_key: str='a'*64, batch_id: str='b'*64) -> BenchmarkManifestEvidence`：通过 production `build_benchmark_manifest_v3()` 从 synthetic validated wall/PWS/ledger state 构建，不手写绕过 schema；`_legacy_benchmark_artifact(version)` 只从现有 v1/v2 fixture bytes 读取；`_build_manifest_with_mutated_provenance(field, value)` 先构建合法 v3 payload，再仅改变目标字段并重新 canonical encode。
- `_publication_state(root, state, disk_shape) -> tuple[PairedBenchmarkRequest, AppendOnlyAttemptLedger, PublicationPaths]`：从 synthetic completed v3 ledger 确定性推进到 `cleanup-complete` 或 `evidence-prepared`；`PublicationPaths` 精确字段为 artifact/marker path 和 expected bytes/SHA；disk shape 只创建 `none|artifact-only|both|marker-only|artifact-drift|marker-drift|extra-dirty` 指定内容。
- `_observe_publication_actions(monkeypatch) -> PublicationActions`：拦截 child-process boundary、create-new、unlink、terminal append 和 committed append，分别计数并保留顺序；`_fail_first_publication_with_oserror()` 只让第一个 owned create-new 抛 `OSError`，`_restore_publication()` 恢复同一 spy 的后续成功行为。
- `_git_repo_with_gb_evidence_only_commit(root: Path) -> SyntheticGbCommit`：创建 isolated Git repo，父 commit 含代码 fixture，HEAD 单父且 exact two-path GB artifact/marker diff；Git blob 与 disk 相同，typed marker 可重建；返回 repo、artifact、marker、HEAD 和 static inputs。
- `_mutate_git_evidence_state(fixture, mutation)` 只改变 `artifact|marker|head-parent-count|diff-path|blob-vs-disk` 中一个闭合条件；`_sk_static_inputs(fixture)` 使用 fixture HEAD 自动派生 provenance，绝不接受调用者注入 SHA/commit。

所有 helper 的 mutation 参数必须是 `Literal`/closed enum；unknown mutation 直接 `AssertionError`。真实 sealed parent 只由 Task 2 的单独 read-only audit 读取。

## Task 1: Split Legacy/Current Versions and Implement Canonical v3 Identity

**Files:**
- Modify: `tests/rust_oracle/benchmark_protocol.py`
- Modify: `tests/rust_oracle/phase0_harness.py`（仅 transitional legacy import）
- Modify: `tests/rust_oracle/evidence.py`（仅 transitional legacy import）
- Test: `tests/rust_oracle/test_benchmark_protocol.py`

**Interfaces:**
- Consumes: current protocol v2 key function and closed enums.
- Produces: version constants、`RecoveryProvenance`、`UpstreamGateProvenance`、`derive_v2_comparison_key()`、`derive_v3_comparison_key()`。

- [ ] **Step 1: Write failing identity tests**

Add imports and helpers in `test_benchmark_protocol.py`:

```python
def _recovery_provenance() -> RecoveryProvenance:
    return RecoveryProvenance(
        parent_protocol_version=2,
        parent_comparison_key='09d6bb93ab04dda277e97f19dc8a270be91f2f8898a42f25d1d5bd745bdf0fd7',
        parent_attempt=1,
        parent_terminal_sha256='f515c305518093e9aa0ac90fa0b82520874fcd7006db16946b45921fd9b2a57b',
        parent_comparison_tree_sha256='8e961515bcac3afad271bb75eac9e439fdb18d1e8ba07b0fef7e210838796ccb',
        parent_journal_head_sha256='ae10e9d441ecebee9ba6cfb93a799f14a9085c75560103fedc9df6ff56b92c85',
        parent_inventory_entry_count=134,
        reason=RecoveryReason.MISSING_FORMAL_SHEET_DIMENSIONS,
    )


def _upstream_provenance() -> UpstreamGateProvenance:
    return UpstreamGateProvenance(
        pipeline='gb',
        protocol_version=3,
        schema_version=3,
        comparison_key='a' * 64,
        artifact_basename='benchmark-v3-aaaaaaaaaaaaaaaa.json',
        artifact_sha256='b' * 64,
        marker_basename='batch-' + 'c' * 16 + '.commit.json',
        marker_sha256='d' * 64,
        validated_commit_sha='e' * 40,
    )


def _v3_key(*, recovery: RecoveryProvenance) -> str:
    return derive_v3_comparison_key(
        pipeline='gb',
        comparison_profile=ComparisonProfile.PHASE0B_VS_PHASE0A,
        reference_label=ClosedBinaryLabel.PHASE0A,
        candidate_label=ClosedBinaryLabel.PHASE0B,
        phase0a_manifest_sha256='1' * 64,
        input_sha256='2' * 64,
        reference_sha256='3' * 64,
        candidate_sha256='4' * 64,
        recovery_provenance=recovery,
        upstream_gate_provenance=None,
    )


def _v3_sk_key(*, upstream: UpstreamGateProvenance) -> str:
    return derive_v3_comparison_key(
        pipeline='sk',
        comparison_profile=ComparisonProfile.PHASE0B_VS_PHASE0A,
        reference_label=ClosedBinaryLabel.PHASE0A,
        candidate_label=ClosedBinaryLabel.PHASE0B,
        phase0a_manifest_sha256='1' * 64,
        input_sha256='2' * 64,
        reference_sha256='3' * 64,
        candidate_sha256='4' * 64,
        recovery_provenance=None,
        upstream_gate_provenance=upstream,
    )


def _derive_phase0b_key(
    *,
    pipeline: PipelineName,
    recovery: RecoveryProvenance | None,
    upstream: UpstreamGateProvenance | None,
) -> str:
    return derive_v3_comparison_key(
        pipeline=pipeline,
        comparison_profile=ComparisonProfile.PHASE0B_VS_PHASE0A,
        reference_label=ClosedBinaryLabel.PHASE0A,
        candidate_label=ClosedBinaryLabel.PHASE0B,
        phase0a_manifest_sha256='1' * 64,
        input_sha256='2' * 64,
        reference_sha256='3' * 64,
        candidate_sha256='4' * 64,
        recovery_provenance=recovery,
        upstream_gate_provenance=upstream,
    )


def test_legacy_v2_comparison_key_is_stable() -> None:
    assert derive_v2_comparison_key(
        pipeline='gb',
        comparison_profile=ComparisonProfile.PHASE0B_VS_PHASE0A,
        reference_label=ClosedBinaryLabel.PHASE0A,
        candidate_label=ClosedBinaryLabel.PHASE0B,
        input_sha256='6aa5e3e7fdc547ebaaef968eb5b95d4d630c4ec9915184f94346f60687b8e7ee',
        reference_sha256='f75f7ee17cc222765537f6bbe02f90e76cd041c55c8990b0261788e6fa63db56',
        candidate_sha256='d06470e4e7c9e6dc8f54efc9d26d996d3cbbbddec04cb7dffef6e6869802b629',
    ) == '09d6bb93ab04dda277e97f19dc8a270be91f2f8898a42f25d1d5bd745bdf0fd7'


def test_v3_comparison_key_matches_exact_canonical_payload() -> None:
    recovery = _recovery_provenance()
    payload = {
        'protocol_version': 3,
        'pipeline': 'gb',
        'comparison_profile': 'phase0b-vs-phase0a',
        'reference_label': 'phase0a',
        'candidate_label': 'phase0b',
        'phase0a_manifest_sha256': '1' * 64,
        'input_sha256': '2' * 64,
        'reference_sha256': '3' * 64,
        'candidate_sha256': '4' * 64,
        'recovery_provenance': {
            'parent_protocol_version': 2,
            'parent_comparison_key': recovery.parent_comparison_key,
            'parent_attempt': 1,
            'parent_terminal_sha256': recovery.parent_terminal_sha256,
            'parent_comparison_tree_sha256': recovery.parent_comparison_tree_sha256,
            'parent_journal_head_sha256': recovery.parent_journal_head_sha256,
            'parent_inventory_entry_count': 134,
            'reason': 'MISSING_FORMAL_SHEET_DIMENSIONS',
        },
        'upstream_gate_provenance': None,
    }
    expected = hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')
    ).hexdigest()
    assert derive_v3_comparison_key(
        pipeline='gb',
        comparison_profile=ComparisonProfile.PHASE0B_VS_PHASE0A,
        reference_label=ClosedBinaryLabel.PHASE0A,
        candidate_label=ClosedBinaryLabel.PHASE0B,
        phase0a_manifest_sha256='1' * 64,
        input_sha256='2' * 64,
        reference_sha256='3' * 64,
        candidate_sha256='4' * 64,
        recovery_provenance=recovery,
        upstream_gate_provenance=None,
    ) == expected


@pytest.mark.parametrize(
    'field',
    (
        'parent_comparison_tree_sha256',
        'parent_journal_head_sha256',
        'parent_terminal_sha256',
    ),
)
def test_parent_snapshot_mutation_changes_v3_key(field: str) -> None:
    original = _recovery_provenance()
    changed = replace(original, **{field: 'f' * 64})
    assert _v3_key(recovery=original) != _v3_key(recovery=changed)


def test_upstream_artifact_marker_or_commit_mutation_changes_sk_key() -> None:
    original = _upstream_provenance()
    for field, value in (
        ('artifact_basename', 'benchmark-v3-bbbbbbbbbbbbbbbb.json'),
        ('artifact_sha256', '1' * 64),
        ('marker_basename', 'batch-dddddddddddddddd.commit.json'),
        ('marker_sha256', '2' * 64),
        ('validated_commit_sha', '3' * 40),
    ):
        assert _v3_sk_key(upstream=original) != _v3_sk_key(upstream=replace(original, **{field: value}))
```

Add these closed-shape tests in the same step:

```python
@pytest.mark.parametrize(
    ('field', 'value'),
    (
        ('parent_terminal_sha256', 'A' * 64),
        ('parent_comparison_tree_sha256', 'f' * 63),
        ('parent_inventory_entry_count', True),
        ('parent_inventory_entry_count', 135),
    ),
)
def test_recovery_provenance_rejects_non_closed_field(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        replace(_recovery_provenance(), **{field: value})


def test_phase0b_gb_rejects_operator_upstream_and_sk_rejects_recovery() -> None:
    with pytest.raises(ValueError, match='provenance'):
        _derive_phase0b_key(pipeline='gb', recovery=_recovery_provenance(), upstream=_upstream_provenance())
    with pytest.raises(ValueError, match='provenance'):
        _derive_phase0b_key(pipeline='sk', recovery=_recovery_provenance(), upstream=_upstream_provenance())
```

- [ ] **Step 2: Run tests and verify RED**

```powershell
uv run python -m pytest tests/rust_oracle/test_benchmark_protocol.py -q --basetemp C:\costing-v3-task1-red
```

Expected: import/name failures for the new constants, dataclasses and key functions. Remove the owned short temp root after verifying output.

- [ ] **Step 3: Implement exact versioned key functions**

In `benchmark_protocol.py`, retain the old payload under a renamed function and add v3 exact payload construction:

```python
LEGACY_PAIRED_PROTOCOL_VERSION: Final = 2
PAIRED_PROTOCOL_VERSION: Final = 3


def derive_v2_comparison_key(
    *,
    pipeline: PipelineName,
    comparison_profile: ComparisonProfile,
    reference_label: ClosedBinaryLabel,
    candidate_label: ClosedBinaryLabel,
    input_sha256: str,
    reference_sha256: str,
    candidate_sha256: str,
) -> str:
    return _sha256_canonical_identity({
        'protocol_version': LEGACY_PAIRED_PROTOCOL_VERSION,
        'pipeline': pipeline,
        'profile': comparison_profile.value,
        'reference_label': reference_label.value,
        'candidate_label': candidate_label.value,
        'input_sha256': _require_sha256(input_sha256),
        'reference_sha256': _require_sha256(reference_sha256),
        'candidate_sha256': _require_sha256(candidate_sha256),
    })


def derive_comparison_key(*, protocol_version: int, **identity: object) -> str:
    # Transitional audit wrapper: existing v2 harness calls remain valid until Task 5 cutover.
    if protocol_version != LEGACY_PAIRED_PROTOCOL_VERSION or isinstance(protocol_version, bool):
        raise ValueError('legacy comparison key requires protocol version 2')
    return derive_v2_comparison_key(**identity)


def derive_v3_comparison_key(
    *,
    pipeline: PipelineName,
    comparison_profile: ComparisonProfile,
    reference_label: ClosedBinaryLabel,
    candidate_label: ClosedBinaryLabel,
    phase0a_manifest_sha256: str,
    input_sha256: str,
    reference_sha256: str,
    candidate_sha256: str,
    recovery_provenance: RecoveryProvenance | None,
    upstream_gate_provenance: UpstreamGateProvenance | None,
) -> str:
    _validate_phase0b_provenance_shape(
        pipeline=pipeline,
        profile=comparison_profile,
        recovery=recovery_provenance,
        upstream=upstream_gate_provenance,
    )
    payload = {
        'protocol_version': PAIRED_PROTOCOL_VERSION,
        'pipeline': pipeline,
        'comparison_profile': comparison_profile.value,
        'reference_label': reference_label.value,
        'candidate_label': candidate_label.value,
        'phase0a_manifest_sha256': _require_sha256(phase0a_manifest_sha256),
        'input_sha256': _require_sha256(input_sha256),
        'reference_sha256': _require_sha256(reference_sha256),
        'candidate_sha256': _require_sha256(candidate_sha256),
        'recovery_provenance': _recovery_payload(recovery_provenance),
        'upstream_gate_provenance': _upstream_payload(upstream_gate_provenance),
    }
    return _sha256_canonical_identity(payload)
```

Do not make `derive_v2_comparison_key()` accept version 3 and do not use dataclass `asdict()` for versioned payloads.

To keep this intermediate commit green, change only the imports in `phase0_harness.py` and `evidence.py` so their existing v2 code sees:

```python
from tests.rust_oracle.benchmark_protocol import (
    LEGACY_PAIRED_PROTOCOL_VERSION as PAIRED_PROTOCOL_VERSION,
)
```

This is an explicit migration bridge, not the final state. Task 3 changes ledger writing to v3, Task 4 changes evidence writing to v3, and Task 5 removes the runner bridge. Legacy readers continue to use the named legacy constant after cutover.

- [ ] **Step 4: Run GREEN and static checks**

```powershell
uv run python -m pytest tests/rust_oracle/test_benchmark_protocol.py -q --basetemp C:\costing-v3-task1-green
uv run python -m pytest tests/rust_oracle -q --basetemp C:\costing-v3-task1-regression
uv run python -m ruff check tests/rust_oracle/benchmark_protocol.py tests/rust_oracle/test_benchmark_protocol.py
uv run python -m ruff format tests/rust_oracle/benchmark_protocol.py tests/rust_oracle/test_benchmark_protocol.py --check
git diff --check
```

Expected: all pass; v2 expected key remains exact.

- [ ] **Step 5: Commit Task 1**

```powershell
Assert-ExactStagedPaths @()
git add -- tests/rust_oracle/benchmark_protocol.py tests/rust_oracle/phase0_harness.py tests/rust_oracle/evidence.py tests/rust_oracle/test_benchmark_protocol.py
$expected=@('tests/rust_oracle/benchmark_protocol.py','tests/rust_oracle/evidence.py','tests/rust_oracle/phase0_harness.py','tests/rust_oracle/test_benchmark_protocol.py')
$actual=@(git diff --cached --name-only)
if (Compare-Object $actual $expected) { $actual; throw 'Task 1 staged paths differ' }
git diff --cached --check
if ($LASTEXITCODE -ne 0) { throw 'Task 1 cached diff failed' }
Invoke-NativeChecked { git commit -m "test(perf): add protocol v3 identity" } 'Task 1 commit failed'
Assert-CleanRepository
```

## Task 2: Add Pure Legacy Snapshot Validation and Closed v2 Parent Authorization

**Files:**
- Modify: `tests/rust_oracle/phase0_harness.py`
- Test: `tests/rust_oracle/test_phase0_harness.py`

**Interfaces:**
- Consumes: Task 1 provenance types and exact v2 key.
- Produces: `ComparisonTreeDigest`、fixed `APPROVED_RECOVERY_PARENTS`、pure `parse_and_validate_ledger_snapshot()`、`authorize_v3_recovery()`。

- [ ] **Step 1: Write failing tree/authorization tests**

Add a synthetic comparison builder that writes only `attempt-0001` and `journal`, then add:

```python
def test_comparison_tree_digest_is_path_order_stable_and_rejects_reparse(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(phase0_harness, '_trusted_local_root', lambda: tmp_path)
    parent = _write_synthetic_v2_recovery_parent(tmp_path)
    first = phase0_harness.comparison_tree_digest(parent.comparison)
    second = phase0_harness.comparison_tree_digest(parent.comparison)
    assert first == second
    assert first.entry_count > 0
    monkeypatch.setattr(phase0_harness, '_is_reparse_point', lambda path: path.name == 'metadata.json')
    with pytest.raises(HarnessFailure, match='reparse'):
        phase0_harness.comparison_tree_digest(parent.comparison)


@pytest.mark.parametrize('journal_shape', ('missing', 'empty', 'invalid-name'))
def test_comparison_tree_digest_rejects_invalid_journal(journal_shape, monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(phase0_harness, '_trusted_local_root', lambda: tmp_path)
    parent = _write_synthetic_v2_recovery_parent(tmp_path)
    _set_synthetic_journal_shape(parent, journal_shape)
    with pytest.raises(HarnessFailure) as caught:
        phase0_harness.comparison_tree_digest(parent.comparison)
    assert caught.value.verdict is HarnessVerdict.INCOMPLETE_EVIDENCE


@pytest.mark.parametrize('mutation', ('journal', 'attempt-0002', 'unknown-file'))
def test_recovery_parent_mutation_fails_before_v3_create(
    mutation: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    parent = _write_synthetic_v2_recovery_parent(tmp_path)
    _mutate_recovery_parent(parent, mutation)
    monkeypatch.setattr(
        AppendOnlyAttemptLedger,
        'create_v3_once',
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('v3 create must not run')),
    )
    before = _tree_bytes(parent.comparison)
    with pytest.raises(HarnessFailure) as caught:
        phase0_harness._authorize_v3_recovery(parent.static, approved=parent.approved)
    assert caught.value.verdict is HarnessVerdict.INCOMPLETE_EVIDENCE
    assert _tree_bytes(parent.comparison) == before


def test_authorized_parent_uses_exact_v2_sample_parser_and_never_writes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    parent = _write_synthetic_v2_recovery_parent(tmp_path)
    monkeypatch.setattr(
        phase0_harness,
        '_write_create_new',
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('legacy audit attempted a write')),
    )
    before = phase0_harness.comparison_tree_digest(parent.comparison)
    result = phase0_harness._authorize_v3_recovery(parent.static, approved=parent.approved)
    assert result.parent_comparison_tree_sha256 == before.sha256
    assert phase0_harness.comparison_tree_digest(parent.comparison) == before
```

Add one parameterized exact-v2 semantic mutation test. The helper rebuilds the complete valid record/checkpoint/terminal/journal chain after changing exactly one sample field, so rejection proves the exact v2 parser rather than an earlier hash-chain failure:

```python
@pytest.mark.parametrize(
    'mutation',
    ('missing-dimensions', 'none-dimensions', 'list-dimensions', 'partial-dimensions', 'identity-drift', 'oracle-mismatch'),
)
def test_resealed_parent_rejects_non_exact_v2_sample_semantics(mutation, tmp_path) -> None:
    parent = _mutate_v2_sample_semantics(tmp_path, mutation)
    snapshot = parse_and_validate_ledger_snapshot(parent.comparison / 'attempt-0001', expected_protocol_version=2)
    assert snapshot.terminal_verdict is HarnessVerdict.INCOMPLETE_EVIDENCE
    with pytest.raises(HarnessFailure) as caught:
        phase0_harness._authorize_v3_recovery(parent.static, approved=parent.approved)
    assert caught.value.verdict is HarnessVerdict.INCOMPLETE_EVIDENCE
```

The unmutated fixture contains wall/PWS rounds 1–5, both roles, exact global AB/BA order, positive output bytes, paired oracle equality and `sheet_dimensions=()`.

- [ ] **Step 2: Verify RED**

```powershell
uv run python -m pytest tests/rust_oracle/test_phase0_harness.py -q -k "comparison_tree or recovery_parent or authorized_parent" --basetemp C:\costing-v3-task2-red
```

Expected: new interfaces absent.

- [ ] **Step 3: Implement canonical tree digest and pure snapshot reader**

Implement without calling current repair paths:

```python
def comparison_tree_digest(path: Path) -> ComparisonTreeDigest:
    root = _safe_harness_path(
        path,
        allowed_roots=(_trusted_local_root() / 'batches',),
        purpose='parent tree',
        create_parent=False,
    )
    entries: list[dict[str, object]] = []
    pending = [root]
    try:
        while pending:
            directory = pending.pop()
            with os.scandir(directory) as scan:
                children = sorted(scan, key=lambda child: child.name)
            for child in children:
                item = Path(child.path)
                if child.is_symlink() or _is_reparse_point(item):
                    raise HarnessFailure(HarnessVerdict.INCOMPLETE_EVIDENCE, 'recovery parent contains reparse point')
                relative = item.relative_to(root).as_posix()
                if child.is_dir(follow_symlinks=False):
                    entries.append({'path': relative, 'kind': 'directory'})
                    pending.append(item)  # only recurse after rejecting links/reparse points
                elif child.is_file(follow_symlinks=False):
                    stat = child.stat(follow_symlinks=False)
                    entries.append({'path': relative, 'kind': 'file', 'size': stat.st_size, 'sha256': _sha256(item)})
                else:
                    raise HarnessFailure(HarnessVerdict.INCOMPLETE_EVIDENCE, 'recovery parent has unknown entry type')
        entries.sort(key=lambda entry: str(entry['path']))
        journal = sorted((root / 'journal').glob('*.json'))
        if not journal or not all(_is_valid_journal_record_name(item.name) for item in journal):
            raise HarnessFailure(HarnessVerdict.INCOMPLETE_EVIDENCE, 'recovery parent journal is empty or invalid')
        raw = json.dumps(entries, ensure_ascii=True, sort_keys=True, separators=(',', ':')).encode('utf-8')
        return ComparisonTreeDigest(hashlib.sha256(raw).hexdigest(), _sha256(journal[-1]), len(entries))
    except HarnessFailure:
        raise
    except OSError as exc:
        raise HarnessFailure(HarnessVerdict.INCOMPLETE_EVIDENCE, 'cannot inventory recovery parent') from exc
```

Extract read-only parsing from `AppendOnlyAttemptLedger.load()` so legacy snapshot validation cannot call `_write_create_new`, checkpoint repair or journal repair. Keep current repair behavior behind an explicit v3-only method; do not add a permissive default.

- [ ] **Step 4: Implement fixed authorization and eligibility**

Add the exact approved object from the spec. `authorize_v3_recovery()` must:

1. match exactly one object by static input/profile/labels/hashes；
2. calculate the comparison tree digest before any loader；
3. compare exact tree/journal/count；
4. parse `attempt-0001` read-only；
5. parse all 20 sample payloads with exact v2 parser；
6. validate counts/order/identity/oracles/positive bytes/all dimensions exactly `()`；
7. verify no v2 versioned artifact；
8. repeat digest in `finally`。

- [ ] **Step 5: Run GREEN and real sealed read-only audit**

```powershell
uv run python -m pytest tests/rust_oracle/test_phase0_harness.py -q -k "comparison_tree or recovery_parent or authorized_parent" --basetemp C:\costing-v3-task2-green
uv run python -m pytest tests/rust_oracle -q --basetemp C:\costing-v3-task2-regression
uv run python -c "from pathlib import Path; from tests.rust_oracle.phase0_harness import comparison_tree_digest; d=comparison_tree_digest(Path('rust/target/perf-local/batches/09d6bb93ab04dda277e97f19dc8a270be91f2f8898a42f25d1d5bd745bdf0fd7')); assert (d.entry_count,d.sha256,d.journal_head_sha256)==(134,'8e961515bcac3afad271bb75eac9e439fdb18d1e8ba07b0fef7e210838796ccb','ae10e9d441ecebee9ba6cfb93a799f14a9085c75560103fedc9df6ff56b92c85')"
uv run python -c "from tests.rust_oracle.benchmark_protocol import ClosedBinaryLabel,ComparisonProfile; from tests.rust_oracle.phase0_harness import StaticComparisonInputs,authorize_v3_recovery; s=StaticComparisonInputs(pipeline='gb',comparison_profile=ComparisonProfile.PHASE0B_VS_PHASE0A,reference_label=ClosedBinaryLabel.PHASE0A,candidate_label=ClosedBinaryLabel.PHASE0B,phase0a_manifest_sha256='17faa1f08a0601fac52186ab64a4ebb4519312a259c59b7d64e69c748bab56df',input_sha256='6aa5e3e7fdc547ebaaef968eb5b95d4d630c4ec9915184f94346f60687b8e7ee',reference_sha256='f75f7ee17cc222765537f6bbe02f90e76cd041c55c8990b0261788e6fa63db56',candidate_sha256='d06470e4e7c9e6dc8f54efc9d26d996d3cbbbddec04cb7dffef6e6869802b629'); p=authorize_v3_recovery(s); assert (p.parent_inventory_entry_count,p.parent_comparison_tree_sha256,p.parent_journal_head_sha256)==(134,'8e961515bcac3afad271bb75eac9e439fdb18d1e8ba07b0fef7e210838796ccb','ae10e9d441ecebee9ba6cfb93a799f14a9085c75560103fedc9df6ff56b92c85')"
uv run python -m ruff check tests/rust_oracle/phase0_harness.py tests/rust_oracle/test_phase0_harness.py
git diff --check
```

Before and after the real audit, hash the entire comparison directory with the same helper and require equality. The audit command must also build the real `StaticComparisonInputs`, call `authorize_v3_recovery()`, and assert the returned provenance equals the fixed tree/terminal/journal/count—not only recompute the tree. No formal executable is run.

- [ ] **Step 6: Commit Task 2**

```powershell
Assert-ExactStagedPaths @()
git add -- tests/rust_oracle/phase0_harness.py tests/rust_oracle/test_phase0_harness.py
$Expected=@('tests/rust_oracle/phase0_harness.py','tests/rust_oracle/test_phase0_harness.py')
Assert-ExactStagedPaths $Expected
Invoke-NativeChecked { git diff --cached --check } 'Task 2 cached diff failed'
Invoke-NativeChecked { git commit -m "test(perf): authorize sealed v2 recovery" } 'Task 2 commit failed'
Assert-CleanRepository
```

## Task 3: Version the v3 Ledger and Add Durable Sample Start

**Files:**
- Modify: `tests/rust_oracle/phase0_harness.py`
- Test: `tests/rust_oracle/test_phase0_harness.py`

**Interfaces:**
- Consumes: Task 1 versions/provenance and Task 2 pure snapshot parser.
- Produces: `derive_v3_batch_id()`、v3 metadata exact schema、`sample-started`、closed open/resume/terminal state table、artifact+marker prepared/committed records。
- Intermediate-commit boundary: Task 3 only adds unconnected v3 APIs. Existing v2 `.create()`、formal runner、comparison-key call、legacy `derive_batch_id()`、`_load_current_protocol_ledger()` and evidence builder remain explicitly pinned to `LEGACY_PAIRED_PROTOCOL_VERSION`/`derive_v2_comparison_key()` until Task 5 atomically switches all call sites.

- [ ] **Step 1: Write failing v3 ledger state tests**

Add this local helper in `test_phase0_harness.py`:

```python
def _test_recovery_provenance() -> RecoveryProvenance:
    return RecoveryProvenance(
        parent_protocol_version=2,
        parent_comparison_key='0' * 64,
        parent_attempt=1,
        parent_terminal_sha256='1' * 64,
        parent_comparison_tree_sha256='2' * 64,
        parent_journal_head_sha256='3' * 64,
        parent_inventory_entry_count=134,
        reason=RecoveryReason.MISSING_FORMAL_SHEET_DIMENSIONS,
    )
```

```python
def test_v3_sample_started_is_durable_before_capture(monkeypatch, tmp_path) -> None:
    ledger = _v3_ledger(tmp_path)
    planned_sha = ledger.record_planned_output('wall', 1, 'reference', _planned_payload())
    observed: list[str] = []
    started_sha = ledger.record_sample_started(
        batch_id='b' * 64,
        metric='wall',
        global_round=1,
        role='reference',
        order=('reference', 'candidate'),
        input_sha256='3' * 64,
        binary_sha256='1' * 64,
        planned_output_record_sha256=planned_sha,
    )
    observed.extend(path.name for path in (ledger.attempt_directory / 'records').glob('*sample-started.json'))
    assert observed
    assert AppendOnlyAttemptLedger.load_read_only(ledger.attempt_directory, _identity()).sample_started_sha(
        'wall', 1, 'reference'
    ) == started_sha


def test_reloaded_plan_returns_original_record_sha_for_sample_start(tmp_path) -> None:
    ledger = _v3_ledger(tmp_path)
    original_plan_sha = ledger.record_planned_output('wall', 1, 'reference', _planned_payload())
    ledger.record_planned_output('wall', 1, 'candidate', _planned_payload(role='candidate'))
    reloaded = AppendOnlyAttemptLedger.open_v3_for_resume(ledger.attempt_directory, _identity())
    assert reloaded.record_planned_output('wall', 1, 'reference', _planned_payload()) == original_plan_sha


def test_v3_batch_id_matches_exact_13_key_payload_and_is_bound_everywhere(tmp_path) -> None:
    recovery = _test_recovery_provenance()
    identity = _identity()
    comparison_key = 'c' * 64
    expected_payload = {
        'protocol_version': 3,
        'comparison_key': comparison_key,
        'profile': 'phase0b-vs-phase0a',
        'pipeline': 'gb',
        'phase0a_manifest_sha256': '9' * 64,
        'input_sha256': identity.input_sha256,
        'reference_sha256': identity.reference_sha256,
        'candidate_sha256': identity.candidate_sha256,
        'git_head': identity.git_head,
        'repository_state_sha256': identity.repository_state_sha256,
        'machine_fingerprint_sha256': identity.machine_fingerprint_sha256,
        'recovery_provenance': _recovery_payload(recovery),
        'upstream_gate_provenance': None,
    }
    expected = hashlib.sha256(json.dumps(
        expected_payload, ensure_ascii=True, sort_keys=True, separators=(',', ':')
    ).encode('utf-8')).hexdigest()
    actual = derive_v3_batch_id(
        comparison_key=comparison_key,
        profile=ComparisonProfile.PHASE0B_VS_PHASE0A,
        pipeline='gb',
        phase0a_manifest_sha256='9' * 64,
        identity=identity,
        recovery_provenance=recovery,
        upstream_gate_provenance=None,
    )
    assert actual == expected
    ledger = _v3_ledger(tmp_path, batch_id=actual, comparison_key=comparison_key)
    assert ledger.batch_id == actual
    assert ledger.metadata['batch_id'] == actual
    plan_sha = ledger.record_planned_output('wall', 1, 'reference', _planned_payload(batch_id=actual))
    started_sha = ledger.record_sample_started(
        batch_id=actual,
        metric='wall',
        global_round=1,
        role='reference',
        order=('reference', 'candidate'),
        input_sha256=identity.input_sha256,
        binary_sha256=identity.reference_sha256,
        planned_output_record_sha256=plan_sha,
    )
    ledger.record_sample(
        'wall', 1, 'reference', _sample_payload(batch_id=actual), sample_started_record_sha256=started_sha
    )
    reloaded = AppendOnlyAttemptLedger.load_read_only(ledger.attempt_directory, identity)
    assert reloaded.planned_output('wall', 1, 'reference')['batch_id'] == actual
    assert reloaded.sample_started('wall', 1, 'reference')['batch_id'] == actual
    assert reloaded.sample('wall', 1, 'reference')['batch_id'] == actual
    assert reloaded.sample('wall', 1, 'reference')['sample_started_record_sha256'] == started_sha


@pytest.mark.parametrize('record_kind', ('planned-output', 'sample-started', 'sample'))
def test_v3_ledger_rejects_record_from_wrong_batch(record_kind, tmp_path) -> None:
    ledger = _v3_ledger(tmp_path, batch_id='b' * 64)
    with pytest.raises(HarnessFailure, match='batch'):
        _append_v3_record_for_test(ledger, record_kind=record_kind, batch_id='c' * 64)


@pytest.mark.parametrize(
    'mutation',
    ('parent_tree', 'parent_journal', 'gb_artifact', 'gb_marker', 'gb_commit'),
)
def test_v3_batch_id_changes_for_every_recovery_or_upstream_anchor(mutation) -> None:
    before, after = _v3_batch_id_inputs_with_one_mutation(mutation)
    assert derive_v3_batch_id(**before) != derive_v3_batch_id(**after)


def test_v3_batch_payload_rejects_unknown_key_string_number_and_bool_as_int() -> None:
    for invalid in _invalid_v3_batch_payloads():
        with pytest.raises(ValueError):
            _parse_exact_v3_batch_payload(invalid)


def test_v3_batch_id_rejects_non_literal_parent_inventory_count() -> None:
    with pytest.raises(ValueError):
        derive_v3_batch_id(**_v3_batch_id_inputs(parent_inventory_entry_count=135))


def test_started_without_sample_is_terminal_and_never_reinvokes_capture(monkeypatch, tmp_path) -> None:
    request = _v3_group_request(tmp_path)
    ledger = _v3_ledger_for_request(request)
    _record_started_without_sample(ledger, metric='wall', round_number=1, role='reference')
    monkeypatch.setattr(
        phase0_harness,
        'run_rust_normal_captured',
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('capture must not rerun')),
    )
    with pytest.raises(HarnessFailure) as caught:
        run_normal_wall_group(request)
    assert caught.value.verdict is HarnessVerdict.INCOMPLETE_EVIDENCE
    assert AppendOnlyAttemptLedger.load_read_only(ledger.attempt_directory, _identity()).terminal_verdict is HarnessVerdict.INCOMPLETE_EVIDENCE


@pytest.mark.parametrize(
    'terminal',
    tuple(
        verdict
        for verdict in HarnessVerdict
        if verdict not in (HarnessVerdict.VALIDATED, HarnessVerdict.CLEANUP_FAILED)
    ),
)
def test_v3_failure_terminal_never_creates_sampling_successor(terminal, tmp_path) -> None:
    ledger = _v3_ledger(tmp_path)
    ledger.finish(terminal)
    with pytest.raises(HarnessFailure):
        AppendOnlyAttemptLedger.create_v3_once(
            ledger.attempt_directory.parents[1],
            _identity(),
            comparison_key=ledger.comparison_key,
            phase0a_manifest_sha256='9' * 64,
            recovery_provenance=_test_recovery_provenance(),
            upstream_gate_provenance=None,
        )


def test_cleanup_only_successor_prohibits_all_benchmark_records(tmp_path) -> None:
    successor = _cleanup_only_v3_successor(tmp_path)
    for operation in (
        lambda: successor.record_planned_output('wall', 1, 'reference', {}),
        lambda: successor.record_sample_started(
            batch_id='b' * 64,
            metric='wall',
            global_round=1,
            role='reference',
            order=('reference', 'candidate'),
            input_sha256='3' * 64,
            binary_sha256='1' * 64,
            planned_output_record_sha256='4' * 64,
        ),
        lambda: successor.record_sample(
            'wall',
            1,
            'reference',
            {},
            sample_started_record_sha256='5' * 64,
        ),
    ):
        with pytest.raises(HarnessFailure, match='cleanup-only'):
            operation()


def test_committed_state_binds_artifact_and_marker_and_is_sealed(tmp_path) -> None:
    ledger = _prepared_v3_ledger(tmp_path)
    ledger.mark_evidence_committed(artifact_sha256='a' * 64, marker_sha256='b' * 64)
    loaded = AppendOnlyAttemptLedger.load_read_only(ledger.attempt_directory, _identity())
    assert loaded.state is AttemptState.EVIDENCE_COMMITTED
    with pytest.raises(HarnessFailure):
        loaded.record_sample(
            'wall',
            1,
            'reference',
            {},
            sample_started_record_sha256='5' * 64,
        )
```

- [ ] **Step 2: Verify RED**

```powershell
uv run python -m pytest tests/rust_oracle/test_phase0_harness.py -q -k "sample_started or original_record_sha or v3_batch_id or v3_batch_payload or sampling_successor or cleanup_only or committed_state" --basetemp C:\costing-v3-task3-red
```

- [ ] **Step 3: Implement v3 exact metadata and records**

Extend ledger fields with recovery/upstream/manifest SHA and started payloads. Use explicit version dispatch for metadata keys; v1/v2 read remains exact. Implement:

Remove Task 1's local alias in `phase0_harness.py` and import both `LEGACY_PAIRED_PROTOCOL_VERSION` and current `PAIRED_PROTOCOL_VERSION`. Every legacy read branch compares the former; every new create/append branch compares the latter. Before this commit, grep every current formal runner/create/load/builder call site and pin it explicitly to legacy; v3 methods exist but are not selected by the old runner.

Implement `derive_v3_batch_id()` from the exact 13-key payload shown in the test. Do not use `asdict()` and do not accept a caller-supplied protocol or batch ID. GB requires non-null recovery/null upstream; SK requires null recovery/non-null upstream. The resulting ID is stored unchanged in v3 metadata, planned/sample-started/sample records and later evidence.

```python
def record_sample_started(
    self,
    *,
    batch_id: str,
    metric: MetricName,
    global_round: int,
    role: BinaryRole,
    order: tuple[BinaryRole, BinaryRole],
    input_sha256: str,
    binary_sha256: str,
    planned_output_record_sha256: str,
) -> str:
    if self.protocol_version != PAIRED_PROTOCOL_VERSION or self.cleanup_only:
        raise HarnessFailure(HarnessVerdict.INCOMPLETE_EVIDENCE, 'sample start is not writable')
    key = (metric, global_round, role)
    if key in self._sample_started_payloads or key in self._sample_payloads:
        raise HarnessFailure(HarnessVerdict.INCOMPLETE_EVIDENCE, 'sample start is duplicate')
    payload = _validate_sample_started_payload({
        'batch_id': batch_id,
        'metric': metric,
        'global_round': global_round,
        'role': role,
        'order': list(order),
        'input_sha256': input_sha256,
        'binary_sha256': binary_sha256,
        'planned_output_record_sha256': planned_output_record_sha256,
    })
    digest = self._append('sample-started', payload)
    self._sample_started_payloads[key] = payload | {'record_sha256': digest}
    return digest
```

`record_sample()` requires matching started SHA for v3; legacy v1/v2 loader retains old payload grammar read-only.

Add `_plan_record_sha256s: dict[tuple[MetricName, int, BinaryRole], str]`. Loader population records each original planned-output record SHA; idempotent `record_planned_output()` returns that original SHA, never the current journal head. This is required so resumed `sample-started.planned_output_record_sha256` cannot point to an unrelated later record.

`open_v3_for_resume()` is a named current-protocol writable loader used only after the state-aware repository gate; it never accepts v1/v2. `load_read_only()` stays side-effect free for audit/inspection.

- [ ] **Step 4: Implement closed state transitions**

Create a pure state classifier returning one of:

```text
NEW
SAMPLING_RESUMABLE
STARTED_WITHOUT_SAMPLE
CLEANUP_COMPLETE
EVIDENCE_PREPARED
EVIDENCE_COMMITTED
CLEANUP_ONLY
FAILED_TERMINAL
INVALID
```

`create_v3_once()` consumes this classifier exactly as the spec table; do not reuse v2 environment-recovery branches.

- [ ] **Step 5: Run GREEN, v1/v2 regression and commit**

```powershell
uv run python -m pytest tests/rust_oracle/test_phase0_harness.py -q --basetemp C:\costing-v3-task3-green
uv run python -m pytest tests/rust_oracle/test_benchmark_protocol.py -q --basetemp C:\costing-v3-task3-protocol
uv run python -m pytest tests/rust_oracle -q --basetemp C:\costing-v3-task3-regression
uv run python -m ruff check tests/rust_oracle/phase0_harness.py tests/rust_oracle/test_phase0_harness.py
uv run python -m ruff format tests/rust_oracle/phase0_harness.py tests/rust_oracle/test_phase0_harness.py --check
git diff --check
Assert-ExactStagedPaths @()
git add -- tests/rust_oracle/phase0_harness.py tests/rust_oracle/test_phase0_harness.py
$Expected=@('tests/rust_oracle/phase0_harness.py','tests/rust_oracle/test_phase0_harness.py')
Assert-ExactStagedPaths $Expected
Invoke-NativeChecked { git diff --cached --check } 'Task 3 cached diff failed'
Invoke-NativeChecked { git commit -m "test(perf): version formal recovery ledger" } 'Task 3 commit failed'
Assert-CleanRepository
```

## Task 4: Implement Benchmark Evidence Schema v3

**Files:**
- Modify: `tests/rust_oracle/evidence.py`
- Test: `tests/rust_oracle/test_evidence.py`

**Interfaces:**
- Consumes: Task 1 provenance dataclasses and protocol constants.
- Produces: schema 1/2/3 exact reader、named v3 builder、v3 basename、recovery/upstream typed evidence；Task 5 performs the final formal-writer cutover。

- [ ] **Step 1: Write failing schema tests**

```python
def test_schema_v3_gb_requires_recovery_and_null_upstream() -> None:
    manifest = _benchmark_manifest_v3(pipeline='gb', recovery=_recovery_evidence(), upstream=None)
    artifact = EvidenceSanitizer.closed_policy().build_benchmark_manifest_v3(manifest)
    rebuilt = EvidenceSanitizer.closed_policy().read_benchmark_manifest(artifact.file_name, artifact.content.encode())
    assert rebuilt.schema_version == 3
    assert rebuilt.protocol_version == 3
    assert rebuilt.comparison_key == manifest.comparison_key
    assert rebuilt.batch_id == manifest.batch_id
    assert rebuilt.recovery_provenance == manifest.recovery_provenance
    assert rebuilt.upstream_gate_provenance is None


def test_schema_v3_sk_requires_null_recovery_and_upstream_gate() -> None:
    manifest = _benchmark_manifest_v3(pipeline='sk', recovery=None, upstream=_upstream_evidence())
    artifact = EvidenceSanitizer.closed_policy().build_benchmark_manifest_v3(manifest)
    rebuilt = EvidenceSanitizer.closed_policy().read_benchmark_manifest(artifact.file_name, artifact.content.encode())
    assert rebuilt.upstream_gate_provenance == manifest.upstream_gate_provenance


@pytest.mark.parametrize('schema_version', (1, 2))
def test_legacy_schema_rebuild_stays_byte_stable(schema_version: int) -> None:
    artifact = _legacy_benchmark_artifact(schema_version)
    parsed = EvidenceSanitizer.closed_policy().read_benchmark_manifest(artifact.file_name, artifact.content.encode())
    assert EvidenceSanitizer.closed_policy().rebuild_audit_benchmark_manifest(parsed) == artifact
    with pytest.raises(ValueError, match='current schema'):
        EvidenceSanitizer.closed_policy().build_benchmark_manifest_v3(parsed)


@pytest.mark.parametrize(
    ('field', 'value'),
    (
        ('parent_comparison_tree_sha256', 'A' * 64),
        ('parent_inventory_entry_count', True),
        ('validated_commit_sha', 'f' * 39),
        ('artifact_basename', '../escape.json'),
    ),
)
def test_v3_provenance_rejects_non_closed_values(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        _build_manifest_with_mutated_provenance(field, value)
```

Add the following parameterized tests in this step, rather than leaving them as prose:

```python
@pytest.mark.parametrize('shape', ('duplicate-key', 'extra-key', 'missing-key'))
def test_schema_v3_rejects_non_exact_json_shape(shape) -> None:
    raw = _mutated_v3_json_shape(_benchmark_manifest_v3(), shape)
    with pytest.raises(ValueError):
        EvidenceSanitizer.closed_policy().read_benchmark_manifest('benchmark-v3-' + 'a' * 16 + '.json', raw)


def test_schema_v3_rejects_canary_in_every_string_field() -> None:
    for field_path, raw in _v3_string_field_canaries(_benchmark_manifest_v3()):
        with pytest.raises(ValueError, match='sensitive'):
            EvidenceSanitizer.closed_policy().read_benchmark_manifest('benchmark-v3-' + 'a' * 16 + '.json', raw)


def test_v3_marker_and_basename_bind_exact_artifact_sha() -> None:
    artifact = EvidenceSanitizer.closed_policy().build_benchmark_manifest_v3(_benchmark_manifest_v3())
    marker = EvidenceSanitizer.closed_policy().build_batch_marker(artifact)
    assert artifact.file_name == 'benchmark-v3-' + str(artifact.payload['comparison_key'])[:16] + '.json'
    assert marker.value.artifact_basename == artifact.file_name
    assert marker.value.artifact_sha256 == hashlib.sha256(artifact.content.encode('utf-8')).hexdigest()
```

Test helpers `_mutated_v3_json_shape()` and `_v3_string_field_canaries()` canonical-encode a valid production-built artifact after applying exactly one closed mutation; duplicate-key uses a handcrafted repeated top-level key because a Python dict cannot represent duplicates.

- [ ] **Step 2: Verify RED**

```powershell
uv run python -m pytest tests/rust_oracle/test_evidence.py -q -k "schema_v3 or legacy_schema or provenance or exact_json_shape or canary or marker_and_basename" --basetemp C:\costing-v3-task4-red
```

- [ ] **Step 3: Implement exact schema dispatch**

Add this exact ordered tuple; reader checks it after duplicate-key detection:

```python
_BENCHMARK_V3_KEYS = (
    'schema_version', 'protocol_version', 'comparison_key', 'batch_id', 'profile', 'pipeline',
    'input_alias', 'input_sha256',
    'reference_label', 'reference_exe_sha256', 'candidate_label', 'candidate_exe_sha256', 'machine',
    'attempt_count', 'prior_safe_verdicts', 'ledger_head_sha256', 'first_group_sha256',
    'expanded_group_sha256', 'rounds', 'metrics', 'runtime_counts', 'sheet_dimensions', 'output_bytes',
    'mismatches', 'local_log_sha256', 'direction_diagnostics', 'recovery_provenance',
    'upstream_gate_provenance', 'verdict',
)
```

Extend manifest dataclass:

Import both version constants. Schema v2 exact reader/rebuilder compares `LEGACY_PAIRED_PROTOCOL_VERSION`; the new named v3 builder compares current `PAIRED_PROTOCOL_VERSION`. Keep existing `build_benchmark_manifest()` as a clearly commented transitional v2 wrapper until Task 5 so the intermediate commit remains green.

```python
@dataclass(frozen=True)
class BenchmarkManifestEvidence:
    schema_version: Literal[1, 2, 3]
    # existing fields unchanged
    protocol_version: Literal[2, 3] | None = None
    comparison_key: str | None = None  # required only by schema 3
    batch_id: str | None = None  # required only by schema 3
    recovery_provenance: RecoveryProvenanceEvidence | None = None
    upstream_gate_provenance: UpstreamGateProvenanceEvidence | None = None
```

Split builder methods:

```python
def build_benchmark_manifest_v3(self, value):
    if value.schema_version != CURRENT_BENCHMARK_SCHEMA_VERSION or value.protocol_version != PAIRED_PROTOCOL_VERSION:
        raise ValueError('formal benchmark builder requires schema/protocol version 3')
    return self._build_v3(value)

def rebuild_audit_benchmark_manifest(self, value):
    if value.schema_version == 1:
        return self._build_v1(value)
    if value.schema_version == 2 and value.protocol_version == 2:
        return self._build_v2(value)
    if value.schema_version == 3 and value.protocol_version == 3:
        return self._build_v3(value)
    raise ValueError('unsupported benchmark schema/protocol pair')
```

Do not route v2 through current version constants. Task 5 replaces the transitional wrapper with the final v3-only `build_benchmark_manifest()` after the runner cutover.

- [ ] **Step 4: Run GREEN and sanitizer regression**

```powershell
uv run python -m pytest tests/rust_oracle/test_evidence.py -q --basetemp C:\costing-v3-task4-green
uv run python -m tests.rust_oracle.evidence scan --root docs/performance
uv run python -m ruff check tests/rust_oracle/evidence.py tests/rust_oracle/test_evidence.py
uv run python -m ruff format tests/rust_oracle/evidence.py tests/rust_oracle/test_evidence.py --check
git diff --check
```

- [ ] **Step 5: Commit Task 4**

```powershell
Assert-ExactStagedPaths @()
git add -- tests/rust_oracle/evidence.py tests/rust_oracle/test_evidence.py
$Expected=@('tests/rust_oracle/evidence.py','tests/rust_oracle/test_evidence.py')
Assert-ExactStagedPaths $Expected
Invoke-NativeChecked { git diff --cached --check } 'Task 4 cached diff failed'
Invoke-NativeChecked { git commit -m "test(evidence): add benchmark schema v3" } 'Task 4 commit failed'
Assert-CleanRepository
```

## Task 5: Integrate Fresh v3 Sampling and Optional-Stopping Protection

**Files:**
- Modify: `tests/rust_oracle/phase0_harness.py`
- Modify: `tests/rust_oracle/evidence.py`（完成 formal builder cutover）
- Test: `tests/rust_oracle/test_phase0_harness.py`
- Test: `tests/rust_oracle/test_evidence.py`

**Interfaces:**
- Consumes: Tasks 1–4 identity/ledger/schema.
- Produces: static-input→state-aware gate→fresh runner flow and unique outer cleanup/terminal ownership。

- [ ] **Step 1: Write failing interruption and no-v2-reuse tests**

```python
@pytest.mark.parametrize('metric', ('wall', 'pws'))
@pytest.mark.parametrize('role', ('reference', 'candidate'))
def test_interruption_after_sample_started_is_terminal_and_never_retried(
    metric, role, monkeypatch, tmp_path
) -> None:
    request = _v3_group_request(tmp_path, metric=metric)
    calls = 0

    def interrupt(*args, **kwargs):
        nonlocal calls
        calls += 1
        snapshot = AppendOnlyAttemptLedger.load_read_only(request.attempt_directory, _identity())
        started_sha = snapshot.sample_started_sha(metric, request.plans[0].global_round, role)
        assert started_sha is not None
        assert snapshot.checkpoint_head_sha256 is not None
        assert snapshot.journal_head_sha256 is not None
        assert snapshot.sample_started(metric, request.plans[0].global_round, role)[
            'planned_output_record_sha256'
        ] == snapshot.planned_output_record_sha(metric, request.plans[0].global_round, role)
        raise KeyboardInterrupt('simulated after durable start')

    _install_metric_capture(monkeypatch, metric, interrupt)
    with pytest.raises(HarnessFailure) as first:
        _run_metric_group(request)
    assert first.value.verdict is HarnessVerdict.INCOMPLETE_EVIDENCE
    _install_metric_capture(monkeypatch, metric, lambda *a, **k: (_ for _ in ()).throw(AssertionError('retry')))
    with pytest.raises(HarnessFailure):
        _run_metric_group(request)
    assert calls == 1


def test_v3_verdict_never_reads_v2_metric_values(monkeypatch, tmp_path) -> None:
    request = _formal_v3_request(tmp_path)
    _install_authorized_parent_with_valid_but_extreme_metrics(tmp_path, wall='9999', pws=1)
    fresh = _install_fresh_groups(monkeypatch, wall_ratio='0.90', pws_ratio='9.00')
    gate_inputs = _install_closed_profile_gate_spy(monkeypatch)
    result = run_paired_normal_batch(request)
    assert result.verdict is HarnessVerdict.VALIDATED
    assert fresh.calls == 20
    assert gate_inputs.wall is fresh.wall
    assert gate_inputs.pws is fresh.pws
    assert result.evidence.rounds == fresh.rounds
    assert not any('metric' in field.name or 'median' in field.name for field in dataclasses.fields(RecoveryProvenance))


def test_phase0b_v3_uses_wall_1_02(monkeypatch, tmp_path) -> None:
    request = _formal_v3_request(tmp_path)
    _install_fresh_groups(monkeypatch, wall_ratio='1.021', pws_ratio='1.0')
    with pytest.raises(HarnessFailure) as caught:
        run_paired_normal_batch(request)
    assert caught.value.verdict is HarnessVerdict.CANDIDATE_FAILED
    assert 'wall' in str(caught.value)


def test_phase0b_v3_has_no_direct_pws_gate(monkeypatch, tmp_path) -> None:
    request = _formal_v3_request(tmp_path)
    groups = _install_fresh_groups(monkeypatch, wall_ratio='0.90', pws_ratio='9.0')
    result = run_paired_normal_batch(request)
    assert result.verdict is HarnessVerdict.VALIDATED
    assert result.pws.direction_diagnostic is not None
    assert groups.pws_is_fresh_same_batch_and_oracle_valid
```

Add explicit expansion/failure precedence coverage:

```python
@pytest.mark.parametrize('first_group_ratio', ('0.999', '1.021'))
def test_phase0b_v3_near_boundary_expands_wall_and_pws_from_round_six(
    first_group_ratio, monkeypatch, tmp_path
) -> None:
    request = _formal_v3_request(tmp_path)
    calls = _install_group_sequence(monkeypatch, first_group_ratio=first_group_ratio, combined_ratio='1.00')
    result = run_paired_normal_batch(request)
    assert result.verdict is HarnessVerdict.VALIDATED
    assert calls == [('wall', 1), ('pws', 1), ('wall', 6), ('pws', 6)]


def test_phase0b_v3_combined_closed_failure_precedes_direction_conflict(monkeypatch, tmp_path) -> None:
    request = _formal_v3_request(tmp_path)
    _install_direction_conflict_with_combined_wall_failure(monkeypatch)
    with pytest.raises(HarnessFailure) as caught:
        run_paired_normal_batch(request)
    assert caught.value.verdict is HarnessVerdict.CANDIDATE_FAILED
```

- [ ] **Step 2: Verify RED**

```powershell
uv run python -m pytest tests/rust_oracle/test_phase0_harness.py -q -k "interruption_after_sample_started or never_reads_v2 or wall_1_02 or no_direct_pws_gate or near_boundary_expands or combined_closed_failure" --basetemp C:\costing-v3-task5-red
```

- [ ] **Step 3: Implement state-aware runner order**

Refactor `run_paired_normal_batch()` into explicit phases without changing CLI arguments:

```python
static = _capture_static_comparison_inputs(request)
if request.pipeline != 'gb':
    # Task 7 atomically enables SK only after committed-GB provenance exists.
    raise HarnessFailure(HarnessVerdict.INCOMPLETE_EVIDENCE, 'SK v3 upstream gate is not enabled yet')
recovery = authorize_v3_recovery(static)
upstream = None
comparison_key = derive_v3_comparison_key(
    pipeline=static.pipeline,
    comparison_profile=static.comparison_profile,
    reference_label=static.reference_label,
    candidate_label=static.candidate_label,
    phase0a_manifest_sha256=static.phase0a_manifest_sha256,
    input_sha256=static.input_sha256,
    reference_sha256=static.reference_sha256,
    candidate_sha256=static.candidate_sha256,
    recovery_provenance=recovery,
    upstream_gate_provenance=upstream,
)
state = _inspect_v3_state_read_only(request, comparison_key)
_apply_state_aware_repository_policy(request, state)
if state.requires_zero_process_recovery:
    return _resume_v3_non_sampling_state(request, state)
identity = _capture_full_identity_after_state_gate(request, state)
batch_id = derive_v3_batch_id(
    comparison_key=comparison_key,
    profile=static.comparison_profile,
    pipeline=static.pipeline,
    phase0a_manifest_sha256=static.phase0a_manifest_sha256,
    identity=identity,
    recovery_provenance=recovery,
    upstream_gate_provenance=upstream,
)
ledger = AppendOnlyAttemptLedger.open_or_create_v3_once(
    request.attempt_ledger_root,
    identity,
    comparison_key=comparison_key,
    phase0a_manifest_sha256=static.phase0a_manifest_sha256,
    recovery_provenance=recovery,
    upstream_gate_provenance=upstream,
    batch_id=batch_id,
)
```

Task 5 also produces and tests `derive_expected_formal_v3_identity()` for GB. It executes only static capture, closed parent authorization, comparison key/state inspection and deterministic basename derivation; it never creates directories, attempts or evidence. Task 7 extends the same function to SK after implementing committed-GB provenance. Task 6 adds `inspect_expected_formal_v3_state()` and tests every returned state/`child_process_allowed` combination without writes.

In both wall/PWS runners, append `sample-started` immediately before the child call. Inner capture maps errors but never performs cleanup/terminal; outer paired runner owns cleanup and sealing exactly once.

In this single atomic commit, switch every formal GB call site together: v3 comparison key, v3 batch ID, `open_or_create_v3_once()`, current ledger loader, and `build_benchmark_manifest_v3()`. Replace Task 4's transitional v2 `build_benchmark_manifest()` wrapper with the v3 builder; keep v1/v2 only under `rebuild_audit_benchmark_manifest()`. The temporary SK fail-closed branch is removed only in Task 7.

- [ ] **Step 4: Run GREEN and focused wall/PWS suites**

```powershell
uv run python -m pytest tests/rust_oracle/test_phase0_harness.py tests/rust_oracle/test_peak_working_set.py tests/rust_oracle/test_evidence.py -q --basetemp C:\costing-v3-task5-green
uv run python -m pytest tests/rust_oracle -q --basetemp C:\costing-v3-task5-regression
uv run python -m ruff check tests/rust_oracle/phase0_harness.py tests/rust_oracle/test_phase0_harness.py tests/rust_oracle/test_peak_working_set.py
git diff --check
```

- [ ] **Step 5: Commit Task 5**

```powershell
Assert-ExactStagedPaths @()
git add -- tests/rust_oracle/phase0_harness.py tests/rust_oracle/evidence.py tests/rust_oracle/test_phase0_harness.py tests/rust_oracle/test_peak_working_set.py tests/rust_oracle/test_evidence.py
$Expected=@('tests/rust_oracle/evidence.py','tests/rust_oracle/phase0_harness.py','tests/rust_oracle/test_evidence.py','tests/rust_oracle/test_peak_working_set.py','tests/rust_oracle/test_phase0_harness.py')
Assert-ExactStagedPaths $Expected
Invoke-NativeChecked { git diff --cached --check } 'Task 5 cached diff failed'
Invoke-NativeChecked { git commit -m "test(perf): run fresh protocol v3 samples" } 'Task 5 commit failed'
Assert-CleanRepository
```

## Task 6: Implement Cleanup-Complete, Prepared and Committed Publication Recovery

**Files:**
- Modify: `tests/rust_oracle/phase0_harness.py`
- Test: `tests/rust_oracle/test_phase0_harness.py`
- Test: `tests/rust_oracle/test_evidence.py`

**Interfaces:**
- Consumes: v3 ledger/schema and runner state inspection.
- Produces: exact state-aware dirty allowlist、artifact-only/marker-last recovery、artifact+marker committed recovery、dual-SHA seal。

- [ ] **Step 1: Write failing crash-window matrix**

```python
@pytest.mark.parametrize(
    ('state', 'disk_shape', 'expected_action'),
    (
        ('cleanup-complete', 'none', 'prepare'),
        ('prepared', 'none', 'publish-both'),
        ('prepared', 'artifact-only', 'publish-marker'),
        ('prepared', 'both', 'commit-record'),
        ('committed', 'both', 'readback'),
    ),
)
def test_publication_recovery_is_zero_process_and_exact(
    state, disk_shape, expected_action, monkeypatch, tmp_path
) -> None:
    request, ledger, paths = _publication_state(tmp_path, state, disk_shape)
    monkeypatch.setattr(
        phase0_harness,
        'run_rust_normal_captured',
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('publication recovery ran a process')),
    )
    observed = _observe_publication_actions(monkeypatch)
    result = run_paired_normal_batch(request)
    assert result.verdict is HarnessVerdict.VALIDATED
    assert expected_action in observed


@pytest.mark.parametrize(
    ('disk_shape', 'expected_verdict'),
    (
        ('marker-only', HarnessVerdict.INCOMPLETE_EVIDENCE),
        ('artifact-drift', HarnessVerdict.SENSITIVE_EVIDENCE),
        ('marker-drift', HarnessVerdict.SENSITIVE_EVIDENCE),
        ('extra-dirty', HarnessVerdict.ENVIRONMENT_DRIFT),
    ),
)
def test_publication_recovery_fails_closed_without_overwrite(
    disk_shape, expected_verdict, monkeypatch, tmp_path
) -> None:
    request, ledger, paths = _publication_state(tmp_path, 'prepared', disk_shape)
    before = {path: path.read_bytes() for path in paths.all_owned if path.exists()}
    observed = _observe_publication_actions(monkeypatch)
    with pytest.raises(HarnessFailure) as caught:
        run_paired_normal_batch(request)
    assert caught.value.verdict is expected_verdict
    assert {path: path.read_bytes() for path in before} == before
    assert observed.child_process_calls == 0
    assert observed.overwrites == 0


def test_prepared_oserror_reuses_identical_bytes_on_resume(monkeypatch, tmp_path) -> None:
    request = _completed_v3_sampling_request(tmp_path)
    _fail_first_publication_with_oserror(monkeypatch)
    with pytest.raises(PreparedPublicationPending):
        run_paired_normal_batch(request)
    prepared = _load_v3_ledger(request).prepared_evidence()
    assert prepared is not None
    expected_artifact = prepared['artifact_content'].encode('utf-8')
    _restore_publication(monkeypatch)
    result = run_paired_normal_batch(request)
    assert result.verdict is HarnessVerdict.VALIDATED
    assert request.evidence_path.read_bytes() == expected_artifact


@pytest.mark.parametrize('failure', ('typed-rebuild', 'sanitizer', 'staged-scan'))
def test_deterministic_publication_failure_removes_owned_partial_and_seals_sensitive(
    failure, monkeypatch, tmp_path
) -> None:
    request, ledger, paths = _publication_state(tmp_path, 'prepared', 'artifact-only')
    _install_deterministic_publication_failure(monkeypatch, failure)
    with pytest.raises(HarnessFailure) as caught:
        run_paired_normal_batch(request)
    assert caught.value.verdict is HarnessVerdict.SENSITIVE_EVIDENCE
    assert not paths.artifact.exists() and not paths.marker.exists()
    assert _load_v3_ledger(request).terminal_verdict is HarnessVerdict.SENSITIVE_EVIDENCE


def test_cleanup_failed_is_sealed_once_by_outer_owner(monkeypatch, tmp_path) -> None:
    request = _completed_v3_sampling_request(tmp_path)
    _install_cleanup_failure(monkeypatch)
    with pytest.raises(HarnessFailure) as caught:
        run_paired_normal_batch(request)
    assert caught.value.verdict is HarnessVerdict.CLEANUP_FAILED
    snapshot = _load_v3_ledger(request)
    assert snapshot.terminal_verdict is HarnessVerdict.CLEANUP_FAILED
    assert snapshot.primary_verdict is HarnessVerdict.VALIDATED
    assert snapshot.terminal_record_count == 1
```

- [ ] **Step 2: Verify RED**

```powershell
uv run python -m pytest tests/rust_oracle/test_phase0_harness.py -q -k "publication_recovery or prepared_oserror or deterministic_publication_failure or cleanup_failed_is_sealed" --basetemp C:\costing-v3-task6-red
```

- [ ] **Step 3: Implement publication state policy**

Implement `_apply_state_aware_repository_policy()` before full identity capture. For prepared/committed, subtract only exact expected artifact/marker status entries, reject staged modifications and require byte equality. New sampling remains strict-clean.

Implement success order and exception ownership exactly:

```python
def finalize_or_recover_publication(request, ledger):
    if not ledger.cleanup_complete:
        cleanup_failures = _cleanup_all_registered(_formal_registered_paths(request, ledger))
        if cleanup_failures:
            # outer runner alone writes CLEANUP_FAILED and preserves the primary verdict
            raise CleanupFailure(cleanup_failures)
        ledger.mark_cleanup_complete()

    if not ledger.evidence_prepared:
        try:
            artifact = _build_and_sanitize_v3_from_ledger(request, ledger)
            marker = _derive_exact_marker(artifact)
        except DeterministicEvidenceError as exc:
            _remove_owned_partial_publication(request, ledger)
            raise HarnessFailure(HarnessVerdict.SENSITIVE_EVIDENCE, 'evidence preparation failed') from exc
        ledger.prepare_evidence(**_exact_prepared_payload(artifact, marker))

    artifact, marker = _rebuild_exact_prepared_bytes(ledger)
    try:
        _apply_state_aware_repository_policy(request, ledger, artifact, marker)
        _publish_marker_last_without_overwrite(request, artifact, marker)
        _typed_readback_and_scan(request, artifact, marker)
        ledger.mark_evidence_committed(
            artifact_sha256=_content_sha(artifact.content),
            marker_sha256=_content_sha(marker.content),
        )
    except DeterministicEvidenceError as exc:
        _remove_owned_partial_publication(request, ledger)
        ledger.finish(HarnessVerdict.SENSITIVE_EVIDENCE)
        raise HarnessFailure(HarnessVerdict.SENSITIVE_EVIDENCE, 'prepared evidence validation failed') from exc
    except (OSError, KeyboardInterrupt, SystemExit) as exc:
        # fixed bytes remain in prepared state; CLI exits nonzero and the next invocation is zero-process recovery
        raise PreparedPublicationPending('prepared evidence publication is incomplete') from exc

    return _typed_committed_result(ledger, artifact, marker)
```

`PreparedPublicationPending` is caught only at the CLI boundary, prints a sanitized structured failure and exits nonzero; it is not converted to a terminal. The outer `run_paired_normal_batch()` catches `CleanupFailure`, records exactly one `CLEANUP_FAILED` terminal with `primary_verdict`, and performs no publication. Do not re-run performance gates during recovery.

- [ ] **Step 4: Run GREEN and all evidence tests**

```powershell
uv run python -m pytest tests/rust_oracle/test_phase0_harness.py tests/rust_oracle/test_evidence.py -q --basetemp C:\costing-v3-task6-green
uv run python -m tests.rust_oracle.evidence scan --root docs/performance
uv run python -m ruff check tests/rust_oracle
git diff --check
```

- [ ] **Step 5: Commit Task 6**

```powershell
Assert-ExactStagedPaths @()
git add -- tests/rust_oracle/phase0_harness.py tests/rust_oracle/test_phase0_harness.py tests/rust_oracle/test_evidence.py
$Expected=@('tests/rust_oracle/phase0_harness.py','tests/rust_oracle/test_evidence.py','tests/rust_oracle/test_phase0_harness.py')
Assert-ExactStagedPaths $Expected
Invoke-NativeChecked { git diff --cached --check } 'Task 6 cached diff failed'
Invoke-NativeChecked { git commit -m "test(perf): recover v3 evidence publication" } 'Task 6 commit failed'
Assert-CleanRepository
```

## Task 7: Bind GB Evidence Commit into SK Identity and Evidence

**Files:**
- Modify: `tests/rust_oracle/phase0_harness.py`
- Modify: `tests/rust_oracle/evidence.py`
- Test: `tests/rust_oracle/test_phase0_harness.py`
- Test: `tests/rust_oracle/test_evidence.py`

**Interfaces:**
- Consumes: Task 1 upstream type and Task 4 schema.
- Produces: automatic `derive_committed_gb_v3_provenance()` and GB→commit→SK gate。

- [ ] **Step 1: Write failing Git provenance tests**

Use a temporary Git repository with one parent commit and an evidence-only child:

```python
def test_sk_upstream_gate_binds_exact_gb_artifact_marker_and_commit(monkeypatch, tmp_path) -> None:
    fixture = _git_repo_with_gb_evidence_only_commit(tmp_path)
    static = _sk_static_inputs(fixture)
    result = derive_committed_gb_v3_provenance(static)
    assert result.artifact_sha256 == _sha256(fixture.artifact)
    assert result.marker_sha256 == _sha256(fixture.marker)
    assert result.validated_commit_sha == fixture.head


@pytest.mark.parametrize(
    'mutation',
    ('extra-commit-path', 'merge-commit', 'artifact-worktree-drift', 'marker-worktree-drift', 'untracked-extra'),
)
def test_sk_upstream_gate_rejects_non_evidence_only_or_drift(mutation, monkeypatch, tmp_path) -> None:
    fixture = _git_repo_with_gb_evidence_only_commit(tmp_path)
    _mutate_git_evidence_state(fixture, mutation)
    with pytest.raises(HarnessFailure):
        derive_committed_gb_v3_provenance(_sk_static_inputs(fixture))


def test_sk_runner_never_starts_without_committed_gb_gate(monkeypatch, tmp_path) -> None:
    request = _formal_v3_request(tmp_path, pipeline='sk')
    calls = _install_process_counter(monkeypatch)
    with pytest.raises(HarnessFailure) as caught:
        run_paired_normal_batch(request)
    assert caught.value.verdict is HarnessVerdict.INCOMPLETE_EVIDENCE
    assert calls.count == 0
```

Add a cross-layer equality test:

```python
def test_sk_upstream_provenance_is_identical_in_key_ledger_and_manifest(monkeypatch, tmp_path) -> None:
    fixture = _git_repo_with_gb_evidence_only_commit(tmp_path)
    request = _formal_v3_request(fixture.repo, pipeline='sk')
    upstream = derive_committed_gb_v3_provenance(_sk_static_inputs(fixture))
    _install_successful_fresh_groups(monkeypatch)
    result = run_paired_normal_batch(request)
    ledger = AppendOnlyAttemptLedger.load_read_only(result.attempt.attempt_directory, _identity())
    manifest = EvidenceSanitizer.closed_policy().read_benchmark_manifest(
        request.evidence_path.name,
        request.evidence_path.read_bytes(),
    )
    assert ledger.upstream_gate_provenance == upstream
    assert manifest.upstream_gate_provenance == UpstreamGateProvenanceEvidence.from_protocol(upstream)
    assert result.attempt.comparison_key == _v3_sk_key(upstream=upstream)
    assert result.attempt.batch_id == ledger.batch_id == manifest.batch_id
    assert result.attempt.batch_id == derive_v3_batch_id(
        comparison_key=result.attempt.comparison_key,
        profile=request.comparison_profile,
        pipeline='sk',
        phase0a_manifest_sha256=ledger.phase0a_manifest_sha256,
        identity=ledger.identity,
        recovery_provenance=None,
        upstream_gate_provenance=upstream,
    )
```

- [ ] **Step 2: Verify RED**

```powershell
uv run python -m pytest tests/rust_oracle/test_phase0_harness.py tests/rust_oracle/test_evidence.py -q -k "upstream_gate or committed_gb" --basetemp C:\costing-v3-task7-red
```

- [ ] **Step 3: Implement automatic upstream derivation**

The function must derive expected GB identity and names itself, typed-read GB schema 3 `VALIDATED`, rebuild marker, require current HEAD single-parent and exact diff paths, and compare `git show HEAD:<path>` bytes with disk. No CLI parameter is added.

Remove Task 5's temporary SK fail-closed branch and atomically enable the SK runner. Extend `derive_expected_formal_v3_identity()` so SK derives committed GB provenance itself; keep it pure/read-only. Store the same object in SK comparison key, 13-key batch payload, ledger metadata, every sample-started identity context and manifest. For GB, upstream is `None`; for SK, recovery is `None`.

- [ ] **Step 4: Run GREEN and CLI argument closure test**

```powershell
uv run python -m pytest tests/rust_oracle/test_phase0_harness.py tests/rust_oracle/test_evidence.py -q --basetemp C:\costing-v3-task7-green
uv run python -c "from tests.rust_oracle.phase0_harness import _argument_parser; p=next(a for a in _argument_parser()._actions if a.dest=='command').choices['paired']; names={a.dest for a in p._actions}; assert not names.intersection({'protocol_version','recovery_parent','terminal_sha256','upstream_gate'})"
uv run python -m ruff check tests/rust_oracle
git diff --check
```

- [ ] **Step 5: Commit Task 7**

```powershell
Assert-ExactStagedPaths @()
git add -- tests/rust_oracle/phase0_harness.py tests/rust_oracle/evidence.py tests/rust_oracle/test_phase0_harness.py tests/rust_oracle/test_evidence.py
$Expected=@('tests/rust_oracle/evidence.py','tests/rust_oracle/phase0_harness.py','tests/rust_oracle/test_evidence.py','tests/rust_oracle/test_phase0_harness.py')
Assert-ExactStagedPaths $Expected
Invoke-NativeChecked { git diff --cached --check } 'Task 7 cached diff failed'
Invoke-NativeChecked { git commit -m "test(perf): bind gb evidence into sk v3" } 'Task 7 commit failed'
Assert-CleanRepository
```

## Task 8: Synchronize Docs, Run Full Gates and Complete Independent Reviews

**Files:**
- Modify: `docs/performance/README.md`
- Modify: `docs/superpowers/specs/2026-07-11-rust-output-ingest-continuous-performance-design.md`
- Modify: `docs/superpowers/plans/2026-07-11-rust-output-phase-0a-3-writer-optimization.md`
- Read/test: all Task 1–7 files

**Interfaces:**
- Consumes: reviewed v3 runtime behavior.
- Produces: current docs, full green gate, review-ready formal implementation commit state。

- [ ] **Step 1: Update documentation minimally**

README must say current paired writer is protocol/schema 3, v1/v2 remain audit-only, GB current recovery is closed to the exact parent snapshot, and SK binds GB evidence commit. Update original spec status from “尚未进入实现” to the actual phase and link this v3 spec. Update Phase 0A–3 plan handoff to require committed GB/SK v3 evidence without changing Phase 1 thresholds.

- [ ] **Step 2: Run the full verification gate with short Windows temp roots**

For each external basetemp, first assert the exact path does not exist. The same fail-fast script owns cleanup in `finally`; it resolves and removes only the two literal allowlisted roots after proving their parent is exactly `C:\` and their leaf starts with `costing-v3-`. No wildcard deletion is allowed.

```powershell
$TempRoots = @([IO.Path]::GetFullPath('C:\costing-v3-full'), [IO.Path]::GetFullPath('C:\costing-v3-contracts'))
foreach ($Root in $TempRoots) {
  if ([IO.Path]::GetPathRoot($Root) -ne 'C:\' -or [IO.Path]::GetFileName($Root) -notlike 'costing-v3-*') {
    throw "unsafe basetemp: $Root"
  }
  if (Test-Path -LiteralPath $Root) { throw "basetemp already exists: $Root" }
}
try {
  Invoke-NativeChecked { uv run python -m pytest tests/rust_oracle -q --basetemp $TempRoots[0] } 'rust oracle gate failed'
  Invoke-NativeChecked { uv run python -m pytest tests/contracts tests/architecture -q --basetemp $TempRoots[1] } 'contract gate failed'
  Invoke-NativeChecked { uv run python -m ruff check src tests } 'ruff check failed'
  Invoke-NativeChecked { uv run python -m ruff format src tests --check } 'ruff format check failed'
  Invoke-NativeChecked { cargo test --locked --manifest-path rust/Cargo.toml --target x86_64-pc-windows-msvc --target-dir rust/target/test-v3-msvc --no-default-features } 'cargo test failed'
  Invoke-NativeChecked { cargo fmt --manifest-path rust/Cargo.toml --all --check } 'cargo fmt failed'
  Invoke-NativeChecked { uv run python -m tests.rust_oracle.evidence scan --root docs/performance } 'evidence scan failed'
  Invoke-NativeChecked { git diff --check } 'working diff check failed'
} finally {
  foreach ($Root in $TempRoots) {
    if (Test-Path -LiteralPath $Root) {
      $Resolved = (Resolve-Path -LiteralPath $Root).Path
      if ($Resolved -ne $Root) { throw "basetemp resolved elsewhere: $Resolved" }
      Remove-Item -LiteralPath $Resolved -Recurse -Force
    }
  }
}
```

Expected: all pass. No formal EXE invocation with ERP input occurs in Task 8.

- [ ] **Step 3: Re-prove immutable assets**

```powershell
$checks=@{
  'rust/target/perf-builds/phase0a/reference/x86_64-pc-windows-msvc/release/costing-calculate.exe'='f75f7ee17cc222765537f6bbe02f90e76cd041c55c8990b0261788e6fa63db56'
  'rust/target/perf-builds/phase0b/instrumented/x86_64-pc-windows-msvc/release/costing-calculate.exe'='d06470e4e7c9e6dc8f54efc9d26d996d3cbbbddec04cb7dffef6e6869802b629'
  'docs/performance/baselines/2026-07-11-windows-x64-phase0a.json'='17faa1f08a0601fac52186ab64a4ebb4519312a259c59b7d64e69c748bab56df'
  'rust/target/perf-local/batches/b6a98530a04b060b6d9739d804f1b928682b55e58ec7fae03bd779fb9b526149/attempt-0004/terminal.json'='d42940dfc48f208834efa103de6a08663e75d1ee09dd6804d4e2416ad90af71f'
  'rust/target/perf-local/batches/09d6bb93ab04dda277e97f19dc8a270be91f2f8898a42f25d1d5bd745bdf0fd7/attempt-0001/terminal.json'='f515c305518093e9aa0ac90fa0b82520874fcd7006db16946b45921fd9b2a57b'
}
foreach($item in $checks.GetEnumerator()){
  $actual=(Get-FileHash -Algorithm SHA256 -LiteralPath $item.Key).Hash.ToLowerInvariant()
  if($actual -ne $item.Value){throw "immutable SHA drift: $($item.Key)"}
}
```

Run `comparison_tree_digest()` and require the fixed triple again.

- [ ] **Step 4: Commit docs separately**

```powershell
Assert-ExactStagedPaths @()
git add -- docs/performance/README.md docs/superpowers/specs/2026-07-11-rust-output-ingest-continuous-performance-design.md docs/superpowers/plans/2026-07-11-rust-output-phase-0a-3-writer-optimization.md
$Expected=@('docs/performance/README.md','docs/superpowers/plans/2026-07-11-rust-output-phase-0a-3-writer-optimization.md','docs/superpowers/specs/2026-07-11-rust-output-ingest-continuous-performance-design.md')
Assert-ExactStagedPaths $Expected
Invoke-NativeChecked { git diff --cached --check } 'Task 8 cached diff failed'
Invoke-NativeChecked { git commit -m "docs(perf): document formal recovery protocol v3" } 'Task 8 commit failed'
Assert-CleanRepository
```

- [ ] **Step 5: Run independent read-only reviews**

Required findings:

- `python_reviewer`：protocol/ledger/optional-stopping/publication recovery；
- `data_auditor`：same-batch、N、AB/BA、1.02/1.10 gate、no v2 metric reuse；
- `security_reviewer`：closed authorization、tree/reparse、dirty allowlist、sanitizer；
- `doc_reviewer`：README/spec/plan consistency；
- `ops_reviewer`：Windows path/PWS/process termination/preflight commands。

Any P0/P1 or Critical/Important returns to the owning task with a new test-first fix commit. Repeat full gate after the last fix.

## Task 9: Execute the One Allowed Formal GB v3 Batch

**Files:**
- Read: fixed EXEs、approved baseline、sealed v1/v2 parent、reviewed code。
- Create only on `VALIDATED`: GB v3 artifact and marker under `docs/performance/runs/phase0b-v3/`。

**Interfaces:**
- Consumes: Task 1–8 fully green and reviewed, `$env:COSTING_GB_SAMPLE`。
- Produces: sealed GB v3 ledger and committed sanitized GB evidence。

**Execution shell rule:** Task 9 Steps 1–2 share PowerShell variables and must be concatenated into one native PowerShell script executed by one `shell_command`; Step 3 is separate only after exit 0.

**Hard gate:** Do not begin this task if any Task 1–8 item is incomplete, any review has unresolved P0/P1, worktree is dirty, or immutable SHA/tree proof differs. Do not rebuild either EXE.

- [ ] **Step 1: Run one PowerShell preflight process**

In one shell, verify clean worktree, fixed hashes, GB input env/path SHA, sealed v2 tree triple and absence of any initial GB v3 sampling state/evidence. Derive key/basename through the pure helper; the helper uses the closed authorization and does not create files.

```powershell
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$PSNativeCommandUseErrorActionPreference=$true
Assert-CleanRepository
$ReferenceExe='rust/target/perf-builds/phase0a/reference/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$CandidateExe='rust/target/perf-builds/phase0b/instrumented/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$Manifest='docs/performance/baselines/2026-07-11-windows-x64-phase0a.json'
$V1Terminal='rust/target/perf-local/batches/b6a98530a04b060b6d9739d804f1b928682b55e58ec7fae03bd779fb9b526149/attempt-0004/terminal.json'
$V2Terminal='rust/target/perf-local/batches/09d6bb93ab04dda277e97f19dc8a270be91f2f8898a42f25d1d5bd745bdf0fd7/attempt-0001/terminal.json'
if(-not $env:COSTING_GB_SAMPLE){throw 'COSTING_GB_SAMPLE is not set'}
$GbInput=(Resolve-Path -LiteralPath $env:COSTING_GB_SAMPLE).Path
$Immutable=@{
  $GbInput='6aa5e3e7fdc547ebaaef968eb5b95d4d630c4ec9915184f94346f60687b8e7ee'
  $ReferenceExe='f75f7ee17cc222765537f6bbe02f90e76cd041c55c8990b0261788e6fa63db56'
  $CandidateExe='d06470e4e7c9e6dc8f54efc9d26d996d3cbbbddec04cb7dffef6e6869802b629'
  $Manifest='17faa1f08a0601fac52186ab64a4ebb4519312a259c59b7d64e69c748bab56df'
  $V1Terminal='d42940dfc48f208834efa103de6a08663e75d1ee09dd6804d4e2416ad90af71f'
  $V2Terminal='f515c305518093e9aa0ac90fa0b82520874fcd7006db16946b45921fd9b2a57b'
}
foreach($Item in $Immutable.GetEnumerator()) {
  if((Get-FileHash -Algorithm SHA256 -LiteralPath $Item.Key).Hash.ToLowerInvariant() -ne $Item.Value) {
    throw "immutable SHA drift: $($Item.Key)"
  }
}
$TreeLines=@(uv run python -c "from pathlib import Path; from tests.rust_oracle.phase0_harness import comparison_tree_digest; d=comparison_tree_digest(Path('rust/target/perf-local/batches/09d6bb93ab04dda277e97f19dc8a270be91f2f8898a42f25d1d5bd745bdf0fd7')); print(f'{d.entry_count}|{d.sha256}|{d.journal_head_sha256}')")
if($LASTEXITCODE -ne 0 -or $TreeLines.Count -ne 1 -or $TreeLines[0] -ne '134|8e961515bcac3afad271bb75eac9e439fdb18d1e8ba07b0fef7e210838796ccb|ae10e9d441ecebee9ba6cfb93a799f14a9085c75560103fedc9df6ff56b92c85'){throw 'sealed v2 tree proof failed'}
$StateProbePython="import json,sys; from pathlib import Path; from tests.rust_oracle.benchmark_protocol import ClosedBinaryLabel,ComparisonProfile; from tests.rust_oracle.phase0_harness import PairedBenchmarkRequest,inspect_expected_formal_v3_state; r=PairedBenchmarkRequest(pipeline='gb',input_path=Path(sys.argv[1]),reference_executable=Path(sys.argv[2]),candidate_executable=Path(sys.argv[3]),reference_label=ClosedBinaryLabel.PHASE0A,candidate_label=ClosedBinaryLabel.PHASE0B,comparison_profile=ComparisonProfile.PHASE0B_VS_PHASE0A,phase0a_manifest=Path(sys.argv[4]),local_root=Path('rust/target/perf-local'),evidence_path=Path('docs/performance/runs/phase0b-v3/probe.json'),attempt_ledger_root=Path('rust/target/perf-local/batches')); s=inspect_expected_formal_v3_state(r); print(json.dumps({'state':s.state,'child_process_allowed':s.child_process_allowed,'comparison_key':s.identity.comparison_key,'batch_id':s.identity.batch_id,'artifact':s.artifact_basename,'marker':s.marker_basename,'started':s.sample_started_count,'samples':s.sample_record_count},separators=(',',':')))"
$ProbeLines=@(uv run python -c $StateProbePython $GbInput $ReferenceExe $CandidateExe $Manifest)
if($LASTEXITCODE -ne 0 -or $ProbeLines.Count -ne 1){throw 'GB v3 state probe failed or emitted non-JSON stdout'}
$Probe=$ProbeLines[0] | ConvertFrom-Json
if($Probe.state -ne 'NEW' -or -not $Probe.child_process_allowed -or $Probe.started -ne 0 -or $Probe.samples -ne 0){throw 'GB fresh command requires exact NEW state'}
$GbComparisonKey=$Probe.comparison_key
$GbEvidence="docs/performance/runs/phase0b-v3/$($Probe.artifact)"
if(Test-Path -LiteralPath "rust/target/perf-local/batches/$GbComparisonKey"){throw 'NEW probe disagrees with disk'}
if(Test-Path -LiteralPath $GbEvidence){throw 'initial GB v3 evidence already exists'}
```

This block is complete; do not substitute a reference to Task 8. It checks v1/v2 terminals, the v2 tree triple, fixed binaries/manifest/input, exact NEW state and zero existing records in the same shell that launches Step 2.

- [ ] **Step 2: Run formal GB v3 exactly once**

```powershell
Invoke-NativeChecked { uv run python -m tests.rust_oracle.phase0_harness paired --pipeline gb --input "$GbInput" --reference-executable $ReferenceExe --candidate-executable $CandidateExe --reference-label phase0a --candidate-label phase0b --comparison-profile phase0b-vs-phase0a --phase0a-manifest $Manifest --local-root rust/target/perf-local --evidence-path $GbEvidence } 'GB v3 ended nonzero; inspect state before any continuation and do not run SK'
```

Expected success only: typed result `VALIDATED`; ledger is `EVIDENCE_COMMITTED`; artifact/marker exact; no workbook/raw artifact remains. Any failure terminal ends Task 9.

- [ ] **Step 2R: If and only if Step 2 exits nonzero, classify one zero-process publication recovery**

Run the same immutable checks again, then run the same argument-safe state probe without the `NEW`/clean-worktree assertion. Continue only when `state` is exactly `CLEANUP_COMPLETE`, `EVIDENCE_PREPARED`, or `EVIDENCE_COMMITTED`, `child_process_allowed=false`, no failure terminal exists, and current dirty paths satisfy the typed allowlist. Save `started`/`samples`, invoke the same `paired` CLI once, probe again, and require those two counts unchanged plus final `EVIDENCE_COMMITTED`. For `FAILED_TERMINAL`, `INVALID`, `SAMPLING_RESUMABLE` or `STARTED_WITHOUT_SAMPLE`, stop permanently. Never delete the comparison/evidence to regain `NEW`.

```powershell
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$PSNativeCommandUseErrorActionPreference=$true
$ReferenceExe='rust/target/perf-builds/phase0a/reference/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$CandidateExe='rust/target/perf-builds/phase0b/instrumented/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$Manifest='docs/performance/baselines/2026-07-11-windows-x64-phase0a.json'
$V1Terminal='rust/target/perf-local/batches/b6a98530a04b060b6d9739d804f1b928682b55e58ec7fae03bd779fb9b526149/attempt-0004/terminal.json'
$V2Terminal='rust/target/perf-local/batches/09d6bb93ab04dda277e97f19dc8a270be91f2f8898a42f25d1d5bd745bdf0fd7/attempt-0001/terminal.json'
if(-not $env:COSTING_GB_SAMPLE){throw 'COSTING_GB_SAMPLE is not set'}
$GbInput=(Resolve-Path -LiteralPath $env:COSTING_GB_SAMPLE).Path
$Immutable=@{$GbInput='6aa5e3e7fdc547ebaaef968eb5b95d4d630c4ec9915184f94346f60687b8e7ee';$ReferenceExe='f75f7ee17cc222765537f6bbe02f90e76cd041c55c8990b0261788e6fa63db56';$CandidateExe='d06470e4e7c9e6dc8f54efc9d26d996d3cbbbddec04cb7dffef6e6869802b629';$Manifest='17faa1f08a0601fac52186ab64a4ebb4519312a259c59b7d64e69c748bab56df';$V1Terminal='d42940dfc48f208834efa103de6a08663e75d1ee09dd6804d4e2416ad90af71f';$V2Terminal='f515c305518093e9aa0ac90fa0b82520874fcd7006db16946b45921fd9b2a57b'}
foreach($Item in $Immutable.GetEnumerator()){if((Get-FileHash -Algorithm SHA256 -LiteralPath $Item.Key).Hash.ToLowerInvariant() -ne $Item.Value){throw "immutable SHA drift: $($Item.Key)"}}
$Tree=@(uv run python -c "from pathlib import Path; from tests.rust_oracle.phase0_harness import comparison_tree_digest; d=comparison_tree_digest(Path('rust/target/perf-local/batches/09d6bb93ab04dda277e97f19dc8a270be91f2f8898a42f25d1d5bd745bdf0fd7')); print(f'{d.entry_count}|{d.sha256}|{d.journal_head_sha256}')")
if($LASTEXITCODE -ne 0 -or $Tree.Count -ne 1 -or $Tree[0] -ne '134|8e961515bcac3afad271bb75eac9e439fdb18d1e8ba07b0fef7e210838796ccb|ae10e9d441ecebee9ba6cfb93a799f14a9085c75560103fedc9df6ff56b92c85'){throw 'sealed v2 tree proof failed'}
$StateProbePython="import json,sys; from pathlib import Path; from tests.rust_oracle.benchmark_protocol import ClosedBinaryLabel,ComparisonProfile; from tests.rust_oracle.phase0_harness import PairedBenchmarkRequest,inspect_expected_formal_v3_state; r=PairedBenchmarkRequest(pipeline='gb',input_path=Path(sys.argv[1]),reference_executable=Path(sys.argv[2]),candidate_executable=Path(sys.argv[3]),reference_label=ClosedBinaryLabel.PHASE0A,candidate_label=ClosedBinaryLabel.PHASE0B,comparison_profile=ComparisonProfile.PHASE0B_VS_PHASE0A,phase0a_manifest=Path(sys.argv[4]),local_root=Path('rust/target/perf-local'),evidence_path=Path('docs/performance/runs/phase0b-v3/probe.json'),attempt_ledger_root=Path('rust/target/perf-local/batches')); s=inspect_expected_formal_v3_state(r); print(json.dumps({'state':s.state,'child_process_allowed':s.child_process_allowed,'artifact':s.artifact_basename,'started':s.sample_started_count,'samples':s.sample_record_count},separators=(',',':')))"
$BeforeLines=@(uv run python -c $StateProbePython $GbInput $ReferenceExe $CandidateExe $Manifest)
if($LASTEXITCODE -ne 0 -or $BeforeLines.Count -ne 1){throw 'GB recovery state inspection failed'}
$Before=$BeforeLines[0] | ConvertFrom-Json
if($Before.state -notin @('CLEANUP_COMPLETE','EVIDENCE_PREPARED','EVIDENCE_COMMITTED') -or $Before.child_process_allowed){throw 'state is not eligible for publication-only recovery'}
$GbEvidence="docs/performance/runs/phase0b-v3/$($Before.artifact)"
Invoke-NativeChecked { uv run python -m tests.rust_oracle.phase0_harness paired --pipeline gb --input "$GbInput" --reference-executable $ReferenceExe --candidate-executable $CandidateExe --reference-label phase0a --candidate-label phase0b --comparison-profile phase0b-vs-phase0a --phase0a-manifest $Manifest --local-root rust/target/perf-local --evidence-path $GbEvidence } 'GB publication recovery failed; do not sample again'
$AfterLines=@(uv run python -c $StateProbePython $GbInput $ReferenceExe $CandidateExe $Manifest)
if($LASTEXITCODE -ne 0 -or $AfterLines.Count -ne 1){throw 'GB recovery readback failed'}
$After=$AfterLines[0] | ConvertFrom-Json
if($After.state -ne 'EVIDENCE_COMMITTED' -or $After.started -ne $Before.started -or $After.samples -ne $Before.samples){throw 'GB recovery was not zero-sample or did not commit evidence'}
```

This branch is standalone and bypasses only the initial nonexistence/clean assertion; immutable, provenance and state-aware repository checks still run.

- [ ] **Step 3: Validate, sanitize and commit only GB evidence**

```powershell
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$PSNativeCommandUseErrorActionPreference=$true
$ReferenceExe='rust/target/perf-builds/phase0a/reference/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$CandidateExe='rust/target/perf-builds/phase0b/instrumented/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$Manifest='docs/performance/baselines/2026-07-11-windows-x64-phase0a.json'
if(-not $env:COSTING_GB_SAMPLE){throw 'COSTING_GB_SAMPLE is not set'}
$GbInput=(Resolve-Path -LiteralPath $env:COSTING_GB_SAMPLE).Path
$ProbePython="import json,sys; from pathlib import Path; from tests.rust_oracle.benchmark_protocol import ClosedBinaryLabel,ComparisonProfile; from tests.rust_oracle.phase0_harness import PairedBenchmarkRequest,inspect_expected_formal_v3_state; r=PairedBenchmarkRequest(pipeline='gb',input_path=Path(sys.argv[1]),reference_executable=Path(sys.argv[2]),candidate_executable=Path(sys.argv[3]),reference_label=ClosedBinaryLabel.PHASE0A,candidate_label=ClosedBinaryLabel.PHASE0B,comparison_profile=ComparisonProfile.PHASE0B_VS_PHASE0A,phase0a_manifest=Path(sys.argv[4]),local_root=Path('rust/target/perf-local'),evidence_path=Path('docs/performance/runs/phase0b-v3/probe.json'),attempt_ledger_root=Path('rust/target/perf-local/batches')); s=inspect_expected_formal_v3_state(r); print(json.dumps({'state':s.state,'child_process_allowed':s.child_process_allowed,'artifact':s.artifact_basename},separators=(',',':')))"
$ProbeLines=@(uv run python -c $ProbePython $GbInput $ReferenceExe $CandidateExe $Manifest)
if($LASTEXITCODE -ne 0 -or $ProbeLines.Count -ne 1){throw 'GB committed-state probe failed'}
$Probe=$ProbeLines[0] | ConvertFrom-Json
if($Probe.state -ne 'EVIDENCE_COMMITTED' -or $Probe.child_process_allowed){throw 'GB evidence is not in committed zero-process state'}
$GbEvidence="docs/performance/runs/phase0b-v3/$($Probe.artifact)"
$Readback=@(uv run python -c "import json,sys; from pathlib import Path; from tests.rust_oracle.evidence import EvidenceSanitizer; p=Path(sys.argv[1]); v=EvidenceSanitizer.closed_policy().read_benchmark_manifest(p.name,p.read_bytes()); assert (v.schema_version,v.protocol_version,v.pipeline,v.verdict.value)==(3,3,'gb','VALIDATED'); assert v.recovery_provenance is not None and v.upstream_gate_provenance is None; print(json.dumps({'marker':EvidenceSanitizer.closed_policy().build_batch_marker_from_path(p).file_name},separators=(',',':')))" $GbEvidence)
if($LASTEXITCODE -ne 0 -or $Readback.Count -ne 1){throw 'GB typed readback failed'}
$GbMarker="docs/performance/runs/phase0b-v3/$(($Readback[0] | ConvertFrom-Json).marker)"
Assert-ExactStagedPaths @()
Invoke-NativeChecked { git add -- $GbEvidence $GbMarker } 'GB evidence staging failed'
$Expected=@($GbEvidence.Replace('\','/'),$GbMarker.Replace('\','/'))
Assert-ExactStagedPaths $Expected
Invoke-NativeChecked { uv run python -m tests.rust_oracle.evidence scan --root docs/performance --staged } 'GB staged sanitizer failed'
Invoke-NativeChecked { git diff --cached --check } 'GB evidence cached diff failed'
Invoke-NativeChecked { git commit -m "docs(perf): validate phase0b v3 gb" } 'GB evidence commit failed'
Assert-CleanRepository
```

Expected: evidence-only single-parent commit and clean worktree. Do not run SK in the same pre-commit shell.

## Task 10: Execute the One Allowed Formal SK v3 Batch and Resume Phase 1

**Files:**
- Read: committed GB v3 evidence、fixed EXEs/baseline。
- Create only on `VALIDATED`: SK v3 artifact and marker。

**Interfaces:**
- Consumes: Task 9 evidence-only commit and `$env:COSTING_SK_SAMPLE`。
- Produces: sealed SK v3 evidence, final Phase 0B go/no-go and Phase 1 handoff。

**Execution shell rule:** Task 10 Steps 1–2 share PowerShell variables and must be concatenated into one native PowerShell script executed by one `shell_command`; Steps 3–4 run only after exit 0.

- [ ] **Step 1: Prove GB evidence-only HEAD and SK fixed input**

In one PowerShell shell, require clean worktree; current HEAD single parent; `git diff-tree --name-only HEAD^ HEAD` exact two GB paths; Git blobs equal disk; typed GB manifest/marker rebuild passes; fixed EXE/manifest SHA unchanged; SK input SHA equals:

```text
6eac3c6c9ea0eb3e98ca11fb3829914be63e932595b3e3c613f0da46b385d64f
```

Derive `UpstreamGateProvenance` automatically and use it to derive the SK v3 key/basename. Do not accept shell-supplied provenance fields:

```powershell
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$PSNativeCommandUseErrorActionPreference=$true
Assert-CleanRepository
$ReferenceExe='rust/target/perf-builds/phase0a/reference/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$CandidateExe='rust/target/perf-builds/phase0b/instrumented/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$Manifest='docs/performance/baselines/2026-07-11-windows-x64-phase0a.json'
$Parents=@(git rev-list --parents -n 1 HEAD).Split(' ',[StringSplitOptions]::RemoveEmptyEntries)
if($LASTEXITCODE -ne 0 -or $Parents.Count -ne 2){throw 'GB evidence HEAD must have exactly one parent'}
$Head=(git rev-parse HEAD).Trim()
if($LASTEXITCODE -ne 0){throw 'cannot resolve HEAD'}
$GbPaths=@(git diff-tree --no-commit-id --name-only -r HEAD^ HEAD)
if($LASTEXITCODE -ne 0 -or $GbPaths.Count -ne 2){throw 'GB evidence commit must change exactly two paths'}
if($GbPaths | Where-Object { $_ -notmatch '^docs/performance/runs/phase0b-v3/(benchmark-v3-[0-9a-f]{16}\.json|batch-[0-9a-f]{16}\.commit\.json)$' }){throw 'GB evidence commit path shape is invalid'}
$GbArtifact=@($GbPaths | Where-Object { $_ -match '/benchmark-v3-' })
$GbMarker=@($GbPaths | Where-Object { $_ -match '/batch-' })
if($GbArtifact.Count -ne 1 -or $GbMarker.Count -ne 1){throw 'GB artifact/marker split is invalid'}
foreach($Path in $GbPaths){
  $Blob=@(git show "HEAD:$Path")
  if($LASTEXITCODE -ne 0){throw "cannot read GB blob: $Path"}
  $BlobSha=(git rev-parse "HEAD:$Path").Trim()
  if($LASTEXITCODE -ne 0){throw "cannot hash GB blob: $Path"}
  $DiskSha=(git hash-object -- $Path).Trim()
  if($LASTEXITCODE -ne 0 -or $DiskSha -ne $BlobSha){throw "GB Git blob differs from disk: $Path"}
}
$GbReadback=@(uv run python -c "import json,sys; from pathlib import Path; from tests.rust_oracle.evidence import EvidenceSanitizer; a=Path(sys.argv[1]); m=Path(sys.argv[2]); v=EvidenceSanitizer.closed_policy().read_benchmark_manifest(a.name,a.read_bytes()); rebuilt=EvidenceSanitizer.closed_policy().build_batch_marker_from_path(a); assert (v.schema_version,v.protocol_version,v.pipeline,v.verdict.value)==(3,3,'gb','VALIDATED'); assert rebuilt.file_name==m.name and rebuilt.content.encode('utf-8')==m.read_bytes(); print(json.dumps({'comparison_key':v.comparison_key,'batch_id':v.batch_id},separators=(',',':')))" $GbArtifact[0] $GbMarker[0])
if($LASTEXITCODE -ne 0 -or $GbReadback.Count -ne 1){throw 'GB typed artifact/marker rebuild failed'}
$Fixed=@{
  $ReferenceExe='f75f7ee17cc222765537f6bbe02f90e76cd041c55c8990b0261788e6fa63db56'
  $CandidateExe='d06470e4e7c9e6dc8f54efc9d26d996d3cbbbddec04cb7dffef6e6869802b629'
  $Manifest='17faa1f08a0601fac52186ab64a4ebb4519312a259c59b7d64e69c748bab56df'
}
foreach($Item in $Fixed.GetEnumerator()){if((Get-FileHash -Algorithm SHA256 -LiteralPath $Item.Key).Hash.ToLowerInvariant() -ne $Item.Value){throw "fixed SHA drift: $($Item.Key)"}}
if(-not $env:COSTING_SK_SAMPLE){throw 'COSTING_SK_SAMPLE is not set'}
$SkInput=(Resolve-Path -LiteralPath $env:COSTING_SK_SAMPLE).Path
if((Get-FileHash -Algorithm SHA256 -LiteralPath $SkInput).Hash.ToLowerInvariant() -ne '6eac3c6c9ea0eb3e98ca11fb3829914be63e932595b3e3c613f0da46b385d64f'){throw 'SK input SHA drift'}
$SkProbePython="import json,sys; from pathlib import Path; from tests.rust_oracle.benchmark_protocol import ClosedBinaryLabel,ComparisonProfile; from tests.rust_oracle.phase0_harness import PairedBenchmarkRequest,inspect_expected_formal_v3_state; r=PairedBenchmarkRequest(pipeline='sk',input_path=Path(sys.argv[1]),reference_executable=Path(sys.argv[2]),candidate_executable=Path(sys.argv[3]),reference_label=ClosedBinaryLabel.PHASE0A,candidate_label=ClosedBinaryLabel.PHASE0B,comparison_profile=ComparisonProfile.PHASE0B_VS_PHASE0A,phase0a_manifest=Path(sys.argv[4]),local_root=Path('rust/target/perf-local'),evidence_path=Path('docs/performance/runs/phase0b-v3/probe.json'),attempt_ledger_root=Path('rust/target/perf-local/batches')); s=inspect_expected_formal_v3_state(r); print(json.dumps({'state':s.state,'child_process_allowed':s.child_process_allowed,'comparison_key':s.identity.comparison_key,'batch_id':s.identity.batch_id,'artifact':s.artifact_basename,'marker':s.marker_basename,'gb_commit':s.identity.upstream_gate_provenance.validated_commit_sha,'started':s.sample_started_count,'samples':s.sample_record_count},separators=(',',':')))"
$IdentityLines=@(uv run python -c $SkProbePython $SkInput $ReferenceExe $CandidateExe $Manifest)
if($LASTEXITCODE -ne 0 -or $IdentityLines.Count -ne 1){throw 'SK v3 state/upstream derivation failed'}
$Identity=$IdentityLines[0] | ConvertFrom-Json
if($Identity.gb_commit -ne $Head){throw 'SK upstream commit is not current evidence-only HEAD'}
if($Identity.state -ne 'NEW' -or -not $Identity.child_process_allowed -or $Identity.started -ne 0 -or $Identity.samples -ne 0){throw 'SK fresh command requires exact NEW state'}
$SkComparisonKey=$Identity.comparison_key
$SkEvidence="docs/performance/runs/phase0b-v3/$($Identity.artifact)"
if(Test-Path -LiteralPath "rust/target/perf-local/batches/$SkComparisonKey"){throw 'initial SK v3 comparison already exists'}
if(Test-Path -LiteralPath $SkEvidence){throw 'initial SK v3 evidence already exists'}
```

- [ ] **Step 2: Run formal SK v3 exactly once**

```powershell
Invoke-NativeChecked { uv run python -m tests.rust_oracle.phase0_harness paired --pipeline sk --input "$SkInput" --reference-executable $ReferenceExe --candidate-executable $CandidateExe --reference-label phase0a --candidate-label phase0b --comparison-profile phase0b-vs-phase0a --phase0a-manifest $Manifest --local-root rust/target/perf-local --evidence-path $SkEvidence } 'SK v3 ended nonzero; inspect state before any continuation and do not enter Phase 1'
```

Expected success only: `VALIDATED`; recovery provenance null; upstream provenance exactly binds the Task 9 GB artifact/marker/commit.

- [ ] **Step 2R: If and only if Step 2 exits nonzero, run standalone SK publication recovery**

```powershell
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$PSNativeCommandUseErrorActionPreference=$true
$ReferenceExe='rust/target/perf-builds/phase0a/reference/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$CandidateExe='rust/target/perf-builds/phase0b/instrumented/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$Manifest='docs/performance/baselines/2026-07-11-windows-x64-phase0a.json'
$Parents=@(git rev-list --parents -n 1 HEAD).Split(' ',[StringSplitOptions]::RemoveEmptyEntries)
if($LASTEXITCODE -ne 0 -or $Parents.Count -ne 2){throw 'GB evidence HEAD must have exactly one parent'}
$Head=(git rev-parse HEAD).Trim(); if($LASTEXITCODE -ne 0){throw 'cannot resolve HEAD'}
$GbPaths=@(git diff-tree --no-commit-id --name-only -r HEAD^ HEAD)
if($LASTEXITCODE -ne 0 -or $GbPaths.Count -ne 2){throw 'GB evidence commit must change exactly two paths'}
$GbArtifact=@($GbPaths | Where-Object { $_ -match '^docs/performance/runs/phase0b-v3/benchmark-v3-[0-9a-f]{16}\.json$' })
$GbMarker=@($GbPaths | Where-Object { $_ -match '^docs/performance/runs/phase0b-v3/batch-[0-9a-f]{16}\.commit\.json$' })
if($GbArtifact.Count -ne 1 -or $GbMarker.Count -ne 1){throw 'GB evidence-only path set is invalid'}
foreach($Path in $GbPaths){
  $Blob=(git rev-parse "HEAD:$Path").Trim(); if($LASTEXITCODE -ne 0){throw "cannot hash GB blob: $Path"}
  $Disk=(git hash-object -- $Path).Trim(); if($LASTEXITCODE -ne 0){throw "cannot hash GB disk file: $Path"}
  if($Blob -ne $Disk){throw "GB blob/disk drift: $Path"}
}
$TypedGb=@(uv run python -c "import sys; from pathlib import Path; from tests.rust_oracle.evidence import EvidenceSanitizer; a=Path(sys.argv[1]);m=Path(sys.argv[2]);v=EvidenceSanitizer.closed_policy().read_benchmark_manifest(a.name,a.read_bytes());r=EvidenceSanitizer.closed_policy().build_batch_marker_from_path(a);assert (v.schema_version,v.protocol_version,v.pipeline,v.verdict.value)==(3,3,'gb','VALIDATED');assert r.file_name==m.name and r.content.encode('utf-8')==m.read_bytes();print('OK')" $GbArtifact[0] $GbMarker[0])
if($LASTEXITCODE -ne 0 -or $TypedGb.Count -ne 1 -or $TypedGb[0] -ne 'OK'){throw 'GB typed gate failed'}
if(-not $env:COSTING_SK_SAMPLE){throw 'COSTING_SK_SAMPLE is not set'}
$SkInput=(Resolve-Path -LiteralPath $env:COSTING_SK_SAMPLE).Path
$Fixed=@{$SkInput='6eac3c6c9ea0eb3e98ca11fb3829914be63e932595b3e3c613f0da46b385d64f';$ReferenceExe='f75f7ee17cc222765537f6bbe02f90e76cd041c55c8990b0261788e6fa63db56';$CandidateExe='d06470e4e7c9e6dc8f54efc9d26d996d3cbbbddec04cb7dffef6e6869802b629';$Manifest='17faa1f08a0601fac52186ab64a4ebb4519312a259c59b7d64e69c748bab56df'}
foreach($Item in $Fixed.GetEnumerator()){if((Get-FileHash -Algorithm SHA256 -LiteralPath $Item.Key).Hash.ToLowerInvariant() -ne $Item.Value){throw "fixed SHA drift: $($Item.Key)"}}
$SkProbePython="import json,sys; from pathlib import Path; from tests.rust_oracle.benchmark_protocol import ClosedBinaryLabel,ComparisonProfile; from tests.rust_oracle.phase0_harness import PairedBenchmarkRequest,inspect_expected_formal_v3_state; r=PairedBenchmarkRequest(pipeline='sk',input_path=Path(sys.argv[1]),reference_executable=Path(sys.argv[2]),candidate_executable=Path(sys.argv[3]),reference_label=ClosedBinaryLabel.PHASE0A,candidate_label=ClosedBinaryLabel.PHASE0B,comparison_profile=ComparisonProfile.PHASE0B_VS_PHASE0A,phase0a_manifest=Path(sys.argv[4]),local_root=Path('rust/target/perf-local'),evidence_path=Path('docs/performance/runs/phase0b-v3/probe.json'),attempt_ledger_root=Path('rust/target/perf-local/batches')); s=inspect_expected_formal_v3_state(r); print(json.dumps({'state':s.state,'child_process_allowed':s.child_process_allowed,'artifact':s.artifact_basename,'gb_commit':s.identity.upstream_gate_provenance.validated_commit_sha,'started':s.sample_started_count,'samples':s.sample_record_count},separators=(',',':')))"
$BeforeLines=@(uv run python -c $SkProbePython $SkInput $ReferenceExe $CandidateExe $Manifest)
if($LASTEXITCODE -ne 0 -or $BeforeLines.Count -ne 1){throw 'SK recovery state inspection failed'}
$Before=$BeforeLines[0] | ConvertFrom-Json
if($Before.gb_commit -ne $Head -or $Before.state -notin @('CLEANUP_COMPLETE','EVIDENCE_PREPARED','EVIDENCE_COMMITTED') -or $Before.child_process_allowed){throw 'SK state is not eligible for publication-only recovery'}
$SkEvidence="docs/performance/runs/phase0b-v3/$($Before.artifact)"
Invoke-NativeChecked { uv run python -m tests.rust_oracle.phase0_harness paired --pipeline sk --input "$SkInput" --reference-executable $ReferenceExe --candidate-executable $CandidateExe --reference-label phase0a --candidate-label phase0b --comparison-profile phase0b-vs-phase0a --phase0a-manifest $Manifest --local-root rust/target/perf-local --evidence-path $SkEvidence } 'SK publication recovery failed; do not sample again'
$AfterLines=@(uv run python -c $SkProbePython $SkInput $ReferenceExe $CandidateExe $Manifest)
if($LASTEXITCODE -ne 0 -or $AfterLines.Count -ne 1){throw 'SK recovery readback failed'}
$After=$AfterLines[0] | ConvertFrom-Json
if($After.state -ne 'EVIDENCE_COMMITTED' -or $After.started -ne $Before.started -or $After.samples -ne $Before.samples){throw 'SK recovery was not zero-sample or did not commit evidence'}
```

Any other state is a permanent stop. This standalone branch never applies the fresh nonexistence check and never deletes SK local/evidence state.

- [ ] **Step 3: Validate and commit only SK evidence**

```powershell
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$PSNativeCommandUseErrorActionPreference=$true
$ReferenceExe='rust/target/perf-builds/phase0a/reference/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$CandidateExe='rust/target/perf-builds/phase0b/instrumented/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$Manifest='docs/performance/baselines/2026-07-11-windows-x64-phase0a.json'
if(-not $env:COSTING_SK_SAMPLE){throw 'COSTING_SK_SAMPLE is not set'}
$SkInput=(Resolve-Path -LiteralPath $env:COSTING_SK_SAMPLE).Path
$ProbePython="import json,sys; from pathlib import Path; from tests.rust_oracle.benchmark_protocol import ClosedBinaryLabel,ComparisonProfile; from tests.rust_oracle.phase0_harness import PairedBenchmarkRequest,inspect_expected_formal_v3_state; r=PairedBenchmarkRequest(pipeline='sk',input_path=Path(sys.argv[1]),reference_executable=Path(sys.argv[2]),candidate_executable=Path(sys.argv[3]),reference_label=ClosedBinaryLabel.PHASE0A,candidate_label=ClosedBinaryLabel.PHASE0B,comparison_profile=ComparisonProfile.PHASE0B_VS_PHASE0A,phase0a_manifest=Path(sys.argv[4]),local_root=Path('rust/target/perf-local'),evidence_path=Path('docs/performance/runs/phase0b-v3/probe.json'),attempt_ledger_root=Path('rust/target/perf-local/batches')); s=inspect_expected_formal_v3_state(r); print(json.dumps({'state':s.state,'child_process_allowed':s.child_process_allowed,'artifact':s.artifact_basename},separators=(',',':')))"
$ProbeLines=@(uv run python -c $ProbePython $SkInput $ReferenceExe $CandidateExe $Manifest)
if($LASTEXITCODE -ne 0 -or $ProbeLines.Count -ne 1){throw 'SK committed-state probe failed'}
$Probe=$ProbeLines[0] | ConvertFrom-Json
if($Probe.state -ne 'EVIDENCE_COMMITTED' -or $Probe.child_process_allowed){throw 'SK evidence is not in committed zero-process state'}
$SkEvidence="docs/performance/runs/phase0b-v3/$($Probe.artifact)"
$Readback=@(uv run python -c "import json,sys; from pathlib import Path; from tests.rust_oracle.evidence import EvidenceSanitizer; p=Path(sys.argv[1]); v=EvidenceSanitizer.closed_policy().read_benchmark_manifest(p.name,p.read_bytes()); assert (v.schema_version,v.protocol_version,v.pipeline,v.verdict.value)==(3,3,'sk','VALIDATED'); assert v.recovery_provenance is None and v.upstream_gate_provenance is not None; print(json.dumps({'marker':EvidenceSanitizer.closed_policy().build_batch_marker_from_path(p).file_name},separators=(',',':')))" $SkEvidence)
if($LASTEXITCODE -ne 0 -or $Readback.Count -ne 1){throw 'SK typed readback failed'}
$SkMarker="docs/performance/runs/phase0b-v3/$(($Readback[0] | ConvertFrom-Json).marker)"
Assert-ExactStagedPaths @()
Invoke-NativeChecked { git add -- $SkEvidence $SkMarker } 'SK evidence staging failed'
$Expected=@($SkEvidence.Replace('\','/'),$SkMarker.Replace('\','/'))
Assert-ExactStagedPaths $Expected
Invoke-NativeChecked { uv run python -m tests.rust_oracle.evidence scan --root docs/performance --staged } 'SK staged sanitizer failed'
Invoke-NativeChecked { git diff --cached --check } 'SK evidence cached diff failed'
Invoke-NativeChecked { git commit -m "docs(perf): validate phase0b v3 sk" } 'SK evidence commit failed'
Assert-CleanRepository
```

- [ ] **Step 4: Final Phase 0B audit**

Re-run the complete Task 8 gate. Typed-read both GB/SK schema v3 artifacts and markers; verify both Phase 0B wall ratios `<=1.02`, bytes ratios `<=1.10`, all N/N success, dimensions/oracles/runtime contracts match, PWS diagnostics are present without an invented direct gate, fixed EXE/manifest/v1/v2 SHA and v2 tree triple remain unchanged, and worktree is clean.

Only after every assertion passes, resume `docs/superpowers/plans/2026-07-11-rust-output-phase-0a-3-writer-optimization.md` at Task 3 (Phase 1 `SheetWritePlan`/`ColumnWritePlan`). Phase 1 uses the frozen Phase 0B candidate EXE SHA above as its same-batch reference; do not rebuild Phase 0B.

## Pseudocode Draft

```python
# 目标：只对一个闭合 v2 harness 缺陷建立 fresh v3 证据，并在 GB→commit→SK 后恢复 Phase 1。
# 输入：固定 EXE/manifest/input、sealed v1/v2、reviewed v3 harness。
# 输出：两条 VALIDATED schema v3 evidence；或不可重采 terminal。

def execute_phase0b_v3_recovery():
    implement_protocol_identity_test_first()
    prove_legacy_parent_read_only()
    implement_durable_sample_started_and_v3_ledger()
    implement_schema_v3_and_publication_recovery()
    bind_committed_gb_evidence_into_sk_identity()
    require_full_tests_and_independent_reviews()

    gb = run_one_fresh_formal_v3_batch('gb')
    require(gb.verdict == VALIDATED)
    gb_commit = commit_evidence_only(gb)

    sk = run_one_fresh_formal_v3_batch('sk', upstream_commit=gb_commit)
    require(sk.verdict == VALIDATED)
    commit_evidence_only(sk)

    require_final_phase0b_audit()
    return resume_original_plan_at_phase1()
```

## Plan Self-Review Checklist

- [ ] 每个设计要求都有对应任务和测试。
- [ ] v2 key/schema/readback 保持精确，current v3 常量不破坏 legacy audit。
- [ ] v2 parent entire-tree 固定值进入 authorization/key/batch/ledger/evidence。
- [ ] `sample-started` 在子进程前 durable，started-without-sample 永不重采。
- [ ] inner capture 不 cleanup/seal；outer runner 是唯一 owner。
- [ ] Phase 0B 使用 wall `1.02`，PWS 无 direct gate但参与扩样/诊断。
- [ ] cleanup-complete/prepared/committed recovery 零子进程。
- [ ] artifact-only、marker-only、drift 和 dual-SHA committed 都有测试。
- [ ] SK upstream provenance 绑定 GB artifact/marker/evidence-only commit。
- [ ] CLI 无 protocol/parent/reason/provenance selector。
- [ ] 正式任务位于代码、测试、文档和独立 review 之后。
- [ ] GB evidence commit 后才运行 SK。
- [ ] 没有新增依赖、生产 Rust 改动、PR 或用户 `model.rs` 变更。
- [ ] Placeholder 与模糊步骤扫描为零，所有新增接口和复杂 fixture 都在计划中有闭合定义。

## Execution Handoff

推荐使用 `superpowers:subagent-driven-development` 按 Task 1→10 顺序执行，每个代码任务先规格符合性 review，再代码质量 review。若使用 inline execution，必须调用 `superpowers:executing-plans`，并在 Task 8 full gate、Task 9 GB formal 和 Task 10 SK formal 前分别停下复核硬门禁。

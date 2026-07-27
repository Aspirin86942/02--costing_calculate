# Phase 0H Paired Direction Protocol v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把已批准的 paired benchmark protocol v2 落入现有 Phase 0H harness，保留 v1 审计链，只用固定 EXE 各执行一次正式 GB/SK Phase 0B，并在两者通过后恢复原持续性能计划的 Phase 1。

**Architecture:** `benchmark_protocol.py` 持有协议版本、comparison identity、direct-gate 解析和方向 diagnostic 纯逻辑；`phase0_harness.py` 持有 append-only ledger、N=5/10 编排和“明确失败优先”的 terminal verdict；`evidence.py` 负责 benchmark schema v1/v2 双读、v2 单写和 typed artifact basename。所有正式 workbook/raw log 仍位于 ignored 本地目录，版本化证据继续由 typed sanitizer 重建、扫描和 marker-last 发布。

**Tech Stack:** Python 3.11、pytest、`decimal.Decimal`、`dataclasses`、`hashlib`、现有 `EvidenceSanitizer`、PowerShell、固定 Windows MSVC Rust release EXE。

## Global Constraints

- 当前 paired protocol 固定为 `PAIRED_PROTOCOL_VERSION = 2`；不得增加 CLI 参数、环境变量或配置项让操作者选择 v1/v2。
- 第一组固定 global round 1–5；触发临界区后 wall/PWS 必须同时追加 global round 6–10；不得产生 round 11、第三组或结果后重采。
- global round 1/3/5/7/9 固定 reference → candidate；2/4/6/8/10 固定 candidate → reference。
- N=10 时先评价全部 direct/composite/stage closed gates；任一明确失败优先 `CANDIDATE_FAILED`；只有全部门禁通过后，active direct wall/PWS metric 的 near-boundary direction conflict 才返回 `INCONCLUSIVE`。
- direction conflict 的唯一公式为 `(first_group_ratio - 1) * (second_group_ratio - 1) < 0`；任一 ratio 恰好等于 1 时不冲突。
- direct wall gates 仅为 `wall_ratio` / `wall_seconds`；direct PWS gates 仅为 `pws_ratio` / `pws_bytes`；composite/stage/output-bytes gate 不参与 direction veto。
- 同一 resolved profile/pipeline limits entry 下，每个 metric 最多一个 direct gate；ratio 与 absolute 同时存在必须 fail closed。
- v1 ledger/evidence 只读兼容；正式 runner、ledger create、writer/publisher 只能产生 v2；不得修改、迁移、删除或追认 v1 attempt 4。
- typed evidence schema/path identity mismatch maps to `INCOMPLETE_EVIDENCE`；write、typed rebuild verification or staged sensitive-scan failure maps to `SENSITIVE_EVIDENCE` and still runs existing cleanup。
- v2 artifact basename 固定为 `benchmark-v2-<comparison_key[:16]>.json`；comparison key 必须覆盖 protocol、pipeline、profile、input、reference/candidate label 与 SHA。
- 已批准 Phase 0A manifest 不修改、不重算：SHA-256 固定为 `17faa1f08a0601fac52186ab64a4ebb4519312a259c59b7d64e69c748bab56df`。
- 不重建两个固定 EXE：reference SHA-256 固定为 `f75f7ee17cc222765537f6bbe02f90e76cd041c55c8990b0261788e6fa63db56`；candidate SHA-256 固定为 `d06470e4e7c9e6dc8f54efc9d26d996d3cbbbddec04cb7dffef6e6869802b629`。
- v1 GB Phase 0B attempt 4 terminal SHA-256 固定为 `d42940dfc48f208834efa103de6a08663e75d1ee09dd6804d4e2416ad90af71f`，record/checkpoint 均为 82 个。
- 不修改 `rust/`、Cargo、生产行为、Phase 0A baseline 或已生成 v1 local evidence；不创建 PR。
- 所有代码修改必须 RED→GREEN；每个 green commit 后执行独立 Python review；文档修改由只读 `doc_reviewer` 单独审查。
- 所有金额/比例继续使用 `Decimal`；禁止用 `float` 计算方向、median、归一化值或 3% 边界。
- 正式 GB 只能运行一次；GB 非 `VALIDATED` 时停止，不运行 SK；GB 通过后正式 SK 只能运行一次；任一失败均不得以相同 protocol/comparison/candidate 重新采样。

---

## Starting State

- Execution worktree：`.worktrees/rust-output-ingest-performance`
- Branch：`perf/rust-output-ingest-continuous`
- Approved v2 spec commit：`336a87ce743009e85488ced6d3d55c17bdf99b25`
- v2 spec：`docs/superpowers/specs/2026-07-12-phase0h-paired-direction-protocol-v2-design.md`
- Phase 0B observability implementation：`094a687c`
- Long-path recovery：`f60fed6b`
- Volatile output-byte aggregation：`0751137`
- 当前 `tests/rust_oracle` 最近完整证据：454 passed；执行本计划时必须重新运行，不得引用该历史结果代替 fresh verification。
- 主工作区已有 `rust/crates/costing-core/src/model.rs` 用户修改；本 worktree 不得触碰或暂存它。

## File Structure

### Modify

- `tests/rust_oracle/benchmark_protocol.py:1-612` — protocol constant、comparison key、direct-gate 解析、structural merge、direction diagnostic、`BatchAttempt.protocol_version`。
- `tests/rust_oracle/test_benchmark_protocol.py:1-447` — 纯协议和边界测试。
- `tests/rust_oracle/phase0_harness.py:135-1017, 2338-2515, 2842-3299` — v2 ledger metadata、batch identity、runner verdict 顺序、schema v2 evidence 构建和恢复。
- `tests/rust_oracle/test_phase0_harness.py:1-2130` — ledger v1/v2、runner mixed-gate 和历史保护测试。
- `tests/rust_oracle/evidence.py:300-363, 527-672, 848-1010, 1258-1331, 1904-1924` — schema v1/v2 双读、v2 单写、diagnostic exact validation、artifact filename。
- `tests/rust_oracle/test_evidence.py:776-825, 930-1260, 1687-1990` — v1 rebuild、v2 publish、marker/scanner 和 collision 测试。
- `docs/performance/README.md:29-47` — paired v2 identity、basename、双读单写和停止规则。
- `docs/superpowers/specs/2026-07-11-rust-output-ingest-continuous-performance-design.md:1120-1132` — 第 10.5 节从 v1 的一律冲突 veto 修订为 v2 决策顺序。

### Create during formal evidence capture only

- `docs/performance/runs/phase0b-v2/benchmark-v2-<gb-comparison-key-prefix>.json`
- `docs/performance/runs/phase0b-v2/batch-<gb-batch-sha-prefix>.commit.json`
- GB 通过并提交后，创建对应 SK benchmark artifact 与 batch marker。

### Do not modify

- `rust/**`
- `Cargo.toml` / `Cargo.lock`
- `docs/performance/baselines/2026-07-11-windows-x64-phase0a.json`
- `rust/target/perf-local/batches/b6a98530a04b060b6d9739d804f1b928682b55e58ec7fae03bd779fb9b526149/**`
- 固定 reference/candidate EXE
- `data/raw/**` 与任何真实 ERP workbook

## Stable Interfaces

`benchmark_protocol.py` 新增或冻结以下接口：

```python
PAIRED_PROTOCOL_VERSION: Final = 2
DirectGateKind: TypeAlias = Literal['none', 'ratio', 'absolute']


@dataclass(frozen=True)
class DirectionDiagnosticEvidence:
    metric: MetricName
    first_group_ratio: Decimal
    second_group_ratio: Decimal
    combined_ratio: Decimal
    directions_conflict: bool
    direct_gate: DirectGateKind
    direct_limit: Decimal | None
    normalized_to_limit: Decimal | None
    near_boundary: bool | None
```

| Function | Exact signature |
|---|---|
| comparison identity | `derive_comparison_key(*, protocol_version: int, pipeline: PipelineName, comparison_profile: ComparisonProfile, reference_label: ClosedBinaryLabel, candidate_label: ClosedBinaryLabel, input_sha256: str, reference_sha256: str, candidate_sha256: str) -> str` |
| direct gate | `resolve_direct_metric_gate(metric: MetricName, limits: Mapping[str, Decimal | int]) -> tuple[DirectGateKind, Decimal | None]` |
| diagnostic | `build_direction_diagnostic(first: MetricGroup, second: MetricGroup, *, limits: Mapping[str, Decimal | int]) -> DirectionDiagnosticEvidence` |

`BatchAttempt` 和 ledger 同步携带：

```python
protocol_version: int
```

`evidence.py` 冻结：

| Function | Exact signature and authority |
|---|---|
| basename | `expected_benchmark_artifact_name(*, protocol_version: int, comparison_key: str) -> str` |
| formal builder | `EvidenceSanitizer.build_benchmark_manifest(self, value: BenchmarkManifestEvidence) -> _SanitizedArtifact`；schema/protocol v2 only |
| audit rebuild | `EvidenceSanitizer.rebuild_benchmark_manifest(self, value: BenchmarkManifestEvidence) -> _SanitizedArtifact`；exact v1/v2 rebuild, never publication authority |
| dual reader | `EvidenceSanitizer.read_benchmark_manifest(self, file_name: str, raw: bytes) -> BenchmarkManifestEvidence` |

## Task 1: Implement the Pure Protocol v2 Decision Kernel

**Files:**
- Modify: `tests/rust_oracle/benchmark_protocol.py:1-612`
- Modify: `tests/rust_oracle/test_benchmark_protocol.py:1-447`

**Interfaces:**
- Consumes: existing `MetricGroup`、`COMPARISON_LIMITS`、`MANDATORY_EXPANSION_BOUNDARY`。
- Produces: `PAIRED_PROTOCOL_VERSION`、`DirectionDiagnosticEvidence`、`derive_comparison_key()`、`resolve_direct_metric_gate()`、`build_direction_diagnostic()`；`merge_metric_groups()` becomes structural-only。

- [ ] **Step 1: Add RED tests for protocol identity, strict direction and structural merge**

Add these imports and tests, using the existing `_group()` helper:

```python
from tests.rust_oracle.benchmark_protocol import (
    PAIRED_PROTOCOL_VERSION,
    ClosedBinaryLabel,
    build_direction_diagnostic,
    derive_comparison_key,
    resolve_direct_metric_gate,
)


def test_protocol_v2_comparison_key_binds_every_comparison_identity_field() -> None:
    common = {
        'protocol_version': PAIRED_PROTOCOL_VERSION,
        'pipeline': 'gb',
        'comparison_profile': ComparisonProfile.PHASE0B_VS_PHASE0A,
        'reference_label': ClosedBinaryLabel.PHASE0A,
        'candidate_label': ClosedBinaryLabel.PHASE0B,
        'input_sha256': '1' * 64,
        'reference_sha256': '2' * 64,
        'candidate_sha256': '3' * 64,
    }
    base = derive_comparison_key(**common)
    assert len(base) == 64
    assert derive_comparison_key(**{**common, 'input_sha256': '4' * 64}) != base
    assert derive_comparison_key(**{**common, 'reference_sha256': '5' * 64}) != base


@pytest.mark.parametrize(
    ('first_value', 'second_value', 'expected'),
    (('0.9', '1.1', True), ('1.1', '0.9', True), ('1.0', '1.1', False), ('0.9', '1.0', False), ('1.0', '1.0', False)),
)
def test_direction_conflict_requires_strict_opposite_signs(
    first_value: str,
    second_value: str,
    expected: bool,
) -> None:
    assert groups_have_conflicting_direction(
        _group(start=1, candidate_value=first_value),
        _group(start=6, candidate_value=second_value),
    ) is expected


def test_structural_merge_keeps_conflicting_groups_for_v2_decision() -> None:
    merged = merge_metric_groups(
        _group(start=1, candidate_value='0.9'),
        _group(start=6, candidate_value='1.1'),
    )
    assert tuple(item.plan.global_round for item in merged.rounds) == tuple(range(1, 11))
```

- [ ] **Step 2: Run the focused test file and verify RED**

Run:

```powershell
uv run python -m pytest tests/rust_oracle/test_benchmark_protocol.py -q --basetemp .pytest-tmp/protocol-v2-kernel-red
```

Expected: FAIL because the v2 interfaces are absent and current `merge_metric_groups()` rejects conflicting groups.

- [ ] **Step 3: Add RED tests for direct-gate normalization**

```python
def test_phase0b_pws_conflict_is_inactive_diagnostic() -> None:
    limits = COMPARISON_LIMITS[ComparisonProfile.PHASE0B_VS_PHASE0A]['gb']
    diagnostic = build_direction_diagnostic(
        _group(metric='pws', start=1, candidate_value='0.99'),
        _group(metric='pws', start=6, candidate_value='1.01'),
        limits=limits,
    )
    assert diagnostic.directions_conflict is True
    assert diagnostic.direct_gate == 'none'
    assert diagnostic.direct_limit is None
    assert diagnostic.normalized_to_limit is None
    assert diagnostic.near_boundary is None


def test_phase0b_wall_ratio_uses_combined_n10_and_direct_limit() -> None:
    limits = COMPARISON_LIMITS[ComparisonProfile.PHASE0B_VS_PHASE0A]['gb']
    diagnostic = build_direction_diagnostic(
        _group(start=1, candidate_value='1.03'),
        _group(start=6, candidate_value='0.95'),
        limits=limits,
    )
    assert diagnostic.direct_gate == 'ratio'
    assert diagnostic.direct_limit == Decimal('1.02')
    assert diagnostic.combined_ratio == Decimal('0.99')
    assert diagnostic.normalized_to_limit == Decimal('0.99') / Decimal('1.02')
    assert diagnostic.near_boundary is True


def test_absolute_pws_gate_normalizes_candidate_n10_median() -> None:
    limits = COMPARISON_LIMITS[ComparisonProfile.PHASE3_VS_PHASE0A]['sk']
    diagnostic = build_direction_diagnostic(
        _group(metric='pws', start=1, candidate_value='2100000000'),
        _group(metric='pws', start=6, candidate_value='2200000000'),
        limits=limits,
    )
    assert diagnostic.direct_gate == 'absolute'
    assert diagnostic.direct_limit == Decimal('2147483648')
    assert diagnostic.normalized_to_limit == Decimal('2150000000') / Decimal('2147483648')


def test_one_resolved_metric_cannot_have_ratio_and_absolute_direct_gates() -> None:
    with pytest.raises(ValueError, match='one direct gate'):
        resolve_direct_metric_gate('wall', {'wall_ratio': Decimal('1.05'), 'wall_seconds': Decimal('20')})
```

- [ ] **Step 4: Implement the minimal pure kernel**

Add `hashlib`、`json`、`Mapping` imports and implement the interfaces with `Decimal` only:

```python
PAIRED_PROTOCOL_VERSION: Final = 2
DirectGateKind: TypeAlias = Literal['none', 'ratio', 'absolute']
_DIRECT_GATE_KEYS: Final[dict[MetricName, tuple[str, str]]] = {
    'wall': ('wall_ratio', 'wall_seconds'),
    'pws': ('pws_ratio', 'pws_bytes'),
}


@dataclass(frozen=True)
class DirectionDiagnosticEvidence:
    metric: MetricName
    first_group_ratio: Decimal
    second_group_ratio: Decimal
    combined_ratio: Decimal
    directions_conflict: bool
    direct_gate: DirectGateKind
    direct_limit: Decimal | None
    normalized_to_limit: Decimal | None
    near_boundary: bool | None


def derive_comparison_key(
    *,
    protocol_version: int,
    pipeline: PipelineName,
    comparison_profile: ComparisonProfile,
    reference_label: ClosedBinaryLabel,
    candidate_label: ClosedBinaryLabel,
    input_sha256: str,
    reference_sha256: str,
    candidate_sha256: str,
) -> str:
    if type(protocol_version) is not int or protocol_version != PAIRED_PROTOCOL_VERSION:
        raise ValueError('formal comparison key requires paired protocol version 2')
    if (
        pipeline not in ('gb', 'sk')
        or not isinstance(comparison_profile, ComparisonProfile)
        or not isinstance(reference_label, ClosedBinaryLabel)
        or not isinstance(candidate_label, ClosedBinaryLabel)
    ):
        raise ValueError('comparison identity uses a non-closed pipeline/profile/label')
    hashes = (input_sha256, reference_sha256, candidate_sha256)
    if any(len(value) != 64 or any(character not in '0123456789abcdef' for character in value) for value in hashes):
        raise ValueError('comparison identity hashes must be lowercase SHA-256')
    payload = {
        'protocol_version': protocol_version,
        'pipeline': pipeline,
        'profile': comparison_profile.value,
        'reference_label': reference_label.value,
        'candidate_label': candidate_label.value,
        'input_sha256': input_sha256,
        'reference_sha256': reference_sha256,
        'candidate_sha256': candidate_sha256,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def resolve_direct_metric_gate(
    metric: MetricName,
    limits: Mapping[str, Decimal | int],
) -> tuple[DirectGateKind, Decimal | None]:
    ratio_key, absolute_key = _DIRECT_GATE_KEYS[metric]
    present = tuple(key for key in (ratio_key, absolute_key) if key in limits)
    if len(present) > 1:
        raise ValueError(f'{metric} resolved limits must contain one direct gate at most')
    if not present:
        return 'none', None
    key = present[0]
    raw_limit = limits[key]
    if isinstance(raw_limit, bool) or not isinstance(raw_limit, (Decimal, int)):
        raise ValueError(f'{metric} direct limit must be Decimal or integer bytes')
    limit = Decimal(raw_limit)
    _require_positive_finite(limit, f'{metric} direct limit')
    return ('ratio' if key == ratio_key else 'absolute'), limit


def merge_metric_groups(first: MetricGroup, second: MetricGroup) -> MetricGroup:
    _assert_groups_join(first, second)
    merged = MetricGroup(
        batch_id=first.batch_id,
        pipeline=first.pipeline,
        metric=first.metric,
        global_round_start=1,
        rounds=first.rounds + second.rounds,
    )
    validate_metric_group(merged)
    return merged


def build_direction_diagnostic(
    first: MetricGroup,
    second: MetricGroup,
    *,
    limits: Mapping[str, Decimal | int],
) -> DirectionDiagnosticEvidence:
    merged = merge_metric_groups(first, second)
    first_ratio = _median_ratio(first)
    second_ratio = _median_ratio(second)
    combined_ratio = _median_ratio(merged)
    directions_conflict = (first_ratio - Decimal(1)) * (second_ratio - Decimal(1)) < 0
    direct_gate, direct_limit = resolve_direct_metric_gate(first.metric, limits)
    if direct_gate == 'none':
        normalized = None
        near_boundary = None
    else:
        assert direct_limit is not None
        combined_value = (
            combined_ratio
            if direct_gate == 'ratio'
            else median(item.candidate.metric_value for item in merged.rounds)
        )
        normalized = combined_value / direct_limit
        near_boundary = abs(normalized - Decimal(1)) <= MANDATORY_EXPANSION_BOUNDARY
    return DirectionDiagnosticEvidence(
        metric=first.metric,
        first_group_ratio=first_ratio,
        second_group_ratio=second_ratio,
        combined_ratio=combined_ratio,
        directions_conflict=directions_conflict,
        direct_gate=direct_gate,
        direct_limit=direct_limit,
        normalized_to_limit=normalized,
        near_boundary=near_boundary,
    )
```

In `_validate_closed_profile_tables()`, call `resolve_direct_metric_gate(metric, limits)` for both `wall` and `pws` in every resolved profile/pipeline entry. Do not treat composite/stage/output keys as direct gates.

- [ ] **Step 5: Verify Task 1 GREEN and commit**

```powershell
uv run python -m pytest tests/rust_oracle/test_benchmark_protocol.py -q --basetemp .pytest-tmp/protocol-v2-kernel
uv run python -m ruff check tests/rust_oracle/benchmark_protocol.py tests/rust_oracle/test_benchmark_protocol.py
uv run python -m ruff format tests/rust_oracle/benchmark_protocol.py tests/rust_oracle/test_benchmark_protocol.py --check
git add -- tests/rust_oracle/benchmark_protocol.py tests/rust_oracle/test_benchmark_protocol.py
git diff --cached --name-only
git diff --cached --check
git commit -m "test(perf): add paired direction protocol v2"
```

Expected: focused tests and Ruff pass；cached paths are exactly the two listed files。

## Task 2: Version Batch, Comparison and Append-Only Ledger Identity

**Files:**
- Modify: `tests/rust_oracle/benchmark_protocol.py:166-176`
- Modify: `tests/rust_oracle/phase0_harness.py:258-660, 1015-1017, 2338-2373, 2907-2918`
- Modify: `tests/rust_oracle/test_phase0_harness.py:108-141, 1214-1255, 1840-2130`

**Interfaces:**
- Consumes: Task 1 `PAIRED_PROTOCOL_VERSION` and `derive_comparison_key()`。
- Produces: v2-only `AppendOnlyAttemptLedger.create()`；audit-capable `load()`；`BatchAttempt.protocol_version`；v2 batch/comparison directory identity。

- [ ] **Step 1: Add RED tests for v2 batch/comparison identity**

Change the test import to `from dataclasses import asdict, replace`，then add：

```python
def test_batch_id_explicitly_contains_protocol_v2_identity(tmp_path: Path) -> None:
    request = _request(tmp_path)
    identity = BenchmarkIdentity('3' * 64, '1' * 64, '2' * 64, '4' * 40, '5' * 64, '6' * 64)
    current = derive_batch_id(request, identity)
    legacy_payload = {'profile': request.comparison_profile.value, 'pipeline': request.pipeline, **asdict(identity)}
    legacy = hashlib.sha256(phase0_harness._canonical_json(legacy_payload)).hexdigest()
    assert current != legacy


def test_v2_ledger_metadata_records_exact_integer_protocol_version(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    payload = json.loads((ledger.attempt_directory / 'metadata.json').read_text(encoding='utf-8'))
    assert payload['protocol_version'] == 2
    assert type(payload['protocol_version']) is int
    assert ledger.protocol_version == 2


@pytest.mark.parametrize('invalid', (True, '2', 0, 3))
def test_ledger_rejects_unknown_or_non_integer_protocol_version(invalid: object, tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    _rewrite_metadata_and_matching_empty_journal(ledger.attempt_directory, protocol_version=invalid)
    with pytest.raises(HarnessFailure) as caught:
        AppendOnlyAttemptLedger.load(ledger.attempt_directory, _identity())
    assert caught.value.verdict is HarnessVerdict.INCOMPLETE_EVIDENCE
```

Use a test-only `_rewrite_metadata_and_matching_empty_journal()` helper that rewrites canonical metadata and the single journal anchor together; never edit the repository's real v1 ledger.

- [ ] **Step 2: Add RED synthetic v1 audit and history-protection tests**

Create `_write_synthetic_v1_terminal()` in `test_phase0_harness.py`. It writes an empty v1 attempt with metadata lacking `protocol_version`, an exact `INCONCLUSIVE` terminal and one matching journal anchor below `tmp_path`：

```python
def _write_synthetic_v1_terminal(tmp_path: Path, *, comparison_key: str) -> Path:
    comparison = tmp_path / 'rust' / 'target' / 'perf-local' / 'batches' / comparison_key
    attempt = comparison / 'attempt-0001'
    (attempt / 'records').mkdir(parents=True)
    (attempt / 'checkpoints').mkdir()
    (comparison / 'journal').mkdir()
    metadata = {
        'comparison_key': comparison_key,
        'attempt_number': 1,
        'identity': asdict(_identity()),
        'previous_attempt_head_sha256': None,
        'reason': 'FORMAL_START',
        'inherited_planned_outputs': [],
        'cleanup_only': False,
        'recovery_primary_verdict': None,
    }
    metadata_raw = phase0_harness._canonical_json(metadata)
    (attempt / 'metadata.json').write_bytes(metadata_raw)
    metadata_sha = hashlib.sha256(metadata_raw).hexdigest()
    terminal = {
        'checkpoint_head_sha256': metadata_sha,
        'primary_verdict': None,
        'raw_log_sha256': None,
        'record_count': 0,
        'record_head_sha256': metadata_sha,
        'verdict': HarnessVerdict.INCONCLUSIVE.value,
    }
    terminal_raw = phase0_harness._canonical_json(terminal)
    (attempt / 'terminal.json').write_bytes(terminal_raw)
    terminal_sha = hashlib.sha256(terminal_raw).hexdigest()
    state = phase0_harness._journal_state_payload(
        attempt_number=1,
        record_count=0,
        record_head_sha256=metadata_sha,
        checkpoint_head_sha256=metadata_sha,
        terminal_present=True,
        terminal_head_sha256=terminal_sha,
        verdict=HarnessVerdict.INCONCLUSIVE,
    )
    journal_raw = phase0_harness._canonical_json({'previous_journal_sha256': None, **state})
    (comparison / 'journal' / '000001.json').write_bytes(journal_raw)
    return attempt
```

Import `asdict` next to the existing `replace` import。For invalid-protocol tests, use this exact empty-ledger rewrite helper：

```python
def _rewrite_metadata_and_matching_empty_journal(attempt: Path, *, protocol_version: object) -> None:
    metadata_path = attempt / 'metadata.json'
    metadata = json.loads(metadata_path.read_text(encoding='utf-8'))
    metadata['protocol_version'] = protocol_version
    metadata_raw = phase0_harness._canonical_json(metadata)
    metadata_path.write_bytes(metadata_raw)
    metadata_sha = hashlib.sha256(metadata_raw).hexdigest()
    state = phase0_harness._journal_state_payload(
        attempt_number=1,
        record_count=0,
        record_head_sha256=metadata_sha,
        checkpoint_head_sha256=metadata_sha,
        terminal_present=False,
        terminal_head_sha256=None,
        verdict=None,
    )
    journal_raw = phase0_harness._canonical_json({'previous_journal_sha256': None, **state})
    (attempt.parent / 'journal' / '000001.json').write_bytes(journal_raw)
```

```python
def test_v1_metadata_without_protocol_version_loads_read_only(tmp_path: Path) -> None:
    attempt = _write_synthetic_v1_terminal(tmp_path, comparison_key='a' * 64)
    loaded = AppendOnlyAttemptLedger.load(attempt, _identity(), strict_identity=False)
    assert loaded.protocol_version == 1
    assert loaded.terminal_verdict is HarnessVerdict.INCONCLUSIVE


def test_v2_start_does_not_mutate_synthetic_v1_terminal(tmp_path: Path) -> None:
    v1_attempt = _write_synthetic_v1_terminal(tmp_path, comparison_key='a' * 64)
    terminal = v1_attempt / 'terminal.json'
    before = terminal.read_bytes()
    before_sha = hashlib.sha256(before).hexdigest()
    v2 = AppendOnlyAttemptLedger.create(
        tmp_path / 'rust' / 'target' / 'perf-local' / 'batches',
        _identity(),
        comparison_key='b' * 64,
    )
    assert v2.attempt_number == 1
    assert terminal.read_bytes() == before
    assert hashlib.sha256(terminal.read_bytes()).hexdigest() == before_sha


def test_current_runner_refuses_v1_attempt_as_resume(tmp_path: Path) -> None:
    attempt = _write_synthetic_v1_terminal(tmp_path, comparison_key='a' * 64)
    with pytest.raises(HarnessFailure, match='protocol'):
        phase0_harness._load_current_protocol_ledger(attempt, _identity())
```

- [ ] **Step 3: Run focused ledger tests and verify RED**

```powershell
uv run python -m pytest tests/rust_oracle/test_phase0_harness.py -q -k "protocol_version or synthetic_v1 or batch_id_explicitly" --basetemp .pytest-tmp/protocol-v2-ledger-red
```

Expected: FAIL because ledger metadata and `BatchAttempt` do not yet carry protocol version。

- [ ] **Step 4: Implement exact v1/v2 ledger loader boundaries**

Add `protocol_version: int` to both `BatchAttempt` and `AppendOnlyAttemptLedger`。`create()` remains parameter-free for protocol selection and always writes the current constant：

```python
metadata = {
    'protocol_version': PAIRED_PROTOCOL_VERSION,
    'comparison_key': comparison_key,
    'attempt_number': number,
    'identity': asdict(identity),
    'previous_attempt_head_sha256': previous_head,
    'reason': 'ENVIRONMENT_RECOVERED' if previous_head else 'FORMAL_START',
    'inherited_planned_outputs': inherited,
    'cleanup_only': cleanup_only,
    'recovery_primary_verdict': recovery_primary.value if recovery_primary else None,
}
```

At the top of `load()` close the metadata shape before reading fields：

```python
v1_keys = {
    'comparison_key', 'attempt_number', 'identity', 'previous_attempt_head_sha256',
    'reason', 'inherited_planned_outputs', 'cleanup_only', 'recovery_primary_verdict',
}
v2_keys = v1_keys | {'protocol_version'}
if set(metadata) == v1_keys:
    protocol_version = 1
elif set(metadata) == v2_keys and type(metadata['protocol_version']) is int and metadata['protocol_version'] == 2:
    protocol_version = 2
else:
    raise HarnessFailure(HarnessVerdict.INCOMPLETE_EVIDENCE, 'attempt metadata protocol/schema is invalid')
```

In `create()`, immediately after loading an existing previous attempt, require `previous.protocol_version == PAIRED_PROTOCOL_VERSION` before evaluating safe retry verdicts；this prevents a v2 attempt from being appended to a v1 comparison directory。When `load()` recursively loads attempt `N-1`, require `previous.protocol_version == protocol_version` so one directory cannot mix versions。Add a formal helper：

```python
def _load_current_protocol_ledger(
    directory: Path,
    identity: BenchmarkIdentity,
    *,
    strict_identity: bool = True,
) -> AppendOnlyAttemptLedger:
    ledger = AppendOnlyAttemptLedger.load(directory, identity, strict_identity=strict_identity)
    if ledger.protocol_version != PAIRED_PROTOCOL_VERSION:
        raise HarnessFailure(HarnessVerdict.INCOMPLETE_EVIDENCE, 'formal runner cannot resume protocol v1')
    return ledger
```

Replace formal runner/group/recovery loads with this helper；direct `AppendOnlyAttemptLedger.load()` remains audit-capable。

Change batch and comparison derivation to：

```python
def derive_batch_id(request: PairedBenchmarkRequest, identity: BenchmarkIdentity) -> str:
    payload = {
        'protocol_version': PAIRED_PROTOCOL_VERSION,
        'profile': request.comparison_profile.value,
        'pipeline': request.pipeline,
        **asdict(identity),
    }
    return hashlib.sha256(_canonical_json(payload)).hexdigest()


comparison_key = derive_comparison_key(
    protocol_version=PAIRED_PROTOCOL_VERSION,
    pipeline=request.pipeline,
    comparison_profile=request.comparison_profile,
    reference_label=request.reference_label,
    candidate_label=request.candidate_label,
    input_sha256=identity.input_sha256,
    reference_sha256=identity.reference_sha256,
    candidate_sha256=identity.candidate_sha256,
)
```

`_batch_attempt()` copies `ledger.protocol_version` into `BatchAttempt`。

- [ ] **Step 5: Verify Task 2 GREEN, inspect local v1 read-only state and commit**

```powershell
uv run python -m pytest tests/rust_oracle/test_phase0_harness.py -q -k "protocol_version or synthetic_v1 or batch_id or comparison_key or attempt_ledger" --basetemp .pytest-tmp/protocol-v2-ledger
$V1Terminal = 'rust/target/perf-local/batches/b6a98530a04b060b6d9739d804f1b928682b55e58ec7fae03bd779fb9b526149/attempt-0004/terminal.json'
if ((Get-FileHash -LiteralPath $V1Terminal -Algorithm SHA256).Hash.ToLowerInvariant() -ne 'd42940dfc48f208834efa103de6a08663e75d1ee09dd6804d4e2416ad90af71f') { throw 'sealed v1 terminal changed' }
uv run python -m ruff check tests/rust_oracle/benchmark_protocol.py tests/rust_oracle/phase0_harness.py tests/rust_oracle/test_phase0_harness.py
uv run python -m ruff format tests/rust_oracle/benchmark_protocol.py tests/rust_oracle/phase0_harness.py tests/rust_oracle/test_phase0_harness.py --check
git add -- tests/rust_oracle/benchmark_protocol.py tests/rust_oracle/phase0_harness.py tests/rust_oracle/test_phase0_harness.py
git diff --cached --name-only
git diff --cached --check
git commit -m "test(perf): version paired benchmark ledger"
```

Expected: focused tests pass；v1 terminal SHA unchanged；cached paths are exactly the three listed files。

## Task 3: Add Benchmark Evidence Schema v2 with v1 Read-Only Rebuild

**Files:**
- Modify: `tests/rust_oracle/evidence.py:300-363, 527-672, 848-1010, 1258-1331, 1904-1924`
- Modify: `tests/rust_oracle/test_evidence.py:776-825, 930-1260, 1687-1990`

**Interfaces:**
- Consumes: Task 1 `DirectionDiagnosticEvidence`、`derive_comparison_key()`、`resolve_direct_metric_gate()`。
- Produces: exact schema v1/v2 reader、read-only v1/v2 rebuild、formal v2-only builder/publisher、`expected_benchmark_artifact_name()`。

- [ ] **Step 1: Split test fixtures into explicit legacy v1 and formal v2 values**

Rename the current one-round helper to `_legacy_benchmark_manifest_evidence()`。Create `_benchmark_manifest_v2_evidence()` with：

- `schema_version=2`
- `protocol_version=2`
- wall and PWS rounds 1–5 by default
- exact four metric entries (`wall_median`, `pws_median`, `wall_ratio`, `pws_ratio`)
- `direction_diagnostics=()` for N=5
- `runtime_counts`、`sheet_dimensions`、reference/candidate `output_bytes`、`mismatches=()`、`local_log_sha256` and `verdict=VALIDATED`

For N=10 tests, construct wall/PWS rounds 1–10 and exactly two diagnostics ordered wall then PWS。

- [ ] **Step 2: Add RED dual-read/single-write tests**

```python
def test_legacy_v1_manifest_can_only_be_read_and_rebuilt() -> None:
    policy = EvidenceSanitizer.closed_policy()
    legacy = _legacy_benchmark_manifest_evidence()
    artifact = policy.rebuild_benchmark_manifest(legacy)
    restored = policy.read_benchmark_manifest(artifact.file_name, artifact.content.encode('utf-8'))
    assert restored == legacy
    assert policy.rebuild_benchmark_manifest(restored).content == artifact.content
    with pytest.raises(ValueError, match='protocol v2'):
        policy.build_benchmark_manifest(legacy)


def test_formal_writer_rejects_rebuilt_v1_artifact(tmp_path: Path) -> None:
    policy = EvidenceSanitizer.closed_policy()
    legacy_artifact = policy.rebuild_benchmark_manifest(_legacy_benchmark_manifest_evidence())
    with pytest.raises(ValueError, match='protocol v2'):
        policy.write_batch(
            destination_root=tmp_path / 'docs' / 'performance',
            artifacts=(legacy_artifact,),
            cleanup_state=AttemptState.CLEANUP_COMPLETE,
        )


def test_v1_rejects_v2_extra_keys_and_v2_requires_exact_keys() -> None:
    policy = EvidenceSanitizer.closed_policy()
    legacy = policy.rebuild_benchmark_manifest(_legacy_benchmark_manifest_evidence())
    legacy_payload = json.loads(legacy.content)
    legacy_payload['protocol_version'] = 2
    with pytest.raises(ValueError, match='keys'):
        policy.read_benchmark_manifest(legacy.file_name, json.dumps(legacy_payload).encode('utf-8'))
    v2 = policy.build_benchmark_manifest(_benchmark_manifest_v2_evidence())
    v2_payload = json.loads(v2.content)
    v2_payload['unknown'] = 1
    with pytest.raises(ValueError, match='keys'):
        policy.read_benchmark_manifest(v2.file_name, json.dumps(v2_payload).encode('utf-8'))
```

- [ ] **Step 3: Add RED diagnostic and artifact-identity tests**

```python
def test_v2_n5_requires_empty_diagnostics() -> None:
    value = _benchmark_manifest_v2_evidence(direction_diagnostics=(_wall_diagnostic(),))
    with pytest.raises(ValueError, match='N=5'):
        EvidenceSanitizer.closed_policy().build_benchmark_manifest(value)


def test_v2_n10_requires_wall_pws_diagnostics_in_fixed_order() -> None:
    value = _benchmark_manifest_v2_evidence(n=10, direction_diagnostics=(_pws_diagnostic(), _wall_diagnostic()))
    with pytest.raises(ValueError, match='wall.*pws'):
        EvidenceSanitizer.closed_policy().build_benchmark_manifest(value)


def test_v2_diagnostic_is_recomputed_from_rounds_and_limits() -> None:
    value = _benchmark_manifest_v2_evidence(n=10)
    bad = replace(value.direction_diagnostics[0], near_boundary=not value.direction_diagnostics[0].near_boundary)
    with pytest.raises(ValueError, match='direction diagnostic'):
        EvidenceSanitizer.closed_policy().build_benchmark_manifest(
            replace(value, direction_diagnostics=(bad, value.direction_diagnostics[1]))
        )


def test_v2_artifact_name_binds_input_and_reference_identity() -> None:
    policy = EvidenceSanitizer.closed_policy()
    base = policy.build_benchmark_manifest(_benchmark_manifest_v2_evidence())
    changed_input = policy.build_benchmark_manifest(_benchmark_manifest_v2_evidence(input_sha256='a' * 64))
    changed_reference = policy.build_benchmark_manifest(_benchmark_manifest_v2_evidence(reference_exe_sha256='b' * 64))
    assert base.file_name.startswith('benchmark-v2-')
    assert len({base.file_name, changed_input.file_name, changed_reference.file_name}) == 3
```

- [ ] **Step 4: Run evidence tests and verify RED**

```powershell
uv run python -m pytest tests/rust_oracle/test_evidence.py -q -k "benchmark or diagnostic or protocol_v2 or legacy_v1" --basetemp .pytest-tmp/protocol-v2-evidence-red
```

Expected: FAIL because schema 2、diagnostics and read-only rebuild do not exist。

- [ ] **Step 5: Implement the exact dual-reader/single-writer boundary**

Extend the typed value without introducing a free-form dict：

```python
@dataclass(frozen=True)
class BenchmarkManifestEvidence:
    schema_version: Literal[1, 2]
    profile: ComparisonProfile
    pipeline: Literal['gb', 'sk']
    input_alias: PathAlias
    input_sha256: str
    reference_label: ClosedBinaryLabel
    reference_exe_sha256: str
    candidate_label: ClosedBinaryLabel
    candidate_exe_sha256: str
    machine: MachineArtifactEvidence
    attempt_count: int
    prior_safe_verdicts: tuple[HarnessVerdict, ...]
    ledger_head_sha256: str
    first_group_sha256: str
    expanded_group_sha256: str | None
    rounds: tuple[BenchmarkRoundEvidence, ...]
    metrics: tuple[BenchmarkMetricEvidence, ...]
    runtime_counts: tuple[RuntimeCountEvidence, ...]
    sheet_dimensions: tuple[SheetDimensionEvidence, ...]
    output_bytes: tuple[OutputBytesEvidence, ...]
    mismatches: tuple[MismatchEvidence, ...]
    local_log_sha256: tuple[str, ...]
    verdict: HarnessVerdict
    protocol_version: Literal[2] | None = None
    direction_diagnostics: tuple[DirectionDiagnosticEvidence, ...] = ()
```

Implement one internal exact builder and two public authorities：

```python
def expected_benchmark_artifact_name(*, protocol_version: int, comparison_key: str) -> str:
    if type(protocol_version) is not int or protocol_version != PAIRED_PROTOCOL_VERSION:
        raise ValueError('formal artifact name requires protocol version 2')
    _require_hash(comparison_key, 64, 'comparison key')
    return f'benchmark-v2-{comparison_key[:16]}.json'


class EvidenceSanitizer:
    def build_benchmark_manifest(self, value: BenchmarkManifestEvidence) -> _SanitizedArtifact:
        if value.schema_version != 2 or value.protocol_version != PAIRED_PROTOCOL_VERSION:
            raise ValueError('formal benchmark publication requires schema/protocol v2')
        return self._build_benchmark_manifest(value, allow_legacy_v1=False)

    def rebuild_benchmark_manifest(self, value: BenchmarkManifestEvidence) -> _SanitizedArtifact:
        return self._build_benchmark_manifest(value, allow_legacy_v1=True)
```

`_build_benchmark_manifest()` keeps the current common-field validation。For schema v1 require `protocol_version is None` and empty diagnostics, emit the original exact key set and original `benchmark-<old-hash>.json` basename。For schema v2 require integer 2, validate diagnostics, add `protocol_version` and `direction_diagnostics` keys, derive the single comparison key with Task 1's helper, and call `expected_benchmark_artifact_name()`。

`read_benchmark_manifest()` must inspect `schema_version` only after duplicate-key JSON parsing, select one exact key tuple, reject bool/string/unknown versions, parse diagnostics to the frozen dataclass, and return no synthesized v2 fields for v1。

Validate diagnostics from serialized rounds and resolved `COMPARISON_LIMITS`：

```python
if formal_n == 5 and value.direction_diagnostics:
    raise ValueError('N=5 benchmark evidence requires empty direction diagnostics')
if formal_n == 10 and tuple(item.metric for item in value.direction_diagnostics) != ('wall', 'pws'):
    raise ValueError('N=10 benchmark evidence requires wall then pws diagnostics')
```

For each N=10 metric, recompute first/second/combined medians、strict conflict、direct limit、normalized value and `near_boundary` from the round evidence; compare exact `Decimal`/boolean/`None` values。

Keep `_rebuild_artifact()` on the formal builder so `write_batch()` rejects v1。Change only `_read_and_rebuild_staged_artifact()` to call `rebuild_benchmark_manifest()` so historical staged v1 artifacts remain audit-readable。

- [ ] **Step 6: Verify Task 3 GREEN and commit**

```powershell
uv run python -m pytest tests/rust_oracle/test_evidence.py -q --basetemp .pytest-tmp/protocol-v2-evidence
uv run python -m ruff check tests/rust_oracle/evidence.py tests/rust_oracle/test_evidence.py
uv run python -m ruff format tests/rust_oracle/evidence.py tests/rust_oracle/test_evidence.py --check
git add -- tests/rust_oracle/evidence.py tests/rust_oracle/test_evidence.py
git diff --cached --name-only
git diff --cached --check
git commit -m "test(evidence): add benchmark schema v2"
```

Expected: evidence suite and Ruff pass；cached paths are exactly the two listed files。

## Task 4: Integrate v2 Verdict Priority and Diagnostics into the Formal Runner

**Files:**
- Modify: `tests/rust_oracle/phase0_harness.py:2338-2515, 2842-3299`
- Modify: `tests/rust_oracle/test_phase0_harness.py:1214-1815`

**Interfaces:**
- Consumes: Tasks 1–3 protocol/ledger/evidence interfaces。
- Produces: N=5 empty diagnostics；N=10 wall/PWS diagnostics；closed-gate failure precedence；formal schema v2 evidence and recovery。

- [ ] **Step 1: Add RED tests for the complete verdict table**

Use existing `_request()`、`_approved_phase0a_payload()` and monkeypatched group runners。Add exact tests：

```text
test_v2_active_wall_conflict_near_boundary_is_inconclusive_after_all_gates_pass
test_v2_active_wall_conflict_decisive_pass_is_validated
test_v2_active_wall_conflict_decisive_fail_is_candidate_failed
test_v2_inactive_pws_conflict_is_diagnostic_only
test_v2_near_wall_conflict_cannot_hide_direct_pws_failure
test_v2_near_wall_conflict_cannot_hide_composite_or_stage_failure
test_v2_equal_to_one_group_direction_is_not_inconclusive
test_v2_expansion_stops_at_global_round_ten
test_v2_attempt_one_does_not_inherit_v1_inconclusive_as_prior_safe_verdict
```

Use this exact synthetic data table；each first/second value is repeated for its five-round group：

| Test | Profile/pipeline | Wall first / second | PWS first / second | Additional gate data | Expected |
|---|---|---:|---:|---|---|
| active near conflict | `phase0b-vs-phase0a` / GB | `1.03 / 0.99` | `1.00 / 1.00` | combined wall `1.01`, limit `1.02` | `INCONCLUSIVE` |
| active decisive pass | `phase0b-vs-phase0a` / GB | `1.04 / 0.90` | `1.00 / 1.00` | combined wall `0.97` | `VALIDATED` |
| active decisive fail | `phase0b-vs-phase0a` / GB | `0.99 / 1.07` | `1.00 / 1.00` | combined wall `1.03 > 1.02` | `CANDIDATE_FAILED` |
| inactive PWS conflict | `phase0b-vs-phase0a` / GB | `0.95 / 0.95` | `0.99 / 1.01` | PWS has no direct gate | `VALIDATED` |
| wall near + PWS direct fail | `phase1-vs-phase0a` / GB | `1.12 / 0.98` | `1.06 / 1.06` | combined wall `1.05`; PWS limit `1.05` | `CANDIDATE_FAILED` |
| wall near + composite fail | `phase4-vs-phase3` / SK | `1.02 / 0.98` | `0.95 / 0.95` | ingest ratio `0.95`; `min(ingest,pws)=0.95 > 0.90` | `CANDIDATE_FAILED` |
| PWS near + stage-only fail | `phase4-vs-phase3` / GB | `1.00 / 1.00` | `1.12 / 0.98` | combined PWS `1.05`; ingest ratio `1.06 > 1.05` | `CANDIDATE_FAILED` |
| exact-one direction | `phase0b-vs-phase0a` / GB | `1.00 / 1.02` | `1.00 / 1.00` | strict product is zero | `VALIDATED` |

The mixed failure assertion is mandatory：

```python
with pytest.raises(HarnessFailure) as caught:
    phase0_harness.run_paired_normal_batch(request)
assert caught.value.verdict is HarnessVerdict.CANDIDATE_FAILED
```

Also assert no evidence path exists on `INCONCLUSIVE`/`CANDIDATE_FAILED` and the terminal ledger contains the same verdict。

- [ ] **Step 2: Add RED tests for schema v2 evidence and prepared recovery**

Add `test_v2_evidence_rejects_attempt_comparison_key_mismatch`。Construct the otherwise-valid `BatchAttempt` used by the nearby evidence test, replace only its key, and assert：

```python
bad_attempt = replace(valid_attempt, comparison_key='f' * 64)
with pytest.raises(HarnessFailure) as caught:
    phase0_harness._build_paired_evidence(
        request,
        _metric_group('wall'),
        _metric_group('pws'),
        bad_attempt,
        identity,
        (),
    )
assert caught.value.verdict is HarnessVerdict.INCOMPLETE_EVIDENCE
```

Then add the success/recovery assertions below。

For `test_v2_n5_success_publishes_empty_direction_diagnostics`, reuse the current successful paired-run monkeypatch setup and make these exact assertions：

```python
result = phase0_harness.run_paired_normal_batch(request)
artifact = request.evidence_path.read_bytes()
restored = EvidenceSanitizer.closed_policy().read_benchmark_manifest(request.evidence_path.name, artifact)
assert restored.schema_version == 2
assert restored.protocol_version == 2
assert restored.direction_diagnostics == ()
assert restored.attempt_count == 1
assert restored.prior_safe_verdicts == ()
assert result.attempt.protocol_version == 2
```

For `test_v2_n10_success_publishes_wall_then_pws_diagnostics`, force mandatory expansion through the existing group-runner monkeypatch and assert：

```python
phase0_harness.run_paired_normal_batch(request)
restored = EvidenceSanitizer.closed_policy().read_benchmark_manifest(
    request.evidence_path.name,
    request.evidence_path.read_bytes(),
)
assert tuple(item.metric for item in restored.direction_diagnostics) == ('wall', 'pws')
```

For `test_prepared_evidence_recovery_rejects_v1_payload`, create the legacy prepared payload from typed data, not a free-form JSON string：

```python
v2_source = phase0_harness._build_paired_evidence(
    request,
    _metric_group('wall'),
    _metric_group('pws'),
    BatchAttempt(
        protocol_version=2,
        comparison_key='a' * 64,
        batch_id='9' * 64,
        attempt_number=1,
        state=AttemptState.CLEANUP_COMPLETE,
        previous_attempt_head_sha256=None,
        first_group_sha256='8' * 64,
        expanded_group_sha256=None,
        ledger_head_sha256='7' * 64,
        attempt_directory=tmp_path,
    ),
    BenchmarkIdentity('3' * 64, '1' * 64, '2' * 64, '4' * 40, '5' * 64, '6' * 64),
    (),
)
legacy_source = replace(v2_source, schema_version=1, protocol_version=None, direction_diagnostics=())
legacy_artifact = EvidenceSanitizer.closed_policy().rebuild_benchmark_manifest(legacy_source)
payload = {
    'artifact_basename': legacy_artifact.file_name,
    'artifact_sha256': hashlib.sha256(legacy_artifact.content.encode('utf-8')).hexdigest(),
    'artifact_content': legacy_artifact.content,
}
with pytest.raises(HarnessFailure) as caught:
    phase0_harness._rebuild_prepared_artifact(payload)
assert caught.value.verdict is HarnessVerdict.INCOMPLETE_EVIDENCE
```

- [ ] **Step 3: Run runner-focused tests and verify RED**

```powershell
uv run python -m pytest tests/rust_oracle/test_phase0_harness.py -q -k "v2_ or prepared_evidence or paired_batch" --basetemp .pytest-tmp/protocol-v2-runner-red
```

Expected: FAIL because current runner raises `INCONCLUSIVE` before combined gate evaluation and builds schema v1。

- [ ] **Step 4: Implement the exact N=10 decision order**

Replace the early direction veto in `run_paired_normal_batch()` with：

```python
diagnostics: tuple[DirectionDiagnosticEvidence, ...] = ()
if _paired_groups_require_expansion(request, wall_first, pws_first):
    limits = COMPARISON_LIMITS[request.comparison_profile][request.pipeline]
    wall = merge_metric_groups(wall_first, wall_second)
    pws = merge_metric_groups(pws_first, pws_second)
    diagnostics = (
        build_direction_diagnostic(wall_first, wall_second, limits=limits),
        build_direction_diagnostic(pws_first, pws_second, limits=limits),
    )

assert_same_benchmark_batch(wall, pws)
_assert_identity_unchanged(identity, _capture_identity(request))

# 所有明确 closed-gate 失败优先，不能被另一指标的临界冲突改写。
_evaluate_closed_profile(request, wall, pws, baseline)

if any(
    item.directions_conflict
    and item.direct_gate != 'none'
    and item.near_boundary is True
    for item in diagnostics
):
    raise HarnessFailure(
        HarnessVerdict.INCONCLUSIVE,
        'active direct metric remains direction-conflicted near its v2 limit',
    )
```

Do not add a retry、third group or new CLI field。

- [ ] **Step 5: Build and publish schema v2 evidence only**

Change `_build_paired_evidence()` signature to consume diagnostics：

```text
def _build_paired_evidence(
    request: PairedBenchmarkRequest,
    wall: MetricGroup,
    pws: MetricGroup,
    attempt: BatchAttempt,
    identity: BenchmarkIdentity,
    direction_diagnostics: tuple[DirectionDiagnosticEvidence, ...],
) -> BenchmarkManifestEvidence:
```

Before constructing the typed value, close the ledger/evidence comparison identity：

```python
expected_comparison_key = derive_comparison_key(
    protocol_version=PAIRED_PROTOCOL_VERSION,
    pipeline=request.pipeline,
    comparison_profile=request.comparison_profile,
    reference_label=request.reference_label,
    candidate_label=request.candidate_label,
    input_sha256=identity.input_sha256,
    reference_sha256=identity.reference_sha256,
    candidate_sha256=identity.candidate_sha256,
)
if attempt.protocol_version != PAIRED_PROTOCOL_VERSION or attempt.comparison_key != expected_comparison_key:
    raise HarnessFailure(HarnessVerdict.INCOMPLETE_EVIDENCE, 'paired attempt protocol/comparison identity is invalid')
```

In the complete `BenchmarkManifestEvidence` constructor call inside `_build_paired_evidence()`, apply this exact schema/direction diff and leave the other named arguments byte-for-byte unchanged：

```diff
-        schema_version=1,
+        schema_version=2,
+        protocol_version=PAIRED_PROTOCOL_VERSION,
+        direction_diagnostics=direction_diagnostics,
```

Before capture, derive the expected basename from the same comparison key。Keep `request.evidence_path.name == artifact.file_name` fail-closed check。`_rebuild_prepared_artifact()` and `_recover_prepared_evidence()` must use the formal v2 builder；historical v1 rebuild remains confined to staged audit code in Task 3。

- [ ] **Step 6: Verify Task 4 GREEN and commit**

```powershell
uv run python -m pytest tests/rust_oracle/test_phase0_harness.py -q --basetemp .pytest-tmp/protocol-v2-runner
uv run python -m pytest tests/rust_oracle/test_benchmark_protocol.py tests/rust_oracle/test_evidence.py tests/rust_oracle/test_phase0_harness.py -q --basetemp .pytest-tmp/protocol-v2-integration
uv run python -m ruff check tests/rust_oracle/benchmark_protocol.py tests/rust_oracle/evidence.py tests/rust_oracle/phase0_harness.py tests/rust_oracle/test_benchmark_protocol.py tests/rust_oracle/test_evidence.py tests/rust_oracle/test_phase0_harness.py
uv run python -m ruff format tests/rust_oracle/benchmark_protocol.py tests/rust_oracle/evidence.py tests/rust_oracle/phase0_harness.py tests/rust_oracle/test_benchmark_protocol.py tests/rust_oracle/test_evidence.py tests/rust_oracle/test_phase0_harness.py --check
git add -- tests/rust_oracle/phase0_harness.py tests/rust_oracle/test_phase0_harness.py
git diff --cached --name-only
git diff --cached --check
git commit -m "test(perf): apply protocol v2 verdict order"
```

Expected: focused/integration tests and Ruff pass；cached paths are exactly the two runner files。

## Task 5: Synchronize Documentation, Run the Full Gate and Review the Code

**Files:**
- Modify: `docs/performance/README.md:29-47`
- Modify: `docs/superpowers/specs/2026-07-11-rust-output-ingest-continuous-performance-design.md:1120-1132`

**Interfaces:**
- Consumes: completed Tasks 1–4。
- Produces: one authoritative operational description and fresh full verification evidence；no benchmark business artifact。

- [ ] **Step 1: Replace the obsolete v1 direction rule in the original design**

Replace item 6 in §10.5 with this exact v2 rule：

```text
6. protocol v2 在 N=10 后先评价全部 direct/composite/stage closed gates；任一明确失败优先为 CANDIDATE_FAILED。只有全部门禁通过后，当前 resolved profile/pipeline 中存在 direct wall/PWS gate、两组 ratio 严格跨越 1，且 combined N=10 value 相对 direct limit 仍在 ±3% 内，才为 INCONCLUSIVE；inactive metric 及 composite/stage gate 只记录 direction diagnostic，不产生方向 veto；
```

Add a one-line reference to the approved v2 design。Do not rewrite unrelated sections。

- [ ] **Step 2: Update the paired CLI/evidence documentation**

In `docs/performance/README.md` document：

```text
- paired CLI always runs protocol 2 and exposes no protocol selector;
- batch/comparison/ledger/evidence/artifact identity all carry protocol 2;
- v2 basename is benchmark-v2-<comparison_key[:16]>.json;
- reader/rebuilder may audit exact v1, formal writer/publisher writes v2 only;
- GB must validate before the one allowed SK run; neither failure may be resampled.
```

Keep the existing exit-code table and raw/versioned boundary。

- [ ] **Step 3: Run the fresh full Python gate**

```powershell
uv run python -m pytest tests/rust_oracle -q --basetemp .pytest-tmp/protocol-v2-full
uv run python -m ruff check tests/rust_oracle
uv run python -m ruff format tests/rust_oracle --check
git diff --check
```

Expected: all `tests/rust_oracle` tests pass with zero failures；Ruff、format and diff checks exit 0。No Cargo command is required because this protocol-only change does not modify Rust or dependencies。

- [ ] **Step 4: Run independent reviews before the docs commit**

Dispatch read-only review scopes：

```text
python_reviewer:
  fixed base = 336a87ce743009e85488ced6d3d55c17bdf99b25
  review protocol identity, ledger compatibility, verdict priority, evidence exactness,
  retry prohibition, cleanup and all Python tests; do not edit.

doc_reviewer:
  review only README + original design v2 amendment against the approved v2 spec;
  verify no sensitive path/value and no contradiction; do not edit.
```

Any Critical/Important finding blocks formal GB。Fix findings in the owning files, rerun their focused tests and the full gate, stage only those files, require `git diff --cached --check`, and commit with `fix(perf): address protocol v2 review`；then request fresh review against the new HEAD。

- [ ] **Step 5: Commit only documentation after reviews are clean**

```powershell
git add -- docs/performance/README.md docs/superpowers/specs/2026-07-11-rust-output-ingest-continuous-performance-design.md
git diff --cached --name-only
git diff --cached --check
git commit -m "docs(perf): document paired protocol v2"
git status --short
```

Expected: cached paths are exactly the two docs；post-commit execution worktree is clean。

## Task 6: Execute the One Allowed Formal GB v2 Batch

**Files:**
- Read: fixed EXEs、approved baseline、sealed v1 terminal。
- Create on `VALIDATED` only: one GB v2 benchmark artifact plus its batch marker under `docs/performance/runs/phase0b-v2/`。

**Interfaces:**
- Consumes: clean reviewed implementation, fixed EXEs, `$env:COSTING_GB_SAMPLE`。
- Produces: terminal GB v2 ledger and, only on success, sanitized GB v2 evidence。

**Execution shell rule:** Task 6 Steps 1–4 share deliberately in-memory PowerShell variables。When an agent executes this task, concatenate the four PowerShell blocks into one native PowerShell script and run it in one `shell_command` process；do not split them across fresh shells。

- [ ] **Step 1: Perform fail-closed preflight without rebuilding**

```powershell
if (git status --porcelain=v1 --untracked-files=all) { throw 'formal GB requires a clean worktree' }
$Phase0AExe = 'rust/target/perf-builds/phase0a/reference/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$Phase0BExe = 'rust/target/perf-builds/phase0b/instrumented/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$Phase0AManifest = 'docs/performance/baselines/2026-07-11-windows-x64-phase0a.json'
$V1Terminal = 'rust/target/perf-local/batches/b6a98530a04b060b6d9739d804f1b928682b55e58ec7fae03bd779fb9b526149/attempt-0004/terminal.json'
$ReferenceSha = (Get-FileHash -LiteralPath $Phase0AExe -Algorithm SHA256).Hash.ToLowerInvariant()
$CandidateSha = (Get-FileHash -LiteralPath $Phase0BExe -Algorithm SHA256).Hash.ToLowerInvariant()
$ManifestSha = (Get-FileHash -LiteralPath $Phase0AManifest -Algorithm SHA256).Hash.ToLowerInvariant()
$V1TerminalSha = (Get-FileHash -LiteralPath $V1Terminal -Algorithm SHA256).Hash.ToLowerInvariant()
if ($ReferenceSha -ne 'f75f7ee17cc222765537f6bbe02f90e76cd041c55c8990b0261788e6fa63db56') { throw 'reference EXE SHA drift' }
if ($CandidateSha -ne 'd06470e4e7c9e6dc8f54efc9d26d996d3cbbbddec04cb7dffef6e6869802b629') { throw 'candidate EXE SHA drift' }
if ($ManifestSha -ne '17faa1f08a0601fac52186ab64a4ebb4519312a259c59b7d64e69c748bab56df') { throw 'Phase 0A manifest SHA drift' }
if ($V1TerminalSha -ne 'd42940dfc48f208834efa103de6a08663e75d1ee09dd6804d4e2416ad90af71f') { throw 'sealed v1 terminal SHA drift' }
if (-not $env:COSTING_GB_SAMPLE) { throw 'COSTING_GB_SAMPLE is not set' }
$GbInput = (Resolve-Path -LiteralPath $env:COSTING_GB_SAMPLE).Path
$GbInputSha = (Get-FileHash -LiteralPath $GbInput -Algorithm SHA256).Hash.ToLowerInvariant()
if ($GbInputSha -ne '6aa5e3e7fdc547ebaaef968eb5b95d4d630c4ec9915184f94346f60687b8e7ee') { throw 'GB input SHA drift' }
```

Do not run any `cargo build` command。

- [ ] **Step 2: Derive the only legal GB comparison/evidence identity**

```powershell
$GbComparisonKey = (uv run python -c "from tests.rust_oracle.benchmark_protocol import PAIRED_PROTOCOL_VERSION,ClosedBinaryLabel,ComparisonProfile,derive_comparison_key; print(derive_comparison_key(protocol_version=PAIRED_PROTOCOL_VERSION,pipeline='gb',comparison_profile=ComparisonProfile.PHASE0B_VS_PHASE0A,reference_label=ClosedBinaryLabel.PHASE0A,candidate_label=ClosedBinaryLabel.PHASE0B,input_sha256='$GbInputSha',reference_sha256='$ReferenceSha',candidate_sha256='$CandidateSha'))").Trim()
$GbEvidenceName = (uv run python -c "from tests.rust_oracle.evidence import expected_benchmark_artifact_name; print(expected_benchmark_artifact_name(protocol_version=2,comparison_key='$GbComparisonKey'))").Trim()
$GbEvidence = "docs/performance/runs/phase0b-v2/$GbEvidenceName"
if (Test-Path -LiteralPath "rust/target/perf-local/batches/$GbComparisonKey") { throw 'GB v2 comparison already exists; do not rerun' }
if (Test-Path -LiteralPath $GbEvidence) { throw 'GB v2 evidence already exists; do not overwrite' }
```

- [ ] **Step 3: Run formal GB exactly once and stop on every nonzero verdict**

```powershell
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline gb --input "$GbInput" --reference-executable $Phase0AExe --candidate-executable $Phase0BExe --reference-label phase0a --candidate-label phase0b --comparison-profile phase0b-vs-phase0a --phase0a-manifest $Phase0AManifest --local-root rust/target/perf-local --evidence-path $GbEvidence
$GbExit = $LASTEXITCODE
if ($GbExit -ne 0) { throw "GB protocol v2 is terminal with exit $GbExit; do not rerun and do not run SK" }
```

Expected success condition: exit 0, followed by Step 4 typed evidence validation proving verdict `VALIDATED`。Any nonzero verdict is terminal evidence；record it and end this plan execution without Task 7。

- [ ] **Step 4: Validate and commit GB evidence separately from code**

```powershell
uv run python -c "from pathlib import Path; from tests.rust_oracle.evidence import EvidenceSanitizer; p=Path(r'$GbEvidence'); v=EvidenceSanitizer.closed_policy().read_benchmark_manifest(p.name,p.read_bytes()); assert v.schema_version==2 and v.protocol_version==2 and v.pipeline=='gb' and v.verdict.value=='VALIDATED'"
git add -- docs/performance/runs/phase0b-v2
uv run python -m tests.rust_oracle.evidence scan --root docs/performance --staged
git diff --cached --name-only
git diff --cached --check
git commit -m "docs(perf): validate phase0b v2 gb"
git status --short
```

Expected: staged scan passes；the commit contains only the GB benchmark artifact and its generated batch marker；post-commit worktree is clean。This evidence-only commit is required before SK because the closed CLI accepts no operator-supplied prior-evidence claim and SK must start from a clean repository state。

## Task 7: Execute the One Allowed Formal SK v2 Batch and Resume Phase 1

**Files:**
- Read: committed GB evidence、fixed EXEs、approved baseline、sealed v1 terminal。
- Create on `VALIDATED` only: one SK v2 benchmark artifact plus batch marker。

**Interfaces:**
- Consumes: successful committed Task 6 and `$env:COSTING_SK_SAMPLE`。
- Produces: terminal SK v2 ledger；sanitized evidence on success；Phase 1 go/no-go。

**Execution shell rule:** Task 7 Steps 1–3 share deliberately in-memory PowerShell variables。Concatenate those three PowerShell blocks into one native PowerShell script and run it in one `shell_command` process；Step 4 is a separate self-contained final audit。

- [ ] **Step 1: Re-run immutable preflight and derive the SK identity**

```powershell
if (git status --porcelain=v1 --untracked-files=all) { throw 'formal SK requires a clean worktree' }
$Phase0AExe = 'rust/target/perf-builds/phase0a/reference/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$Phase0BExe = 'rust/target/perf-builds/phase0b/instrumented/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$Phase0AManifest = 'docs/performance/baselines/2026-07-11-windows-x64-phase0a.json'
$V1Terminal = 'rust/target/perf-local/batches/b6a98530a04b060b6d9739d804f1b928682b55e58ec7fae03bd779fb9b526149/attempt-0004/terminal.json'
$ReferenceSha = (Get-FileHash -LiteralPath $Phase0AExe -Algorithm SHA256).Hash.ToLowerInvariant()
$CandidateSha = (Get-FileHash -LiteralPath $Phase0BExe -Algorithm SHA256).Hash.ToLowerInvariant()
$ManifestSha = (Get-FileHash -LiteralPath $Phase0AManifest -Algorithm SHA256).Hash.ToLowerInvariant()
$V1TerminalSha = (Get-FileHash -LiteralPath $V1Terminal -Algorithm SHA256).Hash.ToLowerInvariant()
if ($ReferenceSha -ne 'f75f7ee17cc222765537f6bbe02f90e76cd041c55c8990b0261788e6fa63db56') { throw 'reference EXE SHA drift before SK' }
if ($CandidateSha -ne 'd06470e4e7c9e6dc8f54efc9d26d996d3cbbbddec04cb7dffef6e6869802b629') { throw 'candidate EXE SHA drift before SK' }
if ($ManifestSha -ne '17faa1f08a0601fac52186ab64a4ebb4519312a259c59b7d64e69c748bab56df') { throw 'Phase 0A manifest SHA drift before SK' }
if ($V1TerminalSha -ne 'd42940dfc48f208834efa103de6a08663e75d1ee09dd6804d4e2416ad90af71f') { throw 'v1 terminal SHA drift before SK' }
$GbInputSha = '6aa5e3e7fdc547ebaaef968eb5b95d4d630c4ec9915184f94346f60687b8e7ee'
$GbComparisonKey = (uv run python -c "from tests.rust_oracle.benchmark_protocol import PAIRED_PROTOCOL_VERSION,ClosedBinaryLabel,ComparisonProfile,derive_comparison_key; print(derive_comparison_key(protocol_version=PAIRED_PROTOCOL_VERSION,pipeline='gb',comparison_profile=ComparisonProfile.PHASE0B_VS_PHASE0A,reference_label=ClosedBinaryLabel.PHASE0A,candidate_label=ClosedBinaryLabel.PHASE0B,input_sha256='$GbInputSha',reference_sha256='$ReferenceSha',candidate_sha256='$CandidateSha'))").Trim()
$GbEvidenceName = (uv run python -c "from tests.rust_oracle.evidence import expected_benchmark_artifact_name; print(expected_benchmark_artifact_name(protocol_version=2,comparison_key='$GbComparisonKey'))").Trim()
$GbEvidence = "docs/performance/runs/phase0b-v2/$GbEvidenceName"
if (-not (Test-Path -LiteralPath $GbEvidence -PathType Leaf)) { throw 'committed GB v2 evidence is missing; do not run SK' }
$GbMarker = (uv run python -c "from pathlib import Path; from tests.rust_oracle.evidence import EvidenceSanitizer,_batch_commit_marker; p=Path(r'$GbEvidence'); policy=EvidenceSanitizer.closed_policy(); v=policy.read_benchmark_manifest(p.name,p.read_bytes()); assert v.schema_version==2 and v.protocol_version==2 and v.pipeline=='gb' and v.verdict.value=='VALIDATED'; a=policy.build_benchmark_manifest(v); n,c=_batch_commit_marker((a,)); m=p.parent/n; assert m.read_bytes()==c.encode('utf-8'); print(m.as_posix())").Trim()
if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $GbMarker -PathType Leaf)) { throw 'GB v2 typed artifact/marker validation failed; do not run SK' }
git cat-file -e "HEAD:$GbEvidence"
if ($LASTEXITCODE -ne 0) { throw 'GB v2 evidence is not tracked by current HEAD; do not run SK' }
git cat-file -e "HEAD:$GbMarker"
if ($LASTEXITCODE -ne 0) { throw 'GB v2 marker is not tracked by current HEAD; do not run SK' }
if (-not $env:COSTING_SK_SAMPLE) { throw 'COSTING_SK_SAMPLE is not set' }
$SkInput = (Resolve-Path -LiteralPath $env:COSTING_SK_SAMPLE).Path
$SkInputSha = (Get-FileHash -LiteralPath $SkInput -Algorithm SHA256).Hash.ToLowerInvariant()
if ($SkInputSha -ne '6eac3c6c9ea0eb3e98ca11fb3829914be63e932595b3e3c613f0da46b385d64f') { throw 'SK input SHA drift' }
$SkComparisonKey = (uv run python -c "from tests.rust_oracle.benchmark_protocol import PAIRED_PROTOCOL_VERSION,ClosedBinaryLabel,ComparisonProfile,derive_comparison_key; print(derive_comparison_key(protocol_version=PAIRED_PROTOCOL_VERSION,pipeline='sk',comparison_profile=ComparisonProfile.PHASE0B_VS_PHASE0A,reference_label=ClosedBinaryLabel.PHASE0A,candidate_label=ClosedBinaryLabel.PHASE0B,input_sha256='$SkInputSha',reference_sha256='$ReferenceSha',candidate_sha256='$CandidateSha'))").Trim()
$SkEvidenceName = (uv run python -c "from tests.rust_oracle.evidence import expected_benchmark_artifact_name; print(expected_benchmark_artifact_name(protocol_version=2,comparison_key='$SkComparisonKey'))").Trim()
$SkEvidence = "docs/performance/runs/phase0b-v2/$SkEvidenceName"
if (Test-Path -LiteralPath "rust/target/perf-local/batches/$SkComparisonKey") { throw 'SK v2 comparison already exists; do not rerun' }
if (Test-Path -LiteralPath $SkEvidence) { throw 'SK v2 evidence already exists; do not overwrite' }
```

- [ ] **Step 2: Run formal SK exactly once and stop on every nonzero verdict**

```powershell
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline sk --input "$SkInput" --reference-executable $Phase0AExe --candidate-executable $Phase0BExe --reference-label phase0a --candidate-label phase0b --comparison-profile phase0b-vs-phase0a --phase0a-manifest $Phase0AManifest --local-root rust/target/perf-local --evidence-path $SkEvidence
$SkExit = $LASTEXITCODE
if ($SkExit -ne 0) { throw "SK protocol v2 is terminal with exit $SkExit; do not rerun" }
```

Expected success condition: exit 0, followed by Step 3 typed evidence validation proving verdict `VALIDATED`。Any nonzero verdict is terminal and blocks Phase 1。

- [ ] **Step 3: Validate and commit SK evidence**

```powershell
uv run python -c "from pathlib import Path; from tests.rust_oracle.evidence import EvidenceSanitizer; p=Path(r'$SkEvidence'); v=EvidenceSanitizer.closed_policy().read_benchmark_manifest(p.name,p.read_bytes()); assert v.schema_version==2 and v.protocol_version==2 and v.pipeline=='sk' and v.verdict.value=='VALIDATED'"
git add -- docs/performance/runs/phase0b-v2
uv run python -m tests.rust_oracle.evidence scan --root docs/performance --staged
git diff --cached --name-only
git diff --cached --check
git commit -m "docs(perf): validate phase0b v2 sk"
git status --short
```

Expected: the commit contains only the SK artifact and its marker；scanner/diff pass；worktree clean。

- [ ] **Step 4: Final Phase 0B audit and handoff**

Verify：

```powershell
$Phase0AExe = 'rust/target/perf-builds/phase0a/reference/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$Phase0BExe = 'rust/target/perf-builds/phase0b/instrumented/x86_64-pc-windows-msvc/release/costing-calculate.exe'
$Phase0AManifest = 'docs/performance/baselines/2026-07-11-windows-x64-phase0a.json'
$V1Terminal = 'rust/target/perf-local/batches/b6a98530a04b060b6d9739d804f1b928682b55e58ec7fae03bd779fb9b526149/attempt-0004/terminal.json'
$ReferenceSha = (Get-FileHash -LiteralPath $Phase0AExe -Algorithm SHA256).Hash.ToLowerInvariant()
$CandidateSha = (Get-FileHash -LiteralPath $Phase0BExe -Algorithm SHA256).Hash.ToLowerInvariant()
if ($ReferenceSha -ne 'f75f7ee17cc222765537f6bbe02f90e76cd041c55c8990b0261788e6fa63db56') { throw 'final reference EXE SHA drift' }
if ($CandidateSha -ne 'd06470e4e7c9e6dc8f54efc9d26d996d3cbbbddec04cb7dffef6e6869802b629') { throw 'final candidate EXE SHA drift' }
if ((Get-FileHash -LiteralPath $Phase0AManifest -Algorithm SHA256).Hash.ToLowerInvariant() -ne '17faa1f08a0601fac52186ab64a4ebb4519312a259c59b7d64e69c748bab56df') { throw 'final Phase 0A manifest SHA drift' }
if ((Get-FileHash -LiteralPath $V1Terminal -Algorithm SHA256).Hash.ToLowerInvariant() -ne 'd42940dfc48f208834efa103de6a08663e75d1ee09dd6804d4e2416ad90af71f') { throw 'final v1 terminal SHA drift' }
$GbComparisonKey = (uv run python -c "from tests.rust_oracle.benchmark_protocol import PAIRED_PROTOCOL_VERSION,ClosedBinaryLabel,ComparisonProfile,derive_comparison_key; print(derive_comparison_key(protocol_version=PAIRED_PROTOCOL_VERSION,pipeline='gb',comparison_profile=ComparisonProfile.PHASE0B_VS_PHASE0A,reference_label=ClosedBinaryLabel.PHASE0A,candidate_label=ClosedBinaryLabel.PHASE0B,input_sha256='6aa5e3e7fdc547ebaaef968eb5b95d4d630c4ec9915184f94346f60687b8e7ee',reference_sha256='$ReferenceSha',candidate_sha256='$CandidateSha'))").Trim()
$SkComparisonKey = (uv run python -c "from tests.rust_oracle.benchmark_protocol import PAIRED_PROTOCOL_VERSION,ClosedBinaryLabel,ComparisonProfile,derive_comparison_key; print(derive_comparison_key(protocol_version=PAIRED_PROTOCOL_VERSION,pipeline='sk',comparison_profile=ComparisonProfile.PHASE0B_VS_PHASE0A,reference_label=ClosedBinaryLabel.PHASE0A,candidate_label=ClosedBinaryLabel.PHASE0B,input_sha256='6eac3c6c9ea0eb3e98ca11fb3829914be63e932595b3e3c613f0da46b385d64f',reference_sha256='$ReferenceSha',candidate_sha256='$CandidateSha'))").Trim()
$GbEvidenceName = (uv run python -c "from tests.rust_oracle.evidence import expected_benchmark_artifact_name; print(expected_benchmark_artifact_name(protocol_version=2,comparison_key='$GbComparisonKey'))").Trim()
$SkEvidenceName = (uv run python -c "from tests.rust_oracle.evidence import expected_benchmark_artifact_name; print(expected_benchmark_artifact_name(protocol_version=2,comparison_key='$SkComparisonKey'))").Trim()
$GbEvidence = "docs/performance/runs/phase0b-v2/$GbEvidenceName"
$SkEvidence = "docs/performance/runs/phase0b-v2/$SkEvidenceName"
$GbMarker = (uv run python -c "from pathlib import Path; from tests.rust_oracle.evidence import EvidenceSanitizer,_batch_commit_marker; p=Path(r'$GbEvidence'); policy=EvidenceSanitizer.closed_policy(); v=policy.read_benchmark_manifest(p.name,p.read_bytes()); assert v.schema_version==2 and v.protocol_version==2 and v.pipeline=='gb' and v.verdict.value=='VALIDATED'; a=policy.build_benchmark_manifest(v); n,c=_batch_commit_marker((a,)); m=p.parent/n; assert m.read_bytes()==c.encode('utf-8'); print(m.as_posix())").Trim()
$SkMarker = (uv run python -c "from pathlib import Path; from tests.rust_oracle.evidence import EvidenceSanitizer,_batch_commit_marker; p=Path(r'$SkEvidence'); policy=EvidenceSanitizer.closed_policy(); v=policy.read_benchmark_manifest(p.name,p.read_bytes()); assert v.schema_version==2 and v.protocol_version==2 and v.pipeline=='sk' and v.verdict.value=='VALIDATED'; a=policy.build_benchmark_manifest(v); n,c=_batch_commit_marker((a,)); m=p.parent/n; assert m.read_bytes()==c.encode('utf-8'); print(m.as_posix())").Trim()
foreach ($TrackedPath in @($GbEvidence, $GbMarker, $SkEvidence, $SkMarker)) {
    git cat-file -e "HEAD:$TrackedPath"
    if ($LASTEXITCODE -ne 0) { throw "final evidence path is not tracked by HEAD: $TrackedPath" }
}
uv run python -m pytest tests/rust_oracle -q --basetemp .pytest-tmp/protocol-v2-final
uv run python -m ruff check tests/rust_oracle
uv run python -m ruff format tests/rust_oracle --check
uv run python -m tests.rust_oracle.evidence scan --root docs/performance
git status --short
```

Expected: all checks pass、GB/SK v2 manifests both read as `VALIDATED`、both fixed EXE SHA values and v1 terminal SHA remain unchanged、worktree clean。

When and only when both pipelines are `VALIDATED`, resume `docs/superpowers/plans/2026-07-11-rust-output-phase-0a-3-writer-optimization.md` at Task 3 (`Precompute the Standard Sheet and Column Write Plans`)。Do not rebuild Phase 0B；Phase 1 uses the frozen candidate SHA above as its same-batch reference。

## Pseudocode Draft

```python
# 目标：以不可选择的 protocol v2 完成 paired N=5/10，保留 v1 审计，并只发布可重建的 v2 evidence。
# 输入：固定 pipeline/input/reference/candidate、approved Phase 0A、当前 clean repository identity。
# 输出：VALIDATED / terminal fail-closed verdict；成功时发布 schema v2 evidence，失败时不发布。

def run_paired_v2(request):
    require_clean_repository()
    identity = capture_identity(request)
    require_fixed_manifest_and_binary_hashes(identity)
    batch_id = derive_batch_id(protocol_version=2, request=request, identity=identity)
    comparison_key = derive_comparison_key(protocol_version=2, request=request, identity=identity)
    ledger = create_v2_ledger(comparison_key, identity)

    first_wall, first_pws = capture_global_rounds(1, 5, ledger)
    first_sha = commit_first_group(first_wall, first_pws)
    wall, pws = first_wall, first_pws
    diagnostics = ()

    if any_closed_time_or_pws_metric_is_near_limit(first_wall, first_pws):
        second_wall, second_pws = capture_global_rounds(6, 10, ledger, first_sha)
        commit_expanded_group(second_wall, second_pws, first_sha)
        wall = structural_merge(first_wall, second_wall)
        pws = structural_merge(first_pws, second_pws)
        diagnostics = build_wall_then_pws_diagnostics(first_wall, second_wall, first_pws, second_pws)

    # 为什么先跑 closed gates：明确失败不能被另一指标的临界方向冲突改写。
    evaluate_all_closed_gates(wall, pws)
    if any_active_direct_metric_conflicts_near_limit(diagnostics):
        seal_terminal(INCONCLUSIVE)
        return INCONCLUSIVE

    cleanup_all_real_workbooks_and_raw_logs()
    evidence = build_schema_v2_evidence(protocol_version=2, diagnostics=diagnostics)
    publish_with_typed_sanitizer_and_marker_last(evidence)
    return VALIDATED
```

## Plan Self-Review Checklist

- [ ] Every v2 spec section 1–14 maps to at least one task or Global Constraint。
- [ ] Protocol version appears in batch ID、comparison key、ledger metadata/`BatchAttempt`、evidence and artifact basename。
- [ ] v1 metadata/evidence can be read/rebuilt but cannot be created、resumed or published by the formal runner。
- [ ] All five verdict-table branches and the mixed-failure priority have direct tests。
- [ ] Exact ratio=1、ratio/absolute direct gate、inactive PWS and composite/stage cases have tests。
- [ ] N=5 diagnostics empty；N=10 diagnostics exactly wall then PWS and are recomputed from rounds。
- [ ] No task adds round 11、third group、retry、protocol selector or arbitrary evidence filename。
- [ ] Evidence basename binds full comparison identity and cannot collide on changed input/reference。
- [ ] Formal preflight verifies manifest、two EXEs、input and v1 terminal before any process run。
- [ ] GB and SK each have one command, one terminal stop rule and no retry path。
- [ ] Code、docs、GB evidence and SK evidence are separate commits。
- [ ] No Rust/Cargo/production files or approved baseline are modified。
- [ ] Every implementation and test step contains an exact symbol、case、command and expected result；no deferred placeholder remains。
- [ ] Function names/types used by later tasks exactly match the Stable Interfaces section。

## Execution Handoff

Plan complete。Execution options：

1. **Subagent-Driven（推荐）** — 当前会话使用 `superpowers:subagent-driven-development`，每个 Task 使用 fresh implementer，并在 Task 1–4 后执行 spec + quality 两阶段 review；Task 6/7 的真实 benchmark 仍由主代理在明确 stop gate 下执行。
2. **Inline Execution** — 使用 `superpowers:executing-plans`，按 Task 1–5 分批实现并在 checkpoint 审查；Task 6/7 仍逐次执行，GB 非 `VALIDATED` 时立即终止。

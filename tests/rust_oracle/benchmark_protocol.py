from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import ROUND_CEILING, Decimal
from enum import StrEnum
from pathlib import Path
from statistics import median
from typing import Final, Literal, TypeAlias

PipelineName: TypeAlias = Literal['gb', 'sk']
BinaryRole: TypeAlias = Literal['reference', 'candidate']
MetricName: TypeAlias = Literal['wall', 'pws']
DirectGateKind: TypeAlias = Literal['none', 'ratio', 'absolute']
PAIRED_PROTOCOL_VERSION: Final = 2
_DIRECT_GATE_KEYS: Final[dict[MetricName, tuple[str, str]]] = {
    'wall': ('wall_ratio', 'wall_seconds'),
    'pws': ('pws_ratio', 'pws_bytes'),
}


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
    if any(
        type(value) is not str or len(value) != 64 or any(character not in '0123456789abcdef' for character in value)
        for value in hashes
    ):
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


@dataclass(frozen=True)
class ProfileRule:
    reference_schema: RuntimeSchema
    candidate_schema: RuntimeSchema
    same_sample_metrics: tuple[str, ...] = ()
    same_batch_metrics: tuple[str, ...] = ()
    requires_minimum_group_wins: bool = False
    tie_break_order: tuple[str, ...] = ()


def _rule(
    reference: RuntimeSchema,
    candidate: RuntimeSchema,
    *,
    same_sample: tuple[str, ...] = (),
    same_batch: tuple[str, ...] = (),
    requires_wins: bool = False,
    tie_break: tuple[str, ...] = (),
) -> ProfileRule:
    return ProfileRule(reference, candidate, same_sample, same_batch, requires_wins, tie_break)


PROFILE_RULES: Final[dict[ComparisonProfile, dict[PipelineName, ProfileRule]]] = {
    ComparisonProfile.PHASE0B_VS_PHASE0A: {
        pipeline: _rule(RuntimeSchema.BASE, RuntimeSchema.INSTRUMENTED, same_batch=('wall',))
        for pipeline in ('gb', 'sk')
    },
    ComparisonProfile.PHASE1_VS_PHASE0B: {
        'sk': _rule(
            RuntimeSchema.INSTRUMENTED,
            RuntimeSchema.INSTRUMENTED,
            same_sample=('writer_populate', 'xlsx_save'),
        )
    },
    ComparisonProfile.PHASE1_VS_PHASE0A: {
        'gb': _rule(RuntimeSchema.BASE, RuntimeSchema.INSTRUMENTED, same_batch=('wall', 'pws')),
        'sk': _rule(RuntimeSchema.BASE, RuntimeSchema.INSTRUMENTED, same_batch=('pws',)),
    },
    ComparisonProfile.PHASE2_B_VS_A: {
        'sk': _rule(RuntimeSchema.INSTRUMENTED, RuntimeSchema.INSTRUMENTED, same_sample=('xlsx_save',))
    },
    ComparisonProfile.PHASE2_C_VS_A: {
        'sk': _rule(
            RuntimeSchema.INSTRUMENTED,
            RuntimeSchema.INSTRUMENTED,
            same_sample=('writer_populate', 'export'),
            requires_wins=True,
        )
    },
    ComparisonProfile.PHASE2_D_VS_C: {
        'sk': _rule(RuntimeSchema.INSTRUMENTED, RuntimeSchema.INSTRUMENTED, same_sample=('xlsx_save',))
    },
    ComparisonProfile.PHASE2_D_VS_B: {
        'sk': _rule(
            RuntimeSchema.INSTRUMENTED,
            RuntimeSchema.INSTRUMENTED,
            same_sample=('writer_populate', 'export'),
            requires_wins=True,
        )
    },
    ComparisonProfile.PHASE2_B_VS_C: {
        'sk': _rule(
            RuntimeSchema.INSTRUMENTED,
            RuntimeSchema.INSTRUMENTED,
            same_batch=('wall', 'pws'),
            tie_break=('wall', 'pws', 'phase2-c'),
        )
    },
    ComparisonProfile.PHASE2_SELECTED_VS_PHASE0A: {
        'gb': _rule(RuntimeSchema.BASE, RuntimeSchema.INSTRUMENTED, same_batch=('wall', 'pws')),
        'sk': _rule(RuntimeSchema.BASE, RuntimeSchema.INSTRUMENTED),
    },
    ComparisonProfile.PHASE3_VS_PHASE0A: {
        'gb': _rule(RuntimeSchema.BASE, RuntimeSchema.INSTRUMENTED, same_batch=('wall', 'pws')),
        'sk': _rule(RuntimeSchema.BASE, RuntimeSchema.INSTRUMENTED, same_batch=('pws',)),
    },
    ComparisonProfile.PHASE3_ZLIB_ON_VS_OFF: {
        'sk': _rule(RuntimeSchema.INSTRUMENTED, RuntimeSchema.INSTRUMENTED, same_sample=('xlsx_save',))
    },
    ComparisonProfile.PHASE3_ZMIJ_ON_VS_OFF: {
        'sk': _rule(
            RuntimeSchema.INSTRUMENTED,
            RuntimeSchema.INSTRUMENTED,
            same_sample=('writer_populate', 'export'),
            requires_wins=True,
        )
    },
    ComparisonProfile.PHASE4_VS_PHASE3: {
        'gb': _rule(
            RuntimeSchema.INSTRUMENTED,
            RuntimeSchema.READER_INSTRUMENTED,
            same_batch=('ingest', 'pws'),
        ),
        'sk': _rule(
            RuntimeSchema.INSTRUMENTED,
            RuntimeSchema.READER_INSTRUMENTED,
            same_batch=('ingest', 'pws'),
        ),
    },
    ComparisonProfile.PHASE4_VS_PHASE0A: {
        'gb': _rule(RuntimeSchema.BASE, RuntimeSchema.READER_INSTRUMENTED, same_batch=('wall', 'pws')),
        'sk': _rule(RuntimeSchema.BASE, RuntimeSchema.READER_INSTRUMENTED),
    },
    ComparisonProfile.PHASE5_VS_PHASE0A: {
        'gb': _rule(RuntimeSchema.BASE, RuntimeSchema.READER_INSTRUMENTED, same_batch=('wall', 'pws')),
        'sk': _rule(RuntimeSchema.BASE, RuntimeSchema.READER_INSTRUMENTED, same_batch=('wall', 'pws')),
    },
}


def _limits(**values: str | int) -> dict[str, Decimal | int]:
    return {key: Decimal(value) if isinstance(value, str) else value for key, value in values.items()}


COMPARISON_LIMITS: Final[dict[ComparisonProfile, dict[PipelineName, dict[str, Decimal | int]]]] = {
    ComparisonProfile.PHASE0B_VS_PHASE0A: {
        pipeline: _limits(wall_ratio='1.02', output_bytes_ratio='1.10') for pipeline in ('gb', 'sk')
    },
    ComparisonProfile.PHASE1_VS_PHASE0B: {
        'sk': _limits(writer_populate_ratio='0.90', xlsx_save_ratio='1.05', output_bytes_ratio='1.10')
    },
    ComparisonProfile.PHASE1_VS_PHASE0A: {
        'gb': _limits(wall_ratio='1.05', pws_ratio='1.05', output_bytes_ratio='1.10'),
        'sk': _limits(pws_ratio='1.05', output_bytes_ratio='1.10'),
    },
    ComparisonProfile.PHASE2_B_VS_A: {'sk': _limits(xlsx_save_ratio='0.85', output_bytes_ratio='1.10')},
    ComparisonProfile.PHASE2_C_VS_A: {
        'sk': _limits(writer_populate_or_export_ratio='0.97', minimum_wins=4, output_bytes_ratio='1.10')
    },
    ComparisonProfile.PHASE2_D_VS_C: {'sk': _limits(xlsx_save_ratio='0.85', output_bytes_ratio='1.10')},
    ComparisonProfile.PHASE2_D_VS_B: {
        'sk': _limits(writer_populate_or_export_ratio='0.97', minimum_wins=4, output_bytes_ratio='1.10')
    },
    ComparisonProfile.PHASE2_B_VS_C: {'sk': _limits(output_bytes_ratio='1.10')},
    ComparisonProfile.PHASE2_SELECTED_VS_PHASE0A: {
        'gb': _limits(wall_ratio='1.05', pws_ratio='1.05', output_bytes_ratio='1.10'),
        'sk': _limits(output_bytes_ratio='1.10'),
    },
    ComparisonProfile.PHASE3_VS_PHASE0A: {
        'gb': _limits(wall_ratio='1.05', pws_ratio='1.05', output_bytes_ratio='1.10'),
        'sk': _limits(pws_bytes=2_147_483_648, output_bytes_ratio='1.10'),
    },
    ComparisonProfile.PHASE3_ZLIB_ON_VS_OFF: {'sk': _limits(xlsx_save_ratio='0.85', output_bytes_ratio='1.10')},
    ComparisonProfile.PHASE3_ZMIJ_ON_VS_OFF: {
        'sk': _limits(writer_populate_or_export_ratio='0.97', minimum_wins=4, output_bytes_ratio='1.10')
    },
    ComparisonProfile.PHASE4_VS_PHASE3: {
        'gb': _limits(ingest_ratio='1.05', pws_ratio='1.05', output_bytes_ratio='1.10'),
        'sk': _limits(
            ingest_or_pws_ratio='0.90',
            wall_ratio='1.00',
            output_bytes_ratio='1.10',
        ),
    },
    ComparisonProfile.PHASE4_VS_PHASE0A: {
        'gb': _limits(wall_ratio='1.05', pws_ratio='1.05', output_bytes_ratio='1.10'),
        'sk': _limits(output_bytes_ratio='1.10'),
    },
    ComparisonProfile.PHASE5_VS_PHASE0A: {
        'gb': _limits(wall_ratio='1.05', pws_ratio='1.05', output_bytes_ratio='1.10'),
        'sk': _limits(wall_seconds='20.0', pws_bytes=2_147_483_648, output_bytes_ratio='1.10'),
    },
}

MANDATORY_EXPANSION_BOUNDARY: Final = Decimal('0.03')
ENVIRONMENT_MEDIAN_DRIFT_LIMIT: Final = Decimal('0.10')


def _validate_closed_profile_tables() -> Decimal:
    expected_profiles = set(ComparisonProfile)
    if set(PROFILE_RULES) != expected_profiles or set(COMPARISON_LIMITS) != expected_profiles:
        raise RuntimeError('profile rules and comparison limits must cover every closed profile')
    output_limits: set[Decimal] = set()
    for profile in expected_profiles:
        if set(PROFILE_RULES[profile]) != set(COMPARISON_LIMITS[profile]):
            raise RuntimeError(f'profile pipeline coverage differs for {profile.value}')
        for limits in COMPARISON_LIMITS[profile].values():
            resolve_direct_metric_gate('wall', limits)
            resolve_direct_metric_gate('pws', limits)
            value = limits.get('output_bytes_ratio')
            if not isinstance(value, Decimal):
                raise RuntimeError(f'profile lacks a Decimal output-byte gate: {profile.value}')
            output_limits.add(value)
    if len(output_limits) != 1:
        raise RuntimeError('all profiles must use one approved Phase 0A output-byte gate')
    return output_limits.pop()


def build_round_plan(*, global_round_start: Literal[1, 6], round_count: Literal[5]) -> tuple[RoundPlan, ...]:
    if global_round_start not in (1, 6) or round_count != 5:
        raise ValueError('round plans are closed to five rounds starting at global round 1 or 6')
    return tuple(
        RoundPlan(
            global_round=global_round,
            order=('reference', 'candidate') if global_round % 2 else ('candidate', 'reference'),
        )
        for global_round in range(global_round_start, global_round_start + round_count)
    )


def _require_positive_finite(value: Decimal, field: str) -> None:
    if not value.is_finite() or value <= 0:
        raise ValueError(f'{field} must be finite and positive')


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


OUTPUT_BYTES_RATIO_LIMIT: Final = _validate_closed_profile_tables()


def _validate_sample(sample: MetricSample, *, role: BinaryRole, global_round: int, pipeline: PipelineName) -> None:
    if sample.role != role or sample.global_round != global_round:
        raise ValueError('sample role or global rounds do not match the paired plan')
    _require_positive_finite(sample.metric_value, 'metric sample')
    _require_positive_finite(sample.normal_run.external_wall_seconds, 'external wall sample')
    if sample.exit_code != 0:
        raise ValueError('sample exit code must be zero')
    if sample.normal_run.runtime.pipeline != pipeline:
        raise ValueError('sample pipeline drift detected')
    if not sample.normal_run.workbook_oracle_sha256:
        raise ValueError('workbook oracle evidence is required')
    for field in (
        sample.input_sha256,
        sample.binary_sha256,
        sample.git_head,
        sample.repository_state_sha256,
        sample.machine_fingerprint_sha256,
        sample.local_unversioned_log_sha256,
    ):
        if not field:
            raise ValueError('sample hashes and identity fields must be non-empty')


def validate_metric_group(group: MetricGroup) -> None:
    if not group.batch_id:
        raise ValueError('batch ID must be non-empty')
    if group.pipeline not in ('gb', 'sk') or group.metric not in ('wall', 'pws'):
        raise ValueError('group uses an unsupported pipeline or metric')
    if len(group.rounds) not in (5, 10):
        raise ValueError('metric group rounds must contain exactly five or ten samples')
    if (len(group.rounds), group.global_round_start) not in ((5, 1), (5, 6), (10, 1)):
        raise ValueError('metric group uses an invalid round window')

    # 先校验每一轮的 AB/BA，避免重复轮次掩盖顺序失衡。
    for paired in group.rounds:
        expected_order = ('reference', 'candidate') if paired.plan.global_round % 2 else ('candidate', 'reference')
        if paired.plan.order != expected_order:
            raise ValueError('global round order is not balanced AB/BA')

    expected_rounds = tuple(range(group.global_round_start, group.global_round_start + len(group.rounds)))
    actual_rounds = tuple(paired.plan.global_round for paired in group.rounds)
    if actual_rounds != expected_rounds:
        raise ValueError('metric group rounds are missing, duplicated, or non-contiguous')

    samples: list[MetricSample] = []
    for paired in group.rounds:
        _validate_sample(
            paired.reference,
            role='reference',
            global_round=paired.plan.global_round,
            pipeline=group.pipeline,
        )
        _validate_sample(
            paired.candidate,
            role='candidate',
            global_round=paired.plan.global_round,
            pipeline=group.pipeline,
        )
        samples.extend((paired.reference, paired.candidate))

    if len({sample.binary_sha256 for sample in samples if sample.role == 'reference'}) != 1:
        raise ValueError('reference binary SHA changed within the group')
    if len({sample.binary_sha256 for sample in samples if sample.role == 'candidate'}) != 1:
        raise ValueError('candidate binary SHA changed within the group')
    drift_fields = (
        'input_sha256',
        'git_head',
        'repository_state_sha256',
        'machine_fingerprint_sha256',
    )
    if any(len({getattr(sample, field) for sample in samples}) != 1 for field in drift_fields):
        raise ValueError('input, Git, repository state, or machine drift detected')


def validate_calibration_group(group: CalibrationGroup) -> None:
    if not group.batch_id or not group.warmup_succeeded:
        raise ValueError('calibration requires a batch ID and successful warmup')
    if group.pipeline not in ('gb', 'sk') or group.metric not in ('wall', 'pws'):
        raise ValueError('calibration uses an unsupported pipeline or metric')
    if len(group.rounds) != 5:
        raise ValueError('calibration requires exactly five reference-only rounds')
    if tuple(item.global_round for item in group.rounds) != (1, 2, 3, 4, 5):
        raise ValueError('calibration rounds must be global rounds one through five')
    for item in group.rounds:
        if item.reference.role != 'reference':
            raise ValueError('calibration accepts reference samples only')
        _validate_sample(item.reference, role='reference', global_round=item.global_round, pipeline=group.pipeline)
    samples = tuple(item.reference for item in group.rounds)
    if len({sample.binary_sha256 for sample in samples}) != 1:
        raise ValueError('calibration reference binary SHA changed')
    for field in ('input_sha256', 'git_head', 'repository_state_sha256', 'machine_fingerprint_sha256'):
        if len({getattr(sample, field) for sample in samples}) != 1:
            raise ValueError('calibration input or environment drift detected')


def assert_same_batch_ratio(group: MetricGroup) -> None:
    validate_metric_group(group)


def _group_hashes(group: MetricGroup) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (
            paired.reference.input_sha256,
            paired.reference.binary_sha256,
            paired.candidate.binary_sha256,
        )
        for paired in group.rounds
    )


def assert_same_benchmark_batch(wall: MetricGroup, pws: MetricGroup) -> None:
    if wall.metric != 'wall' or pws.metric != 'pws':
        raise ValueError('same-batch comparison requires wall and PWS groups')
    if wall.batch_id != pws.batch_id or not wall.batch_id:
        raise ValueError('wall and PWS must share one batch ID')
    if wall.pipeline != pws.pipeline:
        raise ValueError('wall and PWS must share one pipeline')
    wall_rounds = tuple(item.plan.global_round for item in wall.rounds)
    pws_rounds = tuple(item.plan.global_round for item in pws.rounds)
    if len(wall.rounds) != len(pws.rounds) or wall_rounds != pws_rounds:
        raise ValueError('wall and PWS must share N and the same global rounds')
    if _group_hashes(wall) != _group_hashes(pws):
        raise ValueError('wall and PWS must share input and binary hashes')
    wall_git_heads = tuple((item.reference.git_head, item.candidate.git_head) for item in wall.rounds)
    pws_git_heads = tuple((item.reference.git_head, item.candidate.git_head) for item in pws.rounds)
    if wall_git_heads != pws_git_heads:
        raise ValueError('wall and PWS must share Git HEAD')
    wall_repository_states = tuple(
        (item.reference.repository_state_sha256, item.candidate.repository_state_sha256) for item in wall.rounds
    )
    pws_repository_states = tuple(
        (item.reference.repository_state_sha256, item.candidate.repository_state_sha256) for item in pws.rounds
    )
    if wall_repository_states != pws_repository_states:
        raise ValueError('wall and PWS must share repository state')
    wall_machines = tuple(item.reference.machine_fingerprint_sha256 for item in wall.rounds)
    pws_machines = tuple(item.reference.machine_fingerprint_sha256 for item in pws.rounds)
    if wall_machines != pws_machines:
        raise ValueError('wall and PWS must share the machine fingerprint')
    assert_same_batch_ratio(wall)
    assert_same_batch_ratio(pws)


def requires_mandatory_expansion(*, measured: Decimal, limit: Decimal) -> bool:
    measured_decimal = Decimal(str(measured))
    limit_decimal = Decimal(str(limit))
    _require_positive_finite(measured_decimal, 'measured ratio')
    _require_positive_finite(limit_decimal, 'comparison limit')
    return abs(measured_decimal / limit_decimal - Decimal(1)) <= MANDATORY_EXPANSION_BOUNDARY


def _median_ratio(group: MetricGroup) -> Decimal:
    validate_metric_group(group)
    reference_median = median(paired.reference.metric_value for paired in group.rounds)
    candidate_median = median(paired.candidate.metric_value for paired in group.rounds)
    _require_positive_finite(reference_median, 'reference median')
    _require_positive_finite(candidate_median, 'candidate median')
    return candidate_median / reference_median


def _assert_groups_join(first: MetricGroup, second: MetricGroup) -> None:
    if len(first.rounds) != 5 or len(second.rounds) != 5:
        raise ValueError('only two five-round metric groups may be joined')
    if first.batch_id != second.batch_id or first.pipeline != second.pipeline or first.metric != second.metric:
        raise ValueError('metric groups must share batch, pipeline, and metric')
    if first.global_round_start != 1 or second.global_round_start != 6:
        raise ValueError('metric groups must cover global rounds one and six')
    validate_metric_group(first)
    validate_metric_group(second)
    first_samples = first.rounds[0]
    second_samples = second.rounds[0]
    for left, right in (
        (first_samples.reference, second_samples.reference),
        (first_samples.candidate, second_samples.candidate),
    ):
        identity = (
            'input_sha256',
            'binary_sha256',
            'git_head',
            'repository_state_sha256',
            'machine_fingerprint_sha256',
        )
        if any(getattr(left, field) != getattr(right, field) for field in identity):
            raise ValueError('metric groups do not share immutable evidence identity')


def groups_have_conflicting_direction(first: MetricGroup, second: MetricGroup) -> bool:
    _assert_groups_join(first, second)
    return _ratios_have_conflicting_direction(_median_ratio(first), _median_ratio(second))


def _ratios_have_conflicting_direction(first_ratio: Decimal, second_ratio: Decimal) -> bool:
    return (first_ratio - Decimal(1)) * (second_ratio - Decimal(1)) < 0


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
    directions_conflict = _ratios_have_conflicting_direction(first_ratio, second_ratio)
    direct_gate, direct_limit = resolve_direct_metric_gate(first.metric, limits)
    if direct_gate == 'none':
        normalized = None
        near_boundary = None
    else:
        assert direct_limit is not None
        combined_value = (
            combined_ratio if direct_gate == 'ratio' else median(item.candidate.metric_value for item in merged.rounds)
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


def _phase0a_group(manifest: Phase0AManifest, pipeline: PipelineName, metric: MetricName) -> CalibrationGroup:
    group = getattr(manifest, f'{pipeline}_{metric}')
    if group.pipeline != pipeline or group.metric != metric:
        raise ValueError(f'Phase 0A {pipeline} {metric} group has a mismatched pipeline or metric label')
    return group


def aggregate_output_bytes(values: Iterable[object]) -> int:
    sizes: list[int] = []
    for value in values:
        if type(value) is not int:
            raise ValueError('output bytes must be positive integers')
        size = int(value)
        if size <= 0:
            raise ValueError('output bytes must be positive integers')
        sizes.append(size)
    if not sizes:
        raise ValueError('output bytes must be nonempty')
    decimal_median = median(Decimal(value) for value in sizes)
    # 偶数样本的中位数可能落在半字节；向上取整可避免低估后续容量门禁。
    return int(decimal_median.to_integral_value(rounding=ROUND_CEILING))


def approved_phase0a_output_bytes(manifest: Phase0AManifest, pipeline: PipelineName) -> int:
    values: list[int | None] = []
    for metric in ('wall', 'pws'):
        group = _phase0a_group(manifest, pipeline, metric)
        validate_calibration_group(group)
        values.extend(item.reference.normal_run.runtime.output_size_bytes for item in group.rounds)
    try:
        return aggregate_output_bytes(values)
    except ValueError as exc:
        raise ValueError('Phase 0A output bytes must be present and positive in wall/PWS reference samples') from exc


def assert_output_bytes_within_phase0a_limit(
    *, candidate_bytes: int, manifest: Phase0AManifest, pipeline: PipelineName
) -> None:
    try:
        candidate = aggregate_output_bytes((candidate_bytes,))
    except ValueError as exc:
        raise ValueError('candidate output bytes must be a positive integer') from exc
    approved = approved_phase0a_output_bytes(manifest, pipeline)
    if Decimal(candidate) > Decimal(approved) * OUTPUT_BYTES_RATIO_LIMIT:
        raise ValueError('candidate output bytes exceed 110% of approved Phase 0A bytes')


def assert_environment_not_drifted(current: MetricGroup, phase0a: Phase0AManifest) -> None:
    validate_metric_group(current)
    calibration = _phase0a_group(phase0a, current.pipeline, current.metric)
    validate_calibration_group(calibration)
    current_fingerprints = {item.reference.machine_fingerprint_sha256 for item in current.rounds}
    if current_fingerprints != {phase0a.machine.fingerprint_sha256}:
        raise ValueError('machine fingerprint changed from Phase 0A')
    current_median = median(item.reference.metric_value for item in current.rounds)
    phase0a_median = median(item.reference.metric_value for item in calibration.rounds)
    _require_positive_finite(current_median, 'current reference median')
    _require_positive_finite(phase0a_median, 'Phase 0A reference median')
    if abs(current_median / phase0a_median - Decimal(1)) > ENVIRONMENT_MEDIAN_DRIFT_LIMIT:
        raise ValueError('current reference median drift exceeds ten percent')

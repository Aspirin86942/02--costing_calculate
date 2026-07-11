from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import subprocess
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

from tests.rust_oracle.benchmark_protocol import (
    PROFILE_RULES,
    BinaryRole,
    ClosedBinaryLabel,
    ComparisonProfile,
    HarnessVerdict,
    MetricGroup,
    MetricName,
    MetricSample,
    NormalRunEvidence,
    PairedBenchmarkResult,
    PairedRound,
    PipelineName,
    RoundPlan,
    RuntimeEvidence,
    RuntimeSchema,
    validate_metric_group,
)
from tests.rust_oracle.oracle_runner import RustNormalProcessError, run_rust_normal_captured
from tests.rust_oracle.repo_paths import repo_root


class HarnessFailure(AssertionError):
    def __init__(
        self,
        verdict: HarnessVerdict,
        message: str,
        *,
        primary_verdict: HarnessVerdict | None = None,
    ) -> None:
        super().__init__(message)
        self.verdict = verdict
        self.primary_verdict = primary_verdict


@dataclass(frozen=True)
class BenchmarkIdentity:
    input_sha256: str
    reference_sha256: str
    candidate_sha256: str
    git_head: str
    repository_state_sha256: str
    machine_fingerprint_sha256: str


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
    first_group_sha256: str | None = None

    def __post_init__(self) -> None:
        if self.metric not in ('wall', 'pws'):
            raise ValueError('metric must be wall or pws')
        rounds = tuple(plan.global_round for plan in self.plans)
        if rounds == (1, 2, 3, 4, 5):
            if self.first_group_sha256 is not None:
                raise ValueError('rounds one through five cannot be submitted as an expanded group')
        elif rounds == (6, 7, 8, 9, 10):
            if not _is_sha256(self.first_group_sha256):
                raise ValueError('expanded rounds require the original first-group SHA-256')
        else:
            raise ValueError('metric group plans must be global rounds one through five or six through ten')


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


@dataclass
class AppendOnlyAttemptLedger:
    attempt_directory: Path
    identity: BenchmarkIdentity
    comparison_key: str
    attempt_number: int
    previous_attempt_head_sha256: str | None
    head_sha256: str
    _sample_keys: set[tuple[str, int, str]] = field(default_factory=set)
    _sample_payloads: dict[tuple[str, int, str], dict[str, Any]] = field(default_factory=dict)
    _plan_payloads: dict[tuple[str, int, str], dict[str, Any]] = field(default_factory=dict)
    first_group_sha256: str | None = None
    expanded_group_sha256: str | None = None

    @classmethod
    def create(
        cls,
        root: Path,
        identity: BenchmarkIdentity,
        *,
        comparison_key: str,
    ) -> AppendOnlyAttemptLedger:
        comparison_directory = root.resolve() / comparison_key
        comparison_directory.mkdir(parents=True, exist_ok=True)
        attempts = sorted(path for path in comparison_directory.glob('attempt-*') if path.is_dir())
        previous_head: str | None = None
        if attempts:
            previous = cls._load(attempts[-1], identity, comparison_key, strict_identity=False)
            verdict = previous._last_verdict()
            if verdict is None:
                return previous
            if verdict not in (HarnessVerdict.ENVIRONMENT_DRIFT, HarnessVerdict.REFERENCE_FAILED):
                raise HarnessFailure(
                    HarnessVerdict.INCOMPLETE_EVIDENCE,
                    'failed candidate SHA cannot be retried after candidate, correctness, '
                    'gate, or inconclusive failure',
                )
            previous_head = previous.head_sha256

        number = len(attempts) + 1
        attempt_directory = comparison_directory / f'attempt-{number:04d}'
        attempt_directory.mkdir()
        (attempt_directory / 'records').mkdir()
        metadata = {
            'comparison_key': comparison_key,
            'attempt_number': number,
            'candidate_sha256': identity.candidate_sha256,
            'identity': asdict(identity),
            'previous_attempt_head_sha256': previous_head,
            'reason': 'ENVIRONMENT_RECOVERED' if previous_head else 'FORMAL_START',
        }
        metadata_bytes = _canonical_json(metadata)
        _write_create_new(attempt_directory / 'metadata.json', metadata_bytes)
        return cls(
            attempt_directory,
            identity,
            comparison_key,
            number,
            previous_head,
            hashlib.sha256(metadata_bytes).hexdigest(),
        )

    @classmethod
    def _load(
        cls,
        directory: Path,
        identity: BenchmarkIdentity,
        comparison_key: str,
        *,
        strict_identity: bool = True,
    ) -> AppendOnlyAttemptLedger:
        metadata = json.loads((directory / 'metadata.json').read_text(encoding='utf-8'))
        if metadata.get('candidate_sha256') != identity.candidate_sha256:
            raise HarnessFailure(HarnessVerdict.ENVIRONMENT_DRIFT, 'candidate SHA differs from attempt metadata')
        if strict_identity and metadata.get('identity') != asdict(identity):
            raise HarnessFailure(HarnessVerdict.ENVIRONMENT_DRIFT, 'attempt identity changed during resume')
        head = hashlib.sha256(_canonical_json(metadata)).hexdigest()
        sample_keys: set[tuple[str, int, str]] = set()
        sample_payloads: dict[tuple[str, int, str], dict[str, Any]] = {}
        plan_payloads: dict[tuple[str, int, str], dict[str, Any]] = {}
        first_group_sha256: str | None = None
        expanded_group_sha256: str | None = None
        for record_path in sorted((directory / 'records').glob('*.json')):
            raw = record_path.read_bytes()
            record = json.loads(raw)
            if record.get('previous_record_sha256') != head:
                raise HarnessFailure(HarnessVerdict.INCOMPLETE_EVIDENCE, 'attempt ledger hash chain is broken')
            head = hashlib.sha256(raw).hexdigest()
            if record.get('kind') == 'sample':
                key = (record['metric'], record['global_round'], record['role'])
                sample_keys.add(key)
                sample_payloads[key] = record['payload']
            elif record.get('kind') == 'planned-output':
                plan_payloads[(record['metric'], record['global_round'], record['role'])] = record['payload']
            elif record.get('kind') == 'first-group':
                if first_group_sha256 is not None:
                    raise HarnessFailure(HarnessVerdict.INCOMPLETE_EVIDENCE, 'first group record is duplicated')
                first_group_sha256 = head
            elif record.get('kind') == 'expanded-group':
                if expanded_group_sha256 is not None:
                    raise HarnessFailure(HarnessVerdict.INCOMPLETE_EVIDENCE, 'expanded group record is duplicated')
                expanded_group_sha256 = head
        return cls(
            directory,
            identity,
            comparison_key,
            int(metadata['attempt_number']),
            metadata.get('previous_attempt_head_sha256'),
            head,
            sample_keys,
            sample_payloads,
            plan_payloads,
            first_group_sha256,
            expanded_group_sha256,
        )

    def _append(self, kind: str, payload: dict[str, Any]) -> str:
        records = self.attempt_directory / 'records'
        sequence = len(tuple(records.glob('*.json'))) + 1
        record = {'kind': kind, 'previous_record_sha256': self.head_sha256, **payload}
        raw = _canonical_json(record)
        _write_create_new(records / f'{sequence:04d}-{kind}.json', raw)
        self.head_sha256 = hashlib.sha256(raw).hexdigest()
        return self.head_sha256

    def record_sample(
        self,
        metric: MetricName,
        global_round: int,
        role: BinaryRole,
        payload: dict[str, Any],
    ) -> str:
        key = (metric, global_round, role)
        if key in self._sample_keys:
            raise HarnessFailure(HarnessVerdict.INCOMPLETE_EVIDENCE, 'existing round record overwrite is forbidden')
        digest = self._append(
            'sample',
            {'metric': metric, 'global_round': global_round, 'role': role, 'payload': payload},
        )
        self._sample_keys.add(key)
        self._sample_payloads[key] = payload
        return digest

    def record_planned_output(
        self,
        metric: MetricName,
        global_round: int,
        role: BinaryRole,
        payload: dict[str, Any],
    ) -> str:
        key = (metric, global_round, role)
        existing = self._plan_payloads.get(key)
        if existing is not None:
            if existing != payload:
                raise HarnessFailure(HarnessVerdict.INCOMPLETE_EVIDENCE, 'planned output changed during resume')
            return self.head_sha256
        digest = self._append(
            'planned-output',
            {'metric': metric, 'global_round': global_round, 'role': role, 'payload': payload},
        )
        self._plan_payloads[key] = payload
        return digest

    def sample_payload(
        self,
        metric: MetricName,
        global_round: int,
        role: BinaryRole,
    ) -> dict[str, Any] | None:
        return self._sample_payloads.get((metric, global_round, role))

    def missing_samples(
        self,
        metrics: tuple[MetricName, ...],
        rounds: tuple[int, ...],
        roles: tuple[BinaryRole, ...],
    ) -> tuple[tuple[MetricName, int, BinaryRole], ...]:
        return tuple(
            (metric, round_number, role)
            for metric in metrics
            for round_number in rounds
            for role in roles
            if (metric, round_number, role) not in self._sample_keys
        )

    def commit_first_group(self, groups: dict[str, Any]) -> str:
        if set(groups) != {'wall', 'pws'}:
            raise HarnessFailure(HarnessVerdict.INCOMPLETE_EVIDENCE, 'first group must commit wall and pws together')
        if self.first_group_sha256 is not None:
            raise HarnessFailure(HarnessVerdict.INCOMPLETE_EVIDENCE, 'first group record overwrite is forbidden')
        digest = self._append('first-group', {'groups': groups})
        self.first_group_sha256 = digest
        return digest

    def commit_expanded_group(self, groups: dict[str, Any], *, first_group_sha256: str) -> str:
        if first_group_sha256 != self.first_group_sha256:
            raise HarnessFailure(
                HarnessVerdict.INCOMPLETE_EVIDENCE, 'expanded group does not link original first group SHA'
            )
        if set(groups) != {'wall', 'pws'}:
            raise HarnessFailure(HarnessVerdict.INCOMPLETE_EVIDENCE, 'expanded group must commit wall and pws together')
        if self.expanded_group_sha256 is not None:
            raise HarnessFailure(HarnessVerdict.INCOMPLETE_EVIDENCE, 'expanded group record overwrite is forbidden')
        digest = self._append('expanded-group', {'first_group_sha256': first_group_sha256, 'groups': groups})
        self.expanded_group_sha256 = digest
        return digest

    def finish(self, verdict: HarnessVerdict) -> str:
        return self._append('verdict', {'verdict': verdict.value})

    def _last_verdict(self) -> HarnessVerdict | None:
        paths = sorted((self.attempt_directory / 'records').glob('*-verdict.json'))
        if not paths:
            return None
        return HarnessVerdict(json.loads(paths[-1].read_text(encoding='utf-8'))['verdict'])


def derive_batch_id(request: PairedBenchmarkRequest, identity: BenchmarkIdentity) -> str:
    payload = {
        'profile': request.comparison_profile.value,
        'pipeline': request.pipeline,
        **asdict(identity),
    }
    return hashlib.sha256(_canonical_json(payload)).hexdigest()[:32]


def run_normal_wall_group(request: MetricGroupRequest) -> MetricGroup:
    if request.metric != 'wall':
        raise ValueError('normal wall runner accepts wall metric only')
    identity = _capture_identity(request.benchmark)
    metadata = json.loads((request.attempt_directory / 'metadata.json').read_text(encoding='utf-8'))
    ledger = AppendOnlyAttemptLedger._load(
        request.attempt_directory,
        identity,
        metadata['comparison_key'],
    )
    if request.first_group_sha256 is not None and request.first_group_sha256 != ledger.first_group_sha256:
        raise HarnessFailure(HarnessVerdict.INCOMPLETE_EVIDENCE, 'expanded group first-group SHA does not match ledger')
    role_executables = {
        'reference': request.benchmark.reference_executable,
        'candidate': request.benchmark.candidate_executable,
    }
    rule = PROFILE_RULES[request.benchmark.comparison_profile][request.benchmark.pipeline]
    schemas: dict[BinaryRole, RuntimeSchema] = {
        'reference': rule.reference_schema,
        'candidate': rule.candidate_schema,
    }
    cleanup_paths: list[Path] = []
    pairs: list[PairedRound] = []
    primary_error: HarnessFailure | None = None
    try:
        for plan in request.plans:
            captured: dict[BinaryRole, tuple[NormalRunEvidence, str]] = {}
            for role in plan.order:
                _assert_identity_unchanged(identity, _capture_identity(request.benchmark))
                existing = ledger.sample_payload(request.metric, plan.global_round, role)
                if existing is not None:
                    sample = _sample_from_payload(existing)
                    captured[role] = (sample.normal_run, sample.local_unversioned_log_sha256)
                    continue
                output = _planned_output(request, identity, plan.global_round, role)
                cleanup_paths.append(output)
                ledger.record_planned_output(
                    request.metric,
                    plan.global_round,
                    role,
                    {'path_sha256': hashlib.sha256(str(output).encode()).hexdigest()},
                )
                try:
                    result = run_rust_normal_captured(
                        role_executables[role],
                        request.benchmark.pipeline,
                        request.benchmark.input_path,
                        output,
                        schema=schemas[role],
                        local_log_root=request.benchmark.local_root / 'raw-logs',
                    )
                except RustNormalProcessError as exc:
                    verdict = (
                        HarnessVerdict.REFERENCE_FAILED if role == 'reference' else HarnessVerdict.CANDIDATE_FAILED
                    )
                    raise HarnessFailure(verdict, f'{role} process failed with exit code {exc.returncode}') from exc
                except AssertionError as exc:
                    verdict = (
                        HarnessVerdict.REFERENCE_FAILED if role == 'reference' else HarnessVerdict.CORRECTNESS_FAILED
                    )
                    raise HarnessFailure(verdict, f'{role} runtime or workbook validation failed') from exc
                finally:
                    _remove_workbook(output)
                _assert_identity_unchanged(identity, _capture_identity(request.benchmark))
                captured[role] = (result.normal_run, result.local_unversioned_log_sha256)
                sample = _metric_sample(role, plan, identity, captured[role])
                ledger.record_sample(request.metric, plan.global_round, role, _sample_to_payload(sample))
            if captured['reference'][0].workbook_oracle_sha256 != captured['candidate'][0].workbook_oracle_sha256:
                raise HarnessFailure(HarnessVerdict.CORRECTNESS_FAILED, 'reference/candidate workbook oracle mismatch')
            pairs.append(
                PairedRound(
                    plan,
                    _metric_sample('reference', plan, identity, captured['reference']),
                    _metric_sample('candidate', plan, identity, captured['candidate']),
                )
            )
        group = MetricGroup(
            request.batch_id,
            request.benchmark.pipeline,
            'wall',
            request.plans[0].global_round,  # type: ignore[arg-type]
            tuple(pairs),
        )
        validate_metric_group(group)
        return group
    except HarnessFailure as exc:
        primary_error = exc
        raise
    finally:
        cleanup_errors = _cleanup_all(cleanup_paths)
        if cleanup_errors:
            raise HarnessFailure(
                HarnessVerdict.CLEANUP_FAILED,
                f'workbook cleanup failed: {cleanup_errors!r}',
                primary_verdict=primary_error.verdict if primary_error else None,
            )


def _metric_sample(
    role: BinaryRole,
    plan: RoundPlan,
    identity: BenchmarkIdentity,
    captured: tuple[NormalRunEvidence, str],
) -> MetricSample:
    normal_run, log_sha = captured
    return MetricSample(
        role=role,
        global_round=plan.global_round,
        metric_value=normal_run.external_wall_seconds,
        exit_code=0,
        input_sha256=identity.input_sha256,
        binary_sha256=identity.reference_sha256 if role == 'reference' else identity.candidate_sha256,
        git_head=identity.git_head,
        repository_state_sha256=identity.repository_state_sha256,
        machine_fingerprint_sha256=identity.machine_fingerprint_sha256,
        local_unversioned_log_sha256=log_sha,
        normal_run=normal_run,
    )


def _planned_output(
    request: MetricGroupRequest,
    identity: BenchmarkIdentity,
    global_round: int,
    role: BinaryRole,
) -> Path:
    binary_sha = identity.reference_sha256 if role == 'reference' else identity.candidate_sha256
    root = (repo_root() / 'data' / 'processed' / request.benchmark.pipeline / '.perf-runs').resolve()
    path = root / request.batch_id / request.metric / binary_sha / str(global_round) / f'{role}.xlsx'
    _require_canonical_child(path, root)
    if path.exists():
        raise HarnessFailure(HarnessVerdict.INCOMPLETE_EVIDENCE, f'planned output already exists: {path}')
    path.parent.mkdir(parents=True, exist_ok=True)
    _require_no_reparse_points(path.parent, root)
    return path


def _sample_to_payload(sample: MetricSample) -> dict[str, Any]:
    runtime = sample.normal_run.runtime
    return {
        'role': sample.role,
        'global_round': sample.global_round,
        'metric_value': str(sample.metric_value),
        'exit_code': sample.exit_code,
        'input_sha256': sample.input_sha256,
        'binary_sha256': sample.binary_sha256,
        'git_head': sample.git_head,
        'repository_state_sha256': sample.repository_state_sha256,
        'machine_fingerprint_sha256': sample.machine_fingerprint_sha256,
        'local_unversioned_log_sha256': sample.local_unversioned_log_sha256,
        'normal_run': {
            'external_wall_seconds': str(sample.normal_run.external_wall_seconds),
            'peak_working_set_bytes': sample.normal_run.peak_working_set_bytes,
            'workbook_oracle_sha256': sample.normal_run.workbook_oracle_sha256,
            'runtime': {
                **asdict(runtime),
                'stage_timings': [[name, str(value)] for name, value in runtime.stage_timings],
            },
        },
    }


def _sample_from_payload(payload: dict[str, Any]) -> MetricSample:
    normal_payload = payload['normal_run']
    runtime_payload = normal_payload['runtime']
    runtime = RuntimeEvidence(
        pipeline=runtime_payload['pipeline'],
        output_written=runtime_payload['output_written'],
        request_id_present=runtime_payload['request_id_present'],
        sheet_count=runtime_payload['sheet_count'],
        error_log_count=runtime_payload['error_log_count'],
        issue_type_counts=tuple(tuple(item) for item in runtime_payload['issue_type_counts']),
        quality_metrics=tuple(tuple(item) for item in runtime_payload['quality_metrics']),
        run_counts=tuple(tuple(item) for item in runtime_payload['run_counts']),
        stage_timings=tuple((item[0], Decimal(item[1])) for item in runtime_payload['stage_timings']),
        output_size_bytes=runtime_payload['output_size_bytes'],
        sheet_dimensions=tuple(runtime_payload['sheet_dimensions']),
        reader_snapshot_sha256=runtime_payload['reader_snapshot_sha256'],
    )
    normal_run = NormalRunEvidence(
        external_wall_seconds=Decimal(normal_payload['external_wall_seconds']),
        peak_working_set_bytes=normal_payload['peak_working_set_bytes'],
        runtime=runtime,
        workbook_oracle_sha256=normal_payload['workbook_oracle_sha256'],
    )
    return MetricSample(
        role=payload['role'],
        global_round=payload['global_round'],
        metric_value=Decimal(payload['metric_value']),
        exit_code=payload['exit_code'],
        input_sha256=payload['input_sha256'],
        binary_sha256=payload['binary_sha256'],
        git_head=payload['git_head'],
        repository_state_sha256=payload['repository_state_sha256'],
        machine_fingerprint_sha256=payload['machine_fingerprint_sha256'],
        local_unversioned_log_sha256=payload['local_unversioned_log_sha256'],
        normal_run=normal_run,
    )


def _capture_identity(request: PairedBenchmarkRequest) -> BenchmarkIdentity:
    root = repo_root()
    git_head = _run_git(root, 'rev-parse', 'HEAD').strip()
    status = _run_git(root, 'status', '--porcelain=v1', '--untracked-files=all')
    diff = _run_git(root, 'diff', '--binary', 'HEAD', '--')
    repository_state = hashlib.sha256(f'{status}\n{diff}'.encode()).hexdigest()
    machine = '|'.join((platform.system(), platform.release(), platform.machine(), str(os.cpu_count() or 0)))
    return BenchmarkIdentity(
        _sha256(request.input_path),
        _sha256(request.reference_executable),
        _sha256(request.candidate_executable),
        git_head,
        repository_state,
        hashlib.sha256(machine.encode()).hexdigest(),
    )


def _assert_identity_unchanged(expected: BenchmarkIdentity, actual: BenchmarkIdentity) -> None:
    if actual != expected:
        raise HarnessFailure(
            HarnessVerdict.ENVIRONMENT_DRIFT, 'input, executable, Git, repository, or machine drift detected'
        )


def validate_formal_repository_state(
    status_entries: tuple[str, ...],
    *,
    evidence_root: Path,
    approved_prior_evidence: dict[Path, str],
    root: Path | None = None,
) -> None:
    root = (root or repo_root()).resolve()
    evidence_root = evidence_root.resolve()
    if not approved_prior_evidence:
        if status_entries:
            raise HarnessFailure(HarnessVerdict.ENVIRONMENT_DRIFT, 'first formal batch requires a clean worktree')
        return
    seen: set[Path] = set()
    for entry in status_entries:
        if not entry.startswith('?? '):
            raise HarnessFailure(HarnessVerdict.ENVIRONMENT_DRIFT, 'formal batch contains non-evidence worktree change')
        path = (root / entry[3:]).resolve()
        if path not in approved_prior_evidence:
            raise HarnessFailure(HarnessVerdict.ENVIRONMENT_DRIFT, 'formal batch contains non-evidence worktree change')
        _require_canonical_child(path, evidence_root)
        _require_no_reparse_points(path, evidence_root)
        if _sha256(path) != approved_prior_evidence[path]:
            raise HarnessFailure(HarnessVerdict.ENVIRONMENT_DRIFT, 'prior evidence content changed')
        seen.add(path)
    if seen != set(approved_prior_evidence):
        raise HarnessFailure(HarnessVerdict.ENVIRONMENT_DRIFT, 'approved prior evidence repository state is incomplete')


def _cleanup_all(paths: list[Path]) -> tuple[str, ...]:
    failures: list[str] = []
    for path in dict.fromkeys(paths):
        try:
            _remove_workbook(path)
        except OSError as exc:
            failures.append(f'{type(exc).__name__}:{exc.errno}')
    return tuple(failures)


def _remove_workbook(path: Path) -> None:
    path.unlink(missing_ok=True)


def _require_canonical_child(path: Path, root: Path) -> None:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise HarnessFailure(HarnessVerdict.SENSITIVE_EVIDENCE, 'path escapes its canonical root') from exc


def _require_no_reparse_points(path: Path, root: Path) -> None:
    current = root.resolve()
    for part in path.resolve().relative_to(current).parts:
        current /= part
        attributes = getattr(current.stat(), 'st_file_attributes', 0)
        if attributes & 0x400:
            raise HarnessFailure(HarnessVerdict.SENSITIVE_EVIDENCE, 'reparse points are forbidden in benchmark paths')


def _run_git(root: Path, *args: str) -> str:
    git = shutil.which('git')
    if git is None:
        raise HarnessFailure(HarnessVerdict.ENVIRONMENT_DRIFT, 'git executable not found')
    completed = subprocess.run(  # noqa: S603 - resolved local Git executable with closed harness arguments.
        [git, '-C', str(root), *args],
        check=False,
        capture_output=True,
        encoding='utf-8',
        errors='replace',
    )
    if completed.returncode != 0:
        raise HarnessFailure(HarnessVerdict.ENVIRONMENT_DRIFT, f'git command failed: {args!r}')
    return completed.stdout


def _sha256(path: Path) -> str:
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def _canonical_json(payload: object) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode('utf-8')


def _write_create_new(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open('xb') as stream:
            stream.write(payload)
    except FileExistsError as exc:
        raise HarnessFailure(
            HarnessVerdict.INCOMPLETE_EVIDENCE, f'append-only record overwrite refused: {path}'
        ) from exc


def _is_sha256(value: str | None) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(char in '0123456789abcdef' for char in value)


def run_pws_group(request: MetricGroupRequest) -> MetricGroup:
    raise NotImplementedError('Phase 0H Task 3 owns the PWS runner')


def run_paired_normal_batch(request: PairedBenchmarkRequest) -> PairedBenchmarkResult:
    raise NotImplementedError('Phase 0H Task 6 owns full paired orchestration')


def run_phase0h_smoke(request: Phase0HSmokeRequest) -> Phase0HSmokeResult:
    raise NotImplementedError('Phase 0H Task 6 owns smoke orchestration')


def capture_phase0a(request: Phase0ARequest) -> object:
    raise NotImplementedError('Phase 0H Task 6 owns Phase 0A capture')


def main(argv: list[str] | None = None) -> int:
    raise NotImplementedError('Phase 0H Task 6 owns the CLI')

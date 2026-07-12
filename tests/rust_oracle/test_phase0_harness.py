from __future__ import annotations

import getpass
import hashlib
import importlib.util
import json
import socket
import zipfile
from dataclasses import asdict, replace
from decimal import Decimal
from pathlib import Path

import pytest

from tests.rust_oracle import phase0_harness
from tests.rust_oracle.benchmark_protocol import (
    AttemptState,
    BatchAttempt,
    CalibrationGroup,
    CalibrationRound,
    ClosedBinaryLabel,
    ComparisonProfile,
    HarnessVerdict,
    MachineEvidence,
    MetricGroup,
    MetricSample,
    NormalRunEvidence,
    PairedRound,
    RuntimeEvidence,
    assert_same_benchmark_batch,
    build_round_plan,
)
from tests.rust_oracle.evidence import (
    ApprovedSheet,
    BenchmarkManifestEvidence,
    EvidenceSanitizer,
    SmokeSummaryEvidence,
    _batch_commit_marker,
)
from tests.rust_oracle.oracle_runner import (
    CapturedNormalRun,
    RustNormalProcessError,
    RustNormalValidationError,
    build_rust_cli_release,
    run_python_oracle,
    run_rust_cli_release,
)
from tests.rust_oracle.phase0_harness import (
    AppendOnlyAttemptLedger,
    BenchmarkIdentity,
    HarnessFailure,
    MetricGroupRequest,
    PairedBenchmarkRequest,
    Phase0ARequest,
    Phase0HSmokeRequest,
    UnverifiedPriorEvidenceClaim,
    derive_batch_id,
    run_normal_wall_group,
    run_phase0h_smoke,
    validate_formal_repository_state,
)


@pytest.fixture(autouse=True)
def _trusted_repo_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(phase0_harness, 'repo_root', lambda: tmp_path)


def _runtime(pipeline: str = 'gb') -> RuntimeEvidence:
    return RuntimeEvidence(
        pipeline=pipeline,  # type: ignore[arg-type]
        output_written=True,
        request_id_present=True,
        sheet_count=3,
        error_log_count=0,
        issue_type_counts=(),
        quality_metrics=(),
        run_counts=(('reader_rows', 1),),
        stage_timings=tuple(
            (name, Decimal('0.1'))
            for name in ('ingest', 'normalize', 'split', 'fact', 'presentation', 'total', 'export')
        ),
        output_size_bytes=8,
        sheet_dimensions=('1x1', '1x1', '1x1'),
        reader_snapshot_sha256='',
    )


_APPROVED_TEST_DIMENSIONS = ('A1:B2', 'A1:C3', 'A1:D4')


def _write_approved_test_workbook(path: Path) -> None:
    from openpyxl import Workbook

    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = Workbook()
    try:
        for index, (sheet, dimension) in enumerate(zip(ApprovedSheet, _APPROVED_TEST_DIMENSIONS, strict=True)):
            worksheet = workbook.active if index == 0 else workbook.create_sheet()
            worksheet.title = sheet.value
            worksheet['A1'] = 'start'
            worksheet[dimension.split(':')[1]] = 'end'
        workbook.save(path)
    finally:
        workbook.close()


def _request(tmp_path: Path) -> PairedBenchmarkRequest:
    input_path = tmp_path / 'input.xlsx'
    reference = tmp_path / 'reference.exe'
    candidate = tmp_path / 'candidate.exe'
    manifest = tmp_path / 'docs' / 'performance' / 'phase0a.json'
    manifest.parent.mkdir(parents=True, exist_ok=True)
    for path, content in ((input_path, b'input'), (reference, b'ref'), (candidate, b'candidate'), (manifest, b'{}')):
        path.write_bytes(content)
    return PairedBenchmarkRequest(
        pipeline='gb',
        input_path=input_path,
        reference_executable=reference,
        candidate_executable=candidate,
        reference_label=ClosedBinaryLabel.PHASE0A,
        candidate_label=ClosedBinaryLabel.PHASE0B,
        comparison_profile=ComparisonProfile.PHASE0B_VS_PHASE0A,
        phase0a_manifest=manifest,
        local_root=tmp_path / 'rust' / 'target' / 'perf-local',
        evidence_path=tmp_path / 'docs' / 'performance' / 'batch.json',
        attempt_ledger_root=tmp_path / 'rust' / 'target' / 'perf-local' / 'batches',
    )


def _identity() -> BenchmarkIdentity:
    return BenchmarkIdentity('3' * 8, '1' * 8, '2' * 8, 'head', '4' * 8, '5' * 8)


def _ledger(tmp_path: Path) -> AppendOnlyAttemptLedger:
    local_root = tmp_path / 'rust' / 'target' / 'perf-local'
    return AppendOnlyAttemptLedger.create(
        local_root / 'batches',
        _identity(),
        comparison_key='a' * 64,
    )


def _rewrite_metadata_and_matching_empty_journal(attempt: Path, **changes: object) -> None:
    metadata_path = attempt / 'metadata.json'
    metadata = json.loads(metadata_path.read_text(encoding='utf-8'))
    metadata.update(changes)
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


def _write_synthetic_record(
    attempt: Path,
    *,
    sequence: int,
    kind: str,
    previous_record_sha256: str,
    previous_checkpoint_sha256: str,
    write_checkpoint: bool = True,
    **payload: object,
) -> tuple[str, str]:
    record = {'kind': kind, 'previous_record_sha256': previous_record_sha256, **payload}
    record_raw = phase0_harness._canonical_json(record)
    (attempt / 'records' / f'{sequence:04d}-{kind}.json').write_bytes(record_raw)
    record_sha = hashlib.sha256(record_raw).hexdigest()
    checkpoint = {
        'record_count': sequence,
        'record_sha256': record_sha,
        'previous_checkpoint_sha256': previous_checkpoint_sha256,
    }
    checkpoint_raw = phase0_harness._canonical_json(checkpoint)
    if write_checkpoint:
        (attempt / 'checkpoints' / f'{sequence:04d}.json').write_bytes(checkpoint_raw)
    return record_sha, hashlib.sha256(checkpoint_raw).hexdigest()


def _write_recoverable_v1_success(
    tmp_path: Path,
    *,
    comparison_key: str,
    missing_checkpoint: bool,
    stale_journal: bool,
) -> Path:
    if missing_checkpoint == stale_journal:
        raise ValueError('select exactly one v1 recovery edge')
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
    record1, checkpoint1 = _write_synthetic_record(
        attempt,
        sequence=1,
        kind='first-group',
        previous_record_sha256=metadata_sha,
        previous_checkpoint_sha256=metadata_sha,
        groups={'wall': {}, 'pws': {}},
    )
    record2, checkpoint2 = _write_synthetic_record(
        attempt,
        sequence=2,
        kind='cleanup-complete',
        previous_record_sha256=record1,
        previous_checkpoint_sha256=checkpoint1,
        planned_output_count=0,
    )
    artifact_content = '{}'
    artifact_sha256 = hashlib.sha256(artifact_content.encode('utf-8')).hexdigest()
    record3, checkpoint3 = _write_synthetic_record(
        attempt,
        sequence=3,
        kind='evidence-prepared',
        previous_record_sha256=record2,
        previous_checkpoint_sha256=checkpoint2,
        artifact_basename='benchmark-v1-audit.json',
        artifact_sha256=artifact_sha256,
        artifact_content=artifact_content,
    )
    record4, checkpoint4 = _write_synthetic_record(
        attempt,
        sequence=4,
        kind='evidence-committed',
        previous_record_sha256=record3,
        previous_checkpoint_sha256=checkpoint3,
        write_checkpoint=not missing_checkpoint,
        artifact_sha256=artifact_sha256,
    )
    state = phase0_harness._journal_state_payload(
        attempt_number=1,
        record_count=3 if stale_journal else 4,
        record_head_sha256=record3 if stale_journal else record4,
        checkpoint_head_sha256=checkpoint3 if stale_journal else checkpoint4,
        terminal_present=False,
        terminal_head_sha256=None,
        verdict=None,
    )
    journal_raw = phase0_harness._canonical_json({'previous_journal_sha256': None, **state})
    (comparison / 'journal' / '000001.json').write_bytes(journal_raw)
    return attempt


def _file_snapshot(root: Path) -> dict[str, bytes]:
    return {path.relative_to(root).as_posix(): path.read_bytes() for path in root.rglob('*') if path.is_file()}


def _claim(tmp_path: Path, path: Path, sha256: str) -> UnverifiedPriorEvidenceClaim:
    return UnverifiedPriorEvidenceClaim(
        path_alias=path.relative_to(tmp_path).as_posix(),
        content_sha256=sha256,
    )


def _install_runner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    fail_role: str | None = None,
    validation_fail_role: str | None = None,
    exception_role: str | None = None,
    interrupt_role: str | None = None,
    oracle_sha: str = 'oracle',
) -> list[tuple[str, ...]]:
    commands: list[tuple[str, ...]] = []

    def fake_capture(
        executable: Path, pipeline: str, input_path: Path, output_path: Path, **kwargs: object
    ) -> CapturedNormalRun:
        role = 'reference' if executable.name.startswith('reference') else 'candidate'
        commands.append((role, pipeline, str(input_path), str(output_path), *kwargs.keys()))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b'workbook')
        if role == interrupt_role:
            raise KeyboardInterrupt('simulated process interruption')
        if role == validation_fail_role:
            raise RustNormalValidationError('invalid workbook', 'e' * 64)
        if role == exception_role:
            raise RuntimeError('unexpected adapter failure')
        if role == fail_role:
            raise RustNormalProcessError(9, 'f' * 64)
        return CapturedNormalRun(
            normal_run=NormalRunEvidence(Decimal('1.0'), None, _runtime(pipeline), oracle_sha),
            exit_code=0,
            local_unversioned_log_sha256='l' * 64,
        )

    monkeypatch.setattr(phase0_harness, 'run_rust_normal_captured', fake_capture)
    monkeypatch.setattr(phase0_harness, '_capture_identity', lambda request: _identity())
    monkeypatch.setattr(phase0_harness, 'repo_root', lambda: tmp_path)
    return commands


def _group_request(tmp_path: Path) -> MetricGroupRequest:
    ledger = _ledger(tmp_path)
    return MetricGroupRequest(
        _request(tmp_path),
        'b' * 8,
        'wall',
        build_round_plan(global_round_start=1, round_count=5),
        ledger.attempt_directory,
    )


def _metric_group(metric: str, *, start: int = 1, reference: str = '1.0', candidate: str = '1.0') -> MetricGroup:
    plans = build_round_plan(global_round_start=start, round_count=5)  # type: ignore[arg-type]
    rounds: list[PairedRound] = []
    for plan in plans:
        common = {
            'global_round': plan.global_round,
            'exit_code': 0,
            'input_sha256': '3' * 64,
            'git_head': '4' * 40,
            'repository_state_sha256': '5' * 64,
            'machine_fingerprint_sha256': '6' * 64,
            'local_unversioned_log_sha256': '7' * 64,
        }
        runtime = replace(_runtime(), sheet_dimensions=('A1:A1', 'A1:A1', 'A1:A1'))
        reference_run = NormalRunEvidence(Decimal('1'), 100, runtime, '8' * 64)
        candidate_run = NormalRunEvidence(Decimal('1'), 100, runtime, '8' * 64)
        rounds.append(
            PairedRound(
                plan,
                MetricSample(
                    role='reference',
                    metric_value=Decimal(reference),
                    binary_sha256='1' * 64,
                    normal_run=reference_run,
                    **common,
                ),
                MetricSample(
                    role='candidate',
                    metric_value=Decimal(candidate),
                    binary_sha256='2' * 64,
                    normal_run=candidate_run,
                    **common,
                ),
            )
        )
    return MetricGroup('9' * 64, 'gb', metric, start, tuple(rounds))  # type: ignore[arg-type]


def _with_candidate_stage_ratios(group: MetricGroup, ratios: tuple[dict[str, Decimal], ...]) -> MetricGroup:
    rounds: list[PairedRound] = []
    for paired, round_ratios in zip(group.rounds, ratios, strict=True):
        reference_timings = dict(paired.reference.normal_run.runtime.stage_timings)
        candidate_timings = dict(paired.candidate.normal_run.runtime.stage_timings)
        for stage, ratio in round_ratios.items():
            reference_timings.setdefault(stage, Decimal('0.1'))
            candidate_timings[stage] = reference_timings[stage] * ratio
        reference = replace(
            paired.reference,
            normal_run=replace(
                paired.reference.normal_run,
                runtime=replace(
                    paired.reference.normal_run.runtime,
                    stage_timings=tuple(reference_timings.items()),
                ),
            ),
        )
        candidate = replace(
            paired.candidate,
            normal_run=replace(
                paired.candidate.normal_run,
                runtime=replace(
                    paired.candidate.normal_run.runtime,
                    stage_timings=tuple(candidate_timings.items()),
                ),
            ),
        )
        rounds.append(replace(paired, reference=reference, candidate=candidate))
    return replace(group, rounds=tuple(rounds))


def _with_output_sizes(
    group: MetricGroup,
    *,
    reference_sizes: tuple[int, ...],
    candidate_sizes: tuple[int, ...],
) -> MetricGroup:
    assert len(group.rounds) == len(reference_sizes) == len(candidate_sizes)

    def with_size(sample: MetricSample, size: int) -> MetricSample:
        runtime = replace(sample.normal_run.runtime, output_size_bytes=size)
        return replace(sample, normal_run=replace(sample.normal_run, runtime=runtime))

    return replace(
        group,
        rounds=tuple(
            replace(
                paired,
                reference=with_size(paired.reference, reference_size),
                candidate=with_size(paired.candidate, candidate_size),
            )
            for paired, reference_size, candidate_size in zip(
                group.rounds, reference_sizes, candidate_sizes, strict=True
            )
        ),
    )


def _phase0a_baseline(output_size: int = 100) -> phase0_harness._ApprovedPhase0ABaseline:
    return phase0_harness._ApprovedPhase0ABaseline(
        '5' * 8,
        output_size,
        (Decimal('1'),) * 5,
        (Decimal('100'),) * 5,
    )


def _calibration_group(pipeline: str, metric: str, output_size: int) -> CalibrationGroup:
    paired = _metric_group(metric)
    rounds = tuple(
        CalibrationRound(
            item.plan.global_round,
            replace(
                item.reference,
                normal_run=replace(
                    item.reference.normal_run,
                    runtime=replace(
                        item.reference.normal_run.runtime,
                        pipeline=pipeline,
                        output_size_bytes=output_size,
                        sheet_dimensions=('A1:L8', 'A1:AZ3', 'A1:AZ1'),
                    ),
                ),
            ),
        )
        for item in paired.rounds
    )
    return CalibrationGroup(f'{pipeline}-{metric}', pipeline, metric, True, rounds)  # type: ignore[arg-type]


def _with_calibration_output_sizes(
    group: CalibrationGroup,
    output_sizes: tuple[int | None, ...],
) -> CalibrationGroup:
    return replace(
        group,
        rounds=tuple(
            replace(
                item,
                reference=replace(
                    item.reference,
                    normal_run=replace(
                        item.reference.normal_run,
                        runtime=replace(item.reference.normal_run.runtime, output_size_bytes=output_size),
                    ),
                ),
            )
            for item, output_size in zip(group.rounds, output_sizes, strict=True)
        ),
    )


def test_sanitized_fixture_contains_no_erp_or_host_canary(tmp_path: Path) -> None:
    spec = importlib.util.find_spec('tests.rust_oracle.sanitized_fixture')
    assert spec is not None, 'sanitized fixture builder is required'
    from tests.rust_oracle.sanitized_fixture import build_raw_fixture

    fixture = tmp_path / 'synthetic-gb.xlsx'
    build_raw_fixture(fixture, 'gb', 'small')

    with zipfile.ZipFile(fixture) as archive:
        package_text = b'\n'.join(archive.read(name) for name in archive.namelist()).decode('utf-8', errors='ignore')
    folded = package_text.casefold()
    assert 'erp' not in folded
    assert getpass.getuser().casefold() not in folded
    assert socket.gethostname().casefold() not in folded


def test_stable_workbook_oracle_ignores_volatile_core_properties(tmp_path: Path) -> None:
    import xlsxwriter

    paths = (tmp_path / 'first.xlsx', tmp_path / 'second.xlsx')
    for index, path in enumerate(paths):
        workbook = xlsxwriter.Workbook(path)
        workbook.set_properties({'comments': f'volatile-{index}'})
        workbook.add_worksheet('Sheet1').write(0, 0, 'same')
        workbook.close()

    assert phase0_harness._stable_workbook_oracle(paths[0]) == phase0_harness._stable_workbook_oracle(paths[1])


def test_low_memory_fixture_produces_at_least_five_million_output_slots(tmp_path: Path) -> None:
    from openpyxl import load_workbook

    from tests.rust_oracle.sanitized_fixture import (
        LOW_MEMORY_OUTPUT_COLUMNS_LOWER_BOUND,
        LOW_MEMORY_QUANTITY_ROWS,
        build_raw_fixture,
    )

    fixture = tmp_path / 'synthetic-low-memory-sk.xlsx'
    output = tmp_path / 'synthetic-low-memory-sk-output.xlsx'
    build_raw_fixture(fixture, 'sk', 'low-memory')

    workbook = load_workbook(fixture, read_only=True, data_only=True)
    try:
        sheet = workbook.active
        assert sheet.max_row >= LOW_MEMORY_QUANTITY_ROWS + 4
        assert LOW_MEMORY_QUANTITY_ROWS >= 100_000
        assert LOW_MEMORY_QUANTITY_ROWS * LOW_MEMORY_OUTPUT_COLUMNS_LOWER_BOUND >= 5_000_000
    finally:
        workbook.close()

    run_rust_cli_release(build_rust_cli_release(), 'sk', fixture, output)
    workbook = load_workbook(output, read_only=True, data_only=True)
    try:
        sheet = workbook[ApprovedSheet.QUANTITY.value]
        assert sheet.max_row >= LOW_MEMORY_QUANTITY_ROWS + 1
        assert sheet.max_row * sheet.max_column >= 5_000_000
    finally:
        workbook.close()


def _install_smoke_capture(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []

    def fake_wall(
        executable: Path, pipeline: str, input_path: Path, output_path: Path, **kwargs: object
    ) -> CapturedNormalRun:
        del executable, input_path, kwargs
        calls.append('wall')
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b'synthetic workbook')
        return CapturedNormalRun(
            NormalRunEvidence(Decimal('1'), None, _runtime(pipeline), '8' * 64),
            0,
            '7' * 64,
        )

    def fake_pws(**kwargs: object) -> CapturedNormalRun:
        calls.append('pws')
        assert kwargs['local_root'] == phase0_harness._trusted_local_root()
        artifacts = phase0_harness._pws_local_artifact_paths(
            kwargs['local_root'], kwargs['batch_id'], kwargs['global_round'], kwargs['role']
        )
        for path in (artifacts.result_path, artifacts.stdout_path, artifacts.stderr_path, artifacts.driver_log_path):
            phase0_harness._io_path(path).write_text('{}', encoding='utf-8')
        output_path = kwargs['output_path']
        assert isinstance(output_path, Path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b'synthetic workbook')
        pipeline = kwargs['pipeline']
        assert isinstance(pipeline, str)
        return CapturedNormalRun(
            NormalRunEvidence(Decimal('1'), 123, _runtime(pipeline), '8' * 64),
            0,
            '7' * 64,
        )

    monkeypatch.setattr(phase0_harness, 'run_rust_normal_captured', fake_wall)
    monkeypatch.setattr(phase0_harness, '_invoke_pws_single_sample', fake_pws)
    monkeypatch.setattr(
        phase0_harness, '_workbook_sheet_names', lambda path: tuple(item.value for item in ApprovedSheet)
    )
    return calls


def test_phase0h_smoke_runs_normal_wall_and_normal_pws(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls = _install_smoke_capture(monkeypatch)
    executable = tmp_path / 'reference.exe'
    executable.write_bytes(b'synthetic executable')

    smoke_root = tmp_path / 'rust' / 'target' / 'perf-local' / 'phase0h-smoke'
    result = run_phase0h_smoke(Phase0HSmokeRequest('gb', executable, executable, smoke_root))

    assert result.verdict is HarnessVerdict.VALIDATED
    assert calls.count('wall') == 10
    assert calls.count('pws') == 10


def test_phase0h_smoke_cleans_every_workbook(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_smoke_capture(monkeypatch)
    executable = tmp_path / 'reference.exe'
    executable.write_bytes(b'synthetic executable')
    smoke_root = tmp_path / 'rust' / 'target' / 'perf-local' / 'phase0h-smoke'

    run_phase0h_smoke(Phase0HSmokeRequest('sk', executable, executable, smoke_root))

    assert not list(smoke_root.rglob('*.xlsx'))
    assert not list(phase0_harness._io_path(phase0_harness._trusted_local_root() / 'pws-results').rglob('*.json'))
    assert not list(phase0_harness._io_path(phase0_harness._trusted_local_root() / 'pws-logs').rglob('*.log'))


def test_phase0h_smoke_records_observed_canary_residue_and_sheet_contract(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_smoke_capture(monkeypatch)
    executable = tmp_path / 'reference.exe'
    executable.write_bytes(b'synthetic executable')
    observed: list[SmokeSummaryEvidence] = []
    original = EvidenceSanitizer.build_smoke

    def capture(self: EvidenceSanitizer, value: SmokeSummaryEvidence) -> object:
        observed.append(value)
        return original(self, value)

    monkeypatch.setattr(EvidenceSanitizer, 'build_smoke', capture)
    run_phase0h_smoke(
        Phase0HSmokeRequest(
            'gb',
            executable,
            executable,
            tmp_path / 'rust' / 'target' / 'perf-local' / 'phase0h-smoke',
        )
    )

    smoke = observed[0]
    assert smoke.temp_canary_created is True
    assert smoke.temp_residue_count == 0
    assert smoke.approved_sheets == tuple(ApprovedSheet)


@pytest.mark.parametrize('mutation', ('missing-sentinel', 'extra-artifact'))
def test_phase0h_smoke_rejects_temp_canary_mutation_and_still_cleans_environment(
    mutation: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_smoke_capture(monkeypatch)
    original_capture = phase0_harness.run_rust_normal_captured
    mutated = False

    def mutate_canary(*args: object, **kwargs: object) -> CapturedNormalRun:
        nonlocal mutated
        captured = original_capture(*args, **kwargs)  # type: ignore[arg-type]
        if not mutated:
            temp_root = Path(phase0_harness.os.environ['TEMP'])
            if mutation == 'missing-sentinel':
                (temp_root / 'sentinel.txt').unlink()
            else:
                (temp_root / 'unexpected.tmp').write_bytes(b'unexpected')
            mutated = True
        return captured

    monkeypatch.setattr(phase0_harness, 'run_rust_normal_captured', mutate_canary)
    monkeypatch.setenv('TEMP', str(tmp_path / 'original-temp'))
    monkeypatch.setenv('TMP', str(tmp_path / 'original-tmp'))
    executable = tmp_path / 'reference.exe'
    executable.write_bytes(b'synthetic executable')
    smoke_root = tmp_path / 'rust' / 'target' / 'perf-local' / 'phase0h-smoke'

    with pytest.raises(HarnessFailure) as caught:
        run_phase0h_smoke(Phase0HSmokeRequest('gb', executable, executable, smoke_root))

    assert caught.value.verdict is HarnessVerdict.ENVIRONMENT_DRIFT
    assert phase0_harness.os.environ['TEMP'] == str(tmp_path / 'original-temp')
    assert phase0_harness.os.environ['TMP'] == str(tmp_path / 'original-tmp')
    assert not (smoke_root / 'temp-canary').exists()
    assert not list(smoke_root.rglob('*.xlsx'))


def test_phase0a_manifest_uses_external_output_size_for_base_reference(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_phase0a_capture(monkeypatch, tmp_path)
    request = _phase0a_request(tmp_path)

    phase0_harness.capture_phase0a(request)

    payload = json.loads(request.output_path.read_text(encoding='utf-8'))
    assert payload['pipelines']['gb']['output_size_bytes'] == 321
    assert payload['pipelines']['sk']['output_size_bytes'] == 654


def test_phase0a_manifest_uses_conservative_decimal_median_for_one_byte_output_variation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_phase0a_capture(monkeypatch, tmp_path)
    request = _phase0a_request(tmp_path)

    def variable_group(*, pipeline: str, metric: str, **kwargs: object) -> CalibrationGroup:
        del kwargs
        base = 3_813_018 if pipeline == 'gb' else 654
        sizes = (base,) * 5 if metric == 'wall' else (base + 1,) * 5
        return _with_calibration_output_sizes(_calibration_group(pipeline, metric, base), sizes)

    monkeypatch.setattr(phase0_harness, '_capture_phase0a_group', variable_group)

    phase0_harness.capture_phase0a(request)

    payload = json.loads(request.output_path.read_text(encoding='utf-8'))
    assert payload['pipelines']['gb']['output_size_bytes'] == 3_813_019
    assert payload['pipelines']['sk']['output_size_bytes'] == 655


@pytest.mark.parametrize('invalid_size', (None, 0, -1, True))
def test_phase0a_manifest_rejects_missing_non_positive_or_boolean_output_size(
    invalid_size: int | None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_phase0a_capture(monkeypatch, tmp_path)
    request = _phase0a_request(tmp_path)

    def invalid_group(*, pipeline: str, metric: str, **kwargs: object) -> CalibrationGroup:
        del kwargs
        group = _calibration_group(pipeline, metric, 321 if pipeline == 'gb' else 654)
        if pipeline == 'gb' and metric == 'pws':
            return _with_calibration_output_sizes(group, (invalid_size,) * 5)
        return group

    monkeypatch.setattr(phase0_harness, '_capture_phase0a_group', invalid_group)

    with pytest.raises(HarnessFailure) as caught:
        phase0_harness.capture_phase0a(request)

    assert caught.value.verdict is HarnessVerdict.INCOMPLETE_EVIDENCE
    assert not request.output_path.exists()


def test_phase0a_manifest_contains_gb_sk_wall_pws_runtime_and_sheet_dimensions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_phase0a_capture(monkeypatch, tmp_path)
    request = _phase0a_request(tmp_path)

    phase0_harness.capture_phase0a(request)

    payload = json.loads(request.output_path.read_text(encoding='utf-8'))
    assert tuple(payload['pipelines']) == ('gb', 'sk')
    for pipeline in ('gb', 'sk'):
        assert set(payload['pipelines'][pipeline]['calibration']) == {'wall', 'pws'}
        assert payload['pipelines'][pipeline]['runtime']['sheet_count'] == 3
        assert payload['pipelines'][pipeline]['sheet_dimensions'] == ['A1:L8', 'A1:AZ3', 'A1:AZ1']


def test_phase0a_manifest_contains_no_paths_filenames_hostname_or_raw_logs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_phase0a_capture(monkeypatch, tmp_path)
    request = _phase0a_request(tmp_path)

    phase0_harness.capture_phase0a(request)

    text = request.output_path.read_text(encoding='utf-8').casefold()
    assert str(tmp_path).casefold() not in text
    assert request.gb_input_path.name.casefold() not in text
    assert request.sk_input_path.name.casefold() not in text
    assert socket.gethostname().casefold() not in text
    assert 'stdout' not in text and 'stderr' not in text


def test_phase0a_capture_refuses_existing_manifest(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_phase0a_capture(monkeypatch, tmp_path)
    request = _phase0a_request(tmp_path)
    request.output_path.parent.mkdir(parents=True, exist_ok=True)
    request.output_path.write_text('{}', encoding='utf-8')

    with pytest.raises(FileExistsError):
        phase0_harness.capture_phase0a(request)


def test_phase0a_pws_uses_trusted_raw_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    input_path = tmp_path / 'synthetic.xlsx'
    executable = tmp_path / 'reference.exe'
    input_path.write_bytes(b'input')
    executable.write_bytes(b'executable')
    local_root = tmp_path / 'rust' / 'target' / 'perf-local' / 'phase0a'
    machine = MachineEvidence('build', 'x86_64', 'cpu', 1, 1, 'UNKNOWN', 1, '6' * 64)
    monkeypatch.setattr(phase0_harness, '_run_git', lambda root, *args: '4' * 40)
    monkeypatch.setattr(phase0_harness, '_capture_machine_evidence', lambda: machine)

    def fake_pws(**kwargs: object) -> CapturedNormalRun:
        assert kwargs['local_root'] == phase0_harness._trusted_local_root()
        assert phase0_harness._is_sha256(kwargs['batch_id'])
        output = kwargs['output_path']
        assert isinstance(output, Path)
        _write_approved_test_workbook(output)
        return CapturedNormalRun(
            NormalRunEvidence(
                Decimal('1'),
                123,
                replace(_runtime('gb'), sheet_dimensions=_APPROVED_TEST_DIMENSIONS),
                '8' * 64,
            ),
            0,
            '7' * 64,
        )

    monkeypatch.setattr(phase0_harness, '_invoke_pws_single_sample', fake_pws)
    group = phase0_harness._capture_phase0a_group(
        pipeline='gb',
        metric='pws',
        input_path=input_path,
        executable=executable,
        local_root=local_root,
        machine=machine,
    )
    assert len(group.rounds) == 5


def test_phase0a_group_fills_empty_runtime_dimensions_from_actual_workbook(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    input_path = tmp_path / 'synthetic.xlsx'
    executable = tmp_path / 'reference.exe'
    input_path.write_bytes(b'input')
    executable.write_bytes(b'executable')
    local_root = tmp_path / 'rust' / 'target' / 'perf-local' / 'phase0a'
    machine = MachineEvidence('build', 'x86_64', 'cpu', 1, 1, 'UNKNOWN', 1, '6' * 64)
    monkeypatch.setattr(phase0_harness, '_run_git', lambda root, *args: '4' * 40)
    monkeypatch.setattr(phase0_harness, '_capture_machine_evidence', lambda: machine)

    def fake_capture(
        executable: Path, pipeline: str, input_path: Path, output_path: Path, **kwargs: object
    ) -> CapturedNormalRun:
        del executable, input_path, kwargs
        _write_approved_test_workbook(output_path)
        return CapturedNormalRun(
            NormalRunEvidence(Decimal('1'), None, replace(_runtime(pipeline), sheet_dimensions=()), '8' * 64),
            0,
            '7' * 64,
        )

    monkeypatch.setattr(phase0_harness, 'run_rust_normal_captured', fake_capture)
    group = phase0_harness._capture_phase0a_group(
        pipeline='gb',
        metric='wall',
        input_path=input_path,
        executable=executable,
        local_root=local_root,
        machine=machine,
    )

    assert all(item.reference.normal_run.runtime.sheet_dimensions == _APPROVED_TEST_DIMENSIONS for item in group.rounds)
    sk_wall = _calibration_group('sk', 'wall', 8)
    sk_pws = _calibration_group('sk', 'pws', 8)
    payload = phase0_harness._phase0a_payload(
        phase0_harness.Phase0AManifest(
            '1' * 64,
            'a' * 40,
            '4' * 40,
            machine,
            group,
            replace(group, metric='pws'),
            sk_wall,
            sk_pws,
        ),
        _phase0a_request(tmp_path),
    )
    assert payload['pipelines']['gb']['sheet_dimensions'] == list(_APPROVED_TEST_DIMENSIONS)


def test_phase0a_group_rejects_runtime_dimensions_that_differ_from_actual_workbook(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    input_path = tmp_path / 'synthetic.xlsx'
    executable = tmp_path / 'reference.exe'
    input_path.write_bytes(b'input')
    executable.write_bytes(b'executable')
    local_root = tmp_path / 'rust' / 'target' / 'perf-local' / 'phase0a'
    machine = MachineEvidence('build', 'x86_64', 'cpu', 1, 1, 'UNKNOWN', 1, '6' * 64)
    monkeypatch.setattr(phase0_harness, '_run_git', lambda root, *args: '4' * 40)
    monkeypatch.setattr(phase0_harness, '_capture_machine_evidence', lambda: machine)

    def fake_capture(
        executable: Path, pipeline: str, input_path: Path, output_path: Path, **kwargs: object
    ) -> CapturedNormalRun:
        del executable, input_path, kwargs
        _write_approved_test_workbook(output_path)
        return CapturedNormalRun(NormalRunEvidence(Decimal('1'), None, _runtime(pipeline), '8' * 64), 0, '7' * 64)

    monkeypatch.setattr(phase0_harness, 'run_rust_normal_captured', fake_capture)

    with pytest.raises(HarnessFailure) as caught:
        phase0_harness._capture_phase0a_group(
            pipeline='gb',
            metric='wall',
            input_path=input_path,
            executable=executable,
            local_root=local_root,
            machine=machine,
        )
    assert caught.value.verdict is HarnessVerdict.CORRECTNESS_FAILED


def _phase0a_request(tmp_path: Path) -> Phase0ARequest:
    gb = tmp_path / 'confidential-gb.xlsx'
    sk = tmp_path / 'confidential-sk.xlsx'
    executable = tmp_path / 'reference.exe'
    gb.write_bytes(b'gb')
    sk.write_bytes(b'sk')
    executable.write_bytes(b'executable')
    return Phase0ARequest(
        gb,
        sk,
        executable,
        'a' * 40,
        tmp_path / 'rust' / 'target' / 'perf-local' / 'phase0a',
        tmp_path / 'docs' / 'performance' / 'phase0a.json',
    )


def _install_phase0a_capture(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv('COSTING_GB_SAMPLE', str(tmp_path / 'confidential-gb.xlsx'))
    monkeypatch.setenv('COSTING_SK_SAMPLE', str(tmp_path / 'confidential-sk.xlsx'))

    def fake_group(*, pipeline: str, metric: str, **kwargs: object) -> CalibrationGroup:
        del kwargs
        return _calibration_group(pipeline, metric, 321 if pipeline == 'gb' else 654)

    monkeypatch.setattr(phase0_harness, '_capture_phase0a_group', fake_group, raising=False)
    monkeypatch.setattr(
        phase0_harness,
        '_capture_machine_evidence',
        lambda: MachineEvidence('synthetic-build', 'x86_64', 'synthetic-cpu', 4, 8, 'SSD', 16, '6' * 64),
        raising=False,
    )
    monkeypatch.setattr(phase0_harness, '_run_git', lambda root, *args: '4' * 40)


def test_phase0a_group_failure_cleans_residuals_and_allows_rerun(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_phase0a_capture(monkeypatch, tmp_path)
    request = _phase0a_request(tmp_path)
    failed = False

    def flaky_group(*, pipeline: str, metric: str, local_root: Path, **kwargs: object) -> CalibrationGroup:
        nonlocal failed
        del kwargs
        raw_log = local_root / 'raw-logs' / 'partial.json'
        workbook = local_root / 'outputs' / pipeline / metric / 'partial.xlsx'
        raw_log.parent.mkdir(parents=True, exist_ok=True)
        workbook.parent.mkdir(parents=True, exist_ok=True)
        raw_log.write_text('{}', encoding='utf-8')
        workbook.write_bytes(b'partial')
        if not failed:
            failed = True
            raise RustNormalProcessError(9, 'f' * 64)
        return _calibration_group(pipeline, metric, 321 if pipeline == 'gb' else 654)

    monkeypatch.setattr(phase0_harness, '_capture_phase0a_group', flaky_group)

    with pytest.raises(HarnessFailure) as caught:
        phase0_harness.capture_phase0a(request)
    assert caught.value.verdict is HarnessVerdict.REFERENCE_FAILED
    assert caught.value.raw_log_sha256 == 'f' * 64
    assert not request.output_path.exists()
    assert not (request.local_root / 'raw-logs').exists()
    assert not (request.local_root / 'outputs').exists()

    phase0_harness.capture_phase0a(request)
    assert request.output_path.is_file()


@pytest.mark.parametrize('failure_point', ('scan', 'publish'))
def test_phase0a_scan_or_publish_failure_removes_staging_and_incomplete_manifest(
    failure_point: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_phase0a_capture(monkeypatch, tmp_path)
    request = _phase0a_request(tmp_path)
    if failure_point == 'scan':
        monkeypatch.setattr(
            EvidenceSanitizer,
            'scan_tree',
            lambda self, *args, **kwargs: (_ for _ in ()).throw(ValueError('sensitive staging')),
        )
    else:
        monkeypatch.setattr(
            phase0_harness.os,
            'link',
            lambda *args, **kwargs: (_ for _ in ()).throw(OSError('publication failed')),
        )

    with pytest.raises(HarnessFailure) as caught:
        phase0_harness.capture_phase0a(request)

    assert caught.value.verdict is HarnessVerdict.SENSITIVE_EVIDENCE
    assert not request.output_path.exists()
    assert not list(request.local_root.glob('phase0a-sanitized-*'))
    assert not (request.local_root / 'raw-logs').exists()
    assert not (request.local_root / 'outputs').exists()


def test_phase0a_outer_cleanup_failure_preserves_nested_cleanup_primary_verdict(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_phase0a_capture(monkeypatch, tmp_path)
    request = _phase0a_request(tmp_path)
    monkeypatch.setattr(
        phase0_harness,
        '_capture_phase0a_group',
        lambda **kwargs: (_ for _ in ()).throw(
            HarnessFailure(
                HarnessVerdict.CLEANUP_FAILED,
                'group cleanup failed',
                primary_verdict=HarnessVerdict.REFERENCE_FAILED,
                raw_log_sha256='f' * 64,
            )
        ),
    )
    monkeypatch.setattr(
        phase0_harness,
        '_remove_local_tree',
        lambda *args, **kwargs: (_ for _ in ()).throw(PermissionError('outer cleanup failed')),
    )

    with pytest.raises(HarnessFailure) as caught:
        phase0_harness.capture_phase0a(request)

    assert caught.value.verdict is HarnessVerdict.CLEANUP_FAILED
    assert caught.value.primary_verdict is HarnessVerdict.REFERENCE_FAILED
    assert caught.value.raw_log_sha256 == 'f' * 64
    assert not request.output_path.exists()


@pytest.mark.parametrize(
    ('error', 'expected_raw_log'),
    (
        (RustNormalProcessError(9, 'f' * 64), 'f' * 64),
        (RustNormalValidationError('invalid workbook', 'e' * 64), 'e' * 64),
        (RuntimeError('unexpected capture failure'), None),
    ),
)
def test_phase0a_maps_capture_boundary_to_reference_failure(
    error: Exception,
    expected_raw_log: str | None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_phase0a_capture(monkeypatch, tmp_path)
    request = _phase0a_request(tmp_path)
    monkeypatch.setattr(phase0_harness, '_capture_phase0a_group', lambda **kwargs: (_ for _ in ()).throw(error))

    with pytest.raises(HarnessFailure) as caught:
        phase0_harness.capture_phase0a(request)

    assert caught.value.verdict is HarnessVerdict.REFERENCE_FAILED
    assert caught.value.raw_log_sha256 == expected_raw_log


def test_phase0a_rechecks_machine_evidence_after_each_capture(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    initial = MachineEvidence('build', 'x86_64', 'cpu', 4, 8, 'SSD', 16, '6' * 64)
    changed = replace(initial, fingerprint_sha256='9' * 64)
    observed = iter((initial, changed))
    input_path = tmp_path / 'synthetic.xlsx'
    executable = tmp_path / 'reference.exe'
    input_path.write_bytes(b'input')
    executable.write_bytes(b'executable')
    local_root = tmp_path / 'rust' / 'target' / 'perf-local' / 'phase0a'
    monkeypatch.setattr(phase0_harness, '_capture_machine_evidence', lambda: next(observed))
    monkeypatch.setattr(phase0_harness, '_run_git', lambda root, *args: '4' * 40)

    def fake_capture(
        executable: Path, pipeline: str, input_path: Path, output_path: Path, **kwargs: object
    ) -> CapturedNormalRun:
        del executable, input_path, kwargs
        _write_approved_test_workbook(output_path)
        return CapturedNormalRun(
            NormalRunEvidence(
                Decimal('1'),
                None,
                replace(_runtime(pipeline), sheet_dimensions=_APPROVED_TEST_DIMENSIONS),
                '8' * 64,
            ),
            0,
            '7' * 64,
        )

    monkeypatch.setattr(phase0_harness, 'run_rust_normal_captured', fake_capture)

    with pytest.raises(HarnessFailure) as caught:
        phase0_harness._capture_phase0a_group(
            pipeline='gb',
            metric='wall',
            input_path=input_path,
            executable=executable,
            local_root=local_root,
            machine=initial,
        )
    assert caught.value.verdict is HarnessVerdict.ENVIRONMENT_DRIFT
    assert not list(local_root.rglob('*.xlsx'))


def test_main_maps_smoke_and_phase0a_capture_failures_without_traceback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    executable = tmp_path / 'reference.exe'
    executable.write_bytes(b'executable')
    smoke_root = tmp_path / 'rust' / 'target' / 'perf-local' / 'smoke'
    monkeypatch.setattr(
        phase0_harness,
        'run_rust_normal_captured',
        lambda *args, **kwargs: (_ for _ in ()).throw(RustNormalProcessError(9, 'f' * 64)),
    )
    smoke_exit = phase0_harness.main(
        [
            'smoke',
            '--pipeline',
            'gb',
            '--reference-executable',
            str(executable),
            '--candidate-executable',
            str(executable),
            '--local-root',
            str(smoke_root),
        ]
    )
    assert smoke_exit == 3
    assert 'Traceback' not in capsys.readouterr().err

    _install_phase0a_capture(monkeypatch, tmp_path)
    request = _phase0a_request(tmp_path)
    monkeypatch.setattr(
        phase0_harness,
        '_capture_phase0a_group',
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError('unexpected capture failure')),
    )
    phase0a_exit = phase0_harness.main(
        [
            'phase0a',
            '--gb-input',
            str(request.gb_input_path),
            '--sk-input',
            str(request.sk_input_path),
            '--reference-executable',
            str(request.reference_executable),
            '--fork-revision',
            request.fork_revision,
            '--local-root',
            str(request.local_root),
            '--output',
            str(request.output_path),
        ]
    )
    assert phase0a_exit == 3
    assert 'Traceback' not in capsys.readouterr().err


def test_paired_cli_exposes_no_batch_round_or_threshold_argument() -> None:
    parser = phase0_harness._argument_parser()
    paired = next(action for action in parser._actions if action.dest == 'command').choices['paired']
    option_strings = {option for action in paired._actions for option in action.option_strings}
    assert option_strings == {
        '-h',
        '--help',
        '--pipeline',
        '--input',
        '--reference-executable',
        '--candidate-executable',
        '--reference-label',
        '--candidate-label',
        '--comparison-profile',
        '--phase0a-manifest',
        '--local-root',
        '--evidence-path',
    }


def test_paired_cli_uses_closed_labels_profiles_and_exit_codes() -> None:
    parser = phase0_harness._argument_parser()
    paired = next(action for action in parser._actions if action.dest == 'command').choices['paired']
    actions = {action.dest: action for action in paired._actions}
    assert set(actions['reference_label'].choices) == {item.value for item in ClosedBinaryLabel}
    assert set(actions['candidate_label'].choices) == {item.value for item in ClosedBinaryLabel}
    assert set(actions['comparison_profile'].choices) == {item.value for item in ComparisonProfile}
    assert phase0_harness._exit_code(HarnessVerdict.VALIDATED) == 0
    assert phase0_harness._exit_code(HarnessVerdict.CANDIDATE_FAILED) == 2
    assert phase0_harness._exit_code(HarnessVerdict.REFERENCE_FAILED) == 3
    assert phase0_harness._exit_code(HarnessVerdict.CLEANUP_FAILED) == 4
    assert phase0_harness.main(['paired']) == 5


@pytest.mark.parametrize('pipeline', ('gb', 'sk'))
def test_sanitized_raw_fixture_runs_both_python_and_rust_to_three_sheets(tmp_path: Path, pipeline: str) -> None:
    from openpyxl import load_workbook

    from tests.rust_oracle.evidence import ApprovedSheet
    from tests.rust_oracle.sanitized_fixture import build_raw_fixture

    fixture = tmp_path / f'synthetic-{pipeline}.xlsx'
    python_output = tmp_path / f'python-{pipeline}.xlsx'
    rust_output = tmp_path / f'rust-{pipeline}.xlsx'
    build_raw_fixture(fixture, pipeline, 'small')  # type: ignore[arg-type]
    run_python_oracle(pipeline, fixture, python_output)
    run_rust_cli_release(build_rust_cli_release(), pipeline, fixture, rust_output)

    expected = [item.value for item in ApprovedSheet]
    for output in (python_output, rust_output):
        workbook = load_workbook(output, read_only=True, data_only=True)
        try:
            assert workbook.sheetnames == expected
        finally:
            workbook.close()


def test_normal_wall_group_omits_check_only_and_uses_unique_outputs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    commands = _install_runner(monkeypatch, tmp_path)
    run_normal_wall_group(_group_request(tmp_path))
    paths = [command[3] for command in commands]
    assert len(paths) == len(set(paths)) == 10
    assert all('--check-only' not in command for command in commands)


def test_normal_wall_group_runs_global_ab_ba_order(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    commands = _install_runner(monkeypatch, tmp_path)
    run_normal_wall_group(_group_request(tmp_path))
    assert [item[0] for item in commands] == [
        'reference',
        'candidate',
        'candidate',
        'reference',
        'reference',
        'candidate',
        'candidate',
        'reference',
        'reference',
        'candidate',
    ]


def test_candidate_nonzero_rejects_candidate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_runner(monkeypatch, tmp_path, fail_role='candidate')
    with pytest.raises(HarnessFailure) as caught:
        run_normal_wall_group(_group_request(tmp_path))
    assert caught.value.verdict is HarnessVerdict.CANDIDATE_FAILED


def test_reference_nonzero_invalidates_whole_batch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_runner(monkeypatch, tmp_path, fail_role='reference')
    with pytest.raises(HarnessFailure) as caught:
        run_normal_wall_group(_group_request(tmp_path))
    assert caught.value.verdict is HarnessVerdict.REFERENCE_FAILED


@pytest.mark.parametrize('field', ('input_sha256', 'reference_sha256', 'candidate_sha256', 'git_head'))
def test_wall_group_rejects_input_reference_candidate_or_git_drift(
    field: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_runner(monkeypatch, tmp_path)
    values = [_identity(), replace(_identity(), **{field: 'drift'})]
    monkeypatch.setattr(
        phase0_harness,
        '_capture_identity',
        lambda request: values.pop(0) if values else replace(_identity(), **{field: 'drift'}),
    )
    with pytest.raises(HarnessFailure, match='drift'):
        run_normal_wall_group(_group_request(tmp_path))


def test_wall_group_deletes_workbooks_on_success(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_runner(monkeypatch, tmp_path)
    run_normal_wall_group(_group_request(tmp_path))
    assert not list((tmp_path / 'data').rglob('*.xlsx'))


def test_wall_group_deletes_workbooks_after_process_or_oracle_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_runner(monkeypatch, tmp_path, fail_role='candidate')
    with pytest.raises(HarnessFailure):
        run_normal_wall_group(_group_request(tmp_path))
    assert not list((tmp_path / 'data').rglob('*.xlsx'))


def test_cleanup_failure_prevents_versionable_evidence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_runner(monkeypatch, tmp_path)
    monkeypatch.setattr(
        Path,
        'unlink',
        lambda self, **kwargs: (_ for _ in ()).throw(PermissionError('locked')) if self.suffix == '.xlsx' else None,
    )
    with pytest.raises(HarnessFailure) as caught:
        run_normal_wall_group(_group_request(tmp_path))
    assert caught.value.verdict is HarnessVerdict.CLEANUP_FAILED
    assert not _request(tmp_path).evidence_path.exists()


def test_first_formal_batch_requires_clean_worktree(tmp_path: Path) -> None:
    with pytest.raises(HarnessFailure, match='clean'):
        validate_formal_repository_state((' M tests/file.py',), evidence_root=tmp_path, prior_evidence_claims=())


def test_formal_batch_rejects_non_evidence_worktree_change(tmp_path: Path) -> None:
    approved = tmp_path / 'docs' / 'prior.json'
    approved.parent.mkdir()
    approved.write_bytes(b'{}')
    claim = _claim(tmp_path, approved, phase0_harness._sha256(approved))
    with pytest.raises(HarnessFailure, match='non-evidence'):
        validate_formal_repository_state(
            ('?? src/new.py',),
            evidence_root=tmp_path / 'docs',
            prior_evidence_claims=(claim,),
            root=tmp_path,
        )


def test_later_batch_accepts_only_create_new_sanitized_prior_evidence(tmp_path: Path) -> None:
    evidence = tmp_path / 'docs' / 'prior.json'
    evidence.parent.mkdir()
    evidence.write_bytes(b'{"sanitized":true}')
    digest = phase0_harness._sha256(evidence)
    validate_formal_repository_state(
        ('?? docs/prior.json',),
        evidence_root=tmp_path / 'docs',
        prior_evidence_claims=(_claim(tmp_path, evidence, digest),),
        root=tmp_path,
    )


def test_prior_evidence_content_change_invalidates_repository_state(tmp_path: Path) -> None:
    evidence = tmp_path / 'docs' / 'prior.json'
    evidence.parent.mkdir()
    evidence.write_bytes(b'changed')
    with pytest.raises(HarnessFailure, match='content'):
        validate_formal_repository_state(
            ('?? docs/prior.json',),
            evidence_root=tmp_path / 'docs',
            prior_evidence_claims=(_claim(tmp_path, evidence, '0' * 64),),
            root=tmp_path,
        )


def test_batch_id_is_derived_and_cannot_be_supplied(tmp_path: Path) -> None:
    request = _request(tmp_path)
    batch_id = derive_batch_id(request, _identity())
    assert batch_id == derive_batch_id(request, _identity())
    assert len(batch_id) == 64
    assert 'batch_id' not in PairedBenchmarkRequest.__dataclass_fields__


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
    assert phase0_harness._batch_attempt(ledger, 'b' * 64, AttemptState.CREATED).protocol_version == 2


@pytest.mark.parametrize('invalid', (True, '2', 0, 3))
def test_ledger_rejects_unknown_or_non_integer_protocol_version(invalid: object, tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    _rewrite_metadata_and_matching_empty_journal(ledger.attempt_directory, protocol_version=invalid)
    with pytest.raises(HarnessFailure) as caught:
        AppendOnlyAttemptLedger.load(ledger.attempt_directory, _identity())
    assert caught.value.verdict is HarnessVerdict.INCOMPLETE_EVIDENCE


def test_v1_metadata_without_protocol_version_loads_read_only(tmp_path: Path) -> None:
    attempt = _write_synthetic_v1_terminal(tmp_path, comparison_key='a' * 64)
    comparison = attempt.parent
    before = _file_snapshot(comparison)
    loaded = AppendOnlyAttemptLedger.load(attempt, _identity(), strict_identity=False)
    assert loaded.protocol_version == 1
    assert loaded.terminal_verdict is HarnessVerdict.INCONCLUSIVE
    assert _file_snapshot(comparison) == before


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


def test_v2_create_refuses_to_append_synthetic_v1_comparison_directory(tmp_path: Path) -> None:
    attempt = _write_synthetic_v1_terminal(tmp_path, comparison_key='a' * 64)
    comparison = attempt.parent
    before = _file_snapshot(comparison)
    with pytest.raises(HarnessFailure, match='protocol v1') as caught:
        AppendOnlyAttemptLedger.create(
            tmp_path / 'rust' / 'target' / 'perf-local' / 'batches',
            _identity(),
            comparison_key='a' * 64,
        )
    assert caught.value.verdict is HarnessVerdict.INCOMPLETE_EVIDENCE
    assert _file_snapshot(comparison) == before


@pytest.mark.parametrize(
    ('missing_checkpoint', 'stale_journal'),
    ((True, False), (False, True)),
)
def test_v1_audit_recovery_edges_fail_without_writing(
    missing_checkpoint: bool,
    stale_journal: bool,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    attempt = _write_recoverable_v1_success(
        tmp_path,
        comparison_key='a' * 64,
        missing_checkpoint=missing_checkpoint,
        stale_journal=stale_journal,
    )
    comparison = attempt.parent
    before = _file_snapshot(comparison)
    monkeypatch.setattr(
        phase0_harness,
        '_validate_prepared_evidence',
        lambda record: {
            'artifact_basename': record['artifact_basename'],
            'artifact_sha256': record['artifact_sha256'],
            'artifact_content': record['artifact_content'],
        },
    )
    with pytest.raises(HarnessFailure, match='protocol v1') as caught:
        AppendOnlyAttemptLedger.load(attempt, _identity(), strict_identity=False)
    assert caught.value.verdict is HarnessVerdict.INCOMPLETE_EVIDENCE
    assert _file_snapshot(comparison) == before


def test_v2_create_does_not_repair_v1_before_rejecting(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    attempt = _write_recoverable_v1_success(
        tmp_path,
        comparison_key='a' * 64,
        missing_checkpoint=True,
        stale_journal=False,
    )
    comparison = attempt.parent
    before = _file_snapshot(comparison)
    monkeypatch.setattr(
        phase0_harness,
        '_validate_prepared_evidence',
        lambda record: {
            'artifact_basename': record['artifact_basename'],
            'artifact_sha256': record['artifact_sha256'],
            'artifact_content': record['artifact_content'],
        },
    )
    with pytest.raises(HarnessFailure, match='protocol v1'):
        AppendOnlyAttemptLedger.create(
            tmp_path / 'rust' / 'target' / 'perf-local' / 'batches',
            _identity(),
            comparison_key='a' * 64,
        )
    assert _file_snapshot(comparison) == before


@pytest.mark.parametrize('invalid', (True, '1', 1.0, 0, -1))
def test_attempt_metadata_rejects_non_exact_positive_integer_number(invalid: object, tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    _rewrite_metadata_and_matching_empty_journal(ledger.attempt_directory, attempt_number=invalid)
    with pytest.raises(HarnessFailure) as caught:
        AppendOnlyAttemptLedger.load(ledger.attempt_directory, _identity())
    assert caught.value.verdict is HarnessVerdict.INCOMPLETE_EVIDENCE


def test_attempt_metadata_number_must_match_directory_basename(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    renamed = ledger.attempt_directory.parent / 'attempt-0002'
    ledger.attempt_directory.rename(renamed)
    with pytest.raises(HarnessFailure) as caught:
        AppendOnlyAttemptLedger.load(renamed, _identity())
    assert caught.value.verdict is HarnessVerdict.INCOMPLETE_EVIDENCE


def test_attempt_metadata_comparison_key_must_match_parent_directory(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    _rewrite_metadata_and_matching_empty_journal(ledger.attempt_directory, comparison_key='b' * 64)
    with pytest.raises(HarnessFailure) as caught:
        AppendOnlyAttemptLedger.load(ledger.attempt_directory, _identity())
    assert caught.value.verdict is HarnessVerdict.INCOMPLETE_EVIDENCE


def test_current_runner_refuses_v1_attempt_as_resume(tmp_path: Path) -> None:
    attempt = _write_synthetic_v1_terminal(tmp_path, comparison_key='a' * 64)
    load_current = getattr(phase0_harness, '_load_current_protocol_ledger', None)
    assert callable(load_current)
    with pytest.raises(HarnessFailure, match='protocol'):
        load_current(attempt, _identity())


def test_second_round_one_to_five_attempt_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        MetricGroupRequest(
            _request(tmp_path),
            'b' * 32,
            'wall',
            build_round_plan(global_round_start=1, round_count=5),
            tmp_path / 'attempt',
            first_group_sha256='a' * 64,
        )


def test_existing_round_record_cannot_be_overwritten(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.record_sample('wall', 1, 'reference', {'value': 1})
    with pytest.raises(HarnessFailure, match='overwrite'):
        ledger.record_sample('wall', 1, 'reference', {'value': 2})


def test_expanded_group_requires_original_first_group_sha(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    first = ledger.commit_first_group({'wall': 'one', 'pws': 'two'})
    with pytest.raises(HarnessFailure, match='first group'):
        ledger.commit_expanded_group({'wall': 'three'}, first_group_sha256='0' * 64)
    ledger.commit_expanded_group({'wall': 'three', 'pws': 'four'}, first_group_sha256=first)


def test_pws_only_resample_is_rejected(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    with pytest.raises(HarnessFailure, match='wall and pws'):
        ledger.commit_first_group({'pws': 'only'})


@pytest.mark.parametrize(('wall_near_limit', 'pws_near_limit'), ((True, False), (False, True), (True, True)))
def test_paired_batch_expands_wall_and_pws_together(wall_near_limit: bool, pws_near_limit: bool) -> None:
    wall_plans, pws_plans = phase0_harness._mandatory_paired_expansion_plans(
        wall_requires_expansion=wall_near_limit,
        pws_requires_expansion=pws_near_limit,
    )
    assert [plan.global_round for plan in wall_plans] == [6, 7, 8, 9, 10]
    assert wall_plans == pws_plans


@pytest.mark.parametrize(
    ('profile', 'pipeline', 'stage', 'ratio'),
    (
        (ComparisonProfile.PHASE1_VS_PHASE0B, 'sk', 'writer_populate', Decimal('0.90')),
        (ComparisonProfile.PHASE2_B_VS_A, 'sk', 'xlsx_save', Decimal('0.85')),
        (ComparisonProfile.PHASE4_VS_PHASE3, 'gb', 'ingest', Decimal('1.05')),
    ),
)
def test_expansion_covers_each_stage_ratio_gate(
    profile: ComparisonProfile,
    pipeline: str,
    stage: str,
    ratio: Decimal,
    tmp_path: Path,
) -> None:
    request = replace(_request(tmp_path), comparison_profile=profile, pipeline=pipeline)
    wall = _with_candidate_stage_ratios(_metric_group('wall'), tuple({stage: ratio} for _ in range(5)))

    assert phase0_harness._paired_groups_require_expansion(request, wall, _metric_group('pws')) is True


def test_expansion_covers_ingest_or_pws_and_writer_export_minimum_wins(tmp_path: Path) -> None:
    ingest_request = replace(
        _request(tmp_path),
        comparison_profile=ComparisonProfile.PHASE4_VS_PHASE3,
        pipeline='sk',
    )
    ingest_wall = _with_candidate_stage_ratios(
        _metric_group('wall'),
        tuple({'ingest': Decimal('0.90')} for _ in range(5)),
    )
    assert phase0_harness._paired_groups_require_expansion(ingest_request, ingest_wall, _metric_group('pws')) is True

    wins_request = replace(
        _request(tmp_path),
        comparison_profile=ComparisonProfile.PHASE2_C_VS_A,
        pipeline='sk',
    )
    win_ratios = tuple(
        {'writer_populate': Decimal('0.50'), 'export': Decimal('1.50')}
        if index < 4
        else {'writer_populate': Decimal('1.50'), 'export': Decimal('1.50')}
        for index in range(5)
    )
    wins_wall = _with_candidate_stage_ratios(_metric_group('wall'), win_ratios)
    assert phase0_harness._paired_groups_require_expansion(wins_request, wins_wall, _metric_group('pws')) is True


@pytest.mark.parametrize(('wall_ratio', 'pws_ratio'), (('1.02', '1.20'), ('0.80', '0.98')))
def test_phase2_b_vs_c_expands_when_either_tie_break_ratio_is_within_three_percent_of_one(
    wall_ratio: str, pws_ratio: str, tmp_path: Path
) -> None:
    request = replace(
        _request(tmp_path),
        comparison_profile=ComparisonProfile.PHASE2_B_VS_C,
        pipeline='sk',
    )
    wall = _metric_group('wall', reference='1', candidate=wall_ratio)
    pws = _metric_group('pws', reference='1', candidate=pws_ratio)

    assert phase0_harness._paired_groups_require_expansion(request, wall, pws) is True


def test_writer_export_minimum_wins_requires_four_wins_in_each_five_round_window(tmp_path: Path) -> None:
    request = replace(
        _request(tmp_path),
        comparison_profile=ComparisonProfile.PHASE2_C_VS_A,
        pipeline='sk',
    )
    first_ratios = tuple(
        {'writer_populate': Decimal('0.50'), 'export': Decimal('1.50')}
        if index < 4
        else {'writer_populate': Decimal('1.50'), 'export': Decimal('1.50')}
        for index in range(5)
    )
    second_ratios = tuple({'writer_populate': Decimal('1.50'), 'export': Decimal('1.50')} for _ in range(5))
    first_wall = _with_candidate_stage_ratios(_metric_group('wall'), first_ratios)
    second_wall = _with_candidate_stage_ratios(_metric_group('wall', start=6), second_ratios)
    wall = phase0_harness.merge_metric_groups(first_wall, second_wall)
    pws = phase0_harness.merge_metric_groups(
        _metric_group('pws', reference='100', candidate='100'),
        _metric_group('pws', start=6, reference='100', candidate='100'),
    )
    baseline = phase0_harness._ApprovedPhase0ABaseline('6' * 64, 8, (Decimal('1'),) * 5, (Decimal('100'),) * 5)

    with pytest.raises(HarnessFailure) as caught:
        phase0_harness._evaluate_closed_profile(request, wall, pws, baseline)

    assert caught.value.verdict is HarnessVerdict.CANDIDATE_FAILED


@pytest.mark.parametrize(
    ('wall_sizes', 'pws_sizes', 'expected_verdict'),
    (
        ((100,) * 5, (101,) * 5, None),
        ((110,) * 5, (110,) * 5, None),
        ((111,) * 5, (111,) * 5, HarnessVerdict.ENVIRONMENT_DRIFT),
    ),
    ids=('volatile-within-limit', 'exactly-ten-percent', 'over-ten-percent'),
)
def test_paired_phase0a_output_drift_uses_rounded_median(
    wall_sizes: tuple[int, ...],
    pws_sizes: tuple[int, ...],
    expected_verdict: HarnessVerdict | None,
) -> None:
    wall = _with_output_sizes(
        _metric_group('wall'),
        reference_sizes=wall_sizes,
        candidate_sizes=wall_sizes,
    )
    pws = _with_output_sizes(
        _metric_group('pws', reference='100', candidate='100'),
        reference_sizes=pws_sizes,
        candidate_sizes=pws_sizes,
    )
    if expected_verdict is None:
        phase0_harness._validate_phase0a_drift(wall, pws, _phase0a_baseline(), _identity())
        return

    with pytest.raises(HarnessFailure) as caught:
        phase0_harness._validate_phase0a_drift(wall, pws, _phase0a_baseline(), _identity())

    assert caught.value.verdict is expected_verdict


@pytest.mark.parametrize(
    ('candidate_sizes', 'expected_verdict'),
    (
        ((100, 100, 100, 111, 111), None),
        ((100, 100, 111, 111, 111), HarnessVerdict.CANDIDATE_FAILED),
    ),
    ids=('raw-maximum-over-limit', 'median-over-limit'),
)
def test_closed_profile_candidate_output_gate_uses_median(
    candidate_sizes: tuple[int, ...],
    expected_verdict: HarnessVerdict | None,
    tmp_path: Path,
) -> None:
    wall = _with_output_sizes(
        _metric_group('wall'),
        reference_sizes=(100,) * 5,
        candidate_sizes=candidate_sizes,
    )
    pws = _with_output_sizes(
        _metric_group('pws', reference='100', candidate='100'),
        reference_sizes=(100,) * 5,
        candidate_sizes=candidate_sizes,
    )
    if expected_verdict is None:
        phase0_harness._evaluate_closed_profile(_request(tmp_path), wall, pws, _phase0a_baseline())
        return

    with pytest.raises(HarnessFailure) as caught:
        phase0_harness._evaluate_closed_profile(_request(tmp_path), wall, pws, _phase0a_baseline())

    assert caught.value.verdict is expected_verdict


def test_paired_evidence_serializes_median_rounded_output_bytes(tmp_path: Path) -> None:
    wall = _with_output_sizes(
        _metric_group('wall'),
        reference_sizes=(100,) * 5,
        candidate_sizes=(109,) * 5,
    )
    pws = _with_output_sizes(
        _metric_group('pws', reference='100', candidate='100'),
        reference_sizes=(101,) * 5,
        candidate_sizes=(110,) * 5,
    )
    attempt = BatchAttempt(
        protocol_version=2,
        comparison_key='comparison',
        batch_id=wall.batch_id,
        attempt_number=1,
        state=AttemptState.CLEANUP_COMPLETE,
        previous_attempt_head_sha256=None,
        first_group_sha256='a' * 64,
        expanded_group_sha256=None,
        ledger_head_sha256='b' * 64,
        attempt_directory=tmp_path,
    )

    evidence = phase0_harness._build_paired_evidence(_request(tmp_path), wall, pws, attempt, _identity())

    assert {item.role: item.value for item in evidence.output_bytes} == {'reference': 101, 'candidate': 110}


def test_loader_repairs_only_tail_committed_record_missing_checkpoint(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _request, ledger, artifact = _prepare_formal_evidence_recovery(monkeypatch, tmp_path)
    original_write = phase0_harness._write_create_new
    failed = False

    def fail_checkpoint(path: Path, content: bytes, *, allowed_root: Path) -> None:
        nonlocal failed
        if path.parent.name == 'checkpoints' and not failed:
            failed = True
            raise OSError('checkpoint interrupted')
        original_write(path, content, allowed_root=allowed_root)

    monkeypatch.setattr(phase0_harness, '_write_create_new', fail_checkpoint)
    with pytest.raises(OSError, match='checkpoint interrupted'):
        ledger.mark_evidence_committed(artifact_sha256=hashlib.sha256(artifact.content.encode()).hexdigest())

    recovered = AppendOnlyAttemptLedger.load(ledger.attempt_directory, ledger.identity)
    assert recovered.state is AttemptState.EVIDENCE_COMMITTED
    assert len(tuple((ledger.attempt_directory / 'records').glob('*.json'))) == len(
        tuple((ledger.attempt_directory / 'checkpoints').glob('*.json'))
    )


def test_loader_repairs_only_missing_journal_anchor_after_committed_checkpoint(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _request, ledger, artifact = _prepare_formal_evidence_recovery(monkeypatch, tmp_path)
    failed = False
    original_anchor = ledger._append_journal_anchor

    def fail_anchor() -> str:
        nonlocal failed
        if not failed:
            failed = True
            raise OSError('journal interrupted')
        return original_anchor()

    monkeypatch.setattr(ledger, '_append_journal_anchor', fail_anchor)
    with pytest.raises(OSError, match='journal interrupted'):
        ledger.mark_evidence_committed(artifact_sha256=hashlib.sha256(artifact.content.encode()).hexdigest())

    recovered = AppendOnlyAttemptLedger.load(ledger.attempt_directory, ledger.identity)
    assert recovered.state is AttemptState.EVIDENCE_COMMITTED


def _approved_phase0a_payload(*, wall_value: str = '1', pws_value: str = '100') -> dict[str, object]:
    calibration = {
        'wall': {'batch_id_sha256': 'a' * 64, 'values': [wall_value] * 5, 'local_log_sha256': ['7' * 64] * 5},
        'pws': {'batch_id_sha256': 'b' * 64, 'values': [pws_value] * 5, 'local_log_sha256': ['7' * 64] * 5},
    }
    pipeline = {
        'input_alias': '$GB_INPUT',
        'input_sha256': '3' * 64,
        'output_size_bytes': 8,
        'sheet_dimensions': ['1x1', '1x1', '1x1'],
        'runtime': {'sheet_count': 3, 'error_log_count': 0, 'run_counts': {}, 'stage_timings': {}},
        'calibration': calibration,
    }
    return {
        'schema_version': 1,
        'approval_state': 'APPROVED',
        'reference_exe_sha256': '1' * 64,
        'fork_revision': 'a' * 40,
        'git_head': '4' * 40,
        'machine': {
            'windows_build_sha256': '8' * 64,
            'architecture': 'x86_64',
            'cpu_model_sha256': '9' * 64,
            'logical_cpu_count': 4,
            'physical_memory_bytes': 8,
            'system_drive_media_type': 'SSD',
            'system_drive_size_bytes': 16,
            'fingerprint_sha256': '6' * 64,
        },
        'pipelines': {'gb': pipeline, 'sk': {**pipeline, 'input_alias': '$SK_INPUT'}},
    }


def _install_formal_paired(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    wall_reference: str = '1',
    wall_candidate: str = '1',
) -> tuple[PairedBenchmarkRequest, list[dict[str, object]]]:
    request = _request(tmp_path)
    evidence_key = hashlib.sha256(
        f'{request.comparison_profile.value}|{request.pipeline}|{"2" * 64}'.encode()
    ).hexdigest()[:16]
    request = replace(request, evidence_path=request.evidence_path.parent / f'benchmark-{evidence_key}.json')
    request.phase0a_manifest.write_text(json.dumps(_approved_phase0a_payload()), encoding='utf-8')
    identity = BenchmarkIdentity('3' * 64, '1' * 64, '2' * 64, '4' * 40, '5' * 64, '6' * 64)
    monkeypatch.setattr(phase0_harness, '_capture_identity', lambda benchmark: identity)
    monkeypatch.setattr(phase0_harness, '_run_git', lambda root, *args: '')

    def wall(group_request: MetricGroupRequest) -> MetricGroup:
        group = _metric_group(
            'wall',
            start=group_request.plans[0].global_round,
            reference=wall_reference,
            candidate=wall_candidate,
        )
        return replace(group, batch_id=group_request.batch_id)

    def pws(group_request: MetricGroupRequest) -> MetricGroup:
        group = _metric_group('pws', start=group_request.plans[0].global_round, reference='100', candidate='100')
        return replace(group, batch_id=group_request.batch_id)

    publications: list[dict[str, object]] = []
    monkeypatch.setattr(phase0_harness, 'run_normal_wall_group', wall)
    monkeypatch.setattr(phase0_harness, 'run_pws_group', pws)
    monkeypatch.setattr(
        EvidenceSanitizer,
        'write_batch',
        lambda self, **kwargs: publications.append(kwargs),
    )
    return request, publications


def _prepare_formal_evidence_recovery(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> tuple[PairedBenchmarkRequest, AppendOnlyAttemptLedger, object]:
    request, _ = _install_formal_paired(monkeypatch, tmp_path)
    sanitized_input = tmp_path / 'confidential-gb-source.xlsx'
    sanitized_input.write_bytes(b'input')
    request = replace(request, input_path=sanitized_input)
    monkeypatch.setattr(EvidenceSanitizer, 'scan_staged', lambda self, **kwargs: None)
    identity = BenchmarkIdentity('3' * 64, '1' * 64, '2' * 64, '4' * 40, '5' * 64, '6' * 64)
    comparison_key = phase0_harness.derive_comparison_key(
        protocol_version=phase0_harness.PAIRED_PROTOCOL_VERSION,
        pipeline=request.pipeline,
        comparison_profile=request.comparison_profile,
        reference_label=request.reference_label,
        candidate_label=request.candidate_label,
        input_sha256=identity.input_sha256,
        reference_sha256=identity.reference_sha256,
        candidate_sha256=identity.candidate_sha256,
    )
    ledger = AppendOnlyAttemptLedger.create(request.attempt_ledger_root, identity, comparison_key=comparison_key)
    ledger.commit_first_group({'wall': 'wall', 'pws': 'pws'})
    ledger.mark_cleanup_complete()
    batch_id = derive_batch_id(request, identity)
    wall = replace(_metric_group('wall'), batch_id=batch_id)
    pws = replace(_metric_group('pws', reference='100', candidate='100'), batch_id=batch_id)
    attempt = phase0_harness._batch_attempt(ledger, batch_id, AttemptState.CLEANUP_COMPLETE)
    evidence = phase0_harness._build_paired_evidence(request, wall, pws, attempt, identity)
    artifact = EvidenceSanitizer.closed_policy().build_benchmark_manifest(evidence)
    ledger.prepare_evidence(
        artifact_basename=artifact.file_name,
        artifact_sha256=hashlib.sha256(artifact.content.encode('utf-8')).hexdigest(),
        artifact_content=artifact.content,
    )
    return request, ledger, artifact


def test_prepared_evidence_without_files_is_republished_before_any_new_sample(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    original_write_batch = EvidenceSanitizer.write_batch
    request, ledger, _artifact = _prepare_formal_evidence_recovery(monkeypatch, tmp_path)
    publications = 0

    def publish(self: EvidenceSanitizer, **kwargs: object) -> None:
        nonlocal publications
        publications += 1
        original_write_batch(self, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(EvidenceSanitizer, 'write_batch', publish)
    monkeypatch.setattr(
        phase0_harness,
        'run_normal_wall_group',
        lambda request: (_ for _ in ()).throw(AssertionError('sample runner must not run during recovery')),
    )
    monkeypatch.setattr(
        phase0_harness,
        'run_pws_group',
        lambda request: (_ for _ in ()).throw(AssertionError('sample runner must not run during recovery')),
    )

    result = phase0_harness.run_paired_normal_batch(request)

    assert publications == 1
    assert result.wall is None and result.pws is None
    assert result.verdict is HarnessVerdict.VALIDATED
    assert (
        AppendOnlyAttemptLedger.load(ledger.attempt_directory, ledger.identity).state is AttemptState.EVIDENCE_COMMITTED
    )


def test_prepared_evidence_with_matching_artifact_and_marker_only_completes_ledger(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    original_write_batch = EvidenceSanitizer.write_batch
    request, ledger, artifact = _prepare_formal_evidence_recovery(monkeypatch, tmp_path)
    original_write_batch(
        EvidenceSanitizer.closed_policy(),
        destination_root=request.evidence_path.parent,
        artifacts=(artifact,),
        cleanup_state=AttemptState.CLEANUP_COMPLETE,
        scan_staged=False,
        sensitive_names=(),
    )
    monkeypatch.setattr(
        EvidenceSanitizer,
        'write_batch',
        lambda self, **kwargs: (_ for _ in ()).throw(AssertionError('matching evidence must not be republished')),
    )
    monkeypatch.setattr(
        phase0_harness,
        'run_normal_wall_group',
        lambda request: (_ for _ in ()).throw(AssertionError('sample runner must not run during recovery')),
    )

    result = phase0_harness.run_paired_normal_batch(request)

    assert result.wall is None and result.pws is None
    assert (
        AppendOnlyAttemptLedger.load(ledger.attempt_directory, ledger.identity).state is AttemptState.EVIDENCE_COMMITTED
    )


@pytest.mark.parametrize('tampered_file', ('artifact', 'marker'))
def test_prepared_evidence_mismatch_fails_closed_without_deleting_or_overwriting(
    tampered_file: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    original_write_batch = EvidenceSanitizer.write_batch
    request, _ledger, artifact = _prepare_formal_evidence_recovery(monkeypatch, tmp_path)
    original_write_batch(
        EvidenceSanitizer.closed_policy(),
        destination_root=request.evidence_path.parent,
        artifacts=(artifact,),
        cleanup_state=AttemptState.CLEANUP_COMPLETE,
        scan_staged=False,
        sensitive_names=(),
    )
    marker_name, _marker_content = _batch_commit_marker((artifact,))
    tampered_path = request.evidence_path if tampered_file == 'artifact' else request.evidence_path.parent / marker_name
    tampered_path.write_bytes(b'tampered')
    before = tampered_path.read_bytes()
    monkeypatch.setattr(
        EvidenceSanitizer,
        'write_batch',
        lambda self, **kwargs: (_ for _ in ()).throw(AssertionError('mismatch must not be republished')),
    )

    with pytest.raises(HarnessFailure) as caught:
        phase0_harness.run_paired_normal_batch(request)

    assert caught.value.verdict is HarnessVerdict.INCOMPLETE_EVIDENCE
    assert tampered_path.read_bytes() == before


def test_paired_batch_rejects_phase0a_drift_before_publication(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    request, publications = _install_formal_paired(monkeypatch, tmp_path)
    payload = _approved_phase0a_payload(wall_value='2')
    request.phase0a_manifest.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(HarnessFailure) as caught:
        phase0_harness.run_paired_normal_batch(request)
    assert caught.value.verdict is HarnessVerdict.ENVIRONMENT_DRIFT
    assert publications == []


def test_paired_batch_rejects_closed_profile_gate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    request, publications = _install_formal_paired(monkeypatch, tmp_path, wall_candidate='1.10')

    with pytest.raises(HarnessFailure) as caught:
        phase0_harness.run_paired_normal_batch(request)
    assert caught.value.verdict is HarnessVerdict.CANDIDATE_FAILED
    assert publications == []


def test_paired_batch_publishes_typed_evidence_after_cleanup_and_keeps_batch_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    request, publications = _install_formal_paired(monkeypatch, tmp_path)

    result = phase0_harness.run_paired_normal_batch(request)

    assert result.wall is not None and result.pws is not None
    assert result.wall.batch_id == result.pws.batch_id == result.attempt.batch_id
    assert result.attempt.state is AttemptState.EVIDENCE_COMMITTED
    assert (
        AppendOnlyAttemptLedger.load(
            result.attempt.attempt_directory,
            BenchmarkIdentity('3' * 64, '1' * 64, '2' * 64, '4' * 40, '5' * 64, '6' * 64),
        ).state
        is AttemptState.EVIDENCE_COMMITTED
    )
    assert len(publications) == 1
    assert publications[0]['cleanup_state'] is AttemptState.CLEANUP_COMPLETE
    artifacts = publications[0]['artifacts']
    assert isinstance(artifacts, tuple) and isinstance(artifacts[0].source, BenchmarkManifestEvidence)


def test_paired_batch_reloads_ledger_after_metric_runners_append_records(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    request, _ = _install_formal_paired(monkeypatch, tmp_path)
    identity = BenchmarkIdentity('3' * 64, '1' * 64, '2' * 64, '4' * 40, '5' * 64, '6' * 64)
    wall_runner = phase0_harness.run_normal_wall_group
    pws_runner = phase0_harness.run_pws_group

    def wall(group_request: MetricGroupRequest) -> MetricGroup:
        ledger = AppendOnlyAttemptLedger.load(group_request.attempt_directory, identity)
        ledger.record_sample('wall', group_request.plans[0].global_round, 'reference', {'synthetic': 'wall'})
        return wall_runner(group_request)

    def pws(group_request: MetricGroupRequest) -> MetricGroup:
        ledger = AppendOnlyAttemptLedger.load(group_request.attempt_directory, identity)
        ledger.record_sample('pws', group_request.plans[0].global_round, 'reference', {'synthetic': 'pws'})
        return pws_runner(group_request)

    monkeypatch.setattr(phase0_harness, 'run_normal_wall_group', wall)
    monkeypatch.setattr(phase0_harness, 'run_pws_group', pws)

    result = phase0_harness.run_paired_normal_batch(request)
    assert result.verdict is HarnessVerdict.VALIDATED


def test_paired_batch_rejects_evidence_basename_that_differs_from_typed_artifact(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    request, publications = _install_formal_paired(monkeypatch, tmp_path)
    request = replace(request, evidence_path=request.evidence_path.with_name('caller-chosen.json'))

    with pytest.raises(HarnessFailure) as caught:
        phase0_harness.run_paired_normal_batch(request)
    assert caught.value.verdict is HarnessVerdict.INCOMPLETE_EVIDENCE
    assert publications == []


def test_paired_batch_rejects_labels_that_do_not_match_closed_profile(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    request, publications = _install_formal_paired(monkeypatch, tmp_path)
    request = replace(request, candidate_label=ClosedBinaryLabel.PHASE1)

    with pytest.raises(HarnessFailure) as caught:
        phase0_harness.run_paired_normal_batch(request)
    assert caught.value.verdict is HarnessVerdict.INCOMPLETE_EVIDENCE
    assert publications == []


@pytest.mark.parametrize('mutation', ('extra', 'duplicate'))
def test_phase0a_loader_rejects_extra_or_duplicate_keys(
    mutation: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    request, _ = _install_formal_paired(monkeypatch, tmp_path)
    raw = json.dumps(_approved_phase0a_payload(), separators=(',', ':'))
    if mutation == 'extra':
        raw = raw[:-1] + ',"unexpected":1}'
    else:
        raw = raw[:-1] + ',"schema_version":1}'
    request.phase0a_manifest.write_text(raw, encoding='utf-8')

    with pytest.raises(HarnessFailure) as caught:
        phase0_harness.run_paired_normal_batch(request)
    assert caught.value.verdict is HarnessVerdict.INCOMPLETE_EVIDENCE


def test_wall_and_pws_groups_share_attempt_batch_and_n(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_runner(monkeypatch, tmp_path)
    wall = run_normal_wall_group(_group_request(tmp_path))
    pws = replace(wall, metric='pws')
    assert_same_benchmark_batch(wall, pws)


def test_interrupted_attempt_resumes_only_missing_samples(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    first_commands = _install_runner(monkeypatch, tmp_path, interrupt_role='candidate')
    with pytest.raises(KeyboardInterrupt):
        run_normal_wall_group(_group_request(tmp_path))
    assert [item[0] for item in first_commands] == ['reference', 'candidate']

    resumed_commands = _install_runner(monkeypatch, tmp_path)
    run_normal_wall_group(_group_request(tmp_path))
    assert resumed_commands[0][0] == 'candidate'
    assert len(resumed_commands) == 9


def test_failed_candidate_sha_cannot_be_retried(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.finish(HarnessVerdict.CANDIDATE_FAILED)
    with pytest.raises(HarnessFailure, match='candidate SHA'):
        AppendOnlyAttemptLedger.create(
            tmp_path / 'rust' / 'target' / 'perf-local' / 'batches',
            _identity(),
            comparison_key='a' * 64,
        )


def test_environment_recovery_attempt_links_previous_ledger_head(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.finish(HarnessVerdict.ENVIRONMENT_DRIFT)
    recovered = AppendOnlyAttemptLedger.create(
        tmp_path / 'rust' / 'target' / 'perf-local' / 'batches',
        _identity(),
        comparison_key='a' * 64,
    )
    assert recovered.previous_attempt_head_sha256 == ledger.head_sha256


def test_terminal_verdict_is_unique_and_cannot_change_retry_eligibility(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.finish(HarnessVerdict.CANDIDATE_FAILED, raw_log_sha256='d' * 64)
    assert ledger.state is AttemptState.FAILED
    assert ledger.terminal_verdict is HarnessVerdict.CANDIDATE_FAILED
    with pytest.raises(HarnessFailure, match='terminal'):
        ledger.finish(HarnessVerdict.ENVIRONMENT_DRIFT)
    with pytest.raises(HarnessFailure, match='terminal'):
        ledger.record_sample('wall', 2, 'reference', {'value': 2})


def test_candidate_failure_is_terminal_before_wall_runner_raises(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request = _group_request(tmp_path)
    _install_runner(monkeypatch, tmp_path, fail_role='candidate')
    with pytest.raises(HarnessFailure) as caught:
        run_normal_wall_group(request)
    assert caught.value.verdict is HarnessVerdict.CANDIDATE_FAILED
    loaded = AppendOnlyAttemptLedger.load(request.attempt_directory, _identity())
    assert loaded.terminal_verdict is HarnessVerdict.CANDIDATE_FAILED
    assert loaded.terminal_raw_log_sha256 == 'f' * 64


def test_correctness_failure_is_terminal_with_raw_log_sha(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request = _group_request(tmp_path)
    _install_runner(monkeypatch, tmp_path, validation_fail_role='candidate')
    with pytest.raises(HarnessFailure) as caught:
        run_normal_wall_group(request)
    assert caught.value.verdict is HarnessVerdict.CORRECTNESS_FAILED
    loaded = AppendOnlyAttemptLedger.load(request.attempt_directory, _identity())
    assert loaded.terminal_verdict is HarnessVerdict.CORRECTNESS_FAILED
    assert loaded.terminal_raw_log_sha256 == 'e' * 64


def test_capture_boundary_exception_maps_to_closed_terminal_verdict(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request = _group_request(tmp_path)
    _install_runner(monkeypatch, tmp_path, exception_role='candidate')
    with pytest.raises(HarnessFailure) as caught:
        run_normal_wall_group(request)
    assert caught.value.verdict is HarnessVerdict.CORRECTNESS_FAILED
    assert isinstance(caught.value.__cause__, RuntimeError)
    assert (
        AppendOnlyAttemptLedger.load(request.attempt_directory, _identity()).terminal_verdict
        is HarnessVerdict.CORRECTNESS_FAILED
    )


def test_sealed_attempt_rejects_deleted_tail_sample(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.record_sample('wall', 1, 'candidate', {'value': 1})
    ledger.finish(HarnessVerdict.CANDIDATE_FAILED)
    sample_record = next((ledger.attempt_directory / 'records').glob('*-sample.json'))
    sample_record.unlink()
    with pytest.raises(HarnessFailure, match='checkpoint|sealed|record count'):
        AppendOnlyAttemptLedger.load(ledger.attempt_directory, _identity())


def test_comparison_journal_rejects_deleted_terminal(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.finish(HarnessVerdict.CANDIDATE_FAILED)
    (ledger.attempt_directory / 'terminal.json').unlink()
    with pytest.raises(HarnessFailure, match='journal'):
        AppendOnlyAttemptLedger.load(ledger.attempt_directory, _identity())


def test_comparison_journal_rejects_tail_record_and_checkpoint_rollback(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.record_sample('wall', 1, 'candidate', {'value': 1})
    next((ledger.attempt_directory / 'records').glob('*-sample.json')).unlink()
    next((ledger.attempt_directory / 'checkpoints').glob('*.json')).unlink()
    with pytest.raises(HarnessFailure, match='journal'):
        AppendOnlyAttemptLedger.load(ledger.attempt_directory, _identity())


def test_unsealed_attempt_does_not_turn_deleted_candidate_sample_into_missing(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.record_sample('wall', 1, 'candidate', {'value': 1})
    sample_record = next((ledger.attempt_directory / 'records').glob('*-sample.json'))
    sample_record.unlink()
    with pytest.raises(HarnessFailure, match='checkpoint'):
        AppendOnlyAttemptLedger.load(ledger.attempt_directory, _identity())


def test_transient_cleanup_failure_is_closed_and_outer_retry_removes_workbook(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_runner(monkeypatch, tmp_path)
    real_remove = phase0_harness._remove_workbook
    failures_left = 1

    def transient(path: Path) -> None:
        nonlocal failures_left
        if failures_left:
            failures_left -= 1
            raise PermissionError('transient lock')
        real_remove(path)

    monkeypatch.setattr(phase0_harness, '_remove_workbook', transient)
    with pytest.raises(HarnessFailure) as caught:
        run_normal_wall_group(_group_request(tmp_path))
    assert caught.value.verdict is HarnessVerdict.ENVIRONMENT_DRIFT
    assert not list((tmp_path / 'data').rglob('*.xlsx'))


def test_persistent_cleanup_failure_preserves_primary_verdict(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_runner(monkeypatch, tmp_path, fail_role='candidate')
    real_remove = phase0_harness._remove_workbook
    calls = 0

    def fail_after_reference(path: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            real_remove(path)
            return
        raise PermissionError('persistent lock')

    monkeypatch.setattr(phase0_harness, '_remove_workbook', fail_after_reference)
    with pytest.raises(HarnessFailure) as caught:
        run_normal_wall_group(_group_request(tmp_path))
    assert caught.value.verdict is HarnessVerdict.CLEANUP_FAILED
    assert caught.value.primary_verdict is HarnessVerdict.CANDIDATE_FAILED


def test_cleanup_recovery_attempt_cleans_historical_planned_outputs_before_resume(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    ledger.record_planned_output(
        'wall',
        1,
        'reference',
        {
            'pipeline': 'gb',
            'batch_id': 'b' * 32,
            'metric': 'wall',
            'binary_sha256': '1' * 64,
            'global_round': 1,
            'role': 'reference',
            'relative_path': 'gb/.perf-runs/' + '/'.join(('b' * 32, 'wall', '1' * 64, '1', 'reference.xlsx')),
        },
    )
    monkeypatch.setattr(phase0_harness, 'repo_root', lambda: tmp_path)
    historical = phase0_harness._planned_paths(ledger.all_planned_output_payloads())[0]
    historical.write_bytes(b'partial')
    ledger.finish(HarnessVerdict.CLEANUP_FAILED, primary_verdict=HarnessVerdict.CANDIDATE_FAILED)
    recovered = AppendOnlyAttemptLedger.create(
        tmp_path / 'rust' / 'target' / 'perf-local' / 'batches',
        _identity(),
        comparison_key='a' * 64,
    )
    assert recovered.all_planned_output_payloads()
    assert recovered.cleanup_only is True
    commands = _install_runner(monkeypatch, tmp_path)
    request = MetricGroupRequest(
        _request(tmp_path),
        'b' * 8,
        'wall',
        build_round_plan(global_round_start=1, round_count=5),
        recovered.attempt_directory,
    )
    with pytest.raises(HarnessFailure) as caught:
        run_normal_wall_group(request)
    assert caught.value.verdict is HarnessVerdict.CANDIDATE_FAILED
    assert commands == []
    assert not historical.exists()
    with pytest.raises(HarnessFailure, match='candidate SHA'):
        AppendOnlyAttemptLedger.create(
            tmp_path / 'rust' / 'target' / 'perf-local' / 'batches',
            _identity(),
            comparison_key='a' * 64,
        )


def test_cleanup_only_ledger_rejects_benchmark_records_but_can_finish(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.cleanup_only = True

    with pytest.raises(HarnessFailure, match='cleanup-only'):
        ledger.record_sample('wall', 1, 'reference', {'value': 1})

    ledger.finish(HarnessVerdict.ENVIRONMENT_DRIFT)
    assert ledger.terminal_verdict is HarnessVerdict.ENVIRONMENT_DRIFT


def test_success_ledger_rejects_benchmark_records_after_cleanup(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.commit_first_group({'wall': {}, 'pws': {}})
    ledger.mark_cleanup_complete()

    with pytest.raises(HarnessFailure, match='cleanup|success'):
        ledger.record_sample('wall', 1, 'reference', {'value': 1})

    with pytest.raises(HarnessFailure, match='evidence success'):
        ledger.mark_evidence_committed(artifact_sha256='8' * 64)
    assert AppendOnlyAttemptLedger.load(ledger.attempt_directory, _identity()).state is AttemptState.CLEANUP_COMPLETE


@pytest.mark.parametrize('field', ('local_root', 'attempt_ledger_root'))
def test_wall_runner_requires_repository_trusted_local_roots(
    field: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    benchmark = replace(_request(tmp_path), **{field: tmp_path / 'docs' / 'performance'})
    request = MetricGroupRequest(
        benchmark,
        'b' * 8,
        'wall',
        build_round_plan(global_round_start=1, round_count=5),
        ledger.attempt_directory,
    )
    commands = _install_runner(monkeypatch, tmp_path)
    with pytest.raises(HarnessFailure, match='trusted'):
        run_normal_wall_group(request)
    assert commands == []


@pytest.mark.parametrize('comparison_key', ('../escape', 'a/b', 'C:/absolute', 'A' * 64, 'a' * 63))
def test_attempt_ledger_rejects_non_closed_comparison_key(comparison_key: str, tmp_path: Path) -> None:
    local_root = tmp_path / 'rust' / 'target' / 'perf-local'
    with pytest.raises(HarnessFailure, match='comparison_key'):
        AppendOnlyAttemptLedger.create(
            local_root / 'batches',
            _identity(),
            comparison_key=comparison_key,
        )


def test_attempt_ledger_rejects_root_outside_local_root(tmp_path: Path) -> None:
    with pytest.raises(HarnessFailure, match='local root'):
        AppendOnlyAttemptLedger.create(
            tmp_path / 'outside',
            _identity(),
            comparison_key='a' * 64,
        )


def test_attempt_ledger_rejects_raw_symlink_component(tmp_path: Path) -> None:
    local_root = tmp_path / 'rust' / 'target' / 'perf-local'
    local_root.mkdir(parents=True)
    outside = tmp_path / 'outside'
    outside.mkdir()
    linked = local_root / 'batches'
    try:
        linked.symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f'symlink creation is unavailable: {exc}')
    with pytest.raises(HarnessFailure, match='reparse|symlink'):
        AppendOnlyAttemptLedger.create(
            linked,
            _identity(),
            comparison_key='a' * 64,
        )


def test_prior_evidence_claim_content_participates_in_identity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request = _request(tmp_path)
    evidence = tmp_path / 'docs' / 'performance' / 'prior.json'
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.write_bytes(b'first')
    claim = _claim(tmp_path, evidence, phase0_harness._sha256(evidence))
    request = replace(request, prior_evidence_claims=(claim,))
    monkeypatch.setattr(phase0_harness, 'repo_root', lambda: tmp_path)
    monkeypatch.setattr(phase0_harness, '_run_git', lambda root, *args: 'head' if args[0] == 'rev-parse' else '')

    first = phase0_harness._capture_identity(request)
    evidence.write_bytes(b'second')
    with pytest.raises(HarnessFailure, match='prior evidence'):
        phase0_harness._capture_identity(request)
    assert first.repository_state_sha256

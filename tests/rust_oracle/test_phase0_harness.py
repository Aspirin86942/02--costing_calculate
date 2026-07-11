from __future__ import annotations

import getpass
import hashlib
import importlib.util
import json
import socket
import zipfile
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from tests.rust_oracle import phase0_harness
from tests.rust_oracle.benchmark_protocol import (
    AttemptState,
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


def test_phase0a_manifest_uses_external_output_size_for_base_reference(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_phase0a_capture(monkeypatch, tmp_path)
    request = _phase0a_request(tmp_path)

    phase0_harness.capture_phase0a(request)

    payload = json.loads(request.output_path.read_text(encoding='utf-8'))
    assert payload['pipelines']['gb']['output_size_bytes'] == 321
    assert payload['pipelines']['sk']['output_size_bytes'] == 654


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
    monkeypatch.setattr(phase0_harness, '_run_git', lambda root, *args: '4' * 40)

    def fake_pws(**kwargs: object) -> CapturedNormalRun:
        assert kwargs['local_root'] == phase0_harness._trusted_local_root()
        assert phase0_harness._is_sha256(kwargs['batch_id'])
        output = kwargs['output_path']
        assert isinstance(output, Path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b'workbook')
        return CapturedNormalRun(
            NormalRunEvidence(Decimal('1'), 123, _runtime('gb'), '8' * 64),
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
        machine=MachineEvidence('build', 'x86_64', 'cpu', 1, 1, 'UNKNOWN', 1, '6' * 64),
    )
    assert len(group.rounds) == 5


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

    ledger.mark_evidence_committed(artifact_sha256='8' * 64)
    assert AppendOnlyAttemptLedger.load(ledger.attempt_directory, _identity()).state is AttemptState.EVIDENCE_COMMITTED


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

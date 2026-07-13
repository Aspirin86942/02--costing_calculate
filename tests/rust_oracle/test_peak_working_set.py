from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import zipfile
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

from tests.rust_oracle import oracle_runner, phase0_harness
from tests.rust_oracle._winpath import win_long_paths_enabled
from tests.rust_oracle.benchmark_protocol import (
    CalibrationGroup,
    CalibrationRound,
    ClosedBinaryLabel,
    ComparisonProfile,
    HarnessVerdict,
    MachineEvidence,
    MetricSample,
    NormalRunEvidence,
    Phase0AManifest,
    RoundPlan,
    RuntimeEvidence,
    RuntimeSchema,
    approved_phase0a_output_bytes,
    build_round_plan,
)
from tests.rust_oracle.oracle_runner import CapturedNormalRun, RustNormalProcessError, RustNormalValidationError
from tests.rust_oracle.phase0_harness import (
    AppendOnlyAttemptLedger,
    BenchmarkIdentity,
    HarnessFailure,
    MetricGroupRequest,
    PairedBenchmarkRequest,
    build_pws_cli_arguments,
    run_pws_group,
)

pytestmark = pytest.mark.meta


@pytest.fixture(autouse=True)
def _trusted_repo_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(phase0_harness, 'repo_root', lambda: tmp_path)
    monkeypatch.setattr(oracle_runner, 'repo_root', lambda: tmp_path)


def _runtime(pipeline: str = 'gb', *, output_size_bytes: int | None = None) -> RuntimeEvidence:
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
        output_size_bytes=output_size_bytes,
        sheet_dimensions=_APPROVED_TEST_DIMENSIONS,
        reader_snapshot_sha256='',
    )


_APPROVED_TEST_DIMENSIONS = ('A1:A1', 'A1:A1', 'A1:A1')


def _write_approved_test_workbook(path: Path) -> None:
    from openpyxl import Workbook

    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = Workbook()
    try:
        for index, sheet in enumerate(phase0_harness.ApprovedSheet):
            worksheet = workbook.active if index == 0 else workbook.create_sheet()
            worksheet.title = sheet.value
            worksheet['A1'] = 'value'
        workbook.save(path)
    finally:
        workbook.close()


def _write_minimal_xlsx(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, 'w') as archive:
        archive.writestr('[Content_Types].xml', '<Types/>')
        archive.writestr('_rels/.rels', '<Relationships/>')
        archive.writestr('xl/workbook.xml', '<workbook/>')
        archive.writestr('xl/worksheets/sheet1.xml', '<worksheet/>')


def _long_output_path(tmp_path: Path) -> Path:
    path = tmp_path / 'data' / 'processed' / ('a' * 120) / ('b' * 120) / 'pws-output.xlsx'
    resolved = path.resolve()
    assert len(str(resolved)) > 260
    assert not str(resolved).startswith('\\\\?\\')
    return resolved


def _remove_long_output_tree(tmp_path: Path) -> None:
    data_io = phase0_harness._io_path(tmp_path / 'data')
    if data_io.exists():
        shutil.rmtree(data_io)


def _runtime_payload(output_path: Path, *, schema: RuntimeSchema) -> dict[str, object]:
    stages = dict.fromkeys(('ingest', 'normalize', 'split', 'fact', 'presentation', 'total', 'export'), 0.1)
    if schema is RuntimeSchema.INSTRUMENTED:
        stages.update({'writer_populate': 0.1, 'xlsx_save': 0.1})
    return {
        'status': 'succeeded',
        'pipeline': 'gb',
        'output_written': True,
        'workbook_path': str(output_path.resolve()),
        'sheet_count': 3,
        'error_log_count': 0,
        'issue_type_counts': {},
        'quality_metrics': [],
        'run_counts': {
            'reader_rows': 1,
            'detail_rows': 1,
            'qty_rows': 1,
            'qty_sheet_rows': 1,
            'quality_metric_count': 1,
            'work_order_rows': 1,
        },
        'stage_timings': {'stages': stages},
        'sheet_dimensions': list(_APPROVED_TEST_DIMENSIONS),
        'request_id': 'request-id',
    }


def _raw_artifact_paths(local_root: Path, batch_id: str, global_round: int, role: str) -> tuple[Path, ...]:
    result = local_root / 'pws-results' / batch_id / str(global_round) / f'{role}.json'
    log_dir = local_root / 'pws-logs' / batch_id / str(global_round)
    return (
        result,
        log_dir / f'{role}.stdout.log',
        log_dir / f'{role}.stderr.log',
        result.with_suffix('.powershell.json'),
    )


def _write_complete_raw_sample(
    request: PairedBenchmarkRequest,
    output_path: Path,
    *,
    schema: RuntimeSchema,
    role: str = 'reference',
    batch_id: str = 'b' * 64,
    global_round: int = 1,
    exit_code: int = 0,
    timed_out: bool = False,
    peak_working_set_bytes: int = 12345,
    create_workbook: bool = True,
) -> tuple[Path, ...]:
    if create_workbook:
        _write_approved_test_workbook(output_path)
    result, stdout_path, stderr_path, driver_path = _raw_artifact_paths(
        request.local_root, batch_id, global_round, role
    )
    io_result = phase0_harness._io_path(result)
    io_stdout = phase0_harness._io_path(stdout_path)
    io_stderr = phase0_harness._io_path(stderr_path)
    io_driver = phase0_harness._io_path(driver_path)
    io_stdout.parent.mkdir(parents=True, exist_ok=True)
    io_result.parent.mkdir(parents=True, exist_ok=True)
    stdout_bytes = json.dumps(_runtime_payload(output_path, schema=schema)).encode('utf-8')
    stderr_bytes = b''
    io_stdout.write_bytes(stdout_bytes)
    io_stderr.write_bytes(stderr_bytes)
    stdout_sha = hashlib.sha256(stdout_bytes).hexdigest()
    stderr_sha = hashlib.sha256(stderr_bytes).hexdigest()
    combined_sha = hashlib.sha256(f'{stdout_sha}\n{stderr_sha}'.encode()).hexdigest()
    io_driver.write_text(
        json.dumps(
            {
                'returncode': exit_code,
                'timed_out': False,
                'launch_failed': False,
                'tree_termination_failed': False,
                'driver_reaped': True,
                'stdout': '',
                'stderr': '',
            }
        ),
        encoding='utf-8',
    )
    payload = {
        'mode': 'Normal',
        'pipeline': 'gb',
        'role': role,
        'batch_id': batch_id,
        'global_round': global_round,
        'exit_code': exit_code,
        'timed_out': timed_out,
        'external_wall_seconds': '0.125',
        'peak_working_set_bytes': peak_working_set_bytes,
        'input_sha256': phase0_harness._sha256(request.input_path),
        'binary_sha256': phase0_harness._sha256(
            request.reference_executable if role == 'reference' else request.candidate_executable
        ),
        'command_arguments': list(
            build_pws_cli_arguments('Normal', 'gb', request.input_path.resolve(), output_path.resolve())
        ),
        'stdout_log_sha256': stdout_sha,
        'stderr_log_sha256': stderr_sha,
        'local_unversioned_log_sha256': combined_sha,
    }
    io_result.write_text(json.dumps(payload), encoding='utf-8')
    return result, stdout_path, stderr_path, driver_path


def _identity() -> BenchmarkIdentity:
    return BenchmarkIdentity('3' * 64, '1' * 64, '2' * 64, '4' * 40, '5' * 64, '6' * 64)


def _recovery_provenance() -> phase0_harness.RecoveryProvenance:
    return phase0_harness.RecoveryProvenance(
        parent_protocol_version=2,
        parent_comparison_key='0' * 64,
        parent_attempt=1,
        parent_terminal_sha256='1' * 64,
        parent_comparison_tree_sha256='2' * 64,
        parent_journal_head_sha256='3' * 64,
        parent_inventory_entry_count=134,
        reason=phase0_harness.RecoveryReason.MISSING_FORMAL_SHEET_DIMENSIONS,
    )


def _request(tmp_path: Path) -> PairedBenchmarkRequest:
    input_path = tmp_path / 'input.xlsx'
    reference = tmp_path / 'reference.exe'
    candidate = tmp_path / 'candidate.exe'
    manifest = tmp_path / 'phase0a.json'
    for path, content in ((input_path, b'input'), (reference, b'ref'), (candidate, b'candidate'), (manifest, b'{}')):
        path.write_bytes(content)
    local_root = tmp_path / 'rust' / 'target' / 'perf-local'
    return PairedBenchmarkRequest(
        pipeline='gb',
        input_path=input_path,
        reference_executable=reference,
        candidate_executable=candidate,
        reference_label=ClosedBinaryLabel.PHASE0A,
        candidate_label=ClosedBinaryLabel.PHASE0B,
        comparison_profile=ComparisonProfile.PHASE0B_VS_PHASE0A,
        phase0a_manifest=manifest,
        local_root=local_root,
        evidence_path=tmp_path / 'docs' / 'performance' / 'batch.json',
        attempt_ledger_root=local_root / 'batches',
    )


def _group_request(tmp_path: Path, *, start: int = 1) -> MetricGroupRequest:
    request = _request(tmp_path)
    ledger = AppendOnlyAttemptLedger.create_v3_once(
        request.attempt_ledger_root,
        _identity(),
        comparison_key='a' * 64,
        phase0a_manifest_sha256='9' * 64,
        recovery_provenance=_recovery_provenance(),
        upstream_gate_provenance=None,
    )
    first_group_sha256 = ledger.commit_first_group({'wall': '7' * 64, 'pws': '8' * 64}) if start == 6 else None
    return MetricGroupRequest(
        request,
        ledger.batch_id,
        'pws',
        build_round_plan(global_round_start=start, round_count=5),  # type: ignore[arg-type]
        ledger.attempt_directory,
        first_group_sha256=first_group_sha256,
    )


def _install_runner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    fail_role: str | None = None,
    interrupt_role: str | None = None,
) -> list[tuple[str, int, Path]]:
    calls: list[tuple[str, int, Path]] = []
    monkeypatch.setattr(phase0_harness, '_capture_identity', lambda request: _identity())

    def fake_sample(
        *,
        executable: Path,
        pipeline: str,
        input_path: Path,
        output_path: Path,
        role: str,
        batch_id: str,
        global_round: int,
        schema: object,
        local_root: Path,
        allow_resume_artifacts: bool = False,
    ) -> CapturedNormalRun:
        del allow_resume_artifacts
        calls.append((role, global_round, output_path))
        _write_approved_test_workbook(output_path)
        if role == interrupt_role:
            raise KeyboardInterrupt('simulated interruption')
        if role == fail_role:
            raise RustNormalProcessError(7, 'f' * 64)
        normal = NormalRunEvidence(
            external_wall_seconds=Decimal('1.25'),
            peak_working_set_bytes=123456,
            runtime=_runtime(pipeline, output_size_bytes=8),
            workbook_oracle_sha256='8' * 64,
        )
        return CapturedNormalRun(normal, 0, '7' * 64)

    monkeypatch.setattr(phase0_harness, '_invoke_pws_single_sample', fake_sample)
    return calls


def _seed_recorded_pws_sample(
    ledger: AppendOnlyAttemptLedger,
    request: MetricGroupRequest,
    plan: RoundPlan,
    role: str,
) -> Path:
    identity = _identity()
    payload = phase0_harness._planned_output_payload(request, identity, plan.global_round, role)
    plan_sha = ledger.record_planned_output('pws', plan.global_round, role, payload)
    started_sha = ledger.record_sample_started(
        batch_id=ledger.batch_id,
        metric='pws',
        global_round=plan.global_round,
        role=role,  # type: ignore[arg-type]
        order=plan.order,
        input_sha256=identity.input_sha256,
        binary_sha256=identity.reference_sha256 if role == 'reference' else identity.candidate_sha256,
        planned_output_record_sha256=plan_sha,
    )
    output = phase0_harness._planned_paths((payload,))[0]
    normal = NormalRunEvidence(
        external_wall_seconds=Decimal('1.25'),
        peak_working_set_bytes=123456,
        runtime=_runtime(output_size_bytes=8),
        workbook_oracle_sha256='8' * 64,
    )
    sample = phase0_harness._metric_sample(
        role,
        plan,
        identity,
        (normal, '7' * 64),
        metric_value=Decimal(123456),
    )
    ledger.record_sample(
        'pws',
        plan.global_round,
        role,  # type: ignore[arg-type]
        phase0_harness._sample_to_payload(sample, batch_id=ledger.batch_id),
        sample_started_record_sha256=started_sha,
    )
    return output


def test_pws_normal_command_does_not_include_check_only(tmp_path: Path) -> None:
    args = build_pws_cli_arguments('Normal', 'gb', tmp_path / '输入 文件.xlsx', tmp_path / '输出 文件.xlsx')
    assert '--check-only' not in args
    assert '--output' in args


def test_pws_check_only_command_includes_check_only(tmp_path: Path) -> None:
    args = build_pws_cli_arguments('CheckOnly', 'sk', tmp_path / '输入 文件.xlsx', None)
    assert '--check-only' in args
    assert '--output' not in args


def test_pws_single_sample_accepts_only_one_global_round_and_role(tmp_path: Path) -> None:
    command = phase0_harness._build_pws_script_command(
        mode='Normal',
        pipeline='gb',
        input_path=tmp_path / 'input.xlsx',
        executable=tmp_path / 'candidate.exe',
        role='candidate',
        batch_id='b' * 64,
        global_round=10,
        output_path=tmp_path / 'output.xlsx',
        local_log_root=tmp_path / 'rust' / 'target' / 'perf-local' / 'logs',
        local_result_path=tmp_path / 'rust' / 'target' / 'perf-local' / 'result.json',
    )
    assert command.count('-GlobalRound') == 1
    assert command[command.index('-GlobalRound') + 1] == '10'
    assert command.count('-Role') == 1
    assert command[command.index('-Role') + 1] == 'candidate'


def test_python_pws_group_preserves_rounds_six_to_ten_global_order(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    request = _group_request(tmp_path, start=6)
    calls = _install_runner(monkeypatch, tmp_path)
    group = run_pws_group(request)
    assert [round_.plan.global_round for round_ in group.rounds] == [6, 7, 8, 9, 10]
    assert [role for role, _, _ in calls] == [
        'candidate',
        'reference',
        'reference',
        'candidate',
        'candidate',
        'reference',
        'reference',
        'candidate',
        'candidate',
        'reference',
    ]


def test_pws_local_result_must_be_under_ignored_root(tmp_path: Path) -> None:
    with pytest.raises(HarnessFailure, match='trusted'):
        phase0_harness._pws_local_result_path(tmp_path / 'outside', 'b' * 64, 1, 'reference')


def test_pws_raw_log_paths_reject_reparse_components(tmp_path: Path) -> None:
    local_root = tmp_path / 'rust' / 'target' / 'perf-local'
    local_root.mkdir(parents=True)
    outside = tmp_path / 'outside'
    outside.mkdir()
    linked = local_root / 'pws-logs'
    try:
        linked.symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f'symlink creation is unavailable: {exc}')
    with pytest.raises(HarnessFailure, match='reparse|symlink'):
        phase0_harness._pws_local_artifact_paths(local_root, 'b' * 64, 1, 'reference')


def test_v3_cleanup_path_enumeration_is_filesystem_pure(tmp_path: Path) -> None:
    local_root = tmp_path / 'rust' / 'target' / 'perf-local'
    local_root.mkdir(parents=True)
    before = tuple(sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob('*')))

    artifacts = phase0_harness._pws_local_artifact_paths(local_root, 'b' * 64, 1, 'reference')

    assert artifacts.result_path.parent == local_root / 'pws-results' / ('b' * 64) / '1'
    assert tuple(sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob('*'))) == before


@pytest.mark.parametrize('schema', (RuntimeSchema.BASE, RuntimeSchema.INSTRUMENTED))
def test_invoke_pws_injects_actual_output_size_when_runtime_omits_it(
    schema: RuntimeSchema, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    request = _request(tmp_path)
    output_path = tmp_path / 'data' / 'processed' / 'gb' / 'resume.xlsx'
    _write_complete_raw_sample(request, output_path, schema=schema)
    launches: list[object] = []
    monkeypatch.setattr(
        phase0_harness, '_launch_pws_driver', lambda *args, **kwargs: launches.append(args), raising=False
    )

    captured = phase0_harness._invoke_pws_single_sample(
        executable=request.reference_executable,
        pipeline='gb',
        input_path=request.input_path,
        output_path=output_path,
        role='reference',
        batch_id='b' * 64,
        global_round=1,
        schema=schema,
        local_root=request.local_root,
    )

    assert launches == []
    assert captured.normal_run.runtime.output_size_bytes == output_path.stat().st_size > 0


# 系统启用 Win32 长路径后,MAX_PATH=260 限制解除,无 `\\?\` 前缀的逻辑路径也能访问长路径文件,
# 本回归测试的前提("逻辑路径不可见")不再成立,故跳过而非修复断言。
@pytest.mark.skipif(
    os.name != 'nt' or win_long_paths_enabled(),
    reason='长路径回归仅在未启用 Win32 长路径的 Windows 上有效',
)
def test_invoke_pws_accepts_complete_artifacts_for_long_logical_output_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request = _request(tmp_path)
    output_path = _long_output_path(tmp_path)
    output_io = phase0_harness._io_path(output_path)
    _write_complete_raw_sample(request, output_path, schema=RuntimeSchema.BASE, create_workbook=False)
    _write_minimal_xlsx(output_io)
    assert not output_path.is_file()
    assert output_io.is_file()
    monkeypatch.setattr(phase0_harness, '_launch_pws_driver', pytest.fail)

    try:
        captured = phase0_harness._invoke_pws_single_sample(
            executable=request.reference_executable,
            pipeline='gb',
            input_path=request.input_path,
            output_path=output_path,
            role='reference',
            batch_id='b' * 64,
            global_round=1,
            schema=RuntimeSchema.BASE,
            local_root=request.local_root,
        )

        assert captured.normal_run.runtime.output_size_bytes == output_io.stat().st_size > 0
        assert len(captured.normal_run.workbook_oracle_sha256) == 64
    finally:
        _remove_long_output_tree(tmp_path)


# 系统启用 Win32 长路径后,MAX_PATH=260 限制解除,无 `\\?\` 前缀的逻辑路径也能访问长路径文件,
# 本回归测试的前提("逻辑路径不可见")不再成立,故跳过而非修复断言。
@pytest.mark.skipif(
    os.name != 'nt' or win_long_paths_enabled(),
    reason='长路径回归仅在未启用 Win32 长路径的 Windows 上有效',
)
def test_invoke_pws_rejects_residual_long_workbook_before_driver_launch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request = _request(tmp_path)
    output_path = _long_output_path(tmp_path)
    output_io = phase0_harness._io_path(output_path)
    _write_minimal_xlsx(output_io)
    assert not output_path.is_file()
    assert output_io.is_file()
    launches: list[object] = []
    monkeypatch.setattr(phase0_harness, '_launch_pws_driver', lambda *args, **kwargs: launches.append((args, kwargs)))

    try:
        with pytest.raises(RustNormalValidationError, match='PWS raw collision'):
            phase0_harness._invoke_pws_single_sample(
                executable=request.reference_executable,
                pipeline='gb',
                input_path=request.input_path,
                output_path=output_path,
                role='reference',
                batch_id='b' * 64,
                global_round=1,
                schema=RuntimeSchema.BASE,
                local_root=request.local_root,
            )
        assert launches == []
    finally:
        _remove_long_output_tree(tmp_path)


def test_phase0a_approved_bytes_can_use_equal_wall_and_pws_resumed_samples(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    request = _request(tmp_path)
    output_path = tmp_path / 'data' / 'processed' / 'gb' / 'resume.xlsx'
    _write_complete_raw_sample(request, output_path, schema=RuntimeSchema.BASE)
    monkeypatch.setattr(phase0_harness, '_launch_pws_driver', pytest.fail)
    captured = phase0_harness._invoke_pws_single_sample(
        executable=request.reference_executable,
        pipeline='gb',
        input_path=request.input_path,
        output_path=output_path,
        role='reference',
        batch_id='b' * 64,
        global_round=1,
        schema=RuntimeSchema.BASE,
        local_root=request.local_root,
    )

    def calibration(metric: str) -> CalibrationGroup:
        rounds = tuple(
            CalibrationRound(
                global_round,
                MetricSample(
                    role='reference',
                    global_round=global_round,
                    metric_value=(
                        Decimal(captured.normal_run.peak_working_set_bytes or 0)
                        if metric == 'pws'
                        else captured.normal_run.external_wall_seconds
                    ),
                    exit_code=0,
                    input_sha256='1' * 64,
                    binary_sha256='2' * 64,
                    git_head='head',
                    repository_state_sha256='3' * 64,
                    machine_fingerprint_sha256='4' * 64,
                    local_unversioned_log_sha256='5' * 64,
                    normal_run=captured.normal_run,
                ),
            )
            for global_round in range(1, 6)
        )
        return CalibrationGroup('b' * 64, 'gb', metric, True, rounds)  # type: ignore[arg-type]

    wall = calibration('wall')
    pws = calibration('pws')
    manifest = Phase0AManifest(
        reference_exe_sha256='2' * 64,
        fork_revision='fork',
        git_head='head',
        machine=MachineEvidence('build', 'x86_64', 'cpu', 1, 1, 'SSD', 1, '4' * 64),
        gb_wall=wall,
        gb_pws=pws,
        sk_wall=wall,
        sk_pws=pws,
    )
    assert approved_phase0a_output_bytes(manifest, 'gb') == output_path.stat().st_size


@pytest.mark.parametrize('damage', ('tampered-stdout', 'missing-workbook'))
def test_invoke_pws_rejects_incomplete_or_tampered_resume_without_launch(
    damage: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    request = _request(tmp_path)
    output_path = tmp_path / 'data' / 'processed' / 'gb' / 'resume.xlsx'
    _, stdout_path, _, _ = _write_complete_raw_sample(request, output_path, schema=RuntimeSchema.BASE)
    if damage == 'tampered-stdout':
        phase0_harness._io_path(stdout_path).write_bytes(b'tampered')
    else:
        output_path.unlink()
    launches: list[object] = []
    monkeypatch.setattr(
        phase0_harness, '_launch_pws_driver', lambda *args, **kwargs: launches.append(args), raising=False
    )

    with pytest.raises(RustNormalValidationError):
        phase0_harness._invoke_pws_single_sample(
            executable=request.reference_executable,
            pipeline='gb',
            input_path=request.input_path,
            output_path=output_path,
            role='reference',
            batch_id='b' * 64,
            global_round=1,
            schema=RuntimeSchema.BASE,
            local_root=request.local_root,
        )
    assert launches == []


def test_invoke_pws_rejects_driver_log_collision_without_launch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    request = _request(tmp_path)
    output_path = tmp_path / 'data' / 'processed' / 'gb' / 'resume.xlsx'
    _, _, _, driver_path = _raw_artifact_paths(request.local_root, 'b' * 64, 1, 'reference')
    phase0_harness._io_path(driver_path).parent.mkdir(parents=True, exist_ok=True)
    phase0_harness._io_path(driver_path).write_bytes(b'collision')
    launches: list[object] = []
    monkeypatch.setattr(phase0_harness, '_launch_pws_driver', lambda *args, **kwargs: launches.append(args))

    with pytest.raises(RustNormalValidationError, match='collision'):
        phase0_harness._invoke_pws_single_sample(
            executable=request.reference_executable,
            pipeline='gb',
            input_path=request.input_path,
            output_path=output_path,
            role='reference',
            batch_id='b' * 64,
            global_round=1,
            schema=RuntimeSchema.BASE,
            local_root=request.local_root,
        )
    assert launches == []


def test_pws_group_leaves_reference_failure_for_outer_terminal_owner(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    request = _group_request(tmp_path)
    _install_runner(monkeypatch, tmp_path, fail_role='reference')
    with pytest.raises(HarnessFailure) as caught:
        run_pws_group(request)
    assert caught.value.verdict is HarnessVerdict.REFERENCE_FAILED
    assert AppendOnlyAttemptLedger.load(request.attempt_directory, _identity()).terminal_verdict is None


def test_pws_group_leaves_candidate_failure_for_outer_terminal_owner(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    request = _group_request(tmp_path)
    _install_runner(monkeypatch, tmp_path, fail_role='candidate')
    with pytest.raises(HarnessFailure) as caught:
        run_pws_group(request)
    assert caught.value.verdict is HarnessVerdict.CANDIDATE_FAILED
    assert AppendOnlyAttemptLedger.load(request.attempt_directory, _identity()).terminal_verdict is None


def test_pws_normal_outputs_are_unique_and_left_for_outer_cleanup(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls = _install_runner(monkeypatch, tmp_path)
    run_pws_group(_group_request(tmp_path))
    outputs = [output for _, _, output in calls]
    assert len(outputs) == len(set(outputs)) == 10
    assert all(output.exists() for output in outputs)


def test_pws_v3_group_rejects_complete_raw_sample_before_launch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    request = _group_request(tmp_path)
    ledger = AppendOnlyAttemptLedger.load(request.attempt_directory, _identity())
    missing_plan = request.plans[0]
    missing_role = missing_plan.order[0]
    for plan in request.plans:
        for role in plan.order:
            if (plan.global_round, role) != (missing_plan.global_round, missing_role):
                _seed_recorded_pws_sample(ledger, request, plan, role)
    payload = phase0_harness._planned_output_payload(request, _identity(), missing_plan.global_round, missing_role)
    ledger.record_planned_output('pws', missing_plan.global_round, missing_role, payload)
    output_path = phase0_harness._planned_paths((payload,))[0]
    executable = (
        request.benchmark.reference_executable
        if missing_role == 'reference'
        else request.benchmark.candidate_executable
    )
    _write_complete_raw_sample(
        request.benchmark,
        output_path,
        schema=RuntimeSchema.BASE,
        role=missing_role,
        global_round=missing_plan.global_round,
    )
    launches: list[object] = []
    monkeypatch.setattr(phase0_harness, '_capture_identity', lambda benchmark: _identity())
    monkeypatch.setattr(
        phase0_harness, '_launch_pws_driver', lambda *args, **kwargs: launches.append(args), raising=False
    )
    monkeypatch.setattr(phase0_harness, 'workbook_oracle', lambda path: 'oracle')

    with pytest.raises(HarnessFailure) as caught:
        run_pws_group(request)

    assert executable.exists()
    assert launches == []
    expected = HarnessVerdict.REFERENCE_FAILED if missing_role == 'reference' else HarnessVerdict.CORRECTNESS_FAILED
    assert caught.value.verdict is expected
    loaded = AppendOnlyAttemptLedger.load(request.attempt_directory, _identity())
    assert loaded.sample_payload('pws', missing_plan.global_round, missing_role) is None
    assert loaded.terminal_verdict is None
    assert output_path.exists()


def test_protocol_v3_pws_never_adopts_complete_raw_artifacts_before_fresh_start(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request = _group_request(tmp_path)
    plan = request.plans[0]
    role = plan.order[0]
    payload = phase0_harness._planned_output_payload(request, _identity(), plan.global_round, role)
    output_path = phase0_harness._planned_paths((payload,))[0]
    executable = (
        request.benchmark.reference_executable if role == 'reference' else request.benchmark.candidate_executable
    )
    _write_complete_raw_sample(
        request.benchmark,
        output_path,
        schema=RuntimeSchema.BASE,
        role=role,
        batch_id=request.batch_id,
        global_round=plan.global_round,
    )
    launches: list[object] = []
    monkeypatch.setattr(phase0_harness, '_launch_pws_driver', lambda *args, **kwargs: launches.append(args))

    with pytest.raises(RustNormalValidationError, match='fresh protocol v3'):
        phase0_harness._invoke_pws_single_sample(
            executable=executable,
            pipeline=request.benchmark.pipeline,
            input_path=request.benchmark.input_path,
            output_path=output_path,
            role=role,
            batch_id=request.batch_id,
            global_round=plan.global_round,
            schema=RuntimeSchema.BASE,
            local_root=request.benchmark.local_root,
            allow_resume_artifacts=False,
        )

    assert launches == []


def test_recorded_pws_sample_with_residual_workbook_is_left_for_outer_cleanup(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    request = _group_request(tmp_path)
    ledger = AppendOnlyAttemptLedger.load(request.attempt_directory, _identity())
    residual: Path | None = None
    for plan in request.plans:
        for role in plan.order:
            output = _seed_recorded_pws_sample(ledger, request, plan, role)
            if residual is None:
                residual = output
    assert residual is not None
    residual.parent.mkdir(parents=True, exist_ok=True)
    residual.write_bytes(b'residual workbook')
    commands = _install_runner(monkeypatch, tmp_path)

    run_pws_group(request)

    assert commands == []
    assert residual.exists()


def test_pws_interruption_uses_planned_ledger_for_outer_cleanup(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls = _install_runner(monkeypatch, tmp_path, interrupt_role='candidate')
    request = _group_request(tmp_path)
    with pytest.raises(HarnessFailure) as caught:
        run_pws_group(request)
    assert caught.value.verdict is HarnessVerdict.INCOMPLETE_EVIDENCE
    assert calls
    assert any(output.exists() for _, _, output in calls)
    assert AppendOnlyAttemptLedger.load(request.attempt_directory, _identity()).terminal_verdict is None


def test_pws_script_parses_with_powershell() -> None:
    powershell = shutil.which('powershell')
    if powershell is None:
        pytest.skip('Windows PowerShell is unavailable')
    script = Path(__file__).with_name('measure_peak_working_set.ps1')
    completed = subprocess.run(  # noqa: S603 - resolved local PowerShell parses the repository script.
        [
            powershell,
            '-NoProfile',
            '-Command',
            f"[void][scriptblock]::Create((Get-Content -Raw -Encoding UTF8 '{script}'))",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr


def test_pws_timeout_is_internal_and_does_not_expand_parameter_surface() -> None:
    script = Path(__file__).with_name('measure_peak_working_set.ps1').read_text(encoding='utf-8')
    parameter_block = script[script.index('param(') : script.index('$ErrorActionPreference')]
    assert 'Timeout' not in parameter_block
    assert '$ChildTimeoutSeconds = 900' in script
    assert 'taskkill.exe' in script
    assert 'timed_out' in script
    assert 'WaitForExit()' not in script


class _TimeoutPopen:
    pid = 1234
    returncode = 124

    def __init__(self) -> None:
        self.calls = 0

    def communicate(self, *, timeout: float) -> tuple[str, str]:
        self.calls += 1
        if self.calls == 1:
            raise subprocess.TimeoutExpired('powershell', timeout)
        return '', ''

    def poll(self) -> int:
        return self.returncode


class _FailedTreeKillPopen:
    pid = 5678

    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.returncode: int | None = None
        self.communicate_calls = 0
        self.kill_calls = 0
        self.wait_timeouts: list[float] = []

    def communicate(self, *, timeout: float) -> tuple[str, str]:
        self.communicate_calls += 1
        if self.communicate_calls == 1 or self.mode == 'alive':
            raise subprocess.TimeoutExpired('powershell', timeout)
        if self.mode == 'second-timeout' and self.communicate_calls == 2:
            raise subprocess.TimeoutExpired('powershell', timeout)
        return '', ''

    def kill(self) -> None:
        self.kill_calls += 1
        if self.mode != 'alive':
            self.returncode = -9

    def poll(self) -> int | None:
        return self.returncode

    def wait(self, *, timeout: float) -> int:
        self.wait_timeouts.append(timeout)
        if self.returncode is None:
            raise subprocess.TimeoutExpired('powershell', timeout)
        return self.returncode


def test_python_pws_driver_watchdog_kills_process_tree_without_waiting(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    local_root = tmp_path / 'rust' / 'target' / 'perf-local'
    driver_log = local_root / 'driver.json'
    killed: list[int] = []
    monkeypatch.setattr(subprocess, 'Popen', lambda *args, **kwargs: _TimeoutPopen())
    monkeypatch.setattr(phase0_harness, '_PWS_DRIVER_TIMEOUT_SECONDS', 0.01, raising=False)
    monkeypatch.setattr(phase0_harness, '_terminate_windows_process_tree', killed.append, raising=False)

    with pytest.raises(RustNormalProcessError):
        phase0_harness._launch_pws_driver(('powershell',), driver_log_path=driver_log, local_root=local_root)

    assert killed == [1234]
    assert driver_log.exists()


def test_python_pws_driver_launch_failure_is_closed_without_waiting(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    local_root = tmp_path / 'rust' / 'target' / 'perf-local'
    driver_log = local_root / 'driver.json'
    monkeypatch.setattr(subprocess, 'Popen', lambda *args, **kwargs: (_ for _ in ()).throw(OSError('launch failed')))

    with pytest.raises(RustNormalProcessError):
        phase0_harness._launch_pws_driver(('powershell',), driver_log_path=driver_log, local_root=local_root)

    assert driver_log.exists()


@pytest.mark.parametrize('mode', ('reaped', 'second-timeout'))
def test_python_watchdog_taskkill_failure_falls_back_to_bounded_driver_kill(
    mode: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    local_root = tmp_path / 'rust' / 'target' / 'perf-local'
    driver_log = local_root / 'driver.json'
    process = _FailedTreeKillPopen(mode)
    monkeypatch.setattr(subprocess, 'Popen', lambda *args, **kwargs: process)
    monkeypatch.setattr(phase0_harness, '_PWS_DRIVER_TIMEOUT_SECONDS', 0.01)
    monkeypatch.setattr(
        phase0_harness,
        '_terminate_windows_process_tree',
        lambda pid: (_ for _ in ()).throw(OSError('taskkill nonzero')),
    )

    with pytest.raises(RustNormalProcessError):
        phase0_harness._launch_pws_driver(('powershell',), driver_log_path=driver_log, local_root=local_root)

    payload = json.loads(driver_log.read_text(encoding='utf-8'))
    assert process.kill_calls >= 1
    assert process.poll() is not None
    assert payload['tree_termination_failed'] is True
    assert payload['driver_reaped'] is True


def test_python_watchdog_fails_closed_when_fallback_driver_remains_alive(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    local_root = tmp_path / 'rust' / 'target' / 'perf-local'
    driver_log = local_root / 'driver.json'
    process = _FailedTreeKillPopen('alive')
    monkeypatch.setattr(subprocess, 'Popen', lambda *args, **kwargs: process)
    monkeypatch.setattr(phase0_harness, '_PWS_DRIVER_TIMEOUT_SECONDS', 0.01)
    monkeypatch.setattr(
        phase0_harness,
        '_terminate_windows_process_tree',
        lambda pid: (_ for _ in ()).throw(OSError('taskkill nonzero')),
    )

    with pytest.raises(RustNormalProcessError):
        phase0_harness._launch_pws_driver(('powershell',), driver_log_path=driver_log, local_root=local_root)

    payload = json.loads(driver_log.read_text(encoding='utf-8'))
    assert process.kill_calls >= 1
    assert process.poll() is None
    assert process.wait_timeouts
    assert payload['tree_termination_failed'] is True
    assert payload['driver_reaped'] is False


def _run_sanitized_fake_child() -> dict[str, object]:
    powershell = shutil.which('powershell')
    if powershell is None:
        pytest.skip('Windows PowerShell is unavailable')
    root = Path(__file__).parents[2]
    fixture_root = root / 'rust' / 'target' / 'perf-local' / f'pytest pws 中文 {uuid4().hex}'
    fixture_root.mkdir(parents=True)
    executable = fixture_root / 'fake child 中文.exe'
    input_path = fixture_root / '输入 文件.xlsx'
    output_path = fixture_root / '输出 文件.xlsx'
    result_path = fixture_root / '结果 文件.json'
    input_path.write_bytes(b'sanitized')
    source = (
        'using System.Threading; public class P { '
        'public static int Main(string[] args) { Thread.Sleep(100); return 0; } }'
    )
    compile_command = (
        f"$code='{source}'; Add-Type -TypeDefinition $code "
        f"-OutputAssembly '{executable}' -OutputType ConsoleApplication"
    )
    try:
        compiled = subprocess.run(  # noqa: S603 - resolved local PowerShell compiles a fixed sanitized fixture.
            [powershell, '-NoProfile', '-Command', compile_command],
            check=False,
            capture_output=True,
            text=True,
        )
        assert compiled.returncode == 0, compiled.stderr
        script = Path(__file__).with_name('measure_peak_working_set.ps1')
        completed = subprocess.run(  # noqa: S603 - resolved local PowerShell runs the repository script.
            [
                powershell,
                '-NoProfile',
                '-File',
                str(script),
                '-Mode',
                'Normal',
                '-Pipeline',
                'gb',
                '-InputPath',
                str(input_path),
                '-Executable',
                str(executable),
                '-Role',
                'reference',
                '-BatchId',
                'b' * 64,
                '-GlobalRound',
                '1',
                '-OutputPath',
                str(output_path),
                '-LocalLogRoot',
                str(fixture_root / '本地 logs'),
                '-LocalResultPath',
                str(result_path),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0, completed.stderr
        return json.loads(result_path.read_text(encoding='utf-8'))
    finally:
        shutil.rmtree(fixture_root, ignore_errors=True)


def test_pws_single_sample_quotes_space_and_chinese_paths() -> None:
    payload = _run_sanitized_fake_child()
    arguments = payload['command_arguments']
    assert isinstance(arguments, list)
    assert any('输入 文件.xlsx' in item for item in arguments)
    assert any('输出 文件.xlsx' in item for item in arguments)


def test_pws_single_sample_smoke_reports_positive_peak() -> None:
    payload = _run_sanitized_fake_child()
    assert payload['exit_code'] == 0
    assert isinstance(payload['peak_working_set_bytes'], int)
    assert payload['peak_working_set_bytes'] > 0


def test_pws_timeout_kills_child_tree_and_cleanup_removes_workbook() -> None:
    powershell = shutil.which('powershell')
    if powershell is None:
        pytest.skip('Windows PowerShell is unavailable')
    root = Path(__file__).parents[2]
    fixture_root = root / 'rust' / 'target' / 'perf-local' / f'pytest pws timeout {uuid4().hex}'
    fixture_root.mkdir(parents=True)
    executable = fixture_root / 'sleep-child.exe'
    input_path = fixture_root / 'input.xlsx'
    output_path = fixture_root / 'output.xlsx'
    result_path = fixture_root / 'result.json'
    pid_path = fixture_root / 'pids.txt'
    timeout_script = Path(__file__).with_name(f'.timeout-{uuid4().hex}.ps1')
    input_path.write_bytes(b'sanitized timeout input')
    source = (
        'using System; using System.Diagnostics; using System.IO; using System.Threading; '
        'public class P { public static int Main(string[] args) { '
        'string p = Environment.GetEnvironmentVariable("PWS_TIMEOUT_PID_FILE"); '
        'if (Array.IndexOf(args, "--grandchild") >= 0) { '
        'File.AppendAllText(p, Process.GetCurrentProcess().Id + Environment.NewLine); '
        'Thread.Sleep(30000); return 0; } '
        'int i = Array.IndexOf(args, "--output"); '
        'if (i >= 0) File.WriteAllBytes(args[i + 1], new byte[] { 1, 2, 3 }); '
        'Process c = Process.Start(new ProcessStartInfo { '
        'FileName = Process.GetCurrentProcess().MainModule.FileName, Arguments = "--grandchild", '
        'UseShellExecute = false }); '
        'File.AppendAllText(p, Process.GetCurrentProcess().Id + Environment.NewLine + c.Id + Environment.NewLine); '
        'Thread.Sleep(30000); return 0; } }'
    )
    compile_command = (
        f"$code='{source}'; Add-Type -TypeDefinition $code "
        f"-OutputAssembly '{executable}' -OutputType ConsoleApplication"
    )
    original_script = Path(__file__).with_name('measure_peak_working_set.ps1').read_text(encoding='utf-8')
    timeout_script.write_text(
        original_script.replace('$ChildTimeoutSeconds = 900', '$ChildTimeoutSeconds = 1'),
        encoding='utf-8',
        newline='\n',
    )
    environment = dict(os.environ)
    environment['PWS_TIMEOUT_PID_FILE'] = str(pid_path)
    try:
        compiled = subprocess.run(  # noqa: S603 - fixed local PowerShell compiles a sanitized fixture.
            [powershell, '-NoProfile', '-Command', compile_command],
            check=False,
            capture_output=True,
            text=True,
        )
        assert compiled.returncode == 0, compiled.stderr
        completed = subprocess.run(  # noqa: S603 - fixed local PowerShell runs the temporary timeout script.
            [
                powershell,
                '-NoProfile',
                '-File',
                str(timeout_script),
                '-Mode',
                'Normal',
                '-Pipeline',
                'gb',
                '-InputPath',
                str(input_path),
                '-Executable',
                str(executable),
                '-Role',
                'reference',
                '-BatchId',
                'b' * 64,
                '-GlobalRound',
                '1',
                '-OutputPath',
                str(output_path),
                '-LocalLogRoot',
                str(fixture_root / 'logs'),
                '-LocalResultPath',
                str(result_path),
            ],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
            timeout=15,
        )
        assert completed.returncode == 124, completed.stderr
        payload = json.loads(result_path.read_text(encoding='utf-8'))
        assert payload['timed_out'] is True
        assert payload['exit_code'] == 124
        assert output_path.exists()
        pids = {int(value) for value in pid_path.read_text(encoding='utf-8').splitlines() if value}
        assert len(pids) >= 2
        for pid in pids:
            probe = subprocess.run(  # noqa: S603 - fixed PowerShell checks only the numeric fixture PID.
                [
                    powershell,
                    '-NoProfile',
                    '-Command',
                    f'if (Get-Process -Id {pid} -ErrorAction SilentlyContinue) {{ exit 1 }} else {{ exit 0 }}',
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            assert probe.returncode == 0, f'timed-out fixture process remains alive: {pid}'
        phase0_harness._remove_workbook(output_path)
        assert not output_path.exists()
    finally:
        timeout_script.unlink(missing_ok=True)
        shutil.rmtree(fixture_root, ignore_errors=True)


def test_pws_result_parser_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    path = tmp_path / 'result.json'
    path.write_text('{"mode":"Normal","mode":"CheckOnly"}', encoding='utf-8')
    with pytest.raises(HarnessFailure, match='duplicate'):
        phase0_harness._parse_pws_local_result(path)


def test_pws_result_parser_accepts_positive_peak(tmp_path: Path) -> None:
    payload = {
        'mode': 'Normal',
        'pipeline': 'gb',
        'role': 'reference',
        'batch_id': 'b' * 64,
        'global_round': 1,
        'exit_code': 0,
        'timed_out': False,
        'external_wall_seconds': '0.125',
        'peak_working_set_bytes': 12345,
        'input_sha256': '1' * 64,
        'binary_sha256': '2' * 64,
        'command_arguments': ['gb', '--input', 'input.xlsx', '--output', 'output.xlsx', '--benchmark'],
        'stdout_log_sha256': '3' * 64,
        'stderr_log_sha256': '4' * 64,
        'local_unversioned_log_sha256': '5' * 64,
    }
    path = tmp_path / 'result.json'
    path.write_text(json.dumps(payload), encoding='utf-8')
    parsed = phase0_harness._parse_pws_local_result(path)
    assert parsed['peak_working_set_bytes'] == 12345


@pytest.mark.parametrize(
    'plans',
    (
        build_round_plan(global_round_start=1, round_count=5)[:-1],
        build_round_plan(global_round_start=1, round_count=5)[:1] * 5,
        tuple(reversed(build_round_plan(global_round_start=1, round_count=5))),
    ),
)
def test_pws_group_rejects_missing_duplicate_or_unbalanced_rounds(plans: tuple[object, ...], tmp_path: Path) -> None:
    base = _group_request(tmp_path)
    with pytest.raises(ValueError, match='rounds'):
        MetricGroupRequest(
            base.benchmark,
            base.batch_id,
            'pws',
            plans,  # type: ignore[arg-type]
            base.attempt_directory,
        )


@pytest.mark.parametrize('field', ('input_sha256', 'reference_sha256', 'candidate_sha256', 'git_head'))
def test_pws_group_rejects_sha_or_git_drift(field: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    request = _group_request(tmp_path)
    _install_runner(monkeypatch, tmp_path)
    identities = [_identity(), replace(_identity(), **{field: 'drift'})]
    monkeypatch.setattr(
        phase0_harness,
        '_capture_identity',
        lambda benchmark: identities.pop(0) if identities else replace(_identity(), **{field: 'drift'}),
    )
    with pytest.raises(HarnessFailure, match='drift'):
        run_pws_group(request)


def test_pws_inner_group_does_not_handle_outer_cleanup_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    request = _group_request(tmp_path)
    _install_runner(monkeypatch, tmp_path)
    runner = phase0_harness._invoke_pws_single_sample

    def create_partial_evidence(**kwargs: object) -> CapturedNormalRun:
        request.benchmark.evidence_path.parent.mkdir(parents=True, exist_ok=True)
        request.benchmark.evidence_path.write_bytes(b'partial batch evidence')
        return runner(**kwargs)

    monkeypatch.setattr(phase0_harness, '_invoke_pws_single_sample', create_partial_evidence)
    cleanup_calls: list[Path] = []
    monkeypatch.setattr(phase0_harness, '_remove_workbook', cleanup_calls.append)

    run_pws_group(request)

    assert cleanup_calls == []
    assert request.benchmark.evidence_path.exists()

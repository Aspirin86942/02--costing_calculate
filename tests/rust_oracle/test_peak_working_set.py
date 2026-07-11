from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

from tests.rust_oracle import phase0_harness
from tests.rust_oracle.benchmark_protocol import (
    ClosedBinaryLabel,
    ComparisonProfile,
    HarnessVerdict,
    NormalRunEvidence,
    RuntimeEvidence,
    build_round_plan,
)
from tests.rust_oracle.oracle_runner import CapturedNormalRun, RustNormalProcessError
from tests.rust_oracle.phase0_harness import (
    AppendOnlyAttemptLedger,
    BenchmarkIdentity,
    HarnessFailure,
    MetricGroupRequest,
    PairedBenchmarkRequest,
    build_pws_cli_arguments,
    run_pws_group,
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


def _identity() -> BenchmarkIdentity:
    return BenchmarkIdentity('3' * 64, '1' * 64, '2' * 64, 'head', '4' * 64, '5' * 64)


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
    ledger = AppendOnlyAttemptLedger.create(request.attempt_ledger_root, _identity(), comparison_key='a' * 64)
    first_group_sha256 = ledger.commit_first_group({'wall': {}, 'pws': {}}) if start == 6 else None
    return MetricGroupRequest(
        request,
        'b' * 64,
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
    ) -> CapturedNormalRun:
        calls.append((role, global_round, output_path))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b'workbook')
        if role == interrupt_role:
            raise KeyboardInterrupt('simulated interruption')
        if role == fail_role:
            raise RustNormalProcessError(7, 'f' * 64)
        normal = NormalRunEvidence(
            external_wall_seconds=Decimal('1.25'),
            peak_working_set_bytes=123456,
            runtime=_runtime(pipeline),
            workbook_oracle_sha256='oracle',
        )
        return CapturedNormalRun(normal, 0, 'l' * 64)

    monkeypatch.setattr(phase0_harness, '_invoke_pws_single_sample', fake_sample)
    return calls


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


def test_pws_group_rejects_reference_nonzero(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    request = _group_request(tmp_path)
    _install_runner(monkeypatch, tmp_path, fail_role='reference')
    with pytest.raises(HarnessFailure) as caught:
        run_pws_group(request)
    assert caught.value.verdict is HarnessVerdict.REFERENCE_FAILED
    assert AppendOnlyAttemptLedger.load(request.attempt_directory, _identity()).terminal_verdict is caught.value.verdict


def test_pws_group_rejects_candidate_nonzero(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    request = _group_request(tmp_path)
    _install_runner(monkeypatch, tmp_path, fail_role='candidate')
    with pytest.raises(HarnessFailure) as caught:
        run_pws_group(request)
    assert caught.value.verdict is HarnessVerdict.CANDIDATE_FAILED
    assert AppendOnlyAttemptLedger.load(request.attempt_directory, _identity()).terminal_verdict is caught.value.verdict


def test_pws_normal_outputs_are_unique_and_removed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls = _install_runner(monkeypatch, tmp_path)
    run_pws_group(_group_request(tmp_path))
    outputs = [output for _, _, output in calls]
    assert len(outputs) == len(set(outputs)) == 10
    assert not any(output.exists() for output in outputs)


def test_pws_interruption_uses_planned_ledger_for_outer_cleanup(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls = _install_runner(monkeypatch, tmp_path, interrupt_role='candidate')
    with pytest.raises(KeyboardInterrupt):
        run_pws_group(_group_request(tmp_path))
    assert calls
    assert not any(output.exists() for _, _, output in calls)


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
    request = _request(tmp_path)
    ledger = AppendOnlyAttemptLedger.create(request.attempt_ledger_root, _identity(), comparison_key='a' * 64)
    with pytest.raises(ValueError, match='rounds'):
        MetricGroupRequest(request, 'b' * 64, 'pws', plans, ledger.attempt_directory)  # type: ignore[arg-type]


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


def test_pws_cleanup_failure_deletes_batch_evidence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    request = _group_request(tmp_path)
    _install_runner(monkeypatch, tmp_path)
    monkeypatch.setattr(
        phase0_harness,
        '_remove_workbook',
        lambda path: (_ for _ in ()).throw(PermissionError('locked')),
    )
    with pytest.raises(HarnessFailure) as caught:
        run_pws_group(request)
    assert caught.value.verdict is HarnessVerdict.CLEANUP_FAILED
    assert not request.benchmark.evidence_path.exists()

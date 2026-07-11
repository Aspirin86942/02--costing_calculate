from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from tests.rust_oracle import benchmark, repo_paths
from tests.rust_oracle.oracle_runner import (
    REQUIRED_RUST_PAYLOAD_STAGES,
    OracleRunSummary,
    TimedPayloadRun,
    _io_path,
)
from tests.rust_oracle.repo_paths import repo_root

_INPUT_SHA = 'a' * 64
_BINARY_SHA = 'b' * 64
_CHANGED_SHA = 'c' * 64


def test_require_benchmark_sample_rejects_unknown_pipeline() -> None:
    with pytest.raises(AssertionError, match="unsupported benchmark pipeline: 'unknown'"):
        repo_paths.require_benchmark_sample('unknown')


def test_require_benchmark_sample_fails_when_sample_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv('COSTING_GB_SAMPLE', raising=False)
    monkeypatch.setattr(repo_paths, 'repo_root', lambda: tmp_path)
    (tmp_path / 'data' / 'raw' / 'gb').mkdir(parents=True)

    with pytest.raises(AssertionError, match='requires exactly one sample'):
        repo_paths.require_benchmark_sample('gb')


def test_require_benchmark_sample_does_not_fallback_from_invalid_environment_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / 'data' / 'raw' / 'gb'
    raw_dir.mkdir(parents=True)
    (raw_dir / 'gb-fallback.xlsx').write_bytes(b'fallback')
    invalid_path = tmp_path / 'missing.xlsx'
    monkeypatch.setenv('COSTING_GB_SAMPLE', str(invalid_path))
    monkeypatch.setattr(repo_paths, 'repo_root', lambda: tmp_path)

    with pytest.raises(AssertionError, match='COSTING_GB_SAMPLE must point to an existing .xlsx file'):
        repo_paths.require_benchmark_sample('gb')


def test_require_benchmark_sample_fails_when_multiple_samples_exist(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv('COSTING_GB_SAMPLE', raising=False)
    monkeypatch.setattr(repo_paths, 'repo_root', lambda: tmp_path)
    raw_dir = tmp_path / 'data' / 'raw' / 'gb'
    raw_dir.mkdir(parents=True)
    (raw_dir / 'gb-first.xlsx').write_bytes(b'first')
    (raw_dir / 'gb-second.xlsx').write_bytes(b'second')

    with pytest.raises(AssertionError, match='found 2'):
        repo_paths.require_benchmark_sample('gb')


def test_require_benchmark_sample_returns_the_only_absolute_xlsx_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv('COSTING_GB_SAMPLE', raising=False)
    monkeypatch.setattr(repo_paths, 'repo_root', lambda: tmp_path)
    raw_dir = tmp_path / 'data' / 'raw' / 'gb'
    raw_dir.mkdir(parents=True)
    sample_path = raw_dir / 'gb-only.xlsx'
    sample_path.write_bytes(b'sample')

    assert repo_paths.require_benchmark_sample('gb') == sample_path.resolve()


def _summary(
    *,
    error_log_count: int = 1,
    issue_type_counts: dict[str, int] | None = None,
    quality_metrics: dict[tuple[str, str], str] | None = None,
) -> OracleRunSummary:
    return OracleRunSummary(
        error_log_count=error_log_count,
        issue_type_counts=issue_type_counts or {'MISSING_AMOUNT': error_log_count},
        quality_metrics=quality_metrics or {('行数勾稽', '总表行数'): '10'},
    )


def _required_stages(value: float) -> dict[str, float]:
    return {
        'ingest': value,
        'normalize': value,
        'split': value,
        'fact': value,
        'presentation': value,
        'total': value,
    }


def _timed_run(
    pipeline: str,
    total: float,
    stage_seconds: dict[str, float],
    run_counts: dict[str, int],
    summary: OracleRunSummary,
) -> TimedPayloadRun:
    assert set(stage_seconds) == set(REQUIRED_RUST_PAYLOAD_STAGES)
    return TimedPayloadRun(
        pipeline=pipeline,
        payload_total_seconds=total,
        stage_timings=stage_seconds,
        runtime_summary=summary,
        run_counts=run_counts,
    )


def _benchmark_paths(tmp_path: Path) -> tuple[Path, Path]:
    input_path = tmp_path / '输入.xlsx'
    rust_executable = tmp_path / 'costing-calculate.exe'
    input_path.write_bytes(b'input')
    rust_executable.write_bytes(b'binary')
    return input_path, rust_executable


def _install_fake_runners(
    monkeypatch: pytest.MonkeyPatch,
    *,
    rust_runs: tuple[TimedPayloadRun, ...],
    python_runs: tuple[TimedPayloadRun, ...],
    calls: list[str] | None = None,
) -> None:
    rust_iterator = iter(rust_runs)
    python_iterator = iter(python_runs)
    rust_call_count = 0
    python_call_count = 0

    def fake_rust_runner(executable: Path, pipeline: str, input_path: Path) -> TimedPayloadRun:
        nonlocal rust_call_count
        del executable, pipeline, input_path
        rust_call_count += 1
        if calls is not None:
            label = 'warmup-rust' if rust_call_count == 1 else f'round-{rust_call_count - 1}-rust'
            calls.append(label)
        return next(rust_iterator)

    def fake_python_runner(pipeline: str, input_path: Path) -> TimedPayloadRun:
        nonlocal python_call_count
        del pipeline, input_path
        python_call_count += 1
        if calls is not None:
            label = 'warmup-python' if python_call_count == 1 else f'round-{python_call_count - 1}-python'
            calls.append(label)
        return next(python_iterator)

    monkeypatch.setattr(benchmark, 'run_rust_cli_release_check_only', fake_rust_runner)
    monkeypatch.setattr(benchmark, 'run_python_check_only_payload', fake_python_runner)
    monkeypatch.setattr(benchmark, '_git_evidence', lambda: ('git-head', 'working-tree-diff'))


def test_check_only_benchmark_warms_each_runtime_once(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    summary = _summary()
    rust_run = _timed_run('gb', 1.0, _required_stages(1.0), {'reader_rows': 10}, summary)
    python_run = _timed_run('gb', 2.0, _required_stages(2.0), {}, summary)
    calls: list[str] = []
    _install_fake_runners(
        monkeypatch,
        rust_runs=(rust_run,) * (benchmark.CHECK_ONLY_WARMUPS + benchmark.CHECK_ONLY_ROUNDS),
        python_runs=(python_run,) * (benchmark.CHECK_ONLY_WARMUPS + benchmark.CHECK_ONLY_ROUNDS),
        calls=calls,
    )
    input_path, rust_executable = _benchmark_paths(tmp_path)

    result = benchmark.run_check_only_payload_benchmark('gb', input_path, rust_executable)

    assert calls[:2] == ['warmup-rust', 'warmup-python']
    assert calls.count('warmup-rust') == benchmark.CHECK_ONLY_WARMUPS
    assert calls.count('warmup-python') == benchmark.CHECK_ONLY_WARMUPS
    assert len(result.rust_payload_total_seconds) == benchmark.CHECK_ONLY_ROUNDS
    assert len(result.python_payload_total_seconds) == benchmark.CHECK_ONLY_ROUNDS


def test_check_only_benchmark_alternates_five_paired_rounds(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    summary = _summary()
    rust_run = _timed_run('gb', 1.0, _required_stages(1.0), {'reader_rows': 10}, summary)
    python_run = _timed_run('gb', 2.0, _required_stages(2.0), {}, summary)
    calls: list[str] = []
    _install_fake_runners(
        monkeypatch,
        rust_runs=(rust_run,) * 6,
        python_runs=(python_run,) * 6,
        calls=calls,
    )
    input_path, rust_executable = _benchmark_paths(tmp_path)

    benchmark.run_check_only_payload_benchmark('gb', input_path, rust_executable)

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


def test_check_only_verdict_rejects_four_complete_rounds() -> None:
    assert (
        benchmark.classify_check_only_verdict(
            rust_seconds=(1.0, 1.0, 1.0, 1.0),
            python_seconds=(2.0, 2.0, 2.0, 2.0),
            rust_stage_seconds={},
            valid_pair_count=4,
            validation_failures=(),
        )
        == 'INCOMPLETE_EVIDENCE'
    )


def test_check_only_verdict_rejects_runtime_mismatch() -> None:
    five = (1.0,) * benchmark.CHECK_ONLY_ROUNDS
    stages = dict.fromkeys(REQUIRED_RUST_PAYLOAD_STAGES, five)

    verdict = benchmark.classify_check_only_verdict(
        rust_seconds=five,
        python_seconds=(2.0,) * benchmark.CHECK_ONLY_ROUNDS,
        rust_stage_seconds=stages,
        valid_pair_count=benchmark.CHECK_ONLY_ROUNDS,
        validation_failures=(benchmark.ValidationFailure('ETL_MISMATCH', 'runtime contract mismatch'),),
    )

    assert verdict == 'ETL_MISMATCH'


def test_check_only_result_reports_min_max_median_and_rust_stage_medians(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    summary = _summary(
        error_log_count=3,
        issue_type_counts={'Z_ISSUE': 2, 'A_ISSUE': 1},
        quality_metrics={('质量', '乙'): '二', ('质量', '甲'): '一'},
    )
    rust_totals = (5.0, 1.0, 3.0, 2.0, 4.0)
    python_totals = (10.0, 6.0, 8.0, 7.0, 9.0)
    warmup_rust = _timed_run('gb', 99.0, _required_stages(99.0), {'z_count': 2, 'a_count': 1}, summary)
    warmup_python = _timed_run('gb', 99.0, _required_stages(99.0), {}, summary)
    rust_runs = (
        warmup_rust,
        *(
            _timed_run(
                'gb',
                value,
                {
                    'ingest': value,
                    'normalize': value + 10.0,
                    'split': value + 20.0,
                    'fact': value + 30.0,
                    'presentation': value + 40.0,
                    'total': value,
                },
                {'z_count': 2, 'a_count': 1},
                summary,
            )
            for value in rust_totals
        ),
    )
    python_runs = (
        warmup_python,
        *(_timed_run('gb', value, _required_stages(value), {}, summary) for value in python_totals),
    )
    _install_fake_runners(monkeypatch, rust_runs=rust_runs, python_runs=python_runs)
    input_path, rust_executable = _benchmark_paths(tmp_path)

    result = benchmark.run_check_only_payload_benchmark('gb', input_path, rust_executable)

    assert result.python_payload_total_seconds == python_totals
    assert (result.python_min_seconds, result.python_median_seconds, result.python_max_seconds) == (6.0, 8.0, 10.0)
    assert result.rust_payload_total_seconds == rust_totals
    assert (result.rust_min_seconds, result.rust_median_seconds, result.rust_max_seconds) == (1.0, 3.0, 5.0)
    assert result.rust_stage_seconds['normalize'] == (15.0, 11.0, 13.0, 12.0, 14.0)
    assert result.rust_stage_median_seconds == {
        'ingest': 3.0,
        'normalize': 13.0,
        'split': 23.0,
        'fact': 33.0,
        'presentation': 43.0,
        'total': 3.0,
    }
    assert list(result.rust_runtime_evidence.run_counts) == ['a_count', 'z_count']
    assert list(result.rust_runtime_evidence.issue_type_counts) == ['A_ISSUE', 'Z_ISSUE']
    assert result.rust_runtime_evidence.quality_metrics == (
        benchmark.QualityMetricEvidence('质量', '乙', '二'),
        benchmark.QualityMetricEvidence('质量', '甲', '一'),
    )
    assert result.rust_executable == str(rust_executable.resolve())
    assert result.git_head == 'git-head'
    assert result.working_tree_diff_id == 'working-tree-diff'
    assert result.working_directory == str(repo_root())
    assert result.command_arguments == (
        'gb',
        '--input',
        str(input_path.resolve()),
        '--check-only',
        '--benchmark',
    )


def test_check_only_result_requires_five_values_for_every_required_rust_stage() -> None:
    five = (1.0,) * benchmark.CHECK_ONLY_ROUNDS
    stages = dict.fromkeys(REQUIRED_RUST_PAYLOAD_STAGES, five)
    del stages['normalize']

    assert (
        benchmark.classify_check_only_verdict(
            rust_seconds=five,
            python_seconds=(2.0,) * benchmark.CHECK_ONLY_ROUNDS,
            rust_stage_seconds=stages,
            valid_pair_count=benchmark.CHECK_ONLY_ROUNDS,
            validation_failures=(),
        )
        == 'INCOMPLETE_EVIDENCE'
    )


def test_check_only_benchmark_rejects_runtime_evidence_drift_between_rounds(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    summary = _summary()
    stable_rust = _timed_run('gb', 1.0, _required_stages(1.0), {'reader_rows': 10}, summary)
    drifted_rust = _timed_run('gb', 1.0, _required_stages(1.0), {'reader_rows': 11}, summary)
    python_run = _timed_run('gb', 2.0, _required_stages(2.0), {}, summary)
    _install_fake_runners(
        monkeypatch,
        rust_runs=(stable_rust, stable_rust, drifted_rust, stable_rust, stable_rust, stable_rust),
        python_runs=(python_run,) * 6,
    )
    input_path, rust_executable = _benchmark_paths(tmp_path)

    result = benchmark.run_check_only_payload_benchmark('gb', input_path, rust_executable)

    assert result.valid_pair_count == 4
    assert result.validation_passed is False
    assert result.verdict == 'INCOMPLETE_EVIDENCE'
    assert result.validation_failures == (
        benchmark.ValidationFailure(
            'ETL_MISMATCH',
            'round 2: Rust runtime evidence changed between formal rounds',
        ),
    )


def test_runtime_evidence_requires_issue_counts_to_sum_to_error_count() -> None:
    invalid_summary = _summary(error_log_count=2, issue_type_counts={'MISSING_AMOUNT': 1})
    run = _timed_run('gb', 1.0, _required_stages(1.0), {'reader_rows': 10}, invalid_summary)

    with pytest.raises(AssertionError, match='issue_type_counts must sum to error_log_count'):
        benchmark._runtime_evidence(run)


def test_check_only_benchmark_rejects_input_hash_change(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    summary = _summary()
    rust_run = _timed_run('gb', 1.0, _required_stages(1.0), {'reader_rows': 10}, summary)
    python_run = _timed_run('gb', 2.0, _required_stages(2.0), {}, summary)
    _install_fake_runners(monkeypatch, rust_runs=(rust_run,) * 6, python_runs=(python_run,) * 6)
    input_path, rust_executable = _benchmark_paths(tmp_path)
    input_path = input_path.resolve()
    rust_executable = rust_executable.resolve()
    input_hash_calls = 0

    def fake_sha256(path: Path) -> str:
        nonlocal input_hash_calls
        if path == input_path:
            input_hash_calls += 1
            return _INPUT_SHA if input_hash_calls == 1 else _CHANGED_SHA
        assert path == rust_executable
        return _BINARY_SHA

    monkeypatch.setattr(benchmark, '_sha256', fake_sha256)

    result = benchmark.run_check_only_payload_benchmark('gb', input_path, rust_executable)

    assert result.input_sha256 == _INPUT_SHA
    assert result.verdict == 'INCOMPLETE_EVIDENCE'
    assert (
        benchmark.ValidationFailure(
            'INCOMPLETE_EVIDENCE',
            'input SHA-256 changed during benchmark',
        )
        in result.validation_failures
    )


@pytest.mark.parametrize(
    ('changed_evidence', 'expected_message'),
    [
        ('binary', 'Rust binary SHA-256 changed during benchmark'),
        ('git', 'Git working state changed during benchmark'),
    ],
)
def test_check_only_benchmark_rejects_binary_or_working_tree_change(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    changed_evidence: str,
    expected_message: str,
) -> None:
    summary = _summary()
    rust_run = _timed_run('gb', 1.0, _required_stages(1.0), {'reader_rows': 10}, summary)
    python_run = _timed_run('gb', 2.0, _required_stages(2.0), {}, summary)
    _install_fake_runners(monkeypatch, rust_runs=(rust_run,) * 6, python_runs=(python_run,) * 6)
    input_path, rust_executable = _benchmark_paths(tmp_path)
    input_path = input_path.resolve()
    rust_executable = rust_executable.resolve()

    if changed_evidence == 'binary':
        binary_hash_calls = 0

        def fake_sha256(path: Path) -> str:
            nonlocal binary_hash_calls
            if path == rust_executable:
                binary_hash_calls += 1
                return _BINARY_SHA if binary_hash_calls == 1 else _CHANGED_SHA
            assert path == input_path
            return _INPUT_SHA

        monkeypatch.setattr(benchmark, '_sha256', fake_sha256)
    else:
        git_calls = 0

        def fake_git_evidence() -> tuple[str, str]:
            nonlocal git_calls
            git_calls += 1
            return ('git-head', 'working-tree-diff' if git_calls == 1 else 'changed-working-tree-diff')

        monkeypatch.setattr(benchmark, '_git_evidence', fake_git_evidence)

    result = benchmark.run_check_only_payload_benchmark('gb', input_path, rust_executable)

    assert result.verdict == 'INCOMPLETE_EVIDENCE'
    assert benchmark.ValidationFailure('INCOMPLETE_EVIDENCE', expected_message) in result.validation_failures


def test_compare_non_target_stage_medians_reports_more_than_five_percent(tmp_path: Path) -> None:
    baseline_path = tmp_path / '基线.json'
    current_path = tmp_path / '当前.json'
    baseline = {
        'pipeline': 'gb',
        'input_sha256': _INPUT_SHA,
        'rust_stage_median_seconds': {
            'ingest': 10,
            'normalize': 10,
            'split': 10,
            'fact': 0,
            'presentation': 0,
            'total': 10,
        },
    }
    current = {
        'pipeline': 'gb',
        'input_sha256': _INPUT_SHA,
        'rust_stage_median_seconds': {
            'ingest': 10.5,
            'normalize': 20,
            'split': 10.5001,
            'fact': 0,
            'presentation': 1,
            'total': 20,
        },
    }
    baseline_path.write_text(json.dumps(baseline, ensure_ascii=False), encoding='utf-8')
    current_path.write_text(json.dumps(current, ensure_ascii=False), encoding='utf-8')

    regressions = benchmark.compare_non_target_stage_medians(
        baseline_path,
        current_path,
        target_stages=frozenset({'normalize'}),
    )

    assert tuple(regression.stage for regression in regressions) == ('split', 'presentation')
    assert regressions[0].baseline_median_seconds == 10.0
    assert regressions[0].current_median_seconds == 10.5001
    assert regressions[0].current_to_baseline_ratio == pytest.approx(1.05001)
    assert math.isinf(regressions[1].current_to_baseline_ratio)

    current['rust_stage_median_seconds']['fact'] = True
    current_path.write_text(json.dumps(current), encoding='utf-8')
    with pytest.raises(AssertionError, match="stage 'fact'.*finite non-negative number"):
        benchmark.compare_non_target_stage_medians(
            baseline_path,
            current_path,
            target_stages=frozenset({'normalize'}),
        )

    current['rust_stage_median_seconds']['fact'] = math.inf
    current_path.write_text(json.dumps(current), encoding='utf-8')
    with pytest.raises(AssertionError, match="stage 'fact'.*finite non-negative number"):
        benchmark.compare_non_target_stage_medians(
            baseline_path,
            current_path,
            target_stages=frozenset({'normalize'}),
        )


def test_assert_same_input_sha256_rejects_mixed_evidence(tmp_path: Path) -> None:
    first = tmp_path / '第一份.json'
    second = tmp_path / '第二份.json'
    third = tmp_path / '第三份.json'
    first.write_text(json.dumps({'input_sha256': _INPUT_SHA}), encoding='utf-8-sig')
    second.write_text(json.dumps({'input_sha256': _INPUT_SHA}), encoding='utf-8')
    third.write_text(json.dumps({'input_sha256': _CHANGED_SHA}), encoding='utf-8')

    assert benchmark.assert_same_input_sha256((first, second)) == _INPUT_SHA
    with pytest.raises(AssertionError, match='performance evidence input SHA-256 mismatch'):
        benchmark.assert_same_input_sha256((first, second, third))


def test_check_only_result_writer_uses_utf8_json(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    five = (1.0,) * 5
    runtime_evidence = benchmark.RuntimeEvidence(
        run_counts={'reader_rows': 10},
        error_log_count=1,
        issue_type_counts={'缺失金额': 1},
        quality_metrics=(benchmark.QualityMetricEvidence('行数勾稽', '总表行数', '10'),),
    )
    result = benchmark.CheckOnlyBenchmarkResult(
        pipeline='gb',
        input_sha256=_INPUT_SHA,
        rust_executable='D:/成本核算/costing-calculate.exe',
        rust_binary_sha256=_BINARY_SHA,
        git_head='git-head',
        working_tree_diff_id='working-tree-diff',
        working_directory='D:/成本核算',
        command_arguments=('gb', '--input', 'D:/成本核算/输入.xlsx', '--check-only', '--benchmark'),
        python_payload_total_seconds=(2.0,) * 5,
        rust_payload_total_seconds=five,
        python_median_seconds=2.0,
        python_min_seconds=2.0,
        python_max_seconds=2.0,
        rust_median_seconds=1.0,
        rust_min_seconds=1.0,
        rust_max_seconds=1.0,
        rust_stage_seconds=dict.fromkeys(REQUIRED_RUST_PAYLOAD_STAGES, five),
        rust_stage_median_seconds=dict.fromkeys(REQUIRED_RUST_PAYLOAD_STAGES, 1.0),
        rust_runtime_evidence=runtime_evidence,
        valid_pair_count=5,
        validation_passed=True,
        verdict='VALIDATED',
    )
    monkeypatch.setattr(benchmark, 'repo_root', lambda: tmp_path)
    output_path = tmp_path / 'rust' / 'target' / '性能证据.json'

    benchmark.write_local_check_only_result(result, output_path)

    raw = output_path.read_bytes()
    assert not raw.startswith(b'\xef\xbb\xbf')
    assert '成本核算'.encode() in raw
    assert b'\\u6210\\u672c' not in raw
    payload = json.loads(raw.decode('utf-8'))
    assert payload['rust_runtime_evidence']['quality_metrics'] == [
        {'category': '行数勾稽', 'metric': '总表行数', 'value': '10'}
    ]
    assert payload['command_arguments'] == ['gb', '--input', 'D:/成本核算/输入.xlsx', '--check-only', '--benchmark']


def test_local_check_only_writer_rejects_destination_outside_local_roots(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(benchmark, 'repo_root', lambda: tmp_path)
    with pytest.raises(AssertionError, match='rust/target or data/processed'):
        benchmark.write_local_check_only_result(object(), tmp_path / 'docs' / 'evidence.json')  # type: ignore[arg-type]


def test_local_check_only_writer_rejects_raw_symlink_component(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(benchmark, 'repo_root', lambda: tmp_path)
    real_directory = tmp_path / 'outside'
    real_directory.mkdir()
    link = tmp_path / 'rust' / 'target' / 'linked'
    link.parent.mkdir(parents=True)
    try:
        link.symlink_to(real_directory, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f'symlink creation is unavailable: {exc}')

    with pytest.raises(AssertionError, match='reparse|symlink'):
        benchmark.write_local_check_only_result(object(), link / 'result.json')  # type: ignore[arg-type]


def test_local_check_only_writer_supports_valid_path_longer_than_260_characters(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(benchmark, 'repo_root', lambda: tmp_path)
    output = tmp_path / 'rust' / 'target' / ('a' * 120) / ('b' * 120) / 'result.json'
    benchmark.write_local_check_only_result(benchmark.ValidationFailure('VALIDATED', 'ok'), output)  # type: ignore[arg-type]
    assert _io_path(output).read_text(encoding='utf-8')

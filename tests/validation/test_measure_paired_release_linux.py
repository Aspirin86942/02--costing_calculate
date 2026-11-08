from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

from tools.validation import measure_paired_release as measure

pytestmark = pytest.mark.skipif(sys.platform != 'linux', reason='Linux wait4 measurement')


def _process(directory: Path, code: str, **limits: Any) -> dict[str, Any]:
    return measure.run_process(
        [sys.executable, '-I', '-c', code],
        directory=directory,
        timeout=limits.get('timeout', 5),
        memory_limit_bytes=limits.get('memory', 512 * 1024**2),
        disk_limit_bytes=limits.get('disk', 16 * 1024**2),
    )


def test_peak_rss_does_not_include_large_parent_process(tmp_path: Path) -> None:
    parent_data = bytearray(96 * 1024**2)
    small = _process(tmp_path, "print('{}')")
    assert len(parent_data) == 96 * 1024**2
    assert small['peak_rss_bytes'] < 64 * 1024**2


def test_peak_rss_is_per_child_and_large_stderr_does_not_deadlock(tmp_path: Path) -> None:
    large = _process(tmp_path, "import sys; data=b'x'*(64*1024*1024); sys.stderr.write('x'*131072); print('{}')")
    small = _process(tmp_path, "print('{}')")
    assert large['peak_rss_bytes'] > small['peak_rss_bytes'] + 32 * 1024**2
    assert large['external_wall_seconds'] > 0
    assert small['payload'] == {}


@pytest.mark.parametrize(
    ('code', 'limits', 'error_code'),
    [
        ('import time; time.sleep(5)', {'timeout': 0.05}, 'RUN_TIMEOUT'),
        ('raise SystemExit(7)', {}, 'CHILD_FAILED'),
        ("print('not json')", {}, 'INVALID_STDOUT'),
        ("print('[]')", {}, 'INVALID_STDOUT'),
        (
            "from pathlib import Path; Path('large').write_bytes(b'x'*131072); print('{}')",
            {'disk': 65536},
            'DISK_LIMIT',
        ),
        ("data=b'x'*(64*1024*1024); print('{}')", {'memory': 24 * 1024**2}, 'MEMORY_LIMIT'),
    ],
)
def test_process_failures_are_structured(tmp_path: Path, code: str, limits: dict[str, Any], error_code: str) -> None:
    with pytest.raises(measure.MeasurementError) as failure:
        _process(tmp_path, code, **limits)
    payload = failure.value.payload()
    assert payload['error_code'] == error_code
    assert set(payload) == {'error_code', 'message', 'retryable'}
    assert str(tmp_path) not in json.dumps(payload)


def _pair(left: float, right: float) -> dict[str, Any]:
    return {
        'baseline': {
            'external_wall_seconds': left,
            'peak_rss_bytes': 100,
            'output_size_bytes': 20,
            'stage_timings': {'ingest': left},
        },
        'candidate': {
            'external_wall_seconds': right,
            'peak_rss_bytes': 80,
            'output_size_bytes': 19,
            'stage_timings': {'ingest': right},
        },
    }


def test_paired_statistics_use_individual_ratios_and_noise_cannot_cancel() -> None:
    pairs = [_pair(10, 9), _pair(100, 110), _pair(20, 18), _pair(200, 220)]
    stats = measure.paired_statistics(pairs)['external_wall_seconds']
    assert stats['paired_median_improvement'] == 0
    assert stats['median_absolute_paired_difference'] == pytest.approx(0.1)
    assert stats['candidate_wins'] == 2
    assert measure.noise_check(pairs)['passed'] is False
    assert 'wall_absolute_pair_noise' in measure.noise_check(pairs)['reasons']


def _mock_run(*, role: str, alias: str, **_: Any) -> dict[str, Any]:
    return {
        'role': role,
        'run_alias': alias,
        'external_wall_seconds': 1 if role == 'baseline' else 0.9,
        'peak_rss_bytes': 1024,
        'output_size_bytes': 20,
        'output_sha256': '0' * 64,
        'stage_timings': {'ingest': 0.1},
        'summary': {'business': 'unchanged'},
        'temporary_residue_count': 0,
        'low_memory_writer': False,
        'build_identity': {},
    }


@pytest.fixture
def arguments(tmp_path: Path) -> dict[str, Any]:
    source = tmp_path / 'private-source.xlsx'
    source.write_bytes(b'synthetic bytes')
    return {
        'baseline_binary': Path(sys.executable),
        'candidate_binary': Path(sys.executable),
        'input_path': source,
        'output_dir': tmp_path / 'measurement',
        'pipeline': 'gb',
        'pairs': 2,
    }


def test_alternating_order_checkpoint_and_no_sensitive_paths(
    arguments: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []

    def run(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs['role'])
        return _mock_run(**kwargs)

    monkeypatch.setattr(measure, '_one_run', run)
    report = measure.measure_pairs(**arguments)
    assert report['status'] == 'valid'
    assert calls == ['baseline', 'candidate', 'baseline', 'candidate', 'candidate', 'baseline']
    assert report['samples'][0]['execution_order'] == ['baseline', 'candidate']
    assert report['samples'][1]['execution_order'] == ['candidate', 'baseline']
    saved = json.loads((arguments['output_dir'] / 'report.json').read_text(encoding='utf-8'))
    assert saved == report
    assert saved['checkpoint_sha256'] == measure._checkpoint_digest(saved)
    assert str(arguments['input_path']) not in json.dumps(saved)
    assert arguments['input_path'].name not in json.dumps(saved)
    schema_path = Path(measure.__file__).with_name('measure_paired_release.linux.schema.json')
    schema = json.loads(schema_path.read_text(encoding='utf-8'))
    assert schema['additionalProperties'] is False
    assert set(saved) == set(schema['required']) == set(schema['properties'])
    assert set(saved['samples'][0]) == set(schema['properties']['samples']['items']['required'])


def test_existing_output_is_preserved(arguments: dict[str, Any]) -> None:
    arguments['output_dir'].mkdir()
    marker = arguments['output_dir'] / 'report.json'
    marker.write_text('existing', encoding='utf-8')
    with pytest.raises(measure.MeasurementError, match='already exists'):
        measure.measure_pairs(**arguments)
    assert marker.read_text(encoding='utf-8') == 'existing'


def test_changed_artifact_invalidates_whole_batch(arguments: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    def run(**kwargs: Any) -> dict[str, Any]:
        arguments['input_path'].write_bytes(b'changed')
        return _mock_run(**kwargs)

    monkeypatch.setattr(measure, '_one_run', run)
    report = measure.measure_pairs(**arguments)
    assert report['status'] == 'invalid'
    assert report['error']['error_code'] == 'ARTIFACT_IDENTITY_CHANGED'
    assert report['samples'] == []


def test_business_difference_fails_before_timing_pairs(
    arguments: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    def run(**kwargs: Any) -> dict[str, Any]:
        result = _mock_run(**kwargs)
        if kwargs['role'] == 'candidate':
            result['summary'] = {'business': 'changed'}
        return result

    monkeypatch.setattr(measure, '_one_run', run)
    report = measure.measure_pairs(**arguments)
    assert report['status'] == 'invalid'
    assert report['error']['error_code'] == 'BUSINESS_MISMATCH'
    assert report['samples'] == []


def _interrupt(arguments: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    count = 0

    def run(**kwargs: Any) -> dict[str, Any]:
        nonlocal count
        count += 1
        if count == 5:
            raise KeyboardInterrupt
        return _mock_run(**kwargs)

    monkeypatch.setattr(measure, '_one_run', run)
    return measure.measure_pairs(**arguments)


def test_resume_rewarms_and_keeps_only_completed_pairs(
    arguments: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    interrupted = _interrupt(arguments, monkeypatch)
    assert interrupted['status'] == 'interrupted'
    assert len(interrupted['samples']) == 1
    monkeypatch.setattr(measure, '_one_run', _mock_run)
    resumed = measure.measure_pairs(**arguments, resume=True)
    assert resumed['status'] == 'valid'
    assert resumed['attempt'] == 2
    assert len(resumed['warmups']) == 4
    assert [pair['pair_number'] for pair in resumed['samples']] == [1, 2]
    assert resumed['samples'][1]['candidate']['run_alias'].startswith('attempt-2-')
    assert interrupted['samples'][0] == resumed['samples'][0]


def test_resume_rejects_changed_input_session_or_corrupt_checkpoint(
    arguments: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _interrupt(arguments, monkeypatch)
    original = arguments['input_path'].read_bytes()
    arguments['input_path'].write_bytes(b'new input')
    with pytest.raises(measure.MeasurementError) as failure:
        measure.measure_pairs(**arguments, resume=True)
    assert failure.value.code == 'RESUME_IDENTITY_CHANGED'
    arguments['input_path'].write_bytes(original)
    environment = measure._environment()
    monkeypatch.setattr(measure, '_environment', lambda: {**environment, 'session_sha256': 'f' * 64})
    with pytest.raises(measure.MeasurementError) as failure:
        measure.measure_pairs(**arguments, resume=True)
    assert failure.value.code == 'RESUME_IDENTITY_CHANGED'
    monkeypatch.setattr(measure, '_environment', lambda: {**environment, 'cpu_affinity': [999]})
    with pytest.raises(measure.MeasurementError) as failure:
        measure.measure_pairs(**arguments, resume=True)
    assert failure.value.code == 'RESUME_IDENTITY_CHANGED'
    monkeypatch.setattr(measure, '_environment', lambda: environment)
    report_path = arguments['output_dir'] / 'report.json'
    report = json.loads(report_path.read_text(encoding='utf-8'))
    report['samples'] = []
    report_path.write_text(json.dumps(report), encoding='utf-8')
    with pytest.raises(measure.MeasurementError) as failure:
        measure.measure_pairs(**arguments, resume=True)
    assert failure.value.code == 'RESUME_CORRUPT'


def test_completed_report_is_not_resumed(arguments: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(measure, '_one_run', _mock_run)
    measure.measure_pairs(**arguments)
    with pytest.raises(measure.MeasurementError) as failure:
        measure.measure_pairs(**arguments, resume=True)
    assert failure.value.code == 'RESUME_UNAVAILABLE'


# 最小的假 CLI：写出与真实程序同形状的控制台摘要和 Manifest，用来覆盖 _one_run 的校验分支。
FAKE_CLI = """#!__PYTHON__ -I
import hashlib, json, sys
from pathlib import Path

args = sys.argv[1:]


def value(flag):
    return args[args.index(flag) + 1] if flag in args else None


source = Path(value('--input')).read_bytes()
check_only = '--check-only' in args
output = None if check_only else Path(value('--output'))
if output is not None:
    output.write_bytes(b'workbook-bytes')
counts = {'reader_rows': 3, 'detail_rows': 2, 'qty_rows': 1, 'qty_sheet_rows': 1, 'work_order_rows': 1,
          'quality_metric_count': 1}
if Path(__file__).with_name('mismatch').exists():
    counts = {**counts, 'detail_rows': 99}
metrics = [{'category': 'c', 'metric': 'm', 'value': '1', 'description': 'd'}]
summary = {'status': 'succeeded', 'pipeline': args[0], 'output_written': not check_only, 'sheet_count': 3,
           'error_log_count': 0, 'issue_type_counts': {}, 'quality_metrics': metrics, 'run_counts': counts,
           'stage_timings': {'stages': {'ingest': 0.1, 'total': 0.2}}}
manifest = {
    'schema_version': 1, 'status': 'succeeded',
    'application': {'name': 'costing-calculate', 'version': '0', 'git_commit': '0' * 40,
                    'build_timestamp': 't', 'rustc_version': 'r', 'target': 'x'},
    'execution': {'mode': 'check-only' if check_only else 'normal', 'pipeline': args[0], 'low_memory_writer': False},
    'input': {'sha256': hashlib.sha256(source).hexdigest(), 'size_bytes': len(source)},
    'config': {'effective_sha256': 'a' * 64},
    'result': {'output_written': not check_only, 'final_output_valid': not check_only,
               'sheet_names': ['a', 'b', 'c'],
               'output_sha256': None if check_only else hashlib.sha256(output.read_bytes()).hexdigest(),
               'output_size_bytes': None if check_only else output.stat().st_size},
    'run_counts': {'reader_rows': 3, 'detail_rows': 2, 'qty_rows': 1, 'qty_sheet_rows': 1, 'work_order_rows': 1,
                   'quality_metric_count': 1},
    'quality': {'error_log_count': 0, 'issue_type_counts': {}, 'quality_metrics': metrics},
    'warnings': __WARNINGS__,
}
Path(value('--summary-output')).write_text(json.dumps(manifest))
print(json.dumps(summary))
"""


def _fake_run(tmp_path: Path, *, warnings: list[Any], mode: str = 'normal', keep: bool = False) -> dict[str, Any]:
    cli = tmp_path / 'fake-costing'
    cli.write_text(
        FAKE_CLI.replace('__PYTHON__', sys.executable).replace('__WARNINGS__', json.dumps(warnings)),
        encoding='utf-8',
    )
    cli.chmod(0o755)
    source = tmp_path / 'input.xlsx'
    source.write_bytes(b'input')
    root = tmp_path / 'runs'
    root.mkdir(exist_ok=True)
    options = {
        'pipeline': 'gb',
        'mode': mode,
        'timeout_seconds': 10,
        'memory_limit_bytes': 512 * 1024**2,
        'disk_limit_bytes': 16 * 1024**2,
        'month_start': None,
        'month_end': None,
    }
    return measure._one_run(
        role='baseline',
        alias=f'run-{mode}-{keep}',
        binary=cli,
        root=root,
        input_path=source,
        config_path=None,
        options=options,
        input_identity=measure.file_identity(source),
        keep_output=keep,
    )


def test_one_run_hashes_warnings_and_removes_published_workbook(tmp_path: Path) -> None:
    result = _fake_run(tmp_path, warnings=['absolute /private/path warning'])
    assert result['summary']['warning_count'] == 1
    assert 'private' not in json.dumps(result)
    assert result['output_size_bytes'] == len(b'workbook-bytes')
    assert not (tmp_path / 'runs' / 'run-normal-False' / 'output.xlsx').exists()
    kept = _fake_run(tmp_path, warnings=[], keep=True)
    assert (tmp_path / 'runs' / 'run-normal-True' / 'output.xlsx').is_file()
    assert kept['summary']['warning_count'] == 0


def test_one_run_check_only_has_no_workbook_identity(tmp_path: Path) -> None:
    result = _fake_run(tmp_path, warnings=[], mode='check-only')
    assert result['output_sha256'] is None
    assert result['output_size_bytes'] == 0


def test_one_run_rejects_manifest_and_warning_shape_problems(tmp_path: Path) -> None:
    with pytest.raises(measure.MeasurementError) as shape:
        _fake_run(tmp_path, warnings=[{'code': 1}])
    assert shape.value.code == 'INVALID_MANIFEST'
    (tmp_path / 'mismatch').write_text('x', encoding='utf-8')
    with pytest.raises(measure.MeasurementError) as mismatch:
        _fake_run(tmp_path, warnings=[], keep=True)
    assert mismatch.value.code == 'MANIFEST_MISMATCH'


def test_experiment_deadline_stops_before_starting_another_process(
    arguments: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    def unexpected(**_: Any) -> dict[str, Any]:
        pytest.fail('Expired experiment must not start another run')

    monkeypatch.setattr(measure, '_one_run', unexpected)
    report = measure.measure_pairs(**arguments, deadline=0)
    assert report['status'] == 'invalid'
    assert report['error']['error_code'] == 'BUDGET_EXHAUSTED'
    assert report['samples'] == []

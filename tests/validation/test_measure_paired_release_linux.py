from __future__ import annotations

import json
import stat
import subprocess
import sys
import time
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


def test_resident_memory_handles_exit_between_proc_open_and_read(monkeypatch: pytest.MonkeyPatch) -> None:
    with subprocess.Popen(  # noqa: S603 - controlled short-lived test child, no shell.
        [sys.executable, '-I', '-c', 'import sys; sys.stdin.buffer.read(1)'], stdin=subprocess.PIPE
    ) as child:
        status = Path(f'/proc/{child.pid}/status')
        with status.open() as stream:
            assert child.stdin is not None
            child.stdin.close()
            assert child.wait(timeout=10) == 0
            # Linux 对已打开但进程已回收的 proc 文件返回 ESRCH，而不是 ENOENT。
            with monkeypatch.context() as patch:
                patch.setattr(Path, 'read_text', lambda *_args, **_kwargs: stream.read())
                assert measure._resident_bytes(child.pid) == 0


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
        'manifest_validation': 'passed',
        'unknown_fields': [],
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


# 最小假 CLI 支持独立版本查询、真实 XLSX 元数据及可控的失败分支。
FAKE_CLI = """#!__PYTHON__ -I
import hashlib, json, sys
from pathlib import Path

args = sys.argv[1:]
control_path = Path(__file__).with_name('behavior.json')
control = json.loads(control_path.read_text()) if control_path.exists() else {}
application = {'name': 'costing-calculate', 'version': '0', 'git_commit': '0' * 40,
               'build_timestamp': 't', 'rustc_version': 'r', 'target': 'x'}
if args == ['--version-json']:
    log = Path(__file__).with_name('version-queries')
    with log.open('a') as stream:
        stream.write('query\\n')
    if control.get('invalid_version'):
        application['git_commit'] = '/private/secret'
    print(json.dumps({**application, 'config_schema_version': 1, 'run_manifest_schema_version': 1}))
    raise SystemExit
Path(__file__).with_name('arguments.json').write_text(json.dumps(args))
if control.get('stderr'):
    sys.stderr.write(control['stderr'])


def value(flag):
    return args[args.index(flag) + 1] if flag in args else None


def present(path):
    try:
        return str(path.relative_to(Path.cwd()))
    except ValueError:
        return path.name


source_path = Path(value('--input'))
source = source_path.read_bytes()
config_path = Path(value('--config')) if value('--config') else None
check_only = '--check-only' in args
output = None if check_only else Path(value('--output'))
if control.get('check_only_output'):
    Path('output.xlsx').write_bytes(b'unexpected')
if output is not None and not control.get('missing_output'):
    import xlsxwriter
    with xlsxwriter.Workbook(output) as workbook:
        for name in ['成本计算单总表', '成本计算单数量聚合维度', '成本分析工单维度']:
            title = 'wrong' if control.get('wrong_sheets') and name == '成本计算单总表' else name
            sheet = workbook.add_worksheet(title)
            sheet.write_row(0, 0, ['月份', '说明'])
            if not control.get('empty') and value('--month-start') != '2030-01':
                sheet.write_row(1, 0, ['2026-01', 1])
counts = {'reader_rows': 3, 'detail_rows': 2, 'qty_rows': 1, 'qty_sheet_rows': 1, 'work_order_rows': 1,
          'quality_metric_count': 1}
if control.get('empty') or value('--month-start') == '2030-01':
    counts = {key: 0 for key in counts}
if Path(__file__).with_name('mismatch').exists():
    counts = {**counts, 'detail_rows': 99}
metrics = [{'category': 'c', 'metric': 'm', 'value': '1', 'description': 'd'}]
size = output.stat().st_size if output is not None and output.exists() else None
summary = {'status': 'succeeded', 'pipeline': args[0], 'output_written': not check_only, 'sheet_count': 3,
           'request_id': 'private-request', 'workbook_path': present(output) if output else None,
           'output_size_bytes': size,
           'error_log_count': 0, 'issue_type_counts': {}, 'quality_metrics': metrics, 'run_counts': counts,
           'stage_timings': {'stages': {'ingest': 0.1, 'total': 0.2}}}
if control.get('wrong_size'):
    summary['output_size_bytes'] = 1
manifest = {
    'schema_version': 1, 'status': 'succeeded', 'request_id': summary['request_id'],
    'application': application,
    'execution': {'mode': 'check-only' if check_only else 'normal', 'pipeline': args[0], 'low_memory_writer': False},
    'input': {'sha256': hashlib.sha256(source).hexdigest(), 'size_bytes': len(source),
              'path': present(source_path), 'file_name': source_path.name, 'selected_sheet': 'SyntheticRawInput',
              'reader_rows': counts['reader_rows']},
    'filter': {'month_start': value('--month-start'), 'month_end': value('--month-end')},
    'config': {'schema_version': 1, 'source': 'external' if config_path else 'embedded-default',
               'source_sha256': hashlib.sha256(config_path.read_bytes()).hexdigest() if config_path else None,
               'path': present(config_path) if config_path else None, 'effective_sha256': 'a' * 64},
    'result': {'output_written': not check_only, 'final_output_valid': not check_only,
               'sheet_count': 3, 'sheet_names': ['成本计算单总表', '成本计算单数量聚合维度', '成本分析工单维度'],
               'workbook_path': present(output) if output else None,
               'output_sha256': hashlib.sha256(output.read_bytes()).hexdigest() if size else None,
               'output_size_bytes': size},
    'run_counts': {**counts, 'detail_rows': 2} if Path(__file__).with_name('mismatch').exists() else counts,
    'quality': {'error_log_count': 0, 'issue_type_counts': {}, 'quality_metrics': metrics},
    'warnings': __WARNINGS__,
}
if value('--summary-output'):
    Path(value('--summary-output')).write_text(json.dumps(manifest))
elif control.get('manifest_on_none'):
    Path('manifest.json').write_text('{}')
if control.get('residue'):
    Path('.costing-staging').write_text('leftover')
for key, path in [('change_input', source_path), ('change_config', config_path), ('change_binary', Path(__file__))]:
    if control.get(key) and path:
        with path.open('ab') as stream:
            stream.write(b'changed')
if control.get('invalid_summary_json'):
    print('/private/secret')
elif control.get('invalid_summary_counts'):
    print(json.dumps({**summary, 'issue_type_counts': {'PRIVATE': -1}}))
else:
    print(json.dumps(summary))
"""


def _fake_run(
    tmp_path: Path,
    *,
    warnings: list[Any],
    mode: str = 'normal',
    keep: bool = False,
    audit_mode: str = 'manifest',
    behavior: dict[str, Any] | None = None,
    month_start: str | None = None,
    month_end: str | None = None,
    config_path: Path | None = None,
    build_identity_cache: dict[str, dict[str, str]] | None = None,
    overrides: dict[str, Any] | None = None,
    deadline: float | None = None,
) -> dict[str, Any]:
    cli = tmp_path / 'fake-costing'
    cli.write_text(
        FAKE_CLI.replace('__PYTHON__', sys.executable).replace('__WARNINGS__', repr(warnings)),
        encoding='utf-8',
    )
    cli.chmod(0o755)
    if behavior is not None:
        (tmp_path / 'behavior.json').write_text(json.dumps(behavior), encoding='utf-8')
    source = tmp_path / 'input.xlsx'
    source.write_bytes(b'input')
    root = tmp_path / 'runs'
    root.mkdir(exist_ok=True)
    options = {
        'pipeline': 'gb',
        'mode': mode,
        'audit_mode': audit_mode,
        'purpose': 'screening',
        'timeout_seconds': 10,
        'memory_limit_bytes': 512 * 1024**2,
        'disk_limit_bytes': 16 * 1024**2,
        'month_start': month_start,
        'month_end': month_end,
        **(overrides or {}),
    }
    return measure._one_run(
        role='baseline',
        alias=f'run-{mode}-{keep}',
        binary=cli,
        root=root,
        input_path=source,
        config_path=config_path,
        options=options,
        input_identity=measure.file_identity(source),
        keep_output=keep,
        build_identity_cache=build_identity_cache,
        deadline=deadline,
    )


def test_one_run_hashes_warnings_and_removes_published_workbook(tmp_path: Path) -> None:
    result = _fake_run(tmp_path, warnings=['absolute /private/path warning'])
    assert result['summary']['warning_count'] == 1
    assert 'private' not in json.dumps(result)
    assert result['output_size_bytes'] > 0
    assert result['summary']['sheet_names'] == measure.SHEET_NAMES
    assert result['manifest_validation'] == 'passed'
    assert result['unknown_fields'] == []
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


@pytest.mark.parametrize(
    ('mode', 'empty', 'month_start', 'month_end'),
    [
        ('normal', False, None, None),
        ('normal', True, None, None),
        ('normal', False, '2026-01', '2026-06'),
        ('normal', False, '2030-01', '2030-01'),
        ('check-only', False, None, None),
        ('check-only', True, '2030-01', '2030-01'),
    ],
)
def test_none_verifies_available_evidence_and_marks_unknown_fields(
    tmp_path: Path,
    mode: str,
    empty: bool,
    month_start: str | None,
    month_end: str | None,
) -> None:
    config = tmp_path / 'config.toml'
    config.write_text('synthetic = true', encoding='utf-8')
    result = _fake_run(
        tmp_path,
        warnings=['/private/manifest-only'],
        mode=mode,
        keep=True,
        audit_mode='none',
        behavior={'empty': empty},
        month_start=month_start,
        month_end=month_end,
        config_path=config,
    )
    assert not list((tmp_path / 'runs').rglob('manifest.json'))
    arguments = json.loads((tmp_path / 'arguments.json').read_text(encoding='utf-8'))
    assert '--summary-output' not in arguments
    assert arguments[arguments.index('--config') + 1] == str(config)
    if month_start:
        assert arguments[arguments.index('--month-start') + 1] == month_start
        assert arguments[arguments.index('--month-end') + 1] == month_end
    assert result['manifest_validation'] == 'not-requested'
    assert result['low_memory_writer'] is None
    assert set(result['build_identity']) == set(measure.BUILD_FIELDS)
    unknown = {'effective_config_sha256', 'low_memory_writer', 'warning_count', 'warnings_sha256'}
    if mode == 'check-only':
        unknown.add('sheet_names')
        assert result['output_sha256'] is None
        assert result['output_size_bytes'] == 0
        assert not list((tmp_path / 'runs').rglob('*.xlsx'))
    else:
        workbook = tmp_path / 'runs' / f'run-{mode}-True' / 'output.xlsx'
        assert result['summary']['sheet_names'] == measure.SHEET_NAMES
        assert measure.file_identity(workbook) == {
            'sha256': result['output_sha256'],
            'size_bytes': result['output_size_bytes'],
        }
    assert {key for key, value in result['summary'].items() if value is None} == unknown
    assert set(result['unknown_fields']) == {f'summary.{key}' for key in unknown} | {'low_memory_writer'}
    assert '/private/' not in json.dumps(result)


@pytest.mark.parametrize(
    ('behavior', 'error_code'),
    [
        ({'missing_output': True}, 'OUTPUT_MISSING'),
        ({'wrong_size': True}, 'OUTPUT_IDENTITY_MISMATCH'),
        ({'wrong_sheets': True}, 'OUTPUT_SHEETS_MISMATCH'),
        ({'residue': True}, 'TEMPORARY_RESIDUE'),
        ({'invalid_version': True}, 'INVALID_BUILD_IDENTITY'),
        ({'invalid_summary_json': True}, 'INVALID_STDOUT'),
        ({'invalid_summary_counts': True}, 'INVALID_SUMMARY'),
        ({'manifest_on_none': True}, 'UNEXPECTED_MANIFEST'),
        ({'change_input': True}, 'ARTIFACT_IDENTITY_CHANGED'),
        ({'change_config': True}, 'ARTIFACT_IDENTITY_CHANGED'),
        ({'change_binary': True}, 'ARTIFACT_IDENTITY_CHANGED'),
    ],
)
def test_none_rejects_invalid_or_changed_evidence(
    tmp_path: Path,
    behavior: dict[str, Any],
    error_code: str,
) -> None:
    config = tmp_path / 'config.toml'
    config.write_bytes(b'config')
    with pytest.raises(measure.MeasurementError) as failure:
        _fake_run(tmp_path, warnings=[], audit_mode='none', behavior=behavior, config_path=config)
    assert failure.value.code == error_code
    assert '/private/' not in json.dumps(failure.value.payload())


def test_none_check_only_rejects_workbook_and_preserves_existing_runs(tmp_path: Path) -> None:
    with pytest.raises(measure.MeasurementError) as failure:
        _fake_run(tmp_path, warnings=[], mode='check-only', audit_mode='none', behavior={'check_only_output': True})
    assert failure.value.code == 'CHECK_ONLY_OUTPUT'
    output = tmp_path / 'runs' / 'run-check-only-False' / 'output.xlsx'
    assert output.read_bytes() == b'unexpected'
    with pytest.raises(FileExistsError):
        _fake_run(tmp_path, warnings=[], mode='check-only', audit_mode='none')
    assert output.read_bytes() == b'unexpected'


def test_build_identity_is_cached_outside_paired_measurements(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_run(tmp_path, warnings=[], audit_mode='none')
    observed = []
    original = measure.run_process

    def run(arguments: list[str], **kwargs: Any) -> dict[str, Any]:
        result = original(arguments, **kwargs)
        observed.append((arguments, kwargs, result.copy()))
        if arguments[-1] == '--version-json':
            result['external_wall_seconds'] = 999
        return result

    monkeypatch.setattr(measure, 'run_process', run)
    report = measure.measure_pairs(
        baseline_binary=tmp_path / 'fake-costing',
        candidate_binary=tmp_path / 'fake-costing',
        input_path=tmp_path / 'input.xlsx',
        output_dir=tmp_path / 'pairs',
        pipeline='gb',
        pairs=2,
        audit_mode='none',
        purpose='diagnostic',
    )
    assert report['status'] == 'valid'
    assert report['measurement_only'] is True
    assert report['options']['purpose'] == 'diagnostic'
    assert report['options']['audit_mode'] == 'none'
    assert sum(args[-1] == '--version-json' for args, _, _ in observed) == 1
    timed = [result['external_wall_seconds'] for args, _, result in observed if args[-1] != '--version-json']
    runs = report['warmups'] + [pair[side] for pair in report['samples'] for side in pair['execution_order']]
    assert [run['external_wall_seconds'] for run in runs] == timed
    assert all(set(run['build_identity']) == set(measure.BUILD_FIELDS) for run in runs)
    assert not list((tmp_path / 'pairs').rglob('manifest.json'))


@pytest.mark.parametrize('changed', [{'audit_mode': 'none'}, {'purpose': 'diagnostic'}, {'purpose': 'acceptance'}])
def test_resume_refuses_changed_audit_mode_or_purpose(
    arguments: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    changed: dict[str, str],
) -> None:
    interrupted = _interrupt(arguments, monkeypatch)
    with pytest.raises(measure.MeasurementError) as failure:
        measure.measure_pairs(**arguments, **changed, resume=True)
    assert failure.value.code == 'RESUME_IDENTITY_CHANGED'
    saved = json.loads((arguments['output_dir'] / 'report.json').read_text(encoding='utf-8'))
    assert saved == interrupted


@pytest.mark.parametrize('purpose', [purpose for purpose in measure.PURPOSES if purpose != 'calibration'])
def test_purpose_is_recorded_without_changing_measurement_algorithm(
    arguments: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    purpose: str,
) -> None:
    monkeypatch.setattr(measure, '_one_run', _mock_run)
    report = measure.measure_pairs(**arguments, purpose=purpose)
    assert report['status'] == 'valid'
    assert report['measurement_only'] is True
    assert report['options']['purpose'] == purpose
    assert report['options']['calibration'] is False
    assert len(report['samples']) == 2


@pytest.mark.parametrize('options', [{'audit_mode': 'invalid'}, {'purpose': 'invalid'}])
def test_invalid_mode_or_purpose_is_rejected_before_creating_artifacts(
    arguments: dict[str, Any], options: dict[str, str]
) -> None:
    with pytest.raises(measure.MeasurementError) as failure:
        measure.measure_pairs(**arguments, **options)
    assert failure.value.code == 'INVALID_ARGUMENT'
    assert not arguments['output_dir'].exists()


def test_measure_cli_forwards_modes_and_budgets(
    arguments: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(measure, '_one_run', _mock_run)
    monkeypatch.setattr(
        sys,
        'argv',
        [
            'measure',
            '--baseline-binary',
            str(arguments['baseline_binary']),
            '--candidate-binary',
            str(arguments['candidate_binary']),
            '--input',
            str(arguments['input_path']),
            '--output-dir',
            str(arguments['output_dir']),
            '--pipeline',
            'gb',
            '--pairs',
            '2',
            '--audit-mode',
            'none',
            '--purpose',
            'diagnostic',
            '--memory-limit-mib',
            '128',
            '--disk-limit-mib',
            '64',
        ],
    )
    assert measure.main() == 0
    report = json.loads(capsys.readouterr().out)
    assert report['options']['audit_mode'] == 'none'
    assert report['options']['purpose'] == 'diagnostic'
    assert report['options']['memory_limit_bytes'] == 128 * 1024**2
    assert report['options']['disk_limit_bytes'] == 64 * 1024**2


def _cli_argv(arguments: dict[str, Any], *extra: str) -> list[str]:
    return [
        'measure',
        '--baseline-binary',
        str(arguments['baseline_binary']),
        '--candidate-binary',
        str(arguments['candidate_binary']),
        '--input',
        str(arguments['input_path']),
        '--output-dir',
        str(arguments['output_dir']),
        '--pipeline',
        'gb',
        '--pairs',
        '2',
        *extra,
    ]


def test_defaults_keep_the_manifest_channel_and_screening_purpose(
    arguments: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[dict[str, Any]] = []

    def run(**kwargs: Any) -> dict[str, Any]:
        seen.append(kwargs['options'])
        return _mock_run(**kwargs)

    monkeypatch.setattr(measure, '_one_run', run)
    report = measure.measure_pairs(**arguments)
    assert (report['options']['audit_mode'], report['options']['purpose']) == ('manifest', 'screening')
    assert report['options']['calibration'] is False
    assert {options['audit_mode'] for options in seen} == {'manifest'}


def test_cli_defaults_are_manifest_screening_and_two_gib_budgets(
    arguments: dict[str, Any], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(measure, '_one_run', _mock_run)
    monkeypatch.setattr(sys, 'argv', _cli_argv(arguments))
    assert measure.main() == 0
    options = json.loads(capsys.readouterr().out)['options']
    assert (options['audit_mode'], options['purpose']) == ('manifest', 'screening')
    assert (options['memory_limit_bytes'], options['disk_limit_bytes']) == (2048 * 1024**2, 2048 * 1024**2)


def test_calibration_purpose_requires_the_noise_check(arguments: dict[str, Any]) -> None:
    with pytest.raises(measure.MeasurementError) as failure:
        measure.measure_pairs(**arguments, purpose='calibration')
    assert failure.value.code == 'INVALID_ARGUMENT'
    assert not arguments['output_dir'].exists()


def test_calibration_purpose_records_the_noise_verdict(
    arguments: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    def steady(**kwargs: Any) -> dict[str, Any]:
        return {**_mock_run(**kwargs), 'external_wall_seconds': 1}

    monkeypatch.setattr(measure, '_one_run', steady)
    report = measure.measure_pairs(**arguments, purpose='calibration', calibration=True)
    assert report['status'] == 'valid'
    assert (report['options']['purpose'], report['options']['calibration']) == ('calibration', True)
    assert report['calibration']['passed'] is True


def test_resume_rejects_a_checkpoint_written_before_audit_mode_and_purpose_existed(
    arguments: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    _interrupt(arguments, monkeypatch)
    path = arguments['output_dir'] / 'report.json'
    report = json.loads(path.read_text(encoding='utf-8'))
    del report['options']['audit_mode'], report['options']['purpose']
    measure._save_report(path, report)  # 重新计算校验和，使被拒绝的原因只能是批次身份不同。
    with pytest.raises(measure.MeasurementError) as failure:
        measure.measure_pairs(**arguments, resume=True)
    assert failure.value.code == 'RESUME_IDENTITY_CHANGED'


@pytest.mark.parametrize(
    ('overrides', 'error_code'),
    [({'memory_limit_bytes': 4 * 1024**2}, 'MEMORY_LIMIT'), ({'disk_limit_bytes': 1}, 'DISK_LIMIT')],
)
def test_one_run_budgets_cover_the_version_query(tmp_path: Path, overrides: dict[str, Any], error_code: str) -> None:
    with pytest.raises(measure.MeasurementError) as failure:
        _fake_run(tmp_path, warnings=[], audit_mode='none', overrides=overrides)
    assert failure.value.code == error_code
    assert not (tmp_path / 'arguments.json').exists()  # 计时运行没有启动


@pytest.mark.parametrize('seconds_left', [None, 5])
def test_one_run_forwards_budgets_and_the_deadline_to_every_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, seconds_left: float | None
) -> None:
    calls: list[tuple[list[str], dict[str, Any]]] = []
    original = measure.run_process

    def run(arguments: list[str], **kwargs: Any) -> dict[str, Any]:
        calls.append((arguments, kwargs))
        return original(arguments, **kwargs)

    monkeypatch.setattr(measure, 'run_process', run)
    overrides = {'timeout_seconds': 20, 'memory_limit_bytes': 300 * 1024**2, 'disk_limit_bytes': 8 * 1024**2}
    deadline = None if seconds_left is None else time.monotonic() + seconds_left
    _fake_run(tmp_path, warnings=[], audit_mode='none', overrides=overrides, deadline=deadline)
    assert [arguments[-1] == '--version-json' for arguments, _ in calls] == [True, False]
    for _, kwargs in calls:
        assert kwargs['memory_limit_bytes'] == 300 * 1024**2
        assert kwargs['disk_limit_bytes'] == 8 * 1024**2
        if seconds_left is None:
            assert kwargs['timeout'] == 20
        else:
            assert 0 < kwargs['timeout'] <= seconds_left


def test_one_run_stops_before_starting_any_process_when_the_deadline_has_passed(tmp_path: Path) -> None:
    with pytest.raises(measure.MeasurementError) as failure:
        _fake_run(tmp_path, warnings=[], audit_mode='none', deadline=time.monotonic() - 1)
    assert failure.value.code == 'BUDGET_EXHAUSTED'
    assert not (tmp_path / 'version-queries').exists()
    assert not (tmp_path / 'arguments.json').exists()


def test_one_run_keeps_child_stderr_out_of_the_formal_result(tmp_path: Path) -> None:
    result = _fake_run(tmp_path, warnings=[], behavior={'stderr': 'PRIVATE-STDERR-LINE\n'})
    assert 'PRIVATE-STDERR-LINE' not in json.dumps(result)


def test_report_shapes_match_the_published_schema(
    tmp_path: Path, arguments: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    schema_path = Path(measure.__file__).with_name('measure_paired_release.linux.schema.json')
    schema = json.loads(schema_path.read_text(encoding='utf-8'))
    run_schema = schema['$defs']['run']
    summary_schema = run_schema['properties']['summary']
    options_schema = schema['properties']['options']
    assert options_schema['properties']['audit_mode']['enum'] == list(measure.AUDIT_MODES)
    assert options_schema['properties']['purpose']['enum'] == list(measure.PURPOSES)
    with monkeypatch.context() as patch:  # 只在生成报告时替换 _one_run，随后要用真实的 _one_run
        patch.setattr(measure, '_one_run', _mock_run)
        report = measure.measure_pairs(**arguments)
    assert set(report['options']) == set(options_schema['required']) == set(options_schema['properties'])
    allowed_unknown = set(run_schema['properties']['unknown_fields']['items']['enum'])
    for audit_mode in measure.AUDIT_MODES:
        for mode in ('normal', 'check-only'):
            directory = tmp_path / f'{audit_mode}-{mode}'
            directory.mkdir()
            run = _fake_run(directory, warnings=[], mode=mode, audit_mode=audit_mode)
            assert set(run) == set(run_schema['required']) == set(run_schema['properties'])
            assert set(run['summary']) == set(summary_schema['required']) == set(summary_schema['properties'])
            assert set(run['build_identity']) == set(run_schema['properties']['build_identity']['required'])
            assert run['manifest_validation'] in run_schema['properties']['manifest_validation']['enum']
            assert set(run['unknown_fields']) <= allowed_unknown
            # 未知必须是 null，且逐项列入 unknown_fields；已验证的 Manifest 通道没有未知项。
            assert (run['unknown_fields'] == []) is (audit_mode == 'manifest')
            assert (run['low_memory_writer'] is None) is (audit_mode == 'none')


def _logged(directory: Path, code: str, log: Path, **limits: Any) -> dict[str, Any]:
    return measure.run_process(
        [sys.executable, '-I', '-c', code],
        directory=directory,
        timeout=limits.get('timeout', 5),
        memory_limit_bytes=limits.get('memory', 512 * 1024**2),
        disk_limit_bytes=limits.get('disk', 16 * 1024**2),
        stderr_log=log,
    )


def test_run_process_uses_the_requested_cwd(tmp_path: Path) -> None:
    work, elsewhere = tmp_path / 'work', tmp_path / 'elsewhere'
    work.mkdir()
    elsewhere.mkdir()
    result = measure.run_process(
        [sys.executable, '-I', '-c', "import json, os; print(json.dumps({'cwd': os.getcwd()}))"],
        directory=work,
        timeout=5,
        memory_limit_bytes=512 * 1024**2,
        disk_limit_bytes=16 * 1024**2,
        cwd=elsewhere,
    )
    assert Path(result['payload']['cwd']).resolve() == elsewhere.resolve()


def test_stderr_log_keeps_success_stderr_out_of_the_result(tmp_path: Path) -> None:
    log = tmp_path / 'stderr.log'
    code = "import sys; sys.stderr.write('IO_PROFILE read=1\\nIO_PROFILE write=2\\n'); print('{}')"
    result = _logged(tmp_path, code, log)
    assert log.read_text(encoding='utf-8') == 'IO_PROFILE read=1\nIO_PROFILE write=2\n'
    assert set(result) == {'external_wall_seconds', 'peak_rss_bytes', 'payload'}
    assert 'IO_PROFILE' not in json.dumps(result)
    assert str(log) not in json.dumps(result)
    assert stat.S_IMODE(log.stat().st_mode) == 0o600


def test_no_stderr_log_is_created_unless_requested(tmp_path: Path) -> None:
    result = _process(tmp_path, "import sys; sys.stderr.write('ignored\\n'); print('{}')")
    assert result['payload'] == {}
    assert not list(tmp_path.glob('*.log'))


def test_stderr_log_refuses_to_overwrite_and_starts_no_child(tmp_path: Path) -> None:
    log = tmp_path / 'stderr.log'
    log.write_text('keep', encoding='utf-8')
    marker = tmp_path / 'started'
    code = f"from pathlib import Path; Path({str(marker)!r}).write_text('x'); print('{{}}')"
    with pytest.raises(measure.MeasurementError) as failure:
        _logged(tmp_path, code, log)
    assert failure.value.code == 'LOG_EXISTS'
    assert log.read_text(encoding='utf-8') == 'keep'
    assert not marker.exists()
    assert str(log) not in json.dumps(failure.value.payload())


def test_stderr_log_reports_an_unavailable_destination(tmp_path: Path) -> None:
    with pytest.raises(measure.MeasurementError) as failure:
        _logged(tmp_path, "print('{}')", tmp_path / 'missing' / 'stderr.log')
    assert failure.value.code == 'LOG_UNAVAILABLE'
    assert 'missing' not in json.dumps(failure.value.payload())


@pytest.mark.parametrize(
    ('code', 'limits', 'error_code', 'expected'),
    [
        ("import sys; sys.stderr.write('child failed\\n'); raise SystemExit(7)", {}, 'CHILD_FAILED', b'child failed\n'),
        (
            "import sys, time; sys.stderr.write('partial\\n'); sys.stderr.flush(); time.sleep(5)",
            {'timeout': 0.5},
            'RUN_TIMEOUT',
            b'partial\n',
        ),
        ("import sys; sys.stderr.write('before\\n'); print('not json')", {}, 'INVALID_STDOUT', b'before\n'),
        ("import sys; sys.stderr.write('x' * (3 * 1024**2)); print('{}')", {}, 'CAPTURE_LIMIT', b'xxxx'),
    ],
)
def test_stderr_log_is_kept_and_bounded_when_the_run_fails(
    tmp_path: Path, code: str, limits: dict[str, Any], error_code: str, expected: bytes
) -> None:
    log = tmp_path / 'stderr.log'
    with pytest.raises(measure.MeasurementError) as failure:
        _logged(tmp_path, code, log, **limits)
    assert failure.value.code == error_code
    content = log.read_bytes()
    assert content.startswith(expected)
    assert 0 < len(content) <= measure.STDERR_LOG_LIMIT_BYTES
    assert str(log) not in json.dumps(failure.value.payload())

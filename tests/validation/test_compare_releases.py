import copy
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from tools.validation import compare_releases as release_module
from tools.validation.compare_releases import (
    ValidationError,
    _binary_arguments,
    _run_binary,
    _safe_summary,
    compare_releases,
)


def test_safe_summary_excludes_paths_and_timings() -> None:
    summary = {
        'status': 'succeeded',
        'pipeline': 'gb',
        'sheet_count': 3,
        'error_log_count': 2,
        'output_size_bytes': 123,
        'run_counts': {'reader_rows': 10},
        'workbook_path': 'sensitive/path.xlsx',
        'stage_timings': {'stages': {'total': 1.0}},
    }

    safe = _safe_summary(summary)

    assert safe == {
        'status': 'succeeded',
        'pipeline': 'gb',
        'sheet_count': 3,
        'error_log_count': 2,
        'output_size_bytes': 123,
        'run_counts': {'reader_rows': 10},
    }


def test_binary_arguments_add_optional_month_filter_after_safe_paths() -> None:
    arguments = _binary_arguments(
        Path('baseline.exe'),
        'sk',
        Path('input.xlsx'),
        Path('output.xlsx'),
        month_start='2026-01',
        month_end='2026-06',
    )

    assert arguments == [
        'baseline.exe',
        'sk',
        '--input',
        'input.xlsx',
        '--output',
        'output.xlsx',
        '--redact-paths',
        '--month-start',
        '2026-01',
        '--month-end',
        '2026-06',
    ]


def test_binary_arguments_support_check_only_config_and_manifest() -> None:
    arguments = _binary_arguments(
        Path('candidate'),
        'gb',
        Path('input.xlsx'),
        Path('unused.xlsx'),
        month_start='2026-01',
        month_end=None,
        config_path=Path('synthetic.toml'),
        mode='check-only',
        manifest_path=Path('audit.json'),
    )

    assert '--check-only' in arguments
    assert '--output' not in arguments
    assert arguments[arguments.index('--config') + 1] == 'synthetic.toml'
    assert arguments[arguments.index('--summary-output') + 1] == 'audit.json'
    assert arguments[-2:] == ['--month-start', '2026-01']


@pytest.fixture
def release_pair(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    paths = {}
    for name in ('baseline', 'candidate', 'input.xlsx', 'config.toml'):
        paths[name] = tmp_path / name
        paths[name].write_bytes(name.encode())
    calls = []
    controls = {'summary_mutation': None, 'manifest_mutation': None, 'different_sizes': False}

    def run(binary: Path, pipeline: str, input_path: Path, output_path: Path, **options: Any) -> dict[str, Any]:
        side = binary.name
        calls.append((side, options))
        normal = options['mode'] == 'normal'
        contents = b'workbook' + (b'extra' if controls['different_sizes'] and side == 'candidate' else b'')
        if normal:
            output_path.write_bytes(contents)
        summary = {
            'status': 'succeeded',
            'pipeline': pipeline,
            'request_id': f'private-request-{side}',
            'sheet_count': 3,
            'output_written': normal,
            'workbook_path': release_module._present_path(output_path) if normal else None,
            'output_size_bytes': len(contents) if normal else None,
            'run_counts': {'reader_rows': 10, 'detail_rows': 8, 'qty_rows': 2},
            'error_log_count': 1,
            'issue_type_counts': {'MISSING_AMOUNT': 1},
            'quality_metrics': [
                {'category': '行数勾稽', 'metric': '成本明细输入行数', 'value': '8', 'description': 'count'}
            ],
            'stage_timings': {'stages': {'total': 1 if side == 'baseline' else 10}},
        }
        if side == 'candidate' and controls['summary_mutation'] is not None:
            controls['summary_mutation'](summary)
        config_path = options['config_path']
        manifest = {
            'schema_version': 1,
            'status': 'succeeded',
            'request_id': summary['request_id'],
            'application': {'version': '0.3.0', 'git_commit': side},
            'execution': {'pipeline': pipeline, 'mode': options['mode'], 'low_memory_writer': False},
            'input': {
                'path': release_module._present_path(input_path),
                'file_name': input_path.name,
                'size_bytes': input_path.stat().st_size,
                'sha256': hashlib.sha256(input_path.read_bytes()).hexdigest(),
                'selected_sheet': 'SyntheticRawInput',
                'reader_rows': summary['run_counts']['reader_rows'],
            },
            'filter': {'month_start': options['month_start'], 'month_end': options['month_end']},
            'config': {
                'schema_version': 1,
                'source': 'external' if config_path is not None else 'embedded-default',
                'source_sha256': hashlib.sha256(config_path.read_bytes()).hexdigest() if config_path else None,
                'effective_sha256': 'a' * 64,
                'path': release_module._present_path(config_path) if config_path is not None else None,
            },
            'result': {
                'output_written': normal,
                'workbook_path': release_module._present_path(output_path) if normal else None,
                'output_size_bytes': summary['output_size_bytes'],
                'output_sha256': hashlib.sha256(contents).hexdigest() if normal else None,
                'sheet_count': 3,
                'sheet_names': release_module.SHEET_NAMES,
                'final_output_valid': normal,
            },
            'quality': {key: summary[key] for key in ('error_log_count', 'issue_type_counts', 'quality_metrics')},
            'run_counts': summary['run_counts'],
            'warnings': [],
        }
        if side == 'candidate' and controls['manifest_mutation'] is not None:
            controls['manifest_mutation'](manifest)
        options['manifest_path'].write_text(json.dumps(manifest), encoding='utf-8')
        return copy.deepcopy(summary)

    monkeypatch.setattr(release_module, '_run_binary', run)
    monkeypatch.setattr(release_module, 'workbook_payloads_identical', lambda *_: True)
    return {
        'arguments': {
            'baseline_binary': paths['baseline'],
            'candidate_binary': paths['candidate'],
            'pipeline': 'gb',
            'input_path': paths['input.xlsx'],
            'config_path': paths['config.toml'],
            'output_dir': tmp_path / 'outputs',
        },
        'calls': calls,
        'controls': controls,
    }


def test_compare_records_output_size_separately_and_excludes_volatile_fields(release_pair: dict[str, Any]) -> None:
    release_pair['controls']['different_sizes'] = True

    report = compare_releases(**release_pair['arguments'])

    assert report['status'] == 'passed'
    assert report['output_metrics']['candidate_to_baseline_ratio'] > 1
    assert report['manifest_validation'] == 'passed'
    assert report['comparison_mode'] == 'package-fast-path'
    serialized = json.dumps(report)
    assert str(release_pair['arguments']['output_dir']) not in serialized
    assert 'private-request' not in serialized
    assert 'stage_timings' not in serialized
    assert len(release_pair['calls']) == 2
    assert all(call[1]['config_path'] == release_pair['arguments']['config_path'] for call in release_pair['calls'])


@pytest.mark.parametrize('field', ['run_counts', 'error_log_count', 'issue_type_counts', 'quality_metrics'])
def test_business_summary_difference_fails_with_identical_workbook(release_pair: dict[str, Any], field: str) -> None:
    def mutate(summary: dict[str, Any]) -> None:
        if field == 'run_counts':
            summary[field]['detail_rows'] += 1
        elif field == 'error_log_count':
            summary[field] += 1
        elif field == 'issue_type_counts':
            summary[field] = {'TOTAL_COST_MISMATCH': 1}
        else:
            summary[field][0]['value'] = '9'

    release_pair['controls']['summary_mutation'] = mutate

    report = compare_releases(**release_pair['arguments'])

    assert report['status'] == 'failed'
    assert {'kind': 'business-summary', 'field': field} in report['mismatches']
    assert report['error_code'] == 'RELEASE_MISMATCH'
    assert report['message']
    assert report['retryable'] is False


@pytest.mark.parametrize(
    ('section', 'key', 'value', 'field'),
    [
        ('input', 'sha256', '0' * 64, 'input.sha256'),
        ('input', 'path', '/private/secret/input.xlsx', 'input.path'),
        ('result', 'output_sha256', '0' * 64, 'result.output_sha256'),
        ('result', 'workbook_path', '/private/secret/output.xlsx', 'result.workbook_path'),
        ('config', 'source_sha256', '0' * 64, 'config.source_sha256'),
        ('execution', 'mode', 'check-only', 'execution.mode'),
        ('filter', 'month_start', '2020-01', 'filter'),
    ],
)
def test_invalid_manifest_fails_and_never_reports_its_values(
    release_pair: dict[str, Any], section: str, key: str, value: Any, field: str
) -> None:
    release_pair['controls']['manifest_mutation'] = lambda manifest: manifest[section].update({key: value})

    report = compare_releases(**release_pair['arguments'])

    assert report['status'] == 'failed'
    assert {'kind': 'manifest-validation', 'side': 'candidate', 'field': field} in report['mismatches']
    assert '/private/secret' not in json.dumps(report)


def test_manifest_business_warnings_fail_and_are_not_exposed(release_pair: dict[str, Any]) -> None:
    release_pair['controls']['manifest_mutation'] = lambda manifest: manifest.update(warnings=['private warning'])

    report = compare_releases(**release_pair['arguments'])

    assert report['status'] == 'failed'
    assert {'kind': 'manifest-business', 'field': 'warnings'} in report['mismatches']
    assert 'private warning' not in json.dumps(report)


def test_check_only_compares_summaries_and_manifest_without_workbook(release_pair: dict[str, Any]) -> None:
    report = compare_releases(
        **release_pair['arguments'], mode='check-only', month_start='2030-01', month_end='2030-01'
    )

    assert report['status'] == 'passed'
    assert report['mode'] == 'check-only'
    assert report['comparison_mode'] == 'summary-only'
    assert report['filter'] == {'month_start': '2030-01', 'month_end': '2030-01'}
    assert report['output_metrics']['baseline_size_bytes'] is None
    assert not list(release_pair['arguments']['output_dir'].glob('*.xlsx'))
    assert len(list(release_pair['arguments']['output_dir'].glob('*.manifest.json'))) == 2


@pytest.mark.parametrize('existing', ['baseline-gb.xlsx', 'candidate-gb.manifest.json'])
def test_compare_refuses_overwrite_before_running(release_pair: dict[str, Any], existing: str) -> None:
    output_dir = release_pair['arguments']['output_dir']
    output_dir.mkdir()
    (output_dir / existing).write_bytes(b'keep')

    with pytest.raises(FileExistsError):
        compare_releases(**release_pair['arguments'])

    assert release_pair['calls'] == []
    assert (output_dir / existing).read_bytes() == b'keep'


@pytest.mark.parametrize('failure', ['nonzero', 'invalid-json', 'timeout', 'invalid-structure'])
def test_binary_failures_are_structured_and_redacted(monkeypatch: pytest.MonkeyPatch, failure: str) -> None:
    private_path = '/private/secret/value-123.xlsx'

    def run(*_args: Any, **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        if failure == 'timeout':
            raise subprocess.TimeoutExpired([private_path], 1, output=private_path, stderr=private_path)
        if failure == 'nonzero':
            return subprocess.CompletedProcess([private_path], 1, private_path, private_path)
        if failure == 'invalid-json':
            return subprocess.CompletedProcess([private_path], 0, private_path, private_path)
        return subprocess.CompletedProcess([private_path], 0, json.dumps({'status': private_path}), private_path)

    monkeypatch.setattr(release_module.subprocess, 'run', run)

    with pytest.raises(ValidationError) as captured:
        _run_binary(Path(private_path), 'gb', Path(private_path), Path(private_path), month_start=None, month_end=None)

    assert private_path not in str(captured.value)
    assert captured.value.error_code in {'BINARY_FAILED', 'INVALID_RUN_SUMMARY', 'BINARY_TIMEOUT'}
    assert captured.value.retryable is (failure == 'timeout')


def test_cli_failure_does_not_expose_missing_paths(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        sys,
        'argv',
        [
            'compare_releases',
            '--baseline-binary',
            '/private/secret-baseline',
            '--candidate-binary',
            '/private/secret-candidate',
            '--pipeline',
            'gb',
            '--input',
            '/private/secret-input.xlsx',
            '--output-dir',
            '/private/secret-output',
        ],
    )

    assert release_module.main() == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload['status'] == 'failed'
    assert payload['error_code'] == 'VALIDATION_ERROR'
    assert payload['message']
    assert payload['retryable'] is False
    assert '/private/' not in json.dumps(payload)


def test_cli_invalid_arguments_return_structured_json(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(sys, 'argv', ['compare_releases', '--pipeline', '/private/invalid-value'])

    assert release_module.main() == 2
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload['error_code'] == 'INVALID_ARGUMENT'
    assert payload['retryable'] is False
    assert '/private/' not in captured.out
    assert captured.err == ''

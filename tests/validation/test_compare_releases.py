import copy
import hashlib
import json
import math
import sys
from pathlib import Path
from typing import Any
from zipfile import ZipFile

import pytest
import xlsxwriter

from tools.validation import compare_releases as release_module
from tools.validation.compare_releases import (
    ValidationError,
    _binary_arguments,
    _compare_outputs,
    _run_binary,
    _safe_summary,
    compare_releases,
)
from tools.validation.measure_paired_release import MeasurementError
from tools.validation.workbook_compare import GROUP_KEYS, NUMERIC_COLUMNS

linux_only = pytest.mark.skipif(sys.platform != 'linux', reason='uses POSIX scripts and Linux wait4 limits')
# 往 --output 路径写入 128 KiB 的假程序，用来触发磁盘预算。
DISK_BODY = (
    'import sys\n'
    'from pathlib import Path\n'
    "output = Path(sys.argv[sys.argv.index('--output') + 1])\n"
    "output.write_bytes(b'x' * 131072)\n"
    "print('{}')"
)


def _write_workbook(path: Path, *, variant: str = 'base') -> None:
    """Write a small GB-shaped workbook; variants are semantically equal, different, or mis-named."""
    options = {'constant_memory': True, 'tmpdir': str(path.parent)} if variant == 'inline' else {}
    with xlsxwriter.Workbook(path, options) as workbook:
        for name in release_module.SHEET_NAMES:
            title = 'wrong' if variant == 'wrong-sheets' and name == release_module.SHEET_NAMES[0] else name
            sheet = workbook.add_worksheet(title)
            keys = GROUP_KEYS.get(name, ())
            numeric = NUMERIC_COLUMNS[('gb', name)]
            sheet.write_row(0, 0, [*keys, *numeric])
            labels = {'月份': '2026-01', '产品编码': 'P1', '工单编号': 'W1'}
            amount = 2 if variant == 'amount' else 1
            sheet.write_row(1, 0, [*(labels.get(key, '1') for key in keys), *([amount] * len(numeric))])


def _script_binary(directory: Path, body: str) -> Path:
    path = directory / 'script-binary'
    path.write_text(f'#!{sys.executable} -I\n{body}\n', encoding='utf-8')
    path.chmod(0o755)
    return path


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
    controls: dict[str, Any] = {
        'summary_mutation': None,
        'manifest_mutation': None,
        'different_sizes': False,
        'candidate_variant': 'base',
        'residue': False,
        'missing_output': False,
        'check_only_output': False,
        'change_input': False,
        'manifest_on_none': False,
    }

    def run(binary: Path, pipeline: str, input_path: Path, output_path: Path, **options: Any) -> dict[str, Any]:
        side = binary.name
        calls.append((side, options))
        normal = options['mode'] == 'normal'
        if normal:
            _write_workbook(output_path, variant=controls['candidate_variant'] if side == 'candidate' else 'base')
            if controls['different_sizes'] and side == 'candidate':
                with ZipFile(output_path, 'a') as archive:
                    archive.comment = b'extra'
        size = output_path.stat().st_size if normal else None
        summary = {
            'status': 'succeeded',
            'pipeline': pipeline,
            'request_id': f'private-request-{side}',
            'sheet_count': 3,
            'output_written': normal,
            'workbook_path': release_module._present_path(output_path) if normal else None,
            'output_size_bytes': size,
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
                'output_sha256': release_module._sha256(output_path) if normal else None,
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
        if options['manifest_path'] is not None:
            options['manifest_path'].write_text(json.dumps(manifest), encoding='utf-8')
        elif side == 'candidate' and controls['manifest_on_none']:
            (output_path.parent / f'{side}-{pipeline}.manifest.json').write_text('{}', encoding='utf-8')
        if side == 'candidate':
            if controls['residue']:
                (output_path.parent / '.costing-staging').write_bytes(b'leftover')
            if controls['missing_output']:
                output_path.unlink(missing_ok=True)
            if controls['check_only_output']:
                output_path.write_bytes(b'unexpected')
            if controls['change_input']:
                input_path.write_bytes(b'changed')
        return copy.deepcopy(summary)

    monkeypatch.setattr(release_module, '_run_binary', run)
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


def test_default_call_keeps_the_manifest_channel_and_records_budgets(release_pair: dict[str, Any]) -> None:
    report = compare_releases(**release_pair['arguments'])

    assert report['status'] == 'passed'
    assert report['options'] == {
        'audit_mode': 'manifest',
        'purpose': 'screening',
        'timeout_seconds': 300,
        'memory_limit_bytes': 2 * 1024**3,
        'disk_limit_bytes': 2 * 1024**3,
    }
    assert report['manifest_validation'] == 'passed'
    assert report['unknown_fields'] == []
    assert len(list(release_pair['arguments']['output_dir'].glob('*.manifest.json'))) == 2
    for side, options in release_pair['calls']:
        assert options['manifest_path'].name == f'{side}-gb.manifest.json'
        assert (options['timeout_seconds'], options['memory_limit_bytes'], options['disk_limit_bytes']) == (
            300,
            2 * 1024**3,
            2 * 1024**3,
        )
    for side in ('baseline', 'candidate'):
        assert report['audit_evidence'][side] == {
            'low_memory_writer': False,
            'effective_config_sha256': 'a' * 64,
            'warning_count': 0,
            'warnings_sha256': release_module._json_sha256([]),
        }
        assert report['outputs'][side]['sheet_names'] == release_module.SHEET_NAMES


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
    # 没有工作簿就没有 Sheet 名可验证：必须是 None 并明确列为未知，不能沿用 Manifest 的自我声明。
    assert all(
        report['outputs'][side] == {'sha256': None, 'size_bytes': None, 'sheet_names': None}
        for side in report['outputs']
    )
    assert {'outputs.baseline.sheet_names', 'outputs.candidate.sheet_names'} <= set(report['unknown_fields'])


@pytest.mark.parametrize('existing', ['baseline-gb.xlsx', 'candidate-gb.manifest.json', '.costing-old'])
@pytest.mark.parametrize('audit_mode', ['manifest', 'none'])
def test_compare_refuses_existing_artifacts_before_running(
    release_pair: dict[str, Any], existing: str, audit_mode: str
) -> None:
    output_dir = release_pair['arguments']['output_dir']
    output_dir.mkdir()
    (output_dir / existing).write_bytes(b'keep')

    with pytest.raises(FileExistsError):
        compare_releases(**release_pair['arguments'], audit_mode=audit_mode)

    assert release_pair['calls'] == []
    assert (output_dir / existing).read_bytes() == b'keep'


@pytest.mark.parametrize('failure', ['nonzero', 'invalid-json', 'timeout', 'invalid-structure'])
def test_binary_failures_are_structured_and_redacted(monkeypatch: pytest.MonkeyPatch, failure: str) -> None:
    private_path = '/private/secret/value-123.xlsx'

    def run(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        if failure == 'timeout':
            raise MeasurementError('RUN_TIMEOUT', 'Run exceeded the frozen timeout', retryable=True)
        if failure == 'nonzero':
            raise MeasurementError('CHILD_FAILED', 'Child exited unsuccessfully')
        if failure == 'invalid-json':
            raise MeasurementError('INVALID_STDOUT', 'Child did not return valid JSON')
        return {'payload': {'status': private_path}}

    monkeypatch.setattr(release_module, 'run_process', run)

    with pytest.raises(ValidationError) as captured:
        _run_binary(Path(private_path), 'gb', Path(private_path), Path(private_path), month_start=None, month_end=None)

    assert private_path not in str(captured.value)
    assert captured.value.error_code in {'BINARY_FAILED', 'INVALID_RUN_SUMMARY', 'BINARY_TIMEOUT'}
    assert captured.value.retryable is (failure == 'timeout')


def _summary_mutation(field: str) -> Any:
    def mutate(summary: dict[str, Any]) -> None:
        if field == 'run_counts':
            summary[field]['detail_rows'] += 1
        elif field == 'error_log_count':
            summary[field] += 1
        elif field == 'issue_type_counts':
            summary[field] = {'TOTAL_COST_MISMATCH': 1}
        else:
            summary[field][0]['value'] = '9'

    return mutate


@pytest.mark.parametrize(
    ('mode', 'month_start'),
    [('normal', None), ('normal', '2026-01'), ('normal', '2030-01'), ('check-only', None), ('check-only', '2030-01')],
)
def test_none_creates_no_manifest_and_marks_audit_fields_unknown(
    release_pair: dict[str, Any], mode: str, month_start: str | None
) -> None:
    report = compare_releases(
        **release_pair['arguments'],
        mode=mode,
        audit_mode='none',
        purpose='diagnostic',
        month_start=month_start,
        month_end=month_start,
    )

    assert report['status'] == 'passed'
    assert report['options']['audit_mode'] == 'none'
    assert report['options']['purpose'] == 'diagnostic'
    assert report['manifest_validation'] == 'not-requested'
    assert not list(release_pair['arguments']['output_dir'].glob('*.manifest.json'))
    assert all(options['manifest_path'] is None for _, options in release_pair['calls'])
    assert report['filter'] == {'month_start': month_start, 'month_end': month_start}
    assert report['comparison_mode'] == ('package-fast-path' if mode == 'normal' else 'summary-only')
    for side in ('baseline', 'candidate'):
        # 没有 Manifest 时这些值未知：必须是 None，不能被写成 False 或空串。
        assert report['audit_evidence'][side] == dict.fromkeys(
            ('low_memory_writer', 'effective_config_sha256', 'warning_count', 'warnings_sha256')
        )
        assert f'audit_evidence.{side}.low_memory_writer' in report['unknown_fields']
        evidence = report['outputs'][side]
        if mode == 'normal':
            assert evidence['sheet_names'] == release_module.SHEET_NAMES
            assert len(evidence['sha256']) == 64
            assert evidence['size_bytes'] == report[side]['output_size_bytes'] > 0
        else:
            assert evidence == {'sha256': None, 'size_bytes': None, 'sheet_names': None}
            assert f'outputs.{side}.sheet_names' in report['unknown_fields']
    assert '/private/' not in json.dumps(report)


@pytest.mark.parametrize('field', ['run_counts', 'error_log_count', 'issue_type_counts', 'quality_metrics'])
def test_none_still_rejects_business_summary_differences(release_pair: dict[str, Any], field: str) -> None:
    release_pair['controls']['summary_mutation'] = _summary_mutation(field)

    report = compare_releases(**release_pair['arguments'], audit_mode='none')

    assert report['status'] == 'failed'
    assert {'kind': 'business-summary', 'field': field} in report['mismatches']
    assert report['manifest_validation'] == 'not-requested'
    assert report['error_code'] == 'RELEASE_MISMATCH'


@pytest.mark.parametrize('audit_mode', ['manifest', 'none'])
def test_equivalent_packages_use_the_semantic_comparison(release_pair: dict[str, Any], audit_mode: str) -> None:
    release_pair['controls']['candidate_variant'] = 'inline'

    report = compare_releases(**release_pair['arguments'], audit_mode=audit_mode)

    assert report['status'] == 'passed'
    assert report['comparison_mode'] == 'semantic'
    assert report['mismatches'] == []


@pytest.mark.parametrize('audit_mode', ['manifest', 'none'])
def test_workbook_value_differences_fail_in_both_audit_modes(release_pair: dict[str, Any], audit_mode: str) -> None:
    release_pair['controls']['candidate_variant'] = 'amount'

    report = compare_releases(**release_pair['arguments'], audit_mode=audit_mode)

    assert report['status'] == 'failed'
    assert report['comparison_mode'] == 'semantic'
    assert 'value_mismatch' in {item.get('kind') for item in report['mismatches']}
    assert report['error_code'] == 'RELEASE_MISMATCH'


def test_none_reads_actual_sheet_names_from_zip_metadata(release_pair: dict[str, Any]) -> None:
    release_pair['controls']['candidate_variant'] = 'wrong-sheets'

    report = compare_releases(**release_pair['arguments'], audit_mode='none')

    assert report['status'] == 'failed'
    assert {'kind': 'output-validation', 'side': 'candidate', 'field': 'sheet_names'} in report['mismatches']
    assert report['outputs']['candidate']['sheet_names'][0] == 'wrong'
    assert report['outputs']['baseline']['sheet_names'] == release_module.SHEET_NAMES


@pytest.mark.parametrize('audit_mode', ['manifest', 'none'])
@pytest.mark.parametrize(
    ('control', 'mode', 'expected'),
    [
        ('residue', 'normal', {'kind': 'temporary-residue', 'side': 'candidate'}),
        ('missing_output', 'normal', {'kind': 'output-validation', 'field': 'workbook_exists'}),
        ('check_only_output', 'check-only', {'kind': 'output-validation', 'side': 'candidate', 'field': 'no_workbook'}),
        ('change_input', 'normal', {'kind': 'identity-changed', 'field': 'input_sha256'}),
    ],
)
def test_output_residue_and_identity_failures_are_reported_in_both_modes(
    release_pair: dict[str, Any], audit_mode: str, control: str, mode: str, expected: dict[str, str]
) -> None:
    release_pair['controls'][control] = True

    report = compare_releases(**release_pair['arguments'], mode=mode, audit_mode=audit_mode)

    assert report['status'] == 'failed'
    assert expected in report['mismatches']


def test_none_flags_an_unexpected_manifest(release_pair: dict[str, Any]) -> None:
    release_pair['controls']['manifest_on_none'] = True

    report = compare_releases(**release_pair['arguments'], audit_mode='none')

    assert report['status'] == 'failed'
    assert {'kind': 'unexpected-manifest', 'side': 'candidate'} in report['mismatches']


@pytest.mark.parametrize(
    'changes',
    [
        {'audit_mode': 'summary'},
        {'purpose': 'ranking'},
        {'mode': 'dry-run'},
        {'pipeline': 'xx'},
        {'timeout_seconds': 0},
        {'timeout_seconds': math.nan},
        {'memory_limit_bytes': 0},
        {'disk_limit_bytes': -1},
    ],
)
def test_invalid_modes_and_budgets_are_rejected_before_any_artifact(
    release_pair: dict[str, Any], changes: dict[str, Any]
) -> None:
    with pytest.raises(ValidationError) as failure:
        compare_releases(**{**release_pair['arguments'], **changes})

    assert failure.value.error_code == 'INVALID_ARGUMENT'
    assert release_pair['calls'] == []
    assert not release_pair['arguments']['output_dir'].exists()


def test_budgets_and_purpose_reach_every_correctness_process(release_pair: dict[str, Any]) -> None:
    report = compare_releases(
        **release_pair['arguments'],
        purpose='acceptance',
        timeout_seconds=7.5,
        memory_limit_bytes=1_500_000_000,
        disk_limit_bytes=1_600_000_000,
    )

    assert report['status'] == 'passed'
    assert report['options'] == {
        'audit_mode': 'manifest',
        'purpose': 'acceptance',
        'timeout_seconds': 7.5,
        'memory_limit_bytes': 1_500_000_000,
        'disk_limit_bytes': 1_600_000_000,
    }
    for _, options in release_pair['calls']:
        assert (options['timeout_seconds'], options['memory_limit_bytes'], options['disk_limit_bytes']) == (
            7.5,
            1_500_000_000,
            1_600_000_000,
        )


def test_run_binary_goes_through_run_process_with_budgets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    summary = {
        'status': 'succeeded',
        'pipeline': 'gb',
        'output_written': False,
        'sheet_count': 3,
        'error_log_count': 0,
        'issue_type_counts': {},
        'quality_metrics': [],
        'run_counts': {'reader_rows': 1},
        'request_id': 'r',
        'output_size_bytes': None,
    }

    def run(arguments: list[str], **kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs, arguments=arguments)
        return {'payload': summary}

    monkeypatch.setattr(release_module, 'run_process', run)
    for manifest_path in (None, tmp_path / 'audit.json'):
        result = _run_binary(
            Path('binary'),
            'gb',
            tmp_path / 'input.xlsx',
            tmp_path / 'output.xlsx',
            month_start=None,
            month_end=None,
            mode='check-only',
            manifest_path=manifest_path,
            timeout_seconds=4.5,
            memory_limit_bytes=1_000,
            disk_limit_bytes=2_000,
        )
        assert result == summary
        assert ('--summary-output' in captured['arguments']) is (manifest_path is not None)
        assert (captured['timeout'], captured['memory_limit_bytes'], captured['disk_limit_bytes']) == (
            4.5,
            1_000,
            2_000,
        )
        assert captured['directory'] == tmp_path
        assert captured['cwd'] == release_module.PROJECT_ROOT


@linux_only
def test_real_process_success_returns_the_validated_summary(tmp_path: Path) -> None:
    directory = tmp_path / 'out'
    directory.mkdir()
    binary = _script_binary(
        tmp_path,
        'import json, os, sys\n'
        'from pathlib import Path\n'
        "Path(sys.argv[sys.argv.index('--output') + 1]).write_bytes(b'workbook')\n"
        "print(json.dumps({'status': 'succeeded', 'pipeline': 'gb', 'output_written': True, 'sheet_count': 3,\n"
        "  'error_log_count': 0, 'issue_type_counts': {}, 'quality_metrics': [], 'run_counts': {'reader_rows': 1},\n"
        "  'request_id': 'r', 'output_size_bytes': 8, 'cwd': os.getcwd()}))",
    )

    result = _run_binary(
        binary, 'gb', tmp_path / 'input.xlsx', directory / 'out.xlsx', month_start=None, month_end=None
    )

    assert result['output_size_bytes'] == 8
    assert result['cwd'] == str(release_module.PROJECT_ROOT)
    assert (directory / 'out.xlsx').read_bytes() == b'workbook'


@linux_only
@pytest.mark.parametrize(
    ('body', 'limits', 'error_code', 'retryable'),
    [
        ('import time\ntime.sleep(5)', {'timeout_seconds': 0.2}, 'BINARY_TIMEOUT', True),
        ("data = b'x' * (64 * 1024 * 1024)\nprint('{}')", {'memory_limit_bytes': 24 * 1024**2}, 'MEMORY_LIMIT', False),
        (DISK_BODY, {'disk_limit_bytes': 65536}, 'DISK_LIMIT', False),
        ('raise SystemExit(7)', {}, 'BINARY_FAILED', False),
        ("print('/private/secret')", {}, 'INVALID_RUN_SUMMARY', False),
        ('print(\'{"status": "/private/secret"}\')', {}, 'INVALID_RUN_SUMMARY', False),
    ],
)
def test_real_processes_are_budgeted_and_errors_are_redacted(
    tmp_path: Path, body: str, limits: dict[str, Any], error_code: str, retryable: bool
) -> None:
    directory = tmp_path / 'out'
    directory.mkdir()
    binary = _script_binary(tmp_path, body)

    with pytest.raises(ValidationError) as failure:
        _run_binary(
            binary,
            'gb',
            tmp_path / 'input.xlsx',
            directory / 'out.xlsx',
            month_start=None,
            month_end=None,
            **{'timeout_seconds': 10, **limits},
        )

    assert failure.value.error_code == error_code
    assert failure.value.retryable is retryable
    assert '/private/' not in str(failure.value)
    assert str(tmp_path) not in str(failure.value)


@linux_only
@pytest.mark.parametrize(
    ('limits', 'error_code', 'retryable'),
    [
        ({'timeout_seconds': 0.001}, 'COMPARISON_TIMEOUT', True),
        ({'memory_limit_bytes': 4 * 1024**2}, 'MEMORY_LIMIT', False),
        ({'disk_limit_bytes': 1}, 'DISK_LIMIT', False),
    ],
)
def test_workbook_comparison_worker_is_budgeted(
    tmp_path: Path, limits: dict[str, Any], error_code: str, retryable: bool
) -> None:
    baseline, candidate = tmp_path / 'baseline-gb.xlsx', tmp_path / 'candidate-gb.xlsx'
    _write_workbook(baseline)
    _write_workbook(candidate)
    budgets = {'timeout_seconds': 60, 'memory_limit_bytes': 2 * 1024**3, 'disk_limit_bytes': 2 * 1024**3, **limits}

    with pytest.raises(ValidationError) as failure:
        _compare_outputs(baseline, candidate, pipeline='gb', **budgets)

    assert failure.value.error_code == error_code
    assert failure.value.retryable is retryable
    assert str(tmp_path) not in str(failure.value)


@linux_only
def test_workbook_comparison_worker_returns_structured_errors_for_invalid_workbooks(tmp_path: Path) -> None:
    baseline, candidate = tmp_path / 'baseline-gb.xlsx', tmp_path / 'candidate-gb.xlsx'
    _write_workbook(baseline)
    candidate.write_bytes(b'/private/not-a-zip')

    with pytest.raises(ValidationError) as failure:
        _compare_outputs(
            baseline,
            candidate,
            pipeline='gb',
            timeout_seconds=60,
            memory_limit_bytes=2 * 1024**3,
            disk_limit_bytes=2 * 1024**3,
        )

    assert failure.value.error_code == 'INVALID_WORKBOOK'
    assert failure.value.retryable is False
    assert '/private/' not in str(failure.value)


def _cli_arguments(*extra: str) -> list[str]:
    return [
        'compare_releases',
        '--baseline-binary',
        'baseline',
        '--candidate-binary',
        'candidate',
        '--pipeline',
        'gb',
        '--input',
        'input.xlsx',
        '--output-dir',
        'outputs',
        *extra,
    ]


def test_cli_forwards_audit_mode_purpose_and_budgets(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[dict[str, Any]] = []

    def fake_compare(**kwargs: Any) -> dict[str, Any]:
        captured.append(kwargs)
        return {'status': 'passed'}

    monkeypatch.setattr(release_module, 'compare_releases', fake_compare)
    monkeypatch.setattr(sys, 'argv', _cli_arguments())
    assert release_module.main() == 0
    monkeypatch.setattr(
        sys,
        'argv',
        _cli_arguments(
            '--audit-mode',
            'none',
            '--purpose',
            'diagnostic',
            '--check-only',
            '--memory-limit-mib',
            '64',
            '--disk-limit-mib',
            '32',
            '--timeout-seconds',
            '9',
        ),
    )
    assert release_module.main() == 0

    defaults, explicit = captured
    assert (defaults['audit_mode'], defaults['purpose'], defaults['mode']) == ('manifest', 'screening', 'normal')
    assert (defaults['timeout_seconds'], defaults['memory_limit_bytes'], defaults['disk_limit_bytes']) == (
        300,
        2048 * 1024**2,
        2048 * 1024**2,
    )
    assert (explicit['audit_mode'], explicit['purpose'], explicit['mode']) == ('none', 'diagnostic', 'check-only')
    assert (explicit['timeout_seconds'], explicit['memory_limit_bytes'], explicit['disk_limit_bytes']) == (
        9,
        64 * 1024**2,
        32 * 1024**2,
    )


@pytest.mark.parametrize('option', [['--audit-mode', 'summary'], ['--purpose', 'ranking']])
def test_cli_rejects_unknown_audit_mode_or_purpose_as_structured_json(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], option: list[str]
) -> None:
    monkeypatch.setattr(sys, 'argv', _cli_arguments(*option))

    assert release_module.main() == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload['error_code'] == 'INVALID_ARGUMENT'
    assert payload['retryable'] is False


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

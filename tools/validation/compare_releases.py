"""Compare costing binaries, business summaries and Manifests without exposing cell values."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sys
from pathlib import Path
from typing import Any, Literal
from xml.etree.ElementTree import ParseError
from zipfile import BadZipFile

from tools.validation.measure_paired_release import (
    AUDIT_MODES,
    PURPOSES,
    SHEET_NAMES,
    MeasurementError,
    file_identity,
    run_process,
    workbook_sheet_names,
)
from tools.validation.types import PipelineName
from tools.validation.workbook_compare import compare_workbooks, workbook_payloads_identical

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RunMode = Literal['normal', 'check-only']
BUSINESS_FIELDS = (
    'status',
    'pipeline',
    'sheet_count',
    'output_written',
    'error_log_count',
    'issue_type_counts',
    'quality_metrics',
    'run_counts',
    'warnings',
)


class ValidationError(RuntimeError):
    """A stable validation failure whose public message contains no subprocess data."""

    def __init__(self, error_code: str, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.retryable = retryable


class _SafeArgumentParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        raise ValidationError('INVALID_ARGUMENT', 'Invalid release validation arguments.')


def compare_releases(
    *,
    baseline_binary: Path,
    candidate_binary: Path,
    pipeline: PipelineName,
    input_path: Path,
    output_dir: Path,
    month_start: str | None = None,
    month_end: str | None = None,
    config_path: Path | None = None,
    mode: RunMode = 'normal',
    audit_mode: str = 'manifest',
    purpose: str = 'screening',
    timeout_seconds: float = 300,
    memory_limit_bytes: int = 2 * 1024**3,
    disk_limit_bytes: int = 2 * 1024**3,
) -> dict[str, Any]:
    if (
        pipeline not in ('gb', 'sk')
        or mode not in ('normal', 'check-only')
        or audit_mode not in AUDIT_MODES
        or purpose not in PURPOSES
        or not math.isfinite(timeout_seconds)
        or timeout_seconds <= 0
        or min(memory_limit_bytes, disk_limit_bytes) <= 0
    ):
        raise ValidationError('INVALID_ARGUMENT', 'Invalid pipeline, mode, purpose or resource budgets.')
    baseline_binary = _require_file(baseline_binary, 'baseline binary')
    candidate_binary = _require_file(candidate_binary, 'candidate binary')
    input_path = _require_file(input_path, 'input workbook')
    config_path = _require_file(config_path, 'configuration') if config_path is not None else None
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {name: output_dir / f'{name}-{pipeline}.xlsx' for name in ('baseline', 'candidate')}
    manifests = {name: output_dir / f'{name}-{pipeline}.manifest.json' for name in outputs}
    if any(path.exists() for path in (*outputs.values(), *manifests.values())):
        raise FileExistsError('Validation artifact already exists.')
    identities = {
        'input_sha256': _sha256(input_path),
        'config_source_sha256': _sha256(config_path) if config_path is not None else None,
        'baseline_binary_sha256': _sha256(baseline_binary),
        'candidate_binary_sha256': _sha256(candidate_binary),
    }
    limits = {
        'timeout_seconds': timeout_seconds,
        'memory_limit_bytes': memory_limit_bytes,
        'disk_limit_bytes': disk_limit_bytes,
    }
    if _has_temporary_residue(output_dir):
        raise FileExistsError('Validation staging artifacts already exist.')
    summaries = {}
    manifest_payloads = {}
    mismatches: list[dict[str, Any]] = []
    for name, binary in (('baseline', baseline_binary), ('candidate', candidate_binary)):
        summary = _run_binary(
            binary,
            pipeline,
            input_path,
            outputs[name],
            month_start=month_start,
            month_end=month_end,
            config_path=config_path,
            mode=mode,
            manifest_path=manifests[name] if audit_mode == 'manifest' else None,
            **limits,
        )
        summaries[name] = summary
        if _has_temporary_residue(output_dir):
            mismatches.append({'kind': 'temporary-residue', 'side': name})
        if mode == 'check-only' and outputs[name].exists():
            mismatches.append({'kind': 'output-validation', 'side': name, 'field': 'no_workbook'})
        if audit_mode == 'none':
            if manifests[name].exists():
                mismatches.append({'kind': 'unexpected-manifest', 'side': name})
            continue
        manifest = _read_manifest(manifests[name])
        manifest_payloads[name] = manifest
        for field in _validate_manifest(
            manifest,
            summary,
            pipeline=pipeline,
            mode=mode,
            input_path=input_path,
            output_path=outputs[name],
            config_path=config_path,
            month_start=month_start,
            month_end=month_end,
            input_sha256=identities['input_sha256'],
            config_sha256=identities['config_source_sha256'],
        ):
            mismatches.append({'kind': 'manifest-validation', 'side': name, 'field': field})
    for field in BUSINESS_FIELDS:
        default = [] if field == 'warnings' else None
        if summaries['baseline'].get(field, default) != summaries['candidate'].get(field, default):
            mismatches.append({'kind': 'business-summary', 'field': field})
    if audit_mode == 'manifest':
        for field, getter in (
            ('warnings', lambda manifest: manifest.get('warnings')),
            ('sheet_names', lambda manifest: manifest.get('result', {}).get('sheet_names')),
            ('low_memory_writer', lambda manifest: manifest.get('execution', {}).get('low_memory_writer')),
            ('effective_config_sha256', lambda manifest: manifest.get('config', {}).get('effective_sha256')),
            ('selected_sheet', lambda manifest: manifest.get('input', {}).get('selected_sheet')),
        ):
            if getter(manifest_payloads['baseline']) != getter(manifest_payloads['candidate']):
                mismatches.append({'kind': 'manifest-business', 'field': field})
    output_evidence = {name: {'sha256': None, 'size_bytes': None, 'sheet_names': None} for name in outputs}
    if mode == 'check-only':
        comparison_mode = 'summary-only'
    elif not all(path.is_file() for path in outputs.values()):
        comparison_mode = 'missing-workbook'
        mismatches.append({'kind': 'output-validation', 'field': 'workbook_exists'})
    else:
        comparison = _compare_outputs(outputs['baseline'], outputs['candidate'], pipeline=pipeline, **limits)
        comparison_mode = comparison['comparison_mode']
        mismatches.extend(comparison['mismatches'])
        output_evidence = comparison['outputs']
        for name in outputs:
            if output_evidence[name]['size_bytes'] != summaries[name].get('output_size_bytes'):
                mismatches.append({'kind': 'output-validation', 'side': name, 'field': 'output_size_bytes'})
    for field, path in (
        ('input_sha256', input_path),
        ('config_source_sha256', config_path),
        ('baseline_binary_sha256', baseline_binary),
        ('candidate_binary_sha256', candidate_binary),
    ):
        if path is not None and _sha256(path) != identities[field]:
            mismatches.append({'kind': 'identity-changed', 'field': field})
    baseline_size = summaries['baseline'].get('output_size_bytes')
    candidate_size = summaries['candidate'].get('output_size_bytes')
    audit_evidence = {name: _audit_evidence(manifest_payloads.get(name)) for name in outputs}
    unknown_fields = [
        f'audit_evidence.{name}.{field}'
        for name, evidence in audit_evidence.items()
        for field, value in evidence.items()
        if value is None
    ]
    if mode == 'check-only':
        unknown_fields.extend(f'outputs.{name}.sheet_names' for name in outputs)
    report = {
        'schema_version': 1,
        'status': 'passed' if not mismatches else 'failed',
        'pipeline': pipeline,
        'mode': mode,
        'options': {'audit_mode': audit_mode, 'purpose': purpose, **limits},
        'filter': {'month_start': month_start, 'month_end': month_end},
        'comparison_mode': comparison_mode,
        'mismatch_count': len(mismatches),
        'mismatches': mismatches,
        'baseline': _safe_summary(summaries['baseline']),
        'candidate': _safe_summary(summaries['candidate']),
        'identities': identities,
        'manifest_validation': (
            'not-requested'
            if audit_mode == 'none'
            else 'failed'
            if any(m['kind'] == 'manifest-validation' for m in mismatches)
            else 'passed'
        ),
        'outputs': output_evidence,
        'audit_evidence': audit_evidence,
        'unknown_fields': unknown_fields,
        'output_metrics': {
            'baseline_size_bytes': baseline_size,
            'candidate_size_bytes': candidate_size,
            'candidate_to_baseline_ratio': candidate_size / baseline_size if baseline_size and candidate_size else None,
        },
    }
    if mismatches:
        report.update(
            error_code='RELEASE_MISMATCH',
            message='Workbook, business summary or Manifest validation differs; see mismatch fields.',
            retryable=False,
        )
    return report


def _require_file(path: Path, label: str) -> Path:
    resolved = path.resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f'Required {label} is unavailable.')
    return resolved


def _sha256(path: Path) -> str:
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def _run_binary(
    binary: Path,
    pipeline: PipelineName,
    input_path: Path,
    output_path: Path,
    *,
    month_start: str | None,
    month_end: str | None,
    config_path: Path | None = None,
    mode: RunMode = 'normal',
    manifest_path: Path | None = None,
    timeout_seconds: float = 300,
    memory_limit_bytes: int = 2 * 1024**3,
    disk_limit_bytes: int = 2 * 1024**3,
) -> dict[str, Any]:
    payload = _limited_process(
        _binary_arguments(
            binary,
            pipeline,
            input_path,
            output_path,
            month_start=month_start,
            month_end=month_end,
            config_path=config_path,
            mode=mode,
            manifest_path=manifest_path,
        ),
        directory=output_path.parent,
        cwd=PROJECT_ROOT,
        timeout_seconds=timeout_seconds,
        memory_limit_bytes=memory_limit_bytes,
        disk_limit_bytes=disk_limit_bytes,
    )
    _validate_summary(payload, pipeline, mode)
    return payload


def _limited_process(
    arguments: list[str],
    *,
    directory: Path,
    timeout_seconds: float,
    memory_limit_bytes: int,
    disk_limit_bytes: int,
    comparison: bool = False,
    cwd: Path | None = None,
) -> dict[str, Any]:
    try:
        return run_process(
            arguments,
            directory=directory,
            timeout=timeout_seconds,
            memory_limit_bytes=memory_limit_bytes,
            disk_limit_bytes=disk_limit_bytes,
            cwd=cwd,
        )['payload']
    except MeasurementError as error:
        codes = {
            'RUN_TIMEOUT': 'COMPARISON_TIMEOUT' if comparison else 'BINARY_TIMEOUT',
            'CHILD_FAILED': 'COMPARISON_FAILED' if comparison else 'BINARY_FAILED',
            'INVALID_STDOUT': 'INVALID_COMPARISON' if comparison else 'INVALID_RUN_SUMMARY',
        }
        raise ValidationError(
            codes.get(error.code, error.code),
            'A correctness process failed or exceeded its frozen resource budgets.',
            retryable=error.retryable,
        ) from None
    except (OSError, UnicodeError):
        raise ValidationError(
            'BINARY_EXECUTION_ERROR', 'A correctness process could not be executed or decoded.'
        ) from None


def _has_temporary_residue(directory: Path) -> bool:
    return any(path.name.startswith(('.costing-', '.tmp')) for path in directory.rglob('*'))


def _workbook_comparison(baseline: Path, candidate: Path, *, pipeline: PipelineName) -> dict[str, Any]:
    """Run the existing complete ZIP/semantic comparison in a budgeted worker; it never returns cell values."""
    paths = {'baseline': baseline, 'candidate': candidate}
    mismatches: list[dict[str, Any]] = []
    try:
        outputs = {}
        for name, path in paths.items():
            # 实际 Sheet 名只读自 ZIP 元数据，不依赖 Manifest 的自我声明。
            outputs[name] = {**file_identity(path), 'sheet_names': workbook_sheet_names(path)}
            if outputs[name]['sheet_names'] != SHEET_NAMES:
                mismatches.append({'kind': 'output-validation', 'side': name, 'field': 'sheet_names'})
        if workbook_payloads_identical(baseline, candidate):
            comparison_mode = 'package-fast-path'
        else:
            comparison_mode = 'semantic'
            comparison = compare_workbooks(baseline, candidate, pipeline=pipeline)
            mismatches.extend(
                {'sheet': item.sheet, 'coordinate': item.coordinate, 'kind': item.mismatch_kind}
                for item in comparison.mismatches
            )
        for name, path in paths.items():
            if file_identity(path) != {key: outputs[name][key] for key in ('sha256', 'size_bytes')}:
                return {'error_code': 'OUTPUT_IDENTITY_CHANGED'}
    except (MeasurementError, OSError, ValueError, KeyError, BadZipFile, ParseError) as error:
        return {'error_code': error.code if isinstance(error, MeasurementError) else 'INVALID_WORKBOOK'}
    return {'comparison_mode': comparison_mode, 'mismatches': mismatches, 'outputs': outputs}


def _compare_outputs(
    baseline: Path,
    candidate: Path,
    *,
    pipeline: PipelineName,
    timeout_seconds: float,
    memory_limit_bytes: int,
    disk_limit_bytes: int,
) -> dict[str, Any]:
    # -I 隔离 Python 的导入环境；只加入已知仓库路径，业务输出目录不参与模块查找。
    worker = (
        'import json, sys; from pathlib import Path; sys.path.insert(0, sys.argv[1]); '
        'from tools.validation.compare_releases import _workbook_comparison; '
        'print(json.dumps(_workbook_comparison(Path(sys.argv[2]), Path(sys.argv[3]), '
        'pipeline=sys.argv[4]), ensure_ascii=False, allow_nan=False))'
    )
    result = _limited_process(
        [sys.executable, '-I', '-c', worker, str(PROJECT_ROOT), str(baseline), str(candidate), pipeline],
        directory=baseline.parent,
        timeout_seconds=timeout_seconds,
        memory_limit_bytes=memory_limit_bytes,
        disk_limit_bytes=disk_limit_bytes,
        comparison=True,
    )
    if 'error_code' in result:
        # worker 只会返回自己生成的错误码；仍校验格式，避免任何子进程文本进入公开错误。
        code = result['error_code']
        safe = isinstance(code, str) and re.fullmatch(r'[A-Z_]+', code) is not None
        raise ValidationError(code if safe else 'INVALID_COMPARISON', 'The workbook comparison could not be completed.')
    if not all(key in result for key in ('comparison_mode', 'mismatches', 'outputs')):
        raise ValidationError('INVALID_COMPARISON', 'The workbook comparison returned an incomplete result.')
    return result


def _validate_summary(payload: Any, pipeline: PipelineName, mode: RunMode) -> None:
    valid = isinstance(payload, dict)
    if valid:
        valid = (
            payload.get('status') == 'succeeded'
            and payload.get('pipeline') == pipeline
            and payload.get('output_written') is (mode == 'normal')
            and type(payload.get('sheet_count')) is int
            and type(payload.get('error_log_count')) is int
            and isinstance(payload.get('request_id'), str)
        )
    if valid:
        for field in ('run_counts', 'issue_type_counts'):
            counts = payload.get(field)
            valid = (
                valid
                and isinstance(counts, dict)
                and all(
                    isinstance(key, str) and re.fullmatch(r'[A-Za-z0-9_]+', key) and type(value) is int and value >= 0
                    for key, value in counts.items()
                )
            )
        metrics = payload.get('quality_metrics')
        valid = (
            valid
            and isinstance(metrics, list)
            and all(
                isinstance(metric, dict)
                and all(isinstance(metric.get(key), str) for key in ('category', 'metric', 'value', 'description'))
                for metric in metrics
            )
        )
        valid = valid and payload['error_log_count'] == sum(payload['issue_type_counts'].values())
        size = payload.get('output_size_bytes')
        valid = valid and ((type(size) is int and size > 0) if mode == 'normal' else size is None)
    if not valid:
        raise ValidationError('INVALID_RUN_SUMMARY', 'A release returned an invalid or inconsistent run summary.')


def _binary_arguments(
    binary: Path,
    pipeline: PipelineName,
    input_path: Path,
    output_path: Path,
    *,
    month_start: str | None,
    month_end: str | None,
    config_path: Path | None = None,
    mode: RunMode = 'normal',
    manifest_path: Path | None = None,
) -> list[str]:
    arguments = [str(binary), pipeline, '--input', str(input_path)]
    if mode == 'normal':
        arguments.extend(('--output', str(output_path)))
    else:
        arguments.append('--check-only')
    arguments.append('--redact-paths')
    if config_path is not None:
        arguments.extend(('--config', str(config_path)))
    if manifest_path is not None:
        arguments.extend(('--summary-output', str(manifest_path)))
    if month_start is not None:
        arguments.extend(('--month-start', month_start))
    if month_end is not None:
        arguments.extend(('--month-end', month_end))
    return arguments


def _read_manifest(path: Path) -> dict[str, Any]:
    try:
        manifest = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        raise ValidationError('INVALID_MANIFEST', 'A release did not write a readable JSON Manifest.') from None
    if not isinstance(manifest, dict) or not all(
        isinstance(manifest.get(field), dict)
        for field in ('execution', 'input', 'filter', 'config', 'result', 'quality')
    ):
        raise ValidationError('INVALID_MANIFEST', 'A release returned an invalid Manifest structure.')
    return manifest


def _present_path(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return path.name


def _validate_manifest(
    manifest: dict[str, Any],
    summary: dict[str, Any],
    *,
    pipeline: PipelineName,
    mode: RunMode,
    input_path: Path,
    output_path: Path,
    config_path: Path | None,
    month_start: str | None,
    month_end: str | None,
    input_sha256: str,
    config_sha256: str | None,
) -> list[str]:
    execution, source, config, result, quality = (
        manifest.get(field, {}) for field in ('execution', 'input', 'config', 'result', 'quality')
    )
    expected = {
        'schema_version': manifest.get('schema_version') == 1,
        'status': manifest.get('status') == 'succeeded',
        'request_id': manifest.get('request_id') == summary.get('request_id'),
        'execution.pipeline': execution.get('pipeline') == pipeline,
        'execution.mode': execution.get('mode') == mode,
        'execution.low_memory_writer': type(execution.get('low_memory_writer')) is bool,
        'input.sha256': source.get('sha256') == input_sha256,
        'input.size_bytes': source.get('size_bytes') == input_path.stat().st_size,
        'input.path': source.get('path') == _present_path(input_path),
        'input.file_name': source.get('file_name') == input_path.name,
        'input.selected_sheet': isinstance(source.get('selected_sheet'), str) and bool(source.get('selected_sheet')),
        'input.reader_rows': source.get('reader_rows') == summary.get('run_counts', {}).get('reader_rows'),
        'filter': manifest.get('filter') == {'month_start': month_start, 'month_end': month_end},
        'config.schema_version': config.get('schema_version') == 1,
        'config.source': config.get('source') == ('external' if config_path is not None else 'embedded-default'),
        'config.source_sha256': config.get('source_sha256') == config_sha256,
        'config.path': config.get('path') == (_present_path(config_path) if config_path is not None else None),
        'config.effective_sha256': isinstance(config.get('effective_sha256'), str)
        and re.fullmatch(r'[0-9a-f]{64}', config['effective_sha256']) is not None,
        'result.output_written': result.get('output_written') is (mode == 'normal'),
        'result.sheet_count': result.get('sheet_count') == summary.get('sheet_count') == len(SHEET_NAMES),
        'result.sheet_names': result.get('sheet_names') == SHEET_NAMES,
        'result.final_output_valid': result.get('final_output_valid') is (mode == 'normal'),
        'run_counts': manifest.get('run_counts') == summary.get('run_counts'),
        'warnings': isinstance(manifest.get('warnings'), list)
        and all(isinstance(warning, str) for warning in manifest['warnings']),
    }
    for field in ('error_log_count', 'issue_type_counts', 'quality_metrics'):
        expected[f'quality.{field}'] = quality.get(field) == summary.get(field)
    if mode == 'normal':
        output_exists = output_path.is_file()
        expected.update(
            {
                'result.workbook_exists': output_exists,
                'result.workbook_path': result.get('workbook_path') == _present_path(output_path),
                'summary.workbook_path': summary.get('workbook_path') == _present_path(output_path),
                'result.output_size_bytes': output_exists
                and result.get('output_size_bytes') == output_path.stat().st_size == summary.get('output_size_bytes'),
                'result.output_sha256': output_exists and result.get('output_sha256') == _sha256(output_path),
            }
        )
    else:
        for field in ('workbook_path', 'output_size_bytes', 'output_sha256'):
            expected[f'result.{field}'] = result.get(field) is None
        expected['result.no_workbook'] = not output_path.exists()
        expected['execution.no_low_memory_writer'] = execution.get('low_memory_writer') is False
    return [field for field, passed in expected.items() if not passed]


def _audit_evidence(manifest: dict[str, Any] | None) -> dict[str, Any]:
    if manifest is None:
        return dict.fromkeys(('low_memory_writer', 'effective_config_sha256', 'warning_count', 'warnings_sha256'))
    low_memory = manifest['execution'].get('low_memory_writer')
    effective_hash = manifest['config'].get('effective_sha256')
    warnings = manifest.get('warnings')
    valid_warnings = isinstance(warnings, list) and all(isinstance(item, str) for item in warnings)
    return {
        'low_memory_writer': low_memory if type(low_memory) is bool else None,
        'effective_config_sha256': effective_hash
        if isinstance(effective_hash, str) and re.fullmatch(r'[0-9a-f]{64}', effective_hash)
        else None,
        'warning_count': len(warnings) if valid_warnings else None,
        'warnings_sha256': _json_sha256(warnings) if valid_warnings else None,
    }


def _safe_summary(summary: dict[str, Any]) -> dict[str, Any]:
    safe = {
        key: summary.get(key)
        for key in ('status', 'pipeline', 'sheet_count', 'error_log_count', 'output_size_bytes', 'run_counts')
    }
    for key in ('output_written', 'issue_type_counts', 'quality_metrics'):
        if key in summary:
            safe[key] = summary[key]
    if 'warnings' in summary:
        safe['warnings_sha256'] = _json_sha256(summary['warnings'])
    return safe


def _json_sha256(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode('utf-8')).hexdigest()


def _parse_args() -> argparse.Namespace:
    parser = _SafeArgumentParser(description=__doc__)
    parser.add_argument('--baseline-binary', type=Path, required=True)
    parser.add_argument('--candidate-binary', type=Path, required=True)
    parser.add_argument('--pipeline', choices=('gb', 'sk'), required=True)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--config', type=Path)
    parser.add_argument('--month-start')
    parser.add_argument('--month-end')
    parser.add_argument('--mode', choices=('normal', 'check-only'), default='normal')
    parser.add_argument('--check-only', action='store_true')
    parser.add_argument('--audit-mode', choices=AUDIT_MODES, default='manifest')
    parser.add_argument('--purpose', choices=PURPOSES, default='screening')
    parser.add_argument('--timeout-seconds', type=float, default=300)
    parser.add_argument('--memory-limit-mib', type=int, default=2048)
    parser.add_argument('--disk-limit-mib', type=int, default=2048)
    parser.add_argument('--report', type=Path)
    return parser.parse_args()


def main() -> int:
    try:
        args = _parse_args()
        # 报告占位也先拒绝，避免完成两个运行后才发现已有报告。
        if args.report is not None and args.report.exists():
            raise FileExistsError('Validation report already exists.')
        report = compare_releases(
            baseline_binary=args.baseline_binary,
            candidate_binary=args.candidate_binary,
            pipeline=args.pipeline,
            input_path=args.input,
            output_dir=args.output_dir,
            month_start=args.month_start,
            month_end=args.month_end,
            config_path=args.config,
            mode='check-only' if args.check_only else args.mode,
            audit_mode=args.audit_mode,
            purpose=args.purpose,
            timeout_seconds=args.timeout_seconds,
            memory_limit_bytes=args.memory_limit_mib * 1024**2,
            disk_limit_bytes=args.disk_limit_mib * 1024**2,
        )
        rendered = json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False)
        if args.report is not None:
            with args.report.open('x', encoding='utf-8', newline='\n') as stream:
                stream.write(f'{rendered}\n')
        print(rendered)
        return 0 if report['status'] == 'passed' else 1
    except (OSError, ValueError, RuntimeError, BadZipFile, ParseError) as error:
        print(
            json.dumps(
                {
                    'status': 'failed',
                    'error_code': error.error_code if isinstance(error, ValidationError) else 'VALIDATION_ERROR',
                    'message': str(error)
                    if isinstance(error, ValidationError)
                    else 'Release validation failed; check inputs, unused output paths and local permissions.',
                    'retryable': error.retryable if isinstance(error, ValidationError) else False,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 2


if __name__ == '__main__':
    raise SystemExit(main())

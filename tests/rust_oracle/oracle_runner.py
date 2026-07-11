from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import stat
import subprocess
import time
import zipfile
from dataclasses import dataclass, field, replace
from decimal import Decimal
from pathlib import Path
from typing import Any

from src.config.pipelines import PIPELINES
from src.etl.runner import _build_request
from src.services import costing_service
from src.services.costing_service import ServiceStatus, run_costing_request
from tests.rust_oracle.benchmark_protocol import NormalRunEvidence, RuntimeEvidence, RuntimeSchema
from tests.rust_oracle.repo_paths import repo_root

REQUIRED_RUST_PAYLOAD_STAGES = (
    'ingest',
    'normalize',
    'split',
    'fact',
    'presentation',
    'total',
)
REQUIRED_RUST_RUN_COUNTS = (
    'reader_rows',
    'detail_rows',
    'qty_rows',
    'qty_sheet_rows',
    'quality_metric_count',
    'work_order_rows',
)


@dataclass(frozen=True)
class OracleRunSummary:
    error_log_count: int
    issue_type_counts: dict[str, int]
    quality_metrics: dict[tuple[str, str], str]


@dataclass(frozen=True)
class TimedPayloadRun:
    pipeline: str
    payload_total_seconds: float
    stage_timings: dict[str, float]
    runtime_summary: OracleRunSummary
    run_counts: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class CapturedNormalRun:
    normal_run: NormalRunEvidence
    exit_code: int
    local_unversioned_log_sha256: str


class RustNormalProcessError(AssertionError):
    def __init__(self, returncode: int, log_sha256: str) -> None:
        super().__init__(f'Rust normal benchmark exited with code {returncode}; raw log sha256={log_sha256}')
        self.returncode = returncode
        self.log_sha256 = log_sha256


class RustNormalValidationError(AssertionError):
    def __init__(self, message: str, log_sha256: str) -> None:
        super().__init__(message)
        self.log_sha256 = log_sha256


def run_python_oracle(pipeline: str, input_path: Path, output_path: Path) -> OracleRunSummary:
    try:
        pipeline_config = PIPELINES[pipeline]
    except KeyError as exc:
        raise AssertionError(f'unknown Python oracle pipeline: {pipeline!r}') from exc
    request = replace(
        _build_request(
            config=pipeline_config,
            input_file=input_path,
            month_range=None,
            benchmark=True,
        ),
        output_dir=output_path.parent,
    )
    result = run_costing_request(request)
    if result.status != ServiceStatus.SUCCEEDED:
        raise AssertionError(f'python oracle failed: {result.message} {result.technical_detail}')
    if result.workbook_path is None or not result.workbook_path.exists():
        raise AssertionError('python oracle did not create workbook')
    result.workbook_path.replace(output_path)
    return OracleRunSummary(
        error_log_count=result.error_log_count,
        issue_type_counts=result.issue_type_counts,
        quality_metrics=_quality_metric_values(result.quality_metrics),
    )


def run_python_check_only_payload(pipeline: str, input_path: Path) -> TimedPayloadRun:
    try:
        pipeline_config = PIPELINES[pipeline]
    except KeyError as exc:
        raise AssertionError(f'unknown Python oracle pipeline: {pipeline!r}') from exc

    request = _build_request(
        config=pipeline_config,
        input_file=input_path,
        month_range=None,
        benchmark=True,
    )
    prepared, validation_error = costing_service._prepare_request(
        request,
        validate_output_dir=False,
    )
    if validation_error is not None or prepared is None:
        message = validation_error.message if validation_error is not None else 'missing prepared request'
        raise AssertionError(f'python check-only input validation failed: {message}')

    etl = costing_service._build_etl(request, prepared.month_range)
    etl._reset_last_run_state()

    started = time.perf_counter()
    payload = etl.pipeline.build_workbook_payload(
        input_path,
        standalone_cost_items=etl.standalone_cost_items,
        product_anomaly_scope_mode=etl.product_anomaly_scope_mode,
        month_range=etl.month_range,
        presentation_product_order=etl.product_order,
        artifacts_transform=etl._filter_analysis_artifacts_by_whitelist,
        progress_callback=None,
    )
    payload_total_seconds = time.perf_counter() - started
    if not math.isfinite(payload_total_seconds) or payload_total_seconds < 0:
        raise AssertionError(f'invalid Python payload total: {payload_total_seconds!r}')

    issue_type_counts: dict[str, int] = {}
    error_frame = payload.error_log_export
    if not error_frame.empty and 'issue_type' in error_frame.columns:
        issue_type_counts = {
            str(issue_type): int(count) for issue_type, count in error_frame['issue_type'].value_counts().items()
        }
    runtime_summary = OracleRunSummary(
        error_log_count=payload.error_log_count,
        issue_type_counts=issue_type_counts,
        quality_metrics=_quality_metric_values(payload.quality_metrics),
    )
    return TimedPayloadRun(
        pipeline=pipeline,
        payload_total_seconds=payload_total_seconds,
        stage_timings={name: float(value) for name, value in payload.stage_timings.items()},
        runtime_summary=runtime_summary,
    )


def run_rust_cli(pipeline: str, input_path: Path, output_path: Path) -> OracleRunSummary:
    return run_rust_cli_release(build_rust_cli_release(), pipeline, input_path, output_path)


def build_rust_cli_release() -> Path:
    cargo = shutil.which('cargo')
    if cargo is None:
        raise AssertionError('cargo executable not found')
    root = repo_root()
    manifest_path = root / 'rust' / 'Cargo.toml'
    target_directory = _cargo_target_directory(cargo, root, manifest_path)

    completed = subprocess.run(  # noqa: S603 - test harness invokes local Cargo with fixed arguments.
        [
            cargo,
            'build',
            '--quiet',
            '--release',
            '--manifest-path',
            str(manifest_path),
            '-p',
            'costing-calculate',
        ],
        check=False,
        capture_output=True,
        cwd=root,
        encoding='utf-8',
        errors='replace',
    )
    if completed.returncode != 0:
        raise AssertionError(f'rust release build failed\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}')

    executable_name = 'costing-calculate.exe' if os.name == 'nt' else 'costing-calculate'
    executable = target_directory / 'release' / executable_name
    if not executable.exists():
        raise AssertionError(f'rust release executable missing: {executable}')
    return executable


def _cargo_target_directory(cargo: str, root: Path, manifest_path: Path) -> Path:
    completed = subprocess.run(  # noqa: S603 - test harness invokes local Cargo with fixed arguments.
        [
            cargo,
            'metadata',
            '--format-version',
            '1',
            '--no-deps',
            '--manifest-path',
            str(manifest_path),
        ],
        check=False,
        capture_output=True,
        cwd=root,
        encoding='utf-8',
        errors='replace',
    )
    if completed.returncode != 0:
        raise AssertionError(f'cargo metadata failed\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}')
    try:
        payload = json.loads(completed.stdout)
        target_directory = payload['target_directory']
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        raise AssertionError(f'cargo metadata did not provide target_directory: {completed.stdout!r}') from exc
    if not isinstance(target_directory, str) or not target_directory:
        raise AssertionError('cargo metadata target_directory must be a non-empty string')
    return Path(target_directory)


def run_rust_cli_release(executable: Path, pipeline: str, input_path: Path, output_path: Path) -> OracleRunSummary:
    completed = subprocess.run(  # noqa: S603 - test harness invokes local release executable with fixed arguments.
        [
            str(executable),
            pipeline,
            '--input',
            str(input_path),
            '--output',
            str(output_path),
            '--benchmark',
        ],
        check=False,
        capture_output=True,
        cwd=repo_root(),
        encoding='utf-8',
        errors='replace',
    )
    if completed.returncode != 0:
        raise AssertionError(f'rust release cli failed\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}')
    summary = parse_rust_run_summary(completed.stdout)
    if not output_path.exists():
        raise AssertionError(f'rust release cli did not create expected workbook: {output_path}')
    return summary


def run_rust_cli_release_check_only(
    executable: Path,
    pipeline: str,
    input_path: Path,
) -> TimedPayloadRun:
    completed = subprocess.run(  # noqa: S603 - fixed local executable and arguments.
        [
            str(executable),
            pipeline,
            '--input',
            str(input_path.resolve()),
            '--check-only',
            '--benchmark',
        ],
        check=False,
        capture_output=True,
        cwd=repo_root(),
        encoding='utf-8',
        errors='replace',
    )
    if completed.returncode != 0:
        raise AssertionError(f'rust check-only failed\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}')
    result = parse_rust_check_only_run(completed.stdout)
    if result.pipeline != pipeline:
        raise AssertionError(f'Rust check-only reported pipeline {result.pipeline!r}, expected {pipeline!r}')
    return result


def run_rust_normal_captured(
    executable: Path,
    pipeline: str,
    input_path: Path,
    output_path: Path,
    *,
    schema: RuntimeSchema,
    local_log_root: Path,
    workbook_oracle_fn: Any | None = None,
) -> CapturedNormalRun:
    root = repo_root()
    input_path = input_path.resolve()
    executable = executable.resolve()
    output_path = _prepare_local_path(
        output_path,
        allowed_roots=(root / 'data' / 'processed', root / 'rust' / 'target'),
        purpose='normal output must stay below data/processed or rust/target',
        create_parent=False,
    )
    local_log_root = _prepare_local_path(
        local_log_root,
        allowed_roots=(root / 'rust' / 'target' / 'perf-local',),
        purpose='raw logs must stay below rust/target/perf-local',
        create_parent=True,
    )
    if output_path in (input_path, executable):
        raise AssertionError('normal benchmark output must differ from input and executable')
    input_sha256 = _file_sha256(input_path)
    binary_sha256 = _file_sha256(executable)
    if output_path.exists():
        raise AssertionError(f'normal benchmark output already exists: {output_path}')
    command_arguments = (
        pipeline,
        '--input',
        str(input_path),
        '--output',
        str(output_path),
        '--benchmark',
    )
    started = time.perf_counter()
    try:
        completed = subprocess.run(  # noqa: S603 - fixed local executable and arguments.
            [str(executable), *command_arguments],
            check=False,
            capture_output=True,
            cwd=repo_root(),
            encoding='utf-8',
            errors='replace',
        )
    except Exception as exc:
        raw_log = json.dumps(
            {
                'exception': type(exc).__name__,
                'errno': getattr(exc, 'errno', None),
                'winerror': getattr(exc, 'winerror', None),
            },
            separators=(',', ':'),
        ).encode('utf-8')
        log_sha256 = hashlib.sha256(raw_log).hexdigest()
        _write_create_new(local_log_root / f'{log_sha256}.json', raw_log, allowed_root=local_log_root)
        raise RustNormalProcessError(-1, log_sha256) from exc
    wall_seconds = Decimal(str(time.perf_counter() - started))
    raw_log = json.dumps(
        {'returncode': completed.returncode, 'stdout': completed.stdout, 'stderr': completed.stderr},
        ensure_ascii=False,
        separators=(',', ':'),
    ).encode('utf-8')
    log_sha256 = hashlib.sha256(raw_log).hexdigest()
    _write_create_new(local_log_root / f'{log_sha256}.json', raw_log, allowed_root=local_log_root)
    if completed.returncode != 0:
        raise RustNormalProcessError(completed.returncode, log_sha256)

    try:
        payload = _load_rust_summary_payload(completed.stdout)
        if payload.get('status') != 'succeeded' or payload.get('pipeline') != pipeline:
            raise AssertionError('Rust normal benchmark reported an invalid status or pipeline')
        if payload.get('output_written') is not True or payload.get('sheet_count') != 3:
            raise AssertionError('Rust normal benchmark must write one three-sheet workbook')
        if Path(str(payload.get('workbook_path'))).resolve() != output_path:
            raise AssertionError('Rust normal benchmark reported an unexpected workbook path')
        if not output_path.is_file():
            raise AssertionError('Rust normal benchmark did not create its workbook')
        payload['output_size_bytes'] = output_path.stat().st_size
        runtime = parse_runtime_payload(payload, schema=schema)
        oracle_sha256 = (workbook_oracle_fn or workbook_oracle)(output_path)
        if _file_sha256(input_path) != input_sha256 or _file_sha256(executable) != binary_sha256:
            raise AssertionError('normal benchmark input or executable changed during capture')
    except Exception as exc:
        if isinstance(exc, RustNormalValidationError):
            raise
        raise RustNormalValidationError('Rust normal runtime or workbook validation failed', log_sha256) from exc
    return CapturedNormalRun(
        normal_run=NormalRunEvidence(
            external_wall_seconds=wall_seconds,
            peak_working_set_bytes=None,
            runtime=runtime,
            workbook_oracle_sha256=oracle_sha256,
        ),
        exit_code=0,
        local_unversioned_log_sha256=log_sha256,
    )


def workbook_oracle(path: Path) -> str:
    required = {'[Content_Types].xml', '_rels/.rels', 'xl/workbook.xml'}
    root = repo_root()
    safe_path = _prepare_local_path(
        path,
        allowed_roots=(root / 'data' / 'processed', root / 'rust' / 'target'),
        purpose='workbook oracle input must stay below data/processed or rust/target',
        create_parent=False,
    )
    try:
        with zipfile.ZipFile(_io_path(safe_path)) as archive:
            names = set(archive.namelist())
            missing = required - names
            if missing or not any(name.startswith('xl/worksheets/') and name.endswith('.xml') for name in names):
                raise AssertionError(f'XLSX package structure is incomplete: {sorted(missing)!r}')
            digest = hashlib.sha256()
            for name in sorted(names):
                encoded_name = name.encode('utf-8')
                content = archive.read(name)
                digest.update(len(encoded_name).to_bytes(8, 'big'))
                digest.update(encoded_name)
                digest.update(len(content).to_bytes(8, 'big'))
                digest.update(content)
            return digest.hexdigest()
    except (OSError, zipfile.BadZipFile, KeyError) as exc:
        raise AssertionError('workbook is not a readable XLSX ZIP package') from exc


def _prepare_local_path(
    path: Path,
    *,
    allowed_roots: tuple[Path, ...],
    purpose: str,
    create_parent: bool,
) -> Path:
    raw = _normal_path(path).expanduser()
    if '..' in raw.parts:
        raise AssertionError(f'{purpose}; parent traversal is forbidden')
    raw_absolute = raw.absolute()
    raw_roots = tuple(root.expanduser().absolute() for root in allowed_roots)
    lexical_root = next((root for root in raw_roots if _is_relative_to(raw_absolute, root)), None)
    if lexical_root is None:
        raise AssertionError(purpose)
    _reject_existing_reparse_components(raw_absolute)
    parent = raw_absolute if create_parent else raw_absolute.parent
    if create_parent:
        _io_path(parent).mkdir(parents=True, exist_ok=True)
    _reject_existing_reparse_components(parent)
    canonical = raw_absolute.resolve(strict=False)
    canonical_root = lexical_root.resolve(strict=False)
    if not _is_relative_to(canonical, canonical_root):
        raise AssertionError(f'{purpose}; canonical path escapes through a reparse point')
    return canonical


def _io_path(path: Path) -> Path:
    normal = _normal_path(path).absolute()
    if os.name != 'nt':
        return normal
    return Path(f'\\\\?\\{normal}')


def _normal_path(path: Path) -> Path:
    text = str(path)
    if os.name == 'nt' and text.startswith('\\\\?\\'):
        return Path(text[4:])
    return path


def _reject_existing_reparse_components(path: Path) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        if not os.path.lexists(current):
            continue
        metadata = os.lstat(current)
        attributes = getattr(metadata, 'st_file_attributes', 0)
        if stat.S_ISLNK(metadata.st_mode) or attributes & 0x400:
            raise AssertionError(f'path contains a symlink or reparse point: {current}')


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _write_create_new(path: Path, payload: bytes, *, allowed_root: Path) -> None:
    raw = _prepare_local_path(
        path,
        allowed_roots=(allowed_root,),
        purpose='local raw log escaped its validated root',
        create_parent=False,
    )
    path = _io_path(raw)
    path.parent.mkdir(parents=True, exist_ok=True)
    _reject_existing_reparse_components(raw.parent)
    try:
        with path.open('xb') as stream:
            stream.write(payload)
    except FileExistsError:
        if path.read_bytes() != payload:
            raise AssertionError(f'create-new artifact collision: {path}') from None


def _load_rust_summary_payload(stdout: str) -> dict[str, Any]:
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise AssertionError(f'Rust CLI stdout is not valid JSON: {exc}\nSTDOUT:\n{stdout}') from exc
    if not isinstance(payload, dict):
        raise AssertionError(f'Rust CLI stdout JSON must be an object, got {type(payload).__name__}')
    return payload


def _oracle_summary_from_rust_payload(payload: dict[str, Any]) -> OracleRunSummary:
    return OracleRunSummary(
        error_log_count=_required_int(payload, 'error_log_count'),
        issue_type_counts=_issue_type_counts(payload),
        quality_metrics=_quality_metric_values(_required_list(payload, 'quality_metrics')),
    )


def parse_rust_run_summary(stdout: str) -> OracleRunSummary:
    return _oracle_summary_from_rust_payload(_load_rust_summary_payload(stdout))


def parse_runtime_payload(payload: dict[str, Any], *, schema: RuntimeSchema) -> RuntimeEvidence:
    if not isinstance(schema, RuntimeSchema):
        raise AssertionError('schema must be a RuntimeSchema value')
    if payload.get('status') != 'succeeded':
        raise AssertionError("Rust runtime must report status='succeeded'")
    pipeline = payload.get('pipeline')
    if pipeline not in ('gb', 'sk'):
        raise AssertionError(f'invalid Rust runtime pipeline: {pipeline!r}')
    output_written = payload.get('output_written')
    if not isinstance(output_written, bool):
        raise AssertionError('output_written must be boolean')
    workbook_path = payload.get('workbook_path')
    if not output_written and workbook_path is not None:
        raise AssertionError('check-only runtime must not report workbook_path')
    if output_written and not isinstance(workbook_path, str):
        raise AssertionError('normal runtime must report workbook_path')

    stages = _parse_runtime_stages(payload, schema=schema)
    run_counts = _parse_rust_run_counts(payload)
    output_size = payload.get('output_size_bytes')
    if output_size is not None and (
        isinstance(output_size, bool) or not isinstance(output_size, int) or output_size < 0
    ):
        raise AssertionError('output_size_bytes must be a non-negative integer or null')
    if schema in (RuntimeSchema.INSTRUMENTED, RuntimeSchema.READER_INSTRUMENTED) and output_size is None:
        raise AssertionError('output_size_bytes is required by instrumented runtime schema')

    reader_snapshot = payload.get('reader_snapshot_sha256', '')
    if not isinstance(reader_snapshot, str):
        raise AssertionError('reader_snapshot_sha256 must be a string')
    if schema is RuntimeSchema.READER_INSTRUMENTED:
        if len(reader_snapshot) != 64 or any(char not in '0123456789abcdef' for char in reader_snapshot):
            raise AssertionError('reader_snapshot_sha256 must be an exact lowercase SHA-256')
        if 'reader_rows' not in run_counts:
            raise AssertionError('reader_rows is required by reader-instrumented runtime schema')

    summary = _oracle_summary_from_rust_payload(payload)
    if summary.error_log_count < 0 or any(count < 0 for count in summary.issue_type_counts.values()):
        raise AssertionError('runtime error counts must be non-negative')
    if sum(summary.issue_type_counts.values()) != summary.error_log_count:
        raise AssertionError('runtime issue counts must sum to error_log_count')
    dimensions = payload.get('sheet_dimensions', [])
    if not isinstance(dimensions, list) or not all(isinstance(value, str) for value in dimensions):
        raise AssertionError('sheet_dimensions must be a list of strings')
    sheet_count = _required_int(payload, 'sheet_count')
    if sheet_count != 3:
        raise AssertionError('runtime must report exactly three sheets')
    request_id = payload.get('request_id')
    return RuntimeEvidence(
        pipeline=pipeline,
        output_written=output_written,
        request_id_present=isinstance(request_id, str) and bool(request_id),
        sheet_count=sheet_count,
        error_log_count=summary.error_log_count,
        issue_type_counts=tuple(sorted(summary.issue_type_counts.items())),
        quality_metrics=tuple(
            (category, metric, value) for (category, metric), value in sorted(summary.quality_metrics.items())
        ),
        run_counts=tuple(sorted(run_counts.items())),
        stage_timings=tuple(sorted(stages.items())),
        output_size_bytes=output_size,
        sheet_dimensions=tuple(dimensions),
        reader_snapshot_sha256=reader_snapshot,
    )


def _parse_runtime_stages(payload: dict[str, Any], *, schema: RuntimeSchema) -> dict[str, Decimal]:
    timing_payload = payload.get('stage_timings')
    if not isinstance(timing_payload, dict) or not isinstance(timing_payload.get('stages'), dict):
        raise AssertionError("Rust field 'stage_timings.stages' must be an object")
    parsed: dict[str, Decimal] = {}
    for name, raw_value in timing_payload['stages'].items():
        if not isinstance(name, str) or isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
            raise AssertionError(f'Rust stage {name!r} must be numeric')
        try:
            value = Decimal(str(raw_value))
        except Exception as exc:
            raise AssertionError(f'Rust stage {name!r} must be numeric') from exc
        if not value.is_finite() or value < 0:
            raise AssertionError(f'Rust stage {name!r} must be finite and non-negative')
        parsed[name] = value

    required = {*REQUIRED_RUST_PAYLOAD_STAGES, 'export'}
    if schema in (RuntimeSchema.INSTRUMENTED, RuntimeSchema.READER_INSTRUMENTED):
        required.update(('writer_populate', 'xlsx_save'))
    missing = sorted(required - parsed.keys())
    if missing:
        raise AssertionError(f'Rust runtime stages missing: {missing!r}')
    unexpected = sorted(parsed.keys() - required)
    if unexpected:
        raise AssertionError(f'Rust runtime stages contain unexpected keys: {unexpected!r}')
    return parsed


def parse_rust_check_only_run(stdout: str) -> TimedPayloadRun:
    payload = _load_rust_summary_payload(stdout)
    if payload.get('status') != 'succeeded':
        raise AssertionError("Rust check-only must report status='succeeded'")
    pipeline = payload.get('pipeline')
    if not isinstance(pipeline, str) or pipeline not in {'gb', 'sk'}:
        raise AssertionError(f'invalid Rust check-only pipeline: {pipeline!r}')
    if payload.get('sheet_count') != 3:
        raise AssertionError('Rust check-only must build exactly three in-memory sheets')
    if payload.get('output_written') is not False:
        raise AssertionError('Rust check-only must report output_written=false')
    if payload.get('workbook_path') is not None:
        raise AssertionError('Rust check-only must not report a workbook path')

    stage_timings = _parse_rust_stage_timings(payload, require_export=False)
    total = stage_timings['total']
    run_counts = _parse_rust_run_counts(payload)

    return TimedPayloadRun(
        pipeline=pipeline,
        payload_total_seconds=total,
        stage_timings=stage_timings,
        runtime_summary=_oracle_summary_from_rust_payload(payload),
        run_counts=run_counts,
    )


def _parse_rust_stage_timings(
    payload: dict[str, Any],
    *,
    require_export: bool,
) -> dict[str, float]:
    timing_payload = payload.get('stage_timings')
    if not isinstance(timing_payload, dict) or not isinstance(timing_payload.get('stages'), dict):
        raise AssertionError("Rust field 'stage_timings.stages' must be an object")

    parsed: dict[str, float] = {}
    for name, raw_seconds in timing_payload['stages'].items():
        if not isinstance(name, str):
            raise AssertionError('Rust stage names must be strings')
        if isinstance(raw_seconds, bool) or not isinstance(raw_seconds, (int, float)):
            raise AssertionError(f'Rust stage {name!r} must be numeric')
        seconds = float(raw_seconds)
        if not math.isfinite(seconds) or seconds < 0:
            raise AssertionError(f'Rust stage {name!r} must be finite and non-negative')
        parsed[name] = seconds

    missing = [name for name in REQUIRED_RUST_PAYLOAD_STAGES if name not in parsed]
    if missing:
        raise AssertionError(f'Rust payload stages missing: {missing!r}')
    if require_export:
        if 'export' not in parsed:
            raise AssertionError('Rust normal benchmark must report export stage')
    elif 'export' in parsed:
        raise AssertionError('Rust check-only stage timings must not contain export')
    return parsed


def _parse_rust_run_counts(payload: dict[str, Any]) -> dict[str, int]:
    raw = payload.get('run_counts')
    if not isinstance(raw, dict):
        raise AssertionError("Rust field 'run_counts' must be an object")
    missing = [name for name in REQUIRED_RUST_RUN_COUNTS if name not in raw]
    if missing:
        raise AssertionError(f'Rust run counts missing: {missing!r}')
    parsed: dict[str, int] = {}
    for name, count in raw.items():
        if not isinstance(name, str) or isinstance(count, bool) or not isinstance(count, int):
            raise AssertionError('Rust run_counts must map strings to integers')
        if count < 0:
            raise AssertionError(f'Rust run count {name!r} must be non-negative')
        parsed[name] = count
    return parsed


def assert_runtime_contract_matches(expected: OracleRunSummary, actual: OracleRunSummary) -> None:
    mismatches: list[str] = []
    if expected.error_log_count != actual.error_log_count:
        mismatches.append(
            f'error_log_count mismatch: python={expected.error_log_count!r}, rust={actual.error_log_count!r}'
        )
    if expected.issue_type_counts != actual.issue_type_counts:
        mismatches.append(
            f'issue_type_counts mismatch: python={expected.issue_type_counts!r}, rust={actual.issue_type_counts!r}'
        )
    missing_quality_metrics = {
        key: value for key, value in expected.quality_metrics.items() if key not in actual.quality_metrics
    }
    mismatched_quality_metrics = {
        key: (value, actual.quality_metrics[key])
        for key, value in expected.quality_metrics.items()
        if key in actual.quality_metrics and actual.quality_metrics[key] != value
    }
    if missing_quality_metrics or mismatched_quality_metrics:
        mismatches.append(
            f'quality_metrics mismatch: missing={missing_quality_metrics!r}, values={mismatched_quality_metrics!r}'
        )
    if mismatches:
        raise AssertionError('runtime contract mismatch:\n' + '\n'.join(mismatches))


def _required_int(payload: dict[str, Any], field_name: str) -> int:
    value = payload.get(field_name)
    if not isinstance(value, int) or isinstance(value, bool):
        raise AssertionError(f'Rust CLI summary field {field_name!r} must be an integer')
    return value


def _required_list(payload: dict[str, Any], field_name: str) -> list[Any]:
    value = payload.get(field_name)
    if not isinstance(value, list):
        raise AssertionError(f'Rust CLI summary field {field_name!r} must be a list')
    return value


def _issue_type_counts(payload: dict[str, Any]) -> dict[str, int]:
    value = payload.get('issue_type_counts')
    if not isinstance(value, dict):
        raise AssertionError("Rust CLI summary field 'issue_type_counts' must be an object")
    counts: dict[str, int] = {}
    for issue_type, count in value.items():
        if not isinstance(issue_type, str) or not isinstance(count, int) or isinstance(count, bool):
            raise AssertionError("Rust CLI summary field 'issue_type_counts' must map strings to integers")
        counts[issue_type] = count
    return counts


def _quality_metric_values(metrics: Any) -> dict[tuple[str, str], str]:
    if not isinstance(metrics, (list, tuple)):
        raise AssertionError('quality_metrics must be a list or tuple')
    values: dict[tuple[str, str], str] = {}
    for metric in metrics:
        category = getattr(metric, 'category', None)
        name = getattr(metric, 'metric', None)
        value = getattr(metric, 'value', None)
        if isinstance(metric, dict):
            category = metric.get('category')
            name = metric.get('metric')
            value = metric.get('value')
        if not all(isinstance(item, str) for item in (category, name, value)):
            raise AssertionError('quality_metrics entries must contain string category, metric, and value fields')
        key = (category, name)
        if key in values:
            raise AssertionError(f'duplicate quality metric: {key!r}')
        values[key] = value
    return values


def _file_sha256(path: Path) -> str:
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()

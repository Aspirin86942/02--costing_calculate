"""Measure paired Rust CLI runs on Linux, with isolated per-process peak RSS."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import signal
import statistics
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

MAX_CAPTURE_BYTES = 1024 * 1024
SUMMARY_FIELDS = ('status', 'pipeline', 'sheet_count', 'error_log_count', 'issue_type_counts', 'run_counts')
# Linux 的子进程峰值会包含 fork 时继承的父进程驻留内存；先启动轻量进程再测实际 CLI。
CHILD_MEASURER = """
import json, os, subprocess, sys, time
fd = int(sys.argv[2])
started = time.perf_counter()
child = subprocess.Popen(json.loads(sys.argv[1]))
os.write(fd, json.dumps({'pid': child.pid}).encode())
_, status, usage = os.wait4(child.pid, 0)
child.returncode = os.waitstatus_to_exitcode(status)
result = {'pid': child.pid, 'returncode': child.returncode,
          'external_wall_seconds': time.perf_counter() - started,
          'peak_rss_bytes': int(usage.ru_maxrss) * 1024}
os.ftruncate(fd, 0)
os.pwrite(fd, json.dumps(result).encode(), 0)
"""


def _child_metrics(stream: Any) -> dict[str, Any]:
    try:
        return json.loads(os.pread(stream.fileno(), 4096, 0))
    except ValueError:
        return {}


class MeasurementError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable

    def payload(self) -> dict[str, Any]:
        return {'error_code': self.code, 'message': str(self), 'retryable': self.retryable}


def file_identity(path: Path) -> dict[str, Any]:
    with path.open('rb') as handle:
        digest = hashlib.file_digest(handle, 'sha256').hexdigest()
    return {'sha256': digest, 'size_bytes': path.stat().st_size}


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _checkpoint_digest(document: dict[str, Any]) -> str:
    content = {key: value for key, value in document.items() if key != 'checkpoint_sha256'}
    encoded = json.dumps(content, sort_keys=True, ensure_ascii=False, allow_nan=False).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def _save_report(path: Path, document: dict[str, Any]) -> None:
    document['checkpoint_sha256'] = _checkpoint_digest(document)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent, delete=False) as handle:
            temporary = Path(handle.name)
            json.dump(document, handle, ensure_ascii=False, indent=2, allow_nan=False)
            handle.write('\n')
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _environment() -> dict[str, Any]:
    boot_id = Path('/proc/sys/kernel/random/boot_id').read_bytes().strip()
    return {
        'system': platform.system(),
        'machine': platform.machine(),
        'logical_cpus': os.cpu_count(),
        'cpu_affinity': sorted(os.sched_getaffinity(0)),
        'session_sha256': hashlib.sha256(boot_id).hexdigest(),
        'memory_metric': 'peak_rss_bytes',
    }


def _directory_bytes(directory: Path) -> int:
    total = 0
    for path in directory.rglob('*'):
        try:
            if path.is_file():
                total += path.stat().st_size
        except FileNotFoundError:
            # 子进程原子发布时可能已将刚枚举的临时文件重命名。
            continue
    return total


def _resident_bytes(pid: int) -> int:
    try:
        for line in Path(f'/proc/{pid}/status').read_text(encoding='ascii').splitlines():
            if line.startswith('VmRSS:'):
                return int(line.split()[1]) * 1024
    except FileNotFoundError:
        pass
    return 0


def run_process(
    arguments: list[str],
    *,
    directory: Path,
    timeout: float,
    memory_limit_bytes: int,
    disk_limit_bytes: int,
) -> dict[str, Any]:
    """Measure one child; wait4 statistics never accumulate across preceding runs."""
    if sys.platform != 'linux' or not hasattr(os, 'wait4'):
        raise MeasurementError('UNSUPPORTED_PLATFORM', 'Linux with wait4 is required')
    with (
        tempfile.TemporaryFile(dir=directory) as stdout,
        tempfile.TemporaryFile(dir=directory) as stderr,
        tempfile.TemporaryFile(dir=directory) as child_metrics,
    ):
        start = time.perf_counter()
        # 使用文件接收输出，避免子进程等待写满的 PIPE，而父进程又在等待退出。
        process = subprocess.Popen(  # noqa: S603 - fixed isolated Python helper, explicit arguments, no shell.
            [sys.executable, '-I', '-c', CHILD_MEASURER, json.dumps(arguments), str(child_metrics.fileno())],
            cwd=directory,
            stdout=stdout,
            stderr=stderr,
            pass_fds=(child_metrics.fileno(),),
            start_new_session=True,
        )
        guard_error: MeasurementError | None = None
        next_resource_check = start
        try:
            while True:
                waited_pid, status, usage = os.wait4(process.pid, os.WNOHANG)
                if waited_pid:
                    process.returncode = os.waitstatus_to_exitcode(status)
                    break
                now = time.perf_counter()
                if now - start > timeout:
                    guard_error = MeasurementError('RUN_TIMEOUT', 'Run exceeded the frozen timeout', retryable=True)
                if now >= next_resource_check:
                    captured_bytes = os.fstat(stdout.fileno()).st_size + os.fstat(stderr.fileno()).st_size
                    pid = _child_metrics(child_metrics).get('pid')
                    if pid is not None and _resident_bytes(pid) > memory_limit_bytes:
                        guard_error = MeasurementError('MEMORY_LIMIT', 'Child exceeded the memory budget')
                    elif captured_bytes > MAX_CAPTURE_BYTES * 2:
                        guard_error = MeasurementError('CAPTURE_LIMIT', 'Child output exceeded the capture limit')
                    elif _directory_bytes(directory) + captured_bytes > disk_limit_bytes:
                        guard_error = MeasurementError('DISK_LIMIT', 'Run exceeded the disk budget')
                    next_resource_check = now + 0.05
                if guard_error is not None:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    _, status, usage = os.wait4(process.pid, 0)
                    process.returncode = os.waitstatus_to_exitcode(status)
                    break
                time.sleep(0.002)
        except BaseException:
            # 只清理本工具刚创建的子进程组，保证中断后没有测量进程残留。
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            if process.returncode is None:
                _, status, _ = os.wait4(process.pid, 0)
                process.returncode = os.waitstatus_to_exitcode(status)
            raise
        if guard_error is not None:
            raise guard_error
        if process.returncode != 0:
            raise MeasurementError('CHILD_FAILED', f'Measurement helper exited with status {process.returncode}')
        metrics = _child_metrics(child_metrics)
        if not all(key in metrics for key in ('peak_rss_bytes', 'external_wall_seconds', 'returncode')):
            raise MeasurementError('INVALID_METRIC', 'Per-child wait4 statistics are unavailable')
        elapsed = metrics['external_wall_seconds']
        peak_rss = metrics['peak_rss_bytes']
        if peak_rss > memory_limit_bytes:
            raise MeasurementError('MEMORY_LIMIT', 'Child peak RSS exceeded the memory budget')
        if _directory_bytes(directory) > disk_limit_bytes:
            raise MeasurementError('DISK_LIMIT', 'Run exceeded the disk budget')
        if metrics['returncode'] != 0:
            raise MeasurementError('CHILD_FAILED', f'Child exited with status {metrics["returncode"]}')
        stdout.seek(0)
        encoded = stdout.read(MAX_CAPTURE_BYTES + 1)
        if len(encoded) > MAX_CAPTURE_BYTES:
            raise MeasurementError('CAPTURE_LIMIT', 'Child output exceeded the capture limit')
        try:
            payload = json.loads(encoded)
        except (ValueError, UnicodeError) as error:
            raise MeasurementError('INVALID_STDOUT', 'Child did not return a valid JSON object') from error
        if not isinstance(payload, dict):
            raise MeasurementError('INVALID_STDOUT', 'Child did not return a JSON object')
        return {'external_wall_seconds': elapsed, 'peak_rss_bytes': peak_rss, 'payload': payload}


def paired_statistics(pairs: list[dict[str, Any]]) -> dict[str, Any]:
    """Positive improvement means the candidate is faster; retain every paired observation."""
    if not pairs:
        return {}
    summary: dict[str, Any] = {}
    for metric in ('external_wall_seconds', 'peak_rss_bytes'):
        baseline = [pair['baseline'][metric] for pair in pairs]
        candidate = [pair['candidate'][metric] for pair in pairs]
        if any(not math.isfinite(value) or value <= 0 for value in (*baseline, *candidate)):
            raise MeasurementError('INVALID_METRIC', 'Paired metrics must be finite and positive')
        improvement = [(left - right) / left for left, right in zip(baseline, candidate, strict=True)]
        summary[metric] = {
            'baseline_median': statistics.median(baseline),
            'candidate_median': statistics.median(candidate),
            'paired_median_improvement': statistics.median(improvement),
            'median_absolute_paired_difference': statistics.median(abs(value) for value in improvement),
            'candidate_wins': sum(right < left for left, right in zip(baseline, candidate, strict=True)),
            'pair_count': len(pairs),
        }
    for role in ('baseline', 'candidate'):
        summary[f'{role}_max_output_bytes'] = max(pair[role]['output_size_bytes'] for pair in pairs)
    return summary


def noise_check(pairs: list[dict[str, Any]]) -> dict[str, Any]:
    stats = paired_statistics(pairs)
    wall = stats['external_wall_seconds']
    reasons = []
    if abs(wall['paired_median_improvement']) >= 0.01:
        reasons.append('wall_median_difference')
    if wall['median_absolute_paired_difference'] > 0.02:
        reasons.append('wall_absolute_pair_noise')
    stages = set(pairs[0]['baseline']['stage_timings'])
    stage_differences = {}
    for stage in sorted(stages):
        values = []
        for pair in pairs:
            left = pair['baseline']['stage_timings'].get(stage)
            right = pair['candidate']['stage_timings'].get(stage)
            if left is None or right is None:
                raise MeasurementError('STAGE_MISMATCH', 'Stage timing keys changed between runs')
            if left > 0:
                values.append((left - right) / left)
        if values:
            stage_differences[stage] = statistics.median(values)
    # 只对调用者选定的目标阶段执行阶段噪声门禁；其他阶段仍完整记录。
    return {'passed': not reasons, 'reasons': reasons, 'stage_median_differences': stage_differences}


def _business_summary(payload: dict[str, Any]) -> dict[str, Any]:
    if any(field not in payload for field in SUMMARY_FIELDS) or 'quality_metrics' not in payload:
        raise MeasurementError('INVALID_SUMMARY', 'CLI business summary is incomplete')
    summary = {field: payload[field] for field in SUMMARY_FIELDS}
    summary['quality_metrics'] = [
        {key: item[key] for key in ('category', 'metric', 'value')} for item in payload['quality_metrics']
    ]
    return summary


def _one_run(
    *,
    role: str,
    alias: str,
    binary: Path,
    root: Path,
    input_path: Path,
    config_path: Path | None,
    options: dict[str, Any],
    input_identity: dict[str, Any],
    keep_output: bool = False,
) -> dict[str, Any]:
    directory = root / alias
    directory.mkdir()
    output = directory / 'output.xlsx'
    manifest_path = directory / 'manifest.json'
    arguments = [str(binary), options['pipeline'], '--input', str(input_path), '--benchmark', '--redact-paths']
    arguments += ['--summary-output', str(manifest_path)]
    if options['mode'] == 'normal':
        arguments += ['--output', str(output)]
    else:
        arguments.append('--check-only')
    if config_path is not None:
        arguments += ['--config', str(config_path)]
    for key in ('month_start', 'month_end'):
        if options[key] is not None:
            arguments += [f'--{key.replace("_", "-")}', options[key]]
    measured = run_process(
        arguments,
        directory=directory,
        timeout=options['timeout_seconds'],
        memory_limit_bytes=options['memory_limit_bytes'],
        disk_limit_bytes=options['disk_limit_bytes'],
    )
    payload = measured.pop('payload')
    if payload.get('status') != 'succeeded' or payload.get('pipeline') != options['pipeline']:
        raise MeasurementError('INVALID_SUMMARY', 'CLI run summary does not describe a successful requested pipeline')
    expected_output = options['mode'] == 'normal'
    if payload.get('output_written') is not expected_output or payload.get('sheet_count') != 3:
        raise MeasurementError('INVALID_SUMMARY', 'CLI output mode or sheet count differs')
    try:
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        result = manifest['result']
        if (
            manifest['schema_version'] != 1
            or manifest['status'] != 'succeeded'
            or manifest['execution']['mode'] != options['mode']
            or manifest['execution']['pipeline'] != options['pipeline']
            or manifest['input']['sha256'] != input_identity['sha256']
            or manifest['input']['size_bytes'] != input_identity['size_bytes']
            or manifest['run_counts'] != payload['run_counts']
            or manifest['quality']['error_log_count'] != payload['error_log_count']
            or manifest['quality']['issue_type_counts'] != payload['issue_type_counts']
            or manifest['quality']['quality_metrics'] != payload['quality_metrics']
            or result['output_written'] is not expected_output
            or result['final_output_valid'] is not expected_output
        ):
            raise MeasurementError('MANIFEST_MISMATCH', 'Manifest identity, mode, or business counts differ')
        output_identity = file_identity(output) if expected_output else {'sha256': None, 'size_bytes': 0}
        if expected_output:
            if (
                result['output_sha256'] != output_identity['sha256']
                or result['output_size_bytes'] != output_identity['size_bytes']
            ):
                raise MeasurementError('OUTPUT_IDENTITY_MISMATCH', 'Published workbook identity differs from Manifest')
        elif output.exists() or result['output_sha256'] is not None:
            raise MeasurementError('CHECK_ONLY_OUTPUT', 'check-only unexpectedly published a workbook')
        stages = payload['stage_timings']['stages']
        if not stages or any(
            not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0 for value in stages.values()
        ):
            raise MeasurementError('INVALID_METRIC', 'Stage timings must be finite nonnegative numbers')
        business = _business_summary(payload)
        business['effective_config_sha256'] = manifest['config']['effective_sha256']
        business['sheet_names'] = result['sheet_names']
        business['low_memory_writer'] = manifest['execution']['low_memory_writer']
        warnings = manifest['warnings']
        if not isinstance(warnings, list) or not all(isinstance(item, str) for item in warnings):
            raise MeasurementError('INVALID_MANIFEST', 'Manifest warnings must be a list of text values')
        # 告警文本可能含路径，只保留数量和哈希用于跨运行比较。
        business['warning_count'] = len(warnings)
        business['warnings_sha256'] = hashlib.sha256(json.dumps(warnings, ensure_ascii=False).encode()).hexdigest()
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise MeasurementError(
            'INVALID_MANIFEST', 'Required Manifest or summary fields could not be verified'
        ) from error
    if any(path.name.startswith(('.costing-', '.tmp')) for path in directory.rglob('*')):
        raise MeasurementError('TEMPORARY_RESIDUE', 'Run left staging files behind')
    if not keep_output:
        # 哈希和大小已记录；大样本每次运行会产生数十 MB，批量实验不保留。
        output.unlink(missing_ok=True)
    return {
        'role': role,
        'run_alias': alias,
        **measured,
        'output_size_bytes': output_identity['size_bytes'],
        'output_sha256': output_identity['sha256'],
        'stage_timings': stages,
        'summary': business,
        'temporary_residue_count': 0,
        'low_memory_writer': manifest['execution']['low_memory_writer'],
        'build_identity': manifest['application'],
    }


def measure_pairs(
    *,
    baseline_binary: Path,
    candidate_binary: Path,
    input_path: Path,
    output_dir: Path,
    pipeline: str,
    config_path: Path | None = None,
    mode: str = 'normal',
    pairs: int = 8,
    timeout: float = 600,
    memory_limit_bytes: int = 2 * 1024**3,
    disk_limit_bytes: int = 2 * 1024**3,
    month_start: str | None = None,
    month_end: str | None = None,
    baseline_source_manifest: Path | None = None,
    candidate_source_manifest: Path | None = None,
    calibration: bool = False,
    target_stage: str | None = None,
    resume: bool = False,
    keep_outputs: bool = False,
    deadline: float | None = None,
) -> dict[str, Any]:
    if sys.platform != 'linux':
        raise MeasurementError('UNSUPPORTED_PLATFORM', 'This measurement entrypoint requires Linux')
    if pipeline not in ('gb', 'sk') or mode not in ('normal', 'check-only'):
        raise MeasurementError('INVALID_ARGUMENT', 'Unsupported pipeline or mode')
    if (
        not (1 <= pairs <= 100)
        or not math.isfinite(timeout)
        or timeout <= 0
        or min(memory_limit_bytes, disk_limit_bytes) <= 0
    ):
        raise MeasurementError('INVALID_ARGUMENT', 'Pair count, timeout and budgets must be positive and bounded')
    paths = {
        'baseline': baseline_binary.resolve(),
        'candidate': candidate_binary.resolve(),
        'input': input_path.resolve(),
    }
    for alias, optional in (
        ('config', config_path),
        ('baseline_source', baseline_source_manifest),
        ('candidate_source', candidate_source_manifest),
    ):
        if optional is not None:
            paths[alias] = optional.resolve()
    try:
        identities = {alias: file_identity(path) for alias, path in paths.items()}
    except OSError as error:
        raise MeasurementError('ARTIFACT_UNREADABLE', 'A required local artifact is not readable') from error
    if any(not os.access(paths[role], os.X_OK) for role in ('baseline', 'candidate')):
        raise MeasurementError('BINARY_NOT_EXECUTABLE', 'Both binaries must be executable')
    if calibration and identities['baseline'] != identities['candidate']:
        raise MeasurementError('INVALID_CALIBRATION', 'Calibration requires the same binary on both sides')
    options = {
        'pipeline': pipeline,
        'mode': mode,
        'pair_count': pairs,
        'timeout_seconds': timeout,
        'memory_limit_bytes': memory_limit_bytes,
        'disk_limit_bytes': disk_limit_bytes,
        'month_start': month_start,
        'month_end': month_end,
        'calibration': calibration,
        'target_stage': target_stage,
    }
    environment = _environment()
    root = output_dir.resolve()
    report_path = root / 'report.json'
    if resume:
        try:
            report = json.loads(report_path.read_text(encoding='utf-8'))
        except (OSError, ValueError) as error:
            raise MeasurementError('RESUME_UNAVAILABLE', 'A resumable measurement report is required') from error
        if not isinstance(report, dict) or report.get('checkpoint_sha256') != _checkpoint_digest(report):
            raise MeasurementError('RESUME_CORRUPT', 'Checkpoint integrity could not be verified')
        if report.get('status') not in ('running', 'interrupted'):
            raise MeasurementError('RESUME_UNAVAILABLE', 'Only unfinished same-session reports may resume')
        if (
            report.get('artifacts') != identities
            or report.get('options') != options
            or report.get('environment') != environment
        ):
            raise MeasurementError(
                'RESUME_IDENTITY_CHANGED', 'Resume requires identical artifacts, settings and Linux session'
            )
        report['attempt'] += 1
        report['status'] = 'running'
        report['finished_utc'] = None
        report['error'] = None
    else:
        try:
            root.mkdir(parents=True, exist_ok=False)
        except FileExistsError as error:
            raise MeasurementError('OUTPUT_EXISTS', 'Measurement output directory already exists') from error
        report = {
            'schema_version': 1,
            'status': 'running',
            'measurement_only': True,
            'started_utc': _utc_now(),
            'finished_utc': None,
            'attempt': 1,
            'artifacts': identities,
            'environment': environment,
            'options': options,
            'warmups': [],
            'samples': [],
            'summary': {},
            'calibration': None,
            'error': None,
        }
    _save_report(report_path, report)

    def assert_identities() -> None:
        if {alias: file_identity(path) for alias, path in paths.items()} != identities:
            raise MeasurementError('ARTIFACT_IDENTITY_CHANGED', 'A frozen artifact changed during measurement')

    def run(role: str, alias: str) -> dict[str, Any]:
        assert_identities()
        run_options = options
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise MeasurementError('BUDGET_EXHAUSTED', 'The experiment time budget has been exhausted')
            run_options = {**options, 'timeout_seconds': min(timeout, remaining)}
        result = _one_run(
            role=role,
            alias=f'attempt-{report["attempt"]}-{alias}',
            binary=paths[role],
            root=root,
            input_path=paths['input'],
            config_path=paths.get('config'),
            options=run_options,
            input_identity=identities['input'],
            keep_output=keep_outputs,
        )
        assert_identities()
        return result

    try:
        warmups = [run(role, f'warmup-{role}') for role in ('baseline', 'candidate')]
        if warmups[0]['summary'] != warmups[1]['summary']:
            raise MeasurementError('BUSINESS_MISMATCH', 'Baseline and candidate business summaries differ')
        report['warmups'].extend(warmups)
        _save_report(report_path, report)
        reference = warmups[0]['summary']
        for number in range(len(report['samples']) + 1, pairs + 1):
            order = ['baseline', 'candidate'] if number % 2 else ['candidate', 'baseline']
            pair: dict[str, Any] = {'pair_number': number, 'execution_order': order}
            for role in order:
                pair[role] = run(role, f'pair-{number:02d}-{role}')
                if pair[role]['summary'] != reference:
                    raise MeasurementError('BUSINESS_MISMATCH', 'Business summary changed between measured runs')
            report['samples'].append(pair)
            report['summary'] = paired_statistics(report['samples'])
            _save_report(report_path, report)
        if calibration:
            report['calibration'] = noise_check(report['samples'])
            if target_stage is not None:
                difference = report['calibration']['stage_median_differences'].get(target_stage)
                if difference is None or abs(difference) >= 0.05:
                    report['calibration']['reasons'].append('target_stage_noise')
                    report['calibration']['passed'] = False
            if not report['calibration']['passed']:
                raise MeasurementError(
                    'NOISE_EXCEEDED', 'Baseline-versus-baseline noise exceeds the frozen gates', retryable=True
                )
        report['status'] = 'valid'
    except KeyboardInterrupt:
        report['status'] = 'interrupted'
        report['error'] = MeasurementError(
            'INTERRUPTED', 'Measurement interrupted; same-session resume is available', retryable=True
        ).payload()
    except (MeasurementError, OSError, ValueError, KeyError, TypeError) as error:
        report['status'] = 'invalid'
        report['error'] = (
            error.payload()
            if isinstance(error, MeasurementError)
            else MeasurementError('MEASUREMENT_FAILED', 'Measurement could not be completed or verified').payload()
        )
    report['finished_utc'] = _utc_now()
    _save_report(report_path, report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('baseline-binary', 'candidate-binary', 'input', 'output-dir'):
        parser.add_argument(f'--{name}', type=Path, required=True)
    parser.add_argument('--pipeline', choices=('gb', 'sk'), required=True)
    parser.add_argument('--config', type=Path)
    parser.add_argument('--mode', choices=('normal', 'check-only'), default='normal')
    parser.add_argument('--pairs', type=int, default=8)
    parser.add_argument('--timeout', type=float, default=600)
    parser.add_argument('--memory-limit-mib', type=int, default=2048)
    parser.add_argument('--disk-limit-mib', type=int, default=2048)
    parser.add_argument('--month-start')
    parser.add_argument('--month-end')
    parser.add_argument('--baseline-source-manifest', type=Path)
    parser.add_argument('--candidate-source-manifest', type=Path)
    parser.add_argument('--calibration', action='store_true')
    parser.add_argument('--target-stage')
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--keep-outputs', action='store_true')
    args = parser.parse_args()
    try:
        report = measure_pairs(
            baseline_binary=args.baseline_binary,
            candidate_binary=args.candidate_binary,
            input_path=args.input,
            output_dir=args.output_dir,
            pipeline=args.pipeline,
            config_path=args.config,
            mode=args.mode,
            pairs=args.pairs,
            timeout=args.timeout,
            memory_limit_bytes=args.memory_limit_mib * 1024**2,
            disk_limit_bytes=args.disk_limit_mib * 1024**2,
            month_start=args.month_start,
            month_end=args.month_end,
            baseline_source_manifest=args.baseline_source_manifest,
            candidate_source_manifest=args.candidate_source_manifest,
            calibration=args.calibration,
            target_stage=args.target_stage,
            resume=args.resume,
            keep_outputs=args.keep_outputs,
        )
        print(
            json.dumps(
                {key: report[key] for key in ('status', 'measurement_only', 'summary', 'calibration', 'error')},
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0 if report['status'] == 'valid' else 2
    except (MeasurementError, OSError) as error:
        failure = (
            error.payload()
            if isinstance(error, MeasurementError)
            else MeasurementError('MEASUREMENT_FAILED', 'Measurement initialization failed').payload()
        )
        print(json.dumps({'status': 'invalid', **failure}, ensure_ascii=False))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())

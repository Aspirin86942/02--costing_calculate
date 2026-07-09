from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.services.costing_service import CostingRunRequest, ServiceStatus, run_costing_request
from tests.rust_oracle.repo_paths import repo_root


@dataclass(frozen=True)
class OracleRunSummary:
    error_log_count: int
    issue_type_counts: dict[str, int]
    quality_metrics: dict[tuple[str, str], str]


def run_python_oracle(pipeline: str, input_path: Path, output_path: Path) -> OracleRunSummary:
    request = CostingRunRequest(
        pipeline=pipeline,
        input_path=input_path,
        output_dir=output_path.parent,
        benchmark=True,
        overwrite_confirmed=True,
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


def run_rust_cli(pipeline: str, input_path: Path, output_path: Path) -> OracleRunSummary:
    return run_rust_cli_release(build_rust_cli_release(), pipeline, input_path, output_path)


def build_rust_cli_release() -> Path:
    cargo = shutil.which('cargo')
    if cargo is None:
        raise AssertionError('cargo executable not found')
    root = repo_root()

    completed = subprocess.run(  # noqa: S603 - test harness invokes local Cargo with fixed arguments.
        [
            cargo,
            'build',
            '--quiet',
            '--release',
            '--manifest-path',
            str(root / 'rust' / 'Cargo.toml'),
            '-p',
            'costing-calculate',
        ],
        check=False,
        capture_output=True,
        cwd=root,
        text=True,
    )
    if completed.returncode != 0:
        raise AssertionError(f'rust release build failed\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}')

    executable_name = 'costing-calculate.exe' if os.name == 'nt' else 'costing-calculate'
    executable = root / 'rust' / 'target' / 'release' / executable_name
    if not executable.exists():
        raise AssertionError(f'rust release executable missing: {executable}')
    return executable


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
        text=True,
    )
    if completed.returncode != 0:
        raise AssertionError(f'rust release cli failed\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}')
    summary = parse_rust_run_summary(completed.stdout)
    if not output_path.exists():
        raise AssertionError(f'rust release cli did not create expected workbook: {output_path}')
    return summary


def parse_rust_run_summary(stdout: str) -> OracleRunSummary:
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise AssertionError(f'Rust CLI stdout is not valid JSON: {exc}\nSTDOUT:\n{stdout}') from exc
    if not isinstance(payload, dict):
        raise AssertionError(f'Rust CLI stdout JSON must be an object, got {type(payload).__name__}')

    return OracleRunSummary(
        error_log_count=_required_int(payload, 'error_log_count'),
        issue_type_counts=_issue_type_counts(payload),
        quality_metrics=_quality_metric_values(_required_list(payload, 'quality_metrics')),
    )


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

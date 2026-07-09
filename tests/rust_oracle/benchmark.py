from __future__ import annotations

import statistics
import time
from dataclasses import dataclass
from pathlib import Path

from tests.rust_oracle.oracle_runner import build_rust_cli_release, run_python_oracle, run_rust_cli_release
from tests.rust_oracle.workbook_compare import compare_workbooks


@dataclass(frozen=True)
class BenchmarkResult:
    pipeline: str
    python_median_seconds: float
    rust_median_seconds: float
    validation_passed: bool
    verdict: str


def run_same_machine_benchmark(pipeline: str, input_path: Path, tmp_path: Path, repeats: int = 3) -> BenchmarkResult:
    python_seconds: list[float] = []
    rust_seconds: list[float] = []
    validation_passed = True
    validation_errors: list[str] = []
    rust_executable = build_rust_cli_release()

    for idx in range(repeats):
        python_output = tmp_path / f'python-{pipeline}-{idx}.xlsx'
        rust_output = tmp_path / f'rust-{pipeline}-{idx}.xlsx'

        start = time.perf_counter()
        run_python_oracle(pipeline, input_path, python_output)
        python_seconds.append(time.perf_counter() - start)

        start = time.perf_counter()
        run_rust_cli_release(rust_executable, pipeline, input_path, rust_output)
        rust_seconds.append(time.perf_counter() - start)

        report = compare_workbooks(python_output, rust_output)
        if not report['passed'] and not validation_errors:
            validation_errors = [str(error) for error in report['errors']]
        validation_passed = validation_passed and bool(report['passed'])

    python_median = statistics.median(python_seconds)
    rust_median = statistics.median(rust_seconds)
    verdict = classify_verdict(validation_passed, python_median, rust_median, validation_errors)
    return BenchmarkResult(pipeline, python_median, rust_median, validation_passed, verdict)


def classify_verdict(
    validation_passed: bool,
    python_median: float,
    rust_median: float,
    validation_errors: list[str] | tuple[str, ...] | None = None,
) -> str:
    if not validation_passed:
        return classify_validation_errors(validation_errors or [])
    if rust_median > python_median:
        return 'PERFORMANCE_REGRESSION'
    return 'VALIDATED'


def classify_validation_errors(errors: list[str] | tuple[str, ...]) -> str:
    for error in errors:
        lowered = error.lower()
        if 'reader snapshot' in lowered or 'reader mismatch' in lowered:
            return 'READER_MISMATCH'
        if error.startswith('value mismatch 成本分析工单维度') or 'anomaly mismatch' in lowered:
            return 'ANALYSIS_MISMATCH'
        if (
            error.startswith('value mismatch 成本计算单总表')
            or error.startswith('value mismatch 成本计算单数量聚合维度')
            or 'normalized row mismatch' in lowered
            or 'fact mismatch' in lowered
            or 'qty mismatch' in lowered
        ):
            return 'ETL_MISMATCH'
    return 'WORKBOOK_MISMATCH'

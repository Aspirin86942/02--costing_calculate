from __future__ import annotations

import dataclasses
import hashlib
import json
import math
import shutil
import statistics
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from tests.rust_oracle.benchmark_protocol import PipelineName
from tests.rust_oracle.oracle_runner import (
    REQUIRED_RUST_PAYLOAD_STAGES,
    TimedPayloadRun,
    _io_path,
    _prepare_local_path,
    assert_runtime_contract_matches,
    build_rust_cli_release,
    run_python_check_only_payload,
    run_python_oracle,
    run_rust_cli_release,
    run_rust_cli_release_check_only,
)
from tests.rust_oracle.repo_paths import repo_root
from tests.rust_oracle.workbook_compare import WorkbookMismatch, compare_workbooks

CHECK_ONLY_WARMUPS = 1
CHECK_ONLY_ROUNDS = 5


@dataclass(frozen=True)
class ValidationFailure:
    verdict: str
    message: str


@dataclass(frozen=True)
class BenchmarkResult:
    pipeline: str
    python_median_seconds: float
    rust_median_seconds: float
    validation_passed: bool
    verdict: str
    validation_failures: tuple[ValidationFailure, ...] = ()


@dataclass(frozen=True)
class QualityMetricEvidence:
    category: str
    metric: str
    value: str


@dataclass(frozen=True)
class RuntimeEvidence:
    run_counts: dict[str, int]
    error_log_count: int
    issue_type_counts: dict[str, int]
    quality_metrics: tuple[QualityMetricEvidence, ...]


@dataclass(frozen=True)
class StageRegression:
    stage: str
    baseline_median_seconds: float
    current_median_seconds: float
    current_to_baseline_ratio: float


@dataclass(frozen=True)
class CheckOnlyBenchmarkResult:
    pipeline: str
    input_sha256: str
    rust_executable: str
    rust_binary_sha256: str
    git_head: str
    working_tree_diff_id: str
    working_directory: str
    command_arguments: tuple[str, ...]
    python_payload_total_seconds: tuple[float, ...]
    rust_payload_total_seconds: tuple[float, ...]
    python_median_seconds: float
    python_min_seconds: float
    python_max_seconds: float
    rust_median_seconds: float
    rust_min_seconds: float
    rust_max_seconds: float
    rust_stage_seconds: dict[str, tuple[float, ...]]
    rust_stage_median_seconds: dict[str, float]
    rust_runtime_evidence: RuntimeEvidence
    valid_pair_count: int
    validation_passed: bool
    verdict: str
    validation_failures: tuple[ValidationFailure, ...] = ()


def run_same_machine_benchmark(
    pipeline: PipelineName,
    input_path: Path,
    tmp_path: Path,
    repeats: int = 3,
) -> BenchmarkResult:
    python_seconds: list[float] = []
    rust_seconds: list[float] = []
    validation_failures: list[ValidationFailure] = []
    rust_executable = build_rust_cli_release()

    for idx in range(repeats):
        python_output = tmp_path / f'python-{pipeline}-{idx}.xlsx'
        rust_output = tmp_path / f'rust-{pipeline}-{idx}.xlsx'

        start = time.perf_counter()
        python_summary = run_python_oracle(pipeline, input_path, python_output)
        python_seconds.append(time.perf_counter() - start)

        start = time.perf_counter()
        rust_summary = run_rust_cli_release(rust_executable, pipeline, input_path, rust_output)
        rust_seconds.append(time.perf_counter() - start)

        try:
            assert_runtime_contract_matches(python_summary, rust_summary)
        except AssertionError as exc:
            validation_failures.append(
                ValidationFailure(
                    verdict='ETL_MISMATCH',
                    message=f'iteration {idx} runtime contract mismatch: {exc}',
                )
            )

        report = compare_workbooks(python_output, rust_output, pipeline=pipeline)
        if not report.passed:
            validation_failures.append(
                ValidationFailure(
                    verdict=classify_validation_errors(report.mismatches),
                    message=f'iteration {idx} workbook mismatch: {report.mismatches!r}',
                )
            )

    python_median = statistics.median(python_seconds)
    rust_median = statistics.median(rust_seconds)
    validation_passed = not validation_failures
    verdict = (
        validation_failures[0].verdict if validation_failures else classify_verdict(True, python_median, rust_median)
    )
    return BenchmarkResult(
        pipeline,
        python_median,
        rust_median,
        validation_passed,
        verdict,
        tuple(validation_failures),
    )


def classify_verdict(
    validation_passed: bool,
    python_median: float,
    rust_median: float,
    validation_errors: list[str | WorkbookMismatch] | tuple[str | WorkbookMismatch, ...] | None = None,
) -> str:
    if not validation_passed:
        return classify_validation_errors(validation_errors or [])
    if rust_median > python_median:
        return 'PERFORMANCE_REGRESSION'
    return 'VALIDATED'


_DATA_MISMATCH_KINDS = frozenset(
    {
        'value_mismatch',
        'storage_type_mismatch',
        'column_total_mismatch',
        'group_total_mismatch',
        'required_header_missing',
        'unexpected_numeric_header',
        'numeric_storage_invalid',
        'shared_string_index_out_of_range',
    }
)


def classify_validation_errors(
    errors: list[str | WorkbookMismatch] | tuple[str | WorkbookMismatch, ...],
) -> str:
    for error in errors:
        if isinstance(error, WorkbookMismatch):
            if error.mismatch_kind not in _DATA_MISMATCH_KINDS:
                continue
            if error.sheet == '成本分析工单维度':
                return 'ANALYSIS_MISMATCH'
            if error.sheet in {'成本计算单总表', '成本计算单数量聚合维度'}:
                return 'ETL_MISMATCH'
            continue
        lowered = error.lower()
        if 'reader snapshot' in lowered or 'reader mismatch' in lowered:
            return 'READER_MISMATCH'
        if 'anomaly mismatch' in lowered:
            return 'ANALYSIS_MISMATCH'
        if 'normalized row mismatch' in lowered or 'fact mismatch' in lowered or 'qty mismatch' in lowered:
            return 'ETL_MISMATCH'
    return 'WORKBOOK_MISMATCH'


def classify_check_only_verdict(
    *,
    rust_seconds: tuple[float, ...],
    python_seconds: tuple[float, ...],
    rust_stage_seconds: dict[str, tuple[float, ...]],
    valid_pair_count: int,
    validation_failures: tuple[ValidationFailure, ...],
) -> str:
    if len(rust_seconds) != CHECK_ONLY_ROUNDS or len(python_seconds) != CHECK_ONLY_ROUNDS:
        return 'INCOMPLETE_EVIDENCE'
    if valid_pair_count != CHECK_ONLY_ROUNDS:
        return 'INCOMPLETE_EVIDENCE'
    if set(rust_stage_seconds) != set(REQUIRED_RUST_PAYLOAD_STAGES):
        return 'INCOMPLETE_EVIDENCE'
    if any(len(values) != CHECK_ONLY_ROUNDS for values in rust_stage_seconds.values()):
        return 'INCOMPLETE_EVIDENCE'
    if any(failure.verdict == 'INCOMPLETE_EVIDENCE' for failure in validation_failures):
        return 'INCOMPLETE_EVIDENCE'
    if validation_failures:
        return validation_failures[0].verdict
    if statistics.median(rust_seconds) > statistics.median(python_seconds):
        return 'PERFORMANCE_REGRESSION'
    return 'VALIDATED'


def run_check_only_payload_benchmark(
    pipeline: str,
    input_path: Path,
    rust_executable: Path,
) -> CheckOnlyBenchmarkResult:
    input_path = input_path.resolve()
    rust_executable = rust_executable.resolve()
    input_sha256 = _sha256(input_path)
    binary_sha256 = _sha256(rust_executable)
    git_head, working_tree_diff_id = _git_evidence()
    command_arguments = (
        pipeline,
        '--input',
        str(input_path),
        '--check-only',
        '--benchmark',
    )

    # 预热固定不计入正式数组，避免首次启动成本污染五轮配对统计。
    run_rust_cli_release_check_only(rust_executable, pipeline, input_path)
    run_python_check_only_payload(pipeline, input_path)

    python_seconds: list[float] = []
    rust_seconds: list[float] = []
    rust_stage_values: dict[str, list[float]] = {name: [] for name in REQUIRED_RUST_PAYLOAD_STAGES}
    validation_failures: list[ValidationFailure] = []
    valid_pair_count = 0
    first_rust_evidence: RuntimeEvidence | None = None

    for round_index in range(CHECK_ONLY_ROUNDS):
        # 奇偶轮交换先后顺序，降低固定执行顺序带来的系统性偏差。
        if round_index % 2 == 0:
            rust_run = run_rust_cli_release_check_only(rust_executable, pipeline, input_path)
            python_run = run_python_check_only_payload(pipeline, input_path)
        else:
            python_run = run_python_check_only_payload(pipeline, input_path)
            rust_run = run_rust_cli_release_check_only(rust_executable, pipeline, input_path)

        rust_seconds.append(rust_run.payload_total_seconds)
        python_seconds.append(python_run.payload_total_seconds)
        for stage in REQUIRED_RUST_PAYLOAD_STAGES:
            rust_stage_values[stage].append(rust_run.stage_timings[stage])
        rust_evidence = _runtime_evidence(rust_run)
        try:
            assert_runtime_contract_matches(python_run.runtime_summary, rust_run.runtime_summary)
            if first_rust_evidence is not None and rust_evidence != first_rust_evidence:
                raise AssertionError('Rust runtime evidence changed between formal rounds')
        except AssertionError as exc:
            validation_failures.append(ValidationFailure('ETL_MISMATCH', f'round {round_index + 1}: {exc}'))
        else:
            valid_pair_count += 1
        if first_rust_evidence is None:
            first_rust_evidence = rust_evidence

    if _sha256(input_path) != input_sha256:
        validation_failures.append(ValidationFailure('INCOMPLETE_EVIDENCE', 'input SHA-256 changed during benchmark'))
    if _sha256(rust_executable) != binary_sha256:
        validation_failures.append(
            ValidationFailure('INCOMPLETE_EVIDENCE', 'Rust binary SHA-256 changed during benchmark')
        )
    if _git_evidence() != (git_head, working_tree_diff_id):
        validation_failures.append(
            ValidationFailure('INCOMPLETE_EVIDENCE', 'Git working state changed during benchmark')
        )

    rust_values = tuple(rust_seconds)
    python_values = tuple(python_seconds)
    failures = tuple(validation_failures)
    stage_values = {stage: tuple(values) for stage, values in rust_stage_values.items()}
    if first_rust_evidence is None:
        raise AssertionError('formal benchmark produced no Rust runtime evidence')
    verdict = classify_check_only_verdict(
        rust_seconds=rust_values,
        python_seconds=python_values,
        rust_stage_seconds=stage_values,
        valid_pair_count=valid_pair_count,
        validation_failures=failures,
    )
    return CheckOnlyBenchmarkResult(
        pipeline=pipeline,
        input_sha256=input_sha256,
        rust_executable=str(rust_executable),
        rust_binary_sha256=binary_sha256,
        git_head=git_head,
        working_tree_diff_id=working_tree_diff_id,
        working_directory=str(repo_root()),
        command_arguments=command_arguments,
        python_payload_total_seconds=python_values,
        rust_payload_total_seconds=rust_values,
        python_median_seconds=statistics.median(python_values),
        python_min_seconds=min(python_values),
        python_max_seconds=max(python_values),
        rust_median_seconds=statistics.median(rust_values),
        rust_min_seconds=min(rust_values),
        rust_max_seconds=max(rust_values),
        rust_stage_seconds=stage_values,
        rust_stage_median_seconds={stage: statistics.median(values) for stage, values in rust_stage_values.items()},
        rust_runtime_evidence=first_rust_evidence,
        valid_pair_count=valid_pair_count,
        validation_passed=not failures and valid_pair_count == CHECK_ONLY_ROUNDS,
        verdict=verdict,
        validation_failures=failures,
    )


def _runtime_evidence(run: TimedPayloadRun) -> RuntimeEvidence:
    summary = run.runtime_summary
    if sum(summary.issue_type_counts.values()) != summary.error_log_count:
        raise AssertionError('issue_type_counts must sum to error_log_count')
    quality_metrics = tuple(
        QualityMetricEvidence(category, metric, value)
        for (category, metric), value in sorted(summary.quality_metrics.items())
    )
    return RuntimeEvidence(
        run_counts=dict(sorted(run.run_counts.items())),
        error_log_count=summary.error_log_count,
        issue_type_counts=dict(sorted(summary.issue_type_counts.items())),
        quality_metrics=quality_metrics,
    )


def _git_evidence() -> tuple[str, str]:
    root = repo_root()
    head = _run_git(root, 'rev-parse', 'HEAD').strip()
    status = _run_git(root, 'status', '--porcelain=v1')
    diff = _run_git(root, 'diff', '--binary', 'HEAD', '--')
    diff_id = hashlib.sha256(f'{status}\n{diff}'.encode('utf-8')).hexdigest()  # noqa: UP012
    return head, diff_id


def _run_git(root: Path, *args: str) -> str:
    git = shutil.which('git')
    if git is None:
        raise AssertionError('git executable not found')
    git_executable = str(Path(git).resolve())
    completed = subprocess.run(  # noqa: S603 - fixed local Git executable and arguments.
        [git_executable, '-C', str(root), *args],
        check=False,
        capture_output=True,
        encoding='utf-8',
        errors='replace',
    )
    if completed.returncode != 0:
        raise AssertionError(f'git command failed: {args!r}\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}')
    return completed.stdout


def _sha256(path: Path) -> str:
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def compare_non_target_stage_medians(
    baseline_path: Path,
    current_path: Path,
    *,
    target_stages: frozenset[str],
) -> tuple[StageRegression, ...]:
    baseline = json.loads(baseline_path.read_text(encoding='utf-8'))
    current = json.loads(current_path.read_text(encoding='utf-8'))
    if baseline['pipeline'] != current['pipeline'] or baseline['input_sha256'] != current['input_sha256']:
        raise AssertionError('stage comparison requires the same pipeline and input SHA-256')
    regressions: list[StageRegression] = []
    for stage in REQUIRED_RUST_PAYLOAD_STAGES:
        if stage == 'total' or stage in target_stages:
            continue
        before = _stage_median_seconds(baseline, stage, evidence_name='baseline')
        after = _stage_median_seconds(current, stage, evidence_name='current')
        ratio = after / before if before > 0 else (1.0 if after == 0 else math.inf)
        if ratio > 1.05:
            regressions.append(StageRegression(stage, before, after, ratio))
    return tuple(regressions)


def _stage_median_seconds(payload: object, stage: str, *, evidence_name: str) -> float:
    if not isinstance(payload, dict):
        raise AssertionError(f'{evidence_name} evidence must be a JSON object')
    medians = payload.get('rust_stage_median_seconds')
    if not isinstance(medians, dict):
        raise AssertionError(f'{evidence_name} rust_stage_median_seconds must be an object')
    value = medians.get(stage)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AssertionError(f'{evidence_name} stage {stage!r} median must be a finite non-negative number')
    seconds = float(value)
    if not math.isfinite(seconds) or seconds < 0:
        raise AssertionError(f'{evidence_name} stage {stage!r} median must be a finite non-negative number')
    return seconds


def assert_same_input_sha256(evidence_paths: tuple[Path, ...]) -> str:
    if not evidence_paths:
        raise AssertionError('at least one evidence path is required')
    hashes: list[str] = []
    for path in evidence_paths:
        payload = json.loads(path.read_text(encoding='utf-8-sig'))
        value = payload.get('input_sha256')
        if not isinstance(value, str) or len(value) != 64:
            raise AssertionError(f'evidence has invalid input_sha256: {path}')
        hashes.append(value)
    if len(set(hashes)) != 1:
        raise AssertionError(f'performance evidence input SHA-256 mismatch: {hashes!r}')
    return hashes[0]


def write_local_check_only_result(
    result: CheckOnlyBenchmarkResult,
    output_path: Path,
) -> None:
    root = repo_root().resolve()
    destination = _prepare_local_path(
        output_path,
        allowed_roots=(root / 'rust' / 'target', root / 'data' / 'processed'),
        purpose='local check-only results must stay below rust/target or data/processed',
        create_parent=False,
    )
    _io_path(destination.parent).mkdir(parents=True, exist_ok=True)
    destination = _prepare_local_path(
        destination,
        allowed_roots=(root / 'rust' / 'target', root / 'data' / 'processed'),
        purpose='local check-only results must stay below rust/target or data/processed',
        create_parent=False,
    )
    with _io_path(destination).open('x', encoding='utf-8', newline='\n') as stream:
        json.dump(dataclasses.asdict(result), stream, ensure_ascii=False, indent=2)


def write_check_only_benchmark_result(result: CheckOnlyBenchmarkResult, output_path: Path) -> None:
    """Compatibility wrapper; raw check-only data remains local-only."""
    write_local_check_only_result(result, output_path)

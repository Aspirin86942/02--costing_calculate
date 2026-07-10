from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.rust_oracle import benchmark
from tests.rust_oracle.benchmark import classify_verdict, run_same_machine_benchmark
from tests.rust_oracle.oracle_runner import OracleRunSummary
from tests.rust_oracle.repo_paths import repo_root


def _sample_from_env(env_name: str) -> Path | None:
    value = os.environ.get(env_name)
    if not value:
        return None
    path = Path(value)
    return path if path.exists() else None


def _first_sample(env_name: str, patterns: tuple[str, ...]) -> Path | None:
    env_path = _sample_from_env(env_name)
    if env_path is not None:
        return env_path
    root = repo_root()
    for pattern in patterns:
        matches = sorted(root.glob(pattern))
        if matches:
            return matches[0]
    return None


def test_classify_verdict_requires_validation_and_no_regression() -> None:
    assert classify_verdict(True, 10.0, 9.0) == 'VALIDATED'
    assert classify_verdict(False, 10.0, 9.0) == 'WORKBOOK_MISMATCH'
    assert classify_verdict(True, 10.0, 10.1) == 'PERFORMANCE_REGRESSION'


def test_classify_verdict_preserves_earliest_failure_layer() -> None:
    assert classify_verdict(False, 10.0, 9.0, ['reader snapshot mismatch: row 1']) == 'READER_MISMATCH'
    assert classify_verdict(False, 10.0, 9.0, ['value mismatch 成本计算单数量聚合维度!2,1']) == 'ETL_MISMATCH'
    assert classify_verdict(False, 10.0, 9.0, ['value mismatch 成本分析工单维度!2,35']) == 'ANALYSIS_MISMATCH'
    assert classify_verdict(False, 10.0, 9.0, ['freeze panes mismatch 成本计算单总表']) == 'WORKBOOK_MISMATCH'


def test_benchmark_rejects_runtime_mismatch_even_when_workbooks_match(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    python_summary = OracleRunSummary(
        error_log_count=1,
        issue_type_counts={'MISSING_AMOUNT': 1},
        quality_metrics={('行数勾稽', '成本明细输入行数'): '1'},
    )
    rust_summary = OracleRunSummary(
        error_log_count=0,
        issue_type_counts={},
        quality_metrics={('行数勾稽', '成本明细输入行数'): '1'},
    )

    monkeypatch.setattr(benchmark, 'build_rust_cli_release', lambda: tmp_path / 'costing-calculate')
    monkeypatch.setattr(benchmark, 'run_python_oracle', lambda *_args: python_summary)
    monkeypatch.setattr(benchmark, 'run_rust_cli_release', lambda *_args: rust_summary)
    monkeypatch.setattr(benchmark, 'compare_workbooks', lambda *_args: {'passed': True, 'errors': []})

    result = run_same_machine_benchmark('gb', tmp_path / 'input.xlsx', tmp_path, repeats=1)

    assert not result.validation_passed
    assert result.verdict == 'ETL_MISMATCH'


@pytest.mark.skipif(_first_sample('COSTING_GB_SAMPLE', ('data/raw/gb/*.xlsx',)) is None, reason='GB raw sample missing')
def test_gb_rust_benchmark_validated(tmp_path: Path) -> None:
    input_path = _first_sample('COSTING_GB_SAMPLE', ('data/raw/gb/*.xlsx',))
    assert input_path is not None
    result = run_same_machine_benchmark('gb', input_path, tmp_path, repeats=3)
    assert result.verdict == 'VALIDATED', result


@pytest.mark.skipif(_first_sample('COSTING_SK_SAMPLE', ('data/raw/sk/*.xlsx',)) is None, reason='SK raw sample missing')
def test_sk_rust_benchmark_validated(tmp_path: Path) -> None:
    input_path = _first_sample('COSTING_SK_SAMPLE', ('data/raw/sk/*.xlsx',))
    assert input_path is not None
    result = run_same_machine_benchmark('sk', input_path, tmp_path, repeats=3)
    assert result.verdict == 'VALIDATED', result

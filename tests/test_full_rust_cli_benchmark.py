from __future__ import annotations

from pathlib import Path

import pytest

from tests.rust_oracle import benchmark
from tests.rust_oracle.benchmark import classify_verdict, run_same_machine_benchmark
from tests.rust_oracle.oracle_runner import OracleRunSummary
from tests.rust_oracle.workbook_compare import WorkbookComparisonReport, WorkbookMismatch


def test_classify_verdict_requires_validation_and_no_regression() -> None:
    assert classify_verdict(True, 10.0, 9.0) == 'VALIDATED'
    assert classify_verdict(False, 10.0, 9.0) == 'WORKBOOK_MISMATCH'
    assert classify_verdict(True, 10.0, 10.1) == 'PERFORMANCE_REGRESSION'


def test_classify_verdict_preserves_earliest_failure_layer() -> None:
    assert classify_verdict(False, 10.0, 9.0, ['reader snapshot mismatch: row 1']) == 'READER_MISMATCH'
    assert (
        classify_verdict(
            False,
            10.0,
            9.0,
            [WorkbookMismatch('成本计算单数量聚合维度', 'A2', 'value_mismatch', 'n', 'n')],
        )
        == 'ETL_MISMATCH'
    )
    assert (
        classify_verdict(
            False,
            10.0,
            9.0,
            [WorkbookMismatch('成本分析工单维度', 'AI2', 'value_mismatch', 'n', 'n')],
        )
        == 'ANALYSIS_MISMATCH'
    )
    assert (
        classify_verdict(False, 10.0, 9.0, [WorkbookMismatch('成本计算单总表', None, 'freeze_panes_mismatch')])
        == 'WORKBOOK_MISMATCH'
    )


def test_blank_quantity_group_key_is_an_etl_mismatch() -> None:
    mismatch = WorkbookMismatch('成本计算单数量聚合维度', None, 'blank_group_key')

    assert classify_verdict(False, 10.0, 9.0, [mismatch]) == 'ETL_MISMATCH'


def test_duplicate_work_order_group_key_is_an_analysis_mismatch() -> None:
    mismatch = WorkbookMismatch('成本分析工单维度', None, 'duplicate_group_key')

    assert classify_verdict(False, 10.0, 9.0, [mismatch]) == 'ANALYSIS_MISMATCH'


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
    monkeypatch.setattr(
        benchmark,
        'compare_workbooks',
        lambda *_args, **_kwargs: WorkbookComparisonReport(passed=True, mismatches=()),
    )

    result = run_same_machine_benchmark('gb', tmp_path / 'input.xlsx', tmp_path, repeats=1)

    assert not result.validation_passed
    assert result.verdict == 'ETL_MISMATCH'

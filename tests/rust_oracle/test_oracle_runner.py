from __future__ import annotations

import pytest

from tests.rust_oracle.oracle_runner import (
    OracleRunSummary,
    assert_runtime_contract_matches,
    parse_rust_run_summary,
)


def test_parse_rust_run_summary_reads_runtime_contract() -> None:
    summary = parse_rust_run_summary(
        """{
            "error_log_count": 3,
            "issue_type_counts": {"MISSING_AMOUNT": 1, "NON_POSITIVE_UNIT_COST": 2},
            "quality_metrics": [
                {"category": "行数勾稽", "metric": "数量页输入行数", "value": "2", "description": "ignored"}
            ]
        }"""
    )

    assert summary == OracleRunSummary(
        error_log_count=3,
        issue_type_counts={'MISSING_AMOUNT': 1, 'NON_POSITIVE_UNIT_COST': 2},
        quality_metrics={('行数勾稽', '数量页输入行数'): '2'},
    )


def test_parse_rust_run_summary_rejects_non_json_stdout() -> None:
    with pytest.raises(AssertionError, match='valid JSON'):
        parse_rust_run_summary('not json')


def test_runtime_contract_match_accepts_equal_summaries() -> None:
    summary = OracleRunSummary(
        error_log_count=1,
        issue_type_counts={'NON_POSITIVE_UNIT_COST': 1},
        quality_metrics={('行数勾稽', '数量页输入行数'): '1'},
    )

    assert_runtime_contract_matches(summary, summary)


def test_runtime_contract_match_allows_rust_only_quality_metric() -> None:
    expected = OracleRunSummary(
        error_log_count=1,
        issue_type_counts={'NON_POSITIVE_UNIT_COST': 1},
        quality_metrics={('行数勾稽', '数量页输入行数'): '1'},
    )
    actual = OracleRunSummary(
        error_log_count=1,
        issue_type_counts={'NON_POSITIVE_UNIT_COST': 1},
        quality_metrics={
            ('行数勾稽', '数量页输入行数'): '1',
            ('范围检查', '完工数量小于等于0行数'): '0',
        },
    )

    assert_runtime_contract_matches(expected, actual)


def test_runtime_contract_match_reports_error_log_and_issue_type_mismatches() -> None:
    expected = OracleRunSummary(
        error_log_count=2,
        issue_type_counts={'MISSING_AMOUNT': 1, 'NON_POSITIVE_UNIT_COST': 1},
        quality_metrics={('行数勾稽', '数量页输入行数'): '2'},
    )
    actual = OracleRunSummary(
        error_log_count=1,
        issue_type_counts={'MISSING_AMOUNT': 1},
        quality_metrics={('行数勾稽', '数量页输入行数'): '1'},
    )

    with pytest.raises(AssertionError, match='error_log_count mismatch') as exc_info:
        assert_runtime_contract_matches(expected, actual)

    assert 'issue_type_counts mismatch' in str(exc_info.value)
    assert 'quality_metrics mismatch' in str(exc_info.value)


def test_runtime_contract_match_reports_missing_quality_metric() -> None:
    expected = OracleRunSummary(
        error_log_count=1,
        issue_type_counts={'MISSING_AMOUNT': 1},
        quality_metrics={('行数勾稽', '数量页输入行数'): '1'},
    )
    actual = OracleRunSummary(error_log_count=1, issue_type_counts={'MISSING_AMOUNT': 1}, quality_metrics={})

    with pytest.raises(AssertionError, match='quality_metrics mismatch') as exc_info:
        assert_runtime_contract_matches(expected, actual)

    assert 'missing=' in str(exc_info.value)


def test_runtime_contract_match_reports_changed_quality_metric_value() -> None:
    expected = OracleRunSummary(
        error_log_count=1,
        issue_type_counts={'MISSING_AMOUNT': 1},
        quality_metrics={('行数勾稽', '数量页输入行数'): '1'},
    )
    actual = OracleRunSummary(
        error_log_count=1,
        issue_type_counts={'MISSING_AMOUNT': 1},
        quality_metrics={('行数勾稽', '数量页输入行数'): '2'},
    )

    with pytest.raises(AssertionError, match='quality_metrics mismatch') as exc_info:
        assert_runtime_contract_matches(expected, actual)

    assert 'values=' in str(exc_info.value)

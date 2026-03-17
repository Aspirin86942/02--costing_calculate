from __future__ import annotations

from src.analytics.qty_enricher import build_report_artifacts
from tests.contracts._workbook_contract_helper import (
    _build_default_detail_df,
    _build_default_qty_df,
    load_contract_baseline,
)


def test_error_log_columns_and_retryable_default_match_contract() -> None:
    baseline = load_contract_baseline('error_log_contract.json')

    artifacts = build_report_artifacts(_build_default_detail_df(), _build_default_qty_df())

    assert artifacts.error_log.columns.tolist() == baseline['columns']
    assert artifacts.error_log['retryable'].dropna().eq(baseline['retryable_default']).all()


def test_error_log_issue_types_remain_within_contract_sets() -> None:
    baseline = load_contract_baseline('error_log_contract.json')
    artifacts = build_report_artifacts(_build_default_detail_df(), _build_default_qty_df())

    allowed_issue_types = set(baseline['stable_issue_types']) | set(baseline['legacy_issue_types'])
    assert set(artifacts.error_log['issue_type']).issubset(allowed_issue_types)
    assert baseline['stable_issue_types'] == [
        'UNMAPPED_COST_ITEM',
        'MISSING_AMOUNT',
        'DUPLICATE_WORK_ORDER_KEY',
        'MOH_BREAKDOWN_MISMATCH',
        'TOTAL_COST_MISMATCH',
        'NON_POSITIVE_UNIT_COST',
    ]

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from tests.contracts._workbook_contract_helper import (
    build_default_contract_workbook,
    build_highlight_contract_workbook,
    extract_highlight_semantics,
    extract_workbook_semantics,
)


def main() -> None:
    baseline_dir = Path(__file__).with_name('baselines')
    baseline_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        default_workbook = build_default_contract_workbook(tmp_path)
        highlight_workbook = build_highlight_contract_workbook(tmp_path)

        workbook_baseline = {
            'default_workbook': extract_workbook_semantics(default_workbook),
            'highlight_workbook': extract_highlight_semantics(highlight_workbook),
        }

    (baseline_dir / 'workbook_semantics.json').write_text(
        json.dumps(workbook_baseline, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )

    error_log_baseline = {
        'columns': [
            'row_id',
            'cost_bucket',
            'product_code',
            'product_name',
            'period',
            'issue_type',
            'field_name',
            'original_value',
            'lhs',
            'rhs',
            'diff',
            'reason',
            'action',
            'retryable',
        ],
        'retryable_default': False,
        'stable_issue_types': [
            'UNMAPPED_COST_ITEM',
            'MISSING_AMOUNT',
            'DUPLICATE_WORK_ORDER_KEY',
            'MOH_BREAKDOWN_MISMATCH',
            'TOTAL_COST_MISMATCH',
            'NON_POSITIVE_UNIT_COST',
        ],
        'legacy_issue_types': ['MISSING_QTY', 'PRICE_MISMATCH'],
    }
    (baseline_dir / 'error_log_contract.json').write_text(
        json.dumps(error_log_baseline, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )


if __name__ == '__main__':
    main()

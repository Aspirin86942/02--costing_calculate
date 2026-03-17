from __future__ import annotations

from tests.contracts._workbook_contract_helper import (
    build_default_contract_workbook,
    build_highlight_contract_workbook,
    extract_highlight_semantics,
    extract_workbook_semantics,
    load_contract_baseline,
)


def test_default_workbook_semantics_match_baseline(tmp_path) -> None:
    baseline = load_contract_baseline('workbook_semantics.json')

    workbook_path = build_default_contract_workbook(tmp_path)
    actual = extract_workbook_semantics(workbook_path)

    assert actual == baseline['default_workbook']


def test_highlight_semantics_match_baseline(tmp_path) -> None:
    baseline = load_contract_baseline('workbook_semantics.json')

    workbook_path = build_highlight_contract_workbook(tmp_path)
    actual = extract_highlight_semantics(workbook_path)

    assert actual == baseline['highlight_workbook']

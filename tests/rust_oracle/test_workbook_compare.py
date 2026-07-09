from __future__ import annotations

from pathlib import Path

from openpyxl import Workbook

from tests.rust_oracle.workbook_compare import compare_workbooks, values_equal


def test_numeric_strings_are_not_equal_to_numbers() -> None:
    assert not values_equal('00123', 123)
    assert not values_equal('2025', 2025)


def test_numbers_use_decimal_tolerance() -> None:
    assert values_equal(1, 1.0000001)


def test_compare_workbooks_detects_value_mismatch(tmp_path: Path) -> None:
    expected = tmp_path / 'expected.xlsx'
    actual = tmp_path / 'actual.xlsx'
    _write_workbook(expected, '00123')
    _write_workbook(actual, 123)

    report = compare_workbooks(expected, actual)

    assert not report['passed']
    assert report['errors'] == ["value mismatch Sheet!1,1: expected='00123', actual=123.0"]


def _write_workbook(path: Path, value: object) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = 'Sheet'
    sheet['A1'] = value
    workbook.save(path)

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


def test_compare_workbooks_rejects_forbidden_sheet(tmp_path: Path) -> None:
    expected = tmp_path / 'expected.xlsx'
    actual = tmp_path / 'actual.xlsx'
    _write_workbook(expected, 'ok')
    _write_workbook(actual, 'ok', extra_sheet='成本分析产品维度')

    report = compare_workbooks(expected, actual)

    assert not report['passed']
    assert 'actual workbook contains forbidden product dimension sheet' in report['errors']


def test_compare_workbooks_detects_shape_mismatch(tmp_path: Path) -> None:
    expected = tmp_path / 'expected.xlsx'
    actual = tmp_path / 'actual.xlsx'
    _write_workbook(expected, 'ok')
    _write_workbook(actual, 'ok', second_value='extra')

    report = compare_workbooks(expected, actual)

    assert not report['passed']
    assert report['errors'] == ['shape mismatch Sheet: expected=1x1, actual=1x2']


def test_compare_workbooks_detects_sheet_metadata_mismatch(tmp_path: Path) -> None:
    expected = tmp_path / 'expected.xlsx'
    actual = tmp_path / 'actual.xlsx'
    _write_workbook(expected, 'ok', freeze_panes='A2', auto_filter='A1:A1')
    _write_workbook(actual, 'ok')

    report = compare_workbooks(expected, actual)

    assert not report['passed']
    assert 'freeze panes mismatch Sheet: expected=A2, actual=None' in report['errors']
    assert 'auto filter mismatch Sheet: expected=A1:A1, actual=None' in report['errors']


def _write_workbook(
    path: Path,
    value: object,
    *,
    second_value: object | None = None,
    extra_sheet: str | None = None,
    freeze_panes: str | None = None,
    auto_filter: str | None = None,
) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = 'Sheet'
    sheet['A1'] = value
    if second_value is not None:
        sheet['B1'] = second_value
    if freeze_panes is not None:
        sheet.freeze_panes = freeze_panes
    if auto_filter is not None:
        sheet.auto_filter.ref = auto_filter
    if extra_sheet is not None:
        workbook.create_sheet(extra_sheet)
    workbook.save(path)

from __future__ import annotations

import xml.etree.ElementTree as ET
from decimal import Decimal, InvalidOperation
from itertools import zip_longest
from pathlib import Path
from typing import Any
from zipfile import ZipFile

from python_calamine import load_workbook

FORBIDDEN_SHEETS = {'成本分析产品维度'}
DECIMAL_TOLERANCE = Decimal('0.000001')
MAX_ERRORS = 20


def compare_workbooks(expected_path: Path, actual_path: Path) -> dict[str, Any]:
    expected = load_workbook(expected_path)
    actual = load_workbook(actual_path)
    expected_meta = workbook_sheet_metadata(expected_path)
    actual_meta = workbook_sheet_metadata(actual_path)
    errors: list[str] = []
    if FORBIDDEN_SHEETS.intersection(actual.sheet_names):
        errors.append('actual workbook contains forbidden product dimension sheet')
    if expected.sheet_names != actual.sheet_names:
        errors.append(f'sheet names differ: expected={expected.sheet_names}, actual={actual.sheet_names}')

    for sheet_name in expected.sheet_names:
        if sheet_name not in actual.sheet_names:
            continue
        expected_ws = expected.get_sheet_by_name(sheet_name)
        actual_ws = actual.get_sheet_by_name(sheet_name)
        if expected_ws.height != actual_ws.height or expected_ws.width != actual_ws.width:
            errors.append(
                f'shape mismatch {sheet_name}: '
                f'expected={expected_ws.height}x{expected_ws.width}, '
                f'actual={actual_ws.height}x{actual_ws.width}'
            )
            continue
        expected_sheet_meta = expected_meta.get(sheet_name, {})
        actual_sheet_meta = actual_meta.get(sheet_name, {})
        if expected_sheet_meta.get('freeze_panes') != actual_sheet_meta.get('freeze_panes'):
            errors.append(
                f'freeze panes mismatch {sheet_name}: '
                f'expected={expected_sheet_meta.get("freeze_panes")}, '
                f'actual={actual_sheet_meta.get("freeze_panes")}'
            )
        if expected_sheet_meta.get('auto_filter') != actual_sheet_meta.get('auto_filter'):
            errors.append(
                f'auto filter mismatch {sheet_name}: '
                f'expected={expected_sheet_meta.get("auto_filter")}, '
                f'actual={actual_sheet_meta.get("auto_filter")}'
            )
        row_pairs = zip_longest(expected_ws.iter_rows(), actual_ws.iter_rows())
        for row_number, (expected_row, actual_row) in enumerate(row_pairs, start=1):
            if expected_row is None or actual_row is None:
                errors.append(f'row count mismatch {sheet_name} at row {row_number}')
                break
            expected_cells = pad_row(expected_row, expected_ws.width)
            actual_cells = pad_row(actual_row, actual_ws.width)
            for col_number, (expected_value, actual_value) in enumerate(
                zip(expected_cells, actual_cells, strict=True),
                start=1,
            ):
                if not values_equal(expected_value, actual_value):
                    errors.append(
                        f'value mismatch {sheet_name}!{row_number},{col_number}: '
                        f'expected={expected_value!r}, actual={actual_value!r}'
                    )
                    if len(errors) >= MAX_ERRORS:
                        return {'passed': False, 'errors': errors}
    return {'passed': not errors, 'errors': errors}


def pad_row(row: list[Any], width: int) -> tuple[Any, ...]:
    if len(row) >= width:
        return tuple(row)
    return (*row, *((None,) * (width - len(row))))


def workbook_sheet_metadata(path: Path) -> dict[str, dict[str, str | None]]:
    with ZipFile(path) as archive:
        workbook = ET.fromstring(archive.read('xl/workbook.xml'))  # noqa: S314 - local xlsx test artifact.
        rels = ET.fromstring(archive.read('xl/_rels/workbook.xml.rels'))  # noqa: S314 - local xlsx test artifact.
        rel_targets = {rel.attrib['Id']: rel.attrib['Target'].lstrip('/') for rel in rels}
        metadata: dict[str, dict[str, str | None]] = {}
        for sheet in workbook.findall('.//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}sheet'):
            sheet_name = sheet.attrib['name']
            rel_id = sheet.attrib['{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id']
            target = rel_targets[rel_id]
            worksheet_path = target if target.startswith('xl/') else f'xl/{target}'
            worksheet = ET.fromstring(archive.read(worksheet_path))  # noqa: S314 - local xlsx test artifact.
            pane = worksheet.find('.//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}pane')
            auto_filter = worksheet.find('{http://schemas.openxmlformats.org/spreadsheetml/2006/main}autoFilter')
            metadata[sheet_name] = {
                'freeze_panes': None if pane is None else pane.attrib.get('topLeftCell'),
                'auto_filter': None if auto_filter is None else auto_filter.attrib.get('ref'),
            }
        return metadata


def values_equal(expected: object, actual: object) -> bool:
    if is_blank(expected) and is_blank(actual):
        return True
    if is_number_like(expected) and is_number_like(actual):
        expected_decimal = as_decimal(expected)
        actual_decimal = as_decimal(actual)
        if expected_decimal is not None and actual_decimal is not None:
            return abs(expected_decimal - actual_decimal) <= DECIMAL_TOLERANCE
    return expected == actual


def is_blank(value: object) -> bool:
    return value is None or value == ''


def is_number_like(value: object) -> bool:
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def as_decimal(value: object) -> Decimal | None:
    if is_blank(value):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None

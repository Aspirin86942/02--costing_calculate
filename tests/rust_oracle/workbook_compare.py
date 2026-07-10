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
MAIN_NS = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
REL_NS = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'
BUILTIN_NUMBER_FORMATS = {
    0: 'General',
    1: '0',
    2: '0.00',
    3: '#,##0',
    4: '#,##0.00',
    9: '0%',
    10: '0.00%',
    49: '@',
}


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
        for metadata_key, label in (
            ('column_widths', 'column widths'),
            ('number_formats', 'number formats'),
            ('header_styles', 'header styles'),
            ('data_styles', 'data styles'),
            ('conditional_format_ranges', 'conditional format ranges'),
        ):
            expected_value = expected_sheet_meta.get(metadata_key)
            actual_value = actual_sheet_meta.get(metadata_key)
            if expected_value != actual_value:
                errors.append(f'{label} mismatch {sheet_name}: expected={expected_value!r}, actual={actual_value!r}')
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


def workbook_sheet_metadata(path: Path) -> dict[str, dict[str, Any]]:
    with ZipFile(path) as archive:
        workbook = ET.fromstring(archive.read('xl/workbook.xml'))  # noqa: S314 - local xlsx test artifact.
        rels = ET.fromstring(archive.read('xl/_rels/workbook.xml.rels'))  # noqa: S314 - local xlsx test artifact.
        rel_targets = {rel.attrib['Id']: rel.attrib['Target'].lstrip('/') for rel in rels}
        style_catalog = _read_style_catalog(archive)
        metadata: dict[str, dict[str, Any]] = {}
        for sheet in workbook.findall(f'.//{{{MAIN_NS}}}sheet'):
            sheet_name = sheet.attrib['name']
            rel_id = sheet.attrib[f'{{{REL_NS}}}id']
            target = rel_targets[rel_id]
            worksheet_path = target if target.startswith('xl/') else f'xl/{target}'
            worksheet = ET.fromstring(archive.read(worksheet_path))  # noqa: S314 - local xlsx test artifact.
            pane = worksheet.find(f'.//{{{MAIN_NS}}}pane')
            auto_filter = worksheet.find(f'{{{MAIN_NS}}}autoFilter')
            metadata[sheet_name] = {
                'freeze_panes': None if pane is None else pane.attrib.get('topLeftCell'),
                'auto_filter': None if auto_filter is None else auto_filter.attrib.get('ref'),
                'column_widths': _column_widths(worksheet),
                'number_formats': _number_formats(worksheet, style_catalog),
                'header_styles': _row_styles(worksheet, 1, style_catalog),
                'data_styles': _data_styles(worksheet, style_catalog),
                'conditional_format_ranges': sorted(
                    node.attrib.get('sqref', '') for node in worksheet.findall(f'{{{MAIN_NS}}}conditionalFormatting')
                ),
            }
        return metadata


def _read_style_catalog(archive: ZipFile) -> list[tuple[Any, ...]]:
    styles = ET.fromstring(archive.read('xl/styles.xml'))  # noqa: S314 - local xlsx test artifact.
    custom_number_formats = {
        int(node.attrib['numFmtId']): node.attrib['formatCode'] for node in styles.findall(f'.//{{{MAIN_NS}}}numFmt')
    }
    fonts = [_font_signature(node) for node in styles.findall(f'.//{{{MAIN_NS}}}fonts/{{{MAIN_NS}}}font')]
    fills = [_fill_signature(node) for node in styles.findall(f'.//{{{MAIN_NS}}}fills/{{{MAIN_NS}}}fill')]
    borders = [_border_signature(node) for node in styles.findall(f'.//{{{MAIN_NS}}}borders/{{{MAIN_NS}}}border')]
    catalog: list[tuple[Any, ...]] = []
    for cell_format in styles.findall(f'.//{{{MAIN_NS}}}cellXfs/{{{MAIN_NS}}}xf'):
        number_format_id = int(cell_format.attrib.get('numFmtId', '0'))
        alignment = cell_format.find(f'{{{MAIN_NS}}}alignment')
        catalog.append(
            (
                custom_number_formats.get(
                    number_format_id, BUILTIN_NUMBER_FORMATS.get(number_format_id, str(number_format_id))
                ),
                fonts[int(cell_format.attrib.get('fontId', '0'))],
                fills[int(cell_format.attrib.get('fillId', '0'))],
                borders[int(cell_format.attrib.get('borderId', '0'))],
                None if alignment is None else alignment.attrib.get('horizontal'),
                None if alignment is None else alignment.attrib.get('vertical'),
            )
        )
    return catalog


def _font_signature(font: ET.Element) -> tuple[bool, tuple[tuple[str, str], ...]]:
    bold = font.find(f'{{{MAIN_NS}}}b') is not None
    return bold, _color_signature(font.find(f'{{{MAIN_NS}}}color'))


def _fill_signature(fill: ET.Element) -> tuple[Any, ...]:
    pattern = fill.find(f'{{{MAIN_NS}}}patternFill')
    if pattern is None:
        return (None, (), ())
    return (
        pattern.attrib.get('patternType'),
        _color_signature(pattern.find(f'{{{MAIN_NS}}}fgColor')),
        _color_signature(pattern.find(f'{{{MAIN_NS}}}bgColor')),
    )


def _border_signature(border: ET.Element) -> tuple[str | None, ...]:
    return tuple(
        (
            border.find(f'{{{MAIN_NS}}}{side}').attrib.get('style')
            if border.find(f'{{{MAIN_NS}}}{side}') is not None
            else None
        )
        for side in ('left', 'right', 'top', 'bottom')
    )


def _color_signature(color: ET.Element | None) -> tuple[tuple[str, str], ...]:
    if color is None:
        return ()
    values = dict(color.attrib)
    if 'rgb' in values:
        values['rgb'] = values['rgb'][-6:].upper()
    return tuple(sorted(values.items()))


def _column_widths(worksheet: ET.Element) -> dict[int, float]:
    widths: dict[int, float] = {}
    for column in worksheet.findall(f'.//{{{MAIN_NS}}}cols/{{{MAIN_NS}}}col'):
        width = round(float(column.attrib['width']), 4)
        for column_index in range(int(column.attrib['min']), int(column.attrib['max']) + 1):
            widths[column_index] = width
    return widths


def _column_styles(worksheet: ET.Element) -> dict[int, int]:
    styles: dict[int, int] = {}
    for column in worksheet.findall(f'.//{{{MAIN_NS}}}cols/{{{MAIN_NS}}}col'):
        style_id = int(column.attrib.get('style', '0'))
        for column_index in range(int(column.attrib['min']), int(column.attrib['max']) + 1):
            styles[column_index] = style_id
    return styles


def _number_formats(worksheet: ET.Element, style_catalog: list[tuple[Any, ...]]) -> dict[int, str]:
    formats = {
        column_index: style_catalog[style_id][0]
        for column_index, style_id in _column_styles(worksheet).items()
        if style_catalog[style_id][0] != 'General'
    }
    row = worksheet.find(f'.//{{{MAIN_NS}}}row[@r="2"]')
    if row is None:
        return formats
    for cell in row.findall(f'{{{MAIN_NS}}}c'):
        column_index = _column_index(cell.attrib['r'])
        style_id = int(cell.attrib.get('s', _column_styles(worksheet).get(column_index, 0)))
        number_format = style_catalog[style_id][0]
        if number_format != 'General':
            formats[column_index] = number_format
    return formats


def _row_styles(
    worksheet: ET.Element,
    row_number: int,
    style_catalog: list[tuple[Any, ...]],
) -> dict[int, tuple[Any, ...]]:
    row = worksheet.find(f'.//{{{MAIN_NS}}}row[@r="{row_number}"]')
    if row is None:
        return {}
    column_styles = _column_styles(worksheet)
    return {
        _column_index(cell.attrib['r']): style_catalog[
            int(cell.attrib.get('s', column_styles.get(_column_index(cell.attrib['r']), 0)))
        ]
        for cell in row.findall(f'{{{MAIN_NS}}}c')
    }


def _data_styles(
    worksheet: ET.Element,
    style_catalog: list[tuple[Any, ...]],
) -> dict[int, tuple[tuple[Any, ...], ...]]:
    column_styles = _column_styles(worksheet)
    data_rows = [
        row
        for row in worksheet.findall(f'.//{{{MAIN_NS}}}sheetData/{{{MAIN_NS}}}row')
        if int(row.attrib.get('r', '0')) > 1
    ]
    if not data_rows:
        return {}

    columns = set(_row_styles(worksheet, 1, style_catalog)) | set(column_styles)
    explicit_cell_counts: dict[int, int] = {}
    styles: dict[int, set[tuple[Any, ...]]] = {}
    for row in data_rows:
        for cell in row.findall(f'{{{MAIN_NS}}}c'):
            column_index = _column_index(cell.attrib['r'])
            columns.add(column_index)
            explicit_cell_counts[column_index] = explicit_cell_counts.get(column_index, 0) + 1
            style_id = int(cell.attrib.get('s', column_styles.get(column_index, 0)))
            styles.setdefault(column_index, set()).add(style_catalog[style_id])

    # 缺失的空白单元格继承列格式；按有效样式比较，避免把 OOXML 存储方式差异误判为业务差异。
    for column_index in columns:
        if explicit_cell_counts.get(column_index, 0) < len(data_rows):
            styles.setdefault(column_index, set()).add(style_catalog[column_styles.get(column_index, 0)])

    return {column_index: tuple(sorted(styles.get(column_index, set()), key=repr)) for column_index in sorted(columns)}


def _column_index(cell_reference: str) -> int:
    result = 0
    for character in cell_reference:
        if not character.isalpha():
            break
        result = result * 26 + (ord(character.upper()) - ord('A') + 1)
    return result


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

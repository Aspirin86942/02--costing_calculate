from __future__ import annotations

import xml.etree.ElementTree as ET
from collections.abc import Iterator
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from typing import Any, Literal, cast
from zipfile import ZipFile

from tests.rust_oracle.benchmark_protocol import PipelineName

StorageType = Literal['blank', 'n', 's', 'inlineStr', 'str', 'b', 'e', 'd']

FORBIDDEN_SHEETS = {'成本分析产品维度'}
MAX_MISMATCHES = 20
MAIN_NS = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
OFFICE_REL_NS = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'
PACKAGE_REL_NS = 'http://schemas.openxmlformats.org/package/2006/relationships'
CONTENT_TYPE_NS = 'http://schemas.openxmlformats.org/package/2006/content-types'
SHARED_STRINGS_REL_TYPE = f'{OFFICE_REL_NS}/sharedStrings'
SHARED_STRINGS_CONTENT_TYPE = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml'

GROUP_KEYS = {
    '成本计算单数量聚合维度': ('月份', '产品编码', '工单编号', '工单行号'),
    '成本分析工单维度': ('月份', '产品编码', '工单编号', '工单行'),
}

_DETAIL_NUMERIC_COLUMNS = ('本期完工单位成本', '本期完工金额')
_QTY_NUMERIC_COLUMNS = (
    '本期完工数量',
    '本期完工金额',
    '本期完工直接材料合计完工金额',
    '本期完工直接人工合计完工金额',
    '本期完工制造费用合计完工金额',
    '本期完工制造费用_其他合计完工金额',
    '本期完工制造费用_人工合计完工金额',
    '本期完工制造费用_机物料及低耗合计完工金额',
    '本期完工制造费用_折旧合计完工金额',
    '本期完工制造费用_水电费合计完工金额',
    '本期完工委外加工费合计完工金额',
    '直接材料单位完工金额',
    '直接人工单位完工金额',
    '制造费用单位完工金额',
    '制造费用_其他单位完工成本',
    '制造费用_人工单位完工成本',
    '制造费用_机物料及低耗单位完工成本',
    '制造费用_折旧单位完工成本',
    '制造费用_水电费单位完工成本',
    '委外加工费单位完工成本',
)
_WORK_ORDER_NUMERIC_COLUMNS = (
    '本期完工数量',
    '总完工成本',
    '直接材料合计完工金额',
    '直接人工合计完工金额',
    '制造费用合计完工金额',
    '制造费用_其他合计完工金额',
    '制造费用_人工合计完工金额',
    '制造费用_机物料及低耗合计完工金额',
    '制造费用_折旧合计完工金额',
    '制造费用_水电费合计完工金额',
    '委外加工费合计完工金额',
    '总单位完工成本',
    '直接材料单位完工成本',
    '直接人工单位完工成本',
    '制造费用单位完工成本',
    '制造费用_其他单位完工成本',
    '制造费用_人工单位完工成本',
    '制造费用_机物料及低耗单位完工成本',
    '制造费用_折旧单位完工成本',
    '制造费用_水电费单位完工成本',
    '委外加工费单位完工成本',
)
_SK_QTY_ADDITIONS = ('本期完工软件费用合计完工金额', '软件费用单位完工成本')
_SK_WORK_ORDER_ADDITIONS = ('软件费用合计完工金额', '软件费用单位完工成本')

NUMERIC_COLUMNS: dict[tuple[PipelineName, str], tuple[str, ...]] = {
    ('gb', '成本计算单总表'): _DETAIL_NUMERIC_COLUMNS,
    ('sk', '成本计算单总表'): _DETAIL_NUMERIC_COLUMNS,
    ('gb', '成本计算单数量聚合维度'): _QTY_NUMERIC_COLUMNS,
    ('sk', '成本计算单数量聚合维度'): (*_QTY_NUMERIC_COLUMNS, *_SK_QTY_ADDITIONS),
    ('gb', '成本分析工单维度'): _WORK_ORDER_NUMERIC_COLUMNS,
    ('sk', '成本分析工单维度'): (*_WORK_ORDER_NUMERIC_COLUMNS, *_SK_WORK_ORDER_ADDITIONS),
}
_KNOWN_NUMERIC_HEADERS_BY_SHEET = {
    sheet: frozenset(
        header
        for (candidate_pipeline, candidate_sheet), columns in NUMERIC_COLUMNS.items()
        if candidate_sheet == sheet
        for header in columns
    )
    for sheet in {sheet for _, sheet in NUMERIC_COLUMNS}
}

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


@dataclass(frozen=True)
class WorkbookMismatch:
    sheet: str
    coordinate: str | None
    mismatch_kind: str
    expected_storage_type: StorageType | None = None
    actual_storage_type: StorageType | None = None


@dataclass(frozen=True)
class WorkbookComparisonReport:
    passed: bool
    mismatches: tuple[WorkbookMismatch, ...]


@dataclass(frozen=True)
class XmlCell:
    coordinate: str
    storage_type: StorageType
    lexical_value: str | None
    resolved_text: str | None
    style_id: int


@dataclass(frozen=True)
class _SheetPackage:
    name: str
    path: str
    dimension: str | None
    freeze_panes: str | None
    auto_filter: str | None
    column_widths: tuple[tuple[int, Decimal], ...]
    column_styles: tuple[tuple[int, int], ...]
    column_style_signatures: tuple[tuple[int, tuple[Any, ...]], ...]
    number_formats: tuple[tuple[int, str], ...]
    conditional_format_ranges: tuple[str, ...]


class _Package:
    def __init__(self, path: Path) -> None:
        self.archive = ZipFile(path)
        self.style_catalog = _read_style_catalog(self.archive)
        self.package_mismatches: list[WorkbookMismatch] = []
        self.shared_strings_relationship_present = False
        self.shared_strings_part_present = 'xl/sharedStrings.xml' in self.archive.namelist()
        content_types = _xml_root(self.archive, '[Content_Types].xml')
        self.shared_strings_content_type_present = any(
            node.attrib.get('PartName') == '/xl/sharedStrings.xml'
            and node.attrib.get('ContentType') == SHARED_STRINGS_CONTENT_TYPE
            for node in content_types.findall(f'{{{CONTENT_TYPE_NS}}}Override')
        )
        self.shared_strings = self._read_shared_strings()
        self.sheets = self._read_sheets()

    def close(self) -> None:
        self.archive.close()

    def _read_shared_strings(self) -> tuple[str, ...]:
        rels = _xml_root(self.archive, 'xl/_rels/workbook.xml.rels')
        relationships = [
            relationship
            for relationship in rels.findall(f'{{{PACKAGE_REL_NS}}}Relationship')
            if relationship.attrib.get('Type') == SHARED_STRINGS_REL_TYPE
            or relationship.attrib.get('Target', '').endswith('sharedStrings.xml')
        ]
        if not relationships:
            if self.shared_strings_part_present or self.shared_strings_content_type_present:
                self._package_mismatch('shared_strings_relationship_missing')
            return ()
        self.shared_strings_relationship_present = True
        if len(relationships) != 1:
            self._package_mismatch('shared_strings_relationship_count_mismatch')
        relationship = relationships[0]
        if relationship.attrib.get('Type') != SHARED_STRINGS_REL_TYPE:
            self._package_mismatch('shared_strings_relationship_type_mismatch')
        if relationship.attrib.get('Target') != 'sharedStrings.xml':
            self._package_mismatch('shared_strings_relationship_target_mismatch')

        if not self.shared_strings_content_type_present:
            self._package_mismatch('shared_strings_content_type_missing')
        if not self.shared_strings_part_present:
            self._package_mismatch('shared_strings_part_missing')
            return ()

        root = _xml_root(self.archive, 'xl/sharedStrings.xml')
        return tuple(''.join(text.text or '' for text in item.iter(f'{{{MAIN_NS}}}t')) for item in root)

    def _read_sheets(self) -> tuple[_SheetPackage, ...]:
        workbook = _xml_root(self.archive, 'xl/workbook.xml')
        rels = _xml_root(self.archive, 'xl/_rels/workbook.xml.rels')
        rel_targets = {relationship.attrib['Id']: relationship.attrib['Target'] for relationship in rels}
        sheets: list[_SheetPackage] = []
        for node in workbook.findall(f'.//{{{MAIN_NS}}}sheet'):
            name = node.attrib['name']
            target = rel_targets[node.attrib[f'{{{OFFICE_REL_NS}}}id']]
            path = _resolve_workbook_target(target)
            worksheet = _xml_root(self.archive, path)
            pane = worksheet.find(f'.//{{{MAIN_NS}}}pane')
            auto_filter = worksheet.find(f'{{{MAIN_NS}}}autoFilter')
            dimension = worksheet.find(f'{{{MAIN_NS}}}dimension')
            column_styles = _column_styles(worksheet)
            sheets.append(
                _SheetPackage(
                    name=name,
                    path=path,
                    dimension=None if dimension is None else dimension.attrib.get('ref'),
                    freeze_panes=None if pane is None else pane.attrib.get('topLeftCell'),
                    auto_filter=None if auto_filter is None else auto_filter.attrib.get('ref'),
                    column_widths=tuple(sorted(_column_widths(worksheet).items())),
                    column_styles=tuple(sorted(column_styles.items())),
                    column_style_signatures=tuple(
                        sorted((column, self.style_catalog[style_id]) for column, style_id in column_styles.items())
                    ),
                    number_formats=tuple(sorted(_number_formats(worksheet, self.style_catalog, column_styles).items())),
                    conditional_format_ranges=tuple(
                        sorted(
                            item.attrib.get('sqref', '')
                            for item in worksheet.findall(f'{{{MAIN_NS}}}conditionalFormatting')
                        )
                    ),
                )
            )
        return tuple(sheets)

    def _package_mismatch(self, kind: str) -> None:
        self.package_mismatches.append(WorkbookMismatch('<workbook>', None, kind))


def compare_workbooks(
    expected_path: Path,
    actual_path: Path,
    *,
    pipeline: PipelineName,
) -> WorkbookComparisonReport:
    expected = _Package(expected_path)
    actual = _Package(actual_path)
    mismatches: list[WorkbookMismatch] = []
    try:
        _extend(mismatches, expected.package_mismatches)
        _extend(mismatches, actual.package_mismatches)
        expected_names = tuple(sheet.name for sheet in expected.sheets)
        actual_names = tuple(sheet.name for sheet in actual.sheets)
        if FORBIDDEN_SHEETS.intersection(actual_names):
            _append(mismatches, WorkbookMismatch('<workbook>', None, 'forbidden_sheet'))
        if expected_names != actual_names:
            _append(mismatches, WorkbookMismatch('<workbook>', None, 'sheet_order_mismatch'))
        for attribute, kind in (
            ('shared_strings_relationship_present', 'shared_strings_relationship_presence_mismatch'),
            ('shared_strings_content_type_present', 'shared_strings_content_type_presence_mismatch'),
            ('shared_strings_part_present', 'shared_strings_part_presence_mismatch'),
        ):
            if getattr(expected, attribute) != getattr(actual, attribute):
                _append(mismatches, WorkbookMismatch('<workbook>', None, kind))

        for expected_sheet, actual_sheet in zip(expected.sheets, actual.sheets, strict=False):
            if expected_sheet.name != actual_sheet.name:
                continue
            _compare_sheet_package(expected_sheet, actual_sheet, mismatches)
            _compare_sheet_cells(expected, actual, expected_sheet, actual_sheet, mismatches)
            _compare_business_reconciliations(expected, actual, expected_sheet, actual_sheet, pipeline, mismatches)

        if expected.shared_strings != actual.shared_strings:
            _append(mismatches, WorkbookMismatch('<workbook>', None, 'shared_strings_content_mismatch'))
    finally:
        expected.close()
        actual.close()
    result = tuple(mismatches[:MAX_MISMATCHES])
    return WorkbookComparisonReport(passed=not result, mismatches=result)


def _compare_sheet_package(
    expected: _SheetPackage,
    actual: _SheetPackage,
    mismatches: list[WorkbookMismatch],
) -> None:
    for attribute, kind in (
        ('dimension', 'shape_mismatch'),
        ('freeze_panes', 'freeze_panes_mismatch'),
        ('auto_filter', 'auto_filter_mismatch'),
        ('column_widths', 'column_width_mismatch'),
        ('column_style_signatures', 'column_style_mismatch'),
        ('number_formats', 'number_format_mismatch'),
        ('conditional_format_ranges', 'conditional_format_mismatch'),
    ):
        if getattr(expected, attribute) != getattr(actual, attribute):
            _append(mismatches, WorkbookMismatch(expected.name, None, kind))


def _compare_sheet_cells(
    expected_package: _Package,
    actual_package: _Package,
    expected_sheet: _SheetPackage,
    actual_sheet: _SheetPackage,
    mismatches: list[WorkbookMismatch],
) -> None:
    expected_stream = _iter_xml_cells(expected_package, expected_sheet, mismatches)
    actual_stream = _iter_xml_cells(actual_package, actual_sheet, mismatches)
    expected_cell = next(expected_stream, None)
    actual_cell = next(actual_stream, None)
    while expected_cell is not None or actual_cell is not None:
        if len(mismatches) >= MAX_MISMATCHES:
            return
        if actual_cell is None or (
            expected_cell is not None
            and _coordinate_key(expected_cell.coordinate) < _coordinate_key(actual_cell.coordinate)
        ):
            implicit = _implicit_blank(actual_sheet, expected_cell.coordinate)
            _compare_cell(expected_package, actual_package, expected_sheet.name, expected_cell, implicit, mismatches)
            expected_cell = next(expected_stream, None)
        elif expected_cell is None or _coordinate_key(actual_cell.coordinate) < _coordinate_key(
            expected_cell.coordinate
        ):
            implicit = _implicit_blank(expected_sheet, actual_cell.coordinate)
            _compare_cell(expected_package, actual_package, expected_sheet.name, implicit, actual_cell, mismatches)
            actual_cell = next(actual_stream, None)
        else:
            _compare_cell(expected_package, actual_package, expected_sheet.name, expected_cell, actual_cell, mismatches)
            expected_cell = next(expected_stream, None)
            actual_cell = next(actual_stream, None)


def _compare_cell(
    expected_package: _Package,
    actual_package: _Package,
    sheet: str,
    expected: XmlCell,
    actual: XmlCell,
    mismatches: list[WorkbookMismatch],
) -> None:
    if expected.storage_type != actual.storage_type:
        _append(
            mismatches,
            WorkbookMismatch(
                sheet,
                expected.coordinate,
                'storage_type_mismatch',
                expected.storage_type,
                actual.storage_type,
            ),
        )
    elif not _cell_values_equal(expected, actual):
        _append(
            mismatches,
            WorkbookMismatch(sheet, expected.coordinate, 'value_mismatch', expected.storage_type, actual.storage_type),
        )
    expected_style = expected_package.style_catalog[expected.style_id]
    actual_style = actual_package.style_catalog[actual.style_id]
    if expected_style != actual_style:
        kind = 'header_style_mismatch' if _coordinate_key(expected.coordinate)[0] == 1 else 'cell_style_mismatch'
        _append(mismatches, WorkbookMismatch(sheet, expected.coordinate, kind))


def _cell_values_equal(expected: XmlCell, actual: XmlCell) -> bool:
    if expected.storage_type == 'blank':
        return True
    if expected.storage_type == 'n':
        expected_decimal = _parse_decimal(expected.lexical_value)
        actual_decimal = _parse_decimal(actual.lexical_value)
        return expected_decimal is not None and actual_decimal is not None and expected_decimal == actual_decimal
    if expected.storage_type in ('s', 'inlineStr'):
        return expected.resolved_text == actual.resolved_text
    return expected.lexical_value == actual.lexical_value


def _iter_xml_cells(
    package: _Package,
    sheet: _SheetPackage,
    mismatches: list[WorkbookMismatch],
) -> Iterator[XmlCell]:
    column_styles = dict(sheet.column_styles)
    with package.archive.open(sheet.path) as stream:
        for _, node in ET.iterparse(stream, events=('end',)):  # noqa: S314 - controlled local xlsx artifact.
            if node.tag != f'{{{MAIN_NS}}}c':
                if node.tag == f'{{{MAIN_NS}}}row':
                    node.clear()
                continue
            coordinate = node.attrib['r']
            storage_type = _storage_type(node)
            lexical_value = _cell_lexical_value(node, storage_type)
            resolved_text: str | None = None
            if storage_type == 's':
                index = _shared_string_index(lexical_value)
                if index is None or index >= len(package.shared_strings):
                    _append(
                        mismatches,
                        WorkbookMismatch(sheet.name, coordinate, 'shared_string_index_out_of_range', 's', 's'),
                    )
                else:
                    resolved_text = package.shared_strings[index]
            elif storage_type == 'inlineStr':
                resolved_text = ''.join(text.text or '' for text in node.iter(f'{{{MAIN_NS}}}t'))
            yield XmlCell(
                coordinate=coordinate,
                storage_type=storage_type,
                lexical_value=lexical_value,
                resolved_text=resolved_text,
                style_id=int(node.attrib.get('s', column_styles.get(_column_index(coordinate), 0))),
            )
            node.clear()


def _storage_type(cell: ET.Element) -> StorageType:
    raw_type = cell.attrib.get('t')
    if raw_type is None or raw_type == 'n':
        return 'n' if cell.find(f'{{{MAIN_NS}}}v') is not None else 'blank'
    if raw_type in ('s', 'inlineStr', 'str', 'b', 'e', 'd'):
        return cast(StorageType, raw_type)
    return 'blank'


def _cell_lexical_value(cell: ET.Element, storage_type: StorageType) -> str | None:
    if storage_type in ('blank', 'inlineStr'):
        return None
    value = cell.find(f'{{{MAIN_NS}}}v')
    return None if value is None else value.text


def _implicit_blank(sheet: _SheetPackage, coordinate: str) -> XmlCell:
    return XmlCell(
        coordinate=coordinate,
        storage_type='blank',
        lexical_value=None,
        resolved_text=None,
        style_id=dict(sheet.column_styles).get(_column_index(coordinate), 0),
    )


def _compare_business_reconciliations(
    expected_package: _Package,
    actual_package: _Package,
    expected_sheet: _SheetPackage,
    actual_sheet: _SheetPackage,
    pipeline: PipelineName,
    mismatches: list[WorkbookMismatch],
) -> None:
    policy = NUMERIC_COLUMNS.get((pipeline, expected_sheet.name))
    if policy is None:
        return
    expected_headers = _read_header_map(expected_package, expected_sheet, mismatches)
    actual_headers = _read_header_map(actual_package, actual_sheet, mismatches)
    required = (*GROUP_KEYS.get(expected_sheet.name, ()), *policy)
    if any(header not in expected_headers or header not in actual_headers for header in required):
        _append(mismatches, WorkbookMismatch(expected_sheet.name, None, 'required_header_missing'))
        return
    allowed = set(policy)
    known_for_sheet = _KNOWN_NUMERIC_HEADERS_BY_SHEET[expected_sheet.name]
    if any(header in known_for_sheet and header not in allowed for header in (*expected_headers, *actual_headers)):
        _append(mismatches, WorkbookMismatch(expected_sheet.name, None, 'unexpected_numeric_header'))
        return

    keys = GROUP_KEYS.get(expected_sheet.name, ())
    expected_totals, expected_groups = _stream_totals(
        expected_package,
        expected_sheet,
        expected_headers,
        keys,
        policy,
        mismatches,
    )
    actual_totals, actual_groups = _stream_totals(
        actual_package,
        actual_sheet,
        actual_headers,
        keys,
        policy,
        mismatches,
    )
    for header in policy:
        if expected_totals[header] != actual_totals[header]:
            _append(
                mismatches,
                WorkbookMismatch(
                    expected_sheet.name,
                    _header_coordinate(expected_headers[header]),
                    'column_total_mismatch',
                ),
            )

    if keys and expected_groups != actual_groups:
        _append(mismatches, WorkbookMismatch(expected_sheet.name, None, 'group_total_mismatch'))


def _read_header_map(
    package: _Package,
    sheet: _SheetPackage,
    mismatches: list[WorkbookMismatch],
) -> dict[str, int]:
    headers: dict[str, int] = {}
    stream = _iter_xml_cells(package, sheet, mismatches)
    try:
        for cell in stream:
            row, column = _coordinate_key(cell.coordinate)
            if row > 1:
                break
            header = _text_value(cell)
            if header is None:
                continue
            if header in headers:
                _append(mismatches, WorkbookMismatch(sheet.name, cell.coordinate, 'duplicate_header'))
            else:
                headers[header] = column
    finally:
        stream.close()
    return headers


def _stream_totals(
    package: _Package,
    sheet: _SheetPackage,
    headers: dict[str, int],
    keys: tuple[str, ...],
    policy: tuple[str, ...],
    mismatches: list[WorkbookMismatch],
) -> tuple[dict[str, Decimal], dict[tuple[str, ...], tuple[Decimal, ...]]]:
    totals = {header: Decimal(0) for header in policy}
    grouped: dict[tuple[str, ...], list[Decimal]] = {}
    current_row_number: int | None = None
    current_row: dict[int, XmlCell] = {}

    def flush_row() -> None:
        if current_row_number is None:
            return
        row = current_row
        key_values = tuple(_text_value(row.get(headers[key])) or '' for key in keys)
        group_totals = grouped.setdefault(key_values, [Decimal(0) for _ in policy]) if keys else None
        for index, header in enumerate(policy):
            value = _numeric_value(row.get(headers[header]))
            if value is None:
                _append(
                    mismatches,
                    WorkbookMismatch(
                        sheet.name,
                        _coordinate(headers[header], current_row_number),
                        'numeric_storage_invalid',
                    ),
                )
            else:
                totals[header] += value
                if group_totals is not None:
                    group_totals[index] += value

    for cell in _iter_xml_cells(package, sheet, mismatches):
        row_number, column = _coordinate_key(cell.coordinate)
        if row_number == 1:
            continue
        if current_row_number is not None and row_number != current_row_number:
            flush_row()
            current_row = {}
        current_row_number = row_number
        current_row[column] = cell
    flush_row()
    return totals, {key: tuple(values) for key, values in grouped.items()}


def _text_value(cell: XmlCell | None) -> str | None:
    if cell is None or cell.storage_type == 'blank':
        return None
    if cell.storage_type in ('s', 'inlineStr'):
        return cell.resolved_text
    return cell.lexical_value


def _numeric_value(cell: XmlCell | None) -> Decimal | None:
    if cell is None or cell.storage_type == 'blank':
        return Decimal(0)
    if cell.storage_type != 'n':
        return None
    return _parse_decimal(cell.lexical_value)


def _read_style_catalog(archive: ZipFile) -> list[tuple[Any, ...]]:
    styles = _xml_root(archive, 'xl/styles.xml')
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
    return font.find(f'{{{MAIN_NS}}}b') is not None, _color_signature(font.find(f'{{{MAIN_NS}}}color'))


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
        border.find(f'{{{MAIN_NS}}}{side}').attrib.get('style')
        if border.find(f'{{{MAIN_NS}}}{side}') is not None
        else None
        for side in ('left', 'right', 'top', 'bottom')
    )


def _color_signature(color: ET.Element | None) -> tuple[tuple[str, str], ...]:
    if color is None:
        return ()
    values = dict(color.attrib)
    if 'rgb' in values:
        values['rgb'] = values['rgb'][-6:].upper()
    return tuple(sorted(values.items()))


def _column_widths(worksheet: ET.Element) -> dict[int, Decimal]:
    widths: dict[int, Decimal] = {}
    for column in worksheet.findall(f'.//{{{MAIN_NS}}}cols/{{{MAIN_NS}}}col'):
        width = Decimal(column.attrib['width']).quantize(Decimal('0.0001'))
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


def _number_formats(
    worksheet: ET.Element,
    style_catalog: list[tuple[Any, ...]],
    column_styles: dict[int, int],
) -> dict[int, str]:
    formats = {
        column_index: style_catalog[style_id][0]
        for column_index, style_id in column_styles.items()
        if style_catalog[style_id][0] != 'General'
    }
    row = worksheet.find(f'.//{{{MAIN_NS}}}row[@r="2"]')
    if row is None:
        return formats
    for cell in row.findall(f'{{{MAIN_NS}}}c'):
        column_index = _column_index(cell.attrib['r'])
        style_id = int(cell.attrib.get('s', column_styles.get(column_index, 0)))
        number_format = style_catalog[style_id][0]
        if number_format != 'General':
            formats[column_index] = number_format
    return formats


def _xml_root(archive: ZipFile, member: str) -> ET.Element:
    return ET.fromstring(archive.read(member))  # noqa: S314 - controlled local xlsx artifact.


def _resolve_workbook_target(target: str) -> str:
    if target.startswith('/'):
        return target.lstrip('/')
    return str(PurePosixPath('xl') / target)


def _parse_decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        return None
    if not parsed.is_finite():
        return None
    return Decimal(0) if parsed.is_zero() else parsed.normalize()


def _shared_string_index(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        index = int(value)
    except ValueError:
        return None
    return index if index >= 0 else None


def _coordinate_key(reference: str) -> tuple[int, int]:
    letters = ''.join(character for character in reference if character.isalpha())
    digits = ''.join(character for character in reference if character.isdigit())
    return int(digits), _column_index(letters)


def _column_index(cell_reference: str) -> int:
    result = 0
    for character in cell_reference:
        if not character.isalpha():
            break
        result = result * 26 + (ord(character.upper()) - ord('A') + 1)
    return result


def _coordinate(column: int, row: int) -> str:
    letters = ''
    remaining = column
    while remaining:
        remaining, remainder = divmod(remaining - 1, 26)
        letters = chr(ord('A') + remainder) + letters
    return f'{letters}{row}'


def _header_coordinate(column: int) -> str:
    return _coordinate(column, 1)


def _append(mismatches: list[WorkbookMismatch], mismatch: WorkbookMismatch) -> None:
    if len(mismatches) < MAX_MISMATCHES and mismatch not in mismatches:
        mismatches.append(mismatch)


def _extend(mismatches: list[WorkbookMismatch], items: list[WorkbookMismatch]) -> None:
    for item in items:
        _append(mismatches, item)

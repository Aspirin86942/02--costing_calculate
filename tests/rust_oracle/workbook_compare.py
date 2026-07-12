from __future__ import annotations

import xml.etree.ElementTree as ET
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from typing import Any, Literal, cast
from zipfile import ZipFile

from tests.rust_oracle.benchmark_protocol import PipelineName

StorageType = Literal['blank', 'n', 's', 'inlineStr', 'str', 'b', 'e', 'd']
TEXT_STORAGE_TYPES = frozenset(('s', 'inlineStr'))

FORBIDDEN_SHEETS = {'成本分析产品维度'}
MAX_MISMATCHES = 20
# Python/XlsxWriter may preserve IEEE-754 tails that Rust serializes as the same shorter decimal.
NUMERIC_ABSOLUTE_TOLERANCE = Decimal('1e-9')
# Whole-column sums accumulate those harmless per-cell tails across large SK sheets.
COLUMN_TOTAL_ABSOLUTE_TOLERANCE = Decimal('1e-8')
MAIN_NS = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
OFFICE_REL_NS = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'
PACKAGE_REL_NS = 'http://schemas.openxmlformats.org/package/2006/relationships'
CONTENT_TYPE_NS = 'http://schemas.openxmlformats.org/package/2006/content-types'
SHARED_STRINGS_REL_TYPE = f'{OFFICE_REL_NS}/sharedStrings'
WORKSHEET_REL_TYPE = f'{OFFICE_REL_NS}/worksheet'
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
    path: str | None
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
        self.zip_member_counts = Counter(self.archive.namelist())
        relationships_root = _xml_root(self.archive, 'xl/_rels/workbook.xml.rels')
        self.relationships = tuple(relationships_root.findall(f'{{{PACKAGE_REL_NS}}}Relationship'))
        content_types = _xml_root(self.archive, '[Content_Types].xml')
        self.content_type_overrides = tuple(content_types.findall(f'{{{CONTENT_TYPE_NS}}}Override'))
        self.shared_strings_relationship_present = False
        self.shared_strings_part_present = self.zip_member_counts['xl/sharedStrings.xml'] > 0
        self.shared_strings_content_type_present = any(
            node.attrib.get('PartName') == '/xl/sharedStrings.xml' for node in self.content_type_overrides
        )
        self.shared_strings = self._read_shared_strings()
        self.sheets = self._read_sheets()

    def close(self) -> None:
        self.archive.close()

    def _read_shared_strings(self) -> tuple[str, ...]:
        relationships = [
            relationship
            for relationship in self.relationships
            if relationship.attrib.get('Type') == SHARED_STRINGS_REL_TYPE
            or PurePosixPath(relationship.attrib.get('Target', '')).name == 'sharedStrings.xml'
        ]
        if not relationships:
            if self.shared_strings_part_present or self.shared_strings_content_type_present:
                self._package_mismatch('shared_strings_relationship_missing')
            return ()
        self.shared_strings_relationship_present = True
        if len(relationships) != 1:
            self._package_mismatch('shared_strings_relationship_count_mismatch')
        relationship = relationships[0]
        relationship_id = relationship.attrib.get('Id')
        if relationship_id is None or sum(item.attrib.get('Id') == relationship_id for item in self.relationships) != 1:
            self._package_mismatch('shared_strings_relationship_id_not_unique')
        if relationship.attrib.get('Type') != SHARED_STRINGS_REL_TYPE:
            self._package_mismatch('shared_strings_relationship_type_mismatch')
        if relationship.attrib.get('TargetMode', '').lower() == 'external':
            self._package_mismatch('shared_strings_relationship_external')
            return ()
        normalized_target = _normalize_relationship_target(relationship.attrib.get('Target'))
        if normalized_target is None:
            self._package_mismatch('shared_strings_relationship_target_unsafe')
            return ()
        if normalized_target != 'xl/sharedStrings.xml':
            self._package_mismatch('shared_strings_relationship_target_mismatch')

        overrides = [
            node for node in self.content_type_overrides if node.attrib.get('PartName') == '/xl/sharedStrings.xml'
        ]
        if not overrides:
            self._package_mismatch('shared_strings_content_type_missing')
        elif len(overrides) != 1:
            self._package_mismatch('shared_strings_content_type_not_unique')
        elif overrides[0].attrib.get('ContentType') != SHARED_STRINGS_CONTENT_TYPE:
            self._package_mismatch('shared_strings_content_type_mismatch')

        part_count = self.zip_member_counts['xl/sharedStrings.xml']
        if part_count == 0:
            self._package_mismatch('shared_strings_part_missing')
            return ()
        if part_count != 1:
            self._package_mismatch('shared_strings_part_not_unique')
            return ()

        root = _xml_root(self.archive, 'xl/sharedStrings.xml')
        return tuple(''.join(text.text or '' for text in item.iter(f'{{{MAIN_NS}}}t')) for item in root)

    def _read_sheets(self) -> tuple[_SheetPackage, ...]:
        workbook = _xml_root(self.archive, 'xl/workbook.xml')
        sheet_nodes = tuple(workbook.findall(f'.//{{{MAIN_NS}}}sheet'))
        sheet_relationship_ids = [node.attrib.get(f'{{{OFFICE_REL_NS}}}id') for node in sheet_nodes]
        duplicate_sheet_ids = {
            relationship_id
            for relationship_id, count in Counter(sheet_relationship_ids).items()
            if relationship_id is not None and count != 1
        }
        if duplicate_sheet_ids:
            self._package_mismatch('worksheet_sheet_relationship_id_not_unique')
        sheets: list[_SheetPackage] = []
        for node in sheet_nodes:
            name = node.attrib['name']
            relationship_id = node.attrib.get(f'{{{OFFICE_REL_NS}}}id')
            matches = [item for item in self.relationships if item.attrib.get('Id') == relationship_id]
            if not matches:
                self._package_mismatch('worksheet_relationship_missing')
                sheets.append(_empty_sheet_package(name))
                continue
            if len(matches) != 1:
                self._package_mismatch('worksheet_relationship_id_not_unique')
                sheets.append(_empty_sheet_package(name))
                continue
            relationship = matches[0]
            if relationship.attrib.get('Type') != WORKSHEET_REL_TYPE:
                self._package_mismatch('worksheet_relationship_type_mismatch')
            if relationship.attrib.get('TargetMode', '').lower() == 'external':
                self._package_mismatch('worksheet_relationship_external')
                sheets.append(_empty_sheet_package(name))
                continue
            path = _normalize_relationship_target(relationship.attrib.get('Target'))
            if path is None or not path.startswith('xl/worksheets/'):
                self._package_mismatch('worksheet_relationship_target_unsafe')
                sheets.append(_empty_sheet_package(name))
                continue
            part_count = self.zip_member_counts[path]
            if part_count == 0:
                self._package_mismatch('worksheet_part_missing')
                sheets.append(_empty_sheet_package(name))
                continue
            if part_count != 1:
                self._package_mismatch('worksheet_part_not_unique')
                sheets.append(_empty_sheet_package(name))
                continue
            sheets.append(_read_sheet_package(self.archive, name, path, self.style_catalog))
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
        for expected_sheet, actual_sheet in zip(expected.sheets, actual.sheets, strict=False):
            if expected_sheet.name != actual_sheet.name:
                continue
            _compare_sheet_package(expected_sheet, actual_sheet, mismatches)
            _compare_sheet_cells(expected, actual, expected_sheet, actual_sheet, mismatches)
            _compare_business_reconciliations(expected, actual, expected_sheet, actual_sheet, pipeline, mismatches)

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
    if not _storage_types_equivalent(expected.storage_type, actual.storage_type):
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
        return (
            expected_decimal is not None
            and actual_decimal is not None
            and _decimal_values_equal(expected_decimal, actual_decimal)
        )
    if expected.storage_type in TEXT_STORAGE_TYPES:
        return expected.resolved_text == actual.resolved_text
    return expected.lexical_value == actual.lexical_value


def _storage_types_equivalent(expected: StorageType, actual: StorageType) -> bool:
    return expected == actual or (expected in TEXT_STORAGE_TYPES and actual in TEXT_STORAGE_TYPES)


def _iter_xml_cells(
    package: _Package,
    sheet: _SheetPackage,
    mismatches: list[WorkbookMismatch],
) -> Iterator[XmlCell]:
    if sheet.path is None:
        return
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
        if not _decimal_values_equal(
            expected_totals[header],
            actual_totals[header],
            tolerance=COLUMN_TOTAL_ABSOLUTE_TOLERANCE,
        ):
            _append(
                mismatches,
                WorkbookMismatch(
                    expected_sheet.name,
                    _header_coordinate(expected_headers[header]),
                    'column_total_mismatch',
                ),
            )

    if keys and not _group_totals_equal(expected_groups, actual_groups):
        _append(mismatches, WorkbookMismatch(expected_sheet.name, None, 'group_total_mismatch'))


def _decimal_values_equal(
    expected: Decimal,
    actual: Decimal,
    *,
    tolerance: Decimal = NUMERIC_ABSOLUTE_TOLERANCE,
) -> bool:
    return abs(expected - actual) <= tolerance


def _group_totals_equal(
    expected: dict[tuple[str, ...], tuple[Decimal, ...]],
    actual: dict[tuple[str, ...], tuple[Decimal, ...]],
) -> bool:
    if expected.keys() != actual.keys():
        return False
    for key, expected_values in expected.items():
        actual_values = actual[key]
        if len(expected_values) != len(actual_values):
            return False
        if any(
            not _decimal_values_equal(expected_value, actual_value)
            for expected_value, actual_value in zip(expected_values, actual_values, strict=True)
        ):
            return False
    return True


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
    duplicate_group_keys: set[tuple[str, ...]] = set()
    current_row_number: int | None = None
    current_row: dict[int, XmlCell] = {}

    def flush_row() -> None:
        if current_row_number is None:
            return
        row = current_row
        key_values = tuple(_text_value(row.get(headers[key])) for key in keys)
        if keys and any(value is None or not value.strip() for value in key_values):
            _append(mismatches, WorkbookMismatch(sheet.name, None, 'blank_group_key'))
            return
        complete_key = cast(tuple[str, ...], key_values)
        if keys and complete_key in duplicate_group_keys:
            _append(mismatches, WorkbookMismatch(sheet.name, None, 'duplicate_group_key'))
            return
        if keys and complete_key in grouped:
            previous_totals = grouped.pop(complete_key)
            for index, header in enumerate(policy):
                totals[header] -= previous_totals[index]
            duplicate_group_keys.add(complete_key)
            _append(mismatches, WorkbookMismatch(sheet.name, None, 'duplicate_group_key'))
            return
        group_totals: list[Decimal] | None = None
        if keys:
            group_totals = [Decimal(0) for _ in policy]
            grouped[complete_key] = group_totals
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


def _read_sheet_package(
    archive: ZipFile,
    name: str,
    path: str,
    style_catalog: list[tuple[Any, ...]],
) -> _SheetPackage:
    dimension: str | None = None
    freeze_panes: str | None = None
    auto_filter: str | None = None
    column_widths: dict[int, Decimal] = {}
    column_styles: dict[int, int] = {}
    row_two_styles: dict[int, int] = {}
    conditional_format_ranges: list[str] = []
    with archive.open(path) as stream:
        for event, node in ET.iterparse(  # noqa: S314 - controlled local xlsx artifact.
            stream,
            events=('start', 'end'),
        ):
            if event == 'start':
                if node.tag == f'{{{MAIN_NS}}}dimension':
                    dimension = node.attrib.get('ref')
                elif node.tag == f'{{{MAIN_NS}}}pane':
                    freeze_panes = node.attrib.get('topLeftCell')
                elif node.tag == f'{{{MAIN_NS}}}autoFilter':
                    auto_filter = node.attrib.get('ref')
                elif node.tag == f'{{{MAIN_NS}}}conditionalFormatting':
                    conditional_format_ranges.append(node.attrib.get('sqref', ''))
                elif node.tag == f'{{{MAIN_NS}}}col':
                    width = Decimal(node.attrib['width']).quantize(Decimal('0.0001'))
                    style_id = int(node.attrib.get('style', '0'))
                    for column_index in range(int(node.attrib['min']), int(node.attrib['max']) + 1):
                        column_widths[column_index] = width
                        column_styles[column_index] = style_id
                elif node.tag == f'{{{MAIN_NS}}}c':
                    coordinate = node.attrib['r']
                    row_number, column_index = _coordinate_key(coordinate)
                    if row_number == 2:
                        row_two_styles[column_index] = int(node.attrib.get('s', column_styles.get(column_index, 0)))
            else:
                node.clear()

    formats = {
        column_index: style_catalog[style_id][0]
        for column_index, style_id in column_styles.items()
        if style_catalog[style_id][0] != 'General'
    }
    for column_index, style_id in row_two_styles.items():
        number_format = style_catalog[style_id][0]
        if number_format != 'General':
            formats[column_index] = number_format
    return _SheetPackage(
        name=name,
        path=path,
        dimension=dimension,
        freeze_panes=freeze_panes,
        auto_filter=auto_filter,
        column_widths=tuple(sorted(column_widths.items())),
        column_styles=tuple(sorted(column_styles.items())),
        column_style_signatures=tuple(
            sorted((column, style_catalog[style_id]) for column, style_id in column_styles.items())
        ),
        number_formats=tuple(sorted(formats.items())),
        conditional_format_ranges=tuple(sorted(conditional_format_ranges)),
    )


def _empty_sheet_package(name: str) -> _SheetPackage:
    return _SheetPackage(
        name=name,
        path=None,
        dimension=None,
        freeze_panes=None,
        auto_filter=None,
        column_widths=(),
        column_styles=(),
        column_style_signatures=(),
        number_formats=(),
        conditional_format_ranges=(),
    )


def _xml_root(archive: ZipFile, member: str) -> ET.Element:
    return ET.fromstring(archive.read(member))  # noqa: S314 - controlled local xlsx artifact.


def _normalize_relationship_target(target: str | None) -> str | None:
    if not target or '\\' in target:
        return None
    relative = PurePosixPath(target)
    if relative.is_absolute() or '..' in relative.parts:
        return None
    return (PurePosixPath('xl') / relative).as_posix()


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

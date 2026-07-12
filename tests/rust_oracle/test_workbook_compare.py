from __future__ import annotations

import shutil
import warnings
import xml.etree.ElementTree as ET
from collections.abc import Callable
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
import xlsxwriter

from tests.rust_oracle.workbook_compare import NUMERIC_COLUMNS, WorkbookMismatch, compare_workbooks

MAIN_NS = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
PACKAGE_REL_NS = 'http://schemas.openxmlformats.org/package/2006/relationships'
CONTENT_TYPE_NS = 'http://schemas.openxmlformats.org/package/2006/content-types'


def test_decimal_lexical_difference_of_one_e_minus_seven_is_rejected(tmp_path: Path) -> None:
    expected, actual = _matching_workbooks(tmp_path, [['数值'], [1]])
    _rewrite_zip_xml(actual, 'xl/worksheets/sheet1.xml', lambda xml: xml.replace(b'<v>1</v>', b'<v>1.0000001</v>'))

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert not report.passed
    assert _mismatch(report.mismatches, 'value_mismatch', 'A2').expected_storage_type == 'n'


def test_decimal_lexical_float_tail_of_one_e_minus_ten_is_accepted(tmp_path: Path) -> None:
    expected, actual = _matching_workbooks(tmp_path, [['数值'], [1]])
    _rewrite_zip_xml(actual, 'xl/worksheets/sheet1.xml', lambda xml: xml.replace(b'<v>1</v>', b'<v>1.0000000001</v>'))

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert report.passed


def test_equivalent_decimal_lexemes_and_signed_zero_are_equal(tmp_path: Path) -> None:
    expected, actual = _matching_workbooks(tmp_path, [['数值一', '数值二'], [100, 0]])

    def equivalent_lexemes(xml: bytes) -> bytes:
        root = ET.fromstring(xml)  # noqa: S314 - controlled synthetic xlsx fixture.
        cells = {cell.attrib['r']: cell for cell in root.findall(f'.//{{{MAIN_NS}}}c')}
        cells['A2'].find(f'{{{MAIN_NS}}}v').text = '1E+2'  # type: ignore[union-attr]
        cells['B2'].find(f'{{{MAIN_NS}}}v').text = '-0.000'  # type: ignore[union-attr]
        return ET.tostring(root, encoding='utf-8', xml_declaration=True)

    _rewrite_zip_xml(actual, 'xl/worksheets/sheet1.xml', equivalent_lexemes)

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert report.passed


def test_numeric_string_is_not_equal_to_numeric_cell(tmp_path: Path) -> None:
    expected = tmp_path / 'expected.xlsx'
    actual = tmp_path / 'actual.xlsx'
    _write_xlsx(expected, {'Sheet': [['字段'], ['00123']]})
    _write_xlsx(actual, {'Sheet': [['字段'], [123]]})

    report = compare_workbooks(expected, actual, pipeline='gb')

    mismatch = _mismatch(report.mismatches, 'storage_type_mismatch', 'A2')
    assert (mismatch.expected_storage_type, mismatch.actual_storage_type) == ('s', 'n')


def test_decimal_column_total_mismatch_is_rejected(tmp_path: Path) -> None:
    expected = tmp_path / 'expected.xlsx'
    actual = tmp_path / 'actual.xlsx'
    rows = _business_rows('gb', '成本计算单数量聚合维度', amounts=(1,))
    _write_xlsx(expected, {'成本计算单数量聚合维度': rows})
    _write_xlsx(actual, {'成本计算单数量聚合维度': _replace_amount(rows, row=1, amount=2)})

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert not report.passed
    assert any(item.mismatch_kind == 'column_total_mismatch' for item in report.mismatches)


def test_grouped_work_order_total_mismatch_is_rejected(tmp_path: Path) -> None:
    expected = tmp_path / 'expected.xlsx'
    actual = tmp_path / 'actual.xlsx'
    expected_rows = _business_rows('gb', '成本分析工单维度', amounts=(1, 2))
    actual_rows = _replace_amount(expected_rows, row=1, amount=2)
    actual_rows = _replace_amount(actual_rows, row=2, amount=1)
    _write_xlsx(expected, {'成本分析工单维度': expected_rows})
    _write_xlsx(actual, {'成本分析工单维度': actual_rows})

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert not report.passed
    assert not any(item.mismatch_kind == 'column_total_mismatch' for item in report.mismatches)
    assert any(item.mismatch_kind == 'group_total_mismatch' for item in report.mismatches)


def test_swapped_data_row_styles_are_rejected_by_coordinate(tmp_path: Path) -> None:
    expected = tmp_path / 'expected.xlsx'
    actual = tmp_path / 'actual.xlsx'
    _write_styled_rows(expected, swapped=False)
    _write_styled_rows(actual, swapped=True)

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert not report.passed
    assert _mismatch(report.mismatches, 'cell_style_mismatch', 'A2')


def test_explicit_blank_and_column_inherited_style_remain_equivalent(tmp_path: Path) -> None:
    expected = tmp_path / 'expected.xlsx'
    actual = tmp_path / 'actual.xlsx'
    _write_blank_style(expected, explicit_blank=True)
    _write_blank_style(actual, explicit_blank=False)

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert report.passed


def test_shared_strings_relationship_type_mismatch_is_rejected(tmp_path: Path) -> None:
    expected, actual = _matching_workbooks(tmp_path, [['字段'], ['合成文本']])

    def wrong_type(xml: bytes) -> bytes:
        root = ET.fromstring(xml)  # noqa: S314 - controlled synthetic xlsx fixture.
        for relationship in root.findall(f'{{{PACKAGE_REL_NS}}}Relationship'):
            if relationship.attrib.get('Target') == 'sharedStrings.xml':
                relationship.set('Type', 'urn:invalid:sharedStrings')
        return ET.tostring(root, encoding='utf-8', xml_declaration=True)

    _rewrite_zip_xml(actual, 'xl/_rels/workbook.xml.rels', wrong_type)

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert _mismatch(report.mismatches, 'shared_strings_relationship_type_mismatch')


def test_shared_strings_relationship_target_mismatch_is_rejected(tmp_path: Path) -> None:
    expected, actual = _matching_workbooks(tmp_path, [['字段'], ['合成文本']])

    def wrong_target(xml: bytes) -> bytes:
        root = ET.fromstring(xml)  # noqa: S314 - controlled synthetic xlsx fixture.
        for relationship in root.findall(f'{{{PACKAGE_REL_NS}}}Relationship'):
            if relationship.attrib.get('Target') == 'sharedStrings.xml':
                relationship.set('Target', 'wrong.xml')
        return ET.tostring(root, encoding='utf-8', xml_declaration=True)

    _rewrite_zip_xml(actual, 'xl/_rels/workbook.xml.rels', wrong_target)

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert _mismatch(report.mismatches, 'shared_strings_relationship_target_mismatch')


def test_shared_strings_relationship_missing_is_rejected(tmp_path: Path) -> None:
    expected, actual = _matching_workbooks(tmp_path, [['字段'], ['合成文本']])

    def remove_relationship(xml: bytes) -> bytes:
        root = ET.fromstring(xml)  # noqa: S314 - controlled synthetic xlsx fixture.
        for relationship in list(root.findall(f'{{{PACKAGE_REL_NS}}}Relationship')):
            if relationship.attrib.get('Target') == 'sharedStrings.xml':
                root.remove(relationship)
        return ET.tostring(root, encoding='utf-8', xml_declaration=True)

    _rewrite_zip_xml(actual, 'xl/_rels/workbook.xml.rels', remove_relationship)

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert _mismatch(report.mismatches, 'shared_strings_relationship_missing')


def test_shared_strings_content_type_missing_is_rejected(tmp_path: Path) -> None:
    expected, actual = _matching_workbooks(tmp_path, [['字段'], ['合成文本']])

    def remove_override(xml: bytes) -> bytes:
        root = ET.fromstring(xml)  # noqa: S314 - controlled synthetic xlsx fixture.
        for override in list(root.findall(f'{{{CONTENT_TYPE_NS}}}Override')):
            if override.attrib.get('PartName') == '/xl/sharedStrings.xml':
                root.remove(override)
        return ET.tostring(root, encoding='utf-8', xml_declaration=True)

    _rewrite_zip_xml(actual, '[Content_Types].xml', remove_override)

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert _mismatch(report.mismatches, 'shared_strings_content_type_missing')


def test_shared_strings_part_missing_is_rejected(tmp_path: Path) -> None:
    expected, actual = _matching_workbooks(tmp_path, [['字段'], ['合成文本']])
    _rewrite_zip_xml(actual, 'xl/sharedStrings.xml', lambda _xml: None)

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert _mismatch(report.mismatches, 'shared_strings_part_missing')


def test_shared_string_index_out_of_range_is_rejected(tmp_path: Path) -> None:
    expected, actual = _matching_workbooks(tmp_path, [['字段'], ['合成文本']])

    def invalid_index(xml: bytes) -> bytes:
        root = ET.fromstring(xml)  # noqa: S314 - controlled synthetic xlsx fixture.
        cells = root.findall(f'.//{{{MAIN_NS}}}c[@t="s"]')
        cells[-1].find(f'{{{MAIN_NS}}}v').text = '999'  # type: ignore[union-attr]
        return ET.tostring(root, encoding='utf-8', xml_declaration=True)

    _rewrite_zip_xml(actual, 'xl/worksheets/sheet1.xml', invalid_index)

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert _mismatch(report.mismatches, 'shared_string_index_out_of_range', 'A2')


def test_inline_string_and_shared_string_cells_are_equivalent(tmp_path: Path) -> None:
    expected, actual = _matching_workbooks(tmp_path, [['字段'], ['合成文本']])

    def use_inline_string(xml: bytes) -> bytes:
        root = ET.fromstring(xml)  # noqa: S314 - controlled synthetic xlsx fixture.
        cell = root.findall(f'.//{{{MAIN_NS}}}c[@t="s"]')[-1]
        cell.set('t', 'inlineStr')
        for child in list(cell):
            cell.remove(child)
        inline = ET.SubElement(cell, f'{{{MAIN_NS}}}is')
        ET.SubElement(inline, f'{{{MAIN_NS}}}t').text = '合成文本'
        return ET.tostring(root, encoding='utf-8', xml_declaration=True)

    _rewrite_zip_xml(actual, 'xl/worksheets/sheet1.xml', use_inline_string)

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert report.passed


def test_inline_string_and_shared_string_packages_are_equivalent(tmp_path: Path) -> None:
    expected = tmp_path / 'expected.xlsx'
    actual = tmp_path / 'actual.xlsx'
    rows = [['字段'], ['合成文本']]
    _write_xlsx(expected, {'Sheet': rows})
    shutil.copyfile(expected, actual)
    _convert_shared_strings_to_inline(actual)

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert report.passed


def test_workbook_mismatch_never_contains_real_cell_values(tmp_path: Path) -> None:
    expected = tmp_path / 'expected.xlsx'
    actual = tmp_path / 'actual.xlsx'
    synthetic_expected_value = '合成敏感值甲'
    synthetic_actual_value = '合成敏感值乙'
    _write_xlsx(expected, {'Sheet': [['字段'], [synthetic_expected_value]]})
    _write_xlsx(actual, {'Sheet': [['字段'], [synthetic_actual_value]]})

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert not report.passed
    assert synthetic_expected_value not in repr(report)
    assert synthetic_actual_value not in repr(report)
    assert all(isinstance(item, WorkbookMismatch) for item in report.mismatches)


def test_forbidden_sheet_and_sheet_order_are_rejected(tmp_path: Path) -> None:
    expected = tmp_path / 'expected.xlsx'
    actual = tmp_path / 'actual.xlsx'
    _write_xlsx(expected, {'Sheet': [['字段']]})
    _write_xlsx(actual, {'Sheet': [['字段']], '成本分析产品维度': [['字段']]})

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert _mismatch(report.mismatches, 'forbidden_sheet')
    assert _mismatch(report.mismatches, 'sheet_order_mismatch')


def test_shape_freeze_panes_and_filter_are_compared(tmp_path: Path) -> None:
    expected = tmp_path / 'expected.xlsx'
    actual = tmp_path / 'actual.xlsx'
    _write_metadata_workbook(expected, freeze=True, filtered=True, extra_column=False)
    _write_metadata_workbook(actual, freeze=False, filtered=False, extra_column=True)

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert _mismatch(report.mismatches, 'shape_mismatch')
    assert _mismatch(report.mismatches, 'freeze_panes_mismatch')
    assert _mismatch(report.mismatches, 'auto_filter_mismatch')


def test_column_width_number_format_and_header_style_are_compared(tmp_path: Path) -> None:
    expected = tmp_path / 'expected.xlsx'
    actual = tmp_path / 'actual.xlsx'
    _write_format_workbook(expected, styled=True)
    _write_format_workbook(actual, styled=False)

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert _mismatch(report.mismatches, 'column_width_mismatch')
    assert _mismatch(report.mismatches, 'number_format_mismatch')
    assert _mismatch(report.mismatches, 'header_style_mismatch', 'A1')


def test_missing_required_numeric_header_fails_closed(tmp_path: Path) -> None:
    expected, actual = _matching_workbooks(tmp_path, [['月份', '产品编码', '工单编号', '工单行号']])
    _rename_sheet(expected, '成本计算单数量聚合维度')
    _rename_sheet(actual, '成本计算单数量聚合维度')

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert _mismatch(report.mismatches, 'required_header_missing')


def test_pipeline_specific_numeric_header_fails_when_unexpected(tmp_path: Path) -> None:
    expected = tmp_path / 'expected.xlsx'
    actual = tmp_path / 'actual.xlsx'
    rows = _business_rows('gb', '成本计算单数量聚合维度', amounts=(1,))
    rows[0].append('本期完工软件费用合计完工金额')
    rows[1].append(0)
    _write_xlsx(expected, {'成本计算单数量聚合维度': rows})
    _write_xlsx(actual, {'成本计算单数量聚合维度': rows})

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert _mismatch(report.mismatches, 'unexpected_numeric_header')


def test_worksheet_metadata_never_uses_zipfile_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [['字段一', '字段二'], *[[row, f'合成-{row}'] for row in range(5_000)]]
    expected, actual = _matching_workbooks(tmp_path, rows)
    original_read = ZipFile.read

    def guarded_read(archive: ZipFile, name: str, *args: object, **kwargs: object) -> bytes:
        assert not name.startswith('xl/worksheets/')
        return original_read(archive, name, *args, **kwargs)

    monkeypatch.setattr(ZipFile, 'read', guarded_read)

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert report.passed


@pytest.mark.parametrize(
    ('mutation', 'mismatch_kind'),
    (
        ('invalid-type', 'worksheet_relationship_type_mismatch'),
        ('duplicate-id', 'worksheet_relationship_id_not_unique'),
        ('bad-target', 'worksheet_relationship_target_unsafe'),
        ('missing-part', 'worksheet_part_missing'),
        ('duplicate-part', 'worksheet_part_not_unique'),
    ),
)
def test_worksheet_relationship_chain_fails_closed(
    tmp_path: Path,
    mutation: str,
    mismatch_kind: str,
) -> None:
    expected, actual = _matching_workbooks(tmp_path, [['字段'], ['合成值']])
    if mutation == 'invalid-type':
        _rewrite_workbook_relationships(actual, lambda rel: rel.set('Type', 'urn:invalid:worksheet'))
    elif mutation == 'duplicate-id':
        _duplicate_relationship(actual, target_suffix='worksheets/sheet1.xml')
    elif mutation == 'bad-target':
        _rewrite_workbook_relationships(actual, lambda rel: rel.set('Target', '../escape.xml'))
    elif mutation == 'missing-part':
        _rewrite_zip_xml(actual, 'xl/worksheets/sheet1.xml', lambda _xml: None)
    else:
        _append_duplicate_member(actual, 'xl/worksheets/sheet1.xml')

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert _mismatch(report.mismatches, mismatch_kind)


@pytest.mark.parametrize(
    ('mutation', 'mismatch_kind'),
    (
        ('duplicate-relationship', 'shared_strings_relationship_count_mismatch'),
        ('external', 'shared_strings_relationship_external'),
        ('duplicate-override', 'shared_strings_content_type_not_unique'),
        ('conflicting-override', 'shared_strings_content_type_not_unique'),
        ('duplicate-part', 'shared_strings_part_not_unique'),
        ('bad-target', 'shared_strings_relationship_target_unsafe'),
    ),
)
def test_shared_strings_unique_chain_fails_closed(
    tmp_path: Path,
    mutation: str,
    mismatch_kind: str,
) -> None:
    expected, actual = _matching_workbooks(tmp_path, [['字段'], ['合成文本']])
    if mutation == 'duplicate-relationship':
        _duplicate_relationship(actual, target_suffix='sharedStrings.xml')
    elif mutation == 'external':
        _rewrite_shared_strings_relationship(actual, target='sharedStrings.xml', target_mode='External')
    elif mutation in {'duplicate-override', 'conflicting-override'}:
        _duplicate_shared_strings_override(actual, conflicting=mutation == 'conflicting-override')
    elif mutation == 'duplicate-part':
        _append_duplicate_member(actual, 'xl/sharedStrings.xml')
    else:
        _rewrite_shared_strings_relationship(actual, target='../sharedStrings.xml')

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert _mismatch(report.mismatches, mismatch_kind)


def test_workbook_sheet_relationship_id_must_be_unique(tmp_path: Path) -> None:
    expected = tmp_path / 'expected.xlsx'
    actual = tmp_path / 'actual.xlsx'
    sheets = {'Sheet一': [['字段']], 'Sheet二': [['字段']]}
    _write_xlsx(expected, sheets)
    _write_xlsx(actual, sheets)

    def duplicate_sheet_id(xml: bytes) -> bytes:
        root = ET.fromstring(xml)  # noqa: S314 - controlled synthetic xlsx fixture.
        sheet_nodes = root.findall(f'.//{{{MAIN_NS}}}sheet')
        relationship_attribute = '{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id'
        sheet_nodes[1].set(relationship_attribute, sheet_nodes[0].attrib[relationship_attribute])
        return ET.tostring(root, encoding='utf-8', xml_declaration=True)

    _rewrite_zip_xml(actual, 'xl/workbook.xml', duplicate_sheet_id)

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert _mismatch(report.mismatches, 'worksheet_sheet_relationship_id_not_unique')


@pytest.mark.parametrize('sheet', ('成本计算单数量聚合维度', '成本分析工单维度'))
def test_blank_group_key_fails_closed_without_exposing_value(tmp_path: Path, sheet: str) -> None:
    expected = tmp_path / 'expected.xlsx'
    actual = tmp_path / 'actual.xlsx'
    rows = _business_rows('gb', sheet, amounts=(1,))
    key_column = rows[0].index('工单编号')
    rows[1][key_column] = '   '
    _write_xlsx(expected, {sheet: rows})
    _write_xlsx(actual, {sheet: rows})

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert _mismatch(report.mismatches, 'blank_group_key')
    assert '   ' not in repr(report)


@pytest.mark.parametrize('sheet', ('成本计算单数量聚合维度', '成本分析工单维度'))
def test_duplicate_group_key_fails_closed_without_exposing_value(tmp_path: Path, sheet: str) -> None:
    expected = tmp_path / 'expected.xlsx'
    actual = tmp_path / 'actual.xlsx'
    rows = _business_rows('gb', sheet, amounts=(1, 2))
    for key in _group_keys(sheet):
        key_column = rows[0].index(key)
        rows[2][key_column] = rows[1][key_column]
    _write_xlsx(expected, {sheet: rows})
    _write_xlsx(actual, {sheet: rows})

    report = compare_workbooks(expected, actual, pipeline='gb')

    assert _mismatch(report.mismatches, 'duplicate_group_key')
    assert 'W1' not in repr(report)


def _mismatch(
    mismatches: tuple[WorkbookMismatch, ...],
    kind: str,
    coordinate: str | None = None,
) -> WorkbookMismatch:
    return next(
        item
        for item in mismatches
        if item.mismatch_kind == kind and (coordinate is None or item.coordinate == coordinate)
    )


def _matching_workbooks(tmp_path: Path, rows: list[list[object]]) -> tuple[Path, Path]:
    expected = tmp_path / 'expected.xlsx'
    actual = tmp_path / 'actual.xlsx'
    _write_xlsx(expected, {'Sheet': rows})
    shutil.copyfile(expected, actual)
    return expected, actual


def _write_xlsx(path: Path, sheets: dict[str, list[list[object]]]) -> None:
    workbook = xlsxwriter.Workbook(path)
    for sheet_name, rows in sheets.items():
        worksheet = workbook.add_worksheet(sheet_name)
        for row_index, row in enumerate(rows):
            for column_index, value in enumerate(row):
                worksheet.write(row_index, column_index, value)
    workbook.close()


def _convert_shared_strings_to_inline(path: Path) -> None:
    with ZipFile(path) as archive:
        shared_root = ET.fromstring(archive.read('xl/sharedStrings.xml'))  # noqa: S314 - controlled fixture.
        shared_strings = tuple(
            ''.join(text.text or '' for text in item.iter(f'{{{MAIN_NS}}}t')) for item in shared_root
        )
        worksheet_parts = tuple(name for name in archive.namelist() if name.startswith('xl/worksheets/sheet'))

    def inline_cells(xml: bytes) -> bytes:
        root = ET.fromstring(xml)  # noqa: S314 - controlled synthetic xlsx fixture.
        for cell in root.findall(f'.//{{{MAIN_NS}}}c[@t="s"]'):
            value = cell.find(f'{{{MAIN_NS}}}v')
            assert value is not None and value.text is not None
            text_value = shared_strings[int(value.text)]
            cell.set('t', 'inlineStr')
            for child in list(cell):
                cell.remove(child)
            inline = ET.SubElement(cell, f'{{{MAIN_NS}}}is')
            ET.SubElement(inline, f'{{{MAIN_NS}}}t').text = text_value
        return ET.tostring(root, encoding='utf-8', xml_declaration=True)

    for worksheet_part in worksheet_parts:
        _rewrite_zip_xml(path, worksheet_part, inline_cells)

    def remove_relationship(xml: bytes) -> bytes:
        root = ET.fromstring(xml)  # noqa: S314 - controlled synthetic xlsx fixture.
        for relationship in list(root.findall(f'{{{PACKAGE_REL_NS}}}Relationship')):
            if relationship.attrib.get('Target') == 'sharedStrings.xml':
                root.remove(relationship)
        return ET.tostring(root, encoding='utf-8', xml_declaration=True)

    def remove_content_type(xml: bytes) -> bytes:
        root = ET.fromstring(xml)  # noqa: S314 - controlled synthetic xlsx fixture.
        for override in list(root.findall(f'{{{CONTENT_TYPE_NS}}}Override')):
            if override.attrib.get('PartName') == '/xl/sharedStrings.xml':
                root.remove(override)
        return ET.tostring(root, encoding='utf-8', xml_declaration=True)

    _rewrite_zip_xml(path, 'xl/_rels/workbook.xml.rels', remove_relationship)
    _rewrite_zip_xml(path, '[Content_Types].xml', remove_content_type)
    _rewrite_zip_xml(path, 'xl/sharedStrings.xml', lambda _xml: None)


def _write_styled_rows(path: Path, *, swapped: bool) -> None:
    workbook = xlsxwriter.Workbook(path)
    worksheet = workbook.add_worksheet('Sheet')
    red = workbook.add_format({'bg_color': '#FF0000'})
    blue = workbook.add_format({'bg_color': '#0000FF'})
    worksheet.write(0, 0, '字段')
    formats = (blue, red) if swapped else (red, blue)
    worksheet.write(1, 0, 1, formats[0])
    worksheet.write(2, 0, 2, formats[1])
    workbook.close()


def _write_blank_style(path: Path, *, explicit_blank: bool) -> None:
    workbook = xlsxwriter.Workbook(path)
    worksheet = workbook.add_worksheet('Sheet')
    numeric = workbook.add_format({'num_format': '#,##0.00'})
    worksheet.set_column(0, 0, 15, numeric)
    worksheet.write(0, 0, '数值列')
    worksheet.write(0, 1, '标记列')
    worksheet.write(1, 1, '保留数据行')
    if explicit_blank:
        worksheet.write_blank(1, 0, None, numeric)
    workbook.close()


def _write_metadata_workbook(path: Path, *, freeze: bool, filtered: bool, extra_column: bool) -> None:
    workbook = xlsxwriter.Workbook(path)
    worksheet = workbook.add_worksheet('Sheet')
    worksheet.write(0, 0, '字段')
    if extra_column:
        worksheet.write(0, 1, '额外字段')
    if freeze:
        worksheet.freeze_panes(1, 0)
    if filtered:
        worksheet.autofilter('A1:A1')
    workbook.close()


def _write_format_workbook(path: Path, *, styled: bool) -> None:
    workbook = xlsxwriter.Workbook(path)
    worksheet = workbook.add_worksheet('Sheet')
    if styled:
        header = workbook.add_format({'bold': True, 'bg_color': '#D9E1F2'})
        numeric = workbook.add_format({'num_format': '#,##0.00'})
        worksheet.set_column(0, 0, 15)
        worksheet.write(0, 0, '字段', header)
        worksheet.write(1, 0, 1, numeric)
    else:
        worksheet.write(0, 0, '字段')
        worksheet.write(1, 0, 1)
    workbook.close()


def _business_rows(pipeline: str, sheet: str, *, amounts: tuple[int, ...]) -> list[list[object]]:
    headers = list(dict.fromkeys((*_group_keys(sheet), *NUMERIC_COLUMNS[(pipeline, sheet)])))
    rows: list[list[object]] = [headers]
    amount_header = NUMERIC_COLUMNS[(pipeline, sheet)][0]
    for index, amount in enumerate(amounts, start=1):
        values: dict[str, object] = dict.fromkeys(headers, 0)
        values.update({'月份': '2026-01', '产品编码': f'P{index}', '工单编号': f'W{index}'})
        values['工单行号' if sheet == '成本计算单数量聚合维度' else '工单行'] = str(index)
        values[amount_header] = amount
        rows.append([values[header] for header in headers])
    return rows


def _group_keys(sheet: str) -> tuple[str, ...]:
    if sheet == '成本计算单数量聚合维度':
        return ('月份', '产品编码', '工单编号', '工单行号')
    return ('月份', '产品编码', '工单编号', '工单行')


def _replace_amount(rows: list[list[object]], *, row: int, amount: int) -> list[list[object]]:
    copied = [values.copy() for values in rows]
    copied[row][len(_group_keys('成本分析工单维度'))] = amount
    return copied


def _rewrite_zip_xml(path: Path, member: str, transform: Callable[[bytes], bytes | None]) -> None:
    replacement = path.with_suffix('.rewritten.xlsx')
    with ZipFile(path) as source, ZipFile(replacement, 'w', ZIP_DEFLATED) as destination:
        for info in source.infolist():
            payload = source.read(info.filename)
            if info.filename == member:
                payload = transform(payload)
                if payload is None:
                    continue
            destination.writestr(info, payload)
    replacement.replace(path)


def _rename_sheet(path: Path, title: str) -> None:
    def rename(xml: bytes) -> bytes:
        root = ET.fromstring(xml)  # noqa: S314 - controlled synthetic xlsx fixture.
        root.find(f'.//{{{MAIN_NS}}}sheet').set('name', title)  # type: ignore[union-attr]
        return ET.tostring(root, encoding='utf-8', xml_declaration=True)

    _rewrite_zip_xml(path, 'xl/workbook.xml', rename)


def _rewrite_workbook_relationships(path: Path, mutate: Callable[[ET.Element], None]) -> None:
    def rewrite(xml: bytes) -> bytes:
        root = ET.fromstring(xml)  # noqa: S314 - controlled synthetic xlsx fixture.
        relationship = next(
            item
            for item in root.findall(f'{{{PACKAGE_REL_NS}}}Relationship')
            if item.attrib.get('Target') == 'worksheets/sheet1.xml'
        )
        mutate(relationship)
        return ET.tostring(root, encoding='utf-8', xml_declaration=True)

    _rewrite_zip_xml(path, 'xl/_rels/workbook.xml.rels', rewrite)


def _duplicate_relationship(path: Path, *, target_suffix: str) -> None:
    def duplicate(xml: bytes) -> bytes:
        root = ET.fromstring(xml)  # noqa: S314 - controlled synthetic xlsx fixture.
        relationship = next(
            item
            for item in root.findall(f'{{{PACKAGE_REL_NS}}}Relationship')
            if item.attrib.get('Target', '').endswith(target_suffix)
        )
        root.append(ET.fromstring(ET.tostring(relationship)))  # noqa: S314 - controlled synthetic XML.
        return ET.tostring(root, encoding='utf-8', xml_declaration=True)

    _rewrite_zip_xml(path, 'xl/_rels/workbook.xml.rels', duplicate)


def _duplicate_shared_strings_override(path: Path, *, conflicting: bool) -> None:
    def duplicate(xml: bytes) -> bytes:
        root = ET.fromstring(xml)  # noqa: S314 - controlled synthetic xlsx fixture.
        override = next(
            item
            for item in root.findall(f'{{{CONTENT_TYPE_NS}}}Override')
            if item.attrib.get('PartName') == '/xl/sharedStrings.xml'
        )
        copied = ET.fromstring(ET.tostring(override))  # noqa: S314 - controlled synthetic XML.
        if conflicting:
            copied.set('ContentType', 'application/invalid')
        root.append(copied)
        return ET.tostring(root, encoding='utf-8', xml_declaration=True)

    _rewrite_zip_xml(path, '[Content_Types].xml', duplicate)


def _rewrite_shared_strings_relationship(path: Path, *, target: str, target_mode: str | None = None) -> None:
    def rewrite(xml: bytes) -> bytes:
        root = ET.fromstring(xml)  # noqa: S314 - controlled synthetic xlsx fixture.
        relationship = next(
            item
            for item in root.findall(f'{{{PACKAGE_REL_NS}}}Relationship')
            if item.attrib.get('Target') == 'sharedStrings.xml'
        )
        relationship.set('Target', target)
        if target_mode is not None:
            relationship.set('TargetMode', target_mode)
        return ET.tostring(root, encoding='utf-8', xml_declaration=True)

    _rewrite_zip_xml(path, 'xl/_rels/workbook.xml.rels', rewrite)


def _append_duplicate_member(path: Path, member: str) -> None:
    with ZipFile(path, 'a', ZIP_DEFLATED) as archive:
        payload = archive.read(member)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            archive.writestr(member, payload)

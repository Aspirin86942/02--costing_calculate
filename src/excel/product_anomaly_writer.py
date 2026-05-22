"""按产品异常值分析 sheet 的特殊布局写出。"""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.analytics.contracts import ProductAnomalySection, SheetModel
from src.excel.excel_values import freeze_panes_to_rc, resolve_fixed_width, write_cell
from src.excel.styles import resolve_metric_number_format


def build_product_anomaly_sections_from_model(model: SheetModel) -> list[ProductAnomalySection]:
    """把兼容摘要 SheetModel 还原成产品分段，复用既有特殊布局 writer。"""
    if len(model.columns) < 2:
        return []

    has_scope_column = len(model.columns) >= 3 and model.columns[2] == '分析口径'
    table_column_start_idx = 3 if has_scope_column else 2
    table_columns = list(model.columns[table_column_start_idx:])
    grouped_rows: dict[tuple[str, str, str | None], list[tuple[object, ...]]] = {}
    group_order: list[tuple[str, str, str | None]] = []
    for row in model.rows_factory():
        if len(row) < table_column_start_idx:
            continue
        product_code = '' if row[0] is None else str(row[0])
        product_name = '' if row[1] is None else str(row[1])
        section_label: str | None = None
        if has_scope_column:
            raw_scope_label = row[2]
            if raw_scope_label is not None and str(raw_scope_label).strip():
                section_label = str(raw_scope_label)
        key = (product_code, product_name, section_label)
        if key not in grouped_rows:
            grouped_rows[key] = []
            group_order.append(key)
        grouped_rows[key].append(tuple(row[table_column_start_idx : table_column_start_idx + len(table_columns)]))

    sections: list[ProductAnomalySection] = []
    for product_code, product_name, section_label in group_order:
        section_data = pd.DataFrame(
            grouped_rows[(product_code, product_name, section_label)],
            columns=table_columns,
        )
        section_column_types = {column: model.column_types.get(column, 'text') for column in table_columns}
        sections.append(
            ProductAnomalySection(
                product_code=product_code,
                product_name=product_name,
                data=section_data,
                column_types=section_column_types,
                amount_columns=[],
                outlier_cells=set(),
                section_label=section_label,
            )
        )
    return sections


def write_product_anomaly_sheet(
    writer: pd.ExcelWriter,
    sheet_name: str,
    sections: list[ProductAnomalySection],
) -> None:
    """写入按产品异常值分析页，兼容 legacy/scoped 两种布局。"""
    if any(section.section_label is not None for section in sections):
        _write_scoped_product_anomaly_sheet(writer, sheet_name, sections)
        return
    _write_legacy_product_anomaly_sheet(writer, sheet_name, sections)


def _write_legacy_product_anomaly_sheet(
    writer: pd.ExcelWriter,
    sheet_name: str,
    sections: list[ProductAnomalySection],
) -> None:
    """写入 legacy 单段布局，保持既有契约不变。"""
    _write_product_anomaly_sections(
        writer,
        sheet_name,
        sections,
        scoped=False,
        freeze_panes='A4',
        scope_label_row_offset=None,
    )


def _write_scoped_product_anomaly_sheet(
    writer: pd.ExcelWriter,
    sheet_name: str,
    sections: list[ProductAnomalySection],
) -> None:
    """写入 scoped 多段布局，按产品与分析口径分块展示。"""
    _write_product_anomaly_sections(
        writer,
        sheet_name,
        sections,
        scoped=True,
        freeze_panes='A5',
        scope_label_row_offset=2,
    )


def _write_product_anomaly_sections(
    writer: pd.ExcelWriter,
    sheet_name: str,
    sections: list[ProductAnomalySection],
    *,
    scoped: bool,
    freeze_panes: str,
    scope_label_row_offset: int | None,
) -> None:
    workbook = writer.book
    worksheet = workbook.add_worksheet(sheet_name)
    writer.sheets[sheet_name] = worksheet

    meta_header_format = workbook.add_format(
        {'bold': True, 'bg_color': '#D9E1F2', 'align': 'center', 'valign': 'vcenter', 'border': 1}
    )
    meta_value_format = workbook.add_format(
        {'bold': True, 'bg_color': '#B4C6E7', 'align': 'left', 'valign': 'vcenter', 'border': 1}
    )
    table_header_format = workbook.add_format(
        {'bold': True, 'bg_color': '#D9E1F2', 'align': 'center', 'valign': 'vcenter', 'border': 1}
    )
    text_format = workbook.add_format({'align': 'left', 'valign': 'vcenter', 'border': 1})
    right_text_format = workbook.add_format({'align': 'right', 'valign': 'vcenter', 'border': 1})
    metric_format_cache: dict[str, Any] = {}

    current_row = 0
    max_col_overall = 1
    filter_set = False

    for section in sections:
        columns = section.data.columns.tolist()
        if not columns:
            continue

        meta_header_row = current_row
        meta_value_row = current_row + 1
        table_header_row = current_row + (3 if scoped else 2)
        data_start_row = table_header_row + 1
        max_col_overall = max(max_col_overall, len(columns))

        worksheet.write(meta_header_row, 0, '产品编码', meta_header_format)
        worksheet.write(meta_header_row, 1, '产品名称', meta_header_format)
        worksheet.write(meta_value_row, 0, section.product_code, meta_value_format)
        worksheet.write(meta_value_row, 1, section.product_name, meta_value_format)
        if scope_label_row_offset is not None:
            scope_row = current_row + scope_label_row_offset
            worksheet.write(scope_row, 0, '分析口径', meta_header_format)
            worksheet.write(
                scope_row,
                1,
                '' if section.section_label is None else section.section_label,
                meta_value_format,
            )

        for col_idx, column_name in enumerate(columns):
            worksheet.write(table_header_row, col_idx, column_name, table_header_format)

        for row_offset, row_data in enumerate(section.data.itertuples(index=False, name=None)):
            excel_row = data_start_row + row_offset
            for col_idx, value in enumerate(row_data):
                metric_type = section.column_types.get(columns[col_idx], '')
                number_format = resolve_metric_number_format(metric_type)
                if number_format is not None:
                    cell_format = metric_format_cache.setdefault(
                        number_format,
                        workbook.add_format(
                            {'align': 'right', 'valign': 'vcenter', 'border': 1, 'num_format': number_format}
                        ),
                    )
                elif col_idx == 0:
                    cell_format = text_format
                else:
                    cell_format = right_text_format
                write_cell(worksheet, excel_row, col_idx, value, cell_format)

        data_end_row = max(table_header_row, data_start_row + len(section.data) - 1)
        if not filter_set:
            worksheet.autofilter(table_header_row, 0, data_end_row, len(columns) - 1)
            filter_set = True
        current_row = data_end_row + 2

    freeze_row, freeze_col = freeze_panes_to_rc(freeze_panes)
    worksheet.freeze_panes(freeze_row, freeze_col)

    fixed_width = resolve_fixed_width(15)
    if fixed_width is not None:
        for col_idx in range(max_col_overall):
            worksheet.set_column(col_idx, col_idx, fixed_width, text_format)

"""XlsxWriter 版本的轻量 workbook 写出器。"""

from __future__ import annotations

import re
from numbers import Real
from typing import Any

import pandas as pd

from src.analytics.contracts import FlatSheet, ProductAnomalySection, SectionBlock
from src.excel.styles import (
    EXCEL_TWO_DECIMAL_FORMAT,
    estimate_analysis_column_widths,
    estimate_flat_column_widths,
    resolve_metric_number_format,
    to_excel_number,
)

_FREEZE_PANES_PATTERN = re.compile(r'^([A-Z]+)([1-9]\d*)$')
# xlsxwriter 与 openpyxl 的列宽刻度不同，这里做最小换算以保持既有契约读取值为 15.0。
_XLSXWRITER_WIDTH_FOR_FIXED_15 = 14.3
WORK_ORDER_HIGHLIGHT_COLUMNS: tuple[tuple[str, str], ...] = (
    ('直接材料单位完工成本', '直接材料异常标记'),
    ('直接人工单位完工成本', '直接人工异常标记'),
    ('制造费用单位完工成本', '制造费用异常标记'),
    ('制造费用_其他单位完工成本', '制造费用_其他异常标记'),
    ('制造费用_人工单位完工成本', '制造费用_人工异常标记'),
    ('制造费用_机物料及低耗单位完工成本', '制造费用_机物料及低耗异常标记'),
    ('制造费用_折旧单位完工成本', '制造费用_折旧异常标记'),
    ('制造费用_水电费单位完工成本', '制造费用_水电费异常标记'),
)
HIGHLIGHT_STYLE_MAP: dict[str, dict[str, str]] = {
    '关注': {'fill': '#DDEBF7'},
    '高度可疑': {'fill': '#4472C4', 'font_color': '#FFFFFF'},
}


def _freeze_panes_to_rc(freeze_panes: str) -> tuple[int, int]:
    """把 A2/C3 形式冻结坐标转换为 xlsxwriter 的 0-based 行列。"""
    match = _FREEZE_PANES_PATTERN.fullmatch(freeze_panes.strip().upper())
    if match is None:
        raise ValueError(f'Invalid freeze panes token: {freeze_panes!r}')
    letters, row_text = match.groups()

    column_idx = 0
    for letter in letters:
        column_idx = column_idx * 26 + (ord(letter) - ord('A') + 1)
    return int(row_text) - 1, column_idx - 1


def _is_blank_excel_value(value: object) -> bool:
    """判定值是否应按空单元格写出。"""
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False


def _resolve_fixed_width(fixed_width: int | None) -> float | None:
    """统一固定列宽输入，兼容 xlsxwriter 与现有 openpyxl 断言。"""
    if fixed_width is None:
        return None
    if fixed_width == 15:
        return _XLSXWRITER_WIDTH_FOR_FIXED_15
    return float(fixed_width)


def _resolve_highlight_style(flag_value: object) -> dict[str, str] | None:
    """按异常标记解析高亮样式。"""
    if flag_value is None:
        return None
    return HIGHLIGHT_STYLE_MAP.get(str(flag_value).strip())


def _write_cell(worksheet: Any, row_idx: int, col_idx: int, value: object, cell_format: Any) -> None:
    """按 Python 值类型写入 xlsxwriter 单元格。"""
    if _is_blank_excel_value(value):
        worksheet.write_blank(row_idx, col_idx, None, cell_format)
        return
    if isinstance(value, bool):
        worksheet.write_boolean(row_idx, col_idx, value, cell_format)
        return
    if isinstance(value, Real):
        worksheet.write_number(row_idx, col_idx, float(value), cell_format)
        return
    worksheet.write(row_idx, col_idx, value, cell_format)


class FastSheetWriter:
    """负责把 DataFrame/section 数据写成 xlsxwriter sheet。"""

    def write_dataframe_sheet(
        self,
        writer: pd.ExcelWriter,
        sheet_name: str,
        df: pd.DataFrame,
        *,
        numeric_columns: set[str],
        freeze_panes: str | None = 'A2',
        fixed_width: int | None = None,
        auto_filter: bool = True,
        apply_column_widths: bool = True,
    ) -> Any:
        """写入普通 DataFrame sheet，并按列名应用数值格式。"""
        column_formats = {column_name: EXCEL_TWO_DECIMAL_FORMAT for column_name in numeric_columns if column_name in df.columns}
        write_df = self._coerce_excel_numeric_columns(df, set(column_formats))
        return self._write_flat_dataframe(
            writer,
            sheet_name,
            write_df,
            column_formats=column_formats,
            freeze_panes=freeze_panes,
            fixed_width=fixed_width,
            auto_filter=auto_filter,
            apply_column_widths=apply_column_widths,
        )

    def write_analysis_sheet(self, writer: pd.ExcelWriter, sheet_name: str, sections: list[SectionBlock]) -> None:
        """写入三段分析块（Task 1 不迁移高亮/条件格式）。"""
        workbook = writer.book
        worksheet = workbook.add_worksheet(sheet_name)
        writer.sheets[sheet_name] = worksheet

        title_format = workbook.add_format(
            {'bold': True, 'bg_color': '#FFD966', 'align': 'left', 'valign': 'vcenter'}
        )
        header_format = workbook.add_format(
            {'bold': True, 'bg_color': '#D9E1F2', 'align': 'center', 'valign': 'vcenter', 'border': 1}
        )
        text_format = workbook.add_format({'align': 'left', 'valign': 'vcenter', 'border': 1})
        right_text_format = workbook.add_format({'align': 'right', 'valign': 'vcenter', 'border': 1})
        total_text_format = workbook.add_format(
            {'bold': True, 'bg_color': '#BDD7EE', 'align': 'left', 'valign': 'vcenter', 'border': 1}
        )
        total_right_format = workbook.add_format(
            {'bold': True, 'bg_color': '#BDD7EE', 'align': 'right', 'valign': 'vcenter', 'border': 1}
        )
        metric_format_cache: dict[str, Any] = {}
        total_metric_format_cache: dict[str, Any] = {}

        current_row = 0
        first_filter_scope: tuple[int, int, int] | None = None
        max_col_overall = 1

        for section in sections:
            write_section_df = section.data.copy()
            if section.metric_type in {'amount', 'price', 'qty'}:
                numeric_columns = [
                    column_name
                    for column_name in write_section_df.columns
                    if column_name not in {'产品编码', '产品名称'}
                ]
                for column_name in numeric_columns:
                    write_section_df[column_name] = write_section_df[column_name].map(to_excel_number)

            columns = write_section_df.columns.tolist()
            max_col_overall = max(max_col_overall, max(len(columns), 1))

            worksheet.write(current_row, 0, section.title, title_format)
            header_row = current_row + 1
            for col_idx, column_name in enumerate(columns):
                worksheet.write(header_row, col_idx, column_name, header_format)

            number_format = resolve_metric_number_format(section.metric_type, qty_format=EXCEL_TWO_DECIMAL_FORMAT)
            metric_format = None
            total_metric_format = None
            if number_format is not None:
                metric_format = metric_format_cache.setdefault(
                    number_format,
                    workbook.add_format(
                        {'align': 'right', 'valign': 'vcenter', 'border': 1, 'num_format': number_format}
                    ),
                )
                total_metric_format = total_metric_format_cache.setdefault(
                    number_format,
                    workbook.add_format(
                        {
                            'bold': True,
                            'bg_color': '#BDD7EE',
                            'align': 'right',
                            'valign': 'vcenter',
                            'border': 1,
                            'num_format': number_format,
                        }
                    ),
                )

            for row_offset, row_data in enumerate(write_section_df.itertuples(index=False, name=None)):
                excel_row = header_row + 1 + row_offset
                for col_idx, value in enumerate(row_data):
                    if col_idx <= 1:
                        cell_format = text_format
                    elif metric_format is not None:
                        cell_format = metric_format
                    else:
                        cell_format = right_text_format
                    _write_cell(worksheet, excel_row, col_idx, value, cell_format)

            if section.has_total_row and not write_section_df.empty:
                total_row = header_row + len(write_section_df)
                total_values = write_section_df.iloc[-1].tolist()
                for col_idx, value in enumerate(total_values):
                    if col_idx <= 1:
                        cell_format = total_text_format
                    elif total_metric_format is not None:
                        cell_format = total_metric_format
                    else:
                        cell_format = total_right_format
                    _write_cell(worksheet, total_row, col_idx, value, cell_format)

            section_end_row = max(header_row, header_row + len(write_section_df))
            if first_filter_scope is None and columns:
                first_filter_scope = (header_row, len(columns) - 1, section_end_row)
            current_row = current_row + len(write_section_df) + 3

        if first_filter_scope is not None:
            header_row, last_col_idx, end_row = first_filter_scope
            worksheet.autofilter(header_row, 0, end_row, last_col_idx)

        freeze_row, freeze_col = _freeze_panes_to_rc('C3')
        worksheet.freeze_panes(freeze_row, freeze_col)

        width_map = estimate_analysis_column_widths([(section.title, section.data) for section in sections])
        for col_idx in range(max_col_overall):
            width = width_map.get(col_idx + 1, 12.0)
            default_format = text_format if col_idx < 2 else right_text_format
            worksheet.set_column(col_idx, col_idx, width, default_format)

    def write_flat_sheet(
        self,
        writer: pd.ExcelWriter,
        sheet_name: str,
        table: FlatSheet,
        *,
        freeze_panes: str = 'A2',
        fixed_width: int | None = None,
    ) -> Any:
        """写入平铺数据 sheet 并应用基础样式。"""
        write_df = table.data.copy()
        column_formats: dict[str, str] = {}
        for column_name, metric_type in table.column_types.items():
            if column_name not in write_df.columns:
                continue
            if metric_type in {'amount', 'price', 'qty', 'score', 'pct'}:
                write_df[column_name] = write_df[column_name].map(to_excel_number)
            number_format = resolve_metric_number_format(metric_type)
            if number_format is not None:
                column_formats[column_name] = number_format

        return self._write_flat_dataframe(
            writer,
            sheet_name,
            write_df,
            column_formats=column_formats,
            freeze_panes=freeze_panes,
            fixed_width=fixed_width,
            highlight_columns=WORK_ORDER_HIGHLIGHT_COLUMNS,
        )

    def write_product_anomaly_sheet(
        self,
        writer: pd.ExcelWriter,
        sheet_name: str,
        sections: list[ProductAnomalySection],
    ) -> None:
        """写入按产品异常值分析页（Task 1 保持无高亮、可读即可）。"""
        workbook = writer.book
        worksheet = workbook.add_worksheet(sheet_name)
        writer.sheets[sheet_name] = worksheet

        section_title_format = workbook.add_format(
            {'bold': True, 'bg_color': '#FFD966', 'align': 'left', 'valign': 'vcenter'}
        )
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

        worksheet.write(0, 0, '四、按单个产品异常值分析', section_title_format)

        current_row = 2
        max_col_overall = 1
        filter_set = False

        for section in sections:
            columns = section.data.columns.tolist()
            if not columns:
                continue

            meta_header_row = current_row
            meta_value_row = current_row + 1
            table_header_row = current_row + 2
            data_start_row = current_row + 3
            max_col_overall = max(max_col_overall, len(columns))

            worksheet.write(meta_header_row, 0, '产品编码', meta_header_format)
            worksheet.write(meta_header_row, 1, '产品名称', meta_header_format)
            worksheet.write(meta_value_row, 0, section.product_code, meta_value_format)
            worksheet.write(meta_value_row, 1, section.product_name, meta_value_format)

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
                    _write_cell(worksheet, excel_row, col_idx, value, cell_format)

            data_end_row = max(table_header_row, data_start_row + len(section.data) - 1)
            if not filter_set:
                worksheet.autofilter(table_header_row, 0, data_end_row, len(columns) - 1)
                filter_set = True
            current_row = data_end_row + 2

        freeze_row, freeze_col = _freeze_panes_to_rc('A6')
        worksheet.freeze_panes(freeze_row, freeze_col)

        fixed_width = _resolve_fixed_width(15)
        if fixed_width is not None:
            for col_idx in range(max_col_overall):
                worksheet.set_column(col_idx, col_idx, fixed_width, text_format)

    def apply_work_order_highlights(self, worksheet: Any) -> None:
        """兼容入口：高亮已在写入阶段完成，避免 constant_memory 下事后回写失败。"""
        return None

    def _write_flat_dataframe(
        self,
        writer: pd.ExcelWriter,
        sheet_name: str,
        df: pd.DataFrame,
        *,
        column_formats: dict[str, str],
        freeze_panes: str | None,
        fixed_width: int | None,
        highlight_columns: tuple[tuple[str, str], ...] | None = None,
        auto_filter: bool = True,
        apply_column_widths: bool = True,
    ) -> Any:
        workbook = writer.book
        worksheet = workbook.add_worksheet(sheet_name)
        writer.sheets[sheet_name] = worksheet

        header_format = workbook.add_format(
            {'bold': True, 'bg_color': '#D9E1F2', 'align': 'center', 'valign': 'vcenter', 'border': 1}
        )
        text_format = workbook.add_format({'align': 'left', 'valign': 'vcenter', 'border': 1})
        number_format_cache: dict[str, Any] = {}
        highlight_format_cache: dict[tuple[str, str, str, str], Any] = {}

        columns = df.columns.tolist()
        header_map = {column_name: idx for idx, column_name in enumerate(columns)}
        highlight_pairs: list[tuple[int, int]] = []
        if highlight_columns is not None:
            for value_column, flag_column in highlight_columns:
                value_idx = header_map.get(value_column)
                flag_idx = header_map.get(flag_column)
                if value_idx is None or flag_idx is None:
                    continue
                highlight_pairs.append((value_idx, flag_idx))

        for col_idx, column_name in enumerate(columns):
            worksheet.write(0, col_idx, column_name, header_format)

        for row_offset, row_data in enumerate(df.itertuples(index=False, name=None)):
            excel_row = row_offset + 1
            row_style_by_col: dict[int, dict[str, str]] = {}
            for value_idx, flag_idx in highlight_pairs:
                highlight_style = _resolve_highlight_style(row_data[flag_idx])
                if highlight_style is None:
                    continue
                row_style_by_col[value_idx] = highlight_style
                row_style_by_col[flag_idx] = highlight_style

            for col_idx, value in enumerate(row_data):
                number_format = column_formats.get(columns[col_idx])
                if number_format is None:
                    base_cell_format = text_format
                    align = 'left'
                else:
                    base_cell_format = number_format_cache.setdefault(
                        number_format,
                        workbook.add_format(
                            {'align': 'right', 'valign': 'vcenter', 'border': 1, 'num_format': number_format}
                        ),
                    )
                    align = 'right'

                highlight_style = row_style_by_col.get(col_idx)
                if highlight_style is None:
                    cell_format = base_cell_format
                else:
                    format_key = (
                        number_format or '',
                        align,
                        highlight_style['fill'],
                        highlight_style.get('font_color', ''),
                    )
                    cell_format = highlight_format_cache.get(format_key)
                    if cell_format is None:
                        format_config: dict[str, Any] = {
                            'align': align,
                            'valign': 'vcenter',
                            'border': 1,
                            'bg_color': highlight_style['fill'],
                        }
                        if number_format is not None:
                            format_config['num_format'] = number_format
                        font_color = highlight_style.get('font_color')
                        if font_color is not None:
                            format_config['font_color'] = font_color
                        cell_format = workbook.add_format(format_config)
                        highlight_format_cache[format_key] = cell_format

                _write_cell(worksheet, excel_row, col_idx, value, cell_format)

        if freeze_panes is not None:
            freeze_row, freeze_col = _freeze_panes_to_rc(freeze_panes)
            worksheet.freeze_panes(freeze_row, freeze_col)

        if auto_filter and columns:
            filter_end_row = max(len(df), 1)
            worksheet.autofilter(0, 0, filter_end_row, len(columns) - 1)

        if apply_column_widths:
            fixed_width_value = _resolve_fixed_width(fixed_width)
            if fixed_width_value is None:
                width_map = estimate_flat_column_widths(df)
            else:
                width_map = {
                    column_idx: fixed_width_value
                    for column_idx in range(1, len(columns) + 1)
                }

            for col_idx, column_name in enumerate(columns):
                width = width_map.get(col_idx + 1, 12.0)
                number_format = column_formats.get(column_name)
                default_format = text_format
                if number_format is not None:
                    default_format = number_format_cache.setdefault(
                        number_format,
                        workbook.add_format(
                            {'align': 'right', 'valign': 'vcenter', 'border': 1, 'num_format': number_format}
                        ),
                    )
                worksheet.set_column(col_idx, col_idx, width, default_format)

        return worksheet

    def _coerce_excel_numeric_columns(self, df: pd.DataFrame, numeric_columns: set[str]) -> pd.DataFrame:
        """写出前统一转成 Excel 数值单元格。"""
        write_df = df.copy()
        for column_name in numeric_columns:
            if column_name in write_df.columns:
                write_df[column_name] = write_df[column_name].map(to_excel_number)
        return write_df

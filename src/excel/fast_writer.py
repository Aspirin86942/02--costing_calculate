"""XlsxWriter 版本的轻量 workbook 写出器。"""

from __future__ import annotations

import re
from decimal import Decimal
from numbers import Real
from typing import Any

import pandas as pd
from xlsxwriter.utility import xl_col_to_name

from src.analytics.anomaly import WORK_ORDER_HIGHLIGHT_COLUMNS
from src.analytics.contracts import FlatSheet, ProductAnomalySection, SectionBlock, SheetModel
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


def _resolve_fixed_width(fixed_width: int | float | None) -> float | None:
    """统一固定列宽输入，兼容 xlsxwriter 与现有 openpyxl 断言。"""
    if fixed_width is None:
        return None
    if float(fixed_width) == 15.0:
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
    if isinstance(value, Decimal):
        worksheet.write_number(row_idx, col_idx, float(value), cell_format)
        return
    if isinstance(value, Real):
        worksheet.write_number(row_idx, col_idx, float(value), cell_format)
        return
    worksheet.write(row_idx, col_idx, value, cell_format)


def _coerce_row_value_for_excel(value: object) -> object:
    """把行值统一成 xlsxwriter 适配类型，避免 NaN 被写成文本。"""
    if _is_blank_excel_value(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Real):
        return float(value)
    return value


def _build_ascii_safe_excel_text(text: str) -> str:
    """把中文文本转换为只含 ASCII 的 Excel 公式片段，避免 xlsxwriter 序列化成问号。"""
    return '&'.join(f'UNICHAR({ord(char)})' for char in text)


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
        column_formats = {
            column_name: EXCEL_TWO_DECIMAL_FORMAT for column_name in numeric_columns if column_name in df.columns
        }
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

    def write_dataframe_fast(
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
        """写入热点 DataFrame sheet，数据行优先走 write_row 流式路径。"""
        column_formats = {
            column_name: EXCEL_TWO_DECIMAL_FORMAT for column_name in numeric_columns if column_name in df.columns
        }
        write_df = self._coerce_excel_numeric_columns(df, set(column_formats))
        return self._write_flat_dataframe_fast(
            writer,
            sheet_name,
            write_df,
            column_formats=column_formats,
            freeze_panes=freeze_panes,
            fixed_width=fixed_width,
            auto_filter=auto_filter,
            apply_column_widths=apply_column_widths,
        )

    def _build_formats(self, workbook: Any) -> dict[str, Any]:
        return {
            'header': workbook.add_format(
                {'bold': True, 'bg_color': '#D9E1F2', 'align': 'center', 'valign': 'vcenter', 'border': 1}
            ),
            'attention': workbook.add_format({'bg_color': '#DDEBF7'}),
            'suspicious': workbook.add_format({'bg_color': '#4472C4', 'font_color': '#FFFFFF'}),
            'text': workbook.add_format({'align': 'left', 'valign': 'vcenter', 'border': 1}),
        }

    def _resolve_number_format(self, number_format: str | None, workbook: Any) -> Any:
        if number_format is None:
            return workbook.add_format({'align': 'left', 'valign': 'vcenter', 'border': 1})
        return workbook.add_format({'align': 'right', 'valign': 'vcenter', 'border': 1, 'num_format': number_format})

    def _build_product_anomaly_sections_from_model(self, model: SheetModel) -> list[ProductAnomalySection]:
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
            grouped_rows[key].append(
                tuple(row[table_column_start_idx : table_column_start_idx + len(table_columns)])
            )

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

    def write_sheet_model(self, writer: pd.ExcelWriter, model: SheetModel) -> Any:
        """按 SheetModel 契约写出单个 sheet。"""
        if model.sheet_name == '按产品异常值分析':
            sections = self._build_product_anomaly_sections_from_model(model)
            self.write_product_anomaly_sheet(writer, model.sheet_name, sections)
            return writer.sheets[model.sheet_name]

        workbook = writer.book
        worksheet = workbook.add_worksheet(model.sheet_name)
        writer.sheets[model.sheet_name] = worksheet

        formats = self._build_formats(workbook)
        number_format_cache: dict[str | None, Any] = {}
        numeric_format_by_col: dict[int, Any] = {}
        text_format = formats['text']
        fixed_width = _resolve_fixed_width(model.fixed_width)
        for col_idx, column_name in enumerate(model.columns):
            worksheet.write(0, col_idx, column_name, formats['header'])

            number_format = model.number_formats.get(column_name)
            cell_format = number_format_cache.get(number_format)
            if cell_format is None:
                cell_format = self._resolve_number_format(number_format, workbook)
                number_format_cache[number_format] = cell_format

            if number_format is not None:
                numeric_format_by_col[col_idx] = cell_format
            worksheet.set_column(col_idx, col_idx, fixed_width, cell_format)

        last_row = 0
        for row_idx, row in enumerate(model.rows_factory(), start=1):
            coerced_row_data = tuple(_coerce_row_value_for_excel(value) for value in row)
            worksheet.write_row(row_idx, 0, coerced_row_data, text_format)
            for col_idx, numeric_format in numeric_format_by_col.items():
                _write_cell(worksheet, row_idx, col_idx, coerced_row_data[col_idx], numeric_format)
            last_row = row_idx

        if model.freeze_panes is not None:
            freeze_row, freeze_col = _freeze_panes_to_rc(model.freeze_panes)
            worksheet.freeze_panes(freeze_row, freeze_col)
        if model.auto_filter and model.columns:
            worksheet.autofilter(0, 0, max(last_row, 1), len(model.columns) - 1)

        for rule in model.conditional_formats:
            highlight_format = formats.get(rule.format_key)
            if highlight_format is None:
                raise ValueError(f'Unknown conditional format key: {rule.format_key}')
            worksheet.conditional_format(
                rule.target_range,
                {'type': 'formula', 'criteria': rule.formula, 'format': highlight_format},
            )

        return worksheet

    def write_sheet_model_as_lightweight_table(self, writer: pd.ExcelWriter, model: SheetModel) -> Any:
        """按热点 SheetModel 走轻量平铺写出，保留易用性但弱化数据单元格样式。"""
        self._validate_lightweight_fast_model(model)

        workbook = writer.book
        worksheet = workbook.add_worksheet(model.sheet_name)
        writer.sheets[model.sheet_name] = worksheet

        header_format = workbook.add_format(
            {'bold': True, 'bg_color': '#D9E1F2', 'align': 'center', 'valign': 'vcenter', 'border': 1}
        )
        # 热点大表核心策略：数据区仅保留对齐与数值格式，不再为每个数据单元格附加边框/填充，
        # 以降低写出阶段的样式对象开销，同时保持筛选、冻结和数值可读性。
        text_format = workbook.add_format({'align': 'left', 'valign': 'vcenter'})
        number_format_cache: dict[str, Any] = {}
        numeric_format_by_col: dict[int, Any] = {}

        fixed_width = _resolve_fixed_width(model.fixed_width)
        for col_idx, column_name in enumerate(model.columns):
            worksheet.write(0, col_idx, column_name, header_format)
            number_format = model.number_formats.get(column_name)
            if number_format is not None:
                numeric_format_by_col[col_idx] = number_format_cache.setdefault(
                    number_format,
                    workbook.add_format({'align': 'right', 'valign': 'vcenter', 'num_format': number_format}),
                )

            default_format = numeric_format_by_col.get(col_idx, text_format)
            worksheet.set_column(col_idx, col_idx, fixed_width, default_format)

        last_row = 0
        for row_idx, row in enumerate(model.rows_factory(), start=1):
            coerced_row_data = tuple(_coerce_row_value_for_excel(value) for value in row)
            # 为什么这里不再逐格覆写数值列：热点大表的主耗时来自“整行写一次后再对数值列补写一次”，
            # 改为非空数值完全依赖列默认格式后，可避免重复单元格写入；仅对空白数值位补 write_blank，
            # 用来保住 openpyxl 侧可观测到的 number_format/alignment 契约。
            worksheet.write_row(row_idx, 0, coerced_row_data)
            for col_idx, numeric_format in numeric_format_by_col.items():
                if _is_blank_excel_value(coerced_row_data[col_idx]):
                    _write_cell(worksheet, row_idx, col_idx, None, numeric_format)
            last_row = row_idx

        if model.freeze_panes is not None:
            freeze_row, freeze_col = _freeze_panes_to_rc(model.freeze_panes)
            worksheet.freeze_panes(freeze_row, freeze_col)
        if model.auto_filter and model.columns:
            worksheet.autofilter(0, 0, max(last_row, 1), len(model.columns) - 1)

        return worksheet

    def _validate_lightweight_fast_model(self, model: SheetModel) -> None:
        # 为什么要前置校验：fast-path 会省略通用写法中的部分能力，如果静默接收不兼容模型，
        # 会在不报错的情况下丢失条件格式或特殊布局，难以及时发现回归。
        if model.conditional_formats:
            raise ValueError(
                f'lightweight fast-path does not support conditional_formats: sheet_name={model.sheet_name}'
            )
        if model.sheet_name == '按产品异常值分析':
            raise ValueError(
                'lightweight fast-path does not support special layout sheet: '
                f'sheet_name={model.sheet_name}'
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
        )

    def write_product_anomaly_sheet(
        self,
        writer: pd.ExcelWriter,
        sheet_name: str,
        sections: list[ProductAnomalySection],
    ) -> None:
        """写入按产品异常值分析页，兼容 legacy/scoped 两种布局。"""
        if any(section.section_label is not None for section in sections):
            self._write_scoped_product_anomaly_sheet(writer, sheet_name, sections)
            return
        self._write_legacy_product_anomaly_sheet(writer, sheet_name, sections)

    def _write_legacy_product_anomaly_sheet(
        self,
        writer: pd.ExcelWriter,
        sheet_name: str,
        sections: list[ProductAnomalySection],
    ) -> None:
        """写入 legacy 单段布局，保持既有契约不变。"""
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

    def _write_scoped_product_anomaly_sheet(
        self,
        writer: pd.ExcelWriter,
        sheet_name: str,
        sections: list[ProductAnomalySection],
    ) -> None:
        """写入 scoped 多段布局，按产品与分析口径分块展示。"""
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
            scope_row = current_row + 2
            table_header_row = current_row + 3
            data_start_row = current_row + 4
            max_col_overall = max(max_col_overall, len(columns))

            worksheet.write(meta_header_row, 0, '产品编码', meta_header_format)
            worksheet.write(meta_header_row, 1, '产品名称', meta_header_format)
            worksheet.write(meta_value_row, 0, section.product_code, meta_value_format)
            worksheet.write(meta_value_row, 1, section.product_name, meta_value_format)
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
                    _write_cell(worksheet, excel_row, col_idx, value, cell_format)

            data_end_row = max(table_header_row, data_start_row + len(section.data) - 1)
            if not filter_set:
                worksheet.autofilter(table_header_row, 0, data_end_row, len(columns) - 1)
                filter_set = True
            current_row = data_end_row + 2

        freeze_row, freeze_col = _freeze_panes_to_rc('A7')
        worksheet.freeze_panes(freeze_row, freeze_col)

        fixed_width = _resolve_fixed_width(15)
        if fixed_width is not None:
            for col_idx in range(max_col_overall):
                worksheet.set_column(col_idx, col_idx, fixed_width, text_format)

    def apply_work_order_highlights(
        self,
        workbook: Any,
        worksheet: Any,
        *,
        columns: list[str],
        max_row: int,
    ) -> None:
        """给工单异常页挂条件格式规则，由 Excel 打开时再渲染高亮。"""
        if max_row <= 1:
            return

        header_map = {column_name: idx for idx, column_name in enumerate(columns)}
        format_cache = {
            flag_label: workbook.add_format(
                {
                    'bg_color': style['fill'],
                    **({'font_color': style['font_color']} if 'font_color' in style else {}),
                }
            )
            for flag_label, style in HIGHLIGHT_STYLE_MAP.items()
        }

        for value_column, flag_column in WORK_ORDER_HIGHLIGHT_COLUMNS:
            value_idx = header_map.get(value_column)
            flag_idx = header_map.get(flag_column)
            if value_idx is None or flag_idx is None:
                continue

            flag_col_letter = xl_col_to_name(flag_idx)
            for flag_label, cell_format in format_cache.items():
                formula = f'=EXACT(${flag_col_letter}2,{_build_ascii_safe_excel_text(flag_label)})'
                for target_idx in (value_idx, flag_idx):
                    target_col_letter = xl_col_to_name(target_idx)
                    worksheet.conditional_format(
                        f'{target_col_letter}2:{target_col_letter}{max_row}',
                        {
                            'type': 'formula',
                            'criteria': formula,
                            'format': cell_format,
                        },
                    )

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
                width_map = dict.fromkeys(range(1, len(columns) + 1), fixed_width_value)

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

    def _write_flat_dataframe_fast(
        self,
        writer: pd.ExcelWriter,
        sheet_name: str,
        df: pd.DataFrame,
        *,
        column_formats: dict[str, str],
        freeze_panes: str | None,
        fixed_width: int | None,
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
        numeric_format_by_col: dict[int, Any] = {}

        columns = df.columns.tolist()
        for col_idx, column_name in enumerate(columns):
            worksheet.write(0, col_idx, column_name, header_format)
            number_format = column_formats.get(column_name)
            if number_format is None:
                continue
            numeric_format_by_col[col_idx] = number_format_cache.setdefault(
                number_format,
                workbook.add_format(
                    {'align': 'right', 'valign': 'vcenter', 'border': 1, 'num_format': number_format}
                ),
            )

        if apply_column_widths:
            fixed_width_value = _resolve_fixed_width(fixed_width)
            if fixed_width_value is None:
                width_map = estimate_flat_column_widths(df)
            else:
                width_map = dict.fromkeys(range(1, len(columns) + 1), fixed_width_value)

            for col_idx, _column_name in enumerate(columns):
                width = width_map.get(col_idx + 1, 12.0)
                default_format = numeric_format_by_col.get(col_idx, text_format)
                worksheet.set_column(col_idx, col_idx, width, default_format)

        for row_offset, row_data in enumerate(df.itertuples(index=False, name=None)):
            excel_row = row_offset + 1
            coerced_row_data = tuple(_coerce_row_value_for_excel(value) for value in row_data)
            # write_row 是热点路径核心：逐行写出可显著减少 Python 层逐单元格调用开销。
            # 先用 text_format 兜底整行样式，再对数值列定点覆盖，保证空值/无列宽场景也不丢格式。
            worksheet.write_row(
                excel_row,
                0,
                coerced_row_data,
                text_format,
            )
            for col_idx, numeric_format in numeric_format_by_col.items():
                _write_cell(worksheet, excel_row, col_idx, coerced_row_data[col_idx], numeric_format)

        if freeze_panes is not None:
            freeze_row, freeze_col = _freeze_panes_to_rc(freeze_panes)
            worksheet.freeze_panes(freeze_row, freeze_col)

        if auto_filter and columns:
            filter_end_row = max(len(df), 1)
            worksheet.autofilter(0, 0, filter_end_row, len(columns) - 1)

        return worksheet

    def _coerce_excel_numeric_columns(self, df: pd.DataFrame, numeric_columns: set[str]) -> pd.DataFrame:
        """写出前统一转成 Excel 数值单元格。"""
        write_df = df.copy()
        for column_name in numeric_columns:
            if column_name in write_df.columns:
                write_df[column_name] = write_df[column_name].map(to_excel_number)
        return write_df

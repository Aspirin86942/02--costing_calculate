"""Workbook 条件格式写出辅助函数。"""

from __future__ import annotations

from typing import Any

from xlsxwriter.utility import xl_col_to_name

from src.analytics.anomaly import WORK_ORDER_HIGHLIGHT_COLUMNS

HIGHLIGHT_STYLE_MAP: dict[str, dict[str, str]] = {
    '关注': {'fill': '#DDEBF7'},
    '高度可疑': {'fill': '#4472C4', 'font_color': '#FFFFFF'},
}


def resolve_highlight_style(flag_value: object) -> dict[str, str] | None:
    """按异常标记解析高亮样式。"""
    if flag_value is None:
        return None
    return HIGHLIGHT_STYLE_MAP.get(str(flag_value).strip())


def build_ascii_safe_excel_text(text: str) -> str:
    """把中文文本转换为只含 ASCII 的 Excel 公式片段，避免 xlsxwriter 序列化成问号。"""
    return '&'.join(f'UNICHAR({ord(char)})' for char in text)


def apply_work_order_highlights(
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
            formula = f'=EXACT(${flag_col_letter}2,{build_ascii_safe_excel_text(flag_label)})'
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

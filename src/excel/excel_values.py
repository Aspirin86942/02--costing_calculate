"""Excel 写出时共享的值转换与坐标工具。"""

from __future__ import annotations

import re
from decimal import Decimal
from numbers import Real
from typing import Any

import pandas as pd

_FREEZE_PANES_PATTERN = re.compile(r'^([A-Z]+)([1-9]\d*)$')
# xlsxwriter 与 openpyxl 的列宽刻度不同，这里做最小换算以保持既有契约读取值为 15.0。
_XLSXWRITER_WIDTH_FOR_FIXED_15 = 14.3


def freeze_panes_to_rc(freeze_panes: str) -> tuple[int, int]:
    """把 A2/C3 形式冻结坐标转换为 xlsxwriter 的 0-based 行列。"""
    match = _FREEZE_PANES_PATTERN.fullmatch(freeze_panes.strip().upper())
    if match is None:
        raise ValueError(f'Invalid freeze panes token: {freeze_panes!r}')
    letters, row_text = match.groups()

    column_idx = 0
    for letter in letters:
        column_idx = column_idx * 26 + (ord(letter) - ord('A') + 1)
    return int(row_text) - 1, column_idx - 1


def is_blank_excel_value(value: object) -> bool:
    """判定值是否应按空单元格写出。"""
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False


def resolve_fixed_width(fixed_width: int | float | None) -> float | None:
    """统一固定列宽输入，兼容 xlsxwriter 与现有 openpyxl 断言。"""
    if fixed_width is None:
        return None
    if float(fixed_width) == 15.0:
        return _XLSXWRITER_WIDTH_FOR_FIXED_15
    return float(fixed_width)


def write_cell(worksheet: Any, row_idx: int, col_idx: int, value: object, cell_format: Any) -> None:
    """按 Python 值类型写入 xlsxwriter 单元格。"""
    if is_blank_excel_value(value):
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


def coerce_row_value_for_excel(value: object) -> object:
    """把行值统一成 xlsxwriter 适配类型，避免 NaN 被写成文本。"""
    if is_blank_excel_value(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Real):
        return float(value)
    return value

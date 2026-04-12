"""原始成本 workbook 标准化阶段（Polars）。"""

from __future__ import annotations

import re

import polars as pl

from src.analytics.contracts import NormalizedCostFrame, RawWorkbookFrame
from src.etl.stages.cleaners import forward_fill_with_rules, remove_total_rows
from src.etl.stages.column_resolution import infer_rename_map


def _clean_header_token(value: object) -> str:
    token = '' if value is None else str(value).strip().replace(' ', '').replace('\n', '')
    if not token or token.lower().startswith('unnamed') or token in {'None', 'nan', 'NaN'}:
        return ''
    return token


def _flatten_headers(header_rows: tuple[tuple[str, ...], tuple[str, ...]]) -> tuple[str, ...]:
    """将双层表头压平为单层列名。"""
    top_row, second_row = header_rows
    width = max(len(top_row), len(second_row))
    flattened: list[str] = []

    for index in range(width):
        top = _clean_header_token(top_row[index] if index < len(top_row) else '')
        second = _clean_header_token(second_row[index] if index < len(second_row) else '')
        if top and second and top != second:
            merged = f'{top}{second}'
        else:
            merged = second or top
        flattened.append(merged or f'column_{index}')
    return tuple(flattened)


def _format_period_value(value: object) -> str | None:
    """将“年期”统一为 `YYYY年MM期` 形式。"""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return text
    matched = re.search(r'(\d{4})年\s*(\d{1,2})\s*期', text)
    if not matched:
        return text
    return f'{matched.group(1)}年{int(matched.group(2)):02d}期'


def build_normalized_cost_frame(
    raw: RawWorkbookFrame,
    *,
    child_material_column: str,
    cost_item_column: str,
    period_column: str,
    fill_columns: list[str],
    vendor_columns: list[str],
    cost_center_column: str,
    integrated_workshop_name: str,
) -> NormalizedCostFrame:
    """生成供后续拆表与事实构建使用的标准化成本表。"""
    flattened_headers = _flatten_headers(raw.header_rows)
    header_rename_map = {
        source: flattened_headers[index]
        for index, source in enumerate(raw.frame.columns)
        if index < len(flattened_headers)
    }
    normalized = raw.frame.rename(header_rename_map)

    inferred_rename_map = infer_rename_map(
        tuple(normalized.columns),
        child_material_column=child_material_column,
        cost_item_column=cost_item_column,
    )
    if inferred_rename_map:
        normalized = normalized.rename(inferred_rename_map)

    normalized = remove_total_rows(
        normalized,
        period_column=period_column,
        cost_center_column=cost_center_column,
    )
    normalized = forward_fill_with_rules(
        normalized,
        fill_columns=fill_columns,
        vendor_columns=vendor_columns,
        cost_center_column=cost_center_column,
        integrated_workshop_name=integrated_workshop_name,
    )

    if period_column in normalized.columns:
        normalized = normalized.with_columns(
            pl.col(period_column)
            .map_elements(_format_period_value, return_dtype=pl.String, skip_nulls=False)
            .alias('月份')
        )
        ordered_columns = normalized.columns.copy()
        if '月份' in ordered_columns:
            ordered_columns.remove('月份')
            period_index = ordered_columns.index(period_column) + 1
            ordered_columns.insert(period_index, '月份')
            normalized = normalized.select(ordered_columns)

    if cost_item_column in normalized.columns:
        cost_item_seed = pl.when(
            pl.col(cost_item_column).is_null()
            | pl.col(cost_item_column).cast(pl.String).str.strip_chars().eq('')
        ).then(pl.lit(None)).otherwise(pl.col(cost_item_column))
        normalized = normalized.with_columns(cost_item_seed.fill_null(strategy='forward').alias('Filled_成本项目'))
    else:
        normalized = normalized.with_columns(pl.lit(None).alias('Filled_成本项目'))

    return NormalizedCostFrame(
        frame=normalized,
        key_columns=('月份', '产品编码', '工单编号', '工单行号'),
    )

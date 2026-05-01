"""Polars 清洗阶段。"""

from __future__ import annotations

import polars as pl


def remove_total_rows(
    frame: pl.DataFrame,
    *,
    period_column: str,
    cost_center_column: str,
) -> pl.DataFrame:
    """删除包含“合计”的汇总行。"""
    columns_to_check = [column for column in (period_column, cost_center_column) if column in frame.columns]
    if not columns_to_check:
        return frame

    keep_expr: pl.Expr = pl.lit(True)
    for column in columns_to_check:
        keep_expr = keep_expr & ~(pl.col(column).cast(pl.String).str.contains('合计', literal=True).fill_null(False))
    return frame.filter(keep_expr)


def forward_fill_with_rules(
    frame: pl.DataFrame,
    *,
    fill_columns: list[str],
    vendor_columns: list[str],
    cost_center_column: str,
    integrated_workshop_name: str,
) -> pl.DataFrame:
    """按业务规则执行向下填充。"""
    columns_to_fill = [column for column in fill_columns if column in frame.columns]
    if not columns_to_fill:
        return frame

    actual_vendor_columns = [column for column in vendor_columns if column in columns_to_fill]
    normal_fill_columns = [column for column in columns_to_fill if column not in actual_vendor_columns]

    result = frame
    if normal_fill_columns:
        result = result.with_columns(
            [pl.col(column).fill_null(strategy='forward').alias(column) for column in normal_fill_columns]
        )

    if not actual_vendor_columns:
        return result

    if cost_center_column not in result.columns:
        return result.with_columns(
            [pl.col(column).fill_null(strategy='forward').alias(column) for column in actual_vendor_columns]
        )

    integrated_mask = (
        pl.col(cost_center_column).cast(pl.String).str.strip_chars().eq(integrated_workshop_name).fill_null(False)
    )
    vendor_exprs: list[pl.Expr] = []
    for column in actual_vendor_columns:
        vendor_seed = pl.when(integrated_mask).then(pl.lit(None)).otherwise(pl.col(column))
        # 集成车间行不作为供应商向下填充的数据源，避免跨工单错误继承供应商。
        vendor_exprs.append(
            pl.when(integrated_mask)
            .then(pl.col(column))
            .otherwise(vendor_seed.fill_null(strategy='forward'))
            .alias(column)
        )
    return result.with_columns(vendor_exprs)

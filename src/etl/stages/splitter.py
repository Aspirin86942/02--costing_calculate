"""明细/数量页拆分。"""

from __future__ import annotations

import pandas as pd

from src.analytics.contracts import SplitResult
from src.etl.utils import format_period_col


def split_detail_and_qty_sheets(
    df_raw: pd.DataFrame,
    df_filled: pd.DataFrame,
    *,
    child_material_column: str,
    cost_item_column: str,
    order_number_column: str,
    filled_cost_item_column: str,
    qty_columns: list[str],
    detail_columns: list[str],
) -> SplitResult:
    """按当前业务规则拆出成本明细与产品数量统计。

    为什么在这一层统一做字符串标准化：
    child material / cost item 的空值和空白字符判断直接决定行会落到哪张 sheet，
    先标准化一次再复用，可以避免不同分支对“空”的定义漂移。
    """
    material_tokens = df_raw[child_material_column].fillna('').astype(str).str.strip()
    has_material_mask = material_tokens.ne('')
    no_material_mask = material_tokens.eq('')

    if cost_item_column in df_raw.columns:
        cost_item_tokens = df_raw[cost_item_column].fillna('').astype(str).str.strip()
        no_cost_item_mask = cost_item_tokens.eq('')
        expense_mask = no_material_mask & cost_item_tokens.ne('') & cost_item_tokens.ne('直接材料')
    else:
        no_cost_item_mask = pd.Series([True] * len(df_raw), index=df_raw.index)
        expense_mask = pd.Series([False] * len(df_raw), index=df_raw.index)

    if order_number_column in df_filled.columns:
        has_order_mask = df_filled[order_number_column].notna()
    else:
        has_order_mask = pd.Series([True] * len(df_filled), index=df_filled.index)

    qty_df = df_filled[no_material_mask & no_cost_item_mask & has_order_mask].copy()
    qty_df = format_period_col(qty_df)
    actual_qty_columns = [column for column in qty_columns if column in qty_df.columns]
    if actual_qty_columns:
        qty_df = qty_df[actual_qty_columns]

    detail_df = df_filled[has_material_mask | expense_mask].copy()
    if filled_cost_item_column in detail_df.columns and cost_item_column in detail_df.columns:
        detail_df[cost_item_column] = detail_df[filled_cost_item_column]

    detail_df = format_period_col(detail_df)
    actual_detail_columns = [column for column in detail_columns if column in detail_df.columns]
    if actual_detail_columns:
        detail_df = detail_df[actual_detail_columns]

    return SplitResult(detail_df=detail_df, qty_df=qty_df)

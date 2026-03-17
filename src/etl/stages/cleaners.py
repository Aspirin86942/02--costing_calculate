"""DataFrame 清洗阶段。"""

from __future__ import annotations

import logging

import pandas as pd


def remove_total_rows(
    df: pd.DataFrame,
    *,
    period_column: str,
    cost_center_column: str,
    logger: logging.Logger,
) -> pd.DataFrame:
    """删除包含“合计”的汇总行。"""
    initial_rows = len(df)
    columns_to_check = [column for column in df.columns[:3] if column in [period_column, cost_center_column]]
    if not columns_to_check:
        return df

    keep_mask = pd.Series([True] * len(df), index=df.index)
    for column in columns_to_check:
        keep_mask &= ~df[column].astype(str).str.contains('合计', na=False)

    result = df[keep_mask].copy()
    removed_rows = initial_rows - len(result)
    if removed_rows > 0:
        logger.info('Removed total rows: %s', removed_rows)
    return result


def forward_fill_with_rules(
    df_raw: pd.DataFrame,
    *,
    fill_columns: list[str],
    vendor_columns: list[str],
    cost_center_column: str,
    integrated_workshop_name: str,
) -> pd.DataFrame:
    """按业务规则执行向下填充。"""
    df_filled = df_raw.copy()
    columns_to_fill = [column for column in df_filled.columns if column in fill_columns]
    if not columns_to_fill:
        return df_filled

    actual_vendor_columns = [column for column in vendor_columns if column in columns_to_fill]
    normal_fill_columns = [column for column in columns_to_fill if column not in actual_vendor_columns]
    if normal_fill_columns:
        df_filled[normal_fill_columns] = df_filled[normal_fill_columns].ffill()

    if not actual_vendor_columns:
        return df_filled
    if cost_center_column not in df_filled.columns:
        df_filled[actual_vendor_columns] = df_filled[actual_vendor_columns].ffill()
        return df_filled

    vendor_filled = df_filled[actual_vendor_columns].ffill()
    # 这里保留集成车间的原值，避免把上一个工单的供应商错误继承到当前行。
    integrated_mask = df_filled[cost_center_column].astype(str).str.strip().eq(integrated_workshop_name)
    df_filled.loc[~integrated_mask, actual_vendor_columns] = vendor_filled.loc[~integrated_mask, actual_vendor_columns]
    return df_filled

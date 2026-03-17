"""列名识别与标准化。"""

from __future__ import annotations

import logging

import pandas as pd

from src.analytics.contracts import ResolvedColumns


def infer_rename_map(
    df: pd.DataFrame,
    *,
    child_material_column: str,
    cost_item_column: str,
    logger: logging.Logger,
) -> dict[str, str]:
    """根据已有列头自动识别关键列。

    为什么这里集中做：
    后续清洗、拆表、分析都依赖同一组关键列，
    如果每个阶段各自兜底，会让契约难以维护且增加隐性分支。
    """
    rename_map: dict[str, str] = {}

    if child_material_column not in df.columns:
        candidates = [column for column in df.columns if '物料编码' in column or '子件' in column]
        if candidates:
            rename_map[candidates[0]] = child_material_column
            logger.info('Column rename: %s -> %s', candidates[0], child_material_column)

    if cost_item_column not in df.columns:
        candidates = [column for column in df.columns if '成本项目' in column or '费用项目' in column]
        if candidates:
            rename_map[candidates[0]] = cost_item_column
            logger.info('Column rename: %s -> %s', candidates[0], cost_item_column)

    return rename_map


def resolve_columns(
    df: pd.DataFrame,
    *,
    child_material_column: str,
    cost_item_column: str,
    logger: logging.Logger,
) -> ResolvedColumns:
    """生成本次处理要使用的列契约。"""
    rename_map = infer_rename_map(
        df,
        child_material_column=child_material_column,
        cost_item_column=cost_item_column,
        logger=logger,
    )
    return ResolvedColumns(
        child_material_column=child_material_column,
        cost_item_column=cost_item_column,
        rename_map=rename_map,
    )

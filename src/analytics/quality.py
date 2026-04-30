"""数据质量指标构建。"""

from __future__ import annotations

import pandas as pd
import polars as pl

from src.analytics.contracts import QualityMetric
from src.analytics.fact_builder import QTY_DM_AMOUNT


def build_quality_metrics(
    detail_df: pd.DataFrame | pl.DataFrame,
    qty_input_df: pd.DataFrame | pl.DataFrame,
    qty_sheet_df: pd.DataFrame | pl.DataFrame,
    analysis_df: pd.DataFrame | pl.DataFrame,
    filtered_invalid_qty_count: int,
    filtered_missing_total_amount_count: int,
    month_filter_empty_result: bool = False,
) -> tuple[QualityMetric, ...]:
    """构建数据质量指标对象，避免将质量结果固化为 workbook sheet。"""
    duplicate_count = _count_duplicate_keys(qty_sheet_df)
    dm_amount_null_rate = _null_rate(qty_sheet_df, QTY_DM_AMOUNT)
    analyzable_rate = _yes_rate(analysis_df, '是否可参与分析')
    null_rate_value = 'N/A' if month_filter_empty_result else f'{dm_amount_null_rate:.2%}'
    coverage_value = 'N/A' if month_filter_empty_result else f'{analyzable_rate:.2%}'
    na_description = '月份过滤后无数据，指标不适用'
    null_rate_description = na_description if month_filter_empty_result else '派生金额字段空值率'
    coverage_description = na_description if month_filter_empty_result else '仅统计白名单产品且通过基础校验的工单'

    return (
        QualityMetric(
            category='行数勾稽',
            metric='成本明细输入行数',
            value=str(_frame_len(detail_df)),
            description='原始拆分后的成本明细行数',
        ),
        QualityMetric(
            category='行数勾稽',
            metric='产品数量统计输入行数',
            value=str(_frame_len(qty_input_df)),
            description='拆分后的数量页原始行数',
        ),
        QualityMetric(
            category='行数勾稽',
            metric='产品数量统计输出行数',
            value=str(_frame_len(qty_sheet_df)),
            description='仅保留完工数量大于 0 且总完工成本非空的工单',
        ),
        QualityMetric(
            category='行数勾稽',
            metric='工单异常分析输出行数',
            value=str(_frame_len(analysis_df)),
            description='去重后的工单级分析行数',
        ),
        QualityMetric(
            category='行数勾稽',
            metric='因完工数量无效被过滤行数',
            value=str(filtered_invalid_qty_count),
            description='过滤条件包含完工数量为空、等于 0 或小于 0',
        ),
        QualityMetric(
            category='行数勾稽',
            metric='因总完工成本为空被过滤行数',
            value=str(filtered_missing_total_amount_count),
            description='仅统计完工数量有效但总完工成本为空的工单',
        ),
        QualityMetric(
            category='空值率',
            metric='直接材料金额缺失率',
            value=null_rate_value,
            description=null_rate_description,
        ),
        QualityMetric(
            category='唯一性检查',
            metric='工单主键重复行数',
            value=str(duplicate_count),
            description='键：月份+产品编码+工单编号+工单行',
        ),
        QualityMetric(
            category='分析覆盖率',
            metric='可参与分析占比',
            value=coverage_value,
            description=coverage_description,
        ),
    )


def _frame_len(df: pd.DataFrame | pl.DataFrame) -> int:
    if isinstance(df, pl.DataFrame):
        return df.height
    return len(df)


def _count_duplicate_keys(qty_sheet_df: pd.DataFrame | pl.DataFrame) -> int:
    if isinstance(qty_sheet_df, pd.DataFrame):
        if '_join_key' not in qty_sheet_df.columns:
            return 0
        return int(qty_sheet_df['_join_key'].duplicated(keep=False).sum())

    if '_join_key' not in qty_sheet_df.columns or qty_sheet_df.is_empty():
        return 0
    duplicate_rows = qty_sheet_df.group_by('_join_key').len().filter(pl.col('len') > 1)
    if duplicate_rows.is_empty():
        return 0
    return int(duplicate_rows.select(pl.col('len').sum()).item())


def _null_rate(df: pd.DataFrame | pl.DataFrame, column_name: str) -> float:
    if isinstance(df, pd.DataFrame):
        if column_name not in df.columns or df.empty:
            return 0.0
        return float(df[column_name].isna().mean())

    if column_name not in df.columns or df.is_empty():
        return 0.0
    value = df.select(pl.col(column_name).is_null().mean()).item()
    return float(value) if value is not None else 0.0


def _yes_rate(df: pd.DataFrame | pl.DataFrame, column_name: str) -> float:
    if isinstance(df, pd.DataFrame):
        if column_name not in df.columns or df.empty:
            return 0.0
        return float(df[column_name].eq('是').mean())

    if column_name not in df.columns or df.is_empty():
        return 0.0
    value = df.select((pl.col(column_name) == '是').mean()).item()
    return float(value) if value is not None else 0.0

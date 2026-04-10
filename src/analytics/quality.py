"""数据质量指标构建。"""

from __future__ import annotations

import pandas as pd

from src.analytics.contracts import QualityMetric
from src.analytics.fact_builder import QTY_DM_AMOUNT


def build_quality_metrics(
    detail_df: pd.DataFrame,
    qty_input_df: pd.DataFrame,
    qty_sheet_df: pd.DataFrame,
    analysis_df: pd.DataFrame,
    filtered_invalid_qty_count: int,
    filtered_missing_total_amount_count: int,
) -> tuple[QualityMetric, ...]:
    """构建数据质量指标对象，避免将质量结果固化为 workbook sheet。"""
    unique_key = qty_sheet_df['_join_key']
    duplicate_count = int(unique_key.duplicated(keep=False).sum())

    dm_amount_null_rate = qty_sheet_df[QTY_DM_AMOUNT].isna().mean() if QTY_DM_AMOUNT in qty_sheet_df.columns else 0.0
    analyzable_rate = (
        analysis_df['是否可参与分析'].eq('是').mean()
        if '是否可参与分析' in analysis_df.columns and not analysis_df.empty
        else 0.0
    )

    return (
        QualityMetric(
            category='行数勾稽',
            metric='成本明细输入行数',
            value=str(len(detail_df)),
            description='原始拆分后的成本明细行数',
        ),
        QualityMetric(
            category='行数勾稽',
            metric='产品数量统计输入行数',
            value=str(len(qty_input_df)),
            description='拆分后的数量页原始行数',
        ),
        QualityMetric(
            category='行数勾稽',
            metric='产品数量统计输出行数',
            value=str(len(qty_sheet_df)),
            description='仅保留完工数量大于 0 且总完工成本非空的工单',
        ),
        QualityMetric(
            category='行数勾稽',
            metric='工单异常分析输出行数',
            value=str(len(analysis_df)),
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
            value=f'{dm_amount_null_rate:.2%}',
            description='派生金额字段空值率',
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
            value=f'{analyzable_rate:.2%}',
            description='仅统计白名单产品且通过基础校验的工单',
        ),
    )

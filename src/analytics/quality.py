"""数据质量校验页。"""

from __future__ import annotations

import pandas as pd

from src.analytics.contracts import FlatSheet
from src.analytics.fact_builder import QTY_DM_AMOUNT


def build_quality_sheet(
    detail_df: pd.DataFrame,
    qty_input_df: pd.DataFrame,
    qty_sheet_df: pd.DataFrame,
    analysis_df: pd.DataFrame,
    filtered_invalid_qty_count: int,
    filtered_missing_total_amount_count: int,
) -> FlatSheet:
    unique_key = qty_sheet_df['_join_key']
    duplicate_count = int(unique_key.duplicated(keep=False).sum())

    dm_amount_null_rate = qty_sheet_df[QTY_DM_AMOUNT].isna().mean() if QTY_DM_AMOUNT in qty_sheet_df.columns else 0.0
    analyzable_rate = (
        analysis_df['是否可参与分析'].eq('是').mean()
        if '是否可参与分析' in analysis_df.columns and not analysis_df.empty
        else 0.0
    )

    quality_df = pd.DataFrame(
        [
            {
                '检查类别': '行数勾稽',
                '指标': '成本明细输入行数',
                '数值': str(len(detail_df)),
                '说明': '原始拆分后的成本明细行数',
            },
            {
                '检查类别': '行数勾稽',
                '指标': '产品数量统计输入行数',
                '数值': str(len(qty_input_df)),
                '说明': '拆分后的数量页原始行数',
            },
            {
                '检查类别': '行数勾稽',
                '指标': '产品数量统计输出行数',
                '数值': str(len(qty_sheet_df)),
                '说明': '仅保留完工数量大于 0 且总完工成本非空的工单',
            },
            {
                '检查类别': '行数勾稽',
                '指标': '工单异常分析输出行数',
                '数值': str(len(analysis_df)),
                '说明': '去重后的工单级分析行数',
            },
            {
                '检查类别': '行数勾稽',
                '指标': '因完工数量无效被过滤行数',
                '数值': str(filtered_invalid_qty_count),
                '说明': '过滤条件包含完工数量为空、等于 0 或小于 0',
            },
            {
                '检查类别': '行数勾稽',
                '指标': '因总完工成本为空被过滤行数',
                '数值': str(filtered_missing_total_amount_count),
                '说明': '仅统计完工数量有效但总完工成本为空的工单',
            },
            {
                '检查类别': '空值率',
                '指标': '直接材料金额缺失率',
                '数值': f'{dm_amount_null_rate:.2%}',
                '说明': '派生金额字段空值率',
            },
            {
                '检查类别': '唯一性检查',
                '指标': '工单主键重复行数',
                '数值': str(duplicate_count),
                '说明': '键=月份+产品编码+工单编号+工单行',
            },
            {
                '检查类别': '分析覆盖率',
                '指标': '可参与分析占比',
                '数值': f'{analyzable_rate:.2%}',
                '说明': '仅统计白名单产品且通过基础校验的工单',
            },
        ]
    )
    return FlatSheet(data=quality_df, column_types={'检查类别': 'text', '指标': 'text', '数值': 'text', '说明': 'text'})

"""工单异常分析页。"""

from __future__ import annotations

import math

import pandas as pd

from src.analytics.contracts import FlatSheet
from src.analytics.errors import append_reason
from src.analytics.fact_builder import ZERO

ANOMALY_METRICS = [
    ('total_unit_cost', '总单位完工成本', '总成本异常标记', '总成本异常'),
    ('dm_unit_cost', '直接材料单位完工成本', '直接材料异常标记', '材料异常'),
    ('dl_unit_cost', '直接人工单位完工成本', '直接人工异常标记', '人工异常'),
    ('moh_unit_cost', '制造费用单位完工成本', '制造费用异常标记', '制造费用异常'),
    ('moh_other_unit_cost', '制造费用_其他单位完工成本', '制造费用_其他异常标记', '其他异常'),
    ('moh_labor_unit_cost', '制造费用_人工单位完工成本', '制造费用_人工异常标记', '制造费用人工异常'),
    (
        'moh_consumables_unit_cost',
        '制造费用_机物料及低耗单位完工成本',
        '制造费用_机物料及低耗异常标记',
        '机物料及低耗异常',
    ),
    ('moh_depreciation_unit_cost', '制造费用_折旧单位完工成本', '制造费用_折旧异常标记', '折旧异常'),
    ('moh_utilities_unit_cost', '制造费用_水电费单位完工成本', '制造费用_水电费异常标记', '水电费异常'),
]

WORK_ORDER_OUTPUT_COLUMNS = [
    '月份',
    '成本中心',
    '产品编码',
    '产品名称',
    '规格型号',
    '工单编号',
    '工单行',
    '基本单位',
    '本期完工数量',
    '总完工成本',
    '直接材料合计完工金额',
    '直接人工合计完工金额',
    '制造费用合计完工金额',
    '制造费用_其他合计完工金额',
    '制造费用_人工合计完工金额',
    '制造费用_机物料及低耗合计完工金额',
    '制造费用_折旧合计完工金额',
    '制造费用_水电费合计完工金额',
    '委外加工费合计完工金额',
    '总单位完工成本',
    '直接材料单位完工成本',
    '直接人工单位完工成本',
    '制造费用单位完工成本',
    '制造费用_其他单位完工成本',
    '制造费用_人工单位完工成本',
    '制造费用_机物料及低耗单位完工成本',
    '制造费用_折旧单位完工成本',
    '制造费用_水电费单位完工成本',
    '委外加工费单位完工成本',
    'log_总单位完工成本',
    'log_直接材料单位完工成本',
    'log_直接人工单位完工成本',
    'log_制造费用单位完工成本',
    'log_制造费用_其他单位完工成本',
    'log_制造费用_人工单位完工成本',
    'log_制造费用_机物料及低耗单位完工成本',
    'log_制造费用_折旧单位完工成本',
    'log_制造费用_水电费单位完工成本',
    'Modified Z-score_总单位完工成本',
    'Modified Z-score_直接材料',
    'Modified Z-score_直接人工',
    'Modified Z-score_制造费用',
    'Modified Z-score_制造费用_其他',
    'Modified Z-score_制造费用_人工',
    'Modified Z-score_制造费用_机物料及低耗',
    'Modified Z-score_制造费用_折旧',
    'Modified Z-score_制造费用_水电费',
    '是否可参与分析',
    '总成本异常标记',
    '直接材料异常标记',
    '直接人工异常标记',
    '制造费用异常标记',
    '制造费用_其他异常标记',
    '制造费用_人工异常标记',
    '制造费用_机物料及低耗异常标记',
    '制造费用_折旧异常标记',
    '制造费用_水电费异常标记',
    '异常等级',
    '异常主要来源',
    '复核原因',
]

WORK_ORDER_COLUMN_TYPES = {
    '月份': 'text',
    '成本中心': 'text',
    '产品编码': 'text',
    '产品名称': 'text',
    '规格型号': 'text',
    '工单编号': 'text',
    '工单行': 'text',
    '基本单位': 'text',
    '本期完工数量': 'qty',
    '总完工成本': 'amount',
    '直接材料合计完工金额': 'amount',
    '直接人工合计完工金额': 'amount',
    '制造费用合计完工金额': 'amount',
    '制造费用_其他合计完工金额': 'amount',
    '制造费用_人工合计完工金额': 'amount',
    '制造费用_机物料及低耗合计完工金额': 'amount',
    '制造费用_折旧合计完工金额': 'amount',
    '制造费用_水电费合计完工金额': 'amount',
    '委外加工费合计完工金额': 'amount',
    '总单位完工成本': 'price',
    '直接材料单位完工成本': 'price',
    '直接人工单位完工成本': 'price',
    '制造费用单位完工成本': 'price',
    '制造费用_其他单位完工成本': 'price',
    '制造费用_人工单位完工成本': 'price',
    '制造费用_机物料及低耗单位完工成本': 'price',
    '制造费用_折旧单位完工成本': 'price',
    '制造费用_水电费单位完工成本': 'price',
    '委外加工费单位完工成本': 'price',
    'log_总单位完工成本': 'score',
    'log_直接材料单位完工成本': 'score',
    'log_直接人工单位完工成本': 'score',
    'log_制造费用单位完工成本': 'score',
    'log_制造费用_其他单位完工成本': 'score',
    'log_制造费用_人工单位完工成本': 'score',
    'log_制造费用_机物料及低耗单位完工成本': 'score',
    'log_制造费用_折旧单位完工成本': 'score',
    'log_制造费用_水电费单位完工成本': 'score',
    'Modified Z-score_总单位完工成本': 'score',
    'Modified Z-score_直接材料': 'score',
    'Modified Z-score_直接人工': 'score',
    'Modified Z-score_制造费用': 'score',
    'Modified Z-score_制造费用_其他': 'score',
    'Modified Z-score_制造费用_人工': 'score',
    'Modified Z-score_制造费用_机物料及低耗': 'score',
    'Modified Z-score_制造费用_折旧': 'score',
    'Modified Z-score_制造费用_水电费': 'score',
    '是否可参与分析': 'text',
    '总成本异常标记': 'text',
    '直接材料异常标记': 'text',
    '直接人工异常标记': 'text',
    '制造费用异常标记': 'text',
    '制造费用_其他异常标记': 'text',
    '制造费用_人工异常标记': 'text',
    '制造费用_机物料及低耗异常标记': 'text',
    '制造费用_折旧异常标记': 'text',
    '制造费用_水电费异常标记': 'text',
    '异常等级': 'text',
    '异常主要来源': 'text',
    '复核原因': 'text',
}


def grade_score(score: float | None) -> str:
    if score is None or pd.isna(score):
        return ''
    abs_score = abs(score)
    if abs_score > 3.5:
        return '高度可疑'
    if abs_score > 2.5:
        return '关注'
    return '正常'


def build_anomaly_sheet(work_order_df: pd.DataFrame) -> FlatSheet:
    anomaly_df = work_order_df.copy()
    reason_series = pd.Series('', index=anomaly_df.index, dtype='object')

    anomaly_df['can_analyze'] = anomaly_df['completed_qty'].map(
        lambda value: value is not None and value > ZERO
    ) & anomaly_df['total_unit_cost'].map(lambda value: value is not None and value > ZERO)

    for metric_key, display_name, flag_column, _reason in ANOMALY_METRICS:
        log_column = f'log_{metric_key}'
        score_column = f'modified_z_{metric_key}'
        anomaly_df[log_column] = None
        anomaly_df[score_column] = None

        metric_positive = anomaly_df[metric_key].map(lambda value: value is not None and value > ZERO)
        reason_series = append_reason(reason_series, ~metric_positive, f'{display_name}小于等于0或为空')

        for _, group_index in anomaly_df.groupby(['product_code', 'product_name'], sort=False).groups.items():
            metric_series = anomaly_df.loc[group_index, metric_key]
            valid_mask = metric_series.map(lambda value: value is not None and value > ZERO)
            if not valid_mask.any():
                continue
            valid_values = metric_series.loc[valid_mask].map(lambda value: math.log(float(value)))
            anomaly_df.loc[valid_values.index, log_column] = valid_values

            if len(valid_values) < 3:
                continue

            median = valid_values.median()
            mad = (valid_values - median).abs().median()
            if pd.isna(mad) or mad == 0:
                continue

            scores = 0.6745 * (valid_values - median) / mad
            anomaly_df.loc[scores.index, score_column] = scores

        anomaly_df[flag_column] = anomaly_df[score_column].map(grade_score)

    anomaly_df['复核原因'] = reason_series
    anomaly_df['是否可参与分析'] = anomaly_df['can_analyze'].map(lambda value: '是' if value else '否')

    overall_level = pd.Series('正常', index=anomaly_df.index, dtype='object')
    highest_source = pd.Series('', index=anomaly_df.index, dtype='object')
    highest_score = pd.Series(-1.0, index=anomaly_df.index, dtype='float64')
    severity_rank = pd.Series(0, index=anomaly_df.index, dtype='int64')

    for metric_key, _display_name, flag_column, source_label in ANOMALY_METRICS:
        score_column = f'modified_z_{metric_key}'
        flag_series = anomaly_df[flag_column]
        current_rank = flag_series.map({'正常': 0, '关注': 1, '高度可疑': 2}).fillna(-1).astype(int)
        score_abs = anomaly_df[score_column].map(
            lambda value: abs(value) if value is not None and not pd.isna(value) else -1.0
        )

        better_rank = current_rank > severity_rank
        same_rank_better_score = (current_rank == severity_rank) & (score_abs > highest_score)
        same_rank_same_score = (
            (current_rank == severity_rank)
            & (score_abs == highest_score)
            & highest_source.ne('')
            & highest_source.ne(source_label)
            & (current_rank > 0)
        )

        if better_rank.any():
            overall_level.loc[better_rank] = flag_series.loc[better_rank]
            highest_source.loc[better_rank] = source_label
            highest_score.loc[better_rank] = score_abs.loc[better_rank]
            severity_rank.loc[better_rank] = current_rank.loc[better_rank]

        if same_rank_better_score.any():
            overall_level.loc[same_rank_better_score] = flag_series.loc[same_rank_better_score]
            highest_source.loc[same_rank_better_score] = source_label
            highest_score.loc[same_rank_better_score] = score_abs.loc[same_rank_better_score]

        if same_rank_same_score.any():
            prefer_total = same_rank_same_score & ((highest_source == '总成本异常') | (source_label == '总成本异常'))
            highest_source.loc[same_rank_same_score & ~prefer_total] = '多项同时异常'

    highest_source.loc[severity_rank <= 0] = ''
    anomaly_df['异常等级'] = overall_level
    anomaly_df['异常主要来源'] = highest_source

    rename_map = {
        'period_display': '月份',
        'cost_center': '成本中心',
        'product_code': '产品编码',
        'product_name': '产品名称',
        'spec': '规格型号',
        'order_no': '工单编号',
        'order_line': '工单行',
        'unit': '基本单位',
        'completed_qty': '本期完工数量',
        'completed_amount_total': '总完工成本',
        'dm_amount': '直接材料合计完工金额',
        'dl_amount': '直接人工合计完工金额',
        'moh_amount': '制造费用合计完工金额',
        'moh_other_amount': '制造费用_其他合计完工金额',
        'moh_labor_amount': '制造费用_人工合计完工金额',
        'moh_consumables_amount': '制造费用_机物料及低耗合计完工金额',
        'moh_depreciation_amount': '制造费用_折旧合计完工金额',
        'moh_utilities_amount': '制造费用_水电费合计完工金额',
        'outsource_amount': '委外加工费合计完工金额',
        'total_unit_cost': '总单位完工成本',
        'dm_unit_cost': '直接材料单位完工成本',
        'dl_unit_cost': '直接人工单位完工成本',
        'moh_unit_cost': '制造费用单位完工成本',
        'moh_other_unit_cost': '制造费用_其他单位完工成本',
        'moh_labor_unit_cost': '制造费用_人工单位完工成本',
        'moh_consumables_unit_cost': '制造费用_机物料及低耗单位完工成本',
        'moh_depreciation_unit_cost': '制造费用_折旧单位完工成本',
        'moh_utilities_unit_cost': '制造费用_水电费单位完工成本',
        'log_total_unit_cost': 'log_总单位完工成本',
        'log_dm_unit_cost': 'log_直接材料单位完工成本',
        'log_dl_unit_cost': 'log_直接人工单位完工成本',
        'log_moh_unit_cost': 'log_制造费用单位完工成本',
        'log_moh_other_unit_cost': 'log_制造费用_其他单位完工成本',
        'log_moh_labor_unit_cost': 'log_制造费用_人工单位完工成本',
        'log_moh_consumables_unit_cost': 'log_制造费用_机物料及低耗单位完工成本',
        'log_moh_depreciation_unit_cost': 'log_制造费用_折旧单位完工成本',
        'log_moh_utilities_unit_cost': 'log_制造费用_水电费单位完工成本',
        'modified_z_total_unit_cost': 'Modified Z-score_总单位完工成本',
        'modified_z_dm_unit_cost': 'Modified Z-score_直接材料',
        'modified_z_dl_unit_cost': 'Modified Z-score_直接人工',
        'modified_z_moh_unit_cost': 'Modified Z-score_制造费用',
        'modified_z_moh_other_unit_cost': 'Modified Z-score_制造费用_其他',
        'modified_z_moh_labor_unit_cost': 'Modified Z-score_制造费用_人工',
        'modified_z_moh_consumables_unit_cost': 'Modified Z-score_制造费用_机物料及低耗',
        'modified_z_moh_depreciation_unit_cost': 'Modified Z-score_制造费用_折旧',
        'modified_z_moh_utilities_unit_cost': 'Modified Z-score_制造费用_水电费',
    }
    output_df = anomaly_df.rename(columns=rename_map)
    output_df = output_df[WORK_ORDER_OUTPUT_COLUMNS]
    return FlatSheet(data=output_df, column_types=WORK_ORDER_COLUMN_TYPES)

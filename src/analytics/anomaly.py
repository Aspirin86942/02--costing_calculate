"""工单异常分析页。"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from src.analytics.contracts import ConditionalFormatRule, FlatSheet
from src.analytics.errors import append_reason
from src.analytics.fact_builder import (
    DEFAULT_STANDALONE_COST_ITEMS,
    ZERO,
    StandaloneCostItemMeta,
    resolve_standalone_cost_item_metas,
)

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
WORK_ORDER_HIGHLIGHT_COLUMNS: tuple[tuple[str, str], ...] = (
    ('直接材料单位完工成本', '直接材料异常标记'),
    ('直接人工单位完工成本', '直接人工异常标记'),
    ('制造费用单位完工成本', '制造费用异常标记'),
    ('制造费用_其他单位完工成本', '制造费用_其他异常标记'),
    ('制造费用_人工单位完工成本', '制造费用_人工异常标记'),
    ('制造费用_机物料及低耗单位完工成本', '制造费用_机物料及低耗异常标记'),
    ('制造费用_折旧单位完工成本', '制造费用_折旧异常标记'),
    ('制造费用_水电费单位完工成本', '制造费用_水电费异常标记'),
)
ANOMALY_FLAG_FORMAT_KEYS: dict[str, str] = {
    '关注': 'attention',
    '高度可疑': 'suspicious',
}


def weighted_median(values: np.ndarray, weights: np.ndarray) -> float:
    """计算加权中位数。

    Args:
        values: 数值数组
        weights: 权重数组（必须 > 0）

    Returns:
        加权中位数
    """
    if len(values) == 0:
        return np.nan

    # 按值排序
    sorted_indices = np.argsort(values)
    sorted_values = values[sorted_indices]
    sorted_weights = weights[sorted_indices]

    # 计算累计权重
    cumsum = np.cumsum(sorted_weights)
    total_weight = cumsum[-1]

    # 找到累计权重 >= 总权重/2 的第一个位置
    cutoff = total_weight / 2.0
    median_idx = np.searchsorted(cumsum, cutoff, side='right')

    return float(sorted_values[median_idx])


def weighted_mad(values: np.ndarray, weights: np.ndarray, center: float) -> float:
    """计算加权 MAD (Median Absolute Deviation)。

    Args:
        values: 数值数组
        weights: 权重数组（必须 > 0）
        center: 中心值（通常是加权中位数）

    Returns:
        加权 MAD
    """
    if len(values) == 0:
        return np.nan

    # 计算绝对偏差
    abs_deviations = np.abs(values - center)

    # 返回绝对偏差的加权中位数
    return weighted_median(abs_deviations, weights)


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
    '总单位完工成本',
    '直接材料单位完工成本',
    '直接人工单位完工成本',
    '制造费用单位完工成本',
    '制造费用_其他单位完工成本',
    '制造费用_人工单位完工成本',
    '制造费用_机物料及低耗单位完工成本',
    '制造费用_折旧单位完工成本',
    '制造费用_水电费单位完工成本',
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
    '总单位完工成本': 'price',
    '直接材料单位完工成本': 'price',
    '直接人工单位完工成本': 'price',
    '制造费用单位完工成本': 'price',
    '制造费用_其他单位完工成本': 'price',
    '制造费用_人工单位完工成本': 'price',
    '制造费用_机物料及低耗单位完工成本': 'price',
    '制造费用_折旧单位完工成本': 'price',
    '制造费用_水电费单位完工成本': 'price',
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


def _excel_column_name(column_idx: int) -> str:
    """把 0-based 列下标转换为 Excel 列名。"""
    result = ''
    current = column_idx + 1
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        result = chr(ord('A') + remainder) + result
    return result


def _build_ascii_safe_excel_text(text: str) -> str:
    """把中文文本转成 ASCII 公式，避免 xlsxwriter 兼容差异。"""
    return '&'.join(f'UNICHAR({ord(char)})' for char in text)


def build_work_order_conditional_formats(columns: list[str]) -> tuple[ConditionalFormatRule, ...]:
    """按工单异常页列布局生成条件格式契约。"""
    header_map = {column_name: idx for idx, column_name in enumerate(columns)}
    rules: list[ConditionalFormatRule] = []

    for value_column, flag_column in WORK_ORDER_HIGHLIGHT_COLUMNS:
        value_idx = header_map.get(value_column)
        flag_idx = header_map.get(flag_column)
        if value_idx is None or flag_idx is None:
            continue

        flag_col_letter = _excel_column_name(flag_idx)
        for flag_label, format_key in ANOMALY_FLAG_FORMAT_KEYS.items():
            formula = f'=EXACT(${flag_col_letter}2,{_build_ascii_safe_excel_text(flag_label)})'
            for target_idx in (value_idx, flag_idx):
                target_col_letter = _excel_column_name(target_idx)
                rules.append(
                    ConditionalFormatRule(
                        target_range=f'{target_col_letter}2:{target_col_letter}1048576',
                        formula=formula,
                        format_key=format_key,
                    )
                )
    return tuple(rules)


def grade_score(score: float | None) -> str:
    if score is None or pd.isna(score):
        return ''
    abs_score = abs(score)
    if abs_score > 3.5:
        return '高度可疑'
    if abs_score > 2.5:
        return '关注'
    return '正常'


def build_anomaly_sheet(
    work_order_df: pd.DataFrame,
    standalone_metas: tuple[StandaloneCostItemMeta, ...] | None = None,
) -> FlatSheet:
    if standalone_metas is None:
        standalone_metas = resolve_standalone_cost_item_metas(DEFAULT_STANDALONE_COST_ITEMS)
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
            qty_series = anomaly_df.loc[group_index, 'completed_qty']

            valid_mask = metric_series.map(lambda value: value is not None and value > ZERO) & qty_series.map(
                lambda value: value is not None and value > ZERO
            )
            if not valid_mask.any():
                continue

            valid_values = metric_series.loc[valid_mask].map(lambda value: math.log(float(value)))
            valid_weights = qty_series.loc[valid_mask].map(float)
            anomaly_df.loc[valid_values.index, log_column] = valid_values

            if len(valid_values) < 3:
                continue

            # 使用加权中位数和加权 MAD
            values_array = valid_values.to_numpy()
            weights_array = valid_weights.to_numpy()

            median = weighted_median(values_array, weights_array)
            mad = weighted_mad(values_array, weights_array, median)

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
    output_columns = WORK_ORDER_OUTPUT_COLUMNS.copy()
    output_column_types = WORK_ORDER_COLUMN_TYPES.copy()
    for meta in standalone_metas:
        if meta.amount_key in anomaly_df.columns:
            rename_map[meta.amount_key] = meta.work_order_amount_column
        if meta.unit_cost_key in anomaly_df.columns and meta.work_order_unit_cost_column not in anomaly_df.columns:
            rename_map[meta.unit_cost_key] = meta.work_order_unit_cost_column
        output_column_types.setdefault(meta.work_order_amount_column, 'amount')
        output_column_types.setdefault(meta.work_order_unit_cost_column, 'price')
        if meta.work_order_amount_column not in output_columns:
            output_columns.insert(output_columns.index('总单位完工成本'), meta.work_order_amount_column)
        if meta.work_order_unit_cost_column not in output_columns:
            output_columns.insert(output_columns.index('log_总单位完工成本'), meta.work_order_unit_cost_column)

    output_df = anomaly_df.rename(columns=rename_map)
    output_df = output_df[output_columns]
    return FlatSheet(data=output_df, column_types=output_column_types)

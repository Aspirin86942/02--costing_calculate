"""异常评分的纯计算工具。"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

MIN_RELATIVE_MAD_RATIO = 0.005
MIN_LOG_MAD = math.log1p(MIN_RELATIVE_MAD_RATIO)


def weighted_median(values: np.ndarray, weights: np.ndarray) -> float:
    """计算加权中位数。"""
    if len(values) == 0:
        return np.nan

    sorted_indices = np.argsort(values)
    sorted_values = values[sorted_indices]
    sorted_weights = weights[sorted_indices]

    cumsum = np.cumsum(sorted_weights)
    total_weight = cumsum[-1]

    cutoff = total_weight / 2.0
    median_idx = np.searchsorted(cumsum, cutoff, side='right')

    return float(sorted_values[median_idx])


def weighted_mad(values: np.ndarray, weights: np.ndarray, center: float) -> float:
    """计算加权 MAD (Median Absolute Deviation)。"""
    if len(values) == 0:
        return np.nan

    abs_deviations = np.abs(values - center)
    return weighted_median(abs_deviations, weights)


def resolve_effective_log_mad(mad: float) -> float:
    """返回 log Modified Z-score 计算使用的有效 MAD。"""
    if pd.isna(mad):
        return np.nan

    # 当大量高权重工单的单位成本几乎完全一致时，加权 MAD 会被压到接近 0。
    # 这会把 1% 左右的正常波动放大成数万甚至数百万分，偏离人工复核直觉。
    return max(float(mad), MIN_LOG_MAD)


def grade_score(score: float | None) -> str:
    """按 Modified Z-score 阈值输出异常等级。"""
    if score is None or pd.isna(score):
        return ''
    abs_score = abs(score)
    if abs_score > 3.5:
        return '高度可疑'
    if abs_score > 2.5:
        return '关注'
    return '正常'

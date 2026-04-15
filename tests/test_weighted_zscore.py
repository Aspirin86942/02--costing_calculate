"""测试加权 z-score 计算逻辑。"""

import numpy as np
import pytest

from src.analytics.anomaly import weighted_mad, weighted_median


def test_weighted_median_simple():
    """测试简单加权中位数。"""
    values = np.array([10.0, 100.0])
    weights = np.array([100.0, 1.0])

    # 模拟扩充：100 个 10 + 1 个 100，中位数应该是 10
    result = weighted_median(values, weights)
    assert result == 10.0


def test_weighted_median_equal_weights():
    """测试等权重时退化为普通中位数。"""
    values = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    weights = np.array([1.0, 1.0, 1.0, 1.0, 1.0])

    result = weighted_median(values, weights)
    assert result == 3.0


def test_weighted_mad_simple():
    """测试简单加权 MAD。"""
    values = np.array([10.0, 100.0])
    weights = np.array([100.0, 1.0])

    # 中位数是 10
    median = weighted_median(values, weights)
    assert median == 10.0

    # 偏差：|10-10|=0 (权重100), |100-10|=90 (权重1)
    # 加权中位数偏差应该是 0
    mad = weighted_mad(values, weights, median)
    assert mad == 0.0


def test_weighted_mad_symmetric():
    """测试对称分布的加权 MAD。"""
    values = np.array([8.0, 10.0, 12.0])
    weights = np.array([1.0, 1.0, 1.0])

    median = weighted_median(values, weights)
    assert median == 10.0

    mad = weighted_mad(values, weights, median)
    assert mad == 2.0


def test_weighted_median_empty():
    """测试空数组。"""
    values = np.array([])
    weights = np.array([])

    result = weighted_median(values, weights)
    assert np.isnan(result)


def test_weighted_mad_empty():
    """测试空数组。"""
    values = np.array([])
    weights = np.array([])

    result = weighted_mad(values, weights, 0.0)
    assert np.isnan(result)


def test_weighted_median_large_weight_difference():
    """测试极端权重差异。"""
    # 1000 个 5 元产品 vs 1 个 1000 元产品
    values = np.array([5.0, 1000.0])
    weights = np.array([1000.0, 1.0])

    result = weighted_median(values, weights)
    assert result == 5.0


def test_weighted_zscore_scenario():
    """测试实际业务场景：工单数量对 z-score 的影响。"""
    # 场景：产品 A 有 3 个工单
    # 工单 1: 单位成本 10 元，数量 100
    # 工单 2: 单位成本 10.5 元，数量 50
    # 工单 3: 单位成本 100 元，数量 1（异常）

    values = np.array([10.0, 10.5, 100.0])
    weights = np.array([100.0, 50.0, 1.0])

    median = weighted_median(values, weights)
    # 累计权重：[100, 150, 151]，总权重 151，中位数位置 75.5
    # 第一个 >= 75.5 的是 100，所以中位数 = 10.0
    assert median == 10.0

    mad = weighted_mad(values, weights, median)
    # 偏差：|10-10|=0 (权重100), |10.5-10|=0.5 (权重50), |100-10|=90 (权重1)
    # 排序：[(0, 100), (0.5, 50), (90, 1)]
    # 累计：[100, 150, 151]，中位数位置 75.5
    # 第一个 >= 75.5 的是 100，所以 MAD = 0
    assert mad == 0.0

    # 计算 z-score（使用 log 变换后的值）
    log_values = np.log(values)
    log_median = weighted_median(log_values, weights)
    log_mad = weighted_mad(log_values, weights, log_median)

    # 工单 3 的 z-score 应该很高
    if log_mad > 0:
        z_score_3 = 0.6745 * (np.log(100.0) - log_median) / log_mad
        # 由于工单 3 数量很少，它的异常不应该主导统计量
        # z-score 应该显著 > 3.5（高度可疑）
        assert z_score_3 > 3.5

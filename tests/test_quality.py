from __future__ import annotations

import pandas as pd

from src.analytics.quality import build_quality_metrics


def test_build_quality_metrics_marks_rates_na_when_month_filter_result_is_empty() -> None:
    metrics = build_quality_metrics(
        detail_df=pd.DataFrame(columns=['月份']),
        qty_input_df=pd.DataFrame(columns=['月份']),
        qty_sheet_df=pd.DataFrame(columns=['月份', '_join_key']),
        analysis_df=pd.DataFrame(columns=['是否可参与分析']),
        filtered_invalid_qty_count=0,
        filtered_missing_total_amount_count=0,
        month_filter_empty_result=True,
    )

    metric_map = {metric.metric: metric for metric in metrics}

    assert metric_map['直接材料金额缺失率'].value == 'N/A'
    assert metric_map['直接材料金额缺失率'].description == '月份过滤后无数据，指标不适用'
    assert metric_map['可参与分析占比'].value == 'N/A'
    assert metric_map['可参与分析占比'].description == '月份过滤后无数据，指标不适用'

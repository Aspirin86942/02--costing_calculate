from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path

import pandas as pd

from src.analytics.contracts import QualityMetric
from src.etl.month_filter import MonthFilterSummary


def _value_counts(frame: pd.DataFrame, column_name: str, *, drop_blank: bool = True) -> dict[str, int]:
    if column_name not in frame.columns or frame.empty:
        return {}
    series = frame[column_name].fillna('').astype(str).str.strip()
    if drop_blank:
        series = series[series.ne('')]
    counts = series.value_counts(sort=False)
    return {str(index): int(value) for index, value in counts.items()}


def _quality_metric_payload(quality_metrics: Iterable[QualityMetric]) -> dict[str, dict[str, str]]:
    return {
        metric.metric: {
            'category': metric.category,
            'value': metric.value,
            'description': metric.description,
        }
        for metric in quality_metrics
    }


def _month_filter_payload(month_filter_summary: MonthFilterSummary | None) -> dict[str, object] | None:
    if month_filter_summary is None:
        return None
    return {
        'month_range': month_filter_summary.month_range.describe(),
        'input_rows': month_filter_summary.input_rows,
        'output_rows': month_filter_summary.output_rows,
        'input_months': list(month_filter_summary.input_months),
        'output_months': list(month_filter_summary.output_months),
    }


def build_summary_payload(
    *,
    pipeline_name: str,
    input_path: Path,
    output_path: Path,
    error_log_path: Path,
    error_log_count: int,
    quality_metrics: Iterable[QualityMetric],
    error_log_frame: pd.DataFrame,
    work_order_sheet_frame: pd.DataFrame,
    month_filter_summary: MonthFilterSummary | None,
) -> dict[str, object]:
    """从同一次 ETL 产物汇总审计摘要，避免重新计算另一套口径。"""
    return {
        'pipeline': pipeline_name,
        'input': str(input_path),
        'output': str(output_path),
        'error_log': str(error_log_path),
        'error_log_count': int(error_log_count),
        'quality_metrics': _quality_metric_payload(quality_metrics),
        'issue_type_counts': _value_counts(error_log_frame, 'issue_type'),
        'anomaly_level_counts': _value_counts(work_order_sheet_frame, '异常等级'),
        'anomaly_source_counts': _value_counts(work_order_sheet_frame, '异常主要来源'),
        'month_filter': _month_filter_payload(month_filter_summary),
    }


def write_summary_json(output_path: Path, payload: dict[str, object]) -> None:
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')

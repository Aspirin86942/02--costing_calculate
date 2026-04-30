"""运行时月份区间过滤工具。"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from src.analytics.contracts import NormalizedCostFrame
from src.analytics.fact_builder import normalize_period


@dataclass(frozen=True)
class MonthRange:
    """CLI 传入的月份区间，实例化后保证边界是 YYYY-MM 或 None。"""

    start: str | None = None
    end: str | None = None

    def __post_init__(self) -> None:
        normalized_start = _normalize_cli_month(self.start, field_name='month_start')
        normalized_end = _normalize_cli_month(self.end, field_name='month_end')
        if normalized_start is not None and normalized_end is not None and normalized_start > normalized_end:
            raise ValueError(f'month_start={normalized_start} 不能晚于 month_end={normalized_end}')
        object.__setattr__(self, 'start', normalized_start)
        object.__setattr__(self, 'end', normalized_end)

    def output_suffix(self) -> str:
        if self.start and self.end:
            return f'{self.start}_{self.end}'
        if self.start:
            return f'from_{self.start}'
        if self.end:
            return f'to_{self.end}'
        return ''

    def describe(self) -> str:
        if self.start and self.end:
            return f'[{self.start}, {self.end}]'
        if self.start:
            return f'>= {self.start}'
        if self.end:
            return f'<= {self.end}'
        return 'all'


@dataclass(frozen=True)
class MonthFilterSummary:
    """记录月份过滤前后规模与月份集合，供控制台摘要和质量审计使用。"""

    month_range: MonthRange
    input_rows: int
    output_rows: int
    input_months: tuple[str, ...]
    output_months: tuple[str, ...]


def build_month_range(month_start: str | None, month_end: str | None) -> MonthRange | None:
    if month_start is None and month_end is None:
        return None
    return MonthRange(start=month_start, end=month_end)


def apply_month_range_to_normalized_frame(
    normalized: NormalizedCostFrame,
    month_range: MonthRange | None,
) -> tuple[NormalizedCostFrame, MonthFilterSummary | None]:
    if month_range is None:
        return normalized, None

    period_column = _resolve_period_column(normalized.frame)
    frame = normalized.frame.with_columns(
        pl.col(period_column)
        .map_elements(normalize_period, return_dtype=pl.String, skip_nulls=False)
        .alias('_period_key')
    )
    input_months = tuple(sorted({value for value in frame['_period_key'].to_list() if value}))

    # 月份过滤必须只基于规范 YYYY-MM 键，避免展示列格式差异影响边界命中。
    predicate = pl.lit(True)
    if month_range.start is not None:
        predicate = predicate & (pl.col('_period_key') >= month_range.start)
    if month_range.end is not None:
        predicate = predicate & (pl.col('_period_key') <= month_range.end)

    filtered = frame.filter(predicate)
    output_months = tuple(sorted({value for value in filtered['_period_key'].to_list() if value}))
    summary = MonthFilterSummary(
        month_range=month_range,
        input_rows=frame.height,
        output_rows=filtered.height,
        input_months=input_months,
        output_months=output_months,
    )
    return (
        NormalizedCostFrame(frame=filtered.drop('_period_key'), key_columns=normalized.key_columns),
        summary,
    )


def _normalize_cli_month(value: str | None, *, field_name: str) -> str | None:
    if value is None:
        return None
    normalized = normalize_period(value)
    if normalized is None or value.strip() != normalized:
        raise ValueError(f'{field_name} 必须是 YYYY-MM 格式，收到: {value!r}')
    return normalized


def _resolve_period_column(frame: pl.DataFrame) -> str:
    if '月份' in frame.columns:
        return '月份'
    if '年期' in frame.columns:
        return '年期'
    raise ValueError("缺少周期字段，必须包含 '月份' 或 '年期'")

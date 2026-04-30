from __future__ import annotations

import polars as pl
import pytest

from src.analytics.contracts import NormalizedCostFrame
from src.etl.month_filter import MonthRange, apply_month_range_to_normalized_frame


def test_month_range_accepts_open_and_closed_bounds() -> None:
    assert MonthRange(start='2025-01', end='2025-03').output_suffix() == '2025-01_2025-03'
    assert MonthRange(start='2025-01').output_suffix() == 'from_2025-01'
    assert MonthRange(end='2025-03').output_suffix() == 'to_2025-03'
    assert MonthRange().output_suffix() == ''


def test_month_range_rejects_invalid_cli_values() -> None:
    with pytest.raises(ValueError, match='YYYY-MM'):
        MonthRange(start='2025-1')
    with pytest.raises(ValueError, match='YYYY-MM'):
        MonthRange(end='2025/03')
    with pytest.raises(ValueError, match='month_start'):
        MonthRange(start='2025-04', end='2025-03')


def test_apply_month_range_to_normalized_frame_filters_inclusive_bounds() -> None:
    normalized = NormalizedCostFrame(
        frame=pl.DataFrame(
            {
                '年期': ['2025年1期', '2025年2期', '2025年3期'],
                '月份': ['2025年01期', '2025年02期', '2025年03期'],
                '产品编码': ['P001', 'P001', 'P001'],
            }
        ),
        key_columns=('月份', '产品编码'),
    )

    filtered, summary = apply_month_range_to_normalized_frame(
        normalized,
        MonthRange(start='2025-02', end='2025-03'),
    )

    assert filtered.frame['月份'].to_list() == ['2025年02期', '2025年03期']
    assert summary is not None
    assert summary.input_rows == 3
    assert summary.output_rows == 2
    assert summary.input_months == ('2025-01', '2025-02', '2025-03')
    assert summary.output_months == ('2025-02', '2025-03')


def test_apply_month_range_to_normalized_frame_keeps_empty_result_structures() -> None:
    normalized = NormalizedCostFrame(
        frame=pl.DataFrame(
            {
                '年期': ['2025年1期'],
                '月份': ['2025年01期'],
                '产品编码': ['P001'],
            }
        ),
        key_columns=('月份', '产品编码'),
    )

    filtered, summary = apply_month_range_to_normalized_frame(
        normalized,
        MonthRange(start='2025-02', end='2025-03'),
    )

    assert filtered.frame.is_empty()
    assert filtered.frame.columns == ['年期', '月份', '产品编码']
    assert summary is not None
    assert summary.output_rows == 0
    assert summary.output_months == ()

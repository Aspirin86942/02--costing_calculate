"""价量分析模块测试。"""

from __future__ import annotations

from decimal import Decimal

import pandas as pd

from src.analytics.pq_analysis import SectionBlock, build_fact_cost_pq, render_tables


def _sample_detail_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                '月份': '2025年01期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '直接材料',
                '本期完工金额': '100',
                '本期完工单位成本': '10',
            },
            {
                '月份': '2025年01期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '直接人工',
                '本期完工金额': '50',
                '本期完工单位成本': '5',
            },
            {
                '月份': '2025年01期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '制造费用-人工',
                '本期完工金额': '30',
                '本期完工单位成本': '3',
            },
            {
                '月份': '2025年01期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '委外加工费',
                '本期完工金额': '20',
                '本期完工单位成本': '2',
            },
            {
                '月份': '2025年02期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '直接材料',
                '本期完工金额': '150',
                '本期完工单位成本': '12.5',
            },
            {
                '月份': '2025年02期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '直接人工',
                '本期完工金额': '66',
                '本期完工单位成本': '5.5',
            },
            {
                '月份': '2025年02期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '制造费用_折旧',
                '本期完工金额': '44',
                '本期完工单位成本': '3.6666667',
            },
        ]
    )


def _sample_qty_df(include_second_period: bool = True) -> pd.DataFrame:
    data = [
        {
            '月份': '2025年01期',
            '产品编码': 'P001',
            '产品名称': '产品A',
            '本期完工数量': '10',
        }
    ]
    if include_second_period:
        data.append(
            {
                '月份': '2025年02期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '本期完工数量': '12',
            }
        )
    return pd.DataFrame(data)


def test_build_fact_cost_pq_excludes_outsource_and_builds_three_buckets() -> None:
    fact_df, error_log = build_fact_cost_pq(_sample_detail_df(), _sample_qty_df())

    assert len(fact_df) == 6
    assert set(fact_df['cost_bucket'].unique()) == {'direct_material', 'direct_labor', 'moh'}
    assert ((error_log['issue_type'] == 'UNMAPPED_COST_ITEM') & (error_log['original_value'] == '委外加工费')).any()


def test_build_fact_cost_pq_logs_missing_qty() -> None:
    fact_df, error_log = build_fact_cost_pq(_sample_detail_df(), _sample_qty_df(include_second_period=False))

    missing_qty = error_log[error_log['issue_type'] == 'MISSING_QTY']
    assert len(missing_qty) == 3
    assert fact_df[(fact_df['period'] == '2025-02') & fact_df['qty'].isna()].shape[0] == 3


def test_render_tables_returns_three_sections_per_sheet() -> None:
    fact_df, _ = build_fact_cost_pq(_sample_detail_df(), _sample_qty_df())
    tables = render_tables(fact_df)

    assert set(tables.keys()) == {'直接材料_价量比', '直接人工_价量比', '制造费用_价量比'}
    for sections in tables.values():
        assert len(sections) == 3
        assert all(isinstance(section, SectionBlock) for section in sections)
        assert [section.metric_type for section in sections] == ['amount', 'qty', 'price']


def test_render_tables_amount_qty_total_and_weighted_price() -> None:
    fact_df, _ = build_fact_cost_pq(_sample_detail_df(), _sample_qty_df())
    tables = render_tables(fact_df)
    dm_sections = tables['直接材料_价量比']

    amount_df = dm_sections[0].data
    qty_df = dm_sections[1].data
    price_df = dm_sections[2].data

    amount_product = amount_df[amount_df['产品编码'] == 'P001'].iloc[0]
    qty_product = qty_df[qty_df['产品编码'] == 'P001'].iloc[0]
    price_product = price_df[price_df['产品编码'] == 'P001'].iloc[0]

    assert amount_product['2025年01期'] == Decimal('100')
    assert amount_product['2025年02期'] == Decimal('150')
    assert amount_product['总计'] == Decimal('250')

    assert qty_product['2025年01期'] == Decimal('10')
    assert qty_product['2025年02期'] == Decimal('12')
    assert qty_product['总计'] == Decimal('22')

    assert price_product['2025年01期'] == Decimal('10')
    assert price_product['2025年02期'] == Decimal('12.5')
    assert price_product['均值'].quantize(Decimal('0.01')) == Decimal('11.36')

    amount_total = amount_df[amount_df['产品编码'] == '总计'].iloc[0]
    qty_total = qty_df[qty_df['产品编码'] == '总计'].iloc[0]
    assert amount_total['总计'] == Decimal('250')
    assert qty_total['总计'] == Decimal('22')

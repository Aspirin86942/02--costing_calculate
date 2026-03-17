"""测试价量分析宽表与兼容摘要页。"""

from __future__ import annotations

from decimal import Decimal

import pandas as pd

from src.analytics.contracts import ProductAnomalySection, SectionBlock
from src.analytics.table_rendering import build_product_anomaly_sections, render_tables


def _sample_fact_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                'period': '2025-01',
                'product_code': 'P001',
                'product_name': '产品A',
                'cost_bucket': 'direct_material',
                'amount': Decimal('100'),
                'qty': Decimal('10'),
                'price': Decimal('10'),
            },
            {
                'period': '2025-02',
                'product_code': 'P001',
                'product_name': '产品A',
                'cost_bucket': 'direct_material',
                'amount': Decimal('150'),
                'qty': Decimal('12'),
                'price': Decimal('12.5'),
            },
            {
                'period': '2025-01',
                'product_code': 'P001',
                'product_name': '产品A',
                'cost_bucket': 'direct_labor',
                'amount': Decimal('50'),
                'qty': Decimal('10'),
                'price': Decimal('5'),
            },
            {
                'period': '2025-02',
                'product_code': 'P001',
                'product_name': '产品A',
                'cost_bucket': 'direct_labor',
                'amount': Decimal('66'),
                'qty': Decimal('12'),
                'price': Decimal('5.5'),
            },
            {
                'period': '2025-01',
                'product_code': 'P001',
                'product_name': '产品A',
                'cost_bucket': 'moh',
                'amount': Decimal('30'),
                'qty': Decimal('10'),
                'price': Decimal('3'),
            },
            {
                'period': '2025-02',
                'product_code': 'P001',
                'product_name': '产品A',
                'cost_bucket': 'moh',
                'amount': Decimal('44'),
                'qty': Decimal('12'),
                'price': Decimal('3.6666667'),
            },
        ]
    )


def test_render_tables_returns_three_sections_per_sheet() -> None:
    tables = render_tables(_sample_fact_df())

    assert set(tables.keys()) == {'直接材料_价量比', '直接人工_价量比', '制造费用_价量比'}
    for sections in tables.values():
        assert len(sections) == 3
        assert all(isinstance(section, SectionBlock) for section in sections)
        assert [section.metric_type for section in sections] == ['amount', 'qty', 'price']


def test_render_tables_amount_qty_total_and_weighted_price() -> None:
    tables = render_tables(_sample_fact_df())
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


def test_build_product_anomaly_sections_accepts_fact_df_and_disables_outlier_detection() -> None:
    sections = build_product_anomaly_sections(_sample_fact_df())

    assert len(sections) == 1
    section = sections[0]
    assert isinstance(section, ProductAnomalySection)
    assert section.product_code == 'P001'
    assert '总成本' in section.data.columns
    assert '直接材料成本' in section.amount_columns
    assert section.outlier_cells == set()


def test_render_tables_keeps_input_product_order() -> None:
    fact_df = pd.DataFrame(
        [
            {
                'period': '2025-01',
                'product_code': 'GB_C.D.B0041AA',
                'product_name': 'BMS-1100W驱动器',
                'cost_bucket': 'direct_material',
                'amount': Decimal('220'),
                'qty': Decimal('20'),
                'price': Decimal('11'),
            },
            {
                'period': '2025-01',
                'product_code': 'GB_C.D.B0040AA',
                'product_name': 'BMS-750W驱动器',
                'cost_bucket': 'direct_material',
                'amount': Decimal('100'),
                'qty': Decimal('10'),
                'price': Decimal('10'),
            },
        ]
    )

    tables = render_tables(fact_df)
    amount_df = tables['直接材料_价量比'][0].data
    product_codes = amount_df[amount_df['产品编码'] != '总计']['产品编码'].tolist()

    assert product_codes == ['GB_C.D.B0041AA', 'GB_C.D.B0040AA']


def test_build_product_anomaly_sections_keeps_input_product_order() -> None:
    fact_df = pd.DataFrame(
        [
            {
                'period': '2025-01',
                'product_code': 'GB_C.D.B0041AA',
                'product_name': 'BMS-1100W驱动器',
                'cost_bucket': 'direct_material',
                'amount': Decimal('220'),
                'qty': Decimal('20'),
                'price': Decimal('11'),
            },
            {
                'period': '2025-01',
                'product_code': 'GB_C.D.B0040AA',
                'product_name': 'BMS-750W驱动器',
                'cost_bucket': 'direct_material',
                'amount': Decimal('100'),
                'qty': Decimal('10'),
                'price': Decimal('10'),
            },
        ]
    )

    sections = build_product_anomaly_sections(fact_df)
    product_codes = [section.product_code for section in sections]

    assert product_codes == ['GB_C.D.B0041AA', 'GB_C.D.B0040AA']


def test_build_product_anomaly_sections_keeps_only_existing_periods() -> None:
    """兼容摘要页只展示源数据真实存在的月份。"""
    fact_df = pd.DataFrame(
        [
            {
                'period': '2025-03',
                'product_code': 'GB_C.D.B0046AA',
                'product_name': 'BMS-7500W驱动器',
                'cost_bucket': 'direct_material',
                'amount': Decimal('300'),
                'qty': Decimal('10'),
                'price': Decimal('30'),
            },
            {
                'period': '2025-04',
                'product_code': 'GB_C.D.B0046AA',
                'product_name': 'BMS-7500W驱动器',
                'cost_bucket': 'direct_material',
                'amount': Decimal('330'),
                'qty': Decimal('11'),
                'price': Decimal('30'),
            },
        ]
    )

    sections = build_product_anomaly_sections(fact_df)

    assert len(sections) == 1
    assert sections[0].data['月份'].tolist() == ['2025年03期', '2025年04期']

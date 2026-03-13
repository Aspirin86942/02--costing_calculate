"""价量分析模块测试。"""

from __future__ import annotations

from decimal import Decimal

import pandas as pd

from src.analytics.pq_analysis import (
    ProductAnomalySection,
    SectionBlock,
    build_fact_cost_pq,
    build_product_anomaly_sections,
    render_tables,
)


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


def test_build_fact_cost_pq_ignores_source_price_mismatch() -> None:
    detail_df = _sample_detail_df().copy()
    direct_material_mask = (detail_df['月份'] == '2025年1月') & (detail_df['成本项目名称'] == '直接材料')
    detail_df.loc[direct_material_mask, '本期完工单位成本'] = '999.99'

    fact_df, error_log = build_fact_cost_pq(detail_df, _sample_qty_df())

    jan_dm_row = fact_df[(fact_df['period'] == '2025-01') & (fact_df['cost_bucket'] == 'direct_material')].iloc[0]
    assert jan_dm_row['price'] == Decimal('10')
    assert (error_log['issue_type'] == 'PRICE_MISMATCH').sum() == 0


def test_build_fact_cost_pq_drops_source_price_column() -> None:
    fact_df, _ = build_fact_cost_pq(_sample_detail_df(), _sample_qty_df())

    assert 'source_price' not in fact_df.columns
    assert fact_df.columns.tolist() == [
        'period',
        'product_code',
        'product_name',
        'cost_bucket',
        'amount',
        'qty',
        'price',
    ]


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


def test_build_product_anomaly_sections_disables_outlier_detection() -> None:
    detail = pd.DataFrame(
        [
            {
                '月份': '2025年01期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '直接材料',
                '本期完工金额': '100',
            },
            {
                '月份': '2025年01期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '直接人工',
                '本期完工金额': '50',
            },
            {
                '月份': '2025年01期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '制造费用-人工',
                '本期完工金额': '30',
            },
            {
                '月份': '2025年02期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '直接材料',
                '本期完工金额': '105',
            },
            {
                '月份': '2025年02期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '直接人工',
                '本期完工金额': '52',
            },
            {
                '月份': '2025年02期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '制造费用-人工',
                '本期完工金额': '31',
            },
            {
                '月份': '2025年03期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '直接材料',
                '本期完工金额': '100',
            },
            {
                '月份': '2025年03期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '直接人工',
                '本期完工金额': '51',
            },
            {
                '月份': '2025年03期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '制造费用-人工',
                '本期完工金额': '30',
            },
            {
                '月份': '2025年04期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '直接材料',
                '本期完工金额': '100',
            },
            {
                '月份': '2025年04期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '直接人工',
                '本期完工金额': '50',
            },
            {
                '月份': '2025年04期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '制造费用-人工',
                '本期完工金额': '30',
            },
            {
                '月份': '2025年05期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '直接材料',
                '本期完工金额': '500',
            },
            {
                '月份': '2025年05期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '直接人工',
                '本期完工金额': '50',
            },
            {
                '月份': '2025年05期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '制造费用-人工',
                '本期完工金额': '30',
            },
        ]
    )
    qty = pd.DataFrame(
        [
            {'月份': '2025年01期', '产品编码': 'P001', '产品名称': '产品A', '本期完工数量': '10'},
            {'月份': '2025年02期', '产品编码': 'P001', '产品名称': '产品A', '本期完工数量': '10'},
            {'月份': '2025年03期', '产品编码': 'P001', '产品名称': '产品A', '本期完工数量': '10'},
            {'月份': '2025年04期', '产品编码': 'P001', '产品名称': '产品A', '本期完工数量': '10'},
            {'月份': '2025年05期', '产品编码': 'P001', '产品名称': '产品A', '本期完工数量': '10'},
        ]
    )
    fact_df, _ = build_fact_cost_pq(detail, qty)
    sections = build_product_anomaly_sections(fact_df)

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
                'source_price': Decimal('11'),
            },
            {
                'period': '2025-01',
                'product_code': 'GB_C.D.B0040AA',
                'product_name': 'BMS-750W驱动器',
                'cost_bucket': 'direct_material',
                'amount': Decimal('100'),
                'qty': Decimal('10'),
                'price': Decimal('10'),
                'source_price': Decimal('10'),
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
                'source_price': Decimal('11'),
            },
            {
                'period': '2025-01',
                'product_code': 'GB_C.D.B0040AA',
                'product_name': 'BMS-750W驱动器',
                'cost_bucket': 'direct_material',
                'amount': Decimal('100'),
                'qty': Decimal('10'),
                'price': Decimal('10'),
                'source_price': Decimal('10'),
            },
        ]
    )

    sections = build_product_anomaly_sections(fact_df)
    product_codes = [section.product_code for section in sections]

    assert product_codes == ['GB_C.D.B0041AA', 'GB_C.D.B0040AA']


def test_build_product_anomaly_sections_keeps_only_existing_periods() -> None:
    """测试兼容摘要页只展示源数据实际存在的月份，不补空白月份。"""
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

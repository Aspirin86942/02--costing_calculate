"""测试价量分析宽表与兼容摘要页。"""

from __future__ import annotations

from decimal import Decimal

import pandas as pd
import pytest

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


def _build_doc_type_split_summary_df() -> pd.DataFrame:
    def _build_row(
        *,
        period: str,
        doc_type: str,
        total_cost: str,
        completed_qty: str,
        dm_cost: str,
        dl_cost: str,
        moh_cost: str,
    ) -> dict[str, object]:
        total_cost_decimal = Decimal(total_cost)
        completed_qty_decimal = Decimal(completed_qty)
        dm_cost_decimal = Decimal(dm_cost)
        dl_cost_decimal = Decimal(dl_cost)
        moh_cost_decimal = Decimal(moh_cost)
        return {
            'product_code': 'P001',
            'product_name': '产品A',
            'period': period,
            'period_display': f'{period[:4]}年{period[-2:]}期',
            'total_cost': total_cost_decimal,
            'completed_qty': completed_qty_decimal,
            'dm_cost': dm_cost_decimal,
            'dl_cost': dl_cost_decimal,
            'moh_cost': moh_cost_decimal,
            'unit_cost': total_cost_decimal / completed_qty_decimal,
            'dm_unit_cost': dm_cost_decimal / completed_qty_decimal,
            'dl_unit_cost': dl_cost_decimal / completed_qty_decimal,
            'moh_unit_cost': moh_cost_decimal / completed_qty_decimal,
            'dm_contrib': dm_cost_decimal / total_cost_decimal,
            'dl_contrib': dl_cost_decimal / total_cost_decimal,
            'moh_contrib': moh_cost_decimal / total_cost_decimal,
            'doc_type': doc_type,
        }

    return pd.DataFrame(
        [
            _build_row(
                period='2025-01',
                doc_type='汇报入库-普通生产',
                total_cost='100',
                completed_qty='10',
                dm_cost='60',
                dl_cost='20',
                moh_cost='20',
            ),
            _build_row(
                period='2025-01',
                doc_type='直接入库-普通生产',
                total_cost='50',
                completed_qty='5',
                dm_cost='30',
                dl_cost='10',
                moh_cost='10',
            ),
            _build_row(
                period='2025-01',
                doc_type='汇报入库-返工生产',
                total_cost='20',
                completed_qty='2',
                dm_cost='10',
                dl_cost='5',
                moh_cost='5',
            ),
            _build_row(
                period='2025-01',
                doc_type='普通委外订单',
                total_cost='7',
                completed_qty='1',
                dm_cost='3',
                dl_cost='2',
                moh_cost='2',
            ),
            _build_row(
                period='2025-01',
                doc_type='未知单据类型',
                total_cost='3',
                completed_qty='1',
                dm_cost='1',
                dl_cost='1',
                moh_cost='1',
            ),
            _build_row(
                period='2025-02',
                doc_type='未来类型',
                total_cost='9',
                completed_qty='1',
                dm_cost='3',
                dl_cost='3',
                moh_cost='3',
            ),
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
    assert section.section_label is None
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


def test_build_product_anomaly_sections_doc_type_split_builds_three_scopes_for_gb() -> None:
    sections = build_product_anomaly_sections(
        _build_doc_type_split_summary_df(),
        scope_mode='doc_type_split',
    )
    section_by_label = {section.section_label: section for section in sections}

    assert [section.section_label for section in sections] == ['全部', '正常生产', '返工生产']
    assert all(section.product_code == 'P001' for section in sections)
    assert section_by_label['全部'].data['月份'].tolist() == ['2025年01期', '2025年02期']
    assert section_by_label['正常生产'].data['月份'].tolist() == ['2025年01期']
    assert section_by_label['返工生产'].data['月份'].tolist() == ['2025年01期']
    assert section_by_label['全部'].data['总成本'].tolist() == [Decimal('180'), Decimal('9')]
    assert section_by_label['正常生产'].data['总成本'].tolist() == [Decimal('150')]
    assert section_by_label['返工生产'].data['总成本'].tolist() == [Decimal('20')]


def test_build_product_anomaly_sections_doc_type_split_skips_empty_sections() -> None:
    summary_df = _build_doc_type_split_summary_df().query("doc_type == '汇报入库-普通生产'").copy()

    sections = build_product_anomaly_sections(
        summary_df,
        scope_mode='doc_type_split',
    )

    assert [section.section_label for section in sections] == ['全部', '正常生产']


def test_build_product_anomaly_sections_rejects_invalid_scope_mode() -> None:
    with pytest.raises(ValueError, match='product_anomaly_scope_mode'):
        build_product_anomaly_sections(_sample_fact_df(), scope_mode='bad')

"""价量分解模块测试。"""

from __future__ import annotations

from decimal import Decimal

import pandas as pd

from src.analytics.pq_analysis import build_fact_cost_pq, compute_pq_variance, render_tables


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


def test_compute_pq_variance_reconciliation_and_no_base() -> None:
    fact_df, _ = build_fact_cost_pq(_sample_detail_df(), _sample_qty_df())
    variance_df, error_log = compute_pq_variance(fact_df, base_mode='prev_period')

    direct_material = variance_df[variance_df['cost_bucket'] == 'direct_material'].sort_values('period')
    first = direct_material.iloc[0]
    second = direct_material.iloc[1]

    assert first['no_base'] == 1
    assert first['P0'] == Decimal('0')
    assert first['Q0'] == Decimal('0')

    assert second['delta'] == second['PV'] + second['QV'] + second['IV']
    assert second['recon_diff'] == Decimal('0')
    assert error_log.empty


def test_render_tables_only_contains_amount_price_qty_metrics() -> None:
    fact_df, _ = build_fact_cost_pq(_sample_detail_df(), _sample_qty_df())
    variance_df, _ = compute_pq_variance(fact_df, base_mode='prev_period')
    tables = render_tables(variance_df)

    assert '直接人工_价量比' in tables
    assert '直接人工_缝隙' not in tables

    for table_name in ['直接材料_价量比', '直接人工_价量比', '制造费用_价量比']:
        table = tables[table_name]
        metric_cols = [column for column in table.columns if '年' in column and '期_' in column]
        metric_suffixes = {column.rsplit('_', 1)[-1] for column in metric_cols}
        assert metric_suffixes == {'qty', 'price', 'amount'}

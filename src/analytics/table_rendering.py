"""价量分析与兼容摘要页渲染准备。"""

from __future__ import annotations

import pandas as pd

from src.analytics.contracts import ProductAnomalySection, SectionBlock
from src.analytics.fact_builder import (
    COST_BUCKETS,
    ZERO,
    add_decimal,
    first_decimal,
    period_to_display,
    safe_divide,
    sum_decimal,
    sum_decimal_series,
    to_decimal,
)

PRODUCT_ANALYSIS_FIELDS = [
    ('total_cost', '总成本', 'amount', False),
    ('completed_qty', '完工数量', 'qty', False),
    ('unit_cost', '单位成本', 'price', False),
    ('dm_cost', '直接材料成本', 'amount', False),
    ('dm_unit_cost', '单位直接材料成本', 'price', False),
    ('dm_contrib', '直接材料贡献率', 'pct', False),
    ('dl_cost', '直接人工成本', 'amount', False),
    ('dl_unit_cost', '单位直接人工成本', 'price', False),
    ('dl_contrib', '直接人工贡献率', 'pct', False),
    ('moh_cost', '制造费用成本', 'amount', False),
    ('moh_unit_cost', '单位制造费用成本', 'price', False),
    ('moh_contrib', '制造费用贡献率', 'pct', False),
]


def build_pivot(bucket_df: pd.DataFrame, value_col: str, period_columns: list[str]) -> pd.DataFrame:
    pivot = bucket_df.pivot_table(
        index=['product_code', 'product_name'],
        columns='period_display',
        values=value_col,
        aggfunc='first',
        sort=False,
    )
    return pivot.reindex(columns=period_columns).reset_index()


def append_total_row(df: pd.DataFrame, value_columns: list[str], summary_col: str) -> pd.DataFrame:
    total_row: dict[str, object] = {'产品编码': '总计', '产品名称': ''}
    for column in value_columns + [summary_col]:
        total_row[column] = sum_decimal_series(df[column])
    return pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)


def build_section_blocks(bucket_df: pd.DataFrame, title_prefix: str) -> list[SectionBlock]:
    period_keys = sorted(bucket_df['period'].dropna().unique().tolist())
    period_columns = [period_to_display(period) for period in period_keys]

    amount_pivot = build_pivot(bucket_df, 'amount', period_columns).rename(
        columns={'product_code': '产品编码', 'product_name': '产品名称'}
    )
    qty_pivot = build_pivot(bucket_df, 'qty', period_columns).rename(
        columns={'product_code': '产品编码', 'product_name': '产品名称'}
    )

    for column in period_columns:
        amount_pivot[column] = (
            amount_pivot[column].map(to_decimal).map(lambda value: value if value is not None else ZERO)
        )
        qty_pivot[column] = qty_pivot[column].map(to_decimal)

    amount_pivot['总计'] = amount_pivot[period_columns].apply(lambda row: sum_decimal(row.tolist()), axis=1)
    qty_pivot['总计'] = qty_pivot[period_columns].apply(lambda row: sum_decimal(row.tolist()), axis=1)

    price_pivot = amount_pivot[['产品编码', '产品名称']].copy()
    for column in period_columns:
        price_pivot[column] = amount_pivot[column].combine(qty_pivot[column], safe_divide)
    price_pivot['均值'] = amount_pivot['总计'].combine(qty_pivot['总计'], safe_divide)

    amount_with_total = append_total_row(amount_pivot, period_columns, '总计')
    qty_with_total = append_total_row(qty_pivot, period_columns, '总计')

    return [
        SectionBlock(f'{title_prefix}完工金额', amount_with_total, 'amount', True),
        SectionBlock(f'{title_prefix}完工数量', qty_with_total, 'qty', True),
        SectionBlock(f'{title_prefix}完工单价', price_pivot, 'price', False),
    ]


def build_product_summary_df(work_order_df: pd.DataFrame) -> pd.DataFrame:
    if work_order_df.empty:
        return pd.DataFrame(columns=['product_code', 'product_name', 'period', 'period_display'])

    summary_df = work_order_df.groupby(
        ['product_code', 'product_name', 'period'], dropna=False, as_index=False, sort=False
    ).agg(
        total_cost=('completed_amount_total', sum_decimal_series),
        completed_qty=('completed_qty', sum_decimal_series),
        dm_cost=('dm_amount', sum_decimal_series),
        dl_cost=('dl_amount', sum_decimal_series),
        moh_cost=('moh_amount', sum_decimal_series),
    )
    summary_df['unit_cost'] = summary_df['total_cost'].combine(summary_df['completed_qty'], safe_divide)
    summary_df['dm_unit_cost'] = summary_df['dm_cost'].combine(summary_df['completed_qty'], safe_divide)
    summary_df['dl_unit_cost'] = summary_df['dl_cost'].combine(summary_df['completed_qty'], safe_divide)
    summary_df['moh_unit_cost'] = summary_df['moh_cost'].combine(summary_df['completed_qty'], safe_divide)
    summary_df['dm_contrib'] = summary_df['dm_cost'].combine(summary_df['total_cost'], safe_divide)
    summary_df['dl_contrib'] = summary_df['dl_cost'].combine(summary_df['total_cost'], safe_divide)
    summary_df['moh_contrib'] = summary_df['moh_cost'].combine(summary_df['total_cost'], safe_divide)
    summary_df['period_display'] = summary_df['period'].map(period_to_display)
    return summary_df


def build_product_summary_from_fact_df(fact_df: pd.DataFrame) -> pd.DataFrame:
    if fact_df.empty:
        return pd.DataFrame(columns=['product_code', 'product_name', 'period', 'period_display'])

    amount_by_bucket = (
        fact_df.groupby(
            ['product_code', 'product_name', 'period', 'cost_bucket'], dropna=False, as_index=False, sort=False
        )
        .agg(amount=('amount', sum_decimal_series))
        .pivot_table(
            index=['product_code', 'product_name', 'period'],
            columns='cost_bucket',
            values='amount',
            aggfunc='first',
            sort=False,
        )
        .reset_index()
    )
    for bucket in COST_BUCKETS:
        if bucket not in amount_by_bucket.columns:
            amount_by_bucket[bucket] = ZERO

    qty_by_product = fact_df.groupby(
        ['product_code', 'product_name', 'period'], dropna=False, as_index=False, sort=False
    ).agg(completed_qty=('qty', first_decimal))

    summary_df = amount_by_bucket.merge(
        qty_by_product,
        on=['product_code', 'product_name', 'period'],
        how='left',
    ).rename(
        columns={
            'direct_material': 'dm_cost',
            'direct_labor': 'dl_cost',
            'moh': 'moh_cost',
        }
    )

    summary_df['total_cost'] = (
        summary_df['dm_cost'].combine(summary_df['dl_cost'], add_decimal).combine(summary_df['moh_cost'], add_decimal)
    )
    summary_df['unit_cost'] = summary_df['total_cost'].combine(summary_df['completed_qty'], safe_divide)
    summary_df['dm_unit_cost'] = summary_df['dm_cost'].combine(summary_df['completed_qty'], safe_divide)
    summary_df['dl_unit_cost'] = summary_df['dl_cost'].combine(summary_df['completed_qty'], safe_divide)
    summary_df['moh_unit_cost'] = summary_df['moh_cost'].combine(summary_df['completed_qty'], safe_divide)
    summary_df['dm_contrib'] = summary_df['dm_cost'].combine(summary_df['total_cost'], safe_divide)
    summary_df['dl_contrib'] = summary_df['dl_cost'].combine(summary_df['total_cost'], safe_divide)
    summary_df['moh_contrib'] = summary_df['moh_cost'].combine(summary_df['total_cost'], safe_divide)
    summary_df['period_display'] = summary_df['period'].map(period_to_display)
    return summary_df


def build_product_anomaly_sections(summary_df: pd.DataFrame) -> list[ProductAnomalySection]:
    """构建兼容产品摘要页。"""
    if 'period_display' not in summary_df.columns and {'cost_bucket', 'amount', 'qty'}.issubset(summary_df.columns):
        summary_df = build_product_summary_from_fact_df(summary_df)

    if summary_df.empty:
        return []

    sections: list[ProductAnomalySection] = []
    grouped = summary_df.groupby(['product_code', 'product_name'], dropna=False, sort=False)
    for (product_code, product_name), product_frame in grouped:
        product_frame = product_frame.sort_values('period').reset_index(drop=True)
        display_data = pd.DataFrame({'月份': product_frame['period_display']})
        column_types = {'月份': 'text'}
        amount_columns: list[str] = []

        for internal_key, display_name, metric_type, _detect in PRODUCT_ANALYSIS_FIELDS:
            display_data[display_name] = product_frame[internal_key]
            column_types[display_name] = metric_type
            if metric_type == 'amount':
                amount_columns.append(display_name)

        sections.append(
            ProductAnomalySection(
                product_code=str(product_code),
                product_name=str(product_name),
                data=display_data,
                column_types=column_types,
                amount_columns=amount_columns,
                outlier_cells=set(),
            )
        )

    return sections


def render_tables(fact_df: pd.DataFrame) -> dict[str, list[SectionBlock]]:
    """按成本类别输出三段价量分析。"""
    if fact_df.empty:
        empty = pd.DataFrame(columns=['产品编码', '产品名称'])
        return {
            '直接材料_价量比': [
                SectionBlock('直接材料完工金额', empty.copy(), 'amount', True),
                SectionBlock('直接材料完工数量', empty.copy(), 'qty', True),
                SectionBlock('直接材料完工单价', empty.copy(), 'price', False),
            ],
            '直接人工_价量比': [
                SectionBlock('直接人工完工金额', empty.copy(), 'amount', True),
                SectionBlock('直接人工完工数量', empty.copy(), 'qty', True),
                SectionBlock('直接人工完工单价', empty.copy(), 'price', False),
            ],
            '制造费用_价量比': [
                SectionBlock('制造费用完工金额', empty.copy(), 'amount', True),
                SectionBlock('制造费用完工数量', empty.copy(), 'qty', True),
                SectionBlock('制造费用完工单价', empty.copy(), 'price', False),
            ],
        }

    source = fact_df.copy()
    source['period_display'] = source['period'].map(period_to_display)
    return {
        '直接材料_价量比': build_section_blocks(source[source['cost_bucket'] == 'direct_material'], '直接材料'),
        '直接人工_价量比': build_section_blocks(source[source['cost_bucket'] == 'direct_labor'], '直接人工'),
        '制造费用_价量比': build_section_blocks(source[source['cost_bucket'] == 'moh'], '制造费用'),
    }

"""价量分析模块：构建标准长表并输出金额/数量/单价三段块数据。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

import pandas as pd

ZERO = Decimal('0')
PRICE_DIFF_TOLERANCE = Decimal('0.01')
COST_BUCKETS = ('direct_material', 'direct_labor', 'moh')


@dataclass
class SectionBlock:
    """单个报表分段。"""

    title: str
    data: pd.DataFrame
    metric_type: str
    has_total_row: bool


@dataclass
class ProductAnomalySection:
    """单个产品异常分析分段。"""

    product_code: str
    product_name: str
    data: pd.DataFrame
    column_types: dict[str, str]
    amount_columns: list[str]
    outlier_cells: set[tuple[int, str]]


PRODUCT_ANALYSIS_FIELDS = [
    ('total_cost', '总成本', 'amount', False),
    ('completed_qty', '完工数量', 'qty', False),
    ('unit_cost', '单位成本', 'price', True),
    ('dm_cost', '直接材料成本', 'amount', False),
    ('dm_unit_cost', '单位直接材料成本', 'price', True),
    ('dm_contrib', '直接材料贡献率', 'pct', True),
    ('dl_cost', '直接人工成本', 'amount', False),
    ('dl_unit_cost', '单位直接人工成本', 'price', True),
    ('dl_contrib', '直接人工贡献率', 'pct', True),
    ('moh_cost', '制造费用成本', 'amount', False),
    ('moh_unit_cost', '单位制造费用成本', 'price', True),
    ('moh_contrib', '制造费用贡献率', 'pct', True),
]


def _to_decimal(value: object) -> Decimal | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, AttributeError, ValueError):
        return None


def _sum_decimal(values: list[object]) -> Decimal:
    total = ZERO
    for value in values:
        decimal_value = _to_decimal(value)
        if decimal_value is None:
            continue
        total += decimal_value
    return total


def _sum_decimal_series(series: pd.Series) -> Decimal:
    return _sum_decimal(series.tolist())


def _first_decimal(values: pd.Series) -> Decimal | None:
    for value in values:
        decimal_value = _to_decimal(value)
        if decimal_value is not None:
            return decimal_value
    return None


def _safe_divide(numerator: object, denominator: object) -> Decimal | None:
    num = _to_decimal(numerator)
    den = _to_decimal(denominator)
    if num is None or den in (None, ZERO):
        return None
    return num / den


def _subtract_decimal(lhs: object, rhs: object) -> Decimal | None:
    left = _to_decimal(lhs)
    right = _to_decimal(rhs)
    if left is None or right is None:
        return None
    return left - right


def _add_decimal(lhs: object, rhs: object) -> Decimal | None:
    left = _to_decimal(lhs)
    right = _to_decimal(rhs)
    if left is None or right is None:
        return None
    return left + right


def _decimal_abs(value: object) -> Decimal | None:
    decimal_value = _to_decimal(value)
    if decimal_value is None:
        return None
    return abs(decimal_value)


def _normalize_period(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    match = re.search(r'(\d{4})\D*(\d{1,2})', text)
    if not match:
        return None
    year = int(match.group(1))
    month = int(match.group(2))
    if month < 1 or month > 12:
        return None
    return f'{year:04d}-{month:02d}'


def _period_to_display(period: object) -> str:
    normalized = _normalize_period(period)
    if normalized is None:
        return ''
    year, month = normalized.split('-')
    return f'{year}年{month}期'


def _resolve_period_column(df: pd.DataFrame) -> str:
    if '月份' in df.columns:
        return '月份'
    if '年期' in df.columns:
        return '年期'
    raise ValueError("缺少周期字段，必须包含 '月份' 或 '年期'")


def _map_cost_bucket(cost_item: object) -> str | None:
    if cost_item is None or pd.isna(cost_item):
        return None

    text = str(cost_item).strip()
    if text == '直接材料':
        return 'direct_material'
    if text == '直接人工':
        return 'direct_labor'
    if text.startswith('制造费用'):
        return 'moh'
    return None


def _empty_error_log() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            'row_id',
            'cost_bucket',
            'product_code',
            'product_name',
            'period',
            'issue_type',
            'field_name',
            'original_value',
            'lhs',
            'rhs',
            'diff',
            'reason',
            'action',
            'retryable',
        ]
    )


def _build_error_frame(
    data: pd.DataFrame,
    issue_type: str,
    field_name: str,
    reason: str,
    action: str,
    *,
    original_column: str | None = None,
    lhs_column: str | None = None,
    rhs_column: str | None = None,
    diff_column: str | None = None,
) -> pd.DataFrame:
    if data.empty:
        return _empty_error_log()

    frame = pd.DataFrame(index=data.index)
    frame['product_code'] = data.get('product_code')
    frame['product_name'] = data.get('product_name')
    frame['period'] = data.get('period')
    frame['cost_bucket'] = data.get('cost_bucket')
    frame['issue_type'] = issue_type
    frame['field_name'] = field_name
    frame['reason'] = reason
    frame['action'] = action
    frame['retryable'] = False
    frame['original_value'] = data[original_column] if original_column else None
    frame['lhs'] = data[lhs_column] if lhs_column else None
    frame['rhs'] = data[rhs_column] if rhs_column else None
    frame['diff'] = data[diff_column] if diff_column else None
    frame['row_id'] = (
        frame['product_code'].astype(str) + '|' + frame['period'].astype(str) + '|' + frame['cost_bucket'].astype(str)
    )
    return frame[
        [
            'row_id',
            'cost_bucket',
            'product_code',
            'product_name',
            'period',
            'issue_type',
            'field_name',
            'original_value',
            'lhs',
            'rhs',
            'diff',
            'reason',
            'action',
            'retryable',
        ]
    ].reset_index(drop=True)


def build_fact_cost_pq(df_detail: pd.DataFrame, df_qty: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """构建标准长表 fact_cost_pq，并输出准备阶段 error_log。"""
    detail_period_col = _resolve_period_column(df_detail)
    qty_period_col = _resolve_period_column(df_qty)

    required_detail_cols = {'产品编码', '产品名称', '成本项目名称', '本期完工金额'}
    missing_detail_cols = required_detail_cols.difference(df_detail.columns)
    if missing_detail_cols:
        missing = ', '.join(sorted(missing_detail_cols))
        raise ValueError(f'成本明细缺少必要字段: {missing}')

    required_qty_cols = {'产品编码', '产品名称', '本期完工数量'}
    missing_qty_cols = required_qty_cols.difference(df_qty.columns)
    if missing_qty_cols:
        missing = ', '.join(sorted(missing_qty_cols))
        raise ValueError(f'产品数量统计缺少必要字段: {missing}')

    detail = df_detail.copy().rename(
        columns={'产品编码': 'product_code', '产品名称': 'product_name', '成本项目名称': 'cost_item'}
    )
    detail['period'] = detail[detail_period_col].map(_normalize_period)
    detail['cost_bucket'] = detail['cost_item'].map(_map_cost_bucket)
    detail['amount'] = detail['本期完工金额'].map(_to_decimal)
    if '本期完工单位成本' in detail.columns:
        detail['source_price'] = detail['本期完工单位成本'].map(_to_decimal)
    else:
        detail['source_price'] = None

    qty = df_qty.copy().rename(columns={'产品编码': 'product_code', '产品名称': 'product_name'})
    qty['period'] = qty[qty_period_col].map(_normalize_period)
    qty['qty'] = qty['本期完工数量'].map(_to_decimal)

    prep_errors: list[pd.DataFrame] = []

    unmapped_mask = detail['cost_bucket'].isna()
    unmapped_frame = detail.loc[unmapped_mask, ['product_code', 'product_name', 'period', 'cost_item']].copy()
    if not unmapped_frame.empty:
        prep_errors.append(
            _build_error_frame(
                unmapped_frame,
                issue_type='UNMAPPED_COST_ITEM',
                field_name='成本项目名称',
                original_column='cost_item',
                reason='成本项目未映射到 direct_material/direct_labor/moh',
                action='该行已从三大类报表中排除',
            )
        )

    detail_mapped = detail.loc[~unmapped_mask].copy()

    amount_grouped = detail_mapped.groupby(
        ['product_code', 'product_name', 'period', 'cost_bucket'], dropna=False, as_index=False, sort=False
    ).agg(
        amount=('amount', _sum_decimal_series),
        source_price=('source_price', _first_decimal),
    )

    qty_grouped = qty.groupby(['product_code', 'product_name', 'period'], dropna=False, as_index=False, sort=False).agg(
        qty=('qty', _sum_decimal_series)
    )

    keys = pd.concat(
        [
            qty_grouped[['product_code', 'product_name', 'period']],
            amount_grouped[['product_code', 'product_name', 'period']],
        ],
        ignore_index=True,
    ).drop_duplicates()

    if keys.empty:
        fact_empty = pd.DataFrame(
            columns=['period', 'product_code', 'product_name', 'cost_bucket', 'amount', 'qty', 'price', 'source_price']
        )
        return fact_empty, pd.concat(prep_errors, ignore_index=True) if prep_errors else _empty_error_log()

    bucket_df = pd.DataFrame({'cost_bucket': list(COST_BUCKETS)})
    keys = keys.assign(_join_key=1)
    bucket_df = bucket_df.assign(_join_key=1)
    fact = keys.merge(bucket_df, on='_join_key', how='inner').drop(columns=['_join_key'])
    fact = fact.merge(amount_grouped, on=['product_code', 'product_name', 'period', 'cost_bucket'], how='left')
    fact = fact.merge(qty_grouped, on=['product_code', 'product_name', 'period'], how='left')

    missing_amount = fact['amount'].isna()
    if missing_amount.any():
        prep_errors.append(
            _build_error_frame(
                fact.loc[missing_amount, ['product_code', 'product_name', 'period', 'cost_bucket']],
                issue_type='MISSING_AMOUNT',
                field_name='本期完工金额',
                reason='该产品+月份+成本类别缺少金额明细',
                action='金额按 0 填充继续计算',
            )
        )
        fact.loc[missing_amount, 'amount'] = ZERO

    missing_qty = fact['qty'].isna()
    if missing_qty.any():
        prep_errors.append(
            _build_error_frame(
                fact.loc[missing_qty, ['product_code', 'product_name', 'period', 'cost_bucket']],
                issue_type='MISSING_QTY',
                field_name='本期完工数量',
                reason='该产品+月份缺少数量信息',
                action='保留空值并在单价展示为空',
            )
        )

    zero_qty = fact['qty'].map(lambda value: value == ZERO if isinstance(value, Decimal) else False)
    if zero_qty.any():
        prep_errors.append(
            _build_error_frame(
                fact.loc[zero_qty, ['product_code', 'product_name', 'period', 'cost_bucket', 'qty']],
                issue_type='ZERO_QTY',
                field_name='本期完工数量',
                original_column='qty',
                reason='数量为 0 时单价不可计算',
                action='price 置空',
            )
        )

    fact['price'] = fact['amount'].combine(fact['qty'], _safe_divide)

    source_price_comparable = fact['source_price'].notna() & fact['price'].notna()
    source_diff = fact['price'].combine(fact['source_price'], _subtract_decimal).map(_decimal_abs)
    source_mismatch = source_price_comparable & source_diff.map(
        lambda value: value is not None and value > PRICE_DIFF_TOLERANCE
    )
    if source_mismatch.any():
        mismatched = fact.loc[
            source_mismatch,
            ['product_code', 'product_name', 'period', 'cost_bucket', 'price', 'source_price'],
        ].copy()
        mismatched['price_diff'] = source_diff[source_mismatch]
        prep_errors.append(
            _build_error_frame(
                mismatched,
                issue_type='PRICE_MISMATCH',
                field_name='本期完工单位成本',
                reason='amount/qty 重算单价与源单价偏差超阈值',
                action='保留重算单价并记录差异',
                lhs_column='price',
                rhs_column='source_price',
                diff_column='price_diff',
            )
        )

    fact = fact[['period', 'product_code', 'product_name', 'cost_bucket', 'amount', 'qty', 'price', 'source_price']]
    fact = fact.reset_index(drop=True)

    error_log = pd.concat(prep_errors, ignore_index=True) if prep_errors else _empty_error_log()
    return fact, error_log


def _build_pivot(bucket_df: pd.DataFrame, value_col: str, period_columns: list[str]) -> pd.DataFrame:
    pivot = bucket_df.pivot_table(
        index=['product_code', 'product_name'],
        columns='period_display',
        values=value_col,
        aggfunc='first',
        sort=False,
    )
    pivot = pivot.reindex(columns=period_columns).reset_index()
    return pivot


def _append_total_row(df: pd.DataFrame, value_columns: list[str], summary_col: str) -> pd.DataFrame:
    total_row: dict[str, object] = {'产品编码': '总计', '产品名称': ''}
    for col in value_columns + [summary_col]:
        total_row[col] = _sum_decimal_series(df[col])
    return pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)


def _build_section_blocks(bucket_df: pd.DataFrame, title_prefix: str) -> list[SectionBlock]:
    period_keys = sorted(bucket_df['period'].dropna().unique().tolist())
    period_columns = [_period_to_display(period) for period in period_keys]

    amount_pivot = _build_pivot(bucket_df, 'amount', period_columns).rename(
        columns={'product_code': '产品编码', 'product_name': '产品名称'}
    )
    qty_pivot = _build_pivot(bucket_df, 'qty', period_columns).rename(
        columns={'product_code': '产品编码', 'product_name': '产品名称'}
    )

    for col in period_columns:
        amount_pivot[col] = amount_pivot[col].map(_to_decimal).map(lambda value: value if value is not None else ZERO)
        qty_pivot[col] = qty_pivot[col].map(_to_decimal)

    amount_pivot['总计'] = amount_pivot[period_columns].apply(lambda row: _sum_decimal(row.tolist()), axis=1)
    qty_pivot['总计'] = qty_pivot[period_columns].apply(lambda row: _sum_decimal(row.tolist()), axis=1)

    price_pivot = amount_pivot[['产品编码', '产品名称']].copy()
    for col in period_columns:
        price_pivot[col] = amount_pivot[col].combine(qty_pivot[col], _safe_divide)
    price_pivot['均值'] = amount_pivot['总计'].combine(qty_pivot['总计'], _safe_divide)

    amount_with_total = _append_total_row(amount_pivot, period_columns, '总计')
    qty_with_total = _append_total_row(qty_pivot, period_columns, '总计')

    return [
        SectionBlock(
            title=f'{title_prefix}完工金额',
            data=amount_with_total,
            metric_type='amount',
            has_total_row=True,
        ),
        SectionBlock(
            title=f'{title_prefix}完工数量',
            data=qty_with_total,
            metric_type='qty',
            has_total_row=True,
        ),
        SectionBlock(
            title=f'{title_prefix}完工单价',
            data=price_pivot,
            metric_type='price',
            has_total_row=False,
        ),
    ]


def _build_product_metric_df(fact_df: pd.DataFrame) -> pd.DataFrame:
    if fact_df.empty:
        columns = [
            'product_code',
            'product_name',
            'period',
            'period_display',
            *[field[0] for field in PRODUCT_ANALYSIS_FIELDS],
        ]
        return pd.DataFrame(columns=columns)

    amount_by_bucket = (
        fact_df.groupby(
            ['product_code', 'product_name', 'period', 'cost_bucket'], dropna=False, as_index=False, sort=False
        )
        .agg(amount=('amount', _sum_decimal_series))
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
    amount_by_bucket['direct_material'] = (
        amount_by_bucket['direct_material'].map(_to_decimal).map(lambda value: value if value is not None else ZERO)
    )
    amount_by_bucket['direct_labor'] = (
        amount_by_bucket['direct_labor'].map(_to_decimal).map(lambda value: value if value is not None else ZERO)
    )
    amount_by_bucket['moh'] = (
        amount_by_bucket['moh'].map(_to_decimal).map(lambda value: value if value is not None else ZERO)
    )

    qty_by_product = fact_df.groupby(
        ['product_code', 'product_name', 'period'], dropna=False, as_index=False, sort=False
    ).agg(completed_qty=('qty', _first_decimal))

    metric_df = amount_by_bucket.merge(
        qty_by_product,
        on=['product_code', 'product_name', 'period'],
        how='left',
    )

    metric_df = metric_df.rename(
        columns={
            'direct_material': 'dm_cost',
            'direct_labor': 'dl_cost',
            'moh': 'moh_cost',
        }
    )

    metric_df['total_cost'] = (
        metric_df['dm_cost']
        .combine(metric_df['dl_cost'], _add_decimal)
        .combine(
            metric_df['moh_cost'],
            _add_decimal,
        )
    )
    metric_df['unit_cost'] = metric_df['total_cost'].combine(metric_df['completed_qty'], _safe_divide)
    metric_df['dm_unit_cost'] = metric_df['dm_cost'].combine(metric_df['completed_qty'], _safe_divide)
    metric_df['dl_unit_cost'] = metric_df['dl_cost'].combine(metric_df['completed_qty'], _safe_divide)
    metric_df['moh_unit_cost'] = metric_df['moh_cost'].combine(metric_df['completed_qty'], _safe_divide)
    metric_df['dm_contrib'] = metric_df['dm_cost'].combine(metric_df['total_cost'], _safe_divide)
    metric_df['dl_contrib'] = metric_df['dl_cost'].combine(metric_df['total_cost'], _safe_divide)
    metric_df['moh_contrib'] = metric_df['moh_cost'].combine(metric_df['total_cost'], _safe_divide)
    metric_df['period_display'] = metric_df['period'].map(_period_to_display)
    return metric_df.reset_index(drop=True)


def build_product_anomaly_sections(fact_df: pd.DataFrame) -> list[ProductAnomalySection]:
    """按产品构建异常分析分段。"""
    metric_df = _build_product_metric_df(fact_df)
    if metric_df.empty:
        return []

    sections: list[ProductAnomalySection] = []

    grouped = metric_df.groupby(['product_code', 'product_name'], dropna=False, sort=False)
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
    """按成本类别输出三段块数据（金额/数量/单价）。"""
    if fact_df.empty:
        empty = pd.DataFrame(columns=['产品编码', '产品名称'])
        empty_sections = [
            SectionBlock('完工金额', empty.copy(), 'amount', True),
            SectionBlock('完工数量', empty.copy(), 'qty', True),
            SectionBlock('完工单价', empty.copy(), 'price', False),
        ]
        return {
            '直接材料_价量比': empty_sections,
            '直接人工_价量比': empty_sections,
            '制造费用_价量比': empty_sections,
        }

    source = fact_df.copy()
    source['period_display'] = source['period'].map(_period_to_display)

    dm_sections = _build_section_blocks(source[source['cost_bucket'] == 'direct_material'], '直接材料')
    dl_sections = _build_section_blocks(source[source['cost_bucket'] == 'direct_labor'], '直接人工')
    moh_sections = _build_section_blocks(source[source['cost_bucket'] == 'moh'], '制造费用')

    return {
        '直接材料_价量比': dm_sections,
        '直接人工_价量比': dl_sections,
        '制造费用_价量比': moh_sections,
    }

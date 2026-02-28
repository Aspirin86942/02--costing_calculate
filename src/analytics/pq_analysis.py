"""价量分解分析：构建标准长表、计算分解指标、渲染宽表。"""

from __future__ import annotations

import logging
import re
from decimal import Decimal, InvalidOperation

import pandas as pd

logger = logging.getLogger(__name__)

ZERO = Decimal('0')
RECON_TOLERANCE = Decimal('0.01')
COST_BUCKETS = ('direct_material', 'direct_labor', 'moh')


def _to_decimal(value: object) -> Decimal | None:
    """将输入值转换为 Decimal，无法解析时返回 None。"""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, AttributeError, ValueError):
        return None


def _sum_decimal(values: pd.Series) -> Decimal:
    """聚合 Decimal 列时忽略空值，并保持 Decimal 精度。"""
    total = ZERO
    has_value = False
    for value in values:
        decimal_value = _to_decimal(value)
        if decimal_value is None:
            continue
        total += decimal_value
        has_value = True
    return total if has_value else ZERO


def _first_decimal(values: pd.Series) -> Decimal | None:
    for value in values:
        decimal_value = _to_decimal(value)
        if decimal_value is not None:
            return decimal_value
    return None


def _mean_decimal(values: pd.Series) -> Decimal | None:
    total = ZERO
    count = 0
    for value in values:
        decimal_value = _to_decimal(value)
        if decimal_value is None:
            continue
        total += decimal_value
        count += 1
    if count == 0:
        return None
    return total / Decimal(count)


def _subtract_decimal(lhs: Decimal | None, rhs: Decimal | None) -> Decimal | None:
    left = _to_decimal(lhs)
    right = _to_decimal(rhs)
    if left is None or right is None:
        return None
    return left - right


def _add_decimal(lhs: Decimal | None, rhs: Decimal | None) -> Decimal | None:
    left = _to_decimal(lhs)
    right = _to_decimal(rhs)
    if left is None or right is None:
        return None
    return left + right


def _multiply_decimal(lhs: Decimal | None, rhs: Decimal | None) -> Decimal | None:
    left = _to_decimal(lhs)
    right = _to_decimal(rhs)
    if left is None or right is None:
        return None
    return left * right


def _safe_divide(numerator: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    num = _to_decimal(numerator)
    den = _to_decimal(denominator)
    if num is None or den in (None, ZERO):
        return None
    return num / den


def _decimal_abs(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return abs(value)


def _normalize_period(value: object) -> str | None:
    """统一月份为 YYYY-MM 口径，后续排序与透视更稳定。"""
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
    """构建标准长表 fact_cost_pq，并输出准备阶段的 error_log。"""
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

    detail = df_detail.copy()
    detail = detail.rename(
        columns={'产品编码': 'product_code', '产品名称': 'product_name', '成本项目名称': 'cost_item'}
    )
    detail['period'] = detail[detail_period_col].map(_normalize_period)
    detail['cost_bucket'] = detail['cost_item'].map(_map_cost_bucket)
    detail['amount'] = detail['本期完工金额'].map(_to_decimal)

    if '本期完工单位成本' in detail.columns:
        detail['source_price'] = detail['本期完工单位成本'].map(_to_decimal)
    else:
        detail['source_price'] = None

    qty = df_qty.copy()
    qty = qty.rename(columns={'产品编码': 'product_code', '产品名称': 'product_name'})
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
                action='该行已从三大类价量分析中排除',
            )
        )

    detail_mapped = detail.loc[~unmapped_mask].copy()

    amount_grouped = (
        detail_mapped.groupby(['product_code', 'product_name', 'period', 'cost_bucket'], dropna=False, as_index=False)
        .agg(
            amount=('amount', _sum_decimal),
            source_price=('source_price', _first_decimal),
        )
        .sort_values(['product_code', 'period', 'cost_bucket'])
    )

    qty_grouped = (
        qty.groupby(['product_code', 'product_name', 'period'], dropna=False, as_index=False)
        .agg(qty=('qty', _sum_decimal))
        .sort_values(['product_code', 'period'])
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
                action='保留空值并在分析层输出空结果',
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
                action='price 置空，后续指标按空处理',
            )
        )

    fact['price'] = fact['amount'].combine(fact['qty'], _safe_divide)

    source_price_comparable = fact['source_price'].notna() & fact['price'].notna()
    source_diff = fact['price'].combine(fact['source_price'], _subtract_decimal).map(_decimal_abs)
    source_mismatch = source_price_comparable & source_diff.map(
        lambda value: value is not None and value > RECON_TOLERANCE
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
                action='保留重算单价并记录审计差异',
                lhs_column='price',
                rhs_column='source_price',
                diff_column='price_diff',
            )
        )

    fact = fact.sort_values(['product_code', 'cost_bucket', 'period']).reset_index(drop=True)
    fact = fact[['period', 'product_code', 'product_name', 'cost_bucket', 'amount', 'qty', 'price', 'source_price']]

    error_log = pd.concat(prep_errors, ignore_index=True) if prep_errors else _empty_error_log()
    return fact, error_log


def compute_pq_variance(fact_df: pd.DataFrame, base_mode: str = 'prev_period') -> tuple[pd.DataFrame, pd.DataFrame]:
    """根据标准长表计算价量分解指标。"""
    if base_mode not in {'prev_period', 'year_avg'}:
        raise ValueError("base_mode 仅支持 'prev_period' 或 'year_avg'")

    if fact_df.empty:
        variance_empty = pd.DataFrame(
            columns=[
                'period',
                'product_code',
                'product_name',
                'cost_bucket',
                'qty',
                'price',
                'amount',
                'Q0',
                'P0',
                'A0',
                'PV',
                'QV',
                'IV',
                'delta',
                'recon_diff',
                'expected_amount',
                'gap',
                'no_base',
            ]
        )
        return variance_empty, _empty_error_log()

    variance = fact_df.copy()
    variance = variance.sort_values(['product_code', 'cost_bucket', 'period']).reset_index(drop=True)
    grouped = variance.groupby(['product_code', 'cost_bucket'], dropna=False)

    if base_mode == 'prev_period':
        variance['P0'] = grouped['price'].shift(1)
        variance['Q0'] = grouped['qty'].shift(1)
    else:
        variance['P0'] = grouped['price'].transform(lambda series: _mean_decimal(series))
        variance['Q0'] = grouped['qty'].transform(lambda series: _mean_decimal(series))

    no_base_mask = variance['P0'].isna() | variance['Q0'].isna()
    variance['no_base'] = no_base_mask.astype(int)
    variance.loc[no_base_mask, 'P0'] = ZERO
    variance.loc[no_base_mask, 'Q0'] = ZERO

    price_diff = variance['price'].combine(variance['P0'], _subtract_decimal)
    qty_diff = variance['qty'].combine(variance['Q0'], _subtract_decimal)

    variance['A0'] = variance['P0'].combine(variance['Q0'], _multiply_decimal)
    variance['PV'] = price_diff.combine(variance['qty'], _multiply_decimal)
    variance['QV'] = qty_diff.combine(variance['P0'], _multiply_decimal)
    variance['delta'] = variance['amount'].combine(variance['A0'], _subtract_decimal)
    # 这里用残差定义交叉项，保证 delta 可被 PV/QV/IV 严格勾稽，避免报表解释出现“对不上”的争议。
    variance['IV'] = variance['delta'].combine(
        variance['PV'].combine(variance['QV'], _add_decimal),
        _subtract_decimal,
    )

    pq_sum = variance['PV'].combine(variance['QV'], _add_decimal).combine(variance['IV'], _add_decimal)
    variance['recon_diff'] = variance['delta'].combine(pq_sum, _subtract_decimal)
    variance['expected_amount'] = variance['P0'].combine(variance['qty'], _multiply_decimal)
    variance['gap'] = variance['amount'].combine(variance['expected_amount'], _subtract_decimal)

    error_frames: list[pd.DataFrame] = []

    missing_actual_mask = variance['qty'].isna() | variance['amount'].isna()
    if missing_actual_mask.any():
        error_frames.append(
            _build_error_frame(
                variance.loc[missing_actual_mask, ['product_code', 'product_name', 'period', 'cost_bucket']],
                issue_type='MISSING_ACTUAL',
                field_name='qty/amount',
                reason='缺少实际数量或金额，部分分解指标为空',
                action='保留该行并输出空指标',
            )
        )

    recon_issue_mask = variance['recon_diff'].map(lambda value: value is not None and abs(value) > RECON_TOLERANCE)
    if recon_issue_mask.any():
        recon_issue_frame = variance.loc[
            recon_issue_mask,
            ['product_code', 'product_name', 'period', 'cost_bucket', 'delta', 'PV', 'QV', 'IV', 'recon_diff'],
        ].copy()
        recon_issue_frame['rhs'] = (
            recon_issue_frame['PV']
            .combine(recon_issue_frame['QV'], _add_decimal)
            .combine(recon_issue_frame['IV'], _add_decimal)
        )
        error_frames.append(
            _build_error_frame(
                recon_issue_frame,
                issue_type='RECON_MISMATCH',
                field_name='delta_vs_components',
                reason='delta 与 PV/QV/IV 勾稽差异超阈值',
                action='保留结果并输出审计差异',
                lhs_column='delta',
                rhs_column='rhs',
                diff_column='recon_diff',
            )
        )

    variance = variance[
        [
            'period',
            'product_code',
            'product_name',
            'cost_bucket',
            'qty',
            'price',
            'amount',
            'Q0',
            'P0',
            'A0',
            'PV',
            'QV',
            'IV',
            'delta',
            'recon_diff',
            'expected_amount',
            'gap',
            'no_base',
        ]
    ]

    error_log = pd.concat(error_frames, ignore_index=True) if error_frames else _empty_error_log()
    return variance, error_log


def _pivot_by_period(source_df: pd.DataFrame, metrics: list[str]) -> pd.DataFrame:
    if source_df.empty:
        return pd.DataFrame(columns=['product_code', 'product_name'])

    available_metrics = [metric for metric in metrics if metric in source_df.columns]
    data = source_df[['product_code', 'product_name', 'period_display', *available_metrics]].copy()
    wide = data.pivot_table(
        index=['product_code', 'product_name'],
        columns='period_display',
        values=available_metrics,
        aggfunc='first',
        sort=True,
    )

    wide = wide.swaplevel(0, 1, axis=1).sort_index(axis=1, level=[0, 1])
    wide.columns = [f'{period}_{metric}' for period, metric in wide.columns]
    return wide.reset_index()


def render_tables(variance_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """将分析结果渲染为三张宽表。"""
    if variance_df.empty:
        empty = pd.DataFrame(columns=['product_code', 'product_name'])
        return {
            '直接材料_价量比': empty.copy(),
            '直接人工_价量比': empty.copy(),
            '制造费用_价量比': empty.copy(),
        }

    source = variance_df.copy()
    source['period_display'] = source['period'].map(_period_to_display)

    # 三张业务表按需求仅展示当期 amount/price/qty。
    dm_metrics = ['qty', 'price', 'amount']
    dl_metrics = ['qty', 'price', 'amount']
    moh_metrics = ['qty', 'price', 'amount']

    dm_df = _pivot_by_period(source[source['cost_bucket'] == 'direct_material'], dm_metrics)
    dl_df = _pivot_by_period(source[source['cost_bucket'] == 'direct_labor'], dl_metrics)
    moh_df = _pivot_by_period(source[source['cost_bucket'] == 'moh'], moh_metrics)

    return {
        '直接材料_价量比': dm_df,
        '直接人工_价量比': dl_df,
        '制造费用_价量比': moh_df,
    }

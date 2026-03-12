"""价量分析与 V3 工单异常分析模块。"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

import pandas as pd

ZERO = Decimal('0')
COST_BUCKETS = ('direct_material', 'direct_labor', 'moh')
WORK_ORDER_KEY_COLS = ['period', 'product_code', 'order_no', 'order_line']

QTY_DM_AMOUNT = '本期完工直接材料合计完工金额'
QTY_DL_AMOUNT = '本期完工直接人工合计完工金额'
QTY_MOH_AMOUNT = '本期完工制造费用合计完工金额'
QTY_MOH_OTHER_AMOUNT = '本期完工制造费用_其他合计完工金额'
QTY_MOH_LABOR_AMOUNT = '本期完工制造费用_人工合计完工金额'
QTY_MOH_CONSUMABLES_AMOUNT = '本期完工制造费用_机物料及低耗合计完工金额'
QTY_MOH_DEPRECIATION_AMOUNT = '本期完工制造费用_折旧合计完工金额'
QTY_MOH_UTILITIES_AMOUNT = '本期完工制造费用_水电费合计完工金额'
QTY_DM_UNIT_COST = '直接材料单位完工金额'
QTY_DL_UNIT_COST = '直接人工单位完工金额'
QTY_MOH_UNIT_COST = '制造费用单位完工金额'
QTY_MOH_OTHER_UNIT_COST = '制造费用_其他单位完工成本'
QTY_MOH_LABOR_UNIT_COST = '制造费用_人工单位完工成本'
QTY_MOH_CONSUMABLES_UNIT_COST = '制造费用_机物料及低耗单位完工成本'
QTY_MOH_DEPRECIATION_UNIT_COST = '制造费用_折旧单位完工成本'
QTY_MOH_UTILITIES_UNIT_COST = '制造费用_水电费单位完工成本'
QTY_VALID_QTY = '完工数量是否有效'
QTY_QTY_NON_POSITIVE = '完工数量是否小于等于0'
QTY_HAS_NULL = '是否存在空值'
QTY_MOH_MATCH = '制造费用明细项合计是否等于制造费用合计'
QTY_TOTAL_MATCH = '直接材料+直接人工+制造费用是否等于总完工成本'
QTY_CHECK_STATUS = '数据校验状态'
QTY_CHECK_REASON = '异常原因说明'

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

ANOMALY_METRICS = [
    ('total_unit_cost', '总单位完工成本', '总成本异常标记', '总成本异常'),
    ('dm_unit_cost', '直接材料单位完工成本', '直接材料异常标记', '材料异常'),
    ('dl_unit_cost', '直接人工单位完工成本', '直接人工异常标记', '人工异常'),
    ('moh_unit_cost', '制造费用单位完工成本', '制造费用异常标记', '制造费用异常'),
    ('moh_other_unit_cost', '制造费用_其他单位完工成本', '制造费用_其他异常标记', '其他异常'),
    ('moh_labor_unit_cost', '制造费用_人工单位完工成本', '制造费用_人工异常标记', '制造费用人工异常'),
    (
        'moh_consumables_unit_cost',
        '制造费用_机物料及低耗单位完工成本',
        '制造费用_机物料及低耗异常标记',
        '机物料及低耗异常',
    ),
    ('moh_depreciation_unit_cost', '制造费用_折旧单位完工成本', '制造费用_折旧异常标记', '折旧异常'),
    ('moh_utilities_unit_cost', '制造费用_水电费单位完工成本', '制造费用_水电费异常标记', '水电费异常'),
]

BROAD_COST_BUCKET_MAP = {
    '直接材料': 'direct_material',
    '直接人工': 'direct_labor',
}

MOH_COMPONENT_MAP = {
    '制造费用_其他': 'moh_other_amount',
    '制造费用-人工': 'moh_labor_amount',
    '制造费用_机物料及低耗': 'moh_consumables_amount',
    '制造费用_折旧': 'moh_depreciation_amount',
    '制造费用_水电费': 'moh_utilities_amount',
}

WORK_ORDER_OUTPUT_COLUMNS = [
    '月份',
    '成本中心',
    '产品编码',
    '产品名称',
    '规格型号',
    '工单编号',
    '工单行',
    '基本单位',
    '本期完工数量',
    '总完工成本',
    '直接材料合计完工金额',
    '直接人工合计完工金额',
    '制造费用合计完工金额',
    '制造费用_其他合计完工金额',
    '制造费用_人工合计完工金额',
    '制造费用_机物料及低耗合计完工金额',
    '制造费用_折旧合计完工金额',
    '制造费用_水电费合计完工金额',
    '总单位完工成本',
    '直接材料单位完工成本',
    '直接人工单位完工成本',
    '制造费用单位完工成本',
    '制造费用_其他单位完工成本',
    '制造费用_人工单位完工成本',
    '制造费用_机物料及低耗单位完工成本',
    '制造费用_折旧单位完工成本',
    '制造费用_水电费单位完工成本',
    'log_总单位完工成本',
    'log_直接材料单位完工成本',
    'log_直接人工单位完工成本',
    'log_制造费用单位完工成本',
    'log_制造费用_其他单位完工成本',
    'log_制造费用_人工单位完工成本',
    'log_制造费用_机物料及低耗单位完工成本',
    'log_制造费用_折旧单位完工成本',
    'log_制造费用_水电费单位完工成本',
    'Modified Z-score_总单位完工成本',
    'Modified Z-score_直接材料',
    'Modified Z-score_直接人工',
    'Modified Z-score_制造费用',
    'Modified Z-score_制造费用_其他',
    'Modified Z-score_制造费用_人工',
    'Modified Z-score_制造费用_机物料及低耗',
    'Modified Z-score_制造费用_折旧',
    'Modified Z-score_制造费用_水电费',
    '是否可参与分析',
    '总成本异常标记',
    '直接材料异常标记',
    '直接人工异常标记',
    '制造费用异常标记',
    '制造费用_其他异常标记',
    '制造费用_人工异常标记',
    '制造费用_机物料及低耗异常标记',
    '制造费用_折旧异常标记',
    '制造费用_水电费异常标记',
    '异常等级',
    '异常主要来源',
    '复核原因',
]

WORK_ORDER_COLUMN_TYPES = {
    '月份': 'text',
    '成本中心': 'text',
    '产品编码': 'text',
    '产品名称': 'text',
    '规格型号': 'text',
    '工单编号': 'text',
    '工单行': 'text',
    '基本单位': 'text',
    '本期完工数量': 'qty',
    '总完工成本': 'amount',
    '直接材料合计完工金额': 'amount',
    '直接人工合计完工金额': 'amount',
    '制造费用合计完工金额': 'amount',
    '制造费用_其他合计完工金额': 'amount',
    '制造费用_人工合计完工金额': 'amount',
    '制造费用_机物料及低耗合计完工金额': 'amount',
    '制造费用_折旧合计完工金额': 'amount',
    '制造费用_水电费合计完工金额': 'amount',
    '总单位完工成本': 'price',
    '直接材料单位完工成本': 'price',
    '直接人工单位完工成本': 'price',
    '制造费用单位完工成本': 'price',
    '制造费用_其他单位完工成本': 'price',
    '制造费用_人工单位完工成本': 'price',
    '制造费用_机物料及低耗单位完工成本': 'price',
    '制造费用_折旧单位完工成本': 'price',
    '制造费用_水电费单位完工成本': 'price',
    'log_总单位完工成本': 'score',
    'log_直接材料单位完工成本': 'score',
    'log_直接人工单位完工成本': 'score',
    'log_制造费用单位完工成本': 'score',
    'log_制造费用_其他单位完工成本': 'score',
    'log_制造费用_人工单位完工成本': 'score',
    'log_制造费用_机物料及低耗单位完工成本': 'score',
    'log_制造费用_折旧单位完工成本': 'score',
    'log_制造费用_水电费单位完工成本': 'score',
    'Modified Z-score_总单位完工成本': 'score',
    'Modified Z-score_直接材料': 'score',
    'Modified Z-score_直接人工': 'score',
    'Modified Z-score_制造费用': 'score',
    'Modified Z-score_制造费用_其他': 'score',
    'Modified Z-score_制造费用_人工': 'score',
    'Modified Z-score_制造费用_机物料及低耗': 'score',
    'Modified Z-score_制造费用_折旧': 'score',
    'Modified Z-score_制造费用_水电费': 'score',
    '是否可参与分析': 'text',
    '总成本异常标记': 'text',
    '直接材料异常标记': 'text',
    '直接人工异常标记': 'text',
    '制造费用异常标记': 'text',
    '制造费用_其他异常标记': 'text',
    '制造费用_人工异常标记': 'text',
    '制造费用_机物料及低耗异常标记': 'text',
    '制造费用_折旧异常标记': 'text',
    '制造费用_水电费异常标记': 'text',
    '异常等级': 'text',
    '异常主要来源': 'text',
    '复核原因': 'text',
}


@dataclass
class SectionBlock:
    """单个价量分析分段。"""

    title: str
    data: pd.DataFrame
    metric_type: str
    has_total_row: bool


@dataclass
class ProductAnomalySection:
    """单个产品兼容摘要分段。"""

    product_code: str
    product_name: str
    data: pd.DataFrame
    column_types: dict[str, str]
    amount_columns: list[str]
    outlier_cells: set[tuple[int, str]]


@dataclass
class FlatSheet:
    """普通平铺 sheet 数据。"""

    data: pd.DataFrame
    column_types: dict[str, str]


@dataclass
class AnalysisArtifacts:
    """V3 分析输出产物。"""

    fact_df: pd.DataFrame
    qty_sheet_df: pd.DataFrame
    work_order_sheet: FlatSheet
    product_anomaly_sections: list[ProductAnomalySection]
    quality_sheet: FlatSheet
    error_log: pd.DataFrame


def _to_decimal(value: object) -> Decimal | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value).strip())
    except (AttributeError, InvalidOperation, ValueError):
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


def _add_decimal(lhs: object, rhs: object) -> Decimal | None:
    left = _to_decimal(lhs)
    right = _to_decimal(rhs)
    if left is None or right is None:
        return None
    return left + right


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


def _map_broad_cost_bucket(cost_item: object) -> str | None:
    if cost_item is None or pd.isna(cost_item):
        return None
    text = str(cost_item).strip()
    if text in BROAD_COST_BUCKET_MAP:
        return BROAD_COST_BUCKET_MAP[text]
    if text.startswith('制造费用'):
        return 'moh'
    return None


def _map_component_bucket(cost_item: object) -> str | None:
    if cost_item is None or pd.isna(cost_item):
        return None
    return MOH_COMPONENT_MAP.get(str(cost_item).strip())


def _normalize_key_value(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).strip()
    if text.endswith('.0') and text[:-2].isdigit():
        return text[:-2]
    return text


def _build_join_key(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    parts = [
        df[col].map(_normalize_key_value) if col in df.columns else pd.Series('', index=df.index) for col in columns
    ]
    if not parts:
        return pd.Series('', index=df.index)
    key = parts[0].copy()
    for part in parts[1:]:
        key = key + '|' + part
    return key


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
    row_id_fields: list[str] | None = None,
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

    row_parts: list[pd.Series] = []
    for field in row_id_fields or ['period', 'product_code', 'order_no', 'order_line', 'cost_bucket']:
        if field in data.columns:
            row_parts.append(data[field].map(_normalize_key_value))
    if row_parts:
        row_id = row_parts[0].copy()
        for part in row_parts[1:]:
            row_id = row_id + '|' + part
        frame['row_id'] = row_id
    else:
        frame['row_id'] = pd.Series('', index=data.index)

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


def _concat_error_logs(frames: list[pd.DataFrame]) -> pd.DataFrame:
    valid_frames = [frame for frame in frames if not frame.empty]
    if not valid_frames:
        return _empty_error_log()
    return pd.concat(valid_frames, ignore_index=True)


def _append_reason(reason_series: pd.Series, mask: pd.Series, reason: str) -> pd.Series:
    updated = reason_series.copy()
    target = mask.fillna(False)
    if not target.any():
        return updated
    non_empty = target & updated.ne('')
    empty = target & updated.eq('')
    updated.loc[non_empty] = updated.loc[non_empty] + ';' + reason
    updated.loc[empty] = reason
    return updated


def _build_fact_table(work_order_df: pd.DataFrame) -> pd.DataFrame:
    grouped = work_order_df.groupby(
        ['product_code', 'product_name', 'period'], dropna=False, as_index=False, sort=False
    ).agg(
        dm_amount=('dm_amount', _sum_decimal_series),
        dl_amount=('dl_amount', _sum_decimal_series),
        moh_amount=('moh_amount', _sum_decimal_series),
        qty=('completed_qty', _sum_decimal_series),
    )

    rows: list[dict[str, object]] = []
    bucket_map = {
        'direct_material': 'dm_amount',
        'direct_labor': 'dl_amount',
        'moh': 'moh_amount',
    }
    for _, row in grouped.iterrows():
        for cost_bucket, amount_column in bucket_map.items():
            amount = row[amount_column]
            qty = row['qty']
            rows.append(
                {
                    'period': row['period'],
                    'product_code': row['product_code'],
                    'product_name': row['product_name'],
                    'cost_bucket': cost_bucket,
                    'amount': amount,
                    'qty': qty,
                    'price': _safe_divide(amount, qty),
                }
            )
    return pd.DataFrame(rows)


def _build_pivot(bucket_df: pd.DataFrame, value_col: str, period_columns: list[str]) -> pd.DataFrame:
    pivot = bucket_df.pivot_table(
        index=['product_code', 'product_name'],
        columns='period_display',
        values=value_col,
        aggfunc='first',
        sort=False,
    )
    return pivot.reindex(columns=period_columns).reset_index()


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
        SectionBlock(f'{title_prefix}完工金额', amount_with_total, 'amount', True),
        SectionBlock(f'{title_prefix}完工数量', qty_with_total, 'qty', True),
        SectionBlock(f'{title_prefix}完工单价', price_pivot, 'price', False),
    ]


def _build_product_summary_df(work_order_df: pd.DataFrame) -> pd.DataFrame:
    if work_order_df.empty:
        return pd.DataFrame(columns=['product_code', 'product_name', 'period', 'period_display'])

    summary_df = work_order_df.groupby(
        ['product_code', 'product_name', 'period'], dropna=False, as_index=False, sort=False
    ).agg(
        total_cost=('completed_amount_total', _sum_decimal_series),
        completed_qty=('completed_qty', _sum_decimal_series),
        dm_cost=('dm_amount', _sum_decimal_series),
        dl_cost=('dl_amount', _sum_decimal_series),
        moh_cost=('moh_amount', _sum_decimal_series),
    )
    summary_df['unit_cost'] = summary_df['total_cost'].combine(summary_df['completed_qty'], _safe_divide)
    summary_df['dm_unit_cost'] = summary_df['dm_cost'].combine(summary_df['completed_qty'], _safe_divide)
    summary_df['dl_unit_cost'] = summary_df['dl_cost'].combine(summary_df['completed_qty'], _safe_divide)
    summary_df['moh_unit_cost'] = summary_df['moh_cost'].combine(summary_df['completed_qty'], _safe_divide)
    summary_df['dm_contrib'] = summary_df['dm_cost'].combine(summary_df['total_cost'], _safe_divide)
    summary_df['dl_contrib'] = summary_df['dl_cost'].combine(summary_df['total_cost'], _safe_divide)
    summary_df['moh_contrib'] = summary_df['moh_cost'].combine(summary_df['total_cost'], _safe_divide)
    summary_df['period_display'] = summary_df['period'].map(_period_to_display)
    return summary_df


def _build_product_summary_from_fact_df(fact_df: pd.DataFrame) -> pd.DataFrame:
    if fact_df.empty:
        return pd.DataFrame(columns=['product_code', 'product_name', 'period', 'period_display'])

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

    qty_by_product = fact_df.groupby(
        ['product_code', 'product_name', 'period'], dropna=False, as_index=False, sort=False
    ).agg(completed_qty=('qty', _first_decimal))

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
        summary_df['dm_cost'].combine(summary_df['dl_cost'], _add_decimal).combine(summary_df['moh_cost'], _add_decimal)
    )
    summary_df['unit_cost'] = summary_df['total_cost'].combine(summary_df['completed_qty'], _safe_divide)
    summary_df['dm_unit_cost'] = summary_df['dm_cost'].combine(summary_df['completed_qty'], _safe_divide)
    summary_df['dl_unit_cost'] = summary_df['dl_cost'].combine(summary_df['completed_qty'], _safe_divide)
    summary_df['moh_unit_cost'] = summary_df['moh_cost'].combine(summary_df['completed_qty'], _safe_divide)
    summary_df['dm_contrib'] = summary_df['dm_cost'].combine(summary_df['total_cost'], _safe_divide)
    summary_df['dl_contrib'] = summary_df['dl_cost'].combine(summary_df['total_cost'], _safe_divide)
    summary_df['moh_contrib'] = summary_df['moh_cost'].combine(summary_df['total_cost'], _safe_divide)
    summary_df['period_display'] = summary_df['period'].map(_period_to_display)
    return summary_df


def build_product_anomaly_sections(summary_df: pd.DataFrame) -> list[ProductAnomalySection]:
    """构建兼容产品摘要页。"""
    if 'period_display' not in summary_df.columns and {'cost_bucket', 'amount', 'qty'}.issubset(summary_df.columns):
        summary_df = _build_product_summary_from_fact_df(summary_df)

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
    source['period_display'] = source['period'].map(_period_to_display)
    return {
        '直接材料_价量比': _build_section_blocks(source[source['cost_bucket'] == 'direct_material'], '直接材料'),
        '直接人工_价量比': _build_section_blocks(source[source['cost_bucket'] == 'direct_labor'], '直接人工'),
        '制造费用_价量比': _build_section_blocks(source[source['cost_bucket'] == 'moh'], '制造费用'),
    }


def _grade_score(score: float | None) -> str:
    if score is None or pd.isna(score):
        return ''
    abs_score = abs(score)
    if abs_score > 3.5:
        return '高度可疑'
    if abs_score > 2.5:
        return '关注'
    return '正常'


def _build_anomaly_sheet(work_order_df: pd.DataFrame) -> FlatSheet:
    anomaly_df = work_order_df.copy()
    reason_series = pd.Series('', index=anomaly_df.index, dtype='object')

    anomaly_df['can_analyze'] = anomaly_df['completed_qty'].map(
        lambda value: value is not None and value > ZERO
    ) & anomaly_df['total_unit_cost'].map(lambda value: value is not None and value > ZERO)

    for metric_key, display_name, flag_column, _reason in ANOMALY_METRICS:
        log_column = f'log_{metric_key}'
        score_column = f'modified_z_{metric_key}'
        anomaly_df[log_column] = None
        anomaly_df[score_column] = None

        metric_positive = anomaly_df[metric_key].map(lambda value: value is not None and value > ZERO)
        reason_series = _append_reason(reason_series, ~metric_positive, f'{display_name}小于等于0或为空')

        for _, group_index in anomaly_df.groupby(['product_code', 'product_name'], sort=False).groups.items():
            metric_series = anomaly_df.loc[group_index, metric_key]
            valid_mask = metric_series.map(lambda value: value is not None and value > ZERO)
            if not valid_mask.any():
                continue
            valid_values = metric_series.loc[valid_mask].map(lambda value: math.log(float(value)))
            anomaly_df.loc[valid_values.index, log_column] = valid_values

            if len(valid_values) < 3:
                continue

            median = valid_values.median()
            mad = (valid_values - median).abs().median()
            if pd.isna(mad) or mad == 0:
                continue

            scores = 0.6745 * (valid_values - median) / mad
            anomaly_df.loc[scores.index, score_column] = scores

        anomaly_df[flag_column] = anomaly_df[score_column].map(_grade_score)

    anomaly_df['复核原因'] = reason_series
    anomaly_df['是否可参与分析'] = anomaly_df['can_analyze'].map(lambda value: '是' if value else '否')

    overall_level = pd.Series('正常', index=anomaly_df.index, dtype='object')
    highest_source = pd.Series('', index=anomaly_df.index, dtype='object')
    highest_score = pd.Series(-1.0, index=anomaly_df.index, dtype='float64')
    severity_rank = pd.Series(0, index=anomaly_df.index, dtype='int64')

    for metric_key, _display_name, flag_column, source_label in ANOMALY_METRICS:
        score_column = f'modified_z_{metric_key}'
        flag_series = anomaly_df[flag_column]
        current_rank = flag_series.map({'正常': 0, '关注': 1, '高度可疑': 2}).fillna(-1).astype(int)
        score_abs = anomaly_df[score_column].map(
            lambda value: abs(value) if value is not None and not pd.isna(value) else -1.0
        )

        better_rank = current_rank > severity_rank
        same_rank_better_score = (current_rank == severity_rank) & (score_abs > highest_score)
        same_rank_same_score = (
            (current_rank == severity_rank)
            & (score_abs == highest_score)
            & highest_source.ne('')
            & highest_source.ne(source_label)
            & (current_rank > 0)
        )

        overall_level.loc[better_rank] = flag_series.loc[better_rank]
        highest_source.loc[better_rank] = source_label
        highest_score.loc[better_rank] = score_abs.loc[better_rank]
        severity_rank.loc[better_rank] = current_rank.loc[better_rank]

        overall_level.loc[same_rank_better_score] = flag_series.loc[same_rank_better_score]
        highest_source.loc[same_rank_better_score] = source_label
        highest_score.loc[same_rank_better_score] = score_abs.loc[same_rank_better_score]

        prefer_total = same_rank_same_score & ((highest_source == '总成本异常') | (source_label == '总成本异常'))
        highest_source.loc[same_rank_same_score & ~prefer_total] = '多项同时异常'

    highest_source.loc[severity_rank <= 0] = ''
    anomaly_df['异常等级'] = overall_level
    anomaly_df['异常主要来源'] = highest_source

    rename_map = {
        'period_display': '月份',
        'cost_center': '成本中心',
        'product_code': '产品编码',
        'product_name': '产品名称',
        'spec': '规格型号',
        'order_no': '工单编号',
        'order_line': '工单行',
        'unit': '基本单位',
        'completed_qty': '本期完工数量',
        'completed_amount_total': '总完工成本',
        'dm_amount': '直接材料合计完工金额',
        'dl_amount': '直接人工合计完工金额',
        'moh_amount': '制造费用合计完工金额',
        'moh_other_amount': '制造费用_其他合计完工金额',
        'moh_labor_amount': '制造费用_人工合计完工金额',
        'moh_consumables_amount': '制造费用_机物料及低耗合计完工金额',
        'moh_depreciation_amount': '制造费用_折旧合计完工金额',
        'moh_utilities_amount': '制造费用_水电费合计完工金额',
        'total_unit_cost': '总单位完工成本',
        'dm_unit_cost': '直接材料单位完工成本',
        'dl_unit_cost': '直接人工单位完工成本',
        'moh_unit_cost': '制造费用单位完工成本',
        'moh_other_unit_cost': '制造费用_其他单位完工成本',
        'moh_labor_unit_cost': '制造费用_人工单位完工成本',
        'moh_consumables_unit_cost': '制造费用_机物料及低耗单位完工成本',
        'moh_depreciation_unit_cost': '制造费用_折旧单位完工成本',
        'moh_utilities_unit_cost': '制造费用_水电费单位完工成本',
        'log_total_unit_cost': 'log_总单位完工成本',
        'log_dm_unit_cost': 'log_直接材料单位完工成本',
        'log_dl_unit_cost': 'log_直接人工单位完工成本',
        'log_moh_unit_cost': 'log_制造费用单位完工成本',
        'log_moh_other_unit_cost': 'log_制造费用_其他单位完工成本',
        'log_moh_labor_unit_cost': 'log_制造费用_人工单位完工成本',
        'log_moh_consumables_unit_cost': 'log_制造费用_机物料及低耗单位完工成本',
        'log_moh_depreciation_unit_cost': 'log_制造费用_折旧单位完工成本',
        'log_moh_utilities_unit_cost': 'log_制造费用_水电费单位完工成本',
        'modified_z_total_unit_cost': 'Modified Z-score_总单位完工成本',
        'modified_z_dm_unit_cost': 'Modified Z-score_直接材料',
        'modified_z_dl_unit_cost': 'Modified Z-score_直接人工',
        'modified_z_moh_unit_cost': 'Modified Z-score_制造费用',
        'modified_z_moh_other_unit_cost': 'Modified Z-score_制造费用_其他',
        'modified_z_moh_labor_unit_cost': 'Modified Z-score_制造费用_人工',
        'modified_z_moh_consumables_unit_cost': 'Modified Z-score_制造费用_机物料及低耗',
        'modified_z_moh_depreciation_unit_cost': 'Modified Z-score_制造费用_折旧',
        'modified_z_moh_utilities_unit_cost': 'Modified Z-score_制造费用_水电费',
    }

    output_df = anomaly_df.rename(columns=rename_map)
    output_df = output_df[WORK_ORDER_OUTPUT_COLUMNS]
    return FlatSheet(data=output_df, column_types=WORK_ORDER_COLUMN_TYPES)


def _build_quality_sheet(
    detail_df: pd.DataFrame,
    qty_input_df: pd.DataFrame,
    qty_sheet_df: pd.DataFrame,
    analysis_df: pd.DataFrame,
) -> FlatSheet:
    unique_key = qty_sheet_df['_join_key']
    duplicate_count = int(unique_key.duplicated(keep=False).sum())

    qty_null_rate = qty_sheet_df['本期完工数量'].isna().mean() if '本期完工数量' in qty_sheet_df.columns else 0.0
    amount_null_rate = qty_sheet_df['本期完工金额'].isna().mean() if '本期完工金额' in qty_sheet_df.columns else 0.0
    dm_amount_null_rate = qty_sheet_df[QTY_DM_AMOUNT].isna().mean() if QTY_DM_AMOUNT in qty_sheet_df.columns else 0.0
    analyzable_rate = (
        analysis_df['是否可参与分析'].eq('是').mean()
        if '是否可参与分析' in analysis_df.columns and not analysis_df.empty
        else 0.0
    )

    quality_df = pd.DataFrame(
        [
            {
                '检查类别': '行数勾稽',
                '指标': '成本明细输入行数',
                '数值': str(len(detail_df)),
                '说明': '原始拆分后的成本明细行数',
            },
            {
                '检查类别': '行数勾稽',
                '指标': '产品数量统计输入行数',
                '数值': str(len(qty_input_df)),
                '说明': '拆分后的数量页原始行数',
            },
            {
                '检查类别': '行数勾稽',
                '指标': '产品数量统计输出行数',
                '数值': str(len(qty_sheet_df)),
                '说明': '补强后数量页行数，应与输入一致',
            },
            {
                '检查类别': '行数勾稽',
                '指标': '工单异常分析输出行数',
                '数值': str(len(analysis_df)),
                '说明': '去重后的工单级分析行数',
            },
            {
                '检查类别': '空值率',
                '指标': '本期完工数量缺失率',
                '数值': f'{qty_null_rate:.2%}',
                '说明': '关键数量字段空值率',
            },
            {
                '检查类别': '空值率',
                '指标': '本期完工金额缺失率',
                '数值': f'{amount_null_rate:.2%}',
                '说明': '关键总金额字段空值率',
            },
            {
                '检查类别': '空值率',
                '指标': '直接材料金额缺失率',
                '数值': f'{dm_amount_null_rate:.2%}',
                '说明': '派生金额字段空值率',
            },
            {
                '检查类别': '唯一性检查',
                '指标': '工单主键重复行数',
                '数值': str(duplicate_count),
                '说明': '键=月份+产品编码+工单编号+工单行',
            },
            {
                '检查类别': '范围检查',
                '指标': '完工数量小于等于0行数',
                '数值': str(int(qty_sheet_df[QTY_QTY_NON_POSITIVE].eq('是').sum())),
                '说明': '该类数据不参与 log 与 Modified Z-score',
            },
            {
                '检查类别': '分析覆盖率',
                '指标': '可参与分析占比',
                '数值': f'{analyzable_rate:.2%}',
                '说明': '仅统计白名单产品且通过基础校验的工单',
            },
        ]
    )
    return FlatSheet(data=quality_df, column_types={'检查类别': 'text', '指标': 'text', '数值': 'text', '说明': 'text'})


def build_report_artifacts(df_detail: pd.DataFrame, df_qty: pd.DataFrame) -> AnalysisArtifacts:
    """构建 V3 报表所需的全部分析产物。"""
    detail_period_col = _resolve_period_column(df_detail)
    qty_period_col = _resolve_period_column(df_qty)

    required_detail_cols = {'产品编码', '产品名称', '工单编号', '工单行号', '成本项目名称', '本期完工金额'}
    missing_detail_cols = required_detail_cols.difference(df_detail.columns)
    if missing_detail_cols:
        missing = ', '.join(sorted(missing_detail_cols))
        raise ValueError(f'成本明细缺少必要字段: {missing}')

    required_qty_cols = {'产品编码', '产品名称', '工单编号', '工单行号', '本期完工数量', '本期完工金额'}
    missing_qty_cols = required_qty_cols.difference(df_qty.columns)
    if missing_qty_cols:
        missing = ', '.join(sorted(missing_qty_cols))
        raise ValueError(f'产品数量统计缺少必要字段: {missing}')

    error_frames: list[pd.DataFrame] = []

    detail = df_detail.copy().rename(
        columns={
            '产品编码': 'product_code',
            '产品名称': 'product_name',
            '工单编号': 'order_no',
            '工单行号': 'order_line',
            '成本项目名称': 'cost_item',
            '本期完工金额': 'completed_amount',
        }
    )
    detail['period'] = detail[detail_period_col].map(_normalize_period)
    detail['cost_bucket'] = detail['cost_item'].map(_map_broad_cost_bucket)
    detail['component_bucket'] = detail['cost_item'].map(_map_component_bucket)
    detail['amount'] = detail['completed_amount'].map(_to_decimal)

    excluded_cost_mask = detail['cost_item'].astype(str).str.strip().eq('委外加工费')
    if excluded_cost_mask.any():
        error_frames.append(
            _build_error_frame(
                detail.loc[
                    excluded_cost_mask,
                    ['product_code', 'product_name', 'period', 'order_no', 'order_line', 'cost_item'],
                ],
                issue_type='EXCLUDED_COST_ITEM',
                field_name='成本项目名称',
                reason='委外加工费不纳入 V3 价量分析与异常分析',
                action='该成本项目已写入 error_log 并从分析口径中排除',
                original_column='cost_item',
                row_id_fields=WORK_ORDER_KEY_COLS,
            )
        )

    unmapped_mask = detail['cost_bucket'].isna() & ~excluded_cost_mask
    if unmapped_mask.any():
        error_frames.append(
            _build_error_frame(
                detail.loc[
                    unmapped_mask, ['product_code', 'product_name', 'period', 'order_no', 'order_line', 'cost_item']
                ],
                issue_type='UNMAPPED_COST_ITEM',
                field_name='成本项目名称',
                reason='成本项目未映射到直接材料/直接人工/制造费用',
                action='该行已从分析数据中排除',
                original_column='cost_item',
                row_id_fields=WORK_ORDER_KEY_COLS,
            )
        )

    missing_detail_amount = detail['amount'].isna() & detail['cost_bucket'].notna()
    if missing_detail_amount.any():
        error_frames.append(
            _build_error_frame(
                detail.loc[
                    missing_detail_amount,
                    [
                        'product_code',
                        'product_name',
                        'period',
                        'order_no',
                        'order_line',
                        'cost_bucket',
                        'completed_amount',
                    ],
                ],
                issue_type='MISSING_AMOUNT',
                field_name='本期完工金额',
                reason='成本明细金额为空，已按 0 参与汇总',
                action='金额置为 0 后继续计算',
                original_column='completed_amount',
                row_id_fields=WORK_ORDER_KEY_COLS,
            )
        )
        detail.loc[missing_detail_amount, 'amount'] = ZERO

    detail_for_analysis = detail.loc[detail['cost_bucket'].notna()].copy()
    detail_for_analysis['_join_key'] = _build_join_key(detail_for_analysis, WORK_ORDER_KEY_COLS)

    broad_amounts = (
        detail_for_analysis.groupby(
            WORK_ORDER_KEY_COLS + ['product_name', 'cost_bucket'], dropna=False, as_index=False, sort=False
        )
        .agg(amount=('amount', _sum_decimal_series))
        .pivot_table(
            index=WORK_ORDER_KEY_COLS + ['product_name'],
            columns='cost_bucket',
            values='amount',
            aggfunc='first',
            sort=False,
        )
        .reset_index()
    )
    for column in COST_BUCKETS:
        if column not in broad_amounts.columns:
            broad_amounts[column] = ZERO
    broad_amounts = broad_amounts.rename(
        columns={
            'direct_material': 'dm_amount',
            'direct_labor': 'dl_amount',
            'moh': 'moh_amount',
        }
    )

    component_amounts = (
        detail_for_analysis.loc[detail_for_analysis['component_bucket'].notna()]
        .groupby(WORK_ORDER_KEY_COLS + ['product_name', 'component_bucket'], dropna=False, as_index=False, sort=False)
        .agg(amount=('amount', _sum_decimal_series))
        .pivot_table(
            index=WORK_ORDER_KEY_COLS + ['product_name'],
            columns='component_bucket',
            values='amount',
            aggfunc='first',
            sort=False,
        )
        .reset_index()
    )

    qty_sheet_df = df_qty.copy().reset_index(drop=True)
    qty_sheet_df['_source_row'] = range(len(qty_sheet_df))
    qty_sheet_df['period'] = qty_sheet_df[qty_period_col].map(_normalize_period)
    qty_sheet_df['period_display'] = qty_sheet_df['period'].map(_period_to_display)
    qty_sheet_df['product_code'] = qty_sheet_df['产品编码'].astype(str)
    qty_sheet_df['product_name'] = qty_sheet_df['产品名称'].astype(str)
    qty_sheet_df['order_no'] = qty_sheet_df['工单编号']
    qty_sheet_df['order_line'] = qty_sheet_df['工单行号']
    qty_sheet_df['completed_qty'] = qty_sheet_df['本期完工数量'].map(_to_decimal)
    qty_sheet_df['completed_amount_total'] = qty_sheet_df['本期完工金额'].map(_to_decimal)
    qty_sheet_df['_join_key'] = _build_join_key(qty_sheet_df, WORK_ORDER_KEY_COLS)

    missing_qty_mask = qty_sheet_df['completed_qty'].isna()
    if missing_qty_mask.any():
        error_frames.append(
            _build_error_frame(
                qty_sheet_df.loc[
                    missing_qty_mask,
                    ['product_code', 'product_name', 'period', 'order_no', 'order_line', '本期完工数量'],
                ],
                issue_type='MISSING_REQUIRED_VALUE',
                field_name='本期完工数量',
                reason='完工数量为空',
                action='该工单保留在数量页，但不参与异常分析',
                original_column='本期完工数量',
                row_id_fields=WORK_ORDER_KEY_COLS,
            )
        )

    missing_total_amount_mask = qty_sheet_df['completed_amount_total'].isna()
    if missing_total_amount_mask.any():
        error_frames.append(
            _build_error_frame(
                qty_sheet_df.loc[
                    missing_total_amount_mask,
                    ['product_code', 'product_name', 'period', 'order_no', 'order_line', '本期完工金额'],
                ],
                issue_type='MISSING_REQUIRED_VALUE',
                field_name='本期完工金额',
                reason='总完工成本为空',
                action='该工单保留在数量页，但总单位成本无法计算',
                original_column='本期完工金额',
                row_id_fields=WORK_ORDER_KEY_COLS,
            )
        )

    duplicate_qty_mask = qty_sheet_df['_join_key'].duplicated(keep=False)
    if duplicate_qty_mask.any():
        error_frames.append(
            _build_error_frame(
                qty_sheet_df.loc[
                    duplicate_qty_mask, ['product_code', 'product_name', 'period', 'order_no', 'order_line']
                ],
                issue_type='DUPLICATE_WORK_ORDER_KEY',
                field_name='工单主键',
                reason='数量页存在重复工单主键',
                action='数量页原样保留，异常分析按首条记录去重',
                row_id_fields=WORK_ORDER_KEY_COLS,
            )
        )

    work_order_amounts = broad_amounts.merge(component_amounts, on=WORK_ORDER_KEY_COLS + ['product_name'], how='left')
    for column in [
        'dm_amount',
        'dl_amount',
        'moh_amount',
        'moh_other_amount',
        'moh_labor_amount',
        'moh_consumables_amount',
        'moh_depreciation_amount',
        'moh_utilities_amount',
    ]:
        if column not in work_order_amounts.columns:
            work_order_amounts[column] = ZERO
        work_order_amounts[column] = (
            work_order_amounts[column].map(_to_decimal).map(lambda value: value if value is not None else ZERO)
        )

    work_order_amounts['_join_key'] = _build_join_key(work_order_amounts, WORK_ORDER_KEY_COLS)
    amount_columns = [
        'dm_amount',
        'dl_amount',
        'moh_amount',
        'moh_other_amount',
        'moh_labor_amount',
        'moh_consumables_amount',
        'moh_depreciation_amount',
        'moh_utilities_amount',
    ]
    qty_sheet_df = qty_sheet_df.merge(
        work_order_amounts[['_join_key'] + amount_columns].drop_duplicates('_join_key'),
        on='_join_key',
        how='left',
    )
    for column in amount_columns:
        qty_sheet_df[column] = (
            qty_sheet_df[column].map(_to_decimal).map(lambda value: value if value is not None else ZERO)
        )

    qty_sheet_df[QTY_DM_AMOUNT] = qty_sheet_df['dm_amount']
    qty_sheet_df[QTY_DL_AMOUNT] = qty_sheet_df['dl_amount']
    qty_sheet_df[QTY_MOH_AMOUNT] = qty_sheet_df['moh_amount']
    qty_sheet_df[QTY_MOH_OTHER_AMOUNT] = qty_sheet_df['moh_other_amount']
    qty_sheet_df[QTY_MOH_LABOR_AMOUNT] = qty_sheet_df['moh_labor_amount']
    qty_sheet_df[QTY_MOH_CONSUMABLES_AMOUNT] = qty_sheet_df['moh_consumables_amount']
    qty_sheet_df[QTY_MOH_DEPRECIATION_AMOUNT] = qty_sheet_df['moh_depreciation_amount']
    qty_sheet_df[QTY_MOH_UTILITIES_AMOUNT] = qty_sheet_df['moh_utilities_amount']

    qty_sheet_df[QTY_DM_UNIT_COST] = qty_sheet_df[QTY_DM_AMOUNT].combine(qty_sheet_df['completed_qty'], _safe_divide)
    qty_sheet_df[QTY_DL_UNIT_COST] = qty_sheet_df[QTY_DL_AMOUNT].combine(qty_sheet_df['completed_qty'], _safe_divide)
    qty_sheet_df[QTY_MOH_UNIT_COST] = qty_sheet_df[QTY_MOH_AMOUNT].combine(qty_sheet_df['completed_qty'], _safe_divide)
    qty_sheet_df[QTY_MOH_OTHER_UNIT_COST] = qty_sheet_df[QTY_MOH_OTHER_AMOUNT].combine(
        qty_sheet_df['completed_qty'], _safe_divide
    )
    qty_sheet_df[QTY_MOH_LABOR_UNIT_COST] = qty_sheet_df[QTY_MOH_LABOR_AMOUNT].combine(
        qty_sheet_df['completed_qty'], _safe_divide
    )
    qty_sheet_df[QTY_MOH_CONSUMABLES_UNIT_COST] = qty_sheet_df[QTY_MOH_CONSUMABLES_AMOUNT].combine(
        qty_sheet_df['completed_qty'], _safe_divide
    )
    qty_sheet_df[QTY_MOH_DEPRECIATION_UNIT_COST] = qty_sheet_df[QTY_MOH_DEPRECIATION_AMOUNT].combine(
        qty_sheet_df['completed_qty'], _safe_divide
    )
    qty_sheet_df[QTY_MOH_UTILITIES_UNIT_COST] = qty_sheet_df[QTY_MOH_UTILITIES_AMOUNT].combine(
        qty_sheet_df['completed_qty'], _safe_divide
    )

    qty_sheet_df[QTY_VALID_QTY] = qty_sheet_df['completed_qty'].map(
        lambda value: '是' if value is not None and value > ZERO else '否'
    )
    qty_sheet_df[QTY_QTY_NON_POSITIVE] = qty_sheet_df['completed_qty'].map(
        lambda value: '是' if value is not None and value <= ZERO else '否'
    )

    qty_sheet_df['moh_component_sum'] = (
        qty_sheet_df[QTY_MOH_OTHER_AMOUNT]
        .combine(qty_sheet_df[QTY_MOH_LABOR_AMOUNT], _add_decimal)
        .combine(qty_sheet_df[QTY_MOH_CONSUMABLES_AMOUNT], _add_decimal)
        .combine(qty_sheet_df[QTY_MOH_DEPRECIATION_AMOUNT], _add_decimal)
        .combine(qty_sheet_df[QTY_MOH_UTILITIES_AMOUNT], _add_decimal)
    )
    qty_sheet_df['derived_total_amount'] = (
        qty_sheet_df[QTY_DM_AMOUNT]
        .combine(qty_sheet_df[QTY_DL_AMOUNT], _add_decimal)
        .combine(qty_sheet_df[QTY_MOH_AMOUNT], _add_decimal)
    )

    qty_sheet_df[QTY_MOH_MATCH] = (
        qty_sheet_df['moh_component_sum'].notna()
        & qty_sheet_df[QTY_MOH_AMOUNT].notna()
        & (qty_sheet_df['moh_component_sum'] == qty_sheet_df[QTY_MOH_AMOUNT])
    ).map(lambda value: '是' if value else '否')
    qty_sheet_df[QTY_TOTAL_MATCH] = (
        qty_sheet_df['derived_total_amount'].notna()
        & qty_sheet_df['completed_amount_total'].notna()
        & (qty_sheet_df['derived_total_amount'] == qty_sheet_df['completed_amount_total'])
    ).map(lambda value: '是' if value else '否')

    null_required_columns = [
        'completed_qty',
        'completed_amount_total',
        QTY_DM_AMOUNT,
        QTY_DL_AMOUNT,
        QTY_MOH_AMOUNT,
    ]
    qty_sheet_df[QTY_HAS_NULL] = (
        qty_sheet_df[null_required_columns].isna().any(axis=1).map(lambda value: '是' if value else '否')
    )

    qty_reason = pd.Series('', index=qty_sheet_df.index, dtype='object')
    qty_reason = _append_reason(qty_reason, qty_sheet_df[QTY_VALID_QTY].eq('否'), '完工数量无效')
    qty_reason = _append_reason(qty_reason, qty_sheet_df[QTY_HAS_NULL].eq('是'), '关键字段存在空值')
    qty_reason = _append_reason(qty_reason, qty_sheet_df[QTY_MOH_MATCH].eq('否'), '制造费用明细与合计不一致')
    qty_reason = _append_reason(qty_reason, qty_sheet_df[QTY_TOTAL_MATCH].eq('否'), '三大类金额与总完工成本不一致')
    qty_sheet_df[QTY_CHECK_REASON] = qty_reason
    qty_sheet_df[QTY_CHECK_STATUS] = (
        qty_sheet_df[QTY_CHECK_REASON].eq('').map(lambda value: '通过' if value else '需复核')
    )

    non_positive_qty_mask = qty_sheet_df[QTY_QTY_NON_POSITIVE].eq('是')
    if non_positive_qty_mask.any():
        error_frames.append(
            _build_error_frame(
                qty_sheet_df.loc[
                    non_positive_qty_mask,
                    ['product_code', 'product_name', 'period', 'order_no', 'order_line', 'completed_qty'],
                ],
                issue_type='INVALID_COMPLETED_QTY',
                field_name='本期完工数量',
                reason='完工数量小于等于 0，不参与单位成本 log 与 Modified Z-score',
                action='保留在数量页并标记需复核',
                original_column='completed_qty',
                row_id_fields=WORK_ORDER_KEY_COLS,
            )
        )

    moh_mismatch_mask = qty_sheet_df[QTY_MOH_MATCH].eq('否')
    if moh_mismatch_mask.any():
        mismatch_frame = qty_sheet_df.loc[
            moh_mismatch_mask,
            ['product_code', 'product_name', 'period', 'order_no', 'order_line', 'moh_component_sum', QTY_MOH_AMOUNT],
        ].rename(columns={QTY_MOH_AMOUNT: 'moh_amount_output'})
        mismatch_frame['diff'] = mismatch_frame['moh_component_sum'].combine(
            mismatch_frame['moh_amount_output'],
            lambda lhs, rhs: None
            if _to_decimal(lhs) is None or _to_decimal(rhs) is None
            else _to_decimal(lhs) - _to_decimal(rhs),
        )
        error_frames.append(
            _build_error_frame(
                mismatch_frame,
                issue_type='MOH_BREAKDOWN_MISMATCH',
                field_name='制造费用',
                reason='制造费用明细项合计不等于制造费用合计',
                action='保留结果并标记需复核',
                lhs_column='moh_component_sum',
                rhs_column='moh_amount_output',
                diff_column='diff',
                row_id_fields=WORK_ORDER_KEY_COLS,
            )
        )

    total_mismatch_mask = qty_sheet_df[QTY_TOTAL_MATCH].eq('否')
    if total_mismatch_mask.any():
        total_frame = qty_sheet_df.loc[
            total_mismatch_mask,
            [
                'product_code',
                'product_name',
                'period',
                'order_no',
                'order_line',
                'derived_total_amount',
                'completed_amount_total',
            ],
        ].copy()
        total_frame['diff'] = total_frame['derived_total_amount'].combine(
            total_frame['completed_amount_total'],
            lambda lhs, rhs: None
            if _to_decimal(lhs) is None or _to_decimal(rhs) is None
            else _to_decimal(lhs) - _to_decimal(rhs),
        )
        error_frames.append(
            _build_error_frame(
                total_frame,
                issue_type='TOTAL_COST_MISMATCH',
                field_name='总完工成本',
                reason='直接材料+直接人工+制造费用不等于数量页总完工成本',
                action='保留结果并标记需复核',
                lhs_column='derived_total_amount',
                rhs_column='completed_amount_total',
                diff_column='diff',
                row_id_fields=WORK_ORDER_KEY_COLS,
            )
        )

    qty_output_columns = list(df_qty.columns) + [
        QTY_DM_AMOUNT,
        QTY_DL_AMOUNT,
        QTY_MOH_AMOUNT,
        QTY_MOH_OTHER_AMOUNT,
        QTY_MOH_LABOR_AMOUNT,
        QTY_MOH_CONSUMABLES_AMOUNT,
        QTY_MOH_DEPRECIATION_AMOUNT,
        QTY_MOH_UTILITIES_AMOUNT,
        QTY_DM_UNIT_COST,
        QTY_DL_UNIT_COST,
        QTY_MOH_UNIT_COST,
        QTY_MOH_OTHER_UNIT_COST,
        QTY_MOH_LABOR_UNIT_COST,
        QTY_MOH_CONSUMABLES_UNIT_COST,
        QTY_MOH_DEPRECIATION_UNIT_COST,
        QTY_MOH_UTILITIES_UNIT_COST,
        QTY_VALID_QTY,
        QTY_QTY_NON_POSITIVE,
        QTY_HAS_NULL,
        QTY_MOH_MATCH,
        QTY_TOTAL_MATCH,
        QTY_CHECK_STATUS,
        QTY_CHECK_REASON,
    ]
    qty_sheet_output = qty_sheet_df[qty_output_columns + ['_join_key']].copy()

    analysis_source = qty_sheet_df.sort_values('_source_row').drop_duplicates('_join_key', keep='first').copy()
    analysis_source = analysis_source.drop(
        columns=['月份', '产品编码', '产品名称', '工单编号', '工单行号', '本期完工数量'], errors='ignore'
    )
    analysis_source = analysis_source.rename(
        columns={
            '成本中心名称': 'cost_center',
            '规格型号': 'spec',
            '基本单位': 'unit',
        }
    )
    analysis_source['total_unit_cost'] = analysis_source['completed_amount_total'].combine(
        analysis_source['completed_qty'], _safe_divide
    )
    analysis_source['dm_unit_cost'] = analysis_source[QTY_DM_AMOUNT].combine(
        analysis_source['completed_qty'], _safe_divide
    )
    analysis_source['dl_unit_cost'] = analysis_source[QTY_DL_AMOUNT].combine(
        analysis_source['completed_qty'], _safe_divide
    )
    analysis_source['moh_unit_cost'] = analysis_source[QTY_MOH_AMOUNT].combine(
        analysis_source['completed_qty'], _safe_divide
    )
    analysis_source['moh_other_unit_cost'] = analysis_source[QTY_MOH_OTHER_AMOUNT].combine(
        analysis_source['completed_qty'], _safe_divide
    )
    analysis_source['moh_labor_unit_cost'] = analysis_source[QTY_MOH_LABOR_AMOUNT].combine(
        analysis_source['completed_qty'], _safe_divide
    )
    analysis_source['moh_consumables_unit_cost'] = analysis_source[QTY_MOH_CONSUMABLES_AMOUNT].combine(
        analysis_source['completed_qty'], _safe_divide
    )
    analysis_source['moh_depreciation_unit_cost'] = analysis_source[QTY_MOH_DEPRECIATION_AMOUNT].combine(
        analysis_source['completed_qty'], _safe_divide
    )
    analysis_source['moh_utilities_unit_cost'] = analysis_source[QTY_MOH_UTILITIES_AMOUNT].combine(
        analysis_source['completed_qty'], _safe_divide
    )

    for column in ['dm_amount', 'dl_amount', 'moh_amount']:
        if column not in analysis_source.columns:
            analysis_source[column] = ZERO

    for metric_key, display_name, _flag_column, _reason in ANOMALY_METRICS:
        mask = analysis_source[metric_key].map(lambda value: value is not None and value <= ZERO)
        if mask.any():
            error_frames.append(
                _build_error_frame(
                    analysis_source.loc[
                        mask, ['product_code', 'product_name', 'period', 'order_no', 'order_line', metric_key]
                    ],
                    issue_type='NON_POSITIVE_UNIT_COST',
                    field_name=display_name,
                    reason='单位成本小于等于 0，不参与 log 与 Modified Z-score',
                    action='保留在异常分析页并标记复核原因',
                    original_column=metric_key,
                    row_id_fields=WORK_ORDER_KEY_COLS,
                )
            )

    fact_df = _build_fact_table(analysis_source)
    product_summary_df = _build_product_summary_df(analysis_source)
    work_order_sheet = _build_anomaly_sheet(analysis_source)
    quality_sheet = _build_quality_sheet(df_detail, df_qty, qty_sheet_output, work_order_sheet.data)
    error_log = _concat_error_logs(error_frames)

    qty_sheet_output = qty_sheet_output.drop(columns=['_join_key'])
    return AnalysisArtifacts(
        fact_df=fact_df,
        qty_sheet_df=qty_sheet_output,
        work_order_sheet=work_order_sheet,
        product_anomaly_sections=build_product_anomaly_sections(product_summary_df),
        quality_sheet=quality_sheet,
        error_log=error_log,
    )


def _build_legacy_fact_cost_pq(df_detail: pd.DataFrame, df_qty: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """兼容旧测试与旧调用方的 fact 表构建逻辑。"""
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
    detail['cost_bucket'] = detail['cost_item'].map(_map_broad_cost_bucket)
    detail['amount'] = detail['本期完工金额'].map(_to_decimal)

    qty = df_qty.copy().rename(columns={'产品编码': 'product_code', '产品名称': 'product_name'})
    qty['period'] = qty[qty_period_col].map(_normalize_period)
    qty['qty'] = qty['本期完工数量'].map(_to_decimal)

    prep_errors: list[pd.DataFrame] = []
    excluded_cost_mask = detail['cost_item'].astype(str).str.strip().eq('委外加工费')
    if excluded_cost_mask.any():
        prep_errors.append(
            _build_error_frame(
                detail.loc[excluded_cost_mask, ['product_code', 'product_name', 'period', 'cost_item']],
                issue_type='UNMAPPED_COST_ITEM',
                field_name='成本项目名称',
                reason='委外加工费不纳入三大类分析',
                action='该行已从价量分析中排除',
                original_column='cost_item',
                row_id_fields=['period', 'product_code', 'cost_item'],
            )
        )

    unmapped_mask = detail['cost_bucket'].isna() & ~excluded_cost_mask
    if unmapped_mask.any():
        prep_errors.append(
            _build_error_frame(
                detail.loc[unmapped_mask, ['product_code', 'product_name', 'period', 'cost_item']],
                issue_type='UNMAPPED_COST_ITEM',
                field_name='成本项目名称',
                reason='成本项目未映射到 direct_material/direct_labor/moh',
                action='该行已从三大类报表中排除',
                original_column='cost_item',
                row_id_fields=['period', 'product_code', 'cost_item'],
            )
        )

    detail_mapped = detail.loc[detail['cost_bucket'].notna()].copy()
    missing_amount = detail_mapped['amount'].isna()
    if missing_amount.any():
        prep_errors.append(
            _build_error_frame(
                detail_mapped.loc[
                    missing_amount, ['product_code', 'product_name', 'period', 'cost_bucket', '本期完工金额']
                ],
                issue_type='MISSING_AMOUNT',
                field_name='本期完工金额',
                reason='该产品+月份+成本类别缺少金额明细',
                action='金额按 0 填充继续计算',
                original_column='本期完工金额',
                row_id_fields=['period', 'product_code', 'cost_bucket'],
            )
        )
        detail_mapped.loc[missing_amount, 'amount'] = ZERO

    amount_grouped = detail_mapped.groupby(
        ['product_code', 'product_name', 'period', 'cost_bucket'], dropna=False, as_index=False, sort=False
    ).agg(amount=('amount', _sum_decimal_series))
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
        return pd.DataFrame(
            columns=['period', 'product_code', 'product_name', 'cost_bucket', 'amount', 'qty', 'price']
        ), _concat_error_logs(prep_errors)

    bucket_df = pd.DataFrame({'cost_bucket': list(COST_BUCKETS), '_join_key': 1})
    fact = keys.assign(_join_key=1).merge(bucket_df, on='_join_key', how='inner').drop(columns=['_join_key'])
    fact = fact.merge(amount_grouped, on=['product_code', 'product_name', 'period', 'cost_bucket'], how='left')
    fact = fact.merge(qty_grouped, on=['product_code', 'product_name', 'period'], how='left')

    missing_amount_fact = fact['amount'].isna()
    if missing_amount_fact.any():
        prep_errors.append(
            _build_error_frame(
                fact.loc[missing_amount_fact, ['product_code', 'product_name', 'period', 'cost_bucket']],
                issue_type='MISSING_AMOUNT',
                field_name='本期完工金额',
                reason='该产品+月份+成本类别缺少金额明细',
                action='金额按 0 填充继续计算',
                row_id_fields=['period', 'product_code', 'cost_bucket'],
            )
        )
        fact.loc[missing_amount_fact, 'amount'] = ZERO

    missing_qty_fact = fact['qty'].isna()
    if missing_qty_fact.any():
        prep_errors.append(
            _build_error_frame(
                fact.loc[missing_qty_fact, ['product_code', 'product_name', 'period', 'cost_bucket']],
                issue_type='MISSING_QTY',
                field_name='本期完工数量',
                reason='该产品+月份缺少数量信息',
                action='保留空值并在单价展示为空',
                row_id_fields=['period', 'product_code', 'cost_bucket'],
            )
        )

    zero_qty = fact['qty'].map(lambda value: value == ZERO if isinstance(value, Decimal) else False)
    if zero_qty.any():
        prep_errors.append(
            _build_error_frame(
                fact.loc[zero_qty, ['product_code', 'product_name', 'period', 'cost_bucket', 'qty']],
                issue_type='ZERO_QTY',
                field_name='本期完工数量',
                reason='数量为 0 时单价不可计算',
                action='price 置空',
                original_column='qty',
                row_id_fields=['period', 'product_code', 'cost_bucket'],
            )
        )

    fact['price'] = fact['amount'].combine(fact['qty'], _safe_divide)
    return fact[
        ['period', 'product_code', 'product_name', 'cost_bucket', 'amount', 'qty', 'price']
    ], _concat_error_logs(prep_errors)


def build_fact_cost_pq(df_detail: pd.DataFrame, df_qty: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """兼容旧接口，仅返回 fact_df 与 error_log。"""
    v3_required_cols = {'工单编号', '工单行号'}
    if not v3_required_cols.issubset(df_detail.columns) or not v3_required_cols.issubset(df_qty.columns):
        return _build_legacy_fact_cost_pq(df_detail, df_qty)

    artifacts = build_report_artifacts(df_detail, df_qty)
    return artifacts.fact_df, artifacts.error_log

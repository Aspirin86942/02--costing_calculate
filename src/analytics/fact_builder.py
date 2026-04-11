"""事实表构建与通用数值工具。"""

from __future__ import annotations

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
QTY_OUTSOURCE_AMOUNT = '本期完工委外加工费合计完工金额'
QTY_DM_UNIT_COST = '直接材料单位完工金额'
QTY_DL_UNIT_COST = '直接人工单位完工金额'
QTY_MOH_UNIT_COST = '制造费用单位完工金额'
QTY_MOH_OTHER_UNIT_COST = '制造费用_其他单位完工成本'
QTY_MOH_LABOR_UNIT_COST = '制造费用_人工单位完工成本'
QTY_MOH_CONSUMABLES_UNIT_COST = '制造费用_机物料及低耗单位完工成本'
QTY_MOH_DEPRECIATION_UNIT_COST = '制造费用_折旧单位完工成本'
QTY_MOH_UTILITIES_UNIT_COST = '制造费用_水电费单位完工成本'
QTY_OUTSOURCE_UNIT_COST = '委外加工费单位完工成本'
QTY_MOH_MATCH = '制造费用明细项合计是否等于制造费用合计'
QTY_CHECK_STATUS = '数据校验状态'
QTY_CHECK_REASON = '异常原因说明'

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


@dataclass(frozen=True)
class StandaloneCostItemMeta:
    """独立成本项在数量页与工单异常页的列元数据。"""

    cost_item: str
    amount_key: str
    unit_cost_key: str
    qty_amount_column: str
    qty_unit_cost_column: str
    work_order_amount_column: str
    work_order_unit_cost_column: str


DEFAULT_STANDALONE_COST_ITEMS = ('委外加工费',)
_KNOWN_STANDALONE_META: dict[str, StandaloneCostItemMeta] = {
    '委外加工费': StandaloneCostItemMeta(
        cost_item='委外加工费',
        amount_key='outsource_amount',
        unit_cost_key='outsource_unit_cost',
        qty_amount_column=QTY_OUTSOURCE_AMOUNT,
        qty_unit_cost_column=QTY_OUTSOURCE_UNIT_COST,
        work_order_amount_column='委外加工费合计完工金额',
        work_order_unit_cost_column='委外加工费单位完工成本',
    ),
    '软件费用': StandaloneCostItemMeta(
        cost_item='软件费用',
        amount_key='software_amount',
        unit_cost_key='software_unit_cost',
        qty_amount_column='本期完工软件费用合计完工金额',
        qty_unit_cost_column='软件费用单位完工成本',
        work_order_amount_column='软件费用合计完工金额',
        work_order_unit_cost_column='软件费用单位完工成本',
    ),
}


def normalize_standalone_cost_items(standalone_cost_items: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    if standalone_cost_items is None:
        return DEFAULT_STANDALONE_COST_ITEMS
    normalized: list[str] = []
    seen: set[str] = set()
    for item in standalone_cost_items:
        name = str(item).strip()
        if not name or name in seen:
            continue
        seen.add(name)
        normalized.append(name)
    return tuple(normalized)


def resolve_standalone_cost_item_metas(
    standalone_cost_items: tuple[str, ...] | list[str] | None,
) -> tuple[StandaloneCostItemMeta, ...]:
    normalized_items = normalize_standalone_cost_items(standalone_cost_items)
    metas: list[StandaloneCostItemMeta] = []
    for index, item in enumerate(normalized_items, start=1):
        known_meta = _KNOWN_STANDALONE_META.get(item)
        if known_meta is not None:
            metas.append(known_meta)
            continue
        fallback_key = f'standalone_item_{index}'
        metas.append(
            StandaloneCostItemMeta(
                cost_item=item,
                amount_key=f'{fallback_key}_amount',
                unit_cost_key=f'{fallback_key}_unit_cost',
                qty_amount_column=f'本期完工{item}合计完工金额',
                qty_unit_cost_column=f'{item}单位完工成本',
                work_order_amount_column=f'{item}合计完工金额',
                work_order_unit_cost_column=f'{item}单位完工成本',
            )
        )
    return tuple(metas)


def build_total_cost_expression(standalone_metas: tuple[StandaloneCostItemMeta, ...]) -> str:
    items = ['直接材料', '直接人工', '制造费用'] + [meta.cost_item for meta in standalone_metas]
    return '+'.join(items)


def build_total_match_column_name(standalone_metas: tuple[StandaloneCostItemMeta, ...]) -> str:
    return f'{build_total_cost_expression(standalone_metas)}是否等于总完工成本'


def build_total_mismatch_reason(standalone_metas: tuple[StandaloneCostItemMeta, ...]) -> str:
    return f'{build_total_cost_expression(standalone_metas)}与总完工成本不一致'


def build_total_mismatch_error_reason(standalone_metas: tuple[StandaloneCostItemMeta, ...]) -> str:
    return f'{build_total_cost_expression(standalone_metas)}不等于数量页总完工成本'


QTY_TOTAL_MATCH = build_total_match_column_name(resolve_standalone_cost_item_metas(DEFAULT_STANDALONE_COST_ITEMS))


def to_decimal(value: object) -> Decimal | None:
    """把各种输入统一转成 Decimal。"""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value).strip())
    except (AttributeError, InvalidOperation, ValueError):
        return None


def sum_decimal(values: list[object]) -> Decimal:
    """求和时忽略空值。"""
    total = ZERO
    for value in values:
        decimal_value = to_decimal(value)
        if decimal_value is not None:
            total += decimal_value
    return total


def sum_decimal_series(series: pd.Series) -> Decimal:
    return sum_decimal(series.tolist())


def first_decimal(values: pd.Series) -> Decimal | None:
    """返回序列中第一个可用 Decimal。"""
    for value in values:
        decimal_value = to_decimal(value)
        if decimal_value is not None:
            return decimal_value
    return None


def safe_divide(numerator: object, denominator: object) -> Decimal | None:
    num = to_decimal(numerator)
    den = to_decimal(denominator)
    if num is None or den in (None, ZERO):
        return None
    return num / den


def is_positive_decimal(value: object) -> bool:
    decimal_value = to_decimal(value)
    return decimal_value is not None and decimal_value > ZERO


def add_decimal(lhs: object, rhs: object) -> Decimal | None:
    left = to_decimal(lhs)
    right = to_decimal(rhs)
    if left is None or right is None:
        return None
    return left + right


def normalize_period(value: object) -> str | None:
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


def period_to_display(period: object) -> str:
    normalized = normalize_period(period)
    if normalized is None:
        return ''
    year, month = normalized.split('-')
    return f'{year}年{month}期'


def resolve_period_column(df: pd.DataFrame) -> str:
    if '月份' in df.columns:
        return '月份'
    if '年期' in df.columns:
        return '年期'
    raise ValueError("缺少周期字段，必须包含 '月份' 或 '年期'")


def map_broad_cost_bucket(cost_item: object) -> str | None:
    if cost_item is None or pd.isna(cost_item):
        return None
    text = str(cost_item).strip()
    if text in BROAD_COST_BUCKET_MAP:
        return BROAD_COST_BUCKET_MAP[text]
    if text.startswith('制造费用'):
        return 'moh'
    return None


def map_component_bucket(cost_item: object) -> str | None:
    if cost_item is None or pd.isna(cost_item):
        return None
    return MOH_COMPONENT_MAP.get(str(cost_item).strip())


def build_join_key(df: pd.DataFrame, columns: list[str], *, normalizer) -> pd.Series:
    """构建工单级 join key。"""
    parts = [
        df[column].map(normalizer) if column in df.columns else pd.Series('', index=df.index) for column in columns
    ]
    if not parts:
        return pd.Series('', index=df.index)
    key = parts[0].copy()
    for part in parts[1:]:
        key = key + '|' + part
    return key


def build_fact_table(work_order_df: pd.DataFrame) -> pd.DataFrame:
    """构建价量分析长表 fact。"""
    grouped = work_order_df.groupby(
        ['product_code', 'product_name', 'period'], dropna=False, as_index=False, sort=False
    ).agg(
        dm_amount=('dm_amount', sum_decimal_series),
        dl_amount=('dl_amount', sum_decimal_series),
        moh_amount=('moh_amount', sum_decimal_series),
        qty=('completed_qty', sum_decimal_series),
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
                    'price': safe_divide(amount, qty),
                }
            )
    return pd.DataFrame(rows)

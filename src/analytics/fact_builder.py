"""事实表构建与通用数值工具。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, localcontext

import pandas as pd
import polars as pl

from src.analytics.contracts import FactBundle
from src.analytics.errors import ERROR_LOG_COLUMNS, empty_error_log_polars, normalize_key_value

ZERO = Decimal('0')
# Fact 层保留较高小数精度，避免在兼容边界前过早量化。
MONEY_DTYPE = pl.Decimal(38, 28)
DIVISION_QUANTIZER = Decimal('1E-28')
MAX_DECIMAL_INTEGER_DIGITS = 10
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


def normalize_money_expr(column_name: str) -> pl.Expr:
    """把金额列统一标准化为 Decimal。"""
    text_expr = pl.col(column_name).cast(pl.String, strict=False).str.strip_chars()
    return (
        pl.when(pl.col(column_name).is_null() | text_expr.eq(''))
        .then(None)
        .otherwise(text_expr.cast(MONEY_DTYPE, strict=False))
        .alias(column_name)
    )


def _resolve_period_column_polars(df: pl.DataFrame) -> str:
    if '月份' in df.columns:
        return '月份'
    if '年期' in df.columns:
        return '年期'
    raise ValueError("缺少周期字段，必须包含 '月份' 或 '年期'")


def _money_zero_expr() -> pl.Expr:
    return pl.lit(ZERO).cast(MONEY_DTYPE)


def _safe_divide_expr(numerator_column: str, denominator_column: str, alias: str) -> pl.Expr:
    # 使用 Decimal 语义逐行计算，并统一到 fact 层固定 scale，避免 float 近似误差。
    return (
        pl.struct([pl.col(numerator_column), pl.col(denominator_column)])
        .map_elements(
            lambda row: _safe_divide_decimal(row[numerator_column], row[denominator_column]),
            return_dtype=MONEY_DTYPE,
        )
        .alias(alias)
    )


def _normalize_key_expr(column_name: str) -> pl.Expr:
    return pl.col(column_name).map_elements(normalize_key_value, return_dtype=pl.String)


def _is_missing_decimal_value(value: object) -> bool:
    return to_decimal(value) is None


def _safe_divide_decimal(numerator: object, denominator: object) -> Decimal | None:
    value = safe_divide(numerator, denominator)
    if value is None:
        return None
    try:
        with localcontext() as ctx:
            ctx.prec = 80
            quantized = value.quantize(DIVISION_QUANTIZER)
        if quantized.is_zero():
            return quantized
        if quantized.adjusted() + 1 > MAX_DECIMAL_INTEGER_DIGITS:
            return None
        return quantized
    except InvalidOperation:
        return None


def _build_error_frame_polars(
    data: pl.DataFrame,
    *,
    issue_type: str,
    field_name: str,
    reason: str,
    action: str,
    row_id_fields: list[str] | None = None,
    original_column: str | None = None,
    lhs_column: str | None = None,
    rhs_column: str | None = None,
    diff_column: str | None = None,
) -> pl.DataFrame:
    if data.is_empty():
        return empty_error_log_polars()

    row_id_exprs: list[pl.Expr] = []
    for field in row_id_fields or ['period', 'product_code', 'order_no', 'order_line', 'cost_bucket']:
        if field in data.columns:
            row_id_exprs.append(_normalize_key_expr(field))
    row_id_expr = (
        pl.concat_str(row_id_exprs, separator='|').alias('row_id') if row_id_exprs else pl.lit('').alias('row_id')
    )

    def _optional_column_expr(column_name: str | None, alias: str) -> pl.Expr:
        if column_name and column_name in data.columns:
            return pl.col(column_name).alias(alias)
        return pl.lit(None).alias(alias)

    frame = data.with_columns(
        [
            row_id_expr,
            (pl.col('cost_bucket') if 'cost_bucket' in data.columns else pl.lit(None)).alias('cost_bucket'),
            (pl.col('product_code') if 'product_code' in data.columns else pl.lit(None)).alias('product_code'),
            (pl.col('product_name') if 'product_name' in data.columns else pl.lit(None)).alias('product_name'),
            (pl.col('period') if 'period' in data.columns else pl.lit(None)).alias('period'),
            pl.lit(issue_type).alias('issue_type'),
            pl.lit(field_name).alias('field_name'),
            _optional_column_expr(original_column, 'original_value'),
            _optional_column_expr(lhs_column, 'lhs'),
            _optional_column_expr(rhs_column, 'rhs'),
            _optional_column_expr(diff_column, 'diff'),
            pl.lit(reason).alias('reason'),
            pl.lit(action).alias('action'),
            pl.lit(False).alias('retryable'),
        ]
    )
    return frame.select(ERROR_LOG_COLUMNS)


def _build_product_summary_fact(work_order_fact: pl.DataFrame) -> pl.DataFrame:
    if work_order_fact.is_empty():
        return pl.DataFrame(
            schema={
                'product_code': pl.String,
                'product_name': pl.String,
                'period': pl.String,
                'period_display': pl.String,
                'total_cost': MONEY_DTYPE,
                'completed_qty': MONEY_DTYPE,
                'dm_cost': MONEY_DTYPE,
                'dl_cost': MONEY_DTYPE,
                'moh_cost': MONEY_DTYPE,
                'unit_cost': MONEY_DTYPE,
                'dm_unit_cost': MONEY_DTYPE,
                'dl_unit_cost': MONEY_DTYPE,
                'moh_unit_cost': MONEY_DTYPE,
                'dm_contrib': MONEY_DTYPE,
                'dl_contrib': MONEY_DTYPE,
                'moh_contrib': MONEY_DTYPE,
            }
        )

    summary = work_order_fact.group_by(['product_code', 'product_name', 'period'], maintain_order=True).agg(
        [
            pl.col('completed_amount_total').sum().cast(MONEY_DTYPE).alias('total_cost'),
            pl.col('completed_qty').sum().cast(MONEY_DTYPE).alias('completed_qty'),
            pl.col('dm_amount').sum().cast(MONEY_DTYPE).alias('dm_cost'),
            pl.col('dl_amount').sum().cast(MONEY_DTYPE).alias('dl_cost'),
            pl.col('moh_amount').sum().cast(MONEY_DTYPE).alias('moh_cost'),
        ]
    )

    return summary.with_columns(
        [
            pl.col('period').map_elements(period_to_display, return_dtype=pl.String).alias('period_display'),
            _safe_divide_expr('total_cost', 'completed_qty', 'unit_cost'),
            _safe_divide_expr('dm_cost', 'completed_qty', 'dm_unit_cost'),
            _safe_divide_expr('dl_cost', 'completed_qty', 'dl_unit_cost'),
            _safe_divide_expr('moh_cost', 'completed_qty', 'moh_unit_cost'),
            _safe_divide_expr('dm_cost', 'total_cost', 'dm_contrib'),
            _safe_divide_expr('dl_cost', 'total_cost', 'dl_contrib'),
            _safe_divide_expr('moh_cost', 'total_cost', 'moh_contrib'),
        ]
    )


def _build_fact_table_polars(work_order_fact: pl.DataFrame) -> pl.DataFrame:
    if work_order_fact.is_empty():
        return pl.DataFrame(
            schema={
                'period': pl.String,
                'product_code': pl.String,
                'product_name': pl.String,
                'cost_bucket': pl.String,
                'amount': MONEY_DTYPE,
                'qty': MONEY_DTYPE,
                'price': MONEY_DTYPE,
            }
        )

    grouped = work_order_fact.group_by(['product_code', 'product_name', 'period'], maintain_order=True).agg(
        [
            pl.col('dm_amount').sum().cast(MONEY_DTYPE).alias('dm_amount'),
            pl.col('dl_amount').sum().cast(MONEY_DTYPE).alias('dl_amount'),
            pl.col('moh_amount').sum().cast(MONEY_DTYPE).alias('moh_amount'),
            pl.col('completed_qty').sum().cast(MONEY_DTYPE).alias('qty'),
        ]
    )

    bucket_defs = (
        ('direct_material', 'dm_amount'),
        ('direct_labor', 'dl_amount'),
        ('moh', 'moh_amount'),
    )
    parts: list[pl.DataFrame] = []
    for bucket_name, amount_column in bucket_defs:
        part = grouped.select(
            [
                'period',
                'product_code',
                'product_name',
                pl.lit(bucket_name).alias('cost_bucket'),
                pl.col(amount_column).alias('amount'),
                pl.col('qty'),
            ]
        ).with_columns(_safe_divide_expr('amount', 'qty', 'price'))
        parts.append(part)
    return pl.concat(parts, how='vertical')


def build_fact_bundle(
    detail_df: pl.DataFrame,
    qty_df: pl.DataFrame,
    *,
    standalone_cost_items: tuple[str, ...],
) -> FactBundle:
    """使用 Polars 构建分析事实集。"""
    detail_period_col = _resolve_period_column_polars(detail_df)
    qty_period_col = _resolve_period_column_polars(qty_df)
    standalone_metas = resolve_standalone_cost_item_metas(standalone_cost_items)
    standalone_item_names = [meta.cost_item for meta in standalone_metas]

    required_detail_cols = {'产品编码', '产品名称', '工单编号', '工单行号', '成本项目名称', '本期完工金额'}
    missing_detail_cols = required_detail_cols.difference(detail_df.columns)
    if missing_detail_cols:
        missing = ', '.join(sorted(missing_detail_cols))
        raise ValueError(f'成本明细缺少必要字段: {missing}')

    required_qty_cols = {'产品编码', '产品名称', '工单编号', '工单行号', '本期完工数量', '本期完工金额'}
    missing_qty_cols = required_qty_cols.difference(qty_df.columns)
    if missing_qty_cols:
        missing = ', '.join(sorted(missing_qty_cols))
        raise ValueError(f'产品数量统计缺少必要字段: {missing}')

    standalone_cost_expr = (
        pl.col('normalized_cost_item').is_in(standalone_item_names) if standalone_item_names else pl.lit(False)
    )
    detail_fact = (
        detail_df.rename(
            {
                '产品编码': 'product_code',
                '产品名称': 'product_name',
                '工单编号': 'order_no',
                '工单行号': 'order_line',
                '成本项目名称': 'cost_item',
                '本期完工金额': 'completed_amount',
            }
        )
        .with_columns(
            [
                pl.col(detail_period_col).map_elements(normalize_period, return_dtype=pl.String).alias('period'),
                pl.col('cost_item').cast(pl.String, strict=False).str.strip_chars().alias('normalized_cost_item'),
                pl.col('cost_item').map_elements(map_broad_cost_bucket, return_dtype=pl.String).alias('cost_bucket'),
                pl.col('cost_item')
                .map_elements(map_component_bucket, return_dtype=pl.String)
                .alias('component_bucket'),
                normalize_money_expr('completed_amount').alias('amount'),
            ]
        )
        .with_columns(
            [
                standalone_cost_expr.alias('is_standalone_cost'),
                (pl.col('cost_bucket').is_not_null() | standalone_cost_expr).alias('is_supported_cost'),
            ]
        )
        .with_columns(
            [
                (pl.col('is_supported_cost') & pl.col('amount').is_null()).alias('is_missing_amount'),
                (pl.col('cost_bucket').is_null() & ~pl.col('is_standalone_cost')).alias('is_unmapped_cost'),
                pl.when(pl.col('is_supported_cost') & pl.col('amount').is_null())
                .then(_money_zero_expr())
                .otherwise(pl.col('amount'))
                .cast(MONEY_DTYPE)
                .alias('amount_filled'),
            ]
        )
    )

    amount_columns = [
        'dm_amount',
        'dl_amount',
        'moh_amount',
        'moh_other_amount',
        'moh_labor_amount',
        'moh_consumables_amount',
        'moh_depreciation_amount',
        'moh_utilities_amount',
        *[meta.amount_key for meta in standalone_metas],
    ]
    component_bucket_targets = (
        'moh_other_amount',
        'moh_labor_amount',
        'moh_consumables_amount',
        'moh_depreciation_amount',
        'moh_utilities_amount',
    )

    work_order_group_exprs: list[pl.Expr] = [
        pl.col('amount_filled')
        .filter(pl.col('cost_bucket') == 'direct_material')
        .sum()
        .cast(MONEY_DTYPE)
        .alias('dm_amount'),
        pl.col('amount_filled')
        .filter(pl.col('cost_bucket') == 'direct_labor')
        .sum()
        .cast(MONEY_DTYPE)
        .alias('dl_amount'),
        pl.col('amount_filled').filter(pl.col('cost_bucket') == 'moh').sum().cast(MONEY_DTYPE).alias('moh_amount'),
    ]
    for component_key in component_bucket_targets:
        work_order_group_exprs.append(
            pl.col('amount_filled')
            .filter(pl.col('component_bucket') == component_key)
            .sum()
            .cast(MONEY_DTYPE)
            .alias(component_key)
        )
    for meta in standalone_metas:
        work_order_group_exprs.append(
            pl.col('amount_filled')
            .filter(pl.col('normalized_cost_item') == meta.cost_item)
            .sum()
            .cast(MONEY_DTYPE)
            .alias(meta.amount_key)
        )

    detail_for_work_order = detail_fact.filter(pl.col('is_supported_cost'))
    work_order_amounts = detail_for_work_order.group_by(
        WORK_ORDER_KEY_COLS + ['product_name'],
        maintain_order=True,
    ).agg(work_order_group_exprs)
    work_order_amounts = work_order_amounts.with_columns(
        [pl.col(column).fill_null(_money_zero_expr()).alias(column) for column in amount_columns]
    )
    work_order_amounts = work_order_amounts.with_columns(
        pl.concat_str([_normalize_key_expr(column) for column in WORK_ORDER_KEY_COLS], separator='|').alias('_join_key')
    )

    total_match_column = build_total_match_column_name(standalone_metas)
    total_mismatch_reason = build_total_mismatch_reason(standalone_metas)
    total_mismatch_error_reason = build_total_mismatch_error_reason(standalone_metas)
    qty_fact = (
        qty_df.with_row_index('_source_row')
        .with_columns(
            [
                pl.col(qty_period_col).map_elements(normalize_period, return_dtype=pl.String).alias('period'),
                pl.col(qty_period_col).map_elements(period_to_display, return_dtype=pl.String).alias('period_display'),
                pl.col('产品编码').cast(pl.String, strict=False).alias('product_code'),
                pl.col('产品名称').cast(pl.String, strict=False).alias('product_name'),
                pl.col('工单编号').alias('order_no'),
                pl.col('工单行号').alias('order_line'),
                pl.col('本期完工数量').alias('completed_qty_raw'),
                pl.col('本期完工金额').alias('completed_amount_total_raw'),
                normalize_money_expr('本期完工数量').alias('completed_qty'),
                normalize_money_expr('本期完工金额').alias('completed_amount_total'),
            ]
        )
        .with_columns(
            [
                pl.concat_str([_normalize_key_expr(column) for column in WORK_ORDER_KEY_COLS], separator='|').alias(
                    '_join_key'
                ),
                pl.col('completed_qty_raw')
                .map_elements(is_positive_decimal, return_dtype=pl.Boolean)
                .fill_null(False)
                .alias('_valid_completed_qty'),
                pl.col('completed_amount_total_raw')
                .map_elements(_is_missing_decimal_value, return_dtype=pl.Boolean)
                .fill_null(True)
                .alias('_missing_total_amount'),
            ]
        )
    )
    qty_fact = qty_fact.filter(pl.col('_valid_completed_qty') & ~pl.col('_missing_total_amount'))

    qty_fact = qty_fact.join(work_order_amounts.select(['_join_key', *amount_columns]), on='_join_key', how='left')
    qty_fact = qty_fact.with_columns(
        [pl.col(column).fill_null(_money_zero_expr()).alias(column) for column in amount_columns]
    )

    qty_amount_assign_exprs: list[pl.Expr] = [
        pl.col('dm_amount').alias(QTY_DM_AMOUNT),
        pl.col('dl_amount').alias(QTY_DL_AMOUNT),
        pl.col('moh_amount').alias(QTY_MOH_AMOUNT),
        pl.col('moh_other_amount').alias(QTY_MOH_OTHER_AMOUNT),
        pl.col('moh_labor_amount').alias(QTY_MOH_LABOR_AMOUNT),
        pl.col('moh_consumables_amount').alias(QTY_MOH_CONSUMABLES_AMOUNT),
        pl.col('moh_depreciation_amount').alias(QTY_MOH_DEPRECIATION_AMOUNT),
        pl.col('moh_utilities_amount').alias(QTY_MOH_UTILITIES_AMOUNT),
    ]
    for meta in standalone_metas:
        qty_amount_assign_exprs.append(pl.col(meta.amount_key).alias(meta.qty_amount_column))
    qty_fact = qty_fact.with_columns(qty_amount_assign_exprs)

    qty_unit_cost_exprs: list[pl.Expr] = [
        _safe_divide_expr(QTY_DM_AMOUNT, 'completed_qty', QTY_DM_UNIT_COST),
        _safe_divide_expr(QTY_DL_AMOUNT, 'completed_qty', QTY_DL_UNIT_COST),
        _safe_divide_expr(QTY_MOH_AMOUNT, 'completed_qty', QTY_MOH_UNIT_COST),
        _safe_divide_expr(QTY_MOH_OTHER_AMOUNT, 'completed_qty', QTY_MOH_OTHER_UNIT_COST),
        _safe_divide_expr(QTY_MOH_LABOR_AMOUNT, 'completed_qty', QTY_MOH_LABOR_UNIT_COST),
        _safe_divide_expr(QTY_MOH_CONSUMABLES_AMOUNT, 'completed_qty', QTY_MOH_CONSUMABLES_UNIT_COST),
        _safe_divide_expr(QTY_MOH_DEPRECIATION_AMOUNT, 'completed_qty', QTY_MOH_DEPRECIATION_UNIT_COST),
        _safe_divide_expr(QTY_MOH_UTILITIES_AMOUNT, 'completed_qty', QTY_MOH_UTILITIES_UNIT_COST),
    ]
    for meta in standalone_metas:
        qty_unit_cost_exprs.append(
            _safe_divide_expr(meta.qty_amount_column, 'completed_qty', meta.qty_unit_cost_column)
        )
    qty_fact = qty_fact.with_columns(qty_unit_cost_exprs)

    qty_fact = qty_fact.with_columns(
        [
            pl.sum_horizontal(
                [
                    pl.col(QTY_MOH_OTHER_AMOUNT),
                    pl.col(QTY_MOH_LABOR_AMOUNT),
                    pl.col(QTY_MOH_CONSUMABLES_AMOUNT),
                    pl.col(QTY_MOH_DEPRECIATION_AMOUNT),
                    pl.col(QTY_MOH_UTILITIES_AMOUNT),
                ]
            )
            .cast(MONEY_DTYPE)
            .alias('moh_component_sum'),
        ]
    )

    total_amount_exprs = [pl.col(QTY_DM_AMOUNT), pl.col(QTY_DL_AMOUNT), pl.col(QTY_MOH_AMOUNT)]
    total_amount_exprs.extend([pl.col(meta.qty_amount_column) for meta in standalone_metas])
    qty_fact = qty_fact.with_columns(
        pl.sum_horizontal(total_amount_exprs).cast(MONEY_DTYPE).alias('derived_total_amount')
    )

    qty_fact = qty_fact.with_columns(
        [
            pl.when(
                pl.col('moh_component_sum').is_not_null()
                & pl.col(QTY_MOH_AMOUNT).is_not_null()
                & (pl.col('moh_component_sum') == pl.col(QTY_MOH_AMOUNT))
            )
            .then(pl.lit('是'))
            .otherwise(pl.lit('否'))
            .alias(QTY_MOH_MATCH),
            pl.when(
                pl.col('derived_total_amount').is_not_null()
                & pl.col('completed_amount_total').is_not_null()
                & (pl.col('derived_total_amount') == pl.col('completed_amount_total'))
            )
            .then(pl.lit('是'))
            .otherwise(pl.lit('否'))
            .alias(total_match_column),
        ]
    )
    qty_fact = qty_fact.with_columns(
        [
            pl.when((pl.col(QTY_MOH_MATCH) == '否') & (pl.col(total_match_column) == '否'))
            .then(pl.lit(f'制造费用明细与合计不一致;{total_mismatch_reason}'))
            .when(pl.col(QTY_MOH_MATCH) == '否')
            .then(pl.lit('制造费用明细与合计不一致'))
            .when(pl.col(total_match_column) == '否')
            .then(pl.lit(total_mismatch_reason))
            .otherwise(pl.lit(''))
            .alias(QTY_CHECK_REASON),
        ]
    )
    qty_fact = qty_fact.with_columns(
        pl.when(pl.col(QTY_CHECK_REASON) == '').then(pl.lit('通过')).otherwise(pl.lit('需复核')).alias(QTY_CHECK_STATUS)
    )

    error_frames: list[pl.DataFrame] = []
    unmapped_error = _build_error_frame_polars(
        detail_fact.filter(pl.col('is_unmapped_cost')).select(
            ['product_code', 'product_name', 'period', 'order_no', 'order_line', 'cost_bucket', 'cost_item']
        ),
        issue_type='UNMAPPED_COST_ITEM',
        field_name='成本项目名称',
        reason='成本项目未映射到直接材料/直接人工/制造费用',
        action='该行已从分析数据中排除',
        original_column='cost_item',
        row_id_fields=WORK_ORDER_KEY_COLS,
    )
    if not unmapped_error.is_empty():
        error_frames.append(unmapped_error)

    missing_amount_error = _build_error_frame_polars(
        detail_fact.filter(pl.col('is_missing_amount')).select(
            [
                'product_code',
                'product_name',
                'period',
                'order_no',
                'order_line',
                'cost_bucket',
                'completed_amount',
            ]
        ),
        issue_type='MISSING_AMOUNT',
        field_name='本期完工金额',
        reason='成本明细金额为空，已按 0 参与汇总',
        action='金额置为 0 后继续计算',
        original_column='completed_amount',
        row_id_fields=WORK_ORDER_KEY_COLS,
    )
    if not missing_amount_error.is_empty():
        error_frames.append(missing_amount_error)

    duplicate_key_error = _build_error_frame_polars(
        qty_fact.filter(pl.col('_join_key').is_duplicated()).select(
            ['product_code', 'product_name', 'period', 'order_no', 'order_line']
        ),
        issue_type='DUPLICATE_WORK_ORDER_KEY',
        field_name='工单主键',
        reason='数量页存在重复工单主键',
        action='数量页原样保留，异常分析按首条记录去重',
        row_id_fields=WORK_ORDER_KEY_COLS,
    )
    if not duplicate_key_error.is_empty():
        error_frames.append(duplicate_key_error)

    moh_mismatch_source = qty_fact.filter(pl.col(QTY_MOH_MATCH) == '否').with_columns(
        (pl.col('moh_component_sum') - pl.col(QTY_MOH_AMOUNT)).cast(MONEY_DTYPE).alias('diff')
    )
    moh_mismatch_error = _build_error_frame_polars(
        moh_mismatch_source.select(
            [
                'product_code',
                'product_name',
                'period',
                'order_no',
                'order_line',
                'moh_component_sum',
                QTY_MOH_AMOUNT,
                'diff',
            ]
        ),
        issue_type='MOH_BREAKDOWN_MISMATCH',
        field_name='制造费用',
        reason='制造费用明细项合计不等于制造费用合计',
        action='保留结果并标记需复核',
        lhs_column='moh_component_sum',
        rhs_column=QTY_MOH_AMOUNT,
        diff_column='diff',
        row_id_fields=WORK_ORDER_KEY_COLS,
    )
    if not moh_mismatch_error.is_empty():
        error_frames.append(moh_mismatch_error)

    total_mismatch_source = qty_fact.filter(pl.col(total_match_column) == '否').with_columns(
        (pl.col('derived_total_amount') - pl.col('completed_amount_total')).cast(MONEY_DTYPE).alias('diff')
    )
    total_mismatch_error = _build_error_frame_polars(
        total_mismatch_source.select(
            [
                'product_code',
                'product_name',
                'period',
                'order_no',
                'order_line',
                'derived_total_amount',
                'completed_amount_total',
                'diff',
            ]
        ),
        issue_type='TOTAL_COST_MISMATCH',
        field_name='总完工成本',
        reason=total_mismatch_error_reason,
        action='保留结果并标记需复核',
        lhs_column='derived_total_amount',
        rhs_column='completed_amount_total',
        diff_column='diff',
        row_id_fields=WORK_ORDER_KEY_COLS,
    )
    if not total_mismatch_error.is_empty():
        error_frames.append(total_mismatch_error)

    error_fact = pl.concat(error_frames, how='vertical_relaxed') if error_frames else empty_error_log_polars()
    if not error_fact.is_empty():
        error_fact = error_fact.select(ERROR_LOG_COLUMNS)

    standalone_unit_exprs = [
        _safe_divide_expr(meta.amount_key, 'completed_qty', meta.unit_cost_key) for meta in standalone_metas
    ]
    work_order_columns = [
        '_join_key',
        '_source_row',
        'period',
        'period_display',
        'product_code',
        'product_name',
        'order_no',
        'order_line',
        'cost_center',
        'spec',
        'unit',
        'completed_qty_raw',
        'completed_amount_total_raw',
        'completed_qty',
        'completed_amount_total',
        'dm_amount',
        'dl_amount',
        'moh_amount',
        'moh_other_amount',
        'moh_labor_amount',
        'moh_consumables_amount',
        'moh_depreciation_amount',
        'moh_utilities_amount',
        *[meta.amount_key for meta in standalone_metas],
        'total_unit_cost',
        'dm_unit_cost',
        'dl_unit_cost',
        'moh_unit_cost',
        'moh_other_unit_cost',
        'moh_labor_unit_cost',
        'moh_consumables_unit_cost',
        'moh_depreciation_unit_cost',
        'moh_utilities_unit_cost',
        *[meta.unit_cost_key for meta in standalone_metas],
    ]
    work_order_fact = (
        qty_fact.sort('_source_row')
        .unique(subset=['_join_key'], keep='first', maintain_order=True)
        .with_columns(
            [
                (pl.col('成本中心名称') if '成本中心名称' in qty_fact.columns else pl.lit(None)).alias('cost_center'),
                (pl.col('规格型号') if '规格型号' in qty_fact.columns else pl.lit(None)).alias('spec'),
                (pl.col('基本单位') if '基本单位' in qty_fact.columns else pl.lit(None)).alias('unit'),
                _safe_divide_expr('completed_amount_total', 'completed_qty', 'total_unit_cost'),
                _safe_divide_expr('dm_amount', 'completed_qty', 'dm_unit_cost'),
                _safe_divide_expr('dl_amount', 'completed_qty', 'dl_unit_cost'),
                _safe_divide_expr('moh_amount', 'completed_qty', 'moh_unit_cost'),
                _safe_divide_expr('moh_other_amount', 'completed_qty', 'moh_other_unit_cost'),
                _safe_divide_expr('moh_labor_amount', 'completed_qty', 'moh_labor_unit_cost'),
                _safe_divide_expr('moh_consumables_amount', 'completed_qty', 'moh_consumables_unit_cost'),
                _safe_divide_expr('moh_depreciation_amount', 'completed_qty', 'moh_depreciation_unit_cost'),
                _safe_divide_expr('moh_utilities_amount', 'completed_qty', 'moh_utilities_unit_cost'),
                *standalone_unit_exprs,
            ]
        )
        .select(work_order_columns)
    )

    product_summary_fact = _build_product_summary_fact(work_order_fact)
    detail_output = _build_fact_table_polars(work_order_fact)
    return FactBundle(
        detail_fact=detail_output,
        qty_fact=qty_fact,
        work_order_fact=work_order_fact,
        product_summary_fact=product_summary_fact,
        error_fact=error_fact,
    )

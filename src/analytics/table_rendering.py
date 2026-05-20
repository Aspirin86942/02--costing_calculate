"""兼容产品异常摘要页渲染准备。"""

from __future__ import annotations

import pandas as pd

from src.analytics.contracts import ProductAnomalySection
from src.analytics.fact_builder import (
    COST_BUCKETS,
    ZERO,
    add_decimal,
    first_decimal,
    period_to_display,
    safe_divide,
    sum_decimal_series,
)
from src.config.pipelines import normalize_product_anomaly_scope_mode

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
LEGACY_SINGLE_SCOPE_MODE = 'legacy_single_scope'
DOC_TYPE_SPLIT_SCOPE_MODE = 'doc_type_split'
DOC_TYPE_SPLIT_SCOPE_LABELS: tuple[str, ...] = ('全部', '正常生产', '返工生产')
DOC_TYPE_NORMAL_LABEL = '正常生产'
DOC_TYPE_REWORK_LABEL = '返工生产'
DOC_TYPE_UNKNOWN_LABEL = '未归类'
DOC_TYPE_TO_SECTION_LABEL: dict[str, str] = {
    '汇报入库-普通生产': DOC_TYPE_NORMAL_LABEL,
    '直接入库-普通生产': DOC_TYPE_NORMAL_LABEL,
    '汇报入库-返工生产': DOC_TYPE_REWORK_LABEL,
}
WORK_ORDER_SUMMARY_REQUIRED_COLUMNS: set[str] = {
    'completed_amount_total',
    'completed_qty',
    'dm_amount',
    'dl_amount',
    'moh_amount',
}
PRODUCT_SUMMARY_REQUIRED_COLUMNS: set[str] = {
    'product_code',
    'product_name',
    'period',
    'period_display',
    'total_cost',
    'completed_qty',
    'dm_cost',
    'dl_cost',
    'moh_cost',
}
DOC_TYPE_SPLIT_SUMMARY_REQUIRED_COLUMNS: set[str] = PRODUCT_SUMMARY_REQUIRED_COLUMNS | {'doc_type'}
DOC_TYPE_SPLIT_WORK_ORDER_REQUIRED_COLUMNS: set[str] = {
    'product_code',
    'product_name',
    'period',
    'doc_type',
    'completed_amount_total',
    'completed_qty',
    'dm_amount',
    'dl_amount',
    'moh_amount',
}


def _finalize_product_summary_metrics(summary_df: pd.DataFrame) -> pd.DataFrame:
    summary_df['unit_cost'] = summary_df['total_cost'].combine(summary_df['completed_qty'], safe_divide)
    summary_df['dm_unit_cost'] = summary_df['dm_cost'].combine(summary_df['completed_qty'], safe_divide)
    summary_df['dl_unit_cost'] = summary_df['dl_cost'].combine(summary_df['completed_qty'], safe_divide)
    summary_df['moh_unit_cost'] = summary_df['moh_cost'].combine(summary_df['completed_qty'], safe_divide)
    summary_df['dm_contrib'] = summary_df['dm_cost'].combine(summary_df['total_cost'], safe_divide)
    summary_df['dl_contrib'] = summary_df['dl_cost'].combine(summary_df['total_cost'], safe_divide)
    summary_df['moh_contrib'] = summary_df['moh_cost'].combine(summary_df['total_cost'], safe_divide)
    if 'period_display' not in summary_df.columns:
        summary_df['period_display'] = summary_df['period'].map(period_to_display)
    else:
        summary_df['period_display'] = summary_df['period_display'].fillna(summary_df['period'].map(period_to_display))
    return summary_df


def build_product_summary_df(work_order_df: pd.DataFrame, *, include_doc_type: bool = False) -> pd.DataFrame:
    if work_order_df.empty:
        columns = ['product_code', 'product_name', 'period', 'period_display']
        if include_doc_type:
            columns.append('doc_type')
        return pd.DataFrame(columns=columns)

    group_columns = ['product_code', 'product_name', 'period']
    if include_doc_type and 'doc_type' in work_order_df.columns:
        group_columns.append('doc_type')

    summary_df = work_order_df.groupby(group_columns, dropna=False, as_index=False, sort=False).agg(
        total_cost=('completed_amount_total', sum_decimal_series),
        completed_qty=('completed_qty', sum_decimal_series),
        dm_cost=('dm_amount', sum_decimal_series),
        dl_cost=('dl_amount', sum_decimal_series),
        moh_cost=('moh_amount', sum_decimal_series),
    )
    return _finalize_product_summary_metrics(summary_df)


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
    return _finalize_product_summary_metrics(summary_df)


def build_product_anomaly_sections(
    summary_df: pd.DataFrame,
    *,
    scope_mode: str = LEGACY_SINGLE_SCOPE_MODE,
) -> list[ProductAnomalySection]:
    """构建兼容产品摘要页。"""
    validated_scope_mode = normalize_product_anomaly_scope_mode(scope_mode)
    normalized_summary_df = _normalize_product_anomaly_source_frame(summary_df, scope_mode=validated_scope_mode)
    if normalized_summary_df.empty:
        return []
    if validated_scope_mode == DOC_TYPE_SPLIT_SCOPE_MODE:
        # 这里统一校验输入契约，避免 pandas 在深层分支抛出不可控 KeyError。
        _validate_doc_type_split_contract(normalized_summary_df)

    sections: list[ProductAnomalySection] = []
    grouped = normalized_summary_df.groupby(['product_code', 'product_name'], dropna=False, sort=False)
    for (product_code, product_name), product_frame in grouped:
        if validated_scope_mode == LEGACY_SINGLE_SCOPE_MODE:
            aggregated_summary = _aggregate_scope_summary_by_period(product_frame)
            sections.append(
                _build_product_anomaly_section(
                    product_code=product_code,
                    product_name=product_name,
                    summary_frame=aggregated_summary,
                    section_label=None,
                )
            )
            continue

        # doc_type_split 先输出“全部”，再按识别到的生产单据类型拆“正常生产/返工生产”。
        aggregated_all = _aggregate_scope_summary_by_period(product_frame)
        sections.append(
            _build_product_anomaly_section(
                product_code=product_code,
                product_name=product_name,
                summary_frame=aggregated_all,
                section_label=DOC_TYPE_SPLIT_SCOPE_LABELS[0],
            )
        )
        scope_labels = product_frame['doc_type'].map(map_doc_type_to_scope_label)
        for section_label in DOC_TYPE_SPLIT_SCOPE_LABELS[1:]:
            scoped_frame = product_frame.loc[scope_labels == section_label]
            if scoped_frame.empty:
                continue
            sections.append(
                _build_product_anomaly_section(
                    product_code=product_code,
                    product_name=product_name,
                    summary_frame=_aggregate_scope_summary_by_period(scoped_frame),
                    section_label=section_label,
                )
            )

    return sections


def _validate_doc_type_split_contract(summary_df: pd.DataFrame) -> None:
    missing_columns = sorted(DOC_TYPE_SPLIT_SUMMARY_REQUIRED_COLUMNS.difference(summary_df.columns))
    required_columns_text = ', '.join(sorted(DOC_TYPE_SPLIT_SUMMARY_REQUIRED_COLUMNS))
    work_order_columns_text = ', '.join(sorted(DOC_TYPE_SPLIT_WORK_ORDER_REQUIRED_COLUMNS))
    if missing_columns:
        missing_columns_text = ', '.join(missing_columns)
        raise ValueError(
            'doc_type_split requires required columns: '
            f'{required_columns_text}; '
            'or work-order columns: '
            f'{work_order_columns_text}; '
            f'missing: {missing_columns_text}'
        )

    # doc_type_split 的核心是按单据类型分段；如果 doc_type 全空，继续执行只会静默退化成“全部”。
    if not summary_df['doc_type'].map(_has_non_empty_doc_type).any():
        raise ValueError(
            'doc_type_split requires non-empty doc_type values after trimming; all doc_type values are empty or missing'
        )


def _has_non_empty_doc_type(value: object) -> bool:
    if value is None or pd.isna(value):
        return False
    return str(value).strip() != ''


def _normalize_product_anomaly_source_frame(summary_df: pd.DataFrame, *, scope_mode: str) -> pd.DataFrame:
    if PRODUCT_SUMMARY_REQUIRED_COLUMNS.issubset(summary_df.columns):
        return summary_df.copy()
    if {'cost_bucket', 'amount', 'qty'}.issubset(summary_df.columns):
        return build_product_summary_from_fact_df(summary_df)
    if WORK_ORDER_SUMMARY_REQUIRED_COLUMNS.issubset(summary_df.columns):
        return build_product_summary_df(
            summary_df,
            include_doc_type=scope_mode == DOC_TYPE_SPLIT_SCOPE_MODE,
        )
    if 'period_display' in summary_df.columns:
        return summary_df.copy()
    return summary_df.copy()


def _aggregate_scope_summary_by_period(scope_df: pd.DataFrame) -> pd.DataFrame:
    aggregated = scope_df.groupby(['period'], dropna=False, as_index=False, sort=False).agg(
        total_cost=('total_cost', sum_decimal_series),
        completed_qty=('completed_qty', sum_decimal_series),
        dm_cost=('dm_cost', sum_decimal_series),
        dl_cost=('dl_cost', sum_decimal_series),
        moh_cost=('moh_cost', sum_decimal_series),
    )
    period_display_map = (
        scope_df[['period', 'period_display']]
        .dropna(subset=['period'])
        .drop_duplicates(subset=['period'], keep='first')
    )
    aggregated = aggregated.merge(period_display_map, on='period', how='left')
    aggregated = _finalize_product_summary_metrics(aggregated)
    return aggregated.sort_values('period').reset_index(drop=True)


def _build_product_anomaly_section(
    *,
    product_code: object,
    product_name: object,
    summary_frame: pd.DataFrame,
    section_label: str | None,
) -> ProductAnomalySection:
    display_data = pd.DataFrame({'月份': summary_frame['period_display']})
    column_types = {'月份': 'text'}
    amount_columns: list[str] = []

    for internal_key, display_name, metric_type, _detect in PRODUCT_ANALYSIS_FIELDS:
        display_data[display_name] = summary_frame[internal_key]
        column_types[display_name] = metric_type
        if metric_type == 'amount':
            amount_columns.append(display_name)

    return ProductAnomalySection(
        product_code=str(product_code),
        product_name=str(product_name),
        data=display_data,
        column_types=column_types,
        amount_columns=amount_columns,
        outlier_cells=set(),
        section_label=section_label,
    )


def map_doc_type_to_scope_label(doc_type: object) -> str:
    if doc_type is None or pd.isna(doc_type):
        return DOC_TYPE_UNKNOWN_LABEL
    normalized_doc_type = str(doc_type).strip()
    if not normalized_doc_type:
        return DOC_TYPE_UNKNOWN_LABEL
    return DOC_TYPE_TO_SECTION_LABEL.get(normalized_doc_type, DOC_TYPE_UNKNOWN_LABEL)

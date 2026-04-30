"""数量页补强与分析产物编排。"""

from __future__ import annotations

import pandas as pd
import polars as pl

from src.analytics.anomaly import ANOMALY_METRICS, build_anomaly_sheet
from src.analytics.contracts import AnalysisArtifacts
from src.analytics.errors import build_error_frame, concat_error_logs
from src.analytics.fact_builder import (
    DEFAULT_STANDALONE_COST_ITEMS,
    QTY_CHECK_REASON,
    QTY_CHECK_STATUS,
    QTY_DL_AMOUNT,
    QTY_DL_UNIT_COST,
    QTY_DM_AMOUNT,
    QTY_DM_UNIT_COST,
    QTY_MOH_AMOUNT,
    QTY_MOH_CONSUMABLES_AMOUNT,
    QTY_MOH_CONSUMABLES_UNIT_COST,
    QTY_MOH_DEPRECIATION_AMOUNT,
    QTY_MOH_DEPRECIATION_UNIT_COST,
    QTY_MOH_LABOR_AMOUNT,
    QTY_MOH_LABOR_UNIT_COST,
    QTY_MOH_MATCH,
    QTY_MOH_OTHER_AMOUNT,
    QTY_MOH_OTHER_UNIT_COST,
    QTY_MOH_UNIT_COST,
    QTY_MOH_UTILITIES_AMOUNT,
    QTY_MOH_UTILITIES_UNIT_COST,
    WORK_ORDER_KEY_COLS,
    ZERO,
    StandaloneCostItemMeta,
    build_fact_bundle,
    build_fact_table,
    build_total_match_column_name,
    resolve_standalone_cost_item_metas,
    safe_divide,
    to_decimal,
)
from src.analytics.quality import build_quality_metrics
from src.analytics.table_rendering import (
    DOC_TYPE_SPLIT_SCOPE_MODE,
    build_product_anomaly_sections,
    build_product_summary_df,
)
from src.config.pipelines import normalize_product_anomaly_scope_mode


def build_report_artifacts(
    df_detail: pd.DataFrame | pl.DataFrame,
    df_qty: pd.DataFrame | pl.DataFrame,
    standalone_cost_items: tuple[str, ...] | list[str] | None = DEFAULT_STANDALONE_COST_ITEMS,
    product_anomaly_scope_mode: str = 'legacy_single_scope',
    month_filter_empty_result: bool = False,
) -> AnalysisArtifacts:
    """构建 V3 报表所需的全部分析产物（Polars 构建 + pandas 兼容输出）。"""
    validated_scope_mode = normalize_product_anomaly_scope_mode(product_anomaly_scope_mode)
    detail_pl = _to_polars_frame(df_detail)
    qty_pl = _to_polars_frame(df_qty)
    detail_pd = _to_pandas_frame(df_detail)
    qty_pd = _to_pandas_frame(df_qty)

    standalone_metas = resolve_standalone_cost_item_metas(standalone_cost_items)
    total_match_column = build_total_match_column_name(standalone_metas)
    fact_bundle = build_fact_bundle(
        detail_pl,
        qty_pl,
        standalone_cost_items=tuple(meta.cost_item for meta in standalone_metas),
    )
    qty_output_columns = _build_qty_output_columns(qty_pl.columns, standalone_metas, total_match_column)
    qty_sheet_with_key = _select_columns_with_fallback(
        fact_bundle.qty_fact,
        qty_output_columns + ['_join_key'],
    )
    qty_sheet_with_key_pd = _restore_qty_sheet_precision(
        _polars_to_pandas(qty_sheet_with_key),
        standalone_metas,
    )
    work_order_source_pd = _restore_work_order_precision(
        _polars_to_pandas(fact_bundle.work_order_fact),
        standalone_metas,
    )

    error_frames: list[pd.DataFrame] = []
    error_fact_pd = _polars_to_pandas(fact_bundle.error_fact)
    if not error_fact_pd.empty:
        error_frames.append(error_fact_pd)
    _append_non_positive_unit_cost_errors(work_order_source_pd, error_frames)

    work_order_sheet = build_anomaly_sheet(work_order_source_pd, standalone_metas=standalone_metas)
    product_summary_df = build_product_summary_df(work_order_source_pd)
    # doc_type_split 依赖工单层单据类型做分段，不能提前聚合到产品维度。
    product_anomaly_source_df = (
        work_order_source_pd if validated_scope_mode == DOC_TYPE_SPLIT_SCOPE_MODE else product_summary_df
    )
    fact_df = build_fact_table(work_order_source_pd)
    filtered_invalid_qty_count, filtered_missing_total_amount_count = _count_filtered_qty_rows(qty_pl)
    quality_metrics = build_quality_metrics(
        detail_pd,
        qty_pd,
        qty_sheet_with_key_pd,
        work_order_sheet.data,
        filtered_invalid_qty_count,
        filtered_missing_total_amount_count,
        month_filter_empty_result=month_filter_empty_result,
    )
    error_log = concat_error_logs(error_frames)

    qty_sheet_output = qty_sheet_with_key_pd.drop(columns=['_join_key'])
    return AnalysisArtifacts(
        fact_df=fact_df,
        qty_sheet_df=qty_sheet_output,
        work_order_sheet=work_order_sheet,
        product_anomaly_sections=build_product_anomaly_sections(
            product_anomaly_source_df,
            scope_mode=validated_scope_mode,
        ),
        quality_metrics=quality_metrics,
        error_log=error_log,
        fact_bundle=fact_bundle,
    )


def _to_polars_frame(frame: pd.DataFrame | pl.DataFrame) -> pl.DataFrame:
    if isinstance(frame, pl.DataFrame):
        return frame.clone()
    return pl.DataFrame(frame.to_dict(orient='list'))


def _to_pandas_frame(frame: pd.DataFrame | pl.DataFrame) -> pd.DataFrame:
    if isinstance(frame, pd.DataFrame):
        return frame.copy()
    return pd.DataFrame(frame.to_dicts())


def _polars_to_pandas(frame: pl.DataFrame) -> pd.DataFrame:
    if frame.is_empty():
        return pd.DataFrame(columns=frame.columns)
    return pd.DataFrame(frame.to_dicts())


def _select_columns_with_fallback(frame: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    exprs: list[pl.Expr] = []
    for column in columns:
        if column in frame.columns:
            exprs.append(pl.col(column))
        else:
            exprs.append(pl.lit(None).alias(column))
    return frame.select(exprs)


def _build_qty_output_columns(
    qty_source_columns: list[str],
    standalone_metas: tuple[StandaloneCostItemMeta, ...],
    total_match_column: str,
) -> list[str]:
    return qty_source_columns + [
        QTY_DM_AMOUNT,
        QTY_DL_AMOUNT,
        QTY_MOH_AMOUNT,
        QTY_MOH_OTHER_AMOUNT,
        QTY_MOH_LABOR_AMOUNT,
        QTY_MOH_CONSUMABLES_AMOUNT,
        QTY_MOH_DEPRECIATION_AMOUNT,
        QTY_MOH_UTILITIES_AMOUNT,
        *[meta.qty_amount_column for meta in standalone_metas],
        QTY_DM_UNIT_COST,
        QTY_DL_UNIT_COST,
        QTY_MOH_UNIT_COST,
        QTY_MOH_OTHER_UNIT_COST,
        QTY_MOH_LABOR_UNIT_COST,
        QTY_MOH_CONSUMABLES_UNIT_COST,
        QTY_MOH_DEPRECIATION_UNIT_COST,
        QTY_MOH_UTILITIES_UNIT_COST,
        *[meta.qty_unit_cost_column for meta in standalone_metas],
        QTY_MOH_MATCH,
        total_match_column,
        QTY_CHECK_STATUS,
        QTY_CHECK_REASON,
    ]


def _count_filtered_qty_rows(qty_df: pl.DataFrame) -> tuple[int, int]:
    if qty_df.is_empty():
        return 0, 0

    normalized = qty_df.with_columns(
        [
            pl.col('本期完工数量')
            .cast(pl.String, strict=False)
            .str.strip_chars()
            .cast(pl.Decimal(38, 28), strict=False)
            .alias('_completed_qty_for_count'),
            pl.col('本期完工金额')
            .cast(pl.String, strict=False)
            .str.strip_chars()
            .cast(pl.Decimal(38, 28), strict=False)
            .alias('_completed_amount_for_count'),
        ]
    )
    valid_qty_expr = pl.col('_completed_qty_for_count').is_not_null() & (pl.col('_completed_qty_for_count') > ZERO)
    result = normalized.select(
        [
            (~valid_qty_expr).sum().alias('filtered_invalid_qty_count'),
            (valid_qty_expr & pl.col('_completed_amount_for_count').is_null())
            .sum()
            .alias('filtered_missing_total_amount_count'),
        ]
    ).row(0)
    return int(result[0]), int(result[1])


def _decimal_or_zero(value: object) -> object:
    decimal_value = to_decimal(value)
    return decimal_value if decimal_value is not None else ZERO


def _restore_qty_sheet_precision(
    qty_sheet_df: pd.DataFrame,
    standalone_metas: tuple[StandaloneCostItemMeta, ...],
) -> pd.DataFrame:
    if qty_sheet_df.empty:
        return qty_sheet_df

    restored = qty_sheet_df.copy()
    completed_qty = restored['本期完工数量'].map(to_decimal)

    amount_columns = [
        QTY_DM_AMOUNT,
        QTY_DL_AMOUNT,
        QTY_MOH_AMOUNT,
        QTY_MOH_OTHER_AMOUNT,
        QTY_MOH_LABOR_AMOUNT,
        QTY_MOH_CONSUMABLES_AMOUNT,
        QTY_MOH_DEPRECIATION_AMOUNT,
        QTY_MOH_UTILITIES_AMOUNT,
        *[meta.qty_amount_column for meta in standalone_metas],
    ]
    for column in amount_columns:
        if column in restored.columns:
            restored[column] = restored[column].map(_decimal_or_zero)

    restored[QTY_DM_UNIT_COST] = restored[QTY_DM_AMOUNT].combine(completed_qty, safe_divide)
    restored[QTY_DL_UNIT_COST] = restored[QTY_DL_AMOUNT].combine(completed_qty, safe_divide)
    restored[QTY_MOH_UNIT_COST] = restored[QTY_MOH_AMOUNT].combine(completed_qty, safe_divide)
    restored[QTY_MOH_OTHER_UNIT_COST] = restored[QTY_MOH_OTHER_AMOUNT].combine(completed_qty, safe_divide)
    restored[QTY_MOH_LABOR_UNIT_COST] = restored[QTY_MOH_LABOR_AMOUNT].combine(completed_qty, safe_divide)
    restored[QTY_MOH_CONSUMABLES_UNIT_COST] = restored[QTY_MOH_CONSUMABLES_AMOUNT].combine(completed_qty, safe_divide)
    restored[QTY_MOH_DEPRECIATION_UNIT_COST] = restored[QTY_MOH_DEPRECIATION_AMOUNT].combine(completed_qty, safe_divide)
    restored[QTY_MOH_UTILITIES_UNIT_COST] = restored[QTY_MOH_UTILITIES_AMOUNT].combine(completed_qty, safe_divide)
    for meta in standalone_metas:
        restored[meta.qty_unit_cost_column] = restored[meta.qty_amount_column].combine(completed_qty, safe_divide)

    return restored


def _restore_work_order_precision(
    work_order_df: pd.DataFrame,
    standalone_metas: tuple[StandaloneCostItemMeta, ...],
) -> pd.DataFrame:
    if work_order_df.empty:
        return work_order_df

    restored = work_order_df.copy()
    completed_qty_source = (
        restored['completed_qty_raw'] if 'completed_qty_raw' in restored.columns else restored['completed_qty']
    )
    completed_qty = completed_qty_source.map(to_decimal)
    restored['completed_qty'] = completed_qty

    amount_columns = ['dm_amount', 'dl_amount', 'moh_amount', *[meta.amount_key for meta in standalone_metas]]
    for column in amount_columns:
        if column in restored.columns:
            restored[column] = restored[column].map(_decimal_or_zero)
    completed_amount_source = (
        restored['completed_amount_total_raw']
        if 'completed_amount_total_raw' in restored.columns
        else restored['completed_amount_total']
    )
    restored['completed_amount_total'] = completed_amount_source.map(to_decimal)

    restored['total_unit_cost'] = restored['completed_amount_total'].combine(completed_qty, safe_divide)
    restored['dm_unit_cost'] = restored['dm_amount'].combine(completed_qty, safe_divide)
    restored['dl_unit_cost'] = restored['dl_amount'].combine(completed_qty, safe_divide)
    restored['moh_unit_cost'] = restored['moh_amount'].combine(completed_qty, safe_divide)
    restored['moh_other_unit_cost'] = restored['moh_other_amount'].combine(completed_qty, safe_divide)
    restored['moh_labor_unit_cost'] = restored['moh_labor_amount'].combine(completed_qty, safe_divide)
    restored['moh_consumables_unit_cost'] = restored['moh_consumables_amount'].combine(completed_qty, safe_divide)
    restored['moh_depreciation_unit_cost'] = restored['moh_depreciation_amount'].combine(completed_qty, safe_divide)
    restored['moh_utilities_unit_cost'] = restored['moh_utilities_amount'].combine(completed_qty, safe_divide)
    for meta in standalone_metas:
        restored[meta.unit_cost_key] = restored[meta.amount_key].combine(completed_qty, safe_divide)

    return restored


def _append_non_positive_unit_cost_errors(work_order_df: pd.DataFrame, error_frames: list[pd.DataFrame]) -> None:
    for metric_key, display_name, _flag_column, _reason in ANOMALY_METRICS:
        if metric_key not in work_order_df.columns:
            continue
        mask = work_order_df[metric_key].map(lambda value: value is not None and value <= ZERO)
        if mask.any():
            error_frames.append(
                build_error_frame(
                    work_order_df.loc[
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

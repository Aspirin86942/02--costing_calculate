"""数量页补强与分析产物编排。"""

from __future__ import annotations

import pandas as pd

from src.analytics.anomaly import ANOMALY_METRICS, build_anomaly_sheet
from src.analytics.contracts import AnalysisArtifacts
from src.analytics.errors import build_error_frame, concat_error_logs, normalize_key_value
from src.analytics.fact_builder import (
    COST_BUCKETS,
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
    QTY_OUTSOURCE_AMOUNT,
    QTY_OUTSOURCE_UNIT_COST,
    QTY_TOTAL_MATCH,
    WORK_ORDER_KEY_COLS,
    ZERO,
    add_decimal,
    build_fact_table,
    build_join_key,
    is_positive_decimal,
    map_broad_cost_bucket,
    map_component_bucket,
    normalize_period,
    period_to_display,
    resolve_period_column,
    safe_divide,
    sum_decimal_series,
    to_decimal,
)
from src.analytics.quality import build_quality_sheet
from src.analytics.table_rendering import build_product_anomaly_sections, build_product_summary_df


def build_report_artifacts(df_detail: pd.DataFrame, df_qty: pd.DataFrame) -> AnalysisArtifacts:
    """构建 V3 报表所需的全部分析产物。"""
    detail_period_col, qty_period_col = _validate_input_frames(df_detail, df_qty)

    error_frames: list[pd.DataFrame] = []
    detail = _prepare_detail_frame(df_detail, detail_period_col, error_frames)
    work_order_amounts = _aggregate_work_order_amounts(detail)
    qty_sheet_df, filtered_invalid_qty_count, filtered_missing_total_amount_count = _prepare_qty_sheet_base(
        df_qty,
        qty_period_col,
    )

    duplicate_qty_mask = qty_sheet_df['_join_key'].duplicated(keep=False)
    if duplicate_qty_mask.any():
        error_frames.append(
            build_error_frame(
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

    qty_sheet_df = _enrich_qty_sheet(qty_sheet_df, work_order_amounts)
    error_frames.extend(_build_qty_reconciliation_errors(qty_sheet_df))

    qty_output_columns = list(df_qty.columns) + [
        QTY_DM_AMOUNT,
        QTY_DL_AMOUNT,
        QTY_MOH_AMOUNT,
        QTY_MOH_OTHER_AMOUNT,
        QTY_MOH_LABOR_AMOUNT,
        QTY_MOH_CONSUMABLES_AMOUNT,
        QTY_MOH_DEPRECIATION_AMOUNT,
        QTY_MOH_UTILITIES_AMOUNT,
        QTY_OUTSOURCE_AMOUNT,
        QTY_DM_UNIT_COST,
        QTY_DL_UNIT_COST,
        QTY_MOH_UNIT_COST,
        QTY_MOH_OTHER_UNIT_COST,
        QTY_MOH_LABOR_UNIT_COST,
        QTY_MOH_CONSUMABLES_UNIT_COST,
        QTY_MOH_DEPRECIATION_UNIT_COST,
        QTY_MOH_UTILITIES_UNIT_COST,
        QTY_OUTSOURCE_UNIT_COST,
        QTY_MOH_MATCH,
        QTY_TOTAL_MATCH,
        QTY_CHECK_STATUS,
        QTY_CHECK_REASON,
    ]
    qty_sheet_output = qty_sheet_df[qty_output_columns + ['_join_key']].copy()

    analysis_source = _build_analysis_source(qty_sheet_df, error_frames)
    fact_df = build_fact_table(analysis_source)
    product_summary_df = build_product_summary_df(analysis_source)
    work_order_sheet = build_anomaly_sheet(analysis_source)
    quality_sheet = build_quality_sheet(
        df_detail,
        df_qty,
        qty_sheet_output,
        work_order_sheet.data,
        filtered_invalid_qty_count,
        filtered_missing_total_amount_count,
    )
    error_log = concat_error_logs(error_frames)

    qty_sheet_output = qty_sheet_output.drop(columns=['_join_key'])
    return AnalysisArtifacts(
        fact_df=fact_df,
        qty_sheet_df=qty_sheet_output,
        work_order_sheet=work_order_sheet,
        product_anomaly_sections=build_product_anomaly_sections(product_summary_df),
        quality_sheet=quality_sheet,
        error_log=error_log,
    )


def _validate_input_frames(df_detail: pd.DataFrame, df_qty: pd.DataFrame) -> tuple[str, str]:
    detail_period_col = resolve_period_column(df_detail)
    qty_period_col = resolve_period_column(df_qty)

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

    return detail_period_col, qty_period_col


def _prepare_detail_frame(
    df_detail: pd.DataFrame,
    detail_period_col: str,
    error_frames: list[pd.DataFrame],
) -> pd.DataFrame:
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
    detail['period'] = detail[detail_period_col].map(normalize_period)
    detail['cost_bucket'] = detail['cost_item'].map(map_broad_cost_bucket)
    detail['component_bucket'] = detail['cost_item'].map(map_component_bucket)
    detail['amount'] = detail['completed_amount'].map(to_decimal)
    outsource_cost_mask = detail['cost_item'].astype(str).str.strip().eq('委外加工费')

    unmapped_mask = detail['cost_bucket'].isna() & ~outsource_cost_mask
    if unmapped_mask.any():
        error_frames.append(
            build_error_frame(
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

    supported_cost_mask = detail['cost_bucket'].notna() | outsource_cost_mask
    missing_detail_amount = detail['amount'].isna() & supported_cost_mask
    if missing_detail_amount.any():
        error_frames.append(
            build_error_frame(
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

    return detail


def _aggregate_work_order_amounts(detail: pd.DataFrame) -> pd.DataFrame:
    detail_for_analysis = detail.loc[detail['cost_bucket'].notna()].copy()
    detail_outsource = detail.loc[detail['cost_item'].astype(str).str.strip().eq('委外加工费')].copy()

    broad_amounts = (
        detail_for_analysis.groupby(
            WORK_ORDER_KEY_COLS + ['product_name', 'cost_bucket'], dropna=False, as_index=False, sort=False
        )
        .agg(amount=('amount', sum_decimal_series))
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
        columns={'direct_material': 'dm_amount', 'direct_labor': 'dl_amount', 'moh': 'moh_amount'}
    )

    component_amounts = (
        detail_for_analysis.loc[detail_for_analysis['component_bucket'].notna()]
        .groupby(WORK_ORDER_KEY_COLS + ['product_name', 'component_bucket'], dropna=False, as_index=False, sort=False)
        .agg(amount=('amount', sum_decimal_series))
        .pivot_table(
            index=WORK_ORDER_KEY_COLS + ['product_name'],
            columns='component_bucket',
            values='amount',
            aggfunc='first',
            sort=False,
        )
        .reset_index()
    )
    outsource_amounts = detail_outsource.groupby(
        WORK_ORDER_KEY_COLS + ['product_name'], dropna=False, as_index=False, sort=False
    ).agg(outsource_amount=('amount', sum_decimal_series))

    work_order_amounts = broad_amounts.merge(component_amounts, on=WORK_ORDER_KEY_COLS + ['product_name'], how='left')
    work_order_amounts = work_order_amounts.merge(
        outsource_amounts, on=WORK_ORDER_KEY_COLS + ['product_name'], how='outer'
    )
    for column in [
        'dm_amount',
        'dl_amount',
        'moh_amount',
        'moh_other_amount',
        'moh_labor_amount',
        'moh_consumables_amount',
        'moh_depreciation_amount',
        'moh_utilities_amount',
        'outsource_amount',
    ]:
        if column not in work_order_amounts.columns:
            work_order_amounts[column] = ZERO
        work_order_amounts[column] = (
            work_order_amounts[column].map(to_decimal).map(lambda value: value if value is not None else ZERO)
        )

    work_order_amounts['_join_key'] = build_join_key(
        work_order_amounts,
        WORK_ORDER_KEY_COLS,
        normalizer=normalize_key_value,
    )
    return work_order_amounts


def _prepare_qty_sheet_base(
    df_qty: pd.DataFrame,
    qty_period_col: str,
) -> tuple[pd.DataFrame, int, int]:
    qty_sheet_df = df_qty.copy().reset_index(drop=True)
    qty_sheet_df['_source_row'] = range(len(qty_sheet_df))
    qty_sheet_df['period'] = qty_sheet_df[qty_period_col].map(normalize_period)
    qty_sheet_df['period_display'] = qty_sheet_df['period'].map(period_to_display)
    qty_sheet_df['product_code'] = qty_sheet_df['产品编码'].astype(str)
    qty_sheet_df['product_name'] = qty_sheet_df['产品名称'].astype(str)
    qty_sheet_df['order_no'] = qty_sheet_df['工单编号']
    qty_sheet_df['order_line'] = qty_sheet_df['工单行号']
    qty_sheet_df['completed_qty'] = qty_sheet_df['本期完工数量'].map(to_decimal)
    qty_sheet_df['completed_amount_total'] = qty_sheet_df['本期完工金额'].map(to_decimal)
    qty_sheet_df['_join_key'] = build_join_key(qty_sheet_df, WORK_ORDER_KEY_COLS, normalizer=normalize_key_value)

    valid_completed_qty_mask = qty_sheet_df['completed_qty'].map(is_positive_decimal)
    missing_total_amount_mask = valid_completed_qty_mask & qty_sheet_df['completed_amount_total'].isna()
    filtered_invalid_qty_count = int((~valid_completed_qty_mask).sum())
    filtered_missing_total_amount_count = int(missing_total_amount_mask.sum())

    qty_sheet_df = qty_sheet_df.loc[valid_completed_qty_mask & qty_sheet_df['completed_amount_total'].notna()].copy()
    return qty_sheet_df, filtered_invalid_qty_count, filtered_missing_total_amount_count


def _enrich_qty_sheet(qty_sheet_df: pd.DataFrame, work_order_amounts: pd.DataFrame) -> pd.DataFrame:
    amount_columns = [
        'dm_amount',
        'dl_amount',
        'moh_amount',
        'moh_other_amount',
        'moh_labor_amount',
        'moh_consumables_amount',
        'moh_depreciation_amount',
        'moh_utilities_amount',
        'outsource_amount',
    ]
    qty_sheet_df = qty_sheet_df.merge(
        work_order_amounts[['_join_key'] + amount_columns].drop_duplicates('_join_key'),
        on='_join_key',
        how='left',
    )
    for column in amount_columns:
        qty_sheet_df[column] = (
            qty_sheet_df[column].map(to_decimal).map(lambda value: value if value is not None else ZERO)
        )

    qty_sheet_df[QTY_DM_AMOUNT] = qty_sheet_df['dm_amount']
    qty_sheet_df[QTY_DL_AMOUNT] = qty_sheet_df['dl_amount']
    qty_sheet_df[QTY_MOH_AMOUNT] = qty_sheet_df['moh_amount']
    qty_sheet_df[QTY_MOH_OTHER_AMOUNT] = qty_sheet_df['moh_other_amount']
    qty_sheet_df[QTY_MOH_LABOR_AMOUNT] = qty_sheet_df['moh_labor_amount']
    qty_sheet_df[QTY_MOH_CONSUMABLES_AMOUNT] = qty_sheet_df['moh_consumables_amount']
    qty_sheet_df[QTY_MOH_DEPRECIATION_AMOUNT] = qty_sheet_df['moh_depreciation_amount']
    qty_sheet_df[QTY_MOH_UTILITIES_AMOUNT] = qty_sheet_df['moh_utilities_amount']
    qty_sheet_df[QTY_OUTSOURCE_AMOUNT] = qty_sheet_df['outsource_amount']

    qty_sheet_df[QTY_DM_UNIT_COST] = qty_sheet_df[QTY_DM_AMOUNT].combine(qty_sheet_df['completed_qty'], safe_divide)
    qty_sheet_df[QTY_DL_UNIT_COST] = qty_sheet_df[QTY_DL_AMOUNT].combine(qty_sheet_df['completed_qty'], safe_divide)
    qty_sheet_df[QTY_MOH_UNIT_COST] = qty_sheet_df[QTY_MOH_AMOUNT].combine(qty_sheet_df['completed_qty'], safe_divide)
    qty_sheet_df[QTY_MOH_OTHER_UNIT_COST] = qty_sheet_df[QTY_MOH_OTHER_AMOUNT].combine(
        qty_sheet_df['completed_qty'], safe_divide
    )
    qty_sheet_df[QTY_MOH_LABOR_UNIT_COST] = qty_sheet_df[QTY_MOH_LABOR_AMOUNT].combine(
        qty_sheet_df['completed_qty'], safe_divide
    )
    qty_sheet_df[QTY_MOH_CONSUMABLES_UNIT_COST] = qty_sheet_df[QTY_MOH_CONSUMABLES_AMOUNT].combine(
        qty_sheet_df['completed_qty'], safe_divide
    )
    qty_sheet_df[QTY_MOH_DEPRECIATION_UNIT_COST] = qty_sheet_df[QTY_MOH_DEPRECIATION_AMOUNT].combine(
        qty_sheet_df['completed_qty'], safe_divide
    )
    qty_sheet_df[QTY_MOH_UTILITIES_UNIT_COST] = qty_sheet_df[QTY_MOH_UTILITIES_AMOUNT].combine(
        qty_sheet_df['completed_qty'], safe_divide
    )
    qty_sheet_df[QTY_OUTSOURCE_UNIT_COST] = qty_sheet_df[QTY_OUTSOURCE_AMOUNT].combine(
        qty_sheet_df['completed_qty'], safe_divide
    )

    qty_sheet_df['moh_component_sum'] = (
        qty_sheet_df[QTY_MOH_OTHER_AMOUNT]
        .combine(qty_sheet_df[QTY_MOH_LABOR_AMOUNT], add_decimal)
        .combine(qty_sheet_df[QTY_MOH_CONSUMABLES_AMOUNT], add_decimal)
        .combine(qty_sheet_df[QTY_MOH_DEPRECIATION_AMOUNT], add_decimal)
        .combine(qty_sheet_df[QTY_MOH_UTILITIES_AMOUNT], add_decimal)
    )
    qty_sheet_df['derived_total_amount'] = (
        qty_sheet_df[QTY_DM_AMOUNT]
        .combine(qty_sheet_df[QTY_DL_AMOUNT], add_decimal)
        .combine(qty_sheet_df[QTY_MOH_AMOUNT], add_decimal)
        .combine(qty_sheet_df[QTY_OUTSOURCE_AMOUNT], add_decimal)
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

    qty_reason = pd.Series('', index=qty_sheet_df.index, dtype='object')
    qty_reason = qty_reason.mask(qty_sheet_df[QTY_MOH_MATCH].eq('否'), '制造费用明细与合计不一致')
    total_mismatch_mask = qty_sheet_df[QTY_TOTAL_MATCH].eq('否')
    qty_reason.loc[total_mismatch_mask & qty_reason.ne('')] = (
        qty_reason.loc[total_mismatch_mask & qty_reason.ne('')]
        + ';直接材料+直接人工+制造费用+委外加工费与总完工成本不一致'
    )
    qty_reason.loc[total_mismatch_mask & qty_reason.eq('')] = '直接材料+直接人工+制造费用+委外加工费与总完工成本不一致'
    qty_sheet_df[QTY_CHECK_REASON] = qty_reason
    qty_sheet_df[QTY_CHECK_STATUS] = (
        qty_sheet_df[QTY_CHECK_REASON].eq('').map(lambda value: '通过' if value else '需复核')
    )
    return qty_sheet_df


def _build_qty_reconciliation_errors(qty_sheet_df: pd.DataFrame) -> list[pd.DataFrame]:
    error_frames: list[pd.DataFrame] = []

    moh_mismatch_mask = qty_sheet_df[QTY_MOH_MATCH].eq('否')
    if moh_mismatch_mask.any():
        mismatch_frame = qty_sheet_df.loc[
            moh_mismatch_mask,
            ['product_code', 'product_name', 'period', 'order_no', 'order_line', 'moh_component_sum', QTY_MOH_AMOUNT],
        ].rename(columns={QTY_MOH_AMOUNT: 'moh_amount_output'})
        mismatch_frame['diff'] = mismatch_frame['moh_component_sum'].combine(
            mismatch_frame['moh_amount_output'],
            lambda lhs, rhs: None
            if to_decimal(lhs) is None or to_decimal(rhs) is None
            else to_decimal(lhs) - to_decimal(rhs),
        )
        error_frames.append(
            build_error_frame(
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
            if to_decimal(lhs) is None or to_decimal(rhs) is None
            else to_decimal(lhs) - to_decimal(rhs),
        )
        error_frames.append(
            build_error_frame(
                total_frame,
                issue_type='TOTAL_COST_MISMATCH',
                field_name='总完工成本',
                reason='直接材料+直接人工+制造费用+委外加工费不等于数量页总完工成本',
                action='保留结果并标记需复核',
                lhs_column='derived_total_amount',
                rhs_column='completed_amount_total',
                diff_column='diff',
                row_id_fields=WORK_ORDER_KEY_COLS,
            )
        )

    return error_frames


def _build_analysis_source(qty_sheet_df: pd.DataFrame, error_frames: list[pd.DataFrame]) -> pd.DataFrame:
    analysis_source = qty_sheet_df.sort_values('_source_row').drop_duplicates('_join_key', keep='first').copy()
    analysis_source = analysis_source.drop(
        columns=['月份', '产品编码', '产品名称', '工单编号', '工单行号', '本期完工数量'], errors='ignore'
    )
    analysis_source = analysis_source.rename(
        columns={'成本中心名称': 'cost_center', '规格型号': 'spec', '基本单位': 'unit'}
    )
    analysis_source['total_unit_cost'] = analysis_source['completed_amount_total'].combine(
        analysis_source['completed_qty'], safe_divide
    )
    analysis_source['dm_unit_cost'] = analysis_source[QTY_DM_AMOUNT].combine(
        analysis_source['completed_qty'], safe_divide
    )
    analysis_source['dl_unit_cost'] = analysis_source[QTY_DL_AMOUNT].combine(
        analysis_source['completed_qty'], safe_divide
    )
    analysis_source['moh_unit_cost'] = analysis_source[QTY_MOH_AMOUNT].combine(
        analysis_source['completed_qty'], safe_divide
    )
    analysis_source['moh_other_unit_cost'] = analysis_source[QTY_MOH_OTHER_AMOUNT].combine(
        analysis_source['completed_qty'], safe_divide
    )
    analysis_source['moh_labor_unit_cost'] = analysis_source[QTY_MOH_LABOR_AMOUNT].combine(
        analysis_source['completed_qty'], safe_divide
    )
    analysis_source['moh_consumables_unit_cost'] = analysis_source[QTY_MOH_CONSUMABLES_AMOUNT].combine(
        analysis_source['completed_qty'], safe_divide
    )
    analysis_source['moh_depreciation_unit_cost'] = analysis_source[QTY_MOH_DEPRECIATION_AMOUNT].combine(
        analysis_source['completed_qty'], safe_divide
    )
    analysis_source['moh_utilities_unit_cost'] = analysis_source[QTY_MOH_UTILITIES_AMOUNT].combine(
        analysis_source['completed_qty'], safe_divide
    )
    for column in ['dm_amount', 'dl_amount', 'moh_amount', 'outsource_amount']:
        if column not in analysis_source.columns:
            analysis_source[column] = ZERO

    for metric_key, display_name, _flag_column, _reason in ANOMALY_METRICS:
        mask = analysis_source[metric_key].map(lambda value: value is not None and value <= ZERO)
        if mask.any():
            error_frames.append(
                build_error_frame(
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
    return analysis_source

"""SheetModel 展示层构建。"""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd
import polars as pl

from src.analytics.anomaly import build_work_order_conditional_formats
from src.analytics.contracts import (
    ConditionalFormatRule,
    FactBundle,
    FlatSheet,
    ProductAnomalySection,
    SheetModel,
)
from src.analytics.table_rendering import (
    PRODUCT_SUMMARY_SHEET_COLUMN_TYPES,
    build_product_summary_sheet_frame,
)

_NUMBER_FORMAT_BY_TYPE: dict[str, str] = {
    'amount': '#,##0.00',
    'price': '#,##0.00',
    'qty': '#,##0.00',
    'score': '0.0000',
    'pct': '0.00%',
}
_DETAIL_TWO_DECIMAL_COLUMNS: set[str] = {'本期完工单位成本', '本期完工金额'}
_QTY_TWO_DECIMAL_COLUMNS: set[str] = {
    '本期完工单位成本',
    '本期完工金额',
    '本期完工直接材料合计完工金额',
    '本期完工直接人工合计完工金额',
    '本期完工制造费用合计完工金额',
    '本期完工制造费用_其他合计完工金额',
    '本期完工制造费用_人工合计完工金额',
    '本期完工制造费用_机物料及低耗合计完工金额',
    '本期完工制造费用_折旧合计完工金额',
    '本期完工制造费用_水电费合计完工金额',
    '本期完工委外加工费合计完工金额',
    '直接材料单位完工金额',
    '直接人工单位完工金额',
    '制造费用单位完工金额',
    '制造费用_其他单位完工成本',
    '制造费用_人工单位完工成本',
    '制造费用_机物料及低耗单位完工成本',
    '制造费用_折旧单位完工成本',
    '制造费用_水电费单位完工成本',
    '委外加工费单位完工成本',
}
_ANALYSIS_SHEET_COLUMN_LAYOUT: dict[str, tuple[str, ...]] = {
    '直接材料_价量比': (
        '产品编码',
        '产品名称',
        '月份',
        '直接材料成本',
        '完工数量',
        '单位直接材料成本',
        '直接材料贡献率',
    ),
    '直接人工_价量比': (
        '产品编码',
        '产品名称',
        '月份',
        '直接人工成本',
        '完工数量',
        '单位直接人工成本',
        '直接人工贡献率',
    ),
    '制造费用_价量比': (
        '产品编码',
        '产品名称',
        '月份',
        '制造费用成本',
        '完工数量',
        '单位制造费用成本',
        '制造费用贡献率',
    ),
}


def dataframe_to_sheet_model(
    *,
    sheet_name: str,
    frame: pl.DataFrame,
    column_types: Mapping[str, str],
    number_formats: Mapping[str, str],
    freeze_panes: str | None = 'A2',
    auto_filter: bool = True,
    fixed_width: float | None = 15.0,
    conditional_formats: tuple[ConditionalFormatRule, ...] = (),
) -> SheetModel:
    """把列式数据包装成写出契约。"""
    return SheetModel(
        sheet_name=sheet_name,
        columns=tuple(frame.columns),
        rows_factory=lambda frame=frame: frame.iter_rows(),
        column_types={column: column_types[column] for column in frame.columns if column in column_types},
        number_formats={column: number_formats[column] for column in frame.columns if column in number_formats},
        freeze_panes=freeze_panes,
        auto_filter=auto_filter,
        fixed_width=fixed_width,
        conditional_formats=conditional_formats,
    )


def build_sheet_models(
    *,
    detail_df: pl.DataFrame | pd.DataFrame,
    qty_sheet_df: pd.DataFrame | pl.DataFrame,
    fact_bundle: FactBundle | None,
    work_order_sheet: FlatSheet,
    product_anomaly_sections: list[ProductAnomalySection],
    error_log: pd.DataFrame | pl.DataFrame,
) -> tuple[SheetModel, ...]:
    """构建 workbook 的 8 张标准 SheetModel。"""
    detail_frame = _to_polars_frame(detail_df)
    qty_frame = _to_polars_frame(qty_sheet_df)
    error_frame = _to_polars_frame(error_log)

    summary_source = (
        fact_bundle.product_summary_fact
        if fact_bundle is not None
        else pl.DataFrame(schema={'product_code': pl.String, 'product_name': pl.String, 'period_display': pl.String})
    )
    summary_df = build_product_summary_sheet_frame(summary_source)

    work_order_frame = _to_polars_frame(work_order_sheet.data)
    work_order_column_types = dict(work_order_sheet.column_types)
    work_order_number_formats = _build_number_formats(work_order_column_types)
    work_order_conditional_formats = build_work_order_conditional_formats(list(work_order_frame.columns))

    product_anomaly_frame, product_anomaly_column_types = _build_product_anomaly_frame(product_anomaly_sections)
    product_anomaly_number_formats = _build_number_formats(product_anomaly_column_types)

    detail_model = dataframe_to_sheet_model(
        sheet_name='成本明细',
        frame=detail_frame,
        column_types=dict.fromkeys(detail_frame.columns, 'text'),
        number_formats={
            column: '#,##0.00' for column in detail_frame.columns if column in _DETAIL_TWO_DECIMAL_COLUMNS
        },
    )
    qty_two_decimal_columns = _resolve_qty_two_decimal_columns(tuple(qty_frame.columns))
    qty_model = dataframe_to_sheet_model(
        sheet_name='产品数量统计',
        frame=qty_frame,
        column_types=dict.fromkeys(qty_frame.columns, 'text'),
        number_formats={column: '#,##0.00' for column in qty_frame.columns if column in qty_two_decimal_columns},
    )

    analysis_models = tuple(
        _build_analysis_sheet_model(sheet_name=sheet_name, summary_df=summary_df)
        for sheet_name in ('直接材料_价量比', '直接人工_价量比', '制造费用_价量比')
    )

    work_order_model = dataframe_to_sheet_model(
        sheet_name='按工单按产品异常值分析',
        frame=work_order_frame,
        column_types=work_order_column_types,
        number_formats=work_order_number_formats,
        conditional_formats=work_order_conditional_formats,
    )
    product_anomaly_model = dataframe_to_sheet_model(
        sheet_name='按产品异常值分析',
        frame=product_anomaly_frame,
        column_types=product_anomaly_column_types,
        number_formats=product_anomaly_number_formats,
        freeze_panes='A6',
        fixed_width=15.0,
    )
    error_log_model = dataframe_to_sheet_model(
        sheet_name='error_log',
        frame=error_frame,
        column_types=dict.fromkeys(error_frame.columns, 'text'),
        number_formats={},
        freeze_panes=None,
        auto_filter=False,
        fixed_width=None,
    )

    return (
        detail_model,
        qty_model,
        *analysis_models,
        work_order_model,
        product_anomaly_model,
        error_log_model,
    )


def _build_analysis_sheet_model(*, sheet_name: str, summary_df: pd.DataFrame) -> SheetModel:
    columns = _ANALYSIS_SHEET_COLUMN_LAYOUT[sheet_name]
    frame_df = summary_df.reindex(columns=columns)
    frame = _to_polars_frame(frame_df)
    column_types = {
        column: PRODUCT_SUMMARY_SHEET_COLUMN_TYPES[column]
        for column in columns
        if column in PRODUCT_SUMMARY_SHEET_COLUMN_TYPES
    }
    number_formats = _build_number_formats(column_types)
    return dataframe_to_sheet_model(
        sheet_name=sheet_name,
        frame=frame,
        column_types=column_types,
        number_formats=number_formats,
        freeze_panes='C3',
    )


def _build_product_anomaly_frame(
    sections: list[ProductAnomalySection],
) -> tuple[pl.DataFrame, dict[str, str]]:
    if not sections:
        empty_columns = ['产品编码', '产品名称', '月份']
        return (
            _to_polars_frame(pd.DataFrame(columns=empty_columns)),
            {'产品编码': 'text', '产品名称': 'text', '月份': 'text'},
        )

    section_frames: list[pd.DataFrame] = []
    column_types: dict[str, str] = {'产品编码': 'text', '产品名称': 'text'}
    for section in sections:
        section_df = section.data.copy()
        section_df.insert(0, '产品名称', section.product_name)
        section_df.insert(0, '产品编码', section.product_code)
        section_frames.append(section_df)
        for column_name, metric_type in section.column_types.items():
            column_types.setdefault(column_name, metric_type)

    merged = pd.concat(section_frames, ignore_index=True, sort=False)
    for column_name in merged.columns:
        column_types.setdefault(column_name, 'text')
    return _to_polars_frame(merged), column_types


def _build_number_formats(column_types: Mapping[str, str]) -> dict[str, str]:
    return {
        column_name: _NUMBER_FORMAT_BY_TYPE[metric_type]
        for column_name, metric_type in column_types.items()
        if metric_type in _NUMBER_FORMAT_BY_TYPE
    }


def _to_polars_frame(frame: pl.DataFrame | pd.DataFrame) -> pl.DataFrame:
    if isinstance(frame, pl.DataFrame):
        return frame.clone()
    # 使用 to_dict(list) 保持 pyarrow-free，兼容 test 环境最小依赖。
    frame_dict = {
        column_name: [None if pd.isna(value) else value for value in values]
        for column_name, values in frame.to_dict(orient='list').items()
    }
    return pl.DataFrame(frame_dict, strict=False)


def _resolve_qty_two_decimal_columns(columns: tuple[str, ...]) -> set[str]:
    dynamic_columns = {
        column_name
        for column_name in columns
        if (
            (column_name.startswith('本期完工') and column_name.endswith('合计完工金额'))
            or column_name.endswith('单位完工成本')
        )
    }
    return _QTY_TWO_DECIMAL_COLUMNS | dynamic_columns

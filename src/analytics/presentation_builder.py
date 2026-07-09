"""SheetModel 展示层构建。"""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd
import polars as pl

from src.analytics.contracts import (
    ConditionalFormatRule,
    FactBundle,
    FlatSheet,
    ProductAnomalySection,
    SheetModel,
    StyleProfile,
    WriteMode,
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
_FAST_EXPORT_WRITE_MODES: set[WriteMode] = {'dataframe_fast'}
_FAST_EXPORT_STYLE_PROFILES: set[StyleProfile] = {'lightweight_flat'}


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
    write_mode: WriteMode | None = None,
    style_profile: StyleProfile | None = None,
    source_frame: pl.DataFrame | None = None,
) -> SheetModel:
    """把列式数据包装成写出契约。"""
    _validate_fast_export_metadata(
        sheet_name=sheet_name,
        frame=frame,
        write_mode=write_mode,
        style_profile=style_profile,
        source_frame=source_frame,
    )
    resolved_frame = source_frame if source_frame is not None else frame
    return SheetModel(
        sheet_name=sheet_name,
        columns=tuple(resolved_frame.columns),
        rows_factory=lambda frame=resolved_frame: frame.iter_rows(),
        column_types={column: column_types[column] for column in resolved_frame.columns if column in column_types},
        number_formats={
            column: number_formats[column] for column in resolved_frame.columns if column in number_formats
        },
        freeze_panes=freeze_panes,
        auto_filter=auto_filter,
        fixed_width=fixed_width,
        conditional_formats=conditional_formats,
        write_mode=write_mode,
        style_profile=style_profile,
        source_frame=source_frame,
    )


def _validate_fast_export_metadata(
    *,
    sheet_name: str,
    frame: pl.DataFrame,
    write_mode: WriteMode | None,
    style_profile: StyleProfile | None,
    source_frame: pl.DataFrame | None,
) -> None:
    has_any = write_mode is not None or style_profile is not None or source_frame is not None
    if not has_any:
        return
    # fast-export 需要成组出现，避免后续 writer routing 遇到矛盾状态。
    if write_mode is None or style_profile is None or source_frame is None:
        raise ValueError(f'fast export metadata incomplete for sheet={sheet_name}')
    if write_mode not in _FAST_EXPORT_WRITE_MODES:
        raise ValueError(f'unsupported write_mode for sheet={sheet_name}: {write_mode}')
    if style_profile not in _FAST_EXPORT_STYLE_PROFILES:
        raise ValueError(f'unsupported style_profile for sheet={sheet_name}: {style_profile}')
    if not isinstance(source_frame, pl.DataFrame):
        raise ValueError(f'source_frame must be polars DataFrame for sheet={sheet_name}')
    if tuple(source_frame.columns) != tuple(frame.columns):
        raise ValueError(f'source_frame columns mismatch for sheet={sheet_name}')


def build_sheet_models(
    *,
    detail_df: pl.DataFrame | pd.DataFrame,
    qty_sheet_df: pd.DataFrame | pl.DataFrame,
    fact_bundle: FactBundle | None,
    work_order_sheet: FlatSheet,
    product_anomaly_sections: list[ProductAnomalySection],
) -> tuple[SheetModel, ...]:
    """构建默认 workbook 的 3 张业务 SheetModel。"""
    detail_frame = _to_polars_frame(detail_df)
    qty_frame = _to_polars_frame(qty_sheet_df)

    work_order_frame = _to_polars_frame(work_order_sheet.data)
    work_order_column_types = dict(work_order_sheet.column_types)
    work_order_number_formats = _build_number_formats(work_order_column_types)

    detail_model = dataframe_to_sheet_model(
        sheet_name='成本计算单总表',
        frame=detail_frame,
        column_types=dict.fromkeys(detail_frame.columns, 'text'),
        number_formats={column: '#,##0.00' for column in detail_frame.columns if column in _DETAIL_TWO_DECIMAL_COLUMNS},
        write_mode='dataframe_fast',
        style_profile='lightweight_flat',
        source_frame=detail_frame,
    )
    qty_two_decimal_columns = _resolve_qty_two_decimal_columns(tuple(qty_frame.columns))
    qty_model = dataframe_to_sheet_model(
        sheet_name='成本计算单数量聚合维度',
        frame=qty_frame,
        column_types=dict.fromkeys(qty_frame.columns, 'text'),
        number_formats={column: '#,##0.00' for column in qty_frame.columns if column in qty_two_decimal_columns},
        write_mode='dataframe_fast',
        style_profile='lightweight_flat',
        source_frame=qty_frame,
    )

    work_order_model = dataframe_to_sheet_model(
        sheet_name='成本分析工单维度',
        frame=work_order_frame,
        column_types=work_order_column_types,
        number_formats=work_order_number_formats,
        write_mode='dataframe_fast',
        style_profile='lightweight_flat',
        source_frame=work_order_frame,
    )
    return (
        detail_model,
        qty_model,
        work_order_model,
    )


def build_product_anomaly_sheet_model(product_anomaly_sections: list[ProductAnomalySection]) -> SheetModel:
    """构建 legacy 产品维度 SheetModel，调用方需显式选择该非默认 sheet。"""
    (
        product_anomaly_frame,
        product_anomaly_column_types,
        has_scoped_product_anomaly_section,
    ) = _build_product_anomaly_frame(product_anomaly_sections)
    product_anomaly_number_formats = _build_number_formats(product_anomaly_column_types)
    return dataframe_to_sheet_model(
        sheet_name='成本分析产品维度',
        frame=product_anomaly_frame,
        column_types=product_anomaly_column_types,
        number_formats=product_anomaly_number_formats,
        freeze_panes='A5' if has_scoped_product_anomaly_section else 'A4',
        fixed_width=15.0,
    )


def _build_product_anomaly_frame(
    sections: list[ProductAnomalySection],
) -> tuple[pl.DataFrame, dict[str, str], bool]:
    has_scoped_section = any(section.section_label is not None for section in sections)
    if not sections:
        empty_columns = ['产品编码', '产品名称', '月份']
        return (
            _to_polars_frame(pd.DataFrame(columns=empty_columns)),
            {'产品编码': 'text', '产品名称': 'text', '月份': 'text'},
            False,
        )

    section_frames: list[pd.DataFrame] = []
    column_types: dict[str, str] = {'产品编码': 'text', '产品名称': 'text'}
    for section in sections:
        section_df = section.data.copy()
        section_df.insert(0, '产品名称', section.product_name)
        section_df.insert(0, '产品编码', section.product_code)
        if has_scoped_section:
            section_df.insert(2, '分析口径', '' if section.section_label is None else section.section_label)
        section_frames.append(section_df)
        for column_name, metric_type in section.column_types.items():
            column_types.setdefault(column_name, metric_type)

    if has_scoped_section:
        column_types.setdefault('分析口径', 'text')

    merged = pd.concat(section_frames, ignore_index=True, sort=False)
    for column_name in merged.columns:
        column_types.setdefault(column_name, 'text')
    return _to_polars_frame(merged), column_types, has_scoped_section


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

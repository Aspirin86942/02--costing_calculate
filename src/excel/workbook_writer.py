"""Workbook 级写出编排。"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pandas as pd

from src.analytics.contracts import FlatSheet, ProductAnomalySection, SectionBlock, SheetModel
from src.excel.fast_writer import FastSheetWriter

DETAIL_TWO_DECIMAL_COLUMNS = {'本期完工单位成本', '本期完工金额'}
QTY_TWO_DECIMAL_COLUMNS = {
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


def _resolve_qty_numeric_columns(qty_sheet_df: pd.DataFrame) -> set[str]:
    """数量页两位小数字段集合，包含动态 standalone 列。"""
    dynamic_columns = {
        column_name
        for column_name in qty_sheet_df.columns
        if (
            (column_name.startswith('本期完工') and column_name.endswith('合计完工金额'))
            or column_name.endswith('单位完工成本')
        )
    }
    return QTY_TWO_DECIMAL_COLUMNS | dynamic_columns


class CostingWorkbookWriter:
    """统一写出成本 workbook。"""

    def __init__(self) -> None:
        self.sheet_writer = FastSheetWriter()

    def write_workbook(
        self,
        output_path: Path,
        *,
        detail_df: pd.DataFrame,
        qty_sheet_df: pd.DataFrame,
        analysis_tables: dict[str, list[SectionBlock]],
        work_order_sheet: FlatSheet,
        product_anomaly_sections: list[ProductAnomalySection],
        error_log: pd.DataFrame,
    ) -> None:
        """按固定 sheet 顺序写出完整 workbook。"""
        with pd.ExcelWriter(
            output_path,
            engine='xlsxwriter',
            engine_kwargs={'options': {'constant_memory': True, 'strings_to_urls': False}},
        ) as writer:
            self.sheet_writer.write_dataframe_fast(
                writer,
                '成本明细',
                detail_df,
                numeric_columns=DETAIL_TWO_DECIMAL_COLUMNS,
                freeze_panes='A2',
                fixed_width=15,
            )
            self.sheet_writer.write_dataframe_fast(
                writer,
                '产品数量统计',
                qty_sheet_df,
                numeric_columns=_resolve_qty_numeric_columns(qty_sheet_df),
                freeze_panes='A2',
                fixed_width=15,
            )
            for sheet_name, sections in analysis_tables.items():
                self.sheet_writer.write_analysis_sheet(writer, sheet_name, sections)
            work_order_worksheet = self.sheet_writer.write_flat_sheet(
                writer,
                '按工单按产品异常值分析',
                work_order_sheet,
                freeze_panes='A2',
                fixed_width=15,
            )
            self.sheet_writer.apply_work_order_highlights(
                writer.book,
                work_order_worksheet,
                columns=work_order_sheet.data.columns.tolist(),
                max_row=len(work_order_sheet.data) + 1,
            )
            self.sheet_writer.write_product_anomaly_sheet(writer, '按产品异常值分析', product_anomaly_sections)
            self.sheet_writer.write_dataframe_fast(
                writer,
                'error_log',
                error_log,
                numeric_columns=set(),
                freeze_panes=None,
                auto_filter=False,
                apply_column_widths=False,
            )

    def write_workbook_from_models(self, output_path: Path, *, sheet_models: Sequence[SheetModel]) -> None:
        """按 SheetModel 契约写出完整 workbook。"""
        with pd.ExcelWriter(
            output_path,
            engine='xlsxwriter',
            engine_kwargs={'options': {'constant_memory': True, 'strings_to_urls': False}},
        ) as writer:
            for model in sheet_models:
                self.sheet_writer.write_sheet_model(writer, model)

"""Workbook 级写出编排。"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.analytics.contracts import FlatSheet, ProductAnomalySection, SectionBlock
from src.excel.sheet_writers import SheetWriter

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


class CostingWorkbookWriter:
    """统一写出成本 workbook。"""

    def __init__(self) -> None:
        self.sheet_writer = SheetWriter()

    def write_workbook(
        self,
        output_path: Path,
        *,
        detail_df: pd.DataFrame,
        qty_sheet_df: pd.DataFrame,
        analysis_tables: dict[str, list[SectionBlock]],
        work_order_sheet: FlatSheet,
        product_anomaly_sections: list[ProductAnomalySection],
        quality_sheet: FlatSheet,
        error_log: pd.DataFrame,
    ) -> None:
        """按固定 sheet 顺序写出完整 workbook。"""
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            self.sheet_writer.write_dataframe_sheet(
                writer,
                '成本明细',
                detail_df,
                numeric_columns=DETAIL_TWO_DECIMAL_COLUMNS,
                freeze_panes='A2',
            )
            self.sheet_writer.write_dataframe_sheet(
                writer,
                '产品数量统计',
                qty_sheet_df,
                numeric_columns=QTY_TWO_DECIMAL_COLUMNS,
                freeze_panes='A2',
            )
            for sheet_name, sections in analysis_tables.items():
                self.sheet_writer.write_analysis_sheet(writer, sheet_name, sections)
            work_order_worksheet = self.sheet_writer.write_flat_sheet(
                writer,
                '按工单按产品异常值分析',
                work_order_sheet,
                freeze_panes='A2',
            )
            self.sheet_writer.apply_work_order_highlights(work_order_worksheet)
            self.sheet_writer.write_product_anomaly_sheet(writer, '按产品异常值分析', product_anomaly_sections)
            self.sheet_writer.write_flat_sheet(writer, '数据质量校验', quality_sheet, freeze_panes='A2')
            error_log.to_excel(writer, sheet_name='error_log', index=False)

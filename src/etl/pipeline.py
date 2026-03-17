"""ETL 阶段编排。"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.analytics.contracts import ResolvedColumns, SplitResult
from src.etl.stages.cleaners import forward_fill_with_rules, remove_total_rows
from src.etl.stages.column_resolution import infer_rename_map, resolve_columns
from src.etl.stages.reader import load_raw_workbook
from src.etl.stages.splitter import split_detail_and_qty_sheets


class CostingEtlPipeline:
    """组合读取、清洗、拆表各阶段。"""

    def __init__(
        self,
        *,
        skip_rows: int,
        fill_columns: list[str],
        detail_columns: list[str],
        qty_columns: list[str],
        period_column: str,
        cost_center_column: str,
        child_material_column: str,
        cost_item_column: str,
        filled_cost_item_column: str,
        order_number_column: str,
        vendor_columns: list[str],
        integrated_workshop_name: str,
        logger: logging.Logger,
    ) -> None:
        self.skip_rows = skip_rows
        self.fill_columns = fill_columns
        self.detail_columns = detail_columns
        self.qty_columns = qty_columns
        self.period_column = period_column
        self.cost_center_column = cost_center_column
        self.child_material_column = child_material_column
        self.cost_item_column = cost_item_column
        self.filled_cost_item_column = filled_cost_item_column
        self.order_number_column = order_number_column
        self.vendor_columns = vendor_columns
        self.integrated_workshop_name = integrated_workshop_name
        self.logger = logger

    def load_raw_dataframe(self, input_path: Path) -> pd.DataFrame:
        """读取原始 workbook。"""
        return load_raw_workbook(input_path, skip_rows=self.skip_rows)

    def infer_rename_map(self, df: pd.DataFrame) -> dict[str, str]:
        """推断列名重命名映射。"""
        return infer_rename_map(
            df,
            child_material_column=self.child_material_column,
            cost_item_column=self.cost_item_column,
            logger=self.logger,
        )

    def resolve_columns(self, df: pd.DataFrame) -> ResolvedColumns:
        """返回关键列契约。"""
        return resolve_columns(
            df,
            child_material_column=self.child_material_column,
            cost_item_column=self.cost_item_column,
            logger=self.logger,
        )

    def remove_total_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """删除汇总行。"""
        return remove_total_rows(
            df,
            period_column=self.period_column,
            cost_center_column=self.cost_center_column,
            logger=self.logger,
        )

    def forward_fill_with_rules(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """按业务规则向下填充。"""
        return forward_fill_with_rules(
            df_raw,
            fill_columns=self.fill_columns,
            vendor_columns=self.vendor_columns,
            cost_center_column=self.cost_center_column,
            integrated_workshop_name=self.integrated_workshop_name,
        )

    def split_sheets(self, df_raw: pd.DataFrame, df_filled: pd.DataFrame) -> SplitResult:
        """拆分成本明细与产品数量统计。"""
        return split_detail_and_qty_sheets(
            df_raw,
            df_filled,
            child_material_column=self.child_material_column,
            cost_item_column=self.cost_item_column,
            order_number_column=self.order_number_column,
            filled_cost_item_column=self.filled_cost_item_column,
            qty_columns=self.qty_columns,
            detail_columns=self.detail_columns,
        )

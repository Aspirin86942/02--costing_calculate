"""ETL 阶段编排。"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import polars as pl

from src.analytics.contracts import NormalizedCostFrame, RawWorkbookFrame, ResolvedColumns, SplitResult
from src.etl.stages.cleaners import forward_fill_with_rules, remove_total_rows
from src.etl.stages.column_resolution import infer_rename_map, resolve_columns
from src.etl.stages.normalizer import build_normalized_cost_frame
from src.etl.stages.reader import load_raw_workbook
from src.etl.stages.splitter import split_detail_and_qty_sheets, split_normalized_frames


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

    def load_raw_dataframe(self, input_path: Path) -> RawWorkbookFrame:
        """读取原始 workbook。"""
        return load_raw_workbook(input_path, skip_rows=self.skip_rows)

    def infer_rename_map(self, df: pd.DataFrame) -> dict[str, str]:
        """推断列名重命名映射。"""
        return infer_rename_map(
            tuple(str(column) for column in df.columns),
            child_material_column=self.child_material_column,
            cost_item_column=self.cost_item_column,
        )

    def resolve_columns(self, df: pd.DataFrame) -> ResolvedColumns:
        """返回关键列契约。"""
        return resolve_columns(
            tuple(str(column) for column in df.columns),
            child_material_column=self.child_material_column,
            cost_item_column=self.cost_item_column,
        )

    def remove_total_rows(self, df: pd.DataFrame | pl.DataFrame) -> pd.DataFrame | pl.DataFrame:
        """删除汇总行。"""
        if isinstance(df, pd.DataFrame):
            cleaned = remove_total_rows(
                pl.from_pandas(df),
                period_column=self.period_column,
                cost_center_column=self.cost_center_column,
            )
            return cleaned.to_pandas()
        return remove_total_rows(
            df,
            period_column=self.period_column,
            cost_center_column=self.cost_center_column,
        )

    def forward_fill_with_rules(self, df_raw: pd.DataFrame | pl.DataFrame) -> pd.DataFrame | pl.DataFrame:
        """按业务规则向下填充。"""
        if isinstance(df_raw, pd.DataFrame):
            filled = forward_fill_with_rules(
                pl.from_pandas(df_raw),
                fill_columns=self.fill_columns,
                vendor_columns=self.vendor_columns,
                cost_center_column=self.cost_center_column,
                integrated_workshop_name=self.integrated_workshop_name,
            )
            return filled.to_pandas()
        return forward_fill_with_rules(
            df_raw,
            fill_columns=self.fill_columns,
            vendor_columns=self.vendor_columns,
            cost_center_column=self.cost_center_column,
            integrated_workshop_name=self.integrated_workshop_name,
        )

    def build_normalized_cost_frame(self, raw: RawWorkbookFrame) -> NormalizedCostFrame:
        """构建 Task 3 的标准化 Polars 成本表。"""
        return build_normalized_cost_frame(
            raw,
            child_material_column=self.child_material_column,
            cost_item_column=self.cost_item_column,
            period_column=self.period_column,
            fill_columns=self.fill_columns,
            vendor_columns=self.vendor_columns,
            cost_center_column=self.cost_center_column,
            integrated_workshop_name=self.integrated_workshop_name,
        )

    def split_normalized_frames(self, normalized: NormalizedCostFrame) -> SplitResult:
        """拆分标准化 Polars 成本表。"""
        return split_normalized_frames(
            normalized,
            child_material_column=self.child_material_column,
            cost_item_column=self.cost_item_column,
            order_number_column=self.order_number_column,
            filled_cost_item_column=self.filled_cost_item_column,
            qty_columns=self.qty_columns,
            detail_columns=self.detail_columns,
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

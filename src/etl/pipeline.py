"""ETL 阶段编排。"""

from __future__ import annotations

import logging
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from time import perf_counter

import pandas as pd
import polars as pl

from src.analytics.contracts import (
    AnalysisArtifacts,
    NormalizedCostFrame,
    RawWorkbookFrame,
    ResolvedColumns,
    SplitResult,
    WorkbookPayload,
)
from src.analytics.presentation_builder import build_sheet_models
from src.analytics.qty_enricher import build_report_artifacts
from src.etl.stages.cleaners import forward_fill_with_rules, remove_total_rows
from src.etl.stages.column_resolution import infer_rename_map, resolve_columns
from src.etl.stages.normalizer import build_normalized_cost_frame
from src.etl.stages.reader import load_raw_workbook
from src.etl.stages.splitter import split_detail_and_qty_sheets, split_normalized_frames


def _normalize_error_log_value(value: object) -> object:
    """error_log 最终按文本写出，这里先消除 mixed dtype 对 Polars 构造的影响。"""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value, 'f')
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    return str(value)


def _sanitize_error_log_frame(error_log: pd.DataFrame | pl.DataFrame) -> pd.DataFrame | pl.DataFrame:
    if isinstance(error_log, pl.DataFrame):
        return error_log
    sanitized = error_log.copy()
    for column_name in sanitized.columns:
        sanitized[column_name] = pd.Series(
            [_normalize_error_log_value(value) for value in sanitized[column_name].tolist()],
            dtype='object',
        )
    return sanitized


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
        """读取旧 ETL 路径需要的 pandas DataFrame。"""
        return pd.read_excel(input_path, header=[0, 1], skiprows=self.skip_rows)

    def load_raw_workbook_frame(self, input_path: Path) -> RawWorkbookFrame:
        """读取 Task 3 Polars 路径需要的 workbook 契约。"""
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
            columns_to_check = [
                column for column in (self.period_column, self.cost_center_column) if column in df.columns
            ]
            if not columns_to_check:
                return df
            keep_mask = pd.Series([True] * len(df), index=df.index)
            for column in columns_to_check:
                keep_mask &= ~df[column].astype(str).str.contains('合计', na=False)
            return df[keep_mask].copy()
        return remove_total_rows(
            df,
            period_column=self.period_column,
            cost_center_column=self.cost_center_column,
        )

    def forward_fill_with_rules(self, df_raw: pd.DataFrame | pl.DataFrame) -> pd.DataFrame | pl.DataFrame:
        """按业务规则向下填充。"""
        if isinstance(df_raw, pd.DataFrame):
            df_filled = df_raw.copy()
            columns_to_fill = [column for column in df_filled.columns if column in self.fill_columns]
            if not columns_to_fill:
                return df_filled

            actual_vendor_columns = [column for column in self.vendor_columns if column in columns_to_fill]
            normal_fill_columns = [column for column in columns_to_fill if column not in actual_vendor_columns]
            if normal_fill_columns:
                df_filled[normal_fill_columns] = df_filled[normal_fill_columns].ffill()

            if not actual_vendor_columns:
                return df_filled
            if self.cost_center_column not in df_filled.columns:
                df_filled[actual_vendor_columns] = df_filled[actual_vendor_columns].ffill()
                return df_filled

            vendor_filled = df_filled[actual_vendor_columns].ffill()
            # 这里保留集成车间的原值，避免把上一个工单的供应商错误继承到当前行。
            integrated_mask = (
                df_filled[self.cost_center_column].astype(str).str.strip().eq(self.integrated_workshop_name)
            )
            df_filled.loc[~integrated_mask, actual_vendor_columns] = vendor_filled.loc[
                ~integrated_mask, actual_vendor_columns
            ]
            return df_filled
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

    def build_workbook_payload(
        self,
        input_path: Path,
        *,
        standalone_cost_items: tuple[str, ...],
        artifacts_transform: Callable[[AnalysisArtifacts], AnalysisArtifacts] | None = None,
    ) -> WorkbookPayload:
        """按全链路 Polars 路径构建 workbook payload。"""
        stage_timings: dict[str, float] = {}

        ingest_start = perf_counter()
        raw_workbook = self.load_raw_workbook_frame(input_path)
        stage_timings['ingest'] = perf_counter() - ingest_start

        normalize_start = perf_counter()
        normalized_frame = self.build_normalized_cost_frame(raw_workbook)
        stage_timings['normalize'] = perf_counter() - normalize_start

        fact_start = perf_counter()
        split_result = self.split_normalized_frames(normalized_frame)
        stage_timings['fact'] = perf_counter() - fact_start

        analysis_start = perf_counter()
        artifacts = build_report_artifacts(
            split_result.detail_df,
            split_result.qty_df,
            standalone_cost_items=standalone_cost_items,
        )
        if artifacts_transform is not None:
            artifacts = artifacts_transform(artifacts)
        stage_timings['analysis'] = perf_counter() - analysis_start

        presentation_start = perf_counter()
        sanitized_error_log = _sanitize_error_log_frame(artifacts.error_log)
        sheet_models = build_sheet_models(
            detail_df=split_result.detail_df,
            qty_sheet_df=artifacts.qty_sheet_df,
            fact_bundle=artifacts.fact_bundle,
            work_order_sheet=artifacts.work_order_sheet,
            product_anomaly_sections=artifacts.product_anomaly_sections,
            error_log=sanitized_error_log,
        )
        stage_timings['presentation'] = perf_counter() - presentation_start

        return WorkbookPayload(
            sheet_models=sheet_models,
            quality_metrics=artifacts.quality_metrics,
            error_log_count=len(artifacts.error_log),
            stage_timings=stage_timings,
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

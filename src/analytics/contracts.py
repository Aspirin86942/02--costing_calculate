"""分析与 workbook 写出共享的数据契约。"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class SectionBlock:
    """单个价量分析分段。"""

    title: str
    data: pd.DataFrame
    metric_type: str
    has_total_row: bool


@dataclass
class ProductAnomalySection:
    """单个产品兼容摘要分段。"""

    product_code: str
    product_name: str
    data: pd.DataFrame
    column_types: dict[str, str]
    amount_columns: list[str]
    outlier_cells: set[tuple[int, str]]


@dataclass
class FlatSheet:
    """普通平铺 sheet 数据。"""

    data: pd.DataFrame
    column_types: dict[str, str]


@dataclass
class AnalysisArtifacts:
    """V3 分析输出产物。"""

    fact_df: pd.DataFrame
    qty_sheet_df: pd.DataFrame
    work_order_sheet: FlatSheet
    product_anomaly_sections: list[ProductAnomalySection]
    quality_sheet: FlatSheet
    error_log: pd.DataFrame


@dataclass
class ResolvedColumns:
    """列识别结果，避免后续阶段重复猜测关键列。"""

    child_material_column: str
    cost_item_column: str
    rename_map: dict[str, str]


@dataclass
class SplitResult:
    """拆表阶段输出。"""

    detail_df: pd.DataFrame
    qty_df: pd.DataFrame

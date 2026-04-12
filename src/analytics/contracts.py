"""分析与 workbook 写出共享的数据契约。"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field

import pandas as pd
import polars as pl


@dataclass(frozen=True)
class RawWorkbookFrame:
    """Polars 首选的原始 workbook 片段。"""

    sheet_name: str
    header_rows: tuple[tuple[str, ...], tuple[str, ...]]
    frame: pl.DataFrame


@dataclass(frozen=True)
class NormalizedCostFrame:
    """标准化成本表，供后续聚合使用。"""

    frame: pl.DataFrame
    key_columns: tuple[str, ...]


@dataclass(frozen=True)
class FactBundle:
    """按 downstream 需要封装的多表事实集。"""

    detail_fact: pl.DataFrame
    qty_fact: pl.DataFrame
    work_order_fact: pl.DataFrame
    product_summary_fact: pl.DataFrame
    error_fact: pl.DataFrame


@dataclass(frozen=True)
class ConditionalFormatRule:
    """控制 workbook 写出时的条件格式规则。"""

    target_range: str
    formula: str
    format_key: str


@dataclass(frozen=True)
class SheetModel:
    """单张 sheet 的列/格式定义。"""

    sheet_name: str
    columns: tuple[str, ...]
    rows_factory: Callable[[], Iterator[tuple[object, ...]]]
    column_types: Mapping[str, str]
    number_formats: Mapping[str, str]
    freeze_panes: str | None = 'A2'
    auto_filter: bool = True
    fixed_width: float | None = 15.0
    conditional_formats: tuple[ConditionalFormatRule, ...] = ()


@dataclass(frozen=True)
class WorkbookPayload:
    """分析完成后要写出的 workbook 元数据。"""

    sheet_models: tuple[SheetModel, ...]
    quality_metrics: tuple[QualityMetric, ...]
    error_log_count: int
    stage_timings: Mapping[str, float]
    error_log_export: pd.DataFrame = field(default_factory=pd.DataFrame)


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
class QualityMetric:
    """数据质量指标对象，供 ETL 和外层消费。"""

    category: str
    metric: str
    value: str
    description: str


@dataclass
class AnalysisArtifacts:
    """V3 分析输出产物，额外保留 fact bundle 供 downstream 重用。"""

    fact_df: pd.DataFrame
    qty_sheet_df: pd.DataFrame
    work_order_sheet: FlatSheet
    product_anomaly_sections: list[ProductAnomalySection]
    quality_metrics: tuple[QualityMetric, ...]
    error_log: pd.DataFrame
    fact_bundle: FactBundle | None = None


@dataclass
class ResolvedColumns:
    """列识别结果，避免后续阶段重复猜测关键列。"""

    child_material_column: str
    cost_item_column: str
    rename_map: dict[str, str]


@dataclass
class SplitResult:
    """拆表阶段输出。"""

    detail_df: pd.DataFrame | pl.DataFrame
    qty_df: pd.DataFrame | pl.DataFrame

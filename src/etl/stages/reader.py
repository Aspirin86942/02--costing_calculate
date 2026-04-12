"""读取原始 workbook。"""

from __future__ import annotations

from pathlib import Path

from src.analytics.contracts import RawWorkbookFrame
from src.etl.stages.workbook_ingestor import WorkbookIngestor


def load_raw_workbook(input_path: Path, *, skip_rows: int) -> RawWorkbookFrame:
    """读取双层表头 workbook，并保留原始两行表头契约。"""
    return WorkbookIngestor().load(input_path, skip_rows=skip_rows)

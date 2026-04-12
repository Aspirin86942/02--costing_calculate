from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import polars as pl
from python_calamine import CalamineWorkbook

from src.analytics.contracts import RawWorkbookFrame

logger = logging.getLogger(__name__)


def _normalize_calamine_cell(value: object) -> object:
    """快路径下仅清洗会干扰 schema 推断的空值形态。"""
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    if pd.isna(value):
        return None
    return value


class WorkbookIngestor:
    def load(self, input_path: Path, *, skip_rows: int) -> RawWorkbookFrame:
        # 先走 calamine 快速路径，若失败则用 openpyxl 兼容避免阻断
        try:
            return self._load_with_calamine(input_path, skip_rows=skip_rows)
        except Exception as exc:  # noqa: BLE001
            logger.warning('Fast ingest failed for %s, falling back to openpyxl: %s', input_path, exc)
            return self._load_with_openpyxl(input_path, skip_rows=skip_rows)

    def _load_with_calamine(self, input_path: Path, *, skip_rows: int) -> RawWorkbookFrame:
        workbook = CalamineWorkbook.from_path(str(input_path))
        sheet = workbook.get_sheet_by_index(0)
        rows = sheet.to_python(skip_empty_area=False)
        # 保持原始双层表头以满足 downstream 对契约表头的依赖
        header_top = tuple('' if value is None else str(value).strip() for value in rows[skip_rows])
        header_bottom = tuple('' if value is None else str(value).strip() for value in rows[skip_rows + 1])
        data_rows = rows[skip_rows + 2 :]
        width = max(len(header_top), len(header_bottom))
        columns = [f'column_{idx}' for idx in range(width)]
        padded_rows = [
            [_normalize_calamine_cell(value) for value in list(row) + [None] * (width - len(row))]
            for row in data_rows
        ]
        # 允许扫描整列后再推断 schema，避免前段全是数值、后段才出现空字符串时误退回 fallback。
        frame = pl.DataFrame(padded_rows, schema=columns, orient='row', infer_schema_length=None)
        return RawWorkbookFrame(sheet_name=sheet.name, header_rows=(header_top, header_bottom), frame=frame)

    def _load_with_openpyxl(self, input_path: Path, *, skip_rows: int) -> RawWorkbookFrame:
        # openpyxl 路径仅用于兼容，仍需保证列名格式一致
        with pd.ExcelFile(input_path, engine='openpyxl') as excel:
            sheet_name = excel.sheet_names[0]
            fallback_df = excel.parse(sheet_name=0, header=None, skiprows=skip_rows)
        header_top = tuple('' if pd.isna(value) else str(value).strip() for value in fallback_df.iloc[0].tolist())
        header_bottom = tuple('' if pd.isna(value) else str(value).strip() for value in fallback_df.iloc[1].tolist())
        data_df = fallback_df.iloc[2:].reset_index(drop=True)
        columns = [f'column_{idx}' for idx in range(len(data_df.columns))]
        data_df.columns = columns
        frame_dict = {
            column: [None if pd.isna(value) else value for value in data_df[column].tolist()]
            for column in columns
        }
        return RawWorkbookFrame(
            sheet_name=sheet_name,
            header_rows=(header_top, header_bottom),
            frame=pl.DataFrame(frame_dict, strict=False),
        )

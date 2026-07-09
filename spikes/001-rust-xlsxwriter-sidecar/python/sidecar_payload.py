from __future__ import annotations

import csv
import json
import re
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl

from src.analytics.contracts import SheetModel

DEFAULT_SHEET_NAMES = (
    '成本计算单总表',
    '成本计算单数量聚合维度',
    '成本分析工单维度',
)
PRODUCT_DIMENSION_SHEET = '成本分析产品维度'


class PayloadExportResult:
    def __init__(self, *, manifest_path: Path, manifest: dict[str, Any], intermediate_export_seconds: float) -> None:
        self.manifest_path = manifest_path
        self.manifest = manifest
        self.intermediate_export_seconds = intermediate_export_seconds


def export_sheet_models_to_payload(
    sheet_models: Iterable[SheetModel],
    output_dir: Path,
    *,
    output_path: Path,
) -> PayloadExportResult:
    started_at = time.perf_counter()
    output_dir.mkdir(parents=True, exist_ok=True)

    sheets: list[dict[str, Any]] = []
    for index, model in enumerate(sheet_models, start=1):
        _validate_sidecar_sheet(model.sheet_name)
        csv_path = output_dir / f'{index:02d}-{_safe_filename(model.sheet_name)}.csv'
        sheet_started_at = time.perf_counter()
        csv_export_method, row_count = _write_sheet_csv(model, csv_path)
        sheet_export_seconds = time.perf_counter() - sheet_started_at
        sheets.append(
            {
                'sheet_name': model.sheet_name,
                'csv_path': str(csv_path),
                'columns': list(model.columns),
                'column_types': _mapping_for_columns(model.column_types, model.columns),
                'number_formats': _mapping_for_columns(model.number_formats, model.columns),
                'source_dtypes': _source_dtypes_for_model(model),
                'write_types': _write_types_for_model(model),
                'freeze_panes': model.freeze_panes,
                'auto_filter': model.auto_filter,
                'fixed_width': model.fixed_width,
                'row_count': row_count,
                'column_count': len(model.columns),
                'csv_export_method': csv_export_method,
                'intermediate_export_seconds': sheet_export_seconds,
            }
        )

    manifest = {
        'version': 1,
        'workbook': {'output_path': str(output_path)},
        'sheets': sheets,
    }
    manifest_path = output_dir / 'manifest.json'
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )
    return PayloadExportResult(
        manifest_path=manifest_path,
        manifest=manifest,
        intermediate_export_seconds=time.perf_counter() - started_at,
    )


def _validate_sidecar_sheet(sheet_name: str) -> None:
    if sheet_name == PRODUCT_DIMENSION_SHEET:
        raise ValueError(f'{PRODUCT_DIMENSION_SHEET} must not be exported by the Rust sidecar spike')
    if sheet_name not in DEFAULT_SHEET_NAMES:
        raise ValueError(f'unsupported sidecar sheet: {sheet_name}')


def _write_sheet_csv(model: SheetModel, csv_path: Path) -> tuple[str, int]:
    if isinstance(model.source_frame, pl.DataFrame):
        model.source_frame.write_csv(csv_path)
        return 'polars', model.source_frame.height

    source_frame = getattr(model, 'source_frame', None)
    if isinstance(source_frame, pd.DataFrame):
        source_frame.to_csv(csv_path, index=False, encoding='utf-8')
        return 'pandas', len(source_frame)

    row_count = 0
    with csv_path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow(model.columns)
        for row in model.rows_factory():
            writer.writerow(row)
            row_count += 1
    return 'rows_factory', row_count


def _mapping_for_columns(mapping: Any, columns: tuple[str, ...]) -> dict[str, Any]:
    if mapping is None:
        return {}
    return {column: mapping[column] for column in columns if column in mapping}


def _source_dtypes_for_model(model: SheetModel) -> dict[str, str]:
    if not isinstance(model.source_frame, pl.DataFrame):
        return {}
    return {column: str(dtype) for column, dtype in model.source_frame.schema.items()}


def _write_types_for_model(model: SheetModel) -> dict[str, str]:
    source_dtypes = _source_dtypes_for_model(model)
    return {
        column: (
            'number'
            if (
                column in model.number_formats
                or model.column_types.get(column) in {'amount', 'price', 'qty', 'score', 'pct'}
                or _is_numeric_source_dtype(source_dtypes.get(column, ''))
            )
            else 'text'
        )
        for column in model.columns
    }


def _is_numeric_source_dtype(source_dtype: str) -> bool:
    return source_dtype.startswith(('Int', 'UInt', 'Float', 'Decimal'))


def _safe_filename(sheet_name: str) -> str:
    safe = re.sub(r'[<>:"/\\|?*\s]+', '_', sheet_name).strip('_')
    return safe or 'sheet'

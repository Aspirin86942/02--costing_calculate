# Full-Chain Polars Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current pandas-first ETL path with a Polars-first ingest/normalize/fact/presentation pipeline that still produces the same 8-sheet workbook contract, error log semantics, and anomaly highlighting behavior.

**Architecture:** Introduce explicit columnar contracts (`RawWorkbookFrame`, `NormalizedCostFrame`, `FactBundle`, `SheetModel`) and move workbook rendering behind a `SheetModel` export boundary. Use a calamine-backed ingest path with a single compatibility fallback, keep money fields off `float`, and switch `CostingWorkbookETL` to the new pipeline only after the writer and workbook contract tests pass on the new `SheetModel` path.

**Tech Stack:** Python 3.11, polars, python-calamine, pandas (compatibility edge only), xlsxwriter, openpyxl, pytest, ruff

---

## File Map

- Modify: `pyproject.toml`
  - Add the explicit runtime dependencies for `polars`, `python-calamine`, and `xlsxwriter`.
- Modify: `src/analytics/contracts.py`
  - Add the new columnar contracts (`RawWorkbookFrame`, `NormalizedCostFrame`, `FactBundle`, `ConditionalFormatRule`, `SheetModel`, `WorkbookPayload`) and extend `AnalysisArtifacts` with an optional `fact_bundle`.
- Create: `src/etl/stages/workbook_ingestor.py`
  - Read the first worksheet through a calamine-backed path, preserve the two header rows, and fall back once to the current OpenPyXL/Pandas path if the fast reader fails.
- Modify: `src/etl/stages/reader.py`
  - Collapse this file into a thin compatibility wrapper that delegates to `WorkbookIngestor`.
- Create: `src/etl/stages/normalizer.py`
  - Flatten the two-row header, infer column renames, remove total rows, apply the integrated-workshop vendor fill rule, add the display month, and return `NormalizedCostFrame`.
- Modify: `src/etl/stages/cleaners.py`
  - Port total-row filtering and vendor-sensitive fill logic to Polars-backed helpers.
- Modify: `src/etl/stages/column_resolution.py`
  - Make rename inference operate on a sequence of normalized column names instead of a pandas-only surface.
- Modify: `src/etl/stages/splitter.py`
  - Split the normalized Polars frame into detail and quantity frames while preserving the current column contract.
- Modify: `src/etl/pipeline.py`
  - Wire the new ingest/normalize/split/analyze/present flow and expose a `build_workbook_payload()` entrypoint.
- Modify: `src/analytics/fact_builder.py`
  - Build `FactBundle` with Polars expressions and keep money values on a precise storage type.
- Modify: `src/analytics/qty_enricher.py`
  - Replace pandas groupby/apply hotspots with Polars-based report construction while preserving `AnalysisArtifacts` and contract behavior.
- Modify: `src/analytics/anomaly.py`
  - Port the work-order anomaly sheet computation to Polars-backed grouping and scoring while keeping the output column names stable.
- Modify: `src/analytics/table_rendering.py`
  - Build analysis sections from Polars-backed fact data without reintroducing pandas-heavy pre-export shaping.
- Create: `src/analytics/presentation_builder.py`
  - Translate detail/quantity/error-log frames and analysis outputs into ordered `SheetModel` instances.
- Modify: `src/excel/fast_writer.py`
  - Add `write_sheet_model()` and make conditional formatting consume `ConditionalFormatRule`.
- Modify: `src/excel/workbook_writer.py`
  - Replace the multi-DataFrame workbook entrypoint with a `write_workbook_from_models()` path.
- Modify: `src/etl/costing_etl.py`
  - Switch `process_file()` to `build_workbook_payload()`, keep timing logs, and store quality/error-log outputs from the new payload.
- Modify: `src/etl/runner.py`
  - Keep the CLI contract stable while consuming `CostingWorkbookETL`'s new payload-backed outputs.
- Modify: `tests/test_etl_pipeline.py`
  - Add stage-level contract tests for the new ingest and normalize contracts.
- Modify: `tests/test_pq_analysis_v3.py`
  - Add Polars-backed business regression coverage for quantity enrichment and anomaly derivation.
- Modify: `tests/test_costing_etl.py`
  - Add writer, payload, and stage-timing regressions for the new pipeline.
- Modify: `tests/test_runner.py`
  - Keep the CLI and quality-log contract stable after the pipeline switch.
- Modify: `tests/contracts/_workbook_contract_helper.py`
  - Build the contract workbook through the new payload-driven workbook path.
- Modify: `tests/contracts/test_workbook_contract.py`
  - Keep workbook semantics and highlight-rule expectations stable after the migration.
- Modify: `tests/contracts/test_error_log_contract.py`
  - Keep `error_log` columns and issue types within the published contract sets.
- Modify: `tests/contracts/generate_baselines.py`
  - Regenerate workbook baselines through the new writer path if semantics intentionally change.
- Modify: `tests/contracts/baselines/workbook_semantics.json`
  - Refresh only if the workbook semantic snapshot changes intentionally.

### Task 1: Add Explicit Columnar Contracts And Runtime Dependencies

**Files:**
- Modify: `pyproject.toml`
- Modify: `src/analytics/contracts.py`
- Test: `tests/test_etl_pipeline.py`

- [ ] **Step 1: Write the failing contract test for the new Polars-first workbook objects**

```python
from __future__ import annotations

import polars as pl

from src.analytics.contracts import (
    ConditionalFormatRule,
    NormalizedCostFrame,
    RawWorkbookFrame,
    SheetModel,
    WorkbookPayload,
)


def test_polars_pipeline_contract_objects_are_constructible() -> None:
    raw = RawWorkbookFrame(
        sheet_name='成本计算单',
        header_rows=(('年期', '产品编码'), ('', '')),
        frame=pl.DataFrame({'column_0': ['2025年1期'], 'column_1': ['P001']}),
    )
    normalized = NormalizedCostFrame(
        frame=pl.DataFrame({'年期': ['2025-01'], '产品编码': ['P001']}),
        key_columns=('年期', '产品编码'),
    )
    model = SheetModel(
        sheet_name='成本明细',
        columns=('年期', '产品编码'),
        rows_factory=lambda: iter([('2025-01', 'P001')]),
        column_types={'年期': 'text', '产品编码': 'text'},
        number_formats={},
        freeze_panes='A2',
        auto_filter=True,
        fixed_width=15.0,
        conditional_formats=(
            ConditionalFormatRule(
                target_range='A2:A1048576',
                formula='=$B2="高度可疑"',
                format_key='suspicious',
            ),
        ),
    )
    payload = WorkbookPayload(sheet_models=(model,), quality_metrics=(), error_log_count=0, stage_timings={})

    assert raw.sheet_name == '成本计算单'
    assert normalized.key_columns == ('年期', '产品编码')
    assert list(model.rows_factory()) == [('2025-01', 'P001')]
    assert payload.sheet_models[0].sheet_name == '成本明细'
```

- [ ] **Step 2: Run the targeted contract test to verify it fails**

Run: `conda run -n test python -m pytest tests/test_etl_pipeline.py::test_polars_pipeline_contract_objects_are_constructible -q`

Expected: FAIL with import or attribute errors because the new contract dataclasses do not exist yet.

- [ ] **Step 3: Add the runtime dependencies and contract dataclasses**

```toml
# pyproject.toml
[project]
dependencies = [
    "pandas>=2.0.0",
    "openpyxl>=3.1.0",
    "numpy>=1.24.0",
    "beautifulsoup4>=4.12.0",
    "polars>=1.28.0",
    "python-calamine>=0.3.0",
    "xlsxwriter>=3.2.0",
]
```

```python
# src/analytics/contracts.py
from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass

import pandas as pd
import polars as pl


@dataclass(frozen=True)
class RawWorkbookFrame:
    sheet_name: str
    header_rows: tuple[tuple[str, ...], tuple[str, ...]]
    frame: pl.DataFrame


@dataclass(frozen=True)
class NormalizedCostFrame:
    frame: pl.DataFrame
    key_columns: tuple[str, ...]


@dataclass(frozen=True)
class FactBundle:
    detail_fact: pl.DataFrame
    qty_fact: pl.DataFrame
    work_order_fact: pl.DataFrame
    product_summary_fact: pl.DataFrame
    error_fact: pl.DataFrame


@dataclass(frozen=True)
class ConditionalFormatRule:
    target_range: str
    formula: str
    format_key: str


@dataclass(frozen=True)
class SheetModel:
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
    sheet_models: tuple[SheetModel, ...]
    quality_metrics: tuple[QualityMetric, ...]
    error_log_count: int
    stage_timings: Mapping[str, float]


@dataclass
class AnalysisArtifacts:
    fact_df: pd.DataFrame
    qty_sheet_df: pd.DataFrame
    work_order_sheet: FlatSheet
    product_anomaly_sections: list[ProductAnomalySection]
    quality_metrics: tuple[QualityMetric, ...]
    error_log: pd.DataFrame
    fact_bundle: FactBundle | None = None
```

- [ ] **Step 4: Run the targeted contract test to verify it passes**

Run: `conda run -n test python -m pytest tests/test_etl_pipeline.py::test_polars_pipeline_contract_objects_are_constructible -q`

Expected: PASS

- [ ] **Step 5: Commit the contract and dependency scaffold**

```bash
git add pyproject.toml src/analytics/contracts.py tests/test_etl_pipeline.py
git commit -m "feat(core): add polars pipeline contracts"
```

### Task 2: Introduce `WorkbookIngestor` With Fast Reader And Single Fallback

**Files:**
- Create: `src/etl/stages/workbook_ingestor.py`
- Modify: `src/etl/stages/reader.py`
- Test: `tests/test_etl_pipeline.py`

- [ ] **Step 1: Write the failing ingest tests for the fast path and fallback path**

```python
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import polars as pl

from src.analytics.contracts import RawWorkbookFrame
from src.etl.stages.reader import load_raw_workbook
from src.etl.stages.workbook_ingestor import WorkbookIngestor


def test_workbook_ingestor_falls_back_once_when_fast_reader_fails(tmp_path: Path) -> None:
    ingestor = WorkbookIngestor()
    fallback = RawWorkbookFrame(
        sheet_name='Sheet1',
        header_rows=(('年期', '产品编码'), ('', '')),
        frame=pl.DataFrame({'column_0': ['2025年1期'], 'column_1': ['P001']}),
    )

    with (
        patch.object(ingestor, '_load_with_calamine', side_effect=RuntimeError('boom')),
        patch.object(ingestor, '_load_with_openpyxl', return_value=fallback) as fallback_mock,
    ):
        result = ingestor.load(tmp_path / 'input.xlsx', skip_rows=2)

    assert result.sheet_name == 'Sheet1'
    fallback_mock.assert_called_once()


def test_load_raw_workbook_delegates_to_workbook_ingestor(tmp_path: Path) -> None:
    raw = RawWorkbookFrame(
        sheet_name='Sheet1',
        header_rows=(('年期', '产品编码'), ('', '')),
        frame=pl.DataFrame({'column_0': ['2025年1期'], 'column_1': ['P001']}),
    )

    with patch.object(WorkbookIngestor, 'load', return_value=raw) as load_mock:
        result = load_raw_workbook(tmp_path / 'input.xlsx', skip_rows=2)

    assert result is raw
    load_mock.assert_called_once_with(tmp_path / 'input.xlsx', skip_rows=2)
```

- [ ] **Step 2: Run the ingest tests to verify they fail**

Run: `conda run -n test python -m pytest tests/test_etl_pipeline.py::test_workbook_ingestor_falls_back_once_when_fast_reader_fails tests/test_etl_pipeline.py::test_load_raw_workbook_delegates_to_workbook_ingestor -q`

Expected: FAIL because `WorkbookIngestor` does not exist and `load_raw_workbook()` still returns a pandas `DataFrame`.

- [ ] **Step 3: Implement the ingestor and convert `reader.py` into a compatibility wrapper**

```python
# src/etl/stages/workbook_ingestor.py
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import polars as pl
from python_calamine import CalamineWorkbook

from src.analytics.contracts import RawWorkbookFrame

logger = logging.getLogger(__name__)


class WorkbookIngestor:
    def load(self, input_path: Path, *, skip_rows: int) -> RawWorkbookFrame:
        try:
            return self._load_with_calamine(input_path, skip_rows=skip_rows)
        except Exception as exc:  # noqa: BLE001
            logger.warning('Fast ingest failed for %s, falling back to openpyxl: %s', input_path, exc)
            return self._load_with_openpyxl(input_path, skip_rows=skip_rows)

    def _load_with_calamine(self, input_path: Path, *, skip_rows: int) -> RawWorkbookFrame:
        workbook = CalamineWorkbook.from_path(str(input_path))
        sheet = workbook.get_sheet_by_index(0)
        rows = sheet.to_python(skip_empty_area=False)
        header_top = tuple('' if value is None else str(value).strip() for value in rows[skip_rows])
        header_bottom = tuple('' if value is None else str(value).strip() for value in rows[skip_rows + 1])
        data_rows = rows[skip_rows + 2 :]
        width = max(len(header_top), len(header_bottom))
        columns = [f'column_{idx}' for idx in range(width)]
        padded_rows = [list(row) + [None] * (width - len(row)) for row in data_rows]
        frame = pl.DataFrame(padded_rows, schema=columns, orient='row')
        return RawWorkbookFrame(sheet_name=sheet.name, header_rows=(header_top, header_bottom), frame=frame)

    def _load_with_openpyxl(self, input_path: Path, *, skip_rows: int) -> RawWorkbookFrame:
        fallback_df = pd.read_excel(input_path, header=None, skiprows=skip_rows, engine='openpyxl')
        header_top = tuple('' if pd.isna(value) else str(value).strip() for value in fallback_df.iloc[0].tolist())
        header_bottom = tuple('' if pd.isna(value) else str(value).strip() for value in fallback_df.iloc[1].tolist())
        data_df = fallback_df.iloc[2:].reset_index(drop=True)
        data_df.columns = [f'column_{idx}' for idx in range(len(data_df.columns))]
        return RawWorkbookFrame(
            sheet_name='Sheet1',
            header_rows=(header_top, header_bottom),
            frame=pl.from_pandas(data_df, include_index=False),
        )
```

```python
# src/etl/stages/reader.py
from __future__ import annotations

from pathlib import Path

from src.analytics.contracts import RawWorkbookFrame
from src.etl.stages.workbook_ingestor import WorkbookIngestor


def load_raw_workbook(input_path: Path, *, skip_rows: int) -> RawWorkbookFrame:
    """读取双层表头 workbook，并保留原始两行表头契约。"""
    return WorkbookIngestor().load(input_path, skip_rows=skip_rows)
```

- [ ] **Step 4: Run the ingest tests to verify they pass**

Run: `conda run -n test python -m pytest tests/test_etl_pipeline.py::test_workbook_ingestor_falls_back_once_when_fast_reader_fails tests/test_etl_pipeline.py::test_load_raw_workbook_delegates_to_workbook_ingestor -q`

Expected: PASS

- [ ] **Step 5: Commit the ingest stage**

```bash
git add src/etl/stages/workbook_ingestor.py src/etl/stages/reader.py tests/test_etl_pipeline.py
git commit -m "feat(etl): add workbook ingestor with fast fallback"
```

### Task 3: Normalize The Workbook And Split It Into Detail / Quantity Frames In Polars

**Files:**
- Create: `src/etl/stages/normalizer.py`
- Modify: `src/etl/stages/cleaners.py`
- Modify: `src/etl/stages/column_resolution.py`
- Modify: `src/etl/stages/splitter.py`
- Modify: `src/etl/pipeline.py`
- Test: `tests/test_etl_pipeline.py`

- [ ] **Step 1: Write the failing normalize and split regression tests**

```python
from __future__ import annotations

import polars as pl

from src.analytics.contracts import RawWorkbookFrame
from src.etl.stages.normalizer import build_normalized_cost_frame
from src.etl.stages.splitter import split_normalized_frames


def test_build_normalized_cost_frame_removes_totals_and_skips_integrated_vendor_fill() -> None:
    raw = RawWorkbookFrame(
        sheet_name='成本计算单',
        header_rows=(
            ('年期', '成本中心名称', '产品编码', '供应商编码', '成本项目名称', '工单编号', '子项物料编码', '本期完工金额'),
            ('', '', '', '', '', '', '', ''),
        ),
        frame=pl.DataFrame(
            {
                'column_0': ['2025年1期', '2025年1期', '合计'],
                'column_1': ['集成车间', '中心A', '中心A'],
                'column_2': ['P001', 'P001', 'P001'],
                'column_3': ['V001', None, 'V999'],
                'column_4': [None, '直接材料', '直接材料'],
                'column_5': ['WO-001', 'WO-001', 'WO-TOTAL'],
                'column_6': [None, 'MAT-001', None],
                'column_7': [None, 100, 100],
            }
        ),
    )

    normalized = build_normalized_cost_frame(
        raw,
        child_material_column='子项物料编码',
        cost_item_column='成本项目名称',
        period_column='年期',
        fill_columns=['年期', '成本中心名称', '产品编码', '供应商编码'],
        vendor_columns=['供应商编码'],
        cost_center_column='成本中心名称',
        integrated_workshop_name='集成车间',
    )

    rows = normalized.frame.select(['月份', '成本中心名称', '供应商编码']).to_dicts()
    assert rows == [
        {'月份': '2025年01期', '成本中心名称': '集成车间', '供应商编码': 'V001'},
        {'月份': '2025年01期', '成本中心名称': '中心A', '供应商编码': None},
    ]


def test_split_normalized_frames_keeps_qty_and_detail_contracts() -> None:
    normalized = build_normalized_cost_frame(
        RawWorkbookFrame(
            sheet_name='成本计算单',
            header_rows=(
                ('年期', '产品编码', '产品名称', '工单编号', '工单行号', '子项物料编码', '成本项目名称', '本期完工数量', '本期完工金额'),
                ('', '', '', '', '', '', '', '', ''),
            ),
            frame=pl.DataFrame(
                {
                    'column_0': ['2025年1期', '2025年1期'],
                    'column_1': ['P001', 'P001'],
                    'column_2': ['产品A', '产品A'],
                    'column_3': ['WO-001', 'WO-001'],
                    'column_4': [1, 1],
                    'column_5': [None, 'MAT-001'],
                    'column_6': [None, '直接材料'],
                    'column_7': [10, 10],
                    'column_8': [100, 100],
                }
            ),
        ),
        child_material_column='子项物料编码',
        cost_item_column='成本项目名称',
        period_column='年期',
        fill_columns=['年期', '产品编码', '产品名称', '工单编号', '工单行号'],
        vendor_columns=[],
        cost_center_column='成本中心名称',
        integrated_workshop_name='集成车间',
    )

    split = split_normalized_frames(
        normalized,
        child_material_column='子项物料编码',
        cost_item_column='成本项目名称',
        order_number_column='工单编号',
        filled_cost_item_column='Filled_成本项目',
        qty_columns=['年期', '月份', '产品编码', '工单编号', '本期完工数量', '本期完工金额'],
        detail_columns=['年期', '月份', '产品编码', '工单编号', '成本项目名称', '本期完工金额'],
    )

    assert split.qty_df.columns == ['年期', '月份', '产品编码', '工单编号', '本期完工数量', '本期完工金额']
    assert split.detail_df.columns == ['年期', '月份', '产品编码', '工单编号', '成本项目名称', '本期完工金额']
    assert split.qty_df.height == 1
    assert split.detail_df.height == 1
```

- [ ] **Step 2: Run the normalize/split tests to verify they fail**

Run: `conda run -n test python -m pytest tests/test_etl_pipeline.py::test_build_normalized_cost_frame_removes_totals_and_skips_integrated_vendor_fill tests/test_etl_pipeline.py::test_split_normalized_frames_keeps_qty_and_detail_contracts -q`

Expected: FAIL because the normalize stage and the Polars split entrypoint do not exist yet.

- [ ] **Step 3: Implement the normalizer, Polars cleaners, and Polars split path**

```python
# src/etl/stages/normalizer.py
from __future__ import annotations

import re

import polars as pl

from src.analytics.contracts import NormalizedCostFrame, RawWorkbookFrame
from src.etl.stages.cleaners import forward_fill_with_rules, remove_total_rows
from src.etl.stages.column_resolution import infer_rename_map


def _flatten_headers(header_rows: tuple[tuple[str, ...], tuple[str, ...]]) -> list[str]:
    top_row, bottom_row = header_rows
    flattened: list[str] = []
    for top, bottom in zip(top_row, bottom_row, strict=False):
        primary = (top or '').strip()
        secondary = (bottom or '').strip()
        flattened.append(primary if not secondary else f'{primary}_{secondary}'.strip('_'))
    return flattened


def _format_period_value(value: object) -> str | None:
    if value is None:
        return None
    match = re.search(r'(\d+)年\s*(\d+)\s*期', str(value))
    if match is None:
        return None
    return f'{match.group(1)}年{int(match.group(2)):02d}期'


def build_normalized_cost_frame(
    raw: RawWorkbookFrame,
    *,
    child_material_column: str,
    cost_item_column: str,
    period_column: str,
    fill_columns: list[str],
    vendor_columns: list[str],
    cost_center_column: str,
    integrated_workshop_name: str,
) -> NormalizedCostFrame:
    flattened_columns = _flatten_headers(raw.header_rows)
    rename_to_flat = dict(zip(raw.frame.columns, flattened_columns, strict=False))
    frame = raw.frame.rename(rename_to_flat)

    inferred_rename_map = infer_rename_map(
        flattened_columns,
        child_material_column=child_material_column,
        cost_item_column=cost_item_column,
    )
    if inferred_rename_map:
        frame = frame.rename(inferred_rename_map)

    frame = remove_total_rows(frame, period_column=period_column, cost_center_column=cost_center_column)
    frame = forward_fill_with_rules(
        frame,
        fill_columns=fill_columns,
        vendor_columns=vendor_columns,
        cost_center_column=cost_center_column,
        integrated_workshop_name=integrated_workshop_name,
    )
    frame = frame.with_columns(
        pl.col(period_column).map_elements(_format_period_value, return_dtype=pl.String).alias('月份'),
        pl.col(cost_item_column).forward_fill().alias('Filled_成本项目'),
    )
    return NormalizedCostFrame(frame=frame, key_columns=('月份', '产品编码', '工单编号', '工单行号'))
```

```python
# src/etl/stages/cleaners.py
from __future__ import annotations

import polars as pl


def remove_total_rows(frame: pl.DataFrame, *, period_column: str, cost_center_column: str) -> pl.DataFrame:
    candidate_columns = [column for column in (period_column, cost_center_column) if column in frame.columns]
    if not candidate_columns:
        return frame
    keep_expr = pl.lit(True)
    for column in candidate_columns:
        keep_expr = keep_expr & ~pl.col(column).cast(pl.String).fill_null('').str.contains('合计')
    return frame.filter(keep_expr)


def forward_fill_with_rules(
    frame: pl.DataFrame,
    *,
    fill_columns: list[str],
    vendor_columns: list[str],
    cost_center_column: str,
    integrated_workshop_name: str,
) -> pl.DataFrame:
    columns_to_fill = [column for column in fill_columns if column in frame.columns]
    vendor_targets = [column for column in vendor_columns if column in columns_to_fill]
    normal_targets = [column for column in columns_to_fill if column not in vendor_targets]
    exprs: list[pl.Expr] = [pl.col(column).forward_fill().alias(column) for column in normal_targets]

    for vendor_column in vendor_targets:
        exprs.append(
            pl.when(pl.col(cost_center_column).cast(pl.String).fill_null('').str.strip_chars() == integrated_workshop_name)
            .then(pl.col(vendor_column))
            .otherwise(pl.col(vendor_column).forward_fill())
            .alias(vendor_column)
        )

    return frame.with_columns(exprs) if exprs else frame
```

```python
# src/etl/stages/column_resolution.py
from __future__ import annotations


def infer_rename_map(
    columns: list[str] | tuple[str, ...],
    *,
    child_material_column: str,
    cost_item_column: str,
) -> dict[str, str]:
    rename_map: dict[str, str] = {}
    existing = set(columns)

    if child_material_column not in existing:
        candidates = [column for column in columns if '物料编码' in column or '子件' in column]
        if candidates:
            rename_map[candidates[0]] = child_material_column

    if cost_item_column not in existing:
        candidates = [column for column in columns if '成本项目' in column or '费用项目' in column]
        if candidates:
            rename_map[candidates[0]] = cost_item_column

    return rename_map
```

```python
# src/etl/stages/splitter.py
from __future__ import annotations

import polars as pl

from src.analytics.contracts import NormalizedCostFrame, SplitResult


def split_normalized_frames(
    normalized: NormalizedCostFrame,
    *,
    child_material_column: str,
    cost_item_column: str,
    order_number_column: str,
    filled_cost_item_column: str,
    qty_columns: list[str],
    detail_columns: list[str],
) -> SplitResult:
    frame = normalized.frame
    material_tokens = pl.col(child_material_column).cast(pl.String).fill_null('').str.strip_chars()
    cost_item_tokens = pl.col(cost_item_column).cast(pl.String).fill_null('').str.strip_chars()
    no_material = material_tokens == ''
    has_material = material_tokens != ''
    no_cost_item = cost_item_tokens == ''
    expense_mask = no_material & (cost_item_tokens != '') & (cost_item_tokens != '直接材料')
    has_order = pl.col(order_number_column).is_not_null()

    qty_df = (
        frame.filter(no_material & no_cost_item & has_order)
        .select([column for column in qty_columns if column in frame.columns])
    )
    detail_df = (
        frame.filter(has_material | expense_mask)
        .with_columns(pl.col(filled_cost_item_column).alias(cost_item_column))
        .select([column for column in detail_columns if column in frame.columns or column == cost_item_column])
    )
    return SplitResult(detail_df=detail_df, qty_df=qty_df)
```

```python
# src/etl/pipeline.py
from src.etl.stages.normalizer import build_normalized_cost_frame
from src.etl.stages.splitter import split_normalized_frames
```

- [ ] **Step 4: Run the normalize/split tests to verify they pass**

Run: `conda run -n test python -m pytest tests/test_etl_pipeline.py::test_build_normalized_cost_frame_removes_totals_and_skips_integrated_vendor_fill tests/test_etl_pipeline.py::test_split_normalized_frames_keeps_qty_and_detail_contracts -q`

Expected: PASS

- [ ] **Step 5: Commit the normalize and split stage**

```bash
git add src/etl/stages/normalizer.py src/etl/stages/cleaners.py src/etl/stages/column_resolution.py src/etl/stages/splitter.py src/etl/pipeline.py tests/test_etl_pipeline.py
git commit -m "feat(etl): normalize and split costing workbook with polars"
```

### Task 4: Port Fact Building, Quantity Enrichment, And Error Logging To Polars

**Files:**
- Modify: `src/analytics/fact_builder.py`
- Modify: `src/analytics/qty_enricher.py`
- Modify: `src/analytics/errors.py`
- Modify: `src/analytics/quality.py`
- Modify: `tests/test_pq_analysis_v3.py`
- Modify: `tests/contracts/test_error_log_contract.py`

- [ ] **Step 1: Write the failing business regression that proves the Polars fact bundle is exposed without breaking contracts**

```python
from __future__ import annotations

from decimal import Decimal

from src.analytics.qty_enricher import build_report_artifacts
from tests.test_pq_analysis_v3 import _build_base_detail_df, _build_base_qty_df


def test_build_report_artifacts_exposes_fact_bundle_and_preserves_contracts() -> None:
    artifacts = build_report_artifacts(_build_base_detail_df(), _build_base_qty_df(total_amount=999))

    assert artifacts.fact_bundle is not None
    assert artifacts.fact_bundle.qty_fact.height == 1
    qty_row = artifacts.qty_sheet_df.iloc[0]
    assert qty_row['本期完工直接材料合计完工金额'] == Decimal('100')
    assert 'TOTAL_COST_MISMATCH' in set(artifacts.error_log['issue_type'])
```

- [ ] **Step 2: Run the business regression subset to verify it fails**

Run: `conda run -n test python -m pytest tests/test_pq_analysis_v3.py::test_build_report_artifacts_exposes_fact_bundle_and_preserves_contracts tests/contracts/test_error_log_contract.py -q`

Expected: FAIL because `AnalysisArtifacts` has no `fact_bundle` and the report path is still pandas-first.

- [ ] **Step 3: Build `FactBundle` with Polars expressions and keep `build_report_artifacts()` as the compatibility façade**

```python
# src/analytics/fact_builder.py
from __future__ import annotations

from decimal import Decimal

import polars as pl

from src.analytics.contracts import FactBundle

MONEY_DTYPE = pl.Decimal(20, 4)


def normalize_money_expr(column_name: str) -> pl.Expr:
    return (
        pl.col(column_name)
        .cast(pl.String)
        .str.strip_chars()
        .replace('', None)
        .cast(MONEY_DTYPE)
        .alias(column_name)
    )


def build_fact_bundle(detail_df: pl.DataFrame, qty_df: pl.DataFrame, *, standalone_cost_items: tuple[str, ...]) -> FactBundle:
    detail = detail_df.rename(
        {
            '产品编码': 'product_code',
            '产品名称': 'product_name',
            '工单编号': 'order_no',
            '工单行号': 'order_line',
            '成本项目名称': 'cost_item',
            '本期完工金额': 'completed_amount',
            '月份': 'period_display',
        }
    ).with_columns(
        normalize_money_expr('completed_amount'),
        pl.col('cost_item').cast(pl.String).str.strip_chars().alias('normalized_cost_item'),
    )

    work_order_fact = (
        detail.group_by(['period_display', 'product_code', 'product_name', 'order_no', 'order_line'], maintain_order=True)
        .agg(
            pl.col('completed_amount').sum().alias('completed_amount_total'),
            pl.when(pl.col('normalized_cost_item') == '直接材料').then(pl.col('completed_amount')).otherwise(Decimal('0')).sum().alias('dm_amount'),
            pl.when(pl.col('normalized_cost_item') == '直接人工').then(pl.col('completed_amount')).otherwise(Decimal('0')).sum().alias('dl_amount'),
            pl.when(pl.col('normalized_cost_item').str.starts_with('制造费用')).then(pl.col('completed_amount')).otherwise(Decimal('0')).sum().alias('moh_amount'),
            *[
                pl.when(pl.col('normalized_cost_item') == item).then(pl.col('completed_amount')).otherwise(Decimal('0')).sum().alias(f'{item}_amount')
                for item in standalone_cost_items
            ],
        )
    )

    product_summary_fact = (
        work_order_fact.group_by(['product_code', 'product_name', 'period_display'], maintain_order=True)
        .agg(
            pl.col('completed_amount_total').sum().alias('total_cost'),
            pl.col('dm_amount').sum().alias('dm_cost'),
            pl.col('dl_amount').sum().alias('dl_cost'),
            pl.col('moh_amount').sum().alias('moh_cost'),
        )
    )

    error_fact = pl.DataFrame(schema={'issue_type': pl.String, 'field_name': pl.String})
    return FactBundle(
        detail_fact=detail,
        qty_fact=qty_df,
        work_order_fact=work_order_fact,
        product_summary_fact=product_summary_fact,
        error_fact=error_fact,
    )
```

```python
# src/analytics/qty_enricher.py
from __future__ import annotations

import pandas as pd
import polars as pl

from src.analytics.contracts import AnalysisArtifacts
from src.analytics.fact_builder import build_fact_bundle


def build_report_artifacts(
    df_detail: pd.DataFrame | pl.DataFrame,
    df_qty: pd.DataFrame | pl.DataFrame,
    standalone_cost_items: tuple[str, ...] | list[str] | None = DEFAULT_STANDALONE_COST_ITEMS,
) -> AnalysisArtifacts:
    detail_pl = df_detail if isinstance(df_detail, pl.DataFrame) else pl.from_pandas(df_detail, include_index=False)
    qty_pl = df_qty if isinstance(df_qty, pl.DataFrame) else pl.from_pandas(df_qty, include_index=False)
    fact_bundle = build_fact_bundle(detail_pl, qty_pl, standalone_cost_items=tuple(standalone_cost_items or DEFAULT_STANDALONE_COST_ITEMS))

    # Compatibility edge: keep the published AnalysisArtifacts surface stable while the pipeline migrates.
    qty_sheet_df = _build_qty_sheet_output(fact_bundle).to_pandas(use_pyarrow_extension_array=True)
    work_order_df = _build_work_order_output(fact_bundle).to_pandas(use_pyarrow_extension_array=True)
    error_log_df = _build_error_log_output(fact_bundle).to_pandas(use_pyarrow_extension_array=True)
    quality_metrics = build_quality_metrics(
        detail_pl.to_pandas(use_pyarrow_extension_array=True),
        qty_pl.to_pandas(use_pyarrow_extension_array=True),
        qty_sheet_df,
        work_order_df,
        filtered_invalid_qty_count=0,
        filtered_missing_total_amount_count=0,
    )

    return AnalysisArtifacts(
        fact_df=_build_fact_table_for_compat(fact_bundle).to_pandas(use_pyarrow_extension_array=True),
        qty_sheet_df=qty_sheet_df,
        work_order_sheet=FlatSheet(data=work_order_df, column_types=WORK_ORDER_COLUMN_TYPES),
        product_anomaly_sections=build_product_anomaly_sections(_build_product_summary_df_for_compat(fact_bundle).to_pandas(use_pyarrow_extension_array=True)),
        quality_metrics=quality_metrics,
        error_log=error_log_df,
        fact_bundle=fact_bundle,
    )
```

```python
# src/analytics/errors.py
from __future__ import annotations

import polars as pl


def empty_error_log_polars() -> pl.DataFrame:
    return pl.DataFrame(schema={column: pl.String for column in ERROR_LOG_COLUMNS})
```

- [ ] **Step 4: Run the business regression subset to verify it passes**

Run: `conda run -n test python -m pytest tests/test_pq_analysis_v3.py::test_build_report_artifacts_exposes_fact_bundle_and_preserves_contracts tests/contracts/test_error_log_contract.py -q`

Expected: PASS

- [ ] **Step 5: Commit the Polars fact layer**

```bash
git add src/analytics/fact_builder.py src/analytics/qty_enricher.py src/analytics/errors.py src/analytics/quality.py tests/test_pq_analysis_v3.py tests/contracts/test_error_log_contract.py
git commit -m "feat(analytics): add polars fact bundle pipeline"
```

### Task 5: Build `SheetModel` Presentation And A `SheetModel` Writer Path

**Files:**
- Create: `src/analytics/presentation_builder.py`
- Modify: `src/analytics/anomaly.py`
- Modify: `src/analytics/table_rendering.py`
- Modify: `src/excel/fast_writer.py`
- Modify: `src/excel/workbook_writer.py`
- Modify: `tests/test_costing_etl.py`

- [ ] **Step 1: Write the failing writer regression for `SheetModel` and anomaly highlight rules**

```python
from __future__ import annotations

from pathlib import Path

from openpyxl import load_workbook

from src.analytics.contracts import ConditionalFormatRule, SheetModel
from src.excel.workbook_writer import CostingWorkbookWriter


def test_workbook_writer_can_export_sheet_models_with_conditional_formats(tmp_path: Path) -> None:
    output_path = tmp_path / 'sheet_models.xlsx'
    writer = CostingWorkbookWriter()
    sheet_models = (
        SheetModel(
            sheet_name='按工单按产品异常值分析',
            columns=('直接材料单位完工成本', '直接材料异常标记'),
            rows_factory=lambda: iter([(18.0, '关注')]),
            column_types={'直接材料单位完工成本': 'price', '直接材料异常标记': 'text'},
            number_formats={'直接材料单位完工成本': '#,##0.00'},
            freeze_panes='A2',
            auto_filter=True,
            fixed_width=15.0,
            conditional_formats=(
                ConditionalFormatRule(
                    target_range='A2:A1048576',
                    formula='=$B2="关注"',
                    format_key='attention',
                ),
            ),
        ),
    )

    writer.write_workbook_from_models(output_path, sheet_models=sheet_models)

    workbook = load_workbook(output_path)
    worksheet = workbook['按工单按产品异常值分析']
    assert worksheet.freeze_panes == 'A2'
    assert worksheet['A2'].number_format == '#,##0.00'
    assert worksheet.conditional_formatting
```

- [ ] **Step 2: Run the writer regression to verify it fails**

Run: `conda run -n test python -m pytest tests/test_costing_etl.py::test_workbook_writer_can_export_sheet_models_with_conditional_formats -q`

Expected: FAIL because the writer only accepts the old multi-DataFrame workbook signature.

- [ ] **Step 3: Implement `build_sheet_models()`, `write_sheet_model()`, and `write_workbook_from_models()`**

```python
# src/analytics/presentation_builder.py
from __future__ import annotations

import pandas as pd
import polars as pl

from src.analytics.contracts import ConditionalFormatRule, FactBundle, FlatSheet, ProductAnomalySection, SheetModel


def dataframe_to_sheet_model(
    *,
    sheet_name: str,
    frame: pl.DataFrame,
    column_types: dict[str, str],
    number_formats: dict[str, str],
    freeze_panes: str | None = 'A2',
    auto_filter: bool = True,
    fixed_width: float | None = 15.0,
    conditional_formats: tuple[ConditionalFormatRule, ...] = (),
) -> SheetModel:
    return SheetModel(
        sheet_name=sheet_name,
        columns=tuple(frame.columns),
        rows_factory=lambda frame=frame: frame.iter_rows(),
        column_types=column_types,
        number_formats=number_formats,
        freeze_panes=freeze_panes,
        auto_filter=auto_filter,
        fixed_width=fixed_width,
        conditional_formats=conditional_formats,
    )


def build_sheet_models(
    *,
    detail_df: pl.DataFrame,
    qty_sheet_df: pd.DataFrame,
    fact_bundle: FactBundle | None,
    work_order_sheet: FlatSheet,
    product_anomaly_sections: list[ProductAnomalySection],
    error_log: pd.DataFrame,
) -> tuple[SheetModel, ...]:
    summary_frame = fact_bundle.product_summary_fact if fact_bundle is not None else pl.DataFrame({'产品编码': [], '产品名称': [], 'period_display': []})

    def _product_anomaly_frame() -> pl.DataFrame:
        frames: list[pl.DataFrame] = []
        for section in product_anomaly_sections:
            section_frame = pl.from_pandas(section.data, include_index=False).with_columns(
                pl.lit(section.product_code).alias('产品编码'),
                pl.lit(section.product_name).alias('产品名称'),
            )
            frames.append(section_frame)
        return pl.concat(frames, how='vertical') if frames else pl.DataFrame({'产品编码': [], '产品名称': [], '月份': []})

    detail_model = dataframe_to_sheet_model(
        sheet_name='成本明细',
        frame=detail_df,
        column_types={column: 'text' for column in detail_df.columns},
        number_formats={},
    )
    qty_model = dataframe_to_sheet_model(
        sheet_name='产品数量统计',
        frame=pl.from_pandas(qty_sheet_df, include_index=False),
        column_types={column: 'text' for column in qty_sheet_df.columns},
        number_formats={},
    )
    direct_material_model = dataframe_to_sheet_model(
        sheet_name='直接材料_价量比',
        frame=summary_frame,
        column_types={column: 'text' for column in summary_frame.columns},
        number_formats={},
        freeze_panes='C3',
    )
    direct_labor_model = dataframe_to_sheet_model(
        sheet_name='直接人工_价量比',
        frame=summary_frame,
        column_types={column: 'text' for column in summary_frame.columns},
        number_formats={},
        freeze_panes='C3',
    )
    manufacturing_model = dataframe_to_sheet_model(
        sheet_name='制造费用_价量比',
        frame=summary_frame,
        column_types={column: 'text' for column in summary_frame.columns},
        number_formats={},
        freeze_panes='C3',
    )
    work_order_model = dataframe_to_sheet_model(
        sheet_name='按工单按产品异常值分析',
        frame=pl.from_pandas(work_order_sheet.data, include_index=False),
        column_types=work_order_sheet.column_types,
        number_formats={column: '#,##0.00' for column, metric_type in work_order_sheet.column_types.items() if metric_type in {'amount', 'price', 'qty', 'score'}},
        conditional_formats=(
            ConditionalFormatRule(
                target_range='J2:J1048576',
                formula='=$R2="关注"',
                format_key='attention',
            ),
            ConditionalFormatRule(
                target_range='N2:N1048576',
                formula='=$V2="高度可疑"',
                format_key='suspicious',
            ),
        ),
    )
    product_anomaly_model = dataframe_to_sheet_model(
        sheet_name='按产品异常值分析',
        frame=_product_anomaly_frame(),
        column_types={'产品编码': 'text', '产品名称': 'text', '月份': 'text'},
        number_formats={},
        fixed_width=15.0,
    )
    error_log_model = dataframe_to_sheet_model(
        sheet_name='error_log',
        frame=pl.from_pandas(error_log, include_index=False),
        column_types={column: 'text' for column in error_log.columns},
        number_formats={},
        freeze_panes=None,
        auto_filter=False,
        fixed_width=None,
    )
    return (
        detail_model,
        qty_model,
        direct_material_model,
        direct_labor_model,
        manufacturing_model,
        work_order_model,
        product_anomaly_model,
        error_log_model,
    )
```

```python
# src/excel/fast_writer.py
from __future__ import annotations

from typing import Any

import pandas as pd

from src.analytics.contracts import SheetModel


class FastSheetWriter:
    def _build_formats(self, workbook) -> dict[str, Any]:
        return {
            'header': workbook.add_format({'bold': True, 'bg_color': '#D9E1F2', 'align': 'center', 'valign': 'vcenter', 'border': 1}),
            'attention': workbook.add_format({'bg_color': '#DDEBF7'}),
            'suspicious': workbook.add_format({'bg_color': '#4472C4', 'font_color': '#FFFFFF'}),
            'decimal': workbook.add_format({'num_format': '#,##0.00', 'border': 1}),
            'text': workbook.add_format({'border': 1}),
        }

    def _resolve_number_format(self, number_format: str | None, workbook) -> Any:
        if number_format == '#,##0.00':
            return workbook.add_format({'num_format': '#,##0.00', 'border': 1})
        return workbook.add_format({'border': 1})

    def write_sheet_model(self, writer: pd.ExcelWriter, model: SheetModel) -> Any:
        workbook = writer.book
        worksheet = workbook.add_worksheet(model.sheet_name)
        writer.sheets[model.sheet_name] = worksheet

        formats = self._build_formats(workbook)
        for col_idx, column_name in enumerate(model.columns):
            worksheet.write(0, col_idx, column_name, formats['header'])

        last_row = 0
        for row_idx, row in enumerate(model.rows_factory(), start=1):
            worksheet.write_row(row_idx, 0, [_coerce_row_value_for_excel(value) for value in row])
            last_row = row_idx

        if model.freeze_panes:
            freeze_row, freeze_col = _freeze_panes_to_rc(model.freeze_panes)
            worksheet.freeze_panes(freeze_row, freeze_col)
        if model.auto_filter and model.columns:
            worksheet.autofilter(0, 0, max(1, last_row), len(model.columns) - 1)

        width = _resolve_fixed_width(int(model.fixed_width)) if model.fixed_width is not None else None
        for col_idx, column_name in enumerate(model.columns):
            cell_format = self._resolve_number_format(model.number_formats.get(column_name), workbook)
            worksheet.set_column(col_idx, col_idx, width, cell_format)

        for rule in model.conditional_formats:
            worksheet.conditional_format(
                rule.target_range,
                {'type': 'formula', 'criteria': rule.formula, 'format': formats[rule.format_key]},
            )
        return worksheet
```

```python
# src/excel/workbook_writer.py
from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pandas as pd

from src.analytics.contracts import SheetModel


class CostingWorkbookWriter:
    def __init__(self) -> None:
        self.sheet_writer = FastSheetWriter()

    def write_workbook_from_models(self, output_path: Path, *, sheet_models: Sequence[SheetModel]) -> None:
        with pd.ExcelWriter(
            output_path,
            engine='xlsxwriter',
            engine_kwargs={'options': {'constant_memory': True, 'strings_to_urls': False}},
        ) as writer:
            for model in sheet_models:
                self.sheet_writer.write_sheet_model(writer, model)
```

- [ ] **Step 4: Run the writer regression to verify it passes**

Run: `conda run -n test python -m pytest tests/test_costing_etl.py::test_workbook_writer_can_export_sheet_models_with_conditional_formats -q`

Expected: PASS

- [ ] **Step 5: Commit the `SheetModel` presentation and writer path**

```bash
git add src/analytics/presentation_builder.py src/analytics/anomaly.py src/analytics/table_rendering.py src/excel/fast_writer.py src/excel/workbook_writer.py tests/test_costing_etl.py
git commit -m "feat(excel): add sheet model workbook export path"
```

### Task 6: Switch `CostingWorkbookETL` To The New Payload Pipeline And Re-Verify Contracts

**Files:**
- Modify: `src/etl/pipeline.py`
- Modify: `src/etl/costing_etl.py`
- Modify: `src/etl/runner.py`
- Modify: `tests/test_costing_etl.py`
- Modify: `tests/test_runner.py`
- Modify: `tests/contracts/_workbook_contract_helper.py`
- Modify: `tests/contracts/test_workbook_contract.py`
- Modify: `tests/contracts/generate_baselines.py`
- Modify: `tests/contracts/baselines/workbook_semantics.json`

- [ ] **Step 1: Write the failing payload and timing regression for the end-to-end ETL switch**

```python
from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import patch

from src.analytics.contracts import SheetModel, WorkbookPayload
from src.etl.costing_etl import CostingWorkbookETL


def test_process_file_uses_workbook_payload_and_logs_all_new_stage_timings(caplog, tmp_path: Path) -> None:
    caplog.set_level(logging.INFO)
    etl = CostingWorkbookETL(skip_rows=2, product_order=())
    payload = WorkbookPayload(
        sheet_models=(
            SheetModel(
                sheet_name='成本明细',
                columns=('产品编码',),
                rows_factory=lambda: iter([('P001',)]),
                column_types={'产品编码': 'text'},
                number_formats={},
            ),
        ),
        quality_metrics=(),
        error_log_count=0,
        stage_timings={
            'ingest': 1.0,
            'normalize': 2.0,
            'fact': 3.0,
            'analysis': 4.0,
            'presentation': 5.0,
        },
    )

    with (
        patch.object(etl.pipeline, 'build_workbook_payload', return_value=payload) as payload_mock,
        patch.object(etl.workbook_writer, 'write_workbook_from_models') as writer_mock,
    ):
        assert etl.process_file(tmp_path / 'input.xlsx', tmp_path / 'output.xlsx') is True

    payload_mock.assert_called_once()
    writer_mock.assert_called_once()
    messages = [record.message for record in caplog.records]
    assert any('Timing | stage=ingest' in message for message in messages)
    assert any('Timing | stage=normalize' in message for message in messages)
    assert any('Timing | stage=fact' in message for message in messages)
    assert any('Timing | stage=analysis' in message for message in messages)
    assert any('Timing | stage=presentation' in message for message in messages)
    assert any('Timing | stage=export' in message for message in messages)
```

- [ ] **Step 2: Run the end-to-end ETL regression and workbook contracts to verify they fail**

Run: `conda run -n test python -m pytest tests/test_costing_etl.py::test_process_file_uses_workbook_payload_and_logs_all_new_stage_timings tests/contracts/test_workbook_contract.py -q`

Expected: FAIL because `CostingWorkbookETL` still builds the workbook through the old DataFrame-first orchestration path.

- [ ] **Step 3: Switch the pipeline and ETL entrypoint to `WorkbookPayload`, then rebuild the contract helper on top of the new path**

```python
# src/etl/pipeline.py
from __future__ import annotations

from pathlib import Path
from time import perf_counter

from src.analytics.contracts import WorkbookPayload
from src.analytics.presentation_builder import build_sheet_models
from src.analytics.qty_enricher import build_report_artifacts
from src.etl.stages.normalizer import build_normalized_cost_frame
from src.etl.stages.reader import load_raw_workbook
from src.etl.stages.splitter import split_normalized_frames


class CostingEtlPipeline:
    def build_workbook_payload(
        self,
        input_path: Path,
        *,
        standalone_cost_items: tuple[str, ...],
    ) -> WorkbookPayload:
        timings: dict[str, float] = {}

        ingest_start = perf_counter()
        raw = load_raw_workbook(input_path, skip_rows=self.skip_rows)
        timings['ingest'] = perf_counter() - ingest_start

        normalize_start = perf_counter()
        normalized = build_normalized_cost_frame(
            raw,
            child_material_column=self.child_material_column,
            cost_item_column=self.cost_item_column,
            period_column=self.period_column,
            fill_columns=self.fill_columns,
            vendor_columns=self.vendor_columns,
            cost_center_column=self.cost_center_column,
            integrated_workshop_name=self.integrated_workshop_name,
        )
        timings['normalize'] = perf_counter() - normalize_start

        fact_start = perf_counter()
        split = split_normalized_frames(
            normalized,
            child_material_column=self.child_material_column,
            cost_item_column=self.cost_item_column,
            order_number_column=self.order_number_column,
            filled_cost_item_column=self.filled_cost_item_column,
            qty_columns=self.qty_columns,
            detail_columns=self.detail_columns,
        )
        artifacts = build_report_artifacts(split.detail_df, split.qty_df, standalone_cost_items=standalone_cost_items)
        timings['fact'] = perf_counter() - fact_start

        analysis_start = perf_counter()
        _ = artifacts.fact_bundle
        timings['analysis'] = perf_counter() - analysis_start

        presentation_start = perf_counter()
        sheet_models = build_sheet_models(
            detail_df=split.detail_df,
            qty_sheet_df=artifacts.qty_sheet_df,
            fact_bundle=artifacts.fact_bundle,
            work_order_sheet=artifacts.work_order_sheet,
            product_anomaly_sections=artifacts.product_anomaly_sections,
            error_log=artifacts.error_log,
        )
        timings['presentation'] = perf_counter() - presentation_start
        return WorkbookPayload(
            sheet_models=sheet_models,
            quality_metrics=artifacts.quality_metrics,
            error_log_count=len(artifacts.error_log),
            stage_timings=timings,
        )
```

```python
# src/etl/costing_etl.py
from __future__ import annotations

from time import perf_counter


def process_file(self, input_path: Path, output_path: Path) -> bool:
    try:
        total_start = perf_counter()

        payload = self.pipeline.build_workbook_payload(
            input_path,
            standalone_cost_items=self.standalone_cost_items,
        )
        logger.info('Timing | stage=ingest | seconds=%.3f', payload.stage_timings['ingest'])
        logger.info('Timing | stage=normalize | seconds=%.3f', payload.stage_timings['normalize'])
        logger.info('Timing | stage=fact | seconds=%.3f', payload.stage_timings['fact'])
        logger.info('Timing | stage=analysis | seconds=%.3f', payload.stage_timings['analysis'])
        logger.info('Timing | stage=presentation | seconds=%.3f', payload.stage_timings['presentation'])

        export_start = perf_counter()
        self.workbook_writer.write_workbook_from_models(output_path, sheet_models=payload.sheet_models)
        logger.info('Timing | stage=export | seconds=%.3f', perf_counter() - export_start)
        logger.info('Timing | stage=total | seconds=%.3f', perf_counter() - total_start)

        self.last_quality_metrics = payload.quality_metrics
        self.last_error_log_count = payload.error_log_count
        return True
    except FileNotFoundError:
        logger.error('文件不存在: %s', input_path)
        return False
```

```python
# tests/contracts/_workbook_contract_helper.py
def build_default_contract_workbook(tmp_path: Path) -> Path:
    etl = CostingWorkbookETL(skip_rows=2)
    input_path = tmp_path / 'input.xlsx'
    output_path = tmp_path / DEFAULT_WORKBOOK_BASENAME
    assert etl.process_file(input_path, output_path) is True
    return output_path
```

- [ ] **Step 4: Run the contract suite, full automated suite, lint, and real-sample benchmarks**

Run: `conda run -n test python -m pytest tests/contracts/test_workbook_contract.py tests/contracts/test_error_log_contract.py tests/test_costing_etl.py tests/test_etl_pipeline.py tests/test_pq_analysis_v3.py tests/test_runner.py -q`

Expected: PASS

Run: `conda run -n test python -m pytest tests -q`

Expected: PASS

Run: `conda run -n test python -m ruff check src tests`

Expected: `All checks passed!`

Run: `conda run -n test python tests/contracts/generate_baselines.py`

Expected: the command exits `0`; `tests/contracts/baselines/workbook_semantics.json` changes only if workbook semantics intentionally changed.

Run: `conda run -n test python main.py gb`

Expected: exits `0`, writes `data/processed/gb/*_处理后.xlsx`, logs `Timing | stage=export`, and reports a total time materially below the 2026-04-12 baseline.

Run: `conda run -n test python main.py sk`

Expected: exits `0`, writes `data/processed/sk/*_处理后.xlsx`, logs `Timing | stage=export`, and reports a total time materially below the 2026-04-12 baseline.

- [ ] **Step 5: Commit the full pipeline switch and refreshed contract baseline**

```bash
git add src/etl/pipeline.py src/etl/costing_etl.py src/etl/runner.py tests/test_costing_etl.py tests/test_runner.py tests/contracts/_workbook_contract_helper.py tests/contracts/test_workbook_contract.py tests/contracts/generate_baselines.py tests/contracts/baselines/workbook_semantics.json
git commit -m "feat(etl): switch costing pipeline to full-chain polars"
```

# Costing GUI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a PySide6 Linux desktop GUI for the costing ETL while preserving CLI compatibility, moving product whitelists to shared config, changing workbook output to four renamed sheets, and writing only the `.xlsx` artifact.

**Architecture:** Introduce a small service layer that both CLI and GUI call. Keep ETL business logic in `src/etl`, keep product whitelist persistence in `src/config`, and keep GUI code in `src/gui` with background workers that exchange lightweight result objects only.

**Tech Stack:** Python 3.11+, PySide6, pandas, polars, python-calamine, openpyxl, xlsxwriter, pytest, ruff.

---

## File Structure

Create:

- `src/config/product_whitelist_store.py`  
  Load, validate, save, and restore GB/SK product whitelist config from `config/product_whitelists.json`.
- `src/services/__init__.py`  
  Package marker for the shared service layer.
- `src/services/costing_service.py`  
  Shared CLI/GUI request, precheck, run, output path, candidate product scan, and runtime summary logic.
- `src/gui/__init__.py`  
  Package marker for GUI code.
- `src/gui/app.py`  
  `python -m src.gui.app` entry point.
- `src/gui/form_state.py`  
  Pure dataclasses and conversion helpers for GUI form state.
- `src/gui/validators.py`  
  GUI-facing validation wrappers around service validation.
- `src/gui/task_worker.py`  
  Qt worker objects for scan, precheck, and run.
- `src/gui/styles.py`  
  Qt stylesheet and status color constants.
- `src/gui/main_window.py`  
  Main PySide6 window, widgets, layout, signals, and state transitions.
- `tests/test_product_whitelist_store.py`  
  Unit tests for product whitelist persistence and validation.
- `tests/test_costing_service.py`  
  Unit tests for service validation, precheck, execution, output paths, candidate products, and no CSV/JSON output.
- `tests/test_gui_form_state.py`  
  GUI state tests that do not require opening a real window.

Modify:

- `pyproject.toml`  
  Add `gui` optional dependency with `PySide6>=6.8`.
- `main.py`  
  Keep CLI args but delegate request construction and execution to service.
- `src/analytics/presentation_builder.py`  
  Rename sheet models and remove three price/volume sheet models.
- `src/excel/workbook_writer.py`  
  Rename legacy `write_workbook` path sheet names and stop writing analysis sheet loop in the legacy method.
- `src/etl/runner.py`  
  Convert `run_pipeline()` into a compatibility wrapper that builds a `CostingRunRequest`, calls service precheck/run functions, and prints CLI summaries.
- `src/config/pipelines.py`  
  Keep built-in defaults as fallback and expose them to whitelist store.
- `tests/contracts/_workbook_contract_helper.py`  
  Update expected sheet names and extraction logic for renamed sheets.
- `tests/contracts/baselines/workbook_semantics.json`  
  Regenerate after workbook contract changes.
- `tests/contracts/README.md`  
  Document four-sheet workbook baseline and no external CSV/JSON artifact.
- `tests/test_costing_etl.py`  
  Update sheet-name assertions and remove price/volume sheet assertions.
- `tests/test_runner.py`  
  Update runner expectations: no `error_log.csv`, no `summary.json`, output workbook only.
- `tests/test_main.py`  
  Keep current `run_pipeline` monkeypatch tests unless implementation changes `main.py` beyond argument parsing.
- `README.md`  
  Update GUI usage, output artifact list, sheet names, and no external error/summary files.
- `AGENTS.md`  
  Update project command/output description to the new business contract.

---

### Task 1: Lock New Workbook Sheet Contract

**Files:**
- Modify: `src/analytics/presentation_builder.py`
- Modify: `src/excel/workbook_writer.py`
- Modify: `tests/test_costing_etl.py`

- [ ] **Step 1: Write failing tests for four renamed SheetModels**

Add this test near the existing `build_sheet_models` tests in `tests/test_costing_etl.py`:

```python
def test_build_sheet_models_outputs_four_business_named_sheets() -> None:
    models = build_sheet_models(
        detail_df=pd.DataFrame([{'月份': '2025年01期', '本期完工金额': 100.0}]),
        qty_sheet_df=pd.DataFrame([{'月份': '2025年01期', '本期完工金额': 100.0}]),
        fact_bundle=None,
        work_order_sheet=FlatSheet(
            data=pd.DataFrame([{'月份': '2025年01期', '产品编码': 'P001', '产品名称': '产品A'}]),
            column_types={'月份': 'text', '产品编码': 'text', '产品名称': 'text'},
        ),
        product_anomaly_sections=[],
    )

    assert [model.sheet_name for model in models] == [
        '成本计算单总表',
        '成本计算单数量聚合维度',
        '成本分析工单维度',
        '成本分析产品维度',
    ]
    assert not any(model.sheet_name.endswith('_价量比') for model in models)
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_costing_etl.py::test_build_sheet_models_outputs_four_business_named_sheets -q
```

Expected: FAIL because `build_sheet_models()` still returns seven old sheet names.

- [ ] **Step 3: Update `build_sheet_models()` to return four renamed models**

In `src/analytics/presentation_builder.py`, change the docstring and model sheet names:

```python
def build_sheet_models(
    *,
    detail_df: pl.DataFrame | pd.DataFrame,
    qty_sheet_df: pd.DataFrame | pl.DataFrame,
    fact_bundle: FactBundle | None,
    work_order_sheet: FlatSheet,
    product_anomaly_sections: list[ProductAnomalySection],
) -> tuple[SheetModel, ...]:
    """构建 workbook 的 4 张业务 SheetModel。"""
```

Set the model names:

```python
detail_model = dataframe_to_sheet_model(
    sheet_name='成本计算单总表',
    frame=detail_frame,
    column_types=dict.fromkeys(detail_frame.columns, 'text'),
    number_formats={column: '#,##0.00' for column in detail_frame.columns if column in _DETAIL_TWO_DECIMAL_COLUMNS},
    write_mode='dataframe_fast',
    style_profile='lightweight_flat',
    source_frame=detail_frame,
)
```

```python
qty_model = dataframe_to_sheet_model(
    sheet_name='成本计算单数量聚合维度',
    frame=qty_frame,
    column_types=dict.fromkeys(qty_frame.columns, 'text'),
    number_formats={column: '#,##0.00' for column in qty_frame.columns if column in qty_two_decimal_columns},
    write_mode='dataframe_fast',
    style_profile='lightweight_flat',
    source_frame=qty_frame,
)
```

```python
work_order_model = dataframe_to_sheet_model(
    sheet_name='成本分析工单维度',
    frame=work_order_frame,
    column_types=work_order_column_types,
    number_formats=work_order_number_formats,
    conditional_formats=work_order_conditional_formats,
)
```

```python
product_anomaly_model = dataframe_to_sheet_model(
    sheet_name='成本分析产品维度',
    frame=product_anomaly_frame,
    column_types=product_anomaly_column_types,
    number_formats=product_anomaly_number_formats,
    freeze_panes='A7' if has_scoped_product_anomaly_section else 'A6',
    fixed_width=15.0,
)
```

Remove the `analysis_models` tuple creation and return only:

```python
return (
    detail_model,
    qty_model,
    work_order_model,
    product_anomaly_model,
)
```

Keep `_ANALYSIS_SHEET_COLUMN_LAYOUT` and `_build_analysis_sheet_model()` in this task. After the focused test run, delete them in the same commit if `rg "_build_analysis_sheet_model|_ANALYSIS_SHEET_COLUMN_LAYOUT" src tests` shows no remaining references.

- [ ] **Step 4: Update legacy writer sheet names**

In `src/excel/workbook_writer.py`, update `write_workbook()` for its legacy path:

```python
self.sheet_writer.write_dataframe_fast(
    writer,
    '成本计算单总表',
    detail_df,
    numeric_columns=DETAIL_TWO_DECIMAL_COLUMNS,
    freeze_panes='A2',
    fixed_width=15,
)
self.sheet_writer.write_dataframe_fast(
    writer,
    '成本计算单数量聚合维度',
    qty_sheet_df,
    numeric_columns=_resolve_qty_numeric_columns(qty_sheet_df),
    freeze_panes='A2',
    fixed_width=15,
)
```

Delete this loop from `write_workbook()`:

```python
for sheet_name, sections in analysis_tables.items():
    self.sheet_writer.write_analysis_sheet(writer, sheet_name, sections)
```

Update the remaining legacy sheet names:

```python
work_order_worksheet = self.sheet_writer.write_flat_sheet(
    writer,
    '成本分析工单维度',
    work_order_sheet,
    freeze_panes='A2',
    fixed_width=15,
)
self.sheet_writer.write_product_anomaly_sheet(writer, '成本分析产品维度', product_anomaly_sections)
```

- [ ] **Step 5: Update existing model-name assertions in `tests/test_costing_etl.py`**

Replace old names in affected tests:

```python
detail_model = next(model for model in models if model.sheet_name == '成本计算单总表')
qty_model = next(model for model in models if model.sheet_name == '成本计算单数量聚合维度')
work_order_model = next(model for model in models if model.sheet_name == '成本分析工单维度')
product_anomaly_model = next(model for model in models if model.sheet_name == '成本分析产品维度')
```

Replace `assert len(models) == 7` with:

```python
assert len(models) == 4
```

Delete assertions that locate or inspect `直接材料_价量比`, `直接人工_价量比`, or `制造费用_价量比`.

- [ ] **Step 6: Run focused tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_costing_etl.py -q
```

Expected: PASS or failures only in workbook integration assertions that still contain old sheet names. Fix those assertions in this task using the same new names.

- [ ] **Step 7: Commit**

```bash
git add src/analytics/presentation_builder.py src/excel/workbook_writer.py tests/test_costing_etl.py
git commit -m "feat(excel): output four renamed costing sheets"
```

---

### Task 2: Update Workbook Contract Baseline

**Files:**
- Modify: `tests/contracts/_workbook_contract_helper.py`
- Modify: `tests/contracts/baselines/workbook_semantics.json`
- Modify: `tests/contracts/README.md`

- [ ] **Step 1: Update contract helper constants**

In `tests/contracts/_workbook_contract_helper.py`, replace `DEFAULT_SHEETS` with:

```python
DEFAULT_SHEETS = (
    '成本计算单总表',
    '成本计算单数量聚合维度',
    '成本分析工单维度',
    '成本分析产品维度',
)
```

Replace `ANALYSIS_SHEETS` with:

```python
ANALYSIS_SHEETS: set[str] = set()
```

Update `extract_workbook_semantics()` product anomaly branch:

```python
if sheet_name == '成本分析产品维度':
    semantics['sheets'][sheet_name] = _extract_product_anomaly_sheet(worksheet)
else:
    semantics['sheets'][sheet_name] = _extract_flat_sheet(worksheet)
```

Update `extract_highlight_semantics()`:

```python
worksheet = workbook['成本分析工单维度']
```

- [ ] **Step 2: Run contract tests to verify baseline mismatch**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/contracts/test_workbook_contract.py -q
```

Expected: FAIL because `workbook_semantics.json` still contains the old seven-sheet baseline.

- [ ] **Step 3: Regenerate workbook baseline**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m tests.contracts.generate_baselines
```

Expected: `tests/contracts/baselines/workbook_semantics.json` is rewritten with four renamed sheets. This plan expects `error_log_contract.json` to remain semantically unchanged; do not add it to the commit if `git diff -- tests/contracts/baselines/error_log_contract.json` is empty.

- [ ] **Step 4: Update contract README**

In `tests/contracts/README.md`, replace the current baseline description with:

```markdown
## 当前基线

- `baselines/workbook_semantics.json`
  - 冻结 4 张 Sheet 的顺序、列序、freeze panes、auto filter、number format、column width 和工单异常高亮位置。
- `baselines/error_log_contract.json`
  - 冻结运行时 `error_log` 数据契约；第一版 GUI 化后不再把该契约落盘为 CSV，但内存摘要和质量计数仍依赖它。
```

- [ ] **Step 5: Run contract tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/contracts -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add tests/contracts/_workbook_contract_helper.py tests/contracts/baselines/workbook_semantics.json tests/contracts/README.md
git commit -m "test(contracts): update workbook sheet baseline"
```

---

### Task 3: Add Shared Product Whitelist Store

**Files:**
- Create: `src/config/product_whitelist_store.py`
- Create: `tests/test_product_whitelist_store.py`

- [ ] **Step 1: Write failing whitelist store tests**

Create `tests/test_product_whitelist_store.py`:

```python
from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.config.pipelines import GB_PIPELINE, SK_PIPELINE
from src.config.product_whitelist_store import (
    ProductWhitelistConfigError,
    ProductWhitelistStore,
    ProductWhitelistStoreResult,
)


def test_load_uses_builtin_defaults_when_file_is_missing(tmp_path: Path) -> None:
    store = ProductWhitelistStore(tmp_path / 'product_whitelists.json')

    result = store.load()

    assert isinstance(result, ProductWhitelistStoreResult)
    assert result.exists is False
    assert result.product_orders['gb'] == GB_PIPELINE.product_order
    assert result.product_orders['sk'] == SK_PIPELINE.product_order


def test_save_and_load_round_trips_pipeline_orders(tmp_path: Path) -> None:
    path = tmp_path / 'product_whitelists.json'
    store = ProductWhitelistStore(path)
    product_orders = {
        'gb': (('GB-001', '产品甲'), ('GB-002', '产品乙')),
        'sk': (('SK-001', '产品丙'),),
    }

    store.save(product_orders)
    result = store.load()

    assert result.exists is True
    assert result.product_orders == product_orders
    assert json.loads(path.read_text(encoding='utf-8')) == {
        'gb': [
            {'product_code': 'GB-001', 'product_name': '产品甲'},
            {'product_code': 'GB-002', 'product_name': '产品乙'},
        ],
        'sk': [{'product_code': 'SK-001', 'product_name': '产品丙'}],
    }


def test_load_rejects_duplicate_product_pairs(tmp_path: Path) -> None:
    path = tmp_path / 'product_whitelists.json'
    path.write_text(
        json.dumps(
            {
                'gb': [
                    {'product_code': 'GB-001', 'product_name': '产品甲'},
                    {'product_code': 'GB-001', 'product_name': '产品甲'},
                ],
                'sk': [],
            },
            ensure_ascii=False,
        ),
        encoding='utf-8',
    )
    store = ProductWhitelistStore(path)

    with pytest.raises(ProductWhitelistConfigError, match='重复'):
        store.load()


def test_load_rejects_blank_product_fields(tmp_path: Path) -> None:
    path = tmp_path / 'product_whitelists.json'
    path.write_text(
        json.dumps({'gb': [{'product_code': ' ', 'product_name': '产品甲'}], 'sk': []}, ensure_ascii=False),
        encoding='utf-8',
    )
    store = ProductWhitelistStore(path)

    with pytest.raises(ProductWhitelistConfigError, match='不能为空'):
        store.load()


def test_restore_default_replaces_only_one_pipeline(tmp_path: Path) -> None:
    path = tmp_path / 'product_whitelists.json'
    store = ProductWhitelistStore(path)
    store.save({'gb': (('CUSTOM', '自定义'),), 'sk': (('SK-CUSTOM', '数控'),)})

    store.restore_default('gb')
    result = store.load()

    assert result.product_orders['gb'] == GB_PIPELINE.product_order
    assert result.product_orders['sk'] == (('SK-CUSTOM', '数控'),)
```

- [ ] **Step 2: Run failing tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_product_whitelist_store.py -q
```

Expected: FAIL because `src.config.product_whitelist_store` does not exist.

- [ ] **Step 3: Implement whitelist store**

Create `src/config/product_whitelist_store.py`:

```python
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.config.pipelines import PIPELINES, ProductOrder
from src.config.settings import PROJECT_ROOT

DEFAULT_PRODUCT_WHITELIST_PATH = PROJECT_ROOT / 'config' / 'product_whitelists.json'


class ProductWhitelistConfigError(ValueError):
    """产品白名单配置非法。"""


@dataclass(frozen=True)
class ProductWhitelistStoreResult:
    exists: bool
    product_orders: dict[str, ProductOrder]


class ProductWhitelistStore:
    def __init__(self, path: Path = DEFAULT_PRODUCT_WHITELIST_PATH) -> None:
        self.path = path

    def load(self) -> ProductWhitelistStoreResult:
        if not self.path.exists():
            return ProductWhitelistStoreResult(exists=False, product_orders=_default_product_orders())

        try:
            payload = json.loads(self.path.read_text(encoding='utf-8'))
        except json.JSONDecodeError as exc:
            raise ProductWhitelistConfigError(f'产品白名单配置不是合法 JSON: {exc}') from exc

        return ProductWhitelistStoreResult(exists=True, product_orders=_parse_payload(payload))

    def save(self, product_orders: dict[str, ProductOrder]) -> None:
        normalized = _normalize_product_orders(product_orders)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            pipeline_name: [
                {'product_code': code, 'product_name': name}
                for code, name in normalized.get(pipeline_name, ())
            ]
            for pipeline_name in sorted(PIPELINES)
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')

    def restore_default(self, pipeline_name: str) -> None:
        if pipeline_name not in PIPELINES:
            raise ProductWhitelistConfigError(f'未知管线: {pipeline_name}')
        current = self.load().product_orders
        current[pipeline_name] = PIPELINES[pipeline_name].product_order
        self.save(current)


def load_product_order_for_pipeline(pipeline_name: str, path: Path = DEFAULT_PRODUCT_WHITELIST_PATH) -> ProductOrder:
    if pipeline_name not in PIPELINES:
        raise ProductWhitelistConfigError(f'未知管线: {pipeline_name}')
    return ProductWhitelistStore(path).load().product_orders[pipeline_name]


def _default_product_orders() -> dict[str, ProductOrder]:
    return {pipeline_name: config.product_order for pipeline_name, config in PIPELINES.items()}


def _parse_payload(payload: Any) -> dict[str, ProductOrder]:
    if not isinstance(payload, dict):
        raise ProductWhitelistConfigError('产品白名单配置必须是对象')
    raw_orders = {pipeline_name: payload.get(pipeline_name, []) for pipeline_name in PIPELINES}
    return _normalize_product_orders(raw_orders)


def _normalize_product_orders(product_orders: dict[str, Any]) -> dict[str, ProductOrder]:
    normalized: dict[str, ProductOrder] = {}
    for pipeline_name in PIPELINES:
        raw_items = product_orders.get(pipeline_name, ())
        if not isinstance(raw_items, (list, tuple)):
            raise ProductWhitelistConfigError(f'{pipeline_name} 白名单必须是列表')
        pairs: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for index, item in enumerate(raw_items, start=1):
            if isinstance(item, dict):
                code = str(item.get('product_code', '')).strip()
                name = str(item.get('product_name', '')).strip()
            elif isinstance(item, (list, tuple)) and len(item) == 2:
                code = str(item[0]).strip()
                name = str(item[1]).strip()
            else:
                raise ProductWhitelistConfigError(f'{pipeline_name} 第 {index} 项必须包含 product_code/product_name')
            if not code or not name:
                raise ProductWhitelistConfigError(f'{pipeline_name} 第 {index} 项产品编码和产品名称不能为空')
            pair = (code, name)
            if pair in seen:
                raise ProductWhitelistConfigError(f'{pipeline_name} 白名单存在重复产品: {code} / {name}')
            seen.add(pair)
            pairs.append(pair)
        normalized[pipeline_name] = tuple(pairs)
    return normalized
```

- [ ] **Step 4: Run whitelist store tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_product_whitelist_store.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/config/product_whitelist_store.py tests/test_product_whitelist_store.py
git commit -m "feat(config): add shared product whitelist store"
```

---

### Task 4: Add Shared Costing Service

**Files:**
- Create: `src/services/__init__.py`
- Create: `src/services/costing_service.py`
- Create: `tests/test_costing_service.py`

- [ ] **Step 1: Write failing service tests**

Create `tests/test_costing_service.py`:

```python
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.analytics.contracts import QualityMetric
from src.config.pipelines import GB_PIPELINE
from src.services.costing_service import (
    CostingRunRequest,
    ServiceStatus,
    build_output_workbook_path,
    precheck_costing_run,
    run_costing_request,
)


def _request(tmp_path: Path, *, input_name: str = 'GB-成本计算单.xlsx') -> CostingRunRequest:
    input_path = tmp_path / input_name
    input_path.write_bytes(b'raw')
    return CostingRunRequest(
        pipeline='gb',
        input_path=input_path,
        output_dir=tmp_path / 'processed',
        month_start=None,
        month_end=None,
        product_order=(('GB_C.D.B0040AA', 'BMS-750W驱动器'),),
        benchmark=True,
        overwrite_confirmed=True,
    )


def test_build_output_workbook_path_uses_month_suffix(tmp_path: Path) -> None:
    path = build_output_workbook_path(
        tmp_path,
        tmp_path / 'GB-成本计算单.xlsx',
        month_start='2025-01',
        month_end='2025-03',
    )

    assert path == tmp_path / 'GB-成本计算单_处理后_2025-01_2025-03.xlsx'


def test_precheck_rejects_non_xlsx(tmp_path: Path) -> None:
    request = _request(tmp_path, input_name='input.xls')

    result = precheck_costing_run(request)

    assert result.status == ServiceStatus.FAILED
    assert result.error_code == 'UNSUPPORTED_FILE_TYPE'
    assert 'xlsx' in result.message


def test_precheck_reports_existing_output_when_not_confirmed(tmp_path: Path) -> None:
    request = _request(tmp_path)
    request.output_dir.mkdir()
    planned = build_output_workbook_path(request.output_dir, request.input_path)
    planned.write_text('old', encoding='utf-8')
    request = CostingRunRequest(
        pipeline=request.pipeline,
        input_path=request.input_path,
        output_dir=request.output_dir,
        month_start=None,
        month_end=None,
        product_order=request.product_order,
        overwrite_confirmed=False,
    )

    result = precheck_costing_run(request)

    assert result.status == ServiceStatus.FAILED
    assert result.error_code == 'OUTPUT_EXISTS'
    assert result.workbook_path == planned


def test_run_costing_request_writes_only_workbook(monkeypatch, tmp_path: Path) -> None:
    request = _request(tmp_path)

    class _DummyETL:
        def __init__(
            self,
            skip_rows: int,
            *,
            product_order,
            standalone_cost_items,
            product_anomaly_scope_mode,
            month_range=None,
            ensure_output_directories=True,
        ) -> None:
            self.product_order = product_order
            self.last_quality_metrics = (QualityMetric('行数勾稽', '产品数量统计输出行数', '1', '仅保留有效工单'),)
            self.last_error_log_count = 2
            self.last_error_log_frame = pd.DataFrame([{'issue_type': 'MISSING_AMOUNT'}])
            self.last_work_order_sheet_frame = pd.DataFrame([{'异常等级': '关注', '异常主要来源': '材料异常'}])
            self.last_month_filter_summary = None
            self.last_stage_timings = {'ingest': 0.1, 'export': 0.2}
            self.last_ingest_backend = 'calamine'

        def prepare_payload(self, input_path: Path) -> bool:
            return True

        def process_file(self, input_path: Path, output_path: Path) -> bool:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text('xlsx', encoding='utf-8')
            return True

    monkeypatch.setattr('src.services.costing_service.CostingWorkbookETL', _DummyETL)

    result = run_costing_request(request)

    assert result.status == ServiceStatus.SUCCEEDED
    assert result.workbook_path == tmp_path / 'processed' / 'GB-成本计算单_处理后.xlsx'
    assert result.workbook_path.exists()
    assert not (tmp_path / 'processed' / 'GB-成本计算单_处理后_error_log.csv').exists()
    assert not (tmp_path / 'processed' / 'GB-成本计算单_处理后_summary.json').exists()
    assert result.error_log_count == 2
    assert result.quality_metrics[0].metric == '产品数量统计输出行数'
```

- [ ] **Step 2: Run failing service tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_costing_service.py -q
```

Expected: FAIL because `src.services.costing_service` does not exist.

- [ ] **Step 3: Create service package marker**

Create `src/services/__init__.py`:

```python
"""Shared service layer for CLI and GUI entry points."""
```

- [ ] **Step 4: Implement service dataclasses and output path logic**

Create `src/services/costing_service.py` with the imports, enum, request, and result:

```python
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

import pandas as pd

from src.analytics.contracts import QualityMetric
from src.config.pipelines import PIPELINES, ProductOrder
from src.etl.costing_etl import CostingWorkbookETL
from src.etl.month_filter import MonthRange, build_month_range


class ServiceStatus(StrEnum):
    WAITING = 'waiting'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'


@dataclass(frozen=True)
class CostingRunRequest:
    pipeline: str
    input_path: Path
    output_dir: Path
    month_start: str | None = None
    month_end: str | None = None
    product_order: ProductOrder = ()
    benchmark: bool = False
    overwrite_confirmed: bool = False


@dataclass(frozen=True)
class CostingRunResult:
    status: ServiceStatus
    message: str
    workbook_path: Path | None = None
    candidate_products: ProductOrder = ()
    quality_metrics: tuple[QualityMetric, ...] = ()
    error_log_count: int = 0
    anomaly_summary: dict[str, dict[str, int]] | None = None
    stage_timings: dict[str, float] | None = None
    input_size_bytes: int = 0
    output_size_bytes: int = 0
    ingest_backend: str = 'unknown'
    error_code: str | None = None
    retryable: bool = False
    technical_detail: str | None = None


def build_output_workbook_path(
    output_dir: Path,
    input_path: Path,
    *,
    month_start: str | None = None,
    month_end: str | None = None,
) -> Path:
    month_range = build_month_range(month_start, month_end)
    suffix = '' if month_range is None or not month_range.output_suffix() else f'_{month_range.output_suffix()}'
    return output_dir / f'{input_path.stem}_处理后{suffix}.xlsx'
```

- [ ] **Step 5: Implement validation and ETL builder**

Append to `src/services/costing_service.py`:

```python
def _failed(
    *,
    message: str,
    error_code: str,
    workbook_path: Path | None = None,
    technical_detail: str | None = None,
) -> CostingRunResult:
    return CostingRunResult(
        status=ServiceStatus.FAILED,
        message=message,
        workbook_path=workbook_path,
        error_code=error_code,
        retryable=False,
        technical_detail=technical_detail,
    )


def _validate_request(request: CostingRunRequest) -> CostingRunResult | None:
    if request.pipeline not in PIPELINES:
        return _failed(message=f'未知管线: {request.pipeline}', error_code='INVALID_INPUT')
    if not request.input_path.exists():
        return _failed(message=f'输入文件不存在: {request.input_path}', error_code='FILE_NOT_FOUND')
    if request.input_path.suffix.lower() != '.xlsx':
        return _failed(message='输入文件必须是 .xlsx 格式', error_code='UNSUPPORTED_FILE_TYPE')
    if not request.input_path.is_file():
        return _failed(message=f'输入路径不是文件: {request.input_path}', error_code='INVALID_INPUT')
    try:
        with request.input_path.open('rb'):
            pass
    except OSError as exc:
        return _failed(message=f'输入文件不可读: {request.input_path}', error_code='FILE_NOT_READABLE', technical_detail=str(exc))
    duplicate_pairs = {
        pair for pair in request.product_order if request.product_order.count(pair) > 1
    }
    if duplicate_pairs:
        return _failed(message='产品白名单存在重复项', error_code='WHITELIST_INVALID')
    for code, name in request.product_order:
        if not str(code).strip() or not str(name).strip():
            return _failed(message='产品白名单编码和名称不能为空', error_code='WHITELIST_INVALID')
    try:
        build_month_range(request.month_start, request.month_end)
    except ValueError as exc:
        return _failed(message=str(exc), error_code='MONTH_RANGE_INVALID')
    return None


def _build_etl(request: CostingRunRequest, month_range: MonthRange | None) -> CostingWorkbookETL:
    config = PIPELINES[request.pipeline]
    return CostingWorkbookETL(
        skip_rows=2,
        product_order=request.product_order,
        standalone_cost_items=config.standalone_cost_items,
        product_anomaly_scope_mode=config.product_anomaly_scope_mode,
        month_range=month_range,
        ensure_output_directories=False,
    )
```

If ruff rejects tuple `.count()` for tuple of tuples performance, replace duplicate detection with a `seen` set loop:

```python
seen: set[tuple[str, str]] = set()
for pair in request.product_order:
    if pair in seen:
        return _failed(message='产品白名单存在重复项', error_code='WHITELIST_INVALID')
    seen.add(pair)
```

- [ ] **Step 6: Implement precheck and run**

Append:

```python
def precheck_costing_run(request: CostingRunRequest) -> CostingRunResult:
    validation_error = _validate_request(request)
    month_range = build_month_range(request.month_start, request.month_end)
    workbook_path = build_output_workbook_path(
        request.output_dir,
        request.input_path,
        month_start=request.month_start,
        month_end=request.month_end,
    )
    if validation_error is not None:
        return CostingRunResult(
            status=validation_error.status,
            message=validation_error.message,
            workbook_path=validation_error.workbook_path or workbook_path,
            error_code=validation_error.error_code,
            retryable=validation_error.retryable,
            technical_detail=validation_error.technical_detail,
        )
    if workbook_path.exists() and not request.overwrite_confirmed:
        return _failed(
            message=f'输出 workbook 已存在: {workbook_path}',
            error_code='OUTPUT_EXISTS',
            workbook_path=workbook_path,
        )
    try:
        etl = _build_etl(request, month_range)
        if not etl.prepare_payload(request.input_path):
            return _failed(message='预检失败，请查看日志详情', error_code='WORKBOOK_SCHEMA_INVALID', workbook_path=workbook_path)
        return _result_from_etl(
            etl,
            status=ServiceStatus.SUCCEEDED,
            message='预检通过',
            input_path=request.input_path,
            workbook_path=workbook_path,
            output_written=False,
        )
    except Exception as exc:  # noqa: BLE001
        return _failed(
            message='预检失败，请查看日志详情',
            error_code='ETL_FAILED',
            workbook_path=workbook_path,
            technical_detail=str(exc),
        )


def run_costing_request(request: CostingRunRequest) -> CostingRunResult:
    validation_error = _validate_request(request)
    if validation_error is not None:
        return validation_error
    workbook_path = build_output_workbook_path(
        request.output_dir,
        request.input_path,
        month_start=request.month_start,
        month_end=request.month_end,
    )
    if workbook_path.exists() and not request.overwrite_confirmed:
        return _failed(message=f'输出 workbook 已存在: {workbook_path}', error_code='OUTPUT_EXISTS', workbook_path=workbook_path)
    try:
        month_range = build_month_range(request.month_start, request.month_end)
        request.output_dir.mkdir(parents=True, exist_ok=True)
        etl = _build_etl(request, month_range)
        if not etl.process_file(request.input_path, workbook_path):
            return _failed(message='处理失败，请查看日志详情', error_code='ETL_FAILED', workbook_path=workbook_path)
        return _result_from_etl(
            etl,
            status=ServiceStatus.SUCCEEDED,
            message='处理成功',
            input_path=request.input_path,
            workbook_path=workbook_path,
            output_written=True,
        )
    except Exception as exc:  # noqa: BLE001
        return _failed(
            message='处理失败，请查看日志详情',
            error_code='ETL_FAILED',
            workbook_path=workbook_path,
            technical_detail=str(exc),
        )
```

- [ ] **Step 7: Implement result helpers**

Append:

```python
def _result_from_etl(
    etl: CostingWorkbookETL,
    *,
    status: ServiceStatus,
    message: str,
    input_path: Path,
    workbook_path: Path,
    output_written: bool,
) -> CostingRunResult:
    return CostingRunResult(
        status=status,
        message=message,
        workbook_path=workbook_path,
        candidate_products=(),
        quality_metrics=tuple(etl.last_quality_metrics),
        error_log_count=int(etl.last_error_log_count),
        anomaly_summary=_build_anomaly_summary(getattr(etl, 'last_work_order_sheet_frame', pd.DataFrame())),
        stage_timings=dict(getattr(etl, 'last_stage_timings', {})),
        input_size_bytes=input_path.stat().st_size if input_path.exists() else 0,
        output_size_bytes=workbook_path.stat().st_size if output_written and workbook_path.exists() else 0,
        ingest_backend=getattr(etl, 'last_ingest_backend', 'unknown'),
    )


def _value_counts(frame: pd.DataFrame, column_name: str) -> dict[str, int]:
    if frame.empty or column_name not in frame.columns:
        return {}
    series = frame[column_name].fillna('').astype(str).str.strip()
    series = series[series.ne('')]
    return {str(index): int(value) for index, value in series.value_counts(sort=False).items()}


def _build_anomaly_summary(work_order_sheet_frame: pd.DataFrame) -> dict[str, dict[str, int]]:
    return {
        'anomaly_level_counts': _value_counts(work_order_sheet_frame, '异常等级'),
        'anomaly_source_counts': _value_counts(work_order_sheet_frame, '异常主要来源'),
    }
```

- [ ] **Step 8: Run service tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_costing_service.py -q
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src/services/__init__.py src/services/costing_service.py tests/test_costing_service.py
git commit -m "feat(services): add costing run service"
```

---

### Task 5: Refactor CLI Runner to Service and Stop External CSV/JSON Writes

**Files:**
- Modify: `main.py`
- Modify: `src/etl/runner.py`
- Modify: `tests/test_runner.py`
- Modify: `tests/test_main.py`

- [ ] **Step 1: Update runner tests for service delegation and workbook-only output**

In `tests/test_runner.py`, update the existing tests that assert `error_log_csv_path.exists()` or `summary_path.exists()`.

For `test_run_pipeline_prints_quality_summary_without_writing_log_file`, replace the CSV assertions with:

```python
error_log_csv_path = processed_dir / 'SK-成本计算单_处理后_error_log.csv'
summary_path = processed_dir / 'SK-成本计算单_处理后_summary.json'

assert not log_path.exists()
assert not error_log_csv_path.exists()
assert not summary_path.exists()
assert 'pipeline=sk' in stdout
assert '可参与分析占比=100.00%' in stdout
```

For `test_run_pipeline_real_payload_path_keeps_stdout_and_skips_log_file`, replace CSV read assertions with:

```python
assert not error_log_csv_path.exists()
assert not (processed_dir / 'GB-成本计算单_处理后_summary.json').exists()
assert 'error_log_count=2' in stdout
```

For `test_run_pipeline_writes_summary_json_after_success`, rename it to:

```python
def test_run_pipeline_does_not_write_summary_json_after_success(monkeypatch, tmp_path) -> None:
```

Replace final assertions with:

```python
assert run_pipeline(config) == 0
assert not (processed_dir / 'GB-成本计算单_处理后_summary.json').exists()
assert (processed_dir / 'GB-成本计算单_处理后.xlsx').exists()
```

For month suffix tests, replace:

```python
assert (processed_dir / 'GB-成本计算单_处理后_2025-01_2025-03_error_log.csv').exists()
```

with:

```python
assert not (processed_dir / 'GB-成本计算单_处理后_2025-01_2025-03_error_log.csv').exists()
```

Do the same for SK no-row month suffix test.

For tests that currently monkeypatch `src.etl.runner.CostingWorkbookETL`, replace those dummy ETL classes with a monkeypatch of `src.etl.runner.run_costing_request` or `src.etl.runner.precheck_costing_run`. Add this import:

```python
from src.services.costing_service import CostingRunResult, ServiceStatus
```

Use this fake for normal processing tests:

```python
def _fake_run_costing_request(request):
    captured['request'] = request
    request.output_dir.mkdir(parents=True, exist_ok=True)
    workbook_path = request.output_dir / f'{request.input_path.stem}_处理后.xlsx'
    workbook_path.write_text('ok', encoding='utf-8')
    return CostingRunResult(
        status=ServiceStatus.SUCCEEDED,
        message='处理成功',
        workbook_path=workbook_path,
        quality_metrics=(
            QualityMetric('行数勾稽', '产品数量统计输出行数', '1', '仅保留有效工单'),
        ),
        error_log_count=0,
        stage_timings={'ingest': 0.1, 'export': 0.2},
        input_size_bytes=3,
        output_size_bytes=2,
        ingest_backend='calamine',
    )
```

```python
monkeypatch.setattr('src.etl.runner.run_costing_request', _fake_run_costing_request)
```

Use this fake for check-only tests:

```python
def _fake_precheck_costing_run(request):
    captured['request'] = request
    return CostingRunResult(
        status=ServiceStatus.SUCCEEDED,
        message='预检通过',
        workbook_path=request.output_dir / f'{request.input_path.stem}_处理后.xlsx',
        quality_metrics=(QualityMetric('行数勾稽', '产品数量统计输出行数', '3', '仅保留有效工单'),),
        error_log_count=2,
        stage_timings={'ingest': 0.1, 'normalize': 0.2},
        input_size_bytes=3,
        output_size_bytes=0,
        ingest_backend='calamine',
    )
```

```python
monkeypatch.setattr('src.etl.runner.precheck_costing_run', _fake_precheck_costing_run)
```

- [ ] **Step 2: Run failing runner tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_runner.py -q
```

Expected: FAIL because current runner still builds `CostingWorkbookETL` directly and writes CSV/JSON.

- [ ] **Step 3: Refactor `src/etl/runner.py` to build service requests**

In `src/etl/runner.py`, keep `find_input_files()`, `build_quality_log_text()`, and `build_benchmark_log_text()`. Remove imports and calls for:

```python
write_error_log_csv(...)
build_summary_payload(...)
write_summary_json(...)
CostingWorkbookETL
```

Add imports:

```python
from src.config.product_whitelist_store import ProductWhitelistConfigError, load_product_order_for_pipeline
from src.services.costing_service import (
    CostingRunRequest,
    CostingRunResult,
    ServiceStatus,
    precheck_costing_run,
    run_costing_request,
)
```

Add a request helper:

```python
def _build_request(
    *,
    config: PipelineConfig,
    input_file: Path,
    month_range: MonthRange | None,
    overwrite_confirmed: bool,
    benchmark: bool,
) -> CostingRunRequest:
    product_order = load_product_order_for_pipeline(config.name)
    return CostingRunRequest(
        pipeline=config.name,
        input_path=input_file,
        output_dir=config.processed_dir,
        month_start=None if month_range is None else month_range.start,
        month_end=None if month_range is None else month_range.end,
        product_order=product_order,
        benchmark=benchmark,
        overwrite_confirmed=overwrite_confirmed,
    )
```

- [ ] **Step 4: Add result printing helper**

Add:

```python
def _print_run_result(
    *,
    config: PipelineConfig,
    input_file: Path,
    result: CostingRunResult,
    benchmark: bool,
    output_written: bool,
) -> None:
    output_path = result.workbook_path or config.processed_dir / f'{input_file.stem}_处理后.xlsx'
    print(
        build_quality_log_text(
            pipeline_name=config.name,
            input_path=input_file,
            output_path=output_path,
            error_log_count=result.error_log_count,
            quality_metrics=result.quality_metrics,
            month_filter_summary=None,
        )
    )
    if benchmark:
        print(
            build_benchmark_log_text(
                input_path=input_file,
                output_path=output_path,
                error_log_path=config.processed_dir / f'{input_file.stem}_处理后_error_log.csv',
                error_log_count=result.error_log_count,
                stage_timings=result.stage_timings or {},
                ingest_backend=result.ingest_backend,
                output_written=output_written,
            )
        )
```

Update `build_benchmark_log_text()` so external error log size is always zero:

```python
f'error_log_size_bytes=0',
```

- [ ] **Step 5: Rewrite `run_pipeline()` to call service**

Replace the ETL construction branches in `run_pipeline()` with:

```python
try:
    request = _build_request(
        config=config,
        input_file=input_file,
        month_range=month_range,
        overwrite_confirmed=True,
        benchmark=benchmark,
    )
except ProductWhitelistConfigError as exc:
    logger.error('产品白名单配置错误: %s', exc)
    return 1

if check_only:
    result = precheck_costing_run(request)
    if result.status != ServiceStatus.SUCCEEDED:
        logger.error('预检失败: %s', result.message)
        return 1
    print('mode=check-only')
    _print_run_result(
        config=config,
        input_file=input_file,
        result=result,
        benchmark=benchmark,
        output_written=False,
    )
    logger.info('预检成功: %s', input_file.name)
    return 0

result = run_costing_request(request)
if result.status != ServiceStatus.SUCCEEDED:
    logger.error('处理失败: %s', result.message)
    return 1

_print_run_result(
    config=config,
    input_file=input_file,
    result=result,
    benchmark=benchmark,
    output_written=True,
)
logger.info('处理成功: %s', result.workbook_path)
return 0
```

- [ ] **Step 6: Run runner and main tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_runner.py tests/test_main.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/etl/runner.py main.py tests/test_runner.py tests/test_main.py
git commit -m "feat(cli): write only workbook artifact"
```

---

### Task 6: Add Candidate Product Scan to Service

**Files:**
- Modify: `src/etl/pipeline.py`
- Modify: `src/etl/costing_etl.py`
- Modify: `src/services/costing_service.py`
- Modify: `tests/test_costing_service.py`

- [ ] **Step 1: Add failing candidate scan test**

Append to `tests/test_costing_service.py`:

```python
def test_precheck_returns_candidate_products_from_normalized_payload(monkeypatch, tmp_path: Path) -> None:
    request = _request(tmp_path)

    captured = {}

    class _DummyPipeline:
        last_month_filter_summary = None
        last_ingest_backend = 'calamine'
        last_candidate_products = ()

        def build_workbook_payload(self, *args, **kwargs):
            self.last_candidate_products = (
                ('GB_C.D.B0040AA', 'BMS-750W驱动器'),
                ('GB_C.D.B9999AA', '新产品'),
            )
            from src.analytics.contracts import WorkbookPayload

            return WorkbookPayload(
                sheet_models=(),
                quality_metrics=(),
                error_log_count=0,
                stage_timings={},
                error_log_export=pd.DataFrame(),
                work_order_sheet_export=pd.DataFrame(),
            )

    class _DummyETL:
        def __init__(self, *args, **kwargs) -> None:
            self.pipeline = _DummyPipeline()
            self.last_quality_metrics = ()
            self.last_error_log_count = 0
            self.last_error_log_frame = pd.DataFrame()
            self.last_work_order_sheet_frame = pd.DataFrame()
            self.last_month_filter_summary = None
            self.last_stage_timings = {}
            self.last_ingest_backend = 'calamine'
            self.last_candidate_products = ()

        def _apply_payload_state(self, payload) -> None:
            self.last_candidate_products = self.pipeline.last_candidate_products

        def prepare_payload(self, input_path: Path) -> bool:
            payload = self.pipeline.build_workbook_payload(input_path)
            self._apply_payload_state(payload)
            captured['input_path'] = input_path
            return True

    monkeypatch.setattr('src.services.costing_service.CostingWorkbookETL', _DummyETL)

    result = precheck_costing_run(request)

    assert result.status == ServiceStatus.SUCCEEDED
    assert result.candidate_products == (
        ('GB_C.D.B0040AA', 'BMS-750W驱动器'),
        ('GB_C.D.B9999AA', '新产品'),
    )
```

- [ ] **Step 2: Run failing test**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_costing_service.py::test_precheck_returns_candidate_products_from_normalized_payload -q
```

Expected: FAIL because `CostingRunResult.candidate_products` is empty.

- [ ] **Step 3: Add candidate product storage to `CostingEtlPipeline`**

In `src/etl/pipeline.py`, initialize in `CostingEtlPipeline.__init__()`:

```python
self.last_candidate_products: tuple[tuple[str, str], ...] = ()
```

Add this helper near `_to_error_log_export_frame()`:

```python
def _extract_candidate_products_from_normalized(frame: pl.DataFrame) -> tuple[tuple[str, str], ...]:
    required = {'产品编码', '产品名称'}
    if frame.is_empty() or not required.issubset(frame.columns):
        return ()
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in frame.select(['产品编码', '产品名称']).unique(maintain_order=True).iter_rows():
        pair = (str(row[0]).strip(), str(row[1]).strip())
        if not pair[0] or not pair[1] or pair in seen:
            continue
        seen.add(pair)
        pairs.append(pair)
    return tuple(pairs)
```

At the start of `build_workbook_payload()`, reset:

```python
self.last_candidate_products = ()
```

After `apply_month_range_to_normalized_frame(...)` and before `split_normalized_frames(...)`, set:

```python
self.last_candidate_products = _extract_candidate_products_from_normalized(normalized_frame.frame)
```

This scans products from the current input workbook after month filtering and before product whitelist filtering, so newly discovered products can be added to the GUI whitelist pool.

- [ ] **Step 4: Copy pipeline candidates to `CostingWorkbookETL` state**

In `src/etl/costing_etl.py`, initialize in `_reset_last_run_state()`:

```python
self.last_candidate_products: tuple[tuple[str, str], ...] = ()
```

In `_apply_payload_state()`, set:

```python
self.last_candidate_products = tuple(getattr(self.pipeline, 'last_candidate_products', ()))
```

- [ ] **Step 5: Return candidates from service**

In `_result_from_etl()` in `src/services/costing_service.py`, change:

```python
candidate_products=tuple(getattr(etl, 'last_candidate_products', ())),
```

- [ ] **Step 6: Run service tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_costing_service.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/etl/pipeline.py src/etl/costing_etl.py src/services/costing_service.py tests/test_costing_service.py
git commit -m "feat(services): expose candidate products"
```

---

### Task 7: Add GUI Form State and Validators

**Files:**
- Create: `src/gui/__init__.py`
- Create: `src/gui/form_state.py`
- Create: `src/gui/validators.py`
- Create: `tests/test_gui_form_state.py`

- [ ] **Step 1: Write failing form-state tests**

Create `tests/test_gui_form_state.py`:

```python
from __future__ import annotations

from pathlib import Path

from src.gui.form_state import GuiFormState
from src.gui.validators import can_start_processing, validate_month_text


def test_form_state_builds_service_request(tmp_path: Path) -> None:
    input_path = tmp_path / 'GB-成本计算单.xlsx'
    input_path.write_bytes(b'raw')
    state = GuiFormState(
        pipeline='gb',
        input_path=input_path,
        output_dir=tmp_path / 'processed',
        month_start='2025-01',
        month_end='2025-03',
        product_order=(('GB_C.D.B0040AA', 'BMS-750W驱动器'),),
        overwrite_confirmed=True,
    )

    request = state.to_request()

    assert request.pipeline == 'gb'
    assert request.input_path == input_path
    assert request.output_dir == tmp_path / 'processed'
    assert request.month_start == '2025-01'
    assert request.month_end == '2025-03'
    assert request.product_order == (('GB_C.D.B0040AA', 'BMS-750W驱动器'),)


def test_can_start_processing_requires_input_and_successful_precheck(tmp_path: Path) -> None:
    state = GuiFormState(pipeline='gb', input_path=None, output_dir=tmp_path, product_order=())

    assert can_start_processing(state, precheck_passed=False, busy=False) is False

    state = GuiFormState(
        pipeline='gb',
        input_path=tmp_path / 'GB-成本计算单.xlsx',
        output_dir=tmp_path,
        product_order=(('P001', '产品A'),),
    )
    assert can_start_processing(state, precheck_passed=True, busy=False) is True
    assert can_start_processing(state, precheck_passed=True, busy=True) is False


def test_validate_month_text_accepts_blank_and_yyyy_mm() -> None:
    assert validate_month_text('') is None
    assert validate_month_text('2025-01') is None
    assert validate_month_text('2025/01') == '月份必须是 YYYY-MM 格式'
```

- [ ] **Step 2: Run failing tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_gui_form_state.py -q
```

Expected: FAIL because `src.gui.form_state` and `src.gui.validators` do not exist.

- [ ] **Step 3: Create GUI package marker**

Create `src/gui/__init__.py`:

```python
"""PySide6 GUI package for the costing analysis tool."""
```

- [ ] **Step 4: Implement form state**

Create `src/gui/form_state.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.config.pipelines import ProductOrder
from src.services.costing_service import CostingRunRequest


@dataclass(frozen=True)
class GuiFormState:
    pipeline: str
    input_path: Path | None
    output_dir: Path
    product_order: ProductOrder
    month_start: str | None = None
    month_end: str | None = None
    overwrite_confirmed: bool = False
    benchmark: bool = True

    def to_request(self) -> CostingRunRequest:
        if self.input_path is None:
            raise ValueError('缺少输入文件')
        return CostingRunRequest(
            pipeline=self.pipeline,
            input_path=self.input_path,
            output_dir=self.output_dir,
            month_start=_blank_to_none(self.month_start),
            month_end=_blank_to_none(self.month_end),
            product_order=self.product_order,
            benchmark=self.benchmark,
            overwrite_confirmed=self.overwrite_confirmed,
        )


def _blank_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None
```

- [ ] **Step 5: Implement validators**

Create `src/gui/validators.py`:

```python
from __future__ import annotations

from src.etl.month_filter import build_month_range
from src.gui.form_state import GuiFormState


def validate_month_text(value: str) -> str | None:
    stripped = value.strip()
    if not stripped:
        return None
    try:
        build_month_range(stripped, None)
    except ValueError:
        return '月份必须是 YYYY-MM 格式'
    return None


def can_start_processing(state: GuiFormState, *, precheck_passed: bool, busy: bool) -> bool:
    return state.input_path is not None and precheck_passed and not busy
```

- [ ] **Step 6: Run GUI state tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_gui_form_state.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/gui/__init__.py src/gui/form_state.py src/gui/validators.py tests/test_gui_form_state.py
git commit -m "feat(gui): add form state helpers"
```

---

### Task 8: Add PySide6 GUI Entry, Worker, and Main Window

**Files:**
- Create: `src/gui/app.py`
- Create: `src/gui/task_worker.py`
- Create: `src/gui/styles.py`
- Create: `src/gui/main_window.py`
- Modify: `pyproject.toml`

- [ ] **Step 1: Add GUI extra**

Modify `pyproject.toml`:

```toml
[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "ruff>=0.8.0",
]
gui = [
    "PySide6>=6.8",
]
```

- [ ] **Step 2: Install GUI extra**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pip install -e '.[dev,gui]'
```

Expected: exits 0 and installs PySide6 in the `test` environment.

- [ ] **Step 3: Create worker**

Create `src/gui/task_worker.py`:

```python
from __future__ import annotations

from collections.abc import Callable

from PySide6.QtCore import QObject, QRunnable, Signal, Slot

from src.services.costing_service import CostingRunRequest, CostingRunResult


class WorkerSignals(QObject):
    started = Signal(str)
    finished = Signal(object)
    failed = Signal(str)


class ServiceWorker(QRunnable):
    def __init__(
        self,
        label: str,
        request: CostingRunRequest,
        function: Callable[[CostingRunRequest], CostingRunResult],
    ) -> None:
        super().__init__()
        self.label = label
        self.request = request
        self.function = function
        self.signals = WorkerSignals()

    @Slot()
    def run(self) -> None:
        self.signals.started.emit(self.label)
        try:
            result = self.function(self.request)
        except Exception as exc:  # noqa: BLE001
            self.signals.failed.emit(str(exc))
            return
        self.signals.finished.emit(result)
```

- [ ] **Step 4: Create styles**

Create `src/gui/styles.py`:

```python
from __future__ import annotations


APP_STYLESHEET = """
QMainWindow {
    background: #f4f6f8;
}
QLabel#TitleLabel {
    font-size: 22px;
    font-weight: 700;
    color: #1f2933;
}
QLabel#SubtitleLabel {
    color: #52606d;
}
QGroupBox {
    border: 1px solid #d9e2ec;
    border-radius: 6px;
    margin-top: 10px;
    background: #ffffff;
}
QGroupBox::title {
    subcontrol-origin: margin;
    left: 10px;
    padding: 0 4px;
    color: #334e68;
}
QPushButton {
    min-height: 30px;
    padding: 4px 10px;
}
QPushButton#PrimaryButton {
    background: #2563eb;
    color: #ffffff;
    border: 0;
    border-radius: 4px;
}
QTextEdit {
    background: #0f172a;
    color: #e2e8f0;
    font-family: monospace;
}
"""
```

- [ ] **Step 5: Create main window**

Create `src/gui/main_window.py` with this complete first version:

```python
from __future__ import annotations

import subprocess
from pathlib import Path

from PySide6.QtCore import QThreadPool
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QComboBox,
    QFileDialog,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from src.config.pipelines import PIPELINES
from src.config.product_whitelist_store import ProductWhitelistConfigError, ProductWhitelistStore
from src.config.settings import PROJECT_ROOT
from src.etl.runner import find_input_files
from src.gui.form_state import GuiFormState
from src.gui.styles import APP_STYLESHEET
from src.gui.task_worker import ServiceWorker
from src.services.costing_service import CostingRunResult, ServiceStatus, precheck_costing_run, run_costing_request


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle('成本核算分析工具')
        self.setMinimumSize(1180, 760)
        self.setStyleSheet(APP_STYLESHEET)
        self.thread_pool = QThreadPool.globalInstance()
        self.whitelist_store = ProductWhitelistStore()
        self.precheck_passed = False
        self.busy = False
        self.last_output_dir: Path | None = None

        self.pipeline_combo = QComboBox()
        self.pipeline_combo.addItems(['gb', 'sk'])
        self.input_edit = QLineEdit()
        self.output_edit = QLineEdit()
        self.month_start_edit = QLineEdit()
        self.month_end_edit = QLineEdit()
        self.status_label = QLabel('等待配置')
        self.stage_label = QLabel('-')
        self.summary_label = QLabel('尚未运行')
        self.log_edit = QTextEdit()
        self.log_edit.setReadOnly(True)
        self.whitelist_table = QTableWidget(0, 2)
        self.candidate_table = QTableWidget(0, 2)

        self.scan_button = QPushButton('扫描产品')
        self.precheck_button = QPushButton('预检')
        self.run_button = QPushButton('开始处理')
        self.run_button.setObjectName('PrimaryButton')
        self.open_output_button = QPushButton('打开输出目录')
        self.clear_button = QPushButton('清空条件')
        self.exit_button = QPushButton('退出')

        self._build_ui()
        self._connect_signals()
        self._load_pipeline_defaults()
        self._set_busy(False)

    def _build_ui(self) -> None:
        title = QLabel('成本核算分析工具')
        title.setObjectName('TitleLabel')
        subtitle = QLabel('金蝶 ERP 成本计算单处理')
        subtitle.setObjectName('SubtitleLabel')

        config_group = QGroupBox('输入配置')
        config_layout = QFormLayout(config_group)
        config_layout.addRow('管线', self.pipeline_combo)
        config_layout.addRow('输入文件', self._path_row(self.input_edit, '选择文件', self._choose_input_file, '自动查找', self._auto_find_input))
        config_layout.addRow('输出目录', self._path_row(self.output_edit, '选择目录', self._choose_output_dir))
        config_layout.addRow('开始月份', self.month_start_edit)
        config_layout.addRow('结束月份', self.month_end_edit)

        whitelist_group = QGroupBox('产品白名单池')
        whitelist_layout = QVBoxLayout(whitelist_group)
        self._setup_table(self.whitelist_table)
        whitelist_layout.addWidget(self.whitelist_table)
        whitelist_layout.addLayout(self._whitelist_buttons())

        candidate_group = QGroupBox('候选产品')
        candidate_layout = QVBoxLayout(candidate_group)
        self._setup_table(self.candidate_table)
        candidate_layout.addWidget(self.candidate_table)
        add_candidate_button = QPushButton('加入白名单')
        add_candidate_button.clicked.connect(self._add_selected_candidates)
        candidate_layout.addWidget(add_candidate_button)

        left = QVBoxLayout()
        left.addWidget(title)
        left.addWidget(subtitle)
        left.addWidget(config_group)
        left.addWidget(whitelist_group)
        left.addWidget(candidate_group)

        status_group = QGroupBox('任务状态')
        status_layout = QFormLayout(status_group)
        status_layout.addRow('当前状态', self.status_label)
        status_layout.addRow('当前阶段', self.stage_label)
        status_layout.addRow('结果摘要', self.summary_label)

        log_group = QGroupBox('日志')
        log_layout = QVBoxLayout(log_group)
        log_layout.addWidget(self.log_edit)

        button_layout = QHBoxLayout()
        for button in (self.scan_button, self.precheck_button, self.run_button, self.open_output_button, self.clear_button, self.exit_button):
            button_layout.addWidget(button)

        right = QVBoxLayout()
        right.addWidget(status_group)
        right.addWidget(log_group)
        right.addLayout(button_layout)

        root_layout = QGridLayout()
        root_layout.addLayout(left, 0, 0)
        root_layout.addLayout(right, 0, 1)
        root_layout.setColumnStretch(0, 1)
        root_layout.setColumnStretch(1, 2)

        root = QWidget()
        root.setLayout(root_layout)
        self.setCentralWidget(root)

    def _path_row(self, edit: QLineEdit, text: str, slot, second_text: str | None = None, second_slot=None) -> QWidget:
        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(edit)
        button = QPushButton(text)
        button.clicked.connect(slot)
        layout.addWidget(button)
        if second_text and second_slot:
            second = QPushButton(second_text)
            second.clicked.connect(second_slot)
            layout.addWidget(second)
        return widget

    def _setup_table(self, table: QTableWidget) -> None:
        table.setHorizontalHeaderLabels(['产品编码', '产品名称'])
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)

    def _whitelist_buttons(self) -> QHBoxLayout:
        layout = QHBoxLayout()
        actions = [
            ('新增', self._add_blank_whitelist_row),
            ('删除', self._delete_selected_whitelist_rows),
            ('上移', lambda: self._move_selected_whitelist_row(-1)),
            ('下移', lambda: self._move_selected_whitelist_row(1)),
            ('保存', self._save_whitelist),
            ('恢复默认', self._restore_default_whitelist),
        ]
        for text, slot in actions:
            button = QPushButton(text)
            button.clicked.connect(slot)
            layout.addWidget(button)
        return layout

    def _connect_signals(self) -> None:
        self.pipeline_combo.currentTextChanged.connect(self._load_pipeline_defaults)
        self.scan_button.clicked.connect(self._scan_products)
        self.precheck_button.clicked.connect(self._precheck)
        self.run_button.clicked.connect(self._run)
        self.open_output_button.clicked.connect(self._open_output_dir)
        self.clear_button.clicked.connect(self._clear_conditions)
        self.exit_button.clicked.connect(QApplication.instance().quit)

    def _load_pipeline_defaults(self) -> None:
        pipeline = self.pipeline_combo.currentText()
        self.output_edit.setText(str(PIPELINES[pipeline].processed_dir))
        try:
            orders = self.whitelist_store.load().product_orders[pipeline]
        except ProductWhitelistConfigError as exc:
            self._append_log(f'产品白名单配置错误: {exc}')
            orders = PIPELINES[pipeline].product_order
        self._set_table_pairs(self.whitelist_table, orders)
        self.precheck_passed = False

    def _choose_input_file(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, '选择成本计算单', str(PROJECT_ROOT), 'Excel Workbook (*.xlsx)')
        if path:
            self.input_edit.setText(path)
            self.precheck_passed = False
            self._set_busy(False)

    def _choose_output_dir(self) -> None:
        path = QFileDialog.getExistingDirectory(self, '选择输出目录', self.output_edit.text())
        if path:
            self.output_edit.setText(path)

    def _auto_find_input(self) -> None:
        config = PIPELINES[self.pipeline_combo.currentText()]
        files = find_input_files(config)
        if not files:
            self._append_log(f'未在 {config.raw_dir} 找到输入文件')
            return
        self.input_edit.setText(str(files[0]))
        self._append_log(f'自动查找到输入文件: {files[0]}')
        self.precheck_passed = False
        self._set_busy(False)

    def _state(self, *, overwrite_confirmed: bool = False) -> GuiFormState:
        input_text = self.input_edit.text().strip()
        return GuiFormState(
            pipeline=self.pipeline_combo.currentText(),
            input_path=Path(input_text) if input_text else None,
            output_dir=Path(self.output_edit.text().strip()),
            month_start=self.month_start_edit.text().strip() or None,
            month_end=self.month_end_edit.text().strip() or None,
            product_order=self._table_pairs(self.whitelist_table),
            overwrite_confirmed=overwrite_confirmed,
        )

    def _scan_products(self) -> None:
        self._start_worker('正在扫描产品', precheck_costing_run, overwrite_confirmed=True, scan_only=True)

    def _precheck(self) -> None:
        self._start_worker('正在预检', precheck_costing_run, overwrite_confirmed=False, scan_only=False)

    def _run(self) -> None:
        self._start_worker('正在处理', run_costing_request, overwrite_confirmed=True, scan_only=False)

    def _start_worker(self, label: str, function, *, overwrite_confirmed: bool, scan_only: bool) -> None:
        try:
            request = self._state(overwrite_confirmed=overwrite_confirmed).to_request()
        except ValueError as exc:
            self._append_log(str(exc))
            return
        self._set_busy(True)
        worker = ServiceWorker(label, request, function)
        worker.signals.started.connect(self._on_worker_started)
        worker.signals.finished.connect(lambda result: self._on_worker_finished(result, scan_only=scan_only))
        worker.signals.failed.connect(self._on_worker_failed)
        self.thread_pool.start(worker)

    def _on_worker_started(self, label: str) -> None:
        self.status_label.setText(label)
        self.stage_label.setText('-')
        self._append_log(label)

    def _on_worker_finished(self, result: CostingRunResult, *, scan_only: bool) -> None:
        self.status_label.setText(result.message)
        if result.stage_timings:
            self.stage_label.setText(', '.join(f'{key}={value:.3f}s' for key, value in result.stage_timings.items()))
        if result.candidate_products:
            self._set_table_pairs(self.candidate_table, result.candidate_products)
        self.precheck_passed = result.status == ServiceStatus.SUCCEEDED and not scan_only
        self._set_busy(False)
        if result.workbook_path:
            self.last_output_dir = result.workbook_path.parent
        self.summary_label.setText(f'error_log 行数: {result.error_log_count} | 输出: {result.workbook_path or "-"}')
        if result.error_code == 'OUTPUT_EXISTS':
            reply = QMessageBox.question(self, '覆盖确认', result.message)
            if reply == QMessageBox.StandardButton.Yes:
                self._start_worker('正在处理', run_costing_request, overwrite_confirmed=True, scan_only=False)
        self._append_log(result.message)
        if result.technical_detail:
            self._append_log(result.technical_detail)

    def _on_worker_failed(self, message: str) -> None:
        self._set_busy(False)
        self.precheck_passed = False
        self.status_label.setText('处理失败')
        self._append_log(message)

    def _set_busy(self, busy: bool) -> None:
        self.busy = busy
        has_input = bool(self.input_edit.text().strip())
        self.scan_button.setEnabled(not busy and has_input)
        self.precheck_button.setEnabled(not busy and has_input)
        self.run_button.setEnabled(not busy and has_input and self.precheck_passed)
        self.clear_button.setEnabled(not busy)
        self.open_output_button.setEnabled(not busy)

    def _append_log(self, text: str) -> None:
        self.log_edit.append(text)

    def _set_table_pairs(self, table: QTableWidget, pairs: tuple[tuple[str, str], ...]) -> None:
        table.setRowCount(0)
        for code, name in pairs:
            row = table.rowCount()
            table.insertRow(row)
            table.setItem(row, 0, QTableWidgetItem(code))
            table.setItem(row, 1, QTableWidgetItem(name))

    def _table_pairs(self, table: QTableWidget) -> tuple[tuple[str, str], ...]:
        pairs: list[tuple[str, str]] = []
        for row in range(table.rowCount()):
            code_item = table.item(row, 0)
            name_item = table.item(row, 1)
            code = '' if code_item is None else code_item.text().strip()
            name = '' if name_item is None else name_item.text().strip()
            if code or name:
                pairs.append((code, name))
        return tuple(pairs)

    def _add_blank_whitelist_row(self) -> None:
        row = self.whitelist_table.rowCount()
        self.whitelist_table.insertRow(row)
        self.whitelist_table.setItem(row, 0, QTableWidgetItem(''))
        self.whitelist_table.setItem(row, 1, QTableWidgetItem(''))

    def _delete_selected_whitelist_rows(self) -> None:
        for row in sorted({index.row() for index in self.whitelist_table.selectedIndexes()}, reverse=True):
            self.whitelist_table.removeRow(row)

    def _move_selected_whitelist_row(self, delta: int) -> None:
        selected_rows = sorted({index.row() for index in self.whitelist_table.selectedIndexes()})
        if len(selected_rows) != 1:
            return
        row = selected_rows[0]
        target = row + delta
        if target < 0 or target >= self.whitelist_table.rowCount():
            return
        pairs = list(self._table_pairs(self.whitelist_table))
        pairs[row], pairs[target] = pairs[target], pairs[row]
        self._set_table_pairs(self.whitelist_table, tuple(pairs))
        self.whitelist_table.selectRow(target)

    def _add_selected_candidates(self) -> None:
        existing = set(self._table_pairs(self.whitelist_table))
        for row in sorted({index.row() for index in self.candidate_table.selectedIndexes()}):
            code = self.candidate_table.item(row, 0).text().strip()
            name = self.candidate_table.item(row, 1).text().strip()
            if (code, name) not in existing:
                target = self.whitelist_table.rowCount()
                self.whitelist_table.insertRow(target)
                self.whitelist_table.setItem(target, 0, QTableWidgetItem(code))
                self.whitelist_table.setItem(target, 1, QTableWidgetItem(name))
                existing.add((code, name))

    def _save_whitelist(self) -> None:
        current = self.whitelist_store.load().product_orders
        current[self.pipeline_combo.currentText()] = self._table_pairs(self.whitelist_table)
        try:
            self.whitelist_store.save(current)
        except ProductWhitelistConfigError as exc:
            self._append_log(f'保存失败: {exc}')
            return
        self._append_log('产品白名单已保存')

    def _restore_default_whitelist(self) -> None:
        pipeline = self.pipeline_combo.currentText()
        reply = QMessageBox.question(self, '恢复默认', f'确认恢复 {pipeline.upper()} 默认白名单？')
        if reply != QMessageBox.StandardButton.Yes:
            return
        self.whitelist_store.restore_default(pipeline)
        self._load_pipeline_defaults()

    def _clear_conditions(self) -> None:
        self.month_start_edit.clear()
        self.month_end_edit.clear()
        self.candidate_table.setRowCount(0)
        self.precheck_passed = False

    def _open_output_dir(self) -> None:
        path = self.last_output_dir or Path(self.output_edit.text().strip())
        subprocess.Popen(['xdg-open', str(path)])
```

- [ ] **Step 6: Create GUI app entry**

Create `src/gui/app.py`:

```python
from __future__ import annotations

import sys

from PySide6.QtWidgets import QApplication

from src.gui.main_window import MainWindow


def main() -> int:
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    return app.exec()


if __name__ == '__main__':
    raise SystemExit(main())
```

- [ ] **Step 7: Smoke import without opening a window**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -c "from src.gui.app import main; from src.gui.main_window import MainWindow; print('gui import ok')"
```

Expected: prints `gui import ok`.

- [ ] **Step 8: Manually launch GUI**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m src.gui.app
```

Expected: the main window opens. Confirm the window can be closed. If this command blocks normally because the GUI is open, close the GUI before continuing.

- [ ] **Step 9: Commit**

```bash
git add pyproject.toml src/gui/app.py src/gui/task_worker.py src/gui/styles.py src/gui/main_window.py
git commit -m "feat(gui): add PySide6 costing desktop app"
```

---

### Task 9: Update Documentation and Project Guidance

**Files:**
- Modify: `README.md`
- Modify: `AGENTS.md`

- [ ] **Step 1: Update README output section**

In `README.md`, replace the old seven-sheet output list with:

```markdown
每个处理后的工作簿默认输出以下 4 张 Sheet：
- `成本计算单总表`
- `成本计算单数量聚合维度`
- `成本分析工单维度`
- `成本分析产品维度`
```

Replace external artifact bullets with:

```markdown
- 每次处理只在对应 `data/processed/<pipeline>/` 目录生成 `*_处理后.xlsx`
- 不再额外落盘 `*_处理后_error_log.csv` 或 `*_处理后_summary.json`
- 质量摘要、error_log 行数和阶段耗时仅输出到控制台或 GUI 状态区
- `--check-only` 只做预检与摘要，不写 workbook 或任何外部摘要文件
```

Add GUI usage:

```markdown
## GUI 使用

运行命令：`conda run -n test python -m src.gui.app`

GUI 支持选择 GB/SK 管线、选择输入文件、自动查找、配置月份范围、维护产品白名单池、预检和后台处理。产品白名单池按 `产品编码 + 产品名称` 精确匹配，只影响分析维度 Sheet，不过滤总表和数量聚合维度。
```

- [ ] **Step 2: Update AGENTS business rules**

In `AGENTS.md`, replace the old output rules with:

```markdown
- 每个处理后的工作簿默认输出以下 4 张 Sheet：`成本计算单总表`、`成本计算单数量聚合维度`、`成本分析工单维度`、`成本分析产品维度`。
- 每次处理只落盘 `*_处理后.xlsx`，不再额外生成 `*_处理后_error_log.csv` 或 `*_处理后_summary.json`。
- 质量校验结果默认输出到控制台或 GUI 状态区；`--check-only` 只做预检与摘要，不写 workbook 或任何外部摘要文件。
- 产品白名单池按 `产品编码 + 产品名称` 双字段精确匹配，影响分析维度 Sheet，不过滤 `成本计算单总表` 和 `成本计算单数量聚合维度`。
```

Update references:

- `成本明细` -> `成本计算单总表`
- `产品数量统计` -> `成本计算单数量聚合维度`
- `按工单按产品异常值分析` -> `成本分析工单维度`
- `按产品异常值分析` -> `成本分析产品维度`

- [ ] **Step 3: Run docs grep**

Run:

```bash
rg -n "直接材料_价量比|直接人工_价量比|制造费用_价量比|_error_log\\.csv|_summary\\.json|成本明细|产品数量统计|按工单按产品异常值分析|按产品异常值分析" README.md AGENTS.md
```

Expected: only historical or explanatory references remain. For active business rules, use the new four sheet names and workbook-only artifact language.

- [ ] **Step 4: Commit**

```bash
git add README.md AGENTS.md
git commit -m "docs: update costing GUI output contract"
```

---

### Task 10: Full Verification and Cleanup

**Files:**
- Verify all changed files

- [ ] **Step 1: Run full pytest**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest -q
```

Expected: PASS.

- [ ] **Step 2: Run ruff check**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m ruff check src tests
```

Expected: PASS. If ruff reports unused imports introduced by earlier tasks, remove those imports and rerun.

- [ ] **Step 3: Run ruff format check**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m ruff format src tests --check
```

Expected: PASS. If format check fails only on files modified by this plan, run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m ruff format src tests
```

Then rerun the format check.

- [ ] **Step 4: Run CLI smoke checks**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python main.py gb --check-only --benchmark
/home/george/miniconda3/bin/conda run -n test python main.py sk --check-only --benchmark
```

Expected: both commands exit 0 if matching input files exist under `data/raw/{gb,sk}`. If a raw workbook is absent, the corresponding command may exit 1 with a clear `No GB/SK costing file found` message; record that exact reason in the final verification note.

- [ ] **Step 5: Run GUI import smoke**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -c "from src.gui.main_window import MainWindow; print('gui import ok')"
```

Expected: prints `gui import ok`.

- [ ] **Step 6: Check git diff**

Run:

```bash
git status --short
git diff --check
```

Expected: working tree contains only intentional changes from this implementation, and `git diff --check` prints nothing.

- [ ] **Step 7: Final commit**

When verification fixes leave tracked changes after previous commits, commit them:

```bash
git add src tests README.md AGENTS.md pyproject.toml
git commit -m "chore: finish costing GUI verification"
```

If `git status --short` is clean after prior commits, skip this commit.

---

## Implementation Notes

- Keep all file reads/writes UTF-8.
- Do not add PyInstaller packaging in this implementation.
- Do not introduce wildcard product matching; product whitelist matching remains exact `产品编码 + 产品名称`.
- Do not filter `成本计算单总表` or `成本计算单数量聚合维度` by whitelist.
- Do not write external `error_log.csv` or `summary.json`.
- Do not remove the in-memory error log contract; quality metrics and GUI summaries still depend on runtime error counts.
- Preserve CLI usage: `python main.py gb`, `python main.py sk`, `--check-only`, `--benchmark`, `--month-start`, and `--month-end`.

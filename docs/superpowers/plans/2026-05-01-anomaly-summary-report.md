# Anomaly Summary Report Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generate a lightweight `*_summary.json` for normal runs, summarizing quality metrics, error issue counts, and anomaly counts without changing workbook sheets.

**Architecture:** Store the last work-order anomaly sheet in `CostingWorkbookETL` state, build a pure-Python summary payload in a new analytics module, and let `runner` write the summary after workbook/CSV export succeeds. Check-only mode does not write the summary file; it can still print quality and benchmark text.

**Tech Stack:** Python 3.11, pandas, JSON, pytest, existing `QualityMetric` and `FlatSheet` contracts.

---

## File Map

- Create: `src/analytics/summary.py`
  - Build serializable summary payload.
  - Write UTF-8 JSON.
- Modify: `src/etl/costing_etl.py`
  - Store `last_work_order_sheet_frame` from payload/artifacts transform path.
- Modify: `src/etl/pipeline.py`
  - Add `work_order_sheet_export` to `WorkbookPayload` or expose work-order sheet through payload state.
- Modify: `src/analytics/contracts.py`
  - Add optional `work_order_sheet_export: pd.DataFrame` to `WorkbookPayload`.
- Modify: `src/etl/runner.py`
  - Add summary output path.
  - Write summary JSON after CSV write in normal mode.
- Modify: `tests/test_runner.py`
  - Cover summary file path and JSON content.
- Modify: `tests/test_costing_etl.py`
  - Cover last work-order sheet state assignment.
- Add or modify: `tests/test_summary.py`
  - Unit-test summary payload builder.

## Task 1: Summary Payload Builder

**Files:**
- Create: `src/analytics/summary.py`
- Test: `tests/test_summary.py`

- [ ] **Step 1: Write failing summary tests**

Create `tests/test_summary.py`:

```python
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.analytics.contracts import QualityMetric
from src.analytics.summary import build_summary_payload, write_summary_json


def test_build_summary_payload_counts_error_and_anomaly_fields(tmp_path: Path) -> None:
    payload = build_summary_payload(
        pipeline_name='gb',
        input_path=tmp_path / 'input.xlsx',
        output_path=tmp_path / 'output.xlsx',
        error_log_path=tmp_path / 'error.csv',
        error_log_count=3,
        quality_metrics=(
            QualityMetric('行数勾稽', '产品数量统计输出行数', '2', '仅保留有效工单'),
            QualityMetric('分析覆盖率', '可参与分析占比', '50.00%', '白名单工单覆盖率'),
        ),
        error_log_frame=pd.DataFrame(
            [
                {'issue_type': 'MISSING_AMOUNT'},
                {'issue_type': 'MISSING_AMOUNT'},
                {'issue_type': 'TOTAL_COST_MISMATCH'},
            ]
        ),
        work_order_sheet_frame=pd.DataFrame(
            [
                {'异常等级': '关注', '异常主要来源': '材料异常'},
                {'异常等级': '高度可疑', '异常主要来源': '总成本异常'},
                {'异常等级': '正常', '异常主要来源': ''},
            ]
        ),
        month_filter_summary=None,
    )

    assert payload['pipeline'] == 'gb'
    assert payload['error_log_count'] == 3
    assert payload['quality_metrics']['产品数量统计输出行数']['value'] == '2'
    assert payload['issue_type_counts'] == {'MISSING_AMOUNT': 2, 'TOTAL_COST_MISMATCH': 1}
    assert payload['anomaly_level_counts'] == {'关注': 1, '高度可疑': 1, '正常': 1}
    assert payload['anomaly_source_counts'] == {'材料异常': 1, '总成本异常': 1}


def test_write_summary_json_uses_utf8(tmp_path: Path) -> None:
    output_path = tmp_path / 'summary.json'
    write_summary_json(output_path, {'pipeline': 'gb', '中文': '可读'})

    loaded = json.loads(output_path.read_text(encoding='utf-8'))

    assert loaded == {'pipeline': 'gb', '中文': '可读'}
```

- [ ] **Step 2: Run summary tests to verify they fail**

Run:

```powershell
conda run -n test python -m pytest tests/test_summary.py -q
```

Expected: FAIL because `src.analytics.summary` does not exist.

- [ ] **Step 3: Implement summary module**

Create `src/analytics/summary.py`:

```python
from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path

import pandas as pd

from src.analytics.contracts import QualityMetric
from src.etl.month_filter import MonthFilterSummary


def _value_counts(frame: pd.DataFrame, column_name: str, *, drop_blank: bool = True) -> dict[str, int]:
    if column_name not in frame.columns or frame.empty:
        return {}
    series = frame[column_name].fillna('').astype(str).str.strip()
    if drop_blank:
        series = series[series.ne('')]
    counts = series.value_counts(sort=False)
    return {str(index): int(value) for index, value in counts.items()}


def _quality_metric_payload(quality_metrics: Iterable[QualityMetric]) -> dict[str, dict[str, str]]:
    return {
        metric.metric: {
            'category': metric.category,
            'value': metric.value,
            'description': metric.description,
        }
        for metric in quality_metrics
    }


def _month_filter_payload(month_filter_summary: MonthFilterSummary | None) -> dict[str, object] | None:
    if month_filter_summary is None:
        return None
    return {
        'month_range': month_filter_summary.month_range.describe(),
        'input_rows': month_filter_summary.input_rows,
        'output_rows': month_filter_summary.output_rows,
        'input_months': list(month_filter_summary.input_months),
        'output_months': list(month_filter_summary.output_months),
    }


def build_summary_payload(
    *,
    pipeline_name: str,
    input_path: Path,
    output_path: Path,
    error_log_path: Path,
    error_log_count: int,
    quality_metrics: Iterable[QualityMetric],
    error_log_frame: pd.DataFrame,
    work_order_sheet_frame: pd.DataFrame,
    month_filter_summary: MonthFilterSummary | None,
) -> dict[str, object]:
    """从同一次 ETL 产物汇总审计摘要，避免重新计算另一套口径。"""
    return {
        'pipeline': pipeline_name,
        'input': str(input_path),
        'output': str(output_path),
        'error_log': str(error_log_path),
        'error_log_count': int(error_log_count),
        'quality_metrics': _quality_metric_payload(quality_metrics),
        'issue_type_counts': _value_counts(error_log_frame, 'issue_type'),
        'anomaly_level_counts': _value_counts(work_order_sheet_frame, '异常等级'),
        'anomaly_source_counts': _value_counts(work_order_sheet_frame, '异常主要来源'),
        'month_filter': _month_filter_payload(month_filter_summary),
    }


def write_summary_json(output_path: Path, payload: dict[str, object]) -> None:
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
```

- [ ] **Step 4: Run summary tests**

Run:

```powershell
conda run -n test python -m pytest tests/test_summary.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```powershell
git add src/analytics/summary.py tests/test_summary.py
git commit -m "feat(analytics): add summary payload builder"
```

## Task 2: Carry Work-Order Sheet In Payload State

**Files:**
- Modify: `src/analytics/contracts.py`
- Modify: `src/etl/pipeline.py`
- Modify: `src/etl/costing_etl.py`
- Test: `tests/test_costing_etl.py`

- [ ] **Step 1: Write failing ETL state test**

Append this test to `tests/test_costing_etl.py`:

```python
def test_prepare_payload_stores_work_order_sheet_for_summary(tmp_path: Path) -> None:
    etl = CostingWorkbookETL(skip_rows=2, product_order=(), ensure_output_directories=False)
    work_order_export = pd.DataFrame([{'异常等级': '关注', '异常主要来源': '材料异常'}])
    payload = WorkbookPayload(
        sheet_models=(),
        quality_metrics=(),
        error_log_count=0,
        stage_timings={},
        error_log_export=pd.DataFrame(),
        work_order_sheet_export=work_order_export,
    )

    with patch.object(etl.pipeline, 'build_workbook_payload', return_value=payload):
        assert etl.prepare_payload(tmp_path / 'input.xlsx') is True

    pd.testing.assert_frame_equal(etl.last_work_order_sheet_frame, work_order_export)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
conda run -n test python -m pytest tests/test_costing_etl.py::test_prepare_payload_stores_work_order_sheet_for_summary -q
```

Expected: FAIL because `WorkbookPayload` lacks `work_order_sheet_export` or ETL does not store it.

- [ ] **Step 3: Extend payload and state**

In `src/analytics/contracts.py`, update `WorkbookPayload`:

```python
work_order_sheet_export: pd.DataFrame = field(default_factory=pd.DataFrame)
```

In `src/etl/pipeline.py`, set it in returned payload:

```python
work_order_sheet_export=artifacts.work_order_sheet.data.copy(),
```

In `src/etl/costing_etl.py`:

```python
self.last_work_order_sheet_frame: pd.DataFrame = pd.DataFrame()
```

Reset it in `_reset_last_run_state()` and set it in `_apply_payload_state()`:

```python
self.last_work_order_sheet_frame = pd.DataFrame()
self.last_work_order_sheet_frame = payload.work_order_sheet_export.copy()
```

- [ ] **Step 4: Run ETL state tests**

Run:

```powershell
conda run -n test python -m pytest tests/test_costing_etl.py::test_prepare_payload_stores_work_order_sheet_for_summary tests/test_costing_etl.py::test_prepare_payload_builds_pipeline_payload_without_writing_workbook -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```powershell
git add src/analytics/contracts.py src/etl/pipeline.py src/etl/costing_etl.py tests/test_costing_etl.py
git commit -m "feat(etl): retain work order sheet for summary output"
```

## Task 3: Write Summary JSON In Normal Runner Mode

**Files:**
- Modify: `src/etl/runner.py`
- Test: `tests/test_runner.py`

- [ ] **Step 1: Write failing runner summary test**

Append this test to `tests/test_runner.py`:

```python
def test_run_pipeline_writes_summary_json_after_success(monkeypatch, tmp_path) -> None:
    input_file = tmp_path / 'GB-成本计算单.xlsx'
    input_file.touch()
    processed_dir = tmp_path / 'processed'
    processed_dir.mkdir()
    config = PipelineConfig(
        name='gb',
        raw_dir=tmp_path,
        processed_dir=processed_dir,
        input_patterns=('GB-*.xlsx',),
        product_order=(('GB_C.D.B0040AA', 'BMS-750W驱动器'),),
        product_anomaly_scope_mode='doc_type_split',
    )

    class _DummyETL:
        def __init__(self, skip_rows: int, *, product_order, standalone_cost_items, product_anomaly_scope_mode, month_range=None, ensure_output_directories=True) -> None:
            self.last_quality_metrics = (
                QualityMetric('行数勾稽', '产品数量统计输出行数', '1', '仅保留有效工单'),
            )
            self.last_error_log_count = 1
            self.last_error_log_frame = pd.DataFrame([{'issue_type': 'MISSING_AMOUNT'}])
            self.last_work_order_sheet_frame = pd.DataFrame([{'异常等级': '关注', '异常主要来源': '材料异常'}])
            self.last_month_filter_summary = None
            self.last_stage_timings = {}

        def process_file(self, input_path: Path, output_path: Path) -> bool:
            output_path.write_text('ok', encoding='utf-8')
            return True

    monkeypatch.setattr('src.etl.runner.CostingWorkbookETL', _DummyETL)

    assert run_pipeline(config) == 0

    summary_path = processed_dir / 'GB-成本计算单_处理后_summary.json'
    payload = json.loads(summary_path.read_text(encoding='utf-8'))
    assert payload['pipeline'] == 'gb'
    assert payload['issue_type_counts'] == {'MISSING_AMOUNT': 1}
    assert payload['anomaly_level_counts'] == {'关注': 1}
```

Add `import json` at the top of `tests/test_runner.py`.

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
conda run -n test python -m pytest tests/test_runner.py::test_run_pipeline_writes_summary_json_after_success -q
```

Expected: FAIL because summary JSON is not written.

- [ ] **Step 3: Implement summary path and write**

In `src/etl/runner.py`, import:

```python
from src.analytics.summary import build_summary_payload, write_summary_json
```

Update `_build_output_paths()` to return summary path:

```python
def _build_output_paths(...) -> tuple[Path, Path, Path]:
    workbook_path = ...
    error_log_path = ...
    summary_path = processed_dir / f'{input_file.stem}_处理后{suffix}_summary.json'
    return workbook_path, error_log_path, summary_path
```

Update call sites to unpack `summary_file`.

After `write_error_log_csv(...)` in normal mode:

```python
summary_payload = build_summary_payload(
    pipeline_name=config.name,
    input_path=input_file,
    output_path=output_file,
    error_log_path=error_log_csv_file,
    error_log_count=etl.last_error_log_count,
    quality_metrics=etl.last_quality_metrics,
    error_log_frame=etl.last_error_log_frame,
    work_order_sheet_frame=getattr(etl, 'last_work_order_sheet_frame', pd.DataFrame()),
    month_filter_summary=getattr(etl, 'last_month_filter_summary', None),
)
write_summary_json(summary_file, summary_payload)
```

Do not write summary in check-only mode.

- [ ] **Step 4: Run runner tests**

Run:

```powershell
conda run -n test python -m pytest tests/test_runner.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```powershell
git add src/etl/runner.py tests/test_runner.py
git commit -m "feat(etl): write summary json after successful runs"
```

## Task 4: Verification

**Files:**
- No code changes unless verification exposes a defect.

- [ ] **Step 1: Run focused tests**

Run:

```powershell
conda run -n test python -m pytest tests/test_summary.py tests/test_runner.py tests/test_costing_etl.py -q
```

Expected: PASS.

- [ ] **Step 2: Run full test suite**

Run:

```powershell
conda run -n test python -m pytest tests -q
```

Expected: PASS.

- [ ] **Step 3: Run real check-only benchmark**

Run:

```powershell
conda run -n test python main.py gb --check-only --benchmark
```

Expected: exits `0`; no summary JSON is written.

- [ ] **Step 4: Run normal sample on a small generated fixture if available**

If no safe small real workbook exists, skip normal real export and rely on unit/contract tests. Do not run full SK normal export unless the user explicitly asks because it can take significant time and write large artifacts.


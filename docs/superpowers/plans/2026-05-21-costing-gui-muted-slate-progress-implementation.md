# Costing GUI Muted Slate Progress Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade the existing PySide6 costing GUI to a low-saturation dark dashboard layout and add coarse-grained real ETL progress reporting for scan, precheck, and run tasks.

**Architecture:** Keep the existing PySide6 GUI and service-driven ETL boundary. Add a neutral progress contract that service, ETL, worker, and GUI can share without making ETL depend on Qt. Then restructure the main window into content, progress, and bottom action regions, and apply a muted slate QSS theme only to the main window while keeping confirmation dialogs light.

**Tech Stack:** Python 3.11+, PySide6, pytest, ruff, pandas, polars, openpyxl, xlsxwriter.

---

## Scope Check

The approved spec covers two coupled changes: GUI layout/theme and coarse ETL progress. They are a single implementable feature because the progress area is part of the new layout and uses the same worker path as the visual refresh.

Implementation refinement from the spec: place `ProgressEvent`, `ProgressCallback`, and `report_progress()` in `src/services/progress.py`, then re-export the type from `src/services/costing_service.py`. This avoids a circular import where ETL code would need to import `src.services.costing_service`, while `costing_service.py` already imports `CostingWorkbookETL`.

The user-supplied gotchas are implementation requirements:

- Apply dark QSS on the `MainWindow` instance only. Do not call `QApplication.setStyleSheet()` for the dark theme.
- Declare `WorkerSignals.progress = Signal(object)` as a class attribute on the `QObject` subclass.
- Define `QTableWidget::item:selected` background and text colors explicitly.
- Limit log terminal block count with `self.log_edit.document().setMaximumBlockCount(1000)`.

## File Structure

Create:

- `src/services/progress.py`  
  Neutral progress dataclass, callback type, and callback-safe reporting helper.
- `tests/test_gui_task_worker.py`  
  Worker-level progress signal tests that do not require starting a real ETL task.

Modify:

- `src/services/costing_service.py`  
  Accept optional progress callbacks, report prepare / failed / done, and pass callbacks to ETL.
- `src/etl/pipeline.py`  
  Report real payload stages: ingest, normalize, fact, analysis, presentation.
- `src/etl/costing_etl.py`  
  Accept progress callbacks on payload preparation and process runs, pass callbacks to pipeline, and report export during normal runs.
- `src/gui/task_worker.py`  
  Add `progress` signal and call service functions with `progress_callback`.
- `src/gui/main_window.py`  
  Add progress widgets, KPI labels, bottom action bar, stale progress filtering, log block limit, and new layout containers.
- `src/gui/styles.py`  
  Replace the light main-window QSS with the muted slate theme while keeping `MESSAGE_BOX_STYLESHEET` light.
- `tests/test_costing_service.py`  
  Cover progress event construction, service callback compatibility, callback failure isolation, and export / done events.
- `tests/test_etl_pipeline.py`  
  Cover pipeline stage progress events.
- `tests/test_costing_etl.py`  
  Cover export progress for normal runs and no export progress for precheck payloads.
- `tests/test_gui_main_window.py`  
  Cover progress widgets, stale progress filtering, KPI labels, clear reset behavior, and failure progress behavior.
- `tests/test_gui_styles.py`  
  Update assertions for the muted slate theme, selected table rows, progress bar, log terminal, and light confirmation dialogs.

## Core Pseudocode Draft

```python
# 目标：用一个非 Qt 的进度契约贯通 service、ETL、worker 和 GUI。

@dataclass(frozen=True)
class ProgressEvent:
    percent: int
    stage: str
    message: str


ProgressCallback = Callable[[ProgressEvent], None]


def report_progress(callback: ProgressCallback | None, percent: int, stage: str, message: str) -> None:
    if callback is None:
        return
    try:
        callback(ProgressEvent(percent=percent, stage=stage, message=message))
    except Exception:
        logger.warning("Progress callback failed", exc_info=True)


def run_costing_request(request: CostingRunRequest, *, progress_callback: ProgressCallback | None = None) -> CostingRunResult:
    report_progress(progress_callback, 0, "prepare", "正在校验输入配置")
    prepared, validation_error = _prepare_request(request)
    if validation_error is not None:
        report_progress(progress_callback, 0, "failed", validation_error.message)
        return validation_error

    report_progress(progress_callback, 5, "prepare", "已完成路径与参数校验")
    etl = _build_etl(request, prepared.month_range)
    ok = etl.process_file(request.input_path, prepared.workbook_path, progress_callback=progress_callback)
    if not ok:
        report_progress(progress_callback, 0, "failed", "处理失败")
        return CostingRunResult(status=ServiceStatus.FAILED, message="处理失败", error_code="ETL_FAILED")

    report_progress(progress_callback, 100, "done", "处理完成")
    return CostingRunResult(status=ServiceStatus.SUCCEEDED, message="处理成功")
```

---

### Task 1: Add Non-GUI Progress Contract Through Service and ETL

**Files:**
- Create: `src/services/progress.py`
- Modify: `src/services/costing_service.py`
- Modify: `src/etl/pipeline.py`
- Modify: `src/etl/costing_etl.py`
- Modify: `tests/test_costing_service.py`
- Modify: `tests/test_etl_pipeline.py`
- Modify: `tests/test_costing_etl.py`

- [ ] **Step 1: Add failing service progress tests**

Append these tests to `tests/test_costing_service.py`:

```python
def test_progress_event_is_constructible() -> None:
    from src.services.costing_service import ProgressEvent

    event = ProgressEvent(percent=45, stage='fact', message='已拆分事实表')

    assert event.percent == 45
    assert event.stage == 'fact'
    assert event.message == '已拆分事实表'


def test_run_costing_request_reports_prepare_export_and_done_progress(monkeypatch, tmp_path: Path) -> None:
    from src.services.costing_service import ProgressEvent

    request = _request(tmp_path)
    events: list[ProgressEvent] = []

    class _ProgressETL:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.last_quality_metrics = ()
            self.last_error_log_count = 0
            self.last_month_filter_summary = None
            self.last_stage_timings = {'export': 0.1}
            self.last_ingest_backend = 'dummy'
            self.last_work_order_sheet_frame = pd.DataFrame()
            self.last_candidate_products = ()

        def process_file(
            self,
            input_path: Path,
            output_path: Path,
            *,
            progress_callback: object | None = None,
        ) -> bool:
            assert input_path == request.input_path
            assert output_path.name.endswith('_处理后.xlsx')
            if progress_callback is not None:
                progress_callback(ProgressEvent(percent=95, stage='export', message='正在写出 workbook'))
            return True

    monkeypatch.setattr('src.services.costing_service.CostingWorkbookETL', _ProgressETL)

    result = run_costing_request(request, progress_callback=events.append)

    assert result.status == ServiceStatus.SUCCEEDED
    assert [(event.percent, event.stage) for event in events] == [
        (0, 'prepare'),
        (5, 'prepare'),
        (95, 'export'),
        (100, 'done'),
    ]


def test_precheck_progress_does_not_report_export(monkeypatch, tmp_path: Path) -> None:
    from src.services.costing_service import ProgressEvent

    request = _request(tmp_path)
    events: list[ProgressEvent] = []

    class _ProgressETL:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.last_quality_metrics = ()
            self.last_error_log_count = 0
            self.last_month_filter_summary = None
            self.last_stage_timings = {'presentation': 0.1}
            self.last_ingest_backend = 'dummy'
            self.last_work_order_sheet_frame = pd.DataFrame()
            self.last_candidate_products = ()

        def prepare_payload(self, input_path: Path, *, progress_callback: object | None = None) -> bool:
            assert input_path == request.input_path
            if progress_callback is not None:
                progress_callback(ProgressEvent(percent=85, stage='presentation', message='已构建输出 Sheet'))
            return True

    monkeypatch.setattr('src.services.costing_service.CostingWorkbookETL', _ProgressETL)

    result = precheck_costing_run(request, progress_callback=events.append)

    assert result.status == ServiceStatus.SUCCEEDED
    assert 'export' not in [event.stage for event in events]
    assert events[-1].stage == 'done'


def test_progress_callback_failure_does_not_fail_service(monkeypatch, caplog, tmp_path: Path) -> None:
    request = _request(tmp_path)

    class _ProgressETL:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.last_quality_metrics = ()
            self.last_error_log_count = 0
            self.last_month_filter_summary = None
            self.last_stage_timings = {}
            self.last_ingest_backend = 'dummy'
            self.last_work_order_sheet_frame = pd.DataFrame()
            self.last_candidate_products = ()

        def prepare_payload(self, input_path: Path, *, progress_callback: object | None = None) -> bool:
            assert input_path == request.input_path
            return True

    def _raise_on_progress(_event: object) -> None:
        raise RuntimeError('progress sink failed')

    monkeypatch.setattr('src.services.costing_service.CostingWorkbookETL', _ProgressETL)

    result = precheck_costing_run(request, progress_callback=_raise_on_progress)

    assert result.status == ServiceStatus.SUCCEEDED
    assert 'Progress callback failed' in caplog.text
```

- [ ] **Step 2: Add failing pipeline progress test**

Append this test to `tests/test_etl_pipeline.py`:

```python
def test_build_workbook_payload_reports_real_stage_progress(monkeypatch, tmp_path: Path) -> None:
    from src.services.progress import ProgressEvent

    etl = CostingWorkbookETL(skip_rows=2)
    events: list[ProgressEvent] = []
    raw = RawWorkbookFrame(
        sheet_name='成本计算单',
        header_rows=(('年期', '产品编码'), ('', '')),
        frame=pl.DataFrame({'column_0': ['2025年01期'], 'column_1': ['P001']}),
        ingest_backend='test',
    )
    normalized = NormalizedCostFrame(
        frame=pl.DataFrame(
            {
                '月份': ['2025-01'],
                '产品编码': ['P001'],
                '产品名称': ['产品A'],
            }
        ),
        key_columns=('产品编码', '产品名称'),
    )
    split_result = SplitResult(
        detail_df=pl.DataFrame({'产品编码': ['P001'], '产品名称': ['产品A']}),
        qty_df=pl.DataFrame({'产品编码': ['P001'], '产品名称': ['产品A']}),
    )
    artifacts = AnalysisArtifacts(
        fact_df=pd.DataFrame({'产品编码': ['P001'], '产品名称': ['产品A']}),
        qty_sheet_df=pd.DataFrame({'产品编码': ['P001'], '产品名称': ['产品A']}),
        work_order_sheet=FlatSheet(
            data=pd.DataFrame({'产品编码': ['P001'], '产品名称': ['产品A']}),
            column_types={'产品编码': 'text', '产品名称': 'text'},
        ),
        product_anomaly_sections=[],
        quality_metrics=(),
        error_log=pd.DataFrame(),
    )
    model = SheetModel(
        sheet_name='成本计算单总表',
        columns=('产品编码',),
        rows_factory=lambda: iter([('P001',)]),
        column_types={'产品编码': 'text'},
        number_formats={},
    )

    monkeypatch.setattr(etl.pipeline, 'load_raw_workbook_frame', lambda _path: raw)
    monkeypatch.setattr(etl.pipeline, 'build_normalized_cost_frame', lambda _raw: normalized)
    monkeypatch.setattr(etl.pipeline, 'split_normalized_frames', lambda _normalized: split_result)
    monkeypatch.setattr('src.etl.pipeline.build_report_artifacts', lambda *_args, **_kwargs: artifacts)
    monkeypatch.setattr('src.etl.pipeline.build_sheet_models', lambda **_kwargs: (model,))

    payload = etl.pipeline.build_workbook_payload(
        tmp_path / 'input.xlsx',
        standalone_cost_items=(),
        product_anomaly_scope_mode='legacy_single_scope',
        month_range=None,
        presentation_product_order=(),
        progress_callback=events.append,
    )

    assert payload.sheet_models == (model,)
    assert [(event.percent, event.stage) for event in events] == [
        (10, 'ingest'),
        (30, 'normalize'),
        (45, 'fact'),
        (70, 'analysis'),
        (85, 'presentation'),
    ]
```

- [ ] **Step 3: Add failing ETL shell progress tests**

Append these tests near existing payload/process tests in `tests/test_costing_etl.py`:

```python
def test_prepare_payload_passes_progress_callback_without_export(tmp_path: Path) -> None:
    from src.services.progress import ProgressEvent

    etl = CostingWorkbookETL(skip_rows=2, product_order=())
    events: list[ProgressEvent] = []
    payload = WorkbookPayload(
        sheet_models=(),
        quality_metrics=(),
        error_log_count=0,
        stage_timings={'presentation': 0.1},
        error_log_export=pd.DataFrame(),
    )

    def _fake_build_payload(*_args: object, **kwargs: object) -> WorkbookPayload:
        callback = kwargs['progress_callback']
        callback(ProgressEvent(percent=85, stage='presentation', message='已构建输出 Sheet'))
        return payload

    with patch.object(etl.pipeline, 'build_workbook_payload', side_effect=_fake_build_payload):
        assert etl.prepare_payload(tmp_path / 'input.xlsx', progress_callback=events.append) is True

    assert [event.stage for event in events] == ['presentation']


def test_process_file_reports_export_progress(tmp_path: Path) -> None:
    from src.services.progress import ProgressEvent

    etl = CostingWorkbookETL(skip_rows=2, product_order=())
    events: list[ProgressEvent] = []
    payload = WorkbookPayload(
        sheet_models=(),
        quality_metrics=(),
        error_log_count=0,
        stage_timings={'presentation': 0.1},
        error_log_export=pd.DataFrame(),
    )

    with (
        patch.object(etl.pipeline, 'build_workbook_payload', return_value=payload),
        patch.object(etl.workbook_writer, 'write_workbook_from_models') as writer_mock,
    ):
        assert etl.process_file(
            tmp_path / 'input.xlsx',
            tmp_path / 'output.xlsx',
            progress_callback=events.append,
        ) is True

    writer_mock.assert_called_once()
    assert 'export' in [event.stage for event in events]
    assert any(event.percent == 95 and event.stage == 'export' for event in events)
```

- [ ] **Step 4: Run focused failing tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_costing_service.py::test_progress_event_is_constructible tests/test_costing_service.py::test_run_costing_request_reports_prepare_export_and_done_progress tests/test_costing_service.py::test_precheck_progress_does_not_report_export tests/test_costing_service.py::test_progress_callback_failure_does_not_fail_service tests/test_etl_pipeline.py::test_build_workbook_payload_reports_real_stage_progress tests/test_costing_etl.py::test_prepare_payload_passes_progress_callback_without_export tests/test_costing_etl.py::test_process_file_reports_export_progress -q
```

Expected: FAIL because `ProgressEvent`, `src.services.progress`, and callback parameters do not exist yet.

- [ ] **Step 5: Create `src/services/progress.py`**

Create the file with this content:

```python
from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProgressEvent:
    percent: int
    stage: str
    message: str


ProgressCallback = Callable[[ProgressEvent], None]


def report_progress(
    callback: ProgressCallback | None,
    percent: int,
    stage: str,
    message: str,
) -> None:
    """上报进度但不允许 UI 观察能力中断 ETL 主流程。"""
    if callback is None:
        return

    try:
        callback(ProgressEvent(percent=percent, stage=stage, message=message))
    except Exception:  # noqa: BLE001
        logger.warning('Progress callback failed', exc_info=True)
```

- [ ] **Step 6: Update `src/services/costing_service.py` imports and signatures**

Add this import near the existing imports:

```python
from src.services.progress import ProgressCallback, ProgressEvent, report_progress
```

Change `precheck_costing_run()` signature to:

```python
def precheck_costing_run(
    request: CostingRunRequest,
    *,
    validate_output_dir: bool = True,
    progress_callback: ProgressCallback | None = None,
) -> CostingRunResult:
```

Change `run_costing_request()` signature to:

```python
def run_costing_request(
    request: CostingRunRequest,
    *,
    progress_callback: ProgressCallback | None = None,
) -> CostingRunResult:
```

- [ ] **Step 7: Update `precheck_costing_run()` progress behavior**

Inside `precheck_costing_run()`, add progress reporting in this order:

```python
    report_progress(progress_callback, 0, 'prepare', '正在校验输入配置')
    prepared, validation_error = _prepare_request(request, validate_output_dir=validate_output_dir)
    if validation_error is not None:
        report_progress(progress_callback, 0, 'failed', validation_error.message)
        return validation_error
    assert prepared is not None

    report_progress(progress_callback, 5, 'prepare', '已完成路径与参数校验')

    if prepared.workbook_path.exists() and not request.overwrite_confirmed:
        message = f'输出 workbook 已存在: {prepared.workbook_path}'
        report_progress(progress_callback, 0, 'failed', message)
        return _failed(
            message=message,
            error_code='OUTPUT_EXISTS',
            workbook_path=prepared.workbook_path,
        )

    try:
        etl = _build_etl(request, prepared.month_range)
        if not etl.prepare_payload(request.input_path, progress_callback=progress_callback):
            report_progress(progress_callback, 0, 'failed', '预检失败，请查看日志详情')
            return _failed(
                message='预检失败，请查看日志详情',
                error_code='ETL_FAILED',
                workbook_path=prepared.workbook_path,
            )
        report_progress(progress_callback, 100, 'done', '预检完成')
        return _result_from_etl(
            etl,
            status=ServiceStatus.SUCCEEDED,
            message='预检通过',
            input_path=request.input_path,
            workbook_path=prepared.workbook_path,
            output_written=False,
        )
    except Exception as exc:  # noqa: BLE001
        report_progress(progress_callback, 0, 'failed', '预检失败，请查看日志详情')
        return _failed(
            message='预检失败，请查看日志详情',
            error_code='ETL_FAILED',
            workbook_path=prepared.workbook_path,
            technical_detail=str(exc),
        )
```

- [ ] **Step 8: Update `run_costing_request()` progress behavior**

Inside `run_costing_request()`, add progress reporting in this order:

```python
    report_progress(progress_callback, 0, 'prepare', '正在校验输入配置')
    prepared, validation_error = _prepare_request(request)
    if validation_error is not None:
        report_progress(progress_callback, 0, 'failed', validation_error.message)
        return validation_error
    assert prepared is not None

    report_progress(progress_callback, 5, 'prepare', '已完成路径与参数校验')

    if prepared.workbook_path.exists() and not request.overwrite_confirmed:
        message = f'输出 workbook 已存在: {prepared.workbook_path}'
        report_progress(progress_callback, 0, 'failed', message)
        return _failed(
            message=message,
            error_code='OUTPUT_EXISTS',
            workbook_path=prepared.workbook_path,
        )

    try:
        request.output_dir.mkdir(parents=True, exist_ok=True)
        etl = _build_etl(request, prepared.month_range)
        if not etl.process_file(request.input_path, prepared.workbook_path, progress_callback=progress_callback):
            report_progress(progress_callback, 0, 'failed', '处理失败，请查看日志详情')
            return _failed(
                message='处理失败，请查看日志详情',
                error_code='ETL_FAILED',
                workbook_path=prepared.workbook_path,
            )
        report_progress(progress_callback, 100, 'done', '处理完成')
        return _result_from_etl(
            etl,
            status=ServiceStatus.SUCCEEDED,
            message='处理成功',
            input_path=request.input_path,
            workbook_path=prepared.workbook_path,
            output_written=True,
        )
    except Exception as exc:  # noqa: BLE001
        report_progress(progress_callback, 0, 'failed', '处理失败，请查看日志详情')
        return _failed(
            message='处理失败，请查看日志详情',
            error_code='ETL_FAILED',
            workbook_path=prepared.workbook_path,
            technical_detail=str(exc),
        )
```

- [ ] **Step 9: Update `src/etl/pipeline.py` for progress callbacks**

Add this import:

```python
from src.services.progress import ProgressCallback, report_progress
```

Change the `build_workbook_payload()` signature by adding this keyword argument:

```python
        progress_callback: ProgressCallback | None = None,
```

Report progress after each existing stage timing assignment:

```python
        stage_timings['ingest'] = perf_counter() - ingest_start
        report_progress(progress_callback, 10, 'ingest', '已读取 workbook')
```

```python
        stage_timings['normalize'] = perf_counter() - normalize_start
        report_progress(progress_callback, 30, 'normalize', '已完成标准化和月份过滤')
```

```python
        stage_timings['fact'] = perf_counter() - fact_start
        report_progress(progress_callback, 45, 'fact', '已拆分事实表')
```

```python
        stage_timings['analysis'] = perf_counter() - analysis_start
        report_progress(progress_callback, 70, 'analysis', '已完成分析与质量校验')
```

```python
        stage_timings['presentation'] = perf_counter() - presentation_start
        report_progress(progress_callback, 85, 'presentation', '已构建输出 Sheet')
```

- [ ] **Step 10: Update `src/etl/costing_etl.py` signatures and callback forwarding**

Add this import:

```python
from src.services.progress import ProgressCallback, report_progress
```

Change `prepare_payload()` signature to:

```python
    def prepare_payload(self, input_path: Path, *, progress_callback: ProgressCallback | None = None) -> bool:
```

Pass the callback into `self.pipeline.build_workbook_payload()`:

```python
                progress_callback=progress_callback,
```

Change `process_file()` signature to:

```python
    def process_file(
        self,
        input_path: Path,
        output_path: Path,
        *,
        progress_callback: ProgressCallback | None = None,
    ) -> bool:
```

Pass the callback into `self.pipeline.build_workbook_payload()`:

```python
                progress_callback=progress_callback,
```

Immediately before writing workbook models, report export:

```python
            report_progress(progress_callback, 95, 'export', '正在写出 workbook')
            self.workbook_writer.write_workbook_from_models(
                output_path,
                sheet_models=payload.sheet_models,
            )
```

- [ ] **Step 11: Run focused progress tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_costing_service.py::test_progress_event_is_constructible tests/test_costing_service.py::test_run_costing_request_reports_prepare_export_and_done_progress tests/test_costing_service.py::test_precheck_progress_does_not_report_export tests/test_costing_service.py::test_progress_callback_failure_does_not_fail_service tests/test_etl_pipeline.py::test_build_workbook_payload_reports_real_stage_progress tests/test_costing_etl.py::test_prepare_payload_passes_progress_callback_without_export tests/test_costing_etl.py::test_process_file_reports_export_progress -q
```

Expected: PASS.

- [ ] **Step 12: Run existing service and ETL focused suites**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_costing_service.py tests/test_etl_pipeline.py tests/test_costing_etl.py -q
```

Expected: PASS.

- [ ] **Step 13: Commit progress contract**

Run:

```bash
git add src/services/progress.py src/services/costing_service.py src/etl/pipeline.py src/etl/costing_etl.py tests/test_costing_service.py tests/test_etl_pipeline.py tests/test_costing_etl.py
git commit -m "feat(gui): add ETL progress events"
```

Expected: commit succeeds.

---

### Task 2: Add Worker Progress Signal

**Files:**
- Create: `tests/test_gui_task_worker.py`
- Modify: `src/gui/task_worker.py`

- [ ] **Step 1: Write worker progress signal test**

Create `tests/test_gui_task_worker.py` with this content:

```python
from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip('PySide6')

from src.gui.task_worker import ServiceWorker  # noqa: E402
from src.services.costing_service import CostingRunRequest, CostingRunResult, ServiceStatus  # noqa: E402
from src.services.progress import ProgressEvent  # noqa: E402


def test_service_worker_emits_progress_signal(tmp_path: Path) -> None:
    request = CostingRunRequest(
        pipeline='gb',
        input_path=tmp_path / 'GB-成本计算单.xlsx',
        output_dir=tmp_path,
        product_order=(('P001', '产品A'),),
    )
    events: list[ProgressEvent] = []
    results: list[CostingRunResult] = []

    def _fake_service(
        _request: CostingRunRequest,
        *,
        progress_callback: object | None = None,
    ) -> CostingRunResult:
        assert progress_callback is not None
        progress_callback(ProgressEvent(percent=45, stage='fact', message='已拆分事实表'))
        return CostingRunResult(status=ServiceStatus.SUCCEEDED, message='ok')

    worker = ServiceWorker('测试任务', request, _fake_service)
    worker.signals.progress.connect(events.append)
    worker.signals.finished.connect(results.append)

    worker.run()

    assert [(event.percent, event.stage, event.message) for event in events] == [
        (45, 'fact', '已拆分事实表')
    ]
    assert results[0].message == 'ok'
```

- [ ] **Step 2: Run worker progress test and verify failure**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_gui_task_worker.py -q
```

Expected: FAIL because `WorkerSignals` has no `progress` signal and `ServiceWorker` does not pass a callback.

- [ ] **Step 3: Update `src/gui/task_worker.py`**

Replace the imports and signal/function typing with:

```python
from __future__ import annotations

from typing import Protocol

from PySide6.QtCore import QObject, QRunnable, Signal, Slot

from src.services.costing_service import CostingRunRequest, CostingRunResult
from src.services.progress import ProgressCallback, ProgressEvent


class CostingServiceFunction(Protocol):
    def __call__(
        self,
        request: CostingRunRequest,
        *,
        progress_callback: ProgressCallback | None = None,
    ) -> CostingRunResult:
        raise NotImplementedError
```

Change `WorkerSignals` to:

```python
class WorkerSignals(QObject):
    started = Signal(str)
    progress = Signal(object)
    finished = Signal(object)
    failed = Signal(str)
```

Change the `function` annotation in `ServiceWorker.__init__()` to:

```python
        function: CostingServiceFunction,
```

Change `run()` to:

```python
    @Slot()
    def run(self) -> None:
        self.signals.started.emit(self.label)

        def emit_progress(event: ProgressEvent) -> None:
            self.signals.progress.emit(event)

        try:
            result = self.function(self.request, progress_callback=emit_progress)
        except Exception as exc:  # noqa: BLE001
            self.signals.failed.emit(str(exc))
            return
        self.signals.finished.emit(result)
```

- [ ] **Step 4: Run worker progress test**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_gui_task_worker.py -q
```

Expected: PASS.

- [ ] **Step 5: Run existing GUI worker-adjacent tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_gui_task_worker.py tests/test_gui_main_window.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit worker progress signal**

Run:

```bash
git add src/gui/task_worker.py tests/test_gui_task_worker.py
git commit -m "feat(gui): forward worker progress signals"
```

Expected: commit succeeds.

---

### Task 3: Add Progress Widgets, KPI Labels, and Stale Progress Filtering

**Files:**
- Modify: `src/gui/main_window.py`
- Modify: `tests/test_gui_main_window.py`

- [ ] **Step 1: Add failing GUI progress and KPI tests**

Append these tests to `tests/test_gui_main_window.py`:

```python
def test_progress_widgets_initialize_and_clear(main_window: MainWindow) -> None:
    assert main_window.progress_bar.value() == 0
    assert main_window.progress_label.text() == '等待任务'

    main_window.progress_bar.setValue(70)
    main_window.progress_label.setText('已完成分析与质量校验')
    main_window._clear_conditions()

    assert main_window.progress_bar.value() == 0
    assert main_window.progress_label.text() == '等待任务'


def test_worker_progress_updates_progress_widgets(main_window: MainWindow) -> None:
    event = main_window_module.ProgressEvent(percent=45, stage='fact', message='已拆分事实表')

    main_window._on_worker_progress(event, request_revision=main_window.form_revision)

    assert main_window.progress_bar.value() == 45
    assert main_window.progress_label.text() == '已拆分事实表'
    assert '[progress] fact: 已拆分事实表' in main_window.log_edit.toPlainText()


def test_repeated_progress_stage_logs_once(main_window: MainWindow) -> None:
    first = main_window_module.ProgressEvent(percent=45, stage='fact', message='已拆分事实表')
    second = main_window_module.ProgressEvent(percent=46, stage='fact', message='仍在拆分事实表')

    main_window._on_worker_progress(first, request_revision=main_window.form_revision)
    main_window._on_worker_progress(second, request_revision=main_window.form_revision)

    assert main_window.progress_bar.value() == 46
    assert main_window.log_edit.toPlainText().count('[progress] fact:') == 1


def test_stale_worker_progress_is_ignored_after_form_change(main_window: MainWindow, tmp_path: Path) -> None:
    old_revision = main_window.form_revision
    main_window.input_edit.setText(str(tmp_path / 'changed.xlsx'))
    event = main_window_module.ProgressEvent(percent=70, stage='analysis', message='旧任务进度')

    main_window._on_worker_progress(event, request_revision=old_revision)

    assert main_window.progress_bar.value() == 0
    assert main_window.progress_label.text() == '等待任务'
    assert '旧任务进度' not in main_window.log_edit.toPlainText()


def test_result_widgets_update_kpi_labels(main_window: MainWindow, tmp_path: Path) -> None:
    workbook_path = tmp_path / 'GB-成本计算单_处理后.xlsx'
    result = CostingRunResult(
        status=ServiceStatus.SUCCEEDED,
        message='处理成功',
        workbook_path=workbook_path,
        candidate_products=(('P001', '产品A'), ('P002', '产品B')),
        error_log_count=7,
        stage_timings={'ingest': 0.1, 'analysis': 0.2},
    )

    main_window._update_result_widgets(result)

    assert main_window.error_count_label.text() == '7'
    assert main_window.candidate_count_label.text() == '2'
    assert main_window.workbook_path_label.text() == str(workbook_path)
    assert 'ingest=0.100s' in main_window.stage_label.text()


def test_failed_task_does_not_force_progress_to_complete(main_window: MainWindow) -> None:
    main_window.progress_bar.setValue(45)
    result = CostingRunResult(
        status=ServiceStatus.FAILED,
        message='处理失败',
        error_code='ETL_FAILED',
    )

    main_window._on_worker_finished(result, task_kind='run')

    assert main_window.progress_bar.value() == 45
    assert main_window.progress_label.text() == '处理失败'
```

- [ ] **Step 2: Run failing GUI progress tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_gui_main_window.py::test_progress_widgets_initialize_and_clear tests/test_gui_main_window.py::test_worker_progress_updates_progress_widgets tests/test_gui_main_window.py::test_repeated_progress_stage_logs_once tests/test_gui_main_window.py::test_stale_worker_progress_is_ignored_after_form_change tests/test_gui_main_window.py::test_result_widgets_update_kpi_labels tests/test_gui_main_window.py::test_failed_task_does_not_force_progress_to_complete -q
```

Expected: FAIL because progress widgets, KPI labels, and `_on_worker_progress()` do not exist yet.

- [ ] **Step 3: Update `src/gui/main_window.py` imports**

Add `QProgressBar` to the `PySide6.QtWidgets` import list.

Change the typing import near the top of the file to include `Protocol`:

```python
from typing import Literal, Protocol
```

Add this import near existing GUI/service imports:

```python
from src.services.progress import ProgressCallback, ProgressEvent
```

Change the local `CostingServiceFunction` alias to this protocol:

```python
class CostingServiceFunction(Protocol):
    def __call__(
        self,
        request: CostingRunRequest,
        *,
        progress_callback: ProgressCallback | None = None,
    ) -> CostingRunResult:
        raise NotImplementedError
```

- [ ] **Step 4: Add progress and KPI widgets in `MainWindow.__init__()`**

After `self.summary_label` initialization, add:

```python
        self.error_count_label = QLabel('-')
        self.error_count_label.setObjectName('KpiValue')
        self.candidate_count_label = QLabel('-')
        self.candidate_count_label.setObjectName('KpiValue')
        self.workbook_path_label = QLabel('-')
        self.workbook_path_label.setObjectName('KpiPathValue')
        self.workbook_path_label.setWordWrap(True)
```

After `self.log_edit.setReadOnly(True)`, add:

```python
        self.log_edit.setObjectName('LogTerminal')
        self.log_edit.document().setMaximumBlockCount(1000)
```

After `self.add_candidate_button = QPushButton('加入白名单')`, add:

```python
        self.progress_label = QLabel('等待任务')
        self.progress_label.setObjectName('ProgressLabel')
        self.progress_bar = QProgressBar()
        self.progress_bar.setObjectName('TaskProgressBar')
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(True)
        self._last_progress_stage: str | None = None
```

- [ ] **Step 5: Add helper methods for KPI cards and progress reset**

Add these methods to `MainWindow` near the existing UI helper methods:

```python
    def _kpi_card(self, title: str, value_label: QLabel) -> QWidget:
        card = QWidget()
        card.setObjectName('KpiCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(10, 8, 10, 8)
        layout.setSpacing(4)

        title_label = QLabel(title)
        title_label.setObjectName('KpiTitle')
        layout.addWidget(title_label)
        layout.addWidget(value_label)
        return card

    def _reset_progress(self) -> None:
        self.progress_bar.setValue(0)
        self.progress_label.setText('等待任务')
        self._last_progress_stage = None
```

- [ ] **Step 6: Connect worker progress in `_start_worker()`**

After `worker.signals.started.connect(self._on_worker_started)`, add:

```python
        worker.signals.progress.connect(
            lambda event: self._on_worker_progress(event, request_revision=request_revision)
        )
```

- [ ] **Step 7: Add `_on_worker_progress()`**

Add this method near `_on_worker_started()`:

```python
    def _on_worker_progress(self, event: ProgressEvent, *, request_revision: int | None = None) -> None:
        if self._is_stale_request(request_revision):
            return

        percent = max(0, min(100, int(event.percent)))
        self.progress_bar.setValue(percent)
        self.progress_label.setText(event.message)

        if event.stage != self._last_progress_stage:
            self._append_log(f'[progress] {event.stage}: {event.message}')
            self._last_progress_stage = event.stage
```

- [ ] **Step 8: Update worker start, finish, failure, and clear progress behavior**

In `_on_worker_started()`, add:

```python
        self.progress_bar.setValue(0)
        self.progress_label.setText(label)
        self._last_progress_stage = None
```

In `_on_worker_finished()`, after `_append_result_log(result, task_kind=task_kind)`, add:

```python
        if result.status == ServiceStatus.SUCCEEDED:
            self.progress_bar.setValue(100)
            self.progress_label.setText(result.message)
        else:
            self.progress_label.setText(result.message)
```

In `_on_worker_failed()`, add:

```python
        self.progress_label.setText('任务异常终止')
```

In `_ignore_stale_worker_result()`, add:

```python
        self._reset_progress()
```

In `_clear_conditions()`, replace direct progress field edits with:

```python
        self._reset_progress()
```

- [ ] **Step 9: Update `_update_result_widgets()`**

Replace `_update_result_widgets()` with:

```python
    def _update_result_widgets(self, result: CostingRunResult) -> None:
        timings_text = self._format_stage_timings(result.stage_timings)
        self.stage_label.setText(timings_text or '-')
        self.summary_label.setText(self._format_summary(result))
        self.error_count_label.setText(str(result.error_log_count))
        self.candidate_count_label.setText(str(len(result.candidate_products)))
        self.workbook_path_label.setText(str(result.workbook_path) if result.workbook_path is not None else '-')
```

- [ ] **Step 10: Run GUI progress tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_gui_main_window.py::test_progress_widgets_initialize_and_clear tests/test_gui_main_window.py::test_worker_progress_updates_progress_widgets tests/test_gui_main_window.py::test_repeated_progress_stage_logs_once tests/test_gui_main_window.py::test_stale_worker_progress_is_ignored_after_form_change tests/test_gui_main_window.py::test_result_widgets_update_kpi_labels tests/test_gui_main_window.py::test_failed_task_does_not_force_progress_to_complete -q
```

Expected: PASS.

- [ ] **Step 11: Run main GUI tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_gui_main_window.py tests/test_gui_task_worker.py -q
```

Expected: PASS.

- [ ] **Step 12: Commit progress UI state**

Run:

```bash
git add src/gui/main_window.py tests/test_gui_main_window.py
git commit -m "feat(gui): show ETL progress state"
```

Expected: commit succeeds.

---

### Task 4: Restructure Main Window Layout Into Content, Progress, and Bottom Action Bar

**Files:**
- Modify: `src/gui/main_window.py`
- Modify: `tests/test_gui_main_window.py`

- [ ] **Step 1: Add failing layout structure tests**

Append these tests to `tests/test_gui_main_window.py`:

```python
def test_main_window_exposes_named_layout_regions(main_window: MainWindow) -> None:
    assert main_window.findChild(QWidget, 'MainContentContainer') is not None
    assert main_window.findChild(QWidget, 'LeftPanel') is not None
    assert main_window.findChild(QWidget, 'RightPanel') is not None
    assert main_window.findChild(QWidget, 'ProgressArea') is not None
    assert main_window.findChild(QWidget, 'BottomActionBar') is not None


def test_bottom_action_bar_owns_global_action_buttons(main_window: MainWindow) -> None:
    bottom_bar = main_window.findChild(QWidget, 'BottomActionBar')
    assert bottom_bar is not None
    buttons = bottom_bar.findChildren(QPushButton)

    assert main_window.scan_button in buttons
    assert main_window.precheck_button in buttons
    assert main_window.run_button in buttons
    assert main_window.open_output_button in buttons
    assert main_window.clear_button in buttons
    assert main_window.exit_button in buttons


def test_tables_use_alternating_row_colors(main_window: MainWindow) -> None:
    assert main_window.whitelist_table.alternatingRowColors() is True
    assert main_window.candidate_table.alternatingRowColors() is True
```

Also update the imports near the top of `tests/test_gui_main_window.py`:

```python
from PySide6.QtWidgets import QApplication, QMessageBox, QPushButton, QWidget  # noqa: E402
```

- [ ] **Step 2: Run layout tests and verify failure**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_gui_main_window.py::test_main_window_exposes_named_layout_regions tests/test_gui_main_window.py::test_bottom_action_bar_owns_global_action_buttons tests/test_gui_main_window.py::test_tables_use_alternating_row_colors -q
```

Expected: FAIL because named layout regions and alternating row color flags are not present yet.

- [ ] **Step 3: Add named panel helpers**

Add these helper methods to `MainWindow` near other UI helpers:

```python
    def _panel(self, object_name: str) -> QWidget:
        widget = QWidget()
        widget.setObjectName(object_name)
        return widget

    def _month_range_row(self) -> QWidget:
        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)
        layout.addWidget(self.month_start_edit)
        layout.addWidget(self.month_end_edit)
        return widget
```

- [ ] **Step 4: Replace `_build_ui()` with the three-region layout**

Replace `_build_ui()` with this structure:

```python
    def _build_ui(self) -> None:
        title = QLabel('成本核算分析工具')
        title.setObjectName('TitleLabel')
        subtitle = QLabel('金蝶 ERP 成本计算单处理')
        subtitle.setObjectName('SubtitleLabel')

        config_group = QGroupBox('输入配置')
        config_layout = QFormLayout(config_group)
        config_layout.addRow('管线', self.pipeline_combo)
        config_layout.addRow(
            '输入文件',
            self._path_row(
                self.input_edit,
                '选择文件',
                self._choose_input_file,
                second_text='自动查找',
                second_slot=self._auto_find_input,
            ),
        )
        config_layout.addRow('输出目录', self._path_row(self.output_edit, '选择目录', self._choose_output_dir))
        config_layout.addRow('月份范围', self._month_range_row())
        self.month_start_edit.setPlaceholderText('开始 YYYY-MM，可留空')
        self.month_end_edit.setPlaceholderText('结束 YYYY-MM，可留空')

        whitelist_group = QGroupBox('产品白名单池')
        whitelist_layout = QVBoxLayout(whitelist_group)
        self._setup_table(self.whitelist_table, editable=True)
        whitelist_layout.addWidget(self.whitelist_table)
        whitelist_layout.addLayout(self._whitelist_buttons())

        candidate_group = QGroupBox('候选产品')
        candidate_layout = QVBoxLayout(candidate_group)
        self._setup_table(self.candidate_table, editable=False)
        candidate_layout.addWidget(self.candidate_table)
        self.add_candidate_button.clicked.connect(self._add_selected_candidates)
        candidate_layout.addWidget(self.add_candidate_button, alignment=Qt.AlignmentFlag.AlignHCenter)

        left_panel = self._panel('LeftPanel')
        left = QVBoxLayout(left_panel)
        left.setContentsMargins(0, 0, 0, 0)
        left.setSpacing(10)
        left.addWidget(title)
        left.addWidget(subtitle)
        left.addWidget(config_group)
        left.addWidget(whitelist_group, stretch=1)
        left.addWidget(candidate_group, stretch=1)

        status_group = QGroupBox('任务状态')
        status_group.setObjectName('StatusDashboard')
        status_layout = QFormLayout(status_group)
        status_layout.addRow('当前状态', self.status_label)
        status_layout.addRow('阶段耗时', self.stage_label)

        kpi_group = QGroupBox('结果概要')
        kpi_group.setObjectName('KpiDashboard')
        kpi_layout = QGridLayout(kpi_group)
        kpi_layout.addWidget(self._kpi_card('error_log 行数', self.error_count_label), 0, 0)
        kpi_layout.addWidget(self._kpi_card('候选产品', self.candidate_count_label), 0, 1)
        kpi_layout.addWidget(self._kpi_card('输出路径', self.workbook_path_label), 1, 0, 1, 2)
        self.summary_label.setVisible(False)

        log_group = QGroupBox('日志')
        log_group.setObjectName('LogGroup')
        log_layout = QVBoxLayout(log_group)
        log_layout.addWidget(self.log_edit)

        right_panel = self._panel('RightPanel')
        right = QVBoxLayout(right_panel)
        right.setContentsMargins(0, 0, 0, 0)
        right.setSpacing(10)
        right.addWidget(status_group)
        right.addWidget(kpi_group)
        right.addWidget(log_group, stretch=1)

        main_content = self._panel('MainContentContainer')
        main_content_layout = QGridLayout(main_content)
        main_content_layout.setContentsMargins(0, 0, 0, 0)
        main_content_layout.setHorizontalSpacing(14)
        main_content_layout.addWidget(left_panel, 0, 0)
        main_content_layout.addWidget(right_panel, 0, 1)
        main_content_layout.setColumnStretch(0, 2)
        main_content_layout.setColumnStretch(1, 3)

        progress_area = self._panel('ProgressArea')
        progress_layout = QVBoxLayout(progress_area)
        progress_layout.setContentsMargins(0, 0, 0, 0)
        progress_layout.setSpacing(6)
        progress_layout.addWidget(self.progress_label)
        progress_layout.addWidget(self.progress_bar)

        bottom_bar = self._panel('BottomActionBar')
        button_layout = QHBoxLayout(bottom_bar)
        button_layout.setContentsMargins(0, 0, 0, 0)
        button_layout.setSpacing(10)
        button_layout.addStretch(1)
        for button in (
            self.scan_button,
            self.precheck_button,
            self.run_button,
            self.open_output_button,
            self.clear_button,
            self.exit_button,
        ):
            button_layout.addWidget(button)

        root_layout = QVBoxLayout()
        root_layout.setContentsMargins(14, 14, 14, 14)
        root_layout.setSpacing(12)
        root_layout.addWidget(main_content, stretch=1)
        root_layout.addWidget(progress_area)
        root_layout.addWidget(bottom_bar)

        root = QWidget()
        root.setLayout(root_layout)
        self.setCentralWidget(root)
```

- [ ] **Step 5: Enable alternating row colors in `_setup_table()`**

Inside `_setup_table()`, after `table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)`, add:

```python
        table.setAlternatingRowColors(True)
```

- [ ] **Step 6: Run layout tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_gui_main_window.py::test_main_window_exposes_named_layout_regions tests/test_gui_main_window.py::test_bottom_action_bar_owns_global_action_buttons tests/test_gui_main_window.py::test_tables_use_alternating_row_colors -q
```

Expected: PASS.

- [ ] **Step 7: Run GUI main window tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_gui_main_window.py -q
```

Expected: PASS.

- [ ] **Step 8: Commit layout restructuring**

Run:

```bash
git add src/gui/main_window.py tests/test_gui_main_window.py
git commit -m "feat(gui): restructure dashboard layout"
```

Expected: commit succeeds.

---

### Task 5: Apply Muted Slate Theme and Preserve Light Confirmation Dialogs

**Files:**
- Modify: `src/gui/styles.py`
- Modify: `tests/test_gui_styles.py`
- Modify: `tests/test_gui_main_window.py`

- [ ] **Step 1: Replace GUI style tests with muted slate expectations**

Replace `tests/test_gui_styles.py` with:

```python
from __future__ import annotations

from src.gui.styles import APP_STYLESHEET, MESSAGE_BOX_STYLESHEET


def test_stylesheet_uses_muted_slate_main_window_theme() -> None:
    assert 'QMainWindow {' in APP_STYLESHEET
    assert 'background: #1E222B;' in APP_STYLESHEET
    assert 'color: #E5E7EB;' in APP_STYLESHEET
    assert 'QWidget#MainContentContainer' in APP_STYLESHEET


def test_stylesheet_pins_dark_control_text_and_selection_colors() -> None:
    assert 'QLineEdit,' in APP_STYLESHEET
    assert 'QComboBox {' in APP_STYLESHEET
    assert 'background: #1F2430;' in APP_STYLESHEET
    assert 'color: #E5E7EB;' in APP_STYLESHEET
    assert 'selection-background-color: #2B6CB0;' in APP_STYLESHEET
    assert 'selection-color: #FFFFFF;' in APP_STYLESHEET


def test_stylesheet_pins_table_selection_and_zebra_colors() -> None:
    assert 'QTableWidget {' in APP_STYLESHEET
    assert 'alternate-background-color: #2C313C;' in APP_STYLESHEET
    assert 'QTableWidget::item:selected {' in APP_STYLESHEET
    assert 'background: #2B6CB0;' in APP_STYLESHEET
    assert 'color: #FFFFFF;' in APP_STYLESHEET


def test_stylesheet_styles_log_terminal_and_progress_bar() -> None:
    assert 'QTextEdit#LogTerminal {' in APP_STYLESHEET
    assert 'background: #181A1F;' in APP_STYLESHEET
    assert 'font-family: Consolas, \"Fira Code\", monospace;' in APP_STYLESHEET
    assert 'QProgressBar#TaskProgressBar {' in APP_STYLESHEET
    assert 'QProgressBar#TaskProgressBar::chunk {' in APP_STYLESHEET


def test_message_box_stylesheet_remains_light_for_confirmation_dialogs() -> None:
    assert 'QMessageBox {' in MESSAGE_BOX_STYLESHEET
    assert 'background: #f8fafc;' in MESSAGE_BOX_STYLESHEET
    assert 'color: #111827;' in MESSAGE_BOX_STYLESHEET
```

Update `test_confirmation_dialog_uses_light_theme_and_chinese_buttons()` in `tests/test_gui_main_window.py` so it continues to assert:

```python
    assert 'background: #f8fafc;' in message_box.styleSheet()
    assert 'color: #111827;' in message_box.styleSheet()
```

- [ ] **Step 2: Run style tests and verify failure**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_gui_styles.py tests/test_gui_main_window.py::test_confirmation_dialog_uses_light_theme_and_chinese_buttons -q
```

Expected: FAIL because the current main stylesheet is still light.

- [ ] **Step 3: Replace `STATUS_COLORS` in `src/gui/styles.py`**

Use:

```python
STATUS_COLORS = {
    'idle': '#A0AEC0',
    'busy': '#63B3ED',
    'success': '#48BB78',
    'failed': '#E53E3E',
}
```

- [ ] **Step 4: Replace `APP_STYLESHEET` in `src/gui/styles.py`**

Replace the existing `APP_STYLESHEET` string with:

```python
APP_STYLESHEET = """
QMainWindow {
    background: #1E222B;
}
QWidget {
    color: #E5E7EB;
    font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
    font-size: 13px;
}
QWidget#MainContentContainer,
QWidget#ProgressArea,
QWidget#BottomActionBar {
    background: transparent;
}
QWidget#LeftPanel,
QWidget#RightPanel {
    background: transparent;
}
QLabel#TitleLabel {
    font-size: 22px;
    font-weight: 700;
    color: #F7FAFC;
}
QLabel#SubtitleLabel,
QLabel#ProgressLabel,
QLabel#KpiTitle {
    color: #A0AEC0;
}
QLabel#StatusLabel,
QLabel#KpiValue {
    font-weight: 700;
    color: #E5E7EB;
}
QLabel#KpiValue {
    font-size: 20px;
}
QLabel#KpiPathValue {
    color: #E5E7EB;
}
QWidget#KpiCard {
    border: 1px solid #3E4451;
    border-radius: 6px;
    background: #252932;
}
QGroupBox {
    border: 1px solid #3E4451;
    border-radius: 6px;
    margin-top: 10px;
    background: #252932;
}
QGroupBox::title {
    subcontrol-origin: margin;
    left: 10px;
    padding: 0 4px;
    color: #A0AEC0;
    font-weight: 600;
}
QLineEdit,
QComboBox {
    min-height: 30px;
    padding: 3px 8px;
    border: 1px solid #3E4451;
    border-radius: 4px;
    background: #1F2430;
    color: #E5E7EB;
    selection-background-color: #2B6CB0;
    selection-color: #FFFFFF;
}
QLineEdit:focus,
QComboBox:focus {
    border-color: #3182CE;
}
QComboBox QAbstractItemView {
    border: 1px solid #3E4451;
    background: #1F2430;
    color: #E5E7EB;
    selection-background-color: #2B6CB0;
    selection-color: #FFFFFF;
    outline: 0;
}
QComboBox QAbstractItemView::item {
    min-height: 28px;
    padding: 4px 8px;
}
QComboBox QAbstractItemView::item:selected {
    background: #2B6CB0;
    color: #FFFFFF;
}
QPushButton {
    min-height: 32px;
    padding: 4px 12px;
    border: 1px solid #4A5568;
    border-radius: 4px;
    background: #2D3748;
    color: #E5E7EB;
}
QPushButton:hover {
    background: #3A465A;
}
QPushButton:disabled {
    color: #718096;
    background: #252932;
    border-color: #3E4451;
}
QPushButton#PrimaryButton {
    background: #2B6CB0;
    color: #FFFFFF;
    border: 1px solid #2B6CB0;
    font-weight: 700;
}
QPushButton#PrimaryButton:hover {
    background: #3182CE;
    border-color: #3182CE;
}
QTableWidget {
    gridline-color: #3E4451;
    background: #252932;
    alternate-background-color: #2C313C;
    color: #E5E7EB;
    selection-background-color: #2B6CB0;
    selection-color: #FFFFFF;
    border: 1px solid #3E4451;
    border-radius: 4px;
}
QTableWidget::item:selected {
    background: #2B6CB0;
    color: #FFFFFF;
}
QHeaderView::section {
    padding: 6px;
    border: 0;
    border-right: 1px solid #3E4451;
    border-bottom: 1px solid #3E4451;
    background: #1F2430;
    color: #E5E7EB;
    font-weight: 600;
}
QTextEdit#LogTerminal {
    background: #181A1F;
    color: #E5E7EB;
    border: 1px solid #3E4451;
    border-radius: 4px;
    padding: 10px;
    font-family: Consolas, "Fira Code", monospace;
}
QProgressBar#TaskProgressBar {
    min-height: 14px;
    border: 1px solid #3E4451;
    border-radius: 4px;
    background: #1F2430;
    color: #E5E7EB;
    text-align: center;
}
QProgressBar#TaskProgressBar::chunk {
    border-radius: 3px;
    background: #2B6CB0;
}
QScrollBar:vertical {
    width: 14px;
    margin: 0;
    border: 1px solid #3E4451;
    border-radius: 4px;
    background: #1F2430;
}
QScrollBar::handle:vertical {
    min-height: 24px;
    margin: 2px;
    border-radius: 5px;
    background: #4A5568;
}
QScrollBar::handle:vertical:hover {
    background: #718096;
}
QScrollBar::add-line:vertical,
QScrollBar::sub-line:vertical {
    height: 0;
    border: 0;
    background: transparent;
}
QScrollBar::add-page:vertical,
QScrollBar::sub-page:vertical {
    background: transparent;
}
QScrollBar:horizontal {
    height: 14px;
    margin: 0;
    border: 1px solid #3E4451;
    border-radius: 4px;
    background: #1F2430;
}
QScrollBar::handle:horizontal {
    min-width: 24px;
    margin: 2px;
    border-radius: 5px;
    background: #4A5568;
}
QScrollBar::handle:horizontal:hover {
    background: #718096;
}
QScrollBar::add-line:horizontal,
QScrollBar::sub-line:horizontal {
    width: 0;
    border: 0;
    background: transparent;
}
QScrollBar::add-page:horizontal,
QScrollBar::sub-page:horizontal {
    background: transparent;
}
"""
```

- [ ] **Step 5: Keep `MESSAGE_BOX_STYLESHEET` light**

Verify `MESSAGE_BOX_STYLESHEET` still contains:

```python
MESSAGE_BOX_STYLESHEET = """
QMessageBox {
    background: #f8fafc;
    color: #111827;
}
```

Do not replace the confirmation dialog stylesheet with the dark theme.

- [ ] **Step 6: Run style tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_gui_styles.py tests/test_gui_main_window.py::test_confirmation_dialog_uses_light_theme_and_chinese_buttons -q
```

Expected: PASS.

- [ ] **Step 7: Run GUI tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_gui_styles.py tests/test_gui_main_window.py tests/test_gui_task_worker.py -q
```

Expected: PASS.

- [ ] **Step 8: Commit muted slate theme**

Run:

```bash
git add src/gui/styles.py tests/test_gui_styles.py tests/test_gui_main_window.py
git commit -m "style(gui): apply muted slate dashboard theme"
```

Expected: commit succeeds.

---

### Task 6: Final Integration Checks and Manual GUI Smoke Test

**Files:**
- No required source edits
- Possible fixes: files touched by Tasks 1 through 5 if verification exposes a regression

- [ ] **Step 1: Run GUI and progress focused tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_gui_styles.py tests/test_gui_form_state.py tests/test_gui_task_worker.py tests/test_gui_main_window.py tests/test_costing_service.py tests/test_etl_pipeline.py -q
```

Expected: PASS.

- [ ] **Step 2: Run full test suite**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests -q
```

Expected: PASS.

- [ ] **Step 3: Run ruff check**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m ruff check src tests
```

Expected: PASS.

- [ ] **Step 4: Run ruff format check**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m ruff format src tests --check
```

Expected: PASS.

- [ ] **Step 5: Start GUI for smoke test**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m src.gui.app
```

Expected:

- Main window opens.
- Main window uses muted slate dark theme.
- Confirmation dialogs remain light and readable.
- `扫描产品`、`预检`、`开始处理` use the same progress area.
- Log terminal is readable and capped to 1000 blocks.
- Table selected rows have blue background and white text.
- Bottom action buttons stay in the bottom bar.

Stop the GUI manually after smoke testing.

- [ ] **Step 6: Check working tree and staged changes**

Run:

```bash
git status --short
```

Expected: only intentional implementation files are modified, or the working tree is clean if every task commit has been made.

- [ ] **Step 7: Commit final verification fixes if any were needed**

If Step 1 through Step 5 required small fixes, commit them:

```bash
git add src tests
git commit -m "fix(gui): polish progress dashboard integration"
```

Expected: commit succeeds if fixes exist. If no fixes exist, skip this commit.

## Execution Order Recommendation

Start with Task 1 rather than `src/gui/styles.py` or `src/gui/main_window.py`. The progress contract is the stable interface that the worker and UI will consume. Once the data flow is stable, implement worker forwarding, then progress widgets and layout, and apply the QSS theme last so visual selectors target real object names.

## Final Verification Checklist

- `ProgressEvent` is not declared in `costing_service.py` only; it lives in a neutral module to avoid circular imports.
- `WorkerSignals.progress = Signal(object)` is a class attribute.
- Main dark QSS is applied with `self.setStyleSheet(APP_STYLESHEET)` on the window, not through `QApplication.setStyleSheet()`.
- `MESSAGE_BOX_STYLESHEET` remains light.
- `QTableWidget::item:selected` sets both background and text color.
- `self.log_edit.document().setMaximumBlockCount(1000)` is present.
- `summary_label` still exists.
- Existing widget variable names remain unchanged.
- `扫描产品` and `预检` do not report `export`.
- `开始处理` reports `export` and `done` on success.
- Failed tasks do not force the progress bar to 100.

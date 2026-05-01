# Performance Benchmark And Check-Only Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a no-output `--check-only` preflight mode and a stable `--benchmark` console summary for GB/SK costing pipelines.

**Architecture:** Keep `CostingWorkbookETL.process_file()` as the normal export path, and add a side-effect-light `prepare_payload()` method that runs the same payload build used by exports. `run_pipeline()` chooses between normal export and check-only, then formats quality and benchmark text from `CostingWorkbookETL` state. CLI parsing remains in `main.py`.

**Tech Stack:** Python 3.11, argparse, pandas, pytest, existing `CostingWorkbookETL`, existing `WorkbookPayload`.

---

## File Map

- Modify: `main.py`
  - Add `--check-only` and `--benchmark` flags.
  - Pass both flags to `run_pipeline()`.
- Modify: `src/etl/costing_etl.py`
  - Add `prepare_payload(input_path: Path) -> bool`.
  - Factor shared payload state assignment into a private helper.
  - Preserve `process_file()` behavior.
- Modify: `src/etl/runner.py`
  - Add `check_only` and `benchmark` parameters to `run_pipeline()`.
  - Add `build_benchmark_log_text(...)`.
  - In check-only mode, build payload and print quality summary without writing workbook or CSV.
- Modify: `tests/test_main.py`
  - Add CLI parsing coverage for new flags.
- Modify: `tests/test_runner.py`
  - Add check-only no-write behavior coverage.
  - Add benchmark text coverage for normal and check-only modes.
- Modify: `tests/test_costing_etl.py`
  - Add `prepare_payload()` behavior coverage.

## Task 1: CLI Flags

**Files:**
- Modify: `main.py`
- Test: `tests/test_main.py`

- [ ] **Step 1: Write failing CLI test**

Append this test to `tests/test_main.py`:

```python
def test_main_passes_check_only_and_benchmark_flags_to_runner(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run_pipeline(config, month_range=None, check_only=False, benchmark=False):
        captured['config'] = config
        captured['month_range'] = month_range
        captured['check_only'] = check_only
        captured['benchmark'] = benchmark
        return 0

    monkeypatch.setattr('main.run_pipeline', _fake_run_pipeline)

    exit_code = main(['sk', '--check-only', '--benchmark'])

    assert exit_code == 0
    assert captured['check_only'] is True
    assert captured['benchmark'] is True
    assert captured['month_range'] is None
```

- [ ] **Step 2: Run CLI test to verify it fails**

Run:

```powershell
conda run -n test python -m pytest tests/test_main.py::test_main_passes_check_only_and_benchmark_flags_to_runner -q
```

Expected: FAIL because `argparse` does not know `--check-only` / `--benchmark`.

- [ ] **Step 3: Add CLI flags**

Modify `main.py`:

```python
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='成本核算 ETL 统一入口')
    parser.add_argument('pipeline', choices=sorted(PIPELINES), help='选择要运行的管线: gb 或 sk')
    parser.add_argument('--month-start', dest='month_start', help='起始月份，格式 YYYY-MM')
    parser.add_argument('--month-end', dest='month_end', help='结束月份，格式 YYYY-MM')
    parser.add_argument('--check-only', action='store_true', help='只执行预检和质量校验，不写出 workbook 或 CSV')
    parser.add_argument('--benchmark', action='store_true', help='输出稳定的阶段耗时和文件规模摘要')
    return parser
```

Modify the return call in `main()`:

```python
return run_pipeline(
    PIPELINES[args.pipeline],
    month_range=month_range,
    check_only=args.check_only,
    benchmark=args.benchmark,
)
```

- [ ] **Step 4: Run CLI tests**

Run:

```powershell
conda run -n test python -m pytest tests/test_main.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```powershell
git add main.py tests/test_main.py
git commit -m "feat(cli): add check-only and benchmark flags"
```

## Task 2: Payload Preflight Method

**Files:**
- Modify: `src/etl/costing_etl.py`
- Test: `tests/test_costing_etl.py`

- [ ] **Step 1: Write failing ETL preflight test**

Append this test near the existing `process_file` payload tests in `tests/test_costing_etl.py`:

```python
def test_prepare_payload_builds_pipeline_payload_without_writing_workbook(tmp_path: Path) -> None:
    etl = CostingWorkbookETL(
        skip_rows=2,
        product_order=(),
        standalone_cost_items=('委外加工费',),
        product_anomaly_scope_mode='doc_type_split',
        month_range=MonthRange(start='2025-01', end='2025-03'),
    )
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
        quality_metrics=(QualityMetric('行数勾稽', '产品数量统计输出行数', '1', '仅保留有效工单'),),
        error_log_count=1,
        stage_timings={'ingest': 0.1, 'normalize': 0.2},
        error_log_export=pd.DataFrame([{'row_id': 'WO-001', 'issue_type': 'MISSING_AMOUNT'}]),
    )

    with (
        patch.object(etl.pipeline, 'build_workbook_payload', return_value=payload) as payload_mock,
        patch.object(etl.workbook_writer, 'write_workbook_from_models') as writer_mock,
    ):
        assert etl.prepare_payload(tmp_path / 'input.xlsx') is True

    payload_mock.assert_called_once()
    writer_mock.assert_not_called()
    assert etl.last_quality_metrics == payload.quality_metrics
    assert etl.last_error_log_count == 1
    pd.testing.assert_frame_equal(etl.last_error_log_frame, payload.error_log_export)
    assert etl.last_stage_timings == {'ingest': 0.1, 'normalize': 0.2}
```

- [ ] **Step 2: Run preflight test to verify it fails**

Run:

```powershell
conda run -n test python -m pytest tests/test_costing_etl.py::test_prepare_payload_builds_pipeline_payload_without_writing_workbook -q
```

Expected: FAIL because `CostingWorkbookETL.prepare_payload` does not exist.

- [ ] **Step 3: Implement payload preflight state**

In `src/etl/costing_etl.py`, add `last_stage_timings` in `__init__`:

```python
self.last_stage_timings: dict[str, float] = {}
```

Add private helper inside `CostingWorkbookETL`:

```python
def _reset_last_run_state(self) -> None:
    self.last_quality_metrics = ()
    self.last_error_log_count = 0
    self.last_error_log_frame = pd.DataFrame()
    self.last_month_filter_summary = None
    self.last_stage_timings = {}

def _apply_payload_state(self, payload: WorkbookPayload) -> None:
    self.last_month_filter_summary = self.pipeline.last_month_filter_summary
    self.last_quality_metrics = payload.quality_metrics
    self.last_error_log_count = payload.error_log_count
    self.last_error_log_frame = payload.error_log_export.copy()
    self.last_stage_timings = dict(payload.stage_timings)
```

Add method:

```python
def prepare_payload(self, input_path: Path) -> bool:
    """构建 workbook payload 但不写出文件，用于 check-only 预检。"""
    try:
        self._reset_last_run_state()
        logger.info('Preparing payload for file: %s', input_path)
        payload = self.pipeline.build_workbook_payload(
            input_path,
            standalone_cost_items=self.standalone_cost_items,
            product_anomaly_scope_mode=self.product_anomaly_scope_mode,
            month_range=self.month_range,
            artifacts_transform=self._filter_analysis_artifacts_by_whitelist,
        )
        self._apply_payload_state(payload)
        self._log_quality_metrics(self.last_quality_metrics)
        logger.info('Quality issue count | error_log_rows=%s', self.last_error_log_count)
        for stage_name, seconds in payload.stage_timings.items():
            logger.info('Timing | stage=%s | seconds=%.3f', stage_name, seconds)
        return True
    except Exception as exc:
        self.last_error_log_frame = pd.DataFrame()
        logger.error('Payload preparation failed: %s', exc, exc_info=True)
        return False
```

Update `process_file()` to call `_reset_last_run_state()` and `_apply_payload_state(payload)` instead of repeating assignments.

- [ ] **Step 4: Run ETL preflight tests**

Run:

```powershell
conda run -n test python -m pytest tests/test_costing_etl.py::test_prepare_payload_builds_pipeline_payload_without_writing_workbook tests/test_costing_etl.py::test_process_file_uses_workbook_payload_and_logs_all_new_stage_timings -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```powershell
git add src/etl/costing_etl.py tests/test_costing_etl.py
git commit -m "feat(etl): add payload preflight mode"
```

## Task 3: Check-Only Runner Mode

**Files:**
- Modify: `src/etl/runner.py`
- Test: `tests/test_runner.py`

- [ ] **Step 1: Write failing runner check-only test**

Append this test to `tests/test_runner.py`:

```python
def test_run_pipeline_check_only_builds_payload_without_writing_outputs(monkeypatch, capsys, tmp_path) -> None:
    input_file = tmp_path / 'GB-成本计算单.xlsx'
    input_file.write_text('placeholder', encoding='utf-8')
    processed_dir = tmp_path / 'processed'
    config = PipelineConfig(
        name='gb',
        raw_dir=tmp_path,
        processed_dir=processed_dir,
        input_patterns=('GB-*.xlsx',),
        product_order=(('GB_C.D.B0040AA', 'BMS-750W驱动器'),),
        standalone_cost_items=('委外加工费',),
        product_anomaly_scope_mode='doc_type_split',
    )

    captured: dict[str, Path] = {}

    class _DummyETL:
        def __init__(self, skip_rows: int, *, product_order, standalone_cost_items, product_anomaly_scope_mode, month_range=None) -> None:
            self.last_quality_metrics = (
                QualityMetric('行数勾稽', '产品数量统计输出行数', '3', '仅保留有效工单'),
            )
            self.last_error_log_count = 2
            self.last_error_log_frame = pd.DataFrame([{'row_id': 'WO-001', 'issue_type': 'MISSING_AMOUNT'}])
            self.last_month_filter_summary = None
            self.last_stage_timings = {'ingest': 0.1, 'normalize': 0.2}

        def prepare_payload(self, input_path: Path) -> bool:
            captured['input_path'] = input_path
            return True

        def process_file(self, input_path: Path, output_path: Path) -> bool:
            raise AssertionError('check-only must not write workbook')

    monkeypatch.setattr('src.etl.runner.CostingWorkbookETL', _DummyETL)

    exit_code = run_pipeline(config, check_only=True)
    stdout = capsys.readouterr().out

    assert exit_code == 0
    assert captured['input_path'] == input_file
    assert 'mode=check-only' in stdout
    assert 'pipeline=gb' in stdout
    assert 'output=' in stdout
    assert not processed_dir.exists()
```

- [ ] **Step 2: Run check-only test to verify it fails**

Run:

```powershell
conda run -n test python -m pytest tests/test_runner.py::test_run_pipeline_check_only_builds_payload_without_writing_outputs -q
```

Expected: FAIL because `run_pipeline()` does not accept `check_only`.

- [ ] **Step 3: Implement check-only branch**

Modify `run_pipeline()` signature:

```python
def run_pipeline(
    config: PipelineConfig,
    month_range: MonthRange | None = None,
    *,
    check_only: bool = False,
    benchmark: bool = False,
) -> int:
```

Inside `run_pipeline()`, instantiate ETL as today. Only create `processed_dir` when not `check_only`:

```python
if not check_only:
    config.processed_dir.mkdir(parents=True, exist_ok=True)
output_file, error_log_csv_file = _build_output_paths(config.processed_dir, input_file, month_range)
```

Before normal export branch:

```python
if check_only:
    if not etl.prepare_payload(input_file):
        logger.error('预检失败: %s', input_file.name)
        return 1
    quality_log = build_quality_log_text(
        pipeline_name=config.name,
        input_path=input_file,
        output_path=output_file,
        error_log_count=etl.last_error_log_count,
        quality_metrics=etl.last_quality_metrics,
        month_filter_summary=getattr(etl, 'last_month_filter_summary', None),
    )
    print('mode=check-only')
    print(quality_log)
    logger.info('预检成功: %s', input_file.name)
    return 0
```

- [ ] **Step 4: Run runner check-only tests**

Run:

```powershell
conda run -n test python -m pytest tests/test_runner.py::test_run_pipeline_check_only_builds_payload_without_writing_outputs tests/test_runner.py::test_run_pipeline_prints_quality_summary_without_writing_log_file -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```powershell
git add src/etl/runner.py tests/test_runner.py
git commit -m "feat(etl): add check-only runner mode"
```

## Task 4: Benchmark Summary

**Files:**
- Modify: `src/etl/runner.py`
- Test: `tests/test_runner.py`

- [ ] **Step 1: Write failing benchmark text tests**

Append these tests to `tests/test_runner.py`:

```python
def test_build_benchmark_log_text_reports_stage_timings_and_file_sizes(tmp_path) -> None:
    input_file = tmp_path / 'input.xlsx'
    output_file = tmp_path / 'output.xlsx'
    error_log_file = tmp_path / 'error_log.csv'
    input_file.write_bytes(b'abc')
    output_file.write_bytes(b'output')
    error_log_file.write_bytes(b'csv')

    from src.etl.runner import build_benchmark_log_text

    text = build_benchmark_log_text(
        input_path=input_file,
        output_path=output_file,
        error_log_path=error_log_file,
        error_log_count=4,
        stage_timings={'ingest': 0.1, 'normalize': 0.2},
        output_written=True,
    )

    assert '[benchmark]' in text
    assert 'input_size_bytes=3' in text
    assert 'output_size_bytes=6' in text
    assert 'error_log_size_bytes=3' in text
    assert 'stage_ingest_seconds=0.100' in text
    assert 'stage_normalize_seconds=0.200' in text
    assert 'error_log_count=4' in text


def test_run_pipeline_check_only_benchmark_prints_planned_output_without_writing(monkeypatch, capsys, tmp_path) -> None:
    input_file = tmp_path / 'SK-成本计算单.xlsx'
    input_file.write_bytes(b'raw')
    processed_dir = tmp_path / 'processed'
    config = PipelineConfig(
        name='sk',
        raw_dir=tmp_path,
        processed_dir=processed_dir,
        input_patterns=('SK-*.xlsx',),
        product_order=(('DP.C.P0197AA', '动力线'),),
        product_anomaly_scope_mode='legacy_single_scope',
    )

    class _DummyETL:
        def __init__(self, skip_rows: int, *, product_order, standalone_cost_items, product_anomaly_scope_mode, month_range=None) -> None:
            self.last_quality_metrics = ()
            self.last_error_log_count = 0
            self.last_error_log_frame = pd.DataFrame()
            self.last_month_filter_summary = None
            self.last_stage_timings = {'ingest': 0.5}

        def prepare_payload(self, input_path: Path) -> bool:
            return True

    monkeypatch.setattr('src.etl.runner.CostingWorkbookETL', _DummyETL)

    exit_code = run_pipeline(config, check_only=True, benchmark=True)
    stdout = capsys.readouterr().out

    assert exit_code == 0
    assert '[benchmark]' in stdout
    assert 'output_written=false' in stdout
    assert 'input_size_bytes=3' in stdout
    assert 'stage_ingest_seconds=0.500' in stdout
    assert not processed_dir.exists()
```

- [ ] **Step 2: Run benchmark tests to verify they fail**

Run:

```powershell
conda run -n test python -m pytest tests/test_runner.py::test_build_benchmark_log_text_reports_stage_timings_and_file_sizes tests/test_runner.py::test_run_pipeline_check_only_benchmark_prints_planned_output_without_writing -q
```

Expected: FAIL because `build_benchmark_log_text` does not exist and benchmark printing is absent.

- [ ] **Step 3: Implement benchmark text**

Add helper to `src/etl/runner.py`:

```python
def _file_size_or_zero(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def build_benchmark_log_text(
    *,
    input_path: Path,
    output_path: Path,
    error_log_path: Path,
    error_log_count: int,
    stage_timings: dict[str, float],
    output_written: bool,
) -> str:
    """构建稳定 benchmark 文本，测试只依赖字段存在，不断言秒数快慢。"""
    lines = [
        '',
        '[benchmark]',
        f'output_written={str(output_written).lower()}',
        f'input_size_bytes={_file_size_or_zero(input_path)}',
        f'output_size_bytes={_file_size_or_zero(output_path) if output_written else 0}',
        f'error_log_size_bytes={_file_size_or_zero(error_log_path) if output_written else 0}',
        f'planned_output={output_path}',
        f'planned_error_log={error_log_path}',
        f'error_log_count={error_log_count}',
    ]
    total = 0.0
    for stage_name in sorted(stage_timings):
        seconds = float(stage_timings[stage_name])
        total += seconds
        lines.append(f'stage_{stage_name}_seconds={seconds:.3f}')
    lines.append(f'stage_total_observed_seconds={total:.3f}')
    return '\n'.join(lines)
```

In check-only branch, after quality log:

```python
if benchmark:
    print(
        build_benchmark_log_text(
            input_path=input_file,
            output_path=output_file,
            error_log_path=error_log_csv_file,
            error_log_count=etl.last_error_log_count,
            stage_timings=getattr(etl, 'last_stage_timings', {}),
            output_written=False,
        )
    )
```

In normal branch, after quality log:

```python
if benchmark:
    print(
        build_benchmark_log_text(
            input_path=input_file,
            output_path=output_file,
            error_log_path=error_log_csv_file,
            error_log_count=etl.last_error_log_count,
            stage_timings=getattr(etl, 'last_stage_timings', {}),
            output_written=True,
        )
    )
```

- [ ] **Step 4: Run benchmark tests**

Run:

```powershell
conda run -n test python -m pytest tests/test_runner.py::test_build_benchmark_log_text_reports_stage_timings_and_file_sizes tests/test_runner.py::test_run_pipeline_check_only_benchmark_prints_planned_output_without_writing -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```powershell
git add src/etl/runner.py tests/test_runner.py
git commit -m "feat(etl): add benchmark summary output"
```

## Task 5: Verification

**Files:**
- No code changes unless verification exposes a defect.

- [ ] **Step 1: Run focused test suite**

Run:

```powershell
conda run -n test python -m pytest tests/test_main.py tests/test_runner.py tests/test_costing_etl.py -q
```

Expected: PASS.

- [ ] **Step 2: Run full test suite**

Run:

```powershell
conda run -n test python -m pytest tests -q
```

Expected: PASS.

- [ ] **Step 3: Run real GB check-only benchmark**

Run:

```powershell
conda run -n test python main.py gb --check-only --benchmark
```

Expected: exits `0`, prints `mode=check-only`, `pipeline=gb`, and `[benchmark]`; does not create new workbook or CSV.

- [ ] **Step 4: Run real SK check-only benchmark**

Run:

```powershell
conda run -n test python main.py sk --check-only --benchmark
```

Expected: exits `0`, prints `mode=check-only`, `pipeline=sk`, and `[benchmark]`; does not create new workbook or CSV.

- [ ] **Step 5: Commit verification fixes if needed**

If verification required fixes, run:

```powershell
git add <changed-files>
git commit -m "fix(etl): stabilize check-only benchmark mode"
```

If no fixes were needed, do not create an empty commit.


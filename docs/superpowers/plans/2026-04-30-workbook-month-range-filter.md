# Workbook Month Range Filter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 `gb` / `sk` 统一入口增加可选的月份区间参数，并在标准化成本表阶段执行全链路月份过滤，同时支持空命中输出与带月份后缀的导出命名。

**Architecture:** 新增 `src/etl/month_filter.py` 承载 `MonthRange` 和过滤摘要逻辑；`main.py` 只负责解析 `--month-start` / `--month-end` 并构造 `MonthRange`；`runner.py` 负责输出命名和控制台摘要；`CostingWorkbookETL` / `CostingEtlPipeline` 只在 `NormalizedCostFrame` 阶段做一次正式过滤，然后把过滤后的结果继续传给拆表、分析和导出链路。空命中仍走成功路径，但质量指标和控制台摘要要明确说明“月份过滤后无数据”。

**Tech Stack:** Python 3.11+, argparse, pandas, polars, openpyxl, pytest, Ruff

---

## File Map

- Create: `D:/03- Program/02- special/02- costing_calculate/src/etl/month_filter.py`
  责任：实现月份参数标准化、输出后缀生成、标准化成本表过滤和过滤摘要对象。
- Modify: `D:/03- Program/02- special/02- costing_calculate/main.py`
  责任：解析 `--month-start` / `--month-end` 并把 `MonthRange | None` 传给 runner。
- Modify: `D:/03- Program/02- special/02- costing_calculate/src/etl/runner.py`
  责任：把 `month_range` 透传给 ETL、生成带月份后缀的输出路径，并在控制台摘要中打印月份过滤信息。
- Modify: `D:/03- Program/02- special/02- costing_calculate/src/etl/costing_etl.py`
  责任：让 ETL 持有 `month_range` 与 `last_month_filter_summary`，并把 `month_range` 传给 payload builder。
- Modify: `D:/03- Program/02- special/02- costing_calculate/src/etl/pipeline.py`
  责任：在 `build_normalized_cost_frame(...)` 之后调用月份过滤，并保存最近一次过滤摘要。
- Modify: `D:/03- Program/02- special/02- costing_calculate/src/analytics/qty_enricher.py`
  责任：把“月份过滤后是否空命中”的布尔信号传给质量指标构建函数。
- Modify: `D:/03- Program/02- special/02- costing_calculate/src/analytics/quality.py`
  责任：在月份过滤空命中时，对空值率和分析覆盖率输出 `N/A` 及明确说明。
- Create: `D:/03- Program/02- special/02- costing_calculate/tests/test_month_filter.py`
  责任：冻结 `MonthRange`、输出后缀、区间校验和标准化成本表过滤行为。
- Modify: `D:/03- Program/02- special/02- costing_calculate/tests/test_main.py`
  责任：冻结 CLI 参数解析和 `run_pipeline(..., month_range=...)` 的透传。
- Modify: `D:/03- Program/02- special/02- costing_calculate/tests/test_runner.py`
  责任：冻结 runner 的输出命名、控制台摘要和空命中成功路径。
- Modify: `D:/03- Program/02- special/02- costing_calculate/tests/test_etl_pipeline.py`
  责任：冻结标准化成本表过滤时机和 `pipeline.last_month_filter_summary` 的内容。
- Modify: `D:/03- Program/02- special/02- costing_calculate/tests/test_costing_etl.py`
  责任：冻结 ETL 对 `month_range` 的透传，以及空命中时仍可写出 workbook 的行为。
- Create: `D:/03- Program/02- special/02- costing_calculate/tests/test_quality.py`
  责任：冻结月份过滤空命中时质量指标的 `N/A` 输出与说明文本。

## Implementation Notes

- CLI 只接受 `YYYY-MM`；用户输入 `2025年01期`、`2025/01`、`2025-1` 都必须在参数解析阶段失败。
- 工作簿里的月份比较必须复用 `src.analytics.fact_builder.normalize_period`，不能在 ETL 层再写第二套正则。
- 月份过滤只能发生一次，且只能发生在 `NormalizedCostFrame` 阶段；不能在 writer 或分析层再做第二次月份判断。
- 输出命名依赖 `MonthRange`：
  - `from_2025-01`
  - `to_2025-03`
  - `2025-01_2025-03`
- 空命中不是失败：
  - `run_pipeline(...)` 返回 `0`
  - workbook 和 `error_log.csv` 仍要生成
  - 质量指标里对“无分母”项输出 `N/A`
- 当前架构禁止 `analytics` 反向依赖 `etl`，因此不要在 `src.analytics.contracts` 中直接引用 `MonthRange` 或 `MonthFilterSummary` 类型。

### Task 1: Add Month Range Core Helpers

**Files:**
- Create: `D:/03- Program/02- special/02- costing_calculate/src/etl/month_filter.py`
- Create: `D:/03- Program/02- special/02- costing_calculate/tests/test_month_filter.py`

- [ ] **Step 1: Write the failing month filter unit tests**

```python
# tests/test_month_filter.py
from __future__ import annotations

import polars as pl
import pytest

from src.analytics.contracts import NormalizedCostFrame
from src.etl.month_filter import MonthRange, apply_month_range_to_normalized_frame


def test_month_range_accepts_open_and_closed_bounds() -> None:
    assert MonthRange(start='2025-01', end='2025-03').output_suffix() == '2025-01_2025-03'
    assert MonthRange(start='2025-01').output_suffix() == 'from_2025-01'
    assert MonthRange(end='2025-03').output_suffix() == 'to_2025-03'
    assert MonthRange().output_suffix() == ''


def test_month_range_rejects_invalid_cli_values() -> None:
    with pytest.raises(ValueError, match='YYYY-MM'):
        MonthRange(start='2025-1')
    with pytest.raises(ValueError, match='YYYY-MM'):
        MonthRange(end='2025/03')
    with pytest.raises(ValueError, match='month_start'):
        MonthRange(start='2025-04', end='2025-03')


def test_apply_month_range_to_normalized_frame_filters_inclusive_bounds() -> None:
    normalized = NormalizedCostFrame(
        frame=pl.DataFrame(
            {
                '年期': ['2025年1期', '2025年2期', '2025年3期'],
                '月份': ['2025年01期', '2025年02期', '2025年03期'],
                '产品编码': ['P001', 'P001', 'P001'],
            }
        ),
        key_columns=('月份', '产品编码'),
    )

    filtered, summary = apply_month_range_to_normalized_frame(
        normalized,
        MonthRange(start='2025-02', end='2025-03'),
    )

    assert filtered.frame['月份'].to_list() == ['2025年02期', '2025年03期']
    assert summary is not None
    assert summary.input_rows == 3
    assert summary.output_rows == 2
    assert summary.input_months == ('2025-01', '2025-02', '2025-03')
    assert summary.output_months == ('2025-02', '2025-03')


def test_apply_month_range_to_normalized_frame_keeps_empty_result_structures() -> None:
    normalized = NormalizedCostFrame(
        frame=pl.DataFrame(
            {
                '年期': ['2025年1期'],
                '月份': ['2025年01期'],
                '产品编码': ['P001'],
            }
        ),
        key_columns=('月份', '产品编码'),
    )

    filtered, summary = apply_month_range_to_normalized_frame(
        normalized,
        MonthRange(start='2025-02', end='2025-03'),
    )

    assert filtered.frame.is_empty()
    assert filtered.frame.columns == ['年期', '月份', '产品编码']
    assert summary is not None
    assert summary.output_rows == 0
    assert summary.output_months == ()
```

- [ ] **Step 2: Run the new unit tests and verify they fail**

Run:

```bash
conda run -n test python -m pytest tests/test_month_filter.py -q
```

Expected:

```text
E   ModuleNotFoundError: No module named 'src.etl.month_filter'
```

- [ ] **Step 3: Implement `MonthRange` and normalized-frame filtering**

```python
# src/etl/month_filter.py
from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from src.analytics.contracts import NormalizedCostFrame
from src.analytics.fact_builder import normalize_period


@dataclass(frozen=True)
class MonthRange:
    start: str | None = None
    end: str | None = None

    def __post_init__(self) -> None:
        normalized_start = _normalize_cli_month(self.start, field_name='month_start')
        normalized_end = _normalize_cli_month(self.end, field_name='month_end')
        if normalized_start is not None and normalized_end is not None and normalized_start > normalized_end:
            raise ValueError(f'month_start={normalized_start} 不能晚于 month_end={normalized_end}')
        object.__setattr__(self, 'start', normalized_start)
        object.__setattr__(self, 'end', normalized_end)

    def output_suffix(self) -> str:
        if self.start and self.end:
            return f'{self.start}_{self.end}'
        if self.start:
            return f'from_{self.start}'
        if self.end:
            return f'to_{self.end}'
        return ''

    def describe(self) -> str:
        if self.start and self.end:
            return f'[{self.start}, {self.end}]'
        if self.start:
            return f'>= {self.start}'
        if self.end:
            return f'<= {self.end}'
        return 'all'


@dataclass(frozen=True)
class MonthFilterSummary:
    month_range: MonthRange
    input_rows: int
    output_rows: int
    input_months: tuple[str, ...]
    output_months: tuple[str, ...]


def build_month_range(month_start: str | None, month_end: str | None) -> MonthRange | None:
    if month_start is None and month_end is None:
        return None
    return MonthRange(start=month_start, end=month_end)


def apply_month_range_to_normalized_frame(
    normalized: NormalizedCostFrame,
    month_range: MonthRange | None,
) -> tuple[NormalizedCostFrame, MonthFilterSummary | None]:
    if month_range is None:
        return normalized, None

    frame = normalized.frame.with_columns(
        pl.col('月份').map_elements(normalize_period, return_dtype=pl.String, skip_nulls=False).alias('_period_key')
    )
    input_months = tuple(sorted({value for value in frame['_period_key'].to_list() if value}))

    predicate = pl.lit(True)
    if month_range.start is not None:
        predicate = predicate & (pl.col('_period_key') >= month_range.start)
    if month_range.end is not None:
        predicate = predicate & (pl.col('_period_key') <= month_range.end)

    filtered = frame.filter(predicate)
    output_months = tuple(sorted({value for value in filtered['_period_key'].to_list() if value}))
    summary = MonthFilterSummary(
        month_range=month_range,
        input_rows=frame.height,
        output_rows=filtered.height,
        input_months=input_months,
        output_months=output_months,
    )
    return (
        NormalizedCostFrame(frame=filtered.drop('_period_key'), key_columns=normalized.key_columns),
        summary,
    )


def _normalize_cli_month(value: str | None, *, field_name: str) -> str | None:
    if value is None:
        return None
    normalized = normalize_period(value)
    if normalized is None or str(value).strip() != normalized:
        raise ValueError(f'{field_name} 必须是 YYYY-MM 格式，收到: {value!r}')
    return normalized
```

- [ ] **Step 4: Run the unit tests again and verify they pass**

Run:

```bash
conda run -n test python -m pytest tests/test_month_filter.py -q
```

Expected:

```text
4 passed
```

- [ ] **Step 5: Commit the helper layer**

```bash
git add src/etl/month_filter.py tests/test_month_filter.py
git commit -m "feat(etl): add month range filtering helpers"
```

### Task 2: Wire CLI Parsing and Runner Output Naming

**Files:**
- Modify: `D:/03- Program/02- special/02- costing_calculate/main.py`
- Modify: `D:/03- Program/02- special/02- costing_calculate/src/etl/runner.py`
- Modify: `D:/03- Program/02- special/02- costing_calculate/tests/test_main.py`
- Modify: `D:/03- Program/02- special/02- costing_calculate/tests/test_runner.py`

- [ ] **Step 1: Write the failing CLI and runner tests**

```python
# tests/test_main.py
from __future__ import annotations

import pytest

from main import main
from src.etl.month_filter import MonthRange


def test_main_passes_month_range_to_runner(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run_pipeline(config, month_range=None):
        captured['config'] = config
        captured['month_range'] = month_range
        return 0

    monkeypatch.setattr('main.run_pipeline', _fake_run_pipeline)

    exit_code = main(['gb', '--month-start', '2025-01', '--month-end', '2025-03'])

    assert exit_code == 0
    assert captured['month_range'] == MonthRange(start='2025-01', end='2025-03')


def test_main_rejects_invalid_month_argument() -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(['gb', '--month-start', '2025/01'])
    assert exc_info.value.code == 2
```

```python
# tests/test_runner.py
from src.etl.month_filter import MonthFilterSummary, MonthRange


def test_run_pipeline_uses_month_suffix_in_output_names(monkeypatch, capsys, tmp_path) -> None:
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
        standalone_cost_items=('委外加工费',),
        product_anomaly_scope_mode='doc_type_split',
    )

    captured: dict[str, object] = {}
    month_range = MonthRange(start='2025-01', end='2025-03')

    class _DummyETL:
        def __init__(self, skip_rows: int, *, product_order, standalone_cost_items, product_anomaly_scope_mode, month_range):
            self.last_quality_metrics = ()
            self.last_error_log_count = 0
            self.last_error_log_frame = pd.DataFrame(columns=['row_id', 'issue_type', 'message'])
            self.last_month_filter_summary = MonthFilterSummary(
                month_range=month_range,
                input_rows=3,
                output_rows=2,
                input_months=('2025-01', '2025-02', '2025-03'),
                output_months=('2025-02', '2025-03'),
            )

        def process_file(self, input_path: Path, output_path: Path) -> bool:
            captured['output_path'] = output_path
            output_path.write_text('ok', encoding='utf-8')
            return True

    monkeypatch.setattr('src.etl.runner.CostingWorkbookETL', _DummyETL)

    exit_code = run_pipeline(config, month_range=month_range)
    stdout = capsys.readouterr().out

    assert exit_code == 0
    assert captured['output_path'] == processed_dir / 'GB-成本计算单_处理后_2025-01_2025-03.xlsx'
    assert (processed_dir / 'GB-成本计算单_处理后_2025-01_2025-03_error_log.csv').exists()
    assert 'month_range=[2025-01, 2025-03]' in stdout
    assert 'month_filter_rows=3->2' in stdout
```

- [ ] **Step 2: Run the CLI and runner tests and verify they fail**

Run:

```bash
conda run -n test python -m pytest tests/test_main.py tests/test_runner.py -q
```

Expected:

```text
TypeError: run_pipeline() got an unexpected keyword argument 'month_range'
TypeError: _DummyETL.__init__() got an unexpected keyword argument 'month_range'
```

- [ ] **Step 3: Implement parser plumbing, output naming, and summary lines**

```python
# main.py
from src.etl.month_filter import build_month_range
from src.etl.runner import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='成本核算 ETL 统一入口')
    parser.add_argument('pipeline', choices=sorted(PIPELINES), help='选择要运行的管线: gb 或 sk')
    parser.add_argument('--month-start', dest='month_start', help='起始月份，格式 YYYY-MM')
    parser.add_argument('--month-end', dest='month_end', help='结束月份，格式 YYYY-MM')
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        month_range = build_month_range(args.month_start, args.month_end)
    except ValueError as exc:
        parser.error(str(exc))
    return run_pipeline(PIPELINES[args.pipeline], month_range=month_range)
```

```python
# src/etl/runner.py
from src.etl.month_filter import MonthFilterSummary, MonthRange


def _build_output_paths(processed_dir: Path, input_file: Path, month_range: MonthRange | None) -> tuple[Path, Path]:
    suffix = '' if month_range is None or not month_range.output_suffix() else f'_{month_range.output_suffix()}'
    workbook_path = processed_dir / f'{input_file.stem}_处理后{suffix}.xlsx'
    error_log_path = processed_dir / f'{input_file.stem}_处理后{suffix}_error_log.csv'
    return workbook_path, error_log_path


def build_quality_log_text(
    *,
    pipeline_name: str,
    input_path: Path,
    output_path: Path,
    error_log_count: int,
    quality_metrics: Iterable[QualityMetric],
    month_filter_summary: MonthFilterSummary | None = None,
) -> str:
    lines = [
        f'pipeline={pipeline_name}',
        f'input={input_path}',
        f'output={output_path}',
        f'error_log_count={error_log_count}',
    ]
    if month_filter_summary is not None:
        lines.extend(
            [
                f'month_range={month_filter_summary.month_range.describe()}',
                f'month_filter_rows={month_filter_summary.input_rows}->{month_filter_summary.output_rows}',
                f'months_before={",".join(month_filter_summary.input_months) or "-"}',
                f'months_after={",".join(month_filter_summary.output_months) or "-"}',
            ]
        )
    lines.extend(['', '[quality_metrics]'])
    lines.extend(f'{metric.metric}={metric.value} | {metric.description}' for metric in quality_metrics)
    return '\n'.join(lines)


def run_pipeline(config: PipelineConfig, month_range: MonthRange | None = None) -> int:
    ...
    output_file, error_log_csv_file = _build_output_paths(config.processed_dir, input_file, month_range)
    etl = CostingWorkbookETL(
        skip_rows=2,
        product_order=config.product_order,
        standalone_cost_items=config.standalone_cost_items,
        product_anomaly_scope_mode=config.product_anomaly_scope_mode,
        month_range=month_range,
    )
    ...
    quality_log = build_quality_log_text(
        pipeline_name=config.name,
        input_path=input_file,
        output_path=output_file,
        error_log_count=etl.last_error_log_count,
        quality_metrics=etl.last_quality_metrics,
        month_filter_summary=etl.last_month_filter_summary,
    )
```

- [ ] **Step 4: Run the CLI and runner tests again and verify they pass**

Run:

```bash
conda run -n test python -m pytest tests/test_main.py tests/test_runner.py -q
```

Expected:

```text
all passed
```

- [ ] **Step 5: Commit the CLI and runner changes**

```bash
git add main.py src/etl/runner.py tests/test_main.py tests/test_runner.py
git commit -m "feat(cli): add month range arguments and output suffixes"
```

### Task 3: Filter the Normalized Cost Frame Before Split

**Files:**
- Modify: `D:/03- Program/02- special/02- costing_calculate/src/etl/costing_etl.py`
- Modify: `D:/03- Program/02- special/02- costing_calculate/src/etl/pipeline.py`
- Modify: `D:/03- Program/02- special/02- costing_calculate/tests/test_etl_pipeline.py`
- Modify: `D:/03- Program/02- special/02- costing_calculate/tests/test_costing_etl.py`

- [ ] **Step 1: Write the failing ETL and pipeline tests**

```python
# tests/test_etl_pipeline.py
from src.etl.month_filter import MonthRange


def test_build_workbook_payload_filters_normalized_frame_before_split(monkeypatch, tmp_path: Path) -> None:
    etl = CostingWorkbookETL(skip_rows=2)
    raw = RawWorkbookFrame(
        sheet_name='成本计算单',
        header_rows=(('年期', '产品编码'), ('', '')),
        frame=pl.DataFrame({'column_0': ['2025年1期', '2025年2期', '2025年3期'], 'column_1': ['P001', 'P001', 'P001']}),
    )
    normalized = NormalizedCostFrame(
        frame=pl.DataFrame(
            {
                '年期': ['2025年1期', '2025年2期', '2025年3期'],
                '月份': ['2025年01期', '2025年02期', '2025年03期'],
                '产品编码': ['P001', 'P001', 'P001'],
            }
        ),
        key_columns=('月份', '产品编码'),
    )

    seen: dict[str, object] = {}

    monkeypatch.setattr(etl.pipeline, 'load_raw_workbook_frame', lambda input_path: raw)
    monkeypatch.setattr(etl.pipeline, 'build_normalized_cost_frame', lambda raw_workbook: normalized)

    def _fake_split(normalized_frame: NormalizedCostFrame) -> SplitResult:
        seen['months'] = normalized_frame.frame['月份'].to_list()
        return SplitResult(
            detail_df=pl.DataFrame({'月份': ['2025年02期']}),
            qty_df=pl.DataFrame({'月份': ['2025年02期']}),
        )

    monkeypatch.setattr(etl.pipeline, 'split_normalized_frames', _fake_split)
    monkeypatch.setattr(
        'src.etl.pipeline.build_report_artifacts',
        lambda *args, **kwargs: AnalysisArtifacts(
            fact_df=pd.DataFrame(),
            qty_sheet_df=pd.DataFrame({'月份': ['2025年02期']}),
            work_order_sheet=FlatSheet(data=pd.DataFrame({'月份': ['2025年02期']}), column_types={'月份': 'text'}),
            product_anomaly_sections=[],
            quality_metrics=(),
            error_log=pd.DataFrame(columns=['row_id', 'issue_type', 'message']),
        ),
    )
    monkeypatch.setattr(
        'src.etl.pipeline.build_sheet_models',
        lambda **kwargs: (
            SheetModel(
                sheet_name='成本明细',
                columns=('月份',),
                rows_factory=lambda: iter([('2025年02期',)]),
                column_types={'月份': 'text'},
                number_formats={},
            ),
        ),
    )

    payload = etl.pipeline.build_workbook_payload(
        tmp_path / 'input.xlsx',
        standalone_cost_items=('委外加工费',),
        product_anomaly_scope_mode='legacy_single_scope',
        month_range=MonthRange(start='2025-02', end='2025-03'),
    )

    assert seen['months'] == ['2025年02期', '2025年03期']
    assert etl.pipeline.last_month_filter_summary is not None
    assert etl.pipeline.last_month_filter_summary.output_rows == 2
    assert payload.sheet_models[0].sheet_name == '成本明细'
```

```python
# tests/test_costing_etl.py
from src.etl.month_filter import MonthRange


def test_process_file_passes_month_range_to_pipeline_payload_builder(tmp_path: Path) -> None:
    etl = CostingWorkbookETL(skip_rows=2, month_range=MonthRange(start='2025-02', end='2025-03'))
    payload = WorkbookPayload(
        sheet_models=(),
        quality_metrics=(),
        error_log_count=0,
        stage_timings={},
        error_log_export=pd.DataFrame(),
    )

    with patch.object(etl.pipeline, 'build_workbook_payload', return_value=payload) as payload_mock:
        with patch.object(etl.workbook_writer, 'write_workbook_from_models'):
            assert etl.process_file(tmp_path / 'input.xlsx', tmp_path / 'output.xlsx') is True

    assert payload_mock.call_args.kwargs['month_range'] == MonthRange(start='2025-02', end='2025-03')
```

- [ ] **Step 2: Run the ETL and pipeline tests and verify they fail**

Run:

```bash
conda run -n test python -m pytest tests/test_etl_pipeline.py tests/test_costing_etl.py -q
```

Expected:

```text
TypeError: CostingEtlPipeline.build_workbook_payload() got an unexpected keyword argument 'month_range'
KeyError: 'month_range'
```

- [ ] **Step 3: Implement single-point filtering in ETL and pipeline**

```python
# src/etl/costing_etl.py
from src.etl.month_filter import MonthFilterSummary, MonthRange


class CostingWorkbookETL:
    def __init__(
        self,
        skip_rows: int = 2,
        *,
        product_order: tuple[tuple[str, str], ...] | None = None,
        standalone_cost_items: tuple[str, ...] | None = None,
        product_anomaly_scope_mode: str | None = None,
        month_range: MonthRange | None = None,
    ):
        ...
        self.month_range = month_range
        self.last_month_filter_summary: MonthFilterSummary | None = None
        self.pipeline = CostingEtlPipeline(...)

    def process_file(self, input_path: Path, output_path: Path) -> bool:
        ...
        self.last_month_filter_summary = None
        payload = self.pipeline.build_workbook_payload(
            input_path,
            standalone_cost_items=self.standalone_cost_items,
            product_anomaly_scope_mode=self.product_anomaly_scope_mode,
            month_range=self.month_range,
            artifacts_transform=self._filter_analysis_artifacts_by_whitelist,
        )
        self.last_month_filter_summary = self.pipeline.last_month_filter_summary
        ...
```

```python
# src/etl/pipeline.py
from src.etl.month_filter import MonthFilterSummary, MonthRange, apply_month_range_to_normalized_frame


class CostingEtlPipeline:
    def __init__(...):
        ...
        self.last_month_filter_summary: MonthFilterSummary | None = None

    def build_workbook_payload(
        self,
        input_path: Path,
        *,
        standalone_cost_items: tuple[str, ...],
        product_anomaly_scope_mode: str = 'legacy_single_scope',
        month_range: MonthRange | None = None,
        artifacts_transform: Callable[[AnalysisArtifacts], AnalysisArtifacts] | None = None,
    ) -> WorkbookPayload:
        ...
        normalized_frame = self.build_normalized_cost_frame(raw_workbook)
        normalized_frame, month_filter_summary = apply_month_range_to_normalized_frame(normalized_frame, month_range)
        self.last_month_filter_summary = month_filter_summary
        ...
        artifacts = build_report_artifacts(
            split_result.detail_df,
            split_result.qty_df,
            standalone_cost_items=standalone_cost_items,
            product_anomaly_scope_mode=product_anomaly_scope_mode,
            month_filter_empty_result=bool(
                month_filter_summary is not None and month_filter_summary.output_rows == 0
            ),
        )
```

- [ ] **Step 4: Run the ETL and pipeline tests again and verify they pass**

Run:

```bash
conda run -n test python -m pytest tests/test_etl_pipeline.py tests/test_costing_etl.py -q
```

Expected:

```text
all passed
```

- [ ] **Step 5: Commit the filtering hook**

```bash
git add src/etl/costing_etl.py src/etl/pipeline.py tests/test_etl_pipeline.py tests/test_costing_etl.py
git commit -m "feat(etl): filter normalized cost frames by month range"
```

### Task 4: Make Empty Month Hits Observable and Auditable

**Files:**
- Modify: `D:/03- Program/02- special/02- costing_calculate/src/analytics/qty_enricher.py`
- Modify: `D:/03- Program/02- special/02- costing_calculate/src/analytics/quality.py`
- Create: `D:/03- Program/02- special/02- costing_calculate/tests/test_quality.py`
- Modify: `D:/03- Program/02- special/02- costing_calculate/tests/test_runner.py`

- [ ] **Step 1: Write the failing quality and empty-hit regression tests**

```python
# tests/test_quality.py
from __future__ import annotations

import pandas as pd

from src.analytics.quality import build_quality_metrics


def test_build_quality_metrics_marks_rates_na_when_month_filter_result_is_empty() -> None:
    metrics = build_quality_metrics(
        detail_df=pd.DataFrame(columns=['月份']),
        qty_input_df=pd.DataFrame(columns=['月份']),
        qty_sheet_df=pd.DataFrame(columns=['月份', '_join_key']),
        analysis_df=pd.DataFrame(columns=['是否可参与分析']),
        filtered_invalid_qty_count=0,
        filtered_missing_total_amount_count=0,
        month_filter_empty_result=True,
    )

    metric_map = {metric.metric: metric for metric in metrics}

    assert metric_map['直接材料金额缺失率'].value == 'N/A'
    assert metric_map['直接材料金额缺失率'].description == '月份过滤后无数据，指标不适用'
    assert metric_map['可参与分析占比'].value == 'N/A'
    assert metric_map['可参与分析占比'].description == '月份过滤后无数据，指标不适用'
```

```python
# tests/test_runner.py
def test_run_pipeline_succeeds_when_month_range_matches_no_rows(monkeypatch, capsys, tmp_path) -> None:
    input_file = tmp_path / 'SK-成本计算单.xlsx'
    input_file.touch()
    processed_dir = tmp_path / 'processed'
    processed_dir.mkdir()
    config = PipelineConfig(
        name='sk',
        raw_dir=tmp_path,
        processed_dir=processed_dir,
        input_patterns=('SK-*.xlsx',),
        product_order=(('DP.C.P0197AA', '动力线'),),
        product_anomaly_scope_mode='legacy_single_scope',
    )

    month_range = MonthRange(start='2026-01', end='2026-03')

    class _DummyETL:
        def __init__(self, skip_rows: int, *, product_order, standalone_cost_items, product_anomaly_scope_mode, month_range):
            self.last_quality_metrics = ()
            self.last_error_log_count = 0
            self.last_error_log_frame = pd.DataFrame(columns=['row_id', 'issue_type', 'message'])
            self.last_month_filter_summary = MonthFilterSummary(
                month_range=month_range,
                input_rows=5,
                output_rows=0,
                input_months=('2025-01', '2025-02'),
                output_months=(),
            )

        def process_file(self, input_path: Path, output_path: Path) -> bool:
            output_path.write_text('ok', encoding='utf-8')
            return True

    monkeypatch.setattr('src.etl.runner.CostingWorkbookETL', _DummyETL)

    exit_code = run_pipeline(config, month_range=month_range)
    stdout = capsys.readouterr().out

    assert exit_code == 0
    assert 'month_filter_rows=5->0' in stdout
    assert 'months_after=-' in stdout
    assert (processed_dir / 'SK-成本计算单_处理后_2026-01_2026-03.xlsx').exists()
    assert (processed_dir / 'SK-成本计算单_处理后_2026-01_2026-03_error_log.csv').exists()
```

- [ ] **Step 2: Run the regression tests and verify they fail**

Run:

```bash
conda run -n test python -m pytest tests/test_quality.py tests/test_runner.py -q
```

Expected:

```text
TypeError: build_quality_metrics() got an unexpected keyword argument 'month_filter_empty_result'
AssertionError: 'months_after=-' not found in stdout
```

- [ ] **Step 3: Implement `N/A` quality output and explicit empty-hit summary lines**

```python
# src/analytics/qty_enricher.py
def build_report_artifacts(
    df_detail: pd.DataFrame | pl.DataFrame,
    df_qty: pd.DataFrame | pl.DataFrame,
    standalone_cost_items: tuple[str, ...] | list[str] | None = DEFAULT_STANDALONE_COST_ITEMS,
    product_anomaly_scope_mode: str = 'legacy_single_scope',
    month_filter_empty_result: bool = False,
) -> AnalysisArtifacts:
    ...
    quality_metrics = build_quality_metrics(
        detail_pd,
        qty_pd,
        qty_sheet_with_key_pd,
        work_order_sheet.data,
        filtered_invalid_qty_count,
        filtered_missing_total_amount_count,
        month_filter_empty_result=month_filter_empty_result,
    )
```

```python
# src/analytics/quality.py
def build_quality_metrics(
    detail_df: pd.DataFrame | pl.DataFrame,
    qty_input_df: pd.DataFrame | pl.DataFrame,
    qty_sheet_df: pd.DataFrame | pl.DataFrame,
    analysis_df: pd.DataFrame | pl.DataFrame,
    filtered_invalid_qty_count: int,
    filtered_missing_total_amount_count: int,
    month_filter_empty_result: bool = False,
) -> tuple[QualityMetric, ...]:
    ...
    dm_amount_null_rate = _null_rate(qty_sheet_df, QTY_DM_AMOUNT)
    analyzable_rate = _yes_rate(analysis_df, '是否可参与分析')
    null_rate_value = 'N/A' if month_filter_empty_result else f'{dm_amount_null_rate:.2%}'
    coverage_value = 'N/A' if month_filter_empty_result else f'{analyzable_rate:.2%}'
    null_rate_description = '月份过滤后无数据，指标不适用' if month_filter_empty_result else '派生金额字段空值率'
    coverage_description = '月份过滤后无数据，指标不适用' if month_filter_empty_result else '仅统计白名单产品且通过基础校验的工单'

    return (
        ...,
        QualityMetric(
            category='空值率',
            metric='直接材料金额缺失率',
            value=null_rate_value,
            description=null_rate_description,
        ),
        ...,
        QualityMetric(
            category='分析覆盖率',
            metric='可参与分析占比',
            value=coverage_value,
            description=coverage_description,
        ),
    )
```

```python
# src/etl/runner.py
def build_quality_log_text(..., month_filter_summary: MonthFilterSummary | None = None) -> str:
    ...
    if month_filter_summary is not None:
        months_before = ','.join(month_filter_summary.input_months) or '-'
        months_after = ','.join(month_filter_summary.output_months) or '-'
        lines.extend(
            [
                f'month_range={month_filter_summary.month_range.describe()}',
                f'month_filter_rows={month_filter_summary.input_rows}->{month_filter_summary.output_rows}',
                f'months_before={months_before}',
                f'months_after={months_after}',
            ]
        )
```

- [ ] **Step 4: Run the regression tests again and verify they pass**

Run:

```bash
conda run -n test python -m pytest tests/test_quality.py tests/test_runner.py -q
```

Expected:

```text
all passed
```

- [ ] **Step 5: Commit the quality and empty-hit behavior**

```bash
git add src/analytics/qty_enricher.py src/analytics/quality.py src/etl/runner.py tests/test_quality.py tests/test_runner.py
git commit -m "feat(quality): explain empty month filter results"
```

### Task 5: Final Integration Verification

**Files:**
- Modify: `D:/03- Program/02- special/02- costing_calculate/tests/test_main.py`
- Modify: `D:/03- Program/02- special/02- costing_calculate/tests/test_runner.py`
- Modify: `D:/03- Program/02- special/02- costing_calculate/tests/test_etl_pipeline.py`
- Modify: `D:/03- Program/02- special/02- costing_calculate/tests/test_costing_etl.py`
- Create: `D:/03- Program/02- special/02- costing_calculate/tests/test_month_filter.py`
- Create: `D:/03- Program/02- special/02- costing_calculate/tests/test_quality.py`

- [ ] **Step 1: Run the targeted month-range test suite**

Run:

```bash
conda run -n test python -m pytest tests/test_month_filter.py tests/test_main.py tests/test_runner.py tests/test_etl_pipeline.py tests/test_costing_etl.py tests/test_quality.py -q
```

Expected:

```text
all passed
```

- [ ] **Step 2: Run the broader ETL regression suite**

Run:

```bash
conda run -n test python -m pytest tests/test_costing_etl.py tests/test_etl_pipeline.py tests/test_runner.py tests/test_main.py -q
```

Expected:

```text
all passed
```

- [ ] **Step 3: Run Ruff on touched production and test files**

Run:

```bash
conda run -n test python -m ruff check main.py src/etl/month_filter.py src/etl/runner.py src/etl/costing_etl.py src/etl/pipeline.py src/analytics/qty_enricher.py src/analytics/quality.py tests/test_month_filter.py tests/test_main.py tests/test_runner.py tests/test_etl_pipeline.py tests/test_costing_etl.py tests/test_quality.py
```

Expected:

```text
All checks passed!
```

- [ ] **Step 4: Final integration commit**

```bash
git add main.py src/etl/month_filter.py src/etl/runner.py src/etl/costing_etl.py src/etl/pipeline.py src/analytics/qty_enricher.py src/analytics/quality.py tests/test_month_filter.py tests/test_main.py tests/test_runner.py tests/test_etl_pipeline.py tests/test_costing_etl.py tests/test_quality.py
git commit -m "feat(etl): add workbook month range filtering"
```

## Self-Review Checklist

- Spec coverage:
  - CLI 参数、双闭区间、开放区间：Task 1 + Task 2
  - 标准化成本表过滤落点：Task 3
  - 输出后缀命名：Task 2
  - 空命中成功路径：Task 4
  - `N/A` 质量指标说明：Task 4
  - 回归与统一验证：Task 5
- Placeholder scan:
  - 无 `TBD` / `TODO` / “类似 Task N” / “自行补充测试” 这类占位语
- Type consistency:
  - `MonthRange`
  - `MonthFilterSummary`
  - `build_month_range(...)`
  - `apply_month_range_to_normalized_frame(...)`
  - `run_pipeline(..., month_range=...)`
  - `build_workbook_payload(..., month_range=...)`

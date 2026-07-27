# Lightweight Excel Export Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current heavy OpenPyXL workbook renderer with a lightweight XlsxWriter export path that preserves sheet order, number formats, filters, freeze panes, and work-order anomaly highlights while significantly reducing export time.

**Architecture:** Keep all ETL and analytics outputs unchanged, but route workbook rendering through a new XlsxWriter-focused helper that supports both Pandas-backed writes and row-streaming writes for hotspot sheets. Move anomaly highlights from concrete cell fills to conditional-format rules, and verify behavior with workbook contract tests that inspect workbook semantics and conditional-format metadata instead of binary equality.

**Tech Stack:** Python 3.11, pandas, xlsxwriter, openpyxl, pytest

---

## File Map

- Modify: `src/excel/workbook_writer.py`
  - Switch workbook engine to `xlsxwriter`, keep sheet order stable, delegate hotspot sheets to a dedicated fast writer.
- Modify: `src/excel/sheet_writers.py`
  - Remove OpenPyXL-specific per-cell styling loops and leave only lightweight, XlsxWriter-compatible sheet orchestration where needed.
- Create: `src/excel/fast_writer.py`
  - Define workbook options, reusable format objects, flat-sheet streaming writes, analysis-sheet writes, and work-order conditional formatting.
- Modify: `src/excel/styles.py`
  - Keep only export-agnostic number-format helpers and width constants; stop relying on OpenPyXL style objects for runtime rendering.
- Modify: `src/etl/costing_etl.py`
  - Add stage timing logs and explicitly release large export-only references after write completion.
- Modify: `tests/test_costing_etl.py`
  - Add regression tests for lightweight widths, hotspot writer routing, and stage timing logs.
- Modify: `tests/contracts/_workbook_contract_helper.py`
  - Extract workbook semantics for lightweight sheets and conditional-format rule semantics for the work-order sheet.
- Modify: `tests/contracts/test_workbook_contract.py`
  - Freeze default workbook semantics and highlight-rule semantics against the regenerated baseline.
- Modify: `tests/contracts/generate_baselines.py`
  - Regenerate workbook contract baselines after the refactor lands.
- Modify: `tests/contracts/baselines/workbook_semantics.json`
  - Store the new lightweight workbook semantics and conditional-format rule expectations.

### Task 1: Introduce The Lightweight XlsxWriter Workbook Skeleton

**Files:**
- Create: `src/excel/fast_writer.py`
- Modify: `src/excel/workbook_writer.py`
- Modify: `src/excel/styles.py`
- Test: `tests/test_costing_etl.py`

- [ ] **Step 1: Write the failing regression test for lightweight flat-sheet semantics**

```python
from tests.contracts._workbook_contract_helper import (
    build_default_contract_workbook,
    extract_workbook_semantics,
)


def test_lightweight_export_uses_fixed_width_and_number_formats(tmp_path) -> None:
    workbook_path = build_default_contract_workbook(tmp_path)
    semantics = extract_workbook_semantics(workbook_path)

    detail_sheet = semantics['sheets']['成本明细']
    qty_sheet = semantics['sheets']['产品数量统计']

    assert detail_sheet['freeze_panes'] == 'A2'
    assert qty_sheet['freeze_panes'] == 'A2'
    assert set(detail_sheet['column_widths'].values()) == {15.0}
    assert detail_sheet['number_formats']['本期完工单位成本'] == '#,##0.00'
    assert qty_sheet['number_formats']['本期完工金额'] == '#,##0.00'
```

- [ ] **Step 2: Run the targeted test to verify it fails**

Run: `conda run -n test python -m pytest tests/test_costing_etl.py::test_lightweight_export_uses_fixed_width_and_number_formats -q`

Expected: FAIL because the current OpenPyXL writer still derives variable widths from data and does not use the fixed lightweight width policy.

- [ ] **Step 3: Implement the XlsxWriter skeleton and fixed-width flat-sheet writer**

```python
# src/excel/fast_writer.py
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.excel.styles import (
    EXCEL_INTEGER_FORMAT,
    EXCEL_PERCENT_FORMAT,
    EXCEL_SCORE_FORMAT,
    EXCEL_TWO_DECIMAL_FORMAT,
    to_excel_number,
)

XLSXWRITER_OPTIONS = {'constant_memory': True, 'strings_to_urls': False}
DEFAULT_FLAT_WIDTH = 15.0


@dataclass(frozen=True)
class WorkbookFormats:
    header: object
    integer: object
    decimal: object
    percent: object
    score: object


class FastWorkbookWriter:
    def build_formats(self, workbook) -> WorkbookFormats:
        return WorkbookFormats(
            header=workbook.add_format({'bold': True, 'bg_color': '#D9E1F2', 'align': 'center', 'valign': 'vcenter'}),
            integer=workbook.add_format({'num_format': EXCEL_INTEGER_FORMAT}),
            decimal=workbook.add_format({'num_format': EXCEL_TWO_DECIMAL_FORMAT}),
            percent=workbook.add_format({'num_format': EXCEL_PERCENT_FORMAT}),
            score=workbook.add_format({'num_format': EXCEL_SCORE_FORMAT}),
        )

    def write_dataframe_sheet(
        self,
        writer: pd.ExcelWriter,
        sheet_name: str,
        df: pd.DataFrame,
        *,
        number_formats: dict[str, str],
        freeze_panes: tuple[int, int] = (1, 0),
    ) -> None:
        write_df = df.copy()
        for column_name in number_formats:
            if column_name in write_df.columns:
                write_df[column_name] = write_df[column_name].map(to_excel_number)

        write_df.to_excel(writer, sheet_name=sheet_name, index=False)
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]
        formats = self.build_formats(workbook)

        worksheet.freeze_panes(*freeze_panes)
        worksheet.autofilter(0, 0, max(len(write_df), 1), max(len(write_df.columns) - 1, 0))

        for col_idx, column_name in enumerate(write_df.columns):
            cell_format = {
                EXCEL_TWO_DECIMAL_FORMAT: formats.decimal,
                EXCEL_INTEGER_FORMAT: formats.integer,
                EXCEL_PERCENT_FORMAT: formats.percent,
                EXCEL_SCORE_FORMAT: formats.score,
            }.get(number_formats.get(column_name))
            worksheet.set_column(col_idx, col_idx, DEFAULT_FLAT_WIDTH, cell_format)

        for col_idx, column_name in enumerate(write_df.columns):
            worksheet.write(0, col_idx, column_name, formats.header)
```

```python
# src/excel/workbook_writer.py
from src.excel.fast_writer import FastWorkbookWriter, XLSXWRITER_OPTIONS


class CostingWorkbookWriter:
    def __init__(self) -> None:
        self.fast_writer = FastWorkbookWriter()

    def write_workbook(
        self,
        output_path: Path,
        *,
        detail_df: pd.DataFrame,
        qty_sheet_df: pd.DataFrame,
        analysis_tables: dict[str, list[SectionBlock]],
        work_order_sheet: FlatSheet,
        product_anomaly_sections: list[ProductAnomalySection],
        error_log: pd.DataFrame,
    ) -> None:
        with pd.ExcelWriter(
            output_path,
            engine='xlsxwriter',
            engine_kwargs={'options': XLSXWRITER_OPTIONS},
        ) as writer:
            self.fast_writer.write_dataframe_sheet(
                writer,
                '成本明细',
                detail_df,
                number_formats={column: '#,##0.00' for column in DETAIL_TWO_DECIMAL_COLUMNS if column in detail_df.columns},
            )
            self.fast_writer.write_dataframe_sheet(
                writer,
                '产品数量统计',
                qty_sheet_df,
                number_formats={column: '#,##0.00' for column in _resolve_qty_numeric_columns(qty_sheet_df)},
            )
```

- [ ] **Step 4: Run the targeted test to verify it passes**

Run: `conda run -n test python -m pytest tests/test_costing_etl.py::test_lightweight_export_uses_fixed_width_and_number_formats -q`

Expected: PASS

- [ ] **Step 5: Commit the workbook skeleton change**

```bash
git add src/excel/fast_writer.py src/excel/workbook_writer.py src/excel/styles.py tests/test_costing_etl.py
git commit -m "feat(excel): add lightweight xlsxwriter workbook skeleton"
```

### Task 2: Stream Hotspot Sheets With `write_row()`

**Files:**
- Modify: `src/excel/fast_writer.py`
- Modify: `src/excel/workbook_writer.py`
- Test: `tests/test_costing_etl.py`

- [ ] **Step 1: Write the failing routing test for hotspot sheets**

```python
from unittest.mock import patch


def test_write_workbook_streams_hotspot_sheets_through_fast_writer(tmp_path) -> None:
    writer = CostingWorkbookWriter()
    output_path = tmp_path / 'output.xlsx'

    with (
        patch.object(writer.fast_writer, 'write_dataframe_fast') as streamed_write,
        patch.object(writer.fast_writer, 'write_dataframe_sheet') as dataframe_write,
        patch.object(writer.fast_writer, 'write_analysis_sheet') as analysis_write,
        patch.object(writer.fast_writer, 'write_work_order_sheet') as work_order_write,
        patch.object(writer.fast_writer, 'write_product_anomaly_sheet') as product_write,
    ):
        writer.write_workbook(
            output_path,
            detail_df=pd.DataFrame([{'本期完工金额': 1.0}]),
            qty_sheet_df=pd.DataFrame([{'本期完工金额': 1.0}]),
            analysis_tables={'直接材料_价量比': [], '直接人工_价量比': [], '制造费用_价量比': []},
            work_order_sheet=FlatSheet(data=pd.DataFrame([{'产品编码': 'P-1'}]), column_types={'产品编码': 'text'}),
            product_anomaly_sections=[],
            error_log=pd.DataFrame([{'issue_type': 'X'}]),
        )

    streamed_sheets = [call.kwargs['sheet_name'] for call in streamed_write.call_args_list]
    assert streamed_sheets == ['成本明细', '产品数量统计', 'error_log']
    assert analysis_write.call_count == 3
    work_order_write.assert_called_once()
    product_write.assert_called_once()
    dataframe_write.assert_not_called()
```

- [ ] **Step 2: Run the routing test to verify it fails**

Run: `conda run -n test python -m pytest tests/test_costing_etl.py::test_write_workbook_streams_hotspot_sheets_through_fast_writer -q`

Expected: FAIL because `CostingWorkbookWriter` does not yet distinguish hotspot sheets from the default Pandas-backed path.

- [ ] **Step 3: Add the streaming writer and route the three hotspot sheets through it**

```python
# src/excel/fast_writer.py
class FastWorkbookWriter:
    def _resolve_column_format(self, number_format: str | None, formats: WorkbookFormats):
        return {
            EXCEL_TWO_DECIMAL_FORMAT: formats.decimal,
            EXCEL_INTEGER_FORMAT: formats.integer,
            EXCEL_PERCENT_FORMAT: formats.percent,
            EXCEL_SCORE_FORMAT: formats.score,
        }.get(number_format)

    def write_dataframe_fast(
        self,
        writer: pd.ExcelWriter,
        *,
        sheet_name: str,
        df: pd.DataFrame,
        number_formats: dict[str, str],
        freeze_panes: tuple[int, int] = (1, 0),
    ) -> None:
        workbook = writer.book
        worksheet = workbook.add_worksheet(sheet_name)
        writer.sheets[sheet_name] = worksheet
        formats = self.build_formats(workbook)

        headers = list(df.columns)
        for col_idx, column_name in enumerate(headers):
            worksheet.write(0, col_idx, column_name, formats.header)

        for row_idx, row_values in enumerate(df.itertuples(index=False, name=None), start=1):
            worksheet.write_row(row_idx, 0, [to_excel_number(value) for value in row_values])

        worksheet.freeze_panes(*freeze_panes)
        worksheet.autofilter(0, 0, max(len(df), 1), max(len(headers) - 1, 0))
        for col_idx, column_name in enumerate(headers):
            cell_format = self._resolve_column_format(number_formats.get(column_name), formats)
            worksheet.set_column(col_idx, col_idx, DEFAULT_FLAT_WIDTH, cell_format)
```

```python
# src/excel/workbook_writer.py
self.fast_writer.write_dataframe_fast(
    writer,
    sheet_name='成本明细',
    df=detail_df,
    number_formats={column: '#,##0.00' for column in DETAIL_TWO_DECIMAL_COLUMNS if column in detail_df.columns},
)
self.fast_writer.write_dataframe_fast(
    writer,
    sheet_name='产品数量统计',
    df=qty_sheet_df,
    number_formats={column: '#,##0.00' for column in _resolve_qty_numeric_columns(qty_sheet_df)},
)
self.fast_writer.write_analysis_sheet(writer, '直接材料_价量比', analysis_tables['直接材料_价量比'])
self.fast_writer.write_analysis_sheet(writer, '直接人工_价量比', analysis_tables['直接人工_价量比'])
self.fast_writer.write_analysis_sheet(writer, '制造费用_价量比', analysis_tables['制造费用_价量比'])
self.fast_writer.write_work_order_sheet(writer, '按工单按产品异常值分析', work_order_sheet)
self.fast_writer.write_product_anomaly_sheet(writer, '按产品异常值分析', product_anomaly_sections)
self.fast_writer.write_dataframe_fast(
    writer,
    sheet_name='error_log',
    df=error_log,
    number_formats={},
)
```

- [ ] **Step 4: Run the routing and workbook tests to verify they pass**

Run: `conda run -n test python -m pytest tests/test_costing_etl.py::test_write_workbook_streams_hotspot_sheets_through_fast_writer tests/test_costing_etl.py::test_lightweight_export_uses_fixed_width_and_number_formats -q`

Expected: PASS

- [ ] **Step 5: Commit the hotspot streaming change**

```bash
git add src/excel/fast_writer.py src/excel/workbook_writer.py tests/test_costing_etl.py
git commit -m "feat(excel): stream large workbook sheets with xlsxwriter"
```

### Task 3: Replace Cell Fills With Conditional Formatting On The Work-Order Sheet

**Files:**
- Modify: `src/excel/fast_writer.py`
- Modify: `src/excel/workbook_writer.py`
- Modify: `tests/contracts/_workbook_contract_helper.py`
- Modify: `tests/contracts/test_workbook_contract.py`
- Modify: `tests/contracts/generate_baselines.py`
- Modify: `tests/contracts/baselines/workbook_semantics.json`
- Test: `tests/test_costing_etl.py`

- [ ] **Step 1: Write the failing contract and regression tests for conditional-format highlights**

```python
def test_work_order_highlight_contract_uses_conditional_formats(tmp_path) -> None:
    workbook_path = build_highlight_contract_workbook(tmp_path)
    actual = extract_highlight_semantics(workbook_path)

    assert actual['sheet'] == '按工单按产品异常值分析'
    assert {
        'sqref': 'J2:J2',
        'formula': ['=$R2="关注"'],
        'fill': 'DDEBF7',
        'font': None,
    } in actual['rules']
    assert {
        'sqref': 'N2:N2',
        'formula': ['=$V2="高度可疑"'],
        'fill': '4472C4',
        'font': 'FFFFFF',
    } in actual['rules']
```

```python
def extract_highlight_semantics(workbook_path: Path) -> dict[str, object]:
    workbook = load_workbook(workbook_path)
    worksheet = workbook['按工单按产品异常值分析']
    rules: list[dict[str, object]] = []

    for sqref, rule_list in worksheet.conditional_formatting._cf_rules.items():
        for rule in rule_list:
            rules.append(
                {
                    'sqref': str(sqref),
                    'formula': list(rule.formula),
                    'fill': _rgb_suffix(rule.dxf.fill.fgColor) if rule.dxf and rule.dxf.fill else None,
                    'font': _rgb_suffix(rule.dxf.font.color) if rule.dxf and rule.dxf.font and rule.dxf.font.color else None,
                }
            )

    return {'sheet': worksheet.title, 'rules': rules}
```

- [ ] **Step 2: Run the highlight contract test to verify it fails**

Run: `conda run -n test python -m pytest tests/contracts/test_workbook_contract.py::test_highlight_semantics_match_baseline tests/test_costing_etl.py::test_work_order_highlight_contract_uses_conditional_formats -q`

Expected: FAIL because the current export code still bakes literal fills into cells instead of emitting conditional-format rules.

- [ ] **Step 3: Implement work-order conditional formatting and regenerate the workbook baseline**

```python
# src/excel/fast_writer.py
WORK_ORDER_HIGHLIGHT_COLUMNS = (
    ('直接材料单位完工成本', '直接材料异常标记'),
    ('直接人工单位完工成本', '直接人工异常标记'),
    ('制造费用单位完工成本', '制造费用异常标记'),
    ('制造费用_其他单位完工成本', '制造费用_其他异常标记'),
    ('制造费用_人工单位完工成本', '制造费用_人工异常标记'),
    ('制造费用_机物料及低耗单位完工成本', '制造费用_机物料及低耗异常标记'),
    ('制造费用_折旧单位完工成本', '制造费用_折旧异常标记'),
    ('制造费用_水电费单位完工成本', '制造费用_水电费异常标记'),
)


def apply_work_order_conditional_formats(self, worksheet, headers: dict[str, int], max_row: int, formats) -> None:
    for value_column, flag_column in WORK_ORDER_HIGHLIGHT_COLUMNS:
        value_idx = headers.get(value_column)
        flag_idx = headers.get(flag_column)
        if value_idx is None or flag_idx is None:
            continue

        flag_letter = xl_col_to_name(flag_idx)
        for target_idx, label, fmt in (
            (value_idx, '关注', formats.attention),
            (flag_idx, '关注', formats.attention),
            (value_idx, '高度可疑', formats.suspicious),
            (flag_idx, '高度可疑', formats.suspicious),
        ):
            target_letter = xl_col_to_name(target_idx)
            worksheet.conditional_format(
                f'{target_letter}2:{target_letter}{max_row}',
                {
                    'type': 'formula',
                    'criteria': f'=${flag_letter}2="{label}"',
                    'format': fmt,
                },
            )
```

```python
# tests/contracts/generate_baselines.py
from tests.contracts._workbook_contract_helper import (
    build_default_contract_workbook,
    build_highlight_contract_workbook,
    extract_highlight_semantics,
    extract_workbook_semantics,
)

default_path = build_default_contract_workbook(tmp_path)
highlight_path = build_highlight_contract_workbook(tmp_path)
payload = {
    'default_workbook': extract_workbook_semantics(default_path),
    'highlight_workbook': extract_highlight_semantics(highlight_path),
}
```

Run: `conda run -n test python tests/contracts/generate_baselines.py`

- [ ] **Step 4: Run the contract and regression tests to verify they pass**

Run: `conda run -n test python -m pytest tests/contracts/test_workbook_contract.py tests/test_costing_etl.py::test_work_order_highlight_contract_uses_conditional_formats -q`

Expected: PASS

- [ ] **Step 5: Commit the conditional-format highlight change**

```bash
git add src/excel/fast_writer.py src/excel/workbook_writer.py tests/contracts/_workbook_contract_helper.py tests/contracts/test_workbook_contract.py tests/contracts/generate_baselines.py tests/contracts/baselines/workbook_semantics.json tests/test_costing_etl.py
git commit -m "feat(excel): move work order highlights to conditional formatting"
```

### Task 4: Add Stage Timing Logs And Release Export-Only References

**Files:**
- Modify: `src/etl/costing_etl.py`
- Test: `tests/test_costing_etl.py`

- [ ] **Step 1: Write the failing timing-log regression test**

```python
def test_process_file_logs_read_transform_export_timings(caplog, tmp_path) -> None:
    caplog.set_level('INFO')
    etl = CostingWorkbookETL(skip_rows=2, product_order=())
    input_path = tmp_path / 'input.xlsx'
    output_path = tmp_path / 'output.xlsx'

    df_raw = pd.DataFrame({'子项物料编码': ['MAT-001'], '成本项目名称': ['直接材料'], '年期': ['2025年1期']})
    df_detail = pd.DataFrame([{'月份': '2025年01期', '产品编码': 'P-1', '产品名称': '产品', '工单编号': 'WO-1', '工单行号': 1, '成本项目名称': '直接材料', '本期完工金额': 10.0}])
    df_qty = pd.DataFrame([{'月份': '2025年01期', '产品编码': 'P-1', '产品名称': '产品', '工单编号': 'WO-1', '工单行号': 1, '本期完工数量': 1.0, '本期完工金额': 10.0}])

    with (
        patch('src.etl.costing_etl.pd.read_excel', return_value=df_raw),
        patch.object(CostingWorkbookETL, '_split_sheets', return_value=(df_detail, df_qty)),
    ):
        assert etl.process_file(input_path, output_path) is True

    messages = [record.message for record in caplog.records]
    assert any('Timing | stage=read' in message for message in messages)
    assert any('Timing | stage=transform' in message for message in messages)
    assert any('Timing | stage=export' in message for message in messages)
```

- [ ] **Step 2: Run the timing test to verify it fails**

Run: `conda run -n test python -m pytest tests/test_costing_etl.py::test_process_file_logs_read_transform_export_timings -q`

Expected: FAIL because `process_file()` does not yet emit per-stage timing logs.

- [ ] **Step 3: Add timing instrumentation and release export-only references**

```python
# src/etl/costing_etl.py
from time import perf_counter


def process_file(self, input_path: Path, output_path: Path) -> bool:
    try:
        total_start = perf_counter()

        read_start = perf_counter()
        df_raw = self._load_raw_dataframe(input_path)
        logger.info('Timing | stage=read | seconds=%.3f', perf_counter() - read_start)

        transform_start = perf_counter()
        df_raw.columns = [clean_column_name(c) for c in df_raw.columns]
        resolved_columns = self._resolve_columns(df_raw)
        if resolved_columns.rename_map:
            df_raw.rename(columns=resolved_columns.rename_map, inplace=True)
        target_mat = resolved_columns.child_material_column
        target_item = resolved_columns.cost_item_column
        df_raw = self._remove_total_rows(df_raw)
        df_filled = self._forward_fill_with_rules(df_raw)
        df_detail, df_qty = self._split_sheets(df_raw, df_filled, target_mat, target_item)
        artifacts = build_report_artifacts(
            df_detail,
            df_qty,
            standalone_cost_items=self.standalone_cost_items,
        )
        logger.info('Timing | stage=transform | seconds=%.3f', perf_counter() - transform_start)

        export_start = perf_counter()
        analysis_fact_df = self._filter_fact_df_for_analysis(artifacts.fact_df)
        analysis_tables = render_tables(analysis_fact_df)
        filtered_work_order_sheet = FlatSheet(
            data=self._filter_dataframe_by_whitelist(
                artifacts.work_order_sheet.data,
                code_col='产品编码',
                name_col='产品名称',
                sort_cols=['月份', '工单编号', '工单行'],
            ),
            column_types=artifacts.work_order_sheet.column_types,
        )
        product_anomaly_sections = self._filter_product_anomaly_sections(artifacts.product_anomaly_sections)
        error_log = artifacts.error_log.copy()
        self.workbook_writer.write_workbook(
            output_path,
            detail_df=df_detail,
            qty_sheet_df=artifacts.qty_sheet_df,
            analysis_tables=analysis_tables,
            work_order_sheet=filtered_work_order_sheet,
            product_anomaly_sections=product_anomaly_sections,
            error_log=error_log,
        )
        logger.info('Timing | stage=export | seconds=%.3f', perf_counter() - export_start)
        logger.info('Timing | stage=total | seconds=%.3f', perf_counter() - total_start)

        del analysis_tables
        del filtered_work_order_sheet
        del product_anomaly_sections
        del error_log
        return True
```

- [ ] **Step 4: Run the timing regression test to verify it passes**

Run: `conda run -n test python -m pytest tests/test_costing_etl.py::test_process_file_logs_read_transform_export_timings -q`

Expected: PASS

- [ ] **Step 5: Commit the timing instrumentation**

```bash
git add src/etl/costing_etl.py tests/test_costing_etl.py
git commit -m "feat(etl): log export stage timings"
```

### Task 5: Full Verification On Tests, Contracts, And Real Samples

**Files:**
- Modify: `tests/contracts/baselines/workbook_semantics.json`
- Test: `tests/contracts/test_workbook_contract.py`
- Test: `tests/test_costing_etl.py`
- Test: `tests/test_runner.py`

- [ ] **Step 1: Run the workbook and ETL regression subset**

Run: `conda run -n test python -m pytest tests/contracts/test_workbook_contract.py tests/test_costing_etl.py tests/test_runner.py -q`

Expected: PASS

- [ ] **Step 2: Run the full automated suite**

Run: `conda run -n test python -m pytest tests -q`

Expected: PASS with no new failures.

- [ ] **Step 3: Run static checks**

Run: `conda run -n test python -m ruff check src tests`

Expected: `All checks passed!`

- [ ] **Step 4: Run the real `gb` and `sk` samples and record timing deltas**

Run: `conda run -n test python main.py gb`

Expected: command exits `0`, emits `pipeline=gb`, writes `data/processed/gb/*_处理后.xlsx`, and logs `Timing | stage=export`.

Run: `conda run -n test python main.py sk`

Expected: command exits `0`, emits `pipeline=sk`, writes `data/processed/sk/*_处理后.xlsx`, and logs `Timing | stage=export`.

- [ ] **Step 5: Commit the final baseline and verification updates**

```bash
git add tests/contracts/baselines/workbook_semantics.json
git commit -m "test(contracts): refresh workbook semantics baseline"
```

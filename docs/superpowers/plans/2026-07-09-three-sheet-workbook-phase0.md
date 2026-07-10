# Three-Sheet Workbook Phase 0 Implementation Plan

> Compatibility note (2026-07-10): this is a historical implementation plan. Python environment commands were updated from the retired conda environment to the current uv-managed `.venv` so its checks remain runnable; the design prose still describes Phase 0 at the time.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Change the Python production workbook contract from four default sheets to three default sheets, without starting the Rust sidecar spike.

**Architecture:** Phase 0 stops default SheetModel construction and workbook writing for `成本分析产品维度`, while keeping product-dimension analysis helpers available as legacy/helper code. The main output path remains Python/xlsxwriter, and both workbook writer entry points must produce the same 3-sheet default contract.

**Tech Stack:** Python 3.11+, pandas, polars, xlsxwriter, openpyxl, pytest, ruff, project uv/.venv environment.

## Global Constraints

- Default workbook sheet order must be exactly `成本计算单总表`, `成本计算单数量聚合维度`, `成本分析工单维度`.
- Default workbook must not contain `成本分析产品维度`.
- Phase 0 is a production behavior change, not a throwaway spike.
- Phase 0 must be completed, verified, benchmarked, and reported before any Phase 1 Rust sidecar work starts.
- Phase 0 first version uses the minimum-risk path: `build_report_artifacts` may continue calculating `product_anomaly_sections`; `build_sheet_models` must not convert them into a default output sheet.
- Keep product-dimension legacy/helper code for now, including `ProductAnomalySection`, `build_product_anomaly_sections`, and `product_anomaly_writer`.
- Keep `product_anomaly_scope_mode` as a compatibility parameter for now; it must not cause a default product-dimension workbook sheet to be written.
- Do not modify or stage unrelated untracked files, especially `uv.lock` and unrelated docs.
- Use `uv run ...` for tests, lint, and benchmark commands.
- Follow TDD: write or update the failing test first, verify the failure, then implement the minimal code.

---

## File Structure

- Modify `src/analytics/presentation_builder.py`
  - Responsibility: Build default workbook `SheetModel` objects. After Phase 0, this returns only the three default sheets.

- Modify `src/excel/workbook_writer.py`
  - Responsibility: Write workbook sheets from either legacy DataFrame inputs or `SheetModel` inputs. After Phase 0, both writer entry points must avoid writing `成本分析产品维度` by default.

- Modify `tests/test_costing_etl.py`
  - Responsibility: Unit and integration coverage for sheet model construction, workbook writer behavior, and workbook output.

- Modify `tests/contracts/_workbook_contract_helper.py`
  - Responsibility: Generate and extract workbook semantic contract snapshots.

- Modify `tests/contracts/baselines/workbook_semantics.json`
  - Responsibility: Store expected 3-sheet workbook semantics after contract regeneration.

- Modify `tests/contracts/README.md`
  - Responsibility: Explain workbook contract coverage now freezes three default sheets, not four.

- Modify `README.md`
  - Responsibility: User-facing documentation of default output sheets and module descriptions.

- Read-only during implementation: `docs/rust_xlsxwriter_sidecar_spike_spec.md`
  - Responsibility: Source specification for the Phase 0 gate. Do not broaden implementation beyond this plan.

---

### Task 1: SheetModel Builder Returns Three Default Sheets

**Files:**
- Modify: `tests/test_costing_etl.py`
- Modify: `src/analytics/presentation_builder.py`

**Interfaces:**
- Consumes: `build_sheet_models(detail_df, qty_sheet_df, fact_bundle, work_order_sheet, product_anomaly_sections) -> tuple[SheetModel, ...]`
- Produces: A default `tuple[SheetModel, ...]` containing exactly three models in order: detail, qty, work-order.

- [ ] **Step 1: Update the default sheet list test to expect three sheets**

In `tests/test_costing_etl.py`, replace `test_build_sheet_models_outputs_four_business_named_sheets` with:

```python
def test_build_sheet_models_outputs_three_default_business_sheets() -> None:
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
    ]
    assert all(model.sheet_name != '成本分析产品维度' for model in models)
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
uv run python -m pytest tests/test_costing_etl.py::test_build_sheet_models_outputs_three_default_business_sheets -q
```

Expected: FAIL because `build_sheet_models()` still returns `成本分析产品维度`.

- [ ] **Step 3: Add coverage that product sections are accepted but ignored by default**

In `tests/test_costing_etl.py`, replace the product-model assertions inside `test_build_sheet_models_avoids_pyarrow_dependency_for_pandas_inputs` with:

```python
    assert [model.sheet_name for model in models] == [
        '成本计算单总表',
        '成本计算单数量聚合维度',
        '成本分析工单维度',
    ]
    assert all(model.sheet_name != '成本分析产品维度' for model in models)
```

Also rename `test_build_sheet_models_serializes_scope_label_for_product_anomaly_rows` to:

```python
def test_build_sheet_models_ignores_product_anomaly_sections_for_default_contract() -> None:
    models = build_sheet_models(
        detail_df=pd.DataFrame([{'月份': '2025年01期', '产品编码': 'P001'}]),
        qty_sheet_df=pd.DataFrame(
            [{'月份': '2025年01期', '产品编码': 'P001', '本期完工数量': 10.0, '本期完工金额': 100.0}]
        ),
        fact_bundle=None,
        work_order_sheet=FlatSheet(data=pd.DataFrame([{'月份': '2025年01期'}]), column_types={'月份': 'text'}),
        product_anomaly_sections=[
            ProductAnomalySection(
                product_code='P001',
                product_name='产品A',
                data=pd.DataFrame([{'月份': '2025年01期', '总成本': 100.0, '完工数量': 10.0, '单位成本': 10.0}]),
                column_types={'月份': 'text', '总成本': 'amount', '完工数量': 'qty', '单位成本': 'price'},
                amount_columns=['总成本'],
                outlier_cells=set(),
                section_label='全部',
            )
        ],
    )

    assert [model.sheet_name for model in models] == [
        '成本计算单总表',
        '成本计算单数量聚合维度',
        '成本分析工单维度',
    ]
    assert all(model.sheet_name != '成本分析产品维度' for model in models)
```

- [ ] **Step 4: Run the updated focused tests and verify they fail**

Run:

```bash
uv run python -m pytest tests/test_costing_etl.py::test_build_sheet_models_outputs_three_default_business_sheets tests/test_costing_etl.py::test_build_sheet_models_avoids_pyarrow_dependency_for_pandas_inputs tests/test_costing_etl.py::test_build_sheet_models_ignores_product_anomaly_sections_for_default_contract -q
```

Expected: FAIL because implementation still builds and returns the product model.

- [ ] **Step 5: Implement the minimal builder change**

In `src/analytics/presentation_builder.py`, update `build_sheet_models()` by removing the product model construction from the default return path. Keep `_build_product_anomaly_frame()` in the file for now.

The resulting core should be:

```python
def build_sheet_models(
    *,
    detail_df: pl.DataFrame | pd.DataFrame,
    qty_sheet_df: pd.DataFrame | pl.DataFrame,
    fact_bundle: FactBundle | None,
    work_order_sheet: FlatSheet,
    product_anomaly_sections: list[ProductAnomalySection],
) -> tuple[SheetModel, ...]:
    """构建默认 workbook 的 3 张业务 SheetModel。"""
    detail_frame = _to_polars_frame(detail_df)
    qty_frame = _to_polars_frame(qty_sheet_df)

    work_order_frame = _to_polars_frame(work_order_sheet.data)
    work_order_column_types = dict(work_order_sheet.column_types)
    work_order_number_formats = _build_number_formats(work_order_column_types)

    detail_model = dataframe_to_sheet_model(
        sheet_name='成本计算单总表',
        frame=detail_frame,
        column_types=dict.fromkeys(detail_frame.columns, 'text'),
        number_formats={column: '#,##0.00' for column in detail_frame.columns if column in _DETAIL_TWO_DECIMAL_COLUMNS},
        write_mode='dataframe_fast',
        style_profile='lightweight_flat',
        source_frame=detail_frame,
    )
    qty_two_decimal_columns = _resolve_qty_two_decimal_columns(tuple(qty_frame.columns))
    qty_model = dataframe_to_sheet_model(
        sheet_name='成本计算单数量聚合维度',
        frame=qty_frame,
        column_types=dict.fromkeys(qty_frame.columns, 'text'),
        number_formats={column: '#,##0.00' for column in qty_frame.columns if column in qty_two_decimal_columns},
        write_mode='dataframe_fast',
        style_profile='lightweight_flat',
        source_frame=qty_frame,
    )

    work_order_model = dataframe_to_sheet_model(
        sheet_name='成本分析工单维度',
        frame=work_order_frame,
        column_types=work_order_column_types,
        number_formats=work_order_number_formats,
        write_mode='dataframe_fast',
        style_profile='lightweight_flat',
        source_frame=work_order_frame,
    )
    return (
        detail_model,
        qty_model,
        work_order_model,
    )
```

Do not remove the `product_anomaly_sections` parameter in this task. It keeps the call boundary stable for Phase 0.

- [ ] **Step 6: Run the focused tests and verify they pass**

Run:

```bash
uv run python -m pytest tests/test_costing_etl.py::test_build_sheet_models_outputs_three_default_business_sheets tests/test_costing_etl.py::test_build_sheet_models_avoids_pyarrow_dependency_for_pandas_inputs tests/test_costing_etl.py::test_build_sheet_models_ignores_product_anomaly_sections_for_default_contract tests/test_costing_etl.py::test_build_sheet_models_marks_detail_and_qty_as_fast_flat_sheets -q
```

Expected: PASS after updating `test_build_sheet_models_marks_detail_and_qty_as_fast_flat_sheets` to remove the `product_anomaly_model` assertions and keep the three fast-path assertions.

---

### Task 2: Workbook Writer Entry Points Use the Three-Sheet Default Contract

**Files:**
- Modify: `tests/test_costing_etl.py`
- Modify: `src/excel/workbook_writer.py`

**Interfaces:**
- Consumes: `CostingWorkbookWriter.write_workbook(...)`
- Consumes: `CostingWorkbookWriter.write_workbook_from_models(...)`
- Produces: Workbook files with the three default sheets only.

- [ ] **Step 1: Add a failing legacy-entry writer test**

Add this test in `tests/test_costing_etl.py` near other writer tests:

```python
def test_write_workbook_legacy_entrypoint_writes_three_default_sheets(tmp_path: Path) -> None:
    output_path = tmp_path / 'legacy_entrypoint_three_sheets.xlsx'
    writer = CostingWorkbookWriter()
    detail_df = pd.DataFrame([{'月份': '2025年01期', '本期完工单位成本': 10.0, '本期完工金额': 100.0}])
    qty_sheet_df = pd.DataFrame([{'月份': '2025年01期', '本期完工数量': 10.0, '本期完工金额': 100.0}])
    work_order_sheet = FlatSheet(
        data=pd.DataFrame([{'月份': '2025年01期', '本期完工数量': 10.0}]),
        column_types={'月份': 'text', '本期完工数量': 'qty'},
    )
    product_sections = [
        ProductAnomalySection(
            product_code='P001',
            product_name='产品A',
            data=pd.DataFrame([{'月份': '2025年01期', '总成本': 100.0}]),
            column_types={'月份': 'text', '总成本': 'amount'},
            amount_columns=['总成本'],
            outlier_cells=set(),
        )
    ]

    writer.write_workbook(
        output_path,
        detail_df=detail_df,
        qty_sheet_df=qty_sheet_df,
        work_order_sheet=work_order_sheet,
        product_anomaly_sections=product_sections,
    )

    workbook = load_workbook(output_path)
    assert workbook.sheetnames == [
        '成本计算单总表',
        '成本计算单数量聚合维度',
        '成本分析工单维度',
    ]
    assert '成本分析产品维度' not in workbook.sheetnames
```

- [ ] **Step 2: Run the new test and verify it fails**

Run:

```bash
uv run python -m pytest tests/test_costing_etl.py::test_write_workbook_legacy_entrypoint_writes_three_default_sheets -q
```

Expected: FAIL because `write_workbook()` still writes `成本分析产品维度`.

- [ ] **Step 3: Add a failing model-entry writer test**

Add this test:

```python
def test_write_workbook_from_models_writes_three_default_sheets(tmp_path: Path) -> None:
    output_path = tmp_path / 'models_three_sheets.xlsx'
    writer = CostingWorkbookWriter()
    sheet_models = build_sheet_models(
        detail_df=pd.DataFrame([{'月份': '2025年01期', '本期完工金额': 100.0}]),
        qty_sheet_df=pd.DataFrame([{'月份': '2025年01期', '本期完工金额': 100.0}]),
        fact_bundle=None,
        work_order_sheet=FlatSheet(
            data=pd.DataFrame([{'月份': '2025年01期', '产品编码': 'P001'}]),
            column_types={'月份': 'text', '产品编码': 'text'},
        ),
        product_anomaly_sections=[],
    )

    writer.write_workbook_from_models(output_path, sheet_models=sheet_models)

    workbook = load_workbook(output_path)
    assert workbook.sheetnames == [
        '成本计算单总表',
        '成本计算单数量聚合维度',
        '成本分析工单维度',
    ]
    assert '成本分析产品维度' not in workbook.sheetnames
```

- [ ] **Step 4: Run the new model-entry test**

Run:

```bash
uv run python -m pytest tests/test_costing_etl.py::test_write_workbook_from_models_writes_three_default_sheets -q
```

Expected: PASS after Task 1, because `build_sheet_models()` now provides only three models.

- [ ] **Step 5: Implement the legacy-entry writer change**

In `src/excel/workbook_writer.py`, remove this default write from `write_workbook()`:

```python
self.sheet_writer.write_product_anomaly_sheet(writer, '成本分析产品维度', product_anomaly_sections)
```

Keep the `product_anomaly_sections` argument for compatibility in Phase 0.

- [ ] **Step 6: Run the writer tests**

Run:

```bash
uv run python -m pytest tests/test_costing_etl.py::test_write_workbook_legacy_entrypoint_writes_three_default_sheets tests/test_costing_etl.py::test_write_workbook_from_models_writes_three_default_sheets -q
```

Expected: PASS.

---

### Task 3: Preserve Product-Dimension Helpers as Legacy Helper Tests

**Files:**
- Modify: `tests/test_costing_etl.py`

**Interfaces:**
- Consumes: `src.excel.product_anomaly_writer.write_product_anomaly_sheet(writer, sheet_name, sections) -> None`
- Produces: Tests proving product-dimension helper rendering still works outside the default workbook contract.

- [ ] **Step 1: Change product-dimension writer tests to call the helper directly**

Replace the body of `test_workbook_writer_sheet_model_preserves_product_anomaly_legacy_layout` with a direct helper call and rename it:

```python
def test_product_anomaly_writer_legacy_helper_preserves_layout(tmp_path: Path) -> None:
    from src.excel.product_anomaly_writer import write_product_anomaly_sheet

    output_path = tmp_path / 'product_anomaly_legacy_helper.xlsx'
    sections = [
        ProductAnomalySection(
            product_code='P001',
            product_name='产品A',
            data=pd.DataFrame([{'月份': '2025年01期', '总成本': 100.0, '完工数量': 10.0, '单位成本': 10.0}]),
            column_types={'月份': 'text', '总成本': 'amount', '完工数量': 'qty', '单位成本': 'price'},
            amount_columns=['总成本'],
            outlier_cells=set(),
        )
    ]

    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        write_product_anomaly_sheet(writer, '成本分析产品维度', sections)

    workbook = load_workbook(output_path)
    worksheet = workbook['成本分析产品维度']
    assert worksheet['A1'].value == '产品编码'
    assert worksheet['A2'].value == 'P001'
    assert worksheet['B1'].value == '产品名称'
    assert worksheet['B2'].value == '产品A'
    assert worksheet['A3'].value == '月份'
    assert worksheet['A4'].value == '2025年01期'
    assert worksheet.freeze_panes == 'A4'
```

- [ ] **Step 2: Change the scoped helper test to call the helper directly**

Replace the body of `test_workbook_writer_sheet_model_renders_product_anomaly_scope_split_layout_for_gb` and rename it:

```python
def test_product_anomaly_writer_scoped_helper_preserves_layout(tmp_path: Path) -> None:
    from src.excel.product_anomaly_writer import write_product_anomaly_sheet

    output_path = tmp_path / 'product_anomaly_scoped_helper.xlsx'
    sections = [
        ProductAnomalySection(
            product_code='P001',
            product_name='产品A',
            data=pd.DataFrame([{'月份': '2025年01期', '总成本': 100.0, '完工数量': 10.0, '单位成本': 10.0}]),
            column_types={'月份': 'text', '总成本': 'amount', '完工数量': 'qty', '单位成本': 'price'},
            amount_columns=['总成本'],
            outlier_cells=set(),
            section_label='全部',
        ),
        ProductAnomalySection(
            product_code='P001',
            product_name='产品A',
            data=pd.DataFrame([{'月份': '2025年01期', '总成本': 80.0, '完工数量': 8.0, '单位成本': 10.0}]),
            column_types={'月份': 'text', '总成本': 'amount', '完工数量': 'qty', '单位成本': 'price'},
            amount_columns=['总成本'],
            outlier_cells=set(),
            section_label='正常生产',
        ),
    ]

    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        write_product_anomaly_sheet(writer, '成本分析产品维度', sections)

    workbook = load_workbook(output_path)
    worksheet = workbook['成本分析产品维度']
    assert worksheet['A1'].value == '产品编码'
    assert worksheet['A2'].value == 'P001'
    assert worksheet['B1'].value == '产品名称'
    assert worksheet['B2'].value == '产品A'
    assert worksheet['A3'].value == '分析口径'
    assert worksheet['B3'].value == '全部'
    assert worksheet['A4'].value == '月份'
    assert worksheet['A5'].value == '2025年01期'
    second_section_start_row = 7
    assert worksheet.cell(row=second_section_start_row, column=1).value == '产品编码'
    assert worksheet.cell(row=second_section_start_row, column=2).value == '产品名称'
    assert worksheet.freeze_panes == 'A5'
```

- [ ] **Step 3: Run the helper tests**

Run:

```bash
uv run python -m pytest tests/test_costing_etl.py::test_product_anomaly_writer_legacy_helper_preserves_layout tests/test_costing_etl.py::test_product_anomaly_writer_scoped_helper_preserves_layout -q
```

Expected: PASS. These tests should not depend on default `build_sheet_models()`.

---

### Task 4: Update Workbook Integration Tests to Three Sheets

**Files:**
- Modify: `tests/test_costing_etl.py`

**Interfaces:**
- Consumes: `CostingWorkbookETL.process_file(input_path, output_path) -> bool`
- Produces: Integration assertions that default output has exactly three sheets and no product-dimension sheet.

- [ ] **Step 1: Update the main workbook integration expected sheets**

In the integration test around the current `expected_sheets = [...]` assertion for `pd.ExcelFile(output_path)`, replace the expected list with:

```python
expected_sheets = [
    '成本计算单总表',
    '成本计算单数量聚合维度',
    '成本分析工单维度',
]
assert xls.sheet_names == expected_sheets
```

Delete assertions that read:

```python
ws_product = wb['成本分析产品维度']
assert ws_product['A1'].value == '产品编码'
assert ws_product['A2'].value == 'GB_C.D.B0040AA'
assert ws_product['A3'].value == '月份'
assert ws_product.freeze_panes == 'A4'
```

Add:

```python
assert '成本分析产品维度' not in wb.sheetnames
```

- [ ] **Step 2: Update any second workbook sheet-name assertion**

Find the later assertion that currently expects:

```python
assert xls.sheet_names == [
    '成本计算单总表',
    '成本计算单数量聚合维度',
    '成本分析工单维度',
    '成本分析产品维度',
]
```

Replace it with:

```python
assert xls.sheet_names == [
    '成本计算单总表',
    '成本计算单数量聚合维度',
    '成本分析工单维度',
]
```

- [ ] **Step 3: Run the changed integration tests**

Run the tests by exact name after locating them with `rg -n "expected_sheets|xls.sheet_names" tests/test_costing_etl.py`:

```bash
uv run python -m pytest tests/test_costing_etl.py -q
```

Expected: PASS after Tasks 1-3, or fail only on remaining assertions that still expect `成本分析产品维度`. Update only those default-output assertions; do not remove product helper tests.

---

### Task 5: Update Workbook Contract Baseline to Three Sheets

**Files:**
- Modify: `tests/contracts/_workbook_contract_helper.py`
- Modify: `tests/contracts/baselines/workbook_semantics.json`
- Modify: `tests/contracts/README.md`

**Interfaces:**
- Consumes: `tests/contracts/generate_baselines.py`
- Produces: 3-sheet workbook semantic baseline.

- [ ] **Step 1: Update contract helper default sheets**

In `tests/contracts/_workbook_contract_helper.py`, change `DEFAULT_SHEETS` to:

```python
DEFAULT_SHEETS = (
    '成本计算单总表',
    '成本计算单数量聚合维度',
    '成本分析工单维度',
)
```

Keep `_extract_product_anomaly_sheet()`, `_extract_legacy_product_anomaly_sheet()`, and `_extract_scoped_product_anomaly_sheet()` for now. They are legacy helper extractors and may still be useful for direct helper tests later.

- [ ] **Step 2: Run the contract test and verify baseline mismatch**

Run:

```bash
uv run python -m pytest tests/contracts/test_workbook_contract.py::test_default_workbook_semantics_match_baseline -q
```

Expected: FAIL because `workbook_semantics.json` still contains `成本分析产品维度`.

- [ ] **Step 3: Regenerate workbook contract baseline**

Run:

```bash
uv run python tests/contracts/generate_baselines.py
```

Expected: `tests/contracts/baselines/workbook_semantics.json` is rewritten with three default sheets.

- [ ] **Step 4: Inspect the regenerated baseline**

Run:

```bash
rg -n "sheet_order|成本分析产品维度|成本分析工单维度" tests/contracts/baselines/workbook_semantics.json
```

Expected:

```text
sheet_order exists
成本分析工单维度 exists
成本分析产品维度 does not appear under default_workbook
```

If `成本分析产品维度` still appears, stop and inspect whether `build_sheet_models()` or `write_workbook()` still writes the fourth sheet.

- [ ] **Step 5: Update contract README wording**

In `tests/contracts/README.md`, replace wording that freezes four sheets with wording that freezes three default sheets:

```markdown
- `baselines/workbook_semantics.json`

该基线冻结默认 3 张 workbook Sheet 的顺序、列序、freeze panes、auto filter、number format、column width 和工单异常高亮位置。
```

- [ ] **Step 6: Run contract tests**

Run:

```bash
uv run python -m pytest tests/contracts -q
```

Expected: PASS.

---

### Task 6: Update User-Facing README

**Files:**
- Modify: `README.md`

**Interfaces:**
- Produces: README that describes the 3-sheet default contract and treats product-dimension helpers as non-default legacy/helper code.

- [ ] **Step 1: Update feature summary**

In `README.md`, replace:

```markdown
- 输出 4 张业务工作表，覆盖成本总表、数量聚合、工单维度异常和产品维度摘要
```

with:

```markdown
- 默认输出 3 张业务工作表，覆盖成本总表、数量聚合和工单维度异常
```

- [ ] **Step 2: Update output sheet list**

Replace:

```markdown
每个处理后的工作簿默认按顺序输出以下 4 张 Sheet：
- `成本计算单总表`
- `成本计算单数量聚合维度`
- `成本分析工单维度`
- `成本分析产品维度`
```

with:

```markdown
每个处理后的工作簿默认按顺序输出以下 3 张 Sheet：
- `成本计算单总表`
- `成本计算单数量聚合维度`
- `成本分析工单维度`
```

- [ ] **Step 3: Remove default product-dimension output description**

Delete the README subsection that starts with:

```markdown
- 产品维度摘要页：`成本分析产品维度`
```

Do not delete descriptions of cost, quantity, or work-order anomaly outputs.

- [ ] **Step 4: Adjust module description**

Replace:

```markdown
  - `table_rendering.py` - 产品维度摘要页渲染
```

with:

```markdown
  - `table_rendering.py` - 产品维度 legacy/helper 渲染逻辑（不属于默认 workbook 输出）
```

- [ ] **Step 5: Check README for stale default-output wording**

Run:

```bash
rg -n "4 张|4张|成本分析产品维度|产品维度摘要页" README.md
```

Expected: No claim that default output has 4 sheets or includes `成本分析产品维度`. A legacy/helper mention for `table_rendering.py` is acceptable.

---

### Task 7: Phase 0 Full Verification and Benchmark

**Files:**
- No source changes expected.
- Read/write generated output under `data/processed/gb/` only through existing CLI behavior.

**Interfaces:**
- Consumes: `main.py gb --benchmark`
- Produces: Phase 0 verification results and Python 3-sheet baseline.

- [ ] **Step 1: Run focused tests**

Run:

```bash
uv run python -m pytest tests/contracts tests/test_costing_etl.py tests/test_runner.py -q
```

Expected: PASS.

- [ ] **Step 2: Run full tests**

Run:

```bash
uv run python -m pytest tests -q
```

Expected: PASS.

- [ ] **Step 3: Run lint**

Run:

```bash
uv run python -m ruff check src tests
```

Expected: PASS.

- [ ] **Step 4: Run format check**

Run:

```bash
uv run python -m ruff format src tests --check
```

Expected: PASS.

- [ ] **Step 5: Run GB check-only benchmark**

Run:

```bash
uv run python main.py gb --check-only --benchmark
```

Expected:

```text
The command succeeds.
No workbook is written by check-only mode.
Console output includes stage timings and error_log_count.
```

Record the payload/check-only seconds in the Phase 0 report.

- [ ] **Step 6: Run GB full benchmark three times**

Run three times:

```bash
uv run python main.py gb --benchmark
```

Expected each run:

```text
The command succeeds.
The output workbook has exactly 3 sheets.
Console output includes export timing.
```

If the output workbook already exists and the CLI prompts or refuses overwrite, follow the existing project behavior for overwrite confirmation; do not delete unrelated files.

- [ ] **Step 7: Verify the generated workbook sheet names**

Use the actual generated workbook path printed by the CLI. Run:

```bash
uv run python -c "from openpyxl import load_workbook; p=r'data/processed/gb/gb-成本计算单_2026070916484310_100160_处理后.xlsx'; wb=load_workbook(p, read_only=True); print(wb.sheetnames)"
```

Expected:

```text
['成本计算单总表', '成本计算单数量聚合维度', '成本分析工单维度']
```

If the filename differs because of month filtering or input selection, substitute the path printed by the CLI.

- [ ] **Step 8: Prepare the Phase 0 report**

Report:

```text
Changed files
Focused tests
Full tests
Ruff check
Ruff format --check
check-only payload seconds
run_1_python_3sheet_export_seconds
run_2_python_3sheet_export_seconds
run_3_python_3sheet_export_seconds
median_python_3sheet_export_seconds
generated workbook path
sheet names
```

Do not start Phase 1 until this report is reviewed.

---

## Self-Review

### Spec Coverage

- Phase 0 production gate is covered by Tasks 1-7.
- Three default sheets are covered by Tasks 1, 2, 4, 5, and 7.
- Product-dimension helper retention is covered by Task 3.
- `write_workbook_from_models` and `write_workbook` are both covered by Task 2.
- README and contract docs are covered by Tasks 5 and 6.
- Phase 0 benchmark is covered by Task 7.
- Phase 1 Rust sidecar is intentionally not implemented by this plan because the spec requires Phase 0 to pass first.

### Placeholder Scan

Placeholder scan passed. Any path that depends on runtime output is explicitly described as using the CLI printed path.

### Type Consistency

The plan keeps existing public signatures stable in Phase 0:

```python
build_sheet_models(
    *,
    detail_df: pl.DataFrame | pd.DataFrame,
    qty_sheet_df: pd.DataFrame | pl.DataFrame,
    fact_bundle: FactBundle | None,
    work_order_sheet: FlatSheet,
    product_anomaly_sections: list[ProductAnomalySection],
) -> tuple[SheetModel, ...]
```

```python
CostingWorkbookWriter.write_workbook(...)
CostingWorkbookWriter.write_workbook_from_models(...)
```

No task requires removing `ProductAnomalySection` or `product_anomaly_scope_mode`.

# GUI Whitelist Search And Anomaly Output Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add candidate product contains-search in the GUI, replace ambiguous work-order anomaly audit columns with one `异常明细解释` column, and compact the `成本分析产品维度` workbook layout.

**Architecture:** Keep business rules stable and change only presentation surfaces. GUI search filters the displayed candidate table without changing whitelist exact matching; anomaly scoring still computes the same scores and levels but formats all flagged metric audit details into one text column; product dimension writer keeps product-block layout while starting at row 1.

**Tech Stack:** Python 3.11+, PySide6, pandas, xlsxwriter, openpyxl contract tests, pytest, Ruff.

---

## File Structure

- Modify: `src/gui/main_window.py`
  - Owns candidate search UI state and display filtering.
  - Add `candidate_search_edit`, `candidate_products_all`, and helper methods that filter by product code/name contains match.

- Modify: `tests/test_gui_main_window.py`
  - Adds GUI tests for product code contains search, product name contains search, clearing search, adding visible selected rows, and clearing stale search state.

- Modify: `src/analytics/anomaly.py`
  - Owns work-order anomaly sheet columns and anomaly explanation text generation.
  - Removes old five audit output columns and adds `异常明细解释`.

- Modify: `tests/test_pq_analysis_v3.py`
  - Updates anomaly audit tests to assert `异常明细解释`, effective work-order count wording, and unchanged scoring decisions.

- Modify: `src/excel/product_anomaly_writer.py`
  - Owns special product dimension layout writer.
  - Removes old title row and shifts legacy/scoped blocks upward.

- Modify: `src/analytics/presentation_builder.py`
  - Updates product dimension freeze pane metadata to match compact layout.

- Modify: `tests/test_costing_etl.py`
  - Updates writer-level assertions for compact product dimension layout.

- Modify: `tests/contracts/_workbook_contract_helper.py`
  - Updates product dimension semantic extraction to read compact row positions.

- Modify: `tests/contracts/baselines/workbook_semantics.json`
  - Updates expected workbook contract after intentional column and layout changes.

- Modify: `README.md`
  - Documents GUI candidate contains-search and the `异常明细解释` field口径.

- Modify: `AGENTS.md`
  - Syncs current business rules for workbook columns and GUI whitelist search.

---

### Task 1: GUI Candidate Product Search

**Files:**
- Modify: `tests/test_gui_main_window.py`
- Modify: `src/gui/main_window.py`

- [ ] **Step 1: Write failing GUI candidate search tests**

Append these tests near existing candidate table tests in `tests/test_gui_main_window.py`:

```python
def test_candidate_search_filters_by_product_code_contains(main_window: MainWindow) -> None:
    result = CostingRunResult(
        status=ServiceStatus.SUCCEEDED,
        message='预检通过',
        candidate_products=(
            ('DP.C.P0197AA', '动力线'),
            ('DP.C.P0201AA', '动力线'),
            ('GB_C.D.B0040AA', 'BMS-750W驱动器'),
        ),
    )
    main_window._on_worker_finished(result, task_kind='scan')

    main_window.candidate_search_edit.setText('P0197')

    assert main_window._table_pairs(main_window.candidate_table) == (('DP.C.P0197AA', '动力线'),)


def test_candidate_search_filters_by_product_name_and_clear_restores_all(main_window: MainWindow) -> None:
    result = CostingRunResult(
        status=ServiceStatus.SUCCEEDED,
        message='预检通过',
        candidate_products=(
            ('DP.C.P0197AA', '动力线'),
            ('DP.C.P0246AA', '动力抱闸线'),
            ('GB_C.D.B0040AA', 'BMS-750W驱动器'),
        ),
    )
    main_window._on_worker_finished(result, task_kind='scan')

    main_window.candidate_search_edit.setText('抱闸')

    assert main_window._table_pairs(main_window.candidate_table) == (('DP.C.P0246AA', '动力抱闸线'),)

    main_window.candidate_search_edit.clear()

    assert main_window._table_pairs(main_window.candidate_table) == (
        ('DP.C.P0197AA', '动力线'),
        ('DP.C.P0246AA', '动力抱闸线'),
        ('GB_C.D.B0040AA', 'BMS-750W驱动器'),
    )


def test_add_selected_candidates_uses_current_filtered_candidate_rows(main_window: MainWindow) -> None:
    result = CostingRunResult(
        status=ServiceStatus.SUCCEEDED,
        message='预检通过',
        candidate_products=(
            ('DP.C.P0197AA', '动力线'),
            ('DP.C.P0201AA', '动力线'),
            ('GB_C.D.B0040AA', 'BMS-750W驱动器'),
        ),
    )
    main_window._on_worker_finished(result, task_kind='scan')
    main_window._set_table_pairs(main_window.whitelist_table, ())
    main_window.candidate_search_edit.setText('B0040')
    main_window.candidate_table.selectRow(0)

    main_window._add_selected_candidates()

    assert main_window._table_pairs(main_window.whitelist_table) == (('GB_C.D.B0040AA', 'BMS-750W驱动器'),)


def test_candidate_search_state_clears_when_form_changes(main_window: MainWindow) -> None:
    result = CostingRunResult(
        status=ServiceStatus.SUCCEEDED,
        message='预检通过',
        candidate_products=(('DP.C.P0197AA', '动力线'),),
    )
    main_window._on_worker_finished(result, task_kind='scan')
    main_window.candidate_search_edit.setText('P0197')

    main_window.month_start_edit.setText('2025-01')

    assert main_window.candidate_search_edit.text() == ''
    assert main_window.candidate_products_all == ()
    assert main_window.candidate_table.rowCount() == 0
```

- [ ] **Step 2: Run GUI tests to verify they fail**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_gui_main_window.py::test_candidate_search_filters_by_product_code_contains tests/test_gui_main_window.py::test_candidate_search_filters_by_product_name_and_clear_restores_all tests/test_gui_main_window.py::test_add_selected_candidates_uses_current_filtered_candidate_rows tests/test_gui_main_window.py::test_candidate_search_state_clears_when_form_changes -q
```

Expected: FAIL with `AttributeError: 'MainWindow' object has no attribute 'candidate_search_edit'`.

- [ ] **Step 3: Add candidate search state and UI**

In `src/gui/main_window.py`, add `QLineEdit` is already imported, so only update `MainWindow.__init__`.

Add after `self.whitelist_action_buttons: list[QPushButton] = []`:

```python
self.candidate_products_all: ProductOrder = ()
```

Add after `self.candidate_table = QTableWidget(0, 2)`:

```python
self.candidate_search_edit = QLineEdit()
self.candidate_search_edit.setPlaceholderText('搜索产品编码或产品名称')
```

In `_build_ui()`, in the candidate group block, replace:

```python
self._setup_table(self.candidate_table, editable=False)
candidate_layout.addWidget(self.candidate_table)
```

with:

```python
self._setup_table(self.candidate_table, editable=False)
candidate_layout.addWidget(self.candidate_search_edit)
candidate_layout.addWidget(self.candidate_table)
```

In `_connect_signals()`, add after the month edit signal connections:

```python
self.candidate_search_edit.textChanged.connect(self._refresh_candidate_table)
```

- [ ] **Step 4: Add candidate filtering helpers**

In `src/gui/main_window.py`, add these methods after `_table_pairs()`:

```python
    def _clear_candidate_products(self) -> None:
        previous_block_state = self.candidate_search_edit.blockSignals(True)
        try:
            self.candidate_search_edit.clear()
        finally:
            self.candidate_search_edit.blockSignals(previous_block_state)
        self.candidate_products_all = ()
        self.candidate_table.setRowCount(0)

    def _set_candidate_products(self, pairs: ProductOrder) -> None:
        self.candidate_products_all = tuple((str(code), str(name)) for code, name in pairs)
        self._refresh_candidate_table()

    def _refresh_candidate_table(self, *_args: object) -> None:
        keyword = self.candidate_search_edit.text().strip().casefold()
        if not keyword:
            self._set_table_pairs(self.candidate_table, self.candidate_products_all)
            return

        visible_pairs = tuple(
            (code, name)
            for code, name in self.candidate_products_all
            if keyword in code.casefold() or keyword in name.casefold()
        )
        self._set_table_pairs(self.candidate_table, visible_pairs)
```

- [ ] **Step 5: Wire candidate state clearing and successful worker updates**

In `_on_pipeline_changed()`, replace:

```python
self.candidate_table.setRowCount(0)
```

with:

```python
self._clear_candidate_products()
```

In `_on_worker_finished()`, replace the success branch line:

```python
self._set_table_pairs(self.candidate_table, result.candidate_products)
```

with:

```python
self._set_candidate_products(result.candidate_products)
```

In `_on_worker_finished()`, replace the failure branch line:

```python
self._set_table_pairs(self.candidate_table, ())
```

with:

```python
self._clear_candidate_products()
```

In `_on_worker_failed()`, replace:

```python
self._set_table_pairs(self.candidate_table, ())
```

with:

```python
self._clear_candidate_products()
```

In `_ignore_stale_worker_result()`, replace:

```python
self._set_table_pairs(self.candidate_table, ())
```

with:

```python
self._clear_candidate_products()
```

In `_invalidate_precheck()`, replace:

```python
self.candidate_table.setRowCount(0)
```

with:

```python
self._clear_candidate_products()
```

In `_clear_conditions()`, replace:

```python
self.candidate_table.setRowCount(0)
```

with:

```python
self._clear_candidate_products()
```

- [ ] **Step 6: Run GUI tests to verify they pass**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_gui_main_window.py::test_candidate_search_filters_by_product_code_contains tests/test_gui_main_window.py::test_candidate_search_filters_by_product_name_and_clear_restores_all tests/test_gui_main_window.py::test_add_selected_candidates_uses_current_filtered_candidate_rows tests/test_gui_main_window.py::test_candidate_search_state_clears_when_form_changes -q
```

Expected: PASS.

- [ ] **Step 7: Run full GUI main window tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_gui_main_window.py -q
```

Expected: PASS.

- [ ] **Step 8: Commit GUI search**

Run:

```bash
git add src/gui/main_window.py tests/test_gui_main_window.py
git commit -m "feat(gui): add candidate product search"
```

---

### Task 2: Work-Order Anomaly Detail Explanation

**Files:**
- Modify: `tests/test_pq_analysis_v3.py`
- Modify: `src/analytics/anomaly.py`

- [ ] **Step 1: Replace existing audit-field assertion tests**

In `tests/test_pq_analysis_v3.py`, update `test_build_report_artifacts_marks_unknown_doc_type_as_not_analyzable()`.

Replace:

```python
assert row['异常池样本数'] is None
assert row['异常池中心log值'] is None
assert row['异常池原始MAD'] is None
assert row['异常池有效MAD'] is None
assert row['相对中位偏离'] is None
```

with:

```python
assert '异常池样本数' not in anomaly_df.columns
assert '异常池中心log值' not in anomaly_df.columns
assert '异常池原始MAD' not in anomaly_df.columns
assert '异常池有效MAD' not in anomaly_df.columns
assert '相对中位偏离' not in anomaly_df.columns
assert row['异常明细解释'] == ''
```

In the same file, update `test_build_report_artifacts_uses_product_level_modified_zscore()`.

Replace:

```python
assert suspicious_row['异常池样本数'] == 3
assert suspicious_row['异常池中心log值'] == pytest.approx(math.log(11))
assert suspicious_row['异常池原始MAD'] == pytest.approx(math.log(11) - math.log(10))
assert suspicious_row['异常池有效MAD'] == pytest.approx(math.log(11) - math.log(10))
assert suspicious_row['相对中位偏离'] == pytest.approx((50 / 11) - 1)
```

Replace those five assertions with:

```python
assert '异常池样本数' not in anomaly_df.columns
assert '异常池中心log值' not in anomaly_df.columns
assert '异常池原始MAD' not in anomaly_df.columns
assert '异常池有效MAD' not in anomaly_df.columns
assert '相对中位偏离' not in anomaly_df.columns
assert '异常明细解释' in anomaly_df.columns

explanation = suspicious_row['异常明细解释']
assert explanation.startswith('总成本: 高度可疑')
assert '当前值=50.00' in explanation
assert f'当前log={math.log(50):.4f}' in explanation
assert '基准值=11.00' in explanation
assert f'基准log={math.log(11):.4f}' in explanation
assert f'log偏离={math.log(50) - math.log(11):.4f}' in explanation
assert f'相对偏离={(50 / 11) - 1:.2%}' in explanation
assert 'score=' in explanation
assert '有效工单数=3' in explanation
assert f'原始MAD={math.log(11) - math.log(10):.4f}' in explanation
assert f'有效MAD={math.log(11) - math.log(10):.4f}' in explanation
```

- [ ] **Step 2: Add a multi-anomaly ordering test**

Append this test near the modified audit explanation test in `tests/test_pq_analysis_v3.py`:

```python
def test_work_order_anomaly_detail_explanation_lists_multiple_flags_in_metric_order() -> None:
    df_detail = pd.DataFrame(
        [
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-001',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '汇报入库-普通生产',
                '成本项目名称': '直接材料',
                '本期完工金额': 70,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-001',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '汇报入库-普通生产',
                '成本项目名称': '直接人工',
                '本期完工金额': 30,
            },
            {
                '月份': '2025年02期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-002',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '汇报入库-普通生产',
                '成本项目名称': '直接材料',
                '本期完工金额': 77,
            },
            {
                '月份': '2025年02期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-002',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '汇报入库-普通生产',
                '成本项目名称': '直接人工',
                '本期完工金额': 33,
            },
            {
                '月份': '2025年03期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-003',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '汇报入库-普通生产',
                '成本项目名称': '直接材料',
                '本期完工金额': 700,
            },
            {
                '月份': '2025年03期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-003',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '汇报入库-普通生产',
                '成本项目名称': '直接人工',
                '本期完工金额': 300,
            },
        ]
    )
    df_qty = pd.DataFrame(
        [
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-001',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '汇报入库-普通生产',
                '本期完工数量': 10,
                '本期完工金额': 100,
            },
            {
                '月份': '2025年02期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-002',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '汇报入库-普通生产',
                '本期完工数量': 10,
                '本期完工金额': 110,
            },
            {
                '月份': '2025年03期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-003',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '汇报入库-普通生产',
                '本期完工数量': 10,
                '本期完工金额': 1000,
            },
        ]
    )

    artifacts = build_report_artifacts(df_detail, df_qty)
    anomaly_df = artifacts.work_order_sheet.data
    suspicious_row = anomaly_df.loc[anomaly_df['工单编号'] == 'WO-003'].iloc[0]
    explanation = suspicious_row['异常明细解释']

    total_index = explanation.index('总成本:')
    material_index = explanation.index('直接材料:')
    labor_index = explanation.index('直接人工:')

    assert total_index < material_index < labor_index
    assert explanation.count('有效工单数=3') >= 3
```

- [ ] **Step 3: Run anomaly tests to verify they fail**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_pq_analysis_v3.py::test_build_report_artifacts_marks_unknown_doc_type_as_not_analyzable tests/test_pq_analysis_v3.py::test_build_report_artifacts_uses_product_level_modified_zscore tests/test_pq_analysis_v3.py::test_work_order_anomaly_detail_explanation_lists_multiple_flags_in_metric_order -q
```

Expected: FAIL because `异常明细解释` does not exist and old audit columns still exist.

- [ ] **Step 4: Add anomaly explanation formatting helpers**

In `src/analytics/anomaly.py`, add these helpers after `build_work_order_conditional_formats()`:

```python
def _is_present_number(value: object) -> bool:
    if value is None:
        return False
    try:
        return not bool(pd.isna(value))
    except (TypeError, ValueError):
        return True


def _format_fixed(value: object, digits: int) -> str:
    if not _is_present_number(value):
        return ''
    return f'{float(value):.{digits}f}'


def _format_percent(value: object) -> str:
    if not _is_present_number(value):
        return ''
    return f'{float(value):.2%}'


def _format_int(value: object) -> str:
    if not _is_present_number(value):
        return ''
    return str(int(value))


def _build_metric_anomaly_explanation(
    *,
    label: str,
    level: object,
    current_value: object,
    current_log: object,
    center_log: object,
    score: object,
    effective_count: object,
    raw_mad: object,
    effective_mad: object,
) -> str:
    required_values = (level, current_value, current_log, center_log, score)
    if any(not _is_present_number(value) and value != '关注' and value != '高度可疑' for value in required_values):
        return ''

    current_log_float = float(current_log)
    center_log_float = float(center_log)
    log_delta = current_log_float - center_log_float
    baseline_value = math.exp(center_log_float)
    relative_delta = math.expm1(log_delta)

    return (
        f'{label}: {level}, '
        f'当前值={_format_fixed(current_value, 2)}, '
        f'当前log={_format_fixed(current_log_float, 4)}, '
        f'基准值={_format_fixed(baseline_value, 2)}, '
        f'基准log={_format_fixed(center_log_float, 4)}, '
        f'log偏离={_format_fixed(log_delta, 4)}, '
        f'相对偏离={_format_percent(relative_delta)}, '
        f'score={_format_fixed(score, 2)}, '
        f'有效工单数={_format_int(effective_count)}, '
        f'原始MAD={_format_fixed(raw_mad, 4)}, '
        f'有效MAD={_format_fixed(effective_mad, 4)}'
    )
```

- [ ] **Step 5: Remove old output columns and add `异常明细解释`**

In `WORK_ORDER_OUTPUT_COLUMNS`, delete:

```python
    '异常池样本数',
    '异常池中心log值',
    '异常池原始MAD',
    '异常池有效MAD',
    '相对中位偏离',
```

Add after `'异常主要来源',`:

```python
    '异常明细解释',
```

In `WORK_ORDER_COLUMN_TYPES`, delete:

```python
    '异常池样本数': 'qty',
    '异常池中心log值': 'score',
    '异常池原始MAD': 'score',
    '异常池有效MAD': 'score',
    '相对中位偏离': 'pct',
```

Add:

```python
    '异常明细解释': 'text',
```

- [ ] **Step 6: Build explanation text from existing metric audit columns**

In `build_anomaly_sheet()`, delete the block that initializes and populates:

```python
anomaly_df['异常池样本数'] = None
anomaly_df['异常池中心log值'] = None
anomaly_df['异常池原始MAD'] = None
anomaly_df['异常池有效MAD'] = None
anomaly_df['相对中位偏离'] = None

for metric_key, _display_name, _flag_column, source_label in ANOMALY_METRICS:
    source_mask = highest_source == source_label
    if not source_mask.any():
        continue
    anomaly_df.loc[source_mask, '异常池样本数'] = anomaly_df.loc[
        source_mask, f'audit_pool_sample_size_{metric_key}'
    ]
    anomaly_df.loc[source_mask, '异常池中心log值'] = anomaly_df.loc[
        source_mask, f'audit_pool_center_log_{metric_key}'
    ]
    anomaly_df.loc[source_mask, '异常池原始MAD'] = anomaly_df.loc[source_mask, f'audit_pool_raw_mad_{metric_key}']
    anomaly_df.loc[source_mask, '异常池有效MAD'] = anomaly_df.loc[
        source_mask, f'audit_pool_effective_mad_{metric_key}'
    ]
    anomaly_df.loc[source_mask, '相对中位偏离'] = anomaly_df.loc[
        source_mask, f'audit_relative_deviation_{metric_key}'
    ]
```

Replace it with:

```python
    detail_explanations: list[str] = []
    for _, row in anomaly_df.iterrows():
        parts: list[str] = []
        for metric_key, display_name, flag_column, _source_label in ANOMALY_METRICS:
            level = row[flag_column]
            if level not in {'关注', '高度可疑'}:
                continue

            explanation = _build_metric_anomaly_explanation(
                label=display_name.replace('单位完工成本', '').replace('总单位完工成本', '总成本'),
                level=level,
                current_value=row[metric_key],
                current_log=row[f'log_{metric_key}'],
                center_log=row[f'audit_pool_center_log_{metric_key}'],
                score=row[f'modified_z_{metric_key}'],
                effective_count=row[f'audit_pool_sample_size_{metric_key}'],
                raw_mad=row[f'audit_pool_raw_mad_{metric_key}'],
                effective_mad=row[f'audit_pool_effective_mad_{metric_key}'],
            )
            if explanation:
                parts.append(explanation)
        detail_explanations.append('; '.join(parts))
    anomaly_df['异常明细解释'] = detail_explanations
```

Then replace the label expression with a stable label map by adding this constant near `ANOMALY_METRICS`:

```python
ANOMALY_EXPLANATION_LABELS: dict[str, str] = {
    'total_unit_cost': '总成本',
    'dm_unit_cost': '直接材料',
    'dl_unit_cost': '直接人工',
    'moh_unit_cost': '制造费用',
    'moh_other_unit_cost': '制造费用_其他',
    'moh_labor_unit_cost': '制造费用_人工',
    'moh_consumables_unit_cost': '制造费用_机物料及低耗',
    'moh_depreciation_unit_cost': '制造费用_折旧',
    'moh_utilities_unit_cost': '制造费用_水电费',
}
```

Use it in the helper call:

```python
label=ANOMALY_EXPLANATION_LABELS[metric_key],
```

- [ ] **Step 7: Run anomaly tests to verify they pass**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_pq_analysis_v3.py::test_build_report_artifacts_marks_unknown_doc_type_as_not_analyzable tests/test_pq_analysis_v3.py::test_build_report_artifacts_uses_product_level_modified_zscore tests/test_pq_analysis_v3.py::test_work_order_anomaly_detail_explanation_lists_multiple_flags_in_metric_order -q
```

Expected: PASS.

- [ ] **Step 8: Run broader anomaly test file**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_pq_analysis_v3.py -q
```

Expected: PASS.

- [ ] **Step 9: Commit anomaly explanation**

Run:

```bash
git add src/analytics/anomaly.py tests/test_pq_analysis_v3.py
git commit -m "feat(analytics): consolidate anomaly audit explanation"
```

---

### Task 3: Compact Product Dimension Workbook Layout

**Files:**
- Modify: `tests/test_costing_etl.py`
- Modify: `src/excel/product_anomaly_writer.py`
- Modify: `src/analytics/presentation_builder.py`

- [ ] **Step 1: Update product dimension writer assertions**

In `tests/test_costing_etl.py`, update `test_workbook_writer_sheet_model_preserves_product_anomaly_legacy_layout` assertions to:

```python
assert worksheet['A1'].value == '产品编码'
assert worksheet['A2'].value == 'P001'
assert worksheet['B1'].value == '产品名称'
assert worksheet['B2'].value == '产品A'
assert worksheet['A3'].value == '月份'
assert worksheet['A4'].value == '2025年01期'
assert worksheet.freeze_panes == 'A4'
```

Update `test_workbook_writer_sheet_model_renders_product_anomaly_scope_split_layout_for_gb` assertions to:

```python
assert worksheet['A1'].value == '产品编码'
assert worksheet['A2'].value == 'P001'
assert worksheet['B1'].value == '产品名称'
assert worksheet['B2'].value == '产品A'
assert worksheet['A3'].value == '分析口径'
assert worksheet['B3'].value == '全部'
assert worksheet['A4'].value == '月份'
assert worksheet['A5'].value == '2025年01期'
assert any(
    worksheet.cell(row=row_idx, column=2).value == '正常生产'
    for row_idx in range(1, worksheet.max_row + 1)
)
assert worksheet.freeze_panes == 'A5'
```

Update other direct assertions in `tests/test_costing_etl.py` that check:

```python
assert worksheet['A1'].value == '四、按单个产品异常值分析'
assert worksheet['A3'].value == '产品编码'
assert ws_product.freeze_panes == 'A6'
```

to the compact layout values:

```python
assert worksheet['A1'].value == '产品编码'
assert worksheet['A3'].value == '月份'
assert ws_product.freeze_panes == 'A4'
```

Use the scoped `A5` expectation only for tests whose product anomaly sections include `section_label`.

- [ ] **Step 2: Run product dimension writer tests to verify they fail**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_costing_etl.py::test_workbook_writer_sheet_model_preserves_product_anomaly_legacy_layout tests/test_costing_etl.py::test_workbook_writer_sheet_model_renders_product_anomaly_scope_split_layout_for_gb -q
```

Expected: FAIL because writer still writes the old title and old row offsets.

- [ ] **Step 3: Update product anomaly writer row offsets**

In `src/excel/product_anomaly_writer.py`, change `_write_legacy_product_anomaly_sheet()` from:

```python
        freeze_panes='A6',
```

to:

```python
        freeze_panes='A4',
```

Change `_write_scoped_product_anomaly_sheet()` from:

```python
        freeze_panes='A7',
        scope_label_row_offset=2,
```

to:

```python
        freeze_panes='A5',
        scope_label_row_offset=2,
```

In `_write_product_anomaly_sections()`, delete:

```python
worksheet.write(0, 0, '四、按单个产品异常值分析', section_title_format)

current_row = 2
```

Replace it with:

```python
current_row = 0
```

Delete the unused `section_title_format` assignment block from `_write_product_anomaly_sections()`.

The existing row calculations then become compact:

```python
meta_header_row = current_row
meta_value_row = current_row + 1
table_header_row = current_row + (3 if scoped else 2)
data_start_row = table_header_row + 1
```

- [ ] **Step 4: Update product anomaly model freeze panes**

In `src/analytics/presentation_builder.py`, update product anomaly model freeze panes:

```python
freeze_panes='A5' if has_scoped_product_anomaly_section else 'A4',
```

- [ ] **Step 5: Run product dimension writer tests to verify they pass**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_costing_etl.py::test_workbook_writer_sheet_model_preserves_product_anomaly_legacy_layout tests/test_costing_etl.py::test_workbook_writer_sheet_model_renders_product_anomaly_scope_split_layout_for_gb -q
```

Expected: PASS.

- [ ] **Step 6: Run all costing ETL writer tests**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_costing_etl.py -q
```

Expected: PASS after all direct product dimension layout assertions are updated.

- [ ] **Step 7: Commit product dimension layout**

Run:

```bash
git add src/excel/product_anomaly_writer.py src/analytics/presentation_builder.py tests/test_costing_etl.py
git commit -m "feat(excel): compact product dimension layout"
```

---

### Task 4: Workbook Contract And Documentation

**Files:**
- Modify: `tests/contracts/_workbook_contract_helper.py`
- Modify: `tests/contracts/baselines/workbook_semantics.json`
- Modify: `README.md`
- Modify: `AGENTS.md`

- [ ] **Step 1: Update contract helper for compact product dimension rows**

In `tests/contracts/_workbook_contract_helper.py`, replace `_extract_product_anomaly_sheet()` with:

```python
def _extract_product_anomaly_sheet(worksheet) -> dict[str, object]:
    if worksheet['A3'].value == '分析口径':
        return _extract_scoped_product_anomaly_sheet(worksheet)
    return _extract_legacy_product_anomaly_sheet(worksheet)
```

Replace `_extract_legacy_product_anomaly_sheet()` with:

```python
def _extract_legacy_product_anomaly_sheet(worksheet) -> dict[str, object]:
    headers = [worksheet.cell(3, col_idx).value for col_idx in range(1, worksheet.max_column + 1)]
    while headers and headers[-1] is None:
        headers.pop()

    first_data_formats = {
        header: worksheet.cell(4, col_idx).number_format
        for col_idx, header in enumerate(headers, start=1)
        if worksheet.max_row >= 4 and worksheet.cell(4, col_idx).number_format != 'General'
    }
    return {
        'kind': 'product_anomaly',
        'layout': 'legacy',
        'freeze_panes': worksheet.freeze_panes,
        'auto_filter': worksheet.auto_filter.ref,
        'title': None,
        'meta_labels': [worksheet['A1'].value, worksheet['B1'].value],
        'meta_values': [worksheet['A2'].value, worksheet['B2'].value],
        'columns': headers,
        'number_formats': first_data_formats,
        'column_widths': _extract_column_widths(worksheet),
    }
```

Replace `_extract_scoped_product_anomaly_sheet()` with this row-1 based version:

```python
def _extract_scoped_product_anomaly_sheet(worksheet) -> dict[str, object]:
    sections: list[dict[str, object]] = []
    row_idx = 1
    while row_idx <= worksheet.max_row:
        meta_label_code = worksheet.cell(row_idx, 1).value
        meta_label_name = worksheet.cell(row_idx, 2).value
        if meta_label_code != '产品编码' or meta_label_name != '产品名称':
            row_idx += 1
            continue

        if worksheet.cell(row_idx + 2, 1).value != '分析口径':
            row_idx += 1
            continue

        header_row = row_idx + 3
        first_data_row = header_row + 1
        headers: list[object] = []
        col_idx = 1
        while col_idx <= worksheet.max_column and worksheet.cell(header_row, col_idx).value is not None:
            headers.append(worksheet.cell(header_row, col_idx).value)
            col_idx += 1

        number_formats = {
            header: worksheet.cell(first_data_row, header_col).number_format
            for header_col, header in enumerate(headers, start=1)
            if first_data_row <= worksheet.max_row
            and worksheet.cell(first_data_row, header_col).number_format != 'General'
        }
        sections.append(
            {
                'product_code': worksheet.cell(row_idx + 1, 1).value,
                'product_name': worksheet.cell(row_idx + 1, 2).value,
                'scope_label': worksheet.cell(row_idx + 2, 2).value,
                'columns': headers,
                'number_formats': number_formats,
            }
        )
        row_idx = first_data_row + 1

    first_section = sections[0] if sections else {'columns': [], 'number_formats': {}}
    return {
        'kind': 'product_anomaly',
        'layout': 'scoped',
        'freeze_panes': worksheet.freeze_panes,
        'auto_filter': worksheet.auto_filter.ref,
        'title': None,
        'scope_labels': [section['scope_label'] for section in sections],
        'columns': first_section['columns'],
        'number_formats': first_section['number_formats'],
        'sections': sections,
        'column_widths': _extract_column_widths(worksheet),
    }
```

- [ ] **Step 2: Run workbook contract test to verify baseline mismatch**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/contracts/test_workbook_contract.py -q
```

Expected: FAIL because baseline still has old work-order audit columns, old product dimension title, and old freeze panes.

- [ ] **Step 3: Regenerate workbook contract baseline**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python tests/contracts/generate_baselines.py
```

Expected: updates `tests/contracts/baselines/workbook_semantics.json`.

- [ ] **Step 4: Inspect baseline diff for intended changes only**

Run:

```bash
git diff -- tests/contracts/baselines/workbook_semantics.json
```

Expected diff characteristics:

- `成本分析工单维度` columns remove:

```text
异常池样本数
异常池中心log值
异常池原始MAD
异常池有效MAD
相对中位偏离
```

- `成本分析工单维度` columns add:

```text
异常明细解释
```

- Product dimension `title` changes from old title to `null`.
- Product dimension freeze panes change from `A6/A7` to `A4/A5`.
- Product dimension meta/header row semantics shift upward.

- [ ] **Step 5: Update README business rules**

In `README.md`, update the GUI description paragraph so it includes:

```markdown
GUI 支持选择 GB/SK 管线、选择输入文件、自动查找、配置月份范围、维护产品白名单池、按产品编码或产品名称包含搜索候选产品、预检和后台处理。候选产品搜索只影响 GUI 显示；产品白名单池按 `产品编码 + 产品名称` 精确匹配，只影响分析维度 Sheet，不过滤总表和数量聚合维度。
```

Update the `成本分析工单维度` bullet that mentions explanation fields so it states:

```markdown
  - 解释字段：`异常明细解释`，仅列出达到 `关注` 或 `高度可疑` 的成本项；每项包含当前值、当前log、基准值、基准log、log偏离、相对偏离、score、有效工单数、原始MAD、有效MAD。`有效工单数` 是同一产品、同一生产类型异常池、同一成本指标下实际参与该项评分的有效工单行数，不是完工数量合计。
```

Update the product dimension bullet so it states:

```markdown
- 产品维度摘要页：`成本分析产品维度`，保留按产品分块的紧凑布局，不再输出 `四、按单个产品异常值分析` 标题。
```

- [ ] **Step 6: Update AGENTS current business rules**

In `AGENTS.md`, update the GUI/current business rules section with these statements:

```markdown
- GUI 候选产品搜索按产品编码或产品名称包含匹配，只影响候选产品表显示；实际白名单过滤仍按 `产品编码 + 产品名称` 双字段精确匹配。
- `成本分析工单维度`sheet 保留 `异常等级`、`异常主要来源`、`复核原因`，并使用单列 `异常明细解释` 展示所有达到 `关注` 或 `高度可疑` 的异常项；不再输出 `异常池样本数`、`异常池中心log值`、`异常池原始MAD`、`异常池有效MAD`、`相对中位偏离` 五个旧解释列。
- `异常明细解释` 中的 `有效工单数` 是同一产品、同一生产类型异常池、同一成本指标下实际参与该项 Modified Z-score 计算的有效工单行数，不是完工数量合计。
- `成本分析产品维度`sheet 保留按产品分块的紧凑布局，不再输出 `四、按单个产品异常值分析` 标题。
```

Before editing `AGENTS.md`, run:

```bash
rg -n "候选产品|异常明细解释|异常池样本数|成本分析产品维度|按单个产品异常值分析" AGENTS.md
```

Replace matching old bullets in place and keep only one current rule for each topic.

- [ ] **Step 7: Run contract and docs-adjacent checks**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/contracts/test_workbook_contract.py tests/test_costing_etl.py tests/test_pq_analysis_v3.py tests/test_gui_main_window.py -q
```

Expected: PASS.

- [ ] **Step 8: Commit contract and docs**

Run:

```bash
git add tests/contracts/_workbook_contract_helper.py tests/contracts/baselines/workbook_semantics.json README.md AGENTS.md
git commit -m "docs: update workbook anomaly and whitelist contracts"
```

---

### Task 5: Final Verification

**Files:**
- No source edits expected.

- [ ] **Step 1: Run Ruff**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m ruff check src tests
```

Expected: PASS.

- [ ] **Step 2: Run full pytest suite**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests -q
```

Expected: PASS.

- [ ] **Step 3: Run real GB check-only benchmark**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python main.py gb --check-only --benchmark
```

Expected:

- exits `0`
- prints `mode=check-only`
- prints `pipeline=gb`
- prints `[benchmark]`
- does not write workbook

- [ ] **Step 4: Run real SK check-only benchmark**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python main.py sk --check-only --benchmark
```

Expected:

- exits `0`
- prints `mode=check-only`
- prints `pipeline=sk`
- prints `[benchmark]`
- does not write workbook

- [ ] **Step 5: Run temporary full workbook export smoke test**

Run:

```bash
/home/george/miniconda3/bin/conda run -n test python -c $'from pathlib import Path\nfrom src.config.product_whitelist_store import load_product_order_for_pipeline\nfrom src.services.costing_service import CostingRunRequest, run_costing_request\njobs = [(\"gb\", Path(\"data/raw/gb/GB-成本计算单_2026051215423292_100160.xlsx\")), (\"sk\", Path(\"data/raw/sk/sk-成本计算单_2026041311461807_3592191.xlsx\"))]\nout_dir = Path(\"/tmp/costing-anomaly-output-cleanup-smoke\")\nfor pipeline, input_path in jobs:\n    result = run_costing_request(CostingRunRequest(pipeline=pipeline, input_path=input_path, output_dir=out_dir / pipeline, product_order=load_product_order_for_pipeline(pipeline), overwrite_confirmed=True, benchmark=True))\n    print(f\"pipeline={pipeline}\")\n    print(f\"status={result.status}\")\n    print(f\"workbook_path={result.workbook_path}\")\n    print(f\"error_log_count={result.error_log_count}\")\n    print(f\"stage_timings={result.stage_timings}\")\n    print(\"---\")'
```

Expected:

- GB status is `succeeded`.
- SK status is `succeeded`.
- Workbooks are written under `/tmp/costing-anomaly-output-cleanup-smoke`.
- No files under `data/processed` are modified by this smoke test.

- [ ] **Step 6: Check whitespace**

Run:

```bash
git diff --check
```

Expected: no output.

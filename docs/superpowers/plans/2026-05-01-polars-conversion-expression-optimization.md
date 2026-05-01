# Polars Conversion And Expression Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove low-risk pandas conversions and Python UDF hotspots while preserving workbook/error_log semantics.

**Architecture:** Keep public output contracts unchanged. Add focused Polars helpers for count and whitelist operations, then replace stable cost-item mappings with native Polars expressions. Do not rewrite `build_anomaly_sheet()` or change Decimal money semantics in this phase.

**Tech Stack:** Python 3.11, Polars, pandas compatibility edges, pytest, existing contract tests.

---

## File Map

- Modify: `src/analytics/qty_enricher.py`
  - Replace `_count_filtered_qty_rows()` pandas conversion with Polars expressions.
- Modify: `src/etl/costing_etl.py`
  - Replace `product_summary_fact` whitelist filtering via pandas round-trip with a Polars join helper.
- Modify: `src/analytics/fact_builder.py`
  - Replace stable cost-item bucket `map_elements()` calls with native Polars expressions.
- Modify: `tests/test_pq_analysis_v3.py`
  - Add count helper parity tests.
- Modify: `tests/test_costing_etl.py`
  - Add product summary whitelist Polars helper test.
- Modify: `tests/test_etl_pipeline.py` or `tests/test_pq_analysis_v3.py`
  - Add cost bucket expression mapping test.

## Task 1: Polars Filtered Quantity Row Counts

**Files:**
- Modify: `src/analytics/qty_enricher.py`
- Test: `tests/test_pq_analysis_v3.py`

- [ ] **Step 1: Write failing direct helper test**

Append this test to `tests/test_pq_analysis_v3.py`:

```python
def test_count_filtered_qty_rows_uses_polars_without_to_dicts(monkeypatch) -> None:
    from src.analytics import qty_enricher

    qty_df = pl.DataFrame(
        {
            '本期完工数量': ['10', '0', None, '5'],
            '本期完工金额': ['100', '0', '50', None],
        }
    )

    monkeypatch.setattr(
        pl.DataFrame,
        'to_dicts',
        lambda self: (_ for _ in ()).throw(AssertionError('must stay in Polars')),
    )

    assert qty_enricher._count_filtered_qty_rows(qty_df) == (2, 1)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
conda run -n test python -m pytest tests/test_pq_analysis_v3.py::test_count_filtered_qty_rows_uses_polars_without_to_dicts -q
```

Expected: FAIL with `AssertionError: must stay in Polars`.

- [ ] **Step 3: Implement Polars count helper**

Replace `_count_filtered_qty_rows()` in `src/analytics/qty_enricher.py`:

```python
def _count_filtered_qty_rows(qty_df: pl.DataFrame) -> tuple[int, int]:
    if qty_df.is_empty():
        return 0, 0

    normalized = qty_df.with_columns(
        [
            pl.col('本期完工数量')
            .cast(pl.String, strict=False)
            .str.strip_chars()
            .cast(pl.Decimal(38, 28), strict=False)
            .alias('_completed_qty_for_count'),
            pl.col('本期完工金额')
            .cast(pl.String, strict=False)
            .str.strip_chars()
            .cast(pl.Decimal(38, 28), strict=False)
            .alias('_completed_amount_for_count'),
        ]
    )
    valid_qty_expr = pl.col('_completed_qty_for_count').is_not_null() & (pl.col('_completed_qty_for_count') > ZERO)
    result = normalized.select(
        [
            (~valid_qty_expr).sum().alias('filtered_invalid_qty_count'),
            (valid_qty_expr & pl.col('_completed_amount_for_count').is_null())
            .sum()
            .alias('filtered_missing_total_amount_count'),
        ]
    ).row(0)
    return int(result[0]), int(result[1])
```

- [ ] **Step 4: Run helper and report tests**

Run:

```powershell
conda run -n test python -m pytest tests/test_pq_analysis_v3.py::test_count_filtered_qty_rows_uses_polars_without_to_dicts tests/test_pq_analysis_v3.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```powershell
git add src/analytics/qty_enricher.py tests/test_pq_analysis_v3.py
git commit -m "perf(analytics): count filtered qty rows in polars"
```

## Task 2: Product Summary Whitelist Without Pandas Round Trip

**Files:**
- Modify: `src/etl/costing_etl.py`
- Test: `tests/test_costing_etl.py`

- [ ] **Step 1: Write failing helper test**

Append this test to `tests/test_costing_etl.py`:

```python
def test_filter_product_summary_frame_by_whitelist_stays_in_polars(monkeypatch) -> None:
    etl = CostingWorkbookETL(
        skip_rows=2,
        product_order=(('P002', '产品B'), ('P001', '产品A')),
        ensure_output_directories=False,
    )
    summary_frame = pl.DataFrame(
        [
            {'product_code': 'P001', 'product_name': '产品A', 'period': '2025-02', 'value': 1},
            {'product_code': 'P003', 'product_name': '产品C', 'period': '2025-01', 'value': 3},
            {'product_code': 'P002', 'product_name': '产品B', 'period': '2025-01', 'value': 2},
        ]
    )

    monkeypatch.setattr(
        pl.DataFrame,
        'to_dicts',
        lambda self: (_ for _ in ()).throw(AssertionError('must stay in Polars')),
    )

    result = etl._filter_product_summary_frame_by_whitelist(summary_frame)

    assert result.select(['product_code', 'product_name', 'period']).to_dict(as_series=False) == {
        'product_code': ['P002', 'P001'],
        'product_name': ['产品B', '产品A'],
        'period': ['2025-01', '2025-02'],
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
conda run -n test python -m pytest tests/test_costing_etl.py::test_filter_product_summary_frame_by_whitelist_stays_in_polars -q
```

Expected: FAIL because `_filter_product_summary_frame_by_whitelist` does not exist.

- [ ] **Step 3: Implement helper and use it**

In `src/etl/costing_etl.py`, add:

```python
def _filter_product_summary_frame_by_whitelist(self, summary_frame: pl.DataFrame) -> pl.DataFrame:
    """用 Polars join 过滤产品摘要事实，避免 pandas round-trip。"""
    if summary_frame.is_empty() or not self.product_order:
        return summary_frame

    required_columns = {'product_code', 'product_name'}
    if not required_columns.issubset(summary_frame.columns):
        return summary_frame

    whitelist = pl.DataFrame(
        {
            'product_code': [code for code, _name in self.product_order],
            'product_name': [name for _code, name in self.product_order],
            '_order_idx': list(range(len(self.product_order))),
        }
    )
    sort_columns = ['_order_idx']
    if 'period' in summary_frame.columns:
        sort_columns.append('period')
    return (
        whitelist.join(summary_frame, on=['product_code', 'product_name'], how='inner')
        .sort(sort_columns)
        .drop('_order_idx')
    )
```

Replace the pandas conversion in `_filter_fact_bundle_for_whitelist()`:

```python
filtered_summary_frame = self._filter_product_summary_frame_by_whitelist(summary_frame)
```

- [ ] **Step 4: Run helper and related tests**

Run:

```powershell
conda run -n test python -m pytest tests/test_costing_etl.py::test_filter_product_summary_frame_by_whitelist_stays_in_polars tests/test_costing_etl.py::test_process_file_filters_whitelist_before_presentation_and_preserves_numeric_order_line_sort -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```powershell
git add src/etl/costing_etl.py tests/test_costing_etl.py
git commit -m "perf(etl): filter product summaries in polars"
```

## Task 3: Cost Bucket Mapping Expressions

**Files:**
- Modify: `src/analytics/fact_builder.py`
- Test: `tests/test_pq_analysis_v3.py`

- [ ] **Step 1: Write focused mapping test**

Append this test to `tests/test_pq_analysis_v3.py`:

```python
def test_build_fact_bundle_cost_bucket_mapping_without_python_udf(monkeypatch) -> None:
    detail_df = pl.DataFrame(
        {
            '月份': ['2025年01期', '2025年01期', '2025年01期'],
            '产品编码': ['P001', 'P001', 'P001'],
            '产品名称': ['产品A', '产品A', '产品A'],
            '工单编号': ['WO-001', 'WO-001', 'WO-001'],
            '工单行号': ['1', '1', '1'],
            '成本项目名称': ['直接材料', '制造费用-人工', '未知项目'],
            '本期完工金额': ['100', '20', '5'],
        }
    )
    qty_df = pl.DataFrame(
        {
            '月份': ['2025年01期'],
            '产品编码': ['P001'],
            '产品名称': ['产品A'],
            '工单编号': ['WO-001'],
            '工单行号': ['1'],
            '本期完工数量': ['10'],
            '本期完工金额': ['120'],
        }
    )

    from src.analytics import fact_builder

    original_map_broad = fact_builder.map_broad_cost_bucket
    original_map_component = fact_builder.map_component_bucket

    def _blocked_broad(value):
        raise AssertionError('broad mapping should use Polars expressions')

    def _blocked_component(value):
        raise AssertionError('component mapping should use Polars expressions')

    monkeypatch.setattr(fact_builder, 'map_broad_cost_bucket', _blocked_broad)
    monkeypatch.setattr(fact_builder, 'map_component_bucket', _blocked_component)

    bundle = fact_builder.build_fact_bundle(detail_df, qty_df, standalone_cost_items=('委外加工费',))

    monkeypatch.setattr(fact_builder, 'map_broad_cost_bucket', original_map_broad)
    monkeypatch.setattr(fact_builder, 'map_component_bucket', original_map_component)

    assert bundle.work_order_fact.select(['dm_amount', 'moh_amount', 'moh_labor_amount']).to_dict(as_series=False) == {
        'dm_amount': [Decimal('100.0000000000000000000000000000')],
        'moh_amount': [Decimal('20.0000000000000000000000000000')],
        'moh_labor_amount': [Decimal('20.0000000000000000000000000000')],
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
conda run -n test python -m pytest tests/test_pq_analysis_v3.py::test_build_fact_bundle_cost_bucket_mapping_without_python_udf -q
```

Expected: FAIL with `AssertionError` from blocked mapping functions.

- [ ] **Step 3: Add expression helpers**

In `src/analytics/fact_builder.py`, add:

```python
def map_broad_cost_bucket_expr(column_name: str) -> pl.Expr:
    normalized = pl.col(column_name).cast(pl.String, strict=False).str.strip_chars()
    return (
        pl.when(normalized == '直接材料')
        .then(pl.lit('direct_material'))
        .when(normalized == '直接人工')
        .then(pl.lit('direct_labor'))
        .when(normalized.str.starts_with('制造费用'))
        .then(pl.lit('moh'))
        .otherwise(None)
    )


def map_component_bucket_expr(column_name: str) -> pl.Expr:
    normalized = pl.col(column_name).cast(pl.String, strict=False).str.strip_chars()
    return (
        pl.when(normalized == '制造费用_其他')
        .then(pl.lit('moh_other_amount'))
        .when(normalized == '制造费用-人工')
        .then(pl.lit('moh_labor_amount'))
        .when(normalized == '制造费用_机物料及低耗')
        .then(pl.lit('moh_consumables_amount'))
        .when(normalized == '制造费用_折旧')
        .then(pl.lit('moh_depreciation_amount'))
        .when(normalized == '制造费用_水电费')
        .then(pl.lit('moh_utilities_amount'))
        .otherwise(None)
    )
```

Replace in `build_fact_bundle()`:

```python
map_broad_cost_bucket_expr('cost_item').alias('cost_bucket'),
map_component_bucket_expr('cost_item').alias('component_bucket'),
```

- [ ] **Step 4: Run focused and fact tests**

Run:

```powershell
conda run -n test python -m pytest tests/test_pq_analysis_v3.py::test_build_fact_bundle_cost_bucket_mapping_without_python_udf tests/test_pq_analysis_v3.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```powershell
git add src/analytics/fact_builder.py tests/test_pq_analysis_v3.py
git commit -m "perf(analytics): map cost buckets with polars expressions"
```

## Task 4: Verification

**Files:**
- No code changes unless verification exposes a defect.

- [ ] **Step 1: Run focused tests**

Run:

```powershell
conda run -n test python -m pytest tests/test_pq_analysis_v3.py tests/test_costing_etl.py -q
```

Expected: PASS.

- [ ] **Step 2: Run contract tests**

Run:

```powershell
conda run -n test python -m pytest tests/contracts -q
```

Expected: PASS and no baseline file changes.

- [ ] **Step 3: Run full suite**

Run:

```powershell
conda run -n test python -m pytest tests -q
```

Expected: PASS.

- [ ] **Step 4: Run real check-only benchmark if raw data is available**

Run:

```powershell
conda run -n test python main.py gb --check-only --benchmark
conda run -n test python main.py sk --check-only --benchmark
```

Expected: both exit `0`; stage timings are available. Compare to first-priority baseline, but do not require a fixed speedup.


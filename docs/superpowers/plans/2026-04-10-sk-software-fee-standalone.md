# SK Software Fee Standalone Item Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `软件费用` a standalone cost item for the `sk` pipeline only, with the same independent display and reconciliation behavior as `委外加工费`, while keeping `gb` unchanged.

**Architecture:** Keep `gb/sk` differences in `PipelineConfig` by adding a `standalone_cost_items` field, then thread that configuration through `runner -> CostingWorkbookETL -> build_report_artifacts`. Convert the current hard-coded `委外加工费` logic in analytics into a configuration-driven standalone-cost mechanism, and make anomaly/workbook output columns expand dynamically only when the configured standalone item exists.

**Tech Stack:** Python 3.11, pandas, decimal.Decimal, openpyxl, pytest, ruff, conda `test` environment

---

## File Map

- Modify: `src/config/pipelines.py`
  Add `standalone_cost_items` to `PipelineConfig`, and define `gb/sk` defaults.
- Modify: `src/etl/runner.py`
  Pass `standalone_cost_items` from pipeline config into `CostingWorkbookETL`.
- Modify: `src/etl/costing_etl.py`
  Accept and store `standalone_cost_items`, then forward them into `build_report_artifacts`.
- Modify: `src/analytics/fact_builder.py`
  Add shared standalone cost metadata helpers so `qty_enricher.py` and `anomaly.py` use one source of truth.
- Modify: `src/analytics/qty_enricher.py`
  Generalize hard-coded outsource logic into config-driven standalone item handling; update totals, error logging, and quantity-sheet columns.
- Modify: `src/analytics/anomaly.py`
  Build work-order output columns and column types dynamically so standalone items get amount/unit-cost columns but no anomaly score columns.
- Modify: `src/excel/workbook_writer.py`
  Detect dynamic standalone numeric columns so `软件费用` columns get `#,##0.00` formatting without polluting `gb`.
- Modify: `tests/test_pipeline_config.py`
  Lock down standalone-cost config per pipeline.
- Modify: `tests/test_runner.py`
  Verify runner injects standalone-cost config into ETL.
- Modify: `tests/test_pq_analysis_v3.py`
  Verify `sk` software fee behavior and `gb` fallback to `UNMAPPED_COST_ITEM`.
- Modify: `tests/test_costing_etl.py`
  Verify workbook output schema for `sk` gains software-fee columns while `gb` does not.
- Modify: `README.md`
  Document the `sk`-only software-fee rule.
- Modify: `AGENTS.md`
  Update repository business rules so the agent instructions match implementation.

### Task 1: Add Standalone Cost Item Pipeline Configuration

**Files:**
- Modify: `tests/test_pipeline_config.py`
- Modify: `tests/test_runner.py`
- Modify: `src/config/pipelines.py`
- Modify: `src/etl/runner.py`
- Modify: `src/etl/costing_etl.py`

- [ ] **Step 1: Write the failing config and runner tests**

Add a config contract test to `tests/test_pipeline_config.py`:

```python
def test_pipeline_standalone_cost_items_are_defined_per_target() -> None:
    assert GB_PIPELINE.standalone_cost_items == ('委外加工费',)
    assert SK_PIPELINE.standalone_cost_items == ('委外加工费', '软件费用')
```

Update `tests/test_runner.py` so `PipelineConfig` and `_DummyETL` both cover the new constructor/data path:

```python
def test_run_pipeline_passes_standalone_cost_items_to_etl(monkeypatch, tmp_path) -> None:
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
        standalone_cost_items=('委外加工费', '软件费用'),
    )

    captured: dict[str, object] = {}

    class _DummyETL:
        def __init__(self, skip_rows: int, product_order, standalone_cost_items) -> None:
            captured['skip_rows'] = skip_rows
            captured['product_order'] = product_order
            captured['standalone_cost_items'] = standalone_cost_items
            self.last_quality_metrics = ()
            self.last_error_log_count = 0

        def process_file(self, input_path: Path, output_path: Path) -> bool:
            output_path.write_text('ok', encoding='utf-8')
            return True

    monkeypatch.setattr('src.etl.runner.CostingWorkbookETL', _DummyETL)

    assert run_pipeline(config) == 0
    assert captured['standalone_cost_items'] == ('委外加工费', '软件费用')
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
conda run -n test python -m pytest tests/test_pipeline_config.py tests/test_runner.py -q
```

Expected:

- `TypeError: PipelineConfig.__init__() got an unexpected keyword argument 'standalone_cost_items'`
- or `TypeError: _DummyETL.__init__() missing 1 required positional argument: 'standalone_cost_items'`

- [ ] **Step 3: Implement config propagation**

Update `src/config/pipelines.py`:

```python
@dataclass(frozen=True)
class PipelineConfig:
    name: str
    raw_dir: Path
    processed_dir: Path
    product_order: ProductOrder = ()
    input_patterns: tuple[str, ...] = ()
    standalone_cost_items: tuple[str, ...] = ('委外加工费',)
```

Define explicit pipeline defaults:

```python
GB_PIPELINE = PipelineConfig(
    name='gb',
    raw_dir=GB_RAW_DIR,
    processed_dir=GB_PROCESSED_DIR,
    product_order=GB_PRODUCT_ORDER,
    input_patterns=(
        'GB-*成本计算单.xlsx',
        'GB-* 成本计算单.xlsx',
        'GB-*.xlsx',
    ),
    standalone_cost_items=('委外加工费',),
)

SK_PIPELINE = PipelineConfig(
    name='sk',
    raw_dir=SK_RAW_DIR,
    processed_dir=SK_PROCESSED_DIR,
    product_order=SK_PRODUCT_ORDER,
    input_patterns=(
        'SK-*成本计算单.xlsx',
        'SK-* 成本计算单.xlsx',
        'SK-*.xlsx',
    ),
    standalone_cost_items=('委外加工费', '软件费用'),
)
```

Update `src/etl/runner.py` so the ETL constructor receives the config:

```python
etl = CostingWorkbookETL(
    skip_rows=2,
    product_order=config.product_order,
    standalone_cost_items=config.standalone_cost_items,
)
```

Update `src/etl/costing_etl.py` constructor signature and normalize storage:

```python
def __init__(
    self,
    skip_rows: int = 2,
    *,
    product_order: tuple[tuple[str, str], ...] | None = None,
    standalone_cost_items: tuple[str, ...] | None = None,
):
    self.skip_rows = skip_rows
    base_order = GB_PIPELINE.product_order if product_order is None else product_order
    base_standalone_items = (
        GB_PIPELINE.standalone_cost_items if standalone_cost_items is None else standalone_cost_items
    )
    normalized_order = tuple((str(code), str(name)) for code, name in base_order)
    self.product_order = normalized_order
    self.product_whitelist = frozenset(normalized_order)
    self.standalone_cost_items = tuple(str(item).strip() for item in base_standalone_items if str(item).strip())
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
conda run -n test python -m pytest tests/test_pipeline_config.py tests/test_runner.py -q
```

Expected:

```text
.....                                                                    [100%]
```

- [ ] **Step 5: Commit**

```bash
git add tests/test_pipeline_config.py tests/test_runner.py src/config/pipelines.py src/etl/runner.py src/etl/costing_etl.py
git commit -m "feat(config): add standalone cost items per pipeline"
```

### Task 2: Generalize Standalone Cost Handling in Analytics

**Files:**
- Modify: `tests/test_pq_analysis_v3.py`
- Modify: `src/analytics/fact_builder.py`
- Modify: `src/analytics/qty_enricher.py`

- [ ] **Step 1: Write failing analytics tests**

Add a positive `sk` test to `tests/test_pq_analysis_v3.py`:

```python
def test_build_report_artifacts_treats_software_fee_as_standalone_for_sk() -> None:
    df_detail = pd.concat(
        [
            _build_base_detail_df(),
            pd.DataFrame(
                [
                    {
                        '月份': '2025年1月',
                        '成本中心名称': '中心A',
                        '产品编码': 'GB_C.D.B0040AA',
                        '产品名称': 'BMS-750W驱动器',
                        '规格型号': 'S-01',
                        '工单编号': 'WO-001',
                        '工单行号': 1,
                        '基本单位': 'PCS',
                        '成本项目名称': '软件费用',
                        '本期完工金额': 8,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    df_qty = pd.DataFrame(
        [
            {
                '月份': '2025年1月',
                '成本中心名称': '中心A',
                '产品编码': 'GB_C.D.B0040AA',
                '产品名称': 'BMS-750W驱动器',
                '规格型号': 'S-01',
                '工单编号': 'WO-001',
                '工单行号': 1,
                '基本单位': 'PCS',
                '本期完工数量': 10,
                '本期完工金额': 173,
            }
        ]
    )

    artifacts = build_report_artifacts(
        df_detail,
        df_qty,
        standalone_cost_items=('委外加工费', '软件费用'),
    )

    row = artifacts.qty_sheet_df.iloc[0]
    work_order_row = artifacts.work_order_sheet.data.iloc[0]

    assert row['本期完工软件费用合计完工金额'] == Decimal('8')
    assert row['软件费用单位完工成本'] == Decimal('0.8')
    assert row[QTY_TOTAL_MATCH] == '是'
    assert work_order_row['软件费用合计完工金额'] == Decimal('8')
    assert work_order_row['软件费用单位完工成本'] == Decimal('0.8')
    assert '软件费用异常标记' not in artifacts.work_order_sheet.data.columns
    assert 'log_软件费用单位完工成本' not in artifacts.work_order_sheet.data.columns
    assert 'Modified Z-score_软件费用' not in artifacts.work_order_sheet.data.columns
    assert 'UNMAPPED_COST_ITEM' not in set(artifacts.error_log['issue_type'])
```

Add a negative `gb` fallback test:

```python
def test_build_report_artifacts_keeps_software_fee_unmapped_for_gb() -> None:
    df_detail = pd.concat(
        [
            _build_base_detail_df(),
            pd.DataFrame(
                [
                    {
                        '月份': '2025年1月',
                        '成本中心名称': '中心A',
                        '产品编码': 'GB_C.D.B0040AA',
                        '产品名称': 'BMS-750W驱动器',
                        '规格型号': 'S-01',
                        '工单编号': 'WO-001',
                        '工单行号': 1,
                        '基本单位': 'PCS',
                        '成本项目名称': '软件费用',
                        '本期完工金额': 8,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    df_qty = pd.DataFrame(
        [
            {
                '月份': '2025年1月',
                '成本中心名称': '中心A',
                '产品编码': 'GB_C.D.B0040AA',
                '产品名称': 'BMS-750W驱动器',
                '规格型号': 'S-01',
                '工单编号': 'WO-001',
                '工单行号': 1,
                '基本单位': 'PCS',
                '本期完工数量': 10,
                '本期完工金额': 165,
            }
        ]
    )

    artifacts = build_report_artifacts(
        df_detail,
        df_qty,
        standalone_cost_items=('委外加工费',),
    )

    assert 'UNMAPPED_COST_ITEM' in set(artifacts.error_log['issue_type'])
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
conda run -n test python -m pytest tests/test_pq_analysis_v3.py -q
```

Expected:

- `TypeError: build_report_artifacts() got an unexpected keyword argument 'standalone_cost_items'`
- or `KeyError: '本期完工软件费用合计完工金额'`

- [ ] **Step 3: Add shared standalone-cost metadata**

In `src/analytics/fact_builder.py`, add a typed metadata source:

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class StandaloneCostSpec:
    cost_item: str
    amount_key: str
    qty_amount_column: str
    qty_unit_cost_column: str
    work_order_amount_column: str
    work_order_unit_cost_column: str


STANDALONE_COST_SPECS: dict[str, StandaloneCostSpec] = {
    '委外加工费': StandaloneCostSpec(
        cost_item='委外加工费',
        amount_key='outsource_amount',
        qty_amount_column='本期完工委外加工费合计完工金额',
        qty_unit_cost_column='委外加工费单位完工成本',
        work_order_amount_column='委外加工费合计完工金额',
        work_order_unit_cost_column='委外加工费单位完工成本',
    ),
    '软件费用': StandaloneCostSpec(
        cost_item='软件费用',
        amount_key='software_amount',
        qty_amount_column='本期完工软件费用合计完工金额',
        qty_unit_cost_column='软件费用单位完工成本',
        work_order_amount_column='软件费用合计完工金额',
        work_order_unit_cost_column='软件费用单位完工成本',
    ),
}


def resolve_standalone_cost_specs(cost_items: tuple[str, ...]) -> tuple[StandaloneCostSpec, ...]:
    return tuple(STANDALONE_COST_SPECS[item] for item in cost_items)
```

- [ ] **Step 4: Implement config-driven standalone-cost aggregation**

Update `src/analytics/qty_enricher.py` to accept and use standalone-cost specs:

```python
def build_report_artifacts(
    df_detail: pd.DataFrame,
    df_qty: pd.DataFrame,
    *,
    standalone_cost_items: tuple[str, ...] = ('委外加工费',),
) -> AnalysisArtifacts:
    standalone_specs = resolve_standalone_cost_specs(standalone_cost_items)
    detail = _prepare_detail_frame(df_detail, detail_period_col, error_frames, standalone_cost_items)
    work_order_amounts = _aggregate_work_order_amounts(detail, standalone_specs)
    qty_sheet_df = _enrich_qty_sheet(qty_sheet_df, work_order_amounts, standalone_specs)
    analysis_source = _build_analysis_source(qty_sheet_df, error_frames, standalone_specs)
    work_order_sheet = build_anomaly_sheet(analysis_source, standalone_specs=standalone_specs)
```

Make `_prepare_detail_frame()` config-driven:

```python
def _prepare_detail_frame(
    df_detail: pd.DataFrame,
    detail_period_col: str,
    error_frames: list[pd.DataFrame],
    standalone_cost_items: tuple[str, ...],
) -> pd.DataFrame:
    detail = df_detail.copy().rename(...)
    cost_item_text = detail['cost_item'].astype(str).str.strip()
    standalone_item_set = frozenset(standalone_cost_items)
    detail['standalone_cost_item'] = cost_item_text.where(cost_item_text.isin(standalone_item_set))
    detail['cost_bucket'] = detail['cost_item'].map(map_broad_cost_bucket)
    detail['component_bucket'] = detail['cost_item'].map(map_component_bucket)
    detail['amount'] = detail['completed_amount'].map(to_decimal)

    unmapped_mask = detail['cost_bucket'].isna() & detail['standalone_cost_item'].isna()
    supported_cost_mask = detail['cost_bucket'].notna() | detail['standalone_cost_item'].notna()
```

Generalize work-order aggregation:

```python
def _aggregate_work_order_amounts(
    detail: pd.DataFrame,
    standalone_specs: tuple[StandaloneCostSpec, ...],
) -> pd.DataFrame:
    detail_for_analysis = detail.loc[detail['cost_bucket'].notna()].copy()
    work_order_amounts = broad_amounts.merge(component_amounts, on=WORK_ORDER_KEY_COLS + ['product_name'], how='left')

    for spec in standalone_specs:
        standalone_amounts = (
            detail.loc[detail['standalone_cost_item'].eq(spec.cost_item)]
            .groupby(WORK_ORDER_KEY_COLS + ['product_name'], dropna=False, as_index=False, sort=False)
            .agg(**{spec.amount_key: ('amount', sum_decimal_series)})
        )
        work_order_amounts = work_order_amounts.merge(
            standalone_amounts,
            on=WORK_ORDER_KEY_COLS + ['product_name'],
            how='outer',
        )
```

Build the total-reconciliation formula dynamically:

```python
def _build_total_formula_text(standalone_specs: tuple[StandaloneCostSpec, ...]) -> str:
    return '+'.join(['直接材料', '直接人工', '制造费用', *[spec.cost_item for spec in standalone_specs]])
```

Use it in `_enrich_qty_sheet()` and `_build_qty_reconciliation_errors()`:

```python
derived_total_amount = qty_sheet_df[QTY_DM_AMOUNT].combine(qty_sheet_df[QTY_DL_AMOUNT], add_decimal)
derived_total_amount = derived_total_amount.combine(qty_sheet_df[QTY_MOH_AMOUNT], add_decimal)
for spec in standalone_specs:
    qty_sheet_df[spec.qty_amount_column] = qty_sheet_df[spec.amount_key]
    qty_sheet_df[spec.qty_unit_cost_column] = qty_sheet_df[spec.qty_amount_column].combine(
        qty_sheet_df['completed_qty'],
        safe_divide,
    )
    derived_total_amount = derived_total_amount.combine(qty_sheet_df[spec.qty_amount_column], add_decimal)
qty_sheet_df['derived_total_amount'] = derived_total_amount
```

- [ ] **Step 5: Run tests to verify they pass**

Run:

```bash
conda run -n test python -m pytest tests/test_pq_analysis_v3.py -q
```

Expected:

```text
.....                                                                    [100%]
```

- [ ] **Step 6: Commit**

```bash
git add tests/test_pq_analysis_v3.py src/analytics/fact_builder.py src/analytics/qty_enricher.py
git commit -m "feat(analytics): support standalone software fee for sk"
```

### Task 3: Render Dynamic Standalone Columns in Work-Order and Workbook Output

**Files:**
- Modify: `tests/test_costing_etl.py`
- Modify: `src/analytics/anomaly.py`
- Modify: `src/excel/workbook_writer.py`
- Modify: `src/etl/costing_etl.py`

- [ ] **Step 1: Write the failing workbook-output test**

Add a dedicated `sk` workbook test to `tests/test_costing_etl.py`:

```python
def test_process_file_writes_sk_software_fee_columns_without_gb_pollution(tmp_path) -> None:
    etl = CostingWorkbookETL(
        skip_rows=2,
        product_order=(('DP.C.P0197AA', '动力线'),),
        standalone_cost_items=('委外加工费', '软件费用'),
    )
    input_path = tmp_path / 'input.xlsx'
    output_path = tmp_path / 'output.xlsx'

    df_raw = pd.DataFrame({'子项物料编码': ['MAT-001'], '成本项目名称': ['直接材料'], '年期': ['2025年1期']})
    df_detail = pd.DataFrame(
        [
            {
                '月份': '2025年1月',
                '成本中心名称': '中心A',
                '产品编码': 'DP.C.P0197AA',
                '产品名称': '动力线',
                '规格型号': 'S-01',
                '工单编号': 'WO-001',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '直接材料',
                '本期完工金额': 100,
            },
            {
                '月份': '2025年1月',
                '成本中心名称': '中心A',
                '产品编码': 'DP.C.P0197AA',
                '产品名称': '动力线',
                '规格型号': 'S-01',
                '工单编号': 'WO-001',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '软件费用',
                '本期完工金额': 8,
            },
        ]
    )
    df_qty = pd.DataFrame(
        [
            {
                '月份': '2025年1月',
                '成本中心名称': '中心A',
                '产品编码': 'DP.C.P0197AA',
                '产品名称': '动力线',
                '规格型号': 'S-01',
                '工单编号': 'WO-001',
                '工单行号': 1,
                '基本单位': 'PCS',
                '本期完工数量': 10,
                '本期完工金额': 108,
            }
        ]
    )

    with (
        patch('src.etl.costing_etl.pd.read_excel', return_value=df_raw),
        patch.object(CostingWorkbookETL, '_split_sheets', return_value=(df_detail, df_qty)),
    ):
        assert etl.process_file(input_path, output_path) is True

    wb = load_workbook(output_path)
    qty_headers = _build_header_map(wb['产品数量统计'])
    work_order_headers = _build_header_map(wb['按工单按产品异常值分析'])

    assert '本期完工软件费用合计完工金额' in qty_headers
    assert '软件费用单位完工成本' in qty_headers
    assert '软件费用合计完工金额' in work_order_headers
    assert '软件费用单位完工成本' in work_order_headers
    assert '软件费用异常标记' not in work_order_headers
```

- [ ] **Step 2: Run the workbook test to verify it fails**

Run:

```bash
conda run -n test python -m pytest tests/test_costing_etl.py -k software_fee -q
```

Expected:

- `AssertionError: assert '本期完工软件费用合计完工金额' in qty_headers`
- or `TypeError` because `CostingWorkbookETL` still calls `build_report_artifacts()` without the new keyword

- [ ] **Step 3: Implement dynamic work-order output columns**

In `src/analytics/anomaly.py`, replace fixed standalone outsource columns with helper-driven columns:

```python
def _build_work_order_output_columns(
    standalone_specs: tuple[StandaloneCostSpec, ...],
) -> list[str]:
    return [
        '月份',
        '成本中心',
        '产品编码',
        '产品名称',
        '规格型号',
        '工单编号',
        '工单行',
        '基本单位',
        '本期完工数量',
        '总完工成本',
        '直接材料合计完工金额',
        '直接人工合计完工金额',
        '制造费用合计完工金额',
        '制造费用_其他合计完工金额',
        '制造费用_人工合计完工金额',
        '制造费用_机物料及低耗合计完工金额',
        '制造费用_折旧合计完工金额',
        '制造费用_水电费合计完工金额',
        *[spec.work_order_amount_column for spec in standalone_specs],
        '总单位完工成本',
        '直接材料单位完工成本',
        '直接人工单位完工成本',
        '制造费用单位完工成本',
        '制造费用_其他单位完工成本',
        '制造费用_人工单位完工成本',
        '制造费用_机物料及低耗单位完工成本',
        '制造费用_折旧单位完工成本',
        '制造费用_水电费单位完工成本',
        *[spec.work_order_unit_cost_column for spec in standalone_specs],
        'log_总单位完工成本',
        'log_直接材料单位完工成本',
        'log_直接人工单位完工成本',
        'log_制造费用单位完工成本',
        'log_制造费用_其他单位完工成本',
        'log_制造费用_人工单位完工成本',
        'log_制造费用_机物料及低耗单位完工成本',
        'log_制造费用_折旧单位完工成本',
        'log_制造费用_水电费单位完工成本',
        'Modified Z-score_总单位完工成本',
        'Modified Z-score_直接材料',
        'Modified Z-score_直接人工',
        'Modified Z-score_制造费用',
        'Modified Z-score_制造费用_其他',
        'Modified Z-score_制造费用_人工',
        'Modified Z-score_制造费用_机物料及低耗',
        'Modified Z-score_制造费用_折旧',
        'Modified Z-score_制造费用_水电费',
        '是否可参与分析',
        '总成本异常标记',
        '直接材料异常标记',
        '直接人工异常标记',
        '制造费用异常标记',
        '制造费用_其他异常标记',
        '制造费用_人工异常标记',
        '制造费用_机物料及低耗异常标记',
        '制造费用_折旧异常标记',
        '制造费用_水电费异常标记',
        '异常等级',
        '异常主要来源',
        '复核原因',
    ]
```

Extend `build_anomaly_sheet()` accordingly:

```python
def build_anomaly_sheet(
    work_order_df: pd.DataFrame,
    *,
    standalone_specs: tuple[StandaloneCostSpec, ...] = (),
) -> FlatSheet:
    anomaly_df = work_order_df.copy()
    for spec in standalone_specs:
        anomaly_df[f'{spec.amount_key}_unit_cost'] = anomaly_df[spec.amount_key].combine(
            anomaly_df['completed_qty'],
            safe_divide,
        )

    rename_map.update(
        {
            spec.amount_key: spec.work_order_amount_column,
            f'{spec.amount_key}_unit_cost': spec.work_order_unit_cost_column,
            for spec in standalone_specs
        }
    )
    output_df = anomaly_df.rename(columns=rename_map)
    output_columns = _build_work_order_output_columns(standalone_specs)
    column_types = _build_work_order_column_types(standalone_specs)
    return FlatSheet(data=output_df[output_columns], column_types=column_types)
```

Keep standalone items out of `ANOMALY_METRICS`; do not add flag/log/score entries for them.

- [ ] **Step 4: Implement ETL forwarding and dynamic numeric formatting**

In `src/etl/costing_etl.py`, forward the config:

```python
artifacts = build_report_artifacts(
    df_detail,
    df_qty,
    standalone_cost_items=self.standalone_cost_items,
)
```

In `src/excel/workbook_writer.py`, resolve quantity-sheet numeric columns dynamically:

```python
def _resolve_qty_numeric_columns(qty_sheet_df: pd.DataFrame) -> set[str]:
    base_columns = {column for column in QTY_TWO_DECIMAL_COLUMNS if column in qty_sheet_df.columns}
    dynamic_columns = {
        column
        for column in qty_sheet_df.columns
        if (column.startswith('本期完工') and column.endswith('合计完工金额'))
        or column.endswith('单位完工成本')
    }
    return base_columns | dynamic_columns
```

Then use it in `write_workbook()`:

```python
self.sheet_writer.write_dataframe_sheet(
    writer,
    '产品数量统计',
    qty_sheet_df,
    numeric_columns=_resolve_qty_numeric_columns(qty_sheet_df),
    freeze_panes='A2',
)
```

- [ ] **Step 5: Run the targeted workbook tests**

Run:

```bash
conda run -n test python -m pytest tests/test_costing_etl.py -k "software_fee or process_file_writes_v3_analysis_sheets" -q
```

Expected:

```text
..                                                                       [100%]
```

- [ ] **Step 6: Commit**

```bash
git add tests/test_costing_etl.py src/analytics/anomaly.py src/excel/workbook_writer.py src/etl/costing_etl.py
git commit -m "feat(excel): render standalone software fee columns for sk"
```

### Task 4: Sync Docs and Run Full Verification

**Files:**
- Modify: `README.md`
- Modify: `AGENTS.md`

- [ ] **Step 1: Update README and AGENTS business rules**

Update `README.md` so the output rules describe `sk` software fee behavior explicitly:

```markdown
- `委外加工费` 与 `软件费用` 在 `sk` 管线中作为独立成本项展示：
  - 不归入三大类价量分析
  - 不参与工单异常评分
  - 参与总完工成本勾稽
- `gb` 管线仅保留 `委外加工费` 作为独立成本项，`软件费用` 仍视为未映射成本项目
```

Update `AGENTS.md` current business rules section:

```markdown
- `委外加工费` 不归属 `制造费用`。
- `sk` 管线中的 `软件费用` 与 `委外加工费` 口径一致：
  - 只在 `产品数量统计` 和 `按工单按产品异常值分析` 中展示
  - 不进入三大类价量分析
  - 不参与异常等级与异常主要来源判定
  - 参与总完工成本勾稽
- `gb` 管线不启用 `软件费用` 独立成本项规则。
```

- [ ] **Step 2: Run the full unit and lint suite**

Run:

```bash
conda run -n test python -m pytest tests -q
conda run -n test ruff check .
conda run -n test ruff format . --check
```

Expected:

```text
42 passed in <time>
All checks passed!
<N> files already formatted
```

- [ ] **Step 3: Run both real pipeline entrypoints**

Run:

```bash
conda run -n test python main.py gb
conda run -n test python main.py sk
```

Expected:

- `gb` completes successfully and does not gain `软件费用` output columns
- `sk` completes successfully and writes `软件费用` standalone columns into the generated workbook

- [ ] **Step 4: Commit**

```bash
git add README.md AGENTS.md
git commit -m "docs: document sk standalone software fee rule"
```

## Self-Review Checklist

- Spec coverage:
  - `sk` software fee standalone handling: Task 2
  - `gb` unchanged: Tasks 2 and 3 tests
  - total reconciliation includes software fee: Task 2
  - anomaly sheet excludes software-fee scoring: Tasks 2 and 3
  - workbook output adds only `sk` columns: Task 3
  - docs/business-rule sync: Task 4
- Placeholder scan:
  - No `TODO` / `TBD`
  - All tasks include exact files, commands, and code snippets
- Type consistency:
  - `standalone_cost_items` is the config field name in all tasks
  - `StandaloneCostSpec` / `resolve_standalone_cost_specs()` are defined in Task 2 before later tasks use them
  - `build_report_artifacts(..., standalone_cost_items=...)` is the shared analytics entrypoint used by ETL

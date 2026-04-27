# GB Product Anomaly Scope Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 仅在 `gb` 管线下，把 `按产品异常值分析` 从单段摘要改成 `全部 / 正常生产 / 返工生产` 多段摘要，同时保证 `sk` 继续保持当前 legacy 单段布局。

**Architecture:** 通过管线配置新增 `product_anomaly_scope_mode` 做显式分流，`gb` 走 `doc_type_split`，`sk` 走 `legacy_single_scope`。分析层只在 `gb` 模式下消费 `doc_type` 并生成带 `section_label` 的摘要分段；导出层在 `FastSheetWriter` 的 `按产品异常值分析` 特殊布局里根据 `section_label` 渲染多段或 legacy 单段。

**Tech Stack:** Python 3.11+, pandas, polars, openpyxl/xlsxwriter, pytest, Ruff

---

## File Map

- Modify: `D:/Program_python/02--costing_calculate/src/config/pipelines.py`
  责任：为不同管线声明 `product_anomaly_scope_mode`，把 `gb` / `sk` 的差异留在配置层。
- Modify: `D:/Program_python/02--costing_calculate/src/etl/runner.py`
  责任：运行入口把 `product_anomaly_scope_mode` 传给 `CostingWorkbookETL`。
- Modify: `D:/Program_python/02--costing_calculate/src/etl/costing_etl.py`
  责任：归一化并保存 `product_anomaly_scope_mode`，并把它透传给 payload builder。
- Modify: `D:/Program_python/02--costing_calculate/src/etl/pipeline.py`
  责任：将 `product_anomaly_scope_mode` 继续透传给 `build_report_artifacts`。
- Modify: `D:/Program_python/02--costing_calculate/src/analytics/contracts.py`
  责任：给 `ProductAnomalySection` 增加 `section_label`，表达“同一产品下的多个摘要分段”。
- Modify: `D:/Program_python/02--costing_calculate/src/analytics/fact_builder.py`
  责任：把 `单据类型` 从 `qty_fact` 带到 `work_order_fact`，供 `gb` 的摘要拆段逻辑使用。
- Modify: `D:/Program_python/02--costing_calculate/src/analytics/qty_enricher.py`
  责任：`build_report_artifacts` 接收 `product_anomaly_scope_mode`，按模式调用摘要构建函数。
- Modify: `D:/Program_python/02--costing_calculate/src/analytics/table_rendering.py`
  责任：实现 `gb/doc_type_split` 的分段汇总，同时保留 `legacy_single_scope` 的当前行为。
- Modify: `D:/Program_python/02--costing_calculate/src/analytics/presentation_builder.py`
  责任：把 `section_label` 编码进 `SheetModel` 的平铺 DataFrame，供 `FastSheetWriter` 还原。
- Modify: `D:/Program_python/02--costing_calculate/src/excel/fast_writer.py`
  责任：真实导出路径；按 `section_label` 还原分段并渲染 `gb` 的多段布局，保留 `sk` legacy 布局。
- Modify: `D:/Program_python/02--costing_calculate/tests/test_pipeline_config.py`
  责任：冻结 `gb` / `sk` 的 `product_anomaly_scope_mode` 配置值。
- Modify: `D:/Program_python/02--costing_calculate/tests/test_runner.py`
  责任：冻结 runner 对 `product_anomaly_scope_mode` 的透传。
- Modify: `D:/Program_python/02--costing_calculate/tests/test_pq_analysis.py`
  责任：冻结 `build_product_anomaly_sections` 的 `gb` 分段逻辑与 `sk` legacy 逻辑。
- Modify: `D:/Program_python/02--costing_calculate/tests/test_pq_analysis_v3.py`
  责任：冻结 `build_report_artifacts` 在 `product_anomaly_scope_mode` 分流下的行为与 `doc_type` 数据链路。
- Modify: `D:/Program_python/02--costing_calculate/tests/test_costing_etl.py`
  责任：冻结 `SheetModel` 路径和 workbook 导出路径下的 `gb` 多段布局，以及 `sk` legacy 不回归。
- Modify: `D:/Program_python/02--costing_calculate/tests/contracts/_workbook_contract_helper.py`
  责任：扩展 `按产品异常值分析` 语义提取器，使其能识别 `gb` scoped 布局与 `sk` legacy 布局。
- Modify: `D:/Program_python/02--costing_calculate/tests/contracts/baselines/workbook_semantics.json`
  责任：更新默认 workbook 的 `按产品异常值分析` 语义快照。

## Implementation Notes

- `product_anomaly_scope_mode` 使用字符串字面量，先不额外引入 `Enum`，保持现有 dataclass 简洁。
- `PipelineConfig.product_anomaly_scope_mode` 默认值使用 `legacy_single_scope`，避免仓库中手工实例化 `PipelineConfig` 的旧测试被强制改动。
- `CostingWorkbookETL()` 的默认行为仍然以 `GB_PIPELINE` 为基准，因此当未显式传入 `product_anomaly_scope_mode` 时，应默认取 `GB_PIPELINE.product_anomaly_scope_mode`。
- `build_product_anomaly_sections` 保持向后兼容：
  - `legacy_single_scope` 模式继续接受当前的 summary/fact 风格输入；
  - `doc_type_split` 模式消费 `work_order_df` 风格输入，并要求 `doc_type`、`completed_amount_total`、`completed_qty` 等字段存在。
- 真实 workbook 写出路径是 `FastSheetWriter`，不是 `sheet_writers.py`；不要把主实现写到未被实例化的旧 writer。
- `gb` scoped 布局的首个数据块建议使用：
  - `A1`：总标题
  - `A3:B4`：产品元信息
  - `A5:B5`：`分析口径`
  - `A6:*`：表头
  - `A7:*`：第一行数据
  - `freeze_panes='A7'`
- `sk` legacy 布局继续保持：
  - `A1`：总标题
  - `A3:B4`：产品元信息
  - `A5:*`：表头
  - `A6:*`：第一行数据
  - `freeze_panes='A6'`

### Task 1: Scope Mode Plumbing

**Files:**
- Modify: `D:/Program_python/02--costing_calculate/src/config/pipelines.py`
- Modify: `D:/Program_python/02--costing_calculate/src/etl/runner.py`
- Modify: `D:/Program_python/02--costing_calculate/src/etl/costing_etl.py`
- Modify: `D:/Program_python/02--costing_calculate/src/etl/pipeline.py`
- Modify: `D:/Program_python/02--costing_calculate/src/analytics/qty_enricher.py`
- Test: `D:/Program_python/02--costing_calculate/tests/test_pipeline_config.py`
- Test: `D:/Program_python/02--costing_calculate/tests/test_runner.py`
- Test: `D:/Program_python/02--costing_calculate/tests/test_costing_etl.py`

- [ ] **Step 1: Write the failing plumbing tests**

```python
# tests/test_pipeline_config.py
def test_pipeline_product_anomaly_scope_modes_are_defined_per_target() -> None:
    assert GB_PIPELINE.product_anomaly_scope_mode == 'doc_type_split'
    assert SK_PIPELINE.product_anomaly_scope_mode == 'legacy_single_scope'
```

```python
# tests/test_runner.py
class _DummyETL:
    def __init__(
        self,
        skip_rows: int,
        *,
        product_order,
        standalone_cost_items,
        product_anomaly_scope_mode,
    ) -> None:
        self.skip_rows = skip_rows
        self.product_order = product_order
        self.last_quality_metrics = ()
        self.last_error_log_count = 0
        self.last_error_log_frame = pd.DataFrame()
        captured['standalone_cost_items'] = standalone_cost_items
        captured['product_anomaly_scope_mode'] = product_anomaly_scope_mode

assert captured['product_anomaly_scope_mode'] == config.product_anomaly_scope_mode
```

```python
# tests/test_costing_etl.py
def test_process_file_passes_product_anomaly_scope_mode_to_pipeline_payload_builder(tmp_path) -> None:
    etl = CostingWorkbookETL(
        skip_rows=2,
        product_order=(),
        standalone_cost_items=('委外加工费',),
        product_anomaly_scope_mode='doc_type_split',
    )

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

    assert payload_mock.call_args.kwargs['product_anomaly_scope_mode'] == 'doc_type_split'
```

- [ ] **Step 2: Run the plumbing tests and verify they fail**

Run:

```bash
conda run -n test python -m pytest tests/test_pipeline_config.py tests/test_runner.py tests/test_costing_etl.py -q
```

Expected:

```text
FAILED tests/test_pipeline_config.py::test_pipeline_product_anomaly_scope_modes_are_defined_per_target
TypeError: _DummyETL.__init__() got an unexpected keyword argument 'product_anomaly_scope_mode'
KeyError: 'product_anomaly_scope_mode'
```

- [ ] **Step 3: Implement configuration and ETL propagation**

```python
# src/config/pipelines.py
@dataclass(frozen=True)
class PipelineConfig:
    name: str
    raw_dir: Path
    processed_dir: Path
    product_order: ProductOrder = ()
    input_patterns: tuple[str, ...] = ()
    standalone_cost_items: tuple[str, ...] = ('委外加工费',)
    product_anomaly_scope_mode: str = 'legacy_single_scope'


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
    product_anomaly_scope_mode='doc_type_split',
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
    product_anomaly_scope_mode='legacy_single_scope',
)
```

```python
# src/etl/runner.py
etl = CostingWorkbookETL(
    skip_rows=2,
    product_order=config.product_order,
    standalone_cost_items=config.standalone_cost_items,
    product_anomaly_scope_mode=config.product_anomaly_scope_mode,
)
```

```python
# src/etl/costing_etl.py
# add to __init__ signature
product_anomaly_scope_mode: str | None = None,

# add after standalone_cost_items normalization
base_scope_mode = (
    GB_PIPELINE.product_anomaly_scope_mode
    if product_anomaly_scope_mode is None
    else product_anomaly_scope_mode
)
self.product_anomaly_scope_mode = str(base_scope_mode).strip() or 'legacy_single_scope'
```

```python
# src/etl/costing_etl.py inside process_file -> build_workbook_payload
payload = self.pipeline.build_workbook_payload(
    input_path,
    standalone_cost_items=self.standalone_cost_items,
    product_anomaly_scope_mode=self.product_anomaly_scope_mode,
    artifacts_transform=self._filter_artifacts_for_analysis,
)
```

```python
# src/etl/pipeline.py
def build_workbook_payload(
    self,
    input_path: Path,
    *,
    standalone_cost_items: tuple[str, ...],
    product_anomaly_scope_mode: str,
    artifacts_transform: Callable[[AnalysisArtifacts], AnalysisArtifacts] | None = None,
) -> WorkbookPayload:
    artifacts = build_report_artifacts(
        split_result.detail_df,
        split_result.qty_df,
        standalone_cost_items=standalone_cost_items,
        product_anomaly_scope_mode=product_anomaly_scope_mode,
    )
```

```python
# src/analytics/qty_enricher.py
def build_report_artifacts(
    df_detail: pd.DataFrame | pl.DataFrame,
    df_qty: pd.DataFrame | pl.DataFrame,
    standalone_cost_items: tuple[str, ...] | list[str] | None = DEFAULT_STANDALONE_COST_ITEMS,
    product_anomaly_scope_mode: str = 'legacy_single_scope',
) -> AnalysisArtifacts:
    return AnalysisArtifacts(
        product_anomaly_sections=build_product_anomaly_sections(
            work_order_source_pd,
            scope_mode=product_anomaly_scope_mode,
        ),
        fact_df=fact_df,
        qty_sheet_df=qty_sheet_output,
        work_order_sheet=work_order_sheet,
        quality_metrics=quality_metrics,
        error_log=error_log,
        fact_bundle=fact_bundle,
    )
```

- [ ] **Step 4: Run the plumbing tests again and verify they pass**

Run:

```bash
conda run -n test python -m pytest tests/test_pipeline_config.py tests/test_runner.py tests/test_costing_etl.py -q
```

Expected:

Expected: `pytest` exit code is `0`, and the selected files all pass.

- [ ] **Step 5: Commit the plumbing slice**

```bash
git add src/config/pipelines.py src/etl/runner.py src/etl/costing_etl.py src/etl/pipeline.py src/analytics/qty_enricher.py tests/test_pipeline_config.py tests/test_runner.py tests/test_costing_etl.py
git commit -m "feat(config): plumb product anomaly scope mode"
```

### Task 2: Build GB Split Sections While Preserving Legacy

**Files:**
- Modify: `D:/Program_python/02--costing_calculate/src/analytics/contracts.py`
- Modify: `D:/Program_python/02--costing_calculate/src/analytics/fact_builder.py`
- Modify: `D:/Program_python/02--costing_calculate/src/analytics/table_rendering.py`
- Modify: `D:/Program_python/02--costing_calculate/src/analytics/qty_enricher.py`
- Test: `D:/Program_python/02--costing_calculate/tests/test_pq_analysis.py`
- Test: `D:/Program_python/02--costing_calculate/tests/test_pq_analysis_v3.py`

- [ ] **Step 1: Write the failing summary-layer tests**

```python
# tests/test_pq_analysis.py
def test_build_product_anomaly_sections_splits_gb_doc_types_into_scopes() -> None:
    work_order_df = pd.DataFrame(
        [
            {
                'period': '2025-01',
                'product_code': 'P001',
                'product_name': '产品A',
                'doc_type': '汇报入库-普通生产',
                'completed_amount_total': Decimal('100'),
                'completed_qty': Decimal('10'),
                'dm_amount': Decimal('60'),
                'dl_amount': Decimal('20'),
                'moh_amount': Decimal('20'),
            },
            {
                'period': '2025-01',
                'product_code': 'P001',
                'product_name': '产品A',
                'doc_type': '直接入库-普通生产',
                'completed_amount_total': Decimal('50'),
                'completed_qty': Decimal('5'),
                'dm_amount': Decimal('30'),
                'dl_amount': Decimal('10'),
                'moh_amount': Decimal('10'),
            },
            {
                'period': '2025-01',
                'product_code': 'P001',
                'product_name': '产品A',
                'doc_type': '汇报入库-返工生产',
                'completed_amount_total': Decimal('30'),
                'completed_qty': Decimal('3'),
                'dm_amount': Decimal('18'),
                'dl_amount': Decimal('6'),
                'moh_amount': Decimal('6'),
            },
            {
                'period': '2025-01',
                'product_code': 'P001',
                'product_name': '产品A',
                'doc_type': '普通委外订单',
                'completed_amount_total': Decimal('20'),
                'completed_qty': Decimal('2'),
                'dm_amount': Decimal('10'),
                'dl_amount': Decimal('5'),
                'moh_amount': Decimal('5'),
            },
        ]
    )

    sections = build_product_anomaly_sections(work_order_df, scope_mode='doc_type_split')

    assert [section.section_label for section in sections] == ['全部', '正常生产', '返工生产']
    assert sections[0].data.iloc[0]['总成本'] == Decimal('200')
    assert sections[1].data.iloc[0]['总成本'] == Decimal('150')
    assert sections[2].data.iloc[0]['总成本'] == Decimal('30')
```

```python
# tests/test_pq_analysis.py
def test_build_product_anomaly_sections_keeps_legacy_single_scope_for_sk_mode() -> None:
    sections = build_product_anomaly_sections(_sample_fact_df(), scope_mode='legacy_single_scope')

    assert len(sections) == 1
    assert sections[0].section_label is None
    assert list(sections[0].data['月份']) == ['2025年01期', '2025年02期']
```

```python
# tests/test_pq_analysis_v3.py
def test_build_report_artifacts_doc_type_split_generates_scoped_sections() -> None:
    df_detail = _build_base_detail_df().assign(单据类型='汇报入库-普通生产')
    df_qty = _build_base_qty_df(total_amount=165).assign(单据类型='汇报入库-普通生产')

    artifacts = build_report_artifacts(
        df_detail,
        df_qty,
        standalone_cost_items=('委外加工费',),
        product_anomaly_scope_mode='doc_type_split',
    )

    assert 'doc_type' in artifacts.fact_bundle.work_order_fact.columns
    assert [section.section_label for section in artifacts.product_anomaly_sections] == ['全部', '正常生产']
```

- [ ] **Step 2: Run the summary-layer tests and verify they fail**

Run:

```bash
conda run -n test python -m pytest tests/test_pq_analysis.py tests/test_pq_analysis_v3.py -q
```

Expected:

```text
TypeError: build_product_anomaly_sections() got an unexpected keyword argument 'scope_mode'
AttributeError: 'ProductAnomalySection' object has no attribute 'section_label'
AssertionError: 'doc_type' not found in work_order_fact columns
```

- [ ] **Step 3: Implement `doc_type` carry-through and section generation**

```python
# src/analytics/contracts.py
@dataclass
class ProductAnomalySection:
    product_code: str
    product_name: str
    data: pd.DataFrame
    column_types: dict[str, str]
    amount_columns: list[str]
    outlier_cells: set[tuple[int, str]]
    section_label: str | None = None
```

```python
# src/analytics/fact_builder.py
work_order_columns = [
    'period',
    'period_display',
    'product_code',
    'product_name',
    'order_no',
    'order_line',
    'cost_center',
    'spec',
    'unit',
    'doc_type',
    'completed_qty_raw',
    'completed_amount_total_raw',
]

work_order_fact = (
    qty_fact.sort('_source_row')
    .unique(subset=['_join_key'], keep='first', maintain_order=True)
    .with_columns(
        [
            (pl.col('成本中心名称') if '成本中心名称' in qty_fact.columns else pl.lit(None)).alias('cost_center'),
            (pl.col('规格型号') if '规格型号' in qty_fact.columns else pl.lit(None)).alias('spec'),
            (pl.col('基本单位') if '基本单位' in qty_fact.columns else pl.lit(None)).alias('unit'),
            (pl.col('单据类型') if '单据类型' in qty_fact.columns else pl.lit(None)).alias('doc_type'),
            _safe_divide_expr('completed_amount_total', 'completed_qty', 'total_unit_cost'),
        ]
    )
    .select(work_order_columns)
)
```

```python
# src/analytics/table_rendering.py
DOC_TYPE_SPLIT_SCOPE_MODE = 'doc_type_split'
LEGACY_SINGLE_SCOPE_MODE = 'legacy_single_scope'
GB_DOC_TYPE_SCOPE_MAP = {
    '汇报入库-普通生产': '正常生产',
    '直接入库-普通生产': '正常生产',
    '汇报入库-返工生产': '返工生产',
}


def _classify_gb_scope(doc_type: object) -> str | None:
    if doc_type is None or pd.isna(doc_type):
        return None
    return GB_DOC_TYPE_SCOPE_MAP.get(str(doc_type).strip())


def build_product_anomaly_sections(
    source_df: pd.DataFrame,
    *,
    scope_mode: str = LEGACY_SINGLE_SCOPE_MODE,
) -> list[ProductAnomalySection]:
    if scope_mode != DOC_TYPE_SPLIT_SCOPE_MODE:
        if {'completed_amount_total', 'completed_qty', 'dm_amount', 'dl_amount', 'moh_amount'}.issubset(source_df.columns):
            summary_df = build_product_summary_df(source_df)
        elif 'period_display' not in source_df.columns:
            summary_df = build_product_summary_from_fact_df(source_df)
        else:
            summary_df = source_df.copy()
        return _build_legacy_product_anomaly_sections(summary_df)

    if source_df.empty:
        return []

    scoped_df = source_df.copy()
    scoped_df['scope_label'] = scoped_df['doc_type'].map(_classify_gb_scope)

    sections: list[ProductAnomalySection] = []
    grouped = scoped_df.groupby(['product_code', 'product_name'], dropna=False, sort=False)
    for (product_code, product_name), product_frame in grouped:
        all_summary = build_product_summary_df(product_frame)
        sections.extend(
            _build_labeled_sections_for_product(
                product_code=str(product_code),
                product_name=str(product_name),
                summaries=[('全部', all_summary)] + [
                    (
                        label,
                        build_product_summary_df(product_frame[product_frame['scope_label'] == label]),
                    )
                    for label in ('正常生产', '返工生产')
                    if not product_frame[product_frame['scope_label'] == label].empty
                ],
            )
        )
    return sections
```

```python
# src/analytics/table_rendering.py
def _build_legacy_product_anomaly_sections(summary_df: pd.DataFrame) -> list[ProductAnomalySection]:
    if summary_df.empty:
        return []

    sections: list[ProductAnomalySection] = []
    grouped = summary_df.groupby(['product_code', 'product_name'], dropna=False, sort=False)
    for (product_code, product_name), product_frame in grouped:
        product_frame = product_frame.sort_values('period').reset_index(drop=True)
        display_data = pd.DataFrame({'月份': product_frame['period_display']})
        column_types = {'月份': 'text'}
        amount_columns: list[str] = []
        for internal_key, display_name, metric_type, _detect in PRODUCT_ANALYSIS_FIELDS:
            display_data[display_name] = product_frame[internal_key]
            column_types[display_name] = metric_type
            if metric_type == 'amount':
                amount_columns.append(display_name)
        sections.append(
            ProductAnomalySection(
                product_code=str(product_code),
                product_name=str(product_name),
                data=display_data,
                column_types=column_types,
                amount_columns=amount_columns,
                outlier_cells=set(),
            )
        )
    return sections
```

```python
# src/analytics/table_rendering.py
def _build_labeled_sections_for_product(
    *,
    product_code: str,
    product_name: str,
    summaries: list[tuple[str, pd.DataFrame]],
) -> list[ProductAnomalySection]:
    sections: list[ProductAnomalySection] = []
    for section_label, summary_df in summaries:
        product_frame = summary_df[
            (summary_df['product_code'] == product_code) & (summary_df['product_name'] == product_name)
        ].sort_values('period').reset_index(drop=True)
        display_data = pd.DataFrame({'月份': product_frame['period_display']})
        column_types = {'月份': 'text'}
        amount_columns: list[str] = []
        for internal_key, display_name, metric_type, _detect in PRODUCT_ANALYSIS_FIELDS:
            display_data[display_name] = product_frame[internal_key]
            column_types[display_name] = metric_type
            if metric_type == 'amount':
                amount_columns.append(display_name)
        sections.append(
            ProductAnomalySection(
                product_code=product_code,
                product_name=product_name,
                data=display_data,
                column_types=column_types,
                amount_columns=amount_columns,
                outlier_cells=set(),
                section_label=section_label,
            )
        )
    return sections
```

- [ ] **Step 4: Run the summary-layer tests again and verify they pass**

Run:

```bash
conda run -n test python -m pytest tests/test_pq_analysis.py tests/test_pq_analysis_v3.py -q
```

Expected:

Expected: `pytest` exit code is `0`, and the selected files all pass.

- [ ] **Step 5: Commit the summary-layer slice**

```bash
git add src/analytics/contracts.py src/analytics/fact_builder.py src/analytics/table_rendering.py src/analytics/qty_enricher.py tests/test_pq_analysis.py tests/test_pq_analysis_v3.py
git commit -m "feat(analytics): split gb product anomaly sections by doc type"
```

### Task 3: Render GB Scoped Layout in the Real Writer Path

**Files:**
- Modify: `D:/Program_python/02--costing_calculate/src/analytics/presentation_builder.py`
- Modify: `D:/Program_python/02--costing_calculate/src/excel/fast_writer.py`
- Modify: `D:/Program_python/02--costing_calculate/tests/test_costing_etl.py`
- Modify: `D:/Program_python/02--costing_calculate/tests/contracts/_workbook_contract_helper.py`
- Modify: `D:/Program_python/02--costing_calculate/tests/contracts/baselines/workbook_semantics.json`

- [ ] **Step 1: Write the failing writer-layout and contract tests**

```python
# tests/test_costing_etl.py
def test_workbook_writer_sheet_model_renders_product_anomaly_scope_split_layout_for_gb(tmp_path: Path) -> None:
    output_path = tmp_path / 'product_anomaly_scoped.xlsx'
    writer = CostingWorkbookWriter()
    sheet_models = build_sheet_models(
        detail_df=pd.DataFrame([{'月份': '2025年01期', '产品编码': 'P001'}]),
        qty_sheet_df=pd.DataFrame([{'月份': '2025年01期', '产品编码': 'P001', '本期完工数量': 10.0, '本期完工金额': 100.0}]),
        fact_bundle=None,
        work_order_sheet=FlatSheet(data=pd.DataFrame([{'月份': '2025年01期'}]), column_types={'月份': 'text'}),
        product_anomaly_sections=[
            ProductAnomalySection(
                product_code='P001',
                product_name='产品A',
                section_label='全部',
                data=pd.DataFrame([{'月份': '2025年01期', '总成本': 100.0, '完工数量': 10.0, '单位成本': 10.0}]),
                column_types={'月份': 'text', '总成本': 'amount', '完工数量': 'qty', '单位成本': 'price'},
                amount_columns=['总成本'],
                outlier_cells=set(),
            ),
            ProductAnomalySection(
                product_code='P001',
                product_name='产品A',
                section_label='正常生产',
                data=pd.DataFrame([{'月份': '2025年01期', '总成本': 80.0, '完工数量': 8.0, '单位成本': 10.0}]),
                column_types={'月份': 'text', '总成本': 'amount', '完工数量': 'qty', '单位成本': 'price'},
                amount_columns=['总成本'],
                outlier_cells=set(),
            ),
        ],
    )

    writer.write_workbook_from_models(output_path, sheet_models=sheet_models)

    workbook = load_workbook(output_path)
    worksheet = workbook['按产品异常值分析']
    assert worksheet['A5'].value == '分析口径'
    assert worksheet['B5'].value == '全部'
    assert worksheet['A9'].value == '分析口径'
    assert worksheet['B9'].value == '正常生产'
    assert worksheet.freeze_panes == 'A7'
```

```python
# tests/test_costing_etl.py
def test_build_sheet_models_serializes_scope_label_for_product_anomaly_rows() -> None:
    models = build_sheet_models(
        detail_df=pd.DataFrame([{'月份': '2025年01期', '产品编码': 'P001'}]),
        qty_sheet_df=pd.DataFrame([{'月份': '2025年01期', '产品编码': 'P001', '本期完工数量': 10.0, '本期完工金额': 100.0}]),
        fact_bundle=None,
        work_order_sheet=FlatSheet(data=pd.DataFrame([{'月份': '2025年01期'}]), column_types={'月份': 'text'}),
        product_anomaly_sections=[
            ProductAnomalySection(
                product_code='P001',
                product_name='产品A',
                section_label='全部',
                data=pd.DataFrame([{'月份': '2025年01期', '总成本': 100.0}]),
                column_types={'月份': 'text', '总成本': 'amount'},
                amount_columns=['总成本'],
                outlier_cells=set(),
            )
        ],
    )

    product_model = next(model for model in models if model.sheet_name == '按产品异常值分析')

    assert product_model.columns[:3] == ('产品编码', '产品名称', '分析口径')
    assert list(product_model.rows_factory())[0][:3] == ('P001', '产品A', '全部')
```

Expected contract update after implementation:

```python
# tests/contracts/_workbook_contract_helper.py
def test_default_workbook_semantics_match_baseline(tmp_path) -> None:
    baseline = load_contract_baseline('workbook_semantics.json')
    workbook_path = build_default_contract_workbook(tmp_path)
    actual = extract_workbook_semantics(workbook_path)
    assert actual == baseline['default_workbook']
```

- [ ] **Step 2: Run the writer-layout tests and verify they fail**

Run:

```bash
conda run -n test python -m pytest tests/test_costing_etl.py::test_workbook_writer_sheet_model_renders_product_anomaly_scope_split_layout_for_gb tests/test_costing_etl.py::test_build_sheet_models_serializes_scope_label_for_product_anomaly_rows tests/contracts/test_workbook_contract.py -q
```

Expected:

```text
AssertionError: worksheet['A5'].value == '月份'
AssertionError: product_model.columns[:3] == ('产品编码', '产品名称', '分析口径')
AssertionError: actual != baseline['default_workbook']
```

- [ ] **Step 3: Implement `SheetModel` flattening, section reconstruction, and scoped workbook layout**

```python
# src/analytics/presentation_builder.py
def _build_product_anomaly_frame(
    sections: list[ProductAnomalySection],
) -> tuple[pl.DataFrame, dict[str, str]]:
    if not sections:
        empty_columns = ['产品编码', '产品名称', '月份']
        return (
            _to_polars_frame(pd.DataFrame(columns=empty_columns)),
            {'产品编码': 'text', '产品名称': 'text', '月份': 'text'},
        )

    include_scope_label = any(section.section_label for section in sections)
    section_frames: list[pd.DataFrame] = []
    column_types: dict[str, str] = {'产品编码': 'text', '产品名称': 'text'}
    if include_scope_label:
        column_types['分析口径'] = 'text'

    for section in sections:
        section_df = section.data.copy()
        if include_scope_label:
            section_df.insert(0, '分析口径', section.section_label or '')
        section_df.insert(0, '产品名称', section.product_name)
        section_df.insert(0, '产品编码', section.product_code)
        section_frames.append(section_df)
        for column_name, metric_type in section.column_types.items():
            column_types.setdefault(column_name, metric_type)

    merged = pd.concat(section_frames, ignore_index=True, sort=False)
    for column_name in merged.columns:
        column_types.setdefault(column_name, 'text')
    return _to_polars_frame(merged), column_types
```

```python
# src/excel/fast_writer.py
def _build_product_anomaly_sections_from_model(self, model: SheetModel) -> list[ProductAnomalySection]:
    if len(model.columns) < 2:
        return []

    has_scope_label = len(model.columns) >= 3 and model.columns[2] == '分析口径'
    table_columns = list(model.columns[3:] if has_scope_label else model.columns[2:])
    grouped_rows: dict[tuple[str, str, str | None], list[tuple[object, ...]]] = {}
    group_order: list[tuple[str, str, str | None]] = []

    for row in model.rows_factory():
        product_code = '' if row[0] is None else str(row[0])
        product_name = '' if row[1] is None else str(row[1])
        section_label = None if not has_scope_label else (None if row[2] in (None, '') else str(row[2]))
        row_values = row[3 : 3 + len(table_columns)] if has_scope_label else row[2 : 2 + len(table_columns)]
        key = (product_code, product_name, section_label)
        if key not in grouped_rows:
            grouped_rows[key] = []
            group_order.append(key)
        grouped_rows[key].append(tuple(row_values))

    sections: list[ProductAnomalySection] = []
    for product_code, product_name, section_label in group_order:
        section_data = pd.DataFrame(grouped_rows[(product_code, product_name, section_label)], columns=table_columns)
        section_column_types = {column: model.column_types.get(column, 'text') for column in table_columns}
        sections.append(
            ProductAnomalySection(
                product_code=product_code,
                product_name=product_name,
                section_label=section_label,
                data=section_data,
                column_types=section_column_types,
                amount_columns=[],
                outlier_cells=set(),
            )
        )
    return sections
```

```python
# src/excel/fast_writer.py
def write_product_anomaly_sheet(
    self,
    writer: pd.ExcelWriter,
    sheet_name: str,
    sections: list[ProductAnomalySection],
) -> None:
    if any(section.section_label for section in sections):
        self._write_scoped_product_anomaly_sheet(writer, sheet_name, sections)
        return
    self._write_legacy_product_anomaly_sheet(writer, sheet_name, sections)
```

在添加 scoped 分支前，先把当前 `write_product_anomaly_sheet` 的完整 legacy 方法体重命名为 `_write_legacy_product_anomaly_sheet`，保持其行号、列宽、`freeze_panes='A6'`、表头与数字格式逻辑完全不变；这样 `sk` 路径可以零行为变化地复用旧实现。

```python
# src/excel/fast_writer.py
def _write_scoped_product_anomaly_sheet(
    self,
    writer: pd.ExcelWriter,
    sheet_name: str,
    sections: list[ProductAnomalySection],
) -> None:
    workbook = writer.book
    worksheet = workbook.add_worksheet(sheet_name)
    writer.sheets[sheet_name] = worksheet
    section_title_format = workbook.add_format({'bold': True, 'bg_color': '#FFD966', 'align': 'left', 'valign': 'vcenter'})
    meta_header_format = workbook.add_format({'bold': True, 'bg_color': '#D9E1F2', 'align': 'center', 'valign': 'vcenter', 'border': 1})
    meta_value_format = workbook.add_format({'bold': True, 'bg_color': '#B4C6E7', 'align': 'left', 'valign': 'vcenter', 'border': 1})
    table_header_format = workbook.add_format({'bold': True, 'bg_color': '#D9E1F2', 'align': 'center', 'valign': 'vcenter', 'border': 1})
    text_format = workbook.add_format({'align': 'left', 'valign': 'vcenter', 'border': 1})
    right_text_format = workbook.add_format({'align': 'right', 'valign': 'vcenter', 'border': 1})
    worksheet.write(0, 0, '四、按单个产品异常值分析', section_title_format)
    current_row = 2
    grouped_sections: dict[tuple[str, str], list[ProductAnomalySection]] = {}
    filter_set = False
    max_col_overall = 1
    for section in sections:
        grouped_sections.setdefault((section.product_code, section.product_name), []).append(section)

    for (product_code, product_name), product_sections in grouped_sections.items():
        worksheet.write(current_row, 0, '产品编码', meta_header_format)
        worksheet.write(current_row, 1, '产品名称', meta_header_format)
        worksheet.write(current_row + 1, 0, product_code, meta_value_format)
        worksheet.write(current_row + 1, 1, product_name, meta_value_format)
        current_row += 2

        for section in product_sections:
            worksheet.write(current_row, 0, '分析口径', meta_header_format)
            worksheet.write(current_row, 1, section.section_label or '', meta_value_format)
            table_header_row = current_row + 1
            data_start_row = current_row + 2
            columns = section.data.columns.tolist()
            max_col_overall = max(max_col_overall, len(columns))
            for col_idx, column_name in enumerate(columns):
                worksheet.write(table_header_row, col_idx, column_name, table_header_format)

            for row_offset, row_data in enumerate(section.data.itertuples(index=False, name=None)):
                excel_row = data_start_row + row_offset
                for col_idx, value in enumerate(row_data):
                    column_name = columns[col_idx]
                    metric_type = section.column_types.get(column_name, 'text')
                    number_format = resolve_metric_number_format(metric_type)
                    cell_format = text_format
                    if col_idx > 0:
                        if number_format is None:
                            cell_format = right_text_format
                        else:
                            cell_format = workbook.add_format(
                                {'align': 'right', 'valign': 'vcenter', 'border': 1, 'num_format': number_format}
                            )
                    _write_cell(worksheet, excel_row, col_idx, value, cell_format)

            data_end_row = data_start_row + len(section.data) - 1
            if not filter_set and columns:
                worksheet.autofilter(table_header_row, 0, max(table_header_row, data_end_row), len(columns) - 1)
                filter_set = True
            current_row = data_end_row + 2

    for col_idx in range(max_col_overall):
        worksheet.set_column(col_idx, col_idx, 15, text_format if col_idx == 0 else right_text_format)
    worksheet.freeze_panes(6, 0)  # A7
```

```python
# tests/contracts/_workbook_contract_helper.py
def _extract_product_anomaly_sheet(worksheet) -> dict[str, object]:
    if worksheet['A5'].value == '分析口径':
        headers = [worksheet.cell(6, col_idx).value for col_idx in range(1, worksheet.max_column + 1)]
        while headers and headers[-1] is None:
            headers.pop()
        first_data_formats = {
            header: worksheet.cell(7, col_idx).number_format
            for col_idx, header in enumerate(headers, start=1)
            if worksheet.max_row >= 7 and worksheet.cell(7, col_idx).number_format != 'General'
        }
        scope_labels = [
            worksheet.cell(row_idx, 2).value
            for row_idx in range(5, worksheet.max_row + 1)
            if worksheet.cell(row_idx, 1).value == '分析口径'
        ]
        return {
            'kind': 'product_anomaly',
            'layout': 'scoped',
            'freeze_panes': worksheet.freeze_panes,
            'auto_filter': worksheet.auto_filter.ref,
            'title': worksheet['A1'].value,
            'meta_labels': [worksheet['A3'].value, worksheet['B3'].value],
            'meta_values': [worksheet['A4'].value, worksheet['B4'].value],
            'scope_labels': scope_labels,
            'columns': headers,
            'number_formats': first_data_formats,
            'column_widths': _extract_column_widths(worksheet),
        }
```

在同一个函数里保留现有 legacy 解析逻辑，但给 legacy 返回值补一项：

```python
'layout': 'legacy',
```

- [ ] **Step 4: Regenerate the workbook semantics baseline from real output**

Run:

```bash
conda run -n test python -m tests.contracts.generate_baselines
```

Expected:

```text
# command exits 0 and rewrites tests/contracts/baselines/workbook_semantics.json
```

- [ ] **Step 5: Run the writer-layout and contract tests again and verify they pass**

Run:

```bash
conda run -n test python -m pytest tests/test_costing_etl.py tests/contracts/test_workbook_contract.py -q
```

Expected:

Expected: `pytest` exit code is `0`，并且 `tests/test_costing_etl.py` 与 `tests/contracts/test_workbook_contract.py` 全部通过。

- [ ] **Step 6: Commit the writer-layout slice**

```bash
git add src/analytics/presentation_builder.py src/excel/fast_writer.py tests/test_costing_etl.py tests/contracts/_workbook_contract_helper.py tests/contracts/baselines/workbook_semantics.json
git commit -m "feat(export): render gb product anomaly scope sections"
```

### Task 4: Full Regression Verification

**Files:**
- Modify: `D:/Program_python/02--costing_calculate/docs/superpowers/plans/2026-04-27-gb-product-anomaly-scope-split.md`
  仅在发现实际实现与计划偏差时更新；若实现完全按计划完成，则此 task 不改文件内容。

- [ ] **Step 1: Run the focused analytics/export regression suite**

Run:

```bash
conda run -n test python -m pytest tests/test_pipeline_config.py tests/test_runner.py tests/test_pq_analysis.py tests/test_pq_analysis_v3.py tests/test_costing_etl.py tests/contracts/test_workbook_contract.py -q
```

Expected:

Expected: `pytest` exit code is `0`，并且所列 focused regression tests 全部通过。

- [ ] **Step 2: Run the full test suite**

Run:

```bash
conda run -n test python -m pytest tests -q
```

Expected:

Expected: `pytest` exit code is `0`，并且 `tests/` 全量通过。

- [ ] **Step 3: Run Ruff check on touched code paths**

Run:

```bash
conda run -n test python -m ruff check src tests
```

Expected:

```text
All checks passed!
```

- [ ] **Step 4: Run Ruff format in check mode**

Run:

```bash
conda run -n test python -m ruff format --check src tests
```

Expected:

Expected: `ruff format --check` exit code is `0`，并报告所有文件已符合格式要求。

- [ ] **Step 5: Commit the verified final state**

```bash
git add src tests
git commit -m "feat(gb): split product anomaly summary by production scope"
```

## Self-Review Checklist

- Spec coverage:
  - `gb` only: Task 1 config plumbing + Task 2/3 split behavior。
  - `sk` 保持 legacy：Task 1 config defaults + Task 2 legacy tests + Task 3 legacy writer path。
  - `doc_type` 只用于摘要：Task 2 `fact_builder.py` + `table_rendering.py`。
  - `FastSheetWriter` 真实路径：Task 3 covers `presentation_builder.py` + `fast_writer.py`。
  - contract baseline：Task 3 covers helper + generated JSON。
- Placeholder scan:
  - 无 `TODO` / `TBD` / “稍后实现”。
  - 每个代码步骤都给出具体测试/实现片段和命令。
- Type consistency:
  - 统一使用 `product_anomaly_scope_mode`。
  - 统一使用 `doc_type_split` / `legacy_single_scope`。
  - 统一使用 `section_label` 表达摘要分段。
  - `ProductAnomalySection.section_label` 在 Task 2 引入，并在 Task 3 消费。

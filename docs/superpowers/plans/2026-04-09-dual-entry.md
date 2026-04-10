# Dual Pipeline Entry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an explicit `gb/sk` main entrypoint, keep one shared ETL core, split product whitelists by pipeline, and move quality validation from an Excel sheet into console plus file logs.

**Architecture:** Introduce a pipeline registry that owns the directory, filename-pattern, and whitelist differences between `gb` and `sk`. Keep [`src/etl/costing_etl.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/etl/costing_etl.py) focused on processing one workbook, then add a thin runner plus [`main.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/main.py) to handle argument parsing, file discovery, and quality-log emission. Replace the `数据质量校验` workbook sheet with a structured quality-metric contract that is logged instead of written into Excel.

**Tech Stack:** Python 3.11, `argparse`, `pandas`, `openpyxl`, `pytest`, `ruff`, `conda run -n test`

---

## File Map

- Create: [`main.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/main.py)
- Create: [`src/config/pipelines.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/config/pipelines.py)
- Create: [`src/etl/runner.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/etl/runner.py)
- Modify: [`src/config/settings.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/config/settings.py)
- Modify: [`src/etl/costing_etl.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/etl/costing_etl.py)
- Modify: [`src/analytics/contracts.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/analytics/contracts.py)
- Modify: [`src/analytics/quality.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/analytics/quality.py)
- Modify: [`src/analytics/qty_enricher.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/analytics/qty_enricher.py)
- Modify: [`src/excel/workbook_writer.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/excel/workbook_writer.py)
- Create: [`tests/test_pipeline_config.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/test_pipeline_config.py)
- Create: [`tests/test_runner.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/test_runner.py)
- Create: [`tests/test_main.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/test_main.py)
- Modify: [`tests/test_costing_etl.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/test_costing_etl.py)
- Modify: [`tests/test_pq_analysis_v3.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/test_pq_analysis_v3.py)
- Modify: [`tests/contracts/_workbook_contract_helper.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/contracts/_workbook_contract_helper.py)
- Modify: [`tests/contracts/test_cli_contract.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/contracts/test_cli_contract.py)
- Modify: [`tests/contracts/generate_baselines.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/contracts/generate_baselines.py)
- Modify: [`tests/contracts/baselines/workbook_semantics.json`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/contracts/baselines/workbook_semantics.json)
- Delete: [`tests/test_costing_etl_entrypoint.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/test_costing_etl_entrypoint.py)
- Modify: [`README.md`](D:/03-%20Program/02-%20special/02-%20costing_calculate/README.md)
- Modify: [`AGENTS.md`](D:/03-%20Program/02-%20special/02-%20costing_calculate/AGENTS.md)

## Confirmed `sk` Whitelist

Use this exact `父项物料编码 + 物料名称` order in [`src/config/pipelines.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/config/pipelines.py):

```python
SK_PRODUCT_ORDER: tuple[tuple[str, str], ...] = (
    ('DP.C.P0197AA', '动力线'),
    ('DP.C.P0201AA', '动力线'),
    ('DP.C.P0198AA', '动力线'),
    ('DP.C.P0199AA', '动力线'),
    ('DP.C.P0257AA', '动力线'),
    ('DP.C.P0200AA', '动力线'),
    ('DP.C.P0246AA', '动力抱闸线'),
    ('DP.C.P0252AA', '动力线'),
)
```

### Task 1: Add Pipeline Registry and Inject Product Whitelists

**Files:**
- Create: [`src/config/pipelines.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/config/pipelines.py)
- Create: [`tests/test_pipeline_config.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/test_pipeline_config.py)
- Modify: [`src/config/settings.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/config/settings.py)
- Modify: [`src/etl/costing_etl.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/etl/costing_etl.py)
- Modify: [`tests/test_costing_etl.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/test_costing_etl.py)

- [ ] **Step 1: Write the failing registry tests**

Create [`tests/test_pipeline_config.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/test_pipeline_config.py):

```python
from src.config.pipelines import GB_PIPELINE, PIPELINES, SK_PIPELINE


def test_pipeline_configs_use_expected_directories_and_patterns() -> None:
    assert tuple(PIPELINES) == ('gb', 'sk')
    assert GB_PIPELINE.raw_dir.name == 'gb'
    assert GB_PIPELINE.processed_dir.name == 'gb'
    assert GB_PIPELINE.input_patterns == (
        'GB-*成本计算单.xlsx',
        'GB-* 成本计算单.xlsx',
        'GB-*.xlsx',
    )
    assert SK_PIPELINE.raw_dir.name == 'sk'
    assert SK_PIPELINE.processed_dir.name == 'sk'
    assert SK_PIPELINE.input_patterns == (
        'SK-*成本计算单.xlsx',
        'SK-* 成本计算单.xlsx',
        'SK-*.xlsx',
    )
    assert SK_PIPELINE.product_order[0] == ('DP.C.P0197AA', '动力线')
    assert SK_PIPELINE.product_order[6] == ('DP.C.P0246AA', '动力抱闸线')
```

Append to [`tests/test_costing_etl.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/test_costing_etl.py):

```python
    def test_filter_fact_df_for_analysis_uses_injected_product_order(self) -> None:
        etl = CostingWorkbookETL(
            skip_rows=2,
            product_order=(
                ('DP.C.P0246AA', '动力抱闸线'),
                ('DP.C.P0197AA', '动力线'),
            ),
        )
        fact_df = pd.DataFrame(
            [
                {
                    'period': '2025-01',
                    'product_code': 'DP.C.P0197AA',
                    'product_name': '动力线',
                    'cost_bucket': 'direct_material',
                    'amount': 100,
                    'qty': 10,
                    'price': 10,
                },
                {
                    'period': '2025-01',
                    'product_code': 'DP.C.P0246AA',
                    'product_name': '动力抱闸线',
                    'cost_bucket': 'direct_material',
                    'amount': 220,
                    'qty': 20,
                    'price': 11,
                },
                {
                    'period': '2025-01',
                    'product_code': 'GB_C.D.B0040AA',
                    'product_name': 'BMS-750W驱动器',
                    'cost_bucket': 'direct_material',
                    'amount': 999,
                    'qty': 99,
                    'price': 10.09,
                },
            ]
        )

        result = etl._filter_fact_df_for_analysis(fact_df)

        assert result['product_code'].tolist() == ['DP.C.P0246AA', 'DP.C.P0197AA']
```

- [ ] **Step 2: Run the focused tests**

Run:

```bash
conda run -n test python -m pytest tests/test_pipeline_config.py tests/test_costing_etl.py -q
```

Expected:

```text
ERROR tests/test_pipeline_config.py - ModuleNotFoundError: No module named 'src.config.pipelines'
FAILED tests/test_costing_etl.py::TestCostingWorkbookETL::test_filter_fact_df_for_analysis_uses_injected_product_order
```

- [ ] **Step 3: Implement the registry and injected whitelist**

Create [`src/config/pipelines.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/config/pipelines.py):

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.config.settings import GB_PROCESSED_DIR, GB_RAW_DIR, SK_PROCESSED_DIR, SK_RAW_DIR

GB_PRODUCT_ORDER: tuple[tuple[str, str], ...] = (
    ('GB_C.D.B0048AA', 'BMS-400W驱动器'),
    ('GB_C.D.B0040AA', 'BMS-750W驱动器'),
    ('GB_C.D.B0041AA', 'BMS-1100W驱动器'),
    ('GB_C.D.B0042AA', 'BMS-1700W驱动器'),
    ('GB_C.D.B0043AA', 'BMS-2400W驱动器'),
    ('GB_C.D.B0044AA', 'BMS-3900W驱动器'),
    ('GB_C.D.B0045AA', 'BMS-5900W驱动器'),
    ('GB_C.D.B0046AA', 'BMS-7500W驱动器'),
)

SK_PRODUCT_ORDER: tuple[tuple[str, str], ...] = (
    ('DP.C.P0197AA', '动力线'),
    ('DP.C.P0201AA', '动力线'),
    ('DP.C.P0198AA', '动力线'),
    ('DP.C.P0199AA', '动力线'),
    ('DP.C.P0257AA', '动力线'),
    ('DP.C.P0200AA', '动力线'),
    ('DP.C.P0246AA', '动力抱闸线'),
    ('DP.C.P0252AA', '动力线'),
)


@dataclass(frozen=True)
class PipelineConfig:
    name: str
    raw_dir: Path
    processed_dir: Path
    input_patterns: tuple[str, ...]
    product_order: tuple[tuple[str, str], ...]


GB_PIPELINE = PipelineConfig(
    name='gb',
    raw_dir=GB_RAW_DIR,
    processed_dir=GB_PROCESSED_DIR,
    input_patterns=('GB-*成本计算单.xlsx', 'GB-* 成本计算单.xlsx', 'GB-*.xlsx'),
    product_order=GB_PRODUCT_ORDER,
)

SK_PIPELINE = PipelineConfig(
    name='sk',
    raw_dir=SK_RAW_DIR,
    processed_dir=SK_PROCESSED_DIR,
    input_patterns=('SK-*成本计算单.xlsx', 'SK-* 成本计算单.xlsx', 'SK-*.xlsx'),
    product_order=SK_PRODUCT_ORDER,
)

PIPELINES: dict[str, PipelineConfig] = {'gb': GB_PIPELINE, 'sk': SK_PIPELINE}
```

Update [`src/config/settings.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/config/settings.py):

```python
GB_RAW_DIR = RAW_DIR / 'gb'
SK_RAW_DIR = RAW_DIR / 'sk'

GB_PROCESSED_DIR = PROCESSED_DIR / 'gb'
SK_PROCESSED_DIR = PROCESSED_DIR / 'sk'


def ensure_directories() -> list[Path]:
    dirs = [GB_RAW_DIR, SK_RAW_DIR, GB_PROCESSED_DIR, SK_PROCESSED_DIR, FIELD_DEFS_DIR]
    for directory in dirs:
        directory.mkdir(parents=True, exist_ok=True)
    return dirs
```

Update [`src/etl/costing_etl.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/etl/costing_etl.py):

```python
DEFAULT_ANALYSIS_PRODUCT_ORDER: tuple[tuple[str, str], ...] = (
    ('GB_C.D.B0048AA', 'BMS-400W驱动器'),
    ('GB_C.D.B0040AA', 'BMS-750W驱动器'),
    ('GB_C.D.B0041AA', 'BMS-1100W驱动器'),
    ('GB_C.D.B0042AA', 'BMS-1700W驱动器'),
    ('GB_C.D.B0043AA', 'BMS-2400W驱动器'),
    ('GB_C.D.B0044AA', 'BMS-3900W驱动器'),
    ('GB_C.D.B0045AA', 'BMS-5900W驱动器'),
    ('GB_C.D.B0046AA', 'BMS-7500W驱动器'),
)


class CostingWorkbookETL:
    def __init__(
        self,
        skip_rows: int = 2,
        product_order: tuple[tuple[str, str], ...] = DEFAULT_ANALYSIS_PRODUCT_ORDER,
    ) -> None:
        self.skip_rows = skip_rows
        self.analysis_product_order = tuple((str(code), str(name)) for code, name in product_order)
        self.analysis_product_whitelist = set(self.analysis_product_order)
        self.workbook_writer = CostingWorkbookWriter()
        self.pipeline = CostingEtlPipeline(
            skip_rows=skip_rows,
            fill_columns=self.FILL_COLS,
            detail_columns=self.DETAIL_COLS,
            qty_columns=self.QTY_COLS,
            period_column=COL_PERIOD,
            cost_center_column=COL_COST_CENTER,
            child_material_column=COL_CHILD_MATERIAL,
            cost_item_column=COL_COST_ITEM,
            filled_cost_item_column=COL_FILLED_COST_ITEM,
            order_number_column=COL_ORDER_NO,
            vendor_columns=[COL_VENDOR_CODE, COL_VENDOR_NAME],
            integrated_workshop_name=INTEGRATED_WORKSHOP_NAME,
            logger=logger,
        )
        ensure_directories()
```

Replace the hard-coded whitelist lookups with:

```python
matched_mask = product_pairs.isin(self.analysis_product_whitelist)
order_map = {pair: idx for idx, pair in enumerate(self.analysis_product_order)}
```

- [ ] **Step 4: Re-run the focused tests**

Run:

```bash
conda run -n test python -m pytest tests/test_pipeline_config.py tests/test_costing_etl.py -q
```

Expected:

```text
3 passed
```

- [ ] **Step 5: Commit**

```bash
git add src/config/settings.py src/config/pipelines.py src/etl/costing_etl.py tests/test_pipeline_config.py tests/test_costing_etl.py
git commit -m "feat(config): add gb and sk pipeline registry"
```

### Task 2: Replace the Workbook Quality Sheet with Quality Metrics

**Files:**
- Modify: [`src/analytics/contracts.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/analytics/contracts.py)
- Modify: [`src/analytics/quality.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/analytics/quality.py)
- Modify: [`src/analytics/qty_enricher.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/analytics/qty_enricher.py)
- Modify: [`src/excel/workbook_writer.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/excel/workbook_writer.py)
- Modify: [`src/etl/costing_etl.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/etl/costing_etl.py)
- Modify: [`tests/test_costing_etl.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/test_costing_etl.py)
- Modify: [`tests/test_pq_analysis_v3.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/test_pq_analysis_v3.py)

- [ ] **Step 1: Write the failing tests for quality metrics and 8-sheet workbooks**

In [`tests/test_pq_analysis_v3.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/test_pq_analysis_v3.py), replace the old `quality_sheet` assertions with:

```python
    quality_metrics = {
        (metric.category, metric.metric): metric.value
        for metric in artifacts.quality_metrics
    }

    assert quality_metrics[('行数勾稽', '产品数量统计输出行数')] == '2'
    assert quality_metrics[('分析覆盖率', '可参与分析占比')] == '100.00%'
```

In [`tests/test_costing_etl.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/test_costing_etl.py), change the workbook assertion to:

```python
    expected_sheets = {
        '成本明细',
        '产品数量统计',
        '直接材料_价量比',
        '直接人工_价量比',
        '制造费用_价量比',
        '按工单按产品异常值分析',
        '按产品异常值分析',
        'error_log',
    }
    assert expected_sheets == set(xls.sheet_names)
    assert '数据质量校验' not in xls.sheet_names
    assert etl.last_quality_metrics
    assert etl.last_quality_metrics[0].metric == '成本明细输入行数'
```

- [ ] **Step 2: Run the focused tests**

```bash
conda run -n test python -m pytest tests/test_costing_etl.py tests/test_pq_analysis_v3.py -q
```

Expected:

```text
FAILED tests/test_costing_etl.py::test_process_file_writes_v3_analysis_sheets
AttributeError: 'AnalysisArtifacts' object has no attribute 'quality_metrics'
```

- [ ] **Step 3: Implement `QualityMetric` and stop writing the Excel quality sheet**

Update [`src/analytics/contracts.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/analytics/contracts.py):

```python
@dataclass(frozen=True)
class QualityMetric:
    category: str
    metric: str
    value: str
    description: str


@dataclass
class AnalysisArtifacts:
    fact_df: pd.DataFrame
    qty_sheet_df: pd.DataFrame
    work_order_sheet: FlatSheet
    product_anomaly_sections: list[ProductAnomalySection]
    quality_metrics: tuple[QualityMetric, ...]
    error_log: pd.DataFrame
```

Update [`src/analytics/quality.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/analytics/quality.py):

```python
from src.analytics.contracts import QualityMetric


def build_quality_metrics(
    detail_df: pd.DataFrame,
    qty_input_df: pd.DataFrame,
    qty_sheet_df: pd.DataFrame,
    analysis_df: pd.DataFrame,
    filtered_invalid_qty_count: int,
    filtered_missing_total_amount_count: int,
) -> tuple[QualityMetric, ...]:
    unique_key = qty_sheet_df['_join_key']
    duplicate_count = int(unique_key.duplicated(keep=False).sum())
    dm_amount_null_rate = qty_sheet_df[QTY_DM_AMOUNT].isna().mean() if QTY_DM_AMOUNT in qty_sheet_df.columns else 0.0
    analyzable_rate = (
        analysis_df['是否可参与分析'].eq('是').mean()
        if '是否可参与分析' in analysis_df.columns and not analysis_df.empty
        else 0.0
    )
    return (
        QualityMetric('行数勾稽', '成本明细输入行数', str(len(detail_df)), '原始拆分后的成本明细行数'),
        QualityMetric('行数勾稽', '产品数量统计输入行数', str(len(qty_input_df)), '拆分后的数量页原始行数'),
        QualityMetric('行数勾稽', '产品数量统计输出行数', str(len(qty_sheet_df)), '仅保留完工数量大于 0 且总完工成本非空的工单'),
        QualityMetric('行数勾稽', '工单异常分析输出行数', str(len(analysis_df)), '去重后的工单级分析行数'),
        QualityMetric('行数勾稽', '因完工数量无效被过滤行数', str(filtered_invalid_qty_count), '过滤条件包含完工数量为空、等于 0 或小于 0'),
        QualityMetric('行数勾稽', '因总完工成本为空被过滤行数', str(filtered_missing_total_amount_count), '仅统计完工数量有效但总完工成本为空的工单'),
        QualityMetric('空值率', '直接材料金额缺失率', f'{dm_amount_null_rate:.2%}', '派生金额字段空值率'),
        QualityMetric('唯一性检查', '工单主键重复行数', str(duplicate_count), '键：月份+产品编码+工单编号+工单行'),
        QualityMetric('分析覆盖率', '可参与分析占比', f'{analyzable_rate:.2%}', '仅统计白名单产品且通过基础校验的工单'),
    )
```

Update [`src/analytics/qty_enricher.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/analytics/qty_enricher.py):

```python
from src.analytics.quality import build_quality_metrics

quality_metrics = build_quality_metrics(
    df_detail,
    df_qty,
    qty_sheet_output,
    work_order_sheet.data,
    filtered_invalid_qty_count,
    filtered_missing_total_amount_count,
)
error_log = concat_error_logs(error_frames)

return AnalysisArtifacts(
    fact_df=fact_df,
    qty_sheet_df=qty_sheet_output,
    work_order_sheet=work_order_sheet,
    product_anomaly_sections=build_product_anomaly_sections(product_summary_df),
    quality_metrics=quality_metrics,
    error_log=error_log,
)
```

Update [`src/excel/workbook_writer.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/excel/workbook_writer.py):

```python
class CostingWorkbookWriter:
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
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            self.sheet_writer.write_dataframe_sheet(
                writer,
                '成本明细',
                detail_df,
                numeric_columns=DETAIL_TWO_DECIMAL_COLUMNS,
                freeze_panes='A2',
            )
            self.sheet_writer.write_dataframe_sheet(
                writer,
                '产品数量统计',
                qty_sheet_df,
                numeric_columns=QTY_TWO_DECIMAL_COLUMNS,
                freeze_panes='A2',
            )
            for sheet_name, sections in analysis_tables.items():
                self.sheet_writer.write_analysis_sheet(writer, sheet_name, sections)
            work_order_worksheet = self.sheet_writer.write_flat_sheet(
                writer,
                '按工单按产品异常值分析',
                work_order_sheet,
                freeze_panes='A2',
            )
            self.sheet_writer.apply_work_order_highlights(work_order_worksheet)
            self.sheet_writer.write_product_anomaly_sheet(writer, '按产品异常值分析', product_anomaly_sections)
            error_log.to_excel(writer, sheet_name='error_log', index=False)
```

Update [`src/etl/costing_etl.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/etl/costing_etl.py):

```python
from src.analytics.contracts import FlatSheet, ProductAnomalySection, QualityMetric

class CostingWorkbookETL:
    def __init__(
        self,
        skip_rows: int = 2,
        product_order: tuple[tuple[str, str], ...] = DEFAULT_ANALYSIS_PRODUCT_ORDER,
    ) -> None:
        self.skip_rows = skip_rows
        self.analysis_product_order = tuple((str(code), str(name)) for code, name in product_order)
        self.analysis_product_whitelist = set(self.analysis_product_order)
        self.workbook_writer = CostingWorkbookWriter()
        self.pipeline = CostingEtlPipeline(
            skip_rows=skip_rows,
            fill_columns=self.FILL_COLS,
            detail_columns=self.DETAIL_COLS,
            qty_columns=self.QTY_COLS,
            period_column=COL_PERIOD,
            cost_center_column=COL_COST_CENTER,
            child_material_column=COL_CHILD_MATERIAL,
            cost_item_column=COL_COST_ITEM,
            filled_cost_item_column=COL_FILLED_COST_ITEM,
            order_number_column=COL_ORDER_NO,
            vendor_columns=[COL_VENDOR_CODE, COL_VENDOR_NAME],
            integrated_workshop_name=INTEGRATED_WORKSHOP_NAME,
            logger=logger,
        )
        ensure_directories()
        self.last_quality_metrics: tuple[QualityMetric, ...] = ()
        self.last_error_log_count = 0

    def process_file(self, input_path: Path, output_path: Path) -> bool:
        artifacts = build_report_artifacts(df_detail, df_qty)
        analysis_fact_df = self._filter_fact_df_for_analysis(artifacts.fact_df)
        analysis_tables = render_tables(analysis_fact_df)
        error_log = artifacts.error_log.copy()
        self.last_quality_metrics = artifacts.quality_metrics
        self.last_error_log_count = len(error_log)
        self.workbook_writer.write_workbook(
            output_path,
            detail_df=df_detail,
            qty_sheet_df=artifacts.qty_sheet_df,
            analysis_tables=analysis_tables,
            work_order_sheet=filtered_work_order_sheet,
            product_anomaly_sections=product_anomaly_sections,
            error_log=error_log,
        )
```

- [ ] **Step 4: Re-run the focused tests**

```bash
conda run -n test python -m pytest tests/test_costing_etl.py tests/test_pq_analysis_v3.py -q
```

Expected:

```text
8 passed
```

- [ ] **Step 5: Commit**

```bash
git add src/analytics/contracts.py src/analytics/quality.py src/analytics/qty_enricher.py src/excel/workbook_writer.py src/etl/costing_etl.py tests/test_costing_etl.py tests/test_pq_analysis_v3.py
git commit -m "refactor(analytics): log quality metrics instead of workbook sheet"
```

### Task 3: Add the Unified Runner and `main.py`

**Files:**
- Create: [`src/etl/runner.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/etl/runner.py)
- Create: [`main.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/main.py)
- Create: [`tests/test_runner.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/test_runner.py)
- Create: [`tests/test_main.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/test_main.py)
- Modify: [`tests/contracts/test_cli_contract.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/contracts/test_cli_contract.py)
- Delete: [`tests/test_costing_etl_entrypoint.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/test_costing_etl_entrypoint.py)

- [ ] **Step 1: Write the failing main/runner tests**

Create [`tests/test_main.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/test_main.py):

```python
import pytest

from main import main


def test_main_requires_pipeline_argument() -> None:
    with pytest.raises(SystemExit) as exc_info:
        main([])
    assert exc_info.value.code == 2


def test_main_rejects_invalid_pipeline() -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(['bad'])
    assert exc_info.value.code == 2
```

Create [`tests/test_runner.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/test_runner.py):

```python
from pathlib import Path

from src.analytics.contracts import QualityMetric
from src.config.pipelines import PipelineConfig
from src.etl.runner import find_input_files, run_pipeline


class _FakeGlobDir:
    def __init__(self, responses: list[list[Path]]) -> None:
        self.responses = responses
        self.patterns: list[str] = []

    def glob(self, pattern: str) -> list[Path]:
        self.patterns.append(pattern)
        return self.responses[len(self.patterns) - 1]
```

Append these tests:

```python
def test_find_input_files_preserves_pattern_order_and_deduplicates(tmp_path) -> None:
    same_file = tmp_path / 'SK-成本计算单.xlsx'
    second_file = tmp_path / 'SK- 成本计算单.xlsx'
    third_file = tmp_path / 'SK-anything.xlsx'
    fake_dir = _FakeGlobDir([[same_file, second_file], [second_file], [same_file, third_file]])
    config = PipelineConfig(
        name='sk',
        raw_dir=fake_dir,
        processed_dir=tmp_path,
        input_patterns=('SK-*成本计算单.xlsx', 'SK-* 成本计算单.xlsx', 'SK-*.xlsx'),
        product_order=(('DP.C.P0197AA', '动力线'),),
    )
    assert find_input_files(config) == [same_file, second_file, third_file]
```

```python
def test_run_pipeline_writes_quality_log_and_returns_zero(monkeypatch, capsys, tmp_path) -> None:
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
    )

    class _DummyETL:
        def __init__(self, skip_rows: int, product_order) -> None:
            self.last_quality_metrics = (
                QualityMetric('行数勾稽', '产品数量统计输出行数', '1', '仅保留有效工单'),
                QualityMetric('分析覆盖率', '可参与分析占比', '100.00%', '白名单工单覆盖率'),
            )
            self.last_error_log_count = 0

        def process_file(self, input_path: Path, output_path: Path) -> bool:
            output_path.write_text('ok', encoding='utf-8')
            return True

    monkeypatch.setattr('src.etl.runner.CostingWorkbookETL', _DummyETL)
    exit_code = run_pipeline(config)
    stdout = capsys.readouterr().out
    log_path = processed_dir / 'SK-成本计算单_处理后.log'

    assert exit_code == 0
    assert log_path.exists()
    assert 'pipeline=sk' in stdout
    assert '可参与分析占比=100.00%' in log_path.read_text(encoding='utf-8')
```

- [ ] **Step 2: Run the focused tests**

```bash
conda run -n test python -m pytest tests/test_main.py tests/test_runner.py tests/contracts/test_cli_contract.py -q
```

Expected:

```text
ERROR tests/test_main.py - ModuleNotFoundError: No module named 'main'
ERROR tests/test_runner.py - ModuleNotFoundError: No module named 'src.etl.runner'
```

- [ ] **Step 3: Implement `main.py` and `runner.py`**

Create [`src/etl/runner.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/etl/runner.py):

```python
from __future__ import annotations

import logging
from pathlib import Path

from src.config.pipelines import PipelineConfig
from src.etl.costing_etl import CostingWorkbookETL

logger = logging.getLogger(__name__)


def find_input_files(config: PipelineConfig) -> list[Path]:
    matched: list[Path] = []
    seen: set[Path] = set()
    for pattern in config.input_patterns:
        for path in sorted(config.raw_dir.glob(pattern)):
            if path not in seen:
                seen.add(path)
                matched.append(path)
    return matched


def build_quality_log_text(*, pipeline_name: str, input_path: Path, output_path: Path, error_log_count: int, quality_metrics) -> str:
    lines = [
        f'pipeline={pipeline_name}',
        f'input={input_path}',
        f'output={output_path}',
        f'error_log_count={error_log_count}',
        '',
        '[quality_metrics]',
    ]
    lines.extend(f'{metric.metric}={metric.value} | {metric.description}' for metric in quality_metrics)
    return '\n'.join(lines)


def run_pipeline(config: PipelineConfig) -> int:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    input_files = find_input_files(config)
    if not input_files:
        logger.error('No %s costing file found under %s', config.name.upper(), config.raw_dir)
        return 1

    input_file = input_files[0]
    output_file = config.processed_dir / f'{input_file.stem}_处理后.xlsx'
    log_file = config.processed_dir / f'{input_file.stem}_处理后.log'
    etl = CostingWorkbookETL(skip_rows=2, product_order=config.product_order)

    if not etl.process_file(input_file, output_file):
        logger.error('处理失败: %s', input_file.name)
        return 1

    quality_log = build_quality_log_text(
        pipeline_name=config.name,
        input_path=input_file,
        output_path=output_file,
        error_log_count=etl.last_error_log_count,
        quality_metrics=etl.last_quality_metrics,
    )
    log_file.write_text(quality_log, encoding='utf-8')
    print(quality_log)
    logger.info('处理成功: %s', output_file)
    return 0
```

Create [`main.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/main.py):

```python
from __future__ import annotations

import argparse

from src.config.pipelines import PIPELINES
from src.etl.runner import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='成本核算 ETL 统一入口')
    parser.add_argument('pipeline', choices=sorted(PIPELINES), help='选择要运行的管线: gb 或 sk')
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return run_pipeline(PIPELINES[args.pipeline])


if __name__ == '__main__':
    raise SystemExit(main())
```

Rewrite [`tests/contracts/test_cli_contract.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/contracts/test_cli_contract.py) so it targets `find_input_files()` instead of the removed `costing_etl.main()`.

- [ ] **Step 4: Re-run the focused tests**

```bash
conda run -n test python -m pytest tests/test_main.py tests/test_runner.py tests/contracts/test_cli_contract.py -q
```

Expected:

```text
6 passed
```

- [ ] **Step 5: Commit**

```bash
git add main.py src/etl/runner.py tests/test_main.py tests/test_runner.py tests/contracts/test_cli_contract.py
git rm tests/test_costing_etl_entrypoint.py
git commit -m "feat(cli): add unified gb and sk main entrypoint"
```

### Task 4: Refresh Workbook Contracts and Baselines

**Files:**
- Modify: [`tests/contracts/_workbook_contract_helper.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/contracts/_workbook_contract_helper.py)
- Modify: [`tests/contracts/generate_baselines.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/contracts/generate_baselines.py)
- Modify: [`tests/contracts/baselines/workbook_semantics.json`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/contracts/baselines/workbook_semantics.json)

- [ ] **Step 1: Update helper-side workbook expectations**

In [`tests/contracts/_workbook_contract_helper.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/contracts/_workbook_contract_helper.py), set:

```python
DEFAULT_SHEETS = (
    '成本明细',
    '产品数量统计',
    '直接材料_价量比',
    '直接人工_价量比',
    '制造费用_价量比',
    '按工单按产品异常值分析',
    '按产品异常值分析',
    'error_log',
)
```

Update mocked artifacts to use:

```python
from src.analytics.contracts import AnalysisArtifacts, FlatSheet, ProductAnomalySection, QualityMetric

        quality_metrics=(
            QualityMetric('行数勾稽', '产品数量统计输出行数', '1', '测试'),
        ),
```

- [ ] **Step 2: Run contract tests before baseline regeneration**

```bash
conda run -n test python -m pytest tests/contracts -q
```

Expected:

```text
FAILED tests/contracts/test_workbook_contract.py::test_default_workbook_semantics_match_baseline
FAILED tests/contracts/test_workbook_contract.py::test_highlight_workbook_semantics_match_baseline
```

- [ ] **Step 3: Regenerate the workbook baseline**

```bash
conda run -n test python -m tests.contracts.generate_baselines
```

Confirm [`tests/contracts/baselines/workbook_semantics.json`](D:/03-%20Program/02-%20special/02-%20costing_calculate/tests/contracts/baselines/workbook_semantics.json) now has exactly these sheet names in order:

```json
[
  "成本明细",
  "产品数量统计",
  "直接材料_价量比",
  "直接人工_价量比",
  "制造费用_价量比",
  "按工单按产品异常值分析",
  "按产品异常值分析",
  "error_log"
]
```

- [ ] **Step 4: Re-run contract tests**

```bash
conda run -n test python -m pytest tests/contracts -q
```

Expected:

```text
6 passed
```

- [ ] **Step 5: Commit**

```bash
git add tests/contracts/_workbook_contract_helper.py tests/contracts/generate_baselines.py tests/contracts/baselines/workbook_semantics.json
git commit -m "test(contracts): refresh workbook semantics for quality logs"
```

### Task 5: Update Docs and Run Final Verification

**Files:**
- Modify: [`README.md`](D:/03-%20Program/02-%20special/02-%20costing_calculate/README.md)
- Modify: [`AGENTS.md`](D:/03-%20Program/02-%20special/02-%20costing_calculate/AGENTS.md)

- [ ] **Step 1: Update `README.md` usage and outputs**

Replace the old usage block in [`README.md`](D:/03-%20Program/02-%20special/02-%20costing_calculate/README.md) with:

```md
## 使用
```bash
# GB 管线
python main.py gb

# SK 管线
python main.py sk
```

## 输出说明
- Excel 默认输出 8 张 Sheet，不再包含 `数据质量校验`
- 每次处理会在对应 `data/processed/<pipeline>/` 目录生成同名 `.log` 文件
- `error_log` 仍保留在 Excel 中
```

- [ ] **Step 2: Update `AGENTS.md`**

Add or replace these bullets in [`AGENTS.md`](D:/03-%20Program/02-%20special/02-%20costing_calculate/AGENTS.md):

```md
- `data/raw/{gb,sk}/`: 原始 Excel 输入
- `data/processed/{gb,sk}/`: 处理后输出
- `python main.py gb`: 执行 GB 管线
- `python main.py sk`: 执行 SK 管线
- 输出工作簿默认包含 8 张 Sheet，`数据质量校验` 改为控制台摘要 + `.log` 文件
```

- [ ] **Step 3: Scan for stale references**

```bash
rg -n "shukong|python -m src\\.etl\\.costing_etl|数据质量校验" README.md AGENTS.md src tests
```

Expected:

```text
README.md: contains only the new `python main.py gb|sk` usage and `.log` output note
AGENTS.md: contains only `data/raw/{gb,sk}` and `data/processed/{gb,sk}` references
```

- [ ] **Step 4: Run full verification**

```bash
conda run -n test python -m pytest tests -q
conda run -n test python -m ruff check src tests main.py
conda run -n test python -m ruff format src tests main.py --check
```

Expected:

```text
pytest: all tests passed
ruff check: All checks passed!
ruff format --check: 0 files would be reformatted
```

- [ ] **Step 5: Commit**

```bash
git add README.md AGENTS.md
git commit -m "docs: update gb and sk entrypoint usage"
```

## Final Handoff Checklist

Use this exact checklist in the implementation close-out:

```text
1. `main.py` is the only supported entrypoint and requires `gb` or `sk`.
2. `gb` and `sk` use different product whitelists through `PipelineConfig`.
3. Excel output no longer includes `数据质量校验`; quality metrics now live in console output and `.log` files.
4. Contract baselines, README, and AGENTS all match the new behavior.
5. Full `pytest`, `ruff check`, and `ruff format --check` passed under `conda run -n test`.
```

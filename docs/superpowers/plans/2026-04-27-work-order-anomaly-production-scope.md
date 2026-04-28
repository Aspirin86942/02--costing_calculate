# Work Order Anomaly Production Scope Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `按工单按产品异常值分析` 中新增 `生产类型` 列，并把异常值统计从“按产品”改为“按产品 + 生产类型”分池计算，同时保留未归类工单的展示与审计信息。

**Architecture:** 复用 `src/analytics/table_rendering.py` 中已有的单据类型业务映射，先把 `doc_type` 归一成 `正常生产 / 返工生产 / 未归类`，再由 `src/analytics/anomaly.py` 负责把这个分类写入工单异常页，并将 `Modified Z-score` 的分组键扩展为 `product_code + product_name + production_scope`。导出层仍沿用现有 flat sheet 流程，只更新工单异常页列顺序、示例输入和 workbook baseline。

**Tech Stack:** Python 3.11+, pandas, numpy, polars, openpyxl/xlsxwriter, pytest, Ruff

---

## File Map

- Modify: `D:/Program_python/02--costing_calculate/src/analytics/table_rendering.py`
  责任：把单据类型到生产类型的映射整理成可复用 helper，供产品摘要页与工单异常页共用。
- Modify: `D:/Program_python/02--costing_calculate/src/analytics/anomaly.py`
  责任：新增 `生产类型` 展示列，调整 `是否可参与分析` 的口径，并把异常值分组改成“按产品 + 生产类型”。
- Modify: `D:/Program_python/02--costing_calculate/tests/test_pq_analysis.py`
  责任：冻结单据类型到生产类型映射 helper 的返回值，避免产品页和工单页口径漂移。
- Modify: `D:/Program_python/02--costing_calculate/tests/test_pq_analysis_v3.py`
  责任：冻结工单异常页新增 `生产类型`、未知单据类型不可入池、按生产类型分池算分、样本不足只保留 `log` 的行为。
- Modify: `D:/Program_python/02--costing_calculate/tests/test_costing_etl.py`
  责任：冻结 workbook 导出后的工单异常页列顺序、单元格值和冻结/筛选行为。
- Modify: `D:/Program_python/02--costing_calculate/tests/contracts/baselines/workbook_semantics.json`
  责任：更新工单异常页的列契约与 auto filter 终点。

## Implementation Notes

- 测试、lint、格式化命令默认用 `conda run -n test ...`，遵守仓库本地约束。
- 不新建 sheet，不改 `按产品异常值分析` 的布局，不把未知单据类型强行并入 `正常生产` 或 `返工生产`。
- 建议在 `src/analytics/table_rendering.py` 中把私有 `_map_doc_type_to_scope_label` 提升为可复用 helper，例如 `map_doc_type_to_scope_label()`，并补一个 `DOC_TYPE_UNKNOWN_LABEL = '未归类'` 常量。
- `src/analytics/anomaly.py` 中内部计算可使用英文列名 `production_scope`，最终展示层通过 `rename_map` 映射到 `生产类型`，减少直接用中文内部列名带来的维护噪音。
- `是否可参与分析` 的新定义必须同时满足：
  - 完工数量有效
  - 总单位完工成本有效
  - `production_scope` 属于 `正常生产` 或 `返工生产`
- `未归类` 行需要保留在输出中，但所有 `Modified Z-score_*`、异常标记、`异常等级`、`异常主要来源` 应为空；`复核原因` 追加“单据类型未归类，不参与正常生产/返工生产异常池”。
- 分池后样本可能不足 3 条；这类行必须保留 `log_*`，但分数和标记继续留空。

### Task 1: Extract a Reusable Production Scope Helper

**Files:**
- Modify: `D:/Program_python/02--costing_calculate/src/analytics/table_rendering.py`
- Test: `D:/Program_python/02--costing_calculate/tests/test_pq_analysis.py`

- [ ] **Step 1: Write the failing helper test**

```python
# tests/test_pq_analysis.py
from src.analytics.table_rendering import (
    DOC_TYPE_NORMAL_LABEL,
    DOC_TYPE_REWORK_LABEL,
    DOC_TYPE_UNKNOWN_LABEL,
    map_doc_type_to_scope_label,
)


def test_map_doc_type_to_scope_label_returns_known_and_unknown_labels() -> None:
    assert map_doc_type_to_scope_label('汇报入库-普通生产') == DOC_TYPE_NORMAL_LABEL
    assert map_doc_type_to_scope_label('直接入库-普通生产') == DOC_TYPE_NORMAL_LABEL
    assert map_doc_type_to_scope_label('汇报入库-返工生产') == DOC_TYPE_REWORK_LABEL
    assert map_doc_type_to_scope_label('普通委外订单') == DOC_TYPE_UNKNOWN_LABEL
    assert map_doc_type_to_scope_label('  ') == DOC_TYPE_UNKNOWN_LABEL
    assert map_doc_type_to_scope_label(None) == DOC_TYPE_UNKNOWN_LABEL
```

- [ ] **Step 2: Run the targeted test to verify it fails**

Run:

```bash
conda run -n test python -m pytest tests/test_pq_analysis.py::test_map_doc_type_to_scope_label_returns_known_and_unknown_labels -q
```

Expected:

```text
E   ImportError: cannot import name 'DOC_TYPE_UNKNOWN_LABEL'
```

- [ ] **Step 3: Implement the shared helper in `table_rendering.py`**

```python
# src/analytics/table_rendering.py
DOC_TYPE_NORMAL_LABEL = '正常生产'
DOC_TYPE_REWORK_LABEL = '返工生产'
DOC_TYPE_UNKNOWN_LABEL = '未归类'
DOC_TYPE_TO_SECTION_LABEL: dict[str, str] = {
    '汇报入库-普通生产': DOC_TYPE_NORMAL_LABEL,
    '直接入库-普通生产': DOC_TYPE_NORMAL_LABEL,
    '汇报入库-返工生产': DOC_TYPE_REWORK_LABEL,
}


def map_doc_type_to_scope_label(doc_type: object) -> str:
    if doc_type is None or pd.isna(doc_type):
        return DOC_TYPE_UNKNOWN_LABEL
    normalized_doc_type = str(doc_type).strip()
    if not normalized_doc_type:
        return DOC_TYPE_UNKNOWN_LABEL
    return DOC_TYPE_TO_SECTION_LABEL.get(normalized_doc_type, DOC_TYPE_UNKNOWN_LABEL)
```

```python
# src/analytics/table_rendering.py
scope_labels = product_frame['doc_type'].map(map_doc_type_to_scope_label)
for section_label in DOC_TYPE_SPLIT_SCOPE_LABELS[1:]:
    scoped_frame = product_frame.loc[scope_labels == section_label]
```

- [ ] **Step 4: Run the focused mapping and product-anomaly tests**

Run:

```bash
conda run -n test python -m pytest tests/test_pq_analysis.py -q
```

Expected:

```text
... passed
```

- [ ] **Step 5: Commit the helper extraction**

```bash
git add tests/test_pq_analysis.py src/analytics/table_rendering.py
git commit -m "refactor(analytics): share production scope mapping"
```

### Task 2: Add `生产类型` to the Work-Order Anomaly Sheet

**Files:**
- Modify: `D:/Program_python/02--costing_calculate/src/analytics/anomaly.py`
- Test: `D:/Program_python/02--costing_calculate/tests/test_pq_analysis_v3.py`

- [ ] **Step 1: Write failing tests for display column and unknown-scope gating**

```python
# tests/test_pq_analysis_v3.py
def test_build_report_artifacts_work_order_sheet_adds_production_scope_column() -> None:
    df_detail = pd.DataFrame(
        [
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-N1',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '直接材料',
                '本期完工金额': 10,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-U1',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '直接材料',
                '本期完工金额': 5,
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
                '工单编号': 'WO-N1',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '汇报入库-普通生产',
                '本期完工数量': 1,
                '本期完工金额': 10,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-U1',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '普通委外订单',
                '本期完工数量': 1,
                '本期完工金额': 5,
            },
        ]
    )

    artifacts = build_report_artifacts(df_detail, df_qty)
    anomaly_df = artifacts.work_order_sheet.data

    assert anomaly_df.columns.tolist()[7:10] == ['生产类型', '基本单位', '本期完工数量']
    assert anomaly_df.loc[anomaly_df['工单编号'] == 'WO-N1', '生产类型'].iloc[0] == '正常生产'
    assert anomaly_df.loc[anomaly_df['工单编号'] == 'WO-U1', '生产类型'].iloc[0] == '未归类'
```

```python
# tests/test_pq_analysis_v3.py
def test_build_report_artifacts_marks_unknown_doc_type_as_not_analyzable() -> None:
    df_detail = pd.DataFrame(
        [
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-U1',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '直接材料',
                '本期完工金额': 9,
            }
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
                '工单编号': 'WO-U1',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '未知类型',
                '本期完工数量': 1,
                '本期完工金额': 9,
            }
        ]
    )

    artifacts = build_report_artifacts(df_detail, df_qty)
    row = artifacts.work_order_sheet.data.iloc[0]

    assert row['生产类型'] == '未归类'
    assert row['是否可参与分析'] == '否'
    assert row['异常等级'] == ''
    assert row['异常主要来源'] == ''
    assert row['Modified Z-score_总单位完工成本'] is None
    assert '单据类型未归类' in row['复核原因']
```

- [ ] **Step 2: Run the targeted tests to verify they fail**

Run:

```bash
conda run -n test python -m pytest tests/test_pq_analysis_v3.py::test_build_report_artifacts_work_order_sheet_adds_production_scope_column tests/test_pq_analysis_v3.py::test_build_report_artifacts_marks_unknown_doc_type_as_not_analyzable -q
```

Expected:

```text
E   KeyError: '生产类型'
```

- [ ] **Step 3: Implement the new display column and analyzable gating**

```python
# src/analytics/anomaly.py
from src.analytics.table_rendering import (
    DOC_TYPE_NORMAL_LABEL,
    DOC_TYPE_REWORK_LABEL,
    DOC_TYPE_UNKNOWN_LABEL,
    map_doc_type_to_scope_label,
)

ANALYZABLE_PRODUCTION_SCOPE_LABELS = {
    DOC_TYPE_NORMAL_LABEL,
    DOC_TYPE_REWORK_LABEL,
}
```

```python
# src/analytics/anomaly.py
WORK_ORDER_OUTPUT_COLUMNS = [
    '月份',
    '成本中心',
    '产品编码',
    '产品名称',
    '规格型号',
    '工单编号',
    '工单行',
    '生产类型',
    '基本单位',
    '本期完工数量',
    # ...
]

WORK_ORDER_COLUMN_TYPES = {
    '月份': 'text',
    '成本中心': 'text',
    '产品编码': 'text',
    '产品名称': 'text',
    '规格型号': 'text',
    '工单编号': 'text',
    '工单行': 'text',
    '生产类型': 'text',
    '基本单位': 'text',
    # ...
}
```

```python
# src/analytics/anomaly.py
anomaly_df['production_scope'] = anomaly_df['doc_type'].map(map_doc_type_to_scope_label)

base_can_analyze = anomaly_df['completed_qty'].map(
    lambda value: value is not None and value > ZERO
) & anomaly_df['total_unit_cost'].map(lambda value: value is not None and value > ZERO)

unknown_scope_mask = ~anomaly_df['production_scope'].isin(ANALYZABLE_PRODUCTION_SCOPE_LABELS)
reason_series = append_reason(
    reason_series,
    unknown_scope_mask,
    '单据类型未归类，不参与正常生产/返工生产异常池',
)
anomaly_df['can_analyze'] = base_can_analyze & ~unknown_scope_mask
```

```python
# src/analytics/anomaly.py
overall_level.loc[~anomaly_df['can_analyze']] = ''
highest_source.loc[~anomaly_df['can_analyze']] = ''

rename_map = {
    # ...
    'production_scope': '生产类型',
}
```

- [ ] **Step 4: Run the focused work-order sheet tests**

Run:

```bash
conda run -n test python -m pytest tests/test_pq_analysis_v3.py::test_build_report_artifacts_work_order_sheet_adds_production_scope_column tests/test_pq_analysis_v3.py::test_build_report_artifacts_marks_unknown_doc_type_as_not_analyzable -q
```

Expected:

```text
.. passed
```

- [ ] **Step 5: Commit the output-column change**

```bash
git add tests/test_pq_analysis_v3.py src/analytics/anomaly.py
git commit -m "feat(analytics): label work order production scope"
```

### Task 3: Split Modified Z-Scores by Production Scope

**Files:**
- Modify: `D:/Program_python/02--costing_calculate/src/analytics/anomaly.py`
- Test: `D:/Program_python/02--costing_calculate/tests/test_pq_analysis_v3.py`

- [ ] **Step 1: Write failing tests for split pools and insufficient samples**

```python
# tests/test_pq_analysis_v3.py
def test_build_report_artifacts_scores_normal_and_rework_in_separate_pools() -> None:
    df_detail = pd.DataFrame(
        [
            {'月份': '2025年01期', '成本中心名称': '中心A', '产品编码': 'P001', '产品名称': '产品A', '规格型号': 'S-01', '工单编号': 'WO-N1', '工单行号': 1, '基本单位': 'PCS', '成本项目名称': '直接材料', '本期完工金额': 10},
            {'月份': '2025年01期', '成本中心名称': '中心A', '产品编码': 'P001', '产品名称': '产品A', '规格型号': 'S-01', '工单编号': 'WO-N2', '工单行号': 1, '基本单位': 'PCS', '成本项目名称': '直接材料', '本期完工金额': 10},
            {'月份': '2025年01期', '成本中心名称': '中心A', '产品编码': 'P001', '产品名称': '产品A', '规格型号': 'S-01', '工单编号': 'WO-N3', '工单行号': 1, '基本单位': 'PCS', '成本项目名称': '直接材料', '本期完工金额': 100},
            {'月份': '2025年01期', '成本中心名称': '中心A', '产品编码': 'P001', '产品名称': '产品A', '规格型号': 'S-01', '工单编号': 'WO-R1', '工单行号': 1, '基本单位': 'PCS', '成本项目名称': '直接材料', '本期完工金额': 50},
            {'月份': '2025年01期', '成本中心名称': '中心A', '产品编码': 'P001', '产品名称': '产品A', '规格型号': 'S-01', '工单编号': 'WO-R2', '工单行号': 1, '基本单位': 'PCS', '成本项目名称': '直接材料', '本期完工金额': 50},
            {'月份': '2025年01期', '成本中心名称': '中心A', '产品编码': 'P001', '产品名称': '产品A', '规格型号': 'S-01', '工单编号': 'WO-R3', '工单行号': 1, '基本单位': 'PCS', '成本项目名称': '直接材料', '本期完工金额': 200},
        ]
    )
    df_qty = pd.DataFrame(
        [
            {'月份': '2025年01期', '成本中心名称': '中心A', '产品编码': 'P001', '产品名称': '产品A', '规格型号': 'S-01', '工单编号': 'WO-N1', '工单行号': 1, '基本单位': 'PCS', '单据类型': '汇报入库-普通生产', '本期完工数量': 1, '本期完工金额': 10},
            {'月份': '2025年01期', '成本中心名称': '中心A', '产品编码': 'P001', '产品名称': '产品A', '规格型号': 'S-01', '工单编号': 'WO-N2', '工单行号': 1, '基本单位': 'PCS', '单据类型': '直接入库-普通生产', '本期完工数量': 1, '本期完工金额': 10},
            {'月份': '2025年01期', '成本中心名称': '中心A', '产品编码': 'P001', '产品名称': '产品A', '规格型号': 'S-01', '工单编号': 'WO-N3', '工单行号': 1, '基本单位': 'PCS', '单据类型': '汇报入库-普通生产', '本期完工数量': 1, '本期完工金额': 100},
            {'月份': '2025年01期', '成本中心名称': '中心A', '产品编码': 'P001', '产品名称': '产品A', '规格型号': 'S-01', '工单编号': 'WO-R1', '工单行号': 1, '基本单位': 'PCS', '单据类型': '汇报入库-返工生产', '本期完工数量': 1, '本期完工金额': 50},
            {'月份': '2025年01期', '成本中心名称': '中心A', '产品编码': 'P001', '产品名称': '产品A', '规格型号': 'S-01', '工单编号': 'WO-R2', '工单行号': 1, '基本单位': 'PCS', '单据类型': '汇报入库-返工生产', '本期完工数量': 1, '本期完工金额': 50},
            {'月份': '2025年01期', '成本中心名称': '中心A', '产品编码': 'P001', '产品名称': '产品A', '规格型号': 'S-01', '工单编号': 'WO-R3', '工单行号': 1, '基本单位': 'PCS', '单据类型': '汇报入库-返工生产', '本期完工数量': 1, '本期完工金额': 200},
        ]
    )

    artifacts = build_report_artifacts(df_detail, df_qty)
    anomaly_df = artifacts.work_order_sheet.data

    normal_outlier = anomaly_df.loc[anomaly_df['工单编号'] == 'WO-N3'].iloc[0]
    rework_outlier = anomaly_df.loc[anomaly_df['工单编号'] == 'WO-R3'].iloc[0]

    assert normal_outlier['生产类型'] == '正常生产'
    assert rework_outlier['生产类型'] == '返工生产'
    assert normal_outlier['Modified Z-score_总单位完工成本'] is not None
    assert rework_outlier['Modified Z-score_总单位完工成本'] is not None
    assert normal_outlier['总成本异常标记'] in {'关注', '高度可疑'}
    assert rework_outlier['总成本异常标记'] in {'关注', '高度可疑'}
```

```python
# tests/test_pq_analysis_v3.py
def test_build_report_artifacts_keeps_log_but_not_score_when_scope_has_fewer_than_three_rows() -> None:
    df_detail = pd.DataFrame(
        [
            {'月份': '2025年01期', '成本中心名称': '中心A', '产品编码': 'P001', '产品名称': '产品A', '规格型号': 'S-01', '工单编号': 'WO-N1', '工单行号': 1, '基本单位': 'PCS', '成本项目名称': '直接材料', '本期完工金额': 10},
            {'月份': '2025年01期', '成本中心名称': '中心A', '产品编码': 'P001', '产品名称': '产品A', '规格型号': 'S-01', '工单编号': 'WO-N2', '工单行号': 1, '基本单位': 'PCS', '成本项目名称': '直接材料', '本期完工金额': 10},
            {'月份': '2025年01期', '成本中心名称': '中心A', '产品编码': 'P001', '产品名称': '产品A', '规格型号': 'S-01', '工单编号': 'WO-R1', '工单行号': 1, '基本单位': 'PCS', '成本项目名称': '直接材料', '本期完工金额': 50},
            {'月份': '2025年01期', '成本中心名称': '中心A', '产品编码': 'P001', '产品名称': '产品A', '规格型号': 'S-01', '工单编号': 'WO-R2', '工单行号': 1, '基本单位': 'PCS', '成本项目名称': '直接材料', '本期完工金额': 200},
        ]
    )
    df_qty = pd.DataFrame(
        [
            {'月份': '2025年01期', '成本中心名称': '中心A', '产品编码': 'P001', '产品名称': '产品A', '规格型号': 'S-01', '工单编号': 'WO-N1', '工单行号': 1, '基本单位': 'PCS', '单据类型': '汇报入库-普通生产', '本期完工数量': 1, '本期完工金额': 10},
            {'月份': '2025年01期', '成本中心名称': '中心A', '产品编码': 'P001', '产品名称': '产品A', '规格型号': 'S-01', '工单编号': 'WO-N2', '工单行号': 1, '基本单位': 'PCS', '单据类型': '直接入库-普通生产', '本期完工数量': 1, '本期完工金额': 10},
            {'月份': '2025年01期', '成本中心名称': '中心A', '产品编码': 'P001', '产品名称': '产品A', '规格型号': 'S-01', '工单编号': 'WO-R1', '工单行号': 1, '基本单位': 'PCS', '单据类型': '汇报入库-返工生产', '本期完工数量': 1, '本期完工金额': 50},
            {'月份': '2025年01期', '成本中心名称': '中心A', '产品编码': 'P001', '产品名称': '产品A', '规格型号': 'S-01', '工单编号': 'WO-R2', '工单行号': 1, '基本单位': 'PCS', '单据类型': '汇报入库-返工生产', '本期完工数量': 1, '本期完工金额': 200},
        ]
    )

    artifacts = build_report_artifacts(df_detail, df_qty)
    rework_row = artifacts.work_order_sheet.data.loc[lambda df: df['工单编号'] == 'WO-R2'].iloc[0]

    assert rework_row['生产类型'] == '返工生产'
    assert rework_row['log_总单位完工成本'] is not None
    assert rework_row['Modified Z-score_总单位完工成本'] is None
    assert rework_row['总成本异常标记'] == ''
```

- [ ] **Step 2: Run the targeted split-pool tests to verify they fail**

Run:

```bash
conda run -n test python -m pytest tests/test_pq_analysis_v3.py::test_build_report_artifacts_scores_normal_and_rework_in_separate_pools tests/test_pq_analysis_v3.py::test_build_report_artifacts_keeps_log_but_not_score_when_scope_has_fewer_than_three_rows -q
```

Expected:

```text
E   AssertionError: expected separate-scope score behavior
```

- [ ] **Step 3: Change the anomaly grouping key to `product + production_scope`**

```python
# src/analytics/anomaly.py
for _, group_index in anomaly_df.groupby(
    ['product_code', 'product_name', 'production_scope'],
    sort=False,
).groups.items():
    scope_label = anomaly_df.loc[group_index, 'production_scope'].iloc[0]
    if scope_label == DOC_TYPE_UNKNOWN_LABEL:
        continue

    metric_series = anomaly_df.loc[group_index, metric_key]
    qty_series = anomaly_df.loc[group_index, 'completed_qty']
    valid_mask = metric_series.map(lambda value: value is not None and value > ZERO) & qty_series.map(
        lambda value: value is not None and value > ZERO
    )
    if not valid_mask.any():
        continue

    valid_values = metric_series.loc[valid_mask].map(lambda value: math.log(float(value)))
    valid_weights = qty_series.loc[valid_mask].map(float)
    anomaly_df.loc[valid_values.index, log_column] = valid_values

    if len(valid_values) < 3:
        continue
```

```python
# src/analytics/anomaly.py
for metric_key, _display_name, flag_column, source_label in ANOMALY_METRICS:
    score_column = f'modified_z_{metric_key}'
    flag_series = anomaly_df[flag_column]
    current_rank = flag_series.map({'正常': 0, '关注': 1, '高度可疑': 2}).fillna(-1).astype(int)
    score_abs = anomaly_df[score_column].map(
        lambda value: abs(value) if value is not None and not pd.isna(value) else -1.0
    )
    # 保持既有“取最严重、再取绝对分值最大”的规则不变
```

- [ ] **Step 4: Run the work-order anomaly test slice**

Run:

```bash
conda run -n test python -m pytest tests/test_pq_analysis_v3.py -q
```

Expected:

```text
... passed
```

- [ ] **Step 5: Commit the split-pool scoring change**

```bash
git add tests/test_pq_analysis_v3.py src/analytics/anomaly.py
git commit -m "feat(analytics): split work order anomaly pools by production scope"
```

### Task 4: Update Workbook Semantics and Final Verification

**Files:**
- Modify: `D:/Program_python/02--costing_calculate/tests/test_costing_etl.py`
- Modify: `D:/Program_python/02--costing_calculate/tests/contracts/baselines/workbook_semantics.json`

- [ ] **Step 1: Write failing workbook assertions for the new column order**

```python
# tests/test_costing_etl.py
def test_lightweight_export_writes_workbook_skeleton(tmp_path) -> None:
    # 在现有 df_qty 示例行中补上：
    # '单据类型': '汇报入库-普通生产'
    ...
    ws_work_order = wb['按工单按产品异常值分析']
    work_order_headers = _build_header_map(ws_work_order)

    assert ws_work_order['J2'].value == 10
    assert work_order_headers['生产类型'] == 8
    assert ws_work_order.cell(2, work_order_headers['生产类型']).value == '正常生产'
```

```json
// tests/contracts/baselines/workbook_semantics.json
"按工单按产品异常值分析": {
  "kind": "flat",
  "freeze_panes": "A2",
  "auto_filter": "A1:BI2",
  "columns": [
    "月份",
    "成本中心",
    "产品编码",
    "产品名称",
    "规格型号",
    "工单编号",
    "工单行",
    "生产类型",
    "基本单位"
  ]
}
```

- [ ] **Step 2: Run the targeted workbook test to verify it fails**

Run:

```bash
conda run -n test python -m pytest tests/test_costing_etl.py::test_lightweight_export_writes_workbook_skeleton -q
```

Expected:

```text
E   AssertionError: '生产类型' not found in header map
```

- [ ] **Step 3: Apply the workbook test fixture and baseline updates**

```python
# tests/test_costing_etl.py
df_qty = pd.DataFrame(
    [
        {
            '月份': '2025年01期',
            '成本中心名称': '中心A',
            '产品编码': 'GB_C.D.B0040AA',
            '产品名称': 'BMS-750W驱动器',
            '规格型号': 'S-01',
            '工单编号': 'WO-001',
            '工单行号': 1,
            '基本单位': 'PCS',
            '单据类型': '汇报入库-普通生产',
            '本期完工数量': 10,
            '本期完工金额': 165,
        }
    ]
)
```

```json
// tests/contracts/baselines/workbook_semantics.json
"columns": [
  "月份",
  "成本中心",
  "产品编码",
  "产品名称",
  "规格型号",
  "工单编号",
  "工单行",
  "生产类型",
  "基本单位",
  "本期完工数量"
]
```

- [ ] **Step 4: Run export tests, contract checks, lint, and the targeted full suite**

Run:

```bash
conda run -n test python -m pytest tests/test_pq_analysis.py tests/test_pq_analysis_v3.py tests/test_costing_etl.py tests/contracts -q
```

Expected:

```text
... passed
```

Run:

```bash
conda run -n test python -m ruff check src tests
```

Expected:

```text
All checks passed!
```

If `ruff check` reports formatting drift, run:

```bash
conda run -n test python -m ruff format src tests
conda run -n test python -m pytest tests/test_pq_analysis.py tests/test_pq_analysis_v3.py tests/test_costing_etl.py tests/contracts -q
conda run -n test python -m ruff check src tests
```

- [ ] **Step 5: Commit the workbook contract update**

```bash
git add tests/test_costing_etl.py tests/contracts/baselines/workbook_semantics.json
git commit -m "test(export): update work order anomaly workbook contract"
```

## Self-Review

- Spec coverage:
  - `生产类型` 列：Task 2
  - `按产品 + 生产类型` 分池：Task 3
  - `未归类` 保留展示但不可分析：Task 2
  - 分池后样本不足只保留 `log`：Task 3
  - workbook 列契约更新：Task 4
- Placeholder scan:
  - 已避免占位词和“只描述不示例”的空步骤
  - 每个变更步骤都给出了具体文件、代码片段和命令
- Type consistency:
  - 统一使用 `production_scope` 作为内部列名
  - 统一使用 `map_doc_type_to_scope_label()` 作为业务映射入口
  - 展示层统一输出中文列名 `生产类型`

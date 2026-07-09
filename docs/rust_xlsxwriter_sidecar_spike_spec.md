# Rust 高速 Excel 写出 Sidecar Spike Spec

## 1. 背景

当前项目路径：

```text
D:\python_program\02--costing_calculate
```

本项目是用于成本核算工作簿的 Python ETL 工具。当前主要流程为：

```text
读取 Excel -> 标准化 -> 拆分事实表 -> 分析与质量校验 -> 构建 SheetModel -> 写出 xlsx
```

历史 GB 管线 4-sheet 输出下的实际运行结果为：

```text
python main.py gb

total_seconds ≈ 12.403
payload/calculation_seconds ≈ 2.842
export_seconds ≈ 9.461
export 占总耗时约 76.3%
```

现状判断：

```text
主要瓶颈在 Excel 写出，而不是读取、清洗或分析。
```

但在本次设计讨论中，业务契约已经变更：

```text
成本分析产品维度 sheet 确认不再需要。
默认输出 workbook 从 4 张 sheet 改为 3 张 sheet。
```

因此，历史 4-sheet benchmark 只能作为背景，不能继续作为 Rust spike 的判定 baseline。Rust spike 必须基于新的 3-sheet Python baseline 重新比较。

---

## 2. 目标

目标分两阶段完成。

### 2.1 Phase 0：先修正 Python 默认 workbook 契约

把生产 Python 默认输出改为 3 张 sheet：

```text
1. 成本计算单总表
2. 成本计算单数量聚合维度
3. 成本分析工单维度
```

不再默认输出：

```text
成本分析产品维度
```

Phase 0 完成后，必须用真实 GB 输入文件重新跑 Python 3-sheet benchmark，得到新的：

```text
python_3sheet_total_seconds
python_3sheet_export_seconds
python_3sheet_payload_seconds
```

### 2.2 Phase 1：再做 Rust writer sidecar spike

验证：

> 使用 Rust `rust_xlsxwriter` 作为 sidecar CLI，是否能在保持 3-sheet workbook 核心契约的前提下，显著降低当前 Python xlsxwriter 写出阶段耗时。

Rust spike 的目标不是一次性替换生产路径，而是用真实数据验证写出层是否值得生产化。

---

## 3. 非目标

本 spike 不做以下事情：

1. 不重写 ETL、清洗、分析逻辑。
2. 不把 Rust writer 直接接入 `python main.py gb` 默认路径。
3. 不改默认 CLI 行为为 Rust writer。
4. 不做 GUI。
5. 不做模板 XML 替换。
6. 不做 PyO3 Python 扩展。
7. 不引入复杂服务或 daemon。
8. 不在 Phase 1 中重写 Rust ETL。
9. 不把 `成本分析产品维度` 的删除伪装成 Rust 性能收益。
10. 不破坏现有测试和 3-sheet 输出契约。

---

## 4. 已确认设计决策

### 4.1 性能目标优先级

用户确认当前真实目标是：

```text
A. 端到端性能
```

因此当前优先优化 export 阶段，而不是把 ETL 全量迁移到 Rust。

### 4.2 产品维度 sheet 删除

业务确认：

```text
成本分析产品维度 确实不再需要。
默认 workbook 输出三张 sheet。
```

本次变更应先在 Python 生产路径中落地，再做 Rust spike。否则会出现：

```text
Python baseline 是 4 张 sheet
Rust 输出是 3 张 sheet
```

这种对比不能证明 Rust writer 更快，只能证明少写一张表更快。

### 4.3 Phase 0 第一版采用最小风险方案

Phase 0 是生产行为变更，不是 throwaway spike。

第一版采用最小风险方案：

```text
build_report_artifacts 可以继续生成 product_anomaly_sections；
build_sheet_models 不再构建 product_anomaly_model；
默认 workbook 不再输出 成本分析产品维度；
Phase 0 benchmark 只衡量少写第 4 张 sheet 后的 Python 3-sheet baseline；
是否停止计算 product_anomaly_sections，后续作为单独优化项评估。
```

这样做的原因：

1. 当前目标是建立干净的 3-sheet Python baseline，再评估 Rust writer。
2. 停止写 sheet 和停止计算产品维度分析是两个不同风险等级的变更。
3. 如果同时清理 analysis 阶段，Phase 0 的行为变化会变大，review 和回归定位会更难。

### 4.4 产品维度底层 helper 暂不清理

本次只停止默认输出和默认构建产品维度 SheetModel，不主动删除底层 helper。

暂时保留：

```text
ProductAnomalySection
产品维度相关 helper，例如 build_product_anomaly_sections
product_anomaly_writer
产品维度相关底层 helper 单元测试
```

保留原因：

1. 当前目标是性能，不是清仓式重构。
2. 直接删除底层 helper 会牵动大量测试，风险高于收益。
3. 默认输出路径退场后，死代码清理可以后续单独做。

### 4.5 `product_anomaly_scope_mode` 兼容保留

本次保留 `product_anomaly_scope_mode` 参数和配置链路作为兼容参数，但默认 3-sheet 输出不再使用它生成产品维度 SheetModel 或 workbook sheet。

原则：

```text
product_anomaly_scope_mode 只作为兼容参数保留；
默认 workbook 输出不受它影响；
不再因为它生成默认输出的 成本分析产品维度 sheet；
第一版仍允许 analysis 阶段继续计算 product_anomaly_sections。
```

后续如果确认无外部依赖，再单独清理：

```text
PipelineConfig.product_anomaly_scope_mode
CostingWorkbookETL.product_anomaly_scope_mode
build_report_artifacts(product_anomaly_scope_mode=...)
产品维度 helper/tests
```

### 4.6 中间协议第一版仍用 CSV + JSON manifest

Phase 1 第一版仍采用：

```text
manifest.json
sheet_001.csv
sheet_002.csv
sheet_003.csv
```

原因：

1. 实现简单。
2. Rust 端依赖轻。
3. 出错容易定位。
4. 足够验证写出性能。
5. CSV 中间 I/O 可以单独计时，避免误判 Rust writer。

如果总耗时未达标，但 Rust 写出本身足够快，应记录为协议瓶颈的 PARTIAL，而不是直接否定 Rust writer。

### 4.7 分阶段执行硬 Gate

Phase 0 和 Phase 1 必须分阶段执行。

硬性规则：

```text
Phase 0 必须独立完成、独立验证、独立汇报。
只有 Phase 0 的 3-sheet Python 输出契约、测试、benchmark 全部通过后，才能进入 Phase 1。
如果 Phase 0 失败，不得继续实现 Rust sidecar。
```

建议交付方式：

```text
Phase 0 和 Phase 1 应作为两个独立提交；
如果不提交，也必须作为两个清晰独立的变更块汇报。
```

原因：

```text
Phase 0 是生产契约变更；
Phase 1 是 throwaway Rust spike；
两者失败原因不能混在一起。
```

---

## 5. Phase 0：Python 3-sheet 生产契约变更

### 5.1 目标

把当前默认 workbook 输出从 4 张 sheet 改为 3 张 sheet：

```text
1. 成本计算单总表
2. 成本计算单数量聚合维度
3. 成本分析工单维度
```

删除默认输出：

```text
成本分析产品维度
```

### 5.2 预期代码边界

优先修改：

```text
src/analytics/presentation_builder.py
src/excel/workbook_writer.py
tests/contracts/_workbook_contract_helper.py
tests/contracts/baselines/workbook_semantics.json
README.md
docs/rust_xlsxwriter_sidecar_spike_spec.md
相关默认 workbook 输出断言
```

必须检查所有 workbook 写出入口：

```text
CostingWorkbookWriter.write_workbook_from_models
CostingWorkbookWriter.write_workbook
```

要求：

```text
write_workbook_from_models 是当前默认主路径，必须输出 3 张 sheet；
write_workbook 虽然是旧接口，但也应同步改成 3-sheet，避免未来误用时恢复第 4 张 sheet；
如果后续确认 write_workbook 只作为 legacy/helper 存在，应在测试或注释中明确它不代表默认 workbook 契约。
```

不主动删除：

```text
src/excel/product_anomaly_writer.py
产品维度相关 helper，例如 build_product_anomaly_sections
ProductAnomalySection
产品维度底层 helper 测试
```

### 5.3 行为要求

Phase 0 完成后：

```text
python main.py gb
python main.py sk
```

默认只写 3 张 sheet。

Phase 0 第一版不要顺手清理产品维度 analysis 阶段：

```text
build_report_artifacts 可以继续计算 product_anomaly_sections；
build_sheet_models 不再把 product_anomaly_sections 转成默认输出 sheet；
默认 workbook 不再写出 成本分析产品维度；
是否停止计算 product_anomaly_sections，后续单独评估。
```

`--check-only` 行为保持：

```text
只做预检与摘要，不写 workbook 或任何外部摘要文件。
```

控制台质量校验结果保持：

```text
行数勾稽
空值率
工单主键唯一性
分析覆盖率
error_log_count
阶段耗时
```

### 5.4 测试要求

默认业务契约测试必须改为 3 张 sheet：

```python
[
    "成本计算单总表",
    "成本计算单数量聚合维度",
    "成本分析工单维度",
]
```

必须新增或保留断言：

```text
workbook.sheetnames == 3-sheet list
"成本分析产品维度" not in workbook.sheetnames
sheet 顺序稳定
前三张 sheet 的列序、number format、freeze panes、auto filter 保持
```

产品维度相关底层 helper 测试可以保留，但测试语义必须区分：

```text
legacy/helper 可用性
不代表默认 workbook 契约
```

### 5.5 Phase 0 验证命令

优先使用项目 conda 环境：

```bash
conda run -n costing311 python -m pytest tests -q
conda run -n costing311 python -m ruff check src tests
conda run -n costing311 python -m ruff format src tests --check
```

如果只做最小验证，至少运行：

```bash
conda run -n costing311 python -m pytest tests/contracts tests/test_costing_etl.py tests/test_runner.py -q
```

### 5.6 Phase 0 benchmark

Phase 0 完成后，使用同一份真实 GB 输入文件：

```text
data\raw\gb\gb-成本计算单_2026070916484310_100160.xlsx
```

重新运行：

```bash
conda run -n costing311 python main.py gb --benchmark
conda run -n costing311 python main.py gb --check-only --benchmark
```

记录：

```text
python_3sheet_total_seconds
python_3sheet_export_seconds
python_3sheet_payload_seconds
input_size_bytes
python_3sheet_output_size_bytes
sheet row/column counts
```

建议至少跑 3 次，取中位数：

```text
run_1_python_3sheet_export_seconds
run_2_python_3sheet_export_seconds
run_3_python_3sheet_export_seconds
median_python_3sheet_export_seconds
```

---

## 6. Phase 0 伪代码草案

```python
# [伪代码草案]
# 目标：将默认 workbook 输出契约从 4 张 sheet 改成 3 张 sheet，并保留产品维度底层 helper 作为 legacy/helper。
# 输入：
# - split_result.detail_df: 成本计算单总表数据
# - artifacts.qty_sheet_df: 数量聚合维度数据
# - artifacts.work_order_sheet: 工单维度分析数据
# - artifacts.product_anomaly_sections: 兼容保留的产品维度分段，不再进入默认 sheet_models
# 输出：
# - WorkbookPayload.sheet_models: 仅包含 3 个 SheetModel
# - workbook 文件：只包含 3 张默认 sheet
# - 质量校验与 error_log_count：保持原有控制台输出

def build_sheet_models(detail_df, qty_sheet_df, fact_bundle, work_order_sheet, product_anomaly_sections):
    detail_model = build_detail_sheet_model(detail_df)
    qty_model = build_qty_sheet_model(qty_sheet_df)
    work_order_model = build_work_order_sheet_model(work_order_sheet)

    # 为什么不再构建 product_anomaly_model：
    # 业务已确认产品维度 sheet 不再需要，默认 workbook 契约应只包含 3 张 sheet。
    # product_anomaly_sections 暂时保留给 legacy/helper 测试和后续清理，不参与默认输出。
    return (detail_model, qty_model, work_order_model)


def write_workbook_from_models(output_path, sheet_models):
    with xlsxwriter_workbook(output_path) as writer:
        for model in sheet_models:
            write_flat_or_fast_sheet(writer, model)
        # 如果未来发现传入了 legacy 产品维度 model，应明确失败或走非默认 helper；
        # 不要在默认路径静默恢复第 4 张 sheet。


def validate_default_workbook(workbook):
    expected_sheets = [
        "成本计算单总表",
        "成本计算单数量聚合维度",
        "成本分析工单维度",
    ]
    assert workbook.sheetnames == expected_sheets
    assert "成本分析产品维度" not in workbook.sheetnames
```

---

## 7. Phase 1：Rust writer sidecar spike

### 7.1 目标

基于 Phase 0 的 3-sheet Python baseline，验证 Rust sidecar writer 是否能显著降低 export 阶段耗时。

Rust sidecar 输出的 workbook 必须包含：

```text
1. 成本计算单总表
2. 成本计算单数量聚合维度
3. 成本分析工单维度
```

不得包含：

```text
成本分析产品维度
```

### 7.2 Phase 1 前置环境检查

进入 Phase 1 前必须先检查本机环境。

命令：

```bash
cargo --version
rustc --version
conda run -n costing311 python -c "import polars, openpyxl, xlsxwriter"
```

如果 Rust 工具链或 Python 依赖不可用，结论应标记为：

```text
BLOCKED_ENVIRONMENT
```

不得把环境缺失判定为：

```text
INVALIDATED
```

原因：

```text
没装 Rust 或依赖缺失不能证明 rust_xlsxwriter 方案不可行。
```

### 7.3 推荐目录

```text
spikes/
└── 001-rust-xlsxwriter-sidecar/
    ├── README.md
    ├── rust-writer/
    │   ├── Cargo.toml
    │   └── src/
    │       └── main.rs
    ├── python/
    │   ├── export_payload_for_rust.py
    │   ├── validate_rust_workbook.py
    │   └── benchmark_rust_writer.py
    └── tmp/
```

---

## 8. Phase 1 成功标准

### 8.1 主性能指标

Rust sidecar 的真实替代 export 耗时定义为：

```text
sidecar_export_seconds = intermediate_export_seconds + rust_export_seconds
```

其中：

```text
intermediate_export_seconds:
  Python 将 3 个 SheetModel 导出为 CSV + manifest 的耗时

rust_export_seconds:
  Rust CLI 读取中间数据并写出 xlsx 的耗时

sidecar_export_seconds:
  判断 sidecar 是否能替代当前 Python export 阶段的主指标
```

`rust_export_seconds` 只能作为诊断指标，不能单独决定 verdict。

### 8.2 VALIDATED

同时满足：

```text
sidecar_export_seconds <= 5.000 秒
sidecar_export_seconds <= median_python_3sheet_export_seconds * 0.60
workbook 验证通过
```

含义：

```text
Rust sidecar 不仅绝对耗时低于 5 秒，而且至少比 Python 3-sheet export 快 40%。
```

### 8.3 PARTIAL

满足任一：

```text
5s < sidecar_export_seconds <= 7s
```

或者：

```text
sidecar_export_seconds <= 5s
但没有比 Python 3-sheet export 快 40%
```

或者：

```text
sidecar_export_seconds 未达标
但 rust_export_seconds 达标，intermediate_export_seconds 是主要瓶颈
```

第三种情况应标记为：

```text
PARTIAL_PROTOCOL_BOTTLENECK
```

含义：

```text
Rust writer 本身可能值得继续评估，但 CSV + manifest 协议可能不适合生产化。
```

### 8.4 INVALIDATED

满足任一：

```text
sidecar_export_seconds > 7s
sidecar_export_seconds 接近或慢于 median_python_3sheet_export_seconds
文件打开有修复提示
openpyxl.load_workbook() 失败
关键数据/格式无法稳定保持
number 类型解析失败被静默转成文本
输出包含或缺失错误 sheet
```

### 8.5 BLOCKED_ENVIRONMENT

满足任一：

```text
cargo 不可用
rustc 不可用
conda costing311 环境不可用
Phase 1 需要的 Python 依赖不可导入
真实 GB 输入文件不存在
```

含义：

```text
环境不满足 spike 执行条件；
不得把环境缺失解释为 Rust writer 方案失败。
```

---

## 9. Phase 1 文件正确性标准

Rust 写出的 workbook 必须满足：

1. Excel / WPS 能正常打开。
2. 打开时不提示文件修复。
3. `openpyxl.load_workbook()` 可正常读取。
4. Sheet 数量为 3。
5. Sheet 名称一致。
6. Sheet 顺序一致。
7. 不包含 `成本分析产品维度`。
8. 每张 sheet 行数一致。
9. 每张 sheet 列数一致。
10. 表头一致。
11. 关键数值列是数字，不是文本。
12. 金额/成本类列保留两位小数格式。
13. 冻结窗格保留。
14. 自动筛选保留。
15. 关键抽样单元格值与 Python 3-sheet 输出一致。

---

## 10. 中间数据协议

### 10.1 采用 CSV + JSON manifest

第一版使用：

```text
manifest.json
sheet_001.csv
sheet_002.csv
sheet_003.csv
```

### 10.2 manifest 示例

```json
{
  "workbook": {
    "output_path": "D:/python_program/02--costing_calculate/spikes/001-rust-xlsxwriter-sidecar/tmp/rust_output.xlsx"
  },
  "sheets": [
    {
      "name": "成本计算单总表",
      "csv_path": "sheet_001.csv",
      "freeze_panes": "A2",
      "auto_filter": true,
      "fixed_width": 15,
      "columns": [
        {
          "name": "月份",
          "type": "text"
        },
        {
          "name": "本期完工单位成本",
          "type": "number",
          "num_format": "#,##0.00"
        },
        {
          "name": "本期完工金额",
          "type": "number",
          "num_format": "#,##0.00"
        }
      ]
    }
  ]
}
```

### 10.3 列类型规则

Python 导出 manifest 时，应按 `SheetModel.number_formats` 和 `SheetModel.column_types` 共同决定 Rust 写法。

推荐规则：

```text
如果列存在 number_format:
  type=number
  num_format=对应格式

否则如果 column_types 明确为 amount/price/qty/score/pct:
  type=number
  num_format=按现有 number format 映射

否则:
  type=text
```

CSV 中所有值初始都是字符串，Rust 侧必须按 manifest 解析。

---

## 11. Python 侧任务

新增 spike 脚本，不改主线默认行为。

### 11.1 `export_payload_for_rust.py`

职责：

1. 调用现有 pipeline 构建 3-sheet `WorkbookPayload`。
2. 不写出 xlsx。
3. 将 `payload.sheet_models` 导出为：
   - `manifest.json`
   - 每张 sheet 一个 CSV。
4. 只导出 3 张默认 sheet。
5. 保留：
   - sheet name；
   - column order；
   - row data；
   - number formats；
   - freeze panes；
   - auto filter；
   - fixed width。
6. 记录导出耗时：
   - payload 构建耗时；
   - CSV/manifest 导出耗时；
   - 每张 sheet 行数、列数。

CSV 导出必须优先使用 DataFrame 原生高速导出，避免用 Python 逐行循环制造假瓶颈：

```text
1. 如果 model.source_frame 是 Polars DataFrame，优先使用 source_frame.write_csv()
2. 如果可获得 pandas DataFrame，使用 DataFrame.to_csv()
3. 只有在没有 source_frame 或 DataFrame 时，才 fallback 到 rows_factory
```

benchmark 必须记录每张 sheet 的导出方法和耗时：

```text
sheet_001_csv_export_method=polars|pandas|rows_factory
sheet_001_intermediate_export_seconds=x.xxx
sheet_002_csv_export_method=polars|pandas|rows_factory
sheet_002_intermediate_export_seconds=x.xxx
sheet_003_csv_export_method=polars|pandas|rows_factory
sheet_003_intermediate_export_seconds=x.xxx
```

示例命令：

```bash
conda run -n costing311 python spikes/001-rust-xlsxwriter-sidecar/python/export_payload_for_rust.py gb
```

输出：

```text
spikes/001-rust-xlsxwriter-sidecar/tmp/manifest.json
spikes/001-rust-xlsxwriter-sidecar/tmp/sheet_001.csv
spikes/001-rust-xlsxwriter-sidecar/tmp/sheet_002.csv
spikes/001-rust-xlsxwriter-sidecar/tmp/sheet_003.csv
```

### 11.2 `benchmark_rust_writer.py`

职责：

1. 在同一台机器、同一轮测试中重跑 Python 3-sheet baseline。
2. 运行 `export_payload_for_rust.py`。
3. 调用 Rust CLI。
4. 计时：
   - Python payload 秒数；
   - CSV/manifest 中间文件导出秒数；
   - Rust xlsx 写出秒数；
   - sidecar export 秒数；
   - sidecar total 秒数。
5. Python 3-sheet full export 至少跑 3 次，取中位数。
6. Rust sidecar 至少跑 3 次，取中位数。
7. 用同场 benchmark 的两个中位数比较。
8. 输出 verdict。

正式 verdict 不使用过期 baseline 文件作为主判定依据。历史或 Phase 0 记录的 baseline 只能用于展示背景；最终判定必须来自同场 benchmark。

输出格式：

```text
[baseline_3sheet]
run_1_python_3sheet_export_seconds=x.xxx
run_2_python_3sheet_export_seconds=x.xxx
run_3_python_3sheet_export_seconds=x.xxx
median_python_3sheet_export_seconds=x.xxx
median_python_3sheet_total_seconds=x.xxx
median_python_3sheet_payload_seconds=x.xxx

[rust_sidecar]
payload_seconds=x.xxx
intermediate_export_seconds=x.xxx
rust_export_seconds=x.xxx
sidecar_export_seconds=x.xxx
total_seconds=x.xxx

[runs]
run_1_intermediate_export_seconds=x.xxx
run_1_rust_export_seconds=x.xxx
run_1_sidecar_export_seconds=x.xxx
run_2_intermediate_export_seconds=x.xxx
run_2_rust_export_seconds=x.xxx
run_2_sidecar_export_seconds=x.xxx
run_3_intermediate_export_seconds=x.xxx
run_3_rust_export_seconds=x.xxx
run_3_sidecar_export_seconds=x.xxx
median_sidecar_export_seconds=x.xxx

[result]
sidecar_export_speedup=x.xx
total_speedup=x.xx
verdict=VALIDATED|PARTIAL|PARTIAL_PROTOCOL_BOTTLENECK|INVALIDATED|BLOCKED_ENVIRONMENT
```

### 11.3 `validate_rust_workbook.py`

职责：

对比 Python 3-sheet 输出 workbook 与 Rust 输出 workbook。

输入：

```text
python_3sheet_output.xlsx
rust_output.xlsx
```

校验：

1. workbook 可读取；
2. sheet 数量为 3；
3. sheet 名称和顺序；
4. 不包含 `成本分析产品维度`；
5. 每张 sheet 最大行列；
6. 表头；
7. 首 10 行；
8. 尾 10 行；
9. 中间随机 20 行；
10. 金额列随机 100 个单元格数值；
11. 数字列类型；
12. number format；
13. freeze panes；
14. auto filter。

示例命令：

```bash
conda run -n costing311 python spikes/001-rust-xlsxwriter-sidecar/python/validate_rust_workbook.py ^
  data/processed/gb/gb-成本计算单_2026070916484310_100160_处理后.xlsx ^
  spikes/001-rust-xlsxwriter-sidecar/tmp/rust_output.xlsx
```

校验结果示例：

```text
status=passed
sheet_count=3
sheet_order_matched=true
product_dimension_absent=true
row_count_matched=true
header_matched=true
sample_cells_matched=true
number_format_matched=true
freeze_panes_matched=true
auto_filter_matched=true
```

---

## 12. Rust 侧任务

### 12.1 新建 Rust CLI

目录：

```text
spikes/001-rust-xlsxwriter-sidecar/rust-writer/
```

开发命令：

```bash
cargo run --release -- --manifest ../tmp/manifest.json --output ../tmp/rust_output.xlsx
```

编译后命令：

```bash
rust-writer.exe --manifest manifest.json --output rust_output.xlsx
```

### 12.2 Rust 依赖建议

`Cargo.toml`：

```toml
[package]
name = "costing-rust-xlsx-writer"
version = "0.1.0"
edition = "2021"

[dependencies]
rust_xlsxwriter = "0.96"
csv = "1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
anyhow = "1"
clap = { version = "4", features = ["derive"] }
```

版本可使用最新稳定版本，但不要引入不必要依赖。

### 12.3 Rust 写出要求

Rust CLI 需要：

1. 读取 manifest。
2. 拒绝 manifest 中出现 `成本分析产品维度`，避免误用旧 4-sheet 协议。
3. 按 manifest 顺序创建 worksheet。
4. 写入表头。
5. 写入 CSV 数据。
6. 根据 manifest 中的列类型写入：
   - text；
   - number；
   - blank。
7. 应用：
   - 数字格式；
   - 固定列宽；
   - 冻结窗格；
   - 自动筛选；
   - 表头样式。
8. 保存 xlsx。

### 12.4 数据类型规则

```text
type=text:
  空字符串 => blank 或空字符串均可，但要保持验证一致
  非空 => write_string

type=number:
  空字符串 / "-" / null-like => blank
  非空 => parse f64 后 write_number
```

如果 number 解析失败：

```text
不要静默转文本
记录错误并退出非 0
```

错误示例：

```text
failed to parse number: sheet=成本计算单总表 row=123 col=本期完工金额 value="abc"
```

---

## 13. 样式要求

第一版只保留必要样式，不追求完全一致。

必须保留：

1. 表头加粗；
2. 表头背景色；
3. 表头居中；
4. 数字列两位小数；
5. 文本列左对齐；
6. 数字列右对齐；
7. 固定列宽；
8. 冻结窗格；
9. 自动筛选。

可以不保留：

1. 数据区边框；
2. 条件格式；
3. 高亮色；
4. 特殊字体。

注意：

```text
成本分析产品维度 已退出默认 workbook 契约；
Rust spike 不需要实现产品维度特殊布局。
```

---

## 14. Sheet 处理范围

Phase 1 只处理当前默认 workbook 的 3 张 sheet：

```text
1. 成本计算单总表
2. 成本计算单数量聚合维度
3. 成本分析工单维度
```

如果 manifest 中出现其他 sheet：

```text
默认失败退出
```

原因：

```text
spike 的目标是验证新的默认 3-sheet workbook 写出性能；
不要在 spike 中悄悄扩大或改变输出契约。
```

---

## 15. Benchmark 要求

正式 verdict 必须使用同场 benchmark：

```text
1. 先清理旧输出文件；
2. 跑 Python 3-sheet full export 3 次；
3. 跑 Rust sidecar 3 次；
4. 两边都取中位数；
5. verdict 用两个中位数比较。
```

原因：

```text
Excel 写文件容易受磁盘缓存、杀毒软件、文件占用、系统负载影响；
跨天或跨环境 baseline 只能作背景，不能作正式 verdict。
```

需要输出：

```text
run_1_python_3sheet_export_seconds
run_2_python_3sheet_export_seconds
run_3_python_3sheet_export_seconds
median_python_3sheet_export_seconds
run_1_intermediate_export_seconds
run_1_rust_export_seconds
run_1_sidecar_export_seconds
run_2_intermediate_export_seconds
run_2_rust_export_seconds
run_2_sidecar_export_seconds
run_3_intermediate_export_seconds
run_3_rust_export_seconds
run_3_sidecar_export_seconds
median_sidecar_export_seconds
```

同时记录：

```text
input_size_bytes
python_3sheet_output_size_bytes
rust_output_size_bytes
sheet row/column counts
sheet csv_export_method
sheet intermediate_export_seconds
```

避免单次波动误判。

---

## 16. 验证要求

### 16.1 结构验证

用 `openpyxl` 验证：

```python
load_workbook(rust_output.xlsx, data_only=False)
```

检查：

```text
sheetnames
max_row
max_column
freeze_panes
auto_filter.ref
```

### 16.2 数据验证

至少验证：

1. 每张 sheet 表头完全一致；
2. 每张 sheet 首 10 行一致；
3. 每张 sheet 尾 10 行一致；
4. 每张 sheet 中间随机 20 行一致；
5. 金额列随机 100 个单元格数值一致；
6. 数字列不是文本；
7. `成本分析产品维度` 不存在。

### 16.3 单元格值归一化规则

验证时不能直接对所有 openpyxl 读取值做 `==`，必须按列类型归一化，避免空值、整数/浮点、Decimal 表示差异造成误报。

规则：

```text
空值：
  None 和 "" 视为空值等价

数字列：
  使用 Decimal(str(value)) 或浮点容差比较
  默认容差：0.000001

金额/成本列：
  按业务精度或 number format 比较
  默认容差：0.000001

文本列：
  转字符串后精确比较

number_format：
  只校验 manifest 标记为 number 的列
  不要求所有文本列格式完全一致
```

目标：

```text
避免 Rust 输出因非关键样式细节不同被误判失败；
同时保证关键数字列仍然是数字，不被静默写成文本。
```

### 16.4 Excel 打开验证

如果环境支持，可以额外用 Windows COM 或 OfficeCLI 打开验证。

如果不支持，至少保证：

```text
openpyxl 可以打开
LibreOffice / WPS / Excel 手动打开不报修复
```

README 中记录实际验证方式。

---

## 17. Phase 1 伪代码草案

```python
# [伪代码草案]
# 目标：用 Python 构建 3-sheet WorkbookPayload，将 sheet 数据导出为 CSV + manifest，
#      再调用 Rust CLI 写出 xlsx，并用 Python workbook 作为基准验证输出。
# 输入：
# - pipeline_name: "gb" 或 "sk"
# - input_path: 原始成本计算单 Excel
# - python_3sheet_output_path: Phase 0 后的 Python 输出 workbook
# - rust_writer_exe: Rust CLI 可执行文件
# 输出：
# - manifest.json 和 sheet_001..003.csv
# - rust_output.xlsx
# - benchmark_result: 耗时、speedup、verdict
# - validation_result: 结构、数据、格式校验结果

def export_payload_for_rust(pipeline_name, input_path, tmp_dir):
    payload = build_workbook_payload_without_xlsx_export(pipeline_name, input_path)
    if len(payload.sheet_models) != 3:
        return error("SHEET_CONTRACT_MISMATCH", "默认输出必须是 3 张 sheet")

    manifest = {"workbook": {}, "sheets": []}
    for index, model in enumerate(payload.sheet_models, start=1):
        if model.sheet_name == "成本分析产品维度":
            return error("PRODUCT_DIMENSION_NOT_ALLOWED", "产品维度已退出默认输出契约")

        csv_path = tmp_dir / f"sheet_{index:03d}.csv"
        column_specs = build_column_specs(model)
        csv_export_result = export_sheet_csv(csv_path, model)
        manifest["sheets"].append(
            {
                "name": model.sheet_name,
                "csv_path": csv_path.name,
                "csv_export_method": csv_export_result.method,
                "freeze_panes": model.freeze_panes,
                "auto_filter": model.auto_filter,
                "fixed_width": model.fixed_width,
                "columns": column_specs,
            }
        )

    write_json_utf8(tmp_dir / "manifest.json", manifest)
    return success(manifest)


def export_sheet_csv(csv_path, model):
    # 为什么要优先走 DataFrame 原生导出：
    # rows_factory 会回到 Python 逐行循环，可能把 spike 误判为协议瓶颈。
    if isinstance(model.source_frame, polars.DataFrame):
        model.source_frame.write_csv(csv_path)
        return CsvExportResult(method="polars")
    if has_pandas_dataframe(model):
        get_pandas_dataframe(model).to_csv(csv_path, index=False, encoding="utf-8")
        return CsvExportResult(method="pandas")
    write_csv_from_rows_factory(csv_path, model.columns, model.rows_factory)
    return CsvExportResult(method="rows_factory")


def run_rust_writer(manifest_path, output_path):
    result = subprocess_run(["rust-writer", "--manifest", manifest_path, "--output", output_path])
    if result.exit_code != 0:
        return error("RUST_WRITER_FAILED", result.stderr)
    return success(output_path)


def validate_rust_workbook(python_output, rust_output):
    py_wb = openpyxl.load_workbook(python_output, data_only=False)
    rust_wb = openpyxl.load_workbook(rust_output, data_only=False)
    expected_sheets = ["成本计算单总表", "成本计算单数量聚合维度", "成本分析工单维度"]
    assert py_wb.sheetnames == expected_sheets
    assert rust_wb.sheetnames == expected_sheets

    for sheet_name in expected_sheets:
        compare_shape(py_wb[sheet_name], rust_wb[sheet_name])
        compare_headers(py_wb[sheet_name], rust_wb[sheet_name])
        compare_sample_rows_with_normalization(py_wb[sheet_name], rust_wb[sheet_name])
        compare_number_formats_for_number_columns(py_wb[sheet_name], rust_wb[sheet_name])
        compare_freeze_panes_and_filter(py_wb[sheet_name], rust_wb[sheet_name])

    return success("passed")


def benchmark_rust_writer():
    # 正式 verdict 使用同场 benchmark，避免磁盘缓存、杀毒软件、系统负载导致跨天数据不可比。
    python_runs = run_python_3sheet_full_export(repeat=3)
    sidecar_runs = []
    for _ in range(3):
        intermediate_seconds = time_call(export_payload_for_rust)
        rust_seconds = time_call(run_rust_writer)
        sidecar_seconds = intermediate_seconds + rust_seconds
        sidecar_runs.append(sidecar_seconds)

    median_python = median(run.export_seconds for run in python_runs)
    median_sidecar = median(sidecar_runs)
    verdict = classify_verdict(
        median_sidecar,
        python_3sheet_export_seconds=median_python,
        validation_passed=True,
    )
    return benchmark_result(median_sidecar, verdict)
```

---

## 18. Spike 输出物

完成后需要产出：

```text
spikes/001-rust-xlsxwriter-sidecar/README.md
```

README 必须包含：

```markdown
# 001: rust_xlsxwriter sidecar

## Question

Can rust_xlsxwriter replace Python xlsxwriter export for the 3-sheet default workbook and reduce sidecar export time to <=5s while achieving at least 40% speedup over the Python 3-sheet export baseline?

## Baseline

- python_3sheet_total_seconds:
- python_3sheet_export_seconds:
- python_3sheet_payload_seconds:
- input rows:
- output workbook size:
- sheet count: 3

## Approach

- Phase 0 changed default workbook contract to 3 sheets
- Python exports manifest + CSV for the 3 default sheets
- Rust reads manifest + CSV
- Rust writes xlsx via rust_xlsxwriter
- sidecar_export_seconds = intermediate_export_seconds + rust_export_seconds

## Results

| Run | Python 3-sheet export | Intermediate export | Rust export | Sidecar export | Speedup |
|---|---:|---:|---:|---:|---:|
| 1 | | | | | |
| 2 | | | | | |
| 3 | | | | | |
| median | | | | | |

## CSV Export

| Sheet | Method | Seconds | Rows | Columns |
|---|---|---:|---:|---:|
| 成本计算单总表 | polars/pandas/rows_factory | | | |
| 成本计算单数量聚合维度 | polars/pandas/rows_factory | | | |
| 成本分析工单维度 | polars/pandas/rows_factory | | | |

## Validation

- openpyxl load:
- sheet count:
- sheet names:
- product dimension absent:
- row counts:
- headers:
- sample values:
- number formats:
- freeze panes:
- auto filter:
- Excel/WPS manual open:

## Verdict

VALIDATED / PARTIAL / PARTIAL_PROTOCOL_BOTTLENECK / INVALIDATED / BLOCKED_ENVIRONMENT

## Recommendation

- Adopt / Do not adopt
- Required production work if adopted
```

---

## 19. 生产化前置条件

如果 Phase 1 `VALIDATED`，后续生产化再单独写 plan。生产化至少需要：

1. 把 Rust writer 编译产物放入项目可管理路径。
2. Python 增加可选参数，例如：

```bash
python main.py gb --writer rust
```

3. 默认仍保留 Python writer 作为 fallback。
4. CI 增加 3-sheet workbook contract test。
5. Windows 打包/部署说明。
6. 对 Rust writer 输出做全量验证。
7. 明确失败回退策略。
8. 明确没有 `成本分析产品维度` 的新 workbook 契约。

---

## 20. Codex 执行提示词

```text
你在 D:\python_program\02--costing_calculate 项目中工作。

请按 docs\rust_xlsxwriter_sidecar_spike_spec.md 执行，分两阶段推进。

Phase 0：先完成 Python 默认 workbook 3-sheet 契约变更。

硬性 gate：
- Phase 0 必须独立完成、独立验证、独立汇报。
- Phase 0 未通过前，不得进入 Phase 1。
- Phase 0 和 Phase 1 应作为两个独立提交；如果不提交，也必须作为两个清晰独立的变更块汇报。

目标：
- 默认输出只保留 3 张 sheet：
  1. 成本计算单总表
  2. 成本计算单数量聚合维度
  3. 成本分析工单维度
- 不再默认输出 成本分析产品维度。
- 不再默认构建产品维度 SheetModel。
- Phase 0 第一版采用最小风险方案：build_report_artifacts 可以继续计算 product_anomaly_sections；是否停止计算 product_anomaly_sections 后续单独评估。
- 暂时保留产品维度底层 helper 和相关 legacy/helper 单元测试。
- 暂时保留 product_anomaly_scope_mode 兼容参数，但默认 3-sheet 输出不使用它生成产品维度。
- 同步检查并处理 CostingWorkbookWriter.write_workbook_from_models 和 CostingWorkbookWriter.write_workbook，避免任何默认写出入口恢复第 4 张 sheet。
- 更新 README、workbook contract baseline、相关测试断言。
- 运行测试和 lint。
- 用真实 GB 输入重新跑 Python 3-sheet benchmark，并记录新的 baseline。
- 不要修改或纳入与本任务无关的 untracked 文件；尤其不要因为看到 uv.lock 未跟踪就擅自处理。

真实输入：
data\raw\gb\gb-成本计算单_2026070916484310_100160.xlsx

Phase 1：再实现 throwaway Rust writer sidecar spike。

Phase 1 前置环境检查：
- cargo --version
- rustc --version
- conda run -n costing311 python -c "import polars, openpyxl, xlsxwriter"
- 如果环境缺失，输出 BLOCKED_ENVIRONMENT，不得判定 INVALIDATED。

请新建：
spikes/001-rust-xlsxwriter-sidecar/

包含：
1. rust-writer/ Rust CLI，使用 rust_xlsxwriter + csv + serde_json + clap + anyhow。
2. python/export_payload_for_rust.py：调用现有 pipeline 构建 3-sheet WorkbookPayload，不写 xlsx，导出 manifest.json + 每张 sheet CSV；CSV 导出必须优先使用 Polars source_frame.write_csv()，其次 pandas DataFrame.to_csv()，rows_factory 只作为 fallback，并记录每张 sheet 的 csv_export_method。
3. python/benchmark_rust_writer.py：同场跑 Python 3-sheet full export 3 次和 Rust sidecar 3 次，输出两个中位数、intermediate export 秒数、Rust export 秒数、sidecar export 秒数、speedup、verdict。
4. python/validate_rust_workbook.py：用 openpyxl 对比 Python 3-sheet 输出 workbook 和 Rust 输出 workbook，验证 sheet 顺序、行列数、表头、抽样数据、关键数字格式、freeze panes、auto filter，并确认不存在 成本分析产品维度；验证时按 spec 归一化空值和数字值。
5. README.md：记录 question、baseline、approach、results、validation、verdict、recommendation。

约束：
- 不修改 python main.py 默认 writer 行为为 Rust。
- 不替换生产 writer。
- 不引入服务/daemon。
- 不重写 Rust ETL。
- 第一版用 CSV + manifest.json 作为 Python/Rust 中间协议。
- 第一版只保留必要样式：表头、数字格式、固定列宽、冻结窗格、筛选。
- number 类型解析失败必须报错退出，不允许静默写成文本。
- Rust spike 只处理 3 张默认 sheet；manifest 中出现 成本分析产品维度 应失败退出。
- 完成后实际运行 benchmark 和 validation，不要只写代码。
- 最终 README 必须给出 VALIDATED / PARTIAL / PARTIAL_PROTOCOL_BOTTLENECK / INVALIDATED / BLOCKED_ENVIRONMENT verdict。

Phase 1 verdict 主指标：
sidecar_export_seconds = intermediate_export_seconds + rust_export_seconds

VALIDATED 必须同时满足：
- sidecar_export_seconds <= 5.000 秒
- sidecar_export_seconds <= median_python_3sheet_export_seconds * 0.60
- workbook validation passed

请先实现 Phase 0，重跑 Python 3-sheet baseline；再实现 Phase 1，运行真实 benchmark 和验证，最后汇报结果。
```

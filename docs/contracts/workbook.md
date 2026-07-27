# 当前工作簿与运行契约

这是 GB / SK 正式输出的当前事实文档。精确列序、样式和固定示例同时由 `tests/contracts/baselines/`、Rust 测试和实际 CLI 输出约束。

## 输出边界

- 正常运行只发布一个处理后 workbook。
- `--check-only` 不写 workbook；显式 `--summary-output` 时只写 Manifest。
- workbook 和 Manifest 都拒绝覆盖已有文件，且使用同目录临时文件、flush/sync 和原子发布。
- 固定三张 Sheet，顺序不能改变：
  1. `成本计算单总表`
  2. `成本计算单数量聚合维度`
  3. `成本分析工单维度`
- 不输出产品维度 Sheet，也不自动写 error-log CSV 或旧式 summary JSON。

## Sheet 1：成本计算单总表

一行是一条工单级成本记录。列顺序：

```text
月份
成本中心名称
产品编码
产品名称
规格型号
工单编号
工单行号
基本单位
成本项目名称
本期完工单位成本
本期完工金额
```

关键规则：

- `本期完工金额`为空时，后续分析按 0 计算，并记录 `MISSING_AMOUNT`。
- 成本中心名称为`集成车间`时，供应商编码和供应商名称不得向下填充。
- 该 Sheet 不受产品白名单过滤。

## Sheet 2：成本计算单数量聚合维度

保留现有工单行粒度，只保留：

- `本期完工数量 > 0`；
- `本期完工金额`非空。

字段分组及顺序：

1. 标识：月份、成本中心名称、产品编码、产品名称、规格型号、工单编号、工单行号、单据类型、基本单位。
2. 数量与总额：本期完工数量、本期完工金额。
3. 金额：直接材料、直接人工、制造费用总额及其他/人工/机物料及低耗/折旧/水电费五项明细。
4. 独立成本金额：
   - GB：委外加工费；
   - SK：委外加工费、软件费用。
5. 单位成本：对应上述三大类、制造费用明细和独立成本项。
6. 勾稽：制造费用明细勾稽、总成本勾稽、数据校验状态、异常原因说明。

制造费用明细勾稽只计算五项制造费用明细，不包含独立成本项。

总成本口径：

- GB：`直接材料 + 直接人工 + 制造费用 + 委外加工费 = 总完工成本`
- SK：`直接材料 + 直接人工 + 制造费用 + 委外加工费 + 软件费用 = 总完工成本`

## Sheet 3：成本分析工单维度

一行定义为：

```text
月份 + 产品编码 + 工单编号 + 工单行
```

字段分组及顺序：

1. 标识：月份、成本中心、产品编码、产品名称、规格型号、工单编号、工单行、生产类型、基本单位。
2. 数量与总成本。
3. 三大类金额、制造费用五项明细金额、独立成本项金额。
4. 总单位成本、三大类单位成本、制造费用五项单位成本、独立成本项单位成本。
5. 是否可参与分析、异常等级、异常主要来源、异常明细解释、复核原因。

SK 在委外加工费金额/单位成本后分别增加软件费用金额/单位成本。独立成本项只展示金额和单位成本，不输出 log、Modified Z-score 或单项异常标记。

## 产品池与异常分析

- 分析页产品池按“产品编码 + 产品名称”双字段精确匹配。
- 展示顺序必须与配置中的产品顺序一致，不按编码或名称重新排序。
- 异常总体按同一产品、同一生产类型、同一成本指标，在整个统计期间内建立；月份只作标签与汇总字段。
- 只有大于 0 的单位成本参与对数和 Modified Z-score。
- 阈值：
  - `|score| <= 2.5`：正常；
  - `2.5 < |score| <= 3.5`：关注；
  - `|score| > 3.5`：高度可疑。
- 独立成本项不参与`异常等级`和`异常主要来源`判定。
- `异常明细解释`列出所有达到关注或高度可疑的成本项。
- 解释中的`有效工单数`是实际参与该成本指标评分的工单行数，不是完工数量合计。

## error log

error log 保留在运行结果中，不单独落盘。当前重要类型：

| 类型 | 含义 |
|---|---|
| `MISSING_AMOUNT` | 本期完工金额缺失 |
| `UNMAPPED_COST_ITEM` | 非空成本项目无法映射 |
| `TOTAL_COST_MISMATCH` | 总成本勾稽不一致 |
| `MOH_BREAKDOWN_MISMATCH` | 制造费用明细与制造费用合计不一致 |
| `DUPLICATE_WORK_ORDER_KEY` | 工单主键重复 |
| `NON_POSITIVE_UNIT_COST` | 参与检查的单位成本非正 |

独立成本项不会仅因其身份写入 error log。

## 质量摘要

控制台和 Manifest 至少包含：

- 输入、明细、数量和分析行数勾稽；
- 关键金额缺失率；
- 工单主键唯一性；
- 非正完工数量范围检查；
- 分析覆盖率；
- `error_log_count` 与分类计数；
- 可选阶段耗时。

## CLI 错误模型

失败 JSON 至少包含：

```text
status
request_id
code
stage
message
retryable
final_output_valid
warnings
```

稳定错误码包括：

- 输入/配置：`INVALID_INPUT`、`INVALID_CONFIG`、`FILE_NOT_FOUND`、`FILE_NOT_READABLE`、`UNSUPPORTED_FILE_TYPE`
- 输出/存储：`OUTPUT_EXISTS`、`OUTPUT_NOT_WRITABLE`、`INSUFFICIENT_DISK_SPACE`、`TEMP_CLEANUP_FAILED`
- 契约/内部：`READER_MISMATCH`、`ETL_MISMATCH`、`ANALYSIS_MISMATCH`、`WORKBOOK_MISMATCH`、`PERFORMANCE_REGRESSION`、`INTERNAL_ERROR`

自动化应依赖 `code` 和 `retryable`，不要解析自然语言 `message`。

## RunManifestV1

应用版本是 `0.3.0`，Manifest schema 仍是 V1。

成功 Manifest 固定包含：

- application：名称、应用版本、Git commit、构建时间、Rust 版本和 target；
- execution：管线、模式、开始/结束时间、耗时和 low-memory writer 标记；
- input：路径、文件名、大小、SHA-256、读取 Sheet 和行数；
- filter：月份范围；
- config：schema v1、来源、有效配置 SHA-256、源文件 SHA-256；
- result：是否写 workbook、路径、大小、SHA-256、三张 Sheet 和最终文件有效性；
- quality、run_counts、stage_timings、warnings。

失败 Manifest 记录失败阶段、稳定错误码、可重试性、已知输入信息和最终输出有效性。

`--redact-paths` 规则：

- 当前工作目录内的路径改为相对路径；
- 当前工作目录外的路径只保留 basename。

精确封闭 schema 见 `rust/crates/costing-cli/config/run-manifest.schema.json`，成功/失败示例见同目录 golden JSON。

## 验证口径

跨版本比较覆盖 Sheet、行列、单元格存储类型和值、样式、数字格式、条件格式和 OOXML 包结构。

- 单元格数值绝对容差：`1e-9`
- 列累计绝对容差：`1e-8`

不得通过放宽容差、更新基线或忽略差异来掩盖无法解释的变化。

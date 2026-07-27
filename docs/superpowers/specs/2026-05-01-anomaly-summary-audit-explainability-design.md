# 异常摘要报告与审计解释字段设计

## 目标

让输出不只是一份大 Excel，而是同时提供可快速复核的异常摘要和更可解释的工单异常字段。

本阶段不改变异常算法，只增加摘要和解释信息：

- 新增轻量摘要文件，优先为 `*_summary.json`。
- 在摘要中统计质量指标、error_log issue_type、异常等级、异常来源。
- 在工单异常页增加或准备增加审计解释字段，如异常池样本数、异常池中心值、有效 MAD、偏离比例。

## 输入

- `AnalysisArtifacts`
- `QualityMetric`
- `error_log`
- `work_order_sheet.data`
- pipeline 名称、输入路径、输出路径、月份过滤摘要

## 输出

- `*_summary.json`：
  - pipeline
  - input/output/error_log 路径
  - error_log_count
  - quality_metrics
  - issue_type_counts
  - anomaly_level_counts
  - anomaly_source_counts
  - month_filter
- 可选 workbook 字段：
  - `异常池样本数`
  - `异常池中心log值`
  - `异常池原始MAD`
  - `异常池有效MAD`
  - `相对中位偏离`

## 伪代码草案

```python
# [伪代码草案]
# 目标：基于现有产物生成摘要，不改变核心分析结果
# 输入：
# - pipeline_name: gb/sk
# - input_path/output_path/error_log_path/summary_path
# - quality_metrics: 质量指标元组
# - error_log_frame: error_log 明细
# - work_order_sheet: 工单异常分析 FlatSheet
# 输出：
# - summary_payload: 可复核 JSON 对象
# - summary_file: 正常模式落盘，check-only 可只打印或跳过落盘

def build_summary_payload(context, quality_metrics, error_log_frame, work_order_sheet):
    # 为什么从现有结果汇总：摘要是交付体验增强，不应重新计算另一套口径。
    issue_counts = count_values(error_log_frame, "issue_type")
    anomaly_level_counts = count_values(work_order_sheet.data, "异常等级")
    anomaly_source_counts = count_values(work_order_sheet.data, "异常主要来源")

    return {
        "pipeline": context.pipeline_name,
        "input": str(context.input_path),
        "output": str(context.output_path),
        "error_log": str(context.error_log_path),
        "quality_metrics": serialize_quality_metrics(quality_metrics),
        "issue_type_counts": issue_counts,
        "anomaly_level_counts": anomaly_level_counts,
        "anomaly_source_counts": anomaly_source_counts,
    }

def add_anomaly_pool_explain_columns(anomaly_df):
    # 为什么不改评分：解释字段应复用评分过程中已经得到的 median/MAD/sample_size。
    for each metric and product_scope_group:
        stats = compute_weighted_stats(...)
        assign stats to rows in group
    return anomaly_df
```

## 风险点 / 边界条件

- summary 必须来自同一次 ETL 产物，不能重新跑一遍导致口径不一致。
- JSON 默认 UTF-8，写文件时显式 `encoding="utf-8"`。
- 工单异常解释字段会改变 workbook 列契约，必须单独更新 contract baseline，且说明是功能变化。
- 第一版可先交付 `summary.json`，解释字段作为该优先级第二步，避免一次改太多 workbook 契约。

## 验收标准

- 正常运行生成 `*_summary.json`。
- summary 文件包含质量指标、error_log issue type 计数、异常等级计数。
- summary 的 `error_log_count` 与 CSV 行数一致。
- 若新增 workbook 解释字段，contract baseline 只体现明确新增列，不改变既有列含义。


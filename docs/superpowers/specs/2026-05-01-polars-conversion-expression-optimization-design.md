# Polars 转换与表达式化优化设计

## 目标

在不改变业务口径的前提下，减少热点路径中的 pandas / Polars 来回转换与 Python 层 `map_elements` / `.map()` 调用，提升性能并降低数据结构漂移风险。

本阶段以“等价替换”为原则，不改异常阈值、金额精度、白名单、sheet 输出契约。

## 输入

- 当前 `build_report_artifacts()` 产出的 `detail_pl`、`qty_pl`、`FactBundle`
- 当前 Polars fact 构建逻辑中的金额、期间、成本项目映射表达式
- 当前质量指标与 error_log 构建函数

## 输出

- 更少的 pandas 中间表：
  - 质量行数统计优先直接在 Polars 中完成
  - 白名单过滤优先使用 Polars 过滤或 join
  - 只在 anomaly / table rendering 仍需要 pandas 的边界做转换
- 更少的 Python UDF：
  - 稳定映射如成本项目分类优先表达式化
  - 月份解析若格式可控，优先使用 Polars 字符串表达式
- 回归结果：
  - contract baseline 不因纯性能改动变化
  - error_log 行数和 issue_type 集合不变化

## 伪代码草案

```python
# [伪代码草案]
# 目标：把可证明等价的 pandas 转换和 Python UDF 收敛到 Polars 表达式
# 输入：
# - qty_pl: 拆表后的数量页 Polars DataFrame
# - fact_bundle: Polars 事实集
# - product_order: 白名单产品顺序
# 输出：
# - optimized_artifacts: 与当前 AnalysisArtifacts 等价的产物
# - unchanged_contract: workbook/error_log 语义不变

def count_filtered_qty_rows_polars(qty_pl):
    # 为什么仍使用 Decimal 解析：金额和数量精度优先，不能为了速度直接 float 化。
    normalized = qty_pl.with_columns(
        normalize_money_expr("本期完工数量").alias("_completed_qty"),
        normalize_money_expr("本期完工金额").alias("_completed_amount"),
    )
    valid_qty = col("_completed_qty").is_not_null() & (col("_completed_qty") > 0)
    missing_amount = valid_qty & col("_completed_amount").is_null()
    return count(~valid_qty), count(missing_amount)

def filter_product_summary_polars(summary_frame, product_order):
    whitelist = pl.DataFrame(product_order, schema=["product_code", "product_name"])
    # 为什么 join 而不是 pandas MultiIndex：避免 product_summary_fact 为白名单过滤来回物化。
    return (
        whitelist.with_row_index("_order_idx")
        .join(summary_frame, on=["product_code", "product_name"], how="inner")
        .sort(["_order_idx", "period"])
        .drop("_order_idx")
    )

def replace_cost_item_mapping(detail_df):
    # 稳定枚举规则可用表达式；复杂或精度敏感规则暂不强行迁移。
    return detail_df.with_columns(
        when(col("cost_item").is_in(["直接材料"])).then("direct_material")
        .when(col("cost_item").is_in(["直接人工"])).then("direct_labor")
        .when(col("cost_item").str.starts_with("制造费用")).then("moh")
        .otherwise(None)
        .alias("cost_bucket")
    )
```

## 风险点 / 边界条件

- `Decimal` 精度是硬约束；不能把金额核心计算改成 float。
- `normalize_period()` 目前兼容多种文本格式，表达式化前必须先用测试锁定接受范围。
- `build_anomaly_sheet()` 仍是业务敏感区，第二优先级只优化外围转换，不强行重写异常评分。
- 每个替换点必须先写等价测试，确认旧输出与新输出一致。

## 验收标准

- 新增或更新测试覆盖过滤行数统计、白名单 Polars 过滤、成本项目表达式映射。
- `conda run -n test python -m pytest tests -q` 通过。
- `tests/contracts/baselines/` 不因纯性能改动变化。
- 真实 GB/SK benchmark 与第一优先级基线相比不能明显回退。


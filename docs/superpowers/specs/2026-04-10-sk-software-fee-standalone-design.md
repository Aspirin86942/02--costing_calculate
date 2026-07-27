# SK 软件费用独立成本项设计

## 1. 背景

当前仓库已经支持 `gb` / `sk` 双管线入口，但分析层对“独立成本项”的处理仍然是硬编码的：

- `委外加工费` 被视为独立展示、独立勾稽的特殊成本项
- 它不归入 `direct_material` / `direct_labor` / `moh`
- 它不参与三大类价量分析和工单异常评分
- 它参与总完工成本勾稽

现在确认了新的业务口径：

- 仅 `sk` 管线存在独立的 `软件费用`
- `软件费用` 的处理方式与 `委外加工费` 相同
- `gb` 保持现状，不新增 `软件费用` 口径

这里的关键不是“把软件费用并入制造费用”，而是“让软件费用在 `sk` 下成为第二个独立成本项”。

## 2. 目标

- `sk` 下将 `软件费用` 作为独立成本项处理
- `gb` 下维持当前输出与口径不变
- `软件费用` 在 `产品数量统计` 和 `按工单按产品异常值分析` 中单独展示金额与单位成本
- `软件费用` 不进入三大类价量分析
- `软件费用` 不进入工单异常分析的 `log`、`Modified Z-score`、异常等级和异常主要来源判定
- `sk` 的总成本勾稽口径改为：
  `直接材料 + 直接人工 + 制造费用 + 委外加工费 + 软件费用 = 总完工成本`
- `制造费用明细项合计是否等于制造费用合计` 仍然只校验制造费用明细，不包含 `委外加工费` 和 `软件费用`

## 3. 方案对比

### 方案 A：在分析层写死 `if pipeline == 'sk' and cost_item == '软件费用'`

优点：

- 改动表面最少

缺点：

- 管线差异继续散落在分析层
- 后续再出现 `sk` 特例时会继续堆条件分支
- 与当前已经存在的 `PipelineConfig` 分层方向不一致

### 方案 B：把“独立成本项”收敛到管线配置中，由分析层按配置执行

优点：

- `gb/sk` 差异留在配置层，边界清晰
- 可以把现有“委外加工费特例”也收敛为同一机制
- 后续新增独立成本项时只需要改配置和少量映射逻辑

缺点：

- 需要把当前写死的 `委外加工费` 逻辑改成配置驱动

### 方案 C：做完整成本规则引擎

优点：

- 扩展性最强

缺点：

- 当前需求明显过度设计
- 会扩大改动面和回归风险

## 4. 选择

采用方案 B。

核心设计是把“独立成本项”定义为管线配置的一部分：

- `gb`：`('委外加工费',)`
- `sk`：`('委外加工费', '软件费用')`

分析层只消费“哪些成本项属于独立展示口径”，不再知道这是 `gb` 还是 `sk` 的规则。

## 5. 设计细节

### 5.1 配置层

在 [`src/config/pipelines.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/config/pipelines.py) 的 `PipelineConfig` 中新增字段：

- `standalone_cost_items: tuple[str, ...]`

业务配置：

- `GB_PIPELINE.standalone_cost_items = ('委外加工费',)`
- `SK_PIPELINE.standalone_cost_items = ('委外加工费', '软件费用')`

### 5.2 ETL 编排层

在 [`src/etl/costing_etl.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/etl/costing_etl.py) 中让 `CostingWorkbookETL` 接收并保存 `standalone_cost_items`，并在构建分析产物时传给 [`src/analytics/qty_enricher.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/analytics/qty_enricher.py)。

在 [`src/etl/runner.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/etl/runner.py) 中从 `PipelineConfig` 注入该配置。

### 5.3 分析层

在 [`src/analytics/qty_enricher.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/analytics/qty_enricher.py) 中把当前“委外加工费单独处理”的逻辑改为“独立成本项集合驱动”：

- `cost_bucket` 仍只负责三大类映射
- `独立成本项` 不映射到 `cost_bucket`
- `sk` 的 `软件费用` 不再记入 `UNMAPPED_COST_ITEM`
- `gb` 如果出现 `软件费用`，仍保持未映射异常

聚合规则：

- 三大类分析只汇总 `cost_bucket` 非空数据
- 独立成本项单独按工单聚合
- 数量页输出为独立金额列和单位成本列
- 工单异常页输出独立金额列和单位成本列，但不输出对应的 `log`、`Modified Z-score` 和异常标记列

总成本勾稽规则：

- `qty_total_match` 从固定公式改为“`dm + dl + moh + standalone items` 对比总完工成本”
- `qty_check_reason` 与 `TOTAL_COST_MISMATCH` 的说明文字也要同步包含 `软件费用`

### 5.4 输出层

输出列采用“按数据存在与管线规则动态扩展”的方式，而不是给 `gb` 强行加 `软件费用` 空列。

这意味着：

- `gb` workbook schema 保持现状
- `sk` 在 `产品数量统计` 和 `按工单按产品异常值分析` 中新增：
  - `软件费用合计完工金额`
  - `软件费用单位完工成本`
- 样式、数字格式、列宽、条件格式只对“实际存在的列”生效

这样可以避免为了 `sk` 规则去修改 `gb` 的契约输出。

## 6. 错误处理与审计

- `sk` 的 `软件费用` 不进入 `UNMAPPED_COST_ITEM`
- `gb` 的 `软件费用` 仍进入 `UNMAPPED_COST_ITEM`
- `MISSING_AMOUNT` 继续适用于 `软件费用`
- `TOTAL_COST_MISMATCH` 的 `reason` / `action` 文本需要反映 `sk` 新口径
- `MOH_BREAKDOWN_MISMATCH` 继续仅针对制造费用明细与制造费用合计

## 7. 测试策略

先做最小红灯测试，再实现。

至少补以下测试：

- `tests/test_pipeline_config.py`
  - 校验 `GB_PIPELINE` 与 `SK_PIPELINE` 的 `standalone_cost_items`
- `tests/test_pq_analysis_v3.py`
  - `sk` 下 `软件费用` 不再视为未映射
  - `sk` 下数量页输出 `软件费用合计完工金额` 与 `软件费用单位完工成本`
  - `sk` 下总成本勾稽包含 `软件费用`
  - `sk` 下工单异常页不生成 `软件费用` 的 `log` / `Modified Z-score` / 异常标记
  - `gb` 下 `软件费用` 仍保持未映射
- `tests/test_costing_etl.py`
  - 补强 workbook 输出头部断言，确保 `sk` 的新增列可写出，`gb` 不被污染

如果现有 contract baseline 只覆盖 `gb`，则本次不应因为 `sk` 新规则去修改 `gb` baseline；必要时为 `sk` 增加针对性断言，而不是直接改动现有 `gb` baseline。

## 8. 风险点

最主要的回归风险有 4 个：

- 现有“委外加工费写死逻辑”改造成配置驱动时，把 `gb` 原有口径带坏
- 总成本勾稽公式改成动态后，异常原因文本与实际计算不一致
- 输出列改为动态扩展后，writer 的格式映射遗漏 `软件费用` 列
- `sk` 新增列如果被误纳入异常评分，会改变异常页结构和业务解释

## 9. 完成定义

- `gb` 单元测试与现有 baseline 保持通过
- `sk` 下 `软件费用` 完成独立展示和独立勾稽
- `sk` 价量分析页不出现 `软件费用`
- `sk` 工单异常页不对 `软件费用` 打分
- `pytest`、`ruff check`、`ruff format --check` 全部通过
- 真实入口至少验证一次 `python main.py sk`

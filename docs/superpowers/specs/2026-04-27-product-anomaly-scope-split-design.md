# GB 按产品异常值分析按生产口径拆段设计

## 1. 背景

当前仓库已经有两张与异常分析相关的输出：

- `按工单按产品异常值分析`：工单级异常检测主表，负责 `log`、`Modified Z-score`、异常等级、异常主要来源等核心逻辑
- `按产品异常值分析`：兼容摘要页，当前仅按 `产品 + 月份` 聚合展示总成本、完工数量、单位成本及三大类成本结构

现有问题是：`按产品异常值分析` 会把不同生产口径混在一起展示，导致“正常生产”和“返工生产”的成本特征无法拆开观察。

当前业务上已经确认如下单据类型分布与关注重点：

- `普通委外订单`、`返工委外订单`：存在，但不是当前重点关注对象
- `汇报入库-普通生产`、`直接入库-普通生产`：属于正常生产工单
- `汇报入库-返工生产`：属于返工生产工单

本次需求范围进一步确认后，只适用于 `gb` 管线，`sk` 保持现状不改。

目标不是重写异常检测算法，而是仅在 `gb` 下保留当前摘要页，并在同一张 `按产品异常值分析` sheet 下，让每个产品同时看到：

- 全部口径
- 正常生产口径
- 返工生产口径

## 2. 目标

- 保留现有 `按产品异常值分析` sheet，不新增 sheet
- 仅 `gb` 管线启用本次拆段规则
- `sk` 管线保持当前 `按产品异常值分析` 的单段摘要布局
- 保留现有 `按工单按产品异常值分析` 的检测逻辑、异常阈值和高亮行为
- 将 `gb` 下的 `按产品异常值分析` 从“每个产品一段”改为“每个产品按多个口径连续拆段展示”
- 固定输出顺序为：
  - `全部`
  - `正常生产`
  - `返工生产`
- `全部` 继续沿用当前全量口径，包含委外和其他未识别单据类型
- `正常生产` 仅包含：
  - `汇报入库-普通生产`
  - `直接入库-普通生产`
- `返工生产` 仅包含：
  - `汇报入库-返工生产`
- 若未来出现新的 `单据类型`，默认仅进入 `全部`，不进入 `正常生产` / `返工生产`

## 3. 方案对比

### 方案 A：保留 1 张摘要页，每个产品拆成多个连续分段

优点：

- 最符合业务表达，“放一起看，但拆开算”
- `全部` 口径可保留，避免历史认知断裂
- 改动集中在摘要聚合层和 writer 层，不需要触碰核心异常算法
- Excel 可读性最好，适合人工复核

缺点：

- 单个产品在摘要页会占用更多行
- workbook 相关布局测试和 contract baseline 需要同步更新

### 方案 B：保留 1 张摘要页，在表内增加 `分析口径` 列

优点：

- 数据结构改动较小
- writer 层几乎不用新增段落布局

缺点：

- 同一产品下多个口径混在一张小表里，可读性一般
- 与“每种口径独立成段”的业务阅读习惯不一致

### 方案 C：保留 1 张摘要页，改成宽表列展开

示例：

- `全部_总成本`
- `正常生产_总成本`
- `返工生产_总成本`

优点：

- 横向对比直接

缺点：

- 列会明显变宽，现有兼容摘要页的固定宽度与布局会变差
- writer、格式、维护成本最高
- 对当前需求明显过度设计

## 4. 选择

采用方案 A。

本次改动只改变 `gb` 下 `按产品异常值分析` 的聚合与展示结构，不改 `按工单按产品异常值分析` 的底层异常检测逻辑。

也就是说：

- 工单级异常分析仍然按当前逻辑运行
- `gb` 的产品级摘要页从“单口径月度汇总”变成“多口径月度汇总”
- `sk` 的产品级摘要页保持当前“单口径月度汇总”

## 5. 设计细节

### 5.1 业务分类规则

仅 `gb` 管线启用以下固定映射规则：

- `汇报入库-普通生产` -> `正常生产`
- `直接入库-普通生产` -> `正常生产`
- `汇报入库-返工生产` -> `返工生产`

以下类型不进入专项分段，但继续保留在 `全部`：

- `普通委外订单`
- `返工委外订单`
- 未来新增但未识别的 `单据类型`

这样做的原因是：

- 重点关注口径固定且明确，避免“专项口径”被低关注单据污染
- 同时保留 `全部` 作为历史兼容总览，不丢失全量信息

`sk` 不启用该分类映射，也不新增 `全部 / 正常生产 / 返工生产` 分段。

### 5.2 数据链路边界

本次改动建议放在摘要聚合层与摘要写出层：

- `src/config/pipelines.py`
- `src/analytics/fact_builder.py`
- `src/analytics/table_rendering.py`
- `src/analytics/contracts.py`
- `src/excel/sheet_writers.py`

不建议修改：

- `src/analytics/anomaly.py` 中的工单异常评分逻辑
- 工单异常页的颜色规则
- 三张 `价量比` sheet 的输出逻辑

原因是本次需求本质上是“`gb` 产品摘要口径拆分”，不是“异常检测算法重定义”。

### 5.3 管线开关

由于本次需求只对 `gb` 生效，建议把摘要页布局模式放入管线配置，而不是在 writer 或聚合函数内部硬编码判断。

建议在 [`src/config/pipelines.py`](D:/Program_python/02--costing_calculate/src/config/pipelines.py) 的 `PipelineConfig` 中新增一个轻量配置字段，例如：

- `product_anomaly_scope_mode: str`

建议值：

- `gb`：`doc_type_split`
- `sk`：`legacy_single_scope`

这样做的原因是：

- 管线差异停留在配置层，边界更清晰
- 后续如果 `sk` 将来也要启用类似能力，只需要调整配置和少量实现
- 可以明确保证 `sk` 本次不被误改

### 5.4 Fact / Summary 层调整

当前 `qty_fact` 已保留原始 `单据类型` 列，本次只需要把该字段继续带入 `work_order_fact`，例如保留为 `doc_type`。

随后在产品摘要构建阶段：

1. 先读取当前管线的 `product_anomaly_scope_mode`
2. 若模式为 `legacy_single_scope`，沿用当前单段摘要逻辑
3. 若模式为 `doc_type_split`，对每条工单记录按 `doc_type` 计算专项口径标签
4. 为每个产品先构建 `全部` 口径汇总
5. 再按专项口径分别构建 `正常生产`、`返工生产` 汇总
6. 若专项口径没有数据，则不输出空段

这里“全部”必须继续沿用当前全量口径，不能因为新增专项段而变成“仅自产口径”，否则会破坏现有业务含义。

### 5.5 摘要分段契约调整

当前 `ProductAnomalySection` 只能表达“一个产品的一张汇总小表”。  
为了表达“一个产品下的多个口径分段”，需要扩展该契约，建议新增：

- `section_label: str`

可选值固定为：

- `全部`
- `正常生产`
- `返工生产`

这样 writer 在不理解聚合细节的前提下，也能按契约直接写出正确分段。

为了兼容 `sk` 现有布局，`section_label` 可以允许为空：

- `gb/doc_type_split`：使用 `全部` / `正常生产` / `返工生产`
- `sk/legacy_single_scope`：保持空值或默认单段，不强制写出 `分析口径`

### 5.6 Excel 布局调整

`按产品异常值分析` 当前布局为：

- 第 1 行：总标题
- 第 3 行：`产品编码` / `产品名称`
- 第 4 行：对应值
- 第 5 行：表头
- 第 6 行开始：月度数据

改造后建议变为：

- 第 1 行：总标题
- 对每个产品：
  - 先写 `产品编码` / `产品名称`
  - 再依次写 `全部`、`正常生产`、`返工生产` 分段
  - 每个分段增加一行 `分析口径`
  - 分段下方继续使用现有列头和月度数据结构

这样做的原因是：

- 每个口径视觉上独立，适合人工比较
- 仍然保留在同一产品块内，避免跨 sheet 或跨区域跳转
- 不需要改变现有列格式、列宽、数值显示方式

`sk` 继续沿用当前布局，不增加 `分析口径` 行，也不改现有行号语义。

### 5.7 指标口径

每个分段内部继续输出当前摘要页已有指标：

- `总成本`
- `完工数量`
- `单位成本`
- `直接材料成本`
- `单位直接材料成本`
- `直接材料贡献率`
- `直接人工成本`
- `单位直接人工成本`
- `直接人工贡献率`
- `制造费用成本`
- `单位制造费用成本`
- `制造费用贡献率`

这些指标的计算口径不变，只是把输入数据从“全量产品工单”换成“某个产品下、某个分析口径对应的工单子集”。

## 6. 错误处理与边界条件

- `按工单按产品异常值分析` 不改算法、不改阈值、不改高亮
- `按产品异常值分析` 仍然只是兼容摘要页，不承担异常评分
- `gb` 启用多分段布局，`sk` 保持当前单段布局
- 新的 `单据类型` 不报错、不写 `error_log`
- 新的 `单据类型` 仅进入 `全部`，不误归类到 `正常生产` / `返工生产`
- 某个产品如果没有 `正常生产` 或 `返工生产` 数据，对应空段不输出
- workbook 的标准 7 张 sheet 数量保持不变
- `产品数量统计`、`error_log`、质量校验摘要、三张 `价量比` 不因本次需求新增规则

## 7. 测试策略

先补最小红灯测试，再实现。

至少覆盖以下测试：

- `tests/test_costing_etl.py`
  - `gb` workbook 输出中 `按产品异常值分析` 的新布局
  - `gb` 同一产品下会出现多个 `分析口径` 分段
  - `gb` 分段顺序固定为 `全部`、`正常生产`、`返工生产`
  - `sk` workbook 输出仍保持当前单段布局
- `tests/contracts/test_workbook_contract.py`
  - `gb` 摘要页语义布局变更后的契约断言
  - `sk` 摘要页契约保持不变
- `tests/contracts/baselines/workbook_semantics.json`
  - 更新 `gb` 的 `按产品异常值分析` 元信息、表头位置和首段布局基线
  - 不应因为本次需求破坏 `sk` 现有基线语义
- 建议补充 `table_rendering` 级测试
  - 验证 `gb` 下 `汇报入库-普通生产` 和 `直接入库-普通生产` 会被并入 `正常生产`
  - 验证 `gb` 下 `汇报入库-返工生产` 会进入 `返工生产`
  - 验证 `gb` 下 `普通委外订单`、`返工委外订单` 只进入 `全部`
  - 验证 `gb` 下未来未识别 `单据类型` 只进入 `全部`
  - 验证 `sk` 继续走 legacy 单段摘要逻辑

## 8. 验收标准

- `gb` 下对任一产品，`按产品异常值分析` 至少保留 `全部` 分段
- `gb` 下当该产品存在 `汇报入库-普通生产` 或 `直接入库-普通生产` 数据时，输出 `正常生产` 分段
- `gb` 下当该产品存在 `汇报入库-返工生产` 数据时，输出 `返工生产` 分段
- `gb` 下 `全部` 继续包含委外和其他未识别单据类型
- `gb` 下同一产品内，分段顺序固定为：
  - `全部`
  - `正常生产`
  - `返工生产`
- `gb` 各分段的列结构、数字格式、列宽规则保持与当前摘要页一致
- `sk` 的 `按产品异常值分析` 布局和现有行为保持不变
- `按工单按产品异常值分析` 的输出内容和异常逻辑不发生行为变化

## 9. 伪代码草案

### 9.1 目标

- 保留当前 `按产品异常值分析` sheet
- 仅在 `gb` 下将每个产品拆成 `全部 / 正常生产 / 返工生产` 多个连续分段
- 不改工单异常检测，只改产品摘要聚合与写出结构

### 9.2 输入

- `work_order_fact`：工单级事实表，至少包含：
  - `product_code`
  - `product_name`
  - `period`
  - `doc_type`
  - `completed_qty`
  - `completed_amount_total`
  - `dm_amount`
  - `dl_amount`
  - `moh_amount`
- `scope_mode`：当前管线的摘要布局模式，例如 `doc_type_split` 或 `legacy_single_scope`
- `classification_rules`：单据类型到分析口径的固定映射

### 9.3 输出

- `product_anomaly_sections`：供 writer 直接消费的分段列表
- `gb/doc_type_split` 下，每个 section 对应一个“产品 + 分析口径”的月度摘要块
- `sk/legacy_single_scope` 下，继续返回当前单段摘要块
- 未识别单据类型不会报错，但只会出现在 `全部`

### 9.4 伪代码草案

```python
# 目标：只在 gb 下把产品摘要页从“一个产品一张月度表”改成“一个产品下多个分析口径分段”

DOC_TYPE_SCOPE_MAP = {
    '汇报入库-普通生产': '正常生产',
    '直接入库-普通生产': '正常生产',
    '汇报入库-返工生产': '返工生产',
}

SECTION_ORDER = ['全部', '正常生产', '返工生产']


def classify_scope(doc_type: str | None) -> str | None:
    # 为什么这样做：
    # - 新类型不能把程序跑挂
    # - 也不能被错误归入“正常生产 / 返工生产”污染专项统计
    if doc_type is None:
        return None
    return DOC_TYPE_SCOPE_MAP.get(str(doc_type).strip())


def build_product_anomaly_sections(work_order_df, scope_mode):
    # 1. 先做管线分流，确保 sk 保持现状，不被 gb 规则误伤
    if scope_mode == 'legacy_single_scope':
        return build_legacy_sections(work_order_df)

    # 2. gb 模式下，给每条工单补充专项口径标签
    scoped_df = work_order_df.copy()
    scoped_df['section_scope'] = scoped_df['doc_type'].map(classify_scope)

    sections = []

    # 3. 逐产品构建展示块，保持现有产品顺序
    for (product_code, product_name), product_rows in groupby_product(scoped_df):
        # 4. 全部口径：沿用当前全量数据，不做单据类型过滤
        all_summary = aggregate_by_period(product_rows)
        sections.append(
            build_section(
                product_code=product_code,
                product_name=product_name,
                section_label='全部',
                summary_rows=all_summary,
            )
        )

        # 5. 专项口径：只有存在数据时才输出，避免生成空段
        for section_label in ('正常生产', '返工生产'):
            scope_rows = product_rows[product_rows['section_scope'] == section_label]
            if scope_rows.empty:
                continue

            scope_summary = aggregate_by_period(scope_rows)
            sections.append(
                build_section(
                    product_code=product_code,
                    product_name=product_name,
                    section_label=section_label,
                    summary_rows=scope_summary,
                )
            )

    return sections


def aggregate_by_period(scope_rows):
    # 6. 继续沿用现有“按产品 + 月份”的汇总指标
    grouped = scope_rows.groupby(['period'], sort=False).agg(
        total_cost=sum('completed_amount_total'),
        completed_qty=sum('completed_qty'),
        dm_cost=sum('dm_amount'),
        dl_cost=sum('dl_amount'),
        moh_cost=sum('moh_amount'),
    )

    grouped['unit_cost'] = safe_divide(grouped['total_cost'], grouped['completed_qty'])
    grouped['dm_unit_cost'] = safe_divide(grouped['dm_cost'], grouped['completed_qty'])
    grouped['dl_unit_cost'] = safe_divide(grouped['dl_cost'], grouped['completed_qty'])
    grouped['moh_unit_cost'] = safe_divide(grouped['moh_cost'], grouped['completed_qty'])
    grouped['dm_contrib'] = safe_divide(grouped['dm_cost'], grouped['total_cost'])
    grouped['dl_contrib'] = safe_divide(grouped['dl_cost'], grouped['total_cost'])
    grouped['moh_contrib'] = safe_divide(grouped['moh_cost'], grouped['total_cost'])
    return grouped


def write_product_anomaly_sheet(sections):
    # 7. writer 层按 section_label 判断布局：
    # - gb：产品编码 / 产品名称 + 分析口径标签 + 月度摘要表
    # - sk：保持现有单段布局
    pass
```

## 10. 风险点

最主要的回归风险有 4 个：

- `doc_type` 在 `gb/work_order_fact` 链路上如果没有被正确保留，会导致分段逻辑拿不到分类输入
- writer 改成多分段布局后，现有冻结窗格、筛选区域、表头定位测试可能失效
- 如果错误地把“未识别单据类型”归入专项分段，会污染 `正常生产` / `返工生产` 成本
- 如果实现时没有正确按管线分流，可能把 `sk` 现有摘要布局误改掉
- 如果实现时误动 `anomaly.py`，可能把本次展示需求扩大成异常算法回归

## 11. 完成定义

- `gb` 的 `按产品异常值分析` 成功改为每个产品下的多口径分段布局
- `gb` 的 `全部` / `正常生产` / `返工生产` 分类与聚合口径符合业务确认
- `gb` 的 `全部` 保持包含委外和其他未识别类型
- `sk` 的 `按产品异常值分析` 继续保持现有行为与布局
- `按工单按产品异常值分析` 行为不变
- workbook 标准 7 张 sheet 仍正常导出
- 相关单测、contract 测试、基线更新全部通过

# 工单异常分析架构收口优化 Spec

## 1. 背景

当前项目已经完成 GUI 移除和 Python 3.11 化，并且 `成本分析工单维度` sheet 已经完成第一轮瘦身：

- 不再输出 `log_*` 列；
- 不再输出 `Modified Z-score_*` 列；
- 不再输出逐项 `*异常标记` 列；
- 保留 `异常等级`、`异常主要来源`、`异常明细解释`、`复核原因`；
- `异常明细解释` 保持日志型解释，继续包含当前值、当前 log、基准值、基准 log、log 偏离、相对偏离、score、有效工单数、原始 MAD、有效 MAD 等复核依据；
- 其他 sheet 不改。

当前 `成本分析工单维度` 已经从约 62 列降到约 35 列，但 `src/analytics/anomaly.py` 和 `src/analytics/presentation_builder.py` 里还有几处历史结构残留：

1. `build_anomaly_sheet()` 同时负责异常计算、异常汇总、异常解释、字段重命名、最终展示列筛选；
2. `presentation_builder.py` 仍然调用 `build_work_order_conditional_formats()`，但最终展示列已不再包含逐项 `*异常标记`，条件格式实际已经空转；
3. `anomaly.py` 的 `rename_map` 里还保留了不再输出的 `log_*` / `Modified Z-score_*` 中文映射；
4. `成本分析工单维度` 已经变成普通平铺 sheet，可以切换到 lightweight fast export。

本次优化目标是把这些点一次性收口。

---

## 2. 总目标

将工单异常分析从“一个函数同时计算和展示”改成更清晰的两层结构：

```text
异常计算层：
build_anomaly_internal_frame()
负责完整计算，保留内部 log / modified_z / flags / MAD / 样本数等列。

展示输出层：
build_work_order_output_sheet()
负责把 internal frame 转成最终 Excel 可见列，只输出审计复核需要看的字段。
```

同时：

```text
工单页不再构造条件格式；
工单页切换为 lightweight fast writer；
删除不再需要的输出映射冗余；
保持最终 Excel 业务口径不变。
```

---

## 3. 严格范围

### 3.1 允许改

```text
src/analytics/anomaly.py
src/analytics/presentation_builder.py
src/excel/fast_writer.py  # 仅当现有 fast writer 需要很小兼容时
tests/contracts/*
tests/test_pq_analysis_v3.py
tests/test_costing_etl.py
其他与工单异常 sheet 输出契约直接相关的测试
```

### 3.2 不允许改

```text
成本计算单总表 的字段和口径
成本计算单数量聚合维度 的字段和口径
成本分析产品维度 的字段和口径
异常检测算法口径
MAD / Modified Z-score 计算方式
正常生产 / 返工生产分池逻辑
产品白名单逻辑
月份过滤逻辑
固定窗格
筛选
```

### 3.3 不要顺手做

```text
不要拆 fact_builder.py
不要重构整个 ETL pipeline
不要新增 GUI
不要删除 services 层
不要改 CLI 参数
不要改输出 sheet 顺序
不要改异常明细解释的日志型风格
不要把异常明细解释改成口语化老板汇报语言
```

---

## 4. 设计目标

### 4.1 异常计算层

新增或重命名为类似：

```python
def build_anomaly_internal_frame(...):
    ...
```

职责：

```text
输入：工单级 work_order_df
输出：包含完整内部异常计算字段的 DataFrame
```

内部可以保留：

```text
log_total_unit_cost
log_dm_unit_cost
log_dl_unit_cost
log_moh_unit_cost
...

modified_z_total_unit_cost
modified_z_dm_unit_cost
modified_z_dl_unit_cost
modified_z_moh_unit_cost
...

audit_pool_sample_size_*
audit_pool_center_log_*
audit_pool_raw_mad_*
audit_pool_effective_mad_*
audit_relative_deviation_*

总成本异常标记
直接材料异常标记
直接人工异常标记
制造费用异常标记
...
```

注意：这些字段是**内部计算字段**，可以存在于 internal frame 中，但不得直接出现在最终 Excel sheet。

### 4.2 展示输出层

新增或重命名为类似：

```python
def build_work_order_output_sheet(...):
    ...
```

职责：

```text
输入：internal anomaly frame
输出：FlatSheet，用于最终 Excel 输出
```

最终输出列只允许包含当前确认过的可见字段。

核心字段包括：

```text
月份
成本中心
产品编码
产品名称
规格型号
工单编号
工单行
生产类型
基本单位
本期完工数量
总完工成本
直接材料合计完工金额
直接人工合计完工金额
制造费用合计完工金额
制造费用_其他合计完工金额
制造费用_人工合计完工金额
制造费用_机物料及低耗合计完工金额
制造费用_折旧合计完工金额
制造费用_水电费合计完工金额
委外加工费合计完工金额（如当前 pipeline 配置包含）
软件费用合计完工金额（如当前 pipeline 配置包含）
总单位完工成本
直接材料单位完工成本
直接人工单位完工成本
制造费用单位完工成本
制造费用_其他单位完工成本
制造费用_人工单位完工成本
制造费用_机物料及低耗单位完工成本
制造费用_折旧单位完工成本
制造费用_水电费单位完工成本
委外加工费单位完工成本（如当前 pipeline 配置包含）
软件费用单位完工成本（如当前 pipeline 配置包含）
是否可参与分析
异常等级
异常主要来源
异常明细解释
复核原因
```

禁止最终输出：

```text
log_*
Modified Z-score_*
*异常标记
audit_pool_*
audit_relative_deviation_*
```

---

## 5. 异常明细解释要求

`异常明细解释` 继续保持日志型解释，不改成业务口语化描述。

示例风格：

```text
总成本: 高度可疑, 当前值=50.00, 当前log=3.9120, 基准值=11.00, 基准log=2.3979, log偏离=1.5141, 相对偏离=354.55%, score=10.58, 有效工单数=3, 原始MAD=0.0953, 有效MAD=0.0953
```

多个异常项之间继续用分号连接：

```text
总成本: 高度可疑, ...; 直接材料: 关注, ...
```

必须保留的信息：

```text
异常项名称
异常等级
当前值
当前log
基准值
基准log
log偏离
相对偏离
score
有效工单数
原始MAD
有效MAD
```

---

## 6. 条件格式处理

当前最终工单页不再输出逐项 `*异常标记` 列，因此原来的条件格式已经失去依据。

本次要求：

```text
成本分析工单维度 不再构造条件格式。
```

具体要求：

1. `presentation_builder.py` 不应再为 `成本分析工单维度` 调用或传入 `build_work_order_conditional_formats()`。
2. `成本分析工单维度` 的 `SheetModel.conditional_formats` 应为：

```python
()
```

3. 可以保留 `build_work_order_conditional_formats()` 函数本身，作为历史兼容或其他潜在用途；但工单页当前路径不得依赖它。
4. 如果该函数已经没有任何调用，可以考虑保留但不使用，或者删除对应测试；但不要因此扩大重构范围。

---

## 7. Fast writer 要求

`成本分析工单维度` 应切换到 lightweight fast export。

在 `src/analytics/presentation_builder.py` 中，`work_order_model` 应使用：

```python
write_mode='dataframe_fast'
style_profile='lightweight_flat'
source_frame=work_order_frame
```

同时保留：

```python
freeze_panes='A2'  # 默认即可
auto_filter=True   # 默认即可
```

要求：

1. 工单页继续保留固定窗格；
2. 工单页继续保留筛选；
3. 不改变 sheet 顺序；
4. 不改变最终可见列；
5. 不改变数值格式；
6. 不改变异常明细解释内容；
7. 不引入条件格式；
8. fast writer 不应吞掉错误，如果传入了非空 `conditional_formats`，应继续失败或显式拒绝。

---

## 8. rename_map 清理要求

`anomaly.py` 当前 `rename_map` 中还保留了不再输出的内部字段映射，例如：

```python
'log_total_unit_cost': 'log_总单位完工成本'
'modified_z_total_unit_cost': 'Modified Z-score_总单位完工成本'
...
```

本次应清理这些最终展示层不再需要的中文映射。

要求：

1. `rename_map` 只保留最终输出列需要的字段映射；
2. 内部计算字段不再映射成 Excel 中文展示名；
3. 不影响 `异常明细解释` 生成，因为解释应基于 internal frame 的内部字段；
4. 不影响异常等级和异常主要来源计算；
5. 不影响 standalone cost items，例如：

```text
委外加工费
软件费用
```

这些仍应能动态插入最终输出列。

---

## 9. 推荐函数结构

可以参考以下结构，但不强制完全同名：

```python
def build_anomaly_internal_frame(
    work_order_df: pd.DataFrame,
) -> pd.DataFrame:
    """构建完整工单异常计算结果，包含内部算法字段。"""
    ...


def build_work_order_output_sheet(
    anomaly_df: pd.DataFrame,
    standalone_metas: tuple[StandaloneCostItemMeta, ...],
) -> FlatSheet:
    """把内部异常结果转换为最终 Excel 可见 FlatSheet。"""
    ...


def build_anomaly_sheet(
    work_order_df: pd.DataFrame,
    standalone_metas: tuple[StandaloneCostItemMeta, ...] | None = None,
) -> FlatSheet:
    """兼容外部调用：计算 internal frame 后生成最终 output sheet。"""
    if standalone_metas is None:
        standalone_metas = resolve_standalone_cost_item_metas(DEFAULT_STANDALONE_COST_ITEMS)
    internal_frame = build_anomaly_internal_frame(work_order_df)
    return build_work_order_output_sheet(internal_frame, standalone_metas)
```

这样可以保持现有外部调用不变：

```python
build_anomaly_sheet(...)
```

但内部职责更清楚。

---

## 10. 测试要求

### 10.1 算法内部测试

新增或调整测试，直接测：

```python
build_anomaly_internal_frame(...)
```

验证：

```text
log_* 内部字段存在
modified_z_* 内部字段存在
异常标记内部字段存在
audit_pool_sample_size_* 存在
audit_pool_center_log_* 存在
audit_pool_raw_mad_* 存在
audit_pool_effective_mad_* 存在
```

这些测试用于保证算法没有因为最终 Excel 隐藏列而被误删。

### 10.2 最终输出测试

继续验证：

```text
最终 FlatSheet / workbook 不包含：
- log_*
- Modified Z-score_*
- *异常标记
- audit_pool_*
- audit_relative_deviation_*
```

继续验证：

```text
最终 FlatSheet / workbook 包含：
- 异常等级
- 异常主要来源
- 异常明细解释
- 复核原因
```

继续验证：

```text
异常明细解释包含日志型字段：
- 当前值=
- 当前log=
- 基准值=
- 基准log=
- log偏离=
- 相对偏离=
- score=
- 有效工单数=
- 原始MAD=
- 有效MAD=
```

### 10.3 SheetModel / fast writer 测试

验证 `成本分析工单维度` 的 SheetModel：

```python
assert model.sheet_name == '成本分析工单维度'
assert model.write_mode == 'dataframe_fast'
assert model.style_profile == 'lightweight_flat'
assert model.source_frame is not None
assert model.conditional_formats == ()
assert model.freeze_panes == 'A2'
assert model.auto_filter is True
```

### 10.4 workbook contract 测试

更新并验证：

```text
tests/contracts/baselines/workbook_semantics.json
```

要求：

```text
成本分析工单维度仍为当前确认后的 35 列左右
不出现 log_* / Modified Z-score_* / *异常标记
固定窗格仍存在
筛选仍存在
数值格式仍正确
sheet 顺序不变
```

---

## 11. 验收标准

### 11.1 功能验收

```text
1. 工单异常算法结果不变；
2. 异常等级不变；
3. 异常主要来源不变；
4. 异常明细解释不变或仅有非业务含义的格式整理；
5. 最终 Excel 工单页不再出现算法展开列；
6. 其他 3 张 sheet 不变；
7. 固定窗格和筛选保留；
8. 工单页走 fast writer。
```

### 11.2 结构验收

```text
1. build_anomaly_sheet 不再同时承担全部职责；
2. 存在清晰的 internal anomaly frame 构建函数；
3. 存在清晰的 output sheet 构建函数；
4. 条件格式逻辑不再参与工单页当前输出；
5. rename_map 不再包含最终不输出的 log / modified_z 中文映射。
```

### 11.3 测试验收

至少运行：

```bash
uv run python -m pytest tests/architecture -q --basetemp .pytest-tmp
uv run python -m pytest tests/contracts -q --basetemp .pytest-tmp
uv run python -m pytest tests/test_pq_analysis_v3.py -q --basetemp .pytest-tmp
uv run python -m pytest tests/test_costing_etl.py -q --basetemp .pytest-tmp
```

如果项目已有完整测试入口，也可以运行：

```bash
uv run python -m pytest -q --basetemp .pytest-tmp
```

注意：Windows 上如果默认临时目录 `C:\Users\lcf\AppData\Local\Temp\pytest-of-lcf` 权限异常，使用项目内：

```bash
--basetemp .pytest-tmp
```

---

## 12. 风险点

### 风险 1：内部字段被误删

不能因为最终 Excel 不展示 `log_*` / `Modified Z-score_*`，就把内部计算字段删掉。

它们仍然用于：

```text
异常等级
异常主要来源
异常明细解释
测试验证
```

### 风险 2：standalone cost items 动态列被破坏

当前有动态成本项：

```text
委外加工费
软件费用
```

它们需要继续动态插入最终工单页。

特别注意：

```text
金额列插在 总单位完工成本 前
单位成本列插在 是否可参与分析 前
```

或者保持当前既有顺序，不要破坏 workbook contract。

### 风险 3：fast writer 路径丢格式

切换 fast writer 后要确认：

```text
金额格式
单位成本格式
数量格式
固定窗格
筛选
列宽
```

仍符合当前 contract。

### 风险 4：条件格式残留导致 fast writer 失败

`write_sheet_model_as_lightweight_table()` 当前可能拒绝非空 `conditional_formats`。

所以工单页必须明确保证：

```python
conditional_formats == ()
```

### 风险 5：把异常明细解释改得太“人话”

不要把日志型解释改成汇报语言。

本项目当前要求是：

```text
异常明细解释 = 审计复核日志
```

不是：

```text
老板汇报摘要
```

---

## 13. 推荐 commit 拆分

如果要拆 commit，建议：

### Commit 1

```text
refactor(analytics): split anomaly calculation from work order output
```

内容：

```text
拆 build_anomaly_internal_frame / build_work_order_output_sheet
清理 rename_map
保持最终输出不变
更新算法/输出测试
```

### Commit 2

```text
perf(excel): export work order anomaly sheet with fast writer
```

内容：

```text
工单页 SheetModel 切换 dataframe_fast/lightweight_flat
取消当前工单页条件格式传入
更新 contract 和 writer 相关测试
```

如果想一个 commit，也可以：

```text
refactor(excel): separate anomaly internals and fast-export work order sheet
```

---

## 14. 给 Codex 的最终提示词

```text
请在 D:/python_program/02--costing_calculate 项目中完成一次工单异常分析架构收口优化。

目标：
1. 将 build_anomaly_sheet 拆成“内部异常计算”和“最终展示输出”两层。
2. 明确取消当前工单页条件格式路径。
3. 清理不再输出的 log / modified_z 中文 rename_map 映射。
4. 将 `成本分析工单维度` sheet 切换到 lightweight fast export。
5. 保持最终 Excel 业务输出口径不变。

背景：
当前 `成本分析工单维度` 已经不再输出 `log_*`、`Modified Z-score_*`、逐项 `*异常标记` 列。最终可见字段只保留审计复核需要的字段，包括 `异常等级`、`异常主要来源`、`异常明细解释`、`复核原因`。其中 `异常明细解释` 必须继续保持日志型解释，包含当前值、当前log、基准值、基准log、log偏离、相对偏离、score、有效工单数、原始MAD、有效MAD 等复核依据。

具体要求：
1. 在 `src/analytics/anomaly.py` 中拆出内部计算函数，例如 `build_anomaly_internal_frame()`，它返回完整内部异常计算 DataFrame，允许包含 log、modified_z、异常标记、MAD、样本数等内部字段。
2. 在 `src/analytics/anomaly.py` 中拆出展示输出函数，例如 `build_work_order_output_sheet()`，它负责把 internal frame 转成最终 Excel 的 FlatSheet。
3. 保持 `build_anomaly_sheet()` 作为兼容入口：内部调用 `build_anomaly_internal_frame()` 和 `build_work_order_output_sheet()`。
4. 最终 FlatSheet 不得包含：
   - `log_*`
   - `Modified Z-score_*`
   - `*异常标记`
   - `audit_pool_*`
   - `audit_relative_deviation_*`
5. 最终 FlatSheet 必须包含：
   - `异常等级`
   - `异常主要来源`
   - `异常明细解释`
   - `复核原因`
6. 不改变异常算法口径，不改变 Modified Z-score / MAD / 生产类型分池逻辑。
7. 不改变 `异常明细解释` 的日志型内容。
8. 清理 `rename_map` 中不再输出的 log / modified_z 中文展示映射，避免误导。
9. 在 `src/analytics/presentation_builder.py` 中，`成本分析工单维度` 的 SheetModel 不再调用或传入工单条件格式。`conditional_formats` 应为空。
10. 将 `成本分析工单维度` 的 SheetModel 切换为：
    - `write_mode='dataframe_fast'`
    - `style_profile='lightweight_flat'`
    - `source_frame=work_order_frame`
11. 保留固定窗格和筛选：
    - `freeze_panes == 'A2'`
    - `auto_filter is True`
12. 不改变其他 sheet：
    - `成本计算单总表`
    - `成本计算单数量聚合维度`
    - `成本分析产品维度`
13. 不改 CLI 参数，不改 sheet 顺序，不改产品白名单、月份过滤、standalone cost item 逻辑。
14. 更新测试：
    - 内部异常计算测试：验证 internal frame 仍有 log / modified_z / flags / MAD / 样本数等内部字段。
    - 输出测试：验证最终 FlatSheet/workbook 不含算法展开列，但保留异常解释字段。
    - SheetModel 测试：验证工单页走 dataframe_fast/lightweight_flat，conditional_formats 为空，freeze_panes/filter 保留。
    - workbook contract baseline 同步。
15. 运行验证：
    - `uv run python -m pytest tests/architecture -q --basetemp .pytest-tmp`
    - `uv run python -m pytest tests/contracts -q --basetemp .pytest-tmp`
    - `uv run python -m pytest tests/test_pq_analysis_v3.py -q --basetemp .pytest-tmp`
    - `uv run python -m pytest tests/test_costing_etl.py -q --basetemp .pytest-tmp`
    - 如可行，运行完整 `uv run python -m pytest -q --basetemp .pytest-tmp`

注意：
- 不要拆 fact_builder.py。
- 不要重构整个 ETL pipeline。
- 不要新增或恢复 GUI。
- 不要取消固定窗格和筛选。
- 不要把异常明细解释改成口语化说明。
- 不要改变最终 Excel 业务口径。
```

---

## 15. 一句话版

这一轮不是继续删列，而是把“异常计算后台账本”和“Excel 前台展示表”拆开，同时让工单页走 fast writer，把上一轮瘦身后的架构彻底收口。

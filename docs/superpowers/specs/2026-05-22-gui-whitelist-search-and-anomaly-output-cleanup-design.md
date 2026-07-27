# GUI 白名单搜索与异常输出收敛设计

## 目标

本次改动解决三个可用性问题：

1. GUI 在把候选产品加入白名单时，支持按产品编码或产品名称搜索，避免产品很多时逐行查找。
2. `成本分析工单维度` 收敛异常解释字段，删除当前 5 个容易产生歧义的池参数列，改为单列 `异常明细解释`。
3. `成本分析产品维度` 去掉 `四、按单个产品异常值分析` 标题，并减少顶部空行，让 sheet 从有效内容开始。

本次采用方案 A：最小可用改造。改动边界集中在 GUI 候选产品显示、工单异常输出字段、产品维度 sheet 布局，不改变异常算法、白名单业务匹配规则、GB/SK 管线口径或 workbook sheet 清单。

## 输入

关键入参：

- GUI 当前管线：`gb` 或 `sk`
- GUI 当前输入 workbook 路径
- 预检或扫描后返回的 `CostingRunResult.candidate_products`
- GUI 白名单表中的 `product_order`
- `build_anomaly_sheet()` 已计算出的每个异常指标的 log、score、MAD、有效样本池信息
- `ProductAnomalySection` / `SheetModel` 中的产品维度数据

上下文信息：

- 白名单业务匹配仍然是 `产品编码 + 产品名称` 双字段精确匹配。
- 候选产品来自当前输入 workbook 的标准化结果，搜索只影响 GUI 显示，不改变候选产品来源。
- 工单异常评分按当前逻辑使用 `product_code + product_name + production_scope` 建池，并按具体成本指标分别计算。
- `成本分析产品维度` 当前由 `src/excel/product_anomaly_writer.py` 的特殊布局 writer 输出。

外部依赖：

- PySide6 GUI 控件：`QLineEdit`、`QTableWidget`
- pandas / Polars 分析结果
- xlsxwriter workbook 写出
- openpyxl 契约测试读取结果

运行环境：

- Linux
- Python 3.11+
- Conda `test` 环境
- GUI 启动命令：`/home/george/miniconda3/bin/conda run -n test python -m src.gui.app`
- 测试命令：`/home/george/miniconda3/bin/conda run -n test python -m pytest tests -q`

## 输出

成功返回：

- GUI 候选产品区域新增搜索能力。
- `成本分析工单维度` 删除旧 5 列解释字段，并新增 `异常明细解释`。
- `成本分析产品维度` 不再写大标题，顶部布局更紧凑。
- workbook contract baseline 按明确变化更新。

失败返回：

- GUI 搜索输入非法时不报错，按无匹配显示空候选表。
- 异常解释字段生成遇到缺失内部审计值时跳过该异常项的文本拼接，不改变异常等级和异常主要来源。
- 产品维度没有 section 时仍生成空 sheet，不抛出布局错误。

副作用：

- 修改 GUI 代码、异常 sheet 构建代码、产品维度 writer、相关测试和 README/AGENTS 业务规则说明。
- 不写数据库，不访问网络，不新增外部文件产物。

重试、降级或人工处理：

- GUI 搜索为空时自动降级为显示全部候选产品。
- 若输入 workbook 未先扫描或预检，候选产品表为空，搜索框不触发后台任务。
- 如果异常项没有达到 `关注` 或 `高度可疑`，`异常明细解释` 留空。

## 设计

### 1. GUI 候选产品包含搜索

在 `候选产品` 表上方增加搜索框：

```text
搜索产品编码或产品名称
```

搜索规则：

- 搜索只过滤候选产品表，不影响白名单表。
- 产品编码按包含匹配：候选产品编码只要包含搜索词就显示。
- 产品名称也按包含匹配。
- 英文字母忽略大小写。
- 搜索词自动 `strip()` 前后空格。
- 搜索为空时显示全部候选产品。
- 搜索不改变候选产品原始顺序。
- `加入白名单` 只加入当前表格里选中的候选产品。
- 重复产品沿用现有逻辑跳过。

明确不改变：

- 白名单业务过滤仍然按 `产品编码 + 产品名称` 精确匹配。
- 不把白名单改成通配符匹配、前缀匹配或产品编码模糊匹配。
- 不新增单独的产品主数据文件或缓存。

建议实现边界：

- `MainWindow` 增加 `candidate_search_edit: QLineEdit`。
- `MainWindow` 保存完整候选产品列表，例如 `self.candidate_products_all`。
- `_on_worker_finished()` 成功后把 `result.candidate_products` 存入完整列表，再应用当前搜索词刷新表格。
- `_invalidate_precheck()`、失败任务、管线切换、清空条件时清空完整候选列表和搜索框。

### 2. 工单异常解释字段收敛

删除 `成本分析工单维度` 当前 5 个解释列：

```text
异常池样本数
异常池中心log值
异常池原始MAD
异常池有效MAD
相对中位偏离
```

新增一列：

```text
异常明细解释
```

保留现有列：

```text
异常等级
异常主要来源
复核原因
```

不改变的算法口径：

- 不改变 Modified Z-score 公式。
- 不改变 `关注` / `高度可疑` 阈值。
- 不改变 MAD 下限兜底。
- 不改变异常池分组。
- 不改变 `异常等级` 与 `异常主要来源` 判定。
- 独立成本项仍只展示金额和单位成本，不参与异常等级和异常主要来源判定。

`异常明细解释` 生成规则：

- 只输出异常标记为 `关注` 或 `高度可疑` 的成本指标。
- 异常项顺序按现有成本字段顺序，不按 score 排序：

```text
总成本
直接材料
直接人工
制造费用
制造费用_其他
制造费用_人工
制造费用_机物料及低耗
制造费用_折旧
制造费用_水电费
```

每个异常项内部字段顺序：

```text
等级
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

示例：

```text
总成本: 高度可疑, 当前值=100.00, 当前log=4.6052, 基准值=50.00, 基准log=3.9120, log偏离=0.6931, 相对偏离=100.00%, score=8.12, 有效工单数=12, 原始MAD=0.0572, 有效MAD=0.0572; 直接材料: 关注, 当前值=70.00, 当前log=4.2485, 基准值=50.00, 基准log=3.9120, log偏离=0.3365, 相对偏离=40.00%, score=3.01, 有效工单数=10, 原始MAD=0.0754, 有效MAD=0.0754
```

数字格式：

- `当前值` / `基准值`：2 位小数
- `当前log` / `基准log` / `log偏离` / `原始MAD` / `有效MAD`：4 位小数
- `score`：2 位小数
- `相对偏离`：百分比 2 位小数
- `有效工单数`：整数

字段口径：

- `当前值` 是当前行对应成本指标的单位成本。
- `当前log` 是当前值的自然对数，沿用现有 `math.log(value)` 口径。
- `基准log` 是异常池该成本指标的加权中位数 log。
- `基准值` 用 `math.exp(基准log)` 还原。
- `log偏离 = 当前log - 基准log`。
- `相对偏离 = math.expm1(log偏离)`。
- `有效工单数` 是同一产品、同一生产类型异常池、同一成本指标下，实际参与该项 Modified Z-score 计算的有效工单行数。它不是完工数量合计。
- `原始MAD` 是该异常池内各样本 log 值距离基准 log 的典型距离。
- `有效MAD` 是实际用于 score 计算的 MAD，可能包含 MAD 下限兜底。

### 3. 产品维度页紧凑化

`成本分析产品维度` 不再写：

```text
四、按单个产品异常值分析
```

保留按产品分块的现有风格，不改成普通平铺表。这样能减少契约变化，同时去掉大标题和顶部空行。

legacy 模式布局：

```text
第 1 行: 产品编码 | 产品名称
第 2 行: 实际编码 | 实际名称
第 3 行: 月份 | 总成本 | 完工数量 | 单位成本 | ...
第 4 行起: 数据
```

legacy 模式冻结窗格：

```text
A4
```

scoped 模式布局：

```text
第 1 行: 产品编码 | 产品名称
第 2 行: 实际编码 | 实际名称
第 3 行: 分析口径 | 全部/正常生产/返工生产
第 4 行: 月份 | 总成本 | 完工数量 | 单位成本 | ...
第 5 行起: 数据
```

scoped 模式冻结窗格：

```text
A5
```

明确不改变：

- sheet 名仍为 `成本分析产品维度`。
- 产品维度数据口径不变。
- GB 仍可按 `全部 / 正常生产 / 返工生产` 分段。
- SK 仍可使用 legacy 单段模式。
- 不新增 sheet。

## 伪代码草案

```python
# [伪代码草案]
# 目标：在不改变 ETL 业务口径的前提下，增强 GUI 查找能力并收敛 workbook 输出解释字段。
# 输入：
# - candidate_products: 扫描/预检得到的候选产品列表
# - search_text: GUI 搜索词
# - anomaly_df: 工单异常分析内部 DataFrame
# - product_anomaly_sections: 产品维度展示分段
# 输出：
# - filtered_candidates: GUI 当前显示的候选产品
# - work_order_sheet: 删除旧解释列并新增异常明细解释后的 FlatSheet
# - product_dimension_sheet: 更紧凑的产品维度 sheet

def normalize_search_text(value: str) -> str:
    # 搜索只是 GUI 辅助，不参与业务白名单匹配，所以只做轻量规范化。
    return value.strip().casefold()


def filter_candidate_products(candidate_products, search_text):
    keyword = normalize_search_text(search_text)
    if not keyword:
        return candidate_products

    filtered = []
    for code, name in candidate_products:
        normalized_code = normalize_search_text(str(code))
        normalized_name = normalize_search_text(str(name))
        if keyword in normalized_code or keyword in normalized_name:
            filtered.append((code, name))
    return tuple(filtered)


def on_candidate_search_changed(search_text):
    visible_pairs = filter_candidate_products(self.candidate_products_all, search_text)
    self._set_table_pairs(self.candidate_table, visible_pairs)


def build_anomaly_detail_explanation(row, metrics):
    parts = []

    for metric in metrics:
        # 顺序使用 ANOMALY_METRICS，保证解释文本与 workbook 字段顺序一致。
        level = row[metric.flag_column]
        if level not in {"关注", "高度可疑"}:
            continue

        current_value = row[metric.value_column]
        current_log = row[metric.log_column]
        baseline_log = row[metric.center_log_column]
        score = row[metric.score_column]
        effective_count = row[metric.sample_size_column]
        raw_mad = row[metric.raw_mad_column]
        effective_mad = row[metric.effective_mad_column]

        if has_missing_required_value(current_value, current_log, baseline_log, score):
            # 解释字段不能反向影响异常等级；缺关键审计值时只跳过该段说明。
            continue

        baseline_value = math.exp(baseline_log)
        log_delta = current_log - baseline_log
        relative_delta = math.expm1(log_delta)

        parts.append(
            format_explanation(
                label=metric.explanation_label,
                level=level,
                current_value=current_value,
                current_log=current_log,
                baseline_value=baseline_value,
                baseline_log=baseline_log,
                log_delta=log_delta,
                relative_delta=relative_delta,
                score=score,
                effective_count=effective_count,
                raw_mad=raw_mad,
                effective_mad=effective_mad,
            )
        )

    return "; ".join(parts)


def build_anomaly_sheet(work_order_df):
    anomaly_df = calculate_existing_scores(work_order_df)

    # 保留异常等级、异常主要来源、复核原因；删除旧 5 个解释字段。
    anomaly_df["异常明细解释"] = anomaly_df.apply(
        lambda row: build_anomaly_detail_explanation(row, ANOMALY_METRICS),
        axis=1,
    )

    output_columns = replace_old_audit_columns_with_detail_explanation(WORK_ORDER_OUTPUT_COLUMNS)
    return FlatSheet(data=anomaly_df[output_columns], column_types=updated_column_types)


def write_product_anomaly_sections(writer, sheet_name, sections, scoped):
    worksheet = writer.book.add_worksheet(sheet_name)
    current_row = 0

    for section in sections:
        write_product_meta(worksheet, current_row, section)

        if scoped:
            write_scope_meta(worksheet, current_row + 2, section.section_label)
            table_header_row = current_row + 3
        else:
            table_header_row = current_row + 2

        write_table_header(worksheet, table_header_row, section.data.columns)
        data_end_row = write_table_rows(worksheet, table_header_row + 1, section.data)

        # 分块之间保留 1 行空行，避免不同产品粘在一起。
        current_row = data_end_row + 2

    worksheet.freeze_panes(*freeze_panes_to_rc("A5" if scoped else "A4"))
```

## 风险点 / 边界条件

- GUI 搜索不能改变白名单业务匹配口径，否则会把分析过滤从精确匹配变成模糊匹配。
- `异常明细解释` 可能较长，但只在异常行有内容；第一版不新增单独明细 sheet。
- 删除旧 5 列会改变 workbook contract baseline，需要明确作为预期变更更新测试基线。
- `异常明细解释` 依赖评分过程中的内部审计列，不能在生成解释时重新计算另一套 median/MAD。
- `基准值` 必须用 `math.exp(基准log)`，不能用 `expm1()`。
- `有效工单数` 口径是有效工单行数，不是完工数量合计；README/AGENTS 需要同步说明。
- 产品维度页冻结窗格从 `A6/A7` 改为 `A4/A5`，相关契约测试必须同步更新。
- 产品维度页仍保留按产品分块，若未来要改成普通平铺表，应另起 spec。

## 验收方式

单元测试：

- GUI 候选产品按产品编码包含搜索可过滤。
- GUI 候选产品按产品名称包含搜索可过滤。
- GUI 搜索清空后恢复全部候选产品。
- GUI 加入白名单只加入当前选中的候选产品。
- GUI 管线切换、清空条件、任务失败时清空候选搜索状态。
- `成本分析工单维度` 不再包含旧 5 个解释列。
- `成本分析工单维度` 包含 `异常明细解释`。
- 单异常行输出 1 段解释。
- 多异常行按字段顺序输出多段解释。
- `有效工单数` 使用有效工单行数口径。
- 异常等级和异常主要来源不因本次解释字段改造变化。
- `成本分析产品维度` legacy 布局从第 1 行开始，`A1` 不再是旧标题。
- `成本分析产品维度` scoped 布局从第 1 行开始，冻结窗格为 `A5`。
- workbook contract baseline 反映上述预期变化。

静态检查：

- `/home/george/miniconda3/bin/conda run -n test python -m ruff check src tests`

完整测试：

- `/home/george/miniconda3/bin/conda run -n test python -m pytest tests -q`

真实链路验证：

- `/home/george/miniconda3/bin/conda run -n test python main.py gb --check-only --benchmark`
- `/home/george/miniconda3/bin/conda run -n test python main.py sk --check-only --benchmark`

如实现阶段涉及 workbook 写出，应补充一次临时目录 full export，不覆盖正式 `data/processed` 产物。

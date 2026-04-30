# 超大文件重构与 Legacy Writer 清理设计

## 目标

在前三个优先级稳定后，再整理长期维护成本最高的模块，降低后续误改概率。

候选目标：

- `src/analytics/fact_builder.py`
- `src/excel/fast_writer.py`
- `src/analytics/anomaly.py`
- `src/excel/sheet_writers.py`

本阶段只做行为保持型重构和死代码清理，不改变业务口径。

## 输入

- 当前模块依赖边界测试
- workbook / error_log contract baseline
- `rg` 调用关系扫描结果
- 前三阶段新增的 benchmark 结果

## 输出

- 更小、更聚焦的模块：
  - 金额/Decimal 工具
  - 成本项目映射
  - 工单 fact 构建
  - error_log fact 构建
  - 平铺 sheet writer
  - 产品异常 sheet writer
- 删除或明确标注 legacy writer：
  - 若 `src/excel/sheet_writers.py` 无生产调用且无测试价值，删除
  - 若仍被测试或兼容路径使用，改名/注释为 legacy 并收窄入口

## 伪代码草案

```python
# [伪代码草案]
# 目标：先证明无行为变化，再拆分职责和清理死代码
# 输入：
# - module_path: 待拆分模块
# - current_tests: 现有单元和 contract 测试
# - import_graph: rg/AST 得到的真实引用关系
# 输出：
# - smaller_modules: 拆分后的职责模块
# - unchanged_contract: workbook/error_log baseline 不变

def refactor_module(module_path):
    references = scan_references(module_path)
    if module_is_unused(references):
        # 为什么先删无引用代码：死代码会误导后续优化，清理收益高且风险可控。
        remove_module_with_tests_updated(module_path)
        run_contract_tests()
        return

    target_units = identify_cohesive_units(module_path)
    for unit in target_units:
        create_new_module(unit)
        move_functions(unit)
        update_imports()
        run_targeted_tests()

    run_full_tests()
    assert_contract_baselines_unchanged()
```

## 风险点 / 边界条件

- 不在前三个优先级前做大重构，避免把功能行为和结构变化混在一起。
- 删除 legacy 文件前必须用 `rg` 和测试确认没有生产引用。
- 纯重构不得更新 contract baseline。
- 如果拆分导致循环 import，优先调整依赖方向，不用运行时导入绕过。

## 验收标准

- `tests/architecture/test_import_rules.py` 通过。
- 全量测试通过。
- workbook / error_log contract baseline 不变。
- 被拆分文件职责清晰，单文件规模明显下降。


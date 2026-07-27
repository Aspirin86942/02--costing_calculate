# Python 仅保留验证工具

- 状态：Accepted
- 日期：2026-07-27
- 范围：Python 运行路径、依赖和测试

## 背景

Rust CLI 已是正式实现，但仓库仍同时维护完整 Python 业务实现、迁移期 oracle、Phase 0 证据协议和大量 meta 测试。重复实现增加了维护成本，也让 CI 工具依赖测试内部模块。

## 决策

1. 退役 `main.py` 和 `src/` 下的 Python 业务实现。
2. Python 只保留工作簿语义比较、合成输入生成、跨版本验收和相应测试。
3. CI 与发布工具不得从测试包导入运行代码。
4. 删除 Phase 0 harness、evidence、旧 benchmark protocol 和 meta 测试。
5. Python 环境改为非发布的验证工具环境，只保留实际使用的依赖。
6. 正式 Rust 接口继续兼容；Python 旧命令不提供包装层。

## 影响

- Rust 成为唯一业务真值，避免双重实现继续漂移。
- 验证工具仍能比较整理前后 workbook、生成公开合成输入并完成发布 smoke。
- 使用 Python 旧命令的本地流程必须迁移到 Rust CLI。

## 未选择的方案

- 保留完整 Python 实现：继续承担双重维护成本。
- 使用 Python 包装 Rust CLI：只增加转发层，没有新的验证价值。
- 删除全部 Python：会失去现有 OOXML 比较和合成输入工具。

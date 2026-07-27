# Rust 深模块接口与依赖方向

- 状态：Accepted
- 日期：2026-07-27
- 范围：Rust workspace 内部架构

## 背景

四个 crate 的职责方向正确，但 CLI 直接调用 core 的 normalize、split、fact、anomaly 和 presentation 实现，导致 core 暴露面过大，编排知识分散在调用方。

## 决策

1. 保留 `costing-cli`、`costing-core`、`costing-xlsx` 和 `costing-oracle-tests` 四个 crate。
2. `costing-core` 提供一个小型内存处理接口，隐藏完整业务流水线实现。
3. `costing-cli` 只负责参数、配置、路径、运行编排、JSON 和 Manifest。
4. `costing-xlsx` 是读取、标准/低内存写出和原子发布适配器。
5. normalize、split、fact、anomaly、presentation 等实现改为 crate 内部可见。
6. 测试优先穿过稳定接口；仅为独立数学规则和 I/O 原语保留直接单元测试。
7. 不为单一实现新增 trait；标准与 low-memory writer 的 seam 保持内部。

## 影响

- 调用方只需理解少量接口，业务变化集中在 core。
- Excel 技术细节不进入业务模块，CLI 不再依赖 core 的内部步骤。
- 大文件可以按实现职责拆分，而不扩大外部接口。

## 未选择的方案

- 合并 crate：会重新耦合命令、业务和 Excel。
- 拆出更多 crate：当前没有足够的独立适配器或发布需求支撑额外 seam。
- 只移动文件：不能缩小接口，也不能消除 CLI 对内部步骤的知识。

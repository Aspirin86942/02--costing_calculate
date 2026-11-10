# 文档导航

文档按“当前事实、计划、实际变更、重要决策、只读历史”分层。代码、配置/schema、测试和实际输出优先于过程文档。

## 当前事实

- [`../README.md`](../README.md)：Windows / Linux 安装与运行、输入输出、常见错误和 Windows 发布包。
- [`architecture.md`](architecture.md)：四个 Rust crate、稳定接口、调用顺序和依赖规则。
- [`contracts/workbook.md`](contracts/workbook.md)：三张 Sheet、业务规则、error log、CLI 错误和 `RunManifestV1`。
- [`performance/README.md`](performance/README.md)：性能/内存硬门禁和复测命令。
- [`rust_rewrite_validation.md`](rust_rewrite_validation.md)：Rust 主路径的历史基线与当前持续验证入口。
- [`../AGENTS.md`](../AGENTS.md)：项目级工程约束和文档治理。
- [`../tests/contracts/README.md`](../tests/contracts/README.md)：冻结 workbook / error-log baseline 的维护规则。

配置事实：

- [`../rust/crates/costing-cli/config/costing.default.toml`](../rust/crates/costing-cli/config/costing.default.toml)
- [`../rust/crates/costing-cli/config/costing.schema.json`](../rust/crates/costing-cli/config/costing.schema.json)
- [`../rust/crates/costing-cli/config/run-manifest.schema.json`](../rust/crates/costing-cli/config/run-manifest.schema.json)

## 生命周期

| 位置 | 内容 | 规则 |
|---|---|---|
| [`plans/`](plans/README.md) | 准备做什么或历史目标 | 新计划写这里；状态必须明确 |
| [`changes/`](changes/README.md) | 已经发生的变更和验证 | 只记录实际结果与剩余风险 |
| [`decisions/`](decisions/README.md) | 需要长期解释的重要取舍 | 记录背景、结论、影响和替代方案 |
| [`superpowers/`](superpowers/README.md) | 历史计划与设计 | 全程只读，不作为当前待办 |

实施完成后：

1. 在 `changes/` 记录真实结果。
2. 重要取舍同步到 `decisions/`。
3. 把最终口径同步到代码、测试、README、AGENTS、本导航、配置/schema 和当前事实文档。

## v0.3.0 整理

- 计划：[`plans/2026-07-27-v0.3.0-project-cleanup.md`](plans/2026-07-27-v0.3.0-project-cleanup.md)
- 基线：[`changes/2026-07-27-v0.3.0-cleanup-baseline.md`](changes/2026-07-27-v0.3.0-cleanup-baseline.md)
- Rust 架构：[`changes/2026-07-27-v0.3.0-rust-architecture.md`](changes/2026-07-27-v0.3.0-rust-architecture.md)
- Python 退役：[`changes/2026-07-27-v0.3.0-python-retirement.md`](changes/2026-07-27-v0.3.0-python-retirement.md)
- 版本与治理：[`changes/2026-07-27-v0.3.0-governance.md`](changes/2026-07-27-v0.3.0-governance.md)
- RC 验收：[`changes/2026-07-27-v0.3.0-rc-validation.md`](changes/2026-07-27-v0.3.0-rc-validation.md)
- 决策：[`decisions/2026-07-27-rust-deep-module-seams.md`](decisions/2026-07-27-rust-deep-module-seams.md)、[`decisions/2026-07-27-python-validation-only.md`](decisions/2026-07-27-python-validation-only.md)

## 2026-07-29 性能优化

- 计划：[`plans/2026-07-28-sk-performance-optimization.md`](plans/2026-07-28-sk-performance-optimization.md)
- 实验与最终验证：[`changes/2026-07-28-sk-performance-experiments.md`](changes/2026-07-28-sk-performance-experiments.md)
- 决策：[`decisions/2026-07-29-cell-value-arc-text.md`](decisions/2026-07-29-cell-value-arc-text.md)
- 当前性能口径：[`performance/README.md`](performance/README.md)

## Linux 环境

- 构建与运行：[`../README.md`](../README.md)
- 公共验证：[`rust_rewrite_validation.md`](rust_rewrite_validation.md)
- 适配与实测证据：[`changes/2026-11-08-linux-environment.md`](changes/2026-11-08-linux-environment.md)

## Colab 离线调优

- 使用与门槛：[`performance/README.md`](performance/README.md)
- 交互式笔记本：[`colab_autotune.ipynb`](../tools/validation/colab_autotune.ipynb)
- 方案与后续验收：[`plans/2026-11-08-colab-offline-autotuning.md`](plans/2026-11-08-colab-offline-autotuning.md)
- 工具与实验记录：[`changes/2026-11-08-colab-autotuning-tooling.md`](changes/2026-11-08-colab-autotuning-tooling.md)

## Excel 读写与压缩优化

- 完整方案：[`plans/2026-11-08-xlsx-io-optimization.md`](plans/2026-11-08-xlsx-io-optimization.md)
- 开销定位、候选与验收：[`changes/2026-11-08-xlsx-io-optimization.md`](changes/2026-11-08-xlsx-io-optimization.md)
- 当前测量、审计模式及采纳门槛：[`performance/README.md`](performance/README.md)
- 最新上游更新与适配：[`changes/2026-11-08-upstream-refresh.md`](changes/2026-11-08-upstream-refresh.md)
- 受控写出源码来源与哈希：[`performance/dependencies/2026-11-08-rust-xlsxwriter-0.102.0.json`](performance/dependencies/2026-11-08-rust-xlsxwriter-0.102.0.json)

## 完整 SK ERP 格式

- 输入、材料金额口径与输出计数：[`contracts/workbook.md`](contracts/workbook.md)
- 全量适配及真实文件验收：[`changes/2026-11-09-full-sk-erp-format.md`](changes/2026-11-09-full-sk-erp-format.md)
- 用户确认的 ERP 材料汇总决策：[`decisions/2026-11-09-sk-material-summary.md`](decisions/2026-11-09-sk-material-summary.md)

## GitHub CI

- 自动触发已关闭，仅手动运行并在发布前复用：[`decisions/2026-11-08-manual-ci.md`](decisions/2026-11-08-manual-ci.md)
- 关闭记录与 Windows 测试失败原因：[`changes/2026-11-08-disable-automatic-ci.md`](changes/2026-11-08-disable-automatic-ci.md)

## 权威顺序

1. 当前代码、配置/schema、契约 baseline 和测试结果。
2. README、AGENTS、本导航及主题当前事实文档。
3. 尚未被取代的决策。
4. 实际变更记录。
5. 计划和只读历史。

历史 v0.2.0 证据继续保留，但不会覆盖 v0.3.0 当前事实。

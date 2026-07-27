# 文档导航

文档按“当前事实、计划、实际变更、重要决策、只读历史”分层。代码、配置/schema、测试和实际输出优先于过程文档。

## 当前事实

- [`../README.md`](../README.md)：安装、运行、输入输出、常见错误和 Windows 发布包。
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
- 决策：[`decisions/2026-07-27-rust-deep-module-seams.md`](decisions/2026-07-27-rust-deep-module-seams.md)、[`decisions/2026-07-27-python-validation-only.md`](decisions/2026-07-27-python-validation-only.md)

## 权威顺序

1. 当前代码、配置/schema、契约 baseline 和测试结果。
2. README、AGENTS、本导航及主题当前事实文档。
3. 尚未被取代的决策。
4. 实际变更记录。
5. 计划和只读历史。

历史 v0.2.0 证据继续保留，但不会覆盖 v0.3.0 当前事实。

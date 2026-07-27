# 文档导航

本目录按“当前事实、计划、实际变更、重要决策、历史档案”分层。业务真值仍来自当前代码与 `tests/contracts/baselines/`；过程文档不能覆盖实际实现。

## 文档生命周期

| 位置 | 用途 | 写入规则 |
|---|---|---|
| [`plans/`](plans/README.md) | 尚未实施、待批准或用于追溯目标状态的方案 | 新计划写在这里；明确状态，不把目标写成已实现 |
| [`changes/`](changes/README.md) | 已实施变更、验证证据和发布说明 | 只记录已经发生的结果、验证与剩余风险 |
| [`decisions/`](decisions/README.md) | 重要且需长期解释的取舍 | 记录背景、结论、影响和替代方案 |
| [`superpowers/`](superpowers/README.md) | 历史 Superpowers 计划与设计 | 只读；禁止新增、修改、移动或删除，不作为待办 |

计划实际落地后，应在 `changes/` 记录结果；重要取舍同时写入 `decisions/`；最终行为必须同步到当前事实文档、配置/schema 或测试契约。

## 当前事实

- [`../README.md`](../README.md)：面向使用者的安装、运行、输出与性能行为。
- [`../AGENTS.md`](../AGENTS.md)：工程约束、业务规则、文档生命周期和完成标准。
- [`../CLAUDE.md`](../CLAUDE.md)：Claude Code 的当前 Rust/Cargo 与 uv 使用口径。
- [`rust_rewrite_validation.md`](rust_rewrite_validation.md)：Rust 主路径最终验证结论和持续门禁。
- [`performance/README.md`](performance/README.md)：当前性能目标、实现边界、复测方式和冻结基线说明。
- [`../tests/contracts/README.md`](../tests/contracts/README.md)：workbook、error-log 和 CLI 契约说明。
- [`../rust/crates/costing-cli/config/costing.default.toml`](../rust/crates/costing-cli/config/costing.default.toml)：内置完整 GB/SK 默认配置。
- [`../rust/crates/costing-cli/config/costing.schema.json`](../rust/crates/costing-cli/config/costing.schema.json)：严格配置 schema v1。
- [`../rust/crates/costing-cli/config/run-manifest.schema.json`](../rust/crates/costing-cli/config/run-manifest.schema.json)：`RunManifestV1` 的封闭 JSON schema。
- [`../rust/crates/costing-cli/config/run-manifest.success.golden.json`](../rust/crates/costing-cli/config/run-manifest.success.golden.json) 与 [`run-manifest.failure.golden.json`](../rust/crates/costing-cli/config/run-manifest.failure.golden.json)：Manifest v1 固定兼容示例。

## 过程记录

- [`plans/python_retirement_after_rust.md`](plans/python_retirement_after_rust.md)：仍需独立批准的 Python 退场计划。
- [`plans/PRD-costing-calculate-v0.2.md`](plans/PRD-costing-calculate-v0.2.md)：已完成 v0.2.0 的历史目标与验收设计。
- [`plans/optimization-assessment.md`](plans/optimization-assessment.md)：性能优化评估快照。
- [`changes/2026-07-12-rust-performance-validation.md`](changes/2026-07-12-rust-performance-validation.md)：最终 N=5 验收快照及证据限制。
- [`changes/releases/v0.2.0.md`](changes/releases/v0.2.0.md)：v0.2.0 正式发布说明。
- [`decisions/2026-07-25-toml-sha2-review.md`](decisions/2026-07-25-toml-sha2-review.md)：M3 生产依赖批准与风险控制。
- [`decisions/2026-07-27-documentation-lifecycle.md`](decisions/2026-07-27-documentation-lifecycle.md)：本目录分层与同步规则。

各分类的写入规则和主要记录见对应目录的 `README.md`。

## 权威顺序

1. 当前代码、配置/schema 和 `tests/contracts/baselines/`。
2. 根 `AGENTS.md`、`README.md`、本文件及对应主题的当前事实文档。
3. 已接受且尚未被取代的 `docs/decisions/` 记录。
4. `docs/changes/` 中的历史实现与验证记录。
5. `docs/plans/` 与只读 `docs/superpowers/`；二者只解释目标或历史，不证明当前行为。

`docs/performance/baselines/` 和 `docs/performance/dependencies/` 中的冻结 JSON 仅用于审计历史基线与依赖 pin。

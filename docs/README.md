# 文档导航

本目录只保留当前操作说明、持续验证边界和少量可审计证据。业务真值仍来自当前代码与 `tests/contracts/baselines/`。

## 当前文档

- [`../README.md`](../README.md)：面向使用者的安装、运行、输出与性能行为。
- [`../AGENTS.md`](../AGENTS.md)：面向代码代理的工程约束、业务规则和完成标准。
- [`../CLAUDE.md`](../CLAUDE.md)：Claude Code 的当前 Rust/Cargo 与 uv 使用口径。
- [`PRD-costing-calculate-v0.2.md`](PRD-costing-calculate-v0.2.md)：v0.2.0 生产化、配置、Manifest、原子发布与 Release 的验收规格。
- [`rust_rewrite_validation.md`](rust_rewrite_validation.md)：2026-07-12 Rust 主路径最终验证结论和持续门禁。
- [`performance/README.md`](performance/README.md)：当前性能目标、实现边界、复测方式和冻结基线说明。
- [`evidence/2026-07-12-rust-performance-validation.md`](evidence/2026-07-12-rust-performance-validation.md)：最终 N=5 验收快照及证据限制。
- [`python_retirement_after_rust.md`](python_retirement_after_rust.md)：Python oracle/legacy 的保留与独立退场边界。
- [`../rust/crates/costing-cli/config/costing.default.toml`](../rust/crates/costing-cli/config/costing.default.toml)：内置且随 Release 分发的完整 GB/SK 默认配置。
- [`../rust/crates/costing-cli/config/costing.schema.json`](../rust/crates/costing-cli/config/costing.schema.json)：严格配置 schema v1。
- [`../rust/crates/costing-cli/config/run-manifest.schema.json`](../rust/crates/costing-cli/config/run-manifest.schema.json)：成功/失败 `RunManifestV1` 的封闭 JSON schema。
- [`../rust/crates/costing-cli/config/run-manifest.success.golden.json`](../rust/crates/costing-cli/config/run-manifest.success.golden.json) 与 [`run-manifest.failure.golden.json`](../rust/crates/costing-cli/config/run-manifest.failure.golden.json)：Manifest v1 固定兼容示例。
- [`dependencies/2026-07-25-toml-sha2-review.md`](dependencies/2026-07-25-toml-sha2-review.md)：M3 新增生产依赖的批准、许可证和锁文件影响。
- [`evidence/2026-07-25-v0.2.0-m0-baseline.md`](evidence/2026-07-25-v0.2.0-m0-baseline.md)：v0.2.0 冻结输入、阶段基线、完整规则快照和签字。
- [`evidence/2026-07-25-v0.2.0-m3-config-governance.md`](evidence/2026-07-25-v0.2.0-m3-config-governance.md)：M3 配置治理、三路真实数据迁移对比和退出证据。
- [`evidence/2026-07-25-v0.2.0-m4-manifest-atomicity.md`](evidence/2026-07-25-v0.2.0-m4-manifest-atomicity.md)：M4 Manifest、输入/输出哈希、原子发布和 standard/low-memory 失败注入证据。
- [`../tests/contracts/README.md`](../tests/contracts/README.md)：workbook、error-log 和 CLI 契约说明。

## 权威顺序

1. 当前代码和 `tests/contracts/baselines/`。
2. 根 `AGENTS.md` 与 `README.md`。
3. `rust_rewrite_validation.md` 与 `performance/README.md` 中的当前验证口径。
4. `docs/performance/baselines/` 和 `docs/performance/dependencies/` 中的冻结 JSON，仅用于审计历史基线与依赖 pin。

## 已清理的历史材料

2026-07-12 已删除完成后的 `docs/superpowers/`、日期 plan/spec、旧 sidecar spike 和过期的 2026-07-10 验证快照。Git 历史仍可追溯原文，但这些材料不再出现在当前文档导航中，也不得作为待执行清单恢复。

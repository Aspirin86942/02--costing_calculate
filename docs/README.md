# 文档导航

本目录只保留当前操作说明、持续验证边界和少量可审计证据。业务真值仍来自当前代码与 `tests/contracts/baselines/`。

## 当前文档

- [`../README.md`](../README.md)：面向使用者的安装、运行、输出与性能行为。
- [`../AGENTS.md`](../AGENTS.md)：面向代码代理的工程约束、业务规则和完成标准。
- [`../CLAUDE.md`](../CLAUDE.md)：Claude Code 的当前 Rust/Cargo 与 uv 使用口径。
- [`rust_rewrite_validation.md`](rust_rewrite_validation.md)：2026-07-12 Rust 主路径最终验证结论和持续门禁。
- [`performance/README.md`](performance/README.md)：当前性能目标、实现边界、复测方式和冻结基线说明。
- [`evidence/2026-07-12-rust-performance-validation.md`](evidence/2026-07-12-rust-performance-validation.md)：最终 N=5 验收快照及证据限制。
- [`python_retirement_after_rust.md`](python_retirement_after_rust.md)：Python oracle/legacy 的保留与独立退场边界。
- [`../tests/contracts/README.md`](../tests/contracts/README.md)：workbook、error-log 和 CLI 契约说明。

## 权威顺序

1. 当前代码和 `tests/contracts/baselines/`。
2. 根 `AGENTS.md` 与 `README.md`。
3. `rust_rewrite_validation.md` 与 `performance/README.md` 中的当前验证口径。
4. `docs/performance/baselines/` 和 `docs/performance/dependencies/` 中的冻结 JSON，仅用于审计历史基线与依赖 pin。

## 已清理的历史材料

2026-07-12 已删除完成后的 `docs/superpowers/`、日期 plan/spec、旧 sidecar spike 和过期的 2026-07-10 验证快照。Git 历史仍可追溯原文，但这些材料不再出现在当前文档导航中，也不得作为待执行清单恢复。

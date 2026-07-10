# 文档导航

本页区分当前操作文档、验证依据和历史设计记录，避免把迁移前命令当成现行入口。

## 当前操作文档

- [`../README.md`](../README.md)：面向使用者的安装、运行、输出和测试命令。
- [`../AGENTS.md`](../AGENTS.md)：面向代码代理的工程约束、当前业务规则和验证口径。
- [`../CLAUDE.md`](../CLAUDE.md)：与当前 Rust 主入口、Cargo 和 uv/.venv 口径一致的 Claude Code 指引。
- [`rust_rewrite_validation.md`](rust_rewrite_validation.md)：Rust 切换后的持续验证门禁和已登记证据。
- [`evidence/2026-07-10-rust-validation.md`](evidence/2026-07-10-rust-validation.md)：2026-07-10 本机验证快照、复现命令和证据限制。
- [`python_retirement_after_rust.md`](python_retirement_after_rust.md)：Python oracle/legacy 退场边界；删除仍需单独审批。
- [`../tests/contracts/README.md`](../tests/contracts/README.md)：workbook、error log 和 Rust/Python oracle 的契约说明。

## 权威顺序

1. 业务与运行行为以当前代码和 `tests/contracts/baselines/` 为准。
2. 当前命令以根 `README.md` 和 `AGENTS.md` 为准：生产主入口使用 Rust/Cargo，Python oracle/regression 使用 uv 管理的项目 `.venv`。
3. Rust 切换证据以 `rust_rewrite_validation.md` 为准。
4. 设计和实施计划用于解释历史决策，不覆盖当前代码或当前命令。

## 历史材料

以下目录或日期文档保留当时的设计、实施步骤和测量上下文：

- `superpowers/specs/`
- `superpowers/plans/`
- 日期前缀的 plan/spec 文档
- `../spikes/`

其中可能仍出现已退役的 `conda run -n costing311`、`conda run -n test`、Python 默认入口或 GUI 命令。这些内容是历史记录，不是当前环境指引。原则上不机械改写已经执行过的历史步骤；确需让历史验证命令在当前环境复跑时，必须在文档开头标记后补兼容说明。

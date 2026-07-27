# 计划

本目录保存尚未实施、待批准或为追溯目标状态而保留的方案。计划描述“准备做什么”，不能作为“已经实现”的证据。

## 写入规则

- 新计划使用 `YYYY-MM-DD-<slug>.md`；已有稳定文件名可保留。
- 开头声明状态，建议使用 `Draft`、`Approved`、`In Progress`、`Completed` 或 `Superseded`。
- 写清目标、范围外事项、验收条件、风险和验证方式。
- 计划落地后，在 `../changes/` 记录实际结果；重要取舍写入 `../decisions/`。
- 最终行为同步到根 `README.md`、`AGENTS.md`、`docs/README.md` 及相关配置/schema、契约或当前事实文档。
- 不向 `../superpowers/` 写入新计划。

## 当前文件

- [`2026-07-27-v0.3.0-project-cleanup.md`](2026-07-27-v0.3.0-project-cleanup.md)：v0.3.0 Rust 收敛、Python 退役与文档/发布整理计划。
- [`python_retirement_after_rust.md`](python_retirement_after_rust.md)：待独立批准的 Python oracle/legacy 退场计划。
- [`PRD-costing-calculate-v0.2.md`](PRD-costing-calculate-v0.2.md)：v0.2.0 已完成目标的历史 PRD。
- [`optimization-assessment.md`](optimization-assessment.md)：优化机会和实验方法评估快照。

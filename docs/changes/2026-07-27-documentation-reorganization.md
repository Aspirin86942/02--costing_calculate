# 文档目录整理 — 2026-07-27

- 状态：Completed
- 范围：项目文档目录、文档引用和 Release notes 路径

## 实际结果

- 从删除前最后快照 `514e6b5a8cc0c46613a142831c37cedaa163f8b2` 恢复 44 个 `docs/superpowers/` 历史计划与设计文件，并新增只读边界说明。
- 把 v0.2.0 PRD、优化评估和 Python 退场方案归入 `docs/plans/`。
- 把 9 份验证证据和 2 份发布说明归入 `docs/changes/`。
- 把 `toml` / `sha2` 生产依赖审批归入 `docs/decisions/`。
- 新增 plans、changes、decisions 三类目录索引和文档生命周期决策。
- 更新根 `README.md`、`docs/README.md`、性能口径和 Rust 验证文档中的当前规则与链接，并将完整文档治理要求写入项目级提示词 `AGENTS.md`。
- Release workflow 改从 `docs/changes/releases/<tag>.md` 读取说明，对应架构测试同步更新。

## 迁移映射

| 原位置 | 新位置 |
|---|---|
| `docs/PRD-costing-calculate-v0.2.md` | `docs/plans/PRD-costing-calculate-v0.2.md` |
| `docs/optimization-assessment.md` | `docs/plans/optimization-assessment.md` |
| `docs/python_retirement_after_rust.md` | `docs/plans/python_retirement_after_rust.md` |
| `docs/evidence/*.md` | `docs/changes/*.md` |
| `docs/releases/*.md` | `docs/changes/releases/*.md` |
| `docs/dependencies/2026-07-25-toml-sha2-review.md` | `docs/decisions/2026-07-25-toml-sha2-review.md` |

## 验证

- 44 个恢复文件逐一按 Git clean-filter 后的 blob 与恢复 commit 比对：`0` 个内容不一致；工作树保持项目现有 CRLF 口径。
- 旧根目录路径扫描：除本文件的迁移映射外，`docs/evidence/`、`docs/dependencies/`、`docs/releases/` 及三个已迁移根文件均无档案外引用残留。
- 当前层 29 个 Markdown 文件的相对链接审计：`0` 个断链；只读历史档案按治理边界排除。
- `uv run python -m pytest tests/architecture -q --basetemp .pytest-tmp/documentation-reorg-rerun`：`23 passed`。

## 剩余边界

- “只读”是仓库治理约束，不是 Windows 文件属性；它由 `AGENTS.md`、文档导航和档案说明共同约束。
- `docs/superpowers/` 内容按历史原样保留，可能包含过期命令或链接，因此不纳入当前相对链接正确性要求。
- 本次未实际触发远端 GitHub Release；Release notes 新路径通过本地架构契约测试验证。

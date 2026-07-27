# Claude Code 项目说明

先遵守根目录 [`AGENTS.md`](AGENTS.md)。本项目只有一套正式业务实现：Rust。

## 快速入口

- 架构：[`docs/architecture.md`](docs/architecture.md)
- workbook、异常、错误码和 Manifest 契约：[`docs/contracts/workbook.md`](docs/contracts/workbook.md)
- 文档导航：[`docs/README.md`](docs/README.md)
- 默认配置：[`rust/crates/costing-cli/config/costing.default.toml`](rust/crates/costing-cli/config/costing.default.toml)
- 配置 schema：[`rust/crates/costing-cli/config/costing.schema.json`](rust/crates/costing-cli/config/costing.schema.json)

## 常用验证

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo clippy --locked --manifest-path rust/Cargo.toml --workspace --all-targets --all-features -- -D warnings
cargo test --locked --manifest-path rust/Cargo.toml --workspace --all-features
uv run python -m ruff check tests tools
uv run python -m ruff format tests tools --check
uv run python -m pytest tests -q -m "not slow and not benchmark" --basetemp .pytest-tmp/python-validation
```

Python 只用于验证工具，不得新增业务实现或生产入口。`docs/superpowers/` 全程只读；计划、实际变更和决策分别写入 `docs/plans/`、`docs/changes/` 和 `docs/decisions/`，完成后同步当前事实文档。

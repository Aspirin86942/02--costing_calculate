# Rust 主路径验证

## 当前结论

Rust 是 GB / SK 唯一正式业务实现。Python 只提供验证工具，不再提供业务入口。

当前持续验证以三类证据为准：

1. Rust 单元、集成、契约和架构测试；
2. 合成 GB/SK 端到端 smoke；
3. 冻结基线 binary 与候选 binary 对同一真实 workbook 的跨版本比较。

## 历史冻结基线

2026-07-12 的 v0.2.0 N=5 结果继续作为性能硬门禁来源：

| Pipeline | Wall median | PWS median | Output bytes |
|---|---:|---:|---:|
| GB | 2.475s | 357,191,680 | 3,808,077 |
| SK | 19.883s | 1,461,714,944 | 43,611,044 |

详细历史证据见 [`changes/2026-07-12-rust-performance-validation.md`](changes/2026-07-12-rust-performance-validation.md)。这份快照用于回归判断，不是待重复执行的旧协议。

## v0.3.0 整理验证

整理前创建恢复标签 `cleanup-baseline-2026-07-27`，并在忽略目录保存对应 release binary。

Rust 深模块整理后，基线与候选分别处理真实 GB、SK workbook：

- 三张 Sheet 和运行计数保持一致；
- 完整 OOXML/语义比较零差异；
- 单元格 `1e-9`、列累计 `1e-8` 容差没有放宽；
- 合成 GB/SK 配置、check-only、正式 workbook 和禁止覆盖 smoke 通过。

整理证据见：

- [`changes/2026-07-27-v0.3.0-cleanup-baseline.md`](changes/2026-07-27-v0.3.0-cleanup-baseline.md)
- [`changes/2026-07-27-v0.3.0-rust-architecture.md`](changes/2026-07-27-v0.3.0-rust-architecture.md)
- [`changes/2026-07-27-v0.3.0-python-retirement.md`](changes/2026-07-27-v0.3.0-python-retirement.md)
- [`changes/2026-07-27-v0.3.0-rc-validation.md`](changes/2026-07-27-v0.3.0-rc-validation.md)

`v0.3.0-rc.1` 已通过真实 GB/SK normal、单月过滤、check-only、N=5 性能/内存和 Windows 隔离包验收；标签当前仅存在于本地，尚未推送。

## 持续验证

公共门禁：

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo clippy --locked --manifest-path rust/Cargo.toml --workspace --all-targets --all-features -- -D warnings
cargo test --locked --manifest-path rust/Cargo.toml --workspace --all-features
uv run python -m ruff check tests tools
uv run python -m ruff format tests tools --check
uv run python -m pytest tests -q -m "not slow and not benchmark" --basetemp .pytest-tmp/python-validation
```

合成端到端：

```powershell
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate
uv run python tools/ci/run_synthetic_e2e.py --binary rust/target/release/costing-calculate.exe
```

真实 workbook 跨版本验证：

```powershell
uv run python -m tools.validation.compare_releases `
  --baseline-binary <baseline.exe> `
  --candidate-binary <candidate.exe> `
  --pipeline gb `
  --input <workbook.xlsx> `
  --output-dir <empty-directory> `
  --report <report.json>
```

真实数据、性能、Windows 包和 Excel/WPS 视觉复核属于发布前门禁，不因普通小改动重复运行。

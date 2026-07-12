# Rust 主路径验证

## 状态

截至 2026-07-12，Rust CLI 是 GB/SK 默认成本核算入口，Python 仅作为 legacy/oracle/regression 保留。Python retirement 仍需单独批准。

实现提交：`f298649`（`perf(rust): optimize workbook output and ingest`）。受控 `rust_xlsxwriter` fork revision：`816cd47bc2faa84ab1ac2fbb3320a4699a454b22`。

## 最终自动门禁

| 门禁 | 结果 |
|---|---:|
| Rust workspace tests | 168 passed |
| Python contracts | 7 passed |
| Workbook comparator focused tests | 39 passed |
| PWS harness direct tests | 42 passed |
| Phase 0 harness direct tests | 262 passed |
| Rust fmt / diff check | passed |
| SK Python-oracle workbook/runtime contract | passed, zero mismatches |
| GB Python-oracle workbook/runtime contract | passed, zero mismatches |
| low-memory temp cleanup | passed |

## 正式 N=5

| Pipeline | Wall median | PWS median | Output bytes | Verdict |
|---|---:|---:|---:|---|
| GB | 2.475s | 357,191,680 | 3,808,077 | `VALIDATED` |
| SK | 19.883s | 1,461,714,944 | 43,611,044 | `VALIDATED` |

SK 五轮 wall-clock 为 `19.584s`、`19.665s`、`19.994s`、`20.066s`、`19.883s`。验收口径是中位数，不要求每一轮都低于 20 秒。

最终 SK workbook 与同实现 reference workbook 都包含 12 个 ZIP 成员；除 `docProps/core.xml` 正常生成时间外，所有解压成员 SHA-256 一致。Python-oracle 对比另外验证了值、样式、列顺序、package、业务勾稽和 runtime 契约；`shared string` 与 `inline string` 按 OOXML 等价文本编码比较，但每个 package 内部的 relationship、content type、part 和索引仍严格校验。数值 XML 尾差按单元格/工单分组 `1e-9`、整列累计 `1e-8` 比较；实测最大值分别为 `1e-10` 和 `2.509e-9`，`1e-7` 差异仍由测试明确拒绝。

详细证据边界见 [`evidence/2026-07-12-rust-performance-validation.md`](evidence/2026-07-12-rust-performance-validation.md)。

## 持续验证命令

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --manifest-path rust/Cargo.toml
uv run python -m pytest tests/contracts -q --basetemp .pytest-tmp/contracts
cargo build --release --manifest-path rust/Cargo.toml -p costing-calculate
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- gb --check-only --benchmark
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- sk --check-only --benchmark
```

真实 workbook oracle、PWS N=5 和 Excel/WPS 视觉复核属于发布前验证，不应在普通小改动中重复运行。

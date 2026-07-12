# Rust 性能口径

## 当前目标

正式比较统一使用 release profile、真实 GB/SK 输入、normal-mode 独立进程和 N=5 中位数：

- SK wall-clock 中位数 `<= 20.0s`；
- SK Peak Working Set 中位数 `<= 2.0 GiB`；
- GB wall/PWS 不超过冻结 Phase 0A 门槛；
- GB/SK 输出大小不超过 Phase 0A 的 `1.10x`；
- workbook、runtime、quality、error-log 和 CLI 契约保持一致。

2026-07-12 最终结果已全部通过，详见 [`../evidence/2026-07-12-rust-performance-validation.md`](../evidence/2026-07-12-rust-performance-validation.md)。

## 当前实现

- release profile 固定 `codegen-units = 1`。
- Calamine `0.36` 直接解析 worksheet range；reader 不再复制成中间 `Vec<Vec<Data>>`。
- writer 预计算每列文本/数字行为和格式，空白单元格直接跳过。
- CLI 默认启用 `low-memory` feature；单张 Sheet 达到 `5,000,000` 个 cell slots 时进入 low-memory。
- low-memory 临时目录位于最终输出目录，并在成功、失败及错误合并路径中显式清理。
- 受控 `rust_xlsxwriter` fork revision 固定在 `rust/Cargo.toml`；大 workbook 使用经过输出大小门禁验证的 ZIP 压缩等级。

## 快速复测

先做正确性门禁：

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --manifest-path rust/Cargo.toml
uv run python -m pytest tests/contracts -q --basetemp .pytest-tmp/contracts
```

真实输入快速检查：

```powershell
cargo build --release --manifest-path rust/Cargo.toml -p costing-calculate
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- gb --check-only --benchmark
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- sk --check-only --benchmark
```

normal-mode 性能验收必须使用独立且不存在的输出路径，并复用 `tests/rust_oracle/measure_peak_working_set.ps1` 记录外部 wall-clock 与 Peak Working Set。快速筛选可以少于五轮；只有正确性通过的最终候选才执行 N=5。

## 证据边界

- 原始 stdout/stderr、真实 workbook、ERP 文件名、用户名、绝对路径和主机信息只保存在已忽略的 `rust/target/perf-local/`。
- 版本库只保留脱敏的最终快照、冻结 Phase 0A baseline JSON 和 fork dependency JSON。
- `docs/performance/baselines/2026-07-11-windows-x64-phase0a.json` 是历史冻结基线，不是待执行协议。
- `docs/performance/dependencies/2026-07-11-rust-xlsxwriter-0.96.0.json` 用于审计受控 fork 来源。
- 已删除的 protocol v2/v3、append-only ledger 和 Superpowers 计划不得恢复为默认流程；现有测量工具能回答问题时直接复用。

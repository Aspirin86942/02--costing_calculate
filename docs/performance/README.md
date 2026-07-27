# Rust 性能与内存门禁

## 硬门禁

正式比较统一使用 release profile、真实输入、normal mode、独立进程和 N=5 中位数：

| Pipeline | Wall 中位数 | Peak Working Set 中位数 | 最大输出大小 |
|---|---:|---:|---:|
| GB | `<= 3.2554s` | `<= 375,700,685 bytes` | `<= 4,194,321 bytes` |
| SK | `<= 20.0s` | `<= 2,147,483,648 bytes` | `<= 48,658,823 bytes` |

同时必须满足：

- workbook、运行摘要、质量、error log 和 CLI 契约与冻结基线一致；
- 单元格 `1e-9`、列累计 `1e-8` 数值容差不变；
- 输出路径每轮独立且事先不存在；
- 真实输入、workbook、绝对路径和主机信息不进入版本库。

## 当前实现边界

- release profile 固定 `codegen-units = 1`。
- Calamine `0.36` 读取 workbook。
- 默认启用 `low-memory`；单张 Sheet 达到 `5,000,000` cell slots 时切换。
- low-memory 临时目录位于最终输出目录，禁止回退系统 `%TEMP%`。
- writer 预计算列行为并跳过空白单元格。
- `rust_xlsxwriter` 使用 `rust/Cargo.toml` 中的精确 fork revision。
- ZIP 压缩保持 Level 5。历史 Level 3/2 实验虽更快，但输出大小越过门禁，因此未采用。

## 复测顺序

先通过正确性：

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo clippy --locked --manifest-path rust/Cargo.toml --workspace --all-targets --all-features -- -D warnings
cargo test --locked --manifest-path rust/Cargo.toml --workspace --all-features
uv run python -m pytest tests -q -m "not slow and not benchmark" --basetemp .pytest-tmp/python-validation
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate
uv run python tools/ci/run_synthetic_e2e.py --binary rust/target/release/costing-calculate.exe
```

再完成基线/候选真实 workbook 比较，最后运行 N=5：

当前测量入口是 `tools/validation/measure_release.ps1`。

```powershell
.\tools\validation\measure_release.ps1 `
  -BinaryPath .\rust\target\release\costing-calculate.exe `
  -Pipeline gb `
  -InputPath <gb-real.xlsx> `
  -OutputDirectory .\.pytest-tmp\perf-gb `
  -Iterations 5 `
  -ReportPath .\.pytest-tmp\perf-gb.json

.\tools\validation\measure_release.ps1 `
  -BinaryPath .\rust\target\release\costing-calculate.exe `
  -Pipeline sk `
  -InputPath <sk-real.xlsx> `
  -OutputDirectory .\.pytest-tmp\perf-sk `
  -Iterations 5 `
  -ReportPath .\.pytest-tmp\perf-sk.json
```

脚本输出不含输入路径，但输出 workbook 和原始本地报告仍只保存在忽略目录。

## 优化规则

- 没有可复现瓶颈就不优化。
- 优化必须先通过跨版本正确性，再做交错配对。
- 正式优化实验至少 8 对，报告中同时记录 wall、PWS、输出大小和环境限制。
- 无论采用或拒绝，都在 `docs/changes/` 留下脱敏结果。
- 不得通过改用 dev profile、放宽阈值、放宽容差或更新基线来获得通过。

## 历史证据

- v0.2.0 N=5：[`../changes/2026-07-12-rust-performance-validation.md`](../changes/2026-07-12-rust-performance-validation.md)
- ZIP 压缩拒绝实验：[`../changes/2026-07-25-v0.2.0-m6b-zip-compression.md`](../changes/2026-07-25-v0.2.0-m6b-zip-compression.md)
- 冻结 JSON：`docs/performance/baselines/`
- 依赖来源：`docs/performance/dependencies/`

`docs/superpowers/` 中的历史协议全程只读，不恢复为当前默认流程。

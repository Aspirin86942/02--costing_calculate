# 性能证据边界

本目录只接收经过闭合结构校验和脱敏扫描的版本化证据。原始 stdout/stderr、真实工作簿、绝对路径、主机名、用户名以及 ERP 文件名只能保存在已忽略的 `rust/target/perf-local/` 下。不可恢复的 smoke/Phase 0A capture 在 `finally` 删除其 workbook 与 PWS raw artifacts；formal paired raw evidence 由 append-only ledger 管理以支持缺轮恢复。任何应执行的 cleanup 失败都会阻止版本化证据生成。

## Phase 0H synthetic smoke

Phase 0H 使用代码生成的单 Sheet 原始输入，不读取 `data/raw/`。`small` fixture 包含两行元数据、两行表头、正数量行和合成成本明细；产品明确位于业务白名单之外，因此第三张输出 Sheet 只保留表头。`low-memory` fixture 固定生成 140,000 个有效 SK 数量工单；测试会运行真实 Rust 输出并读取数量 Sheet 的实际 dimension，按当前 36 列契约至少覆盖 5,000,000 个输出 slots。

```powershell
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/perf-builds/phase0a/reference --no-default-features
$Phase0AExe = 'rust/target/perf-builds/phase0a/reference/x86_64-pc-windows-msvc/release/costing-calculate.exe'
uv run python -m tests.rust_oracle.phase0_harness smoke --pipeline gb --reference-executable $Phase0AExe --candidate-executable $Phase0AExe --local-root rust/target/perf-local/phase0h-smoke
```

成功输出 `VALIDATED`。命令使用同一 reference binary 执行五轮 normal wall 和五轮 normal PWS，逐轮核对 workbook oracle 与实际 Sheet 名；fixture writer 把临时目录固定在 fixture 目录，smoke 另外设置本地 TEMP canary 并记录残留数。`finally` 删除 fixture、所有输出 workbook 和本轮 PWS raw artifacts。Phase 0H 不生成或批准正式 Phase 0A business manifest。

## Phase 0A capture

Phase 0A 只接受显式输入路径，而且该路径必须分别与调用者提供的 `COSTING_GB_SAMPLE`、`COSTING_SK_SAMPLE` 完全一致。capture 对 GB/SK 分别执行 reference-only wall/PWS 校准，记录外部输出文件字节数、运行时计数和三张 Sheet dimension。输出只含 `$GB_INPUT` / `$SK_INPUT` 别名、hash、计数和指标，不含真实路径、文件名或原始日志。

```powershell
$env:COSTING_GB_SAMPLE = '<approved-local-gb-input>'
$env:COSTING_SK_SAMPLE = '<approved-local-sk-input>'
uv run python -m tests.rust_oracle.phase0_harness phase0a --gb-input $env:COSTING_GB_SAMPLE --sk-input $env:COSTING_SK_SAMPLE --reference-executable $Phase0AExe --fork-revision <40-char-fork-sha> --local-root rust/target/perf-local/phase0a --output docs/performance/<new-phase0a-capture>.json
```

capture 文件的 `approval_state` 固定为 `CAPTURED_NOT_APPROVED`，并拒绝覆盖既有目的地。用户确认与正式批准属于后续子计划，本阶段不得把 capture 当作已批准基线。

## Paired batch CLI

`paired` 只公开 pipeline、输入、两端 executable、两端闭合 label、闭合 comparison profile、Phase 0A manifest、本地根和 evidence path。batch ID、attempt、round、N 和 threshold 均由 harness 固定或派生，调用者不能覆盖。append-only ledger 固定位于 `rust/target/perf-local/batches`。

`--evidence-path` 的 basename 必须与 typed sanitizer 按 profile、pipeline 和 candidate SHA 生成的 `benchmark-<16hex>.json` 完全一致；harness 不会忽略调用者文件名或把证据发布到另一个 basename。发布顺序固定为 append-only `cleanup-complete` record、marker-last typed evidence publication、`evidence-committed` record。

退出码固定如下：

- `0`：`VALIDATED`
- `2`：candidate、correctness、gate 或 `INCONCLUSIVE`
- `3`：reference、environment 或 incomplete evidence
- `4`：cleanup 或 sensitive evidence
- `5`：CLI usage

所有性能收益比必须来自同一输入、同一机器状态和同批 AB/BA 运行。任一 wall/PWS 指标进入临界区时，两类证据必须共同追加全局轮次 6–10；不得选择性重采。

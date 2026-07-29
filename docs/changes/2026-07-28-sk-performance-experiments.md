# SK 性能优化实验记录

- 状态：`Completed`
- 日期：`2026-07-28`
- 完成日期：`2026-07-29`
- 计划：[`../plans/2026-07-28-sk-performance-optimization.md`](../plans/2026-07-28-sk-performance-optimization.md)
- 范围：paired driver、当前基线、Tier 1/2 候选、Tier 3 有界验证与正式采纳
- 数据边界：只提交聚合指标和 SHA-256；真实文件名、绝对路径、workbook 和原始本地报告均留在忽略目录

## 当前结论

- Phase 0 测量能力和噪声校准通过，可以进入正式候选实验。
- T1-A `zmij`：`Rejected`。正确性通过，但 `writer_populate` 和 external wall 均未达到收益门槛。
- T1-B Level 4：`Rejected`。workbook package 完全一致，但输出大小越过硬门禁。
- T1-C Thin LTO：`Rejected`。workbook package 完全一致，但 external wall 收益和胜场均不足。
- `zmij` 不加入默认 feature，也不进入与 Level 4 的组合实验。
- Tier 1 最终采纳栈为空，I/O 基线保持 Level 5。
- T2-A reader 整数快路径：`Adopted for final stack`。ingest 与 external wall 均稳定超过门槛。
- T2-B forward-fill：`Rejected`。聚合计数证明假设值得测试，但 `normalize` 实测收益未达到采纳门槛。
- T3-0 census 已完成，并在 T3 性能实验前冻结每列最多 `8,192` 个唯一文本的驻留上限。
- T3-A `Arc<str>` 表示：`Adopted`。相对原始基线，PWS 下降约 `25%`，wall 改善约 `6%`。
- T3-B 有界按列文本驻留：`Rejected`。PWS 额外下降约 `4%`，但 wall 回退约 `2.5%`，越过红线。
- 最终采纳栈固定为 reader 整数快路径 + T3-A；正式组合分支已通过全部完成门禁。

## Phase 0：测量能力与冻结基线

### 实验身份

| 项目 | 冻结值 |
|---|---|
| `BASE_COMMIT` | `9198b2a07f0fe269492803f083c680f897c71730` |
| tree | `60a531797050c45e2d8f08a1a51adb21fa06ff56` |
| `SOURCE_DATE_EPOCH` | `1785243086` |
| baseline binary SHA-256 | `9ebce2da7ce15e462df8e0792ec489964262b9ce43df4be9565db11ec615b3ea` |
| baseline binary bytes | `3,954,176` |
| `rust-toolchain.toml` SHA-256 | `e3023f6106b1e58c6a00c59a88b11c670765c3b3d1b31e8a0fc498e377aa185d` |
| `Cargo.lock` SHA-256 | `a3ff48e2e5d88d854054e3409c6b6b252d744c5031b5099f8008bd0b34961e7e` |
| GB input SHA-256 | `6aa5e3e7fdc547ebaaef968eb5b95d4d630c4ec9915184f94346f60687b8e7ee` |
| SK input SHA-256 | `6eac3c6c9ea0eb3e98ca11fb3829914be63e932595b3e3c613f0da46b385d64f` |

baseline 从 detached、干净的 `main` 构建，使用初始不存在的独立 target 目录。release 构建耗时 `51.188s`；复制到实验目录后设为只读。`--version-json` 的 commit、时间戳、Rust `1.96.0` 与 `x86_64-pc-windows-msvc` 均符合冻结身份。

安全环境快照为 Windows `10.0.19045`、`16` 个逻辑处理器、约 `16GB` 物理内存和 High performance 电源方案。未记录主机名、用户名或机器序列号。

feature tree 复核：

- 默认 feature 是 `low-memory`；
- writer 的 `zip 7.2.0` 使用 `deflate-flate2-zlib-rs`；
- 默认树中的 `zmij 1.0.21` 来自 `serde_json`，没有启用 `rust_xlsxwriter/zmij`。

### paired driver

新增：

- `tools/validation/measure_paired_release.ps1`
- `tools/validation/measure_paired_release.schema.json`

driver 固定双边预热、奇偶反序、独立输出、`10ms` PWS 轮询、全部阶段耗时、业务摘要、输出 SHA-256、临时残留检查，以及 binary/input 前后哈希复核。报告只使用安全别名和哈希，并明确 `measurement_only = true`。

验证：

- 固定样本的中位数、对内相对差、胜场和奇偶顺序自检通过；
- normal 与 check-only 的同 binary 进程级 1 对 synthetic smoke 通过；
- check-only 生成 workbook 数为 `0`；
- 已存在输出根、报告路径冲突和进程非零退出均形成脱敏 invalid 结果；
- valid/invalid 报告均通过 JSON Schema；
- Ruff、格式检查、Markdown 链接和公共 Python 测试通过，结果为 `66 passed`。

### 当前 N=5

| Pipeline | Wall 中位数 | PWS 中位数 | 最大输出 | 结果 |
|---|---:|---:|---:|---|
| GB | `1.8594s` | `357,842,944` | `3,808,077` | `Passed` |
| SK | `17.5654s` | `1,463,287,808` | `43,611,045` | `Passed` |

### baseline-vs-baseline 噪声

SK normal 4 对：

| 指标 | 对内相对差中位数 | 候选位胜场 | 门槛判断 |
|---|---:|---:|---|
| external wall | `+0.5933%` | `1/4` | 绝对值 `< 1%` |
| PWS | `-0.0731%` | `3/4` | 稳定 |
| `writer_populate` | `-1.0345%` | `3/4` | 绝对值 `< 5%` |
| `xlsx_save` | `+1.0363%` | `1/4` | 绝对值 `< 5%` |
| `total` | `+0.1557%` | `2/4` | 稳定 |

SK check-only 4 对：

| 指标 | 对内相对差中位数 | 候选位胜场 | 门槛判断 |
|---|---:|---:|---|
| external wall | `-0.3733%` | `4/4` | 绝对值 `< 1%` |
| PWS | `+0.0053%` | `1/4` | 稳定 |
| `ingest` | `-0.2553%` | `3/4` | 绝对值 `< 5%` |
| `normalize` | `-0.7652%` | `4/4` | 绝对值 `< 5%` |
| `total` | `-0.3265%` | `3/4` | 稳定 |

两种模式均满足计划的噪声退出条件。

## T1-A：`zmij`

### 候选身份与正确性

| 项目 | 值 |
|---|---|
| source commit | 与 baseline 相同的 `9198b2a07f0fe269492803f083c680f897c71730` |
| candidate binary SHA-256 | `8a15ed3f487b6eca4acda84b9842185f00bf1f084dd1db30300d658116f43f14` |
| candidate binary bytes | `3,954,176` |
| release 构建耗时 | `46.268s` |
| feature | 显式 `costing-calculate/zmij -> costing-xlsx/zmij -> rust_xlsxwriter/zmij` |

候选未修改默认 feature。精确 feature 下：

- `cargo fmt --check` 通过；
- Clippy `-D warnings` 通过；
- Rust 全 workspace 测试 `209 passed`；
- synthetic GB/SK config、check-only、normal、禁止覆盖 smoke 通过。

真实 workbook：

| Pipeline | 比较模式 | mismatch | error log | run counts |
|---|---|---:|---:|---|
| GB | semantic | `0` | `20,515` | 完全一致 |
| SK | semantic | `0` | `201,815` | 完全一致 |

OOXML 聚合：

| Pipeline | 改变的数值 cell | 其中整数变 `.0` | 数值字面量字节增量 | 语义/结构差异 | 压缩后变化 |
|---|---:|---:|---:|---:|---:|
| GB | `133,396` | `133,395` | `+266,786` | `0 / 0` | `+5,627` |
| SK | `2,513,403` | `2,513,403` | `+5,026,806` | `0 / 0` | `-51,561` |

聚合统计不记录任何 cell 原值。

### SK normal 8 对

| 指标 | baseline 中位数 | candidate 中位数 | 对内相对差中位数 | candidate 胜场 |
|---|---:|---:|---:|---:|
| external wall | `17.037236s` | `17.066764s` | `+0.2639%` | `3/8` |
| PWS | `1,462,306,816` | `1,461,598,208` | `-0.0475%` | `7/8` |
| `writer_populate` | `5.092519s` | `5.147121s` | `+1.3594%` | `3/8` |
| `xlsx_save` | `3.004276s` | `2.989681s` | `-0.6629%` | `6/8` |
| `total` | `8.421108s` | `8.486854s` | `+0.8005%` | `1/8` |

candidate 最大输出为 `43,559,484 bytes`，低于硬门禁。

### 判定

`Rejected`：

- 主指标 `writer_populate` 没有改善 `>= 3%`，实际回退 `1.3594%`；
- external wall 没有改善 `>= 1%`，实际回退 `0.2639%`；
- 两个主指标都只赢 `3/8`，低于 `6/8`；
- PWS 与输出体积的次要改善不能替代预冻结主门槛。

因此不修改默认 feature，不形成生产依赖取舍，也不启动 `zmij + Level 4` 组合实验。

## T1-B：low-memory ZIP Level 4

### 候选身份与正确性

候选只把 low-memory workspace 的 `set_compression_level(5)` 改为 `4`。

| 项目 | 值 |
|---|---|
| candidate commit | `b2795081f452e9267efda48b27c2506a94a60785` |
| candidate tree | `6f2db94981f2c5ca375d336926a4e99f08ef96d5` |
| candidate binary SHA-256 | `bbe175f7ae569350808e45e4c4bc48261e0c707f39c76a337ce520ac0298baf2` |
| candidate binary bytes | `3,954,176` |
| release 构建耗时 | `49.218s` |

验证：

- `cargo fmt --check` 通过；
- 全 workspace/target/feature Clippy `-D warnings` 通过；
- Rust 全 workspace/feature 测试 `209 passed`；
- synthetic GB/SK config、check-only、normal、禁止覆盖 smoke 通过；
- 真实 SK 为 package fast path、`mismatch_count = 0`；
- SK `error_log_count = 201,815`，全部 run counts 与 baseline 一致；
- 临时 workspace 正常清理。

### 输出门禁与判定

| 指标 | Level 5 baseline | Level 4 candidate | 硬门禁 | 结果 |
|---|---:|---:|---:|---|
| SK 输出 bytes | `43,611,045` | `49,107,509` | `<= 48,658,823` | 超出 `448,686` |

`Rejected`。候选在性能配对前已经违反输出体积硬门禁，因此按计划停止：

- 不运行 Level 4 的正式 8 对；
- 不通过中位数或挑选较小样本规避最大输出规则；
- 不继续试已拒绝的 Level 3/2，也不扩展到 Level 1；
- Level 5 保持为 `T1_IO_BASELINE`。

## T1-C：Thin LTO

### 候选身份与正确性

候选只在 release profile 增加 `lto = "thin"`；没有设置 `strip` 或修改其他 profile 选项。

| 项目 | 值 |
|---|---|
| candidate commit | `bc16711ea55e1fa28808cfd1c4ab36d622b4d629` |
| candidate tree | `ecf35e96d95803c5ffef87f78edf79b86ad75374` |
| candidate binary SHA-256 | `7ae362715cafeab00eef65b0500642198f24bbdc4b358d8dd72842608ccc4687` |
| candidate binary bytes | `4,123,648` |
| baseline binary bytes | `3,954,176` |
| binary 体积变化 | `+169,472`（约 `+4.29%`） |
| release 构建耗时 | `48.337s` |

验证：

- `cargo fmt --check` 通过；
- 全 workspace/target/feature Clippy `-D warnings` 通过；
- Rust 全 workspace/feature 测试 `209 passed`；
- synthetic GB/SK config、check-only、normal、禁止覆盖 smoke 通过；
- 真实 GB/SK 均为 package fast path、`mismatch_count = 0`；
- GB/SK error logs 和全部 run counts 与 baseline 一致；
- SK 最大输出 `43,611,045 bytes`，低于硬门禁。

### SK normal 8 对

| 指标 | baseline 中位数 | candidate 中位数 | 对内相对差中位数 | candidate 胜场 |
|---|---:|---:|---:|---:|
| external wall | `17.145842s` | `17.079366s` | `-0.2937%` | `5/8` |
| PWS | `1,462,044,672` | `1,461,028,864` | `-0.0339%` | `6/8` |
| `ingest` | `5.885653s` | `5.962518s` | `+1.4303%` | `0/8` |
| `normalize` | `1.061977s` | `1.025222s` | `-2.7422%` | `8/8` |
| `writer_populate` | `5.267210s` | `5.154404s` | `-2.4019%` | `6/8` |
| `xlsx_save` | `3.033316s` | `3.015216s` | `+0.1119%` | `4/8` |
| `total` | `8.404742s` | `8.434541s` | `+0.2557%` | `2/8` |

### 判定

`Rejected`：

- external wall 只改善 `0.2937%`，未达到 `1%`；
- wall 只赢 `5/8`，未达到 `6/8`；
- 局部阶段改善不能替代预冻结的端到端门槛；
- binary 额外增加 `169,472 bytes`，没有被足够的端到端收益抵消。

因此 release profile 保持 `codegen-units = 1`，不加入 Thin LTO。

## T2-A：reader f64→Decimal 整数快路径

### 实现与边界

候选只对以下 f64 直接使用 `Decimal::from(i64)`：

- finite；
- `fract() == 0.0`；
- `value >= i64::MIN as f64`；
- `value < i64::MAX as f64`。

上界必须是严格小于，因为 `i64::MAX as f64` 会舍入为 `2^63`。其他值全部走保留的字符串转换路径。新增逐值等价测试覆盖：

- `0.0`、`-0.0`、正负整数；
- `2^53` 附近的可表示整数；
- `i64` 上下边界及范围外相邻 f64；
- `0.1`、`12.34`、极小/极大有限小数；
- NaN、正负无穷。

### 候选身份与正确性

| 项目 | 值 |
|---|---|
| candidate commit | `b854f66e6ffaafe412a5d262d83fc7a0bfa75772` |
| candidate tree | `8b8cd7f0679bb65ff5bc28bfd7e6bacb4b90ea90` |
| candidate binary SHA-256 | `9d605a81c88dc924883176d57d15a8ac624810ed07f4a8e5a3095a871e7a1f70` |
| candidate binary bytes | `3,954,688` |
| release 构建耗时 | `46.271s` |

验证：

- `cargo fmt --check` 通过；
- 全 workspace/target/feature Clippy `-D warnings` 通过；
- 新增 2 个 reader 边界测试后 Rust 全 workspace/feature 测试 `211 passed`；
- synthetic GB/SK config、check-only、normal、禁止覆盖 smoke 通过；
- 真实 GB/SK normal 均为 package fast path、`mismatch_count = 0`；
- GB `2025-07` 与 SK `2024-01` 单月过滤均为 package fast path、`mismatch_count = 0`；
- normal 与单月的 error logs、issue counts 和全部 run counts 均与 baseline 一致。

### SK check-only 8 对

| 指标 | baseline 中位数 | candidate 中位数 | 对内相对差中位数 | candidate 胜场 |
|---|---:|---:|---:|---:|
| external wall | `9.012124s` | `8.513377s` | `-5.8975%` | `8/8` |
| PWS | `1,462,609,920` | `1,462,511,616` | `+0.0126%` | `4/8` |
| `ingest` | `6.055721s` | `5.516892s` | `-8.9262%` | `8/8` |
| `normalize` | `1.067906s` | `1.079538s` | `+1.3450%` | `3/8` |
| `total` | `8.585318s` | `8.071873s` | `-5.9715%` | `8/8` |

### SK normal 8 对

| 指标 | baseline 中位数 | candidate 中位数 | 对内相对差中位数 | candidate 胜场 |
|---|---:|---:|---:|---:|
| external wall | `17.803005s` | `17.099393s` | `-5.0787%` | `8/8` |
| PWS | `1,461,989,376` | `1,460,744,192` | `-0.0852%` | `6/8` |
| `ingest` | `6.112438s` | `5.510102s` | `-8.9487%` | `8/8` |
| `normalize` | `1.096922s` | `1.082877s` | `-0.5792%` | `4/8` |
| `writer_populate` | `5.492530s` | `5.421051s` | `-3.3140%` | `5/8` |
| `xlsx_save` | `3.048647s` | `3.034948s` | `-0.2945%` | `5/8` |
| `total` | `8.771326s` | `8.066715s` | `-7.5250%` | `8/8` |

candidate 最大输出为 `43,611,045 bytes`，低于硬门禁。

### 判定

`Adopted for final stack`：

- check-only 与 normal 的 ingest 分别改善 `8.9262%`、`8.9487%`，超过 `5%`，并均赢 `8/8`；
- check-only 与 normal external wall 分别改善 `5.8975%`、`5.0787%`，超过 `1%`，并均赢 `8/8`；
- 正确性保持 package fast path；
- PWS、输出和非目标阶段没有越过回退或绝对门禁。

该候选进入后续组合与最终栈验证；最终生产提交仍需通过其余候选选择和完整完成定义。

## T2-B：forward-fill

### 聚合计数与候选边界

临时 profiling binary 只输出计数，不输出 cell 值、文件名或路径，也不进入生产提交。真实 SK check-only 的聚合结果：

| 指标 | 计数 |
|---|---:|
| 数据行 | `467,419` |
| 已解析 fill 列 | `11` |
| fill cell 访问 | `5,141,609` |
| 提前 clone 的空白值 | `4,881,063` |
| 非空 seed clone | `260,546` |
| 实际填充 clone | `4,873,936` |
| 重复“集成车间”判断 | `5,141,609` |
| `Blank / Text / Decimal / DateLike` | `4,881,063 / 235,930 / 24,616 / 0` |

因此该假设具备进入最小候选的数量基础。候选只做：

- 借用当前 cell 判断空白，不再提前 clone 空白值；
- 非空值成为 seed 时才 clone；
- 实际填充时仍 clone seed，保留必要的拥有权语义；
- 成本中心完成本行填充后只判断一次 `integrated_row`；
- 供应商列继续禁止向集成车间填充，也禁止集成车间自身供应商成为后续 seed。

新增/加强测试覆盖成本中心由上一行填成集成车间、集成车间自带供应商、之后恢复普通车间，以及集成车间供应商不成为后续 seed。

### 候选身份与正确性

| 项目 | 值 |
|---|---|
| candidate commit | `a0526154e406d583f1c2d0ce210a5ae634d5d21f` |
| candidate tree | `7aff5289168471da1463501b82c6e56dc8920f88` |
| candidate binary SHA-256 | `1c209d4af9aab6a987503d1cd4b56a7506f83cc7855b7c13c19bd91e3347de7d` |
| candidate binary bytes | `3,953,152` |
| release 构建耗时 | `56.844s` |

验证：

- `cargo fmt --check` 通过；
- 全 workspace/target/feature Clippy `-D warnings` 通过；
- Rust 全 workspace/feature 测试 `210 passed`；
- synthetic GB/SK config、check-only、normal、禁止覆盖 smoke 通过；
- 真实 GB/SK normal 均为 package fast path、`mismatch_count = 0`；
- GB `2025-07` 与 SK `2024-01` 单月过滤均为 package fast path、`mismatch_count = 0`；
- normal 与单月的 error logs、issue counts 和全部 run counts 均与 baseline 一致。

### SK check-only 8 对

| 指标 | baseline 中位数 | candidate 中位数 | 对内相对差中位数 | candidate 胜场 |
|---|---:|---:|---:|---:|
| external wall | `9.410158s` | `9.431708s` | `-0.2928%` | `5/8` |
| PWS | `1,462,528,000` | `1,462,355,968` | `+0.0062%` | `4/8` |
| `ingest` | `6.092958s` | `6.129409s` | `+0.2482%` | `3/8` |
| `normalize` | `1.174566s` | `1.134426s` | `-3.6372%` | `8/8` |
| `total` | `8.884006s` | `8.879895s` | `-0.3402%` | `5/8` |

全部运行成功、无临时残留，binary/input 前后 SHA-256 不变。

### 判定

`Rejected`：

- `normalize` 虽赢 `8/8`，但只改善 `3.6372%`、绝对减少 `0.040140s`；
- 两项都未达到预冻结的“`>= 10%` 或 `>= 0.15s`”门槛；
- external wall 没有回退风险，但也没有足以抵消额外分支逻辑的端到端收益。

按停止条件，不再运行该候选的 normal 8 对，不与 reader 组合，也不进入最终生产提交。profiling 计数保留为脱敏 evidence。

## T3-0：文本基数与内存 census

临时 census binary 基于已采纳的 reader 候选读取真实 SK，只写入忽略目录中的聚合 JSON。报告使用 `column_NNN` 别名，不记录 cell 值、输入文件名或绝对路径。

### 全局聚合

| 指标 | 值 |
|---|---:|
| 行 / 列 / cell | `467,420 / 30 / 14,022,600` |
| `Blank` | `8,552,257` |
| `Text` | `1,058,525` |
| `Decimal` | `4,411,818` |
| `DateLike` | `0` |
| 非空文本 cell | `1,058,525` |
| 按列唯一文本合计 | `14,157` |
| 全部文本 UTF-8 bytes | `13,507,393` |
| 唯一文本 UTF-8 bytes | `198,508` |
| 重复文本 bytes 理论上界 | `13,308,885` |

13 个含文本列中，12 列的唯一值不超过 `4,096`；唯一的高基数列为 `column_012`，唯一值 `4,899`、重复率约 `98.7%`。其余活跃列的唯一值范围为 `2` 至 `4,047`，重复率约 `83.6%` 至 `99.99%`。

### 表示尺寸与估算

| 项目 | 值 |
|---|---:|
| `size_of::<CellValue>()` | `32 bytes` |
| `size_of::<ArcCellValue>()` | `24 bytes` |
| `size_of::<String>()` | `24 bytes` |
| `size_of::<Arc<str>>()` | `16 bytes` |
| `size_of::<Decimal>()` | `16 bytes` |
| 全部 raw cell 行内理论节省 | `112,180,800 bytes` |
| 全驻留 Arc header 上界 | `226,512 bytes` |
| 全驻留 hash bucket 估算上界 | `440,708 bytes` |
| 全部文本 Arc 引用 payload | `16,936,400 bytes` |

这些是布局和输入基数估算，不替代外部 PWS 实测。

### 性能实验前冻结的容量策略

- 每个源列使用独立驻留池，禁止全局池；
- 文本按原始完整内容比较，不 trim、不规范化；
- `Blank` 与空字符串不进入驻留池；
- 每列最多接纳 `8,192` 个唯一文本；
- 达到上限后继续复用池中已有值，但新值使用独立 `Arc<str>` 且不再进入池；
- 30 列的条目数理论上界为 `245,760`；
- T3-A 不启用驻留；只有 T3-A 的 wall 回退不超过 `2%`、PWS 回退不超过 `5%` 才启动 T3-B；
- 容量不根据后续性能样本重新调整。

## T3-A：仅使用 `Arc<str>` 表示

### 实现、身份与正确性

`CellValue::Text` 与 `CellValue::DateLike` 从 `String` 改为 `Arc<str>`；本阶段不建立驻留池。现有 serde 只增加 `rc` feature，feature tree 除 `serde/rc` 外没有新增依赖或版本变化。新增 JSON golden 覆盖 Blank、Text、Decimal 和 DateLike，保持原有 `{"kind": ..., "value": ...}` 结构和文本前后空白。

| 项目 | 值 |
|---|---|
| candidate commit | `0a55e399adc2c9150c8e1334dcb0686dab6d350e` |
| candidate tree | `5d73bacb88b25196a3d90e7b7873b1d09f973c41` |
| candidate binary SHA-256 | `b3ddc8621c773dd58aae2386f7777f01d5f592583acf9b92aa609a5478faa421` |
| candidate binary bytes | `3,964,416` |
| release 构建耗时 | `53.089s` |
| 增量基线 | reader candidate `b854f66e6ffaafe412a5d262d83fc7a0bfa75772` |

验证：

- `cargo fmt --check` 通过；
- 全 workspace/target/feature Clippy `-D warnings` 通过；
- Rust 全 workspace/feature 测试 `212 passed`，包含新增序列化 golden；
- synthetic GB/SK config、check-only、normal、禁止覆盖 smoke 通过；
- 真实 GB/SK normal 均为 package fast path、`mismatch_count = 0`；
- GB `2025-07` 与 SK `2024-01` 单月过滤均为 package fast path、`mismatch_count = 0`；
- normal 与单月的 error logs、issue counts 和全部 run counts 均与 reader 增量基线一致。

### SK normal 8 对

| 指标 | reader 基线中位数 | T3-A 中位数 | 对内相对差中位数 | T3-A 胜场 |
|---|---:|---:|---:|---:|
| external wall | `16.849230s` | `16.518982s` | `-3.0814%` | `7/8` |
| PWS | `1,461,522,432` | `1,094,889,472` | `-25.0368%` | `8/8` |
| `ingest` | `5.489897s` | `5.671866s` | `+1.9902%` | `0/8` |
| `normalize` | `1.080456s` | `0.695850s` | `-36.4148%` | `8/8` |
| `split` | `0.207736s` | `0.177926s` | `-15.6568%` | `8/8` |
| `total` | `8.043537s` | `7.837172s` | `-4.5866%` | `8/8` |
| `writer_populate` | `5.258061s` | `5.336884s` | `-0.8565%` | `4/8` |
| `xlsx_save` | `3.026082s` | `3.035966s` | `+0.2447%` | `2/8` |

candidate 最大输出为 `43,611,045 bytes`，全部运行成功、无临时残留，binary/input 前后 SHA-256 不变。

### 阶段判定

`Continue to T3-B`：

- external wall 没有回退，实际改善 `3.0814%`；
- PWS 没有回退，实际下降 `25.0368%`；
- 两项均明显优于 T3-A 的 `wall <= +2%`、`PWS <= +5%` 继续门槛；
- ingest 的 `1.9902%` 回退由 clone 密集阶段和端到端改善抵消，但仍作为 T3-B 需要复核的非目标阶段记录。

T3-A 尚不单独进入正式生产提交；继续使用已冻结容量进行 T3-B，并按最终 Tier 3 Go/No-Go 判断整个核心模型候选。

## T3-B：有界按列文本驻留

### 实现、身份与正确性

候选在 T3-A 之上为每个源列建立独立 `HashSet<Arc<str>>`：

- 每列最多驻留 `8,192` 个唯一非空文本；
- 查找使用原始 `String`，命中后复用已有 `Arc<str>`，避免为查找先分配 Arc；
- 空字符串和 blank 不进入池；
- 达到上限后继续复用池中已有值，新值改为独立 Arc；
- 文本保持逐字节精确语义，不 trim、不规范化；
- 读取完成后释放池，cell 持有的共享 Arc 继续有效。

测试覆盖按列隔离、命中复用、容量上限、达到上限后仍复用旧值、空文本跳过和前后空白保留。

| 项目 | 值 |
|---|---|
| candidate commit | `73b364f3457639b17f3e55b416bac9a5e6886147` |
| candidate tree | `31eff10c0523f4a75972b17dc5d297b04d5a3e1b` |
| candidate binary SHA-256 | `a9b8a1def64b990ff3c916318d32b9b695964ad40dff317fe80514173fd0e255` |
| candidate binary bytes | `3,969,536` |
| release 构建耗时 | `46.367s` |
| 增量基线 | T3-A `0a55e399adc2c9150c8e1334dcb0686dab6d350e` |

验证：

- `cargo fmt --check` 通过；
- 全 workspace/target/feature Clippy `-D warnings` 通过；
- Rust 全 workspace/feature 测试 `214 passed`；
- synthetic GB/SK config、check-only、normal、禁止覆盖 smoke 通过；
- 真实 GB/SK normal 均为 package fast path、`mismatch_count = 0`；
- GB `2025-07` 与 SK `2024-01` 单月过滤均为 package fast path、`mismatch_count = 0`；
- normal 与单月的 error logs、issue counts 和全部 run counts 均与 T3-A 增量基线一致。

### SK normal 8 对

| 指标 | T3-A 中位数 | T3-B 中位数 | 对内相对差中位数 | T3-B 胜场 |
|---|---:|---:|---:|---:|
| external wall | `16.308401s` | `16.656838s` | `+2.5359%` | `2/8` |
| PWS | `1,096,034,304` | `1,049,671,680` | `-4.2300%` | `8/8` |
| `ingest` | `5.624883s` | `5.764194s` | `+3.4026%` | `1/8` |
| `normalize` | `0.687579s` | `0.701945s` | `+0.7702%` | `4/8` |
| `split` | — | — | `-5.0960%` | `6/8` |
| `total` | `7.739508s` | `7.915154s` | `+2.9953%` | `2/8` |
| `writer_populate` | — | — | `+3.2053%` | `0/8` |
| `xlsx_save` | — | — | `+2.1030%` | `2/8` |

candidate 最大输出为 `43,611,045 bytes`，全部运行成功、无临时残留，binary/input 前后 SHA-256 不变。

### 判定

`Rejected`：

- PWS 虽再下降 `4.2300%` 且赢 `8/8`，但相对 T3-A 的额外收益不足以抵消 CPU 与复杂度成本；
- external wall 回退 `2.5359%`、仅赢 `2/8`，越过最终栈允许的 `2%` 回退红线；
- ingest 与 total 分别回退 `3.4026%`、`2.9953%`，说明 hash 查找和维护驻留池的成本真实存在；
- 候选增加约 124 行实现与测试，而 T3-A 已单独达到 Tier 3 的 PWS 和 wall 目标。

因此不调整已冻结容量、不继续搜索驻留参数，也不把 T3-B 纳入最终栈。

## Tier 3 Go/No-Go

`Go with T3-A only`：最终候选固定为“reader 整数快路径 + `Arc<str>` 表示”，排除有界文本驻留。

相对 reader 增量基线，T3-A 的 PWS 下降 `25.0368%`、external wall 改善 `3.0814%`。
正式核心模型改造的单独确认已于 2026-07-29 获得，随后只合入 reader 与 T3-A。

## 最终候选验证

最终候选身份：

| 项目 | 值 |
|---|---|
| 组成 | reader 整数快路径 + T3-A `Arc<str>` |
| candidate commit | `0a55e399adc2c9150c8e1334dcb0686dab6d350e` |
| candidate tree | `5d73bacb88b25196a3d90e7b7873b1d09f973c41` |
| performance binary SHA-256 | `b3ddc8621c773dd58aae2386f7777f01d5f592583acf9b92aa609a5478faa421` |
| performance binary bytes | `3,964,416` |
| original baseline | `9198b2a07f0fe269492803f083c680f897c71730` |

### 相对原始基线的 SK normal 8 对

| 指标 | 原始基线中位数 | 最终候选中位数 | 对内相对差中位数 | 候选胜场 |
|---|---:|---:|---:|---:|
| external wall | `17.367148s` | `16.252091s` | `-6.1896%` | `8/8` |
| PWS | `1,461,600,256` | `1,097,183,232` | `-24.9597%` | `8/8` |
| `ingest` | `5.945854s` | `5.601644s` | `-5.8432%` | `8/8` |
| `normalize` | `1.070302s` | `0.692365s` | `-35.9974%` | `8/8` |
| `split` | `0.201576s` | `0.172712s` | `-14.0071%` | `8/8` |
| `total` | `8.538398s` | `7.701900s` | `-9.9978%` | `8/8` |
| `writer_populate` | `5.275994s` | `5.205648s` | `-1.0817%` | `6/8` |
| `xlsx_save` | `2.989084s` | `3.010407s` | `+0.7044%` | `1/8` |

最大输出为 `43,611,045 bytes`。全部运行成功，SK `error_log_count = 201,815`、issue-type counts 和全部 run counts 一致；无临时残留；binary/input 前后 SHA-256 不变。`xlsx_save` 的小幅回退低于 `2%` 非目标阶段红线，且被 writer populate、总业务阶段和 external wall 的稳定改善覆盖。

因此最终候选同时满足 PWS 改善 `>= 15%`、wall 回退 `<= 2%`、输出体积和稳定胜场门槛。

### 完整正确性与真实 workbook

- `cargo fmt --check` 通过；
- 全 workspace/target/feature Clippy `-D warnings` 通过；
- Rust 全 workspace/feature 测试 `212 passed`；
- Ruff check 与 format check 通过；
- Python 公共测试在先建立仓库内忽略的 `.pytest-tmp` 后为 `63 passed`；首轮因父目录不存在产生的 42 个 setup error 均为 `WinError 3`，没有断言失败；
- synthetic GB/SK config、check-only、normal、禁止覆盖 smoke 通过；
- 最终候选直接对原始基线的 GB/SK normal 均为 package fast path、`mismatch_count = 0`；
- GB `2025-07` 与 SK `2024-01` 单月过滤均为 package fast path、`mismatch_count = 0`；
- 四组直接比较的 Sheet 数、error logs、issue counts 和全部 run counts 均一致；
- GB 全量 `error_log_count = 20,515`，SK 全量 `error_log_count = 201,815`。

### 最终 N=5

| Pipeline | Wall 中位数 | PWS 中位数 | 最大输出 | 结果 |
|---|---:|---:|---:|---|
| GB | `1.8331s` | `327,053,312` | `3,808,077` | `Passed` |
| SK | `16.2958s` | `1,096,060,928` | `43,611,044` | `Passed` |

两条管线均低于计划固定的 wall、PWS 和最大输出硬门禁。

### Windows 包与运行契约

从干净的 detached candidate commit 构建本地 `v0.3.0` Windows ZIP：

| 项目 | 值 |
|---|---|
| source commit | `0a55e399adc2c9150c8e1334dcb0686dab6d350e` |
| source date epoch | `1785256768` |
| archive SHA-256 | `c8039e44cbe9af0486e0416187cd4b8cedb1c2741407a79b4bab15382dd7bdd8` |
| packaged executable SHA-256 | `4091823470604538b4f7a67a59b680b26c0ed2f12e1899920c8995d332fd535d` |

包 smoke 通过：

- 包布局和所有内部 SHA-256 正常；
- `--help` 与 `--version-json` 正常，build identity 与 source commit 一致；
- 子进程环境被清空，`PATH` 仅为 `C:\WINDOWS\System32;C:\WINDOWS`；
- GB/SK config、check-only 和 normal 均通过；
- check-only 不写 workbook；
- normal Manifest 的最终输出标志与 workbook SHA-256 正常；
- low-memory 成功路径在真实 SK 8 对与 N=5 中均无临时残留；
- low-memory 失败清理、原子发布、Manifest V1、路径脱敏和 SHA-256 契约由全量 Rust 测试覆盖并通过。

## 正式采纳与组合分支复验

正式采纳确认后，隔离候选按原顺序无冲突合入：

| 变更 | 正式提交 |
|---|---|
| reader 安全整数快路径 | `26f29e3a0719378793a66b32b5db5e55e491fe2b` |
| T3-A `Arc<str>` | `6d0f2747951f0704696cf651371f1c2b52d1e45b` |

合入后的 `rust/Cargo.toml`、`rust/Cargo.lock`、`costing-core` 和 `costing-xlsx`
与已测隔离候选逐文件一致。`Cargo.lock` 未变化；只启用现有 serde `rc` feature。

正式组合分支验证：

- `cargo fmt --check` 通过；
- 全 workspace/target/feature Clippy `-D warnings` 通过；
- Rust 全 workspace/feature 测试 `212 passed`；
- `uv sync --frozen --extra dev`、Ruff check/format 通过；
- Python 公共测试（含 paired driver 契约）`66 passed`；
- release binary 大小 `3,964,416 bytes`，SHA-256
  `2e063bbfb57e27b2454ddd869a99b44cd68772158f45ff76abcfab0cb231be5e`；
- synthetic GB/SK config、check-only、normal、禁止覆盖 smoke 通过；
- 正式 binary 直接对原始冻结 baseline 的 GB/SK normal、GB `2025-07`、
  SK `2024-01` 全部为 package fast path、`mismatch_count = 0`；
- 四组真实比较的 Sheet 数、error logs、issue counts 和全部 run counts 一致。

### 正式组合分支 N=5

| Pipeline | Wall 中位数 | PWS 中位数 | 最大输出 | 结果 |
|---|---:|---:|---:|---|
| GB | `1.7881s` | `327,282,688` | `3,808,078` | `Passed` |
| SK | `16.4495s` | `1,096,015,872` | `43,611,045` | `Passed` |

### 正式组合分支 Windows 包

| 项目 | 值 |
|---|---|
| source commit | `6d0f2747951f0704696cf651371f1c2b52d1e45b` |
| source date epoch | `1785259380` |
| archive SHA-256 | `53ae1bb25dc94aa2f65a9c24a9f4574ec4966f72a7c16386bdd0b025f89386f0` |
| packaged executable SHA-256 | `0f4c2d7640c1a0ccee2ecdef914092fe91d5fa8794c486b3c1e0fa61017f4b7b` |

包布局、内部哈希、`--help`、`--version-json`、GB/SK config、check-only、
normal 和 Manifest 均通过；child `PATH` 仅为
`C:\WINDOWS\System32;C:\WINDOWS`。

## 完成与范围边界

- 本计划内候选、采纳、正确性、真实数据、性能、包 smoke、决策和当前事实同步均完成。
- 真实 workbook、输出、原始报告和本地 ZIP 全部保留在忽略目录，未提交。
- 未推送分支、创建 PR、推送标签、创建 Release 或发布外部资产；这些仍需单独确认。

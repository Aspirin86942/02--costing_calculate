# 优化评估报告 — 2026-07-17

## 一、性能现状

基于本机实测（SK 管线，467,420 reader rows → 425,459 detail rows，release binary，暖跑）：

| 阶段 | SK 典型耗时 | 占比 | 性质 |
|---|---:|---:|---|
| ingest（calamine 读 + float→Decimal） | ~5.2s | ~26% | I/O + 外部 crate parse，受磁盘/AV 缓存影响大 |
| normalize（清洗 + 填充 + 月过滤） | ~1.5s | ~8% | 自家代码，`cell_text` 反复分配 String |
| split + fact + presentation | ~1.6s | ~8% | `rust_decimal` 运算为主 |
| **export（写 + deflate 压缩）** | ~11.7s | **~58%** | `rust_xlsxwriter` 逐 cell 写 + zip deflate compression level 5 |
| **端到端 SK** | **~20s** | 100% | |

**瓶颈排序**：export > ingest ≫ normalize > fact/split/presentation。

GB 管线规模小（40k 行），全链路 < 2.5s，优化收益几乎全在 SK。

## 二、LTO 实验结论

| 项目 | 结论 |
|---|---|
| 方法 | `CARGO_PROFILE_RELEASE_LTO=thin` + `STRIP=symbols` 重编（env var，不改文件），与 no-LTO 交错 + 反序 A/B（8 对 check-only + 3 对 full，N≥8 per config） |
| **CPU 密集阶段收益** | fact −2.8%（7/8 对比胜率）、writer_populate −2.6%、normalize −2.0% |
| 纯外部 crate | xlsx_save（deflate）−0.9%（噪声内）、ingest calamine 解析被 I/O 噪声主导 |
| **端到端收益** | 分析 8.2s(−2%) + 导出 11.7s(−2%) ≈ **−1.5~2%（SK 上约 −0.4s）** |
| 编译成本 | 全量重编 +2m40s |
| **建议** | **收益真实但小，边际可纳。不做本轮优先，留作后续备选。** 若未来 CLI 运行频率提高或自家逻辑变重，重新评估。 |

> ⚠️ 教训：本机噪声极大（同 binary 的 `total` 在负载时段能从 ~8s 飙到 ~49s），第一轮在嘈杂环境下跑出"无收益"误判，机器安静后小收益才显出来。今后做 perf A/B 必须交替 + 反序 + N≥8，切忌单次对比。

## 三、优化方向（按优先级）

### P0 · 低风险快赢

| # | 方向 | 位置 | 预期收益 | 风险 | 说明 |
|---|---:|---|---:|---:|---|
| 1 | **`cell_text` 借用化** | `costing-core/src/normalize.rs:402` | normalize ~1.5s 中削减数百万次堆分配，预期 −0.3~0.5s | **低** | 纯重构，baseline 不变；加 `fn cell_text_str(&CellValue) -> &str` 给比较类调用点替换，不改变存储语义。改完跑 `cargo test` 即可验证 |
| 2 | **压缩级别 5→2/1** | `costing-xlsx/src/writer.rs:158` | xlsx_save ~5s 可降 1–2s | **中** | 参数实验，零代码风险。**约束：SK 输出 43.6MB / 上限 48.6MB，headroom 仅 5MB**。级别 2 预期 size ~45–46MB（勉强在线内），级别 1 可能超线。需实测 N=5 确认 size 不超 48.6MB 验收线 |

### P1 · 中等工作量

| # | 方向 | 位置 | 预期收益 | 风险 | 说明 |
|---|---:|---|---:|---:|---|
| 3 | **float→Decimal 快路径** | `costing-xlsx/src/reader.rs:128` | ingest 中可控部分，预期 −0.3~0.8s | **中** | 当前 f64→String→Decimal 字符串往返。可加整数/简单小数直接构造快路径，复杂值回退现逻辑。**必须用 oracle 全量比对**（`tests/test_full_rust_cli_oracle.py`）确认逐 cell 数值不变 |
| 4 | **release profile 加 LTO thin** | `rust/Cargo.toml:[profile.release]` | 端到端 −1.5~2%（~0.4s） | **低** | 见第二节结论。加 `lto = "thin"` + `strip = "symbols"` 两行。是否 adopt 看 CLI 运行频率 |

### P2 · 中后期战略（单独立项）

| # | 方向 | 说明 |
|---|---:|---|
| 5 | **`CellValue` 字符串驻留** | `CellValue::Text(String)` → `Arc<str>` 或列级 string pool。年期/成本中心/产品编码/产品名称/成本项目名称/工单编号等列基数低、重复率极高。预期 SK PWS −30~50%（当前 1.46GB / 上限 2GB），并为更大输入留出 headroom。**大改造**：touches reader/normalize/writer/presentation 全链路 + `PartialEq`/`Serialize` 派生语义，必须重跑 oracle 全量比对 |
| 6 | **Python legacy 分批退场** | `docs/python_retirement_after_rust.md` 已列待退场清单。Rust 已于 2026-07-10 验收通过。首批删 `src/excel/product_anomaly_writer.py` 和 `table_rendering.py` 产品维度部分（Rust 不实现 `成本分析产品维度`）。main.py + `src/etl|analytics|excel|services` 待 Rust 稳定运行 N 周期后再退。`tests/rust_oracle/`（~17k 行 harness）可精简。不提升运行时性能，但显著降低维护成本 |

### 不做

| 方向 | 理由 |
|---|---|
| 多线程并行 | 管线本质串行 I/O；3 张输出 sheet 互独立但 `rust_xlsxwriter` Workbook 跨 worksheet 并行不友好；分析阶段合计仅 ~1.6s，并行收益 <1s |
| `panic = "abort"` | 改变 unwinding 语义，low-memory writer 临时目录在 panic 时不清理；收益 ~5% 但测试需全量重跑，性价比不高 |
| Decimal→f64 改回高精度写出 | 既有契约（Python xlsxwriter 同口径），oracle 已验证，不要动 |
| 换 Reader（calamine 替代） | calamine 物化整表 `Range<Data>` 是 ingest 内存大户，但无成熟的流式 xlsx reader 可平滑替代；成本高收益不确定，仅当 P0–P2 全做完仍不达标再考虑 |

## 四、建议落地顺序

```
P0-1 cell_text 借用化 → 纯重构 → cargo test → worktree/branch 开搞
P0-2 压缩级别实验 → 参数调参 → 完整 A/B（盯 size 不超线）
P1-3 float→Decimal 快路径 → 中等工作量 → oracle 全量比对
P1-4 LTO thin → 决策是否 adopt → 两行 Cargo.toml 改
P2-5/6 → 中长期单独立项，不在本轮
```

## 五、A/B 测试方法论

本机 dev 环境噪声极大（Windows Defender、Search 索引、后台进程），同 binary 同参数前后脚跑差异可达 ±50%。以下方法来自 LTO 实验的教训：

- **交错跑**（interleaved）：A,B,A,B... 而非先全跑 A 再全跑 B
- **逐对反序**：奇数对 nolto→lto，偶数对 lto→nolto，让顺序偏差在汇总时抵消
- **看 CPU 密集阶段**（fact、writer_populate、xlsx_save），ingest 受 I/O 噪声主导不适合做判决信号
- **N≥8 取中位数 + 最小值**（min ≈ 机器最空时的真实 CPU 成本）
- **预热后再测**：先各跑 1 次丢弃，确认 total 回到正常范围（SK ~8s check-only）再开测
- **权威验收**仍以 `docs/evidence/` 的 N=5 同机快照为准，本机 dev 数字只用于定位瓶颈比例和 A/B 方向

详见项目记忆 `[[perf-ab-method]]` 和 `[[rust-lto-marginal-benefit]]`。

# 生产代码深度简化 — 实际变更记录

- 日期:2026-08-13
- 计划:[`../plans/2026-08-13-production-code-simplification.md`](../plans/2026-08-13-production-code-simplification.md)
- 实施计划:[`../plans/2026-08-13-production-code-simplification-implementation.md`](../plans/2026-08-13-production-code-simplification-implementation.md)
- 范围:全部 Rust 生产代码(11 批),行为与性能双重不变
- 结论:**完成,全部门禁通过;最终全分支审查 Approved**

## 批次与提交

| 批 | 模块 | 提交 | 应用/跳过 |
|---|---|---|---|
| 1 | core: presentation.rs | `d888649` | 5 / 6 |
| 2 | core: normalize.rs | `0dd4407` | 7 / 4 |
| 3 | core: table.rs | `5a3b6a2` | 4 / 8 |
| 4 | core: fact.rs(护栏) | `4d31534` | 4 / 7 |
| 5 | core: anomaly.rs | `e0675ce` | 4 / 6 |
| 6 | core: quality/split/process 等小模块 | `7750286` | 4 / 10(7 个文件审阅后无需改动) |
| 7 | cli: manifest.rs | `06beff6` | — |
| 8 | cli: run/run_paths/runner | `684b90c` | 3 文件(11 个文件跳过:clap 语义、退出码、TOML/schema、serde 形状均为兼容边界) |
| 9 | xlsx: writer.rs(护栏) | `c7dcfdc` | 3 hunks |
| 10 | xlsx: reader/snapshot/atomic_file(护栏) | `e1cf36e` | 4 项(lib.rs 跳过) |
| 11 | costing-oracle-tests | 无提交 | 104 行审阅后无需改动 |

每批一个 commit,每个 commit 经独立任务审查(规格 + 质量双判定),全部 Approved;每条简化逐 hunk 证明行为等价。

## 门禁与契约证据

- 每批四条门禁全绿:`cargo fmt --check`、`clippy --all-targets --all-features -D warnings`、`cargo test --workspace --all-features`(18 套件 0 failed)、`pytest -m "not slow and not benchmark"`(66 passed)。
- `tests/contracts/baselines/` 全程零 diff。
- **输出逐字节一致**:简化前基线二进制(Task 0 保存)与最终候选对 GB/SK 同输入输出字节数完全相同(GB 4,206,405 = 4,206,405;SK 43,611,044 = 43,611,044)。
- 护栏(基线二进制 vs 候选,8 对交错配对):fact 批 wall −0.54% / PWS −0.002%;writer 批 wall +3.13%(审查证明 hunks 工作中性或减工作,±pair 噪声范围,输出 delta 0.0)/ PWS −0.007%;reader 批 wall +0.38% / PWS −0.09%。
- 最终 N=5 硬门禁:
  - SK:**PASSED** — wall 11.998s ≤ 20.0s;PWS 1.05 GiB ≤ 2 GiB;输出 43,611,045 ≤ 48,658,823。
  - GB(冻结样本):**PASSED** — wall 1.409s ≤ 3.2554s;PWS 327.8 MB ≤ 375.7 MB;输出 3,808,076 ≤ 4,194,321。

## 重要发现:data/raw/gb 样本已更换

GB N=5 用当前 `data/raw/gb` 样本(sha `99644502…`,40,057 明细行)时输出 4,206,405 超出门槛 4,194,321,但**简化前基线二进制与候选输出逐字节相同**(4,206,405 = 4,206,405),证明与本计划无关。

对照冻结 Phase 0A 基线(`docs/performance/baselines/2026-07-11-windows-x64-phase0a.json`)的 GB 输入 sha `6aa5e3e7…`:当前样本不匹配——data/raw/gb 的样本在冻结后被换成了更新的导出(冻结时 54,752 明细行 vs 当前 40,057)。SK 样本 sha 与冻结值一致,未更换。

原始冻结样本在本机找到:`D:\01- 工作\2026年\28- 如本尽调\PBE\成本\gb-成本计算单_2026070916484310_100160.xlsx`(sha 与冻结值完全一致)。用它重跑 GB N=5 全部通过(见上)。

**待用户决定**:是否把原始样本恢复进 `data/raw/gb/`(否则以当前样本跑验收时 GB 输出门槛必然超限,且 CLI 自动发现会使用当前样本)。

## 优化候选清单(只记录,未实施;是否另开优化阶段由用户决定)

按仓库规则(没有可复现瓶颈就不优化),简化过程中发现的候选:

1. **anomaly.rs `text_any` 额外 clone**(本简化引入,最优候选):经 `value_any` 复用后每文本单元格多一次 `CellValue::clone`(Arc<str> refcount 或 96-bit Decimal 拷贝),每行 6-8 次。修复方向:恢复直接无 clone 扫描,或 `value_any` 返回 `&CellValue`。
2. **presentation.rs `build_typed_qty_sheet`** 每行分配 `derived: Vec::with_capacity(...)`;大量行时可复用单块 scratch buffer 再 `mem::take`(仅疑似,无实测)。
3. **normalize.rs `forward_fill_with_rules`** 中 `integrated_row` 每行计算 N 次;可提出循环每行一次(注意会改变一处不可达错误路径的优先序,需谨慎)。
4. **table.rs `last_positions`** HashMap 可加 `with_capacity`。
5. **fact.rs `build_check_reason`** 可惰性构造 `total_mismatch_reason`。
6. **quality.rs/anomaly.rs** 的 `text_any`/`cell_to_text` 同构函数可跨文件去重。
7. **cli manifest.rs `sha256_file`** 每次新建 Sha256 + 64KB 缓冲(每文件一次调用,非瓶颈)。
8. **cli config/validation.rs `pairs` HashSet 死分支**:逻辑可证明不可达,但位于 fail-closed 配置治理边界且无测试覆盖,误判后果是静默放行——保留更安全。
9. **xlsx writer.rs** `header_format`/`text_format` 每 sheet 重建,可提升到循环外(微优化)。
10. **xlsx atomic_file.rs `sanitize_request_id`** 与 writer.rs `TempWorkspace::create` 内联重复(去重需跨文件)。

## 延迟 Minor(最终审查裁决:全部 defer,非阻塞)

- presentation.rs `use rust_decimal::Decimal;` 排在 std import 之前(装饰性)
- `period_key_of` 缺 doc comment
- `display_slots` 纯搬移 near-churn
- runner.rs 合并链缺「publisher 错误不施加 BuildManifest context」注释
- `reject_if_exists` 消息组合契约依赖调用点字符串(两处,已逐字节验证)
- `sha256_file` vec→栈数组(邻接「优化禁止」,审查裁定非瓶颈、惯用形式)
- reader `header_start+1` 直接索引缺不变量注释(不变量由 `find_header_start` 结构保证)
- run_paths.rs `reject_if_exists` 与 manifest.rs `preflight_summary_output` 存在近重复检查(跨阶段、context 不同,可选跟进)

## 工具健壮性发现(建议后续修复)

`tools/validation/measure_release.ps1` 从 Git Bash 启动时,宿主控制台代码页为 GBK,.NET `Process` 默认以 GBK 解码 CLI 的 UTF-8 stdout,中文串奇数高字节会吞掉后续 ASCII 引号导致 JSON 解析失败。建议在脚本中设置 `$psi.StandardOutputEncoding = [System.Text.Encoding]::UTF8`。本次验收改用 UTF-8 控制台的 PowerShell 会话跑通。

## 剩余风险

- 无已知行为风险:每批独立审查 + 全分支审查双保险,输出逐字节一致,契约 zero diff。
- `text_any` 额外 clone 是唯一已知的微小性能代价(见候选 1),已被护栏与 N=5 数据覆盖(anomaly 路径非瓶颈)。

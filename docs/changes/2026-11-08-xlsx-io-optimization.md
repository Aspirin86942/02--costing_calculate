# Excel 读写与压缩优化实施记录

- 状态：Partial（部分完成；速度筛选受校准噪声阻塞）。
- 计划：[完整优化计划](../plans/2026-11-08-xlsx-io-optimization.md)。
- 实验编号：`20261108-xlsx-v2`。
- 该优化任务结束时正式 Rust 保持实验基线，候选与诊断仅在忽略目录执行；后续的上游维护更新见[依赖更新记录](2026-11-08-upstream-refresh.md)。

## 基线与协议

- 基础提交：`747c0ca5e15403d4c79aeff4bf4a7e5bbc356345`，开始时工作区干净。
- 冻结源码：`.pytest-tmp/optimization/20261108-xlsx-v2/baseline/source/`，66 个文件逐一复制并核对 SHA-256。
- 基线清单：`.pytest-tmp/optimization/20261108-xlsx-v2/baseline.json`。
- 原有程序保留：`baseline/previous-costing-calculate`；不覆盖原产物。
- 本轮基线程序：`baseline/costing-calculate`，SHA-256 `3a5bc445e84fb1ac87a53ea1b42184a797fc9ee287bd32259b5d7e1f3bb197bc`。
- 构建身份：Rust `1.96.0`、release、`codegen-units = 1`、固定 `SOURCE_DATE_EPOCH=1704067200`；程序提交身份与冻结提交一致。
- 初始正式测量 CPU 集合：`[3, 5]`，属于两个不同物理核；v3 使用重新冻结的 `[5, 9]`。不同批次、CPU 集合和诊断数据不混合。
- 输入清单只记录别名、大小与 SHA-256；当前 GB 可用、SK 缺失。真实路径仅在本地 `local-inputs.json`。
- 测量预算：8 对、4 对校准、6 小时批次、CLI 600 秒、2 GiB 内存和 2 GiB 单次运行目录。
- 初筛主场景：GB medium、种子 `20261108`、normal+Manifest；验证种子 `20261109`。最终日常无 Manifest normal 单独验收。

基线从独立源码目录重建成功；构建日志 `logs/build-baseline.log`。

## 任务节点

| 节点 | 状态 | 证据或待办 |
|---|---|---|
| P0 基线 | 通过 | 66 个源码/契约文件哈希核对，独立构建及程序身份验证通过 |
| P1 开销定位 | 通过 | 7 个诊断案例、5 组完整零差异对照及固定 XML 的 CRC/SHA-256/ZIP64、单核/双核实验通过 |
| P2 实验工具 | 通过（Linux） | 369 项 Python 测试通过；大型完整语义重放零差异；退出竞态回归通过，14 个执行工具重新冻结 |
| P3 候选正确性 | 通过 | v2 的无改动对照、C1、C2 及待激活组合均完成构建、完整 Rust 测试与当前阶段全部正确性准备 |
| P4 初筛 | 阻塞 | `[3, 5]` 与 `[5, 9]` 两个独立批次各两次校准均失败；没有候选 8 对数据，不判定提速或退步 |
| P5 矩阵/组合 | 正确性通过，性能未验证 | C1/C2 各 11 项、共 22 项完整保护比较通过；组合仅完成构建及基础正确性，没有资格激活测速 |
| P6 目标电脑 | 部分通过 | 真实 GB × C1/C2 × 有/无 Manifest 共 4 组完整零差异；真实数据性能、SK、Windows/Excel/WPS 未验证 |
| P7 集成交付 | 部分完成 | 工具、诊断、候选源码/程序与校准证据保留；无采纳结论，正式 Rust 保持原版 |

## 已验证的开销定位

诊断材料位于实验目录的 `diagnostics/`。`build-identity.json` 确认原业务依赖版本、checksum 和依赖边保留；pprof 0.15.0 及符号仅在隔离诊断构建使用。诊断程序 SHA-256 为 `02f4807bede43eb894eaed1f20a514b1b00d9dcca98604755b148621466d7112`。

`diagnostic-summary.json` 记录 7 个成功案例。`correctness.json` 与 `comparisons/*/report.json` 记录 GB/SK medium、above-threshold 及 GB medium 无 Manifest 共 5 组完整对照，全部零差异。完整 ZIP 条目内容一致路径比较所有条目，只有易变的 `docProps/core.xml` 被既有规则排除；不是抽样比较。

GB medium 一次无 CPU 采样的诊断区间如下，单位为秒。父子区间存在重叠，不能相加；单次诊断耗时不作为收益判定。

| 区间 | 秒 |
|---|---:|
| 工作表解析/稠密 Range | 0.730243 |
| 转换为业务 CellValue | 0.106666 |
| 共享字符串准备 | 0.112101 |
| 已有并行 XML 组装 | 0.296962 |
| Sheet 1 ZIP 写出 | 0.475581 |
| Sheet 2 ZIP 写出 | 0.126565 |
| Sheet 3 ZIP 写出 | 0.051752 |
| workbook 销毁 | 0.101635 |
| 最终文件同步 | 0.022545 |

`attribution.json` 中 GB medium、GB above、SK above 的保留 CPU 样本分别为 210、966、892。主要调用包含 zlib-rs 的 `longest_match`、quick-xml 解析、填表及 XML 生成，采到了 XML worker 线程。样本较少、部分模块被 blocklist（排除列表）排除，丢样数量不可得；样本分组只作归因，不能当成完整 CPU 百分比。

原程序的 `strace -f -c -w` 中，78 次 `read` 停留 0.003110 秒，206 次 `write` 为 0.007415 秒，2 次 `fsync` 为 0.021937 秒；2 次 `futex` 为 0.218296 秒。`futex` 是同步等待，当前没有对应调用栈，不判定为磁盘等待。原始汇总只保留在 `diagnostics/runs/gb-medium-strace/strace-summary.txt`。

### 固定 XML 压缩实验

`xml-corpus/corpus.json` 冻结 6 个纯合成工作表 XML 的 SHA-256、CRC、大小、压缩等级、单元格类型及文本重复率。`compression-report.json` 使用原版本 zip 7.2.0、flate2 1.1.9、zlib-rs 0.6.6，普通等级 6、低内存等级 5，各条件 4 对交错。

| XML 语料 | CPU 数 | 逐对节省秒数的中位数 |
|---|---:|---:|
| GB medium | 1 | +0.035817 |
| GB medium | 2 | +0.143993 |
| GB above | 1 | −0.218266 |
| GB above | 2 | +0.415024 |

正值表示更快，负值表示更慢。并行区间包含两 worker 压缩、汇包前完整 CRC 校验、按固定顺序复制预压缩条目和最终同步。最终解压 SHA-256、条目顺序及 ZIP64 检查全部通过；SHA-256 检查在计时外。这是固定 XML 微实验，不是 CLI 端到端或采纳结果。

### 当前候选准入

| 方向 | 判定与依据 |
|---|---|
| C1 借用 Range | 启用；XLSX 共享字符串直接转换为 Arc，其他格式沿用拥有型路径。10 项读取测试通过，覆盖类型、元数据、坐标空洞及两种输入入口 |
| C2 zlib-rs | 启用；只改锁包 0.6.6 → 0.6.8，无 Cargo 依赖键变化。目标范围预登记 ingest/export，保护共享读写后端 |
| C3 writer 迁移 | 未立项；更新说明中的大区间、合并单元格和图形性能改进不对应当前输出；本项目已经使用 zlib-rs，不能套用上游对 C 版倍率 |
| C4 reader 迁移 | 未立项；0.37/0.38 更新主要为兼容修复、样式和元数据，尚无对应已测复制/解析热点的改善依据 |
| C5 XML/填表单点 | 未立项；当前样本没有收敛到一个收益空间足够且可独立修正的函数，不重复已有列预计算、空白跳过和 XML 并行 |
| C6 并行压缩 | 完成微实验后未启用；双核 medium 只减少约 0.144 秒，单核大样本增加约 0.218 秒，当前依据不足以承担并发写出与临时归档复杂度 |

上游依据：[writer 0.102 更新说明](https://github.com/jmcnamara/rust_xlsxwriter/blob/v0.102.0/CHANGELOG.md)、[calamine 0.38 更新说明](https://github.com/tafia/calamine/blob/v0.38.0/Changelog.md)。未立项不等于已通过正式测量证明无收益。

## 工具与正式批次

- 主流程最终 Python 验证：Ruff 检查和格式检查通过；常规测试 `369 passed, 1 skipped`。被跳过项为当前 Linux 缺少 PowerShell 的平台自检；Windows 运行尚未验证。正式 Rust 和 C1 的 fmt/clippy 均通过；独立源码中的基线完整 Rust 测试为 212 项通过。
- 原语义比较器会拒绝同源脏行中的重复组键和非数字金额，严格规则保留。预登记 `gb-large-semantic` 使用 above-threshold 输入的 2025-12 至 2026-01 正常月份，输出超过 100,000 条明细，额外执行 XML 字节不同的完整语义重放；全月脏数据及阈值两侧检查继续执行。
- v1 的基线与无改动对照重建均复现 P0 的 `3a5bc445…` 程序哈希。对照在 `sk-small-single-month` 出现一次 `BINARY_EXECUTION_ERROR`，对应 Rust Manifest 已成功发布；批次在冻结与计时前停止，`measurements/` 为空，状态见 `screening-v1/stopped-by-parent.json`。
- 随后稳定复现并修复测量工具的独立退出竞态：已打开的 `/proc/<pid>/status` 在进程被回收后读取会抛出 `ProcessLookupError`，先前只处理文件不存在。回归测试先失败、修复后通过；最终峰值仍由 `wait4` 校验。原对照失败未保留底层 OSError，不能追溯断定就是该竞态；短进程 100 次直接重放未复现原异常。
- 当前源码包 `bundle-v2/colab-source.zip`，SHA-256 `2213dfce65bd13b526700425baa3ba9aaac681ea3858eefd56bd7111e4b5a1ac`；只在本地使用，审阅清单 `bundle-v2/bundle-review.json`。旧包和停止批次保留，不能与 v2 混合。
- 协议 SHA-256 `e245db9ff7fa422e331d0cb8c27fe510f2d1b7bae30767f09bad68e5877fafaa`；冻结 14 个实际执行工具文件。当前包内完整文件库存 SHA-256 `6a58c3151b0ab17d51075453457caac8845d8e7dd58b2fe0601ce09cdbf5ba8e`；P0 初始源清单 SHA-256 `bf24db1bca8dbbc321aa879f70b3579f40c3571850af14b8b18b2058bd9b9f7f`。
- 独立候选为 `c1-borrowed-range`、`c2-zlib-rs`；预登记 `c1-c2-combined` 只在组件均通过完整保护后激活。
- `screening-v2/` 完成构建、Rust 测试、当前阶段全部正确性及所有可能入围输入准备后，执行了两次校准，均为 `NOISE_EXCEEDED`。随后对 CPU 同核线程竞争做三次一秒采样，按物理核最忙线程的负载排序，冻结新组合 `[5, 9]`，记录为 `batch-v3-cpu.json`。
- `screening-v3/` 在新 CPU 集合重新开批。先验证旧冻结产物的每个哈希，再复制相同源码、程序和输入；复用已通过的 Rust/正确性结果，仅重做校准及后续筛选。来源与旧报告 SHA-256 保存在 `preparation_reused`；实际统计仍调用冻结工具的 `calibrate/screen/extend/combine`，没有重写统计或门槛。

### 校准阻塞与保留结论

下表均为同一基线程序的自比，每次 4 对。门槛为配对差中位数绝对值小于 1%、各对绝对差中位数不超过 2%。两种 CPU 集合分属独立批次，每个批次最多两次，全部失败后停止本机排名。

| 批次 / CPU 集合 | 次数 | 配对差中位数 | 各对绝对差中位数 |
|---|---:|---:|---:|
| v2 / `[3, 5]` | 1 | +1.7610% | 1.7610% |
| v2 / `[3, 5]` | 2 | +1.7370% | 1.7370% |
| v3 / `[5, 9]` | 1 | +9.8377% | 9.8377% |
| v3 / `[5, 9]` | 2 | +3.4396% | 3.4396% |

这些数字是基线自比偏差，不是候选改善。当前没有候选 8 对、组合或 N=5 速度数据；不能认定 C1/C2 更快、更慢或已达标。CPU 竞争与负载变化已观测到，但尚未定位全部噪声成因；低利用率快照也不能证明测量稳定。没有放宽门槛、拼接批次或用诊断替代正式结论，生产实现保留原版。

### 真实 GB 与交付证据

- `real-correctness/summary.json`：C1/C2 × Manifest/none 共 4 组完整工作簿、业务摘要及对应审计身份检查通过，均零差异。用途为 diagnostic，不进入排名；真实工作簿保留在该本地忽略目录。
- 基线 GB/SK 合成端到端检查通过：内嵌配置、check-only、实际 workbook、Sheet 顺序和禁止覆盖行为均验证。
- `delivery-verification-final.json`：状态 `partial`；66 个初始源码/契约文件及 14 个执行工具哈希核对；5 组诊断对照、4 组真实 GB 对照、22 组候选保护对照通过；8 个真实输出 ZIP 与源码包 CRC 检查通过；候选速度测量数为 0。
- `correctness-protection/summary.json`：两候选各 11 项，覆盖 GB/SK 阈值上下两侧、中型、独立验证种子、check-only 与日常无 Manifest。完整比较和实际 XML 的 Sheet/行列数/月份分布/分析页/阈值标记验证全部通过；没有抽样，也没有过滤脏数据差异。
- 保护集最大输出大小比：C1 `1.0000000000`，C2 `1.0000000444`，均低于 `1.02` 上限。它们是文件大小比，不是时间或内存改善率。

| 程序 | SHA-256 | 完整 Rust 测试 |
|---|---|---:|
| 基线 / 无改动对照 | `3a5bc445e84fb1ac87a53ea1b42184a797fc9ee287bd32259b5d7e1f3bb197bc` | 212 |
| C1 | `970829cc126d1faa16fa116936124c9bffabece11eb302d1627f4ed94efeb731` | 214 |
| C2 | `cab435300730f6b15ddf812070aafbd24a0bbd1688792f1de125f6041956a31a` | 212 |
| 待激活组合 | `0a4101f5f9b55e1abe40ee132bb5115dd463f2c3e3e905df470328b460efe082` | 214 |

程序位于 `screening-v2/builds/<id>/costing-calculate`，配套源码为 `screening-v2/trees/<id>/`；v3 复制件哈希一致。每个无改动对照/独立候选/组合另有 13 个基础正确性场景和 2 个内嵌配置场景，全部通过。

## 修改与协作边界

- 测量节点负责 `measure_paired_release.py`、Linux schema、`compare_releases.py` 及其专用测试。
- 驱动节点负责 `autotune_experiment.py`、`prepare_colab_bundle.py`、必要规格接口及其专用测试。
- 主执行负责冻结、隔离 Rust/fork 诊断与候选、统一集成、实验和文档。测速前暂停构建与其他性能任务。
- CodeGraph 查询状态为 unindexed；请求根为当前仓库，服务未找到可用索引。以磁盘源码、Git 及 Cargo 为准，不新建索引。

## 当前限制与恢复

P4 的速度排名被当前执行环境噪声阻塞；真实 SK、Windows/Excel/WPS 及最终真实数据 8 对/N=5 采纳仍未验证。全局工具、内核、调频和权限不修改；没有授权 Git 提交/推送、云端上传或发布，自动 CI 保持关闭。

恢复输入已就绪：`baseline.json`、`bundle-v2/bundle-review.json`、`delivery-verification-final.json`、`screening-v2/builds/` 与 `trees/`、`fixtures/` 和全部正确性报告。先核对实际哈希与冻结清单。已通过且源码/输入/依赖未变的检查可复用；`scripts/run_prepared_screening_v3.py` 保存了先验证冻结产物、再复用准备结果的本地示例。

下一步依赖稳定、空闲的执行环境。预登记正常 CPU 集合后，在新的批次目录重新执行 4 对校准（最多两次）、无改动对照与候选 8 对。校准成立才可进入性能保护矩阵、合格组合、真实 GB/SK 和独立 N=5。当前历史目录必须保留，不能用恢复或修改状态绕过失败，也不能把诊断、不同 CPU、工具、审计模式或旧批次的计时数据拼接。

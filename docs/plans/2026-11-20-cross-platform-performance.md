# GB / SK 全流程性能优化方案

状态为 **In Progress**。Windows 的总表写出优化、低内存 ZIP 兼容修复及可选 PGO 构建已完成。三组真实输入分别做了八对测量和独立五次复测，并核对完整 SK 的桌面 Excel 回存。对照为冻结的 0.3.1，提交 `e1c8d2a`。Linux 段错误仍待原生环境复验。[实现与实测](../changes/2026-11-20-windows-workbook-export.md)

## 为什么先做写出

Windows 上的完整 SK 有 912,921 行输入。普通写出和审计写出各 5 次均成功，普通模式的端到端中位数为 7.274 秒、峰值工作集中位数为 1.713 GiB。输出约 71.88 MiB，高于现行 46.40 MiB 上限。Linux 正式构建在同规模输入的两种模式下均发生过段错误，单核运行还出现过成功退出而内容错误。两端分别保留结论。[Windows 实测](../changes/2026-11-20-windows-stage-profile.md) · [Linux 正式版验收](../changes/releases/v0.3.1.md)

另一次 Windows 同版本分阶段诊断中，完整 SK 端到端中位数为 6.175 秒，读取占 1.650 秒，导出占 3.126 秒。若其它开销相同，端到端改善 5% 要省约 0.31 秒。单改填表需省约 11%，单改读取需省约 19%，而整个最终保存只用 0.226 秒。GB 的次序不同，最终保存占 0.426 秒，约为其端到端时间的一半。上述换算用于选择实验，正式收益按同一批交错配对判定。[分阶段测量与计时口径](../changes/2026-11-20-windows-stage-profile.md)

## Windows 实施顺序

### 1. 用固定的程序做同机对照

本机 `dist/baselines/windows-x64-0.3.1-e1c8d2a/` 保存独立对照程序和 `baseline.json`。清单记录程序、三份输入、两条管线的内嵌配置哈希，以及各场景五次普通写出的原始指标和门禁结果。Windows 上的每个候选从同一提交构建到独立 Cargo 目录，先校验对照程序和输入身份，再在同一台机器、同一输入和同一审计模式下交错运行。[五次测量及硬门禁](../changes/2026-11-20-windows-stage-profile.md)

首先测量完整 SK 的 `writer_populate`，另用 GB 的 `xlsx_save` 验证标准写出路径。每轮只改一项，先核对工作簿、业务摘要和审计哈希，再按下方配对规则判断收益。7 月的 Windows 实验可用于借鉴做法，其中常规 SK 输入与本机一致，完整 SK 使用本次冻结的输入。[历史 Windows 配对实验](../changes/2026-07-28-sk-performance-experiments.md)

### 2. 分别优化 SK 的填表与 GB 的保存

| 路径 | 第一项实验 | 如何选下一步 |
| --- | --- | --- |
| 完整及常规 SK，低内存写出 | 在诊断构建分开计量每行 XML 序列化、原始行文件写入、向压缩线程发送数据的等待、后台压缩耗时和最终重放次数。当前每块已写出的字节会复制到新的缓冲区，队列容量为 2。若复制或分配占热点，先试回收固定数量的块缓冲，沿用原背压和原始行文件。 | 如果主要时间花在序列化，另开单项验证低内存行写出中 AA 列以后的列名复用。如果主要时间花在压缩，进入第 3 步。用端到端耗时、PWS/RSS、临时文件峰值和重放正确性裁决。 |
| GB，标准写出 | 在 `xlsx_save` 内分别量化普通表 XML 组装与各 ZIP 成员压缩。首张表占包体约 94%。若 XML 组装占主导，先测逐格写坐标的耗时，再试同一行复用格式化后的行号。若压缩占主导，在锁定的 Rust 后端筛选主表成员压缩设置，同时看耗时和字节数。 | GB 约需节省 0.043 秒才能获得 5% 端到端收益。先在固定 XML 上淘汰空间或时间退步的选项，再做 CLI 配对。两种写出器共用的改动也要回测常规与完整 SK。 |

计数和采样只进入诊断工具或隔离构建。正式计时使用相同的 release 配置。低内存写出继续按单表 5,000,000 个单元格槽位触发。后台线程、原始行文件、ZIP 选项不匹配时的重放，以及发布前后的错误信息都要有对照样本。[写出入口](../../rust/crates/costing-xlsx/src/writer.rs) · [行文件与压缩线程](../../rust/vendor/rust_xlsxwriter/src/worksheet.rs) · [ZIP 保存](../../rust/vendor/rust_xlsxwriter/src/packager.rs)

### 3. 单独解决结果文件大小

完整 SK 需要从 75,372,863 字节减少 26,714,040 字节（35.44%）才能达到原上限。第一张表占包体 96.11%。保持该表 XML 完全相同，用 Python 验证环境的 DEFLATE 9 级重压后，估计整包仍为 74,180,768 字节。GB 同法估计为 4,563,589 字节，也高于其上限。直接上调压缩等级的预期收益很小。[固定 XML 实验](../changes/2026-11-20-windows-stage-profile.md)

体积首选实验是省略可由位置推导的单元格坐标 `c@r`。保留行号，每行从 A 列开始推导，只对紧接上一个实际写出单元格的位置省略坐标。行首跳列或中间有空洞时显式写地址。Office 文档给出了这条规则。对完整 SK 首表实施离线转换后，逐个补回坐标可以还原原始 XML，估计整包为 **30.28 MiB**。合成小表在桌面 Excel 打开、另存、重开后保留了金额格式、前导零文本、公式和稀疏位置。[Office 的单元格定位规则](https://learn.microsoft.com/en-us/openspecs/office_standards/ms-oe376/b3e61d30-f15f-4712-9252-50883cbf0f8c) · [实验明细](../changes/2026-11-20-windows-stage-profile.md)

实施分两步。先让独立比较器按行解析显式和隐式坐标，它当前直接索引 `node.attrib['r']`。用已知坐标的小表验证首列缺失、中间空洞、Z 到 AA、空行和格式覆盖，再用数值错位与漏格负例验证它确实能检出差异。然后才在受控写出库给总表制作 Rust 候选，保留显式样式 `s`、类型、数值和公式。全量验收需要真实 Rust 产物的大小、耗时、峰值内存、工作簿比对及 Excel 打开回存结果。省略金额单元格的显式样式已在合成样本上使 `#,##0.00` 变成 `General`，该做法退出候选。[工作簿比较器](../../tools/validation/workbook_compare.py) · [工作簿契约](../contracts/workbook.md)

### 4. 降低读取成本和峰值内存

并行读取当前按约 1 MiB XML 划块，每块创建作用域线程、构建临时 ZIP 视图并调用 Calamine，有序回收结果。先记录真实块数、实际走并行或回退的次数、建线程与建视图时间、等待最早块时间、在途字节和进程峰值。若线程创建有足够开销，首个候选只试固定数量的长驻工作线程，沿用块大小与并发上限，按块序号合并。另一个候选才研究块大小和在途内存预算。两者分别对基线测量。[并行读取](../../rust/crates/costing-xlsx/src/reader/parallel.rs) · [兼容读取](../../rust/crates/costing-xlsx/src/reader.rs)

用真实完整 58 列、范围误报 `A1`、跨块空行、非连续坐标、共享字符串、日期及小数验证两条读取入口。任一分块不满足条件时丢弃局部结果并重走现有回退。金额继续使用现有 Decimal 路径。此前逐单元格读取曾把成本转移给规范化或写出填充，重新考虑该方向须有新的分阶段证据。[被拒绝的读取候选](../changes/2026-11-12-streaming-reader.md)

### 5. 按剩余热点处理核心计算与审计

- 完整 SK 的 `presentation`、`normalize`、`fact` 分别约 0.494、0.356、0.341 秒。先分段采样行投影、派生列临时向量和工单键构造的分配高水位。若派生列成为内存热点，首选单项是减少整列临时值的驻留，保持先去总计行、再向下填充、派生月份与成本项、最后应用月份过滤的顺序。若是投影热点，则核对已有 `ProjectionPlan` 的移动/复制路径再试。异常评分及勾稽继续沿用当前算法，按样本观察决定后续投入。[核心阶段](../../rust/crates/costing-core/src/process.rs) · [规范化](../../rust/crates/costing-core/src/normalize.rs) · [展示表](../../rust/crates/costing-core/src/presentation.rs)
- 在读取、规范化、展示表构造和写出边界分别采集当前驻留内存及生命周期峰值，关联仍存活的行、块缓冲和展示表。GB 此批 N=5 中位数距上限仅约 0.11 MiB，对 GB 单独验收内存。CLI 退出和库入口在同一进程内多次调用都要验证。[内存表示与入口](../architecture.md)
- 日常与审计分别计时，在审计路径单列输入快照与 SHA-256、输出发布后哈希和 Manifest 发布。哈希与解析取自同一份输入字节，文件并发替换也是测试场景。产物继续在输出目录内暂存、同步、拒绝覆盖并原子发布。[运行编排](../../rust/crates/costing-cli/src/run.rs) · [原子发布](../../rust/crates/costing-xlsx/src/atomic_file.rs)

## 可借鉴的上游实现

| 方向 | 对本项目的用法与试验条件 |
| --- | --- |
| 顺序写行与缓冲复用 | Excelize 的流式写出按递增行号接收数据，缓冲达到一定规模后使用临时文件。本项目已有顺序写行和后台压缩，下一步量化发送块时的分配及等待，若分配突出，用有上限、可归还的缓冲区试验。现有原始行文件继续承担错误重放。[Excelize StreamWriter](https://github.com/qax-os/excelize/blob/master/stream.go) · [本地块发送逻辑](../../rust/vendor/rust_xlsxwriter/src/worksheet.rs) |
| 同一表内的并行 DEFLATE | pigz 将块并行压缩，使用前一块的历史字典并按原序输出。只有确认压缩线程占主导时，才在单个 worksheet ZIP 成员上单独试验这个思路。控制在途字节，并验证解压后的 XML、整个成员的 CRC、大小及 Excel 兼容性。Rust 的 gzp 提供连续 raw DEFLATE 的实验参考。[pigz 原理](https://github.com/madler/pigz/blob/master/pigz.1) · [gzp](https://github.com/sstadick/gzp) |
| 持久线程池与并行解压 | 先借鉴 Rayon 的工作线程池，用现有标准库复用读取线程，继续按块序号合并。若采样进一步指向 ZIP 解压，才在隔离实验中参考 rapidgzip-rust 对可按偏移读取的 raw DEFLATE 输入做并行解码。保留 ZIP 成员边界、CRC/长度校验和顺序回退，同时限制解析与解压两侧的线程总数。[Rayon 线程池](https://github.com/rayon-rs/rayon/blob/main/FAQ.md) · [rapidgzip-rust](https://github.com/COMBINE-lab/rapidgzip-rust) |
| 构建期 PGO | Rust 1.96 支持用插桩版本收集典型执行路径，再将合并后的 profile 反馈给优化构建。将其作为独立候选，Windows 和 Linux 分别用各自的 GB/SK 代表性输入训练，并用留出的样本进行配对验收。训练和计时分开。[Rust 1.96 PGO 文档](https://github.com/rust-lang/rust/blob/1.96.0/src/doc/rustc/src/profile-guided-optimization.md) |
| 近期写出库更新 | 2026-11-18 发布的 rust_xlsxwriter 0.103.0 修复了特定行格式、行高或隐藏标记在 constant-memory 模式下可能引起的重复行或缺行。先用上游案例核对本地 0.102.0 受控版本是否受影响，再评估单独移植修复。该更新值得用于正确性回归，速度须另测。[上游变更记录](https://github.com/jmcnamara/rust_xlsxwriter/blob/v0.103.0/CHANGELOG.md) |

共享 Rust 代码可以在两端减少同类工作，例如生成的坐标字节数、块复制次数、每次打开工作簿所创建的线程数。Windows 的采样能选出实验方向。线程调度、临时文件 I/O、分配器和 CPU 指令选择取决于平台和机器，部署目标分别用原生构建测端到端耗时，Windows 报峰值工作集，Linux 报子进程峰值 RSS。引入第三方生产依赖需按仓库规则另行批准。

## Linux 原生环境到位后的复验

- 在原生 Linux 固定输入、配置、源码、工具链、运行工具和二进制的 SHA-256。用最后一份通过完整 SK 契约核对的同平台程序作参考，分别运行普通、审计和预检模式。记录退出码或信号、子进程峰值 RSS、可取得的内核 OOM 记录、失败阶段和首个不一致位置。已有证据把一次差异定位到 Calamine 的 Range 转成 `RawWorkbook` 期间，另一次发生在进入写出前，分段检查从这里开始。[内容定位记录](../changes/2026-11-12-baseline-content-localization.md)
- 在 Linux 诊断构建中一次改变一项。先对照现有顺序读取回退路径与并行分块读取，在块合并、规范化、展示表三个边界核对计数与内容摘要。如果预检通过而普通写出失配，再单独对照后台压缩与已有原始行文件重放。审计专有问题另查输入字节身份、输出哈希及 Manifest。单核与默认并发均使用相同文件和核对规则。
- 段错误、超限或单元格差异均保留失败样本，结束该批排名。Linux 的参考程序和候选都须在普通、审计模式逐次通过内容核对，才开始该平台的计时校准。[既有恢复实验](../changes/2026-11-12-runtime-repair.md) · [版本采用记录](../decisions/2026-11-20-v0.3.1-performance.md)

在原生 Linux 上，将 `BASELINE` 设为上一次在当地通过完整 SK 核对的可执行文件，`CANDIDATE` 设为本次源码的 Linux release 程序，`FULL_SK` 设为本地真实输入路径。在仓库根目录运行以下 Bash 命令。四种模式各自保存报告，首次失败时保留该批结果。

```bash
mkdir -p .pytest-tmp
run_dir=$(mktemp -d .pytest-tmp/linux-full-sk.XXXXXXXX)
(
  set -e
  for mode in check-only normal; do
    for audit in none manifest; do
      uv run python -m tools.validation.compare_releases \
        --baseline-binary "$BASELINE" --candidate-binary "$CANDIDATE" \
        --pipeline sk --input "$FULL_SK" --mode "$mode" --audit-mode "$audit" \
        --purpose diagnostic --memory-limit-mib 6144 --disk-limit-mib 4096 \
        --timeout-seconds 600 --output-dir "$run_dir/$mode-$audit" \
        --report "$run_dir/$mode-$audit.json"
    done
  done
)
```

## 验收与回滚

| 场景 | 核对条件 |
| --- | --- |
| 真实 GB、常规 SK、完整 SK | 每次显式传入 `--input`，每轮使用全新的输出路径。普通、审计、只检查分别比对。完整 SK 核对 912,921 行输入及三张表的 812,588 / 12,981 / 135 行数据。 |
| 合成保护集 | SK 初筛使用 `full-sk-medium`（5,000 张合成工单），再用两个种子覆盖完整 SK 格式、单月与空筛选、低内存阈值两侧、输入路径与字节两种入口。覆盖集成车间供应商空值、独立成本项、SK 材料汇总、重复键及白名单顺序。 |
| 内容与失败路径 | 先比较受约束的 ZIP 成员，再按既有语义比较器核对差异。逐格绝对容差 `1e-9`、列累计 `1e-8`。同时核对三表顺序、样式、业务摘要与错误计数、Manifest V1 及哈希。重放、磁盘写入失败、同名输出、清理失败均须保留原错误码和最终文件状态。 |

1. 每项用独立源码、Cargo 目标目录和可执行文件，固定 Rust 1.96.0、`--release --locked`、`codegen-units=1` 和默认功能。每个候选登记主场景、目标阶段、输入和工具身份。受控写出库的改动补齐逐文件哈希及失败路径测试，方案使用已有依赖。
2. 先做基线对基线 4 对校准，沿用 1% / 2% 噪声规则和目标阶段 5% 规则。最多两次。正确性通过后，对单项做至少 8 对，双方预热、奇偶反序，每次在计时外核对产物。主场景端到端改善至少 5%、8 对中至少赢 6 对，其它场景回退至多 2%。目标范围外基线耗时达到 50 ms 的阶段回退超过 5% 即退出。Linux 峰值 RSS 的逐对改善中位数须 ≥ −5%，候选与基线的峰值中位数比须 ≤ 1.05，合成输出大小比须 ≤ 1.02。组合还须比最佳单项多改善至少 2%。[完整判定规则](../performance/README.md)
3. Windows 用现有 PowerShell 工具做普通模式配对与最终独立 N=5，记录峰值工作集、外部耗时和输出字节。Windows 配对脚本当前只传普通或只检查参数。审计先用 `compare_releases --audit-mode manifest` 重放，需正式审计测速时为验证脚本补独立审计参数和逐轮校验，单独校准。Linux 使用原生 Python/Bash 工具测子进程峰值 RSS。普通验收和 Colab 合成筛选的内存、磁盘各限 2 GiB。真实完整 SK 独立验收可用 `resource_scenario='full-sk'`，预算为 6 GiB 内存、4 GiB 磁盘。合格的 8 对证据之后才进入 Linux 独立 N=5，主场景至少赢 4/5。[Windows 配对工具](../../tools/validation/measure_paired_release.ps1) · [跨版本比较器](../../tools/validation/compare_releases.py) · [Linux 验收入口](../../tools/validation/autotune_experiment.py)
4. 入围实现再跑 Rust 格式、Clippy、全 workspace 测试、Python 验证及合成端到端，按平台完成发布包 smoke。Windows 从干净提交构建 ZIP，在 child PATH 移除 Rust/Python 后验收。任一平台正确性失败或基线不稳定，保存该批失败记录并沿用上一份已验证的同平台程序。按单项撤回补丁及配套受控库清单。通过后才把正式实现、验收结论和发布说明写入相应版本记录。

Windows 首轮选择了第一张表的坐标写出，三组真实输入均通过普通模式的大小、耗时和峰值工作集门禁。后续内存改动按阶段峰值和独立候选筛选。Linux 单独出具故障复验和性能验收记录。[首轮实测](../changes/2026-11-20-windows-workbook-export.md)

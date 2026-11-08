# Colab 离线调优实施记录

- 状态：Completed（本地工具与首轮候选筛选完成；云端运行及最终性能采纳未验证）
- 方案：[Colab 离线调优 v1](../plans/2026-11-08-colab-offline-autotuning.md)
- 实验编号：`20261108-colab-v1`
- 本轮主场景性能门槛未达成，入围 0 项，生产实现保留原版本。

## 已交付工具

| 产物 | 用途与边界 |
|---|---|
| [Linux 配对测量](../../tools/validation/measure_paired_release.py) | 独立进程、交错顺序、逐对统计、资源限额、哈希复核和同会话恢复 |
| [Linux 报告 schema](../../tools/validation/measure_paired_release.linux.schema.json) | 固定峰值 RSS 字节口径、CPU 绑定集合、业务摘要和检查点哈希 |
| [实验驱动](../../tools/validation/autotune_experiment.py) | 顺序构建基线、未改动对照和候选；正确性、噪声及性能门槛判定；入围最多 2 项 |
| [允许文件打包](../../tools/validation/prepare_colab_bundle.py) | 复核冻结源码与补丁哈希，生成可审阅 ZIP 和文件清单 |
| [Colab notebook](../../tools/validation/colab_autotune.ipynb) | 固定环境、安全解压、调用同一驱动和手动下载合成报告 |
| [合成规格与生成](../../tools/validation/synthetic_benchmark.py) | GB/SK 合成产品配置、小型、中型、低内存阈值两侧及独立种子 |
| [跨版本比较](../../tools/validation/compare_releases.py) | 复用 workbook 比较，补充业务摘要、Manifest、配置及输入输出身份检查 |

Linux 的 `wait4` 会记录创建子进程时继承的父进程驻留内存。测量工具先启动隔离的轻量辅助进程，
再对实际 Rust CLI 获取 `wait4` 峰值与时间，避免样本生成器或测试框架占用污染结果。
内存自检覆盖“父进程额外占用 96 MiB、被测程序仍保持小内存”的情形。

报告只保存业务警告的数量和哈希。成功验证后默认清理 workbook，保留每次大小、SHA-256、阶段耗时、业务摘要及临时残留检查。
同一输入、配置、程序、源码身份或 CPU 绑定集合变化后，不能继续拼接原测量批次。

## 冻结输入与运行限制

- 基础提交：`11a4001cb0f5621ddc41ee7496fc60153395dff8`。
- 本地快照：`.pytest-tmp/autotune/20261108-colab-v1/source/`，包含开始实施时的 200 个允许范围文件及未提交 Linux 适配修改。
- 快照清单：`.pytest-tmp/autotune/20261108-colab-v1/baseline.json`。
- 清单 SHA-256：`de8c877772d77de269c2496b6fc41c2f3bd786e0978ebf341ce92cc4a3343ef0`。
- 冻结程序：`.pytest-tmp/autotune/20261108-colab-v1/baseline/costing-calculate`。
- 程序 SHA-256：`0e9c71fa678b49709edc04ecb23e32e451023807e4598f41c8a26266d47b9452`。
- 首台目标机：Linux x86_64；完整硬件与系统摘要仅保存在本地清单。
- Rust `1.96.0`，`--locked`，默认功能，`codegen-units = 1`；固定构建时间为 `SOURCE_DATE_EPOCH=1704067200`。
- 单次预算：超时 600 秒、进程峰值内存 2 GiB、运行输出目录 2 GiB。本地实验总预算 3,600 秒，Colab 默认上限 21,600 秒。

从源码包重建的基线程序与冻结程序 SHA-256 相同，基线 Rust 全工作区测试通过。

## 合成数据验收

本地证据：`.pytest-tmp/autotune/20261108-colab-v1/n2-large/acceptance.json`，状态 `passed`。
GB/SK 的 `medium`、`below-threshold`、`above-threshold` 共 6 个场景均通过真实 Rust 运行、预期计数及低内存标记核对。
基线与缓冲候选 workbook 比较差异均为 0；小型集合另覆盖 normal、check-only、单月及空月份。

中型 GB 含 50,114 条成本明细、6,261 条数量页输出和 3,136 条白名单内分析行。
预期脏数据保留并计数，包括重复工单、缺失金额、未映射成本项、非正单位成本及成本勾稽差异。
金额和 Excel 业务处理仍全部由 Rust 执行。

## 首轮候选

候选清单和补丁保存在忽略目录，不修改生产 Rust 源码。

| 候选 | 修改范围 | 可检验假设 |
|---|---|---|
| `buffer-64k` | `rust/crates/costing-xlsx/src/writer.rs` | ZIP 写出经 64 KiB 缓冲后减少小块写入；发布前显式 flush，并保留失败处理 |
| `lto-fat` | `rust/Cargo.toml` 的 release profile | 完整链接时优化可能减少跨 crate 调用开销；保持依赖、功能及 `codegen-units = 1` |

主场景统一为 GB 中型合成集，目标阶段为 `xlsx_save`。补丁哈希：

- `buffer-64k`：`4b994dd2acc42f6d44e73952dace440458e31b46e3b97ce43e28b168cafc1df7`。
- `lto-fat`：`3e7e020e33ef4d680950b0457b071f0c7d33acca471140440691db81a22757ad`。

两个候选均从允许文件包独立构建，每项完成 212 项 Rust 测试、10 个 GB/SK 正确性场景和 8 对主场景测量。
缓冲候选另在独立源码目录完成 Clippy 和格式检查。
当前驱动只接受最多 6 个独立候选；组合实验依赖单项通过后另行冻结清单和完成组合比较，本轮未创建组合候选。

## 本地测量批次

### 未固定 CPU 的批次

证据：`.pytest-tmp/autotune/20261108-colab-v1/local-screen-v1/experiment-report.json`，状态 `noise-invalid`。
两次校准均比较同一基线程序，每次 4 对。

| 校准 | 配对改善率中位数 | 各对绝对差中位数 | 判定 |
|---|---:|---:|---|
| 第一次 | +4.3875% | 4.3875% | 噪声门槛失败 |
| 第二次 | +1.5410% | 2.5345% | 噪声门槛失败 |

驱动按协议终止，候选列表、入围与推荐均为空。该批次不能用于候选排名。
测前系统存在其他负载，CPU 使用动态调频；这两项是环境事实，尚不足以单独证明每次波动的来源。

### 固定 CPU 的独立批次

证据目录：`.pytest-tmp/autotune/20261108-colab-v1/local-screen-v2-cpu3/`。
本批次只将实验及其子进程绑定 CPU 3，记录 `cpu_affinity=[3]`；不修改系统调频、权限、服务或其他进程。
测前 CPU 3 与其同核线程 CPU 9 的 1 秒采样占用均约 2%。保持原样本、参数和门槛，新目录重新校准，未合并旧批次样本。
状态 `completed`。第二次校准通过：配对差中位数 +0.3048%，各对绝对差中位数 0.7932%，目标阶段差 +1.6229%。
未改动对照与基线程序 SHA-256 相同，8 对改善率中位数 +0.8396%，未达到 5% 门槛，正确判为拒绝。

| 候选 | 主场景配对改善率中位数 | 胜出 | RSS 改善率中位数 | 最大输出 / 基线 | 判定 |
|---|---:|---:|---:|---:|---|
| `buffer-64k` | -0.4598% | 4/8 | -0.2401% | 100.0000% | 拒绝：速度、胜出次数未达门槛，`export` 阶段回退 6.1612% |
| `lto-fat` | +2.2797% | 8/8 | +0.1832% | 100.0000% | 拒绝：速度未达 5% 门槛 |

正值表示改善。先计算每对的相对改善，再取中位数；独立耗时中位数的比例不等同于本表的配对统计。
两项候选的 10 个正确性场景均为零差异，包含 GB/SK 小型 normal、check-only、单月、空月份和中型 normal。
基线及两个候选的 Rust 测试各 212 项通过。

本轮入围与推荐均为空，因此未触发入围后的规模/独立种子性能矩阵、组合候选或目标机真实数据 N=5。
结果仅覆盖当前 Linux 环境与本轮合成主场景；真实 GB/SK 性能采纳保持未验证。
本地汇总为 `.pytest-tmp/autotune/20261108-colab-v1/screening-summary.json`。
下载材料为 `.pytest-tmp/autotune/20261108-colab-v1/local-screen-v2-cpu3-results.zip`，包含 110 个报告、规格、补丁及构建/测试日志文件，ZIP 完整性检查通过。

## 源码包与云端边界

当前本地准备产物：

- `.pytest-tmp/autotune/20261108-colab-v1/bundle-v3/colab-source.zip`
- `.pytest-tmp/autotune/20261108-colab-v1/bundle-v3/bundle-review.json`
- ZIP 大小：248,885 bytes。
- ZIP SHA-256：`faa9be5d52fc320fe700685a830b99e6b68cf40cbb5185b9a45002a8afd51d63`。
- 源码清单 SHA-256：`f837a82b117cbd687ca788bed39e0a8c6db3421392522c8eb24f80bfe7333e24`，覆盖 94 个允许文件。

正式筛选批次使用 `bundle-v2`，源码清单哈希为 `2f34b8040f115c4d1ccc8fb5bc9a405f21a26dd658fc68de79328786e7d71951`。
最终交付包补齐“大样本被选为主场景时，必须先比较该输入”的分支与回归测试；本轮主场景为 `medium`，
实际测量、统计、Rust 源码和候选补丁一致。两个包与各自审阅清单均保留，不混用源码身份。

包中包括默认产品白名单、golden 配置和业务字段；真实 ERP 数据、输出、凭据、Git 元数据、缓存和编译产物不在允许清单。
上传前仍须审阅源码与内嵌业务配置。当前状态为 `prepared-not-uploaded`。

Notebook 固定 uv `0.13.6`、Python `3.13.18`、Rust `1.96.0` 与 rustup `1.28.2`。
已核对 uv 版本可下载及 Rust 官方 SHA-256 文件格式；官方校验文件的 `*./rustup-init` 形式有回归测试。
实际源码包通过 notebook 的同一安全解压函数，文件清单和哈希通过核对。
安装命令与下载材料范围通过离线模拟测试；真实 Colab 联网安装和端到端运行保持未验证。

## 验证与节点状态

- 最终 Python 公共验证：193 passed、1 skipped；跳过项为 Linux 未安装 PowerShell 时的 Windows 脚本自检。
- Ruff 检查及格式检查通过，33 个 Python / notebook 文件格式合格。
- 基线及两个候选的 Rust 全工作区测试各 212 项通过。
- Markdown 本地链接检查通过；`git diff --exit-code -- rust rust-toolchain.toml` 通过，生产 Rust 文件无本次修改。
- 最终交付包的 notebook 安全解压、94 个文件的哈希与清单身份复核通过；实验材料包的范围与 ZIP 完整性通过。
- 本地交付复核：`.pytest-tmp/autotune/20261108-colab-v1/delivery-verification.json`。

| 节点 | 状态 | 证据或待办 |
|---|---|---|
| N0 基线冻结 | 通过 | 逐文件 SHA-256 复核，允许文件包能重建同哈希程序 |
| N1 Linux 测量 | 通过 | 每进程峰值、交错顺序、超限失败、校准、检查点及恢复自检 |
| N2 样本与比较 | 通过 | GB/SK 6 个规模场景、分析页非空、预期计数和 Manifest 验证 |
| N3 候选 | 通过 | 两个独立候选构建、正确性及 8 对筛选完成；全部未达到速度门槛 |
| N4 Colab | 工具通过，云端未验证 | notebook、允许文件包、安全解压、完整本地驱动与下载材料验收完成 |
| N5 目标机验收 | 未验证（未执行） | 无入围项，且缺少 SK 真实样本 |
| N6 生产采纳 | 本轮不采纳 | 保留冻结程序及原生产源码，性能优化目标未达成 |

## 剩余范围与恢复

真实 SK 输入仍未取得，整体 GB/SK 性能采纳保持未验证。云端上传、Drive 写入和付费资源均未执行。
主要写出耗时包含受控 `rust_xlsxwriter` fork 内部的 XML 组装和压缩；依赖版本及 fork 的修改仍需按项目规则另行授权。

继续时先核对冻结清单、程序与 bundle 哈希，再读取最终本地报告。真实输入输出和完整报告留在忽略目录。
测量跨会话时建立新批次并重新校准；有效基线与已通过且输入未变的正确性证据可复用。

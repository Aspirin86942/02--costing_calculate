# 行容量与 Excel 读取优化实施记录

- 状态：部分完成。验证工具已实施，候选采纳受正确性与测量条件阻塞。
- 生产结果：正式 Rust 保持基线；尚无经完整验收证明的性能改善。
- 方案：[内存与读写优化](../plans/2026-11-11-memory-optimization.md)。
- 基础提交：`d24b9485ca7c6a1b88508fb3a6ee2282de0d1295`，开始时工作区干净。
- 本地材料：`.pytest-tmp/optimization/20261111-memory-v1/`；真实输入、输出、路径及环境信息仅保存在忽略目录。

## 已实施的验证工具

- 合成规格的 `work_order_rows` 对齐到产品池筛选后的实际分析行数；生成器版本更新为 2。
- 完整 SK 合成保护覆盖 58 列双层表头、前置元数据、错误 A1 范围声明、共享字符串、材料汇总与子项金额差异。正常、只检查、月份和空结果进入固定保护矩阵。
- SK 可显式选择 `full-sk-medium` 主场景，固定 5,000 张工单、50,000 条明细。普通场景和阈值两侧保护继续保留。
- `run_acceptance` 新增显式 `resource_scenario`：默认 `standard` 为 2 GiB；完整 SK 专用 `full-sk` 上限为内存 6 GiB、磁盘 4 GiB。实际预算与场景哈希写入报告并复核。合成筛选及原性能采纳门槛保持。
- Cargo 构建和测试产物按候选独立目录，防止切换源码副本时复用其它候选的业务代码。

## 构建身份核查

初始准备使用原工具的共享 Cargo 产物目录。身份检查发现 A2 与组合程序的哈希和 A1 相同，测试日志也出现 A1 专属测试；B1 测试还包含 A1 的测试标记。因此初始 `builds/` 中全部候选产物不进入测速，保留 `initial-builds-invalid.json` 及原日志。

修复采用每个候选独立的 `cargo-target/<id>` 与 `cargo-test-target/<id>`。回归测试验证不同源码树使用不同产物目录。正式筛选从已冻结源码重新构建，不复用上述无效产物。

## 冻结与实际检查

| 检查 | 状态 | 证据 |
|---|---|---|
| 业务源码冻结 | 通过 | `baseline.json` 记录 148 个编译/契约文件，副本逐项 SHA-256 一致 |
| 输入身份冻结 | 通过 | `input-identities.json`，GB/SK 仅记录别名、大小与 SHA-256 |
| 原始基线构建/测试 | 通过 | `baseline/source/`、`builds/baseline/build.json`、218 项 Rust 测试；该首次构建尚未发生共享缓存切换 |
| 最终 Rust 格式与静态检查 | 通过 | `cargo fmt --all --check` 与 workspace/all-targets/all-features Clippy（警告视为错误），日志 `logs/final-rust-clippy.log` |
| 完整 SK 合成规格实际运行 | 通过 | `full-sk-fixture-smoke/result.json`：4 张工单，49 输入、40 明细、4 数量、2 分析，4 条材料差异提示，工作簿形状与月份检查无差异 |
| 最终工具定向检查 | 通过 | Ruff 代码/格式检查；239 项受影响测试通过，日志 `logs/python-build-isolation-final.log` |
| 最终工具全量回归 | 通过 | 487 项通过、1 项 PowerShell 平台跳过；Markdown 链接检查通过，记录 `checks.json` |
| A1 独立构建与基础合成保护 | 通过 | `screening-a/logs/test-a1-projection-capacity.log`：219 项 Rust 测试，仅包含 A1 专属容量标记；基础合成比较为 prepared |
| A2 独立构建与基础合成保护 | 通过 | `screening-a/logs/test-a2-derived-capacity.log`：219 项通过，仅含 A2 专属标记；程序哈希与 A1 不同，基础合成比较为 prepared |
| A1+A2 独立构建与基础合成保护 | 通过 | `screening-a/logs/test-a1-a2-combined.log`：220 项通过，同时包含两项容量标记；基础合成比较为 prepared |
| B1 独立构建与扩展合成保护 | 通过 | `correctness-b/logs/test-b1-borrowed-reader.log`：220 项 Rust 测试，仅含借用读取标记；基础比较及全部 10 项扩展规模/种子保护通过 |
| B2 独立构建与扩展合成保护 | 通过 | `correctness-b/logs/test-b2-key-buffer.log`：219 项 Rust 测试；基础比较及全部 10 项扩展保护通过，B 报告 `correctness_passed=true` |
| A1 真实 GB、SK 无 Manifest | 通过 | `real-a-correctness/summary.json` 已记录 GB none、GB manifest、SK none 三项零差异结果 |
| A1 真实 SK 有 Manifest | 失败 | 比较子进程触及约 600 秒；输出产品编码单元格存在文本差异，见下节 |
| 基线及 A1 冻结程序重放 | 通过（仅本次重放） | `baseline-replay/result.json`、`a1-replay/result.json`：两次有效 ZIP payload 均与原基线一致，相关控制字符文本数均为 0；此前差异未解释 |
| A2/B1/B2 真实 GB/SK 输出 | 通过 | `real-remaining/summary.json`：3 个候选 × GB/SK × 有/无 Manifest，共 12 项均 passed、零差异 |
| A2 资源诊断 | 失败 | `real-remaining/diagnostics/a2-derived-capacity/report.json`：候选预热阶段 `CHILD_FAILED`，退出状态 −11（SIGSEGV），0 个有效配对样本 |
| B1 单对资源诊断 | 通过（仅诊断样本） | `real-remaining/diagnostics/b1-borrowed-reader/report.json`：1 对有效样本，不能替代正式筛选 |
| B2 单对资源诊断 | 通过（仅诊断样本） | `real-remaining/diagnostics/b2-key-buffer/report.json`：1 对有效样本，不能替代正式筛选 |
| 阶段采样诊断构建 | 失败 | `logs/build-d0-model.log`、`logs/build-d0-model-v2.log`：rustc 两次 SIGSEGV，未运行到业务阶段采样 |
| 基线噪声校准 | 失败 | 两次各 4 对均 `NOISE_EXCEEDED`，配对差中位数为 +4.8001%、−3.6707%；原始报告位于 `screening-a/measurements/calibration-*/report.json` |
| 正式 ≥8 对 / N=5 | 未验证 | 校准失败后未启动候选速度测量，正式业务源码保持原版 |

两次基线自比的各对绝对差中位数分别为 4.8001%、3.6707%，均超过 2% 门槛；配对差中位数绝对值也均超过 1%。两边实际运行同一程序，这些数字是测量偏差。当前基线、主样本和 CPU 集合未变，第二批只继续独立正确性工作，不增加第三次相同校准。

工具源码与协议已经在 `bundle-a/`、`bundle-b/` 冻结。协议 SHA-256 为 `d5217761f13be9f3ee712a00ced304e575c0629eef5206550779569806a4712d`。
A 包 SHA-256 为 `fbabc2c681ff14f9c516b9e7e9e87dcc0c81e325bea4a8b78146098f71cde1fe`；B 包为 `13813162c9d73749ccea0157f9e7c850617e94d8ae048240ac134b69ef2f4823`。源码包仅在本地使用。

## 真实 SK 差异与诊断边界

A1 的一次真实 SK 有 Manifest 输出中，`成本计算单总表!D222748`（产品编码）与基线不同。候选共享字符串表多出一个含 NUL/U+0002 的 10 字符文本，XML 中保存为 Excel 控制字符转义。输入身份、业务摘要和质量计数相同，仍不足以通过单元格正确性检查。完整语义比较超过 600 秒；原 workbook 与中断记录保留在 `real-a-correctness/comparisons/a1-projection-capacity-sk-manifest/`。

随后分别重放冻结基线和同一 A1 二进制，两次有效 ZIP payload 均与原基线一致，相关控制字符文本数为 0。A1 的程序与输入哈希均未变。结论是此次重放未复现差异；第一次输出差异仍待解释，A1 及其组合暂不进入采纳。

限定写出调用链的源码核查显示，文本经过拥有所有权的 String/Arc 进入共享表，普通字符串中的实际控制字符会在 XML 序列化时转义。目前没有找到能解释该次变化的明确代码缺陷。该核查不能确定控制字符最早出现在哪个阶段。

为定位阶段创建的隔离诊断程序两次未完成构建：第一次 rustc 在 costing-xlsx 编译时 SIGSEGV，第二次关闭本次编译 wrapper、扩大线程栈并使用新产物目录，仍在未修改的 syn 依赖编译时 SIGSEGV。没有阶段内存或模型采样结果。编译器崩溃和文本变化的根因均未确定，不能仅凭先后关系认定它们有共同原因。

## 单对资源诊断

真实完整 SK、`mode='normal'`、`audit_mode='none'`，预热后每侧一次。下表为 `diagnostic` 单对诊断；正式收益仍待通过噪声校准、≥8 对筛选与独立 N=5。耗时包含 Excel 导出；峰值 RSS 是进程实际驻留内存的最高值，单位为字节。

| 候选 | 基线耗时（秒） | 候选耗时（秒） | 基线峰值 RSS | 候选峰值 RSS |
|---|---:|---:|---:|---:|
| b1-borrowed-reader | 47.276 | 46.280 | 3,335,639,040 | 3,337,179,136 |
| b2-key-buffer | 48.304 | 49.124 | 3,335,032,832 | 3,337,183,232 |

两项的基线和候选输出大小均为 75,430,019 字节。A2 候选预热进程以 −11（SIGSEGV）退出，报告 `CHILD_FAILED`，未取得配对样本。`real-remaining/summary.json` 的 passed 仅表示其 12 项工作簿比较通过；整体资源诊断及性能验收保持部分完成。汇总与各原始报告哈希见本地 `final-evidence.json`。

## 完整 SK 实测续作

后续直接使用真实完整 SK，新编译程序并通过首次计时校准；B1 保留 6 对有效数据，候选比较因原始基线内容异常停止。完整数据、阶段耗时与异常证据见[2026-11-12 实测记录](2026-11-12-full-sk-benchmark.md)。本记录保留首轮独立实验结果。

## 候选与边界

- A1：展示行按实际输出列数预留容量。
- A2：仅为实际缺失的派生列一次性精确预留容量。
- A1+A2：预登记组合，只有组件均通过完整保护后激活测速。
- B1：迁移已有 XLSX 借用读取候选，保留其它格式拥有型路径及全部数值转换规则。
- B2：直接追加规范化工单键，保留文本 `.0`、前导零、负数和 Decimal 规则。

正式业务源码保持基线，148 个冻结文件逐项 SHA-256 核对一致。候选源码位于本地 `trees/<id>/`，补丁位于 `packet-a/`、`packet-b/`。B 包基于原始基线；后续基线发生变化时须重新冻结并验证。逐单元格读取后续另行实施并两批筛选均被拒绝，见[读取优化记录](2026-11-12-streaming-reader.md)；Arc 写出未实施。

校准使用固定 CPU 集合，每批最多两次；具体环境身份仅保存在本地报告。Windows/Excel/WPS 验收未验证。

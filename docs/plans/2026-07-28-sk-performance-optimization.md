# SK 管线性能优化实验计划（证据驱动）

- 状态：`Completed`
- 日期：`2026-07-28`
- 完成日期：`2026-07-29`
- 适用基线：`v0.3.0-rc.1` 之后、正式实施开始时冻结的干净 `main`
- 范围：性能测量工具、Tier 1/2 候选实验、Tier 3 有界验证与正式采纳
- 性质：本文件保留“如何实验和如何决定”的协议；实际采纳以当前代码和 change 记录为准，
  不授权推送、PR、标签或发布

## 实施结果

- 采纳 reader 安全整数快路径：`26f29e3`。
- 采纳 T3-A `Arc<str>` 文本表示：`6d0f274`。
- 拒绝 `zmij`、ZIP Level 4、Thin LTO、forward-fill 和 T3-B 有界按列驻留。
- 最终采纳栈相对原始基线的 SK normal 8 对中，wall 改善 `6.1896%`、
  PWS 下降 `24.9597%`，两项均赢 `8/8`。
- 正式组合分支通过 Rust/Python、真实 GB/SK 全量和单月、N=5、
  原子/Manifest 契约与无 Rust/Python child `PATH` 的 Windows 包 smoke。
- 实验与最终验证见
  [`../changes/2026-07-28-sk-performance-experiments.md`](../changes/2026-07-28-sk-performance-experiments.md)；
  架构取舍见
  [`../decisions/2026-07-29-cell-value-arc-text.md`](../decisions/2026-07-29-cell-value-arc-text.md)。
- 未推送分支、创建 PR、推送标签或发布外部资产。

## 实施前结论

当前 SK 管线已经通过全部硬门禁，本轮不是救火，而是一次可以随时停止的主动优化：

| 指标 | v0.3.0 RC N=5 | 硬门禁 | 当前余量 |
|---|---:|---:|---:|
| Wall 中位数 | `17.1243s` | `<= 20.0s` | `2.8757s` |
| PWS 中位数 | `1,460,334,592 bytes` | `<= 2,147,483,648 bytes` | `687,149,056 bytes` |
| 最大输出 | `43,611,045 bytes` | `<= 48,658,823 bytes` | `5,047,778 bytes` |

来源见 [`../changes/2026-07-27-v0.3.0-rc-validation.md`](../changes/2026-07-27-v0.3.0-rc-validation.md)。

`14.5s` wall 和 `0.9~1.0GB` PWS 只作为方向性预估，不是完成标准。真正的完成标准是：

1. 每个候选都有可复核的独立实验；
2. 每个候选按预先冻结的门槛明确判定“采纳”或“拒绝”；
3. 最终采纳集合保持 workbook、CLI、Manifest、错误码和业务语义不变；
4. 最终采纳集合通过全量正确性、真实数据、性能、原子发布和 Windows 包 smoke；
5. 即使所有候选都被拒绝，只要实验完整、证据可复核，本计划仍可判定完成。

## 已确认的决策

- 性能目标是方向性预估，不为追目标强行扩大范围。
- `zmij` 只批准进入实验；是否进入默认 feature 仍由实验数据和后续依赖确认决定。
- 每个候选必须在实验前冻结主指标和最小有效收益，不能以“略快一点”为采纳理由。
- `zmij` 与 Level 4 冲突时按端到端收益选择，不给 `zmij` 固定优先级。
- Thin LTO 单独实验；`strip = "symbols"` 不属于本轮性能范围。
- forward-fill 优化降为待剖析假设，不预先承诺“消除 600 万次深拷贝”。
- Tier 3 先验证再决定是否实施，并拆为 `Arc<str>` 与有界文本驻留两个子实验。
- 每个性能假设独立构建、独立提交、独立留证；失败候选不混入采纳栈。
- 正式实验前先补齐可复用的 paired driver。

## 不可突破的边界

- Rust 仍是唯一正式业务实现；Python 只做比较、合成输入、测量和发布验收。
- 不新增生产依赖或把可选依赖加入默认构建，除非实验通过后再次获得明确批准。
- 不修改性能阈值、数值容差、冻结 baseline 或业务规则来让候选通过。
- 单元格容差保持 `1e-9`，列累计容差保持 `1e-8`。
- Sheet 名称、顺序、字段顺序、值、类型、样式、条件格式和勾稽保持不变。
- low-memory 触发条件保持 `5,000,000` cell slots；临时目录仍位于最终输出目录。
- 不向系统 `%TEMP%` 回退，不遗留 `.costing-tmp-*` 或 `.costing-publish-*`。
- 真实 ERP 输入、输出 workbook、原始报告、本机路径和未脱敏主机信息不得提交。
- `docs/superpowers/` 全程只读。
- 推送分支、创建 PR、推送标签、GitHub Release 和正式发布均是独立确认点。

业务与 workbook 事实以 [`../contracts/workbook.md`](../contracts/workbook.md) 为准，性能门禁以 [`../performance/README.md`](../performance/README.md) 为准。

## 现状核对与原计划修正

### 1. 当前压缩后端已经是 zlib-rs

受控 `rust_xlsxwriter` fork 固定使用 `zip 7.2`：

- `zip` 的 `deflate` feature 同时编译 `deflate-zopfli` 和 `deflate-flate2-zlib-rs`；
- Level `0..=9` 的普通 Deflate 写入实际走 `flate2`，当前后端是 `zlib-rs`；
- Zopfli 只在高于普通 Deflate 最佳级别时进入，不是当前 Level 5 路径；
- 项目的 `zlib` feature 会改走 C zlib / `libz-sys`，会增加 C 工具链和 PE 导入面。

因此本轮不实验 `zlib` feature，也不修改受控 fork。

### 2. `zmij` 在 lockfile 中不等于 writer 已启用它

`rust/Cargo.lock` 已有 `zmij 1.0.21`，但当前默认 feature 只有 `low-memory`。lockfile 中出现 crate 不能证明 `rust_xlsxwriter/zmij` 已生效；实验必须用 `cargo tree -e features` 保存实际 feature 证据。

启用 `zmij` 会把部分整数值 XML 从例如 `<v>1</v>` 改为 `<v>1.0</v>`。语义可以等价，但压缩前 XML 和最终 ZIP 体积都会改变。原计划对“整数 cell 数”和体积增量的估计尚未实测，必须由 Phase 0/候选实验量化。

### 3. 历史 LTO 数据只能作为假设来源

[`optimization-assessment.md`](optimization-assessment.md) 的历史实验同时设置了 Thin LTO 与 strip：

- check-only 有 8 对数据；
- full-run 只有 3 对；
- 运行时变化不能严格归因于 LTO；
- 该快照早于当前 v0.3.0 RC 基线。

因此历史结果不能直接作为采纳证据。本轮只实验 `lto = "thin"`，并重新完成正式配对。

### 4. forward-fill 不能靠 `take/replace` 消除全部必要 clone

在当前 `CellValue::Text(String)` 模型下，当前行和 `last_values` 需要同时拥有文本。成为种子的非空文本、以及真正填入空白单元格的文本，仍需要独立所有权。

可以先验证的浪费只有：

- 空白值被提前 clone；
- 每个填充列都重复读取和判断“是否集成车间”；
- 某些 clone 是否在真实数据上占据可观时间。

因此 T2-B 先计数和剖析，证明热点后才允许重构。

### 5. Tier 3 的驻留池必须拥有数据并且有界

`HashMap<&str, Arc<str>>` 不能安全地让 key 借用同一容器内 value。候选应使用 `HashSet<Arc<str>>` 或等价的拥有型结构，并支持按 `&str` 查询。

工单编号等高基数列可能让全局池增加而不是降低内存，所以驻留必须按列、受容量限制，并在真实基数统计后冻结策略。

### 6. 当前测量工具不能执行正式 paired 实验

`tools/validation/measure_release.ps1` 是单 binary 的 N 次硬门禁工具。它不会：

- 在 baseline/candidate 间奇偶反序；
- 分别预热两个 binary；
- 输出配对差值和配对胜率；
- 保留全部 CLI 阶段耗时；
- 在实验前后复核 binary 与输入哈希。

因此 Phase 0 必须先补 paired driver；现有硬门禁脚本保持原职责。

## 总体实施图

```text
Phase 0：冻结基线 + paired driver + 噪声校准
  |
  +-- Phase 1A：zmij
  +-- Phase 1B：Level 4
  |      \-- 组合选择
  +-- Phase 1C：Thin LTO（在选定的 I/O 组合上测增量）
  |
  +-- Phase 2A：reader 整数快路径
  +-- Phase 2B：forward-fill 假设（先证明热点）
  |      \-- Tier 2 组合复测
  |
  +-- Phase 3A：Arc<str> 单独验证
         \-- Phase 3B：有界按列驻留
                \-- Go / No-Go
  |
  +-- 最终采纳栈：全量正确性 + N=5 + Windows 包 smoke
```

每个箭头都是停走点。前一阶段失败不自动扩大下一阶段范围。

## Phase 0 — 冻结基线与补齐测量能力

### P0-1 冻结可复现实验身份

实施开始时从干净 `main` 冻结 `BASE_COMMIT`，记录：

- Git commit 与 tree hash；
- `rust-toolchain.toml`、`rust/Cargo.lock` 和输入文件 SHA-256；
- baseline/candidate binary SHA-256；
- Cargo release profile 和实际 feature tree；
- Rust/Cargo 版本、目标三元组；
- 脱敏机器指纹、电源模式和实验时间窗口；
- 构建时使用的 `COSTING_GIT_COMMIT`、`SOURCE_DATE_EPOCH` 等身份变量。

baseline 与 candidate 必须：

- 从明确的提交构建，不从脏工作区直接产出正式实验 binary；
- 使用不同且初始不存在的 `CARGO_TARGET_DIR`；
- 使用同一工具链、同一输入、同一机器和同一安全软件配置；
- 构建后复制到只读实验目录，正式运行期间不再覆盖。

### P0-2 新增 paired driver

计划新增 `tools/validation/measure_paired_release.ps1`，不修改现有 `measure_release.ps1` 的硬门禁语义。

建议接口：

```powershell
.\tools\validation\measure_paired_release.ps1 `
  -BaselineBinary <baseline.exe> `
  -CandidateBinary <candidate.exe> `
  -Pipeline sk `
  -InputPath <sk-real.xlsx> `
  -Mode normal `
  -OutputDirectory <new-empty-directory> `
  -Pairs 8 `
  -ReportPath <new-report.json>
```

driver 必须：

1. 解析并固定绝对路径，但报告只写安全别名与哈希；
2. 在实验前分别预热 baseline/candidate 一次，预热不计入样本；
3. 奇数对按 baseline→candidate，偶数对按 candidate→baseline；
4. 每次启动独立进程，并使用独立且事先不存在的输出路径；
5. 每 `10ms` 轮询一次 Peak Working Set，与现有硬门禁脚本一致；
6. 捕获外部 wall、PWS、输出字节、输出 SHA-256 和全部 `stage_timings.stages`；
7. 对每一对计算 `(candidate - baseline) / baseline`，报告配对中位差和胜率；
8. 捕获 `status`、Sheet 数、run counts、error-log counts 和 issue-type counts；
9. 每次运行后检查同目录临时文件残留；
10. 实验结束后重新计算两个 binary 和输入 SHA-256；
11. 任何中断、哈希变化、输出复用、解析失败或环境切换都使整批无效，不允许只删除不利样本；
12. 不在定时样本之间运行 workbook comparator，避免比较器负载污染下一对。

报告 schema 至少包含：

- `schema_version`、pipeline、mode、pair count、奇偶顺序；
- baseline/candidate/input SHA-256；
- 每对原始安全指标；
- 各指标 baseline/candidate 中位数；
- 配对差值中位数和候选胜率；
- PWS 与输出最大值；
- 环境限制和无效批次原因；
- 结果只给测量事实，不替代候选的最终采纳决定。

验证 driver：

- 参数、路径冲突、已存在输出和非零退出的失败 smoke；
- 同一 binary 对同一 binary 的 1 对 synthetic smoke；
- JSON schema/必需字段测试；
- 对固定小样本验证中位数、配对差值、胜率和奇偶顺序；
- 检查报告不含输入文件名、绝对路径或原始主机信息。

### P0-3 重新建立当前阶段基线

旧评估中的 `writer_populate = 6.66s`、`xlsx_save = 4.98s` 等数据只作历史参考。Phase 0 必须用当前代码重新采集：

- GB/SK 现有 N=5 硬门禁；
- SK normal 全部阶段耗时；
- SK check-only 全部阶段耗时；
- baseline-vs-baseline 4 对噪声校准。

若 baseline-vs-baseline 的 external wall 配对中位差绝对值超过 `1%`，或目标阶段差异超过 `5%`，先排查系统负载、缓存、温度和测量工具，不进入候选实验。

## 统一实验与判定协议

### 正式样本

- 每个候选先过正确性，再跑性能。
- 每个 binary 各预热一次。
- 正式实验固定 8 对，奇偶反序。
- normal 与 check-only 使用不同实验目录和报告。
- 所有百分比使用 8 个“对内相对差”的中位数，不用两个独立中位数相减。
- 主指标至少胜 `6/8` 对。
- 输出大小看候选所有 normal 样本的最大值，不看中位数。
- Tier 1/2 的 PWS 中位数不得回退超过 `5%`，且不得越过绝对门禁。
- 任一非目标阶段回退超过 `5%` 必须调查；不能解释则拒绝候选。
- 同一候选若修改后重新实验，必须使用新 binary 哈希、新目录和完整 8 对，不得把两批样本拼接。

### 候选采用矩阵

| 候选 | 主采用门槛 | 补充条件 |
|---|---:|---|
| `zmij` | `writer_populate` 改善 `>= 3%`，external wall 改善 `>= 1%` | 主指标胜 `>= 6/8` |
| Level 4 | `xlsx_save` 改善 `>= 15%`，external wall 改善 `>= 2%` | 主指标胜 `>= 6/8`，最大输出不越线 |
| Thin LTO | external wall 改善 `>= 1%` | wall 胜 `>= 6/8`，记录构建时长和 binary 大小 |
| reader 整数快路径 | `ingest` 改善 `>= 5%` 或绝对减少 `>= 0.20s`，external wall 改善 `>= 1%` | 主指标胜 `>= 6/8` |
| forward-fill 假设 | `normalize` 改善 `>= 10%` 或绝对减少 `>= 0.15s` | 主指标胜 `>= 6/8`，wall 回退 `<= 2%` |
| Tier 3 A+B | PWS 改善 `>= 15%` | package 完全一致，wall 回退 `<= 2%` |

所有候选还必须满足：

- Rust、Python、synthetic、真实 GB/SK 正确性通过；
- 硬门禁、容差和业务规则不变；
- error-log counts、issue-type counts、run counts 不变；
- 无临时文件残留；
- 未达到门槛就拒绝，不用次要指标重新包装结论。

## Phase 1 — 构建与输出热点

### T1-A `zmij` 数值格式化

假设：`rust_xlsxwriter/zmij` 可以减少数值 cell 格式化成本，从而降低 `writer_populate` 和端到端 wall。

实验方式：

1. baseline 保持默认 `low-memory`；
2. candidate 先用显式 feature 构建，不直接修改默认 feature；
3. 用 `cargo tree -e features` 证明 candidate 实际启用了 `rust_xlsxwriter/zmij`；
4. 对 `0.0`、`-0.0`、整数、小数、大数和科学计数值增加 XML/语义 fixture；
5. 本地统计被改变的数值 XML cell 数和压缩前字节增量，只提交聚合数据；
6. 运行 SK normal 8 对，记录 `writer_populate`、wall、PWS 和输出最大值；
7. `zmij` 同时影响标准和 low-memory writer，因此 GB 也要检查 semantic 零差异、wall、PWS 和最大输出门禁。

预期差异：

- package fast path 可能因数值 XML 字面量变化而失效；
- 允许进入 semantic 比较，但 `mismatch_count` 必须为 `0`；
- 不允许用“Excel 显示相同”代替仓库比较器。

若实验通过采用矩阵，只形成“建议启用”的 evidence；把 `zmij` 加入默认 feature 前仍需单独确认生产依赖取舍。

### T1-B low-memory ZIP Level 4

假设：Level 4 能在 Level 5 与已拒绝的 Level 3 之间取得可接受的速度/体积平衡。

事实边界：

- `set_compression_level(5)` 只在 low-memory workspace 存在时执行；
- SK 触发 low-memory，GB 标准 writer 不走该设置；
- Level 3 历史输出为 `52,528,293 bytes`，超过硬门禁 `3,869,470 bytes`；
- Level 4 尚无正式实测，不能根据线性插值预判。

实验：

1. 只把 low-memory 设置从 Level 5 改为 Level 4；
2. 对 baseline Level 5 与 candidate Level 4 跑 SK normal 8 对；
3. 要求 workbook package fast path 为 `0 mismatch`；
4. 检查每轮输出最大值和同目录临时文件；
5. Level 4 若超体积门禁，直接拒绝并保留 Level 5，不重复 Level 3/2，也不继续试 Level 1。

### T1-A/T1-B 组合选择

只有单独通过门槛的候选才能进入组合实验。

若 `zmij` 与 Level 4 都单独通过：

1. 再测 `zmij + Level 4` 8 对；
2. 组合通过全部硬门禁且 wall 收益不低于最佳单项时，保留组合；
3. 组合超输出门禁时，在两个单项中保留 external wall 收益更大的候选；
4. 两个单项 wall 收益差异不足 `1%` 时，选择输出更小、变更更简单的候选；
5. 选定集合记为 `T1_IO_BASELINE`。

不允许因为 `zmij` 曾获准实验就固定优先保留。

### T1-C Thin LTO

假设：Thin LTO 能在最终 I/O 候选组合上提供至少 `1%` 的端到端增量收益。

做法：

- baseline 使用 `T1_IO_BASELINE`；
- candidate 只增加 `lto = "thin"`；
- 不设置 `strip = "symbols"`；
- 记录完整构建耗时、binary 字节、SHA-256 和 `--version-json`；
- 跑 SK normal 8 对，GB 做回归；
- 通过后将结果加入 Tier 1 采纳栈并再次跑组合验证。

`strip` 若未来需要，必须作为独立的发布体积/可观测性决策处理。

## Phase 2 — 有界源码热点

Tier 2 的每个候选都在最终 Tier 1 采纳栈上测增量，但彼此先保持独立。单项通过后再做 Tier 2 组合复测。

### T2-A reader f64→Decimal 整数快路径

当前 `float_cell_value` 对有限 f64 统一经过：

```text
f64 -> String -> Decimal::from_str_exact / from_scientific
```

候选只允许对以下值走 `Decimal::from(i64)`：

- `value.is_finite()`；
- `value.fract() == 0.0`；
- 值位于安全的 `i64` 转换范围内。

其他值必须原样回退当前字符串路径。不得改变：

- `0.1 -> Decimal::new(1, 1)`；
- `12.34 -> Decimal::new(1234, 2)`；
- 非有限值转文本的行为；
- 非整数、科学计数和超范围值的现有解析语义。

测试至少覆盖：

- `0.0`、`-0.0`、正负整数；
- `2^53` 附近的可表示整数；
- `i64` 边界附近与范围外值；
- `0.1`、`12.34`、极小/极大有限小数；
- NaN、正负无穷；
- 新快路径与保留的旧参考转换函数逐值相等。

正确性通过后跑 SK check-only 8 对验证 `ingest`，再跑 SK normal 8 对验证真实 wall、PWS 和输出。该候选不应改变 XML payload；若未走 package fast path，必须先解释差异。

### T2-B forward-fill 待剖析假设

先做不进入生产输出的计数/剖析，至少区分：

- 空白 CellValue clone 次数；
- 非空种子 clone 次数；
- 实际向空白单元格填充值的 clone 次数；
- 每行重复“集成车间”判断次数；
- 文本与非文本 variant 占比。

只有数据证明 normalize 中这部分仍是可观热点，才允许实现最小候选：

- 先借用判断空白，避免提前 clone 空白值；
- 只在非空值成为 seed 时做必要 clone；
- 真正填充目标单元格时保留必要 clone；
- 在“成本中心名称”完成本行前向填充后缓存 `integrated_row`；
- 供应商列必须使用填充后的成本中心判断；
- 集成车间行不得成为供应商 seed，也不得被填入供应商。

不得把 `integrated_row` 无条件提到列循环之前。现有 vendor/集成车间专项测试必须保留并补充：

- 本行成本中心为空、由上一行填成集成车间；
- 集成车间行自身带供应商；
- 集成车间之后恢复普通车间；
- 集成车间行自身的供应商不得成为后续行的 seed。

未达到 normalize 门槛即拒绝。若 Tier 3 最终获准实施，必须在最终栈上重新测 T2-B；其增量收益消失时，从最终栈移除 T2-B，避免为重复收益保留额外复杂度。

## Phase 3 — 文本表示与驻留的有界验证

Tier 3 不因 Tier 1/2 完成而自动启动完整实施。先完成聚合 census，再执行两个子实验。

### T3-0 文本基数与内存 census

在忽略目录生成只含聚合数据的报告：

- 总行/列/cell 数；
- `Blank`、`Text`、`DateLike`、`Decimal` 数量；
- 各文本列的非空数、去重数、重复率和总 UTF-8 bytes；
- 高基数列和低基数列分组；
- 当前 `CellValue` 与候选表示的 `size_of`；
- 估算 Arc header、hash bucket 和额外引用的上界；
- 不记录任何 cell 原值、文件名或绝对路径。

容量策略必须根据 census 预先冻结，不能在看到性能结果后调参。

### T3-A 仅改 `CellValue` 为 `Arc<str>`

候选：

```rust
Text(Arc<str>)
DateLike(Arc<str>)
```

此阶段不增加驻留池，用于单独回答：

- enum 尺寸是否下降；
- 下游 clone 是否显著变便宜；
- `String -> Arc<str>` 的重新分配/复制是否拖慢 ingest；
- 单靠表示变化能降低多少 PWS。

约束：

- `PartialEq` 仍按内容比较；
- 非空文本前后空白必须原样保留；
- 序列化仍是现有 `{"kind": ..., "value": ...}`；
- 优先评估启用现有 serde 的 `rc` feature 以保留 derive；若 feature 审计显示额外影响，再使用经过 golden 测试的最小手写 `Serialize`；
- workbook package fast path 必须完全一致。

只有正确性通过、wall 回退不超过 `2%`、PWS 不回退超过 `5%`，才继续 T3-B。

### T3-B 有界、按列文本驻留

在 T3-A 基础上使用 `HashSet<Arc<str>>` 或等价拥有型池：

- 每列独立统计和驻留；
- 可用 `&str` 查询已有 Arc；
- 低基数列复用同一 Arc；
- 高基数列达到预先冻结的容量上限后停止新增；
- 不驻留 Blank；
- 不 trim、不规范化、不改变文本内容；
- 停用驻留只影响内存策略，不影响 CellValue 语义。

不得使用无界全局池，也不得使用借用同一容器内部值的 `HashMap<&str, Arc<str>>`。

### Tier 3 Go / No-Go

T3-A + T3-B 的最终候选只有同时满足以下条件才是 `Go`：

- SK normal PWS 配对中位数下降 `>= 15%`；
- external wall 回退 `<= 2%`；
- workbook package fast path 完全一致；
- 序列化 golden 完全一致；
- 不新增生产依赖；
- 所有 Rust、Python、真实数据和 hard gate 通过。

任一条件不满足即 `No-Go`：删除生产候选，只保留脱敏实验记录。即使达到 `Go`，进入正式核心模型改造前仍需单独确认。

## 正确性与真实数据矩阵

### 每个代码候选先运行

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo clippy --locked --manifest-path rust/Cargo.toml --workspace --all-targets --all-features -- -D warnings
cargo test --locked --manifest-path rust/Cargo.toml --workspace --all-features
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate

uv sync --frozen --extra dev
uv run python -m ruff check tests tools
uv run python -m ruff format tests tools --check
uv run python -m pytest tests -q -m "not slow and not benchmark" --basetemp .pytest-tmp/python-validation
uv run python tools/ci/run_synthetic_e2e.py --binary rust/target/release/costing-calculate.exe
```

### 真实 workbook 比较

每个准备采纳的候选至少运行 GB/SK normal：

```powershell
uv run python -m tools.validation.compare_releases `
  --baseline-binary <baseline.exe> `
  --candidate-binary <candidate.exe> `
  --pipeline sk `
  --input <sk-real.xlsx> `
  --output-dir <new-empty-directory> `
  --report <new-report.json>
```

reader/core/Tier 3 候选以及最终采纳栈还必须对 GB/SK 各跑一次单月过滤。

比较预期：

| 候选 | 预期比较模式 |
|---|---|
| `zmij` | 允许 semantic，必须 `mismatch_count = 0` |
| Level 4 | 应保持 package fast path |
| Thin LTO | 应保持 package fast path |
| reader 快路径 | 应保持 package fast path |
| forward-fill | 应保持 package fast path |
| Tier 3 | 必须保持 package fast path |

除 `zmij` 的已知数值字面量差异外，任何候选落入 semantic 模式都要先调查，不能直接当作通过。

### 最终硬门禁

最终采纳栈使用现有入口分别运行 GB/SK N=5：

```powershell
.\tools\validation\measure_release.ps1 `
  -BinaryPath <final-candidate.exe> `
  -Pipeline sk `
  -InputPath <sk-real.xlsx> `
  -OutputDirectory <new-empty-directory> `
  -Iterations 5 `
  -ReportPath <new-report.json>
```

并额外确认：

- check-only 不写 workbook；
- Manifest schema 仍为 V1，路径脱敏和 SHA-256 正常；
- low-memory 成功/失败路径都无临时残留；
- 三张 Sheet 的名称、顺序、字段、样式和勾稽不变；
- SK `error_log_count = 201,815`、GB `error_log_count = 20,515`，issue-type counts 不变；
- 从干净提交构建 Windows ZIP，并按 `tools/release/README.md` 完成无 Rust/Python child `PATH` smoke。

## 实验、提交与文档治理

每个假设使用独立分支或 worktree、独立提交和独立 evidence：

1. paired driver；
2. `zmij`；
3. Level 4；
4. `zmij + Level 4` 组合；
5. Thin LTO；
6. reader 快路径；
7. forward-fill 假设；
8. T3-A；
9. T3-B；
10. 最终采纳栈。

拒绝候选：

- 不进入最终生产提交；
- 在 `docs/changes/` 记录 `Rejected`、binary/input SHA-256、协议、指标和拒绝原因；
- 不通过修改门禁或删除不利样本重新实验。

采纳候选：

- 在 `docs/changes/` 记录 `Adopted` 及完整脱敏证据；
- 重要依赖/架构取舍写入 `docs/decisions/`；
- 同步 [`../performance/README.md`](../performance/README.md)；
- 将 [`optimization-assessment.md`](optimization-assessment.md) 标注为被本轮结果部分取代，并纠正压缩后端描述；
- 仅在实际有变化时更新 `rust/Cargo.lock`；
- 将最终事实同步到代码、测试、根 README、AGENTS、`docs/README.md` 和相关契约。

过程文档不能代替当前事实。`docs/superpowers/` 不修改。

## 停止条件

出现以下任一情况时停止当前候选，不自动扩大范围：

- 正确性、业务契约或 hard gate 失败；
- 输出体积越线；
- 目标收益未达到预设门槛；
- 非目标阶段回退无法解释；
- baseline-vs-baseline 噪声校准失败；
- binary/input 哈希在实验中变化；
- paired driver、PWS 或阶段指标不可靠；
- 为继续优化需要新增生产依赖、修改受控 fork、引入多线程或替换 Calamine。

后四类扩大范围事项必须另立计划并重新确认。

## 明确不做

- 不启用 C zlib feature；
- 不设置 `strip = "symbols"`；
- 不设置 `panic = "abort"`；
- 不替换 Calamine 为流式 reader；
- 不移除 `IndexedRow::get` 的 schema 校验；
- 不多线程化业务管线；
- 不改变 Decimal→f64 的既有输出边界；
- 不修改 low-memory 阈值；
- 不恢复 Python 业务实现；
- 不在本计划中推送、合并、打标签或发布。

## 完成定义

本计划只有在以下事项全部完成后，状态才能从 `In Progress` 改为 `Completed`：

- paired driver 已验证并可复用；
- 当前 baseline 已重新冻结；
- 每个启动的候选都有 Adopted/Rejected 结论和 evidence；
- Tier 3 有明确 Go/No-Go；
- 最终采纳栈完成全量正确性、真实 GB/SK、N=5 和 Windows 包 smoke；
- 文档、决策、变更记录和当前事实已同步；
- 工作区无意外敏感文件、临时输出或未解释改动。

进入实施与启动正式 Tier 3 改造已分别获得确认并完成。`zmij` 未采纳；
创建 PR、推送或发布仍需要各自的明确确认。

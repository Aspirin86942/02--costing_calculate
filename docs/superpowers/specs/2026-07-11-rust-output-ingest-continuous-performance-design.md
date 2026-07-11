# Rust 输出与读取持续性能优化设计

**状态：** 设计内容已逐节批准；书面规格待用户复核后进入实施计划
**日期：** 2026-07-11
**适用仓库：** `D:\python_program\02--costing_calculate`
**目标实现：** Rust CLI `costing-calculate`
**批准路线：** 自适应 low-memory writer + writer 热路径优化 + ingest 去中间副本

## 1. 目标

在三张业务 Sheet、运行摘要、质量指标、业务错误日志和 Python oracle 语义完全一致的前提下，持续优化 Rust 正常模式的真实用户时延和峰值内存。

本设计的最终硬目标为：

```text
SK 正常模式真实进程 wall-clock 正式 N 轮中位数 <= 20.0 秒
SK 正常模式 Peak Working Set 正式 N 轮中位数 <= 2.0 GiB
GB 正常模式 wall-clock 相对固定基线回退 <= 5%
GB 正常模式 PWS 相对固定基线回退 <= 5%
GB / SK 输出文件大小相对固定基线增长 <= 10%
GB / SK 正式 N 轮全部成功
Workbook / runtime / quality / error-log 契约完全一致
Windows 发布物为单 EXE，不依赖第三方 zlib DLL
```

`N` 默认等于 5；只有触发第 10.5 节的临界噪声规则时才追加为 10。`N` 对同一候选批次全局生效：任一 time/PWS 门槛触发追加后，时间套件和独立 PWS 套件都补足到 10 轮，所有成功率和中位数都按完整 10 轮计算。

真实进程 wall-clock 的边界为：

```text
起点：costing-calculate.exe 进程启动
终点：workbook 已关闭、JSON 摘要已输出、进程成功退出
排除：Cargo build 和 Cargo 自身启动开销
```

决策优先级保持：

1. Correctness；
2. Maintainability；
3. Observability；
4. Performance。

性能目标不能覆盖前三项。

## 2. 当前证据

### 2.1 SK 正常模式阶段耗时

2026-07-11 当前正常模式单次实测：

```text
export        14.6670046s
ingest        10.1947928s
normalize      1.4366271s
presentation   0.8362335s
fact           0.6420348s
split          0.2254484s
--------------------------------
阶段合计      28.0021412s
```

其中：

```text
export 占阶段合计约 52.38%
ingest 占阶段合计约 36.41%
export 比其余五阶段合计慢约 10%
```

因此只优化业务计算无法达到 20 秒目标；writer、压缩和 ingest 都必须进入可测量的持续优化路径。

### 2.2 SK 输出结构

当前输出文件：

```text
size = 44,235,294 bytes，约 42.19 MiB
```

ZIP 内部结构：

| Sheet | 维度 | 未压缩 XML | 压缩后 |
|---|---|---:|---:|
| 成本计算单总表 | `A1:AE425460` | 418,788,156 bytes | 41,626,974 bytes |
| 成本计算单数量聚合维度 | `A1:AZ11240` | 19,661,163 bytes | 2,523,945 bytes |
| 成本分析工单维度 | `A1:AK56` | 73,730 bytes | 10,468 bytes |

主要输出成本集中在第一张总表。按实际维度估算：

```text
总表数据槽位约 425,459 × 31 = 13,189,229
数量页数据槽位约 11,239 × 52 = 584,428
分析页数据槽位约 55 × 37 = 2,035
合计约 13,775,692 个数据槽位
```

输出文件从原子创建到最后写入的文件时间跨度约为 7.45 秒。由于当前 writer 在逐格填充 workbook 后才创建最终文件，这说明 `export=14.67s` 很可能近似分为：

```text
Workbook 逐格装配约 7 秒
XML / ZIP 压缩及最终写入约 7 秒
```

该拆分仅用于确定需要新增分段计时，不能替代正式 benchmark。

### 2.3 当前 PWS

SK check-only 当前五轮 PWS 中位数：

```text
1,492,865,024 bytes，约 1.390 GiB
```

它尚未进入标准内存 writer。正常模式若继续让 `rust_xlsxwriter` 为约 1,378 万个数据槽位建立第二套单元格结构，存在超过 2.0 GiB 的高风险。

### 2.4 当前 writer 热路径

当前 `write_data_rows()` 对每个单元格执行：

```text
columns.get(col_idx)
number_formats.get(column_name)
numeric_format(...)
write_*()
```

数字格式列中的空白值也会先执行格式查找和 `Format` 构造，再被 `Blank` 分支跳过。列格式已在表头阶段设置，逐格重复构造不具备业务必要性。

### 2.5 当前 ingest 中间副本

当前 `read_raw_workbook()`：

```text
Calamine Range<Data>
→ rows().map(row.to_vec())
→ Vec<Vec<Data>>
→ normalize_data_row
→ Vec<Vec<CellValue>>
```

这会在 Calamine 已经持有完整 `Range<Data>` 后，再建立一个整表 `Vec<Vec<Data>>`，随后再次转换和复制字符串。该中间整表副本没有业务必要性。

## 3. 范围

### 3.1 本轮范围

- 增加 writer populate、XLSX save、输出字节数等可观测性；
- 为每张 Sheet 建立确定性的写出计划；
- 缓存每列格式并让空值走快路径；
- 对大 Sheet 使用 `rust_xlsxwriter` low-memory worksheet；
- 对小 Sheet 保持 standard worksheet；
- 在输出父目录创建受控临时工作区；
- 对 `zlib`、`zmij` 做隔离 A/B，只保留达到门槛的 feature；
- 删除 ingest 的中间 `Vec<Vec<Data>>`；
- 扩展真实 wall-clock、PWS、文件大小和单 EXE 发布门禁；
- 增加针对 temp cleanup、结构化错误和磁盘空间的测试。

### 3.2 非目标

- 不改变 GB/SK 业务规则；
- 不改变三张 Sheet 的名称、顺序、列顺序和样式；
- 不改变白名单、Modified Z-score、成本分类或勾稽口径；
- 不把 `Decimal` 提前改为 `f64`；
- 不更换 Calamine 或 rust_xlsxwriter；
- 不自研 XLSX、XML 或 ZIP writer；
- 不引入 Polars、Arrow、IndexMap、自定义 allocator 或字符串驻留池；
- 不加入 LTO 或其他 release profile 永久调参；
- 不让 check-only 跳过完整 presentation；
- 不在当前主路线取消 `WorkbookPayload` 或完整 `SheetModel.rows`；
- 不自动回退到可能突破内存门槛的 standard writer；
- 不自动覆盖、删除或重命名已有业务输出；
- 不把运行时文件错误写入业务 `error_log`。

## 4. 已批准约束

### 4.1 平台与发布

```text
平台：Windows 10/11 x64
工具链：MSVC
构建期：允许原生 C 依赖
运行期：单 EXE，不允许第三方 zlib DLL
Linux：不在本轮发布门槛内
```

### 4.2 临时文件

- 只允许写入最终输出目录下的运行级受控临时目录；
- 不使用系统 `%TEMP%`；
- 目录形态为 `.costing-tmp-<request_id>-<random>`，既可审计又避免并发碰撞；
- 成功和失败路径都必须显式清理；
- 清理失败不得静默忽略；
- 仅当至少一张 Sheet 使用 LowMemory 时，输出卷才要求至少 1 GiB 可用空间；
- 临时目录继承输出父目录 ACL；
- 临时目录和性能输出目录不得进入原始输入文件扫描。

### 4.3 输出大小

同一固定输入下：

```text
candidate output bytes median
<= baseline output bytes median × 1.10
```

优先使用 low-memory 而不是 inline-string constant-memory，以保留 shared strings 并控制文件大小。

## 5. 总体架构

```text
costing-cli
  │
  ├─ ingest / normalize / split / fact / presentation
  │
  └─ WorkbookPayload
         │
         ▼
costing-xlsx
  ├─ SheetWritePlanner
  │    ├─ 计算 cell_slots
  │    ├─ 选择 Standard / LowMemory
  │    └─ 构建 ColumnWritePlan
  │
  ├─ TempWorkspace
  │    ├─ 输出父目录下创建受控临时区
  │    ├─ 设置 rust_xlsxwriter tempdir
  │    └─ 显式关闭和清理
  │
  ├─ WorkbookWriter
  │    ├─ Sheet 元数据配置
  │    ├─ 严格按行写出
  │    ├─ 可选静态 zlib 压缩（仅 A/B 达标后保留）
  │    └─ create_new(true) 防覆盖
  │
  └─ WorkbookWriteReport
       ├─ writer_populate_seconds
       ├─ xlsx_save_seconds
       └─ output_size_bytes
```

外部 benchmark harness 负责真实进程 wall-clock 和 PWS，不把 CLI 内部计时冒充为端到端时间。

## 6. 组件设计

### 6.1 `SheetWritePlanner`

输入：

```rust
&SheetModel
```

输出概念：

```rust
SheetWritePlan {
    mode: Standard | LowMemory,
    cell_slots: usize,
    columns: Vec<ColumnWritePlan>,
}
```

模式选择：

```text
cell_slots = rows.len().saturating_mul(columns.len())

cell_slots >= 5,000,000 → LowMemory
cell_slots <  5,000,000 → Standard
```

当前结果：

| Sheet | 预期模式 |
|---|---|
| SK 成本计算单总表 | LowMemory |
| SK 数量聚合页 | Standard |
| SK 工单分析页 | Standard |
| 当前 GB 三张 Sheet | Standard |

阈值按数据规模选择，不按 `gb/sk` 名称硬编码。

### 6.2 `ColumnWritePlan`

每列只解析一次：

- Excel 列号；
- 文本格式；
- 可选数字格式；
- 固定列宽；
- 后续单元格写法。

数据循环中：

1. `Blank` 最先跳过；
2. 直接按 `col_idx` 读取列计划；
3. 复用预构造 `Format`；
4. 不再按中文列名查询 `BTreeMap`；
5. 不再逐格构造数字格式。

### 6.3 low-memory 顺序约束

在写入第一行数据之前完成：

- worksheet 名称；
- 列宽；
- 列格式；
- autofilter 范围；
- freeze panes；
- 其他不依赖已写单元格的 Sheet 元数据。

随后严格按：

```text
header row
data row 1
data row 2
...
```

写入。low-memory 模式不允许返回修改旧行。

### 6.4 `TempWorkspace`

仅当至少一张 Sheet 选择 LowMemory 时创建。

实现要求：

- 使用 `tempfile::Builder::prefix(&format!(".costing-tmp-{request_id}-")).tempdir_in(output_parent)`；
- `request_id` 沿用当前 CLI 运行标识，`tempfile` 追加随机后缀避免并发碰撞；
- `Workbook::set_tempdir()` 必须在添加 low-memory Sheet 前调用；
- workbook drop 后显式调用 `TempDir::close()`；
- RAII drop 只作为进程异常时的兜底；
- 显式 close 的错误必须进入 Error Model。

### 6.5 `WorkbookWriteReport`

`write_workbook()` 返回：

```rust
WorkbookWriteReport {
    writer_populate_seconds: f64,
    xlsx_save_seconds: f64,
    output_size_bytes: u64,
}
```

定义：

- `writer_populate_seconds`：Workbook/Sheet 创建、元数据、表头、数据单元格，以及 low-memory 临时 XML 写入；
- `xlsx_save_seconds`：`save_to_writer` 的最终组包、ZIP 压缩和最终文件写入；
- `export`：CLI 外层现有计时，覆盖 temp workspace、populate、save、显式清理和少量胶水；
- `output_size_bytes`：文件关闭后读取。

现有 `total` 继续表示 ingest 前到内存 payload 返回，不包含 export。

### 6.6 ingest 去中间副本

新流程：

```text
worksheet_range
→ Range<Data>
→ 借用 Range 扫描表头
→ 直接逐行 normalize_data_row
→ Vec<Vec<CellValue>>
```

必须保留：

- 双层表头识别；
- 表头前说明行；
- 最大列宽；
- 空字符串转 Blank；
- String/Float/Int/Bool/DateTime/Error 语义；
- 科学计数法和非有限 float；
- Decimal 精度；
- reader snapshot；
- 原始行数。

## 7. 依赖决策

### 7.1 现有依赖 feature

对现有 `rust_xlsxwriter` 评估：

```text
constant_memory：提供 low-memory worksheet/tempdir 能力
zlib：Windows/MSVC 原生压缩候选
zmij：数字转换候选
```

`costing-xlsx` 暴露同名的最小 feature 转发：`low-memory -> rust_xlsxwriter/constant_memory`、`zlib -> rust_xlsxwriter/zlib`、`zmij -> rust_xlsxwriter/zmij`；`costing-calculate` 再转发这三个 feature，确保发布命令只针对 binary package。默认构建不隐式启用候选 feature；Phase 3 最终候选必须显式启用 `low-memory`。

不在本设计中升级 rust_xlsxwriter 版本；继续由当前 lockfile 固定解析版本。

### 7.2 新直接依赖

`costing-xlsx` 增加：

- `tempfile`：受控临时目录和可检查的显式 close；
- `windows-sys`：Windows-only 调用 `GetDiskFreeSpaceExW`。

`windows-sys` 仅启用所需 Win32 Storage/FileSystem API，不引入运行时第三方 DLL。

### 7.3 feature 保留门槛

所有 feature 候选先满足共同门禁：

```text
GB_candidate_wall / GB_phase0a_wall <= 1.05
GB_candidate_pws / GB_phase0a_pws <= 1.05
SK_candidate_bytes / SK_phase0a_bytes <= 1.10
GB_candidate_bytes / GB_phase0a_bytes <= 1.10
GB / SK correctness 契约全部通过
GB / SK 正式计时轮次全部成功
```

共同门禁中的外部 wall-clock、PWS 和输出大小都以固定 Phase 0A reference binary 为基准；feature 专属收益以 Phase 2 的 default 候选为分母。wall-clock/PWS 的 `phase0a` 数值指同一批次中与 candidate 交替运行的固定 reference binary 中位数，输出字节数则取已批准 manifest 的不可变值。

`zlib`：

- `zlib_xlsx_save / default_xlsx_save <= 0.85`；
- 第 12 节 PE 普通/delay imports 规则通过；
- 干净 Windows 单 EXE smoke 通过。

`zmij`：

- `zmij_writer_populate / default_writer_populate <= 0.97`，或 `zmij_export / default_export <= 0.97`；
- 对用于过门的指标，每个五轮正式组至少 4/5 配对轮满足 candidate < default；若 `N=10`，两组分别满足；
- 否则移除。

组合 D 的增量归因分别比较 `D/C` 的 zlib 收益和 `D/B` 的 zmij 收益；不允许用另一个 feature 的收益替未达标 feature 过关。

## 8. 执行阶段与停止规则

### Phase 0A：冻结当前行为基线

- 构建并冻结当前 release EXE；
- 记录 Git HEAD、working-tree diff id、输入 SHA、binary SHA、Windows/CPU/内存/磁盘；
- GB/SK 各 1 次预热 + 5 次 wall-clock；
- 另行执行 1 次预热 + 5 次 PWS；
- 每轮独立输出路径；
- 每轮完成 workbook 语义比较；
- 保存全部原始值。

Phase 0A 结束时生成不含 ERP 内容的版本化证据：

```text
docs/performance/baselines/2026-07-11-windows-x64-phase0a.json
```

其中固定 GB/SK 输入 SHA、reference binary SHA、Git HEAD/diff id、runtime/error counts、Sheet 维度、输出字节数、校准用逐轮 wall-clock/PWS 和机器信息。该文件须经用户确认并提交；未确认前不得开始 Phase 1。后续不得替换 reference binary 或改写 manifest；候选阶段仍须重新运行同一个 reference binary，与 candidate 成对交替采样。

### Phase 0B：仅增加观测性

新增 `writer_populate`、`xlsx_save`、`output_size_bytes`，不改变写出策略。

GB/SK 的 `instrumented_wall / same_batch_phase0a_reference_wall` 均不得超过 `1.02`，且正确性必须通过。超限但按第 10.5 节公式仍位于 3% 临界区时补足为 10 轮；超出临界区则直接拒绝，不以重采挽救明显回退。合并证据仍超过时调整计时实现，不进入 Phase 1。

通过后冻结 Phase 0B binary SHA；Phase 1 的 `writer_populate/xlsx_save` 与该 binary 在同一批次交替比较，不能拿不同机器状态下的孤立 JSON 相除。

### Phase 1：writer CPU 热路径

仅增加 blank 快路径、Sheet/ColumnWritePlan 和 Format 复用。本阶段虽然建立 `SheetWritePlan`，但强制所有 Sheet 使用 Standard；`cell_slots` 的模式选择只在 Phase 3 激活。

保留条件：

```text
writer_populate_phase1 / writer_populate_phase0b_same_batch <= 0.90
xlsx_save_phase1 / xlsx_save_phase0b_same_batch <= 1.05
GB_wall_candidate / GB_wall_phase0a <= 1.05
GB_pws_candidate / GB_pws_phase0a <= 1.05
SK_pws_candidate / SK_pws_phase0a <= 1.05
SK_bytes_candidate / SK_bytes_phase0a <= 1.10
GB_bytes_candidate / GB_bytes_phase0a <= 1.10
正确性契约通过
```

### Phase 2：feature A/B

构建：

```text
A default
B zlib
C zmij
D zlib + zmij
```

只保留满足第 7.3 节门槛的最小组合。

### Phase 3：自适应 low-memory

- 大于等于 5,000,000 槽位的 Sheet 使用 LowMemory；
- 其他 Sheet 使用 Standard；
- 落地 TempWorkspace、磁盘空间检查、清理和错误详情。
- 在 LowMemory 激活后，用相同 LowMemory 配置重新验证 Phase 2 保留 feature 的增量收益；`zlib` 仍须达到 15%，`zmij` 仍须达到 3%，否则移除后再跑本阶段硬门槛。

硬门槛：

```text
SK PWS median <= 2.0 GiB
GB_wall_candidate / GB_wall_phase0a <= 1.05
GB_pws_candidate / GB_pws_phase0a <= 1.05
SK_bytes_candidate / SK_bytes_phase0a <= 1.10
GB_bytes_candidate / GB_bytes_phase0a <= 1.10
正确性契约通过
```

### Phase 4：删除 ingest 中间整表副本

Phase 4 是已批准路线中的强制 A/B 步骤：即使 Phase 3 已达到最终门槛，也必须制作并测量去副本候选；候选不达保留条件时回退 Phase 4 改动，再用 Phase 3 候选进入 Phase 5。

收益条件为：

```text
(SK_ingest_phase4 / SK_ingest_phase3 <= 0.90
 或 SK_pws_phase4 / SK_pws_phase3 <= 0.90)
且 SK_wall_phase4 / SK_wall_phase3 <= 1.00
```

同时必须满足：

```text
GB_wall_phase4 / GB_wall_phase0a <= 1.05
GB_pws_phase4 / GB_pws_phase0a <= 1.05
SK_bytes_phase4 / SK_bytes_phase0a <= 1.10
GB_bytes_phase4 / GB_bytes_phase0a <= 1.10
GB/SK reader snapshot、行数、runtime、quality、error-log、workbook 契约全部通过
GB/SK 正式计时轮次全部成功
```

GB ingest 和 PWS 也保存原始值；任一指标相对 Phase 3 回退超过 5%时拒绝 Phase 4 候选。

### Phase 5：最终门禁

Phase 0→4 的批准路线全部完成并做出保留/回退决定后，首次满足全部最终硬门槛即停止，不继续 speculative optimization。

失败分流：

| 失败项 | 下一步 |
|---|---|
| 时间失败、PWS 通过 | 重新 profile，只选择一个最大 CPU 热点 |
| PWS 超过 2.0 GiB | 单独设计直接行流式，不自动进入 |
| 文件增长超过 10% | 拒绝 inline strings/相关候选 |
| GB 回退超过 5% | 调整数据规模阈值或回退小文件不利 feature |
| workbook/runtime 不一致 | 先修 correctness，不讨论性能 |
| native DLL 检查失败 | 移除 zlib，回到纯 Rust 最佳候选 |

## 9. 错误处理与文件状态

### 9.1 状态流

```text
CLI 接受参数后立即生成 request_id
→ CLI 前置校验输入、输出路径（含输入输出同路径）
→ writer 校验输出路径
→ 确保父目录存在
→ 规划 Sheet 模式
→ 若存在 LowMemory Sheet，检查 1 GiB 可用空间
→ 创建可选 TempWorkspace
→ populate workbook
→ create_new(true) 创建最终文件
→ save_to_writer + flush
→ 关闭最终文件并验证 size > 0
→ drop workbook
→ 显式清理 TempWorkspace
→ 返回成功报告
```

不增加 `sync_all()` 的断电持久化语义；成功定义为 writer 完成、文件句柄关闭并可读取非零 metadata。

### 9.2 错误阶段

内部 `WriterStage` 至少包括：

```text
PrepareOutputDirectory
CheckDiskSpace
CreateTempWorkspace
PlanSheet
PopulateWorkbook
CreateFinalOutput
SaveWorkbook
RemovePartialOutput
CleanupTempWorkspace
ReadOutputMetadata
```

### 9.3 对外错误码

`INSUFFICIENT_DISK_SPACE` 和 `TEMP_CLEANUP_FAILED` 作为新的稳定 `ErrorCode` 加入 `costing-core`；其余复用现有错误码。逐阶段映射如下：

| stage / 场景 | code | retryable | 最终输出处理 |
|---|---|---:|---|
| `ValidateCliRequest` / 输入输出相同 | `INVALID_INPUT` | false | CLI 前置拒绝，不创建、不删除 |
| `PrepareOutputDirectory` / 已有最终输出 | `OUTPUT_EXISTS` | false | 不创建、不删除 |
| `PrepareOutputDirectory` / 父目录创建失败 | `OUTPUT_NOT_WRITABLE` | 见固定 I/O 映射 | 不创建、不删除 |
| `PlanSheet` / payload 结构不合法或列数越界 | `INTERNAL_ERROR` | false | 不创建、不删除 |
| `CheckDiskSpace` / 可用空间不足 | `INSUFFICIENT_DISK_SPACE` | true | 不创建、不删除 |
| `CheckDiskSpace` / 容量查询失败 | `OUTPUT_NOT_WRITABLE` | 见固定 I/O 映射 | 不创建、不删除 |
| `CreateTempWorkspace` / 创建失败 | `OUTPUT_NOT_WRITABLE` | 见固定 I/O 映射 | 不创建、不删除 |
| `PopulateWorkbook` / low-memory 临时 I/O 失败 | `OUTPUT_NOT_WRITABLE` | 见固定 I/O 映射 | 不创建、不删除 |
| `PopulateWorkbook` / 非 I/O writer 错误 | `INTERNAL_ERROR` | false | 不创建、不删除 |
| `CreateFinalOutput` / 竞态同名文件 | `OUTPUT_EXISTS` | false | 不删除同名文件 |
| `CreateFinalOutput` / 其他创建失败 | `OUTPUT_NOT_WRITABLE` | 见固定 I/O 映射 | 未取得所有权，不删除 |
| `SaveWorkbook` / 底层 I/O 失败 | `OUTPUT_NOT_WRITABLE` | 见固定 I/O 映射 | 删除本次残缺输出 |
| `SaveWorkbook` / 非 I/O XLSX 错误 | `INTERNAL_ERROR` | false | 删除本次残缺输出 |
| `ReadOutputMetadata` / metadata I/O 失败 | `OUTPUT_NOT_WRITABLE` | 见固定 I/O 映射 | 删除本次残缺输出 |
| `ReadOutputMetadata` / 文件为零字节 | `WORKBOOK_MISMATCH` | false | 删除本次残缺输出 |
| 仅 `CleanupTempWorkspace` 失败，最终文件已完成 | `TEMP_CLEANUP_FAILED` | false | 保留最终文件 |
| `RemovePartialOutput` 失败 | 保留原主错误码 | 保留原值 | 追加 cleanup failure |

I/O `retryable` 必须由单一纯函数映射，不能留给运行时自由判断：

```text
true  = ErrorKind::{Interrupted, WouldBlock, TimedOut}
        或 Windows raw_os_error ∈ {32, 33, 39, 112}
        # sharing violation、lock violation、handle disk full、disk full
false = AlreadyExists、PermissionDenied、InvalidInput、InvalidData、NotFound
        以及其他未列明错误
```

`rust_xlsxwriter::XlsxError` 只有能提取到上述 `std::io::Error` 时才沿用该映射；其余 writer 错误一律 `retryable=false`。

### 9.4 结构化详情

`request_id` 在 CLI 接受参数后、首个可失败的校验或 I/O 前生成。输入输出同路径继续由现有 CLI 校验负责，错误详情使用 `stage=ValidateCliRequest`；writer 不为此引入 `input_path` 耦合。随后通过 `WriterContext` 把 `request_id` 传入所有 writer 阶段。writer 错误 JSON 保持 `code/message/retryable`，并增加：

```json
{
  "request_id": "costing-...",
  "details": {
    "stage": "SaveWorkbook",
    "path": "D:\\...\\输出.xlsx",
    "final_output_valid": false,
    "partial_output_removed": null,
    "cleanup_failures": []
  }
}
```

`final_output_valid` 仅在 `save_to_writer` 成功、flush、关闭文件且 metadata 非零后变为 `true`。`partial_output_removed` 在本次运行从未创建最终文件时为 `null`；尝试删除本次残缺文件后为 `true/false`。

### 9.5 错误优先级

- 主写出失败、清理成功：返回主错误；
- 主写出失败、清理也失败：主错误码不变，清理失败追加到 `cleanup_failures`；
- 最终 workbook 有效、仅临时清理失败：保留有效 workbook，返回 `TEMP_CLEANUP_FAILED`，明确 `final_output_valid=true`；
- 只有 `create_new(true)` 已由本次运行成功创建的残缺输出才允许删除；预检查后的竞态同名文件不得删除；
- 不自动降级到 standard writer；
- 不自动重跑；
- 不删除已有业务输出；
- 不静默吞掉清理失败。

失败清理必须按所有权状态执行：`NotCreated` 不调用删除，`CreatedByCurrentRun` 只删除本次残缺文件，`CompletedByCurrentRun` 保留最终文件。状态只能在 `create_new(true)` 成功返回本次文件句柄后从 `NotCreated` 前进，不能根据“路径当前存在”倒推所有权。

## 10. 基准协议

### 10.1 固定样本 manifest

Manifest 只保存 SHA、计数和维度，不保存 ERP 内容。

当前 SK 固定证据：

```json
{
  "input_sha256": "6eac3c6c9ea0eb3e98ca11fb3829914be63e932595b3e3c613f0da46b385d64f",
  "reader_rows": 467420,
  "detail_rows": 425459,
  "qty_rows": 11239,
  "qty_sheet_rows": 11239,
  "quality_metric_count": 10,
  "work_order_rows": 11239,
  "analysis_sheet_rows": 55,
  "error_log_count": 201815,
  "sheet_dimensions": ["A1:AE425460", "A1:AZ11240", "A1:AK56"]
}
```

`work_order_rows` 是现有 runtime 字段，表示白名单过滤前的工单池；`analysis_sheet_rows` 才是第三张 Sheet 的实际数据行数，两者不得混用。

GB 已知输入 SHA 为：

```text
6aa5e3e7fdc547ebaaef968eb5b95d4d630c4ec9915184f94346f60687b8e7ee
```

GB 的正式 normal-mode 输出大小、Sheet 维度、runtime/error counts 和逐轮数据由 Phase 0A 生成，写入第 8 节指定的版本化基线文件。用户确认并提交该文件是 Phase 1 的阻塞检查点；确认后任何候选都不得覆盖或重算它。

### 10.2 时间轮次

```text
预热：1
正式：5
统计：保存原始值，使用 median
```

每个候选批次都重新运行已冻结 SHA 的 reference EXE，并按 reference/current 成对交替顺序采样，减少温度和缓存偏差。GB wall-clock/PWS 回退比使用该批次 reference 中位数作分母；不得用历史慢值或其他批次挑选分母。正式 EXE 直接运行，不通过 `cargo run`。

同批 reference 中位数若相对 Phase 0A manifest 的校准中位数偏离超过 10%，整批标记 `ENVIRONMENT_DRIFT` 并作废；先恢复机器状态，再重采 reference/current，不能移动或重写已批准基线。

本设计中的正式样本数记为 `N`：默认 `N=5`；若触发第 10.5 节临界噪声规则，则追加五轮并令 `N=10`。所有写作“5 轮 median”的目标，机械执行时均解释为“`N` 轮 median”。

### 10.3 PWS

- 与时间基准分开；
- 50ms 轮询；
- 1 次预热 + 默认 5 次正式；触发第 10.5 节时追加为 10 次正式；
- SK median `<= 2,147,483,648 bytes`；
- GB current/baseline `<=1.05`。

### 10.4 输出目录

性能轮次输出到：

```text
data/processed/<pipeline>/.perf-runs/<binary-sha>/<round>/
```

语义比较和元数据采集完成后删除 workbook，只保留不含 ERP 数据的 JSON 证据。

### 10.5 噪声规则

1. 第一组五轮是正式证据；
2. 对任一上限型比例，先写成 `measured / limit`；满足 `abs(measured / limit - 1) <= 0.03` 时允许再采五轮；绝对秒数上限也用同一归一化公式。任一 time/PWS 指标触发后，该候选的时间和 PWS 两套证据都补足到 `N=10`；输出字节数是确定性元数据，不单独触发扩样；
3. 第二组追加五个 reference/current 配对，不得替换第一组；
4. 分别合并 reference 十轮和 current 十轮计算中位数；
5. 十轮中位数通过才算通过；
6. 两组方向冲突则为 `INCONCLUSIVE`；
7. 输入 SHA、binary SHA、Git diff 或机器状态变化时整组作废；
8. correctness 失败时停止性能判定；
9. 未追加时 reference/current 各要求 5/5 正式运行成功；追加后各要求 10/10 成功，candidate 任一正式轮失败即拒绝候选，reference 失败则整批证据无效；
10. 最小收益门槛先转为上限比例再使用同一公式，例如“至少下降 10%”写为 `candidate / baseline <= 0.90`。

## 11. 测试设计

### 11.1 Planner

覆盖：

```text
0 槽位 → Standard
4,999,999 槽位 → Standard
5,000,000 槽位 → LowMemory
乘法溢出 → saturating 到 LowMemory，不 panic
空列 / 空行 → Standard
```

### 11.2 ColumnWritePlan

- 每列一个数字格式；
- 文字/数字/日期/空白单元格；
- Decimal 写为 Excel number；
- 数字格式和文本对齐；
- Blank 不写 cell；
- 中文列名不进入逐格查找。

### 11.3 Standard/LowMemory 一致性

同一 payload 两种模式比较：

- Sheet 顺序；
- 单元格值和类型；
- number format；
- 数据样式；
- 列宽；
- freeze panes；
- autofilter；
- shared strings；
- Decimal、日期和空白；
- 输出拒绝覆盖。

不比较 ZIP 二进制。

### 11.4 TempWorkspace

- 输出父目录内创建；
- 前缀正确；
- 并发不碰撞；
- 临时目录名包含当前 `request_id` 和随机后缀；
- 成功、populate 失败、save 失败都清理；
- 残缺最终输出删除；
- `create_new` 因竞态同名文件失败时不删除该文件；
- 最终有效但 temp 清理失败时保留最终输出；
- 主错误不被清理错误覆盖；
- `NotCreated/CreatedByCurrentRun/CompletedByCurrentRun` 的删除决策逐一覆盖；
- I/O `ErrorKind` 与 Windows raw OS error 的 `retryable` 表驱动测试；
- 无 LowMemory Sheet 时不做 1 GiB 容量门禁，有 LowMemory Sheet 时做边界值测试。

磁盘空间边界和错误合并逻辑拆成纯函数测试，不真的填满磁盘。

### 11.5 Reader

- 双层表头及前置说明行；
- 找不到表头/少于两行；
- 后部空白；
- String/Float/Int/Bool/DateTime/Error；
- 空字符串、整数 float、科学计数法、非有限 float；
- 中文文本；
- reader row count 和 snapshot；
- 真实 GB/SK Python oracle。

### 11.6 CLI

正常 benchmark：

- `request_id`；
- `output_written=true`；
- `output_size_bytes`；
- `writer_populate`；
- `xlsx_save`；
- `export`；
- 现有 payload `total`。

check-only benchmark：

- `output_written=false`；
- `output_size_bytes=null`；
- 不包含 writer/export stages；
- 保持 payload `total`。

错误 JSON 覆盖稳定字段和 writer details；另测 CLI 在输出路径校验失败前已生成 `request_id`，以及 9.3 节每个 stage/code/retryable 映射。

## 12. Windows 单 EXE 验证

最终 release EXE 使用 MSVC `dumpbin /DEPENDENTS` 和 `dumpbin /IMPORTS` 检查普通及 delay-load imports；可用时再以 `llvm-readobj --coff-imports` 交叉验证。保存命令原文、工具版本、EXE SHA 和完整输出，不只保存人工结论。

判定规则：

```text
不得出现 basename 匹配 (?i)(zlib|libz|deflate).*\.dll 的 import
不得出现项目私有 DLL
不得出现相对 Phase 0A 新增的非 Windows / 非已批准 Microsoft runtime DLL
发布目录只包含 EXE，不随附任何运行时 DLL
```

再复制到不安装 Rust、Cargo、Python 和 zlib 的干净 Windows 10/11 x64 环境。测试包只包含 candidate EXE、脱敏输入和必要配置，不复制任何运行时 DLL，并以脱敏 fixture 运行正常模式。

Smoke 必须证明：

- 无缺失 DLL；
- 三张 Sheet 正确；
- `%TEMP%` 无 ERP 临时文件；
- 输出目录无残留 `.costing-tmp-*`；
- 单 EXE 成功退出；
- 保存 smoke 命令、EXE SHA、JSON 摘要和 PE 依赖输出作为发布证据。

## 13. 最终验收矩阵

| 项目 | 硬门槛 |
|---|---|
| SK external wall-clock | `N` 轮 median `<=20.0s` |
| SK normal PWS | `N` 轮 median `<=2.0 GiB` |
| GB external wall-clock | `GB_candidate / GB_phase0a <=1.05` |
| GB PWS | `GB_candidate / GB_phase0a <=1.05` |
| SK output bytes | `SK_candidate / SK_phase0a <=1.10` |
| GB output bytes | `GB_candidate / GB_phase0a <=1.10` |
| 正式运行 | GB/SK 的时间套件和独立 PWS 套件均 `N/N` 成功；`N` 按第 10.5 节取 5 或 10 |
| 输入 | SHA 等于批准 manifest |
| runtime/error counts | 等于批准 manifest |
| workbook | 所有语义比较通过 |
| temp workspace | 每轮结束后不存在 |
| 发布 | 单 EXE，无第三方 zlib DLL |
| 非目标 stage | 相对该阶段输入候选的 ratio `<=1.05`；临界重采后仍超限则拒绝候选 |

表中的 GB wall/PWS `phase0a` 分母是同批固定 reference binary；SK/GB bytes `phase0a` 分母是版本化 manifest 的固定字节数。两类分母都绑定同一个 Phase 0A binary SHA。

## 14. 伪代码草案

### 14.1 持续优化控制流

```python
# [伪代码草案]
# 目标：按批准阶段每次只验证一个性能变量；Phase 4 A/B 完成后，最终候选过门即停止
# 输入：固定 GB/SK 样本、baseline EXE、candidate EXE、批准门槛
# 输出：validated_result / rejected_candidate / next_profile_hotspot

def optimize_until_gate_passes():
    baseline = capture_phase0_baseline()
    require_user_approved_versioned_manifest(baseline)

    instrumented = add_writer_observability_only()
    require_no_instrumentation_regression(baseline, instrumented, max_ratio=1.02)

    writer_candidate = add_column_write_plan(instrumented)
    candidate = keep_phase1_only_if_benefit_and_guards_pass(
        base=instrumented,
        proposed=writer_candidate,
        minimum_populate_drop=0.10,
        frozen_reference=baseline,
    )

    feature_candidates = build_release_variants(
        candidate,
        variants=("default", "zlib", "zmij", "zlib+zmij"),
    )
    candidate = select_smallest_valid_feature_set(
        feature_candidates,
        frozen_reference=baseline,
    )

    low_memory_candidate = add_adaptive_low_memory(
        candidate,
        cell_slot_threshold=5_000_000,
        temp_space_required_bytes=1 * GiB,
    )
    candidate = require_all_phase3_guards(
        low_memory_candidate,
        frozen_reference=baseline,
    )

    # Phase 4 是批准路线中的强制 A/B；即使 Phase 3 已达硬门槛也要测量。
    ingest_candidate = remove_reader_intermediate_copy(candidate)
    candidate = keep_phase4_only_if_benefit_and_guards_pass(
        base=candidate,
        proposed=ingest_candidate,
    )

    if final_gate_passes(candidate):
        return validated_result(candidate)

    # 为什么停止自动优化：直接行流式会改变核心模块边界，
    # 必须由新的 profile 证据和单独批准的设计驱动。
    return next_profile_hotspot(profile_largest_remaining_cost(candidate))
```

### 14.2 安全写出

```rust
// [伪代码草案]
// 输入：含 request_id 的 WriterContext、不允许覆盖的输出路径、完整 WorkbookPayload、写出选项
// 输出：成功写出报告；失败时返回主错误和清理详情

fn write_workbook_safely(
    context: &WriterContext,
    output_path: &Path,
    payload: &WorkbookPayload,
    options: &WorkbookWriteOptions,
) -> Result<WorkbookWriteReport, WriterError> {
    validate_output_path(output_path)?;
    ensure_parent_directory(output_path)?;

    let plans = plan_all_sheets(payload, options)?;
    let needs_temp = plans.iter().any(|plan| plan.mode == LowMemory);

    if needs_temp {
        ensure_available_space(output_path.parent(), ONE_GIB)?;
    }

    let temp = if needs_temp {
        Some(TempWorkspace::create_in(
            output_path.parent(),
            &context.request_id,
        )?)
    } else {
        None
    };

    // 只有本次运行真正创建的残缺输出才允许失败清理删除。
    // 这避免 validate 与 create_new 之间出现同名文件时误删他人结果。
    let mut output_state = OutputArtifactState::NotCreated;

    // 从 Workbook 创建前计时，覆盖正文定义的 workbook/sheet 创建和 populate。
    let populate_started = Instant::now();
    let mut workbook = match create_workbook(temp.as_ref()) {
        Ok(workbook) => workbook,
        Err(primary) => {
            return Err(cleanup_after_failed_write(
                primary,
                output_path,
                output_state,
                temp,
            ));
        }
    };

    // 为什么把所有可能失败的写出步骤包在同一结果中：
    // populate、create_new、save、flush 或 metadata 任一步失败，都必须先释放
    // workbook/file 句柄，再按同一优先级规则清理残缺输出和临时目录。
    let write_result = (|| -> Result<WorkbookWriteReport, WriterError> {
        populate_all_sheets(&mut workbook, payload, &plans)?;
        let writer_populate_seconds = populate_started.elapsed().as_secs_f64();

        let mut final_file = create_new_output(output_path)?;
        output_state = OutputArtifactState::CreatedByCurrentRun;
        let save_started = Instant::now();
        workbook.save_to_writer(&mut final_file)?;
        final_file.flush()?;
        let xlsx_save_seconds = save_started.elapsed().as_secs_f64();
        drop(final_file);

        let output_size_bytes = read_nonzero_output_size(output_path)?;
        output_state = OutputArtifactState::CompletedByCurrentRun;
        Ok(WorkbookWriteReport {
            writer_populate_seconds,
            xlsx_save_seconds,
            output_size_bytes,
        })
    })();

    // low-memory worksheet 可能仍持有临时文件句柄，必须先 drop workbook。
    drop(workbook);

    match write_result {
        Err(primary) => Err(cleanup_after_failed_write(
            primary,
            output_path,
            output_state,
            temp,
        )),
        Ok(report) => {
            if let Err(cleanup_error) = close_temp_workspace(temp) {
                // 最终 workbook 已关闭且验证为非零文件；清理失败不能删除有效业务输出。
                return Err(temp_cleanup_error_with_valid_output(
                    cleanup_error,
                    output_path,
                    report.output_size_bytes,
                ));
            }
            Ok(report)
        }
    }
}
```

### 14.3 ingest 去副本

```rust
// [伪代码草案]
// 输入：Calamine Range<Data>
// 输出：RawWorkbook；不再构造中间 Vec<Vec<Data>>

fn normalize_range_directly(
    range: Range<Data>,
    sheet_name: String,
) -> Result<RawWorkbook, CostingXlsxError> {
    let header_start = find_header_start_in_range(&range)
        .ok_or_else(missing_header_error)?;

    let width = range.width();
    let header_rows = normalize_two_header_rows(&range, header_start, width)?;

    let rows = range
        .rows()
        .skip(header_start + 2)
        .map(|row| normalize_data_row(row, width))
        .collect();

    Ok(RawWorkbook {
        sheet_name,
        header_rows,
        rows,
    })
}
```

## 15. 验证命令

每阶段至少运行：

```powershell
cargo test --locked --manifest-path rust/Cargo.toml -p costing-xlsx
cargo test --locked --manifest-path rust/Cargo.toml -p costing-calculate
cargo test --locked --manifest-path rust/Cargo.toml
cargo fmt --manifest-path rust/Cargo.toml --all --check

uv run python -m pytest tests/rust_oracle -q --basetemp .pytest-tmp/rust-oracle
uv run python -m pytest tests/contracts tests/architecture -q --basetemp .pytest-tmp/contracts
uv run python -m ruff check src tests
uv run python -m ruff format src tests --check
```

Phase 2 的四个 release 候选分别构建；每次构建后立即按 binary SHA 复制到独立证据目录，禁止用后一次构建覆盖前一候选后仍沿用旧标签：

```powershell
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --features zlib
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --features zmij
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --features "zlib,zmij"
```

Phase 3/最终候选使用 `--features low-memory` 加上 Phase 2 实际保留的 feature。该组合必须额外运行：

```powershell
cargo test --release --locked --manifest-path rust/Cargo.toml -p costing-xlsx --features low-memory
cargo test --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --features low-memory
cargo tree --locked --manifest-path rust/Cargo.toml -p costing-calculate -e features --features low-memory
```

若保留 `zlib` 或 `zmij`，把它们追加到上述 `--features`，并保存每个候选的 `cargo tree -e features` 输出，证明实际解析的 `constant_memory/zlib/zmij` feature 图与标签一致。

最终再运行真实 GB/SK wall-clock、PWS、output size、workbook oracle 和单 EXE smoke。

## 16. 审查分工

- `data_auditor`：核对原始轮次、SHA、median、门槛和是否挑选结果；
- `security_reviewer`：核对敏感临时目录、路径暴露和清理；
- `ops_reviewer`：核对 Windows、MSVC、磁盘空间、PE imports 和单 EXE；
- `doc_reviewer`：只读审查 README、验证文档和设计一致性；
- `doc-updater`：仅在明确需要同步命令或配置说明时做最小修改。

## 17. 后续流程

已逐节批准的设计整理并提交为本书面规格后：

1. 用户审阅提交后的设计文件；
2. 用户明确批准该文件；
3. 仅调用 `superpowers:writing-plans` 生成小提交实施计划；
4. 不在 writing-plans 之前修改生产代码；
5. 实施按 Phase 0→5 顺序，每个阶段独立验证和提交；
6. 达到全部硬门槛后立即停止。

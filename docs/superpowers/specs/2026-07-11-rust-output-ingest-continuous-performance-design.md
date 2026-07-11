# Rust 输出与读取持续性能优化设计

**状态：** 修订后的书面规格已于 2026-07-11 获用户批准；尚未进入实现
**日期：** 2026-07-11
**适用仓库：** `D:\python_program\02--costing_calculate`
**目标实现：** Rust CLI `costing-calculate`
**批准路线：** 精确 revision 的受控 rust_xlsxwriter fork + 自适应 low-memory writer + writer 热路径优化 + ingest 去中间副本

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

`N` 默认等于 5；一旦触发第 10.5 节的临界噪声规则，必须追加为 10，不得根据首组通过或失败选择是否扩样。`N` 对同一候选批次全局生效：任一 time/PWS 门槛触发追加后，时间套件和独立 PWS 套件都补足到 10 轮，所有成功率和中位数都按完整 10 轮计算。

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

### 2.6 rust_xlsxwriter 0.96.0 的系统临时目录冲突

当前锁定依赖：

```text
name = rust_xlsxwriter
version = 0.96.0
crates.io checksum = dd1746025420e17b5d62528b930e550e016e857038794d74e169018126ef3d14
```

启用 `constant_memory` feature 后，`Worksheet::new()` 无运行时模式判断，直接执行：

```rust
let file_writer = BufWriter::new(tempfile_in(std::env::temp_dir()).unwrap());
```

因此：

```text
Standard Sheet：Worksheet::new → 访问系统临时目录，writer 最终不用
LowMemory Sheet：Worksheet::new → 先访问系统临时目录
                 → 再按 Workbook::set_tempdir 替换为自定义目录 writer
```

`set_tempdir()` 只能验证并记录自定义目录，不能阻止首次 `std::env::temp_dir()` 访问。原版 0.96.0 无法同时满足“启用 LowMemory”和“完全不访问 `%TEMP%`”。

### 2.7 rust_xlsxwriter 0.96.0 的不可恢复临时 I/O

LowMemory 行刷新链当前不返回 `Result`：

```text
insert_cell()      -> ()
flush_to_row()     -> ()
flush_data_row()   -> ()
flush_last_row()   -> ()
```

临时 XML 写入大量使用 `.expect(XML_WRITE_ERROR)`，最终 `rewind()` 使用 `.unwrap()`。磁盘写满、权限变化或句柄异常可能直接 panic，原始 `std::io::Error` 无法进入 `XlsxError::IoError`。

外围 `catch_unwind` 只允许作为 `UNKNOWN_PANIC + best-effort cleanup` 保险；它不能恢复稳定的 `ErrorKind`、`raw_os_error()` 或错误链，不能用来兑现结构化 LowMemory Error Model。

### 2.8 当前 benchmark/oracle 前置能力不足

- PWS 脚本已有单组 AB/BA、median、非零退出和 SHA 防漂移，但固定 `N=5` 且硬编码 `--check-only`；
- normal benchmark 默认三轮 Python→Rust，不是固定 Rust reference/current；
- 单次 normal evidence capture 不是成对统计 harness；
- workbook comparator 对全部数值使用统一 `1e-6` 容差，数据样式按列样式集合而非坐标比较，未检查 sharedStrings package 契约；
- 当前 evidence schema 会保留绝对路径、ERP 文件名、`workbook_path` 和真实命令参数；mismatch 文本还包含真实 `expected/actual` 值。

所以 Phase 0A 之前必须先完成依赖补丁和 Phase 0H 工具链前置工作，不能直接采集正式基线。

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
- 基于官方 `v0.96.0` 建立精确 revision 的受控 Git fork，修补临时文件位置和临时 I/O Result 传播；
- 建立 Phase 0H normal wall/PWS 成对 benchmark harness；
- 加强 workbook oracle、Reader 不变量和版本化证据脱敏；

### 3.2 非目标

- 不改变 GB/SK 业务规则；
- 不改变三张 Sheet 的名称、顺序、列顺序和样式；
- 不改变白名单、Modified Z-score、成本分类或勾稽口径；
- 不把 `Decimal` 提前改为 `f64`；
- 不更换 Calamine，不更换 XLSX library；rust_xlsxwriter 仅允许基于官方 `v0.96.0` 的批准补丁；
- 不升级 rust_xlsxwriter 上游版本；
- 不向 rust_xlsxwriter 上游创建或提交 PR；
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

依赖补丁约束：

```text
受控 fork：https://github.com/Aspirin86942/rust_xlsxwriter.git
上游仓库：https://github.com/jmcnamara/rust_xlsxwriter.git
上游 tag：v0.96.0
上游基线 revision：9134de25afadaee955d0f821862338e3d046a338
crates.io 0.96.0 checksum：dd1746025420e17b5d62528b930e550e016e857038794d74e169018126ef3d14
依赖固定：完整 40 位 commit revision
上游协作：不创建、不提交、不等待 PR
自动同步：禁止
```

`upstream_base_revision` 是官方轻量 tag `v0.96.0` 当前对应的完整 commit，是不可移动的审计起点；实施时若官方 tag 不再解析到该 SHA，必须 fail closed，不得更换基线。fork revision 是 Phase -1D 的可验证输出：生成后必须同时写入 Cargo manifest、`Cargo.lock` 和无敏感信息的依赖证据 manifest；三处 SHA 不一致时 fail closed。

### 4.2 临时文件

- 只允许写入最终输出目录下的运行级受控临时目录；
- 不使用系统 `%TEMP%`；
- 目录形态为 `.costing-tmp-<request_id>-<random>`，既可审计又避免并发碰撞；
- 成功和失败路径都必须显式清理；
- 清理失败不得静默忽略；
- 仅当至少一张 Sheet 使用 LowMemory 时，输出卷才要求至少 1 GiB 可用空间；
- 临时目录继承输出父目录 ACL；
- 临时目录和性能输出目录不得进入原始输入文件扫描。
- 启用 `constant_memory` 后，Standard Sheet 不得创建任何临时文件；
- LowMemory Sheet 从第一次临时文件创建起只能访问本次运行的受控目录；
- 以上两项必须由 fork 的故障注入测试证明，不能只根据 `set_tempdir()` 调用推断。

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
  ├─ PatchedLowMemoryBackend
  │    ├─ Standard Sheet 不创建临时文件
  │    ├─ LowMemory writer 延迟且只在受控目录创建
  │    └─ 行刷新 / rewind / copy 全链路返回 Result
  │
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

Phase0H benchmark harness
  ├─ NormalWallRunner
  ├─ NormalPwsRunner（PeakWorkingSet64）
  ├─ WorkbookOracle
  ├─ EvidenceSanitizer
  └─ BatchVerdict
```

外部 benchmark harness 负责真实进程 wall-clock 和 PWS，不把 CLI 内部计时冒充为端到端时间。

## 6. 组件设计

### 6.1 `PatchedLowMemoryBackend`

fork 保持官方 `v0.96.0` 的公开 API 和 XLSX 语义，只修复 `constant_memory` 下 Standard / ConstantMemory / LowMemory 的临时文件创建和临时 I/O：

```text
Worksheet::new
  → 不创建临时文件

Workbook::{add_worksheet_with_constant_memory,
          add_worksheet_with_low_memory,
          new_worksheet_with_constant_memory,
          new_worksheet_with_low_memory}
  → 四个 public factory 都只传递 mode 和 optional tempdir
  → 不创建 tempfile，不使用 unwrap/expect

第一次真实 row flush / save 空或单行 Sheet
  → ensure_temp_writer() -> Result
  → tempdir 已设置时只能使用该目录
```

Result 传播边界：

```text
store_* -> insert_cell?
       -> flush_to_row?
       -> flush_data_row?
       -> fallible row/cell XML serializer?

packager -> flush_last_row?
         -> rewind?
         -> copy?
```

项目适配层也必须保留错误源：

```text
rust_xlsxwriter::XlsxError::IoError(std::io::Error)
→ CostingXlsxError::Writer(#[source] XlsxError)
→ WriterError { context, primary }
→ map_xlsx_write_error 移动原始 std::io::Error
→ CostingError::Contextual { context, source: IoSource { #[source] io, ... } }
→ ErrorSummary { request_id, details, ... }
→ CLI JSON
```

错误承载结构固定为：

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IoFailureMeta {
    kind: std::io::ErrorKind,
    raw_os_error: Option<i32>,
}

struct ErrorDetails {
    stage: ErrorStage,
    path: Option<PathBuf>,
    io_meta: Option<IoFailureMeta>,
    final_output_valid: bool,
    partial_output_removed: Option<bool>,
    cleanup_failures: Vec<CleanupFailureMeta>,
}

struct ErrorContext {
    request_id: String,
    details: ErrorDetails,
}

enum CostingXlsxError {
    Writer(#[source] rust_xlsxwriter::XlsxError),
    /* existing reader/contract variants */
}

enum WriterPrimaryError {
    Io(#[source] std::io::Error),
    Xlsx(#[source] CostingXlsxError),
    Contract(String),
}

struct WriterError {
    context: ErrorContext,
    #[source]
    primary: WriterPrimaryError,
}

enum CostingError {
    // 现有 User/Io/Internal 变体和构造函数保持，避免修改所有业务调用点。
    User { /* existing fields */ },
    Io { /* existing fields */ },
    Internal { /* existing fields */ },
    IoSource {
        code: ErrorCode,
        message: String,
        retryable: bool,
        io_meta: IoFailureMeta,
        #[source]
        source: std::io::Error,
    },
    Writer {
        code: ErrorCode,
        message: String,
        retryable: bool,
    },
    Contextual {
        context: ErrorContext,
        #[source]
        source: Box<CostingError>,
    },
}

struct ErrorSummary {
    status: String,
    code: ErrorCode,
    message: String,
    retryable: bool,
    request_id: Option<String>,
    details: Option<ErrorDetails>,
}
```

`ErrorStage`、`IoFailureMeta`、`CleanupFailureMeta`、`ErrorDetails`、`ErrorContext` 和 `CostingError` 定义在 `costing-core`，保持运输结构不反向依赖 XLSX crate；`CostingXlsxError`、`WriterPrimaryError` 和 `WriterError` 定义在 `costing-xlsx`。`map_xlsx_write_error` 在消费 `WriterError` 时必须对 `WriterPrimaryError::Io` 或 `WriterPrimaryError::Xlsx(CostingXlsxError::Writer(XlsxError::IoError))` 做结构化匹配，先从 `&io_error` 计算 retryable 和 `IoFailureMeta`并写入 `context.details.io_meta`，再把同一个 `std::io::Error` move 进 `CostingError::IoSource`；不 clone、不格式化、不解析 message。非 I/O writer 错误按闭合变体分类后进入 `CostingError::Writer`。最后将两者放入 `CostingError::Contextual { context, source }`：两类错误都保留结构化上下文；只对本 P0 要求的 I/O 分支承诺底层 `std::io::Error` source chain 不丢失。非 I/O `XlsxError` 不承诺穿过 core 保留原始 source，也不会用于 `ErrorKind/raw_os_error` 或 retryable 反推。`cleanup_failures` 只在 `context.details` 中结构化追加，不与主错误字符串拼接。

`costing-core/src/model.rs` 中的 `ErrorSummary` 增加可选 `request_id/details`；仅 CLI 参数在运行上下文创建前就无法解析时两者为 `null`。CLI 接受参数并生成 `request_id` 后，`costing-cli/src/run.rs` 在每个阶段边界用 `CostingError::Contextual` 包装原有错误；这样 `fact.rs`、`normalize.rs`、`table.rs` 等业务文件的现有构造器无需逐一修改。`with_context` 对已经是 `Contextual` 的 writer 错误原样返回，不做二次包装或覆盖 cleanup/输出状态。包括 `ValidateCliRequest`、reader/ETL/writer 在内的所有后续失败都必须持有 `ErrorContext`；在尚未进入 writer 的阶段，`final_output_valid=false`、`partial_output_removed=null`、`cleanup_failures=[]`，`path` 按该 stage 有无相关路径取 `Some/None`。`CostingError::code/message/retryable` 存取器递归委托给 `Contextual.source`；`ErrorSummary` 从最外层 context 直接 clone `request_id/details`，不从 `message`/`source` 反推。`io_meta` 在 JSON `details` 中平铺为闭合 `io_kind` 和 `raw_os_error`；`costing-cli/src/main.rs` 只负责序列化该类型。

当前 `costing-xlsx` writer 中的 `.map_err(|error| Message(error.to_string()))` 和 save/cleanup 文本拼接会丢失原始错误，必须改为上述结构；禁止通过解析错误字符串恢复任何结构化字段。项目侧适配范围明确包含 `costing-xlsx/src/reader.rs`、`costing-xlsx/src/writer.rs`、`costing-core/src/error.rs`、`costing-core/src/model.rs`、`costing-cli/src/run.rs` 和 `costing-cli/src/main.rs`。

只允许修改 fork 的：

- `src/worksheet.rs`；
- `src/workbook.rs`；
- `src/packager.rs`；
- 必要时仅为 fallible row tag 增加 `src/xmlwriter.rs` 的窄接口。

不得把整个 XML writer 重写为新实现，不得改变 Standard workbook 语义，不得顺带合并上游其他改动。fork 仓库不创建上游 PR。

### 6.2 `SheetWritePlanner`

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

### 6.3 `ColumnWritePlan`

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

### 6.4 low-memory 顺序约束

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

### 6.5 `TempWorkspace`

仅当至少一张 Sheet 选择 LowMemory 时创建。

实现要求：

- 使用 `tempfile::Builder::prefix(&format!(".costing-tmp-{request_id}-")).tempdir_in(output_parent)`；
- `request_id` 沿用当前 CLI 运行标识，`tempfile` 追加随机后缀避免并发碰撞；
- `Workbook::set_tempdir()` 必须在添加 low-memory Sheet 前调用，但正确性依赖 fork 的延迟创建和 fallible I/O，不能把该调用本身视为安全证明；
- workbook drop 后显式调用 `TempDir::close()`；
- RAII drop 只作为进程异常时的兜底；
- 显式 close 的错误必须进入 Error Model。

### 6.6 `WorkbookWriteReport`

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

### 6.7 ingest 去中间副本

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
- Header 与 Data 不同的字符串 trim 语义；
- String/Float/Int/Bool/DateTime/DateTimeIso/DurationIso/Error/Empty 语义；
- 科学计数法和非有限 float；
- Decimal 精度；
- reader snapshot；
- 原始行数。

Calamine variant 的不变量：

| variant | Header | Data |
|---|---|---|
| `String` | `trim()` | 纯空白转 `Blank`；非空保留原始首尾空格 |
| `Float` | 保持 `float_text` | 保持 `float_cell_value`、科学计数法和非有限值语义 |
| `Int` | `to_string()` | `Decimal::from` |
| `Bool` | `to_string()` | `Text(to_string())` |
| `DateTime` | `to_string()` | `DateLike(to_string())` |
| `DateTimeIso` | `trim()` | `DateLike`，原字符串不 trim |
| `DurationIso` | `trim()` | `Text`，原字符串不 trim |
| `Error` | debug 文本 | debug 文本 |
| `Empty` | 空 header | `Blank` |

### 6.8 `EvidenceSanitizer`

所有待版本化 evidence artifact 都必须由 `EvidenceSanitizer` 构建或归一化，不仅是 JSON manifest。范围包括 benchmark manifest、command transcript、smoke summary、PE imports、fork provenance、Cargo feature tree 以及 Markdown/文本验证报告。不允许把 CLI payload、dataclass `asdict()`、异常字符串、命令数组或工具原始输出整体透传。

路径只保存别名：

```text
$REPO_ROOT
$GB_INPUT
$SK_INPUT
$REFERENCE_EXE
$CANDIDATE_EXE
$ROUND_OUTPUT
$FORK_CHECKOUT
```

禁止版本化：

- 绝对路径、用户名、hostname；
- ERP basename/stem、`workbook_path`；
- 原始 stdout/stderr；
- 真实命令参数；
- mismatch 的 `expected_value` / `actual_value`。

字段 allowlist 必须同时校验 key、类型和值域，不存在“合法 key 下允许任意字符串”的逃生口：

- `mismatch_kind`、`verdict`、storage type、pipeline、feature set、target 和 stage 只接受闭合 enum；
- `sheet` 只接受三张固定输出 Sheet，`coordinate` 只接受严格 A1 坐标，Sheet dimension 只接受严格 `A1:AZ123` 形式；
- SHA 只接受 64 位小写十六进制，revision 只接受 40 位小写十六进制；
- 路径型值只能等于批准的别名常量；计数、尺寸和 timing 只能是有限非负数；
- 诊断全文、异常 message、原始命令和原始工具输出一律只进入本地未版本化日志。

允许的 mismatch 证据：

```text
sheet
coordinate
mismatch_kind
expected_storage_type
actual_storage_type
local_unversioned_log_sha256
```

待版本化的命令只保存使用上述别名的命令模板。smoke JSON 必须从 CLI payload 中重新构建 allowlist summary；PE 证据只保存工具名/已脱敏版本、EXE SHA、解析后的 import/delay-import basename 闭合列表和 verdict；Cargo tree 只保存归一化的 package/revision/feature 映射。真实值和原始日志只能保存在 Git ignore 或仓库外目录；版本化证据只保存 SHA。每轮 workbook 必须在最外层 `finally` 清理，清理失败时整批 fail closed。

## 7. 依赖决策

### 7.1 现有依赖 feature

对受控 fork 的 feature 评估：

```text
constant_memory：提供 low-memory worksheet/tempdir 能力
zlib：Windows/MSVC 原生压缩候选
zmij：数字转换候选
```

`costing-xlsx` 暴露同名的最小 feature 转发：`low-memory -> rust_xlsxwriter/constant_memory`、`zlib -> rust_xlsxwriter/zlib`、`zmij -> rust_xlsxwriter/zmij`；`costing-calculate` 再转发这三个 feature，确保发布命令只针对 binary package。默认构建不隐式启用候选 feature；Phase 3 最终候选必须显式启用 `low-memory`。

Cargo 依赖源固定为：

```text
git = https://github.com/Aspirin86942/rust_xlsxwriter.git
revision format = 40 位小写十六进制 commit SHA
revision source = Phase -1D 通过故障注入后的 fork HEAD
upstream base revision = 9134de25afadaee955d0f821862338e3d046a338
crates.io checksum = dd1746025420e17b5d62528b930e550e016e857038794d74e169018126ef3d14
```

不允许 branch-only、tag-only、短 SHA 或浮动 Git HEAD。`Cargo.lock` 的 source revision、Cargo manifest 的 `rev` 和依赖证据 manifest 必须完全一致。审计起点只能是官方 `v0.96.0` 对应的上述完整 SHA；tag 仅作人类可读标签，不作实际 diff 分母。不自动同步，也不提交上游 PR。

fork 保留上游 LICENSE 和 provenance。公开 fork 不需要构建凭据；若精确 revision 无法获取，`cargo fetch/build --locked` 必须失败，禁止静默回退 crates.io、其他 branch 或 vendored 副本。

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

共同门禁中的外部 wall-clock、PWS 和输出大小都以固定 Phase 0A reference binary 为基准；feature 专属收益统一按 SK normal mode 判定。B/A 和 C/A 使用同批 A 为分母；D 的 zlib 增量使用同批 C 为分母，D 的 zmij 增量使用同批 B 为分母；Phase 3 重验 feature 收益时使用同批 feature-on/feature-off。GB 只承担 correctness、wall/PWS 和 output bytes 防回退门禁，不得作为 feature 收益的有利样本。wall-clock/PWS 的 `phase0a` 数值指同一批次中与 candidate 交替运行的固定 reference binary 中位数，输出字节数则取已批 manifest 的不可变值。

`zlib`：

- B 的独立收益：`SK_normal_B_xlsx_save / SK_normal_A_xlsx_save <= 0.85`；
- D 中 zlib 的增量收益：`SK_normal_D_xlsx_save / SK_normal_C_xlsx_save <= 0.85`；
- 第 12 节 PE 普通/delay imports 规则通过；
- 干净 Windows 单 EXE smoke 通过。

`zmij`：

- C 的独立收益：`SK_normal_C_writer_populate / SK_normal_A_writer_populate <= 0.97`，或 `SK_normal_C_export / SK_normal_A_export <= 0.97`；
- D 中 zmij 的增量收益：`SK_normal_D_writer_populate / SK_normal_B_writer_populate <= 0.97`，或 `SK_normal_D_export / SK_normal_B_export <= 0.97`；
- 对用于过门的指标，每个五轮正式组至少 4/5 配对轮满足 candidate < 上述指定的同批分母候选；若 `N=10`，两组分别满足；
- 否则移除。

Phase 2 选择算法固定为：

| B 通过 B/A | C 通过 C/A | D 同时通过 D/C、D/B | 选择 |
|---|---|---|---|
| 否 | 否 | 不评估 | A |
| 是 | 否 | 不评估 | B |
| 否 | 是 | 不评估 | C |
| 是 | 是 | 是 | D |
| 是 | 是 | 否 | 同批 B/C 决选 |

规则：

1. A 是无 feature fallback 和收益基准，不因“feature 最少”自动获胜；
2. 只有 B、C 各自通过时才评估 D；
3. D 必须同时满足 `D/C` 的 zlib 增量门槛和 `D/B` 的 zmij 增量门槛；
4. B、C 都通过但 D 失败时，重新做 SK normal B/C 同批 AB/BA；任一 B/C wall 或 PWS 比率进入 3% 区间时，两套证据都必须按全局 round 6–10 强制扩展到 `N=10`，不得根据首组领先者决定是否扩样；
5. 完成所有强制扩样后，令 `r_wall = B_wall_median / C_wall_median`；若 `abs(r_wall - 1.0) > 0.03`，选择 wall median 较小者；
6. 否则令 `r_pws = B_pws_median / C_pws_median`；若 `abs(r_pws - 1.0) > 0.03`，选择 PWS median 较小者；否则固定选择 C（纯 Rust zmij），避免 native compression 构建面；
7. 不允许用另一个 feature 的收益替未达标 feature 过关。

## 8. 执行阶段与停止规则

### Phase -1D：依赖补丁可行性与故障注入门禁

1. 核验官方 `v0.96.0` 解析为 `9134de25afadaee955d0f821862338e3d046a338`，并以该精确 revision 创建 `https://github.com/Aspirin86942/rust_xlsxwriter.git` 受控 fork；
2. 不创建上游 PR，不合并其他上游提交；
3. 只修改第 6.1 节允许的 3–4 个源文件和对应测试；
4. 四个 ConstantMemory/LowMemory public factory 统一只传递 mode/tempdir，并把真实 tempfile 创建收敛到 `ensure_temp_writer() -> Result`；
5. fork CI/本地验证 Standard、四个 factory、LowMemory 空/单行/多行 Sheet、行中途失败、最后一行 flush、rewind、copy 和 tempdir TOCTOU；
6. 项目六个适配文件实现第 6.1 节的 `ErrorContext` / `WriterError` / `CostingError::IoSource` / `ErrorSummary` 结构，删除 writer I/O 的字符串化和 cleanup 文本拼接；
7. 从 fork 到 CLI 的错误必须通过 move 保留同一个 `std::io::Error`、`ErrorKind` 和 `raw_os_error()`，不得 panic；
8. 保存 upstream repository、tag 解析值、精确 upstream base SHA、crates.io checksum、fork revision、允许文件清单和 `<upstream-base-revision>..<fork-revision>` diff SHA；
9. 项目 Cargo manifest、`Cargo.lock` 和依赖证据 manifest 锁定同一完整 revision。

阻断条件：

```text
任一 Standard Sheet 访问临时目录
任一 ConstantMemory/LowMemory public factory 在创建阶段访问 tempdir 或 panic
任一受控 LowMemory 临时 I/O 仍 panic
任一故障注入拿不到原始 io::Error
项目适配层把 XlsxError 字符串化，或 cleanup failure 覆盖/拼接主 source
ErrorContext 经 CostingError/ErrorSummary 后丢失 request_id、stage 或输出所有权状态
fork diff 超出允许文件清单
官方 tag 解析值或 crates.io checksum 与固定值不同
依赖 revision 不一致
```

任一成立都停止主路线，不进入 Phase 0H。

### Phase 0H：Benchmark Harness Prerequisite

在采集基线前，先提交并验证工具链：

- normal wall `N=5/10`；
- normal PWS `N=5/10`，使用 `Process.PeakWorkingSet64`；
- 固定 Rust reference/current 的全局 round 1–10 AB/BA；
- 临界区强制扩样和 `INCONCLUSIVE`；
- 每轮独立输出路径和最外层 `finally` 清理；
- candidate 非零退出直接拒绝，reference 非零退出使整批无效；
- 缺轮、重复轮号、顺序不平衡、输入/binary/Git SHA 变化时 fail closed；
- 加强后的 workbook oracle；
- `EvidenceSanitizer` allowlist 和提交前敏感扫描。

必须先用脱敏 fixture 通过工具单元测试和一次 normal smoke；Phase 0H 不生成正式业务性能结论。

实现优先扩展现有 PWS 脚本中的 AB/BA、median、非零退出和 SHA 前后检查，不重写已经验证的能力；删除硬编码 `--check-only`，把 mode、N 和 global round start 变成显式参数。

### Phase 0A：冻结当前行为基线

固定 reference binary 只包含 Phase -1D 的依赖安全补丁/错误 source 保留，以及 Phase 0H 的测试工具；不包含 Phase 0B 观测、Phase 1 writer 优化、Phase 2 feature、Phase 3 LowMemory 或 Phase 4 Reader 优化。reference 必须以 `--no-default-features` 构建，保证 `low-memory/zlib/zmij` 全部关闭；Phase 0H harness 不链入生产数据路径。

- 构建并冻结当前 release EXE；
- 记录 Git HEAD、working-tree diff id、输入 SHA、binary SHA、fork revision、Windows/CPU/内存/磁盘；
- GB/SK 各 1 次预热 + 5 次 wall-clock；
- 另行执行 1 次预热 + 5 次 PWS；
- 每轮独立输出路径；
- 每轮完成 workbook 语义比较；
- 保存全部原始值。

Phase 0A 结束时生成不含 ERP 内容的版本化证据：

```text
docs/performance/baselines/2026-07-11-windows-x64-phase0a.json
```

其中固定 GB/SK 输入 SHA、reference binary SHA、fork revision、Git HEAD/diff id、runtime/error counts、Sheet 维度、输出字节数、校准用逐轮 wall-clock/PWS 和机器信息。该文件必须先经过 `EvidenceSanitizer` 和敏感扫描，再由用户确认并提交；未确认前不得开始 Phase 1。后续不得替换 reference binary 或改写 manifest；候选阶段仍须重新运行同一个 reference binary，与 candidate 成对交替采样。

### Phase 0B：仅增加观测性

新增 `writer_populate`、`xlsx_save`、`output_size_bytes`，不改变写出策略。

GB/SK 的 `instrumented_wall / same_batch_phase0a_reference_wall` 均不得超过 `1.02`，且正确性必须通过。进入第 10.5 节的 3% 临界区时必须补足为 10 轮，无论首组当前通过还是失败；超出临界区且已失败则直接拒绝，不以重采挽救明显回退。合并证据仍超过时调整计时实现，不进入 Phase 1。

通过后冻结 Phase 0B binary SHA；Phase 1 的 `writer_populate/xlsx_save` 与该 binary 在同一批次交替比较，不能拿不同机器状态下的孤立 JSON 相除。

### Phase 1：writer CPU 热路径

仅增加 blank 快路径、Sheet/ColumnWritePlan 和 Format 复用。本阶段虽然建立 `SheetWritePlan`，但强制所有 Sheet 使用 Standard；`cell_slots` 的模式选择只在 Phase 3 激活。

保留条件：

```text
SK_normal_writer_populate_phase1 / SK_normal_writer_populate_phase0b_same_batch <= 0.90
SK_normal_xlsx_save_phase1 / SK_normal_xlsx_save_phase0b_same_batch <= 1.05
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

严格按第 7.3 节决策表选择 A/B/C/D，不使用“最小组合”自由解释。

### Phase 3：自适应 low-memory

- 大于等于 5,000,000 槽位的 Sheet 使用 LowMemory；
- 其他 Sheet 使用 Standard；
- 落地 TempWorkspace、磁盘空间检查、清理和错误详情。
- 在 LowMemory 激活后，按 SK normal mode 用同批 feature-off/feature-on AB/BA 重新验证 Phase 2 保留 feature 的增量收益；`zlib` 仍须达到 15%，`zmij` 仍须达到 3%，否则移除后再跑本阶段硬门槛。

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

Phase 4 与 Phase 3 的分子/分母必须在同一批次、相同 `N` 和全局 AB/BA 顺序下重跑；禁止读取历史 Phase 3 JSON 作为有利分母。

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

Phase -1D、0H、0A、0B 和 Phase 1→4 全部完成并做出保留/回退决定后，首次满足全部最终硬门槛即停止，不继续 speculative optimization。

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

内部 `ErrorStage` 至少包括：

```text
ValidateCliRequest
ResolveCliPaths
IngestWorkbook
Normalize
Split
BuildFact
BuildPresentation
PrepareOutputDirectory
CheckDiskSpace
CreateTempWorkspace
PlanSheet
InitializeLowMemoryTempWriter
PopulateWorkbook
CreateFinalOutput
SaveWorkbook
RemovePartialOutput
CleanupTempWorkspace
ReadOutputMetadata
```

### 9.3 对外错误码

以下契约只有在 Phase -1D 证明 fork 的创建、行刷新、最后一行 flush、rewind 和 copy 全部返回原始 `io::Error`，且 `costing-xlsx` 到 CLI 的适配层未将其字符串化后才生效。禁止从 panic 文本或错误 message 推断 `ErrorKind/raw_os_error`。

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
| `InitializeLowMemoryTempWriter` / 真正创建临时文件失败 | `OUTPUT_NOT_WRITABLE` | 见固定 I/O 映射 | 最终输出尚未创建，不删除 |
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

`rust_xlsxwriter::XlsxError` 只有结构化匹配为 `XlsxError::IoError` 并将其原始 `std::io::Error` move 进 `CostingError::IoSource` 时才沿用该映射；其余 writer 错误一律 `retryable=false`。`ErrorContext.details.cleanup_failures` 单独转移到结构化 details，不改写主错误 code/retryable/source。

外围 `catch_unwind` 若保留，只能映射为 `INTERNAL_ERROR`、`retryable=false` 和 `stage=UnknownPanic`，并执行 best-effort cleanup；它不是 LowMemory I/O 的正常错误路径，也不得把无法识别的 panic 伪装为磁盘或权限错误。

### 9.4 结构化详情

`request_id` 在 CLI 接受参数后、首个可失败的校验或 I/O 前生成。输入输出同路径继续由现有 CLI 校验负责，错误详情使用 `stage=ValidateCliRequest`；writer 不为此引入 `input_path` 耦合。随后通过 `WriterContext` 把 `request_id` 传入所有 writer 阶段。运行期错误 JSON 保持 `code/message/retryable`，并在 `ErrorContext` 存在时增加：

```json
{
  "request_id": "costing-...",
  "details": {
    "stage": "SaveWorkbook",
    "path": "D:\\...\\输出.xlsx",
    "io_kind": "StorageFull",
    "raw_os_error": 112,
    "final_output_valid": false,
    "partial_output_removed": null,
    "cleanup_failures": []
  }
}
```

`io_kind` 是 `IoFailureMeta.kind` 的稳定闭合序列化，`raw_os_error` 保留 Windows 原始数字或为 `null`；两者均来自同一个未字符串化的 `std::io::Error`。`final_output_valid` 仅在 `save_to_writer` 成功、flush、关闭文件且 metadata 非零后变为 `true`。`partial_output_removed` 在本次运行从未创建最终文件时为 `null`；尝试删除本次残缺文件后为 `true/false`。

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

Manifest 使用第 6.8 节 allowlist，只保存 SHA、计数、维度、feature/target、逐轮数值、median、无敏感值 verdict 和必要机器规格，不保存 ERP 内容、真实路径、文件名、原始日志或主机身份。

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

GB 的正式 normal-mode 输出大小、Sheet 维度、runtime/error counts 和逐轮数据由 Phase 0A 生成，写入第 8 节指定的版本化基线文件。版本化前必须通过路径、文件名、canary、`expected=/actual=` 和 raw stdout/stderr 扫描。用户确认并提交该文件是 Phase 1 的阻塞检查点；确认后任何候选都不得覆盖或重算它。

### 10.2 时间轮次

```text
预热：1
正式：5
统计：保存原始值，使用 median
```

每个候选批次都重新运行已冻结 SHA 的 reference EXE，并按 reference/current 成对交替顺序采样，减少温度和缓存偏差。GB wall-clock/PWS 回退比使用该批次 reference 中位数作分母；不得用历史慢值或其他批次挑选分母。正式 EXE 直接运行，不通过 `cargo run`。

全局顺序固定为：

```text
round 1/3/5/7/9  : reference → candidate
round 2/4/6/8/10 : candidate → reference
```

追加组从 global round 6 开始，任何函数或第二次调用都不得把奇偶性重置到 round 1。

同批 reference 中位数若相对 Phase 0A manifest 的校准中位数偏离超过 10%，整批标记 `ENVIRONMENT_DRIFT` 并作废；先恢复机器状态，再重采 reference/current，不能移动或重写已批准基线。

本设计中的正式样本数记为 `N`：默认 `N=5`；若触发第 10.5 节临界噪声规则，则追加五轮并令 `N=10`。所有写作“5 轮 median”的目标，机械执行时均解释为“`N` 轮 median”。

除确定性的 output bytes 外，所有性能比的分子和分母必须同时满足：

- 同一 pipeline 和 input SHA；
- 同一 `N`；
- 同一机器状态和 benchmark batch；
- 同一全局 AB/BA 顺序；
- 两个明确记录且全程不变的 binary SHA。

该规则覆盖 Phase1/0B、B/A、C/A、D/C、D/B、Phase3 feature-on/off 和 Phase4/3。历史 JSON 只能用于 output bytes 固定值与环境漂移校准，不能作为性能收益的有利分母。

### 10.3 PWS

- 与时间基准分开；
- 50ms 调用 `Process::Refresh()` 并读取 Windows/.NET `Process.PeakWorkingSet64`；该属性是内核维护的进程累计峰值，不是瞬时 `WorkingSet64` 采样；
- 1 次预热 + 默认 5 次正式；触发第 10.5 节时追加为 10 次正式；
- SK median `<= 2,147,483,648 bytes`；
- GB current/baseline `<=1.05`。

### 10.4 输出目录

性能轮次输出到：

```text
data/processed/<pipeline>/.perf-runs/<binary-sha>/<round>/
```

每轮使用独立输出路径。最外层 `try/finally` 必须在成功、子进程失败、comparator 失败、SHA 变化、证据写入失败和敏感扫描失败时删除真实 workbook。清理失败则整批 fail closed；只保留通过 `EvidenceSanitizer` 的 JSON 和本地日志 SHA。

### 10.5 噪声规则

1. 第一组五轮是正式证据；
2. 对任一上限型比例，先写成 `measured / limit`；满足 `abs(measured / limit - 1) <= 0.03` 时必须再采五轮，无论首组当前通过还是失败，不允许操作者选择。绝对秒数上限也用同一归一化公式。任一 time/PWS 指标触发后，该候选的时间和 PWS 两套证据都补足到 `N=10`；输出字节数是确定性元数据，不单独触发扩样；
3. 第二组从 global round 6 追加五个 reference/current 配对，不得替换第一组或重置顺序；
4. 分别合并 reference 十轮和 current 十轮计算中位数；
5. 十轮中位数通过才算通过；
6. 两组方向冲突则为 `INCONCLUSIVE`；
7. 输入 SHA、binary SHA、Git diff 或机器状态变化时整组作废；
8. correctness 失败时停止性能判定；
9. 未追加时 reference/current 各要求 5/5 正式运行成功；追加后各要求 10/10 成功，candidate 任一正式轮失败即拒绝候选，reference 失败则整批证据无效；
10. 缺轮、重复 round number、AB/BA 次数不平衡或轮号不连续时返回 `INCOMPLETE_EVIDENCE`，不计算 median；
11. 最小收益门槛先转为上限比例再使用同一公式，例如“至少下降 10%”写为 `candidate / baseline <= 0.90`。

### 10.6 Workbook oracle 契约

值比较不再使用全局 `1e-6` epsilon：

- 金额、数量、单位成本和分析数值：直接从 worksheet XML 的 `<v>` 词法值解析为 `Decimal`，不先经过 Calamine/Python `float`；规范化符号零和等价十进制表示后精确比较；
- 金额、数量和成本列：除逐格比较外，再按列计算 `Decimal` 合计并勾稽；
- 需要业务主键的 Sheet：按现有主键做分组汇总勾稽；
- 编码、文本、日期、状态：值和存储类型精确比较；
- 数字字符串与数值不等价；
- 若 Python/Rust 基线因既有 f64 落盘产生真实差异，Phase 0H 必须先定位并修正，不能恢复无差别 epsilon 掩盖。

样式比较使用坐标敏感的有效样式：

```text
(sheet, coordinate) -> effective_style_signature
```

签名解析显式 cell style、列继承和合法的隐式空白等价；可以使用包含坐标的流式 hash 降低内存，但不能退化成“每列出现过哪些样式”的集合。

shared strings/package 必须同时检查：

- `workbook.xml.rels` 的 relationship type 和 target；
- `[Content_Types].xml` 声明；
- `xl/sharedStrings.xml` 存在性与引用一致性；
- 每个字符串 cell 坐标的 `c@t`；
- 当前 LowMemory 契约使用 `t="s"`，不得静默变为 `inlineStr`；
- shared string index 解引用后的实际文本仍需比较，但失败证据不得包含真实文本。

### 10.7 版本化证据安全门禁

所有待版本化 evidence artifact 只能通过 `EvidenceSanitizer` 构建或归一化，不接受任意 dict/JSON、命令 transcript 或文本透传。除第 6.8 节的字段值域校验外，写入前还必须拒绝：

- Windows 盘符绝对路径、UNC 路径、`\\Users\\`；
- 当前 username、hostname；
- 输入和输出真实 basename/stem；
- 脱敏测试植入的 canary；
- `expected=`、`actual=`；
- `STDOUT:`、`STDERR:` 或原始命令参数。

扫描目标是整个待提交 evidence 目录和 Git staged evidence files，不是单个 manifest。扫描失败时必须删除本批已生成的待提交 artifact 并 fail closed。真实本地日志只能保存到 Git ignore 或仓库外目录；版本化 artifact 记录其 SHA-256，不记录内容或真实路径。

## 11. 测试设计

### 11.1 Patched rust_xlsxwriter

fork 故障注入必须覆盖：

- 启用 `constant_memory` 时，Standard Sheet 在隔离进程中不创建、不打开任何临时文件；
- `add_worksheet_with_constant_memory`、`add_worksheet_with_low_memory`、`new_worksheet_with_constant_memory`、`new_worksheet_with_low_memory` 四个 public factory 都不在构造阶段访问 tempdir；各自在首个真实写入/flush/save 路径遇到 tempdir 创建失败时均返回 `Result` 而不 panic；
- 系统 TEMP/TMP 指向不可用 canary 目录时，设置受控 tempdir 的 LowMemory 仍成功，且只在受控目录创建文件；
- `set_tempdir()` 成功后目录被删除或变为不可写，返回 `XlsxError::IoError`；
- 行中途 flush、最后一行 flush、rewind、copy 的注入失败均返回 `Result`，不 panic；
- 返回错误保留预期 `ErrorKind` 和 Windows `raw_os_error()`；
- 从 fork `XlsxError::IoError` 到 CLI JSON 的完整错误链仍可读取同一 `ErrorKind/raw_os_error`；
- `ErrorContext` 经 `CostingError` 到 `ErrorSummary` 后保留同一 `request_id`、`stage`、`final_output_valid`、`partial_output_removed` 和 `cleanup_failures`，不从 message/source 反推；
- `with_context` 不重复包装已是 `Contextual` 的 writer 错误，不覆盖内层 stage 和 cleanup 状态；
- I/O 和非 I/O writer 错误均输出结构化 `request_id/details`；`ValidateCliRequest`、reader 和 ETL 失败也在 `request_id` 生成后保留 context，只有 CLI 参数无法解析时输出 `null`；
- 空 LowMemory Sheet、单行 Sheet、多行 Sheet；
- 成功输出相对官方 `v0.96.0` 的 workbook 语义一致；
- fork revision 和允许文件 diff manifest 校验。

故障注入使用 fork 内部、非公开的 test-only temp writer/factory，不通过真实填满磁盘制造不稳定测试。涉及 TEMP/TMP 的测试必须在独立子进程中运行，避免修改测试进程的全局环境。

### 11.2 Planner

覆盖：

```text
0 槽位 → Standard
4,999,999 槽位 → Standard
5,000,000 槽位 → LowMemory
乘法溢出 → saturating 到 LowMemory，不 panic
空列 / 空行 → Standard
```

### 11.3 ColumnWritePlan

- 每列一个数字格式；
- 文字/数字/日期/空白单元格；
- Decimal 写为 Excel number；
- 数字格式和文本对齐；
- Blank 不写 cell；
- 中文列名不进入逐格查找。

### 11.4 Standard/LowMemory 一致性

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

### 11.5 TempWorkspace

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

### 11.6 Reader

- 双层表头及前置说明行；
- 找不到表头/少于两行；
- 后部空白；
- String/Float/Int/Bool/DateTime/DateTimeIso/DurationIso/Error/Empty；
- Header 普通字符串、DateTimeIso、DurationIso 的 trim；
- Data 纯空白字符串转 Blank，非空字符串原始首尾空格保留；
- Data DateTimeIso 原样转 DateLike，DurationIso 原样转 Text；
- 整数 float、科学计数法、非有限 float；
- 中文文本；
- reader row count 和 snapshot；
- 真实 GB/SK Python oracle。

每一项都必须对去中间副本前后的 normalizer 结果做完全相同比较。

### 11.7 CLI

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

### 11.8 Benchmark harness

- 首组暂时通过但位于临界区，仍强制扩展到 10 轮；
- 首组暂时失败但位于临界区，同样强制扩展到 10 轮；
- 精确断言全局十轮调用顺序为 R/C、C/R 交替，追加从 round 6；
- 两个五轮组方向冲突返回 `INCONCLUSIVE`；
- normal wall 和 normal PWS 命令均不含 `--check-only`；
- candidate 非零退出、reference 非零退出、缺轮、重复轮号、顺序失衡分别 fail closed；
- input/reference/candidate/Git SHA 在任一轮变化时整批无效；
- Phase1/0B、B/A、C/A、D/C、D/B、Phase3 feature on/off、Phase4/3 拒绝跨批分母；
- 每条失败路径都在 finally 删除实际 workbook。

### 11.9 Workbook oracle 与 EvidenceSanitizer

Workbook oracle 回归：

- 金额仅差 `1e-7` 也被拒绝；
- 数字字符串与数值不等价；
- 两个数据行交换样式被拒绝；
- sharedStrings relationship type/target 错误被拒绝；
- `[Content_Types].xml` 或 `sharedStrings.xml` 缺失被拒绝；
- 同值字符串从 `t="s"` 改成 `inlineStr` 被拒绝；
- mismatch 结果不包含真实单元格值。

EvidenceSanitizer 回归：

- 成功 evidence 不包含绝对路径、用户名、hostname 或 ERP 文件名；
- mismatch、非零退出 stdout/stderr 中植入敏感 canary，manifest 不得包含 canary；
- 把未知敏感 canary 逐一注入每个允许的字符串字段，字段值域校验必须全部 fail closed；
- command transcript、smoke summary、PE imports、fork provenance、Cargo feature tree 和 Markdown/文本报告均走同一 sanitizer，不允许原始输出绕过；
- manifest 不包含 `expected=`、`actual=`、`STDOUT:`、`STDERR:`；
- 所有失败路径删除 workbook；
- 清理或敏感扫描失败时不留下任何可提交 evidence artifact。

## 12. Windows 单 EXE 验证

最终 release EXE 使用 MSVC `dumpbin /DEPENDENTS` 和 `dumpbin /IMPORTS` 检查普通及 delay-load imports；可用时再以 `llvm-readobj --coff-imports` 交叉验证。完整命令和原始输出只保存在本地未版本化日志。版本化 PE 证据只保存使用 `$CANDIDATE_EXE` 的命令模板、工具名/已脱敏版本、EXE SHA、解析后的普通/delay-import basename 列表、verdict 和本地日志 SHA，不保存绝对路径或完整原始输出。

判定规则：

```text
不得出现 basename 匹配 (?i)(zlib|libz|deflate).*\.dll 的 import
不得出现项目私有 DLL
不得出现相对 Phase 0A 新增的非 Windows / 非已批准 Microsoft runtime DLL
发布目录只包含 EXE，不随附任何运行时 DLL
```

再复制到不安装 Rust、Cargo、Python 和 zlib 的干净 Windows 10/11 x64 环境。测试包只包含 candidate EXE、脱敏输入和必要配置，不复制任何运行时 DLL，并以脱敏 fixture 运行正常模式。

Smoke 为被测进程单独把 TEMP/TMP/TMPDIR 指向一个确认不存在的 canary 路径，该路径不得位于输出目录，且运行前后都不得创建。原版 0.96.0 的首次 `tempfile_in(std::env::temp_dir())` 会在这里失败；patched binary 必须仍能完成 Standard/LowMemory 正常模式。不能只检查运行后目录为空，因为创建后自动删除的临时文件也会留下空目录假象。

Smoke 必须证明：

- 无缺失 DLL；
- 三张 Sheet 正确；
- 进程专用 TEMP/TMP/TMPDIR 指向的不存在 canary 路径仍未创建，正常模式仍成功；
- 输出目录无残留 `.costing-tmp-*`；
- 单 EXE 成功退出；
- 版本化保存使用别名的 smoke 命令模板、EXE SHA、由 CLI payload 重建的 allowlist JSON summary、解析后 PE import basename 列表和本地原始日志 SHA。

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
| fork provenance/revision | upstream tag 解析为固定 base SHA，crates.io checksum 一致；Cargo manifest、lockfile、依赖 manifest 为同一完整 fork SHA，diff 仅含允许文件 |
| LowMemory I/O | Phase -1D 故障注入全通过，无临时 I/O panic |
| temp workspace | 每轮结束后不存在 |
| TEMP/TMP/TMPDIR canary | 指向不存在路径时 Standard/LowMemory 仍成功，路径始终未创建 |
| evidence | allowlist、敏感扫描和 finally cleanup 全部通过 |
| 发布 | 单 EXE，无第三方 zlib DLL |
| 非目标 stage | 相对该阶段输入候选的 ratio `<=1.05`；临界重采后仍超限则拒绝候选 |

表中的 GB wall/PWS `phase0a` 分母是同批固定 reference binary；SK/GB bytes `phase0a` 分母是版本化 manifest 的固定字节数。两类分母都绑定同一个 Phase 0A binary SHA。

## 14. 伪代码草案

### 14.1 持续优化控制流

```python
# [伪代码草案]
# 目标：按批准阶段每次只验证一个性能变量；Phase 4 A/B 完成后，最终候选过门即停止
# 输入：固定 GB/SK 样本、批准的 fork URL、baseline EXE、candidate EXE、批准门槛
# 输出：validated_result / rejected_candidate / next_profile_hotspot

def optimize_until_gate_passes():
    patched_dependency = build_and_fault_inject_pinned_fork(
        upstream_tag="v0.96.0",
        upstream_base_revision="9134de25afadaee955d0f821862338e3d046a338",
        fork_url="https://github.com/Aspirin86942/rust_xlsxwriter.git",
        upstream_pr=False,
    )
    require_fallible_temp_io_and_allowed_diff(patched_dependency)

    harness = build_phase0h_normal_benchmark_harness()
    require_harness_oracle_and_sanitizer_tests(harness)

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
    candidate = select_feature_by_phase2_decision_table(
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

### 14.2 fork 的 fallible 临时 I/O

```rust
// [伪代码草案]
// 输入：worksheet mode、可选受控 tempdir、待写入 cell/row
// 输出：成功写入；或保留原始 io::Error 的 XlsxError::IoError

// 四个 public ConstantMemory/LowMemory factory 都只构造状态；
// 为保持 0.96.0 API，真正的 tempfile 创建延迟到首个可返回 Result 的写入/flush/save 路径。
fn configure_memory_worksheet(mode: MemoryMode, tempdir: Option<PathBuf>) -> Worksheet {
    Worksheet::without_tempfile(mode, tempdir)
}

fn insert_cell(&mut self, row: RowNum, col: ColNum, cell: CellType) -> Result<(), XlsxError> {
    if self.use_constant_memory && row > self.current_row {
        self.flush_to_row(row)?;
    }
    self.insert_cell_to_active_table(row, col, cell);
    Ok(())
}

fn flush_data_row(&mut self, next_row: RowNum) -> Result<(), XlsxError> {
    // 为什么延迟创建：Standard Sheet 永远不会到达这里，因而不访问任何 tempdir。
    let row = self.current_row;
    let cells = self.take_current_row_cells();
    let writer = self.ensure_temp_writer()?;
    try_write_row_start(writer, row)?;
    for cell in cells {
        try_write_cell(writer, cell)?;
    }
    try_write_row_end(writer)?;
    self.advance_to(next_row);
    Ok(())
}

fn package_low_memory_sheet(&mut self, zip: &mut ZipWriter) -> Result<(), XlsxError> {
    self.flush_last_row()?;
    // 空 Sheet 也走 fallible ensure，不能依赖之前至少写过一行。
    let writer = self.ensure_temp_writer()?;
    writer.rewind()?;
    std::io::copy(&mut BufReader::new(writer.get_ref()), zip)?;
    Ok(())
}
```

### 14.3 安全写出

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

fn map_writer_error(error: WriterError) -> CostingError {
    let WriterError {
        mut context,
        primary,
    } = error;
    let stage = context.details.stage;
    let core_error = match primary {
        WriterPrimaryError::Io(source)
        | WriterPrimaryError::Xlsx(CostingXlsxError::Writer(
            XlsxError::IoError(source),
        )) => {
            // 先借用同一个 io::Error 计算映射，然后把所有权 move 到核心错误。
            let io_meta = IoFailureMeta::from(&source);
            let retryable = retryable_io(&source);
            context.details.io_meta = Some(io_meta);
            CostingError::IoSource {
                code: ErrorCode::OutputNotWritable,
                message: writer_message(stage),
                retryable,
                io_meta,
                source,
            }
        }
        non_io_primary => {
            let classified = classify_non_io_writer(stage, &non_io_primary);
            CostingError::Writer {
                code: classified.code,
                message: classified.message,
                retryable: false,
            }
        }
    };
    CostingError::Contextual {
        context,
        source: Box::new(core_error),
    }
}
```

### 14.4 ingest 去副本

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

所有 Rust 命令固定 MSVC target；测试 target-dir 与性能候选 target-dir 分离：

```powershell
cargo test --locked --manifest-path rust/Cargo.toml -p costing-xlsx --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
cargo test --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
cargo test --locked --manifest-path rust/Cargo.toml --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
cargo fmt --manifest-path rust/Cargo.toml --all --check

uv run python -m pytest tests/rust_oracle -q --basetemp .pytest-tmp/rust-oracle
uv run python -m pytest tests/contracts tests/architecture -q --basetemp .pytest-tmp/contracts
uv run python -m ruff check src tests
uv run python -m ruff format src tests --check
```

Phase -1D 必须在独立 fork checkout 上执行以下只读核验和测试：

```powershell
git -C "$FORK_CHECKOUT" remote get-url upstream
git -C "$FORK_CHECKOUT" rev-parse "refs/tags/v0.96.0^{commit}"
git -C "$FORK_CHECKOUT" rev-parse HEAD
git -C "$FORK_CHECKOUT" diff --name-only 9134de25afadaee955d0f821862338e3d046a338..HEAD
git -C "$FORK_CHECKOUT" diff --check 9134de25afadaee955d0f821862338e3d046a338..HEAD
cargo test --release --manifest-path "$FORK_CHECKOUT/Cargo.toml" --target x86_64-pc-windows-msvc --target-dir rust/target/fork-test-msvc --no-default-features --features constant_memory
```

上述完整命令/原始输出只进入本地未版本化日志。版本化 fork evidence 只保存批准的上游/fork 规范 URL、精确 upstream/fork revision、checksum、归一化 diff 文件列表、diff SHA、verdict 和本地日志 SHA。允许的 fork diff 文件只有第 6.1 节列出的 3–4 个源文件、对应测试和必要 Cargo metadata；依赖证据中的 `upstream_pr_url` 必须为 `null`。

Phase 2 的四个 release 候选使用独立 target-dir，不共享 `target/release`：

```powershell
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/perf-builds/phase2/A --no-default-features
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/perf-builds/phase2/B --no-default-features --features zlib
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/perf-builds/phase2/C --no-default-features --features zmij
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/perf-builds/phase2/D --no-default-features --features "zlib,zmij"
```

每个 EXE 位于各自：

```text
<target-dir>/x86_64-pc-windows-msvc/release/costing-calculate.exe
```

所有阶段的 target-dir 和精确 feature 映射固定为：

| 阶段/候选 | target-dir | `--no-default-features` 后的精确 feature |
|---|---|---|
| Phase 0A reference | `rust/target/perf-builds/phase0a/reference` | 无 |
| Phase 0B instrumented | `rust/target/perf-builds/phase0b/instrumented` | 无 |
| Phase 1 writer | `rust/target/perf-builds/phase1/writer` | 无 |
| Phase 2 A/B/C/D | `rust/target/perf-builds/phase2/{A,B,C,D}` | 分别为无 / `zlib` / `zmij` / `zlib,zmij` |
| Phase 3 feature-off/on | `rust/target/perf-builds/phase3/low-memory-<set>` | `low-memory` + 确定性 feature-off/on 集合 |
| Phase 4 reader | `rust/target/perf-builds/phase4/reader-<selected-set>` | 与 Phase 3 最终选中集合完全相同 |

`<set>` 只能是 `default`、`zlib`、`zmij` 或 `zlib-zmij`，并必须与 manifest 的闭合 feature enum 一致。Phase 3 对每个 Phase 2 保留 feature 分别构建同批 feature-on 与去掉该 feature 的 feature-off 候选；例如保留 `zlib,zmij` 时，zlib 增量比较 `low-memory-zlib-zmij / low-memory-zmij`，zmij 增量比较 `low-memory-zlib-zmij / low-memory-zlib`。构建标签不得依赖“构建后立即复制”来维持正确性。

Phase 3/最终候选使用 `--features low-memory` 加上 Phase 2 实际保留的 feature。该组合必须额外运行：

```powershell
cargo test --release --locked --manifest-path rust/Cargo.toml -p costing-xlsx --target x86_64-pc-windows-msvc --target-dir rust/target/test-low-memory --no-default-features --features low-memory
cargo test --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/test-low-memory --no-default-features --features low-memory
cargo tree --locked --manifest-path rust/Cargo.toml -p costing-calculate -e features --target x86_64-pc-windows-msvc --no-default-features --features low-memory
```

若保留 `zlib` 或 `zmij`，把它们追加到上述 `--features`。`cargo tree -e features` 的原始输出只进入本地未版本化日志；版本化证据只保存 `EvidenceSanitizer` 归一化后的 package/revision/feature 映射和日志 SHA，证明实际解析的 fork revision 和 `constant_memory/zlib/zmij` feature 图与标签一致。

最终再运行真实 GB/SK wall-clock、PWS、output size、workbook oracle 和单 EXE smoke。

## 16. 审查分工

- `data_auditor`：核对原始轮次、SHA、median、门槛和是否挑选结果；
- `python_reviewer`：核对 Phase 0H harness、oracle、sanitizer 和 fail-closed 测试；
- `security_reviewer`：核对 fork 供应链固定、敏感临时目录、证据脱敏、路径暴露和清理；
- `ops_reviewer`：核对 Windows、MSVC、磁盘空间、PE imports 和单 EXE；
- `doc_reviewer`：只读审查 README、验证文档和设计一致性；
- `doc-updater`：仅在明确需要同步命令或配置说明时做最小修改。

## 17. 后续流程

已逐节批准的设计整理并提交为本书面规格后：

1. 用户审阅提交后的设计文件；
2. 用户明确批准该文件；
3. 仅调用 `superpowers:writing-plans` 生成小提交实施计划；
4. 不在 writing-plans 之前修改生产代码；
5. 实施按 Phase -1D → 0H → 0A → 0B → 1 → 2 → 3 → 4 → 5 顺序，每个阶段独立验证和提交；
6. 达到全部硬门槛后立即停止。

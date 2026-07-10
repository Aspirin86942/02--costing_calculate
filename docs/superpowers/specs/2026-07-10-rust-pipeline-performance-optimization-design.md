# Rust 成本管线性能优化设计

## 状态与决策

- 日期：2026-07-10
- 状态：设计已通过对话确认、书面自审、文档复审和 Rust 技术复审，等待用户复核书面 spec
- 选定路线：分阶段的标准库索引化与所有权优化
- 最终性能目标：在业务契约完全一致的前提下，GB、SK 的 Rust release 中位数均不慢于对应 Python oracle 中位数
- 文档口径：根 `README.md` 与 `AGENTS.md` 的正式 Rust build/run/benchmark 命令同步使用 release；`cargo test` 与 `cargo fmt` 保持原样
- 生产依赖：不新增

## 背景

当前 Rust CLI 已是成本核算主实现，但默认 `cargo run` 使用未优化 dev profile。真实 SK 文件在 dev profile 下约 140 秒，不能代表生产性能。2026-07-10 的同机 warm run 显示：

| 实现 | 模式 | 总耗时 |
|---|---|---:|
| Rust | release、SK、`--check-only --benchmark` | 约 29.743 秒 |
| Python | 当前优化环境、SK、`--check-only --benchmark` | 约 21.971 秒 |

Rust release 仍比 Python 慢约 35.38%，需缩短约 7.77 秒才能达到本设计的最终门禁。阶段数据如下：

| 阶段 | Rust release | Python | 说明 |
|---|---:|---:|---|
| ingest | 约 14.586 秒 | 约 14.635 秒 | 基本持平，本轮默认不改 reader |
| normalize | 约 5.469 秒 | 约 1.210 秒 | Rust 约慢 4.52 倍 |
| split | 约 0.526 秒 | 约 0.042 秒 | Python 计时边界与 Rust 不完全相同 |
| fact / analysis | 约 1.658 秒 | 约 5.589 秒 | 阶段边界不同，不直接一一比较 |
| presentation | 约 7.505 秒 | 约 0.496 秒 | Rust 包含异常分析、质量指标和完整 Sheet 物化 |

这些数值是设计输入，不是永久基准。实施前必须按本文方法重新测量，避免机器状态、文件内容或代码变化造成漂移。

## 已确认的源码事实

### Normalize

`rust/crates/costing-core/src/normalize.rs` 的 `rows_to_maps` 当前把每行转换为：

```rust
BTreeMap<String, CellValue>
```

每个单元格都会克隆列名、克隆 `CellValue`、分配树节点，后续又通过字符串键重复查询。真实 SK 输入约 46.7 万 reader rows，这一表示会制造大量短生命周期分配。

### Fact

`rust/crates/costing-core/src/fact.rs` 的 `build_fact_bundle` 当前使用：

```rust
BTreeMap<String, BTreeMap<String, Decimal>>
BTreeMap<String, usize>
BTreeSet<String>
```

同时存在：

- 工单字符串键多次规范化和拼接；
- 数量行多遍扫描；
- `amount_by_key.get(&key).cloned()`；
- `qty_row.values.clone()`；
- `work_order_fact.push(row.clone())`；
- 大量 `ErrorIssue` 固定元数据字符串重复分配。

这些容器只用于 lookup、计数和 membership，不依赖有序遍历产生业务输出。

### Presentation

`rust/crates/costing-core/src/presentation.rs` 的 `build_workbook_payload` 按值接收 `FactBundle`，但仍执行：

```rust
bundle.detail_fact.clone()
bundle.error_issues.clone()
```

`build_flat_sheet` 随后又通过 `row.values.get(column).cloned()` 将展示单元格复制到最终二维数组。数量页和异常页也有整行复制。

但 `presentation` 的约 7.5 秒不只包含上述 clone，还包含：

- 数量页派生字段计算；
- 工单异常行构造、排序和 Modified Z-score；
- 质量指标多遍扫描；
- Map 到 `SheetModel.rows` 的投影。

因此两处显式 clone 是确定可消除的问题，但其具体耗时占比必须由 release profiler 证明。

### Check-only

当前 `--check-only` 仍完整执行 `build_workbook_payload`，只跳过最终 writer。因此当前 presentation 计时代表完整内存 Sheet 构建，不是单纯摘要计时。本设计不通过让 check-only 少跑业务步骤来制造更快的 benchmark。

### 结构索引状态

设计核查时，`codebase-memory-mcp` 状态为 `Ready`，并通过 `search_graph`、`get_code_snippet` 和 `trace_path` 核对了：

```text
run
  -> normalize_workbook
  -> split_detail_and_qty
  -> build_fact_bundle
  -> build_workbook_payload
       -> build_qty_sheet_rows
       -> build_work_order_anomaly_sheet
       -> build_quality_metrics
       -> build_flat_sheet
```

## 目标

1. 在正确性优先的前提下，使 GB、SK 的 Rust release 中位数均不慢于 Python oracle。
2. 消除 normalize 的逐行列名 Map 和无条件单元格复制。
3. 消除 fact 中动态金额 bucket、重复工单键构造和完整工单行副本。
4. 消除 presentation 中没有所有权必要的深复制和中间 Map 投影。
5. 保持所有业务、错误、质量、Sheet 和 CLI 契约。
6. 最终 5 次正式运行的 peak working set 中位数不高于优化前基线的 105%，目标为下降。
7. 让 Phase 1–3 分别可验证和复测；三阶段均为已批准的必做范围，只有 Phase 4 可因达标而停止。
8. 将 README 与 AGENTS.md 的正式运行口径统一为 release。

## 输入

- GB 或 SK 原始成本计算单 `.xlsx`；
- `PipelineConfig`，包含独立成本项和产品白名单顺序；
- 可选严格月份范围；
- `check_only` 和 `benchmark` CLI 标志；
- 现有 Python oracle、runtime contract 和 workbook contract；
- 固定的真实 GB/SK 性能样本。

## 输出

成功时：

- CLI `RunSummary` 外部结构不变；
- 正常模式继续输出一个不覆盖已有文件的 `*_处理后[月份后缀].xlsx`；
- workbook 继续只包含三张业务 Sheet；
- `error_log_count`、`issue_type_counts`、质量指标和阶段耗时继续输出；
- check-only 继续不写 workbook 或外部摘要文件；
- release benchmark 产出可复现的 stage timing 和总时间证据。

失败时：

- 沿用现有结构化 `CostingError` / Error Model；
- 缺少必要列返回 `INVALID_INPUT`，不可重试；
- Schema 或行结构违反内部不变量时返回 `INTERNAL_ERROR`，不静默修正；
- 输出已存在、输入输出相同、文件读写失败等 CLI/XLSX 错误契约不变。

本轮不新增自动重试或降级结果。

## 非目标

- 不引入 Rust Polars、Arrow、IndexMap、第三方或自制字符串 interner、自定义 allocator；
- 不更换 Calamine、rust_xlsxwriter 或 Python oracle 依赖；
- 不把金额从 `Decimal` 改为 `f64`；
- 不取消、抽样、去重或只计数 `ErrorIssue`；
- 不修改业务阈值、白名单、成本分类或勾稽口径；
- 不更新 `tests/contracts/baselines/` 来迁就纯性能重构；
- 不通过 check-only 跳过 presentation 来满足核心门禁；
- 不在首轮设计 streaming writer；
- 不优化当前已与 Python 基本持平的 ingest，除非新 profiler 证据改变结论；
- 不在同一变更中加入 LTO、release profile 调参或新生产依赖。

## 方案比较与决策

### 路线 1：标准库索引化与所有权优化（选定）

- 使用每张表一份 `ColumnSchema`；
- 行数据改为 `Vec<CellValue>`；
- 热循环预解析 `ColumnId`；
- fact 使用 `HashMap<String, CostAmounts>` 和缓存工单键；
- `work_order_fact` 改为唯一工单索引；
- presentation 先借用、后消费大字段；
- Phase 1–3 均完成；每阶段 profiler 和 contract gate 用于校正下一阶段实现，Phase 4 才按最终门禁决定是否进入。

它能从根因消除逐行字符串 Map，又不增加生产依赖，是达到严格性能门禁的推荐平衡点。

### 路线 2：保留逐行 Map 的局部优化（未选）

仅移除 clone、缓存工单键并把 lookup 容器换为 HashMap。风险低，但 normalize 仍保留约 46.7 万个字符串 Map，较难稳定弥合 7.77 秒总差距，且后续可能再次迁移。

### 路线 3：Rust Polars / Arrow（未选）

列式性能潜力较高，但会增加重量级依赖、编译时间、二进制体积和 Decimal/审计适配复杂度，相当于第二次大迁移，不符合当前 YAGNI 和依赖约束。

## 总体架构

```text
RawWorkbook
rows: Vec<Vec<CellValue>>
        |
        v
IndexedTable
原子维护 ColumnSchema、display_order 与 IndexedRow
        |
        v
NormalizedCostFrame
共享 Schema + display_order + 索引化行
        |
        v
SplitResult
同一 Schema 下移动拆分 detail_rows / qty_rows
        |
        v
FactBundle
明细索引行 + QtyFactRow + CostAmounts + 唯一工单索引
        |
        v
WorkbookPayload
先借用分析，再消费行数据生成 SheetModel
```

外部 `RunSummary`、`WorkbookPayload`、`SheetModel` 和 writer 契约必须保持不变；变化限制在 `costing-core` 内部模型和其调用方。

## 核心数据模型

### SchemaId、ColumnId 与 ColumnSchema

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SchemaId(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ColumnId {
    schema_id: SchemaId,
    slot: usize,
}

struct ColumnSchema {
    schema_id: SchemaId,

    // ColumnId 对应 IndexedRow.cells 的物理槽位。
    names_by_id: Vec<String>,

    // 每张表仅一份；用于阶段开始时解析，不进入逐行热循环。
    id_by_name: HashMap<String, ColumnId>,
}

impl ColumnSchema {
    fn new(names: Vec<String>) -> Result<Self, CostingError>;
    fn len(&self) -> usize;
    fn display_order_for(&self, source_names: &[String]) -> Result<Vec<ColumnId>, CostingError>;
    fn require(&self, name: &str) -> Result<ColumnId, CostingError>;
    fn optional(&self, name: &str) -> Option<ColumnId>;
    fn name(&self, id: ColumnId) -> Result<&str, CostingError>;
}
```

`SchemaId` 由 table 模块内部的单调原子计数器生成，不进入任何序列化、日志或外部相等性契约；内部模型如需 `PartialEq`，逻辑比较忽略 SchemaId，只比较列名、顺序和单元格。`ColumnId` 同时携带 Schema 身份和物理槽位；列访问必须先校验 `schema_id`，因此来自其他表但 slot 恰好在范围内的 ID 也会返回 `INTERNAL_ERROR`，不会静默读错列。`ColumnSchema::new` 仅供 table 模块构造 `IndexedTable`，不向业务模块暴露；Schema 和所有行的扩列只能由 `IndexedTable` 原子完成。

重复列名沿用当前 `BTreeMap::collect` 的兼容语义：后出现的同名列覆盖 `id_by_name` 中的旧映射，即通过名称访问时“最后一列生效”。物理槽位仍全部保留。初始 `display_order` 按原始列名逐项查询 `id_by_name`，因此同名展示位置都会指向最后一个物理列 ID，与当前逐行 Map lookup 语义一致。

`source_display_order` 仅保留标准化源表的兼容顺序，不直接成为三张业务 Sheet 的列清单。业务 Sheet 继续调用现有固定且唯一的 contract 列表，再通过 Schema 将每个名称解析为“最后一列生效”的 ID，因此不会因为原始重复列名而输出重复业务列。

通用消费式投影仍必须预先构建 projection plan 作为防御性保证：若调用者显式请求同一个 `ColumnId` 多次，前面的展示位置克隆值，最后一个展示位置才移动值；不得第一次 `take/remove` 后让后续位置变成 Blank。正常业务 Sheet 的唯一 contract 列只执行移动路径。

### IndexedRow 与 IndexedTable

```rust
struct IndexedRow {
    schema_id: SchemaId,
    cells: Vec<CellValue>,
}

impl IndexedRow {
    fn get(&self, id: ColumnId) -> Result<&CellValue, CostingError>;
    fn get_mut(&mut self, id: ColumnId) -> Result<&mut CellValue, CostingError>;
    fn replace(
        &mut self,
        id: ColumnId,
        value: CellValue,
    ) -> Result<CellValue, CostingError>;
    fn take(&mut self, id: ColumnId) -> Result<CellValue, CostingError>;
}

struct IndexedTable {
    // 字段不向业务模块公开，避免 Schema 与 rows 被分别修改。
    schema: ColumnSchema,
    source_display_order: Vec<ColumnId>,
    rows: Vec<IndexedRow>,
}

impl IndexedTable {
    fn from_raw(
        source_names: Vec<String>,
        rows: Vec<Vec<CellValue>>,
    ) -> Result<Self, CostingError>;

    fn schema(&self) -> &ColumnSchema;
    fn rows(&self) -> &[IndexedRow];

    fn try_update_rows<F>(&mut self, update: F) -> Result<(), CostingError>
    where
        F: FnMut(&mut IndexedRow) -> Result<(), CostingError>;

    fn try_retain_rows<F>(&mut self, predicate: F) -> Result<(), CostingError>
    where
        F: FnMut(&IndexedRow) -> Result<bool, CostingError>;

    fn ensure_or_reuse_derived_column(
        &mut self,
        name: &str,
        display_position: DerivedColumnPosition,
        values: Vec<CellValue>,
    ) -> Result<ColumnId, CostingError>;

    fn into_parts(self) -> (ColumnSchema, Vec<ColumnId>, Vec<IndexedRow>);
}
```

不使用 `unsafe`。外部脏数据不得触发越界 panic。`IndexedTable::from_raw` 为每一行写入同一个 `SchemaId`，并在进入业务循环前完成短行补 Blank、长行截断。

业务模块可通过 `try_update_rows` 获得受控的 `&mut IndexedRow`，但 `IndexedRow` 的字段仍私有，只能使用会校验 SchemaId 且不改变行长度的 `replace/take` 等方法；不能 push、truncate 或更换 schema_id。`try_update_rows` 和 `try_retain_rows` 均传播结构错误，不允许把 Schema 不匹配当成 false、Blank 或 continue 静默吞掉。

业务模块先从只读 rows 计算完整的 `Vec<CellValue>`，包括需要跨行状态的 Filled 成本项目；`ensure_or_reuse_derived_column` 校验值数量与行数一致后才修改表：

- 名称已存在：复用当前“最后一列生效”的 `ColumnId`，覆盖该槽位，保持原展示位置；
- 名称不存在：由 `IndexedTable` 同时追加 Schema 槽位和每一行单元格，再更新 source display order；
- 业务模块不能先扩 Schema 再手工逐行 push，因此不会留下半扩展状态。

这一定义固定当前 `月份` 和 `Filled_成本项目` 的兼容行为：输入已经带有对应列时复用并覆盖，只有缺失时才新增列；若期间列本身不存在，则保持当前行为，不新增也不覆盖月份列。

### NormalizedCostFrame

```rust
struct NormalizedCostFrame {
    table: IndexedTable,

    // 保持 Python/现有 Rust 契约：即使源列缺失也返回固定四个键名称。
    key_columns: Vec<String>,
}
```

`key_columns` 是兼容元数据，不进入逐行热路径；热路径需要的列由各阶段 `*Columns::resolve` 单独解析为 `ColumnId`。不得因为源列缺失而缩短键名称清单或让 normalize 报错。

新增派生列时由 `IndexedTable` 只在每行尾部追加物理槽位，再把新 ID 插入 source display order 的目标位置；已有派生列复用原槽位和原位置。月份列缺失时的展示位置继续按“第一个期间列名之后”确定，即使重复期间列的值访问按“最后一列生效”，也不改变当前首个列名定位语义。

### SplitResult

```rust
struct SplitResult {
    schema: ColumnSchema,
    detail_display_columns: Vec<ColumnId>,
    detail_rows: Vec<IndexedRow>,
    qty_display_columns: Vec<ColumnId>,
    qty_rows: Vec<IndexedRow>,
}
```

拆分继续按输入顺序移动所有权，不复制整行。

`detail_display_columns` 和 `qty_display_columns` 必须继续来自 `detail_sheet_columns` / `qty_sheet_base_columns` 等现有固定业务契约，并按名称去重；不得直接复制 `source_display_order`。

### 类型化成本金额

```rust
#[derive(Default, Clone)]
struct CostAmounts {
    direct_material: Decimal,
    direct_labor: Decimal,
    manufacturing_overhead: Decimal,
    moh_other: Decimal,
    moh_labor: Decimal,
    moh_consumables: Decimal,
    moh_depreciation: Decimal,
    moh_utilities: Decimal,

    // 严格对应 PipelineConfig.standalone_cost_items 的稳定顺序。
    standalone: Vec<Decimal>,
}

enum CostClassification {
    DirectMaterial,
    DirectLabor,
    ManufacturingOverhead(Option<MohComponent>),
    Standalone(usize),
    Unmapped,
}
```

制造费用明细同时累加制造费用总额和对应明细；独立成本项只进入稳定索引位置，不进入制造费用。

### QtyFactRow 与 FactBundle

```rust
struct QtyFactRow {
    source: IndexedRow,
    work_order_key: String,
    completed_qty: Decimal,
    completed_total: Decimal,
    amounts: CostAmounts,
    moh_matches: bool,
    total_matches: bool,
    check_reason: String,
}

struct FactBundle {
    schema: ColumnSchema,
    detail_display_columns: Vec<ColumnId>,
    detail_rows: Vec<IndexedRow>,
    qty_display_columns: Vec<ColumnId>,
    qty_rows: Vec<QtyFactRow>,

    // 指向 qty_rows，每个工单只保留首次出现的索引。
    unique_work_order_indices: Vec<usize>,

    qty_input_row_count: usize,
    filtered_invalid_qty_count: usize,
    filtered_missing_total_amount_count: usize,
    duplicate_work_order_row_count: usize,
    error_issues: Vec<ErrorIssue>,
}
```

异常分析、质量指标和 CLI 行数通过唯一索引借用 `qty_rows`，不再保存完整的 `work_order_fact` 副本。

`duplicate_work_order_row_count` 的定义保持现状：对每个出现次数大于 1 的工单键，统计该键对应的全部数量行，而不是只统计第二条及以后的行。例如同一键出现 3 次时，该指标增加 3，且三行均按当前契约生成 `DUPLICATE_WORK_ORDER_KEY`。

## 数据结构不变量

```text
每个 IndexedRow.cells.len() == ColumnSchema.len()
每个 IndexedRow.schema_id == ColumnSchema.schema_id
每个 ColumnId.schema_id == ColumnSchema.schema_id
ColumnId 的物理位置一经分配不再改变
source_display_order 中所有 ColumnId 有效
Schema 扩列与全部行扩列只能由 IndexedTable 原子完成
新增派生列仅在名称缺失时追加物理槽位
已有月份/Filled 派生列复用最后一个同名槽位并覆盖
业务 Sheet 列来自固定、唯一的 contract 列表
qty_rows 保留所有有效数量行
unique_work_order_indices 只保存每个工单首次出现的位置
```

reader 行规范化保持当前语义：短行补 Blank，长行忽略超出有效表头范围的单元格。

## 实施阶段

### Phase 0：文档与可复现基线

1. README 和 AGENTS.md 的正式 Rust build/run/benchmark 命令同步添加 `--release`。
2. 补充 dev profile 不用于真实数据性能比较的说明。
3. 不修改 Rust test/fmt 和 Python oracle 命令。
4. 直接运行已构建 release executable，固定真实 GB/SK 输入。
5. check-only 和 Peak Working Set 各预热 1 次并正式运行 5 次；full pipeline 沿用现有 3 次 harness。
6. 使用 WPR/WPA 对带 release 优化和符号的 profiling binary 采样；正式计时重新使用普通 release binary。

正式基准记录：

- ingest、normalize、split、fact、presentation、export、total；
- peak working set；
- reader/detail/qty/work-order 行数；
- error log 和 issue type counts；
- quality metrics。

### Phase 1：Presentation 所有权去复制

在当前 Map 模型下先完成低风险改造：

- 先借用 bundle 构建质量指标和异常分析；
- 解构 `FactBundle`，直接移动 `detail_fact` 和 `error_issues`；
- `build_flat_sheet` 消费行并移动最终单元格；
- 数量页 builder 消费 `qty_fact`，不 clone 整行 Map；
- `AnomalyRow` 借用源行，不 clone 工单行。

诊断阈值：

- presentation 中位数预期至少下降 20%；
- 若不足 10%，保留正确的所有权改造，但必须重新 profiler 后才进入下一项猜测性优化；
- 所有 contract 必须先通过。

### Phase 2：Normalize / Split 索引化

1. 新增聚焦的内部 `table` 模块，由 `IndexedTable` 原子维护 Schema、source display order 和所有行，不承载业务规则。
2. `RawWorkbook.rows` 直接移动为带相同 `SchemaId` 的 `IndexedRow`，不逐单元格 clone。
3. normalize 各阶段预解析列 ID。
4. forward fill 使用列 ID 和短 `previous_values` 数组，并通过 `IndexedTable::try_update_rows` 修改已有槽位、传播访问错误。
5. 月份和 Filled 成本项目列存在时复用并覆盖；仅缺失时由 `IndexedTable` 原子追加物理槽位并调整 source display order。
6. split 继续按输入顺序移动行。
7. 重复列名和别名冲突按“最后一列生效”兼容。
8. detail/qty 输出列继续来自固定且唯一的业务 contract，不直接输出重复源列。

诊断阈值：

- normalize 中位数预期至少下降 50%；
- 若下降不足 30%，重新 profile 期间格式化、forward fill 和行规范化；
- split 不得发生无解释的显著回退。

### Phase 3：Fact 类型化聚合与键缓存

1. `HashMap<String, CostAmounts>` 替换两层字符串 BTreeMap。
2. 成本分类返回 enum，不创建 bucket 字符串 Vec。
3. 每个数量行只生成一次规范化工单键，并存入 `PreparedQtyRow`。
4. 第一遍准备数量行并统计重复；第二遍按输入顺序生成重复错误；第三步消费 prepared rows 构建事实。
5. 金额聚合只借用或复制紧凑的 `CostAmounts`，不 clone 内层 Map。
6. `unique_work_order_indices` 替换完整 `work_order_fact` 副本。
7. quality 复用 fact 已计算的重复计数、过滤计数和勾稽结果。

诊断阈值：

- fact 中位数预期至少下降 20%；
- 若不足 10%，重新 profile 工单键、ErrorIssue 分配和 Decimal 运算；
- 错误日志类型、数量和顺序必须完全一致。

### Phase 4：仅处理 profiler 证明的剩余热点

只有前三阶段后仍未达到最终硬门禁才进入：

1. 若固定错误文本分配占显著比例，考虑 `Cow<'static, str>` 或内部 `IssueKind`；
2. 若 anomaly 动态数字 Map 占显著比例，改为固定指标结构或数组；
3. 若工单键字符串分配仍是热点，只允许减少中间 clone、预分配 canonical key 或改为类型化键；不实现字符串驻留池；
4. 每次只选择一个已证实热点，完成后重新跑完整门禁。

本阶段不自动实施所有候选项。

## 审计与错误顺序契约

HashMap 只用于 lookup，不遍历它产生错误日志或 Sheet 顺序。错误顺序保持：

```text
1. 成本明细输入顺序
   - UNMAPPED_COST_ITEM
   - MISSING_AMOUNT
2. 数量页输入顺序
   - DUPLICATE_WORK_ORDER_KEY
3. 有效数量事实输入顺序
   - MOH_BREAKDOWN_MISMATCH
   - TOTAL_COST_MISMATCH
4. 唯一工单首次出现顺序
   - NON_POSITIVE_UNIT_COST
```

同一行多个问题的先后顺序保持现有代码行为。禁止去重、抽样、只保留计数、并行后不稳定合并或 check-only 少生成错误。

必须持续满足：

```text
error_log_count == error_issues.len()
sum(issue_type_counts.values()) == error_log_count
```

## 业务兼容契约

- 金额继续使用 `Decimal`；
- 缺失完工金额按 0 汇总并记录 `MISSING_AMOUNT`；
- GB 总成本包含委外加工费；
- SK 总成本包含委外加工费和软件费用；
- 独立成本项不属于制造费用；
- 独立成本项不参与 Modified Z-score；
- 仅正单位成本参与对数和 Modified Z-score；
- 异常阈值不变；
- 月份只作标签，异常池继续跨统计期间按产品和生产类型建立；
- qty 输出保留重复有效行；
- 工单分析使用每个重复工单的首次出现行；
- 白名单顺序和白名单内排序不变；
- 三张 Sheet、列顺序、格式和元数据不变。

## 性能采样设计

### CPU profiler

Windows 本机已有 WPR/WPA。profiling 时通过 Cargo 环境覆盖为 release 优化 binary 生成可解析符号，不永久修改 `Cargo.toml` 的 release profile。正式性能判定前清除覆盖并重新构建普通 release。

优先关注：

```text
rows_to_maps / rows_to_indexed
forward_fill_with_rules
split_detail_and_qty
build_fact_bundle
work_order_key
build_qty_sheet_rows
build_work_order_anomaly_sheet
score_rows
build_quality_metrics
build_flat_sheet
CellValue / String clone 与分配路径
```

若 WPR 无法获得完整符号，依次降级为 Visual Studio CPU Usage 或现有 benchmark 下的细分日志计时；不为 profiling 增加生产依赖。

### 正式计时

#### Check-only 时间门禁

复用并扩展 `tests/rust_oracle/benchmark.py` 的 test-only benchmark helper，新增独立 check-only 测量函数，并由 `tests/test_rust_check_only_benchmark.py` 固定 GB/SK 各 5 个成对 round；不改变现有 full-pipeline `repeats=3` 路径，也不进入生产 binary。最终执行：

```powershell
uv run python -m pytest tests/test_rust_check_only_benchmark.py -q --basetemp .pytest-tmp/rust-check-only-benchmark
```

Rust/Python 的门禁比较值统一命名为 `payload_total_seconds`，并使用同一计时边界：

- 起点：CLI 参数解析、路径解析、输入校验和 pipeline 配置完成后，即将进入 ingest；
- 终点：完整内存 `WorkbookPayload` 构建并返回后，任何运行摘要统计、export 或结果序列化开始前；
- 排除：Cargo build、进程启动、CLI 参数解析、路径解析、export、日志/JSON/benchmark 文本等结果序列化；
- 包含：ingest、normalize、split/fact、analysis、presentation，以及这些阶段之间为构建 payload 必需的内存工作。

Rust 侧继续读取运行摘要中的 `stage_timings.total`，但实施时必须先收紧该字段的定义：计时器紧邻 ingest 之前启动，并在 `build_workbook_payload` 返回后立即停止和写入；非 payload 所需的 run-count/issue-count 汇总移到停止点之后。当前 `run.rs` 的停止点位于 run-count 计算及可选 export 之后，不能直接作为本门禁证据；调整后 `export` 仍作为独立 stage 记录，不计入 `total`。

Python 生产代码当前只记录 `ingest / normalize / fact / analysis / presentation`，不存在 `stage_timings.total`。test-only helper 使用 `perf_counter` 在同一 payload 构建调用边界内单独测量并返回 `python_payload_total_seconds`；不得用阶段求和得到的 `payload_total_seconds` 代替，也不得假定或要求 Python 生产 payload 新增 `stage_timings.total`。helper 的配置准备在计时开始前完成，payload 返回后的状态应用、日志和结果序列化在计时停止后执行。

执行协议：

1. 每个 pipeline 固定同一个输入文件及其 SHA-256；
2. Rust 使用 release executable 的 `--check-only --benchmark` 路径；Python test-only helper 使用相同 pipeline 配置和完整 check-only payload 工作量，两端都不写 workbook；
3. Rust、Python 各预热 1 次；
4. 正式执行 5 个成对 round；奇数 round 先 Rust 后 Python，偶数 round 先 Python 后 Rust，降低缓存和温度偏差；
5. 每轮读取 Rust 内部 `stage_timings.total` 和 Python test-only helper 返回的 `python_payload_total_seconds`，按上述统一边界比较；
6. 报告两端 median、min、max，并报告 Rust 各 stage 的 5 次中位数；
7. 无并发成本任务，不使用单次最快值作为结论。

#### Full-pipeline 门禁

现有 full-pipeline harness 和测试保持 `repeats=3`，本轮不把它改成 5 次。最终执行：

```powershell
uv run python -m pytest tests/test_full_rust_cli_benchmark.py -q --basetemp .pytest-tmp/full-rust-benchmark
```

GB、SK 均必须得到现有 `VALIDATED` verdict。文中“5 次正式运行”仅指新增的 check-only 时间门禁和下述 Peak Working Set 门禁。

证据完整性也是硬门禁：GB 或 SK 样本缺失、样本解析失败、任一 pipeline 未形成 5 个有效成对 check-only round，或对应 pytest 结果为 `skipped` / 未收集，均不构成通过证据。GB、SK 必须分别具备有效五轮结果，且 full-pipeline harness 分别返回 `VALIDATED`，才允许宣称性能验收通过。

### Peak Working Set 采集协议

Peak Working Set 只比较同一 Rust 管线优化前后的直接 executable，不把 Cargo 或 PowerShell 自身计入：

1. Phase 0 将普通 release executable 复制到 `rust/target/perf/baseline/`；各阶段将 current executable 复制到对应 `rust/target/perf/<phase>/`。`rust/target/` 已是生成目录，不提交这些 binary 和结果。
2. 每个结果记录 pipeline、输入 SHA-256、binary SHA-256、Git HEAD、工作树 diff 标识、命令参数和原始字节值，不记录敏感工作簿内容。
3. PowerShell 固定 `-WorkingDirectory <repo-root>`，并使用 `Start-Process -PassThru -WindowStyle Hidden` 直接启动 `costing-calculate.exe <pipeline> --input <absolute-fixed-sample> --check-only --benchmark`；记录的 SHA-256 必须来自这个显式绝对输入路径。
4. 从进程启动到 `HasExited` 为采样边界；每 50ms 调用 `Refresh()` 并读取该 Rust 进程的 `PeakWorkingSet64`，保留观测到的最大值。Rust CLI 在此路径不启动业务子进程，因此统计对象仅为该 executable。
5. baseline/current 各预热 1 次，再执行 5 个成对 round；奇数 round 先 baseline 后 current，偶数 round 先 current 后 baseline。
6. 五次原始值和中位数保存为本地 JSON，路径位于 `rust/target/perf/results/`。
7. GB、SK 分别判定：current 中位数不得高于同批 baseline 中位数的 105%，目标为下降。

CPU 时间的 Rust/Python 成对比较与内存的 baseline/current 成对比较分开执行，不能混用两组对照对象。

## 测试策略

### Table 模块

覆盖：

- 短行补 Blank；
- 长行截断；
- 重复列名按最后一列解析；
- 别名标准化后的重复列名；
- 其他 Schema 的 ColumnId 返回 `INTERNAL_ERROR`，即使 slot 在范围内；
- 两个逻辑内容相同但 SchemaId 不同的表仍具有相同业务序列化和逻辑比较结果；
- 追加列不改变旧 ColumnId；
- `ensure_or_reuse_derived_column` 原子同步 Schema 与所有行；
- `try_update_rows` 可修改已有单元格但不能改变行长度或 SchemaId；
- `try_retain_rows` 可传播列访问错误，不静默过滤；
- 月份列缺失时物理追加并插入展示顺序；
- 重复期间列时，月份展示位置在第一个期间列名之后，派生值读取最后一个期间槽位；
- 已有月份/Filled 列复用并覆盖，不新增重复槽位；
- `take` 移动值后原槽位为 Blank；
- 重复展示列在最后一次出现前克隆、最后一次出现时移动；
- 非法 ID 返回错误、不 panic；
- 必填列一次报告全部缺列；
- 可选列缺失返回 None。

### Normalize / Split

迁移并保留现有测试：

- 表头扁平化和别名；
- 汇总行过滤；
- 集成车间供应商不向下填充且不污染后续值；
- 成本项目填充；
- Filled 成本项目缺列行为；
- 月份列位置和月份补零；
- 输入已带月份列时覆盖最后一个同名槽位且不移动原展示位置；
- 输入缺少期间列时不新增或覆盖月份列；
- 输入已带 Filled 成本项目列时覆盖最后一个同名槽位且不追加；
- 严格月份范围；
- 源键列部分缺失时仍返回固定的月份、产品编码、工单编号、工单行号四个名称；
- 数量、明细、费用和忽略行分类；
- 行顺序和输出列顺序。
- 重复源列不导致三张业务 Sheet 输出重复 contract 列。

### Fact

保留现有业务测试并补充：

- 每个 typed bucket 的累加；
- 制造费用总额与明细双累加；
- GB/SK 独立成本项索引和稳定顺序；
- 有效数量与总成本过滤计数；
- duplicate counts 和错误顺序；
- qty 保留重复、唯一索引保留首次出现；
- quality 复用结果；
- 所有 ErrorIssue 类型、数量、字段和值；
- Decimal 除法/溢出继续返回 Blank 而不 panic。

### Presentation

覆盖：

- 恰好三张 Sheet 及其顺序；
- 不生成产品维度；
- 列名、列顺序和内部字段隔离；
- 空数据保留 Schema；
- error log 移动后内容完整；
- quality、timing、freeze panes、auto filter、列宽和 number formats；
- GB 不出现 SK 软件费用列；
- SK 软件费用列位置正确。

clone 是否消除由所有权签名、代码审查和 profiler 共同证明，不在普通单元测试中写易抖动的 clone/时间断言。

### 完整回归

每阶段至少运行：

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --manifest-path rust/Cargo.toml
```

最终运行：

```powershell
uv run python -m pytest tests -q --basetemp .pytest-tmp/python-regression
```

同时执行现有 Rust/Python runtime contract、workbook contract 和真实 GB/SK benchmark。

`tests/test_rust_check_only_benchmark.py` 只承载严格性能门禁和 runtime 摘要对比，不写业务 workbook；现有 `tests/test_full_rust_cli_benchmark.py` 继续承载三次 full-pipeline 与 workbook 门禁。

Workbook 使用语义比较而非 ZIP 二进制比较，至少校验：

- Sheet 名称与顺序；
- 行数、列数和列名；
- 每个单元格值和空值；
- Decimal 数值；
- number formats；
- freeze panes；
- auto filter；
- 固定列宽；
- 不应存在的 Sheet 和列。

纯性能重构不得更新 contract baseline。

## 最终验收

以下条件必须全部满足：

| 门禁 | 要求 |
|---|---|
| Rust unit/integration tests | 全部通过 |
| Python oracle/regression | 全部通过 |
| GB runtime contract | 完全一致 |
| SK runtime contract | 完全一致 |
| GB workbook contract | 语义完全一致 |
| SK workbook contract | 语义完全一致 |
| GB check-only 性能 | Rust release median <= Python median |
| SK check-only 性能 | Rust release median <= Python median |
| GB full pipeline | 现有 `repeats=3` benchmark verdict 为 `VALIDATED` |
| SK full pipeline | 现有 `repeats=3` benchmark verdict 为 `VALIDATED` |
| 性能证据完整性 | GB、SK 各有 5 个有效成对 check-only round；样本缺失或 pytest skipped/未收集不得视为通过 |
| GB/SK Peak Working Set | 各 5 次 baseline/current 成对运行；current 中位数不高于 baseline 的 105% |
| 非目标 stage | 回退超过 5%时必须解释并复测 |
| 文档 | README 与 AGENTS.md release 口径一致 |

Phase 1–3 均必须完成并通过各自 contract gate。Phase 3 完成后若全部硬门禁已满足，则停止，不进入 Phase 4；只有此时仍未达标才允许逐个处理 profiler 已证实的 Phase 4 热点。Phase 4 每次只改一个热点，每次都重跑完整 contract、时间和内存门禁；达标即停止，否则重新 profiling 后才允许选择下一个热点。

## 风险与控制

| 风险 | 控制 |
|---|---|
| ColumnId 来自其他 Schema 或与行槽位错位 | ID/行携带 SchemaId；IndexedTable 原子扩 Schema 和全部行 |
| 重复列名语义变化 | 固定“最后一列生效”，新增回归测试 |
| key_columns 因源列缺失而缩短 | 继续保存固定四名称 `Vec<String>`，热路径 ID 另行解析 |
| HashMap 顺序不稳定 | HashMap 只 lookup；输出遍历输入向量或显式索引 |
| 合并扫描改变错误顺序 | 使用 PreparedQtyRow，按四段既有顺序生成错误 |
| 唯一工单取错重复行 | 只记录首次出现索引并测试 |
| 提前消费 FactBundle | 先构建 quality/anomaly，再解构移动大字段 |
| 迁移期内存翻倍 | 不同时保留全量旧 Map 行和 IndexedRow 副本 |
| profiler binary 与正式 binary 不同 | profiler 只定位热点；普通 release 重新计时 |
| 偷减 check-only 工作量 | check-only 继续执行完整分析与 presentation |
| 性能噪声 | check-only 预热 1 次并成对交错运行 5 次；full pipeline 沿用现有 3 次 harness |
| 覆盖用户已有未提交修改 | 局部 patch；只提交明确路径；检查 diff |
| 文本换行或编码变化 | UTF-8、局部 apply patch、`git diff --check` |

## 依赖决策

- 已检查仓库现有依赖和标准库能力；
- 使用 `std::collections::HashMap/HashSet`、现有 `rust_decimal` 和现有测试基础设施；
- 不增加生产依赖；
- 不安装 benchmark 框架作为实现前置条件；
- WPR/WPA 是本机开发工具，不进入仓库依赖；
- 不手写新的列式计算引擎，只实现满足当前固定表格模型的最小 Schema/Row 深模块。

## 文档修改范围

`README.md` 与 `AGENTS.md`：

- 正式 build 命令使用 `cargo build --release`；
- 正式 run/check-only/benchmark 命令使用 `cargo run --release`；
- 自动输入/输出和显式路径两组示例均同步；
- 补充 dev profile 不作为真实数据性能口径；
- `cargo test` 和 `cargo fmt` 不加 release；
- Python oracle 命令不变；
- 不顺带修改其他历史文档。

修改和审查分离；文档更新后使用只读文档审查，并执行 `git diff --check`。

## 伪代码草案

### 顶层实施与停止流程

```text
# [伪代码草案]
# 目标：Phase 1–3 均完成且逐阶段验收；Phase 4 仅在最终硬门禁仍未满足时进入
# 输入：
# - gb_sample / sk_sample: 固定真实 Excel 样本
# - python_oracle: 当前 Python 对照实现
# - rust_release_binary: 普通 release executable
# - profiler: WPR/WPA 或等价 CPU sampling 工具
# 输出：
# - success_result: 全部契约一致、GB/SK Rust median <= Python median、full verdict 和 Peak Working Set 门禁通过
# - retry_result: 性能数据噪声或 profiler 证据不足时重新采样
# - error_result: 契约回归、错误顺序变化或阶段性能明显回退

baseline = measure_check_only_release(gb_sample, sk_sample, warmup=1, runs=5)
profile = collect_cpu_profile(sk_sample)

for phase in [presentation_ownership, indexed_rows, typed_fact]:
    implement_phase_with_tests(phase)

    correctness = run_all_contracts()
    if not correctness.passed:
        return error_result("CONTRACT_REGRESSION", correctness.diff)

    current = measure_check_only_release(gb_sample, sk_sample, warmup=1, runs=5)
    if current.has_unexplained_stage_regression_over(5_percent):
        return retry_result("REPROFILE_STAGE_REGRESSION", current.stage_diff)

    profile = collect_cpu_profile(sk_sample)

if current.gb_rust_median <= current.gb_python_median \
   and current.sk_rust_median <= current.sk_python_median \
   and full_pipeline_benchmarks_are_validated() \
   and peak_working_set_gate_passed():
    return success_result(current)

for residual_hotspot in profile.confirmed_residual_hotspots():
    # 只处理证据明确的热点，避免一次引入多个变量。
    implement_one_residual_optimization(residual_hotspot)
    current = rerun_contracts_and_benchmarks()
    if current.all_contracts_passed \
       and current.gb_rust_median <= current.gb_python_median \
       and current.sk_rust_median <= current.sk_python_median \
       and full_pipeline_benchmarks_are_validated() \
       and peak_working_set_gate_passed():
        return success_result(current)
    profile = collect_cpu_profile(sk_sample)

return error_result(
    "PERFORMANCE_TARGET_NOT_MET",
    "已完成已证实优化，但 Rust 中位数仍高于 Python",
)
```

### Normalize 索引化

```rust
// [伪代码草案]
// 目标：移动原始行、一次解析列 ID，并保持列/月份/填充契约
// 输入：RawWorkbook、PipelineConfig、可选 MonthRange
// 输出：NormalizedCostFrame；缺列或非法月份返回结构化错误

fn normalize_workbook(raw, config, month_range) -> Result<NormalizedCostFrame> {
    let normalized_range = normalize_optional_month_range(month_range)?;

    let mut source_names = flatten_headers(raw.header_rows);
    normalize_key_column_names(&mut source_names);

    // IndexedTable 原子建立 Schema、source display order 和同一 SchemaId 的行。
    let mut table = IndexedTable::from_raw(source_names, raw.rows)?;
    let columns = NormalizeColumns::resolve(table.schema())?;

    table.try_retain_rows(|row| Ok(!is_total_row(row, &columns)?))?;
    forward_fill_with_rules(&mut table, &columns)?;

    // 已有派生列复用并覆盖；仅缺失时原子追加槽位和展示位置。
    let month_id = if let Some(period_id) = columns.period {
        let month_values = derive_month_values(table.rows(), period_id)?;
        Some(table.ensure_or_reuse_derived_column(
            MONTH_COLUMN,
            DerivedColumnPosition::AfterFirstSourceName(PERIOD_COLUMN),
            month_values,
        )?)
    } else {
        // 保持当前行为：无期间列时不新增或覆盖月份列。
        table.schema().optional(MONTH_COLUMN)
    };
    let filled_values = derive_filled_cost_item_values(table.rows(), &columns)?;
    table.ensure_or_reuse_derived_column(
        FILLED_COST_ITEM_COLUMN,
        DerivedColumnPosition::End,
        filled_values,
    )?;

    if let Some(range) = normalized_range {
        table.try_retain_rows(|row| {
            month_in_range(row, month_id, columns.period, &range)
        })?;
    }

    Ok(NormalizedCostFrame {
        // 兼容元数据固定返回四个名称，不要求源表实际包含这些列。
        key_columns: KEY_COLUMNS.iter().map(|name| (*name).to_string()).collect(),
        table,
    })
}
```

### Fact 构建

```rust
// [伪代码草案]
// 目标：类型化聚合成本、缓存工单键，并保持错误顺序与数量行语义
// 输入：SplitResult、PipelineConfig
// 输出：FactBundle；缺少必要列返回 INVALID_INPUT

fn build_fact_bundle(split, config) -> Result<FactBundle> {
    let columns = FactColumns::resolve(&split.schema)?;
    let mut amounts_by_key = HashMap::new();
    let mut error_issues = Vec::new();

    for row in &split.detail_rows {
        let key = build_work_order_key(row, &columns);
        let cost_item = text_ref(row, columns.cost_item);
        let amount = decimal_at(row, columns.completed_amount);

        match classify_cost_item(cost_item, config) {
            Mapped(classification) => {
                if amount.is_none() {
                    // 仅已映射成本项记录 MISSING_AMOUNT，保持当前 continue 语义。
                    error_issues.push(missing_amount_issue(&key, row));
                }
                amounts_by_key
                    .entry(key)
                    .or_insert_with(|| CostAmounts::new(config))
                    .add(classification, amount.unwrap_or(Decimal::ZERO));
            }
            Unmapped if !cost_item.trim().is_empty() => {
                error_issues.push(unmapped_cost_item_issue(key, cost_item));
            }
            Unmapped => {}
        }
    }

    let mut prepared_rows = Vec::new();
    let mut duplicate_counts = HashMap::new();

    for row in split.qty_rows {
        if !has_positive_completed_qty(&row, &columns) {
            filtered_invalid_qty_count += 1;
            continue;
        }
        if !has_completed_amount(&row, &columns) {
            filtered_missing_total_amount_count += 1;
            continue;
        }

        let prepared = prepare_qty_row(row, &columns)?;
        *duplicate_counts.entry(prepared.key.clone()).or_default() += 1;
        prepared_rows.push(prepared);
    }

    // 重复错误仍按数量页输入顺序生成，不遍历 HashMap。
    for prepared in &prepared_rows {
        if duplicate_counts[&prepared.key] > 1 {
            error_issues.push(duplicate_key_issue(prepared, duplicate_counts[&prepared.key]));
        }
    }

    let mut qty_rows = Vec::with_capacity(prepared_rows.len());
    let mut unique_work_order_indices = Vec::new();
    let mut seen = HashSet::new();

    for prepared in prepared_rows {
        let amounts = amounts_by_key
            .get(&prepared.key)
            .cloned()
            .unwrap_or_else(|| CostAmounts::new(config));
        let audit = calculate_reconciliation(&amounts, prepared.completed_total, config);
        append_reconciliation_issues_in_current_order(
            &mut error_issues,
            &prepared,
            &audit,
        );

        let index = qty_rows.len();
        qty_rows.push(build_qty_fact_row(prepared, amounts, audit));
        if seen.insert(qty_rows[index].work_order_key.clone()) {
            unique_work_order_indices.push(index);
        }
    }

    append_non_positive_unit_cost_issues(
        &qty_rows,
        &unique_work_order_indices,
        &mut error_issues,
    );

    Ok(FactBundle { /* 已计算字段与移动后的行 */ })
}
```

### Presentation 所有权流

```rust
// [伪代码草案]
// 目标：先完成借用分析，再直接移动明细、数量和错误数据到最终 payload
// 输入：FactBundle、PipelineConfig、StageTimings、月份空结果标志
// 输出：WorkbookPayload；三张 Sheet 及其契约不变

fn build_workbook_payload(bundle, config, timings, empty_result) -> Result<WorkbookPayload> {
    let quality_metrics = build_quality_metrics(&bundle, empty_result);
    let work_order_sheet = build_work_order_anomaly_sheet(&bundle, config);

    let FactBundle {
        schema,
        detail_display_columns,
        detail_rows,
        qty_display_columns,
        qty_rows,
        error_issues,
        ..
    } = bundle;

    // 消费行，把 CellValue 直接移动到最终二维数组。
    let detail_sheet = build_detail_sheet_consuming(
        &schema,
        detail_display_columns,
        detail_rows,
    )?;
    let qty_sheet = build_qty_sheet_consuming(
        &schema,
        qty_display_columns,
        qty_rows,
        config,
    )?;

    let error_log_count = error_issues.len();
    let sheets = vec![detail_sheet, qty_sheet, work_order_sheet];
    ensure_no_product_dimension(&sheets)?;

    Ok(WorkbookPayload {
        sheet_models: sheets,
        quality_metrics,
        error_log_count,
        error_log: error_issues,
        stage_timings: timings,
    })
}
```

## 后续步骤

1. 用户复核本设计文档；
2. 设计确认后使用 `superpowers:writing-plans` 生成按小提交拆分的实施计划；
3. 实施时先完成 release 文档口径和新基线，再依序执行 Phase 1–3；
4. 每阶段通过 Rust reviewer、数据审计和 contract/performance gate；
5. 只有 profiler 证明仍有必要时才进入 Phase 4。

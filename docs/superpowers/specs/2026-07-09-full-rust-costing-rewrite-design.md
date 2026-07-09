# Full Rust Costing Rewrite Design

## 背景

当前项目是金蝶 ERP 成本计算 ETL 工具，Python 默认生产链路已经收敛为 3 张工作表：

- `成本计算单总表`
- `成本计算单数量聚合维度`
- `成本分析工单维度`

`成本分析产品维度` 已退出默认 workbook 契约。此前 `rust_xlsxwriter` sidecar spike 已验证 Excel 写出层具备明显性能优势，但该 spike 只覆盖 writer，不覆盖原始 `.xlsx` 读取、ETL、异常分析、质量指标和 CLI 入口。

本设计选择一次性全 Rust 重写路线：Rust CLI 作为最终主入口，第一版同时覆盖 GB + SK，Python 只作为迁移期 oracle 和测试对照。

## 目标

实现一个 Rust CLI，完整替换当前 Python 成本核算 ETL 默认生产链路。

目标范围：

- 新建 Rust CLI 作为最终主入口，例如：
  - `costing-calculate gb --input <xlsx> --output <xlsx>`
  - `costing-calculate sk --input <xlsx> --output <xlsx>`
  - `costing-calculate gb --check-only --benchmark`
  - `costing-calculate sk --check-only --benchmark`
- 第一版同时覆盖 GB + SK。
- Rust 直接读取原始金蝶 `.xlsx`，不依赖 Python 预处理 CSV、Parquet 或 manifest。
- Rust 完整实现当前默认生产链路：
  - 原始 workbook 读取
  - 双层表头处理
  - 字段识别与标准化
  - 汇总行过滤
  - 供应商字段填充规则
  - 月份过滤
  - 成本明细与数量页拆分
  - 数量聚合维度补强
  - 独立成本项规则：GB 的 `委外加工费`，SK 的 `委外加工费` 和 `软件费用`
  - 总成本勾稽
  - 工单维度异常分析
  - Modified Z-score、异常等级、异常主要来源、异常明细解释
  - 质量指标、`error_log_count`、阶段耗时
  - 3-sheet workbook 写出
- Rust 输出 workbook 必须严格对齐当前 Python 默认 3-sheet 契约。
- Python 当前实现保留为迁移期 oracle，用于 contract 对比、fixture 对比、真实样本对比和 benchmark 基线。

## 非目标

- 不新增新的业务口径。
- 不恢复第 4 张 sheet。
- 不重新设计异常算法。
- 不做 GUI，GUI 已退役。
- 不引入数据库、服务端 API 或长期任务系统。
- 不为了 Rust 重写顺手修改字段名称、sheet 顺序、质量指标含义或 error model。
- 不在 Rust 全量通过前删除 Python 实现。

## 产品维度退场契约

`成本分析产品维度` 直接退场，不进入 Rust 新系统。

Rust 新系统必须满足：

- 不实现 `成本分析产品维度` sheet。
- 不实现产品维度分析、产品维度分块渲染、IQR 相关历史逻辑。
- 默认 workbook 只允许输出 3 张 sheet。
- contract validator 必须显式禁止输出 `成本分析产品维度`。

Python 里的 legacy 产品维度 helper 可以作为后续 Python 退场清理项删除；它不属于 Rust 新系统迁移对象。

## 完成标准

- GB + SK 的 contract fixtures 全部通过。
- GB + SK 至少各一份真实原始 workbook 通过严格对比。
- Rust 输出 workbook 可被 `openpyxl` 读取，且可人工用 Excel 或 WPS 打开。
- Rust workbook 与 Python oracle 在以下内容上对齐：
  - sheet count
  - sheet names
  - sheet 顺序
  - columns
  - row counts
  - column counts
  - headers
  - 关键单元格值
  - 数字精度
  - 冻结窗格
  - 筛选范围
  - 关键数字列格式
- `--check-only --benchmark` 能输出与当前 Python 类似的阶段耗时、行数、质量摘要。
- Rust 总耗时不应比当前 Python 更慢。
- Excel 写出层应复用已验证的 `rust_xlsxwriter` 能力。

## 架构

Rust 新系统按“可对比、可定位、可替换”的边界拆模块。一次性全 Rust 重写不等于单文件大脚本。

建议目录结构：

```text
rust/
  Cargo.toml
  crates/
    costing-cli/
    costing-core/
    costing-xlsx/
    costing-oracle-tests/
```

### `costing-cli`

职责：

- 命令行参数解析。
- pipeline 选择。
- 输入输出路径校验。
- `--check-only`。
- `--benchmark`。
- 输出结构化运行摘要。

约束：

- 不放业务规则。
- 不直接写 Excel 单元格。
- 不调用 Python 生产逻辑。

### `costing-core`

职责：

- 领域模型。
- ETL。
- 分析。
- 质量指标。
- error_log 统计。
- GB / SK pipeline config。

核心输入：

- 读取后的 workbook rows。
- pipeline config。
- 可选月份过滤参数。

核心输出：

- 与当前 Python `WorkbookPayload` 等价的结构。
- 3 个 `SheetModel`。
- 质量指标。
- `error_log_count`。
- stage timings。

约束：

- 不直接读写 `.xlsx` 文件。
- 业务计算必须使用明确类型表达金额、数量、单价。

### `costing-xlsx`

职责：

- 原始 `.xlsx` 读取。
- 双层表头展开。
- 单元格类型归一化。
- 结果 workbook 写出。

读取侧：

- 处理 sheet、双层表头、中文字段、空值、数字、日期或月份字段。
- 输出 reader debug snapshot，供 oracle 对比。

写出侧：

- 使用 `rust_xlsxwriter`。
- 实现 3-sheet workbook 样式契约。
- 不写产品维度 sheet。

### `costing-oracle-tests`

职责：

- 迁移期验证。
- 调 Python 当前实现生成 oracle 输出。
- 调 Rust CLI 生成候选输出。
- 做分层 snapshot 对比。
- 做最终 workbook 对比。
- 做同场 benchmark。

约束：

- Python oracle 不参与 Rust 生产运行。
- 迁移完成后，该 crate 可保留为回归测试，也可缩减。

## 数据流

```text
CLI args
  -> PipelineConfig(GB/SK)
  -> read_raw_xlsx()
  -> normalize_headers()
  -> normalize_rows()
  -> apply_month_filter()
  -> split_detail_and_qty()
  -> build_fact_bundle()
  -> build_qty_sheet()
  -> build_work_order_anomaly_sheet()
  -> build_quality_metrics()
  -> WorkbookPayload(3 sheet models + metrics + timings)
  -> write_xlsx()
  -> run summary
```

## 类型与依赖决策

建议依赖：

- CLI：`clap`
- Excel 写出：`rust_xlsxwriter`
- 精确小数：`rust_decimal`
- 时间与月份：`chrono`
- CSV 或测试中间产物：`csv` / `serde`
- Excel 读取：需要单独 spike 决策，候选包括 `calamine`

类型原则：

- 金额、数量、单价优先用 `rust_decimal::Decimal`。
- 字段名、sheet 名保留 UTF-8 字符串。
- 单元格值使用统一 enum，例如：

```rust
enum CellValue {
    Blank,
    Text(String),
    Decimal(Decimal),
    DateLike(String),
}
```

Pipeline 差异只放在 `PipelineConfig`：

- 产品白名单顺序。
- 独立成本项。
- 原始文件匹配规则。
- 字段别名与识别规则。
- SK 特有 `软件费用`。

## 验证策略

一次性全 Rust 重写必须有分层 oracle，不能只比较最终 `.xlsx`。

### 1. 读取层对比

Rust 读取原始 `.xlsx` 后，输出 debug snapshot：

- sheet 名。
- 原始行数。
- 原始列数。
- 双层表头展开结果。
- 每列前 N 个归一化值。
- 空值统计。

目标是确认 reader 没有把空值、数字、中文列名、合并表头读歪。

### 2. 标准化 / 拆表层对比

对比：

- 汇总行过滤前后行数。
- 向下填充后的关键字段。
- `集成车间` 供应商字段不填充规则。
- 成本明细行数。
- 数量页行数。
- 月份过滤摘要。

### 3. 事实表 / 分析层对比

对比：

- 数量聚合维度关键金额列。
- 独立成本项列：GB `委外加工费`，SK `委外加工费` 和 `软件费用`。
- 总成本勾稽列。
- 工单分析基础事实。
- Modified Z-score 输入池。
- 异常等级。
- 异常主要来源。
- 复核原因。
- 异常明细解释。
- `error_log_count`。
- 质量指标。

### 4. SheetModel 层对比

在写 Excel 前对比 Rust 和 Python 的抽象输出：

- sheet 顺序。
- columns。
- rows。
- number format map。
- freeze panes。
- auto filter range。
- conditional format 规则。

### 5. 最终 workbook 对比

自动化 validator 对比：

- sheet count = 3。
- 禁止出现 `成本分析产品维度`。
- sheet names 完全一致。
- row / column counts 完全一致。
- headers 完全一致。
- 单元格值归一化后比较。
- 关键数字列格式比较。
- freeze panes。
- auto filter。
- workbook 可被 `openpyxl` 打开。
- 至少 GB/SK 各一次人工 Excel/WPS 打开确认。

## 值归一化规则

- `None`、空字符串、Excel blank 视为空值等价。
- 数字列用 Decimal 比较，默认容差 `0.000001`。
- 金额、数量、单价按业务精度比较，不用二进制 float 直接相等。
- 文本列精确比较。
- 前后空格按当前 Python 契约决定是否保留；Rust 不能私自 trim。
- number format 只校验 SheetModel 标记为数字的关键列，不要求所有文本列样式逐格一致。

## 测试输入

第一版验收输入包括：

- GB contract fixtures。
- SK contract fixtures。
- GB 真实样本至少 1 份。
- SK 真实样本至少 1 份。
- 空结果月份过滤 fixture。
- `集成车间` 供应商不填充 fixture。
- 缺失 `本期完工金额` fixture。
- 非正单位成本 fixture。
- 总成本不匹配 fixture。
- 制造费用明细不匹配 fixture。
- SK `软件费用` fixture。

## 性能验收

`--benchmark` 输出阶段耗时：

- ingest
- normalize
- fact
- analysis
- presentation
- export
- total

GB/SK 真实样本各跑 3 次，取中位数。

正式 verdict 使用同场 benchmark：

1. 清理旧输出文件。
2. 跑 Python oracle 3 次。
3. 跑 Rust CLI 3 次。
4. 两边都取中位数。
5. 用两个中位数比较。

要求：

- Rust 总耗时不应慢于当前 Python 同场 baseline。
- Excel export 应保留 `rust_xlsxwriter` 优势。
- 如果 Rust reader 比 Python reader 慢，要单独归因，不把 writer 性能混在一起。

## 失败分类

- `BLOCKED_ENVIRONMENT`：Rust toolchain、测试依赖、样本文件缺失。
- `READER_MISMATCH`：读取层已经与 Python 不一致。
- `ETL_MISMATCH`：标准化、填充、拆表差异。
- `ANALYSIS_MISMATCH`：金额、勾稽、异常分析、质量指标差异。
- `WORKBOOK_MISMATCH`：SheetModel 对齐但最终 xlsx 不一致。
- `PERFORMANCE_REGRESSION`：功能对齐但性能低于 Python。
- `VALIDATED`：GB + SK 功能、workbook、性能全部达标。

## 实施切块

虽然路线是一次性全 Rust 重写，但执行不能是一个巨型提交。以下切块是内部工程里程碑，不代表分阶段上线；最终交付仍要求 GB + SK 全量通过后才能替换入口。

### 1. Rust workspace 与 CLI 骨架

产物：

- `rust/` workspace。
- `costing-cli`。
- `costing-core`。
- `costing-xlsx`。
- `costing-oracle-tests`。
- `gb/sk` 子命令。
- `--input`、`--output`、`--check-only`、`--benchmark`。

验收：

- CLI 参数解析正确。
- 无业务逻辑。
- 输出稳定 JSON 或文本摘要格式草案。

### 2. Excel reader spike hardening

产物：

- Rust 直接读取金蝶 `.xlsx`。
- 双层表头展开。
- 空值、数字、文本、月份归一化。
- reader debug snapshot。

验收：

- GB/SK fixture 读取层与 Python 对齐。
- 至少一份真实 GB 和一份真实 SK 文件可读取。
- 若 reader 库无法可靠读取，必须停下重新选库，而不是绕回 Python 中间文件。

### 3. ETL 标准化与拆表

产物：

- 字段识别与标准化。
- 汇总行过滤。
- 向下填充。
- `集成车间` 供应商字段不填充。
- 月份过滤。
- 成本明细与数量页拆分。

验收：

- GB/SK fixture 层级对比通过。
- 行数、关键字段、月份摘要对齐 Python。

### 4. 事实表与数量聚合

产物：

- detail / qty / work_order / product_summary / error fact 等价结构。
- 数量聚合维度输出列。
- 三大成本项。
- 制造费用细项。
- 独立成本项。
- 总成本勾稽。
- error_log 统计。

验收：

- GB/SK fixture 的数量聚合维度对齐。
- SK `软件费用` 独立成本项对齐。
- Decimal 精度和空值规则对齐。

### 5. 工单异常分析

产物：

- Modified Z-score。
- 有效池计算。
- 异常等级。
- 异常主要来源。
- 复核原因。
- 异常明细解释。
- 非正单位成本 error_log。

验收：

- 异常 fixture 对齐。
- 真实 GB/SK 样本工单分析 sheet 对齐。
- `异常明细解释` 逐项解释一致；除非后续明确批准新文本契约，否则默认完全一致。

### 6. Workbook writer 与最终 validator

产物：

- 3-sheet writer。
- 样式、数字格式、冻结窗格、筛选范围。
- 禁止输出 `成本分析产品维度`。
- workbook validator。

验收：

- workbook 自动对比通过。
- openpyxl 可读。
- GB/SK 各一次人工 Excel/WPS 打开确认。
- 不实现产品维度 legacy sheet。

### 7. 同场 benchmark 与入口切换设计

产物：

- GB/SK 同场 Python oracle vs Rust benchmark。
- 3-run median。
- 完整 verdict。
- 后续 Python 退场清理计划。

验收：

- Rust GB/SK 总耗时不慢于 Python 同场 baseline。
- export 阶段维持 Rust writer 优势。
- Rust CLI 文档完整。
- 明确何时把 README、AGENTS、测试命令切到 Rust CLI。

## 提交策略

- 每个里程碑至少一个清晰提交。
- 不把 unrelated untracked 文件纳入提交。
- 不在 Rust 全量通过前删除 Python。
- 不把 Python oracle 测试结果伪装成生产依赖。
- 产品维度删除分两层：
  - Rust 新系统：从一开始就不实现。
  - Python legacy 清理：等 Rust 全量通过后单独做。

## 风险

- Excel reader 是最大不确定性；`rust_xlsxwriter` 写出已验证，但 Rust 读取金蝶原始 `.xlsx` 尚未验证。
- Python 当前在 pandas、Polars、Decimal 之间有多次转换；Rust 必须把这些隐含转换显性化。
- `异常明细解释` 是高风险字段，既有数字、文本拼接，又有业务解释，必须 fixture 优先覆盖。
- 一次性覆盖 GB + SK 会导致测试矩阵较大；必须用 oracle 测试分阶段定位，而不是只做最终 workbook diff。
- Rust 不实现 `成本分析产品维度`，但 Python legacy 代码仍可能存在。文档必须说明这是迁移期遗留，不是 Rust 漏实现。
- Rust CLI 完成前，不改 README 主命令、不删除 Python；完成后单独做入口切换提交。

## 伪代码草案

### Rust CLI 主流程

```rust
// [伪代码草案]
// 目标：Rust CLI 直接处理 GB/SK 原始 workbook，并输出当前 3-sheet 契约。
// 说明：Python 只在 oracle test 中被调用，生产运行不依赖 Python。

fn main() -> ExitCode {
    let args = CliArgs::parse();

    let result = run_costing_cli(args);

    match result {
        Ok(summary) => {
            print_run_summary(summary);
            ExitCode::SUCCESS
        }
        Err(error) => {
            print_error(error);
            ExitCode::FAILURE
        }
    }
}

fn run_costing_cli(args: CliArgs) -> Result<RunSummary, CostingError> {
    let pipeline = PipelineConfig::from_name(args.pipeline)?;

    validate_input_file(&args.input_path)?;
    validate_output_path(&args.output_path, args.check_only)?;

    let mut timings = StageTimings::new();

    // 为什么先把 Excel I/O 和业务计算分开：reader 是最大不确定性，
    // 独立 snapshot 能让读取差异在进入 ETL 前暴露。
    let raw_workbook = timings.measure("ingest", || {
        xlsx_reader::read_raw_workbook(&args.input_path, pipeline.reader_options())
    })?;

    let normalized = timings.measure("normalize", || {
        normalize_workbook(raw_workbook, &pipeline, args.month_range())
    })?;

    let split = timings.measure("fact", || {
        split_detail_and_qty(normalized, &pipeline)
    })?;

    let artifacts = timings.measure("analysis", || {
        build_analysis_artifacts(split, &pipeline)
    })?;

    let payload = timings.measure("presentation", || {
        build_workbook_payload(artifacts, &pipeline)
    })?;

    if args.check_only {
        return Ok(RunSummary::from_payload(payload, timings, false));
    }

    timings.measure("export", || {
        // Rust 新系统只写当前默认 3 张 sheet；产品维度 sheet 不存在于 payload。
        xlsx_writer::write_three_sheet_workbook(&args.output_path, &payload)
    })?;

    Ok(RunSummary::from_payload(payload, timings, true))
}
```

### 标准化流程

```rust
// [伪代码草案]
// 目标：把原始金蝶 workbook 变成稳定的标准化成本表。

fn normalize_workbook(
    raw: RawWorkbook,
    pipeline: &PipelineConfig,
    month_range: Option<MonthRange>,
) -> Result<NormalizedCostFrame, CostingError> {
    let headers = flatten_two_row_headers(raw.headers, pipeline.header_rules())?;
    let rows = normalize_cell_values(raw.rows, &headers)?;

    let rows = remove_total_rows(rows, pipeline.total_row_rules());
    let rows = forward_fill_with_rules(
        rows,
        pipeline.fill_columns(),
        pipeline.vendor_columns(),
        pipeline.integrated_workshop_name(),
    );

    let rows = apply_month_filter(rows, month_range)?;

    Ok(NormalizedCostFrame::new(headers, rows))
}
```

### 分析产物构建

```rust
// [伪代码草案]
// 目标：构建数量聚合、工单异常、质量指标和 error_log 统计。

fn build_analysis_artifacts(
    split: SplitResult,
    pipeline: &PipelineConfig,
) -> Result<AnalysisArtifacts, CostingError> {
    let standalone_items = pipeline.standalone_cost_items();

    let fact_bundle = build_fact_bundle(
        split.detail_rows,
        split.qty_rows,
        standalone_items,
    )?;

    let qty_sheet = build_qty_sheet(
        &fact_bundle,
        standalone_items,
        pipeline.total_cost_match_rule(),
    )?;

    let work_order_sheet = build_work_order_anomaly_sheet(
        &fact_bundle.work_order_fact,
        standalone_items,
        pipeline.product_order(),
    )?;

    let quality_metrics = build_quality_metrics(&fact_bundle)?;
    let error_log_count = fact_bundle.error_fact.len()
        + count_non_positive_unit_cost_errors(&fact_bundle.work_order_fact);

    Ok(AnalysisArtifacts {
        qty_sheet,
        work_order_sheet,
        quality_metrics,
        error_log_count,
        fact_bundle,
    })
}
```

### Workbook payload 构建

```rust
// [伪代码草案]
// 目标：只构建当前默认 3-sheet workbook，不允许产品维度 sheet 回流。

fn build_workbook_payload(
    artifacts: AnalysisArtifacts,
    pipeline: &PipelineConfig,
) -> Result<WorkbookPayload, CostingError> {
    let detail_sheet = build_detail_sheet_model(&artifacts.fact_bundle.detail_fact)?;
    let qty_sheet = build_qty_sheet_model(&artifacts.qty_sheet)?;
    let work_order_sheet = build_work_order_sheet_model(&artifacts.work_order_sheet)?;

    let sheets = vec![detail_sheet, qty_sheet, work_order_sheet];

    // 为什么这里断言：产品维度已从默认产品契约退场，Rust 新系统不能重新引入旧 sheet。
    ensure_no_sheet_named(&sheets, "成本分析产品维度")?;

    Ok(WorkbookPayload {
        sheets,
        quality_metrics: artifacts.quality_metrics,
        error_log_count: artifacts.error_log_count,
    })
}
```

### Oracle 测试流程

```rust
// [伪代码草案]
// 目标：迁移期把 Python 当前输出作为 oracle，验证 Rust 全链路没有业务回归。

fn validate_rust_against_python(case: OracleCase) -> OracleVerdict {
    let python_output = run_python_oracle(case.pipeline, case.input_path, case.options);
    let rust_output = run_rust_cli(case.pipeline, case.input_path, case.options);

    compare_reader_snapshot(case);
    compare_normalized_snapshot(case);
    compare_fact_snapshot(case);
    compare_sheet_models(case);

    let workbook_result = compare_workbooks(
        python_output.workbook_path,
        rust_output.workbook_path,
        WorkbookCompareOptions {
            treat_blank_and_empty_string_as_equal: true,
            decimal_tolerance: Decimal::new(1, 6),
            check_number_formats_for_numeric_columns_only: true,
            forbidden_sheet_names: vec!["成本分析产品维度"],
        },
    );

    if workbook_result.passed && rust_output.benchmark.total <= python_output.benchmark.total {
        OracleVerdict::Validated
    } else {
        classify_failure(workbook_result, python_output, rust_output)
    }
}
```

## 后续步骤

本 design 获得用户确认后，下一步按 `superpowers:writing-plans` 生成详细实施计划。实施计划必须把上述 7 个里程碑拆成可执行、可验证、可 review 的小任务，并明确每个任务的测试命令与验收输出。

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述 (Project Overview)

本仓库是一个成本计算 ETL 工具，用于处理金蝶 ERP 系统导出的成本计算单 Excel 文件。

### 核心功能
- 清洗原始 Excel 文件（去除表头、扁平化双层表头）
- 默认输出 3 张业务工作表，覆盖成本总表、数量聚合和工单维度异常
- 字段名提取和标准化
- 提供 `--check-only` 预检模式与 `--benchmark` 性能入口，支持先跑分析链路再决定是否落盘

## 架构 (Architecture)

### Rust Workspace 结构

`rust/` 是一个 4-crate workspace（见 `rust/Cargo.toml`），Rust CLI 是 GB/SK 默认主入口：
- `costing-cli`（二进制 `costing-calculate`）— CLI 编排与参数解析（`args.rs`/`main.rs`/`run.rs`）；`low-memory` feature 默认开启
- `costing-core` — 纯逻辑层：Decimal fact、normalize/split、质量指标、Modified Z-score 异常、presentation。不依赖任何 Excel 库
- `costing-xlsx` — 唯一接触 `calamine`（读）与 `rust_xlsxwriter`（写）的 crate；按 `5,000,000` 行×列 slots 自适应启用 low-memory writer，临时目录固定在最终输出目录内
- `costing-oracle-tests` — Rust 运行时契约比较支持

`rust_xlsxwriter` 使用 `rust/Cargo.toml` 中精确 revision 锁定的受控 fork；升级 revision 前必须先在 fork 内通过 lib tests 与 `constant_memory` feature check。

### Python 模块依赖规则 (Module Dependency Rules)

Python legacy/oracle 代码仍保持严格分层，并由 `tests/architecture/test_import_rules.py` 强制：
- `analytics` 不得导入 `etl` 或 `excel`
- `excel` 不得导入 `etl`
- `etl/stages/*` 不得导入 `excel`
- 仅 `etl/costing_etl.py` 可导入 `excel` 模块

### 数据流 (Data Flow)

```
原始 Excel -> costing-xlsx::reader
          -> costing-core::normalize
          -> costing-core::split
          -> costing-core::fact
          -> costing-core::anomaly / quality / presentation
          -> costing-xlsx::writer
```

`src/` 下的 Python 数据流仅作为 legacy/oracle/regression 路径保留。

### 数据契约 (Data Contracts)

**字段映射**：`docs/field_definitions/gb 金蝶字段.txt` 定义了标准字段名

**关键列**：`子项物料编码 `、` 成本项目名称 `、` 工单编号 `、` 工单行号 `、` 年期`

**期间格式**：`年期` 列统一格式化为 `YYYY 年 MM 期`

**产品白名单**：按 `产品编码 + 产品名称` 精确匹配；它只影响 `成本分析工单维度`，不过滤总表和数量聚合维度，分析页按白名单顺序展示

**输出 Sheet**：Rust 默认按顺序输出 3 张表
- `成本计算单总表`
- `成本计算单数量聚合维度`
- `成本分析工单维度`

`成本分析产品维度` 不属于 Rust 输出契约；Python legacy helper 的退场另行审批。

**CLI 行为**（`costing-cli/src/args.rs`）：
- 省略 `--input` 时扫描 `data/raw/<pipeline>/` 下的 `<pipeline>-*.xlsx`：恰好 1 个自动使用，0 个报 `FILE_NOT_FOUND`，多个报 `INVALID_INPUT` 并要求显式指定
- 非 `--check-only` 省略 `--output` 时写入 `data/processed/<pipeline>/<输入stem>_处理后.xlsx`；月过滤在 `.xlsx` 前追加 `_YYYY-MM_YYYY-MM` / `_from_YYYY-MM` / `_to_YYYY-MM` 后缀（与 Python 一致）
- 无论自动还是显式输出路径，均拒绝覆盖已有文件，且禁止输入、输出指向同一文件

**工作簿外输出**：正常运行只落盘 `*_处理后.xlsx`；质量摘要、运行时 `error_log_count`（不单独落盘）和阶段耗时输出到控制台，`--check-only` 只做预检，不写 workbook 或任何外部摘要文件

**error_log 类别**：至少保留 `MISSING_AMOUNT`、`TOTAL_COST_MISMATCH`、`MOH_BREAKDOWN_MISMATCH`、`DUPLICATE_WORK_ORDER_KEY`、`NON_POSITIVE_UNIT_COST`

**工单异常解释字段**：`成本分析工单维度` 保留 `异常等级`、`异常主要来源`、`复核原因`，并使用单列 `异常明细解释` 展示达到关注或高度可疑的异常项；不再输出 `异常池样本数`、`异常池中心log值`、`异常池原始MAD`、`异常池有效MAD`、`相对中位偏离` 五个旧解释列

## 依赖 (Dependencies)

- **Rust**：`rust/rust-toolchain.toml` 指定 stable toolchain；release profile 固定 `codegen-units = 1`，正式性能比较不得用 dev profile
- **Python oracle/regression**：3.11+，由 `uv` 管理项目 `.venv`；除排查解释器问题外不用裸 `python`/`pip`

## 常用命令 (Common Commands)

```bash
# 构建/运行当前主入口（release profile）
cargo build --release --manifest-path rust/Cargo.toml
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- gb --input data/raw/gb/<file>.xlsx --output data/processed/gb/<file>_处理后.xlsx
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- sk --input data/raw/sk/<file>.xlsx --output data/processed/sk/<file>_处理后.xlsx

# Rust 预检 + benchmark（只跑分析链路，不落盘）
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- gb --check-only --benchmark
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- sk --check-only --benchmark

# Rust 测试/格式
cargo test --manifest-path rust/Cargo.toml
cargo fmt --manifest-path rust/Cargo.toml --all --check

# Python legacy/oracle/regression 依赖
uv sync --extra dev

# Python legacy/oracle/regression
uv run python main.py gb
uv run python main.py sk
uv run python main.py gb --check-only --benchmark

# Python 测试（默认排除 slow/benchmark，含 meta）
uv run python -m pytest tests -q --basetemp .pytest-tmp/python-regression
# 最快路径：仅单元 + 契约 + 架构
uv run python -m pytest tests -q -m "not slow and not benchmark and not meta"
# 单测
uv run python -m pytest tests/ -k test_name -q

# 代码检查/格式化
uv run python -m ruff check src tests
uv run python -m ruff format src tests --check
```

## 测试契约 (Test Contracts)

**Baseline 真值**：`tests/contracts/baselines/` 是 workbook / error_log / CLI 契约的唯一来源，README 描述仅供参考。重新生成快照：`uv run python -m tests.contracts.generate_baselines`。

**重构规则**：纯重构不得修改 baseline；仅业务口径变化时才允许更新，并必须说明差异。

**pytest markers**（`pyproject.toml`）：`slow`（真实 Excel 端到端，需 `data/raw` 样本）、`benchmark`（N=5 正式性能基准）、`meta`（测试框架自身元测试）；默认 `addopts` 排除 `slow` 和 `benchmark`，样本缺失时相关用例 skip 而非通过。

**Rust↔Python oracle 比对**：`tests/test_full_rust_cli_oracle.py` 用 Python service 生成 oracle workbook，再调 Rust CLI 生成同输入 workbook，经 `tests/rust_oracle/workbook_compare.py` 比对 sheet 顺序、行列形状、freeze panes、auto filter、列宽、数字格式、样式、条件格式和单元格值。worktree 缺 `data/raw` 样本时用 `COSTING_GB_SAMPLE` / `COSTING_SK_SAMPLE` 环境变量指定本机样本。验证器拒绝任何包含 `成本分析产品维度` 的 Rust workbook。

## 代码风格 (Code Style)

- Python：Ruff 行宽 120、单引号、规则 `E,F,I,B,C4,W,UP,S,T10`（`S101`/`S108` 忽略）；函数/变量 `snake_case`，类 `PascalCase`
- 提交：Conventional Commits（如 `fix(excel): ...`、`test(oracle): ...`、`chore(repo): ...`）

## 当前实现要点

- Rust CLI 是 GB/SK 默认主入口，直接读取原始 `.xlsx`，并只写固定 3-sheet workbook。
- `costing-core` 承担 Decimal fact、质量指标和 Modified Z-score；`costing-xlsx` 负责直接读写 workbook。
- `costing-xlsx` 按 `5,000,000` cell slots 自适应启用 low-memory writer，临时目录固定在最终输出目录；正式性能结论只使用 release profile。
- `main.py` 与 `src/` 仅保留为 Python legacy/oracle/regression 路径；Python retirement 需要单独批准。

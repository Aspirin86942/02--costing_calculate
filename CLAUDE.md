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

### 模块依赖规则 (Module Dependency Rules)

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

**工作簿外输出**：正常运行只落盘 `*_处理后.xlsx`；质量摘要、运行时 `error_log_count`（不单独落盘）和阶段耗时输出到控制台，`--check-only` 只做预检，不写 workbook 或任何外部摘要文件

**工单异常解释字段**：`成本分析工单维度` 保留 `异常等级`、`异常主要来源`、`复核原因`，并使用单列 `异常明细解释` 展示达到关注或高度可疑的异常项；不再输出 `异常池样本数`、`异常池中心log值`、`异常池原始MAD`、`异常池有效MAD`、`相对中位偏离` 五个旧解释列

## 依赖 (Dependencies)

- **Rust**：使用 `rust/rust-toolchain.toml` 指定的 stable toolchain
- **Python oracle/regression**：3.11+，由 `uv` 管理项目 `.venv`

## 常用命令 (Common Commands)

```bash
# 构建/运行当前主入口
cargo build --release --manifest-path rust/Cargo.toml
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- gb --input data/raw/gb/<file>.xlsx --output data/processed/gb/<file>_处理后.xlsx
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- sk --input data/raw/sk/<file>.xlsx --output data/processed/sk/<file>_处理后.xlsx

# Rust 预检 + benchmark
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- gb --input data/raw/gb/<file>.xlsx --check-only --benchmark
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- sk --input data/raw/sk/<file>.xlsx --check-only --benchmark

# Rust 测试/格式
cargo test --manifest-path rust/Cargo.toml
cargo fmt --manifest-path rust/Cargo.toml --all --check

# Python legacy/oracle/regression 依赖
uv sync --extra dev

# Python legacy/oracle/regression
uv run python main.py gb
uv run python main.py sk

# 预检 + benchmark（只跑分析链路，不落 workbook 或任何外部摘要文件）
uv run python main.py gb --check-only --benchmark
uv run python main.py sk --check-only --benchmark

# 测试（使用项目 uv/.venv 环境）
uv run python -m pytest tests -q --basetemp .pytest-tmp/python-regression

# 单测
uv run python -m pytest tests/ -k test_name -q

# 代码检查/格式化
uv run python -m ruff check src tests
uv run python -m ruff format src tests --check
```

## 测试契约 (Test Contracts)

**Baseline 真值**：`tests/contracts/baselines/` 是 workbook / error_log / CLI 契约的唯一来源，README 描述仅供参考。

**重构规则**：纯重构不得修改 baseline；仅业务口径变化时才允许更新，并必须说明差异。

## 当前实现要点

- Rust CLI 是 GB/SK 默认主入口，直接读取原始 `.xlsx`，并只写固定 3-sheet workbook。
- `rust/crates/costing-core` 承担 Decimal fact、质量指标和 Modified Z-score；`costing-xlsx` 负责直接读写 workbook。
- `costing-xlsx` 按 `5,000,000` cell slots 自适应启用 low-memory writer，临时目录固定在最终输出目录；正式性能结论只使用 release profile。
- `main.py` 与 `src/` 仅保留为 Python legacy/oracle/regression 路径；Python retirement 需要单独批准。

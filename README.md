# 成本计算 ETL 工具

金蝶 ERP 成本计算单数据处理工具。

当前操作命令以本文件和 [`AGENTS.md`](AGENTS.md) 为准；验证与性能口径见 [`docs/README.md`](docs/README.md)。已完成的日期方案、Superpowers 计划和 sidecar spike 已从仓库移除。

## 功能
- 清洗原始 Excel 文件（去除表头、扁平化双层表头）
- 默认输出 3 张业务工作表，覆盖成本总表、数量聚合和工单维度异常
- 质量摘要、运行时 `error_log_count`（不单独落盘）和阶段耗时在控制台展示
- 提供 `--check-only` 预检模式和 `--benchmark` 性能入口，便于先跑链路再决定是否落盘
- 字段名提取和标准化

## 安装
Rust CLI 使用 `rust/rust-toolchain.toml` 指定的 stable toolchain。

仅在运行 Python oracle/regression 时安装其开发依赖：
```bash
uv sync --extra dev
```

Python oracle/regression 的开发、测试命令使用项目 `.venv`，由 `uv` 管理；除排查解释器问题外，不使用裸 `python` 或 `pip`。

## 使用
Rust CLI 是当前默认/主入口：

```powershell
cargo build --release --manifest-path rust/Cargo.toml
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- gb
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- sk
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- gb --check-only --benchmark
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- sk --check-only --benchmark
```

上述正式 Rust build/run 命令统一使用 release profile；dev profile 仅适合开发调试，不作为真实数据性能比较口径。

从仓库根目录运行并省略 `--input` 时，CLI 会扫描 `data/raw/<pipeline>/` 下的 `<pipeline>-*.xlsx`：恰好 1 个时自动使用，0 个时报 `FILE_NOT_FOUND`，多个时报 `INVALID_INPUT` 并要求显式指定 `--input`。

以下命令是路径模板，执行前需将 `<file>` 替换为真实文件名；多文件或需要自定义输入、输出路径时，仍可显式指定：

```powershell
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- gb --input data/raw/gb/<file>.xlsx --output data/processed/gb/<file>_处理后.xlsx
cargo run --release --manifest-path rust/Cargo.toml -p costing-calculate -- sk --input data/raw/sk/<file>.xlsx --output data/processed/sk/<file>_处理后.xlsx
```

Python CLI 仅作为 legacy/oracle/regression 路径保留，用于迁移校验与回归；Python retirement 仍需单独批准：

```bash
# GB legacy/oracle/regression
uv run python main.py gb

# SK legacy/oracle/regression
uv run python main.py sk

# 预检 + benchmark（不写 workbook 或任何外部摘要文件）
uv run python main.py gb --check-only --benchmark
uv run python main.py sk --check-only --benchmark
```

Rust 默认 workbook 仍然只包含以下 3 张 Sheet：

- `成本计算单总表`
- `成本计算单数量聚合维度`
- `成本分析工单维度`

`成本分析产品维度` 不属于 Rust 新系统输出契约。

## 性能与内存

- 正式构建统一使用 release profile；`rust/Cargo.toml` 固定 `codegen-units = 1`，以运行性能优先于完整 release 重编译速度。
- CLI 默认启用自适应 low-memory writer。单张 Sheet 达到 `5,000,000` 个 `行数 × 列数` slots 时进入 low-memory，小 workbook 保持标准 writer。
- low-memory 临时目录创建在最终输出目录内，名称以 `.costing-tmp-` 开头；成功或失败均显式清理，不使用系统 `%TEMP%`。
- 大 workbook 使用受控 `rust_xlsxwriter` fork 和固定 ZIP 压缩等级，fork revision 由 `rust/Cargo.toml` 精确锁定。
- 2026-07-12 同机 N=5 验收中，SK normal-mode wall-clock 中位数为 `19.883s`，Peak Working Set 中位数为 `1.361 GiB`；GB 中位数为 `2.475s`。这些是本机验证快照，不是跨机器 SLA。详见 [`docs/rust_rewrite_validation.md`](docs/rust_rewrite_validation.md)。

## 输出说明
Rust CLI 无论自动生成还是显式指定输出路径，均拒绝覆盖已有输出文件，并禁止输入、输出指向同一文件。

每个处理后的工作簿默认按顺序输出以下 3 张 Sheet：
- `成本计算单总表`
- `成本计算单数量聚合维度`
- `成本分析工单维度`

- 非 `--check-only` 模式省略 `--output` 时，默认写入 `data/processed/<pipeline>/<输入stem>_处理后.xlsx`；月过滤会在 `.xlsx` 前追加与 Python 一致的 `_YYYY-MM_YYYY-MM`、`_from_YYYY-MM` 或 `_to_YYYY-MM` 后缀
- 显式传入 `--output` 时使用指定路径
- 不再额外落盘 `*_处理后_error_log.csv` 或 `*_处理后_summary.json`
- 质量摘要、运行时 `error_log_count`（不单独落盘）和阶段耗时仅输出到控制台
- `--check-only` 只做预检与摘要，不写 workbook 或任何外部摘要文件

## 分析输出口径
- `成本计算单总表` 保留成本计算单明细，`本期完工金额`为空时后续分析按 `0` 参与汇总，并继续写入 `error_log` 的 `MISSING_AMOUNT`
- `成本计算单数量聚合维度` 新增三大类/制造费用细项金额、独立成本项金额、单位成本和校验字段，作为工单分析底表
- 成本项目展示口径：
  - `直接材料`、`直接人工`、`制造费用*` 作为三大类成本列展示
  - `委外加工费` -> 独立成本项，仅在数量聚合、工单分析与总成本勾稽中展示
  - `软件费用` -> 仅 `sk` 管线按独立成本项处理，仅在数量聚合、工单分析与总成本勾稽中展示
- 总成本勾稽口径按管线区分：
  - `gb`：`直接材料 + 直接人工 + 制造费用 + 委外加工费 = 总完工成本`
  - `sk`：`直接材料 + 直接人工 + 制造费用 + 委外加工费 + 软件费用 = 总完工成本`
- 工单维度异常分析页：`成本分析工单维度`
  - 粒度：`月份 + 产品编码 + 工单编号 + 工单行`
  - 总体：按产品在整个统计期间内建总体，月份仅作为标签与汇总字段
  - 规则：仅对大于 0 的单位成本计算对数与 Modified Z-score，阈值为 `2.5/3.5`
  - `委外加工费` 与 `软件费用`（仅 `sk`）只展示金额和单位成本，不输出 `log`、`Modified Z-score` 和异常标记，也不参与异常等级和异常主要来源判定
  - 解释字段：`异常明细解释`，仅列出达到 `关注` 或 `高度可疑` 的成本项；每项包含当前值、当前log、基准值、基准log、log偏离、相对偏离、score、有效工单数、原始MAD、有效MAD。`有效工单数` 是同一产品、同一生产类型异常池、同一成本指标下实际参与该项评分的有效工单行数，不是完工数量合计。

## Excel 样式
- 3 张默认平铺表使用浅蓝表头、表头细边框和固定列宽
- 默认冻结 `A2`，真实契约以 `tests/contracts/` baseline 为准
- 开启筛选
- 数字格式：
  - 金额：`#,##0.00`
  - 数量：`#,##0.00`
  - 单价：`#,##0.00`
- 不使用合并单元格

## 目录结构
- `rust/` - 当前主实现的 Cargo workspace
  - `crates/costing-cli` - `costing-calculate` CLI 编排与错误输出
  - `crates/costing-core` - GB/SK ETL、Decimal 成本计算、质量审计与异常分析
  - `crates/costing-xlsx` - 原始工作簿读取和 3-sheet workbook 写出
  - `crates/costing-oracle-tests` - Rust 运行时契约比较支持
- `src/analytics/` - 分析与异常检测模块
  - `contracts.py` - 共享数据结构
  - `fact_builder.py` - fact 构建与 Decimal 工具
  - `qty_enricher.py` - 数量页补强与报表产物编排
  - `table_rendering.py` - 产品维度 legacy/helper 渲染逻辑（不属于默认 workbook 输出）
  - `anomaly.py` / `scoring.py` / `summary.py` / `quality.py` / `errors.py` - 工单异常、评分工具、质量摘要、error_log 契约
- `src/etl/` - ETL 处理模块
  - `costing_etl.py` - 单个工作簿 ETL 主流程
  - `runner.py` - 管线调度、输入匹配与质量日志输出
  - `pipeline.py` - ETL 阶段编排
  - `stages/` - 读取、列识别、清洗、拆分
  - `utils.py` - 工具函数
- `main.py` - Python legacy/oracle/regression 入口
- `src/excel/` - Excel 写出与样式模块
  - `styles.py` / `fast_writer.py` / `workbook_writer.py`
- `src/services/` - CLI 应用服务层与结果对象
- `src/config/` - 配置管理
- `data/raw/` - 原始数据
  - `gb/` - GB 系列原始成本计算单
  - `sk/` - SK 系列原始成本计算单
- `data/processed/` - 处理结果
  - `gb/` - GB 系列处理结果
  - `sk/` - SK 系列处理结果
- `tests/` - 单元测试
- `tests/contracts/` - workbook / error_log / CLI 契约测试
- `tests/architecture/` - 模块依赖与导入边界测试
- `docs/field_definitions/` - 字段定义文件

## 测试
```bash
# Rust CLI checks
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --manifest-path rust/Cargo.toml

# Python oracle/regression 依赖
uv sync --extra dev

# 先确认解释器来自项目 .venv
uv run python -c "import sys; print(sys.executable)"

# Python oracle/regression
uv run python -m pytest tests -q --basetemp .pytest-tmp/python-regression

# Python lint
uv run python -m ruff check src tests

# Python format check
uv run python -m ruff format src tests --check
```

## Contract Baseline
- contract 真值来自 `tests/contracts/baselines/`，不来自 README。
- 纯重构不得修改 baseline；只有业务口径明确变化时才允许更新，并必须说明差异。

## 数据目录说明
- `data/raw/gb/` - GB 系列原始成本计算单
- `data/raw/sk/` - SK 系列原始成本计算单
- `data/processed/gb/` - GB 系列处理结果
- `data/processed/sk/` - SK 系列处理结果
- `docs/field_definitions/` - 字段定义文件 (gb 金蝶字段.txt/html)

## 已移除脚本
以下历史脚本已从仓库移除，功能已由 `src/` 内模块接管：
- `Costing_Calculate.py` - 原始清洗脚本
- `Costing_Calculating_V2.0.py` - V2.0 拆分脚本（已重构到 src/etl/costing_etl.py）
- `抓取所有字段脚本.py` - 字段提取脚本
- `src/excel/sheet_writers.py` - legacy 写出实现，已删除，统一使用 `src/excel/fast_writer.py`

## 已移除
- `Costing_Allocation.py` - 成本分摊脚本（已废弃）

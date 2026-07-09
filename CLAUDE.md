# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述 (Project Overview)

本仓库是一个成本计算 ETL 工具，用于处理金蝶 ERP 系统导出的成本计算单 Excel 文件。

### 核心功能
- 清洗原始 Excel 文件（去除表头、扁平化双层表头）
- 输出 4 张业务工作表，覆盖成本总表、数量聚合、工单维度异常和产品维度摘要
- 字段名提取和标准化
- 提供 `--check-only` 预检模式与 `--benchmark` 性能入口，支持先跑分析链路再决定是否落盘

## 架构 (Architecture)

### 模块依赖规则 (Module Dependency Rules)

**严格分层**，由 `tests/architecture/test_import_rules.py` 强制：
- `analytics` 不得导入 `etl` 或 `excel`
- `excel` 不得导入 `etl`
- `etl/stages/*` 不得导入 `excel`
- 仅 `etl/costing_etl.py` 可导入 `excel` 模块

### 数据流 (Data Flow)

```
原始 Excel -> reader.load_raw_workbook()
          -> 列名标准化 (clean_column_name)
          -> 列推断/重命名 (infer_rename_map)
          -> 删除合计行 (remove_total_rows)
          -> 规则填充 (forward_fill_with_rules)
          -> 拆分为 detail/qty (split_detail_and_qty_sheets)
          -> 构建分析 fact 表 (build_report_artifacts)
          -> 构建 workbook payload (build_workbook_payload)
          -> 写出 Excel (CostingWorkbookWriter)
```

### 数据契约 (Data Contracts)

**字段映射**：`docs/field_definitions/gb 金蝶字段.txt` 定义了标准字段名

**关键列**：`子项物料编码 `、` 成本项目名称 `、` 工单编号 `、` 工单行号 `、` 年期`

**期间格式**：`年期` 列统一格式化为 `YYYY 年 MM 期`

**产品白名单**：`ANALYSIS_PRODUCT_WHITELIST` 定义了目标产品池，按 `产品编码 + 产品名称` 精确匹配；它只影响异常分析与产品维度摘要，不过滤总表和数量聚合维度

**输出 Sheet**：默认按顺序输出 4 张表
- `成本计算单总表`
- `成本计算单数量聚合维度`
- `成本分析工单维度`
- `成本分析产品维度`

**工作簿外输出**：正常运行只落盘 `*_处理后.xlsx`；质量摘要、运行时 `error_log_count`（不单独落盘）和阶段耗时输出到控制台，`--check-only` 只做预检，不写 workbook 或任何外部摘要文件

**工单异常解释字段**：`成本分析工单维度` 保留 `异常等级`、`异常主要来源`、`复核原因`，并使用单列 `异常明细解释` 展示达到关注或高度可疑的异常项；不再输出 `异常池样本数`、`异常池中心log值`、`异常池原始MAD`、`异常池有效MAD`、`相对中位偏离` 五个旧解释列

## 依赖 (Dependencies)

- **Python**: 3.11+
- **核心包**：`pandas>=2.0.0`, `openpyxl>=3.1.0`, `numpy>=1.24.0`, `beautifulsoup4>=4.12.0`

## 常用命令 (Common Commands)

```bash
# 安装
conda run -n costing311 python -m pip install -e .

# 安装开发依赖
conda run -n costing311 python -m pip install -e '.[dev]'

# 运行 ETL
conda run -n costing311 python main.py gb
conda run -n costing311 python main.py sk

# 预检 + benchmark（只跑分析链路，不落 workbook 或任何外部摘要文件）
conda run -n costing311 python main.py gb --check-only --benchmark
conda run -n costing311 python main.py sk --check-only --benchmark

# 测试 (需使用 conda costing311 环境)
conda run -n costing311 python -m pytest tests -q

# 单测
conda run -n costing311 python -m pytest tests/ -k test_name -q

# 代码检查/格式化
conda run -n costing311 python -m ruff check src tests
conda run -n costing311 python -m ruff format src tests --check
```

## 测试契约 (Test Contracts)

**Baseline 真值**：`tests/contracts/baselines/` 是 workbook / error_log / CLI 契约的唯一来源，README 描述仅供参考。

**重构规则**：纯重构不得修改 baseline；仅业务口径变化时才允许更新，并必须说明差异。

## 当前实现要点

- `main.py` 已支持 `--check-only` 和 `--benchmark`。
- `src/analytics/scoring.py` 已接管加权中位数、加权 MAD、有效 MAD 兜底和异常分级，`anomaly.py` 只负责异常页组装与兼容导出。
- `src/analytics/summary.py` 保留质量摘要 payload / JSON 工具函数，但正常 CLI 输出不再落盘 sidecar。
- `src/excel/sheet_writers.py` 已删除，写出路径统一走 `fast_writer.py`。

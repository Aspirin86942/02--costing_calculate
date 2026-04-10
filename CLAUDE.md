# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述 (Project Overview)

本仓库是一个成本计算 ETL 工具，用于处理金蝶 ERP 系统导出的成本计算单 Excel 文件。

### 核心功能
- 清洗原始 Excel 文件（去除表头、扁平化双层表头）
- 将成本计算单拆分为"成本明细"和"产品数量统计"两个工作表
- 字段名提取和标准化

## 架构 (Architecture)

### 模块依赖规则 (Module Dependency Rules)

**严格分层**，由 `tests/architecture/test_import_rules.py` 强制：
- `analytics` 不得导入 `etl` 或 `excel`
- `excel` 不得导入 `etl`
- `etl/stages/*` 不得导入 `excel`
- 仅 `etl/costing_etl.py` 和 `etl/pipeline.py` 可导入 `excel` 模块

### 数据流 (Data Flow)

```
原始 Excel -> reader.load_raw_workbook()
          -> 列名标准化 (clean_column_name)
          -> 列推断/重命名 (infer_rename_map)
          -> 删除合计行 (remove_total_rows)
          -> 规则填充 (forward_fill_with_rules)
          -> 拆分为 detail/qty (split_detail_and_qty_sheets)
          -> 构建分析 fact 表 (build_report_artifacts)
          -> 渲染价量表 (render_tables)
          -> 写出 Excel (CostingWorkbookWriter)
```

### 数据契约 (Data Contracts)

**字段映射**：`docs/field_definitions/gb 金蝶字段.txt` 定义了标准字段名

**关键列**：`子项物料编码 `、` 成本项目名称 `、` 工单编号 `、` 工单行号 `、` 年期`

**期间格式**：`年期` 列统一格式化为 `YYYY 年 MM 期`

**产品白名单**：`ANALYSIS_PRODUCT_WHITELIST` 定义了 8 个目标产品，仅这些产品进入价量/异常分析

**输出 Sheet**：9 张表
- `成本明细 `、`产品数量统计`
- `直接材料_价量比 `、` 直接人工_价量比 `、`制造费用_价量比`
- `按工单按产品异常值分析 `、` 按产品异常值分析`
- `数据质量校验 `、`error_log`

## 依赖 (Dependencies)

- **Python**: 3.11+
- **核心包**：`pandas>=2.0.0`, `openpyxl>=3.1.0`, `numpy>=1.24.0`, `beautifulsoup4>=4.12.0`

## 常用命令 (Common Commands)

```bash
# 安装
pip install -e .

# 运行 ETL (自动读取 data/raw/gb/下的 GB-*成本计算单*.xlsx)
python -m src.etl.costing_etl

# 测试 (需使用 conda test 环境)
conda run -n test python -m pytest -q

# 单测
conda run -n test python -m pytest tests/ -k test_name -q

# 代码检查/格式化
conda run -n test ruff check .
conda run -n test ruff format . --check
conda run -n test ruff format .
```

## 测试契约 (Test Contracts)

**Baseline 真值**：`tests/contracts/baselines/` 是 workbook / error_log / CLI 契约的唯一来源，README 描述仅供参考。

**重构规则**：纯重构不得修改 baseline；仅业务口径变化时才允许更新，并必须说明差异。

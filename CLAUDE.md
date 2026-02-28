# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述 (Project Overview)

本仓库是一个成本计算 ETL 工具，用于处理金蝶 ERP 系统导出的成本计算单 Excel 文件。

### 核心功能
- 清洗原始 Excel 文件（去除表头、扁平化双层表头）
- 将成本计算单拆分为"成本明细"和"产品数量统计"两个工作表
- 字段名提取和标准化

## 架构 (Architecture)

### 目录结构
```
costing_calculate/
├── src/etl/              # ETL 处理模块
│   └── costing_v2.py     # 主 ETL 脚本
├── src/config/           # 配置管理
│   └── settings.py       # 路径配置
├── data/raw/             # 原始数据
│   ├── gb/               # GB 系列文件
│   └── shukong/          # 数控系列文件
├── data/processed/       # 处理结果
│   ├── gb/
│   └── shukong/
├── tests/                # 测试
├── docs/field_definitions/  # 字段定义文件
└── scripts/              # 归档旧脚本
```

### 数据契约 (Data Contracts)

- **字段映射**：`docs/field_definitions/gb 金蝶字段.txt` 定义了标准字段名
- **关键列**：`子项物料编码 `、` 成本项目名称 `、` 工单编号 `、` 工单行号 `、` 年期` 为核心标识列
- **期间格式**：`年期` 列格式为 `YYYY 年 MM 期 `，脚本中会统一格式化为 `YYYY 年 MM 期`

### 审计特性 (Audit Features)

- **数据完整性校验**：
  - 处理前后行数对比
  - 关键列存在性检查
  - 剔除合计行前后记录行数变化
- **可追溯性**：
  - 所有脚本使用 `logging` 记录关键步骤和异常
  - 错误信息包含具体行数、列名和上下文

## 依赖 (Dependencies)

- **Python**: 3.11+
- **核心包**：
  - `pandas`（数据操作）
  - `openpyxl`（Excel 读写）
  - `numpy`（数值计算）
  - `beautifulsoup4`（HTML 解析）

## 常用命令 (Common Commands)

### 安装
```bash
pip install -e .
```

### 运行 ETL
```bash
# 将原始 Excel 文件放入 data/raw/gb/ 或 data/raw/shukong/
python -m src.etl.costing_v2
```

### 测试
```bash
pytest tests/ -q
```

### 代码检查
```bash
ruff check src/ tests/
ruff format src/ tests/
```

## 归档脚本 (Archived Scripts)

以下脚本已归档到 `scripts/` 目录，不再维护：
- `Costing_Calculate.py` - 原始清洗脚本
- `Costing_Calculating_V2.0.py` - V2.0 拆分脚本（已重构到 src/etl/costing_v2.py）
- `抓取所有字段脚本.py` - 字段提取脚本

## 已移除 (Removed)

- `Costing_Allocation.py` - 成本分摊脚本（已废弃）

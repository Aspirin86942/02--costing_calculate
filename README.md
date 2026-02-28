# 成本计算 ETL 工具

金蝶 ERP 成本计算单数据处理工具。

## 功能
- 清洗原始 Excel 文件（去除表头、扁平化双层表头）
- 将成本计算单拆分为"成本明细"和"产品数量统计"两个工作表
- 字段名提取和标准化

## 安装
```bash
pip install -e .
```

## 使用
```bash
# 1. 将原始 Excel 文件放入对应 data/raw/ 目录
#    - GB 系列文件 → data/raw/gb/
#    - 数控系列文件 → data/raw/shukong/

# 2. 运行主 ETL
python -m src.etl.costing_v2

# 3. 处理结果保存在对应 data/processed/ 目录
```

## 目录结构
- `src/etl/` - ETL 处理模块
  - `costing_v2.py` - 主 ETL 脚本
  - `utils.py` - 工具函数
- `src/config/` - 配置管理
- `data/raw/` - 原始数据
  - `gb/` - GB 系列原始成本计算单
  - `shukong/` - 数控系列原始成本计算单
- `data/processed/` - 处理结果
  - `gb/` - GB 系列处理结果
  - `shukong/` - 数控系列处理结果
- `tests/` - 测试
- `docs/field_definitions/` - 字段定义文件
- `scripts/` - 归档旧脚本

## 测试
```bash
# 运行测试
pytest tests/ -q

# 代码检查
ruff check src/ tests/

# 代码格式化
ruff format src/ tests/
```

## 数据目录说明
- `data/raw/gb/` - GB 系列原始成本计算单
- `data/raw/shukong/` - 数控系列原始成本计算单
- `data/processed/gb/` - GB 系列处理结果
- `data/processed/shukong/` - 数控系列处理结果
- `docs/field_definitions/` - 字段定义文件 (gb 金蝶字段.txt/html)

## 归档脚本
以下脚本已归档到 `scripts/` 目录，不再主动维护：
- `Costing_Calculate.py` - 原始清洗脚本
- `Costing_Calculating_V2.0.py` - V2.0 拆分脚本（已重构到 src/etl/costing_v2.py）
- `抓取所有字段脚本.py` - 字段提取脚本

## 已移除
- `Costing_Allocation.py` - 成本分摊脚本（已废弃）

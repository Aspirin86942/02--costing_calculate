# 成本计算 ETL 工具

金蝶 ERP 成本计算单数据处理工具。

## 功能
- 清洗原始 Excel 文件（去除表头、扁平化双层表头）
- 将成本计算单拆分为 `成本明细` 和 `产品数量统计` 两个工作表
- 新增价量分解分析 (Price/Volume Decomposition)：
  - 直接材料价量比
  - 直接人工价量比
  - 制造费用价量比
- 输出审计日志 `error_log`（未映射项目、缺失值、勾稽异常）
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
python -m src.etl.costing_etl

# 3. 处理结果保存在对应 data/processed/ 目录
```

## 输出工作表
每个处理后的工作簿默认输出以下 9 张 Sheet：
- `成本明细`
- `产品数量统计`
- `直接材料_价量比`
- `直接人工_价量比`
- `制造费用_价量比`
- `按工单按产品异常值分析`
- `按产品异常值分析`
- `数据质量校验`
- `error_log`

## 分析输出口径
- 价量分析粒度：`产品编码 + 月份 + 成本类别`
- 成本类别映射：
  - `直接材料` -> `direct_material`
  - `直接人工` -> `direct_labor`
  - `制造费用*` -> `moh`
  - `委外加工费` -> 不纳入三大类价量分析，仅在工单分析与总成本勾稽中展示
- 每张分析表均按三段展示：
  - `完工金额`（按月 + `总计`，含底部总计行）
  - `完工数量`（按月 + `总计`，含底部总计行）
  - `完工单价`（按月 + `均值`）
- 单价 `均值` 采用加权均价：`总金额 / 总数量`
- `产品数量统计` 新增三大类/制造费用细项金额、单位成本和校验字段，作为工单分析底表
- 新增工单维度异常分析页：`按工单按产品异常值分析`
  - 粒度：`月份 + 产品编码 + 工单编号 + 工单行`
  - 总体：按产品在整个统计期间内建总体，月份仅作为标签与汇总字段
  - 规则：仅对大于 0 的单位成本计算对数与 Modified Z-score，阈值为 `2.5/3.5`
- `按产品异常值分析` 保留为兼容摘要页
  - 字段：总成本、完工数量、单位成本、直接材料/人工/制造费用的成本、单位成本、贡献率
  - 不再执行 IQR 检测，仅输出月度摘要数据

## Excel 样式
- 蓝黄风格：段标题黄底、表头浅蓝、总计行加深蓝
- 冻结窗格按 sheet 类型分别设置，真实契约以 `tests/contracts/` baseline 为准
- 开启筛选
- 数字格式：
  - 金额：`#,##0.00`
  - 数量：`#,##0`
  - 单价：`#,##0.00`
- 不使用合并单元格

## 目录结构
- `src/analytics/` - 分析与异常检测模块
  - `contracts.py` - 共享数据结构
  - `fact_builder.py` - fact 构建与 Decimal 工具
  - `qty_enricher.py` - 数量页补强与报表产物编排
  - `table_rendering.py` - 三大类价量宽表与兼容摘要页
  - `anomaly.py` / `quality.py` / `errors.py` - 工单异常、质量校验、error_log 契约
- `src/etl/` - ETL 处理模块
  - `costing_etl.py` - 主入口与 orchestration
  - `pipeline.py` - ETL 阶段编排
  - `stages/` - 读取、列识别、清洗、拆分
  - `utils.py` - 工具函数
- `src/excel/` - Excel 写出与样式模块
  - `styles.py` / `sheet_writers.py` / `workbook_writer.py`
- `src/config/` - 配置管理
- `data/raw/` - 原始数据
  - `gb/` - GB 系列原始成本计算单
  - `shukong/` - 数控系列原始成本计算单
- `data/processed/` - 处理结果
  - `gb/` - GB 系列处理结果
  - `shukong/` - 数控系列处理结果
- `tests/` - 单元测试
- `tests/contracts/` - workbook / error_log / CLI 契约测试
- `tests/architecture/` - 模块依赖与导入边界测试
- `docs/field_definitions/` - 字段定义文件

## 测试
```bash
# 运行测试
pytest tests/ -q

# 代码检查
ruff check src/ tests/

# 代码格式化
ruff format src/ tests/
```

## Conda（推荐）
```bash
conda run -n test python -m pytest -q
conda run -n test ruff check .
conda run -n test ruff format . --check
```

## 数据目录说明
- `data/raw/gb/` - GB 系列原始成本计算单
- `data/raw/shukong/` - 数控系列原始成本计算单
- `data/processed/gb/` - GB 系列处理结果
- `data/processed/shukong/` - 数控系列处理结果
- `docs/field_definitions/` - 字段定义文件 (gb 金蝶字段.txt/html)

## 已移除脚本
以下历史脚本已从仓库移除，功能已由 `src/` 内模块接管：
- `Costing_Calculate.py` - 原始清洗脚本
- `Costing_Calculating_V2.0.py` - V2.0 拆分脚本（已重构到 src/etl/costing_etl.py）
- `抓取所有字段脚本.py` - 字段提取脚本

## 已移除
- `Costing_Allocation.py` - 成本分摊脚本（已废弃）

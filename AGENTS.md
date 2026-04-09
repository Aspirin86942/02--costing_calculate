
## Repository Guidelines

### Project Structure & Module Organization
本仓库是用于成本核算工作簿的 Python ETL 工具：
- `src/analytics/`: 分析与异常检测（`contracts.py`、`fact_builder.py`、`qty_enricher.py`、`table_rendering.py`、`anomaly.py`、`quality.py`、`errors.py`）
- `src/etl/`: ETL 主逻辑（`costing_etl.py` 主流程，`pipeline.py` 阶段编排，`stages/` 读取/清洗/拆分）
- `src/excel/`: Excel 写出与样式（`styles.py`、`sheet_writers.py`、`workbook_writer.py`）
- `src/config/`: 路径与目录配置（`settings.py`）
- `tests/`: 单元测试（`test_costing_etl.py`、`test_pq_analysis.py`、`test_pq_analysis_v3.py`、`test_etl_pipeline.py`）
- `tests/contracts/`: workbook / error_log / CLI 契约测试
- `tests/architecture/`: 模块依赖边界测试
- `data/raw/{gb,sk}/`: 原始 Excel 输入
- `data/processed/{gb,sk}/`: 处理后输出
- `docs/field_definitions/`: 字段映射参考
- 历史 `scripts/` 已移除；新增功能统一在 `src/` 实现

### Build / Test / Dev Commands
- `python -m pip install -e .`: 可编辑模式安装
- `python main.py gb`: 执行 GB 管线
- `python main.py sk`: 执行 SK 管线
- `python -m pytest tests -q`: 运行测试
- `python -m ruff check src tests`: 代码检查
- `python -m ruff format src tests`: 代码格式化

如缺少 `pytest`/`ruff`：
- `python -m pip install pytest ruff`

### Coding Style & Naming
- 以 `pyproject.toml` 为准，保持与当前 Python 版本兼容（当前项目约束为 3.11+）。
- Ruff 关键配置：行宽 120、单引号、规则 `E,F,I,B,C4,W,UP,S,T10`（按项目忽略项生效）。
- 命名规范：函数/变量 `snake_case`，类 `PascalCase`，常量 `UPPER_SNAKE_CASE`。
- ETL 步骤应显式可追踪，避免隐式副作用。

### Testing Guidelines
- 测试框架：`pytest`
- 命名规范：`tests/test_*.py`，测试函数 `test_*`
- 重点覆盖：
  - 列标准化与自动重命名
  - 汇总行过滤
  - 文件处理成功/失败路径
  - `产品数量统计` 的金额补强、单位成本与勾稽字段
  - `按工单按产品异常值分析` 的 Modified Z-score 分级与白名单过滤
- 优先用小型 DataFrame fixture，避免依赖大体量真实 Excel。

### Commit / PR Guidelines
- 默认使用 Conventional Commits：
  - `feat(etl): add sk file matcher`
  - `fix(utils): handle empty period cell`
  - `test(etl): cover missing material column`
- PR 建议包含：
  - 问题与方案摘要
  - 关联任务/Issue（如有）
  - 测试与 lint 结果
  - 输入/输出影响示例（行数、输出文件名）

### Security & Data Handling
- ERP 导出数据视为敏感信息，不提交涉密原始文件。
- 密钥与环境特定路径不得硬编码进源码。

### 当前业务规则（GB 分析输出）
- 每个处理后的工作簿默认输出以下 8 张 Sheet：`成本明细`、`产品数量统计`、`直接材料_价量比`、`直接人工_价量比`、`制造费用_价量比`、`按工单按产品异常值分析`、`按产品异常值分析`、`error_log`。
- 成本中心名称为`集成车间`时，`供应商编码`与`供应商名称`禁止向下填充（其余字段按既有规则填充）。
- 分析页仅展示白名单产品，匹配规则为`产品编码 + 产品名称`双字段精确匹配。
- 分析页产品展示顺序必须与代码中的白名单顺序一致（不是按编码/名称字典序）。
- `成本明细`sheet保留工单级成本明细；`本期完工金额`为空时，后续分析按`0`参与汇总，并继续写入`error_log`的`MISSING_AMOUNT`。
- `产品数量统计`sheet保留现有行粒度，仅保留`本期完工数量 > 0`且`本期完工金额`非空的工单；输出三大类金额、制造费用细项金额、委外加工费金额、单位成本、勾稽状态与异常原因说明。
- `产品数量统计`sheet中的`制造费用明细项合计是否等于制造费用合计`仅校验制造费用明细，不包含`委外加工费`。
- `产品数量统计`sheet中的总成本勾稽口径为`直接材料 + 直接人工 + 制造费用 + 委外加工费 = 总完工成本`。
- 新增`按工单按产品异常值分析`sheet：
  - 一行定义为`月份 + 产品编码 + 工单编号 + 工单行`
  - 异常分析按`产品`在整个统计期间内建总体，月份仅作标签与汇总字段
  - 仅对大于 0 的单位成本计算对数与 Modified Z-score
  - 异常阈值：`|score| <= 2.5`为正常，`2.5 < |score| <= 3.5`为关注，`|score| > 3.5`为高度可疑
  - `委外加工费`只展示`委外加工费合计完工金额`与`委外加工费单位完工成本`，不输出`log`、`Modified Z-score`和异常标记，也不参与`异常等级`与`异常主要来源`判定
- 质量校验结果默认输出到控制台摘要和同名 `.log` 文件，至少包含行数勾稽、空值率、工单主键唯一性和分析覆盖率。
- `委外加工费`不归属`制造费用`，不纳入三大类价量分析，也不因为“不参与三大类分析”写入`error_log`；它只在`产品数量统计`和`按工单按产品异常值分析`中展示，并参与总完工成本勾稽。
- `error_log`至少保留`MISSING_AMOUNT`、`TOTAL_COST_MISMATCH`、`MOH_BREAKDOWN_MISMATCH`、`DUPLICATE_WORK_ORDER_KEY`、`NON_POSITIVE_UNIT_COST`等可审计异常。
- `按产品异常值分析`sheet保留，但已改为兼容摘要页，不再执行 IQR 检测。
- `按产品异常值分析`sheet列宽固定为`15`，并去除该 sheet 的条件格式数据条与异常值红底红字高亮。

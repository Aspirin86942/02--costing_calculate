# Codex 指令集：Python 后端审计/开发（API / 数据处理 / 任务调度 / ETL）

## 1) 角色与边界
- 你是面向审计与交付的执行型开发助手：以数据与可验证结果为准。
- 适用范围：仅 Python 后端项目（含 Web API、批处理、ETL、任务调度）。
- 优先级：Correctness（正确性）> Maintainability（可维护性）> Observability（可观测性）。
- 安全边界：不做深入安全研究；仅在发现明显风险时提示并给出修复（例如：密钥泄露、显著注入点、明显越权）。
- 仓库根目录必须存在 `AGENTS.md`，且必须使用简体中文编写。

## 2) 输出与沟通规则
- 输出只包含：结论、步骤、代码、命令、结果、风险点；不写客套话。
- 事实不确定时：明确写“无确切信息/需补充材料”，禁止臆测。
- 用户观点或实现方向存在事实/逻辑问题：直接指出并给出可验证依据（代码/测试/日志/数据勾稽）。
- 语言：简体中文；专业术语首次出现需中英双语标注，例如：检索增强生成 (RAG)。

## 3) 工程与代码规范（强制）
### 3.1 Python 与风格
- Python 版本：3.10+。
- 所有公共函数/关键函数必须包含 Type Hints（类型提示）。
- 关键业务逻辑必须有中文注释，解释“为什么”而非仅“做什么”。

### 3.2 数据处理（pandas 优先）
- 大数据处理强制 pandas 向量化；避免对大表逐行 for 循环。
- 金额/精确小数计算必须使用 `decimal.Decimal`，禁止 `float`。
- Excel 读写仅用 `openpyxl` 或 `xlsxwriter`。

### 3.3 错误处理与审计追踪
- 禁止静默失败：不允许 `try/except: pass`。
- 异常/脏数据必须落到可审计载体：
  - 日志（包含 request_id，如项目已有）或
  - 单独的 `error_log` DataFrame（至少含：row_id/主键、错误类型、字段名、原值、原因、处理动作）。
- 对外接口必须有明确错误模型（Error Model）：错误码、message、可重试与否；禁止“全部 500 + 文本”。

### 3.4 数据完整性校验（ETL/对账类任务必做）
至少包含两项，并输出结果：
- 行数勾稽（输入行数、过滤后行数、输出行数一致性解释）
- 空值率/缺失率统计（关键字段）
- 关键字段唯一性/范围检查（按业务选择）

## 4) 默认交付工作流（强制三段式）
### 4.1 执行级方案（先输出再动代码，五项缺一不可）
按以下结构输出（保持标题与字段）：
1) **目标 / 不做的事**
- 目标：……
- 不做的事：不做深入安全审计；不做极限性能优化（除非存在明显 N+1/缺索引）

2) **接口约定（API Contract）**（如无 API 则写“无”）
- Endpoint / Method / Auth（如有）
- Request schema（字段、类型、必填、默认值）
- Response schema（成功/失败）
- Error model（错误码、message、是否可重试）
- 幂等/重试/超时（如适用）

3) **数据约定（Data Contract）**
- 表/索引/约束变更（无则写“无”）
- 迁移策略（可逆/回滚步骤；无则写“无”）
- 兼容策略（灰度期读写兼容；无则写“无”）

4) **验收标准（Acceptance Criteria）**
- 必跑命令：测试 + lint +（如有）格式化 +（如有）类型检查
- 新增测试：≥2（happy path + 关键 edge case）
- 关键日志：失败路径包含 request_id（如有）+ 关键参数（脱敏）
- 性能底线：避免明显 N+1；涉及 DB 时必要索引到位

5) **提交拆分（Commit Plan）**
- Commit 1：改哪些文件/加哪些测试/跑哪些命令
- Commit 2：……
规则：每个提交必须能独立通过测试、可回退；避免把 schema 变更与大规模业务改动混在同一提交。

### 4.2 逐提交执行与自证
每个 Commit 必须遵守：
- 先阅读相关代码/配置/测试，再修改。
- 至少新增 2 个测试（除非明确为纯重命名/纯注释）。
- 必须运行验收命令（能跑就都跑），失败则迭代直到通过。
- 每个 Commit 输出固定三段：
  a) 关键 diff 摘要（模块/函数/SQL/迁移点）
  b) 运行命令与结果（原样粘贴）
  c) 可能回归点（影响面与回归建议）

### 4.3 最终质量 Review（只看质量，不做深入安全）
按固定结构输出：
1) Correctness（边界/空值/异常/并发/事务/幂等/重试）
2) Maintainability（模块边界/复用/命名/注释）
3) Observability（失败日志字段是否足够：request_id + 关键参数；是否缺 metric/trace）
4) Tests（分支覆盖/是否 flaky：时间/随机/外部依赖）
5) Must Fix / Nice to Have

## 5) 验收命令（Conda 版：统一在 test 环境里跑）
> 原则：所有命令默认在 conda 环境 `test` 中执行；统一使用 `conda run -n test ...` 前缀。  
> 不再出现 `conda test ...`（避免歧义/误用）。

### 5.1 执行前检查（强制）
- 每次跑验收命令前先确认实际 Python 来自 `test` 环境：
  - `conda run -n test python -c "import sys; print(sys.executable)"`
- 若该命令失败：停止后续步骤，直接输出错误信息（通常是环境不存在/conda 未初始化）。

### 5.2 测试（必跑）
- `conda run -n test python -m pytest -q`

### 5.3 Lint（优先 ruff，存在则跑；否则按项目现状）
- 若检测到 ruff 配置（`pyproject.toml` 含 `[tool.ruff]` 或存在 `ruff.toml`）：
  - `conda run -n test ruff check .`
- 否则（存在则跑，至少跑一个）：
  - `conda run -n test flake8`
  - `conda run -n test pylint <package_or_src_dir>`

### 5.4 格式化（若存在配置则必跑）
- 若检测到 ruff format（通常与 ruff 配置同在）：
  - `conda run -n test ruff format .`
- 否则若检测到 black 配置：
  - `conda run -n test black .`

### 5.5 类型检查（若存在 mypy 配置/依赖则必跑）
- 若检测到 `mypy.ini` / `pyproject.toml` 含 `[tool.mypy]` / `setup.cfg` 含 mypy 配置，或依赖中包含 mypy：
  - `conda run -n test mypy .`
- 若不存在：输出“未检测到 mypy 配置/依赖，已跳过”（必须写明跳过原因）。

### 5.6 失败处理（强制）
- 任一命令失败：必须粘贴
  - 失败命令（完整）
  - 退出码（如可得）
  - 关键报错片段
  - 修复动作（修复后重跑同一命令直到通过）

## 6) 允许的澄清问题（最多 3 个；需求不明时才问）
按顺序、短句提问：
1) 失败时线上期望返回什么（错误码/是否重试）？
2) 数据一致性要求：强一致/最终一致/允许重复？
3) 可观测性：日志里必须包含哪些定位字段？

## 7) 约束提醒（避免幻觉）
- 不要凭空编造第三方库版本、API 参数、配置项；若无法从仓库/依赖文件确认，明确指出缺口并给出需要用户补充的材料（如 pyproject.toml/requirements.txt/运行报错）。
- 如果现有代码风格/框架已有约定（如 FastAPI/SQLAlchemy/Celery），以项目现状为准并保持一致。

## 8) 本机环境约定：UTF-8 + Conda（强制）
> 目的：避免中文乱码；保证 Codex 读/写/测试使用同一 Conda 解释器与依赖。

### 8.1 源码/文本编码规则（UTF-8）
- 约定：仓库内所有文本文件（`.py/.sql/.md/.json/.yml/.txt` 等）默认 UTF-8。
- 写代码时：凡是读写文本文件，必须显式指定编码：
  - `open(path, 'r', encoding='utf-8')`
  - `open(path, 'w', encoding='utf-8', newline='')`
  - pandas：`pd.read_csv(..., encoding='utf-8')` / `df.to_csv(..., encoding='utf-8', index=False)`
  - 若目标是给 Excel 直接打开且常见乱码：允许 `utf-8-sig`（需在代码注释说明原因）。
- 遇到“看起来是乱码”时的强制自检（不要猜编码）：
  1) 用 Python 按 UTF-8 重新读取验证（优先用这条，不要用系统 `type`/某些终端直接 cat）：
     - `python -c "from pathlib import Path; p=Path('PATH'); print(p.read_text(encoding='utf-8')[:200])"`
  2) 如果 UTF-8 读取失败：停止继续改动，直接汇报“文件可能不是 UTF-8”，并列出报错信息与文件路径（不要私自转码覆盖原文件）。
- 写回文件时的强约束：任何被修改的文本文件必须以 UTF-8 保存；禁止因为编辑器/脚本默认编码导致回写为 GBK/CP936 等。

### 8.2 Windows 控制台输出乱码的兜底（仅在需要时）
- 如果测试/日志输出中文在终端显示异常，优先在执行命令前设置：
  - PowerShell：
    - `$env:PYTHONUTF8=1`
    - `$env:PYTHONIOENCODING='utf-8'`
  - CMD：
    - `set PYTHONUTF8=1`
    - `set PYTHONIOENCODING=utf-8`
    - `chcp 65001`
- 以上属于“显示层”兜底；代码层仍以 UTF-8 文件读写为准。

### 8.3 Conda 环境（默认一切命令走 conda）
- 约定：所有测试/lint/格式化/类型检查命令，默认使用：
  - `conda run -n <ENV> <command ...>`
- `<ENV>` 的获取规则（自动探测，不要问用户）：
  1) 若存在 `environment.yml`：优先从中读取环境名（`name:`）。
  2) 否则：用 `conda info --envs` 推断当前项目对应环境（优先 active 环境）。
- 禁止混用系统 python/pip（除非项目明确要求，并在输出中说明原因）。

### 8.4 你（Codex）执行测试的优先级（按顺序尝试）
1) 优先跑用户既定的 `conda test ...`（如果该命令在项目文档/脚本中明确存在，并且 `conda test --help` 可用）。
2) 如果 `conda test` 不存在/不可用：改用
   - `conda run -n <ENV> pytest -q`
3) 如果项目是 conda-build 包（存在 `meta.yaml` / `recipe/` 等）：按项目既定方式执行（例如 `conda build ...`），并在输出里说明你选择该路径的依据。

---

## Repository Guidelines

### Project Structure & Module Organization
本仓库是用于成本核算工作簿的 Python ETL 工具：
- `src/analytics/`: 分析与异常检测（`pq_analysis.py`，含价量分析、工单异常分析、数据质量校验）
- `src/etl/`: ETL 主逻辑（`costing_v2.py` 主流程，`utils.py` 工具函数）
- `src/config/`: 路径与目录配置（`settings.py`）
- `tests/`: 单元测试（`test_costing_v2.py`、`test_pq_analysis.py`、`test_pq_analysis_v3.py`）
- `data/raw/{gb,shukong}/`: 原始 Excel 输入
- `data/processed/{gb,shukong}/`: 处理后输出
- `docs/field_definitions/`: 字段映射参考
- `scripts/`: 历史脚本归档；新增功能优先在 `src/` 实现

### Build / Test / Dev Commands
- `python -m pip install -e .`: 可编辑模式安装
- `python -m src.etl.costing_v2`: 执行主 ETL（默认读取 `data/raw/gb/`）
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
  - `feat(etl): add shukong file matcher`
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
- 成本中心名称为`集成车间`时，`供应商编码`与`供应商名称`禁止向下填充（其余字段按既有规则填充）。
- 分析页仅展示白名单产品，匹配规则为`产品编码 + 产品名称`双字段精确匹配。
- 分析页产品展示顺序必须与代码中的白名单顺序一致（不是按编码/名称字典序）。
- `产品数量统计`sheet保留现有行粒度，并新增三大类/制造费用细项金额、单位成本、勾稽状态与异常原因说明。
- 新增`按工单按产品异常值分析`sheet：
  - 一行定义为`月份 + 产品编码 + 工单编号 + 工单行`
  - 异常分析按`产品`在整个统计期间内建总体，月份仅作标签与汇总字段
  - 仅对大于 0 的单位成本计算对数与 Modified Z-score
  - 异常阈值：`|score| <= 2.5`为正常，`2.5 < |score| <= 3.5`为关注，`|score| > 3.5`为高度可疑
- 新增`数据质量校验`sheet，至少输出行数勾稽、空值率、工单主键唯一性和分析覆盖率。
- `委外加工费`不纳入三大类价量分析与工单异常分析，必须写入`error_log`。
- `按产品异常值分析`sheet保留，但已改为兼容摘要页，不再执行 IQR 检测。
- `按产品异常值分析`sheet列宽固定为`15`，并去除该 sheet 的条件格式数据条与异常值红底红字高亮。

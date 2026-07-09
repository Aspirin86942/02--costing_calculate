# 02--costing_calculate 去 GUI + Python 3.11 化方案

## 1. 目标

将 `02--costing_calculate` 从「CLI + 可选 PySide6 GUI」收敛为「纯 CLI ETL 工具」。

最终状态：

```text
只保留命令行入口 main.py
删除或归档 PySide6 GUI 代码
删除或归档 GUI 测试
删除 gui optional dependency
项目 Python 版本明确为 >=3.11
README.md / AGENTS.md / CLAUDE.md 不再提供 GUI 安装、启动、测试说明
核心 ETL、analytics、excel 输出口径不变
```

一句话：**拿掉 GUI 外壳，保留 CLI 主链路，把 Python 版本要求正式抬到 3.11+。**

---

## 2. 当前观察结论

### 2.1 GUI 是外层壳，不是核心依赖

当前依赖方向是：

```text
src/gui -> src/services -> src/etl / src/analytics / src/excel
```

没有发现核心模块反向依赖 GUI：

```text
src/analytics/
src/etl/
src/excel/
src/config/
src/services/
```

这些模块不应 import：

```text
src.gui
PySide6
QApplication
QMainWindow
QWidget
QThread
```

这说明 GUI 可以作为一整个外层切片移除，不需要重写核心 ETL。

### 2.2 `src/services/` 不要删

虽然 `src/services/` 可能是为了 CLI / GUI 共用而抽出来的，但它现在已经被 CLI 主链路使用。

当前 CLI 路径：

```text
main.py
  -> src.etl.runner
      -> src.services.costing_service
          -> src.etl.costing_etl
          -> src.analytics
          -> src.excel
```

因此 `src/services/` 应重新定位为：

```text
CLI 应用服务层 / ETL 调用适配层
```

不是 GUI 专属层。去 GUI 时不要删除 `src/services/`。

### 2.3 当前 Python 版本声明与代码实际要求不一致

项目当前声明过 Python 3.10：

```toml
requires-python = ">=3.10"
```

但代码使用了 Python 3.11 才有的：

```python
from enum import StrEnum
```

在 Python 3.10 环境下会失败：

```text
ImportError: cannot import name 'StrEnum' from 'enum'
```

既然本次目标是「Python 3.11 化」，推荐直接把项目要求改为：

```toml
requires-python = ">=3.11"
```

不再为了 3.10 兼容去改 `StrEnum`。

---

## 3. 改造边界

### 3.1 可以动

```text
pyproject.toml
README.md
AGENTS.md
CLAUDE.md
tests/architecture/test_import_rules.py
src/gui/
tests/test_gui_*.py
_archive/
```

### 3.2 不应动

除非测试暴露出纯粹导入问题，否则不要动：

```text
src/analytics/
src/etl/
src/excel/
src/config/
src/services/costing_service.py
src/services/progress.py
tests/contracts/baselines/
```

尤其不要修改业务契约 baseline。

## 3.5 伪代码草案（执行级）

### 目标

把可选 GUI 外壳完整迁出运行路径，同时把项目元数据、文档和防回归测试收敛到纯 CLI + Python 3.11+。

### 输入

- `project_root`: `D:\python_program\02--costing_calculate`
- 当前 Git 工作区状态，尤其是 `pyproject.toml` 已有 diff
- GUI 源码目录：`src/gui/`
- GUI 测试文件：`tests/test_gui_*.py`
- 文档：`README.md`、`AGENTS.md`、`CLAUDE.md`
- 架构测试：`tests/architecture/test_import_rules.py`

### 输出

- 成功：GUI 源码与测试归档，主项目不再依赖 PySide6，文档只保留 CLI 使用说明，测试与 lint 通过。
- 失败：保留当前失败命令、失败原因和下一步建议；不得伪造通过结果。
- 降级：如果 Python 3.11 环境不可用，可先完成静态改造，并明确说明运行时验证缺口。

```python
# [伪代码草案]
# 目标：按最小改动移除 GUI 外壳，同时用测试和搜索证明主链路仍是纯 CLI。
# 输入：
# - project_root: 仓库根目录
# - archive_root: GUI 归档目录
# - docs: 需要同步清理的项目说明文件
# - validation_commands: CLI help、pytest、ruff check、ruff format --check
# 输出：
# - success_result: 修改清单、归档路径、验证结果
# - error_result: 失败命令、错误摘要、是否需要用户补充 Python 3.11 环境

def remove_gui_and_pin_python311(project_root, archive_root, docs, validation_commands):
    status = git_status(project_root)
    pyproject_diff = git_diff(project_root, "pyproject.toml")
    # 先读已有 diff，是为了避免把用户已有改动整文件覆盖掉。
    if pyproject_diff.has_unrelated_changes:
        merge_with_existing_changes(pyproject_diff)

    add_architecture_test(
        "test_project_has_no_gui_package",
        assertion="not (SRC_ROOT / 'gui').exists()",
    )
    red_result = run_pytest("tests/architecture/test_import_rules.py::test_project_has_no_gui_package")
    if not red_result.failed_for_expected_reason:
        return build_error_result("RED_TEST_INVALID", red_result.summary, retryable=False)

    move_path(project_root / "src/gui", archive_root / "src-gui")
    move_paths(project_root.glob("tests/test_gui_*.py"), archive_root / "tests-gui")

    patch_pyproject(
        requires_python=">=3.11",
        remove_optional_dependency="gui",
        remove_dependency="PySide6",
        pytest_testpaths=["tests"],
        pytest_norecursedirs=["_archive"],
    )

    for doc in docs:
        remove_gui_install_start_test_text(doc)
        replace_text(doc, "控制台或 GUI 状态区", "控制台")
        rewrite_services_description_as_cli_service_layer(doc)

    # 源码中残留的 GUI 字样只做语义重命名，不改变 ETL 逻辑。
    replace_source_comments(
        old="GUI 白名单候选产品",
        new="产品白名单候选产品",
    )
    replace_source_comments(
        old="CLI and GUI entry points",
        new="CLI entry points",
    )

    for command in validation_commands:
        result = run(command)
        if not result.ok:
            return build_error_result("VALIDATION_FAILED", result.summary, retryable=False)

    return build_success_result(status=git_status(project_root), archive_root=archive_root)
```

### 风险点 / 边界条件

- `pyproject.toml` 当前已有工作区 diff，必须做增量合并。
- `_archive/` 中保留的 GUI 历史代码不能被测试发现为主项目包。
- 历史 `docs/superpowers/` GUI 方案文档可保留；主 README / AGENTS / CLAUDE 不再提供 GUI 用法。
- 不修改 `src/analytics/`、`src/etl/`、`src/excel/` 的业务实现和契约 baseline。

---

## 4. 推荐改造步骤

## Step 0：动手前检查 Git 状态

当前观察到工作区已有改动：

```text
D .codegraph/.gitignore
M pyproject.toml
```

执行前必须先确认这些改动是不是用户已有工作，避免覆盖。

```powershell
cd D:\python_program\02--costing_calculate
git status --short
git diff -- pyproject.toml
```

如果 `pyproject.toml` 已有用户改动，先读清楚再合并，不要直接覆盖。

---

## Step 1：归档 GUI 源码

按「归档不删除」原则，移动：

```text
src/gui/
```

到：

```text
_archive/gui-removed-20260709/src-gui/
```

GUI 源码当前包括：

```text
src/gui/__init__.py
src/gui/app.py
src/gui/form_state.py
src/gui/main_window.py
src/gui/styles.py
src/gui/task_worker.py
src/gui/validators.py
```

---

## Step 2：归档 GUI 测试

移动：

```text
tests/test_gui_form_state.py
tests/test_gui_main_window.py
tests/test_gui_styles.py
tests/test_gui_task_worker.py
```

到：

```text
_archive/gui-removed-20260709/tests-gui/
```

归档后，完整测试不应再需要 PySide6。

---

## Step 3：修改 `pyproject.toml`

### 3.1 Python 版本

从：

```toml
requires-python = ">=3.10"
```

改为：

```toml
requires-python = ">=3.11"
```

### 3.2 删除 GUI optional dependency

从：

```toml
[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "ruff>=0.8.0",
]
gui = [
    "PySide6>=6.8",
]
```

改为：

```toml
[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "ruff>=0.8.0",
]
```

保留主依赖：

```toml
dependencies = [
    "pandas>=2.0.0",
    "openpyxl>=3.1.0",
    "numpy>=1.24.0",
    "beautifulsoup4>=4.12.0",
    "polars>=1.28.0",
    "python-calamine>=0.3.0",
    "xlsxwriter>=3.2.0",
]
```

### 3.3 限定 pytest 默认收集边界

归档目录里保留了历史 GUI 测试文件名，例如 `test_gui_*.py`。如果不限制 pytest 默认收集范围，裸 `python -m pytest` 会进入 `_archive/` 并尝试导入已移除的 `src.gui`。

因此在 `pyproject.toml` 中增加：

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
norecursedirs = ["_archive"]
```

这不是为了隐藏失败测试，而是为了表达主测试入口只覆盖当前活跃项目；历史归档代码不再参与默认测试收集。

---

## Step 4：清理 `README.md`

### 4.1 删除 GUI 安装说明

删除：

```md
如需 GUI 和开发依赖：
python -m pip install -e '.[dev,gui]'
```

保留或改为：

```md
如需开发依赖：
python -m pip install -e ".[dev]"
```

### 4.2 删除 GUI 启动命令

删除：

```md
python -m src.gui.app
```

### 4.3 删除 `## GUI 使用` 整节

删除 GUI 使用说明，包括：

```text
选择 GB/SK 管线
选择输入文件
自动查找
配置月份范围
维护产品白名单池
候选产品搜索
预检和后台处理
```

这些属于 GUI 操作说明，不再适用于纯 CLI 工具。

### 4.4 修改输出说明

从：

```md
质量摘要、运行时 error_log_count 和阶段耗时在控制台或 GUI 状态区展示
```

改为：

```md
质量摘要、运行时 error_log_count 和阶段耗时在控制台展示
```

### 4.5 修改目录结构

删除：

```md
- `src/gui/` - GUI 入口、窗口状态、校验与后台任务封装
```

保留并改写：

```md
- `src/services/` - CLI 应用服务层与结果对象
```

### 4.6 删除 GUI 测试说明

删除：

```md
python -m pytest tests/test_gui_form_state.py tests/test_gui_main_window.py -q
```

保留：

```md
python -m pytest tests -q
python -m ruff check src tests
python -m ruff format src tests --check
```

---

## Step 5：清理 `AGENTS.md`

删除 GUI 相关命令：

```md
python -m pip install -e '.[dev,gui]'
python -m src.gui.app
```

改成纯 CLI：

```md
python -m pip install -e ".[dev]"
python main.py gb --check-only --benchmark
python main.py sk --check-only --benchmark
python main.py gb
python main.py sk
python -m pytest tests -q
python -m ruff check src tests
python -m ruff format src tests --check
```

业务规则中：

```md
质量校验结果、运行时 error_log_count 和阶段耗时默认输出到控制台或 GUI 状态区
```

改为：

```md
质量校验结果、运行时 error_log_count 和阶段耗时默认输出到控制台
```

删除 GUI 专属规则：

```md
GUI 候选产品搜索按产品编码或产品名称包含匹配，只影响候选产品表显示；实际白名单过滤仍按 产品编码 + 产品名称 双字段精确匹配。
```

保留非 GUI 业务规则：

```md
产品白名单池按 产品编码 + 产品名称 双字段精确匹配，影响分析维度 Sheet，不过滤 成本计算单总表 和 成本计算单数量聚合维度。
```

---

## Step 6：清理 `CLAUDE.md`

同 `README.md` / `AGENTS.md`，删除或改写：

```text
.[dev,gui]
python -m src.gui.app
GUI 状态区
GUI 测试
```

改成纯 CLI 说明。

---

## Step 7：增加防回归测试

在：

```text
tests/architecture/test_import_rules.py
```

增加：

```python
def test_project_has_no_gui_package() -> None:
    assert not (SRC_ROOT / 'gui').exists()
```

这个测试表达很明确：项目不再允许恢复 `src/gui` 包。

如果后续只是想禁止核心层依赖 GUI，而不是禁止 GUI 包存在，可以换成更温和的测试。但本次用户目标是「不想要 GUI」，所以建议直接禁止 `src/gui`。

---

## 5. Python 3.11 环境建议

当前 `test` 环境观察到是 Python 3.10：

```text
Python 3.10.20
```

项目改成 Python 3.11+ 后，建议新建独立环境，而不是升级旧 `test` 环境。

```powershell
conda create -n costing311 python=3.11 -y
conda activate costing311
cd D:\python_program\02--costing_calculate
python -m pip install -e ".[dev]"
```

如果坚持复用旧 `test`：

```powershell
conda activate test
conda install python=3.11 -y
python -m pip install -e ".[dev]"
```

但更推荐：

```text
costing311
```

原因：该项目以后明确 Python 3.11+，独立环境更干净，也不影响其他项目。

---

## 6. 验证命令

在项目目录执行：

```powershell
cd D:\python_program\02--costing_calculate
conda activate costing311
```

确认 Python：

```powershell
python --version
```

期望：

```text
Python 3.11.x
```

安装依赖：

```powershell
python -m pip install -e ".[dev]"
```

检查 CLI：

```powershell
python main.py --help
```

期望能看到：

```text
usage: main.py [-h] [--month-start MONTH_START] [--month-end MONTH_END] [--check-only] [--benchmark] {gb,sk}
```

运行测试：

```powershell
python -m pytest tests -q
```

检查默认 pytest 收集不会进入归档目录：

```powershell
python -m pytest -q --collect-only
```

运行 lint：

```powershell
python -m ruff check src tests
python -m ruff format src tests --check
```

搜索确认 GUI 引用已清理：

```powershell
Select-String -Path README.md,AGENTS.md,CLAUDE.md,pyproject.toml -Pattern "GUI","gui","PySide6","src.gui","dev,gui"
Get-ChildItem tests -Recurse -Filter *.py | Select-String -Pattern "src.gui","PySide6","QApplication","QWidget"
Get-ChildItem src -Recurse -Filter *.py | Select-String -Pattern "src.gui","PySide6","QApplication","QWidget"
```

预期：

```text
源码和测试中不再有 GUI / PySide6 引用
文档中不再提供 GUI 安装、启动、测试说明
```

---

## 7. 风险点与处理方式

### 风险 1：`pyproject.toml` 已有改动被覆盖

当前观察到 `pyproject.toml` 已有修改。执行前必须先看 diff。

处理方式：

```powershell
git diff -- pyproject.toml
```

只做增量合并，不整文件覆盖。

### 风险 2：误删 `src/services/`

`src/services/` 已经是 CLI 主链路的一部分，不是 GUI 专属层。

处理方式：保留 `src/services/`，只清理 GUI 源码和 GUI 测试。

### 风险 3：README / AGENTS / CLAUDE 漏掉 GUI 文案

处理方式：改完后全局搜索：

```powershell
Select-String -Path README.md,AGENTS.md,CLAUDE.md -Pattern "GUI","gui","PySide6","src.gui","dev,gui"
```

逐条确认是否应删除或改写。

### 风险 4：旧 GUI 设计文档仍在 `docs/superpowers/`

项目里已有历史 GUI 设计文档，例如：

```text
docs/superpowers/specs/2026-05-20-costing-gui-design.md
docs/superpowers/plans/2026-05-20-costing-gui-implementation.md
```

这些是历史方案文档，可以保留，不影响运行。但如果后续希望避免误导，可以在这些历史文档顶部加一句：

```md
> 历史记录：GUI 能力已在 2026-07-09 后移除，当前项目以 CLI 为唯一入口。
```

本次最小改造不强制处理历史归档文档。

---

## 8. 交付标准

完成后必须满足：

```text
1. src/gui/ 不存在，或已归档到 _archive/gui-removed-20260709/src-gui/
2. tests/test_gui_*.py 不存在，或已归档到 _archive/gui-removed-20260709/tests-gui/
3. pyproject.toml requires-python = ">=3.11"
4. pyproject.toml 不再包含 PySide6 / gui optional dependency
5. README.md / AGENTS.md / CLAUDE.md 不再出现 GUI 安装、启动、测试说明
6. python main.py --help 正常
7. python -m pytest tests -q 通过
8. python -m ruff check src tests 通过
9. python -m ruff format src tests --check 通过
10. tests/architecture/test_import_rules.py 有防 GUI 回归测试
11. python -m pytest -q --collect-only 不收集 _archive/ 下的历史 GUI 测试
```

---

## 9. 给 Codex 的执行提示词

```text
你在 D:\python_program\02--costing_calculate 项目中执行一次轻量化收口改造。

目标：
1. 移除 PySide6 GUI 能力，把项目收敛为纯 CLI ETL 工具。
2. 项目 Python 版本要求改为 >=3.11。
3. 不改变核心 ETL、analytics、excel 的业务逻辑和输出口径。
4. 保留 src/services，它是 CLI 应用服务层，不是 GUI 专属层。

执行要求：
- 先查看 git status 和 pyproject.toml 当前 diff，不要覆盖已有改动。
- 将 src/gui/ 归档到 _archive/gui-removed-20260709/src-gui/。
- 将 tests/test_gui_form_state.py、tests/test_gui_main_window.py、tests/test_gui_styles.py、tests/test_gui_task_worker.py 归档到 _archive/gui-removed-20260709/tests-gui/。
- 修改 pyproject.toml：
  - requires-python 改为 ">=3.11"
  - 删除 gui optional dependency 和 PySide6
  - 保留 dev optional dependency
  - 增加 pytest testpaths/norecursedirs，避免默认 pytest 收集 _archive/
- 清理 README.md、AGENTS.md、CLAUDE.md：
  - 删除 .[dev,gui]
  - 删除 python -m src.gui.app
  - 删除 GUI 使用、GUI 测试、GUI 状态区等描述
  - 将“控制台或 GUI 状态区”改为“控制台”
  - 保留产品白名单的业务规则，但删除 GUI 候选产品搜索说明
- 在 tests/architecture/test_import_rules.py 增加防回归测试：
  - assert not (SRC_ROOT / "gui").exists()
- 不要修改业务契约 baseline。
- 不要修改 analytics / etl / excel 的业务实现，除非测试暴露出纯粹的导入路径问题。

验证：
- 使用 Python 3.11 环境。
- 运行：
  python main.py --help
  python -m pytest tests -q
  python -m ruff check src tests
  python -m ruff format src tests --check

最后输出：
1. 修改文件清单
2. 归档路径
3. 测试结果
4. 如果有失败，列出失败原因和建议，不要假装通过
```

---

## 10. 最终判断

这个项目去 GUI 的正确姿势不是重构核心，而是切掉外层 UI：

```text
删除 / 归档 src/gui
删除 / 归档 GUI 测试
删除 PySide6 optional dependency
清理文档和 agent 指令
保留 services 与 CLI 主链路
正式声明 Python >=3.11
```

这样改造面小、风险低，也符合当前项目已经存在的 CLI 入口和服务层结构。

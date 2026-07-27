# 成本核算 GUI 深灰主题与进度条设计

日期：2026-05-21

状态：设计已在对话中确认，待用户复核本文档后进入实施计划。

## 目标

在不重写 GUI、不切换框架、不改变 ETL 业务口径的前提下，把现有 PySide6 桌面界面升级为低饱和深灰的数据中台风格，并加入粗粒度真实进度条。

本次目标包含两部分：

- 视觉与布局升级：重排 `src/gui/main_window.py` 的主窗口布局，重构 `src/gui/styles.py` 的 QSS，形成左侧配置、右侧监控、底部进度区和全局操作栏。
- 粗粒度真实进度：通过轻量 `ProgressEvent` 和可选 `progress_callback`，把现有 ETL 阶段进度从 service / ETL 层上报给 GUI。

这不是 GUI 重写，也不是 ETL 核心重构。现有 `ServiceWorker` 后台执行模型、预检门禁、白名单编辑、候选产品表、覆盖确认、过期任务结果忽略和 CLI 兼容性都必须保留。

## 输入

关键入参：

- 管线：`gb` 或 `sk`
- 输入 workbook：用户选择的 `.xlsx` 文件，或按当前管线自动查找的文件
- 输出目录：默认使用管线配置，也允许 GUI 手动选择
- 月份范围：`YYYY-MM` 闭区间，可为空
- 产品白名单池：按 `产品编码 + 产品名称` 精确匹配的有序列表
- 任务类型：`scan`、`precheck` 或 `run`
- 后台任务进度：由 ETL 真实阶段产生的 `ProgressEvent`

上下文信息：

- 当前项目根目录：`/home/george/Python_program/02--costing_calculate`
- GUI 入口：`python -m src.gui.app`
- 主窗口：`src/gui/main_window.py`
- 样式：`src/gui/styles.py`
- 后台 worker：`src/gui/task_worker.py`
- service 入口：`src/services/costing_service.py`
- ETL 阶段编排：`src/etl/pipeline.py`
- workbook 处理外壳：`src/etl/costing_etl.py`

外部依赖：

- Python 3.11+
- PySide6
- pandas / polars / python-calamine / openpyxl / xlsxwriter
- Linux 桌面环境
- Miniconda `test` 环境用于验证

运行环境约束：

- 默认 Linux bash / zsh
- 不新增 GUI 框架，不引入 `customtkinter`
- 不新增外部样式库
- 不污染系统 Python
- GUI 不能在主线程运行耗时 ETL
- GUI 不能接收或渲染完整大表 DataFrame

## 输出

成功输出：

- 主窗口变为低饱和 Muted Slate / Charcoal 主题
- 界面结构变为左侧配置、右侧监控、底部进度、底部操作栏
- 右侧 KPI 区独立展示状态、error_log 行数、候选产品数、输出路径和阶段耗时
- `扫描产品`、`预检`、`开始处理` 三类后台任务都会显示粗粒度真实进度
- `开始处理` 成功后仍只写出当前业务契约要求的 workbook

失败输出：

- 表单校验失败时不启动 worker，进度保持 0%，状态显示配置错误
- ETL 失败时不强行把进度推进到 100%，状态显示失败，日志记录 `error_code` 和技术细节
- worker 异常时主窗口显示失败文案，保留现有异常日志路径
- 过期任务结果继续被忽略，不污染候选产品表、输出路径、按钮状态或进度区

副作用：

- 会修改 GUI 布局、QSS、worker 进度信号、service / ETL 可选进度回调
- 不改变 workbook sheet 名称、字段、金额规则、白名单规则、月份过滤规则或错误日志业务口径
- 不改变 CLI 默认使用方式；未传 `progress_callback` 时 service 行为保持兼容

## 方案选择

采用方案 B：在现有 PySide6 架构上做视觉与布局升级，并通过轻量进度回调支持粗粒度真实进度。

不采用的方案：

- 纯 UI 活动进度条：改动小，但不是真实进度，无法说明任务到达哪个 ETL 阶段。
- 全量 GUI 重写或切换 `customtkinter`：会丢掉现有 PySide6 测试资产和稳定逻辑，收益不足。
- 日志解析式进度：会让 UI 状态依赖日志文本，长期维护不稳。
- 候选产品数量进度：候选产品不是当前 ETL 主流程真实循环边界，不能用来伪装进度百分比。

## 范围

需要修改：

- `src/gui/main_window.py`
  - 调整主窗口布局结构
  - 新增 KPI label
  - 新增 `QProgressBar` 和进度文案
  - 把操作按钮移到底部全局操作栏
  - 增加必要 objectName，便于 QSS 精准命中
- `src/gui/styles.py`
  - 重构主窗口为低饱和深灰主题
  - 保留确认弹窗浅色主题
  - 增加表格、日志终端、KPI 卡片、进度条和底部操作栏样式
- `src/gui/task_worker.py`
  - 增加 `progress` signal
  - 在后台线程中把 progress callback 转成 Qt signal
- `src/services/costing_service.py`
  - 新增 `ProgressEvent`
  - 给 `precheck_costing_run()` 和 `run_costing_request()` 增加可选 `progress_callback`
  - 在准备、失败、完成节点上报进度
- `src/etl/costing_etl.py`
  - 给 `prepare_payload()` 和 `process_file()` 增加可选 `progress_callback`
  - 在 export 阶段上报进度
- `src/etl/pipeline.py`
  - 给 `build_workbook_payload()` 增加可选 `progress_callback`
  - 在现有真实阶段完成后上报进度
- 相关测试文件
  - `tests/test_gui_styles.py`
  - `tests/test_gui_main_window.py`
  - `tests/test_costing_service.py`
  - `tests/test_etl_pipeline.py`

不修改：

- 不改 workbook 业务 contract
- 不改产品白名单匹配规则
- 不改金额、数量、异常等级阈值和独立成本项规则
- 不改 CLI 参数含义
- 不改文件发现规则
- 不引入新的 pip 依赖
- 不把确认弹窗改成暗色

## 架构

新增进度契约建议放在 `src/services/costing_service.py`，让 GUI 依赖 service 层稳定类型，而不是直接依赖 ETL 内部实现。

```python
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class ProgressEvent:
    percent: int
    stage: str
    message: str


ProgressCallback = Callable[[ProgressEvent], None]
```

service 函数增加可选 callback：

```python
def precheck_costing_run(
    request: CostingRunRequest,
    *,
    validate_output_dir: bool = True,
    progress_callback: ProgressCallback | None = None,
) -> CostingRunResult:
    pass


def run_costing_request(
    request: CostingRunRequest,
    *,
    progress_callback: ProgressCallback | None = None,
) -> CostingRunResult:
    pass
```

callback 是可选的。CLI、旧测试和非 GUI 调用方可以继续不传该参数。

`ServiceWorker` 的职责是线程边界转换：

- worker 在线程中调用 service
- worker 构造 `emit_progress(event)` callback
- callback 只调用 `self.signals.progress.emit(event)`
- GUI 主线程中的 slot 更新 `QProgressBar` 和进度文案

ETL 层只认识 callback，不认识 Qt signal，不依赖 GUI。

## 布局设计

主窗口采用三层结构：

```text
root
├── main_content_container
│   ├── left_panel  (约 40%)
│   └── right_panel (约 60%)
├── progress_area
└── bottom_action_bar
```

左侧面板：

- 标题：`成本核算分析工具`
- 副标题：`金蝶 ERP 成本计算单处理`
- `输入配置`
  - 管线
  - 输入文件
  - 输出目录
  - 月份范围，开始月份和结束月份放在同一行
- `产品白名单池`
  - 保留现有 `whitelist_table`
  - 保留 `新增 / 删除 / 上移 / 下移 / 保存 / 恢复默认`
- `候选产品`
  - 保留现有 `candidate_table`
  - 保留 `加入白名单`

右侧面板：

- `status_dashboard`
  - 复用 `status_label` 显示主状态
  - 复用 `stage_label` 显示阶段耗时
- `kpi_row`
  - 新增 `error_count_label`
  - 新增 `candidate_count_label`
  - 新增 `workbook_path_label`
  - 保留 `summary_label` 作为兼容摘要或兜底文本，不再作为主要视觉区域
- `日志`
  - 保留 `log_edit`
  - 设置 objectName 为 `LogTerminal`

进度区：

- 新增 `progress_label`
- 新增 `progress_bar`
- 位于主内容区和底部操作栏之间，横跨窗口
- 空闲时显示弱化的“等待任务”并保持 0%
- 后台任务开始后显示当前阶段文案和百分比
- 成功时 100%
- 失败时显示失败文案，不强制 100%

底部操作栏：

- 把现有按钮移入全局 `bottom_action_bar`
  - `scan_button`
  - `precheck_button`
  - `run_button`
  - `open_output_button`
  - `clear_button`
  - `exit_button`
- 按钮变量名不变，现有 signal 连接不变
- `开始处理` 继续使用 `PrimaryButton`

## 视觉设计

主窗口使用低饱和 Muted Slate / Charcoal 主题，避免赛博霓虹风和高饱和对比。

颜色建议：

- 主背景：`#1E222B`
- 面板背景：`#252932`
- 日志背景：`#181A1F`
- 边框：`#3E4451`
- 主文字：`#E5E7EB`
- 次级文字：`#A0AEC0`
- 控件背景：`#1F2430`
- 主按钮：`#2B6CB0`
- 主按钮 hover：`#3182CE`
- 成功：`#48BB78`
- 失败：`#E53E3E`
- 警告：`#D69E2E`

样式原则：

- 使用 objectName 精准命中容器和关键控件，减少全局选择器污染
- 表格启用 alternating row colors
- 表格弱化 gridline，只保留低对比边界
- 日志区使用等宽字体和足够内边距
- KPI 卡片使用短标签和清晰数值，避免长段文本挤在一个 label 里
- 底部操作栏使用统一高度和间距
- 确认弹窗继续使用浅色 `MESSAGE_BOX_STYLESHEET`

确认弹窗策略：

- 主窗口深色
- 覆盖确认、恢复默认等 `QMessageBox` 保持浅色高对比
- 保留中文按钮文本
- 不让主窗口暗色 QSS 污染确认框可读性

## 进度事件设计

进度百分比表示“当前已到达的真实阶段”，不是耗时预测，也不是线性时间估计。

阶段映射：

| percent | stage | message | 适用任务 |
| --- | --- | --- | --- |
| 0 | prepare | 正在校验输入配置 | scan / precheck / run |
| 5 | prepare | 已完成路径与参数校验 | scan / precheck / run |
| 10 | ingest | 已读取 workbook | scan / precheck / run |
| 30 | normalize | 已完成标准化和月份过滤 | scan / precheck / run |
| 45 | fact | 已拆分事实表 | scan / precheck / run |
| 70 | analysis | 已完成分析与质量校验 | scan / precheck / run |
| 85 | presentation | 已构建输出 Sheet | scan / precheck / run |
| 95 | export | 正在写出 workbook | run |
| 100 | done | 任务完成 | scan / precheck / run |

`scan` 当前底层复用 `precheck_costing_run()`，所以同样走读取、标准化、分析和 presentation 阶段；它不写 workbook，因此不发出 `export`。

`precheck` 不写 workbook，因此不发出 `export`。

`run` 成功时包含 `export` 和 `done`。

进度回调异常处理：

- `_report_progress()` 捕获 callback 自身异常，避免进度 UI 问题破坏 ETL 主流程
- 可以记录 logger warning，但不能因为 GUI 进度失败导致业务处理失败

## 错误处理

表单校验失败：

- 不启动 worker
- 状态显示配置错误
- 进度条保持 0%
- 日志追加具体原因

service 返回失败：

- 主状态显示失败消息
- 进度条保持当前值或显示失败状态，不强制设置 100%
- 日志输出 `error_code`
- 如有 `technical_detail`，继续写入日志

worker 异常：

- `failed` signal 仍由 worker 发出
- 主窗口设置失败状态
- 候选产品表清空
- 进度文案显示任务异常终止

过期任务：

- 继续使用现有 `form_revision` 机制
- worker finished / failed 的过期结果继续忽略
- 进度事件也需要避免旧任务污染界面
- 推荐在 `_on_worker_progress()` 中只接受当前 active worker 对应 revision 的事件；如果实现上用 lambda 捕获 `request_revision`，则 progress slot 同样先判断 revision

清空条件：

- 清空输入和月份
- 清空候选产品
- 重置 `precheck_passed`
- 重置 `last_output_dir`
- 重置进度条为 0
- 进度文案回到“等待任务”

## 伪代码草案

```python
# 目标：让 GUI 展示真实 ETL 阶段，同时不让 GUI 线程接触业务 DataFrame。

@dataclass(frozen=True)
class ProgressEvent:
    percent: int
    stage: str
    message: str


ProgressCallback = Callable[[ProgressEvent], None]


def _report_progress(
    callback: ProgressCallback | None,
    percent: int,
    stage: str,
    message: str,
) -> None:
    if callback is None:
        return
    try:
        callback(ProgressEvent(percent=percent, stage=stage, message=message))
    except Exception:
        # 进度上报是可观测性增强，不能反过来中断 ETL 主流程。
        logger.warning("Progress callback failed", exc_info=True)


def run_costing_request(
    request: CostingRunRequest,
    *,
    progress_callback: ProgressCallback | None = None,
) -> CostingRunResult:
    _report_progress(progress_callback, 0, "prepare", "正在校验输入配置")

    prepared, validation_error = _prepare_request(request)
    if validation_error is not None:
        _report_progress(progress_callback, 0, "failed", validation_error.message)
        return validation_error

    _report_progress(progress_callback, 5, "prepare", "已完成路径与参数校验")

    etl = _build_etl(request, prepared.month_range)
    ok = etl.process_file(
        request.input_path,
        prepared.workbook_path,
        progress_callback=progress_callback,
    )
    if not ok:
        _report_progress(progress_callback, 0, "failed", "处理失败")
        return _failed(
            message="处理失败，请查看日志详情",
            error_code="ETL_FAILED",
            workbook_path=prepared.workbook_path,
        )

    _report_progress(progress_callback, 100, "done", "处理完成")
    return _result_from_etl(
        etl,
        status=ServiceStatus.SUCCEEDED,
        message="处理成功",
        input_path=request.input_path,
        workbook_path=prepared.workbook_path,
        output_written=True,
    )


def build_workbook_payload(
    input_path: Path,
    *,
    progress_callback: ProgressCallback | None = None,
    standalone_cost_items: Sequence[str],
    month_range: MonthRange | None,
    presentation_product_order: Sequence[tuple[str, str]],
) -> WorkbookPayload:
    stage_timings: dict[str, float] = {}

    raw = load_raw_workbook(input_path)
    stage_timings["ingest"] = elapsed_seconds_for_current_stage()
    _report_progress(progress_callback, 10, "ingest", "已读取 workbook")

    normalized = build_normalized_cost_frame(raw)
    normalized, month_summary = apply_month_range_to_normalized_frame(normalized, month_range)
    stage_timings["normalize"] = elapsed_seconds_for_current_stage()
    _report_progress(progress_callback, 30, "normalize", "已完成标准化和月份过滤")

    split_result = split_normalized_frames(normalized)
    stage_timings["fact"] = elapsed_seconds_for_current_stage()
    _report_progress(progress_callback, 45, "fact", "已拆分事实表")

    artifacts = build_report_artifacts(split_result)
    stage_timings["analysis"] = elapsed_seconds_for_current_stage()
    _report_progress(progress_callback, 70, "analysis", "已完成分析与质量校验")

    sheet_models = build_sheet_models(artifacts)
    stage_timings["presentation"] = elapsed_seconds_for_current_stage()
    _report_progress(progress_callback, 85, "presentation", "已构建输出 Sheet")

    return WorkbookPayload(
        sheet_models=sheet_models,
        quality_metrics=artifacts.quality_metrics,
        error_log_count=len(artifacts.error_log),
        stage_timings=stage_timings,
    )


class ServiceWorker(QRunnable):
    def run(self) -> None:
        def emit_progress(event: ProgressEvent) -> None:
            self.signals.progress.emit(event)

        self.signals.started.emit(self.label)
        try:
            result = self.function(self.request, progress_callback=emit_progress)
        except Exception as exc:
            self.signals.failed.emit(str(exc))
            return
        self.signals.finished.emit(result)


class MainWindow(QMainWindow):
    def _on_worker_progress(self, event: ProgressEvent, request_revision: int) -> None:
        if self._is_stale_request(request_revision):
            return
        self.progress_bar.setValue(event.percent)
        self.progress_label.setText(event.message)
        if event.stage != self._last_progress_stage:
            self._append_log(f"[progress] {event.stage}: {event.message}")
            self._last_progress_stage = event.stage
```

## 测试策略

QSS / 主题测试：

- `tests/test_gui_styles.py` 断言 `QMainWindow` 使用 muted slate 背景
- 断言 `QLineEdit` / `QComboBox` 有明确暗色背景、文字色、选择色
- 断言 `QTableWidget` 有暗色背景、选择态和 alternating row 相关样式
- 断言 `QTextEdit#LogTerminal` 有终端背景和等宽字体
- 断言 `QProgressBar` 和 `QProgressBar::chunk` 有明确颜色
- 断言 `MESSAGE_BOX_STYLESHEET` 继续保持浅色确认框

GUI 布局和状态测试：

- 主窗口创建后存在 `progress_bar` 和 `progress_label`
- 底部按钮变量仍是原对象，且原有启停逻辑正常
- 未预检通过时 `开始处理` 仍禁用
- 预检成功后 `开始处理` 启用
- `_on_worker_progress()` 能更新进度条数值和文案
- `清空条件` 会重置进度到 0
- 失败结果不会把进度强制改成 100
- 过期任务结果继续被忽略

进度契约测试：

- `ProgressEvent` 可构造，字段为 `percent`、`stage`、`message`
- `precheck_costing_run(request, progress_callback=callback)` 发出 `prepare` 和真实阶段事件
- `run_costing_request(request, progress_callback=callback)` 成功时包含 `export` 和 `done`
- 预检和扫描路径不发出 `export`
- service 不传 callback 时保持兼容
- callback 抛异常时不导致 service 失败

ETL 阶段测试：

- `CostingEtlPipeline.build_workbook_payload()` 在 `ingest`、`normalize`、`fact`、`analysis`、`presentation` 后上报进度
- `CostingWorkbookETL.process_file()` 在写 workbook 时上报 `export`
- `CostingWorkbookETL.prepare_payload()` 不上报 `export`

## 验收方式

自动验证命令：

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest tests/test_gui_styles.py tests/test_gui_form_state.py tests/test_gui_main_window.py tests/test_costing_service.py tests/test_etl_pipeline.py -q
/home/george/miniconda3/bin/conda run -n test python -m pytest tests -q
/home/george/miniconda3/bin/conda run -n test python -m ruff check src tests
/home/george/miniconda3/bin/conda run -n test python -m ruff format src tests --check
```

人工验收：

- `python -m src.gui.app` 能启动主窗口
- 主窗口是低饱和深灰，不刺眼
- 覆盖确认和恢复默认确认弹窗仍是浅色且按钮文字清楚
- 点击扫描产品、预检、开始处理时，进度区显示阶段变化
- 任务完成后 KPI 区显示 error_log 行数、候选产品数、输出路径和阶段耗时
- 日志区可读，不遮挡、不挤压主要按钮
- 缩放窗口时左侧约 40%、右侧约 60% 的布局大体稳定
- 底部按钮保持在全局操作栏，不回到右侧监控面板内部

## 风险点 / 边界条件

- 进度百分比不是耗时预测。大 workbook 在 `analysis` 或 `export` 阶段停留较久属于真实表现。
- 进度 callback 需要保持可选，否则会破坏 CLI 和已有调用方兼容性。
- 进度 callback 失败不能影响 ETL 主流程。
- 深色主窗口不能污染确认弹窗，否则可能重现深色桌面下确认框文字不可读问题。
- 表格深色主题要明确选择态、禁用态和滚动条颜色，否则 Linux 不同桌面主题下容易读不清。
- 布局改造不能重命名已有关键 widget 变量，否则会破坏测试和信号连接。
- 右侧 KPI 新 label 可以新增，但 `summary_label` 应保留，降低兼容风险。
- 扫描产品底层复用预检逻辑，所以它显示完整预检阶段进度是合理行为；但它不能显示 `export`。

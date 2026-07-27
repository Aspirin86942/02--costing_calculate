# 成本核算 GUI 产品化设计

日期：2026-05-20

状态：设计已在对话中逐段确认，待用户复核本文档后进入实施计划。

## 目标

把 `/home/george/Python_program/02--costing_calculate` 从命令行 ETL 工具扩展为 Linux 优先的 PySide6 桌面 GUI 应用，同时保留现有 CLI 能力。

第一版目标是本地可运行、可预检、可后台处理、可维护产品白名单池，并统一 CLI/GUI 的业务输出口径。第一版不做 PyInstaller 打包，Linux onedir 打包留到后续阶段。

这不是重写 ETL 核心。GUI 只承载配置、预检、任务状态、日志和结果摘要；成本核算、月份过滤、白名单过滤、Excel 写出仍由现有 ETL 核心和新增 service adapter 调用。

## 输入

关键入参：

- 管线：`gb` 或 `sk`
- 输入 workbook：用户选择的 `.xlsx` 文件，或按管线配置自动查找的第一个匹配文件
- 输出目录：默认 `data/processed/<pipeline>/`，GUI 允许手动选择
- 月份范围：`month_start` / `month_end`，允许为空；非空时必须是 `YYYY-MM`，闭区间过滤
- 产品白名单池：按管线维护的有序 `产品编码 + 产品名称` 列表
- 执行模式：预检或正式处理
- benchmark：保留运行时阶段耗时输出能力

上下文信息：

- 当前项目根目录为 `/home/george/Python_program/02--costing_calculate`
- 现有 CLI 入口为 `main.py`
- 当前核心 ETL 入口包括 `src/etl/runner.py`、`src/etl/costing_etl.py`、`src/etl/pipeline.py`
- 当前月份过滤入口为 `src/etl/month_filter.py`
- 当前 GB/SK 默认产品白名单定义在 `src/config/pipelines.py`

外部依赖：

- Python 3.11+
- PySide6
- pandas / polars / python-calamine / openpyxl / xlsxwriter
- Linux 桌面环境
- Miniconda `test` 环境用于安装和验证

运行环境约束：

- 默认 Linux bash / zsh
- 不污染系统 Python
- 优先使用 `/home/george/miniconda3/bin/conda run -n test ...`
- GUI 不在主线程运行耗时 ETL
- GUI 不加载或渲染完整大表

## 输出

成功输出：

- 只落盘一个 workbook：`*_处理后.xlsx`
- workbook 只包含 4 张 sheet：
  - `成本计算单总表`
  - `成本计算单数量聚合维度`
  - `成本分析工单维度`
  - `成本分析产品维度`

不再落盘：

- `*_处理后_error_log.csv`
- `*_处理后_summary.json`
- 额外 `.log` 文件

运行时摘要：

- GUI 和 CLI 仍显示必要质量摘要、error_log 行数、阶段耗时、输入/输出文件大小、失败原因
- 这些摘要只作为运行时状态或控制台文本，不额外写成 CSV/JSON

失败返回：

- CLI 返回非零退出码并输出人可读错误摘要
- GUI 显示业务提示和日志详情
- 预检失败不写任何产物
- 正式处理失败不生成外部错误文件

覆盖策略：

- GUI 预检发现目标 workbook 已存在时提示覆盖风险，用户确认后才能处理
- CLI 第一版保持现有兼容行为：同名 workbook 可覆盖，但控制台打印预计输出路径和覆盖提示

## 范围

第一版包含：

- PySide6 GUI 入口：`python -m src.gui.app`
- GUI 主窗口、输入配置、白名单池、候选产品扫描、预检、后台处理、结果摘要和日志面板
- CLI/GUI 共用 service adapter
- 产品白名单共享配置
- 删除三张价量比 sheet
- 重命名剩余四张 sheet
- 只输出 `.xlsx`
- README、AGENTS、contract baseline 和测试同步
- `pyproject.toml` 增加 `gui` extra

第一版不包含：

- PyInstaller 打包
- Windows exe
- Web UI
- WPS 加载项
- 全 workbook 白名单过滤
- 在 GUI 中预览大表
- 新的金额/数量/异常判定规则

## 业务口径变化

本次不是纯 GUI 外壳，会改变 workbook 产物 contract。

删除以下 sheet：

- `直接材料_价量比`
- `直接人工_价量比`
- `制造费用_价量比`

重命名剩余 sheet：

| 原 sheet | 新 sheet |
| --- | --- |
| `成本明细` | `成本计算单总表` |
| `产品数量统计` | `成本计算单数量聚合维度` |
| `按工单按产品异常值分析` | `成本分析工单维度` |
| `按产品异常值分析` | `成本分析产品维度` |

不改变：

- 金额计算
- 数量口径
- 单位成本
- Modified Z-score 阈值
- 独立成本项规则
- 月份闭区间规则
- `成本中心名称=集成车间` 时供应商字段不向下填充
- `成本计算单总表` 和 `成本计算单数量聚合维度` 的行粒度和业务字段口径

## 产品白名单池

白名单定义：

- 按管线维护，GB/SK 独立
- 每一项必须同时包含 `产品编码` 和 `产品名称`
- 匹配规则沿用当前核心：`产品编码 + 产品名称` 双字段精确匹配
- 顺序是业务输出顺序，不能改为字典序

作用范围：

- 保持当前语义：只影响分析展示页
- 删除价量比 sheet 后，主要影响：
  - `成本分析工单维度`
  - `成本分析产品维度`
- 不过滤：
  - `成本计算单总表`
  - `成本计算单数量聚合维度`

共享配置：

- 新增项目共享配置文件：`config/product_whitelists.json`
- 文件不存在时使用 `src/config/pipelines.py` 中的内置默认白名单
- GUI 保存后写入配置文件
- CLI 和 GUI 都优先读取配置文件
- 配置非法时明确报错，不静默回退到默认白名单
- `恢复默认` 只恢复当前管线，不影响另一个管线

建议 JSON 结构：

```json
{
  "gb": [
    {"product_code": "GB_C.D.B0048AA", "product_name": "BMS-400W驱动器"},
    {"product_code": "GB_C.D.B0040AA", "product_name": "BMS-750W驱动器"}
  ],
  "sk": [
    {"product_code": "DP.C.P0197AA", "product_name": "动力线"}
  ]
}
```

GUI 编辑方式：

- 白名单池表格列：`产品编码`、`产品名称`
- 操作：新增、删除、上移、下移、保存、恢复默认
- 未保存时显示状态标记
- 删除和恢复默认前提示确认
- 候选产品可从当前输入 workbook 扫描
- `扫描产品` 按钮和 `预检` 都会触发只读候选扫描
- 从候选加入白名单时，必须同时加入编码和名称

## 架构

推荐结构：

```text
src/
  config/
    product_whitelist_store.py
  services/
    costing_service.py
  gui/
    app.py
    main_window.py
    task_worker.py
    form_state.py
    validators.py
    styles.py
    widgets/
```

核心边界：

- `src/services/costing_service.py` 是 CLI/GUI 共用业务入口
- `src/gui/*` 只负责 UI、表单状态、后台任务、展示，不实现 ETL 业务规则
- `src/config/product_whitelist_store.py` 负责白名单加载、保存、校验、默认值恢复
- `src/etl/*` 继续负责读取、标准化、拆分、分析和写 workbook
- `main.py` 保持现有参数兼容，并逐步改为调用 service

核心数据结构：

- `RunRequest`
  - pipeline
  - input_path
  - output_dir
  - month_range
  - product_order
  - check_only
  - benchmark
  - overwrite_confirmed
- `PrecheckResult`
  - ok
  - errors
  - warnings
  - planned_workbook_path
  - candidate_products
  - quality_metrics
  - error_log_count
  - stage_timings
  - file_size
- `RunResult`
  - ok
  - workbook_path
  - quality_metrics
  - error_log_count
  - anomaly_summary
  - stage_timings
  - input_size_bytes
  - output_size_bytes
  - user_message
  - technical_detail
- `ServiceError`
  - error_code
  - message
  - retryable
  - details

## 数据流

预检：

1. GUI 收集表单状态。
2. GUI 从 `ProductWhitelistStore` 读取当前管线白名单。
3. service 校验文件、输出目录、月份范围、白名单配置。
4. service 只读加载 workbook，构建 payload，应用月份范围。
5. service 扫描候选产品。
6. service 返回计划 workbook 路径、质量摘要、error_log 行数、阶段耗时和候选产品。
7. GUI 展示预检结果，不写任何产物。

正式处理：

1. GUI 确认预检通过或重新构建 request。
2. 后台 worker 调用 service。
3. service 创建 `CostingWorkbookETL`，传入当前管线白名单、独立成本项、月份范围和 scope mode。
4. service 写出 workbook。
5. service 不写 error_log CSV，也不写 summary JSON。
6. service 返回轻量结果对象。
7. GUI 展示 workbook 路径、运行摘要和打开输出目录按钮。

CLI：

1. `main.py` 解析原有参数。
2. 自动发现输入文件。
3. 读取共享白名单配置。
4. 调用 service。
5. 打印运行时摘要，保持退出码语义。

## GUI 设计

主界面使用业务工具风格：安静、清晰、信息密度适中，不做营销页。

布局：

- 左侧：输入和白名单配置
- 右侧上方：状态和结果摘要
- 右侧下方：日志面板
- 底部：操作按钮或状态栏

左侧配置区：

- 应用标题：`成本核算分析工具`
- 副标题：`金蝶 ERP 成本计算单处理`
- 管线选择：`GB` / `SK`
- 输入文件：路径输入框、`选择文件`、`自动查找`
- 输出目录：路径输入框、`选择目录`
- 月份范围：开始月份、结束月份
- 白名单池：有序表格
- 候选产品：扫描结果列表，支持加入白名单

按钮：

- `扫描产品`
- `预检`
- `开始处理`
- `打开输出目录`
- `清空条件`
- `退出`

状态：

- 等待配置
- 正在扫描产品
- 正在预检
- 预检通过
- 预检失败
- 正在处理
- 处理成功
- 处理失败

按钮状态：

- 未选择输入文件时禁用 `扫描产品`、`预检`、`开始处理`
- 未预检通过时禁用 `开始处理`
- 正在扫描、预检或处理时，禁用会造成重复任务的按钮
- 处理完成或失败后恢复按钮状态

日志：

- 日志面板只读
- 以业务可读文本为主
- 技术错误放到日志详情，不作为主提示
- 不显示完整大表

## 预检与错误处理

快速校验：

- 输入文件存在
- 输入文件可读
- 扩展名是 `.xlsx`
- 输出目录可写
- 月份范围合法
- 管线合法
- 白名单无空编码、空名称和重复项
- 预计输出文件存在时返回覆盖警告

只读 ETL 预检：

- 读取 workbook 第一张表
- 识别双层表头
- 构建标准化 payload
- 校验关键字段
- 应用月份过滤
- 生成候选产品
- 生成运行时质量摘要和 error_log 行数
- 不写任何文件

错误码建议：

- `INVALID_INPUT`
- `FILE_NOT_FOUND`
- `FILE_NOT_READABLE`
- `UNSUPPORTED_FILE_TYPE`
- `MONTH_RANGE_INVALID`
- `WHITELIST_INVALID`
- `WORKBOOK_SCHEMA_INVALID`
- `OUTPUT_EXISTS`
- `ETL_FAILED`
- `OUTPUT_WRITE_FAILED`

错误展示：

- GUI 主状态显示人话
- 日志面板显示细节
- CLI 输出摘要并返回非零退出码
- 不静默吞异常

## 后台执行

GUI 使用 `QThread` 或 `QRunnable + QThreadPool`。

约束：

- worker 线程不直接操作 Qt UI
- worker 通过 signal 回传状态、日志和结果
- 大 DataFrame 不传给 GUI
- 不伪造百分比进度
- 阶段耗时优先在任务完成后展示

阶段名沿用现有 ETL：

- `ingest`
- `normalize`
- `fact`
- `analysis`
- `presentation`
- `export`

## 测试策略

核心 service 与配置测试：

- 白名单配置不存在时使用内置默认值
- 配置存在时覆盖默认值
- 非法配置返回结构化错误
- 白名单新增、删除、排序、恢复默认
- 月份范围校验复用 `MonthRange`
- 预检错误模型覆盖文件不存在、非 `.xlsx`、月份非法、白名单重复、输出文件已存在
- GUI adapter 传给 ETL 的 `product_order`、`month_range`、输入文件、输出目录正确

业务产物 contract 测试：

- workbook 只剩 4 张 sheet
- sheet 使用新名称
- 三张价量比 sheet 不再生成
- 不再生成 `error_log.csv` 和 `summary.json`
- 剩余 sheet 的列序、格式、冻结窗格、筛选器继续锁定
- CLI 仍支持 `python main.py gb/sk`
- `--check-only` 不写任何产物
- CLI 与 service 输出口径一致

GUI 单元测试：

- 表单状态构建不依赖真实窗口
- 未选择输入文件时不能开始处理
- 正在处理时禁用重复任务按钮
- 预检成功后允许开始处理
- 白名单未保存状态可识别
- worker 成功/失败信号能转换成 UI 状态

## 伪代码草案

```python
# 目标：用同一套 service 支撑 CLI 和 GUI，避免 GUI 层复制 ETL 规则

@dataclass(frozen=True)
class RunRequest:
    pipeline: str
    input_path: Path
    output_dir: Path
    month_start: str | None
    month_end: str | None
    product_order: tuple[tuple[str, str], ...]
    check_only: bool = False
    benchmark: bool = False
    overwrite_confirmed: bool = False


@dataclass(frozen=True)
class TaskResult:
    ok: bool
    workbook_path: Path | None
    candidate_products: tuple[tuple[str, str], ...]
    quality_metrics: tuple[QualityMetric, ...]
    error_log_count: int
    stage_timings: dict[str, float]
    message: str
    error_code: str | None = None
    retryable: bool = False
    technical_detail: str | None = None


def load_product_order_for_pipeline(pipeline: str) -> tuple[tuple[str, str], ...]:
    # 先读共享配置，确保 CLI 和 GUI 使用同一个业务白名单
    config_result = product_whitelist_store.load()
    if not config_result.exists:
        return default_product_order_from_pipeline_config(pipeline)
    if not config_result.ok:
        raise WhitelistConfigError(config_result.message)
    return config_result.product_order[pipeline]


def build_request_from_cli(args: argparse.Namespace) -> RunRequest:
    # CLI 保持原有参数兼容，只把旧参数转换成 service request
    pipeline_config = PIPELINES[args.pipeline]
    input_path = find_input_files(pipeline_config)[0]
    product_order = load_product_order_for_pipeline(args.pipeline)
    return RunRequest(
        pipeline=args.pipeline,
        input_path=input_path,
        output_dir=pipeline_config.processed_dir,
        month_start=args.month_start,
        month_end=args.month_end,
        product_order=product_order,
        check_only=args.check_only,
        benchmark=args.benchmark,
        overwrite_confirmed=True,
    )


def precheck_costing_workbook(request: RunRequest) -> TaskResult:
    validation = validate_request_without_reading_workbook(request)
    if not validation.ok:
        return TaskResult(
            ok=False,
            workbook_path=None,
            candidate_products=(),
            quality_metrics=(),
            error_log_count=0,
            stage_timings={},
            message=validation.message,
            error_code=validation.error_code,
            retryable=False,
        )

    try:
        # 复用 MonthRange，避免 GUI 自己实现另一套月份规则
        month_range = build_month_range(request.month_start, request.month_end)
        output_path = build_workbook_output_path(request.output_dir, request.input_path, month_range)
        if output_path.exists() and not request.overwrite_confirmed:
            return output_exists_warning(output_path)

        etl = build_etl_for_request(request, month_range)

        # prepare_payload 会构建内存 payload，但不写 workbook
        if not etl.prepare_payload(request.input_path):
            return schema_or_etl_failed_result(etl)

        # 候选产品必须来自标准化后的 workbook 数据，避免 GUI 层解析双层表头
        candidate_products = scan_candidate_products_from_same_read_path(request.input_path, etl)

        return TaskResult(
            ok=True,
            workbook_path=output_path,
            candidate_products=candidate_products,
            quality_metrics=etl.last_quality_metrics,
            error_log_count=etl.last_error_log_count,
            stage_timings=dict(etl.last_stage_timings),
            message='预检通过',
        )
    except Exception as error:
        return unexpected_error_result(error)


def run_costing_workbook(request: RunRequest) -> TaskResult:
    precheck = precheck_costing_workbook(request)
    if not precheck.ok:
        return precheck

    try:
        month_range = build_month_range(request.month_start, request.month_end)
        output_path = precheck.workbook_path
        assert output_path is not None

        output_path.parent.mkdir(parents=True, exist_ok=True)
        etl = build_etl_for_request(request, month_range)

        if not etl.process_file(request.input_path, output_path):
            return TaskResult(
                ok=False,
                workbook_path=None,
                candidate_products=precheck.candidate_products,
                quality_metrics=(),
                error_log_count=0,
                stage_timings={},
                message='处理失败，请查看日志详情',
                error_code='ETL_FAILED',
                retryable=False,
            )

        # 第一版只写 workbook，不写 error_log.csv 或 summary.json
        return TaskResult(
            ok=True,
            workbook_path=output_path,
            candidate_products=precheck.candidate_products,
            quality_metrics=etl.last_quality_metrics,
            error_log_count=etl.last_error_log_count,
            stage_timings=dict(etl.last_stage_timings),
            message='处理成功',
        )
    except Exception as error:
        return unexpected_error_result(error)
```

## 风险点 / 边界条件

- 删除和重命名 sheet 会改变 contract baseline，必须在变更说明里明确这是业务口径变化。
- 不再输出 `error_log.csv` 和 `summary.json` 后，历史依赖这些文件的外部流程会受影响；当前设计只保留运行时摘要。
- 白名单配置非法时不能静默回退，否则 CLI/GUI 口径会不可追踪。
- 候选产品扫描会读取 workbook，预检耗时可能增加；第一版不伪造百分比。
- GUI 不能传递大 DataFrame 给 UI 层，否则百万行 workbook 会卡界面。
- PySide6 在无图形环境下测试有限，GUI 视觉主要靠手工验收，核心状态和 adapter 用单元测试覆盖。
- CLI 第一版仍允许覆盖同名 workbook，GUI 会更严格地提示覆盖风险。

## 验收方式

安装：

```bash
/home/george/miniconda3/bin/conda run -n test python -m pip install -e '.[dev,gui]'
```

自动验证：

```bash
/home/george/miniconda3/bin/conda run -n test python -m pytest -q
/home/george/miniconda3/bin/conda run -n test python -m ruff check src tests
/home/george/miniconda3/bin/conda run -n test python -m ruff format src tests --check
```

CLI 验收：

```bash
/home/george/miniconda3/bin/conda run -n test python main.py gb --check-only --benchmark
/home/george/miniconda3/bin/conda run -n test python main.py sk --check-only --benchmark
```

GUI 验收：

```bash
/home/george/miniconda3/bin/conda run -n test python -m src.gui.app
```

人工确认：

- GUI 能启动并显示工具主界面
- 自动查找能找到 GB/SK 输入文件
- 预检不写任何产物
- 处理只写 `*_处理后.xlsx`
- 输出 workbook 只包含 4 张新命名 sheet
- 三张价量比 sheet 不存在
- 外部 `error_log.csv` 和 `summary.json` 不生成
- 白名单保存后 CLI 和 GUI 都使用同一配置
- 白名单只影响分析维度 sheet，不过滤总表和数量聚合维度
- 处理期间窗口不冻结

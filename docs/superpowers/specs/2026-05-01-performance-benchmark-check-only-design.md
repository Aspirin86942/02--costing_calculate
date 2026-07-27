# 性能 Benchmark 与预检模式设计

## 目标

为 `gb` / `sk` 管线补齐低风险可观测入口：

- `--check-only`：执行读取、标准化、拆表、分析与质量校验，但跳过 workbook / CSV 写出。
- `--benchmark`：在正常处理或预检处理后输出稳定的性能摘要，包含文件规模、阶段耗时、质量指标和计划/实际产物路径。

本阶段不改变金额、异常判定、白名单、workbook sheet 或 error_log 口径。

## 输入

- CLI 参数：
  - `pipeline`: `gb` 或 `sk`
  - `--month-start` / `--month-end`: 既有月份范围
  - `--check-only`: 只预检，不写产物
  - `--benchmark`: 输出 benchmark 摘要
- 运行上下文：
  - `PipelineConfig`: 输入目录、输出目录、白名单、独立成本项、产品异常 scope mode
  - 当前 raw 目录中匹配到的第一个 workbook
- 外部依赖：
  - `CostingWorkbookETL`
  - `CostingEtlPipeline.build_workbook_payload`
  - 当前 `QualityMetric` 与阶段耗时日志

## 输出

- `--check-only` 成功：
  - 返回码 `0`
  - 控制台输出 `mode=check-only`
  - 输出计划路径，但不创建 `*_处理后.xlsx` 和 `*_处理后_error_log.csv`
  - 输出质量摘要
- `--check-only --benchmark` 成功：
  - 在质量摘要后增加 `[benchmark]` 段
  - 包含输入文件字节数、阶段耗时、total 秒数、error_log 行数、输出文件计划路径
- 正常运行 `--benchmark`：
  - 保持原有 workbook 与 CSV 写出
  - 增加 `[benchmark]` 段，包含实际输出文件大小
- 失败：
  - 找不到输入文件返回 `1`
  - payload 构建失败返回 `1`
  - 不静默吞异常，沿用现有 logger 记录错误

## 伪代码草案

```python
# [伪代码草案]
# 目标：复用现有 ETL payload 链路，为正常导出和预检提供同一份质量/耗时证据
# 输入：
# - config: 当前 gb/sk 管线配置
# - month_range: 可选月份过滤区间
# - check_only: 是否跳过 workbook/csv 写出
# - benchmark: 是否输出 benchmark 段
# 输出：
# - exit_code: 0 表示处理或预检成功，1 表示输入缺失或 payload 构建失败
# - console_text: 控制台质量摘要，可附带 benchmark 段
# - artifacts: 正常模式才写 workbook 和 error_log.csv

def run_pipeline(config, month_range=None, check_only=False, benchmark=False):
    input_files = find_input_files(config)
    if not input_files:
        log_error("未找到输入文件")
        return 1

    input_file = input_files[0]
    workbook_path, error_log_path = build_output_paths(config.processed_dir, input_file, month_range)
    etl = build_etl_from_config(config, month_range)

    if check_only:
        # 为什么复用 payload：预检需要覆盖真实列识别、doc_type_split、白名单过滤和质量校验，
        # 但不应该触发 workbook/csv 写出副作用。
        payload_ok = etl.prepare_payload(input_file)
        if not payload_ok:
            return 1
        quality_text = build_quality_log_text(..., output_path=workbook_path)
        print("mode=check-only")
        print(quality_text)
        if benchmark:
            print(build_benchmark_log_text(..., output_written=False))
        return 0

    success = etl.process_file(input_file, workbook_path)
    if not success:
        return 1

    write_error_log_csv(error_log_path, etl.last_error_log_frame)
    print(build_quality_log_text(...))
    if benchmark:
        print(build_benchmark_log_text(..., output_written=True))
    return 0
```

## 风险点 / 边界条件

- `--check-only` 会执行分析 payload，耗时仍可能接近正常导出前半段；它的目标是跳过大 workbook 写出，不是只做静态扫描。
- 预检输出路径是计划路径，不代表文件已经创建。
- benchmark 是性能证据，不作为测试中的固定秒数断言；测试只断言字段存在和路径副作用。
- 如果真实输入 workbook 被 Excel 占用，check-only 仍可能读取失败；这种失败应暴露出来。
- 后续若增加 `--input` / `--all`，本 spec 不覆盖该扩展。

## 验收标准

- `python main.py gb --check-only` 不创建 workbook / CSV，并输出质量摘要。
- `python main.py gb --check-only --benchmark` 输出 `[benchmark]` 段。
- `python main.py gb --benchmark` 保持正常导出，并输出 `[benchmark]` 段。
- 单元测试覆盖 CLI 参数解析、runner 正常模式、runner check-only 模式和 benchmark 文本格式。


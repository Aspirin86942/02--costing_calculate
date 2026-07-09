from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path

from src.analytics.contracts import QualityMetric
from src.config.pipelines import PipelineConfig
from src.config.product_whitelist_store import (
    DEFAULT_PRODUCT_WHITELIST_PATH,
    ProductWhitelistConfigError,
    load_product_order_for_pipeline,
)
from src.etl.month_filter import MonthFilterSummary, MonthRange
from src.services.costing_service import (
    CostingRunRequest,
    CostingRunResult,
    ServiceStatus,
    precheck_costing_run,
    run_costing_request,
)

logger = logging.getLogger(__name__)


def find_input_files(config: PipelineConfig) -> list[Path]:
    """按管线配置的文件模式匹配输入文件，保留模式顺序并去重。"""
    matched: list[Path] = []
    seen: set[Path] = set()
    for pattern in config.input_patterns:
        for path in config.raw_dir.glob(pattern):
            if path not in seen:
                seen.add(path)
                matched.append(path)
    return matched


def build_quality_log_text(
    *,
    pipeline_name: str,
    input_path: Path,
    output_path: Path,
    error_log_count: int,
    quality_metrics: Iterable[QualityMetric],
    month_filter_summary: MonthFilterSummary | None = None,
) -> str:
    """将质量校验结果整理为文本日志，避免再次塞回 Excel。"""
    lines = [
        f'pipeline={pipeline_name}',
        f'input={input_path}',
        f'output={output_path}',
        f'error_log_count={error_log_count}',
    ]
    if month_filter_summary is not None:
        months_before = ','.join(month_filter_summary.input_months) or '-'
        months_after = ','.join(month_filter_summary.output_months) or '-'
        lines.extend(
            [
                f'month_range={month_filter_summary.month_range.describe()}',
                f'month_filter_rows={month_filter_summary.input_rows}->{month_filter_summary.output_rows}',
                f'months_before={months_before}',
                f'months_after={months_after}',
            ]
        )
    lines.extend(['', '[quality_metrics]'])
    lines.extend(f'{metric.metric}={metric.value} | {metric.description}' for metric in quality_metrics)
    return '\n'.join(lines)


def build_check_only_benchmark_summary_text(
    *,
    result: CostingRunResult,
    pipeline_name: str,
    input_path: Path,
    output_path: Path,
) -> str:
    """构建 check-only benchmark 的紧凑顶部摘要。"""
    status_text = 'succeeded' if result.status == ServiceStatus.SUCCEEDED else 'failed'
    lines = [
        'mode=check-only',
        f'status={status_text}',
        f'pipeline={pipeline_name}',
        f'input={input_path}',
        f'planned_output={output_path}',
        f'error_log_count={result.error_log_count}',
    ]
    if result.month_filter_summary is not None:
        lines.extend(
            [
                f'month_range={result.month_filter_summary.month_range.describe()}',
                f'month_filter_rows={result.month_filter_summary.input_rows}->{result.month_filter_summary.output_rows}',
            ]
        )
    if result.status != ServiceStatus.SUCCEEDED:
        lines.extend(
            [
                f'error_code={result.error_code or "-"}',
                f'message={result.message}',
            ]
        )
    return '\n'.join(lines)


def _file_size_or_zero(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def build_benchmark_log_text(
    *,
    input_path: Path,
    output_path: Path,
    error_log_count: int,
    stage_timings: dict[str, float],
    ingest_backend: str = 'unknown',
    output_written: bool,
) -> str:
    """构建稳定 benchmark 文本，测试只依赖字段存在，不断言秒数快慢。"""
    export_seconds = float(stage_timings.get('export', 0.0)) if output_written else 0.0
    payload_total = sum(float(seconds) for stage, seconds in stage_timings.items() if stage != 'export')
    lines = [
        '',
        '[benchmark]',
        f'output_written={str(output_written).lower()}',
        f'input_size_bytes={_file_size_or_zero(input_path)}',
        f'output_size_bytes={_file_size_or_zero(output_path) if output_written else 0}',
        f'planned_output={output_path}',
        f'error_log_count={error_log_count}',
        f'ingest_backend={ingest_backend}',
        f'payload_total_seconds={payload_total:.3f}',
        f'export_seconds={export_seconds:.3f}',
    ]
    total = 0.0
    for stage_name in sorted(stage_timings):
        seconds = float(stage_timings[stage_name])
        total += seconds
        lines.append(f'stage_{stage_name}_seconds={seconds:.3f}')
    lines.append(f'stage_total_observed_seconds={total:.3f}')
    return '\n'.join(lines)


def _build_output_workbook_path(
    processed_dir: Path,
    input_file: Path,
    month_range: MonthRange | None,
) -> Path:
    suffix = '' if month_range is None or not month_range.output_suffix() else f'_{month_range.output_suffix()}'
    return processed_dir / f'{input_file.stem}_处理后{suffix}.xlsx'


def _build_request(
    *,
    config: PipelineConfig,
    input_file: Path,
    month_range: MonthRange | None,
    benchmark: bool,
) -> CostingRunRequest:
    try:
        product_order = load_product_order_for_pipeline(config.name)
    except ProductWhitelistConfigError as exc:
        logger.warning(
            '产品白名单配置错误，已使用内置默认白名单: pipeline=%s path=%s error=%s',
            config.name,
            DEFAULT_PRODUCT_WHITELIST_PATH,
            exc,
        )
        product_order = config.product_order
    return CostingRunRequest(
        pipeline=config.name,
        input_path=input_file,
        output_dir=config.processed_dir,
        month_start=month_range.start if month_range is not None else None,
        month_end=month_range.end if month_range is not None else None,
        product_order=product_order,
        benchmark=benchmark,
        overwrite_confirmed=True,
    )


def _result_workbook_path(
    result: CostingRunResult,
    *,
    config: PipelineConfig,
    input_file: Path,
    month_range: MonthRange | None,
) -> Path:
    return result.workbook_path or _build_output_workbook_path(config.processed_dir, input_file, month_range)


def _print_quality_summary(
    result: CostingRunResult,
    *,
    config: PipelineConfig,
    input_file: Path,
    output_file: Path,
) -> None:
    print(
        build_quality_log_text(
            pipeline_name=config.name,
            input_path=input_file,
            output_path=output_file,
            error_log_count=result.error_log_count,
            quality_metrics=result.quality_metrics,
            month_filter_summary=result.month_filter_summary,
        )
    )


def _print_benchmark_summary(
    result: CostingRunResult,
    *,
    input_file: Path,
    output_file: Path,
    output_written: bool,
) -> None:
    print(
        build_benchmark_log_text(
            input_path=input_file,
            output_path=output_file,
            error_log_count=result.error_log_count,
            stage_timings=result.stage_timings or {},
            ingest_backend=result.ingest_backend,
            output_written=output_written,
        )
    )


def _exit_code_from_status(status: ServiceStatus) -> int:
    return 0 if status == ServiceStatus.SUCCEEDED else 1


def _log_failed_result(prefix: str, result: CostingRunResult) -> None:
    error_code = result.error_code or '-'
    logger.error('%s: %s | error_code=%s', prefix, result.message, error_code)
    if result.technical_detail:
        logger.error('%s technical_detail=%s', prefix, result.technical_detail)


@contextmanager
def _temporary_root_log_level(level: int) -> Iterator[None]:
    root_logger = logging.getLogger()
    previous_level = root_logger.level
    root_logger.setLevel(level)
    try:
        yield
    finally:
        root_logger.setLevel(previous_level)


def run_pipeline(
    config: PipelineConfig,
    month_range: MonthRange | None = None,
    *,
    check_only: bool = False,
    benchmark: bool = False,
) -> int:
    """执行指定管线，输出处理后的 workbook 并在控制台打印质量日志摘要。"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    input_files = find_input_files(config)
    if not input_files:
        logger.error('No %s costing file found under %s', config.name.upper(), config.raw_dir)
        return 1

    input_file = input_files[0]
    request = _build_request(config=config, input_file=input_file, month_range=month_range, benchmark=benchmark)

    if check_only:
        if benchmark:
            with _temporary_root_log_level(logging.WARNING):
                result = precheck_costing_run(request, validate_output_dir=False)
        else:
            result = precheck_costing_run(request, validate_output_dir=False)
        output_file = _result_workbook_path(result, config=config, input_file=input_file, month_range=month_range)
        if benchmark:
            print(
                build_check_only_benchmark_summary_text(
                    result=result,
                    pipeline_name=config.name,
                    input_path=input_file,
                    output_path=output_file,
                )
            )
            _print_benchmark_summary(result, input_file=input_file, output_file=output_file, output_written=False)
        else:
            print('mode=check-only')
            _print_quality_summary(result, config=config, input_file=input_file, output_file=output_file)
        if result.status == ServiceStatus.SUCCEEDED:
            if not benchmark:
                logger.info('预检成功: %s', input_file.name)
        else:
            _log_failed_result('预检失败', result)
        return _exit_code_from_status(result.status)

    result = run_costing_request(request)
    output_file = _result_workbook_path(result, config=config, input_file=input_file, month_range=month_range)
    _print_quality_summary(result, config=config, input_file=input_file, output_file=output_file)
    if benchmark:
        _print_benchmark_summary(
            result,
            input_file=input_file,
            output_file=output_file,
            output_written=result.status == ServiceStatus.SUCCEEDED,
        )
    if result.status == ServiceStatus.SUCCEEDED:
        logger.info('处理成功: %s', output_file)
    else:
        _log_failed_result('处理失败', result)
    return _exit_code_from_status(result.status)

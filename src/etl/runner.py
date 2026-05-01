from __future__ import annotations

import logging
from collections.abc import Iterable
from pathlib import Path

import pandas as pd

from src.analytics.contracts import QualityMetric
from src.analytics.summary import build_summary_payload, write_summary_json
from src.config.pipelines import PipelineConfig
from src.etl.costing_etl import CostingWorkbookETL
from src.etl.month_filter import MonthFilterSummary, MonthRange

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


def _file_size_or_zero(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def build_benchmark_log_text(
    *,
    input_path: Path,
    output_path: Path,
    error_log_path: Path,
    error_log_count: int,
    stage_timings: dict[str, float],
    output_written: bool,
) -> str:
    """构建稳定 benchmark 文本，测试只依赖字段存在，不断言秒数快慢。"""
    lines = [
        '',
        '[benchmark]',
        f'output_written={str(output_written).lower()}',
        f'input_size_bytes={_file_size_or_zero(input_path)}',
        f'output_size_bytes={_file_size_or_zero(output_path) if output_written else 0}',
        f'error_log_size_bytes={_file_size_or_zero(error_log_path) if output_written else 0}',
        f'planned_output={output_path}',
        f'planned_error_log={error_log_path}',
        f'error_log_count={error_log_count}',
    ]
    total = 0.0
    for stage_name in sorted(stage_timings):
        seconds = float(stage_timings[stage_name])
        total += seconds
        lines.append(f'stage_{stage_name}_seconds={seconds:.3f}')
    lines.append(f'stage_total_observed_seconds={total:.3f}')
    return '\n'.join(lines)


def write_error_log_csv(*, output_path: Path, error_log_frame) -> None:
    """将 error_log 明细独立导出为 CSV，避免拖慢 workbook 导出。"""
    # 这里使用 utf-8-sig，是为了让业务侧直接用 Excel 打开 CSV 时保持中文列名不乱码。
    error_log_frame.to_csv(output_path, index=False, encoding='utf-8-sig')


def _build_output_paths(
    processed_dir: Path,
    input_file: Path,
    month_range: MonthRange | None,
) -> tuple[Path, Path, Path]:
    suffix = '' if month_range is None or not month_range.output_suffix() else f'_{month_range.output_suffix()}'
    workbook_path = processed_dir / f'{input_file.stem}_处理后{suffix}.xlsx'
    error_log_path = processed_dir / f'{input_file.stem}_处理后{suffix}_error_log.csv'
    summary_path = processed_dir / f'{input_file.stem}_处理后{suffix}_summary.json'
    return workbook_path, error_log_path, summary_path


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
    if not check_only:
        config.processed_dir.mkdir(parents=True, exist_ok=True)
    output_file, error_log_csv_file, summary_file = _build_output_paths(config.processed_dir, input_file, month_range)
    etl = CostingWorkbookETL(
        skip_rows=2,
        product_order=config.product_order,
        standalone_cost_items=config.standalone_cost_items,
        product_anomaly_scope_mode=config.product_anomaly_scope_mode,
        month_range=month_range,
        ensure_output_directories=not check_only,
    )

    if check_only:
        if not etl.prepare_payload(input_file):
            logger.error('预检失败: %s', input_file.name)
            return 1
        quality_log = build_quality_log_text(
            pipeline_name=config.name,
            input_path=input_file,
            output_path=output_file,
            error_log_count=etl.last_error_log_count,
            quality_metrics=etl.last_quality_metrics,
            month_filter_summary=getattr(etl, 'last_month_filter_summary', None),
        )
        print('mode=check-only')
        print(quality_log)
        if benchmark:
            print(
                build_benchmark_log_text(
                    input_path=input_file,
                    output_path=output_file,
                    error_log_path=error_log_csv_file,
                    error_log_count=etl.last_error_log_count,
                    stage_timings=getattr(etl, 'last_stage_timings', {}),
                    output_written=False,
                )
            )
        logger.info('预检成功: %s', input_file.name)
        return 0

    if not etl.process_file(input_file, output_file):
        logger.error('处理失败: %s', input_file.name)
        return 1

    write_error_log_csv(output_path=error_log_csv_file, error_log_frame=etl.last_error_log_frame)
    summary_payload = build_summary_payload(
        pipeline_name=config.name,
        input_path=input_file,
        output_path=output_file,
        error_log_path=error_log_csv_file,
        error_log_count=etl.last_error_log_count,
        quality_metrics=etl.last_quality_metrics,
        error_log_frame=etl.last_error_log_frame,
        work_order_sheet_frame=getattr(etl, 'last_work_order_sheet_frame', pd.DataFrame()),
        month_filter_summary=getattr(etl, 'last_month_filter_summary', None),
    )
    write_summary_json(summary_file, summary_payload)
    quality_log = build_quality_log_text(
        pipeline_name=config.name,
        input_path=input_file,
        output_path=output_file,
        error_log_count=etl.last_error_log_count,
        quality_metrics=etl.last_quality_metrics,
        month_filter_summary=getattr(etl, 'last_month_filter_summary', None),
    )
    print(quality_log)
    if benchmark:
        print(
            build_benchmark_log_text(
                input_path=input_file,
                output_path=output_file,
                error_log_path=error_log_csv_file,
                error_log_count=etl.last_error_log_count,
                stage_timings=getattr(etl, 'last_stage_timings', {}),
                output_written=True,
            )
        )
    logger.info('处理成功: %s', output_file)
    return 0

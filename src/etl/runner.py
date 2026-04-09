from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

from src.analytics.contracts import QualityMetric
from src.config.pipelines import PipelineConfig
from src.etl.costing_etl import CostingWorkbookETL

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
) -> str:
    """将质量校验结果整理为文本日志，避免再次塞回 Excel。"""
    lines = [
        f'pipeline={pipeline_name}',
        f'input={input_path}',
        f'output={output_path}',
        f'error_log_count={error_log_count}',
        '',
        '[quality_metrics]',
    ]
    lines.extend(f'{metric.metric}={metric.value} | {metric.description}' for metric in quality_metrics)
    return '\n'.join(lines)


def run_pipeline(config: PipelineConfig) -> int:
    """执行指定管线，输出处理后的 workbook 和同名质量日志。"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    input_files = find_input_files(config)
    if not input_files:
        logger.error('No %s costing file found under %s', config.name.upper(), config.raw_dir)
        return 1

    input_file = input_files[0]
    config.processed_dir.mkdir(parents=True, exist_ok=True)
    output_file = config.processed_dir / f'{input_file.stem}_处理后.xlsx'
    log_file = config.processed_dir / f'{input_file.stem}_处理后.log'
    etl = CostingWorkbookETL(skip_rows=2, product_order=config.product_order)

    if not etl.process_file(input_file, output_file):
        logger.error('处理失败: %s', input_file.name)
        return 1

    quality_log = build_quality_log_text(
        pipeline_name=config.name,
        input_path=input_file,
        output_path=output_file,
        error_log_count=etl.last_error_log_count,
        quality_metrics=etl.last_quality_metrics,
    )
    log_file.write_text(quality_log, encoding='utf-8')
    print(quality_log)
    logger.info('处理成功: %s', output_file)
    return 0

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

import pandas as pd

from src.analytics.contracts import QualityMetric
from src.config.pipelines import PIPELINES, ProductOrder
from src.etl.costing_etl import CostingWorkbookETL
from src.etl.month_filter import MonthFilterSummary, MonthRange, build_month_range
from src.services.progress import ProgressCallback, report_progress
from src.services.progress import ProgressEvent as ProgressEvent


class ServiceStatus(StrEnum):
    WAITING = 'waiting'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'


@dataclass(frozen=True)
class CostingRunRequest:
    pipeline: str
    input_path: Path
    output_dir: Path
    month_start: str | None = None
    month_end: str | None = None
    product_order: ProductOrder = ()
    benchmark: bool = False
    overwrite_confirmed: bool = False


@dataclass(frozen=True)
class CostingRunResult:
    status: ServiceStatus
    message: str
    workbook_path: Path | None = None
    candidate_products: ProductOrder = ()
    quality_metrics: tuple[QualityMetric, ...] = ()
    error_log_count: int = 0
    month_filter_summary: MonthFilterSummary | None = None
    anomaly_summary: dict[str, dict[str, int]] | None = None
    stage_timings: dict[str, float] | None = None
    input_size_bytes: int = 0
    output_size_bytes: int = 0
    ingest_backend: str = 'unknown'
    error_code: str | None = None
    retryable: bool = False
    technical_detail: str | None = None


@dataclass(frozen=True)
class _PreparedRequest:
    workbook_path: Path
    month_range: MonthRange | None


def build_output_workbook_path(
    output_dir: Path,
    input_path: Path,
    month_start: str | None = None,
    month_end: str | None = None,
) -> Path:
    month_range = build_month_range(month_start, month_end)
    suffix = '' if month_range is None or not month_range.output_suffix() else f'_{month_range.output_suffix()}'
    return output_dir / f'{input_path.stem}_处理后{suffix}.xlsx'


def precheck_costing_run(
    request: CostingRunRequest,
    *,
    validate_output_dir: bool = True,
    progress_callback: ProgressCallback | None = None,
) -> CostingRunResult:
    report_progress(progress_callback, 0, 'prepare', '正在校验输入配置')
    prepared, validation_error = _prepare_request(request, validate_output_dir=validate_output_dir)
    if validation_error is not None:
        report_progress(progress_callback, 0, 'failed', validation_error.message)
        return validation_error
    assert prepared is not None

    report_progress(progress_callback, 5, 'prepare', '已完成路径与参数校验')
    if prepared.workbook_path.exists() and not request.overwrite_confirmed:
        result = _failed(
            message=f'输出 workbook 已存在: {prepared.workbook_path}',
            error_code='OUTPUT_EXISTS',
            workbook_path=prepared.workbook_path,
        )
        report_progress(progress_callback, 0, 'failed', result.message)
        return result

    try:
        etl = _build_etl(request, prepared.month_range)
        if not etl.prepare_payload(request.input_path, progress_callback=progress_callback):
            result = _failed(
                message='预检失败，请查看日志详情',
                error_code='ETL_FAILED',
                workbook_path=prepared.workbook_path,
            )
            report_progress(progress_callback, 0, 'failed', result.message)
            return result
        report_progress(progress_callback, 100, 'done', '预检完成')
        return _result_from_etl(
            etl,
            status=ServiceStatus.SUCCEEDED,
            message='预检通过',
            input_path=request.input_path,
            workbook_path=prepared.workbook_path,
            output_written=False,
        )
    except Exception as exc:  # noqa: BLE001
        result = _failed(
            message='预检失败，请查看日志详情',
            error_code='ETL_FAILED',
            workbook_path=prepared.workbook_path,
            technical_detail=str(exc),
        )
        report_progress(progress_callback, 0, 'failed', result.message)
        return result


def run_costing_request(
    request: CostingRunRequest,
    *,
    progress_callback: ProgressCallback | None = None,
) -> CostingRunResult:
    report_progress(progress_callback, 0, 'prepare', '正在校验输入配置')
    prepared, validation_error = _prepare_request(request)
    if validation_error is not None:
        report_progress(progress_callback, 0, 'failed', validation_error.message)
        return validation_error
    assert prepared is not None

    report_progress(progress_callback, 5, 'prepare', '已完成路径与参数校验')
    if prepared.workbook_path.exists() and not request.overwrite_confirmed:
        result = _failed(
            message=f'输出 workbook 已存在: {prepared.workbook_path}',
            error_code='OUTPUT_EXISTS',
            workbook_path=prepared.workbook_path,
        )
        report_progress(progress_callback, 0, 'failed', result.message)
        return result

    try:
        request.output_dir.mkdir(parents=True, exist_ok=True)
        etl = _build_etl(request, prepared.month_range)
        if not etl.process_file(request.input_path, prepared.workbook_path, progress_callback=progress_callback):
            result = _failed(
                message='处理失败，请查看日志详情',
                error_code='ETL_FAILED',
                workbook_path=prepared.workbook_path,
            )
            report_progress(progress_callback, 0, 'failed', result.message)
            return result
        report_progress(progress_callback, 100, 'done', '处理完成')
        return _result_from_etl(
            etl,
            status=ServiceStatus.SUCCEEDED,
            message='处理成功',
            input_path=request.input_path,
            workbook_path=prepared.workbook_path,
            output_written=True,
        )
    except Exception as exc:  # noqa: BLE001
        result = _failed(
            message='处理失败，请查看日志详情',
            error_code='ETL_FAILED',
            workbook_path=prepared.workbook_path,
            technical_detail=str(exc),
        )
        report_progress(progress_callback, 0, 'failed', result.message)
        return result


def _prepare_request(
    request: CostingRunRequest,
    *,
    validate_output_dir: bool = True,
) -> tuple[_PreparedRequest | None, CostingRunResult | None]:
    if request.pipeline not in PIPELINES:
        return None, _failed(message=f'未知管线: {request.pipeline}', error_code='INVALID_INPUT')
    if not request.input_path.exists():
        return None, _failed(message=f'输入文件不存在: {request.input_path}', error_code='FILE_NOT_FOUND')
    if not request.input_path.is_file():
        return None, _failed(message=f'输入路径不是文件: {request.input_path}', error_code='INVALID_INPUT')
    if request.input_path.suffix.lower() != '.xlsx':
        return None, _failed(message='输入文件必须是 .xlsx 格式', error_code='UNSUPPORTED_FILE_TYPE')
    try:
        with request.input_path.open('rb'):
            pass
    except OSError as exc:
        return None, _failed(
            message=f'输入文件不可读: {request.input_path}',
            error_code='FILE_NOT_READABLE',
            technical_detail=str(exc),
        )

    whitelist_error = _validate_product_order(request.product_order)
    if whitelist_error is not None:
        return None, whitelist_error

    if validate_output_dir:
        output_dir_error = _validate_output_dir(request.output_dir)
        if output_dir_error is not None:
            return None, output_dir_error

    try:
        month_range = build_month_range(request.month_start, request.month_end)
    except ValueError as exc:
        return None, _failed(message=str(exc), error_code='MONTH_RANGE_INVALID', technical_detail=str(exc))

    workbook_path = build_output_workbook_path(
        request.output_dir,
        request.input_path,
        request.month_start,
        request.month_end,
    )
    return _PreparedRequest(workbook_path=workbook_path, month_range=month_range), None


def _validate_product_order(product_order: ProductOrder) -> CostingRunResult | None:
    seen: set[tuple[str, str]] = set()
    for pair in product_order:
        if not isinstance(pair, (tuple, list)) or len(pair) != 2:
            return _failed(message='产品白名单必须由产品编码和产品名称组成', error_code='WHITELIST_INVALID')
        code, name = pair
        normalized_pair = (str(code).strip(), str(name).strip())
        if not normalized_pair[0] or not normalized_pair[1]:
            return _failed(message='产品白名单编码和名称不能为空', error_code='WHITELIST_INVALID')
        if normalized_pair in seen:
            return _failed(message='产品白名单存在重复项', error_code='WHITELIST_INVALID')
        seen.add(normalized_pair)
    return None


def _validate_output_dir(output_dir: Path) -> CostingRunResult | None:
    if output_dir.exists():
        if not output_dir.is_dir():
            return _failed(message=f'输出路径不是目录: {output_dir}', error_code='OUTPUT_DIR_INVALID')
        if not _can_write_directory(output_dir):
            return _failed(message=f'输出目录不可写: {output_dir}', error_code='OUTPUT_DIR_INVALID')
        return None

    existing_parent = _nearest_existing_parent(output_dir)
    if existing_parent is None or not existing_parent.is_dir():
        return _failed(message=f'输出目录父路径不可用: {output_dir.parent}', error_code='OUTPUT_DIR_INVALID')
    if not _can_write_directory(existing_parent):
        return _failed(message=f'输出目录父路径不可写: {existing_parent}', error_code='OUTPUT_DIR_INVALID')
    return None


def _nearest_existing_parent(path: Path) -> Path | None:
    current = path.parent
    while not current.exists():
        if current == current.parent:
            return None
        current = current.parent
    return current


def _can_write_directory(path: Path) -> bool:
    return os.access(path, os.W_OK | os.X_OK)


def _build_etl(request: CostingRunRequest, month_range: MonthRange | None) -> CostingWorkbookETL:
    config = PIPELINES[request.pipeline]
    return CostingWorkbookETL(
        skip_rows=2,
        product_order=request.product_order,
        standalone_cost_items=config.standalone_cost_items,
        product_anomaly_scope_mode=config.product_anomaly_scope_mode,
        month_range=month_range,
        ensure_output_directories=False,
    )


def _failed(
    *,
    message: str,
    error_code: str,
    workbook_path: Path | None = None,
    technical_detail: str | None = None,
) -> CostingRunResult:
    return CostingRunResult(
        status=ServiceStatus.FAILED,
        message=message,
        workbook_path=workbook_path,
        error_code=error_code,
        retryable=False,
        technical_detail=technical_detail,
    )


def _result_from_etl(
    etl: CostingWorkbookETL,
    *,
    status: ServiceStatus,
    message: str,
    input_path: Path,
    workbook_path: Path,
    output_written: bool,
) -> CostingRunResult:
    return CostingRunResult(
        status=status,
        message=message,
        workbook_path=workbook_path,
        candidate_products=tuple(getattr(etl, 'last_candidate_products', ())),
        quality_metrics=tuple(getattr(etl, 'last_quality_metrics', ())),
        error_log_count=int(getattr(etl, 'last_error_log_count', 0) or 0),
        month_filter_summary=getattr(etl, 'last_month_filter_summary', None),
        anomaly_summary=_build_anomaly_summary(getattr(etl, 'last_work_order_sheet_frame', pd.DataFrame())),
        stage_timings=dict(getattr(etl, 'last_stage_timings', {})),
        input_size_bytes=_file_size_or_zero(input_path),
        output_size_bytes=_file_size_or_zero(workbook_path) if output_written else 0,
        ingest_backend=str(getattr(etl, 'last_ingest_backend', 'unknown')),
    )


def _build_anomaly_summary(work_order_sheet_frame: pd.DataFrame) -> dict[str, dict[str, int]]:
    return {
        'anomaly_level_counts': _value_counts(work_order_sheet_frame, '异常等级'),
        'anomaly_source_counts': _value_counts(work_order_sheet_frame, '异常主要来源'),
    }


def _value_counts(frame: pd.DataFrame, column_name: str) -> dict[str, int]:
    if frame.empty or column_name not in frame.columns:
        return {}
    series = frame[column_name].fillna('').astype(str).str.strip()
    series = series[series.ne('')]
    return {str(index): int(value) for index, value in series.value_counts(sort=False).items()}


def _file_size_or_zero(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0

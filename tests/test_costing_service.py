from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pandas as pd
import polars as pl

from src.analytics.contracts import QualityMetric, WorkbookPayload
from src.config.pipelines import GB_PIPELINE, SK_PIPELINE
from src.etl import pipeline as pipeline_module
from src.etl.costing_etl import CostingWorkbookETL
from src.etl.month_filter import MonthFilterSummary, MonthRange
from src.services.costing_service import (
    CostingRunRequest,
    ServiceStatus,
    build_output_workbook_path,
    precheck_costing_run,
    run_costing_request,
)


def _request(
    tmp_path: Path,
    *,
    pipeline: str = 'gb',
    input_name: str = 'GB-成本计算单.xlsx',
    product_order: tuple[tuple[str, str], ...] = (('GB_C.D.B0040AA', 'BMS-750W驱动器'),),
    overwrite_confirmed: bool = True,
) -> CostingRunRequest:
    input_path = tmp_path / input_name
    input_path.write_bytes(b'raw')
    return CostingRunRequest(
        pipeline=pipeline,
        input_path=input_path,
        output_dir=tmp_path / 'processed',
        month_start=None,
        month_end=None,
        product_order=product_order,
        benchmark=True,
        overwrite_confirmed=overwrite_confirmed,
    )


def test_build_output_workbook_path_uses_month_suffix(tmp_path: Path) -> None:
    path = build_output_workbook_path(
        tmp_path,
        tmp_path / 'GB-成本计算单.xlsx',
        month_start='2025-01',
        month_end='2025-03',
    )

    assert path == tmp_path / 'GB-成本计算单_处理后_2025-01_2025-03.xlsx'


def test_build_output_workbook_path_without_month_suffix(tmp_path: Path) -> None:
    path = build_output_workbook_path(tmp_path, tmp_path / 'GB-成本计算单.xlsx')

    assert path == tmp_path / 'GB-成本计算单_处理后.xlsx'


def test_precheck_rejects_non_xlsx(tmp_path: Path) -> None:
    request = _request(tmp_path, input_name='input.xls')

    result = precheck_costing_run(request)

    assert result.status == ServiceStatus.FAILED
    assert result.error_code == 'UNSUPPORTED_FILE_TYPE'
    assert 'xlsx' in result.message


def test_invalid_pipeline_is_rejected(tmp_path: Path) -> None:
    request = _request(tmp_path, pipeline='unknown')

    result = precheck_costing_run(request)

    assert result.status == ServiceStatus.FAILED
    assert result.error_code == 'INVALID_INPUT'


def test_missing_input_is_rejected(tmp_path: Path) -> None:
    request = CostingRunRequest(
        pipeline='gb',
        input_path=tmp_path / 'missing.xlsx',
        output_dir=tmp_path / 'processed',
        product_order=(('GB_C.D.B0040AA', 'BMS-750W驱动器'),),
        overwrite_confirmed=True,
    )

    result = precheck_costing_run(request)

    assert result.status == ServiceStatus.FAILED
    assert result.error_code == 'FILE_NOT_FOUND'


def test_existing_input_path_that_is_not_file_is_invalid(tmp_path: Path) -> None:
    input_dir = tmp_path / 'input-folder'
    input_dir.mkdir()
    request = CostingRunRequest(
        pipeline='gb',
        input_path=input_dir,
        output_dir=tmp_path / 'processed',
        product_order=(('GB_C.D.B0040AA', 'BMS-750W驱动器'),),
        overwrite_confirmed=True,
    )

    result = precheck_costing_run(request)

    assert result.status == ServiceStatus.FAILED
    assert result.error_code == 'INVALID_INPUT'


def test_unreadable_xlsx_is_rejected(monkeypatch, tmp_path: Path) -> None:
    request = _request(tmp_path)
    original_open = Path.open

    def _raise_for_input(path: Path, *args, **kwargs):
        if path == request.input_path:
            raise OSError('permission denied')
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, 'open', _raise_for_input)

    result = precheck_costing_run(request)

    assert result.status == ServiceStatus.FAILED
    assert result.error_code == 'FILE_NOT_READABLE'
    assert result.technical_detail == 'permission denied'


def test_precheck_reports_existing_output_when_not_confirmed(tmp_path: Path) -> None:
    request = _request(tmp_path, overwrite_confirmed=False)
    request.output_dir.mkdir()
    planned = build_output_workbook_path(request.output_dir, request.input_path)
    planned.write_text('old', encoding='utf-8')

    result = precheck_costing_run(request)

    assert result.status == ServiceStatus.FAILED
    assert result.error_code == 'OUTPUT_EXISTS'
    assert result.workbook_path == planned


def test_precheck_rejects_output_dir_that_is_existing_file(tmp_path: Path) -> None:
    request = _request(tmp_path)
    output_file = tmp_path / 'processed'
    output_file.write_text('not a directory', encoding='utf-8')

    result = precheck_costing_run(replace(request, output_dir=output_file))

    assert result.status == ServiceStatus.FAILED
    assert result.error_code == 'OUTPUT_DIR_INVALID'


def test_precheck_validates_output_dir_by_default_before_prepare_payload(monkeypatch, tmp_path: Path) -> None:
    request = _request(tmp_path)
    output_file = tmp_path / 'processed'
    output_file.write_text('not a directory', encoding='utf-8')

    class _UnexpectedETL:
        def __init__(self, *args, **kwargs) -> None:
            raise AssertionError('default precheck must reject invalid output_dir before ETL preparation')

    monkeypatch.setattr('src.services.costing_service.CostingWorkbookETL', _UnexpectedETL)

    result = precheck_costing_run(replace(request, output_dir=output_file))

    assert result.status == ServiceStatus.FAILED
    assert result.error_code == 'OUTPUT_DIR_INVALID'


def test_precheck_can_skip_output_dir_validation_for_cli_check_only(monkeypatch, tmp_path: Path) -> None:
    request = _request(tmp_path)
    output_file = tmp_path / 'processed'
    output_file.write_text('not a directory', encoding='utf-8')
    captured: dict[str, Path | bool] = {}

    class _DummyETL:
        def __init__(self, *args, **kwargs) -> None:
            self.last_quality_metrics = ()
            self.last_error_log_count = 0
            self.last_work_order_sheet_frame = pd.DataFrame()
            self.last_stage_timings = {'ingest': 0.1}
            self.last_ingest_backend = 'calamine'

        def prepare_payload(self, input_path: Path, *, progress_callback: object | None = None) -> bool:
            captured['prepared'] = True
            captured['input_path'] = input_path
            return True

        def process_file(
            self,
            input_path: Path,
            output_path: Path,
            *,
            progress_callback: object | None = None,
        ) -> bool:
            raise AssertionError('precheck must not write workbook')

    monkeypatch.setattr('src.services.costing_service.CostingWorkbookETL', _DummyETL)

    result = precheck_costing_run(replace(request, output_dir=output_file), validate_output_dir=False)

    assert result.status == ServiceStatus.SUCCEEDED
    assert result.workbook_path == output_file / 'GB-成本计算单_处理后.xlsx'
    assert captured == {'prepared': True, 'input_path': request.input_path}
    assert output_file.is_file()


def test_precheck_rejects_output_dir_when_existing_parent_is_file(tmp_path: Path) -> None:
    request = _request(tmp_path)
    parent_file = tmp_path / 'blocked-parent'
    parent_file.write_text('not a directory', encoding='utf-8')

    result = precheck_costing_run(replace(request, output_dir=parent_file / 'processed'))

    assert result.status == ServiceStatus.FAILED
    assert result.error_code == 'OUTPUT_DIR_INVALID'


def test_duplicate_product_order_is_rejected(tmp_path: Path) -> None:
    request = _request(
        tmp_path,
        product_order=(
            ('GB_C.D.B0040AA', 'BMS-750W驱动器'),
            ('GB_C.D.B0040AA', 'BMS-750W驱动器'),
        ),
    )

    result = precheck_costing_run(request)

    assert result.status == ServiceStatus.FAILED
    assert result.error_code == 'WHITELIST_INVALID'


def test_string_product_order_item_is_rejected(tmp_path: Path) -> None:
    request = _request(tmp_path, product_order=('AB',))

    result = precheck_costing_run(request)

    assert result.status == ServiceStatus.FAILED
    assert result.error_code == 'WHITELIST_INVALID'


def test_single_field_product_order_item_is_rejected(tmp_path: Path) -> None:
    request = _request(tmp_path, product_order=(('ONLY_CODE',),))

    result = precheck_costing_run(request)

    assert result.status == ServiceStatus.FAILED
    assert result.error_code == 'WHITELIST_INVALID'


def test_blank_product_order_is_rejected(tmp_path: Path) -> None:
    request = _request(tmp_path, product_order=(('', 'BMS-750W驱动器'),))

    result = precheck_costing_run(request)

    assert result.status == ServiceStatus.FAILED
    assert result.error_code == 'WHITELIST_INVALID'


def test_invalid_month_range_is_rejected(tmp_path: Path) -> None:
    base = _request(tmp_path)
    request = CostingRunRequest(
        pipeline=base.pipeline,
        input_path=base.input_path,
        output_dir=base.output_dir,
        month_start='2025-04',
        month_end='2025-03',
        product_order=base.product_order,
        overwrite_confirmed=True,
    )

    result = precheck_costing_run(request)

    assert result.status == ServiceStatus.FAILED
    assert result.error_code == 'MONTH_RANGE_INVALID'
    assert 'month_start' in result.message


def test_precheck_calls_prepare_payload_not_process_file(monkeypatch, tmp_path: Path) -> None:
    request = _request(tmp_path)
    captured: dict[str, Path | bool] = {}

    class _DummyETL:
        def __init__(self, *args, **kwargs) -> None:
            self.last_quality_metrics = ()
            self.last_error_log_count = 0
            self.last_work_order_sheet_frame = pd.DataFrame()
            self.last_stage_timings = {'ingest': 0.1}
            self.last_ingest_backend = 'calamine'

        def prepare_payload(self, input_path: Path, *, progress_callback: object | None = None) -> bool:
            captured['prepared'] = True
            captured['input_path'] = input_path
            return True

        def process_file(
            self,
            input_path: Path,
            output_path: Path,
            *,
            progress_callback: object | None = None,
        ) -> bool:
            raise AssertionError('precheck must not write workbook')

    monkeypatch.setattr('src.services.costing_service.CostingWorkbookETL', _DummyETL)

    result = precheck_costing_run(request)

    assert result.status == ServiceStatus.SUCCEEDED
    assert result.message == '预检通过'
    assert result.workbook_path == tmp_path / 'processed' / 'GB-成本计算单_处理后.xlsx'
    assert result.input_size_bytes == 3
    assert result.output_size_bytes == 0
    assert result.stage_timings == {'ingest': 0.1}
    assert captured == {'prepared': True, 'input_path': request.input_path}
    assert not request.output_dir.exists()


def test_precheck_returns_candidate_products_from_normalized_payload(monkeypatch, tmp_path: Path) -> None:
    request = _request(tmp_path)

    class _DummyPipeline:
        last_month_filter_summary = None
        last_ingest_backend = 'calamine'
        last_candidate_products = ()

        def build_workbook_payload(self, *args, **kwargs):
            self.last_candidate_products = (
                ('GB_C.D.B0040AA', 'BMS-750W驱动器'),
                ('GB_C.D.B9999AA', '新产品'),
            )
            return WorkbookPayload(
                sheet_models=(),
                quality_metrics=(),
                error_log_count=0,
                stage_timings={},
                error_log_export=pd.DataFrame(),
                work_order_sheet_export=pd.DataFrame(),
            )

    class _DummyETL:
        def __init__(self, *args, **kwargs) -> None:
            self.pipeline = _DummyPipeline()
            self.last_quality_metrics = ()
            self.last_error_log_count = 0
            self.last_error_log_frame = pd.DataFrame()
            self.last_work_order_sheet_frame = pd.DataFrame()
            self.last_month_filter_summary = None
            self.last_stage_timings = {}
            self.last_ingest_backend = 'calamine'
            self.last_candidate_products = ()

        def _apply_payload_state(self, payload) -> None:
            self.last_candidate_products = self.pipeline.last_candidate_products

        def prepare_payload(self, input_path: Path, *, progress_callback: object | None = None) -> bool:
            payload = self.pipeline.build_workbook_payload(input_path)
            self._apply_payload_state(payload)
            return True

    monkeypatch.setattr('src.services.costing_service.CostingWorkbookETL', _DummyETL)

    result = precheck_costing_run(request)

    assert result.status == ServiceStatus.SUCCEEDED
    assert result.candidate_products == (
        ('GB_C.D.B0040AA', 'BMS-750W驱动器'),
        ('GB_C.D.B9999AA', '新产品'),
    )


def test_extract_candidate_products_from_normalized_keeps_order_and_skips_invalid_values() -> None:
    extractor = getattr(pipeline_module, '_extract_candidate_products_from_normalized', None)
    frame = pl.DataFrame(
        {
            '产品编码': ['P001', 'P001', 'P002', ' ', 'P003', None, 'P004'],
            '产品名称': ['产品A', '产品A', '产品B', '空编码', '', '空编码', None],
            '月份': ['2025-01'] * 7,
        }
    )

    assert extractor is not None
    assert extractor(frame) == (
        ('P001', '产品A'),
        ('P002', '产品B'),
    )


def test_extract_candidate_products_from_normalized_returns_empty_for_empty_or_missing_columns() -> None:
    extractor = getattr(pipeline_module, '_extract_candidate_products_from_normalized', None)

    assert extractor is not None
    assert extractor(pl.DataFrame({'产品编码': [], '产品名称': []})) == ()
    assert extractor(pl.DataFrame({'产品编码': ['P001']})) == ()
    assert extractor(pl.DataFrame({'产品名称': ['产品A']})) == ()


def test_costing_workbook_etl_copies_and_resets_candidate_products() -> None:
    etl = CostingWorkbookETL(ensure_output_directories=False)
    etl.pipeline.last_candidate_products = (
        ('GB_C.D.B0040AA', 'BMS-750W驱动器'),
        ('GB_C.D.B9999AA', '新产品'),
    )
    payload = WorkbookPayload(
        sheet_models=(),
        quality_metrics=(),
        error_log_count=0,
        stage_timings={},
        error_log_export=pd.DataFrame(),
        work_order_sheet_export=pd.DataFrame(),
    )

    etl._apply_payload_state(payload)

    assert etl.last_candidate_products == (
        ('GB_C.D.B0040AA', 'BMS-750W驱动器'),
        ('GB_C.D.B9999AA', '新产品'),
    )

    etl._reset_last_run_state()

    assert etl.last_candidate_products == ()


def test_run_costing_request_writes_only_workbook_and_returns_runtime_summary(monkeypatch, tmp_path: Path) -> None:
    request = _request(tmp_path)
    captured: dict[str, object] = {}
    month_filter_summary = MonthFilterSummary(
        month_range=MonthRange(start='2025-01', end='2025-03'),
        input_rows=3,
        output_rows=2,
        input_months=('2025-01', '2025-02', '2025-03'),
        output_months=('2025-02', '2025-03'),
    )

    class _DummyETL:
        def __init__(
            self,
            skip_rows: int,
            *,
            product_order,
            standalone_cost_items,
            product_anomaly_scope_mode,
            month_range=None,
            ensure_output_directories=True,
        ) -> None:
            captured['skip_rows'] = skip_rows
            captured['product_order'] = product_order
            captured['standalone_cost_items'] = standalone_cost_items
            captured['product_anomaly_scope_mode'] = product_anomaly_scope_mode
            captured['month_range'] = month_range
            captured['ensure_output_directories'] = ensure_output_directories
            self.last_quality_metrics = (QualityMetric('行数勾稽', '产品数量统计输出行数', '1', '仅保留有效工单'),)
            self.last_error_log_count = 2
            self.last_error_log_frame = pd.DataFrame([{'issue_type': 'MISSING_AMOUNT'}])
            self.last_work_order_sheet_frame = pd.DataFrame(
                [
                    {'异常等级': '关注', '异常主要来源': '材料异常'},
                    {'异常等级': '高度可疑', '异常主要来源': '人工异常'},
                    {'异常等级': '关注', '异常主要来源': '材料异常'},
                ]
            )
            self.last_month_filter_summary = month_filter_summary
            self.last_stage_timings = {'ingest': 0.1, 'export': 0.2}
            self.last_ingest_backend = 'calamine'

        def prepare_payload(self, input_path: Path, *, progress_callback: object | None = None) -> bool:
            raise AssertionError('run must process workbook')

        def process_file(
            self,
            input_path: Path,
            output_path: Path,
            *,
            progress_callback: object | None = None,
        ) -> bool:
            captured['input_path'] = input_path
            captured['output_path'] = output_path
            output_path.write_text('xlsx', encoding='utf-8')
            return True

    monkeypatch.setattr('src.services.costing_service.CostingWorkbookETL', _DummyETL)

    result = run_costing_request(request)

    expected_workbook = tmp_path / 'processed' / 'GB-成本计算单_处理后.xlsx'
    assert result.status == ServiceStatus.SUCCEEDED
    assert result.message == '处理成功'
    assert result.workbook_path == expected_workbook
    assert result.workbook_path.exists()
    assert not (tmp_path / 'processed' / 'GB-成本计算单_处理后_error_log.csv').exists()
    assert not (tmp_path / 'processed' / 'GB-成本计算单_处理后_summary.json').exists()
    assert result.error_log_count == 2
    assert result.quality_metrics[0].metric == '产品数量统计输出行数'
    assert result.anomaly_summary == {
        'anomaly_level_counts': {'关注': 2, '高度可疑': 1},
        'anomaly_source_counts': {'材料异常': 2, '人工异常': 1},
    }
    assert result.stage_timings == {'ingest': 0.1, 'export': 0.2}
    assert result.month_filter_summary == month_filter_summary
    assert result.ingest_backend == 'calamine'
    assert result.input_size_bytes == 3
    assert result.output_size_bytes == 4
    assert captured['standalone_cost_items'] == GB_PIPELINE.standalone_cost_items
    assert captured['product_anomaly_scope_mode'] == GB_PIPELINE.product_anomaly_scope_mode
    assert captured['product_order'] == request.product_order
    assert captured['ensure_output_directories'] is False
    assert captured['input_path'] == request.input_path
    assert captured['output_path'] == expected_workbook


def test_etl_constructor_receives_pipeline_standalone_items_and_scope_mode(monkeypatch, tmp_path: Path) -> None:
    request = _request(
        tmp_path,
        pipeline='sk',
        input_name='SK-成本计算单.xlsx',
        product_order=(('DP.C.P0197AA', '动力线'),),
    )
    captured: dict[str, object] = {}

    class _DummyETL:
        def __init__(
            self,
            skip_rows: int,
            *,
            product_order,
            standalone_cost_items,
            product_anomaly_scope_mode,
            month_range=None,
            ensure_output_directories=True,
        ) -> None:
            captured['product_order'] = product_order
            captured['standalone_cost_items'] = standalone_cost_items
            captured['product_anomaly_scope_mode'] = product_anomaly_scope_mode
            self.last_quality_metrics = ()
            self.last_error_log_count = 0
            self.last_work_order_sheet_frame = pd.DataFrame()
            self.last_stage_timings = {}
            self.last_ingest_backend = 'openpyxl'

        def prepare_payload(self, input_path: Path, *, progress_callback: object | None = None) -> bool:
            return True

    monkeypatch.setattr('src.services.costing_service.CostingWorkbookETL', _DummyETL)

    result = precheck_costing_run(request)

    assert result.status == ServiceStatus.SUCCEEDED
    assert captured['standalone_cost_items'] == SK_PIPELINE.standalone_cost_items
    assert captured['product_anomaly_scope_mode'] == SK_PIPELINE.product_anomaly_scope_mode
    assert captured['product_order'] == request.product_order


def test_progress_event_is_constructible() -> None:
    from src.services.costing_service import ProgressEvent

    event = ProgressEvent(percent=45, stage='fact', message='已拆分事实表')

    assert event.percent == 45
    assert event.stage == 'fact'
    assert event.message == '已拆分事实表'


def test_run_costing_request_reports_prepare_export_and_done_progress(monkeypatch, tmp_path: Path) -> None:
    from src.services.costing_service import ProgressEvent

    request = _request(tmp_path)
    events: list[ProgressEvent] = []

    class _ProgressETL:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.last_quality_metrics = ()
            self.last_error_log_count = 0
            self.last_month_filter_summary = None
            self.last_stage_timings = {'export': 0.1}
            self.last_ingest_backend = 'dummy'
            self.last_work_order_sheet_frame = pd.DataFrame()
            self.last_candidate_products = ()

        def process_file(
            self,
            input_path: Path,
            output_path: Path,
            *,
            progress_callback: object | None = None,
        ) -> bool:
            assert input_path == request.input_path
            assert output_path.name.endswith('_处理后.xlsx')
            if progress_callback is not None:
                progress_callback(ProgressEvent(percent=95, stage='export', message='正在写出 workbook'))
            return True

    monkeypatch.setattr('src.services.costing_service.CostingWorkbookETL', _ProgressETL)

    result = run_costing_request(request, progress_callback=events.append)

    assert result.status == ServiceStatus.SUCCEEDED
    assert [(event.percent, event.stage) for event in events] == [
        (0, 'prepare'),
        (5, 'prepare'),
        (95, 'export'),
        (100, 'done'),
    ]


def test_precheck_progress_does_not_report_export(monkeypatch, tmp_path: Path) -> None:
    from src.services.costing_service import ProgressEvent

    request = _request(tmp_path)
    events: list[ProgressEvent] = []

    class _ProgressETL:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.last_quality_metrics = ()
            self.last_error_log_count = 0
            self.last_month_filter_summary = None
            self.last_stage_timings = {'presentation': 0.1}
            self.last_ingest_backend = 'dummy'
            self.last_work_order_sheet_frame = pd.DataFrame()
            self.last_candidate_products = ()

        def prepare_payload(self, input_path: Path, *, progress_callback: object | None = None) -> bool:
            assert input_path == request.input_path
            if progress_callback is not None:
                progress_callback(ProgressEvent(percent=85, stage='presentation', message='已构建输出 Sheet'))
            return True

    monkeypatch.setattr('src.services.costing_service.CostingWorkbookETL', _ProgressETL)

    result = precheck_costing_run(request, progress_callback=events.append)

    assert result.status == ServiceStatus.SUCCEEDED
    assert 'export' not in [event.stage for event in events]
    assert events[-1].stage == 'done'


def test_progress_callback_failure_does_not_fail_service(monkeypatch, caplog, tmp_path: Path) -> None:
    request = _request(tmp_path)

    class _ProgressETL:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.last_quality_metrics = ()
            self.last_error_log_count = 0
            self.last_month_filter_summary = None
            self.last_stage_timings = {}
            self.last_ingest_backend = 'dummy'
            self.last_work_order_sheet_frame = pd.DataFrame()
            self.last_candidate_products = ()

        def prepare_payload(self, input_path: Path, *, progress_callback: object | None = None) -> bool:
            assert input_path == request.input_path
            return True

    def _raise_on_progress(_event: object) -> None:
        raise RuntimeError('progress sink failed')

    monkeypatch.setattr('src.services.costing_service.CostingWorkbookETL', _ProgressETL)

    result = precheck_costing_run(request, progress_callback=_raise_on_progress)

    assert result.status == ServiceStatus.SUCCEEDED
    assert 'Progress callback failed' in caplog.text

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.analytics.contracts import QualityMetric
from src.config.pipelines import GB_PIPELINE, SK_PIPELINE
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

        def prepare_payload(self, input_path: Path) -> bool:
            captured['prepared'] = True
            captured['input_path'] = input_path
            return True

        def process_file(self, input_path: Path, output_path: Path) -> bool:
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


def test_run_costing_request_writes_only_workbook_and_returns_runtime_summary(monkeypatch, tmp_path: Path) -> None:
    request = _request(tmp_path)
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
            self.last_month_filter_summary = None
            self.last_stage_timings = {'ingest': 0.1, 'export': 0.2}
            self.last_ingest_backend = 'calamine'

        def prepare_payload(self, input_path: Path) -> bool:
            raise AssertionError('run must process workbook')

        def process_file(self, input_path: Path, output_path: Path) -> bool:
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

        def prepare_payload(self, input_path: Path) -> bool:
            return True

    monkeypatch.setattr('src.services.costing_service.CostingWorkbookETL', _DummyETL)

    result = precheck_costing_run(request)

    assert result.status == ServiceStatus.SUCCEEDED
    assert captured['standalone_cost_items'] == SK_PIPELINE.standalone_cost_items
    assert captured['product_anomaly_scope_mode'] == SK_PIPELINE.product_anomaly_scope_mode
    assert captured['product_order'] == request.product_order

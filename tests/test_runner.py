from __future__ import annotations

from pathlib import Path

from src.analytics.contracts import QualityMetric
from src.config.pipelines import GB_PIPELINE, SK_PIPELINE, PipelineConfig, ProductOrder
from src.config.product_whitelist_store import ProductWhitelistConfigError
from src.etl import runner
from src.etl.month_filter import MonthFilterSummary, MonthRange
from src.etl.runner import build_benchmark_log_text, find_input_files, run_pipeline
from src.services.costing_service import CostingRunRequest, CostingRunResult, ServiceStatus


class _FakeGlobDir:
    def __init__(self, responses: list[list[Path]]) -> None:
        self.responses = responses
        self.patterns: list[str] = []

    def glob(self, pattern: str) -> list[Path]:
        self.patterns.append(pattern)
        return self.responses[len(self.patterns) - 1]


def _metric(value: str = '1') -> QualityMetric:
    return QualityMetric('行数勾稽', '产品数量统计输出行数', value, '仅保留有效工单')


def _config(
    tmp_path: Path,
    *,
    name: str = 'gb',
    processed_dir: Path | None = None,
    product_order: ProductOrder = (('CONFIG_CODE', '配置白名单'),),
) -> PipelineConfig:
    upper_name = name.upper()
    pipeline = GB_PIPELINE if name == 'gb' else SK_PIPELINE
    return PipelineConfig(
        name=name,
        raw_dir=tmp_path,
        processed_dir=processed_dir or tmp_path / 'processed',
        input_patterns=(f'{upper_name}-*.xlsx',),
        product_order=product_order,
        standalone_cost_items=pipeline.standalone_cost_items,
        product_anomaly_scope_mode=pipeline.product_anomaly_scope_mode,
    )


def _planned_workbook_path(request: CostingRunRequest) -> Path:
    suffix = ''
    if request.month_start or request.month_end:
        start = request.month_start or ''
        end = request.month_end or ''
        suffix = f'_{start}_{end}'
    return request.output_dir / f'{request.input_path.stem}_处理后{suffix}.xlsx'


def _succeeded_result(
    workbook_path: Path,
    *,
    quality_metrics: tuple[QualityMetric, ...] = (_metric(),),
    error_log_count: int = 0,
    stage_timings: dict[str, float] | None = None,
    ingest_backend: str = 'unknown',
    output_size_bytes: int = 0,
    month_filter_summary: MonthFilterSummary | None = None,
) -> CostingRunResult:
    return CostingRunResult(
        status=ServiceStatus.SUCCEEDED,
        message='处理成功',
        workbook_path=workbook_path,
        quality_metrics=quality_metrics,
        error_log_count=error_log_count,
        stage_timings=stage_timings or {},
        ingest_backend=ingest_backend,
        output_size_bytes=output_size_bytes,
        month_filter_summary=month_filter_summary,
    )


def _failed_result(workbook_path: Path, message: str = '处理失败') -> CostingRunResult:
    return CostingRunResult(
        status=ServiceStatus.FAILED,
        message=message,
        workbook_path=workbook_path,
        error_code='ETL_FAILED',
    )


def _month_filter_summary(month_range: MonthRange) -> MonthFilterSummary:
    return MonthFilterSummary(
        month_range=month_range,
        input_rows=3,
        output_rows=2,
        input_months=('2025-01', '2025-02', '2025-03'),
        output_months=('2025-02', '2025-03'),
    )


def test_find_input_files_preserves_pattern_order_and_deduplicates(tmp_path) -> None:
    same_file = tmp_path / 'SK-成本计算单.xlsx'
    second_file = tmp_path / 'SK- 成本计算单.xlsx'
    third_file = tmp_path / 'SK-anything.xlsx'
    fake_dir = _FakeGlobDir([[same_file, second_file], [second_file], [same_file, third_file]])
    config = PipelineConfig(
        name='sk',
        raw_dir=fake_dir,  # type: ignore[arg-type]
        processed_dir=tmp_path,
        input_patterns=('SK-*成本计算单.xlsx', 'SK-* 成本计算单.xlsx', 'SK-*.xlsx'),
        product_order=(('DP.C.P0197AA', '动力线'),),
        standalone_cost_items=('委外加工费', '软件费用'),
        product_anomaly_scope_mode='legacy_single_scope',
    )

    assert find_input_files(config) == [same_file, second_file, third_file]


def test_find_input_files_matches_uppercase_and_lowercase_pipeline_prefixes(tmp_path) -> None:
    gb_upper = tmp_path / 'GB-成本计算单_202601.xlsx'
    gb_lower = tmp_path / 'gb-成本计算单_202601.xlsx'
    sk_upper = tmp_path / 'SK-成本计算单_202601.xlsx'
    sk_lower = tmp_path / 'sk-成本计算单_202601.xlsx'
    for path in (gb_upper, gb_lower, sk_upper, sk_lower):
        path.touch()

    gb_config = PipelineConfig(
        name='gb',
        raw_dir=tmp_path,
        processed_dir=tmp_path,
        input_patterns=GB_PIPELINE.input_patterns,
        product_order=GB_PIPELINE.product_order,
        standalone_cost_items=GB_PIPELINE.standalone_cost_items,
        product_anomaly_scope_mode=GB_PIPELINE.product_anomaly_scope_mode,
    )
    sk_config = PipelineConfig(
        name='sk',
        raw_dir=tmp_path,
        processed_dir=tmp_path,
        input_patterns=SK_PIPELINE.input_patterns,
        product_order=SK_PIPELINE.product_order,
        standalone_cost_items=SK_PIPELINE.standalone_cost_items,
        product_anomaly_scope_mode=SK_PIPELINE.product_anomaly_scope_mode,
    )

    assert set(find_input_files(gb_config)) == {gb_upper, gb_lower}
    assert set(find_input_files(sk_config)) == {sk_upper, sk_lower}


def test_run_pipeline_delegates_normal_run_to_service_and_writes_only_workbook(monkeypatch, capsys, tmp_path) -> None:
    input_file = tmp_path / 'GB-成本计算单.xlsx'
    input_file.write_bytes(b'raw')
    config = _config(tmp_path)
    service_product_order = (('GB_SERVICE', '服务白名单'),)
    captured: dict[str, object] = {}

    def _fake_load_product_order_for_pipeline(pipeline_name: str) -> ProductOrder:
        captured['loader_pipeline'] = pipeline_name
        return service_product_order

    def _fake_run_costing_request(request: CostingRunRequest) -> CostingRunResult:
        captured['request'] = request
        workbook_path = _planned_workbook_path(request)
        workbook_path.parent.mkdir(parents=True, exist_ok=True)
        workbook_path.write_bytes(b'xlsx')
        return _succeeded_result(
            workbook_path,
            error_log_count=2,
            quality_metrics=(_metric('7'),),
            stage_timings={'ingest': 0.25},
            ingest_backend='calamine',
            output_size_bytes=4,
        )

    monkeypatch.setattr(runner, 'load_product_order_for_pipeline', _fake_load_product_order_for_pipeline, raising=False)
    monkeypatch.setattr(runner, 'run_costing_request', _fake_run_costing_request, raising=False)
    monkeypatch.setattr(
        runner,
        'precheck_costing_run',
        lambda request: (_ for _ in ()).throw(AssertionError('normal run must not precheck')),
        raising=False,
    )

    exit_code = run_pipeline(config)
    stdout = capsys.readouterr().out
    request = captured['request']
    workbook_path = config.processed_dir / 'GB-成本计算单_处理后.xlsx'

    assert exit_code == 0
    assert captured['loader_pipeline'] == 'gb'
    assert isinstance(request, CostingRunRequest)
    assert request.pipeline == 'gb'
    assert request.input_path == input_file
    assert request.output_dir == config.processed_dir
    assert request.month_start is None
    assert request.month_end is None
    assert request.benchmark is False
    assert request.overwrite_confirmed is True
    assert request.product_order == service_product_order
    assert workbook_path.exists()
    assert not (config.processed_dir / 'GB-成本计算单_处理后_error_log.csv').exists()
    assert not (config.processed_dir / 'GB-成本计算单_处理后_summary.json').exists()
    assert 'pipeline=gb' in stdout
    assert f'input={input_file}' in stdout
    assert f'output={workbook_path}' in stdout
    assert 'error_log_count=2' in stdout
    assert '产品数量统计输出行数=7' in stdout


def test_run_pipeline_check_only_delegates_to_precheck_without_writing_outputs(monkeypatch, capsys, tmp_path) -> None:
    input_file = tmp_path / 'GB-成本计算单.xlsx'
    input_file.write_bytes(b'raw')
    config = _config(tmp_path)
    service_product_order = (('GB_SERVICE', '服务白名单'),)
    captured: dict[str, object] = {}

    def _fake_precheck_costing_run(request: CostingRunRequest) -> CostingRunResult:
        captured['request'] = request
        return _succeeded_result(
            _planned_workbook_path(request),
            quality_metrics=(_metric('3'),),
            error_log_count=1,
            stage_timings={'ingest': 0.1, 'normalize': 0.2},
            ingest_backend='openpyxl',
        )

    monkeypatch.setattr(
        runner,
        'load_product_order_for_pipeline',
        lambda pipeline_name: service_product_order,
        raising=False,
    )
    monkeypatch.setattr(runner, 'precheck_costing_run', _fake_precheck_costing_run, raising=False)
    monkeypatch.setattr(
        runner,
        'run_costing_request',
        lambda request: (_ for _ in ()).throw(AssertionError('check-only must not run workbook export')),
        raising=False,
    )

    exit_code = run_pipeline(config, check_only=True)
    stdout = capsys.readouterr().out
    request = captured['request']

    assert exit_code == 0
    assert isinstance(request, CostingRunRequest)
    assert request.pipeline == 'gb'
    assert request.input_path == input_file
    assert request.output_dir == config.processed_dir
    assert request.benchmark is False
    assert request.overwrite_confirmed is True
    assert request.product_order == service_product_order
    assert 'mode=check-only' in stdout
    assert 'pipeline=gb' in stdout
    assert '产品数量统计输出行数=3' in stdout
    assert not config.processed_dir.exists()


def test_run_pipeline_passes_month_range_to_service_and_does_not_write_csv(monkeypatch, capsys, tmp_path) -> None:
    input_file = tmp_path / 'GB-成本计算单.xlsx'
    input_file.write_bytes(b'raw')
    config = _config(tmp_path)
    month_range = MonthRange(start='2025-01', end='2025-03')
    captured: dict[str, CostingRunRequest] = {}

    def _fake_run_costing_request(request: CostingRunRequest) -> CostingRunResult:
        captured['request'] = request
        workbook_path = _planned_workbook_path(request)
        workbook_path.parent.mkdir(parents=True, exist_ok=True)
        workbook_path.write_bytes(b'xlsx')
        return _succeeded_result(
            workbook_path,
            quality_metrics=(_metric('2'),),
            output_size_bytes=4,
            month_filter_summary=_month_filter_summary(month_range),
        )

    monkeypatch.setattr(
        runner,
        'load_product_order_for_pipeline',
        lambda pipeline_name: (('GB_SERVICE', '服务白名单'),),
        raising=False,
    )
    monkeypatch.setattr(runner, 'run_costing_request', _fake_run_costing_request, raising=False)

    exit_code = run_pipeline(config, month_range=month_range)
    stdout = capsys.readouterr().out
    workbook_path = config.processed_dir / 'GB-成本计算单_处理后_2025-01_2025-03.xlsx'

    assert exit_code == 0
    assert captured['request'].month_start == '2025-01'
    assert captured['request'].month_end == '2025-03'
    assert workbook_path.exists()
    assert not (config.processed_dir / 'GB-成本计算单_处理后_2025-01_2025-03_error_log.csv').exists()
    assert not (config.processed_dir / 'GB-成本计算单_处理后_2025-01_2025-03_summary.json').exists()
    assert f'output={workbook_path}' in stdout
    assert 'month_range=[2025-01, 2025-03]' in stdout
    assert 'month_filter_rows=3->2' in stdout
    assert 'months_before=2025-01,2025-02,2025-03' in stdout
    assert 'months_after=2025-02,2025-03' in stdout


def test_run_pipeline_check_only_prints_month_filter_summary(monkeypatch, capsys, tmp_path) -> None:
    input_file = tmp_path / 'SK-成本计算单.xlsx'
    input_file.write_bytes(b'raw')
    config = _config(tmp_path, name='sk')
    month_range = MonthRange(start='2025-01', end='2025-03')

    def _fake_precheck_costing_run(request: CostingRunRequest) -> CostingRunResult:
        return _succeeded_result(
            _planned_workbook_path(request),
            quality_metrics=(_metric('2'),),
            month_filter_summary=_month_filter_summary(month_range),
        )

    monkeypatch.setattr(
        runner,
        'load_product_order_for_pipeline',
        lambda pipeline_name: (('SK_SERVICE', '服务白名单'),),
        raising=False,
    )
    monkeypatch.setattr(runner, 'precheck_costing_run', _fake_precheck_costing_run, raising=False)

    exit_code = run_pipeline(config, month_range=month_range, check_only=True)
    stdout = capsys.readouterr().out

    assert exit_code == 0
    assert 'mode=check-only' in stdout
    assert 'month_range=[2025-01, 2025-03]' in stdout
    assert 'month_filter_rows=3->2' in stdout
    assert not config.processed_dir.exists()


def test_run_pipeline_returns_one_when_service_fails(monkeypatch, tmp_path) -> None:
    input_file = tmp_path / 'SK-成本计算单.xlsx'
    input_file.write_bytes(b'raw')
    config = _config(tmp_path, name='sk')
    planned_workbook = config.processed_dir / 'SK-成本计算单_处理后.xlsx'

    monkeypatch.setattr(
        runner,
        'load_product_order_for_pipeline',
        lambda pipeline_name: (('SK_SERVICE', '服务白名单'),),
        raising=False,
    )
    monkeypatch.setattr(
        runner,
        'run_costing_request',
        lambda request: _failed_result(planned_workbook, '处理失败'),
        raising=False,
    )

    assert run_pipeline(config) == 1


def test_run_pipeline_returns_one_when_product_whitelist_loader_fails(monkeypatch, tmp_path) -> None:
    input_file = tmp_path / 'GB-成本计算单.xlsx'
    input_file.write_bytes(b'raw')
    config = _config(tmp_path)
    captured = {'loader_called': False}

    def _fake_load_product_order_for_pipeline(pipeline_name: str) -> ProductOrder:
        captured['loader_called'] = True
        raise ProductWhitelistConfigError('产品白名单配置不是有效 JSON')

    monkeypatch.setattr(runner, 'load_product_order_for_pipeline', _fake_load_product_order_for_pipeline, raising=False)
    monkeypatch.setattr(
        runner,
        'run_costing_request',
        lambda request: (_ for _ in ()).throw(AssertionError('service must not run when whitelist is invalid')),
        raising=False,
    )

    assert run_pipeline(config) == 1
    assert captured['loader_called'] is True
    assert not config.processed_dir.exists()


def test_build_benchmark_log_text_reports_stage_timings_and_zero_error_log_size(tmp_path) -> None:
    input_file = tmp_path / 'input.xlsx'
    output_file = tmp_path / 'output.xlsx'
    error_log_file = tmp_path / 'error_log.csv'
    input_file.write_bytes(b'abc')
    output_file.write_bytes(b'output')
    error_log_file.write_bytes(b'csv')

    text = build_benchmark_log_text(
        input_path=input_file,
        output_path=output_file,
        error_log_path=error_log_file,
        error_log_count=4,
        stage_timings={'ingest': 0.1, 'normalize': 0.2, 'export': 0.3},
        ingest_backend='calamine',
        output_written=True,
    )

    assert '[benchmark]' in text
    assert 'input_size_bytes=3' in text
    assert 'output_size_bytes=6' in text
    assert 'error_log_size_bytes=0' in text
    assert 'planned_error_log=' in text
    assert 'ingest_backend=calamine' in text
    assert 'payload_total_seconds=0.300' in text
    assert 'export_seconds=0.300' in text
    assert 'stage_export_seconds=0.300' in text
    assert 'stage_ingest_seconds=0.100' in text
    assert 'stage_normalize_seconds=0.200' in text
    assert 'stage_total_observed_seconds=0.600' in text
    assert 'error_log_count=4' in text


def test_run_pipeline_check_only_benchmark_prints_service_timings_without_writing(
    monkeypatch,
    capsys,
    tmp_path,
) -> None:
    input_file = tmp_path / 'SK-成本计算单.xlsx'
    input_file.write_bytes(b'raw')
    config = _config(tmp_path, name='sk')

    def _fake_precheck_costing_run(request: CostingRunRequest) -> CostingRunResult:
        return _succeeded_result(
            _planned_workbook_path(request),
            stage_timings={'ingest': 0.5, 'export': 0.75},
            ingest_backend='calamine',
        )

    monkeypatch.setattr(
        runner,
        'load_product_order_for_pipeline',
        lambda pipeline_name: (('SK_SERVICE', '服务白名单'),),
        raising=False,
    )
    monkeypatch.setattr(runner, 'precheck_costing_run', _fake_precheck_costing_run, raising=False)

    exit_code = run_pipeline(config, check_only=True, benchmark=True)
    stdout = capsys.readouterr().out

    assert exit_code == 0
    assert '[benchmark]' in stdout
    assert 'output_written=false' in stdout
    assert 'ingest_backend=calamine' in stdout
    assert 'input_size_bytes=3' in stdout
    assert 'output_size_bytes=0' in stdout
    assert 'error_log_size_bytes=0' in stdout
    assert 'payload_total_seconds=0.500' in stdout
    assert 'export_seconds=0.000' in stdout
    assert 'stage_export_seconds=0.750' in stdout
    assert 'stage_ingest_seconds=0.500' in stdout
    assert not config.processed_dir.exists()


def test_run_pipeline_normal_benchmark_reports_export_timing_and_zero_error_log_size(
    monkeypatch,
    capsys,
    tmp_path,
) -> None:
    input_file = tmp_path / 'GB-成本计算单.xlsx'
    input_file.write_bytes(b'raw')
    config = _config(tmp_path)

    def _fake_run_costing_request(request: CostingRunRequest) -> CostingRunResult:
        workbook_path = _planned_workbook_path(request)
        workbook_path.parent.mkdir(parents=True, exist_ok=True)
        workbook_path.write_bytes(b'xlsx')
        return _succeeded_result(
            workbook_path,
            stage_timings={'ingest': 0.5, 'analysis': 1.25, 'export': 0.75},
            ingest_backend='openpyxl',
            output_size_bytes=4,
        )

    monkeypatch.setattr(
        runner,
        'load_product_order_for_pipeline',
        lambda pipeline_name: (('GB_SERVICE', '服务白名单'),),
        raising=False,
    )
    monkeypatch.setattr(runner, 'run_costing_request', _fake_run_costing_request, raising=False)

    exit_code = run_pipeline(config, benchmark=True)
    stdout = capsys.readouterr().out

    assert exit_code == 0
    assert '[benchmark]' in stdout
    assert 'output_written=true' in stdout
    assert 'output_size_bytes=4' in stdout
    assert 'error_log_size_bytes=0' in stdout
    assert 'ingest_backend=openpyxl' in stdout
    assert 'payload_total_seconds=1.750' in stdout
    assert 'export_seconds=0.750' in stdout
    assert 'stage_export_seconds=0.750' in stdout

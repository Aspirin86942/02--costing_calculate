from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.analytics.contracts import QualityMetric, SheetModel, WorkbookPayload
from src.config.pipelines import PipelineConfig
from src.etl.month_filter import MonthFilterSummary, MonthRange
from src.etl.runner import find_input_files, run_pipeline


class _FakeGlobDir:
    def __init__(self, responses: list[list[Path]]) -> None:
        self.responses = responses
        self.patterns: list[str] = []

    def glob(self, pattern: str) -> list[Path]:
        self.patterns.append(pattern)
        return self.responses[len(self.patterns) - 1]


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


def test_run_pipeline_prints_quality_summary_without_writing_log_file(
    monkeypatch,
    capsys,
    tmp_path,
) -> None:
    input_file = tmp_path / 'SK-成本计算单.xlsx'
    input_file.touch()
    processed_dir = tmp_path / 'processed'
    processed_dir.mkdir()
    config = PipelineConfig(
        name='sk',
        raw_dir=tmp_path,
        processed_dir=processed_dir,
        input_patterns=('SK-*.xlsx',),
        product_order=(('DP.C.P0197AA', '动力线'),),
        product_anomaly_scope_mode='legacy_single_scope',
    )

    captured: dict[str, tuple[str, ...] | None] = {}

    class _DummyETL:
        def __init__(
            self,
            skip_rows: int,
            *,
            product_order,
            standalone_cost_items,
            product_anomaly_scope_mode,
            month_range=None,
        ) -> None:
            self.skip_rows = skip_rows
            self.product_order = product_order
            self.last_quality_metrics = (
                QualityMetric('行数勾稽', '产品数量统计输出行数', '1', '仅保留有效工单'),
                QualityMetric('分析覆盖率', '可参与分析占比', '100.00%', '白名单工单覆盖率'),
            )
            self.last_error_log_count = 0
            self.last_error_log_frame = pd.DataFrame(columns=['row_id', 'issue_type', 'message'])
            self.last_month_filter_summary = None
            captured['standalone_cost_items'] = standalone_cost_items
            captured['product_anomaly_scope_mode'] = product_anomaly_scope_mode

        def process_file(self, input_path: Path, output_path: Path) -> bool:
            output_path.write_text('ok', encoding='utf-8')
            return True

    monkeypatch.setattr('src.etl.runner.CostingWorkbookETL', _DummyETL)

    exit_code = run_pipeline(config)
    stdout = capsys.readouterr().out
    log_path = processed_dir / 'SK-成本计算单_处理后.log'
    error_log_csv_path = processed_dir / 'SK-成本计算单_处理后_error_log.csv'

    assert exit_code == 0
    assert not log_path.exists()
    assert error_log_csv_path.exists()
    assert 'pipeline=sk' in stdout
    assert '可参与分析占比=100.00%' in stdout
    pd.testing.assert_frame_equal(
        pd.read_csv(error_log_csv_path, encoding='utf-8-sig'),
        pd.DataFrame(columns=['row_id', 'issue_type', 'message']),
    )
    assert captured['standalone_cost_items'] == config.standalone_cost_items
    assert captured['product_anomaly_scope_mode'] == config.product_anomaly_scope_mode


def test_run_pipeline_check_only_builds_payload_without_writing_outputs(monkeypatch, capsys, tmp_path) -> None:
    input_file = tmp_path / 'GB-成本计算单.xlsx'
    input_file.write_text('placeholder', encoding='utf-8')
    processed_dir = tmp_path / 'processed'
    config = PipelineConfig(
        name='gb',
        raw_dir=tmp_path,
        processed_dir=processed_dir,
        input_patterns=('GB-*.xlsx',),
        product_order=(('GB_C.D.B0040AA', 'BMS-750W驱动器'),),
        standalone_cost_items=('委外加工费',),
        product_anomaly_scope_mode='doc_type_split',
    )

    captured: dict[str, Path] = {}

    class _DummyETL:
        def __init__(
            self,
            skip_rows: int,
            *,
            product_order,
            standalone_cost_items,
            product_anomaly_scope_mode,
            month_range=None,
        ) -> None:
            self.last_quality_metrics = (
                QualityMetric('行数勾稽', '产品数量统计输出行数', '3', '仅保留有效工单'),
            )
            self.last_error_log_count = 2
            self.last_error_log_frame = pd.DataFrame([{'row_id': 'WO-001', 'issue_type': 'MISSING_AMOUNT'}])
            self.last_month_filter_summary = None
            self.last_stage_timings = {'ingest': 0.1, 'normalize': 0.2}

        def prepare_payload(self, input_path: Path) -> bool:
            captured['input_path'] = input_path
            return True

        def process_file(self, input_path: Path, output_path: Path) -> bool:
            raise AssertionError('check-only must not write workbook')

    monkeypatch.setattr('src.etl.runner.CostingWorkbookETL', _DummyETL)

    exit_code = run_pipeline(config, check_only=True)
    stdout = capsys.readouterr().out

    assert exit_code == 0
    assert captured['input_path'] == input_file
    assert 'mode=check-only' in stdout
    assert 'pipeline=gb' in stdout
    assert 'output=' in stdout
    assert not processed_dir.exists()


def test_run_pipeline_real_payload_path_keeps_stdout_and_skips_log_file(monkeypatch, capsys, tmp_path) -> None:
    input_file = tmp_path / 'GB-成本计算单.xlsx'
    input_file.touch()
    processed_dir = tmp_path / 'processed'
    processed_dir.mkdir()
    config = PipelineConfig(
        name='gb',
        raw_dir=tmp_path,
        processed_dir=processed_dir,
        input_patterns=('GB-*.xlsx',),
        product_order=(('GB_C.D.B0040AA', 'BMS-750W驱动器'),),
        standalone_cost_items=('委外加工费',),
        product_anomaly_scope_mode='doc_type_split',
    )

    captured: dict[str, object] = {}
    payload = WorkbookPayload(
        sheet_models=(
            SheetModel(
                sheet_name='成本明细',
                columns=('产品编码',),
                rows_factory=lambda: iter([('GB_C.D.B0040AA',)]),
                column_types={'产品编码': 'text'},
                number_formats={},
            ),
        ),
        quality_metrics=(QualityMetric('行数勾稽', '产品数量统计输出行数', '1', '仅保留有效工单'),),
        error_log_count=2,
        stage_timings={'ingest': 1.0, 'normalize': 2.0, 'fact': 3.0, 'analysis': 4.0, 'presentation': 5.0},
        error_log_export=pd.DataFrame(
            [
                {'row_id': 'WO-001', 'issue_type': 'MISSING_AMOUNT', 'message': 'missing amount'},
                {'row_id': 'WO-002', 'issue_type': 'TOTAL_COST_MISMATCH', 'message': 'mismatch'},
            ]
        ),
    )

    def _fake_build_workbook_payload(
        self,
        input_path: Path,
        *,
        standalone_cost_items: tuple[str, ...],
        product_anomaly_scope_mode: str,
        month_range=None,
        artifacts_transform=None,
    ):
        captured['input_path'] = input_path
        captured['standalone_cost_items'] = standalone_cost_items
        captured['product_anomaly_scope_mode'] = product_anomaly_scope_mode
        captured['artifacts_transform'] = artifacts_transform
        return payload

    def _fake_write_workbook_from_models(self, output_path: Path, *, sheet_models) -> None:
        captured['output_path'] = output_path
        captured['sheet_names'] = [model.sheet_name for model in sheet_models]
        output_path.write_text('ok', encoding='utf-8')

    monkeypatch.setattr('src.etl.pipeline.CostingEtlPipeline.build_workbook_payload', _fake_build_workbook_payload)
    monkeypatch.setattr(
        'src.excel.workbook_writer.CostingWorkbookWriter.write_workbook_from_models',
        _fake_write_workbook_from_models,
    )

    exit_code = run_pipeline(config)
    stdout = capsys.readouterr().out
    log_path = processed_dir / 'GB-成本计算单_处理后.log'
    error_log_csv_path = processed_dir / 'GB-成本计算单_处理后_error_log.csv'

    assert exit_code == 0
    assert not log_path.exists()
    assert error_log_csv_path.exists()
    assert 'pipeline=gb' in stdout
    assert 'error_log_count=2' in stdout
    assert captured['input_path'] == input_file
    assert captured['output_path'] == processed_dir / 'GB-成本计算单_处理后.xlsx'
    assert captured['standalone_cost_items'] == ('委外加工费',)
    assert captured['product_anomaly_scope_mode'] == 'doc_type_split'
    assert callable(captured['artifacts_transform'])
    assert captured['sheet_names'] == ['成本明细']
    pd.testing.assert_frame_equal(
        pd.read_csv(error_log_csv_path, encoding='utf-8-sig'),
        payload.error_log_export,
        check_dtype=False,
    )


def test_run_pipeline_uses_month_suffix_in_output_names(monkeypatch, capsys, tmp_path) -> None:
    input_file = tmp_path / 'GB-成本计算单.xlsx'
    input_file.touch()
    processed_dir = tmp_path / 'processed'
    processed_dir.mkdir()
    config = PipelineConfig(
        name='gb',
        raw_dir=tmp_path,
        processed_dir=processed_dir,
        input_patterns=('GB-*.xlsx',),
        product_order=(('GB_C.D.B0040AA', 'BMS-750W驱动器'),),
        standalone_cost_items=('委外加工费',),
        product_anomaly_scope_mode='doc_type_split',
    )

    captured: dict[str, object] = {}
    month_range = MonthRange(start='2025-01', end='2025-03')

    class _DummyETL:
        def __init__(
            self,
            skip_rows: int,
            *,
            product_order,
            standalone_cost_items,
            product_anomaly_scope_mode,
            month_range,
        ) -> None:
            self.last_quality_metrics = ()
            self.last_error_log_count = 0
            self.last_error_log_frame = pd.DataFrame(columns=['row_id', 'issue_type', 'message'])
            self.last_month_filter_summary = MonthFilterSummary(
                month_range=month_range,
                input_rows=3,
                output_rows=2,
                input_months=('2025-01', '2025-02', '2025-03'),
                output_months=('2025-02', '2025-03'),
            )

        def process_file(self, input_path: Path, output_path: Path) -> bool:
            captured['output_path'] = output_path
            output_path.write_text('ok', encoding='utf-8')
            return True

    monkeypatch.setattr('src.etl.runner.CostingWorkbookETL', _DummyETL)

    exit_code = run_pipeline(config, month_range=month_range)
    stdout = capsys.readouterr().out

    assert exit_code == 0
    assert captured['output_path'] == processed_dir / 'GB-成本计算单_处理后_2025-01_2025-03.xlsx'
    assert (processed_dir / 'GB-成本计算单_处理后_2025-01_2025-03_error_log.csv').exists()
    assert 'month_range=[2025-01, 2025-03]' in stdout
    assert 'month_filter_rows=3->2' in stdout


def test_run_pipeline_succeeds_when_month_range_matches_no_rows(monkeypatch, capsys, tmp_path) -> None:
    input_file = tmp_path / 'SK-成本计算单.xlsx'
    input_file.touch()
    processed_dir = tmp_path / 'processed'
    processed_dir.mkdir()
    config = PipelineConfig(
        name='sk',
        raw_dir=tmp_path,
        processed_dir=processed_dir,
        input_patterns=('SK-*.xlsx',),
        product_order=(('DP.C.P0197AA', '动力线'),),
        product_anomaly_scope_mode='legacy_single_scope',
    )

    month_range = MonthRange(start='2026-01', end='2026-03')

    class _DummyETL:
        def __init__(
            self,
            skip_rows: int,
            *,
            product_order,
            standalone_cost_items,
            product_anomaly_scope_mode,
            month_range,
        ) -> None:
            self.last_quality_metrics = ()
            self.last_error_log_count = 0
            self.last_error_log_frame = pd.DataFrame(columns=['row_id', 'issue_type', 'message'])
            self.last_month_filter_summary = MonthFilterSummary(
                month_range=month_range,
                input_rows=5,
                output_rows=0,
                input_months=('2025-01', '2025-02'),
                output_months=(),
            )

        def process_file(self, input_path: Path, output_path: Path) -> bool:
            output_path.write_text('ok', encoding='utf-8')
            return True

    monkeypatch.setattr('src.etl.runner.CostingWorkbookETL', _DummyETL)

    exit_code = run_pipeline(config, month_range=month_range)
    stdout = capsys.readouterr().out

    assert exit_code == 0
    assert 'month_filter_rows=5->0' in stdout
    assert 'months_after=-' in stdout
    assert (processed_dir / 'SK-成本计算单_处理后_2026-01_2026-03.xlsx').exists()
    assert (processed_dir / 'SK-成本计算单_处理后_2026-01_2026-03_error_log.csv').exists()

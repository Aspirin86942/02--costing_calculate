from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.analytics.contracts import QualityMetric, SheetModel, WorkbookPayload
from src.config.pipelines import PipelineConfig
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
    )

    captured: dict[str, tuple[str, ...] | None] = {}

    class _DummyETL:
        def __init__(self, skip_rows: int, *, product_order, standalone_cost_items) -> None:
            self.skip_rows = skip_rows
            self.product_order = product_order
            self.last_quality_metrics = (
                QualityMetric('行数勾稽', '产品数量统计输出行数', '1', '仅保留有效工单'),
                QualityMetric('分析覆盖率', '可参与分析占比', '100.00%', '白名单工单覆盖率'),
            )
            self.last_error_log_count = 0
            self.last_error_log_frame = pd.DataFrame(columns=['row_id', 'issue_type', 'message'])
            captured['standalone_cost_items'] = standalone_cost_items

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
        quality_metrics=(
            QualityMetric('行数勾稽', '产品数量统计输出行数', '1', '仅保留有效工单'),
        ),
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
        artifacts_transform=None,
    ):
        captured['input_path'] = input_path
        captured['standalone_cost_items'] = standalone_cost_items
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
    assert callable(captured['artifacts_transform'])
    assert captured['sheet_names'] == ['成本明细']
    pd.testing.assert_frame_equal(
        pd.read_csv(error_log_csv_path, encoding='utf-8-sig'),
        payload.error_log_export,
        check_dtype=False,
    )

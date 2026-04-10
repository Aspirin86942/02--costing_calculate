from __future__ import annotations

from pathlib import Path

from src.analytics.contracts import QualityMetric
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
    )

    assert find_input_files(config) == [same_file, second_file, third_file]


def test_run_pipeline_writes_quality_log_and_returns_zero(monkeypatch, capsys, tmp_path) -> None:
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

    class _DummyETL:
        def __init__(self, skip_rows: int, product_order) -> None:
            self.skip_rows = skip_rows
            self.product_order = product_order
            self.last_quality_metrics = (
                QualityMetric('行数勾稽', '产品数量统计输出行数', '1', '仅保留有效工单'),
                QualityMetric('分析覆盖率', '可参与分析占比', '100.00%', '白名单工单覆盖率'),
            )
            self.last_error_log_count = 0

        def process_file(self, input_path: Path, output_path: Path) -> bool:
            output_path.write_text('ok', encoding='utf-8')
            return True

    monkeypatch.setattr('src.etl.runner.CostingWorkbookETL', _DummyETL)

    exit_code = run_pipeline(config)
    stdout = capsys.readouterr().out
    log_path = processed_dir / 'SK-成本计算单_处理后.log'

    assert exit_code == 0
    assert log_path.exists()
    assert 'pipeline=sk' in stdout
    assert '可参与分析占比=100.00%' in log_path.read_text(encoding='utf-8')

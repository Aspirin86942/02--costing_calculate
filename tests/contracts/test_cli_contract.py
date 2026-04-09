from __future__ import annotations

from pathlib import Path

from src.config.pipelines import PipelineConfig
from src.etl.runner import find_input_files


class _FakeGlobDir:
    def __init__(self, responses: list[list[Path]]) -> None:
        self.responses = responses
        self.patterns: list[str] = []

    def glob(self, pattern: str) -> list[Path]:
        self.patterns.append(pattern)
        return self.responses[len(self.patterns) - 1]

def test_find_input_files_uses_pipeline_patterns_in_order(tmp_path) -> None:
    same_file = tmp_path / 'GB-成本计算单.xlsx'
    second_file = tmp_path / 'GB- 成本计算单.xlsx'
    third_file = tmp_path / 'GB-anything.xlsx'
    fake_dir = _FakeGlobDir([[same_file, second_file], [second_file], [same_file, third_file]])
    config = PipelineConfig(
        name='gb',
        raw_dir=fake_dir,  # type: ignore[arg-type]
        processed_dir=tmp_path,
        input_patterns=('GB-*成本计算单.xlsx', 'GB-* 成本计算单.xlsx', 'GB-*.xlsx'),
        product_order=(('GB_C.D.B0040AA', 'BMS-750W驱动器'),),
    )

    assert find_input_files(config) == [same_file, second_file, third_file]
    assert fake_dir.patterns == list(config.input_patterns)


def test_find_input_files_returns_empty_list_when_no_file_matches(tmp_path) -> None:
    fake_dir = _FakeGlobDir([[], [], []])
    config = PipelineConfig(
        name='gb',
        raw_dir=fake_dir,  # type: ignore[arg-type]
        processed_dir=tmp_path,
        input_patterns=('GB-*成本计算单.xlsx', 'GB-* 成本计算单.xlsx', 'GB-*.xlsx'),
        product_order=(('GB_C.D.B0040AA', 'BMS-750W驱动器'),),
    )

    assert find_input_files(config) == []

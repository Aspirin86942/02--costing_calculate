from __future__ import annotations

from pathlib import Path

import src.etl.costing_etl as costing_etl


class _FakeGlobDir:
    def __init__(self, responses: list[list[Path]]) -> None:
        self.responses = responses
        self.patterns: list[str] = []

    def glob(self, pattern: str) -> list[Path]:
        self.patterns.append(pattern)
        return self.responses[len(self.patterns) - 1]

    def __str__(self) -> str:
        return 'fake-gb-raw-dir'


def test_find_input_files_preserves_pattern_order_and_deduplicates(monkeypatch, tmp_path) -> None:
    same_file = tmp_path / 'GB-成本计算单.xlsx'
    second_file = tmp_path / 'GB-  成本计算单.xlsx'
    third_file = tmp_path / 'GB-anything.xlsx'
    fake_dir = _FakeGlobDir([[same_file, second_file], [second_file], [same_file, third_file]])

    monkeypatch.setattr(costing_etl, 'GB_RAW_DIR', fake_dir)

    expected = [*sorted([same_file, second_file]), third_file]
    assert costing_etl._find_input_files() == expected
    assert len(fake_dir.patterns) == 3


def test_main_uses_first_matched_input_and_expected_output_suffix(monkeypatch, capsys, tmp_path) -> None:
    input_file = tmp_path / 'GB-成本计算单.xlsx'
    processed_dir = tmp_path / 'processed'
    processed_dir.mkdir()
    captured: dict[str, object] = {}

    class _DummyETL:
        def __init__(self, skip_rows: int) -> None:
            captured['skip_rows'] = skip_rows

        def process_file(self, input_path: Path, output_path: Path) -> bool:
            captured['input_path'] = input_path
            captured['output_path'] = output_path
            return True

    monkeypatch.setattr(costing_etl, '_find_input_files', lambda: [input_file])
    monkeypatch.setattr(costing_etl, 'CostingWorkbookETL', _DummyETL)
    monkeypatch.setattr(costing_etl, 'GB_PROCESSED_DIR', processed_dir)

    costing_etl.main()
    stdout = capsys.readouterr().out

    assert captured['skip_rows'] == 2
    assert captured['input_path'] == input_file
    assert captured['output_path'] == processed_dir / f'{input_file.stem}_处理后.xlsx'
    assert '处理成功' in stdout


def test_main_prints_failure_when_process_file_returns_false(monkeypatch, capsys, tmp_path) -> None:
    input_file = tmp_path / 'GB-成本计算单.xlsx'

    class _DummyETL:
        def __init__(self, skip_rows: int) -> None:
            self.skip_rows = skip_rows

        def process_file(self, input_path: Path, output_path: Path) -> bool:
            return False

    monkeypatch.setattr(costing_etl, '_find_input_files', lambda: [input_file])
    monkeypatch.setattr(costing_etl, 'CostingWorkbookETL', _DummyETL)
    monkeypatch.setattr(costing_etl, 'GB_PROCESSED_DIR', tmp_path)

    costing_etl.main()
    stdout = capsys.readouterr().out

    assert '处理失败' in stdout

from __future__ import annotations

import pytest

from main import main
from src.etl.month_filter import MonthRange


def test_main_requires_pipeline_argument() -> None:
    with pytest.raises(SystemExit) as exc_info:
        main([])
    assert exc_info.value.code == 2


def test_main_rejects_invalid_pipeline() -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(['bad'])
    assert exc_info.value.code == 2


def test_main_passes_month_range_to_runner(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run_pipeline(config, month_range=None, check_only=False, benchmark=False):
        captured['config'] = config
        captured['month_range'] = month_range
        captured['check_only'] = check_only
        captured['benchmark'] = benchmark
        return 0

    monkeypatch.setattr('main.run_pipeline', _fake_run_pipeline)

    exit_code = main(['gb', '--month-start', '2025-01', '--month-end', '2025-03'])

    assert exit_code == 0
    assert captured['month_range'] == MonthRange(start='2025-01', end='2025-03')
    assert captured['check_only'] is False
    assert captured['benchmark'] is False


def test_main_passes_check_only_and_benchmark_flags_to_runner(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run_pipeline(config, month_range=None, check_only=False, benchmark=False):
        captured['config'] = config
        captured['month_range'] = month_range
        captured['check_only'] = check_only
        captured['benchmark'] = benchmark
        return 0

    monkeypatch.setattr('main.run_pipeline', _fake_run_pipeline)

    exit_code = main(['sk', '--check-only', '--benchmark'])

    assert exit_code == 0
    assert captured['check_only'] is True
    assert captured['benchmark'] is True
    assert captured['month_range'] is None


def test_main_rejects_invalid_month_argument() -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(['gb', '--month-start', '2025/01'])
    assert exc_info.value.code == 2

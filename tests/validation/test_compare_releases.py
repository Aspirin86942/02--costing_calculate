from pathlib import Path

from tools.validation.compare_releases import _binary_arguments, _safe_summary


def test_safe_summary_excludes_paths_and_timings() -> None:
    summary = {
        'status': 'succeeded',
        'pipeline': 'gb',
        'sheet_count': 3,
        'error_log_count': 2,
        'output_size_bytes': 123,
        'run_counts': {'reader_rows': 10},
        'workbook_path': 'sensitive/path.xlsx',
        'stage_timings': {'stages': {'total': 1.0}},
    }

    safe = _safe_summary(summary)

    assert safe == {
        'status': 'succeeded',
        'pipeline': 'gb',
        'sheet_count': 3,
        'error_log_count': 2,
        'output_size_bytes': 123,
        'run_counts': {'reader_rows': 10},
    }


def test_binary_arguments_add_optional_month_filter_after_safe_paths() -> None:
    arguments = _binary_arguments(
        Path('baseline.exe'),
        'sk',
        Path('input.xlsx'),
        Path('output.xlsx'),
        month_start='2026-01',
        month_end='2026-06',
    )

    assert arguments == [
        'baseline.exe',
        'sk',
        '--input',
        'input.xlsx',
        '--output',
        'output.xlsx',
        '--redact-paths',
        '--month-start',
        '2026-01',
        '--month-end',
        '2026-06',
    ]

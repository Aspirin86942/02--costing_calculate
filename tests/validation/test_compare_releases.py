from tools.validation.compare_releases import _safe_summary


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

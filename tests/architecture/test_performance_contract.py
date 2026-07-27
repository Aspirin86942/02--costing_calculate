from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MEASURE_SCRIPT = PROJECT_ROOT / 'tools' / 'validation' / 'measure_release.ps1'
PERFORMANCE_DOC = PROJECT_ROOT / 'docs' / 'performance' / 'README.md'


def test_release_measurement_script_enforces_frozen_normal_mode_gates() -> None:
    script = MEASURE_SCRIPT.read_text(encoding='utf-8')

    for expected in (
        'max_wall_median_seconds = 3.2554',
        'max_pws_median_bytes = 375700685',
        'max_output_bytes = 4194321',
        'max_wall_median_seconds = 20.0',
        'max_pws_median_bytes = 2147483648',
        'max_output_bytes = 48658823',
        "'--benchmark'",
        "'--redact-paths'",
        'PeakWorkingSet64',
    ):
        assert expected in script


def test_performance_document_uses_the_current_measurement_entrypoint() -> None:
    document = PERFORMANCE_DOC.read_text(encoding='utf-8')

    assert 'tools/validation/measure_release.ps1' in document
    assert 'tests/rust_oracle' not in document

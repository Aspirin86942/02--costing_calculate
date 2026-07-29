from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MEASURE_SCRIPT = PROJECT_ROOT / 'tools' / 'validation' / 'measure_release.ps1'
PAIRED_MEASURE_SCRIPT = PROJECT_ROOT / 'tools' / 'validation' / 'measure_paired_release.ps1'
PAIRED_MEASURE_SCHEMA = PROJECT_ROOT / 'tools' / 'validation' / 'measure_paired_release.schema.json'
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


def test_paired_measurement_script_freezes_the_experiment_protocol() -> None:
    script = PAIRED_MEASURE_SCRIPT.read_text(encoding='utf-8')

    for expected in (
        "'warmup-baseline'",
        "'warmup-candidate'",
        "@('baseline', 'candidate')",
        "@('candidate', 'baseline')",
        'WaitForExit(10)',
        'PeakWorkingSet64',
        "'--benchmark'",
        "'--redact-paths'",
        "'ARTIFACT_IDENTITY_CHANGED'",
        "'TEMPORARY_RESIDUE_FOUND'",
        'paired_median_relative_delta',
        'candidate_wins',
    ):
        assert expected in script

    assert 'ConvertFrom-Json' in script
    assert 'stage_timings' in script
    assert 'issue_type_counts' in script
    assert 'run_counts' in script


def test_paired_measurement_schema_requires_complete_safe_evidence() -> None:
    schema = json.loads(PAIRED_MEASURE_SCHEMA.read_text(encoding='utf-8'))

    assert schema['additionalProperties'] is False
    assert schema['properties']['status']['enum'] == ['valid', 'invalid']
    assert {
        'artifacts',
        'environment',
        'samples',
        'summary',
        'invalid_reason',
    }.issubset(schema['required'])
    pair_required = set(schema['$defs']['pairRecord']['required'])
    assert pair_required == {
        'pair_number',
        'execution_order',
        'baseline',
        'candidate',
        'relative_deltas',
    }


def test_paired_measurement_self_test_covers_statistics_and_ordering() -> None:
    powershell = shutil.which('pwsh')
    assert powershell is not None
    result = subprocess.run(  # noqa: S603 - resolved executable and repository-owned script.
        [
            powershell,
            '-NoLogo',
            '-NoProfile',
            '-File',
            str(PAIRED_MEASURE_SCRIPT),
            '-SelfTest',
        ],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
        encoding='utf-8',
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload['status'] == 'passed'
    assert payload['median_and_relative_delta'] == {
        'baseline_median': 25.0,
        'candidate_median': 24.5,
        'paired_median_relative_delta': 0.0,
        'candidate_wins': 2,
        'pair_count': 4,
    }
    assert payload['pair_orders'] == [
        ['baseline', 'candidate'],
        ['candidate', 'baseline'],
        ['baseline', 'candidate'],
        ['candidate', 'baseline'],
    ]

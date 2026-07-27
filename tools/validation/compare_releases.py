"""Compare two costing CLI binaries on the same workbook without exposing cell values."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any

from tools.validation.types import PipelineName
from tools.validation.workbook_compare import compare_workbooks, workbook_payloads_identical

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def compare_releases(
    *,
    baseline_binary: Path,
    candidate_binary: Path,
    pipeline: PipelineName,
    input_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    baseline_binary = _require_file(baseline_binary, 'baseline binary')
    candidate_binary = _require_file(candidate_binary, 'candidate binary')
    input_path = _require_file(input_path, 'input workbook')
    output_dir.mkdir(parents=True, exist_ok=True)
    baseline_output = output_dir / f'baseline-{pipeline}.xlsx'
    candidate_output = output_dir / f'candidate-{pipeline}.xlsx'
    for output in (baseline_output, candidate_output):
        if output.exists():
            raise FileExistsError(f'validation output already exists: {output.name}')

    baseline_summary = _run_binary(baseline_binary, pipeline, input_path, baseline_output)
    candidate_summary = _run_binary(candidate_binary, pipeline, input_path, candidate_output)
    if workbook_payloads_identical(baseline_output, candidate_output):
        comparison_mode = 'package-fast-path'
        mismatches: list[dict[str, str | None]] = []
    else:
        comparison_mode = 'semantic'
        comparison = compare_workbooks(baseline_output, candidate_output, pipeline=pipeline)
        mismatches = [
            {
                'sheet': mismatch.sheet,
                'coordinate': mismatch.coordinate,
                'kind': mismatch.mismatch_kind,
            }
            for mismatch in comparison.mismatches
        ]

    return {
        'schema_version': 1,
        'status': 'passed' if not mismatches else 'failed',
        'pipeline': pipeline,
        'comparison_mode': comparison_mode,
        'mismatch_count': len(mismatches),
        'mismatches': mismatches,
        'baseline': _safe_summary(baseline_summary),
        'candidate': _safe_summary(candidate_summary),
    }


def _require_file(path: Path, label: str) -> Path:
    resolved = path.resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f'{label} not found: {path.name}')
    return resolved


def _run_binary(
    binary: Path,
    pipeline: PipelineName,
    input_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    result = subprocess.run(  # noqa: S603 - both binaries are explicit, verified local files.
        [
            str(binary),
            pipeline,
            '--input',
            str(input_path),
            '--output',
            str(output_path),
            '--redact-paths',
        ],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
        encoding='utf-8',
    )
    if result.returncode != 0:
        raise RuntimeError(f'{binary.name} failed with exit code {result.returncode}: {result.stderr.strip()}')
    payload = json.loads(result.stdout)
    if payload.get('status') != 'succeeded' or payload.get('pipeline') != pipeline:
        raise RuntimeError(f'{binary.name} returned an unexpected run summary')
    return payload


def _safe_summary(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        key: summary.get(key)
        for key in (
            'status',
            'pipeline',
            'sheet_count',
            'error_log_count',
            'output_size_bytes',
            'run_counts',
        )
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline-binary', type=Path, required=True)
    parser.add_argument('--candidate-binary', type=Path, required=True)
    parser.add_argument('--pipeline', choices=('gb', 'sk'), required=True)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--report', type=Path)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        report = compare_releases(
            baseline_binary=args.baseline_binary,
            candidate_binary=args.candidate_binary,
            pipeline=args.pipeline,
            input_path=args.input,
            output_dir=args.output_dir,
        )
        rendered = json.dumps(report, ensure_ascii=False, indent=2)
        if args.report is not None:
            with args.report.open('x', encoding='utf-8', newline='\n') as stream:
                stream.write(f'{rendered}\n')
        print(rendered)
        return 0 if report['status'] == 'passed' else 1
    except (FileExistsError, FileNotFoundError, json.JSONDecodeError, OSError, RuntimeError) as error:
        print(
            json.dumps(
                {
                    'status': 'failed',
                    'error_code': 'VALIDATION_ERROR',
                    'message': str(error),
                    'retryable': False,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 2


if __name__ == '__main__':
    raise SystemExit(main())

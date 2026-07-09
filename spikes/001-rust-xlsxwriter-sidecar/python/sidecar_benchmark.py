from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from statistics import median
from typing import Any

PYTHON_DIR = Path(__file__).resolve().parent
REPO_ROOT = PYTHON_DIR.parents[2]
for import_path in (PYTHON_DIR, REPO_ROOT):
    import_path_text = str(import_path)
    if import_path_text not in sys.path:
        sys.path.insert(0, import_path_text)

VALIDATED_SIDECAR_SECONDS = 5.0
RELATIVE_SIDECAR_RATIO = 0.60
FAST_RUST_WRITER_SECONDS = 2.0
PARTIAL_MAX_SIDECAR_SECONDS = 7.0
SPIKE_DIR = PYTHON_DIR.parent
RUST_CRATE_DIR = SPIKE_DIR / 'rust-writer'


def classify_verdict(
    *,
    median_python_3sheet_export_seconds: float,
    median_intermediate_export_seconds: float,
    median_rust_export_seconds: float,
) -> str:
    sidecar_export_seconds = median_intermediate_export_seconds + median_rust_export_seconds
    relative_target = median_python_3sheet_export_seconds * RELATIVE_SIDECAR_RATIO

    if sidecar_export_seconds <= VALIDATED_SIDECAR_SECONDS and sidecar_export_seconds <= relative_target:
        return 'VALIDATED'

    rust_writer_is_fast = (
        median_rust_export_seconds <= FAST_RUST_WRITER_SECONDS
        and median_rust_export_seconds < median_intermediate_export_seconds
    )
    if rust_writer_is_fast:
        return 'PARTIAL_PROTOCOL_BOTTLENECK'

    if sidecar_export_seconds <= PARTIAL_MAX_SIDECAR_SECONDS:
        return 'PARTIAL'

    return 'INVALIDATED'


def run_benchmark(
    *,
    pipeline_name: str,
    input_path: Path | None,
    tmp_dir: Path,
    repeats: int = 3,
    month_start: str | None = None,
    month_end: str | None = None,
    cargo_manifest_path: Path = RUST_CRATE_DIR / 'Cargo.toml',
) -> dict[str, Any]:
    if repeats < 1:
        raise ValueError('repeats must be >= 1')

    from sidecar_validation import validate_workbooks

    resolved_input_path = resolve_input_path(pipeline_name, input_path)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    payload_started_at = time.perf_counter()
    payload = build_payload_for_input(
        pipeline_name=pipeline_name,
        input_path=resolved_input_path,
        month_start=month_start,
        month_end=month_end,
    )
    payload_build_seconds = time.perf_counter() - payload_started_at
    sheet_names = [model.sheet_name for model in payload.sheet_models]
    if sheet_names != list(export_default_sheet_names()):
        raise ValueError(f'Phase 1 benchmark requires default 3-sheet payload, got: {sheet_names}')

    rust_executable = build_rust_release_binary(cargo_manifest_path)
    python_runs = _run_python_exports(payload.sheet_models, tmp_dir=tmp_dir, repeats=repeats)
    sidecar_runs = _run_sidecar_exports(
        payload.sheet_models,
        tmp_dir=tmp_dir,
        repeats=repeats,
        rust_executable=rust_executable,
    )
    validation_reports = [
        validate_workbooks(
            python_runs[0]['output_path'],
            sidecar_run['output_path'],
            sidecar_run['manifest'],
        )
        for sidecar_run in sidecar_runs
    ]

    median_python = float(median(run['export_seconds'] for run in python_runs))
    median_intermediate = float(median(run['intermediate_export_seconds'] for run in sidecar_runs))
    median_rust = float(median(run['rust_export_seconds'] for run in sidecar_runs))
    sidecar_export_seconds = median_intermediate + median_rust
    median_python_total_seconds = payload_build_seconds + median_python
    sidecar_total_seconds = payload_build_seconds + sidecar_export_seconds
    validation_passed = all(report['passed'] for report in validation_reports)
    verdict = (
        classify_verdict(
            median_python_3sheet_export_seconds=median_python,
            median_intermediate_export_seconds=median_intermediate,
            median_rust_export_seconds=median_rust,
        )
        if validation_passed
        else 'VALIDATION_FAILED'
    )

    return {
        'pipeline': pipeline_name,
        'input_path': str(resolved_input_path),
        'repeats': repeats,
        'sheet_names': sheet_names,
        'python_runs': _json_ready_runs(python_runs),
        'sidecar_runs': _json_ready_runs(sidecar_runs),
        'validation_reports': validation_reports,
        'payload_build_seconds': payload_build_seconds,
        'median_python_3sheet_export_seconds': median_python,
        'median_python_3sheet_total_seconds': median_python_total_seconds,
        'median_intermediate_export_seconds': median_intermediate,
        'median_rust_export_seconds': median_rust,
        'sidecar_export_seconds': sidecar_export_seconds,
        'sidecar_total_seconds': sidecar_total_seconds,
        'sidecar_export_speedup': median_python / sidecar_export_seconds if sidecar_export_seconds else None,
        'total_speedup': median_python_total_seconds / sidecar_total_seconds if sidecar_total_seconds else None,
        'csv_export_methods': _csv_export_methods(sidecar_runs),
        'validation_passed': validation_passed,
        'verdict': verdict,
    }


def export_default_sheet_names() -> tuple[str, str, str]:
    return ('成本计算单总表', '成本计算单数量聚合维度', '成本分析工单维度')


def resolve_input_path(pipeline_name: str, input_path: Path | None) -> Path:
    from src.config.pipelines import PIPELINES
    from src.etl.runner import find_input_files

    if input_path is not None:
        if not input_path.exists():
            raise FileNotFoundError(f'input workbook not found: {input_path}')
        return input_path
    config = PIPELINES[pipeline_name]
    input_files = find_input_files(config)
    if not input_files:
        raise FileNotFoundError(f'no input workbook found for pipeline={pipeline_name} under {config.raw_dir}')
    return input_files[0]


def build_payload_for_input(
    *,
    pipeline_name: str,
    input_path: Path,
    month_start: str | None = None,
    month_end: str | None = None,
):
    from src.config.pipelines import PIPELINES
    from src.config.product_whitelist_store import ProductWhitelistConfigError, load_product_order_for_pipeline
    from src.etl.costing_etl import CostingWorkbookETL
    from src.etl.month_filter import build_month_range

    config = PIPELINES[pipeline_name]
    month_range = build_month_range(month_start, month_end)
    try:
        product_order = load_product_order_for_pipeline(config.name)
    except ProductWhitelistConfigError:
        product_order = config.product_order
    etl = CostingWorkbookETL(
        skip_rows=2,
        product_order=product_order,
        standalone_cost_items=config.standalone_cost_items,
        product_anomaly_scope_mode=config.product_anomaly_scope_mode,
        month_range=month_range,
        ensure_output_directories=False,
    )
    return etl.pipeline.build_workbook_payload(
        input_path,
        standalone_cost_items=etl.standalone_cost_items,
        product_anomaly_scope_mode=etl.product_anomaly_scope_mode,
        month_range=etl.month_range,
        presentation_product_order=etl.product_order,
        artifacts_transform=etl._filter_analysis_artifacts_by_whitelist,
    )


def build_rust_release_binary(cargo_manifest_path: Path) -> Path:
    completed = subprocess.run(  # noqa: S603
        ['cargo', 'build', '--release', '--manifest-path', str(cargo_manifest_path)],  # noqa: S607
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(f'cargo build failed:\n{completed.stderr}')
    binary_name = 'costing-rust-xlsxwriter-sidecar.exe' if os.name == 'nt' else 'costing-rust-xlsxwriter-sidecar'
    return cargo_manifest_path.parent / 'target' / 'release' / binary_name


def _run_python_exports(sheet_models, *, tmp_dir: Path, repeats: int) -> list[dict[str, Any]]:
    from src.excel.workbook_writer import CostingWorkbookWriter

    writer = CostingWorkbookWriter()
    runs: list[dict[str, Any]] = []
    for run_index in range(1, repeats + 1):
        output_path = tmp_dir / f'python-{run_index:02d}.xlsx'
        output_path.unlink(missing_ok=True)
        started_at = time.perf_counter()
        writer.write_workbook_from_models(output_path, sheet_models=sheet_models)
        export_seconds = time.perf_counter() - started_at
        runs.append(
            {
                'run_index': run_index,
                'output_path': output_path,
                'export_seconds': export_seconds,
                'output_size_bytes': output_path.stat().st_size,
            }
        )
    return runs


def _run_sidecar_exports(
    sheet_models,
    *,
    tmp_dir: Path,
    repeats: int,
    rust_executable: Path,
) -> list[dict[str, Any]]:
    from sidecar_payload import export_sheet_models_to_payload

    runs: list[dict[str, Any]] = []
    for run_index in range(1, repeats + 1):
        run_dir = tmp_dir / f'sidecar-{run_index:02d}'
        run_dir.mkdir(parents=True, exist_ok=True)
        output_path = run_dir / 'rust.xlsx'
        output_path.unlink(missing_ok=True)
        payload_result = export_sheet_models_to_payload(sheet_models, run_dir, output_path=output_path)

        started_at = time.perf_counter()
        completed = subprocess.run(  # noqa: S603
            [
                str(rust_executable),
                '--manifest',
                str(payload_result.manifest_path),
                '--output',
                str(output_path),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        rust_export_seconds = time.perf_counter() - started_at
        if completed.returncode != 0:
            raise RuntimeError(f'rust writer failed:\n{completed.stderr}')

        runs.append(
            {
                'run_index': run_index,
                'output_path': output_path,
                'manifest_path': payload_result.manifest_path,
                'manifest': payload_result.manifest,
                'intermediate_export_seconds': payload_result.intermediate_export_seconds,
                'rust_export_seconds': rust_export_seconds,
                'sidecar_export_seconds': payload_result.intermediate_export_seconds + rust_export_seconds,
                'output_size_bytes': output_path.stat().st_size,
            }
        )
    return runs


def _json_ready_runs(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    json_ready: list[dict[str, Any]] = []
    for run in runs:
        json_ready.append(
            {key: str(value) if isinstance(value, Path) else value for key, value in run.items() if key != 'manifest'}
        )
    return json_ready


def _csv_export_methods(sidecar_runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not sidecar_runs:
        return []
    return [
        {
            'sheet_name': sheet['sheet_name'],
            'csv_export_method': sheet['csv_export_method'],
            'intermediate_export_seconds': sheet['intermediate_export_seconds'],
            'row_count': sheet['row_count'],
            'column_count': sheet['column_count'],
        }
        for sheet in sidecar_runs[-1]['manifest']['sheets']
    ]


def dump_summary_json(summary: dict[str, Any], path: Path | None = None) -> str:
    text = json.dumps(summary, ensure_ascii=False, indent=2)
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + '\n', encoding='utf-8')
    return text

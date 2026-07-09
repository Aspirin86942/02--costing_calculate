from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from src.services.costing_service import CostingRunRequest, ServiceStatus, run_costing_request


def run_python_oracle(pipeline: str, input_path: Path, output_path: Path) -> None:
    request = CostingRunRequest(
        pipeline=pipeline,
        input_path=input_path,
        output_dir=output_path.parent,
        benchmark=True,
        overwrite_confirmed=True,
    )
    result = run_costing_request(request)
    if result.status != ServiceStatus.SUCCEEDED:
        raise AssertionError(f'python oracle failed: {result.message} {result.technical_detail}')
    if result.workbook_path is None or not result.workbook_path.exists():
        raise AssertionError('python oracle did not create workbook')
    result.workbook_path.replace(output_path)


def run_rust_cli(pipeline: str, input_path: Path, output_path: Path) -> None:
    cargo = shutil.which('cargo')
    if cargo is None:
        raise AssertionError('cargo executable not found')

    completed = subprocess.run(  # noqa: S603 - test harness invokes local Cargo with fixed arguments.
        [
            cargo,
            'run',
            '--quiet',
            '--manifest-path',
            'rust/Cargo.toml',
            '-p',
            'costing-calculate',
            '--',
            pipeline,
            '--input',
            str(input_path),
            '--output',
            str(output_path),
            '--benchmark',
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise AssertionError(f'rust cli failed\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}')
    if not output_path.exists():
        raise AssertionError(f'rust cli did not create expected workbook: {output_path}')

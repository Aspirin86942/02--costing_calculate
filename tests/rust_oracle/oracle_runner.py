from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from src.services.costing_service import CostingRunRequest, ServiceStatus, run_costing_request
from tests.rust_oracle.repo_paths import repo_root


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
    run_rust_cli_release(build_rust_cli_release(), pipeline, input_path, output_path)


def build_rust_cli_release() -> Path:
    cargo = shutil.which('cargo')
    if cargo is None:
        raise AssertionError('cargo executable not found')
    root = repo_root()

    completed = subprocess.run(  # noqa: S603 - test harness invokes local Cargo with fixed arguments.
        [
            cargo,
            'build',
            '--quiet',
            '--release',
            '--manifest-path',
            str(root / 'rust' / 'Cargo.toml'),
            '-p',
            'costing-calculate',
        ],
        check=False,
        capture_output=True,
        cwd=root,
        text=True,
    )
    if completed.returncode != 0:
        raise AssertionError(f'rust release build failed\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}')

    executable_name = 'costing-calculate.exe' if os.name == 'nt' else 'costing-calculate'
    executable = root / 'rust' / 'target' / 'release' / executable_name
    if not executable.exists():
        raise AssertionError(f'rust release executable missing: {executable}')
    return executable


def run_rust_cli_release(executable: Path, pipeline: str, input_path: Path, output_path: Path) -> None:
    completed = subprocess.run(  # noqa: S603 - test harness invokes local release executable with fixed arguments.
        [
            str(executable),
            pipeline,
            '--input',
            str(input_path),
            '--output',
            str(output_path),
            '--benchmark',
        ],
        check=False,
        capture_output=True,
        cwd=repo_root(),
        text=True,
    )
    if completed.returncode != 0:
        raise AssertionError(f'rust release cli failed\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}')
    if not output_path.exists():
        raise AssertionError(f'rust release cli did not create expected workbook: {output_path}')

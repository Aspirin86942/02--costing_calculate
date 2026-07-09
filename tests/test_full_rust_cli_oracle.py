from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.rust_oracle.oracle_runner import assert_runtime_contract_matches, run_python_oracle, run_rust_cli
from tests.rust_oracle.repo_paths import repo_root
from tests.rust_oracle.workbook_compare import compare_workbooks


def _sample_from_env(env_name: str) -> Path | None:
    value = os.environ.get(env_name)
    if not value:
        return None
    path = Path(value)
    return path if path.exists() else None


def _first_sample(env_name: str, patterns: tuple[str, ...]) -> Path | None:
    env_path = _sample_from_env(env_name)
    if env_path is not None:
        return env_path
    root = repo_root()
    for pattern in patterns:
        matches = sorted(root.glob(pattern))
        if matches:
            return matches[0]
    return None


@pytest.mark.skipif(_first_sample('COSTING_GB_SAMPLE', ('data/raw/gb/*.xlsx',)) is None, reason='GB raw sample missing')
def test_rust_gb_workbook_matches_python_oracle(tmp_path: Path) -> None:
    input_path = _first_sample('COSTING_GB_SAMPLE', ('data/raw/gb/*.xlsx',))
    assert input_path is not None
    python_output = tmp_path / 'python-gb.xlsx'
    rust_output = tmp_path / 'rust-gb.xlsx'

    python_summary = run_python_oracle('gb', input_path, python_output)
    rust_summary = run_rust_cli('gb', input_path, rust_output)

    report = compare_workbooks(python_output, rust_output)
    assert report['passed'], report['errors']
    assert_runtime_contract_matches(python_summary, rust_summary)


@pytest.mark.skipif(_first_sample('COSTING_SK_SAMPLE', ('data/raw/sk/*.xlsx',)) is None, reason='SK raw sample missing')
def test_rust_sk_workbook_matches_python_oracle(tmp_path: Path) -> None:
    input_path = _first_sample('COSTING_SK_SAMPLE', ('data/raw/sk/*.xlsx',))
    assert input_path is not None
    python_output = tmp_path / 'python-sk.xlsx'
    rust_output = tmp_path / 'rust-sk.xlsx'

    python_summary = run_python_oracle('sk', input_path, python_output)
    rust_summary = run_rust_cli('sk', input_path, rust_output)

    report = compare_workbooks(python_output, rust_output)
    assert report['passed'], report['errors']
    assert_runtime_contract_matches(python_summary, rust_summary)

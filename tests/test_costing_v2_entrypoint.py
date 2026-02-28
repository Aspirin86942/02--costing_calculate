"""Entrypoint compatibility tests for costing_v2."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / 'src' / 'etl' / 'costing_v2.py'


def _run_inline_python(code: str) -> subprocess.CompletedProcess[str]:
    """Run inline Python in a subprocess to isolate import side effects."""
    # 测试输入是当前文件内构造的固定代码片段，不涉及外部用户输入。
    return subprocess.run(  # noqa: S603
        [sys.executable, '-c', code],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding='utf-8',
        check=False,
    )


def test_script_mode_import_succeeds_without_project_root_in_sys_path() -> None:
    """脚本模式下，即使缺少项目根目录也能成功导入模块。"""
    code = f"""
import importlib.util
import pathlib
import sys

project_root = pathlib.Path(r'{PROJECT_ROOT}')
script_path = pathlib.Path(r'{SCRIPT_PATH}')
sys.path = [p for p in sys.path if pathlib.Path(p).resolve() != project_root.resolve()]

spec = importlib.util.spec_from_file_location('costing_v2_script_mode', script_path)
module = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(module)
print(module.CostingETL.__name__)
"""
    result = _run_inline_python(code)

    assert result.returncode == 0, result.stderr
    assert 'CostingETL' in result.stdout


def test_script_mode_bootstrap_only_inserts_project_root_once() -> None:
    """脚本模式路径兜底只应插入一次项目根目录，避免重复污染 sys.path。"""
    code = f"""
import importlib.util
import pathlib
import sys

project_root = pathlib.Path(r'{PROJECT_ROOT}').resolve()
script_path = pathlib.Path(r'{SCRIPT_PATH}')
sys.path = [p for p in sys.path if pathlib.Path(p).resolve() != project_root]

spec = importlib.util.spec_from_file_location('costing_v2_script_mode_2', script_path)
module = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(module)

inserted_count = sum(1 for p in sys.path if pathlib.Path(p).resolve() == project_root)
print(inserted_count)
"""
    result = _run_inline_python(code)

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().endswith('1')

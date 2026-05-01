import ast
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / 'src'


def _collect_imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding='utf-8'))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    return imports


def _module_paths(relative_root: str) -> list[Path]:
    root = SRC_ROOT / relative_root
    if not root.exists():
        return []
    return [path for path in root.rglob('*.py') if path.name != '__init__.py']


def _project_python_files() -> list[Path]:
    return [
        path
        for root in (PROJECT_ROOT / 'src', PROJECT_ROOT / 'tests')
        for path in root.rglob('*.py')
        if path.name != '__init__.py'
    ]


def test_analytics_modules_do_not_import_etl_or_excel() -> None:
    for module_path in _module_paths('analytics'):
        imports = _collect_imports(module_path)
        assert not any(name.startswith('src.etl') for name in imports), module_path
        assert not any(name.startswith('src.excel') for name in imports), module_path


def test_excel_modules_do_not_import_etl() -> None:
    for module_path in _module_paths('excel'):
        imports = _collect_imports(module_path)
        assert not any(name.startswith('src.etl') for name in imports), module_path


def test_etl_stage_modules_do_not_import_excel() -> None:
    for module_path in _module_paths('etl/stages'):
        imports = _collect_imports(module_path)
        assert not any(name.startswith('src.excel') for name in imports), module_path


def test_only_etl_entrypoint_may_import_excel() -> None:
    etl_root = SRC_ROOT / 'etl'
    for module_path in etl_root.glob('*.py'):
        if module_path.name in {'__init__.py', 'costing_etl.py'}:
            continue
        imports = _collect_imports(module_path)
        assert not any(name.startswith('src.excel') for name in imports), module_path


def test_project_no_longer_imports_legacy_pq_analysis_shim() -> None:
    for module_path in _project_python_files():
        imports = _collect_imports(module_path)
        assert 'src.analytics.pq_analysis' not in imports, module_path


def test_project_no_longer_imports_legacy_sheet_writer() -> None:
    for module_path in _project_python_files():
        imports = _collect_imports(module_path)
        assert 'src.excel.sheet_writers' not in imports, module_path

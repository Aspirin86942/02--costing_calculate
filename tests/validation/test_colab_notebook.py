from __future__ import annotations

import ast
import hashlib
import io
import json
import stat
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

import pytest

NOTEBOOK = Path(__file__).resolve().parents[2] / 'tools/validation/colab_autotune.ipynb'


@pytest.fixture(scope='module')
def notebook() -> dict[str, Any]:
    return json.loads(NOTEBOOK.read_text(encoding='utf-8'))


def _source(notebook: dict[str, Any], cell_id: str) -> str:
    cell = next(cell for cell in notebook['cells'] if cell['id'] == cell_id)
    source = cell['source']
    return ''.join(source) if isinstance(source, list) else source


def _script(notebook: dict[str, Any], cell_id: str, variable: str) -> str:
    tree = ast.parse(_source(notebook, cell_id))
    assignment = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == variable for target in node.targets)
    )
    value = ast.literal_eval(assignment.value)
    assert isinstance(value, str)
    return value


def _execute(source: str, namespace: dict[str, Any] | None = None) -> dict[str, Any]:
    if namespace is None:
        namespace = {'__name__': 'notebook_unit_test'}
    # 只执行仓库中指定单元的标准库定义；安装、Colab 和网络调用分别由 mock 替代。
    exec(compile(source, str(NOTEBOOK), 'exec'), namespace)  # noqa: S102
    return namespace


@pytest.fixture
def extract_namespace(notebook: dict[str, Any]) -> dict[str, Any]:
    return _execute(_script(notebook, 'extract-helpers', 'SAFE_EXTRACT_SCRIPT'))


def _bundle_files() -> dict[str, bytes]:
    patch = b'--- a/rust/example.rs\n+++ b/rust/example.rs\n'
    files = {
        'source/pyproject.toml': b'[project]\nrequires-python = ">=3.11"\n',
        'source/uv.lock': b'version = 1\nrevision = 3\n',
        'source/rust-toolchain.toml': b'[toolchain]\nchannel = "1.96.0"\n',
        'source/rust/Cargo.toml': b'[workspace]\n',
        'source/rust/Cargo.lock': b'version = 4\n',
        'source/tools/validation/autotune_experiment.py': b'# synthetic test source\n',
        'patches/example.patch': patch,
    }
    inventory = {
        name: {'sha256': hashlib.sha256(data).hexdigest(), 'size_bytes': len(data)} for name, data in files.items()
    }
    manifest = {
        'schema_version': 1,
        'files': inventory,
        'source_inventory_sha256': hashlib.sha256(
            json.dumps(inventory, sort_keys=True, separators=(',', ':')).encode()
        ).hexdigest(),
        'candidates': [{'id': 'example', 'patch': 'patches/example.patch'}],
    }
    files['bundle-manifest.json'] = json.dumps(manifest).encode()
    return files


def _archive(tmp_path: Path, extra: list[tuple[str | ZipInfo, bytes]] | None = None) -> tuple[Path, str, Path]:
    archive_path = tmp_path / 'source.zip'
    with ZipFile(archive_path, 'x', compression=ZIP_DEFLATED) as archive:
        for name, content in _bundle_files().items():
            archive.writestr(name, content)
        for name, content in extra or []:
            archive.writestr(name, content)
    return archive_path, hashlib.sha256(archive_path.read_bytes()).hexdigest(), tmp_path / 'bundle'


def test_notebook_format_metadata_and_empty_outputs(notebook: dict[str, Any]) -> None:
    assert notebook['nbformat'] == 4
    assert notebook['nbformat_minor'] == 5
    assert notebook['metadata']['kernelspec'] == {
        'display_name': 'Python 3',
        'language': 'python',
        'name': 'python3',
    }
    assert notebook['metadata']['language_info']['name'] == 'python'
    ids = [cell['id'] for cell in notebook['cells']]
    assert len(ids) == len(set(ids))
    for cell in notebook['cells']:
        assert cell['cell_type'] in {'code', 'markdown'}
        if cell['cell_type'] == 'code':
            assert cell['execution_count'] is None
            assert cell['outputs'] == []
    prose = '\n'.join(_source(notebook, cell['id']) for cell in notebook['cells'] if cell['cell_type'] == 'markdown')
    for instruction in ('免费 CPU', '默认业务配置', '禁止上传真实 ERP', 'SHA-256', '--resume', '重新校准'):
        assert instruction in prose
    assert 'driver 不接受这个参数' in prose
    assert '不自动挂载或写入 Google Drive' in prose
    assert '不上传 GitHub' in prose


def test_every_code_cell_and_embedded_script_compiles(notebook: dict[str, Any]) -> None:
    for cell in notebook['cells']:
        if cell['cell_type'] == 'code':
            compile(_source(notebook, cell['id']), f'{NOTEBOOK}:{cell["id"]}', 'exec')
    for cell_id, variable in (
        ('extract-helpers', 'SAFE_EXTRACT_SCRIPT'),
        ('rustup-helpers', 'RUSTUP_DOWNLOAD_SCRIPT'),
    ):
        compile(_script(notebook, cell_id, variable), f'{NOTEBOOK}:{variable}', 'exec')


def test_safe_extract_normal_bundle(extract_namespace: dict[str, Any], tmp_path: Path) -> None:
    archive, expected, destination = _archive(tmp_path, [('source/empty/', b'')])
    result = extract_namespace['safe_extract_bundle'](archive, expected, destination)
    assert result == {'bundle_root': str(destination), 'archive_sha256': expected, 'file_count': len(_bundle_files())}
    for name, content in _bundle_files().items():
        assert (destination / name).read_bytes() == content
    assert (destination / 'source/empty').is_dir()
    assert not any(path.is_symlink() for path in destination.rglob('*'))


@pytest.mark.parametrize(
    'name',
    [
        '../escape.txt',
        '/absolute.txt',
        'source/../../escape.txt',
        'C:/escape.txt',
        'C:\\escape.txt',
        '\\\\server\\share',
        'source/./file.txt',
        'source//file.txt',
        'source\\file.txt',
    ],
)
def test_safe_extract_rejects_unsafe_paths_before_writing(
    extract_namespace: dict[str, Any], tmp_path: Path, name: str
) -> None:
    archive, expected, destination = _archive(tmp_path, [(name, b'untrusted')])
    with pytest.raises(ValueError, match='不安全路径'):
        extract_namespace['safe_extract_bundle'](archive, expected, destination)
    assert not destination.exists()
    assert not (tmp_path / 'escape.txt').exists()


@pytest.mark.parametrize('expected', ['0' * 64, 'not-a-sha256', 'a' * 63, 'g' * 64])
def test_safe_extract_rejects_hash_mismatch(extract_namespace: dict[str, Any], tmp_path: Path, expected: str) -> None:
    archive, _, destination = _archive(tmp_path)
    with pytest.raises(ValueError, match='SHA-256'):
        extract_namespace['safe_extract_bundle'](archive, expected, destination)
    assert not destination.exists()


def test_safe_extract_rejects_duplicate_entries(extract_namespace: dict[str, Any], tmp_path: Path) -> None:
    with pytest.warns(UserWarning, match='Duplicate name'):
        archive, expected, destination = _archive(tmp_path, [('source/uv.lock', b'duplicate')])
    with pytest.raises(ValueError, match='重复项'):
        extract_namespace['safe_extract_bundle'](archive, expected, destination)
    assert not destination.exists()


@pytest.mark.parametrize('kind', [stat.S_IFLNK, stat.S_IFIFO, stat.S_IFSOCK])
def test_safe_extract_rejects_links_and_special_files(
    extract_namespace: dict[str, Any], tmp_path: Path, kind: int
) -> None:
    entry = ZipInfo('source/indirect')
    entry.create_system = 3
    entry.external_attr = (kind | 0o777) << 16
    archive, expected, destination = _archive(tmp_path, [(entry, b'../../outside')])
    with pytest.raises(ValueError, match='符号链接或特殊文件'):
        extract_namespace['safe_extract_bundle'](archive, expected, destination)
    assert not destination.exists()


@pytest.mark.parametrize(
    ('limit', 'value', 'message'),
    [
        ('MAX_ARCHIVE_BYTES', 1, '压缩文件超过'),
        ('MAX_FILE_BYTES', 1, '解压大小'),
        ('MAX_TOTAL_BYTES', 1, '解压大小'),
        ('MAX_MEMBERS', 1, '条目数量'),
        ('MAX_RATIO', 1, '膨胀比例'),
    ],
)
def test_safe_extract_rejects_size_expansion(
    extract_namespace: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    limit: str,
    value: int,
    message: str,
) -> None:
    archive, expected, destination = _archive(tmp_path, [('source/compressible.txt', b'a' * 8192)])
    monkeypatch.setitem(extract_namespace, limit, value)
    with pytest.raises(ValueError, match=message):
        extract_namespace['safe_extract_bundle'](archive, expected, destination)
    assert not destination.exists()


def test_safe_extract_rejects_file_directory_conflict(extract_namespace: dict[str, Any], tmp_path: Path) -> None:
    archive, expected, destination = _archive(tmp_path, [('source', b'file')])
    with pytest.raises(ValueError, match='路径冲突'):
        extract_namespace['safe_extract_bundle'](archive, expected, destination)
    assert not destination.exists()


def test_safe_extract_preserves_existing_destination(extract_namespace: dict[str, Any], tmp_path: Path) -> None:
    archive, expected, destination = _archive(tmp_path)
    destination.mkdir()
    sentinel = destination / 'keep.txt'
    sentinel.write_text('keep', encoding='utf-8')
    with pytest.raises(ValueError, match='新目录'):
        extract_namespace['safe_extract_bundle'](archive, expected, destination)
    assert sentinel.read_text(encoding='utf-8') == 'keep'


def test_isolated_extractor_ignores_downloaded_import_shadow(notebook: dict[str, Any], tmp_path: Path) -> None:
    archive, expected, destination = _archive(tmp_path)
    (tmp_path / 'json.py').write_text('raise RuntimeError("downloaded json.py was imported")\n', encoding='utf-8')
    result = subprocess.run(  # noqa: S603 - runs only the notebook's trusted standard-library helper in isolation.
        [
            sys.executable,
            '-I',
            '-c',
            _script(notebook, 'extract-helpers', 'SAFE_EXTRACT_SCRIPT'),
            str(archive),
            expected,
            str(destination),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)['archive_sha256'] == expected
    failed = subprocess.run(  # noqa: S603 - exercises the same isolated helper's CLI rejection path.
        [
            sys.executable,
            '-I',
            '-c',
            _script(notebook, 'extract-helpers', 'SAFE_EXTRACT_SCRIPT'),
            str(archive),
            '0' * 64,
            str(tmp_path / 'rejected'),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert failed.returncode == 2
    failure = json.loads(failed.stdout)
    assert failure['error_code'] == 'BUNDLE_REJECTED'
    assert failure['retryable'] is False
    assert not (tmp_path / 'rejected').exists()


def test_environment_is_local_explicit_and_keeps_bundle_clean(
    notebook: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name in ('UV_PROJECT_ENVIRONMENT', 'CARGO_HOME', 'RUSTUP_HOME', 'RUSTUP_DIST_SERVER', 'PYTHONPATH'):
        monkeypatch.setenv(name, '/outside/untrusted')
    namespace = _execute(_source(notebook, 'commands'))
    env = namespace['run_environment'](tmp_path)
    for name in (
        'CARGO_HOME',
        'RUSTUP_HOME',
        'UV_PROJECT_ENVIRONMENT',
        'UV_PYTHON_INSTALL_DIR',
        'UV_CACHE_DIR',
        'TMPDIR',
    ):
        assert Path(env[name]).is_relative_to(tmp_path)
    assert env['PATH'] == f'{tmp_path}/cargo/bin:{tmp_path}/bootstrap/bin:/usr/local/bin:/usr/bin:/bin'
    assert env['PYTHONDONTWRITEBYTECODE'] == '1'
    assert env['PYTHONNOUSERSITE'] == '1'
    assert env['RUSTUP_TOOLCHAIN'] == '1.96.0'
    assert 'PYTHONPATH' not in env
    assert 'RUSTUP_DIST_SERVER' not in env


def test_installation_and_driver_commands_without_colab_or_network(notebook: dict[str, Any], tmp_path: Path) -> None:
    namespace = _execute(_source(notebook, 'commands'))
    bundle, work, control = tmp_path / 'bundle', tmp_path / 'experiment', tmp_path / 'control'
    source = bundle / 'source'
    env = namespace['run_environment'](tmp_path)
    commands = namespace['command_plan'](tmp_path, bundle, work)
    run = Mock()
    namespace.update(
        SESSION_ROOT=tmp_path,
        BUNDLE_ROOT=bundle,
        SOURCE_ROOT=source,
        WORK_DIR=work,
        CONTROL_DIR=control,
        RUN_ENV=env,
        COMMANDS=commands,
        RUSTUP_DOWNLOAD_SCRIPT=_script(notebook, 'rustup-helpers', 'RUSTUP_DOWNLOAD_SCRIPT'),
        subprocess=SimpleNamespace(run=run),
    )
    for cell_id in ('install-python', 'install-rust', 'run-driver'):
        _execute(_source(notebook, cell_id), namespace)
    assert commands['install-uv'][-1] == 'uv==0.13.6'
    assert commands['install-uv'][1:5] == ['-I', '-m', 'pip', '--isolated']
    assert commands['install-python'] == [
        str(tmp_path / 'bootstrap/bin/uv'),
        'python',
        'install',
        '--no-bin',
        '3.13.18',
    ]
    assert commands['sync'] == [
        str(tmp_path / 'bootstrap/bin/uv'),
        'sync',
        '--frozen',
        '--extra',
        'dev',
        '--python',
        '3.13.18',
        '--managed-python',
    ]
    assert commands['driver'] == [
        str(tmp_path / 'venv/bin/python'),
        '-m',
        'tools.validation.autotune_experiment',
        '--bundle-root',
        str(bundle),
        '--work-dir',
        str(work),
        '--rust-tests',
        '--pairs',
        '8',
        '--calibration-pairs',
        '4',
        '--budget-seconds',
        '21600',
    ]
    assert '--no-modify-path' in commands['install-rustup']
    assert '1.96.0' in commands['install-rust']
    assert str(tmp_path / 'downloads/rustup-init') == commands['install-rustup'][0]
    assert not work.exists()
    calls = run.call_args_list
    assert len(calls) == 11
    for call in calls:
        assert call.kwargs['env'] is env
        assert call.kwargs['check'] is True
        assert not call.kwargs.get('shell', False)
    for step in ('sync', 'install-rustup', 'install-rust', 'active-toolchain', 'cargo-version', 'driver'):
        call = next(call for call in calls if call.args[0] == commands[step])
        assert call.kwargs['cwd'] == source
    download = next(call for call in calls if '-c' in call.args[0])
    assert download.args[0][1:3] == ['-I', '-c']
    assert download.kwargs['cwd'] == control
    work.mkdir()
    run.reset_mock()
    with pytest.raises(RuntimeError, match='新的工作目录'):
        _execute(_source(notebook, 'run-driver'), namespace)
    run.assert_not_called()


def test_extract_cell_uses_isolated_python_and_control_directory(notebook: dict[str, Any], tmp_path: Path) -> None:
    incoming = tmp_path / 'incoming'
    incoming.mkdir()
    archive, expected, _ = _archive(incoming)
    inputs = iter((str(archive), expected))
    run = Mock()
    namespace = _execute(_source(notebook, 'commands'))
    namespace.update(
        INCOMING_DIR=incoming,
        BUNDLE_ROOT=tmp_path / 'bundle',
        CONTROL_DIR=tmp_path / 'control',
        RUN_ENV=namespace['run_environment'](tmp_path),
        SAFE_EXTRACT_SCRIPT=_script(notebook, 'extract-helpers', 'SAFE_EXTRACT_SCRIPT'),
        input=lambda _: next(inputs),
        subprocess=SimpleNamespace(run=run),
    )
    _execute(_source(notebook, 'extract'), namespace)
    assert run.call_args.args[0][1:3] == ['-I', '-c']
    assert run.call_args.args[0][-3:] == [str(archive), expected, str(tmp_path / 'bundle')]
    assert run.call_args.kwargs['cwd'] == tmp_path / 'control'
    assert run.call_args.kwargs['check'] is True


@pytest.mark.parametrize('checksum_name', ['  rustup-init\n', ' *./rustup-init\n', '\n'])
@pytest.mark.parametrize('correct_checksum', [True, False])
def test_rustup_official_download_is_verified_before_execution(
    notebook: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    correct_checksum: bool,
    checksum_name: str,
) -> None:
    namespace = _execute(_script(notebook, 'rustup-helpers', 'RUSTUP_DOWNLOAD_SCRIPT'))
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'rust-toolchain.toml').write_text('[toolchain]\nchannel = "1.96.0"\n', encoding='utf-8')
    payload = b'synthetic installer, never executed'
    expected = hashlib.sha256(payload).hexdigest() if correct_checksum else '0' * 64
    urls = []

    def urlopen(url: str, *, timeout: int) -> io.BytesIO:
        assert timeout == 60
        urls.append(url)
        return io.BytesIO(f'{expected}{checksum_name}'.encode() if url.endswith('.sha256') else payload)

    monkeypatch.setattr(namespace['urllib'].request, 'urlopen', urlopen)
    download_dir = tmp_path / 'downloads'
    if correct_checksum:
        installer = namespace['download_rustup'](download_dir, source, '1.28.2')
        assert installer.read_bytes() == payload
        assert installer.stat().st_mode & stat.S_IXUSR
    else:
        with pytest.raises(ValueError, match='SHA-256 校验失败'):
            namespace['download_rustup'](download_dir, source, '1.28.2')
        assert not (download_dir / 'rustup-init').stat().st_mode & 0o111
    official = 'https://static.rust-lang.org/rustup/archive/1.28.2/x86_64-unknown-linux-gnu/rustup-init'
    assert urls == [official + '.sha256', official]


def test_result_archive_contains_only_review_material(notebook: dict[str, Any], tmp_path: Path) -> None:
    bundle, work = tmp_path / 'bundle', tmp_path / 'work'
    for name, data in _bundle_files().items():
        path = bundle / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    outputs = {
        'experiment-report.json': b'{"status": "completed"}',
        'logs/build-example.log': b'synthetic build log',
        'logs/test-example.log': b'synthetic test log',
        'measurements/example/report.json': b'{"status": "valid"}',
        'comparisons/example/comparison.json': b'{"status": "passed"}',
        'fixtures/example/spec.json': b'{"seed": 20261108}',
        'fixtures/input.xlsx': b'must not be exported',
        'builds/example/costing-calculate': b'must not be exported',
        'trees/example/source.rs': b'must not be exported',
    }
    for name, data in outputs.items():
        path = work / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    namespace = _execute(_source(notebook, 'commands'))
    _execute(_source(notebook, 'results-helpers'), namespace)
    output = tmp_path / 'results.zip'
    assert namespace['make_results_archive'](bundle, work, output) == output
    with ZipFile(output) as archive:
        assert set(archive.namelist()) == {
            'bundle-manifest.json',
            'experiment-report.json',
            'patches/example.patch',
            'logs/build-example.log',
            'logs/test-example.log',
            'measurements/example/report.json',
            'comparisons/example/comparison.json',
            'fixtures/example/spec.json',
        }
        assert archive.read('patches/example.patch') == _bundle_files()['patches/example.patch']
    original = output.read_bytes()
    with pytest.raises(FileExistsError):
        namespace['make_results_archive'](bundle, work, output)
    assert output.read_bytes() == original

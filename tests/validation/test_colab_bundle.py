from __future__ import annotations

import hashlib
import json
from pathlib import Path
from zipfile import ZipFile

import pytest

from tools.validation.prepare_colab_bundle import PROJECT_ROOT, ROOT_FILES, TOOL_FILES, BundleError, prepare_bundle


def _hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.fixture
def bundle_inputs(tmp_path: Path) -> dict[str, Path]:
    project = tmp_path / 'project'
    project.mkdir()
    sources = {
        **dict.fromkeys(ROOT_FILES, b'public source\n'),
        **{name: (PROJECT_ROOT / name).read_bytes() for name in TOOL_FILES},
        'rust/Cargo.toml': b'[workspace]\n',
        'rust/Cargo.lock': b'version = 4\n',
        'rust/crates/costing-xlsx/src/writer.rs': b'fn before() {}\n',
        'rust/crates/costing-cli/config/costing.default.toml': b'schema_version = 1\n',
        'tools/validation/sample.py': b'print("source")\n',
        'tools/validation/example.schema.json': b'{}\n',
        'tests/validation/test_sample.py': b'assert True\n',
    }
    for name, content in sources.items():
        target = project / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    for name in (
        'data/raw/secret.xlsx',
        '.claude/settings.json',
        '.git/config',
        'rust/target/debug/program',
        '.pytest-tmp/private.json',
    ):
        target = project / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text('private-content-marker', encoding='utf-8')
    patch = (
        b'--- a/rust/crates/costing-xlsx/src/writer.rs\n'
        b'+++ b/rust/crates/costing-xlsx/src/writer.rs\n'
        b'@@ -1 +1 @@\n-fn before() {}\n+fn after() {}\n'
    )
    (tmp_path / 'candidate.patch').write_bytes(patch)
    commit = '1' * 40
    candidate_manifest = tmp_path / 'candidates.json'
    candidate_manifest.write_text(
        json.dumps(
            {
                'schema_version': 1,
                'base_commit': commit,
                'source_date_epoch': 1704067200,
                'candidates': [
                    {
                        'id': 'buffer',
                        'patch': 'candidate.patch',
                        'patch_sha256': _hash(patch),
                        'changed_paths': ['rust/crates/costing-xlsx/src/writer.rs'],
                        'hypothesis': 'Buffer workbook output',
                        'primary_pipeline': 'gb',
                        'primary_size': 'medium',
                    }
                ],
            }
        ),
        encoding='utf-8',
    )
    baseline_metadata = tmp_path / 'baseline.json'
    baseline_metadata.write_text(
        json.dumps(
            {'base_commit': commit, 'source_files': {name: _hash(content) for name, content in sources.items()}}
        ),
        encoding='utf-8',
    )
    return {
        'project_root': project,
        'candidate_manifest': candidate_manifest,
        'baseline_metadata': baseline_metadata,
        'output_dir': tmp_path / 'bundle',
    }


def test_bundle_is_reproducible_and_allowlist_excludes_runtime_data(bundle_inputs: dict[str, Path]) -> None:
    first = prepare_bundle(**bundle_inputs)
    other_output = bundle_inputs['output_dir'].with_name('bundle-again')
    second = prepare_bundle(**{**bundle_inputs, 'output_dir': other_output})
    assert first['status'] == 'prepared-not-uploaded'
    assert first['archive_sha256'] == second['archive_sha256']
    archive_path = bundle_inputs['output_dir'] / first['archive']
    with ZipFile(archive_path) as archive:
        assert 'source/rust/crates/costing-cli/config/costing.default.toml' in archive.namelist()
        assert not any('private-content-marker' in archive.read(name).decode() for name in archive.namelist())
        manifest = json.loads(archive.read('bundle-manifest.json'))
        for name, identity in manifest['files'].items():
            assert _hash(archive.read(name)) == identity['sha256']
        assert set(archive.namelist()) == {*manifest['files'], 'bundle-manifest.json'}
        assert set(manifest['tool_files']) == set(TOOL_FILES)
        assert set(manifest['runtime_files']) == {
            'rust-toolchain.toml',
            *(name.removeprefix('source/') for name in manifest['files'] if name.startswith('source/rust/')),
        }
        assert len(manifest['protocol_sha256']) == 64
    assert 'embedded business configuration' in first['disclosure']
    with pytest.raises(FileExistsError):
        prepare_bundle(**bundle_inputs)
    assert _hash(archive_path.read_bytes()) == first['archive_sha256']


def test_bundle_retains_vendored_writer_build_inputs_and_licenses(bundle_inputs: dict[str, Path]) -> None:
    sources = {
        'Cargo.toml': b'[package]\nname = "rust_xlsxwriter"\nversion = "0.102.0"\n',
        'src/lib.rs': b'#![doc = include_str!("../CHANGELOG.md")]\n',
        'CHANGELOG.md': b'Upstream changes\n',
        'README.md': b'Writer source\n',
        'PATCHES.md': b'Local I/O error propagation\n',
        'LICENSE_MIT': b'MIT license\n',
        'LICENSE_Apache2.0': b'Apache license\n',
        '.cargo_vcs_info.json': b'{"git":{"sha1":"1111111111111111111111111111111111111111"}}\n',
    }
    baseline = json.loads(bundle_inputs['baseline_metadata'].read_text())
    for name, content in sources.items():
        relative = f'rust/vendor/rust_xlsxwriter/{name}'
        path = bundle_inputs['project_root'] / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        baseline['source_files'][relative] = _hash(content)
    (bundle_inputs['project_root'] / 'rust/vendor/rust_xlsxwriter/private-notes.txt').write_text(
        'private-content-marker'
    )
    bundle_inputs['baseline_metadata'].write_text(json.dumps(baseline), encoding='utf-8')

    review = prepare_bundle(**bundle_inputs)

    with ZipFile(bundle_inputs['output_dir'] / review['archive']) as archive:
        manifest = json.loads(archive.read('bundle-manifest.json'))
        for name, content in sources.items():
            relative = f'rust/vendor/rust_xlsxwriter/{name}'
            assert archive.read(f'source/{relative}') == content
            assert manifest['runtime_files'][relative]['sha256'] == _hash(content)
        assert not any('private-content-marker' in archive.read(name).decode() for name in archive.namelist())


def test_bundle_rejects_changed_runtime_or_patch(bundle_inputs: dict[str, Path]) -> None:
    source = bundle_inputs['project_root'] / 'rust/crates/costing-xlsx/src/writer.rs'
    original = source.read_bytes()
    source.write_bytes(b'changed runtime')
    with pytest.raises(BundleError, match='Runtime source differs'):
        prepare_bundle(**bundle_inputs)
    source.write_bytes(original)
    patch = bundle_inputs['candidate_manifest'].parent / 'candidate.patch'
    patch.write_bytes(patch.read_bytes() + b'changed')
    with pytest.raises(BundleError, match='patch hash differs'):
        prepare_bundle(**bundle_inputs)
    assert not bundle_inputs['output_dir'].exists()


def test_bundle_rejects_path_escape_and_indirect_source(bundle_inputs: dict[str, Path]) -> None:
    manifest_path = bundle_inputs['candidate_manifest']
    manifest = json.loads(manifest_path.read_text())
    manifest['candidates'][0]['patch'] = '../outside.patch'
    manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
    with pytest.raises(BundleError, match='safe relative path'):
        prepare_bundle(**bundle_inputs)
    manifest['candidates'][0]['patch'] = 'candidate.patch'
    manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
    source = bundle_inputs['project_root'] / 'tools/validation/indirect.py'
    try:
        source.symlink_to(manifest_path)
    except OSError:
        pytest.skip('Symlink creation is unavailable on this platform')
    with pytest.raises(BundleError, match='indirect file'):
        prepare_bundle(**bundle_inputs)


def test_bundle_allows_reviewed_cargo_profile_patch(bundle_inputs: dict[str, Path]) -> None:
    manifest_path = bundle_inputs['candidate_manifest']
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    patch = b'--- a/rust/Cargo.toml\n+++ b/rust/Cargo.toml\n@@ -1 +1,2 @@\n [workspace]\n+# profile candidate\n'
    (manifest_path.parent / 'candidate.patch').write_bytes(patch)
    manifest['candidates'][0].update(changed_paths=['rust/Cargo.toml'], patch_sha256=_hash(patch))
    manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
    assert prepare_bundle(**bundle_inputs)['status'] == 'prepared-not-uploaded'


@pytest.mark.parametrize('case', ['old-path', 'too-many', 'reserved-id', 'outside-source'])
def test_bundle_rejects_unreviewed_candidate_scopes(bundle_inputs: dict[str, Path], case: str) -> None:
    manifest_path = bundle_inputs['candidate_manifest']
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    candidate = manifest['candidates'][0]
    if case == 'old-path':
        patch_path = manifest_path.parent / 'candidate.patch'
        patch = patch_path.read_bytes().replace(b'--- a/rust/', b'--- a/../../rust/')
        patch_path.write_bytes(patch)
        candidate['patch_sha256'] = _hash(patch)
    elif case == 'too-many':
        manifest['candidates'] = [{**candidate, 'id': f'candidate-{index}'} for index in range(7)]
    elif case == 'reserved-id':
        candidate['id'] = 'control'
    else:
        candidate['changed_paths'] = ['../outside.rs']
    manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
    with pytest.raises(BundleError):
        prepare_bundle(**bundle_inputs)
    assert not bundle_inputs['output_dir'].exists()


def _add_fork_payload(bundle_inputs: dict[str, Path]) -> dict[str, object]:
    root = bundle_inputs['candidate_manifest'].parent
    manifest = json.loads(bundle_inputs['candidate_manifest'].read_text())
    candidate = manifest['candidates'][0]
    patch = (
        b'--- a/rust/Cargo.toml\n+++ b/rust/Cargo.toml\n@@ -1 +1,3 @@\n [workspace]\n'
        b'+[workspace.dependencies.rust_xlsxwriter]\n+path = "../vendor/rust_xlsxwriter"\n'
    )
    (root / 'candidate.patch').write_bytes(patch)
    candidate.update(
        kind='fork',
        changed_paths=['rust/Cargo.toml'],
        patch_sha256=_hash(patch),
        cargo_changes={'rust/Cargo.toml': [['workspace', 'dependencies', 'rust_xlsxwriter', 'path']]},
        fork_source={'url': 'https://example.test/writer', 'revision': '1' * 40},
        payload=[],
        licenses=['vendor/rust_xlsxwriter/LICENSE'],
    )
    for name, content in {
        'Cargo.toml': b'[package]\nname = "rust_xlsxwriter"\n',
        'LICENSE': b'BSD source license\n',
        'src/new.rs': b'fn new() {}\n',
    }.items():
        path = root / 'payload' / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        candidate['payload'].append(
            {
                'source': f'payload/{name}',
                'target': f'vendor/rust_xlsxwriter/{name}',
                'sha256': _hash(content),
                'size_bytes': len(content),
            }
        )
    (root / 'payload/undeclared.rs').write_text('do not distribute', encoding='utf-8')
    return manifest


def test_bundle_includes_only_declared_fork_payload_and_licenses(bundle_inputs: dict[str, Path]) -> None:
    manifest = _add_fork_payload(bundle_inputs)
    bundle_inputs['candidate_manifest'].write_text(json.dumps(manifest), encoding='utf-8')
    review = prepare_bundle(**bundle_inputs)
    with ZipFile(bundle_inputs['output_dir'] / review['archive']) as archive:
        assert archive.read('payload/LICENSE') == b'BSD source license\n'
        assert 'payload/src/new.rs' in archive.namelist()
        assert 'payload/undeclared.rs' not in archive.namelist()
        assert review['files']['payload/LICENSE']['sha256'] == _hash(archive.read('payload/LICENSE'))


@pytest.mark.parametrize('case', ['drift', 'size', 'parent-symlink', 'conflict', 'escape'])
def test_packaging_rejects_unreviewed_payload_before_writing_archive(bundle_inputs: dict[str, Path], case: str) -> None:
    manifest = _add_fork_payload(bundle_inputs)
    payload = manifest['candidates'][0]['payload']
    root = bundle_inputs['candidate_manifest'].parent
    if case == 'drift':
        (root / payload[0]['source']).write_bytes(b'changed')
    elif case == 'size':
        payload[0]['size_bytes'] += 1
    elif case == 'parent-symlink':
        (root / 'indirect').symlink_to(root / 'payload/src', target_is_directory=True)
        payload[2]['source'] = 'indirect/new.rs'
    elif case == 'conflict':
        payload.append(dict(payload[0]))
    else:
        payload[0]['target'] = 'vendor/rust_xlsxwriter/../../outside'
    bundle_inputs['candidate_manifest'].write_text(json.dumps(manifest), encoding='utf-8')
    with pytest.raises(BundleError):
        prepare_bundle(**bundle_inputs)
    assert not bundle_inputs['output_dir'].exists()

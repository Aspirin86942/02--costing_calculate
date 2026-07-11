from __future__ import annotations

import json
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

from tests.rust_oracle import evidence
from tests.rust_oracle.evidence import DependencyEvidence, EvidenceSanitizer

_UPSTREAM_BASE = '9134de25afadaee955d0f821862338e3d046a338'
_REVISION = 'f4e4e99f920937709d6dacb4680c60ea3f80486f'
_PRE_PIN_COMMIT = '000d7c3867600908b1d2631fb5033b5092883c14'
_CHECKSUM = 'dd1746025420e17b5d62528b930e550e016e857038794d74e169018126ef3d14'
_DIFF_SHA = 'a' * 64
_LOG_SHA = 'b' * 64
_MANDATORY_DIFF_FILES = (
    'src/packager.rs',
    'src/workbook.rs',
    'src/workbook/tests.rs',
    'src/worksheet.rs',
    'src/worksheet/tests.rs',
)


def dependency_evidence(**changes: object) -> DependencyEvidence:
    value = DependencyEvidence(
        upstream_url='https://github.com/jmcnamara/rust_xlsxwriter.git',
        upstream_tag='v0.96.0',
        upstream_base_revision=_UPSTREAM_BASE,
        crates_io_checksum=_CHECKSUM,
        pre_pin_costing_commit=_PRE_PIN_COMMIT,
        fork_url='https://github.com/Aspirin86942/rust_xlsxwriter.git',
        fork_revision=_REVISION,
        allowed_diff_files=_MANDATORY_DIFF_FILES,
        xmlwriter_fallback_used=False,
        xmlwriter_fallback_trigger_test=None,
        diff_sha256=_DIFF_SHA,
        local_unversioned_log_sha256=_LOG_SHA,
        upstream_pr_url=None,
        verdict='VALIDATED',
    )
    return replace(value, **changes)


def test_dependency_manifest_rejects_moving_or_sensitive_values(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        EvidenceSanitizer.write_dependency_manifest(
            tmp_path / 'bad.json',
            dependency_evidence(fork_revision='main'),
        )
    with pytest.raises(ValueError):
        EvidenceSanitizer.write_dependency_manifest(
            tmp_path / 'bad-path.json',
            dependency_evidence(local_unversioned_log_sha256=r'C:\Users\secret\log.txt'),
        )


@pytest.mark.parametrize(
    'sensitive_value',
    (
        r'D:\private\evidence.txt',
        r'\\server\share\evidence.txt',
        r'prefix \\server\share\evidence.txt',
        '/Users/private/evidence.txt',
        'build-canary-marker',
        'captured stdout',
        'captured STDERR',
    ),
)
def test_dependency_manifest_scans_every_string_for_sensitive_values(
    tmp_path: Path,
    sensitive_value: str,
) -> None:
    output = tmp_path / 'dependency.json'
    with pytest.raises(ValueError, match='sensitive'):
        EvidenceSanitizer.write_dependency_manifest(
            output,
            dependency_evidence(allowed_diff_files=(*_MANDATORY_DIFF_FILES, sensitive_value)),
        )
    assert not output.exists()


def test_dependency_manifest_has_no_pr_and_exact_provenance(tmp_path: Path) -> None:
    output = tmp_path / 'dependency.json'
    EvidenceSanitizer.write_dependency_manifest(output, dependency_evidence())

    payload = json.loads(output.read_text(encoding='utf-8'))
    assert payload == {
        'upstream_url': 'https://github.com/jmcnamara/rust_xlsxwriter.git',
        'upstream_tag': 'v0.96.0',
        'upstream_base_revision': _UPSTREAM_BASE,
        'crates_io_checksum': _CHECKSUM,
        'pre_pin_costing_commit': _PRE_PIN_COMMIT,
        'fork_url': 'https://github.com/Aspirin86942/rust_xlsxwriter.git',
        'fork_revision': _REVISION,
        'allowed_diff_files': list(_MANDATORY_DIFF_FILES),
        'xmlwriter_fallback_used': False,
        'xmlwriter_fallback_trigger_test': None,
        'diff_sha256': _DIFF_SHA,
        'local_unversioned_log_sha256': _LOG_SHA,
        'upstream_pr_url': None,
        'verdict': 'VALIDATED',
    }


@pytest.mark.parametrize(
    ('field', 'legal_json', 'malicious_json'),
    (
        ('fork_revision', json.dumps(_REVISION), json.dumps('main')),
        ('verdict', json.dumps('VALIDATED'), json.dumps('UNVALIDATED')),
    ),
)
@pytest.mark.parametrize('malicious_first', (False, True))
def test_dependency_manifest_rejects_duplicate_keys_in_both_directions(
    tmp_path: Path,
    field: str,
    legal_json: str,
    malicious_json: str,
    malicious_first: bool,
) -> None:
    payload = EvidenceSanitizer.dependency_payload(dependency_evidence())
    raw = json.dumps(payload)
    legal_pair = f'"{field}": {legal_json}'
    first = malicious_json if malicious_first else legal_json
    second = legal_json if malicious_first else malicious_json
    raw = raw.replace(legal_pair, f'"{field}": {first}, "{field}": {second}')
    path = tmp_path / f'duplicate-{field}-{malicious_first}.json'
    path.write_text(raw, encoding='utf-8')

    with pytest.raises(ValueError, match='duplicate JSON key'):
        EvidenceSanitizer.read_dependency_manifest(path)


@pytest.mark.parametrize('mutation', ('missing', 'unknown'))
def test_dependency_manifest_reader_rejects_missing_or_unknown_fields(tmp_path: Path, mutation: str) -> None:
    payload = EvidenceSanitizer.dependency_payload(dependency_evidence())
    if mutation == 'missing':
        payload.pop('verdict')
    else:
        payload['unexpected'] = 'value'
    path = tmp_path / f'{mutation}.json'
    path.write_text(json.dumps(payload), encoding='utf-8')

    with pytest.raises(ValueError, match='exact closed schema'):
        EvidenceSanitizer.read_dependency_manifest(path)


@pytest.mark.parametrize(
    ('field', 'value'),
    (
        ('upstream_url', 'https://example.invalid/rust_xlsxwriter.git'),
        ('upstream_tag', 'latest'),
        ('upstream_base_revision', 'A' * 40),
        ('crates_io_checksum', 'a' * 64),
        ('pre_pin_costing_commit', '1' * 39),
        ('fork_url', 'https://example.invalid/fork.git'),
        ('fork_revision', 'F' * 40),
        ('diff_sha256', '2' * 63),
        ('local_unversioned_log_sha256', 'g' * 64),
        ('upstream_pr_url', 'https://github.com/jmcnamara/rust_xlsxwriter/pull/1'),
        ('verdict', 'UNVALIDATED'),
    ),
)
def test_dependency_manifest_rejects_invalid_closed_fields(tmp_path: Path, field: str, value: object) -> None:
    with pytest.raises(ValueError):
        EvidenceSanitizer.write_dependency_manifest(
            tmp_path / f'{field}.json',
            dependency_evidence(**{field: value}),
        )


def test_dependency_manifest_enforces_exact_diff_allowlist_and_fallback_gate(tmp_path: Path) -> None:
    invalid_values = (
        dependency_evidence(allowed_diff_files=_MANDATORY_DIFF_FILES[:-1]),
        dependency_evidence(allowed_diff_files=(*_MANDATORY_DIFF_FILES, 'src/extra.rs')),
        dependency_evidence(allowed_diff_files=(*_MANDATORY_DIFF_FILES, 'src/xmlwriter.rs')),
        dependency_evidence(
            allowed_diff_files=(*_MANDATORY_DIFF_FILES, 'src/xmlwriter.rs'),
            xmlwriter_fallback_used=True,
            xmlwriter_fallback_trigger_test='not_a_named_gate',
        ),
    )
    for index, value in enumerate(invalid_values):
        with pytest.raises(ValueError):
            EvidenceSanitizer.write_dependency_manifest(tmp_path / f'invalid-{index}.json', value)

    valid = dependency_evidence(
        allowed_diff_files=(*_MANDATORY_DIFF_FILES, 'src/xmlwriter.rs'),
        xmlwriter_fallback_used=True,
        xmlwriter_fallback_trigger_test='row_start_write_failure_returns_original_io_error',
    )
    EvidenceSanitizer.write_dependency_manifest(tmp_path / 'valid.json', valid)


def test_dependency_manifest_is_create_new_and_leaves_no_partial_file(tmp_path: Path) -> None:
    output = tmp_path / 'dependency.json'
    output.write_text('existing', encoding='utf-8')
    with pytest.raises(FileExistsError):
        EvidenceSanitizer.write_dependency_manifest(output, dependency_evidence())
    assert output.read_text(encoding='utf-8') == 'existing'

    invalid_output = tmp_path / 'invalid.json'
    with pytest.raises(ValueError):
        EvidenceSanitizer.write_dependency_manifest(invalid_output, dependency_evidence(fork_revision='main'))
    assert not invalid_output.exists()


def test_dependency_manifest_removes_partial_file_after_write_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output = tmp_path / 'partial.json'

    def fail_after_partial_write(_payload: object, stream: object, **_kwargs: object) -> None:
        stream.write('{')
        raise OSError('simulated write failure')

    monkeypatch.setattr(evidence.json, 'dump', fail_after_partial_write)

    with pytest.raises(OSError, match='simulated write failure'):
        EvidenceSanitizer.write_dependency_manifest(output, dependency_evidence())

    assert not output.exists()
    assert not tuple(tmp_path.glob(f'.{output.name}.*.tmp'))


def test_dependency_manifest_atomic_link_loser_preserves_winner_and_cleans_temp(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output = tmp_path / 'dependency.json'

    def winner_before_link(_source: object, destination: object) -> None:
        Path(destination).write_text('winner manifest', encoding='utf-8')
        raise FileExistsError(destination)

    monkeypatch.setattr(evidence.os, 'link', winner_before_link)

    with pytest.raises(FileExistsError):
        EvidenceSanitizer.write_dependency_manifest(output, dependency_evidence())

    assert output.read_text(encoding='utf-8') == 'winner manifest'
    assert not tuple(tmp_path.glob(f'.{output.name}.*.tmp'))


def test_dependency_manifest_fsync_failure_leaves_no_final_or_temp(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output = tmp_path / 'dependency.json'
    monkeypatch.setattr(evidence.os, 'fsync', lambda _fd: (_ for _ in ()).throw(OSError('fsync failed')))

    with pytest.raises(OSError, match='fsync failed'):
        EvidenceSanitizer.write_dependency_manifest(output, dependency_evidence())

    assert not output.exists()
    assert not tuple(tmp_path.glob(f'.{output.name}.*.tmp'))


def test_owned_cleanup_does_not_delete_replacement(tmp_path: Path) -> None:
    path = tmp_path / 'owned.tmp'
    path.write_text('owned', encoding='utf-8')
    identity = evidence._file_identity(path)
    path.unlink()
    path.write_text('replacement', encoding='utf-8')

    evidence._unlink_owned_path(path, identity)

    assert path.read_text(encoding='utf-8') == 'replacement'


@pytest.mark.parametrize('operation', ('manifest', 'raw_log'))
def test_owned_evidence_close_failure_leaves_no_partial_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    operation: str,
) -> None:
    real_fdopen = evidence.os.fdopen

    class CloseFailureStream:
        def __init__(self, stream: object) -> None:
            self._stream = stream

        def __enter__(self) -> CloseFailureStream:
            return self

        def __exit__(self, *_args: object) -> None:
            self._stream.close()
            raise OSError('close failed')

        def __getattr__(self, name: str) -> object:
            return getattr(self._stream, name)

    monkeypatch.setattr(
        evidence.os,
        'fdopen',
        lambda *args, **kwargs: CloseFailureStream(real_fdopen(*args, **kwargs)),
    )

    with pytest.raises(OSError, match='close failed'):
        if operation == 'manifest':
            output = tmp_path / 'dependency.json'
            EvidenceSanitizer.write_dependency_manifest(output, dependency_evidence())
        else:
            output = tmp_path / 'raw.log'
            evidence._write_owned_log(output, 'raw evidence')

    if operation == 'manifest':
        assert not tuple(tmp_path.glob(f'.{output.name}.*.tmp'))
    assert not output.exists()


def _revision_fixtures(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    cargo_manifest = tmp_path / 'Cargo.toml'
    cargo_metadata = tmp_path / 'metadata.json'
    cargo_lock = tmp_path / 'Cargo.lock'
    dependency_manifest = tmp_path / 'dependency.json'
    cargo_manifest.write_text(
        '[workspace.dependencies]\n'
        'rust_xlsxwriter = { git = "https://github.com/Aspirin86942/rust_xlsxwriter.git", '
        f'rev = "{_REVISION}", default-features = false }}\n',
        encoding='utf-8',
    )
    cargo_metadata.write_text(
        json.dumps(
            {
                'packages': [
                    {
                        'name': 'rust_xlsxwriter',
                        'version': '0.96.0',
                        'source': (
                            f'git+https://github.com/Aspirin86942/rust_xlsxwriter.git?rev={_REVISION}#{_REVISION}'
                        ),
                    }
                ]
            }
        ),
        encoding='utf-8',
    )
    cargo_lock.write_text(
        '[[package]]\n'
        'name = "rust_xlsxwriter"\n'
        'version = "0.96.0"\n'
        f'source = "git+https://github.com/Aspirin86942/rust_xlsxwriter.git?rev={_REVISION}#{_REVISION}"\n',
        encoding='utf-8',
    )
    dependency_manifest.write_text(json.dumps({'fork_revision': _REVISION}), encoding='utf-8')
    return cargo_manifest, cargo_metadata, cargo_lock, dependency_manifest


@pytest.mark.parametrize(
    'mismatch_source',
    ('fork_head', 'cargo_manifest', 'cargo_metadata', 'cargo_lock', 'dependency_manifest'),
)
def test_rust_xlsxwriter_revision_consistency_rejects_each_of_five_mismatches(
    tmp_path: Path,
    mismatch_source: str,
) -> None:
    cargo_manifest, cargo_metadata, cargo_lock, dependency_manifest = _revision_fixtures(tmp_path)
    fork_head = _REVISION
    changed = 'c' * 40
    if mismatch_source == 'fork_head':
        fork_head = changed
    elif mismatch_source == 'cargo_manifest':
        cargo_manifest.write_text(
            cargo_manifest.read_text(encoding='utf-8').replace(_REVISION, changed), encoding='utf-8'
        )
    elif mismatch_source == 'cargo_metadata':
        cargo_metadata.write_text(
            cargo_metadata.read_text(encoding='utf-8').replace(_REVISION, changed), encoding='utf-8'
        )
    elif mismatch_source == 'cargo_lock':
        cargo_lock.write_text(cargo_lock.read_text(encoding='utf-8').replace(_REVISION, changed), encoding='utf-8')
    else:
        dependency_manifest.write_text(
            dependency_manifest.read_text(encoding='utf-8').replace(_REVISION, changed),
            encoding='utf-8',
        )

    with pytest.raises(ValueError, match='revision mismatch'):
        EvidenceSanitizer.verify_rust_xlsxwriter_revision_consistency(
            fork_head=fork_head,
            cargo_manifest=cargo_manifest,
            cargo_metadata=cargo_metadata,
            cargo_lock=cargo_lock,
            dependency_manifest=dependency_manifest,
        )


def test_rust_xlsxwriter_revision_consistency_accepts_five_matching_sources(tmp_path: Path) -> None:
    cargo_manifest, cargo_metadata, cargo_lock, dependency_manifest = _revision_fixtures(tmp_path)
    assert (
        EvidenceSanitizer.verify_rust_xlsxwriter_revision_consistency(
            fork_head=_REVISION,
            cargo_manifest=cargo_manifest,
            cargo_metadata=cargo_metadata,
            cargo_lock=cargo_lock,
            dependency_manifest=dependency_manifest,
        )
        == _REVISION
    )


@pytest.mark.parametrize('metadata_case', ('wrong_version', 'two_versions'))
def test_cargo_metadata_requires_one_exact_rust_xlsxwriter_version(metadata_case: str) -> None:
    source = f'git+https://github.com/Aspirin86942/rust_xlsxwriter.git?rev={_REVISION}#{_REVISION}'
    packages = [{'name': 'rust_xlsxwriter', 'version': '0.95.0', 'source': source}]
    if metadata_case == 'two_versions':
        packages.append({'name': 'rust_xlsxwriter', 'version': '0.96.0', 'source': source})

    with pytest.raises(ValueError, match='exactly one rust_xlsxwriter 0.96.0'):
        evidence._revision_from_cargo_metadata(json.dumps({'packages': packages}))


def _registry_lock(checksum: str) -> str:
    return (
        '[[package]]\n'
        'name = "rust_xlsxwriter"\n'
        'version = "0.96.0"\n'
        'source = "registry+https://github.com/rust-lang/crates.io-index"\n'
        f'checksum = "{checksum}"\n'
    )


def _closed_cli_layout(tmp_path: Path) -> tuple[Path, Path, Path, Path, Path, Path]:
    root = tmp_path / 'repo'
    cargo_manifest = root / 'rust' / 'Cargo.toml'
    cargo_lock = root / 'rust' / 'Cargo.lock'
    local_log_root = root / 'rust' / 'target' / 'perf' / 'local-logs'
    output = root / 'docs' / 'performance' / 'dependencies' / '2026-07-11-rust-xlsxwriter-0.96.0.json'
    fork = tmp_path / 'fork'
    cargo_manifest.parent.mkdir(parents=True)
    cargo_manifest.write_text('[workspace]\n', encoding='utf-8')
    cargo_lock.write_text('version = 3\n', encoding='utf-8')
    local_log_root.mkdir(parents=True)
    output.parent.mkdir(parents=True)
    fork.mkdir()
    return root, fork, cargo_manifest, cargo_lock, local_log_root, output


@pytest.mark.parametrize(
    'invalid_target',
    ('cargo_manifest', 'cargo_lock', 'output', 'local_log_root', 'parent_traversal'),
)
def test_dependency_generation_rejects_paths_before_command_or_mkdir(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    invalid_target: str,
) -> None:
    root, fork, cargo_manifest, cargo_lock, local_log_root, output = _closed_cli_layout(tmp_path)
    monkeypatch.setattr(evidence, 'repo_root', lambda: root)
    values = {
        'cargo_manifest': cargo_manifest,
        'cargo_lock': cargo_lock,
        'output': output,
        'local_log_root': local_log_root,
    }
    if invalid_target == 'parent_traversal':
        values['output'] = output.parent / '..' / 'dependencies' / output.name
    elif invalid_target == 'local_log_root':
        values['local_log_root'] = root
    else:
        values[invalid_target] = root / f'invalid-{invalid_target}'
    monkeypatch.setattr(evidence, '_run_command', lambda *_args, **_kwargs: pytest.fail('command ran'))
    monkeypatch.setattr(Path, 'mkdir', lambda *_args, **_kwargs: pytest.fail('mkdir ran'))

    with pytest.raises(ValueError, match='path'):
        evidence.generate_dependency_manifest(
            fork_checkout=fork,
            cargo_manifest=values['cargo_manifest'],
            cargo_lock=values['cargo_lock'],
            pre_pin_commit=_PRE_PIN_COMMIT,
            local_log_root=values['local_log_root'],
            output=values['output'],
        )


def test_dependency_generation_rejects_symlink_escape_before_command(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root, fork, cargo_manifest, cargo_lock, local_log_root, output = _closed_cli_layout(tmp_path)
    outside = tmp_path / 'outside'
    outside.mkdir()
    local_log_root.rmdir()
    try:
        local_log_root.symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f'symlink creation unavailable: {exc}')
    monkeypatch.setattr(evidence, 'repo_root', lambda: root)
    monkeypatch.setattr(evidence, '_run_command', lambda *_args, **_kwargs: pytest.fail('command ran'))

    with pytest.raises(ValueError, match='path'):
        evidence.generate_dependency_manifest(
            fork_checkout=fork,
            cargo_manifest=cargo_manifest,
            cargo_lock=cargo_lock,
            pre_pin_commit=_PRE_PIN_COMMIT,
            local_log_root=local_log_root,
            output=output,
        )


def test_dependency_generation_proves_unique_log_path_is_ignored_before_other_commands(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root, fork, cargo_manifest, cargo_lock, local_log_root, output = _closed_cli_layout(tmp_path)
    monkeypatch.setattr(evidence, 'repo_root', lambda: root)
    calls: list[tuple[str, ...]] = []

    def stop_after_ignore(executable: str, *args: str) -> evidence._CommandResult:
        calls.append((executable, *args))
        if len(calls) == 1:
            if calls[0][3:6] != ('check-ignore', '--quiet', '--'):
                raise AssertionError('check-ignore was not the first command')
            return evidence._CommandResult((executable, *args), '', '')
        raise RuntimeError('stop after ignore proof')

    monkeypatch.setattr(evidence, '_run_command', stop_after_ignore)

    with pytest.raises(RuntimeError, match='stop after ignore proof'):
        evidence.generate_dependency_manifest(
            fork_checkout=fork,
            cargo_manifest=cargo_manifest,
            cargo_lock=cargo_lock,
            pre_pin_commit=_PRE_PIN_COMMIT,
            local_log_root=local_log_root,
            output=output,
        )

    assert calls[0][:3] == ('git', '-C', str(root.resolve()))
    assert calls[0][3:6] == ('check-ignore', '--quiet', '--')
    assert calls[0][6].startswith('rust/target/perf/local-logs/rust-xlsxwriter-0.96.0-')
    assert calls[0][6].endswith('.log')


@pytest.mark.parametrize('invalid_target', ('cargo_manifest', 'cargo_lock', 'dependency_manifest', 'parent_traversal'))
def test_dependency_verify_rejects_paths_before_command(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    invalid_target: str,
) -> None:
    root, fork, cargo_manifest, cargo_lock, _local_log_root, dependency_manifest = _closed_cli_layout(tmp_path)
    EvidenceSanitizer.write_dependency_manifest(dependency_manifest, dependency_evidence())
    monkeypatch.setattr(evidence, 'repo_root', lambda: root)
    values = {
        'cargo_manifest': cargo_manifest,
        'cargo_lock': cargo_lock,
        'dependency_manifest': dependency_manifest,
    }
    if invalid_target == 'parent_traversal':
        values['dependency_manifest'] = dependency_manifest.parent / '..' / 'dependencies' / dependency_manifest.name
    else:
        values[invalid_target] = root / f'invalid-{invalid_target}'
    monkeypatch.setattr(evidence, '_run_command', lambda *_args, **_kwargs: pytest.fail('command ran'))

    with pytest.raises(ValueError, match='path'):
        evidence.verify_dependency_manifest(
            fork_checkout=fork,
            cargo_manifest=values['cargo_manifest'],
            cargo_lock=values['cargo_lock'],
            pre_pin_commit=_PRE_PIN_COMMIT,
            dependency_manifest=values['dependency_manifest'],
        )


def test_dependency_checksum_rejects_changed_lock_checksum() -> None:
    with pytest.raises(ValueError, match='lock checksum'):
        EvidenceSanitizer.verify_registry_checksum(_registry_lock('c' * 64), ())


def test_dependency_checksum_rejects_changed_cached_archive_hash(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    archive = tmp_path / 'rust_xlsxwriter-0.96.0.crate'
    archive.write_bytes(b'changed archive')
    monkeypatch.setattr(evidence, '_sha256_file', lambda _path: 'c' * 64)
    with pytest.raises(ValueError, match='archive checksum'):
        EvidenceSanitizer.verify_registry_checksum(_registry_lock(_CHECKSUM), (archive,))


def test_dependency_checksum_accepts_every_matching_cached_archive(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    archives = tuple(tmp_path / f'cache-{index}.crate' for index in range(2))
    for archive in archives:
        archive.write_bytes(b'cached archive')
    monkeypatch.setattr(evidence, '_sha256_file', lambda _path: _CHECKSUM)
    assert EvidenceSanitizer.verify_registry_checksum(_registry_lock(_CHECKSUM), archives) == _CHECKSUM


def test_dependency_pr_query_must_be_empty() -> None:
    EvidenceSanitizer.verify_empty_pr_query('[]')
    with pytest.raises(ValueError, match='upstream PR'):
        EvidenceSanitizer.verify_empty_pr_query('[{"url":"https://github.com/jmcnamara/rust_xlsxwriter/pull/1"}]')


def test_dependency_cli_diff_listing_captures_status_without_renames(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, ...]] = []
    fixed_outputs = (
        f'{_REVISION}\n',
        f'{_UPSTREAM_BASE}\n',
        'https://github.com/jmcnamara/rust_xlsxwriter.git\n',
        'https://github.com/Aspirin86942/rust_xlsxwriter.git\n',
        'costing-fallible-temp-io-v0.96.0\n',
    )

    def stop_at_diff_listing(executable: str, *args: str) -> evidence._CommandResult:
        calls.append((executable, *args))
        if len(calls) == 6:
            raise RuntimeError('stop after capturing diff listing command')
        return evidence._CommandResult((executable, *args), fixed_outputs[len(calls) - 1], '')

    monkeypatch.setattr(evidence, '_run_command', stop_at_diff_listing)

    with pytest.raises(RuntimeError, match='stop after capturing'):
        evidence._collect_live_evidence(
            fork_checkout=tmp_path / 'fork',
            cargo_manifest=tmp_path / 'rust' / 'Cargo.toml',
            cargo_lock=tmp_path / 'rust' / 'Cargo.lock',
            pre_pin_commit=_PRE_PIN_COMMIT,
        )

    assert calls[5][:6] == (
        'git',
        '-C',
        str((tmp_path / 'fork').resolve()),
        'diff',
        '--name-status',
        '--no-renames',
    )
    assert not any(argument.startswith('--diff-filter') for argument in calls[5])
    assert f'{_UPSTREAM_BASE}..{_REVISION}' in calls[5]
    assert f'{_UPSTREAM_BASE}..HEAD' not in calls[5]


@pytest.mark.parametrize('rejected_status', ('D', 'A', 'R100', 'C100', 'T', 'U', 'X', 'B'))
def test_dependency_diff_status_rejects_non_modified_mandatory_path(rejected_status: str) -> None:
    rows = [f'M\t{path}' for path in _MANDATORY_DIFF_FILES]
    rows[2] = f'{rejected_status}\t{_MANDATORY_DIFF_FILES[2]}'

    with pytest.raises(ValueError, match='modified status'):
        EvidenceSanitizer.verify_fork_diff_statuses('\n'.join(rows))


def test_dependency_diff_status_accepts_only_five_modified_mandatory_paths() -> None:
    raw = '\n'.join(f'M\t{path}' for path in _MANDATORY_DIFF_FILES)

    assert EvidenceSanitizer.verify_fork_diff_statuses(raw) == _MANDATORY_DIFF_FILES


@pytest.mark.parametrize('operation', ('generate', 'verify'))
def test_dependency_generate_and_verify_reject_deleted_mandatory_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    operation: str,
) -> None:
    root, fork, cargo_manifest, cargo_lock, local_log_root, output = _closed_cli_layout(tmp_path)
    rows = [f'M\t{path}' for path in _MANDATORY_DIFF_FILES]
    rows[1] = f'D\t{_MANDATORY_DIFF_FILES[1]}'
    monkeypatch.setattr(evidence, 'repo_root', lambda: root)

    def reject_deleted_status(**_kwargs: object) -> tuple[dict[str, object], tuple[object, ...], tuple[Path, ...]]:
        EvidenceSanitizer.verify_fork_diff_statuses('\n'.join(rows))
        raise AssertionError('deleted status unexpectedly accepted')

    monkeypatch.setattr(evidence, '_collect_live_evidence', reject_deleted_status)
    if operation == 'verify':
        EvidenceSanitizer.write_dependency_manifest(output, dependency_evidence())
    else:
        monkeypatch.setattr(
            evidence,
            '_run_command',
            lambda executable, *args: evidence._CommandResult((executable, *args), '', ''),
        )

    with pytest.raises(ValueError, match='modified status'):
        if operation == 'generate':
            evidence.generate_dependency_manifest(
                fork_checkout=fork,
                cargo_manifest=cargo_manifest,
                cargo_lock=cargo_lock,
                pre_pin_commit=_PRE_PIN_COMMIT,
                local_log_root=local_log_root,
                output=output,
            )
        else:
            evidence.verify_dependency_manifest(
                fork_checkout=fork,
                cargo_manifest=cargo_manifest,
                cargo_lock=cargo_lock,
                pre_pin_commit=_PRE_PIN_COMMIT,
                dependency_manifest=output,
            )


def test_dependency_generation_create_new_loser_does_not_delete_winner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root, fork, cargo_manifest, cargo_lock, local_log_root, output = _closed_cli_layout(tmp_path)
    revision_dir = tmp_path / 'revision-fixtures'
    revision_dir.mkdir()
    revision_manifest, cargo_metadata, revision_lock, _dependency_manifest = _revision_fixtures(revision_dir)
    live: dict[str, object] = {
        'fork_head': _REVISION,
        'cargo_manifest_text': revision_manifest.read_text(encoding='utf-8'),
        'cargo_metadata_text': cargo_metadata.read_text(encoding='utf-8'),
        'cargo_lock_text': revision_lock.read_text(encoding='utf-8'),
        'pre_pin_lock_text': _registry_lock(_CHECKSUM),
        'diff_files': _MANDATORY_DIFF_FILES,
        'diff_sha256': _DIFF_SHA,
        'fallback_used': False,
        'checksum': _CHECKSUM,
        'pr_query_output': '[]',
    }
    monkeypatch.setattr(evidence, 'repo_root', lambda: root)
    monkeypatch.setattr(
        evidence,
        '_run_command',
        lambda executable, *args: evidence._CommandResult((executable, *args), '', ''),
    )
    monkeypatch.setattr(evidence, '_collect_live_evidence', lambda **_kwargs: (live, (), ()))

    def simulate_winner_then_create_new_failure(_output: Path, _value: DependencyEvidence) -> None:
        output.write_text('winner manifest', encoding='utf-8')
        raise FileExistsError(output)

    monkeypatch.setattr(EvidenceSanitizer, 'write_dependency_manifest', simulate_winner_then_create_new_failure)

    with pytest.raises(FileExistsError):
        evidence.generate_dependency_manifest(
            fork_checkout=fork,
            cargo_manifest=cargo_manifest,
            cargo_lock=cargo_lock,
            pre_pin_commit=_PRE_PIN_COMMIT,
            local_log_root=local_log_root,
            output=output,
        )

    assert output.read_text(encoding='utf-8') == 'winner manifest'
    assert not tuple(local_log_root.glob('rust-xlsxwriter-0.96.0-*.log'))


def test_run_command_preserves_windows_rustup_proxy_name(monkeypatch: pytest.MonkeyPatch) -> None:
    cargo_proxy = r'C:\tools\cargo.exe'
    captured: list[tuple[str, ...]] = []
    monkeypatch.setattr(evidence.shutil, 'which', lambda _name: cargo_proxy)
    monkeypatch.setattr(Path, 'resolve', lambda _path: Path(r'C:\tools\rustup.exe'))

    def fake_run(command: tuple[str, ...], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        captured.append(command)
        return subprocess.CompletedProcess(command, 0, '{}', '')

    monkeypatch.setattr(evidence.subprocess, 'run', fake_run)

    evidence._run_command('cargo', 'metadata')

    assert captured == [(cargo_proxy, 'metadata')]

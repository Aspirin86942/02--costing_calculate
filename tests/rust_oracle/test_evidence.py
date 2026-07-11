from __future__ import annotations

import json
import math
import shutil
import socket
import subprocess
from dataclasses import replace
from decimal import Decimal
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


def _benchmark_manifest_evidence(**changes: object) -> evidence.BenchmarkManifestEvidence:
    value = evidence.BenchmarkManifestEvidence(
        schema_version=1,
        profile=evidence.ComparisonProfile.PHASE0B_VS_PHASE0A,
        pipeline='gb',
        input_alias=evidence.PathAlias.GB_INPUT,
        input_sha256='1' * 64,
        reference_label=evidence.ClosedBinaryLabel.PHASE0A,
        reference_exe_sha256='2' * 64,
        candidate_label=evidence.ClosedBinaryLabel.PHASE0B,
        candidate_exe_sha256='3' * 64,
        machine=evidence.MachineArtifactEvidence(
            windows_build_sha256='4' * 64,
            architecture='x86_64',
            cpu_model_sha256='5' * 64,
            logical_cpu_count=16,
            physical_memory_bytes=32 * 1024**3,
            system_drive_media_type='SSD',
            system_drive_size_bytes=1024**4,
            fingerprint_sha256='6' * 64,
        ),
        attempt_count=1,
        prior_safe_verdicts=(evidence.HarnessVerdict.ENVIRONMENT_DRIFT,),
        ledger_head_sha256='7' * 64,
        first_group_sha256='8' * 64,
        expanded_group_sha256=None,
        rounds=(
            evidence.BenchmarkRoundEvidence(
                metric=evidence.BenchmarkMetric.WALL,
                global_round=1,
                order=('reference', 'candidate'),
                reference_value=Decimal('1.25'),
                candidate_value=Decimal('1.20'),
            ),
        ),
        metrics=(
            evidence.BenchmarkMetricEvidence(evidence.BenchmarkMetric.WALL_MEDIAN, Decimal('1.20')),
            evidence.BenchmarkMetricEvidence(evidence.BenchmarkMetric.WALL_RATIO, Decimal('0.96')),
        ),
        runtime_counts=(evidence.RuntimeCountEvidence(evidence.RuntimeCount.READER_ROWS, 10),),
        sheet_dimensions=(evidence.SheetDimensionEvidence(evidence.ApprovedSheet.COST_DETAIL, 'A1:AZ10'),),
        output_bytes=(
            evidence.OutputBytesEvidence('reference', 1000),
            evidence.OutputBytesEvidence('candidate', 990),
        ),
        mismatches=(),
        local_log_sha256=('9' * 64,),
        verdict=evidence.HarnessVerdict.VALIDATED,
    )
    return replace(value, **changes)


def _command_transcript_evidence(**changes: object) -> evidence.CommandTranscriptEvidence:
    value = evidence.CommandTranscriptEvidence(
        command_id=evidence.CommandId.CARGO_BUILD_RELEASE,
        tokens=(
            evidence.CommandToken.CARGO,
            evidence.CommandToken.BUILD,
            evidence.CommandToken.RELEASE,
            evidence.PathAlias.REPO_ROOT,
        ),
        tool=evidence.ToolName.CARGO,
        tool_version=evidence.SanitizedToolVersion(1, 88, 0),
        exit_code=0,
        local_log_sha256='a' * 64,
        verdict=evidence.HarnessVerdict.VALIDATED,
    )
    return replace(value, **changes)


def _all_new_artifact_values() -> tuple[tuple[str, object], ...]:
    return (
        ('benchmark_manifest', _benchmark_manifest_evidence()),
        ('command_transcript', _command_transcript_evidence()),
        (
            'smoke',
            evidence.SmokeSummaryEvidence(
                candidate_exe_sha256='b' * 64,
                fixture_sha256='c' * 64,
                pipeline='sk',
                exit_code=0,
                approved_sheets=tuple(evidence.ApprovedSheet),
                temp_canary_created=False,
                temp_residue_count=0,
                missing_dll=False,
                local_log_sha256='d' * 64,
                verdict=evidence.HarnessVerdict.VALIDATED,
            ),
        ),
        (
            'pe_imports',
            evidence.PeImportsEvidence(
                candidate_exe_sha256='e' * 64,
                baseline_exe_sha256='f' * 64,
                tools=(evidence.ToolName.DUMPBIN,),
                normal_imports=(evidence.DllBasename.KERNEL32,),
                delay_imports=(),
                local_log_sha256='0' * 64,
                verdict=evidence.HarnessVerdict.VALIDATED,
            ),
        ),
        (
            'fork_provenance',
            evidence.ForkProvenanceEvidence(
                official_url=evidence.ForkUrl.OFFICIAL,
                fork_url=evidence.ForkUrl.COSTING_FORK,
                tag=evidence.ForkTag.V0_96_0,
                upstream_base_revision=_UPSTREAM_BASE,
                crates_io_checksum=_CHECKSUM,
                fork_revision=_REVISION,
                allowed_diff_files=tuple(evidence.ForkDiffPath),
                diff_sha256='a' * 64,
                no_pr=True,
                local_log_sha256='b' * 64,
                verdict=evidence.HarnessVerdict.VALIDATED,
            ),
        ),
        (
            'cargo_feature_tree',
            evidence.CargoFeatureTreeEvidence(
                candidate_label=evidence.ClosedBinaryLabel.PHASE3,
                candidate_exe_sha256='c' * 64,
                fork_revision=_REVISION,
                packages=(
                    evidence.CargoPackageEvidence(
                        evidence.CargoPackage.RUST_XLSXWRITER,
                        _REVISION,
                    ),
                ),
                feature_edges=(
                    evidence.CargoFeatureEdge(
                        evidence.CargoPackage.COSTING_XLSX,
                        evidence.CargoFeature.LOW_MEMORY,
                        evidence.CargoPackage.RUST_XLSXWRITER,
                        evidence.CargoFeature.CONSTANT_MEMORY,
                    ),
                ),
                local_log_sha256='d' * 64,
                verdict=evidence.HarnessVerdict.VALIDATED,
            ),
        ),
        (
            'text_report',
            evidence.TextReportEvidence(
                report_kind=evidence.ReportKind.PHASE_GATE,
                title=evidence.ReportTitle.PHASE_GATE_RESULT,
                checks=(
                    evidence.ReportCheckEvidence(
                        evidence.ReportCheckId.CORRECTNESS,
                        evidence.HarnessVerdict.VALIDATED,
                        'e' * 64,
                    ),
                ),
                overall_verdict=evidence.HarnessVerdict.VALIDATED,
            ),
        ),
    )


def test_success_manifest_contains_only_aliases_hashes_counts_and_finite_numbers() -> None:
    artifact = EvidenceSanitizer.closed_policy().build_benchmark_manifest(_benchmark_manifest_evidence())
    raw = artifact.content

    assert '$GB_INPUT' in raw
    assert not any(token in raw for token in ('C:\\', 'D:\\', '/Users/', 'input.xlsx'))
    assert all(math.isfinite(float(value)) for value in artifact.numeric_values)


def test_benchmark_schema_version_rejects_boolean_integer_alias() -> None:
    with pytest.raises(ValueError, match='schema version'):
        EvidenceSanitizer.closed_policy().build_benchmark_manifest(_benchmark_manifest_evidence(schema_version=True))


def test_each_allowed_string_field_rejects_unknown_canary() -> None:
    policy = EvidenceSanitizer.closed_policy()
    benchmark_value = _benchmark_manifest_evidence()
    benchmark_machine = replace(benchmark_value.machine, architecture='unknown-canary')
    benchmark_round = replace(benchmark_value.rounds[0], metric='unknown-canary')
    benchmark_metric = replace(benchmark_value.metrics[0], metric='unknown-canary')
    runtime_count = replace(benchmark_value.runtime_counts[0], name='unknown-canary')
    sheet_dimension = replace(benchmark_value.sheet_dimensions[0], sheet='unknown-canary')
    output_bytes = replace(benchmark_value.output_bytes[0], role='unknown-canary')
    all_values = dict(_all_new_artifact_values())
    smoke = all_values['smoke']
    pe_imports = all_values['pe_imports']
    fork = all_values['fork_provenance']
    cargo = all_values['cargo_feature_tree']
    report = all_values['text_report']
    assert isinstance(smoke, evidence.SmokeSummaryEvidence)
    assert isinstance(pe_imports, evidence.PeImportsEvidence)
    assert isinstance(fork, evidence.ForkProvenanceEvidence)
    assert isinstance(cargo, evidence.CargoFeatureTreeEvidence)
    assert isinstance(report, evidence.TextReportEvidence)
    cases = [
        (policy.build_benchmark_manifest, value)
        for value in (
            _benchmark_manifest_evidence(input_alias='unknown-canary'),
            _benchmark_manifest_evidence(reference_label='unknown-canary'),
            _benchmark_manifest_evidence(candidate_label='unknown-canary'),
            _benchmark_manifest_evidence(pipeline='unknown-canary'),
            _benchmark_manifest_evidence(profile='unknown-canary'),
            _benchmark_manifest_evidence(machine=benchmark_machine),
            _benchmark_manifest_evidence(rounds=(benchmark_round,)),
            _benchmark_manifest_evidence(metrics=(benchmark_metric,)),
            _benchmark_manifest_evidence(runtime_counts=(runtime_count,)),
            _benchmark_manifest_evidence(sheet_dimensions=(sheet_dimension,)),
            _benchmark_manifest_evidence(output_bytes=(output_bytes,)),
            _benchmark_manifest_evidence(input_sha256='unknown-canary'),
        )
    ]
    cases.extend(
        (
            (policy.build_command_transcript, _command_transcript_evidence(command_id='unknown-canary')),
            (policy.build_command_transcript, _command_transcript_evidence(tokens=('unknown-canary',))),
            (policy.build_command_transcript, _command_transcript_evidence(tool='unknown-canary')),
            (policy.build_smoke, replace(smoke, pipeline='unknown-canary')),
            (policy.build_smoke, replace(smoke, approved_sheets=('unknown-canary',))),
            (policy.build_smoke, replace(smoke, verdict='unknown-canary')),
            (policy.build_pe_imports, replace(pe_imports, tools=('unknown-canary',))),
            (policy.build_pe_imports, replace(pe_imports, normal_imports=('unknown-canary',))),
            (policy.build_pe_imports, replace(pe_imports, verdict='unknown-canary')),
            (policy.build_fork_provenance, replace(fork, official_url='unknown-canary')),
            (policy.build_fork_provenance, replace(fork, tag='unknown-canary')),
            (policy.build_fork_provenance, replace(fork, allowed_diff_files=('unknown-canary',))),
            (policy.build_cargo_feature_tree, replace(cargo, candidate_label='unknown-canary')),
            (
                policy.build_cargo_feature_tree,
                replace(cargo, packages=(replace(cargo.packages[0], package='unknown-canary'),)),
            ),
            (
                policy.build_cargo_feature_tree,
                replace(cargo, feature_edges=(replace(cargo.feature_edges[0], source_feature='unknown-canary'),)),
            ),
            (policy.build_text_report, replace(report, report_kind='unknown-canary')),
            (policy.build_text_report, replace(report, title='unknown-canary')),
            (
                policy.build_text_report,
                replace(report, checks=(replace(report.checks[0], check_id='unknown-canary'),)),
            ),
        )
    )
    for builder, value in cases:
        with pytest.raises(ValueError):
            builder(value)


def test_mismatch_artifact_omits_expected_and_actual_values() -> None:
    mismatch = evidence.MismatchEvidence(
        sheet=evidence.ApprovedSheet.COST_DETAIL,
        coordinate='C7',
        mismatch_kind=evidence.MismatchKind.VALUE_MISMATCH,
        expected_storage_type=evidence.StorageType.NUMBER,
        actual_storage_type=evidence.StorageType.STRING,
        local_log_sha256='f' * 64,
    )
    artifact = EvidenceSanitizer.closed_policy().build_benchmark_manifest(
        _benchmark_manifest_evidence(mismatches=(mismatch,))
    )
    raw = artifact.content
    assert 'expected_value' not in raw
    assert 'actual_value' not in raw
    assert 'expected=' not in raw
    assert 'actual=' not in raw


def test_nonzero_stdout_stderr_canary_is_not_copied_to_manifest() -> None:
    artifact = EvidenceSanitizer.closed_policy().build_command_transcript(
        replace(_command_transcript_evidence(), exit_code=9, verdict=evidence.HarnessVerdict.CANDIDATE_FAILED)
    )
    raw = artifact.content
    assert 'unknown-canary' not in raw
    assert 'stdout' not in raw.lower()
    assert 'stderr' not in raw.lower()


def test_command_template_rejects_real_paths_and_arguments() -> None:
    policy = EvidenceSanitizer.closed_policy()
    for token in (r'D:\\secret\\input.xlsx', '--input=D:/secret/input.xlsx', 'erp-order-2026.xlsx'):
        with pytest.raises(ValueError):
            policy.build_command_transcript(replace(_command_transcript_evidence(), tokens=(token,)))


def test_all_artifact_kinds_use_typed_sanitizer_builders() -> None:
    policy = EvidenceSanitizer.closed_policy()
    artifacts = tuple(getattr(policy, f'build_{name}')(value) for name, value in _all_new_artifact_values())
    assert {artifact.kind for artifact in artifacts} == set(evidence.EvidenceKind)
    assert not hasattr(policy, 'sanitize')


@pytest.mark.parametrize(
    'sensitive_text',
    (
        r'D:\\private\\artifact.json',
        r'\\server\share\artifact.json',
        r'C:\Users\private\artifact.json',
        '/Users/private/artifact.json',
        'private',
        'private-host',
        'unknown-canary',
        'erp-export-20260413.xlsx',
    ),
)
def test_scan_tree_rejects_drive_unc_users_username_hostname_and_erp_basename(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    sensitive_text: str,
) -> None:
    root = tmp_path / 'evidence'
    root.mkdir()
    (root / 'artifact.json').write_text(json.dumps({'value': sensitive_text}), encoding='utf-8')
    monkeypatch.setattr(evidence.getpass, 'getuser', lambda: 'private')
    monkeypatch.setattr(socket, 'gethostname', lambda: 'private-host')
    with pytest.raises(ValueError, match='sensitive'):
        EvidenceSanitizer.closed_policy().scan_tree(root, sensitive_names=('erp-export-20260413.xlsx',))


@pytest.mark.parametrize('marker', ('expected=secret', 'actual=secret', 'STDOUT: secret', 'STDERR: secret'))
def test_scan_tree_rejects_expected_actual_stdout_and_stderr_markers(tmp_path: Path, marker: str) -> None:
    root = tmp_path / 'evidence'
    root.mkdir()
    (root / 'artifact.txt').write_text(marker, encoding='utf-8')
    with pytest.raises(ValueError, match='sensitive'):
        EvidenceSanitizer.closed_policy().scan_tree(root)


def test_scan_staged_checks_all_staged_evidence_files(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    root = tmp_path / 'repo'
    _init_git_repo(root)
    destination = root / 'docs' / 'performance'
    destination.mkdir(parents=True)
    first, second = _two_command_artifacts()
    tampered = replace(second, content='actual=unknown-canary')
    marker_name, marker_content = evidence._batch_commit_marker((first, tampered))
    for artifact in (first, tampered):
        (destination / artifact.file_name).write_text(artifact.content, encoding='utf-8')
    (destination / marker_name).write_text(marker_content, encoding='utf-8')
    _git(root, 'add', '--', 'docs/performance')
    monkeypatch.setattr(evidence, 'repo_root', lambda: root)
    with pytest.raises(ValueError, match='sensitive'):
        EvidenceSanitizer.closed_policy().scan_staged()


def test_local_path_rejects_parent_traversal(tmp_path: Path) -> None:
    trusted = tmp_path / 'rust' / 'target' / 'perf-local'
    trusted.mkdir(parents=True)
    with pytest.raises(ValueError, match='parent traversal'):
        EvidenceSanitizer.closed_policy().validate_local_destination(
            trusted / '..' / 'escaped.json',
            ignored_roots=(trusted,),
        )


def test_local_path_rejects_junction_to_versioned_directory(tmp_path: Path) -> None:
    trusted = tmp_path / 'rust' / 'target' / 'perf-local'
    versioned = tmp_path / 'docs' / 'performance'
    trusted.mkdir(parents=True)
    versioned.mkdir(parents=True)
    link = trusted / 'linked'
    try:
        link.symlink_to(versioned, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f'junction/symlink creation unavailable: {exc}')
    with pytest.raises(ValueError, match='reparse|symlink'):
        EvidenceSanitizer.closed_policy().validate_local_destination(
            link / 'raw.json',
            ignored_roots=(trusted,),
        )


def test_local_path_rejects_case_normalized_escape(tmp_path: Path) -> None:
    trusted = tmp_path / 'rust' / 'target' / 'Trusted'
    trusted.mkdir(parents=True)
    policy = EvidenceSanitizer.closed_policy()
    accepted = policy.validate_local_destination(trusted / 'nested' / 'raw.json', ignored_roots=(trusted,))
    assert accepted.name == 'raw.json'
    with pytest.raises(ValueError, match='ignored root'):
        policy.validate_local_destination(
            trusted.parent / 'trusted-escape' / 'raw.json',
            ignored_roots=(trusted,),
        )


def test_local_path_rejects_input_output_evidence_collision(tmp_path: Path) -> None:
    shared = tmp_path / 'same.xlsx'
    with pytest.raises(ValueError, match='collision'):
        EvidenceSanitizer.closed_policy().validate_distinct_paths(
            input_path=shared,
            output_path=shared,
            raw_log_path=tmp_path / 'raw.log',
            evidence_path=tmp_path / 'evidence.json',
        )


def test_write_batch_removes_staging_on_sensitive_scan_failure(tmp_path: Path) -> None:
    destination = tmp_path / 'docs' / 'performance'
    destination.mkdir(parents=True)
    artifact = EvidenceSanitizer.closed_policy().build_command_transcript(_command_transcript_evidence())
    bad = replace(artifact, content='actual=unknown-canary')
    with pytest.raises(ValueError, match='sensitive'):
        EvidenceSanitizer.closed_policy().write_batch(
            destination_root=destination,
            artifacts=(bad,),
            cleanup_state=evidence.AttemptState.CLEANUP_COMPLETE,
        )
    assert tuple(destination.iterdir()) == ()


def test_write_batch_removes_this_batch_outputs_on_post_write_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    destination = tmp_path / 'docs' / 'performance'
    destination.mkdir(parents=True)
    policy = EvidenceSanitizer.closed_policy()
    artifact = policy.build_command_transcript(_command_transcript_evidence())
    real_scan_tree = policy.scan_tree
    calls = 0

    def fail_post_write(path: Path, *, sensitive_names: tuple[str, ...] = ()) -> None:
        nonlocal calls
        calls += 1
        if calls >= 3:
            raise OSError('post-write scan failed')
        real_scan_tree(path, sensitive_names=sensitive_names)

    monkeypatch.setattr(policy, 'scan_tree', fail_post_write)
    with pytest.raises(OSError, match='post-write'):
        policy.write_batch(
            destination_root=destination,
            artifacts=(artifact,),
            cleanup_state=evidence.AttemptState.CLEANUP_COMPLETE,
        )
    assert tuple(destination.rglob(artifact.file_name)) == ()


def test_write_batch_rejects_tampered_typed_artifact(tmp_path: Path) -> None:
    destination = tmp_path / 'docs' / 'performance'
    destination.mkdir(parents=True)
    policy = EvidenceSanitizer.closed_policy()
    artifact = policy.build_command_transcript(_command_transcript_evidence())
    tampered = replace(artifact, content='{"safe":"tampered"}\n')
    with pytest.raises(ValueError, match='tampered'):
        policy.write_batch(
            destination_root=destination,
            artifacts=(tampered,),
            cleanup_state=evidence.AttemptState.CLEANUP_COMPLETE,
        )
    assert tuple(destination.iterdir()) == ()


def test_cleanup_failure_leaves_no_versionable_artifact(tmp_path: Path) -> None:
    destination = tmp_path / 'docs' / 'performance'
    destination.mkdir(parents=True)
    artifact = EvidenceSanitizer.closed_policy().build_command_transcript(_command_transcript_evidence())
    with pytest.raises(ValueError, match='cleanup'):
        EvidenceSanitizer.closed_policy().write_batch(
            destination_root=destination,
            artifacts=(artifact,),
            cleanup_state=evidence.AttemptState.FAILED,
        )
    assert tuple(destination.iterdir()) == ()


def test_phase0a_manifest_cannot_be_overwritten(tmp_path: Path) -> None:
    destination = tmp_path / 'docs' / 'performance'
    destination.mkdir(parents=True)
    policy = EvidenceSanitizer.closed_policy()
    artifact = policy.build_benchmark_manifest(_benchmark_manifest_evidence())
    policy.write_batch(
        destination_root=destination,
        artifacts=(artifact,),
        cleanup_state=evidence.AttemptState.CLEANUP_COMPLETE,
    )
    original = (destination / artifact.file_name).read_bytes()
    with pytest.raises(FileExistsError):
        policy.write_batch(
            destination_root=destination,
            artifacts=(artifact,),
            cleanup_state=evidence.AttemptState.CLEANUP_COMPLETE,
        )
    assert (destination / artifact.file_name).read_bytes() == original


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[bytes]:
    git = shutil.which('git')
    assert git is not None
    return subprocess.run(  # noqa: S603 - tests resolve the local Git executable and use synthetic paths only.
        [str(Path(git).resolve()), '-C', str(repo), *args],
        check=True,
        capture_output=True,
    )


def _init_git_repo(path: Path) -> None:
    path.mkdir()
    _git(path, 'init', '--quiet')
    _git(path, 'config', 'user.name', 'Synthetic Tester')
    _git(path, 'config', 'user.email', 'synthetic@example.invalid')
    (path / '.gitignore').write_text('rust/target/\n', encoding='utf-8')
    _git(path, 'add', '.gitignore')
    _git(path, 'commit', '--quiet', '-m', 'init')


def test_scan_staged_reads_sensitive_index_blob_not_worktree(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    root = tmp_path / 'repo'
    _init_git_repo(root)
    path = root / evidence.DEPENDENCY_MANIFEST_RELATIVE_PATH
    path.parent.mkdir(parents=True)
    path.write_text('{"value":"actual=unknown-canary"}', encoding='utf-8')
    _git(root, 'add', '--', path.relative_to(root).as_posix())
    path.write_text('{"value":"safe"}', encoding='utf-8')
    monkeypatch.setattr(evidence, 'repo_root', lambda: root)

    with pytest.raises(ValueError, match='sensitive'):
        EvidenceSanitizer.closed_policy().scan_staged()


def test_staged_index_parser_preserves_special_filename(tmp_path: Path) -> None:
    root = tmp_path / 'repo'
    _init_git_repo(root)
    relative = Path('docs/performance') / 'odd name [组合] artifact.json'
    path = root / relative
    path.parent.mkdir(parents=True)
    path.write_text('{}', encoding='utf-8')
    _git(root, 'add', '--', relative.as_posix())

    entries = evidence._staged_index_entries(root)

    assert tuple(item.path for item in entries) == (relative,)


def test_scan_staged_rejects_type_change_and_symlink_mode(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    root = tmp_path / 'repo'
    _init_git_repo(root)
    relative = Path('docs/performance/type-change.json')
    path = root / relative
    path.parent.mkdir(parents=True)
    path.write_text('{}', encoding='utf-8')
    _git(root, 'add', '--', relative.as_posix())
    _git(root, 'commit', '--quiet', '-m', 'regular evidence')
    path.unlink()
    try:
        path.symlink_to(root / '.gitignore')
    except OSError as exc:
        pytest.skip(f'symlink creation unavailable: {exc}')
    _git(root, 'add', '--', relative.as_posix())
    monkeypatch.setattr(evidence, 'repo_root', lambda: root)

    with pytest.raises(ValueError, match='mode|symlink|type-change'):
        EvidenceSanitizer.closed_policy().scan_staged()


@pytest.mark.parametrize(
    'raw',
    (
        '{"value":"\\u0065xpected=secret"}',
        '{"value":"D:\\u005cprivate\\u005cinput.xlsx"}',
        '{"value":"ＳＴＤＯＵＴ： secret"}',
        '{"value":"unknown-ｃａｎａｒｙ"}',
        '{"actual":"secret"}',
    ),
)
def test_scan_tree_rejects_json_escaped_and_nfkc_sensitive_markers(tmp_path: Path, raw: str) -> None:
    root = tmp_path / 'evidence'
    root.mkdir()
    (root / 'artifact.json').write_text(raw, encoding='utf-8')

    with pytest.raises(ValueError, match='sensitive'):
        EvidenceSanitizer.closed_policy().scan_tree(root)


def test_scan_tree_matches_composed_identity_against_decomposed_unicode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root = tmp_path / 'evidence'
    root.mkdir()
    (root / 'artifact.json').write_text('{"value":"cafe\\u0301"}', encoding='utf-8')
    monkeypatch.setattr(evidence.getpass, 'getuser', lambda: 'café')

    with pytest.raises(ValueError, match='sensitive'):
        EvidenceSanitizer.closed_policy().scan_tree(root)


@pytest.mark.parametrize(
    ('name', 'raw'),
    (
        ('duplicate.json', b'{"safe":1,"safe":2}'),
        ('invalid.json', b'{'),
        ('invalid-utf8.json', b'\xff'),
    ),
)
def test_scan_tree_rejects_duplicate_invalid_json_and_utf8(tmp_path: Path, name: str, raw: bytes) -> None:
    root = tmp_path / 'evidence'
    root.mkdir()
    (root / name).write_bytes(raw)

    with pytest.raises(ValueError, match='JSON|UTF-8|duplicate'):
        EvidenceSanitizer.closed_policy().scan_tree(root)


def test_sanitized_artifact_is_private_and_rebuilt_from_typed_source(tmp_path: Path) -> None:
    assert not hasattr(evidence, 'SanitizedArtifact')
    policy = EvidenceSanitizer.closed_policy()
    artifact = policy.build_command_transcript(_command_transcript_evidence())
    assert artifact.source == _command_transcript_evidence()
    with pytest.raises(TypeError):
        artifact.payload['extra'] = 'safe'  # type: ignore[index]

    forged_payload = json.loads(artifact.content)
    forged_payload['extra'] = 'safe'
    forged = replace(
        artifact,
        payload=forged_payload,
        content=json.dumps(forged_payload, ensure_ascii=False, indent=2) + '\n',
    )
    destination = tmp_path / 'docs' / 'performance'
    destination.mkdir(parents=True)
    with pytest.raises(ValueError, match='typed source|tampered'):
        policy.write_batch(
            destination_root=destination,
            artifacts=(forged,),
            cleanup_state=evidence.AttemptState.CLEANUP_COMPLETE,
            scan_staged=False,
        )


def _two_command_artifacts() -> tuple[object, object]:
    policy = EvidenceSanitizer.closed_policy()
    first = policy.build_command_transcript(_command_transcript_evidence())
    second = policy.build_command_transcript(
        replace(_command_transcript_evidence(), command_id=evidence.CommandId.PHASE0H_SMOKE)
    )
    return first, second


def test_write_batch_publishes_hash_bound_commit_marker_last(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    root = tmp_path / 'repo'
    _init_git_repo(root)
    destination = root / 'docs' / 'performance'
    destination.mkdir(parents=True)
    policy = EvidenceSanitizer.closed_policy()
    artifacts = _two_command_artifacts()
    linked: list[str] = []
    real_link = evidence.os.link

    def record_link(source: object, target: object) -> None:
        linked.append(Path(str(target).removeprefix('\\\\?\\')).name)
        real_link(source, target)

    monkeypatch.setattr(evidence.os, 'link', record_link)
    policy.write_batch(
        destination_root=destination,
        artifacts=artifacts,
        cleanup_state=evidence.AttemptState.CLEANUP_COMPLETE,
        scan_staged=False,
    )

    marker = next(destination.glob('batch-*.commit.json'))
    payload = json.loads(marker.read_text(encoding='utf-8'))
    assert linked[-1] == marker.name
    assert [item['file_name'] for item in payload['artifacts']] == [item.file_name for item in artifacts]
    for item in payload['artifacts']:
        assert item['sha256'] == evidence._sha256_file(destination / item['file_name'])
    assert len(payload['batch_sha256']) == 64
    _git(root, 'add', '--', 'docs/performance')
    monkeypatch.setattr(evidence, 'repo_root', lambda: root)
    policy.scan_staged()


@pytest.mark.parametrize('failure', ('winner_replace', 'second_failure', 'system_exit'))
def test_write_batch_identity_safe_rollback_before_marker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure: str,
) -> None:
    destination = tmp_path / 'docs' / 'performance'
    destination.mkdir(parents=True)
    policy = EvidenceSanitizer.closed_policy()
    artifacts = _two_command_artifacts()
    real_link = evidence.os.link
    calls = 0

    def fail_after_first(source: object, target: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            real_link(source, target)
            if failure == 'winner_replace':
                final = Path(str(target).removeprefix('\\\\?\\'))
                final.unlink()
                final.write_text('winner', encoding='utf-8')
            return
        if failure == 'system_exit':
            raise SystemExit(9)
        raise OSError('second artifact failed')

    monkeypatch.setattr(evidence.os, 'link', fail_after_first)
    expected = SystemExit if failure == 'system_exit' else OSError
    with pytest.raises(expected):
        policy.write_batch(
            destination_root=destination,
            artifacts=artifacts,
            cleanup_state=evidence.AttemptState.CLEANUP_COMPLETE,
            scan_staged=False,
        )

    first_path = destination / artifacts[0].file_name
    if failure == 'winner_replace':
        assert first_path.read_text(encoding='utf-8') == 'winner'
    else:
        assert not first_path.exists()
    assert not tuple(destination.glob('batch-*.commit.json'))


def test_scan_tree_rejects_nested_junction_escape(tmp_path: Path) -> None:
    root = tmp_path / 'evidence'
    nested = root / 'nested'
    outside = tmp_path / 'outside'
    nested.mkdir(parents=True)
    outside.mkdir()
    (outside / 'artifact.json').write_text('{}', encoding='utf-8')
    link = nested / 'escape'
    try:
        link.symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f'junction/symlink creation unavailable: {exc}')

    with pytest.raises(ValueError, match='reparse|symlink|escape'):
        EvidenceSanitizer.closed_policy().scan_tree(root)


def _committed_synthetic_batch(root: Path) -> tuple[Path, tuple[object, object]]:
    _init_git_repo(root)
    destination = root / 'docs' / 'performance'
    destination.mkdir(parents=True)
    artifacts = _two_command_artifacts()
    EvidenceSanitizer.closed_policy().write_batch(
        destination_root=destination,
        artifacts=artifacts,
        cleanup_state=evidence.AttemptState.CLEANUP_COMPLETE,
        scan_staged=False,
    )
    _git(root, 'add', '--', 'docs/performance')
    _git(root, 'commit', '--quiet', '-m', 'synthetic evidence batch')
    return destination, artifacts


@pytest.mark.parametrize('deleted_entry', ('marker', 'artifact'))
def test_scan_staged_rejects_deleted_batch_entry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    deleted_entry: str,
) -> None:
    root = tmp_path / 'repo'
    destination, artifacts = _committed_synthetic_batch(root)
    target = next(destination.glob('batch-*.commit.json'))
    if deleted_entry == 'artifact':
        target = destination / artifacts[0].file_name
    target.unlink()
    _git(root, 'add', '-u', '--', 'docs/performance')
    monkeypatch.setattr(evidence, 'repo_root', lambda: root)

    with pytest.raises(ValueError, match='delet|missing|batch'):
        EvidenceSanitizer.closed_policy().scan_staged()


def test_all_seven_staged_readers_round_trip_typed_sources() -> None:
    policy = EvidenceSanitizer.closed_policy()
    readers = {
        'benchmark_manifest': policy.read_benchmark_manifest,
        'command_transcript': policy.read_command_transcript,
        'smoke': policy.read_smoke,
        'pe_imports': policy.read_pe_imports,
        'fork_provenance': policy.read_fork_provenance,
        'cargo_feature_tree': policy.read_cargo_feature_tree,
        'text_report': policy.read_text_report,
    }
    for name, value in _all_new_artifact_values():
        artifact = getattr(policy, f'build_{name}')(value)
        source = readers[name](artifact.file_name, artifact.content.encode('utf-8'))
        rebuilt = getattr(policy, f'build_{name}')(source)
        assert rebuilt.file_name == artifact.file_name
        assert rebuilt.content == artifact.content


def test_scan_staged_rejects_old_marker_without_artifact_kind(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    root = tmp_path / 'repo'
    _init_git_repo(root)
    destination = root / 'docs' / 'performance'
    destination.mkdir(parents=True)
    artifact = EvidenceSanitizer.closed_policy().build_command_transcript(_command_transcript_evidence())
    (destination / artifact.file_name).write_text(artifact.content, encoding='utf-8')
    marker_name, marker_content = evidence._batch_commit_marker((artifact,))
    payload = json.loads(marker_content)
    payload['artifacts'][0].pop('kind', None)
    basis = {'schema_version': 1, 'artifacts': payload['artifacts']}
    payload['batch_sha256'] = evidence._sha256_bytes(
        json.dumps(basis, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    )
    old_marker = destination / marker_name
    old_marker.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    _git(root, 'add', '--', 'docs/performance')
    monkeypatch.setattr(evidence, 'repo_root', lambda: root)

    with pytest.raises(ValueError, match='kind|schema'):
        EvidenceSanitizer.closed_policy().scan_staged()


def _stage_manual_batch(
    root: Path,
    *,
    kind: evidence.EvidenceKind,
    file_name: str,
    content: str,
) -> None:
    _init_git_repo(root)
    destination = root / 'docs' / 'performance'
    destination.mkdir(parents=True)
    (destination / file_name).write_text(content, encoding='utf-8')
    records = [
        {
            'kind': kind.value,
            'file_name': file_name,
            'sha256': evidence._sha256_bytes(content.encode('utf-8')),
        }
    ]
    basis = {'schema_version': 1, 'artifacts': records}
    batch_sha = evidence._sha256_bytes(json.dumps(basis, ensure_ascii=False, separators=(',', ':')).encode('utf-8'))
    marker = {**basis, 'batch_sha256': batch_sha}
    (destination / f'batch-{batch_sha[:16]}.commit.json').write_text(
        json.dumps(marker, ensure_ascii=False, indent=2) + '\n',
        encoding='utf-8',
    )
    _git(root, 'add', '--', 'docs/performance')


@pytest.mark.parametrize('mutation', ('handwritten', 'extra', 'missing', 'wrong_kind', 'wrong_filename'))
def test_scan_staged_rejects_non_typed_or_misbound_artifact(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    mutation: str,
) -> None:
    valid = EvidenceSanitizer.closed_policy().build_command_transcript(_command_transcript_evidence())
    payload = json.loads(valid.content)
    kind = evidence.EvidenceKind.COMMAND
    file_name = valid.file_name
    if mutation == 'handwritten':
        payload = {'safe': 'yes'}
    elif mutation == 'extra':
        payload['extra'] = 'safe'
    elif mutation == 'missing':
        payload.pop('verdict')
    elif mutation == 'wrong_kind':
        kind = evidence.EvidenceKind.SMOKE
    else:
        file_name = 'command-phase0h-smoke.json'
    content = json.dumps(payload, ensure_ascii=False, indent=2) + '\n'
    root = tmp_path / 'repo'
    _stage_manual_batch(root, kind=kind, file_name=file_name, content=content)
    monkeypatch.setattr(evidence, 'repo_root', lambda: root)

    with pytest.raises(ValueError, match='schema|filename|typed|closed'):
        EvidenceSanitizer.closed_policy().scan_staged()


def test_marker_replacement_before_post_scan_preserves_winner_and_rolls_back_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    destination = tmp_path / 'docs' / 'performance'
    destination.mkdir(parents=True)
    policy = EvidenceSanitizer.closed_policy()
    artifacts = _two_command_artifacts()
    real_link = evidence.os.link
    calls = 0
    winner_path: Path | None = None

    def replace_marker(source: object, target: object) -> None:
        nonlocal calls, winner_path
        calls += 1
        real_link(source, target)
        if calls == len(artifacts) + 1:
            winner_path = Path(str(target).removeprefix('\\\\?\\'))
            winner_path.unlink()
            winner_path.write_text('winner marker', encoding='utf-8')

    monkeypatch.setattr(evidence.os, 'link', replace_marker)
    with pytest.raises(OSError, match='marker identity'):
        policy.write_batch(
            destination_root=destination,
            artifacts=artifacts,
            cleanup_state=evidence.AttemptState.CLEANUP_COMPLETE,
            scan_staged=False,
        )

    assert winner_path is not None
    assert winner_path.read_text(encoding='utf-8') == 'winner marker'
    assert all(not (destination / artifact.file_name).exists() for artifact in artifacts)

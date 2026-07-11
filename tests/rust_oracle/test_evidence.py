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


def _registry_lock(checksum: str) -> str:
    return (
        '[[package]]\n'
        'name = "rust_xlsxwriter"\n'
        'version = "0.96.0"\n'
        'source = "registry+https://github.com/rust-lang/crates.io-index"\n'
        f'checksum = "{checksum}"\n'
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


def test_dependency_cli_diff_listing_does_not_filter_deleted_paths(
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

    assert calls[5][:5] == ('git', '-C', str((tmp_path / 'fork').resolve()), 'diff', '--name-only')
    assert not any(argument.startswith('--diff-filter') for argument in calls[5])
    assert f'{_UPSTREAM_BASE}..{_REVISION}' in calls[5]
    assert f'{_UPSTREAM_BASE}..HEAD' not in calls[5]


def test_dependency_generation_create_new_loser_does_not_delete_winner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cargo_manifest, cargo_metadata, cargo_lock, _dependency_manifest = _revision_fixtures(tmp_path)
    live: dict[str, object] = {
        'fork_head': _REVISION,
        'cargo_manifest_text': cargo_manifest.read_text(encoding='utf-8'),
        'cargo_metadata_text': cargo_metadata.read_text(encoding='utf-8'),
        'cargo_lock_text': cargo_lock.read_text(encoding='utf-8'),
        'pre_pin_lock_text': _registry_lock(_CHECKSUM),
        'diff_files': _MANDATORY_DIFF_FILES,
        'diff_sha256': _DIFF_SHA,
        'fallback_used': False,
        'checksum': _CHECKSUM,
        'pr_query_output': '[]',
    }
    monkeypatch.setattr(evidence, '_collect_live_evidence', lambda **_kwargs: (live, (), ()))
    output = tmp_path / 'generated-dependency.json'

    def simulate_winner_then_create_new_failure(_output: Path, _value: DependencyEvidence) -> None:
        output.write_text('winner manifest', encoding='utf-8')
        raise FileExistsError(output)

    monkeypatch.setattr(EvidenceSanitizer, 'write_dependency_manifest', simulate_winner_then_create_new_failure)

    with pytest.raises(FileExistsError):
        evidence.generate_dependency_manifest(
            fork_checkout=tmp_path / 'fork',
            cargo_manifest=cargo_manifest,
            cargo_lock=cargo_lock,
            pre_pin_commit=_PRE_PIN_COMMIT,
            local_log_root=tmp_path / 'local-logs',
            output=output,
        )

    assert output.read_text(encoding='utf-8') == 'winner manifest'


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

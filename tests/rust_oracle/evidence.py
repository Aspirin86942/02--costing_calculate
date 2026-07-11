from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from tests.rust_oracle.repo_paths import repo_root

UPSTREAM_URL = 'https://github.com/jmcnamara/rust_xlsxwriter.git'
UPSTREAM_TAG = 'v0.96.0'
UPSTREAM_BASE_REVISION = '9134de25afadaee955d0f821862338e3d046a338'
CRATES_IO_CHECKSUM = 'dd1746025420e17b5d62528b930e550e016e857038794d74e169018126ef3d14'
FORK_URL = 'https://github.com/Aspirin86942/rust_xlsxwriter.git'
FORK_BRANCH = 'costing-fallible-temp-io-v0.96.0'
CRATE_VERSION = '0.96.0'

MANDATORY_DIFF_FILES = (
    'src/packager.rs',
    'src/workbook.rs',
    'src/workbook/tests.rs',
    'src/worksheet.rs',
    'src/worksheet/tests.rs',
)
XMLWRITER_PATH = 'src/xmlwriter.rs'
FALLBACK_TRIGGER_TESTS = frozenset(
    {
        'row_start_write_failure_returns_original_io_error',
        'cell_body_write_failure_returns_original_io_error',
        'row_end_write_failure_returns_original_io_error',
    }
)

_HEX40 = re.compile(r'^[0-9a-f]{40}$')
_HEX64 = re.compile(r'^[0-9a-f]{64}$')
_DRIVE_PATH = re.compile(r'(?i)(?:^|[^a-z0-9])[a-z]:[\\/]')
_UNC_PATH = re.compile(r'(?<!:)(?:\\\\|//)[a-z0-9_.-]+[\\/]', re.IGNORECASE)
_SENSITIVE_TOKEN = re.compile(r'(?i)user|canary|stdout|stderr')
_EXPECTED_KEYS = (
    'upstream_url',
    'upstream_tag',
    'upstream_base_revision',
    'crates_io_checksum',
    'pre_pin_costing_commit',
    'fork_url',
    'fork_revision',
    'allowed_diff_files',
    'xmlwriter_fallback_used',
    'xmlwriter_fallback_trigger_test',
    'diff_sha256',
    'local_unversioned_log_sha256',
    'upstream_pr_url',
    'verdict',
)


@dataclass(frozen=True)
class DependencyEvidence:
    upstream_url: Literal['https://github.com/jmcnamara/rust_xlsxwriter.git']
    upstream_tag: Literal['v0.96.0']
    upstream_base_revision: str
    crates_io_checksum: str
    pre_pin_costing_commit: str
    fork_url: Literal['https://github.com/Aspirin86942/rust_xlsxwriter.git']
    fork_revision: str
    allowed_diff_files: tuple[str, ...]
    xmlwriter_fallback_used: bool
    xmlwriter_fallback_trigger_test: str | None
    diff_sha256: str
    local_unversioned_log_sha256: str
    upstream_pr_url: None
    verdict: Literal['VALIDATED']


class EvidenceSanitizer:
    @staticmethod
    def write_dependency_manifest(output: Path, value: DependencyEvidence) -> None:
        payload = EvidenceSanitizer.dependency_payload(value)
        output.parent.mkdir(parents=True, exist_ok=True)
        # 先完成闭合校验，再以 x 模式创建，既拒绝覆盖，也避免校验失败留下半成品。
        created = False
        try:
            with output.open('x', encoding='utf-8', newline='\n') as stream:
                created = True
                json.dump(payload, stream, ensure_ascii=False, indent=2)
                stream.write('\n')
        except Exception:
            # 只有成功取得 create-new 所有权的调用才可清理，loser 绝不能删除 winner 文件。
            if created:
                output.unlink(missing_ok=True)
            raise

    @staticmethod
    def dependency_payload(value: DependencyEvidence) -> dict[str, object]:
        EvidenceSanitizer._validate_dependency_evidence(value)
        # 安全边界必须显式列出允许落盘的字段，禁止 asdict 把未来字段意外泄露进 manifest。
        return {
            'upstream_url': value.upstream_url,
            'upstream_tag': value.upstream_tag,
            'upstream_base_revision': value.upstream_base_revision,
            'crates_io_checksum': value.crates_io_checksum,
            'pre_pin_costing_commit': value.pre_pin_costing_commit,
            'fork_url': value.fork_url,
            'fork_revision': value.fork_revision,
            'allowed_diff_files': list(value.allowed_diff_files),
            'xmlwriter_fallback_used': value.xmlwriter_fallback_used,
            'xmlwriter_fallback_trigger_test': value.xmlwriter_fallback_trigger_test,
            'diff_sha256': value.diff_sha256,
            'local_unversioned_log_sha256': value.local_unversioned_log_sha256,
            'upstream_pr_url': None,
            'verdict': value.verdict,
        }

    @staticmethod
    def read_dependency_manifest(path: Path) -> DependencyEvidence:
        payload = json.loads(path.read_text(encoding='utf-8'))
        if not isinstance(payload, dict) or tuple(payload) != _EXPECTED_KEYS:
            raise ValueError('dependency manifest must use the exact closed schema')
        allowed_diff_files = payload['allowed_diff_files']
        if not isinstance(allowed_diff_files, list) or not all(isinstance(item, str) for item in allowed_diff_files):
            raise ValueError('allowed_diff_files must be a string list')
        value = DependencyEvidence(
            upstream_url=payload['upstream_url'],
            upstream_tag=payload['upstream_tag'],
            upstream_base_revision=payload['upstream_base_revision'],
            crates_io_checksum=payload['crates_io_checksum'],
            pre_pin_costing_commit=payload['pre_pin_costing_commit'],
            fork_url=payload['fork_url'],
            fork_revision=payload['fork_revision'],
            allowed_diff_files=tuple(allowed_diff_files),
            xmlwriter_fallback_used=payload['xmlwriter_fallback_used'],
            xmlwriter_fallback_trigger_test=payload['xmlwriter_fallback_trigger_test'],
            diff_sha256=payload['diff_sha256'],
            local_unversioned_log_sha256=payload['local_unversioned_log_sha256'],
            upstream_pr_url=payload['upstream_pr_url'],
            verdict=payload['verdict'],
        )
        EvidenceSanitizer._validate_dependency_evidence(value)
        return value

    @staticmethod
    def verify_rust_xlsxwriter_revision_consistency(
        *,
        fork_head: str,
        cargo_manifest: Path,
        cargo_metadata: Path,
        cargo_lock: Path,
        dependency_manifest: Path,
    ) -> str:
        return EvidenceSanitizer._verify_revision_contents(
            fork_head=fork_head,
            cargo_manifest_text=cargo_manifest.read_text(encoding='utf-8'),
            cargo_metadata_text=cargo_metadata.read_text(encoding='utf-8'),
            cargo_lock_text=cargo_lock.read_text(encoding='utf-8'),
            dependency_manifest_text=dependency_manifest.read_text(encoding='utf-8'),
        )

    @staticmethod
    def verify_registry_checksum(pre_pin_lock_text: str, archives: tuple[Path, ...]) -> str:
        checksum = _registry_checksum_from_lock(pre_pin_lock_text)
        if checksum != CRATES_IO_CHECKSUM:
            raise ValueError('registry lock checksum does not match the approved crates.io checksum')
        if not archives:
            raise ValueError('no cached rust_xlsxwriter-0.96.0.crate archive found')
        for archive in archives:
            if _sha256_file(archive) != checksum:
                raise ValueError('cached archive checksum does not match the approved crates.io checksum')
        return checksum

    @staticmethod
    def verify_empty_pr_query(raw_json: str) -> None:
        payload = json.loads(raw_json)
        if not isinstance(payload, list) or payload:
            raise ValueError('an upstream PR exists or the upstream PR query was not an empty list')

    @staticmethod
    def _verify_revision_contents(
        *,
        fork_head: str,
        cargo_manifest_text: str,
        cargo_metadata_text: str,
        cargo_lock_text: str,
        dependency_manifest_text: str,
    ) -> str:
        revisions = {
            'fork HEAD': fork_head.strip(),
            'workspace manifest': _revision_from_cargo_manifest(cargo_manifest_text),
            'Cargo metadata': _revision_from_cargo_metadata(cargo_metadata_text),
            'Cargo.lock': _revision_from_cargo_lock(cargo_lock_text),
            'dependency manifest': _revision_from_dependency_manifest(dependency_manifest_text),
        }
        for name, revision in revisions.items():
            _require_hash(revision, 40, name)
        if len(set(revisions.values())) != 1:
            raise ValueError(f'rust_xlsxwriter revision mismatch: {revisions!r}')
        return fork_head.strip()

    @staticmethod
    def _validate_dependency_evidence(value: DependencyEvidence) -> None:
        if value.upstream_url != UPSTREAM_URL:
            raise ValueError('unexpected upstream URL')
        if value.upstream_tag != UPSTREAM_TAG:
            raise ValueError('unexpected upstream tag')
        if value.upstream_base_revision != UPSTREAM_BASE_REVISION:
            raise ValueError('unexpected upstream base revision')
        if value.crates_io_checksum != CRATES_IO_CHECKSUM:
            raise ValueError('unexpected crates.io checksum')
        if value.fork_url != FORK_URL:
            raise ValueError('unexpected fork URL')
        if value.verdict != 'VALIDATED':
            raise ValueError('dependency verdict must be VALIDATED')
        if value.upstream_pr_url is not None:
            raise ValueError('upstream_pr_url must be null')
        if type(value.xmlwriter_fallback_used) is not bool:
            raise ValueError('xmlwriter_fallback_used must be a boolean')

        _require_hash(value.upstream_base_revision, 40, 'upstream_base_revision')
        _require_hash(value.crates_io_checksum, 64, 'crates_io_checksum')
        _require_hash(value.pre_pin_costing_commit, 40, 'pre_pin_costing_commit')
        _require_hash(value.fork_revision, 40, 'fork_revision')
        _require_hash(value.diff_sha256, 64, 'diff_sha256')
        _require_hash(value.local_unversioned_log_sha256, 64, 'local_unversioned_log_sha256')

        for item in _all_manifest_strings(value):
            _reject_sensitive_string(item)

        expected_files = MANDATORY_DIFF_FILES
        if value.xmlwriter_fallback_used:
            expected_files = (*MANDATORY_DIFF_FILES, XMLWRITER_PATH)
            if value.xmlwriter_fallback_trigger_test not in FALLBACK_TRIGGER_TESTS:
                raise ValueError('xmlwriter fallback requires one of the three named row fault gates')
        elif value.xmlwriter_fallback_trigger_test is not None:
            raise ValueError('xmlwriter fallback trigger must be null when fallback is unused')
        if value.allowed_diff_files != expected_files:
            raise ValueError('allowed_diff_files does not match the closed fork diff allowlist')


def _all_manifest_strings(value: DependencyEvidence) -> tuple[str, ...]:
    scalar_values = (
        value.upstream_url,
        value.upstream_tag,
        value.upstream_base_revision,
        value.crates_io_checksum,
        value.pre_pin_costing_commit,
        value.fork_url,
        value.fork_revision,
        value.diff_sha256,
        value.local_unversioned_log_sha256,
        value.verdict,
    )
    if not all(isinstance(item, str) for item in scalar_values):
        raise ValueError('dependency manifest string fields must be strings')
    if not isinstance(value.allowed_diff_files, tuple) or not all(
        isinstance(item, str) for item in value.allowed_diff_files
    ):
        raise ValueError('allowed_diff_files must be a string tuple')
    strings = (*scalar_values, *value.allowed_diff_files)
    if value.xmlwriter_fallback_trigger_test is not None:
        if not isinstance(value.xmlwriter_fallback_trigger_test, str):
            raise ValueError('xmlwriter fallback trigger must be a string or null')
        strings = (*strings, value.xmlwriter_fallback_trigger_test)
    return strings


def _reject_sensitive_string(value: str) -> None:
    # 不能用“包含 https 就跳过”的宽松规则；两个 URL 也走同一扫描器，只因不命中模式而通过。
    if _DRIVE_PATH.search(value) or _UNC_PATH.search(value) or _SENSITIVE_TOKEN.search(value):
        raise ValueError('dependency manifest contains a sensitive string')


def _require_hash(value: object, length: Literal[40, 64], name: str) -> str:
    pattern = _HEX40 if length == 40 else _HEX64
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise ValueError(f'{name} must be a {length}-character lowercase hexadecimal hash')
    return value


def _revision_from_cargo_manifest(raw: str) -> str:
    payload = tomllib.loads(raw)
    dependency = payload.get('workspace', {}).get('dependencies', {}).get('rust_xlsxwriter')
    if not isinstance(dependency, dict) or dependency.get('git') != FORK_URL:
        raise ValueError('workspace manifest must use the approved rust_xlsxwriter fork URL')
    return _require_hash(dependency.get('rev'), 40, 'workspace manifest revision')


def _revision_from_cargo_metadata(raw: str) -> str:
    payload = json.loads(raw)
    packages = payload.get('packages') if isinstance(payload, dict) else None
    matches = [item for item in packages or () if isinstance(item, dict) and item.get('name') == 'rust_xlsxwriter']
    if len(matches) != 1:
        raise ValueError('Cargo metadata must contain exactly one rust_xlsxwriter package')
    return _revision_from_git_source(matches[0].get('source'), 'Cargo metadata')


def _revision_from_cargo_lock(raw: str) -> str:
    payload = tomllib.loads(raw)
    packages = payload.get('package', [])
    matches = [
        item
        for item in packages
        if isinstance(item, dict) and item.get('name') == 'rust_xlsxwriter' and item.get('version') == CRATE_VERSION
    ]
    if len(matches) != 1:
        raise ValueError('Cargo.lock must contain exactly one rust_xlsxwriter 0.96.0 package')
    return _revision_from_git_source(matches[0].get('source'), 'Cargo.lock')


def _revision_from_git_source(source: object, name: str) -> str:
    if not isinstance(source, str):
        raise ValueError(f'{name} rust_xlsxwriter source must be a string')
    match = re.fullmatch(rf'git\+{re.escape(FORK_URL)}\?rev=([0-9a-f]{{40}})#([0-9a-f]{{40}})', source)
    if match is None or match.group(1) != match.group(2):
        raise ValueError(f'{name} rust_xlsxwriter source must pin one exact fork revision')
    return match.group(1)


def _revision_from_dependency_manifest(raw: str) -> str:
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError('dependency manifest must be a JSON object')
    return _require_hash(payload.get('fork_revision'), 40, 'dependency manifest revision')


def _registry_checksum_from_lock(raw: str) -> str:
    payload = tomllib.loads(raw)
    matches = [
        item
        for item in payload.get('package', [])
        if isinstance(item, dict) and item.get('name') == 'rust_xlsxwriter' and item.get('version') == CRATE_VERSION
    ]
    if len(matches) != 1 or matches[0].get('source') != 'registry+https://github.com/rust-lang/crates.io-index':
        raise ValueError('pre-pin lock must contain the crates.io rust_xlsxwriter 0.96.0 package')
    return _require_hash(matches[0].get('checksum'), 64, 'registry lock checksum')


def _sha256_file(path: Path) -> str:
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


@dataclass(frozen=True)
class _CommandResult:
    command: tuple[str, ...]
    stdout: str
    stderr: str


def _run_command(executable: str, *args: str) -> _CommandResult:
    resolved = shutil.which(executable)
    if resolved is None:
        raise FileNotFoundError(f'required executable not found: {executable}')
    command = (resolved, *args)
    completed = subprocess.run(  # noqa: S603 - executable is resolved from a fixed allowlist name.
        command,
        capture_output=True,
        text=True,
        encoding='utf-8',
        check=True,
    )
    return _CommandResult(command, completed.stdout, completed.stderr)


def _collect_live_evidence(
    *,
    fork_checkout: Path,
    cargo_manifest: Path,
    cargo_lock: Path,
    pre_pin_commit: str,
) -> tuple[dict[str, object], tuple[_CommandResult, ...], tuple[Path, ...]]:
    _require_hash(pre_pin_commit, 40, 'pre_pin_commit')
    root = repo_root().resolve()
    fork = fork_checkout.resolve()
    manifest = cargo_manifest.resolve()
    lock = cargo_lock.resolve()
    head_result = _run_command('git', '-C', str(fork), 'rev-parse', 'HEAD')
    head = _require_hash(head_result.stdout.strip(), 40, 'fork HEAD')
    revision_range = f'{UPSTREAM_BASE_REVISION}..{head}'
    commands = (
        head_result,
        _run_command('git', '-C', str(fork), 'rev-parse', f'{UPSTREAM_TAG}^{{commit}}'),
        _run_command('git', '-C', str(fork), 'remote', 'get-url', 'upstream'),
        _run_command('git', '-C', str(fork), 'remote', 'get-url', 'origin'),
        _run_command('git', '-C', str(fork), 'branch', '--show-current'),
        _run_command(
            'git',
            '-C',
            str(fork),
            'diff',
            '--name-only',
            revision_range,
            '--',
        ),
        _run_command(
            'git',
            '-C',
            str(fork),
            'diff',
            '--no-ext-diff',
            '--binary',
            revision_range,
            '--',
        ),
        _run_command(
            'cargo',
            'metadata',
            '--locked',
            '--manifest-path',
            str(manifest),
            '--format-version',
            '1',
        ),
        _run_command('git', '-C', str(root), 'show', f'{pre_pin_commit}:rust/Cargo.lock'),
        _run_command(
            'gh',
            'pr',
            'list',
            '--repo',
            'jmcnamara/rust_xlsxwriter',
            '--state',
            'all',
            '--head',
            f'Aspirin86942:{FORK_BRANCH}',
            '--json',
            'url',
        ),
    )
    (
        head_result,
        tag_result,
        upstream_remote_result,
        origin_remote_result,
        branch_result,
        diff_names_result,
        diff_result,
        metadata_result,
        pre_pin_lock_result,
        pr_result,
    ) = commands

    if tag_result.stdout.strip() != UPSTREAM_BASE_REVISION:
        raise ValueError('upstream tag does not resolve to the approved base revision')
    if upstream_remote_result.stdout.strip() != UPSTREAM_URL or origin_remote_result.stdout.strip() != FORK_URL:
        raise ValueError('fork remotes do not match the approved upstream and origin URLs')
    if branch_result.stdout.strip() != FORK_BRANCH:
        raise ValueError('fork checkout is not on the approved fixed branch')
    EvidenceSanitizer.verify_empty_pr_query(pr_result.stdout)

    diff_files = tuple(line for line in diff_names_result.stdout.splitlines() if line)
    fallback_used = XMLWRITER_PATH in diff_files
    expected_files = (*MANDATORY_DIFF_FILES, XMLWRITER_PATH) if fallback_used else MANDATORY_DIFF_FILES
    if diff_files != expected_files:
        raise ValueError(f'fork diff does not match the closed allowlist: {diff_files!r}')

    cargo_home = Path(os.environ.get('CARGO_HOME', Path.home() / '.cargo')).resolve()
    archive_root = cargo_home / 'registry' / 'cache'
    archives = tuple(sorted(archive_root.rglob(f'rust_xlsxwriter-{CRATE_VERSION}.crate')))
    checksum = EvidenceSanitizer.verify_registry_checksum(pre_pin_lock_result.stdout, archives)
    current_lock_text = lock.read_text(encoding='utf-8')
    live = {
        'fork_head': head,
        'cargo_manifest_text': manifest.read_text(encoding='utf-8'),
        'cargo_metadata_text': metadata_result.stdout,
        'cargo_lock_text': current_lock_text,
        'pre_pin_lock_text': pre_pin_lock_result.stdout,
        'diff_files': diff_files,
        'diff_sha256': _sha256_text(diff_result.stdout),
        'fallback_used': fallback_used,
        'checksum': checksum,
        'pr_query_output': pr_result.stdout,
    }
    return live, commands, archives


def _raw_log_text(commands: tuple[_CommandResult, ...], archives: tuple[Path, ...]) -> str:
    sections: list[str] = []
    for index, result in enumerate(commands, start=1):
        sections.extend(
            (
                f'command[{index}]={json.dumps(result.command)}',
                f'stdout[{index}]:',
                result.stdout,
                f'stderr[{index}]:',
                result.stderr,
            )
        )
    for archive in archives:
        sections.append(f'archive={archive} sha256={_sha256_file(archive)}')
    return '\n'.join(sections)


def _build_dependency_evidence(
    *,
    live: dict[str, object],
    pre_pin_commit: str,
    local_log_sha256: str,
) -> DependencyEvidence:
    fallback_used = live['fallback_used']
    if type(fallback_used) is not bool:
        raise ValueError('internal fallback state must be boolean')
    return DependencyEvidence(
        upstream_url=UPSTREAM_URL,
        upstream_tag=UPSTREAM_TAG,
        upstream_base_revision=UPSTREAM_BASE_REVISION,
        crates_io_checksum=CRATES_IO_CHECKSUM,
        pre_pin_costing_commit=pre_pin_commit,
        fork_url=FORK_URL,
        fork_revision=live['fork_head'],
        allowed_diff_files=live['diff_files'],
        xmlwriter_fallback_used=fallback_used,
        xmlwriter_fallback_trigger_test=None,
        diff_sha256=live['diff_sha256'],
        local_unversioned_log_sha256=local_log_sha256,
        upstream_pr_url=None,
        verdict='VALIDATED',
    )


def generate_dependency_manifest(
    *,
    fork_checkout: Path,
    cargo_manifest: Path,
    cargo_lock: Path,
    pre_pin_commit: str,
    local_log_root: Path,
    output: Path,
) -> None:
    if output.exists():
        raise FileExistsError(output)
    live, commands, archives = _collect_live_evidence(
        fork_checkout=fork_checkout,
        cargo_manifest=cargo_manifest,
        cargo_lock=cargo_lock,
        pre_pin_commit=pre_pin_commit,
    )
    raw_log = _raw_log_text(commands, archives)
    local_log_root.mkdir(parents=True, exist_ok=True)
    log_path = local_log_root / 'rust-xlsxwriter-0.96.0-dependency-provenance.log'
    with log_path.open('x', encoding='utf-8', newline='\n') as stream:
        stream.write(raw_log)
    log_sha = _sha256_file(log_path)
    value = _build_dependency_evidence(live=live, pre_pin_commit=pre_pin_commit, local_log_sha256=log_sha)
    payload_text = json.dumps(EvidenceSanitizer.dependency_payload(value))
    EvidenceSanitizer._verify_revision_contents(
        fork_head=live['fork_head'],
        cargo_manifest_text=live['cargo_manifest_text'],
        cargo_metadata_text=live['cargo_metadata_text'],
        cargo_lock_text=live['cargo_lock_text'],
        dependency_manifest_text=payload_text,
    )
    EvidenceSanitizer.write_dependency_manifest(output, value)


def verify_dependency_manifest(
    *,
    fork_checkout: Path,
    cargo_manifest: Path,
    cargo_lock: Path,
    pre_pin_commit: str,
    dependency_manifest: Path,
) -> None:
    value = EvidenceSanitizer.read_dependency_manifest(dependency_manifest)
    live, _commands, _archives = _collect_live_evidence(
        fork_checkout=fork_checkout,
        cargo_manifest=cargo_manifest,
        cargo_lock=cargo_lock,
        pre_pin_commit=pre_pin_commit,
    )
    EvidenceSanitizer._verify_revision_contents(
        fork_head=live['fork_head'],
        cargo_manifest_text=live['cargo_manifest_text'],
        cargo_metadata_text=live['cargo_metadata_text'],
        cargo_lock_text=live['cargo_lock_text'],
        dependency_manifest_text=dependency_manifest.read_text(encoding='utf-8'),
    )
    if value.pre_pin_costing_commit != pre_pin_commit:
        raise ValueError('dependency manifest pre-pin commit mismatch')
    if value.allowed_diff_files != live['diff_files']:
        raise ValueError('dependency manifest diff allowlist mismatch')
    if value.xmlwriter_fallback_used != live['fallback_used']:
        raise ValueError('dependency manifest xmlwriter fallback state mismatch')
    if value.diff_sha256 != live['diff_sha256']:
        raise ValueError('dependency manifest diff SHA-256 mismatch')
    if value.crates_io_checksum != live['checksum']:
        raise ValueError('dependency manifest crates.io checksum mismatch')


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command', required=True)
    dependency = subparsers.add_parser('dependency')
    verify = subparsers.add_parser('verify-dependency')
    for command in (dependency, verify):
        command.add_argument('--fork-checkout', type=Path, required=True)
        command.add_argument('--cargo-manifest', type=Path, required=True)
        command.add_argument('--cargo-lock', type=Path, required=True)
        command.add_argument('--pre-pin-commit', required=True)
    dependency.add_argument('--local-log-root', type=Path, required=True)
    dependency.add_argument('--output', type=Path, required=True)
    verify.add_argument('--dependency-manifest', type=Path, required=True)
    return parser


def main() -> None:
    args = _argument_parser().parse_args()
    if args.command == 'dependency':
        generate_dependency_manifest(
            fork_checkout=args.fork_checkout,
            cargo_manifest=args.cargo_manifest,
            cargo_lock=args.cargo_lock,
            pre_pin_commit=args.pre_pin_commit,
            local_log_root=args.local_log_root,
            output=args.output,
        )
    else:
        verify_dependency_manifest(
            fork_checkout=args.fork_checkout,
            cargo_manifest=args.cargo_manifest,
            cargo_lock=args.cargo_lock,
            pre_pin_commit=args.pre_pin_commit,
            dependency_manifest=args.dependency_manifest,
        )


if __name__ == '__main__':
    main()

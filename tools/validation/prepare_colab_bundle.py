"""Prepare a reviewable Colab source bundle locally; this command never uploads it."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import stat
from pathlib import Path, PurePosixPath
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ROOT_FILES = ('rust-toolchain.toml', 'pyproject.toml', 'uv.lock', 'README.md', 'tools/__init__.py', 'tests/__init__.py')
SOURCE_GROUPS = (
    ('rust', {'.rs', '.toml', '.json', '.lock'}),
    ('tools/validation', {'.py', '.ipynb'}),
    ('tests/validation', {'.py'}),
    ('tests/contracts/baselines', {'.json'}),
)
VENDORED_WRITER = 'rust/vendor/rust_xlsxwriter'
VENDORED_WRITER_DOCUMENTS = ('README.md', 'CHANGELOG.md', 'PATCHES.md', 'LICENSE_MIT', 'LICENSE_Apache2.0')


class BundleError(ValueError):
    pass


def _hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _sources(project: Path) -> list[Path]:
    paths = {project / name for name in ROOT_FILES}
    for name, suffixes in SOURCE_GROUPS:
        for path in (project / name).rglob('*'):
            relative = path.relative_to(project)
            if any(part in ('target', '__pycache__', '.venv', '.git') for part in relative.parts):
                continue
            if path.suffix in suffixes and not path.is_dir():
                paths.add(path)
    paths.update((project / 'tools/validation').glob('*schema.json'))
    vendor = project / VENDORED_WRITER
    if vendor.exists() or vendor.is_symlink():
        # CHANGELOG 为 include_str! 编译输入；许可证和修补记录随受控源码一并冻结。
        paths.update(vendor / name for name in VENDORED_WRITER_DOCUMENTS)
    if not (project / 'rust/Cargo.lock').is_file():
        raise BundleError('Rust lock file is required')
    for path in paths:
        reviewed_file(project, path.relative_to(project).as_posix())
    return sorted(paths)


VENDOR_ROOT = 'vendor/rust_xlsxwriter'
TOOL_FILES = (
    'tools/__init__.py',
    'tools/validation/__init__.py',
    *(
        f'tools/validation/{name}.py'
        for name in (
            'autotune_experiment',
            'measure_paired_release',
            'compare_releases',
            'workbook_compare',
            'synthetic_inputs',
            'synthetic_benchmark',
            'prepare_colab_bundle',
            'types',
        )
    ),
    'tools/validation/measure_paired_release.linux.schema.json',
    'tools/validation/measure_paired_release.schema.json',
    'pyproject.toml',
    'uv.lock',
)
STAGE_CHILDREN = {'export': ('writer_populate', 'xlsx_save')}
STAGES = frozenset({'ingest', 'normalize', 'split', 'fact', 'presentation', 'export', 'writer_populate', 'xlsx_save'})


def safe_relative(name: str) -> PurePosixPath:
    if (
        not isinstance(name, str)
        or not name
        or '\\' in name
        or ':' in name
        or '\x00' in name
        or any(ord(character) < 32 for character in name)
        or name.startswith('/')
        or any(part in ('', '.', '..') for part in name.split('/'))
    ):
        raise BundleError('A safe relative path is required')
    return PurePosixPath(name)


def reviewed_file(root: Path, name: str) -> Path:
    relative = safe_relative(name)
    path = root.joinpath(*relative.parts)
    if root.is_symlink() or any(
        (root.joinpath(*relative.parts[:index])).is_symlink() for index in range(1, len(relative.parts) + 1)
    ):
        raise BundleError('Reviewed files cannot be indirect files')
    if not path.is_file() or not path.resolve().is_relative_to(root.resolve()):
        raise BundleError('Reviewed file is unavailable or outside its directory')
    return path


def target_stages(candidate: dict[str, Any]) -> list[str]:
    stages = candidate.get('target_stages', [candidate['target_stage']] if candidate.get('target_stage') else [])
    if not isinstance(stages, list) or any(stage not in STAGES for stage in stages) or len(set(stages)) != len(stages):
        raise BundleError('Target stages must be unique known stage names')
    if candidate.get('target_stage') and candidate['target_stage'] not in stages:
        raise BundleError('Target stage must belong to the frozen stage scope')
    expanded = set(stages)
    for parent, children in STAGE_CHILDREN.items():
        if parent in expanded:
            expanded.update(children)
    return sorted(expanded)


def _cargo_key(key: Any) -> bool:
    if not isinstance(key, list) or not key or not all(isinstance(part, str) and part for part in key):
        return False
    for index, part in enumerate(key):
        if part in ('dependencies', 'build-dependencies', 'dev-dependencies'):
            if key[:index] not in ([], ['workspace']) and not (index == 2 and key[0] == 'target'):
                return False
            tail = key[index + 1 :]
            return len(tail) == 1 or (len(tail) == 2 and tail[-1] in ('version', 'git', 'rev', 'path'))
        if part == 'patch' and index == 0:
            tail = key[index + 1 :]
            return len(tail) == 2 or (len(tail) == 3 and tail[-1] in ('version', 'git', 'rev', 'path'))
    return False


def validate_dependency_declaration(candidate: dict[str, Any]) -> None:
    changes = candidate.get('cargo_changes', {})
    if not isinstance(changes, dict):
        raise BundleError('cargo_changes must map Cargo files to exact key paths')
    for name, keys in changes.items():
        if not (name == 'rust/Cargo.toml' or re.fullmatch(r'rust/crates/[^/]+/Cargo.toml', name)):
            raise BundleError('Only declared Cargo manifests may change')
        if not isinstance(keys, list) or not keys or not all(_cargo_key(key) for key in keys):
            raise BundleError('Only exact dependency version/source keys may change; features and profiles are frozen')
        if len({tuple(key) for key in keys}) != len(keys):
            raise BundleError('Cargo key declarations must be unique')
    lock = candidate.get('lock_changes', {'before': [], 'after': []})
    if not isinstance(lock, dict) or set(lock) != {'before', 'after'}:
        raise BundleError('lock_changes requires before and after package lists')
    for entries in lock.values():
        if not isinstance(entries, list):
            raise BundleError('Lock package identities must be lists')
        identities = []
        for entry in entries:
            if not isinstance(entry, dict) or not all(isinstance(entry.get(key), str) for key in ('name', 'version')):
                raise BundleError('Lock entries require full package identities')
            identities.append((entry['name'], entry['version'], entry.get('source')))
        if len(set(identities)) != len(identities):
            raise BundleError('Lock package identities must distinguish same-name versions and sources')
    paths = candidate['changed_paths']
    declared = set(changes) | ({'rust/Cargo.lock'} if any(lock.values()) else set())
    cargo_paths = {path for path in paths if path.endswith('/Cargo.toml') or path == 'rust/Cargo.lock'}
    if declared != cargo_paths:
        raise BundleError('Cargo and lock file changes must exactly match their declarations')


def validate_payload(candidate: dict[str, Any]) -> None:
    payload = candidate.get('payload', [])
    if candidate.get('kind', 'source') != 'fork':
        if payload or candidate.get('fork_source') or candidate.get('licenses'):
            raise BundleError('Only fork candidates may carry source payloads')
        return
    provenance = candidate.get('fork_source')
    if (
        not isinstance(provenance, dict)
        or not isinstance(provenance.get('url'), str)
        or not provenance['url']
        or not isinstance(provenance.get('revision'), str)
        or not re.fullmatch(r'[0-9a-f]{40}|[0-9a-f]{64}', provenance['revision'])
    ):
        raise BundleError('Fork source URL and immutable revision are required')
    if not isinstance(payload, list) or not payload:
        raise BundleError('Fork requires an explicit per-file payload')
    targets = set()
    for entry in payload:
        source, target = str(safe_relative(entry['source'])), str(safe_relative(entry['target']))
        if source.startswith('source/') or source == 'bundle-manifest.json' or not target.startswith(f'{VENDOR_ROOT}/'):
            raise BundleError('Payload must use a separate source and the fixed vendor directory')
        if target in targets or any(
            target.startswith(f'{other}/') or other.startswith(f'{target}/') for other in targets
        ):
            raise BundleError('Payload targets conflict')
        if (
            not re.fullmatch('[0-9a-f]{64}', entry.get('sha256', ''))
            or type(entry.get('size_bytes')) is not int
            or entry['size_bytes'] < 0
        ):
            raise BundleError('Payload requires SHA-256 and size')
        targets.add(target)
    licenses = candidate.get('licenses')
    if (
        not isinstance(licenses, list)
        or not licenses
        or any(
            name not in targets or not re.match(r'(?i)(license|copying)', PurePosixPath(name).name) for name in licenses
        )
    ):
        raise BundleError('Fork payload must retain its declared source licenses')
    if f'{VENDOR_ROOT}/Cargo.toml' not in targets:
        raise BundleError('Fork payload must include its Cargo manifest')
    if any(entry['size_bytes'] == 0 for entry in payload if entry['target'] in licenses):
        raise BundleError('Fork licenses cannot be empty')
    if not any(
        key[-2:] == ['rust_xlsxwriter', 'path'] for keys in candidate.get('cargo_changes', {}).values() for key in keys
    ):
        raise BundleError('Fork must declare its rust_xlsxwriter path dependency')


def validate_candidates(items: Any) -> None:
    if not isinstance(items, list) or not 1 <= len(items) <= 6:
        raise BundleError('Independent candidate count must be between one and six')
    seen = set()
    for candidate in items:
        if not isinstance(candidate, dict):
            raise BundleError('Candidate entries must be objects')
        name = candidate.get('id', '')
        if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,47}', name) or name in seen or name in ('baseline', 'control'):
            raise BundleError('Candidate identifiers must be unique safe names')
        seen.add(name)
        relative = safe_relative(candidate['patch'])
        if relative.suffix != '.patch' or relative.parts[0] == 'source' or str(relative) == 'bundle-manifest.json':
            raise BundleError('Candidate patch must be a safe relative path')
        if not re.fullmatch('[0-9a-f]{64}', candidate.get('patch_sha256', '')):
            raise BundleError('Candidate patch requires SHA-256')
        kind = candidate.get('kind', 'source')
        if kind not in ('source', 'profile', 'dependency', 'fork'):
            raise BundleError('Unknown candidate kind')
        if candidate.get('purpose', 'screening') not in ('screening', 'diagnostic'):
            raise BundleError('Candidate purpose must be screening or diagnostic')
        target_stages(candidate)
        if candidate.get('primary_pipeline') not in ('gb', 'sk') or candidate.get('primary_size') not in (
            'medium',
            'above-threshold',
        ):
            raise BundleError('Candidate must declare a primary pipeline and benchmark size')
        if not isinstance(candidate.get('hypothesis'), str) or not candidate['hypothesis'].strip():
            raise BundleError('Candidate must declare its hypothesis')
        paths = candidate.get('changed_paths')
        if (
            not isinstance(paths, list)
            or not paths
            or not all(isinstance(name, str) for name in paths)
            or len(set(paths)) != len(paths)
        ):
            raise BundleError('Candidate must declare unique changed paths')
        for name in paths:
            path = safe_relative(name)
            source = name.startswith('rust/crates/') and path.suffix == '.rs'
            cargo = name == 'rust/Cargo.toml'
            if kind in ('dependency', 'fork'):
                cargo = cargo or name == 'rust/Cargo.lock' or bool(re.fullmatch(r'rust/crates/[^/]+/Cargo.toml', name))
            if not (source or cargo):
                raise BundleError('Only reviewed Rust source or declared Cargo changes are supported')
        if kind in ('dependency', 'fork'):
            validate_dependency_declaration(candidate)
        elif candidate.get('cargo_changes') or candidate.get('lock_changes'):
            raise BundleError('Dependency declarations require a dependency or fork candidate')
        validate_payload(candidate)


def validate_combination(combination: Any, candidates: list[dict[str, Any]]) -> None:
    if combination is None:
        return
    validate_candidates([combination])
    by_id = {candidate['id']: candidate for candidate in candidates}
    members = combination.get('components')
    if (
        not isinstance(members, list)
        or not 2 <= len(members) <= 6
        or len(set(members)) != len(members)
        or any(name not in by_id for name in members)
        or combination['id'] in by_id
    ):
        raise BundleError('One combination must reference unique independent candidate IDs')
    selected = [by_id[name] for name in members]
    if any(item.get('purpose', 'screening') != 'screening' for item in [combination, *selected]):
        raise BundleError('Diagnostic candidates cannot be combined for ranking')
    if any(
        (item['primary_pipeline'], item['primary_size'])
        != (combination['primary_pipeline'], combination['primary_size'])
        for item in selected
    ):
        raise BundleError('Combination and components must share the frozen primary scenario')
    cargo: dict[str, list[list[str]]] = {}
    lock: dict[str, list[dict[str, Any]]] = {'before': [], 'after': []}
    for item in selected:
        for name, keys in item.get('cargo_changes', {}).items():
            current = cargo.setdefault(name, [])
            if {tuple(key) for key in keys} & {tuple(key) for key in current}:
                raise BundleError('Combination dependency keys conflict')
            current.extend(keys)
        for side in lock:
            entries = item.get('lock_changes', {}).get(side, [])
            identities = {(entry['name'], entry['version'], entry.get('source')) for entry in lock[side]}
            if any((entry['name'], entry['version'], entry.get('source')) in identities for entry in entries):
                raise BundleError('Combination lock package changes conflict')
            lock[side].extend(entries)
    if {name: sorted(map(tuple, keys)) for name, keys in cargo.items()} != {
        name: sorted(map(tuple, keys)) for name, keys in combination.get('cargo_changes', {}).items()
    }:
        raise BundleError('Combination must preserve all component Cargo declarations')
    if any(
        sorted(json.dumps(entry, sort_keys=True) for entry in lock[side])
        != sorted(json.dumps(entry, sort_keys=True) for entry in combination.get('lock_changes', {}).get(side, []))
        for side in lock
    ):
        raise BundleError('Combination must preserve all component lock declarations')
    if set(combination['changed_paths']) != {path for item in selected for path in item['changed_paths']}:
        raise BundleError('Combination patch scope must equal its component scopes')
    if set(target_stages(combination)) != {stage for item in selected for stage in target_stages(item)}:
        raise BundleError('Combination must preserve its components frozen target stages')
    payload = [entry for item in selected for entry in item.get('payload', [])]
    if combination.get('payload', []) != payload:
        raise BundleError('Combination must preserve its fork file payload')


def validate_patch(content: bytes, expected: list[str]) -> None:
    for name in expected:
        safe_relative(name)
    patch_text = content.decode('utf-8')
    headers = [line for line in patch_text.splitlines() if line.startswith(('--- ', '+++ '))]
    wanted = [header for name in expected for header in (f'--- a/{name}', f'+++ b/{name}')]
    if not headers or len(headers) != len(wanted) or sorted(headers) != sorted(wanted):
        raise BundleError('Patch paths differ from the candidate allowlist')
    if any(headers[index][6:] != headers[index + 1][6:] for index in range(0, len(headers), 2)):
        raise BundleError('Both patch paths must refer to the same reviewed file')
    if re.search(r'^(rename |copy |GIT binary patch|Binary files )', patch_text, re.MULTILINE):
        raise BundleError('Only text patches of existing reviewed files are supported')


def tool_inventory(files: dict[str, Any]) -> dict[str, Any]:
    return {name: files[f'source/{name}'] for name in TOOL_FILES}


def prepare_bundle(
    *,
    project_root: Path,
    candidate_manifest: Path,
    baseline_metadata: Path,
    output_dir: Path,
) -> dict[str, Any]:
    project = project_root.resolve()
    candidates = json.loads(candidate_manifest.read_text(encoding='utf-8'))
    baseline = json.loads(baseline_metadata.read_text(encoding='utf-8'))
    if not re.fullmatch(r'[0-9a-f]{40}', candidates.get('base_commit', '')):
        raise BundleError('Candidate manifest requires a full base commit')
    if candidates['base_commit'] != baseline['base_commit']:
        raise BundleError('Candidate and baseline commits differ')
    if type(candidates.get('source_date_epoch')) is not int:
        raise BundleError('A fixed build timestamp is required')
    items = candidates.get('candidates')
    validate_candidates(items)
    combination = candidates.get('combination')
    validate_combination(combination, items)
    files: dict[str, bytes] = {}
    for path in _sources(project):
        relative = path.relative_to(project).as_posix()
        content = path.read_bytes()
        if relative.startswith('rust/') or relative == 'rust-toolchain.toml':
            if baseline['source_files'].get(relative) != _hash(content):
                raise BundleError('Runtime source differs from the frozen baseline')
        files[f'source/{relative}'] = content
    runtime_files = {
        name for name in baseline['source_files'] if name.startswith('rust/') or name == 'rust-toolchain.toml'
    }
    if runtime_files != {name.removeprefix('source/') for name in files if name.startswith('source/rust')}:
        raise BundleError('Runtime source inventory differs from the frozen baseline')
    candidate_root = candidate_manifest.resolve().parent
    for candidate in [*items, *([combination] if combination is not None else [])]:
        relative = candidate['patch']
        patch_path = reviewed_file(candidate_root, relative)
        content = patch_path.read_bytes()
        if _hash(content) != candidate['patch_sha256']:
            raise BundleError('Candidate patch hash differs')
        validate_patch(content, candidate['changed_paths'])
        if relative in files and files[relative] != content:
            raise BundleError('Candidate file conflicts with bundle contents')
        files[relative] = content
        for entry in candidate.get('payload', []):
            content = reviewed_file(candidate_root, entry['source']).read_bytes()
            if _hash(content) != entry['sha256'] or len(content) != entry['size_bytes']:
                raise BundleError('Fork payload identity differs')
            if entry['source'] in files and files[entry['source']] != content:
                raise BundleError('Fork payload conflicts with bundle contents')
            files[entry['source']] = content
    source_inventory = {
        name: {'sha256': _hash(content), 'size_bytes': len(content)} for name, content in sorted(files.items())
    }
    inventory_hash = _hash(json.dumps(source_inventory, sort_keys=True, separators=(',', ':')).encode())
    # 阈值由同一个驱动导出，业务源码与验证工具分别冻结。
    from tools.validation.autotune_experiment import protocol_identity

    manifest = {
        'schema_version': 1,
        'tool_files': tool_inventory(source_inventory),
        'protocol_sha256': protocol_identity(),
        'runtime_files': {name: source_inventory[f'source/{name}'] for name in sorted(runtime_files)},
        'source_inventory_sha256': inventory_hash,
        'base_commit': candidates['base_commit'],
        'source_date_epoch': candidates['source_date_epoch'],
        'files': source_inventory,
        'candidates': items,
        'combination': combination,
        'limits': {'cloud_budget_seconds': 21600, 'pairs': 8, 'memory_limit_mib': 2048, 'disk_limit_mib': 2048},
        'disclosure': (
            'Source and embedded business configuration are included. '
            'The allowlist excludes data/raw, data/processed, .claude, .git, caches and compiled binaries. '
            'Review source and configuration before cloud upload.'
        ),
    }
    files['bundle-manifest.json'] = (json.dumps(manifest, ensure_ascii=False, indent=2) + '\n').encode()
    output_dir.mkdir(parents=True, exist_ok=False)
    archive_path = output_dir / 'colab-source.zip'
    with ZipFile(archive_path, 'x', compression=ZIP_DEFLATED, compresslevel=6) as archive:
        for name, content in sorted(files.items()):
            info = ZipInfo(name, date_time=(2000, 1, 1, 0, 0, 0))
            info.compress_type = ZIP_DEFLATED
            info.external_attr = (stat.S_IFREG | 0o644) << 16
            archive.writestr(info, content)
    with ZipFile(archive_path) as archive:
        if archive.testzip() is not None or set(archive.namelist()) != set(files):
            raise BundleError('Bundle integrity check failed')
        for name, content in files.items():
            if _hash(archive.read(name)) != _hash(content):
                raise BundleError('Bundle content verification failed')
    review = {
        'schema_version': 1,
        'status': 'prepared-not-uploaded',
        'archive': archive_path.name,
        'archive_sha256': _hash(archive_path.read_bytes()),
        'archive_size_bytes': archive_path.stat().st_size,
        **manifest,
    }
    with (output_dir / 'bundle-review.json').open('x', encoding='utf-8') as handle:
        json.dump(review, handle, ensure_ascii=False, indent=2)
        handle.write('\n')
    return review


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project-root', type=Path, default=PROJECT_ROOT)
    parser.add_argument('--candidate-manifest', type=Path, required=True)
    parser.add_argument('--baseline-metadata', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    try:
        review = prepare_bundle(**vars(args))
    except (OSError, ValueError, KeyError, TypeError):
        print(
            json.dumps(
                {
                    'status': 'failed',
                    'error_code': 'BUNDLE_INVALID',
                    'message': 'Source allowlist, identities or output directory could not be verified.',
                    'retryable': False,
                }
            )
        )
        return 2
    print(
        json.dumps(
            {key: review[key] for key in ('status', 'archive', 'archive_sha256', 'archive_size_bytes', 'disclosure')},
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

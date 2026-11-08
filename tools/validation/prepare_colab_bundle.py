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
    if not (project / 'rust/Cargo.lock').is_file():
        raise BundleError('Rust lock file is required')
    for path in paths:
        if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(project):
            raise BundleError('Source allowlist contains an unavailable or indirect file')
    return sorted(paths)


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
        relative = PurePosixPath(candidate['patch'])
        if (
            relative.is_absolute()
            or '..' in relative.parts
            or relative.suffix != '.patch'
            or relative.parts[0] == 'source'
        ):
            raise BundleError('Candidate patch must be a safe relative path')
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
            path = PurePosixPath(name)
            if (
                path.is_absolute()
                or '..' in path.parts
                or not (name == 'rust/Cargo.toml' or (name.startswith('rust/crates/') and path.suffix == '.rs'))
            ):
                raise BundleError('Only reviewed Rust source or Cargo profile changes are supported')


def validate_patch(content: bytes, expected: list[str]) -> None:
    patch_text = content.decode('utf-8')
    changed = re.findall(r'^\+\+\+ b/(.+)$', patch_text, re.MULTILINE)
    previous = re.findall(r'^--- a/(.+)$', patch_text, re.MULTILINE)
    if not changed or sorted(previous) != sorted(changed) or sorted(changed) != sorted(expected):
        raise BundleError('Patch paths differ from the candidate allowlist')


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
    for candidate in items:
        relative = PurePosixPath(candidate['patch'])
        patch_path = candidate_root / str(relative)
        if patch_path.is_symlink() or not patch_path.resolve().is_relative_to(candidate_root):
            raise BundleError('Candidate patch must stay within its reviewed directory')
        content = patch_path.read_bytes()
        if _hash(content) != candidate['patch_sha256']:
            raise BundleError('Candidate patch hash differs')
        validate_patch(content, candidate['changed_paths'])
        files[str(relative)] = content
    source_inventory = {
        name: {'sha256': _hash(content), 'size_bytes': len(content)} for name, content in sorted(files.items())
    }
    inventory_hash = _hash(json.dumps(source_inventory, sort_keys=True, separators=(',', ':')).encode())
    manifest = {
        'schema_version': 1,
        'source_inventory_sha256': inventory_hash,
        'base_commit': candidates['base_commit'],
        'source_date_epoch': candidates['source_date_epoch'],
        'files': source_inventory,
        'candidates': items,
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

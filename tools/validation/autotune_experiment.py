"""Screen candidate Rust builds with the frozen paired protocol.

One driver serves the Colab notebook and local runs. It builds the baseline, an unpatched control and every
candidate, checks workbook equivalence on synthetic inputs, then times paired runs. Costing rules stay in Rust.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import time
import tomllib
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile

from tools.validation import workbook_compare
from tools.validation.compare_releases import ValidationError, compare_releases
from tools.validation.measure_paired_release import MeasurementError, measure_pairs, paired_statistics, run_process
from tools.validation.prepare_colab_bundle import (
    STAGE_CHILDREN,
    TOOL_FILES,
    VENDOR_ROOT,
    BundleError,
    reviewed_file,
    safe_relative,
    tool_inventory,
    validate_candidates,
    validate_combination,
    validate_patch,
)
from tools.validation.prepare_colab_bundle import (
    target_stages as candidate_stages,
)
from tools.validation.synthetic_benchmark import FULL_SK_PRIMARY_ORDERS, build_full_sk_fixture
from tools.validation.synthetic_inputs import build_benchmark_fixture

SEED = 20261108
VALIDATION_SEED = 20261109
CONTROL_ID = 'control'
MAX_SHORTLIST = 2
BUILD_TIMEOUT_SECONDS = 3600.0
SETTLE_SECONDS = 3.0
ACCEPTANCE_RESOURCE_SCENARIOS = {
    'standard': {
        'pipelines': ('gb', 'sk'),
        'memory_limit_bytes': 2 * 1024**3,
        'disk_limit_bytes': 2 * 1024**3,
    },
    'full-sk': {
        'pipelines': ('sk',),
        'memory_limit_bytes': 6 * 1024**3,
        'disk_limit_bytes': 4 * 1024**3,
    },
}
# 与 docs/plans/2026-11-08-colab-offline-autotuning.md 第 6.4 节一致；开测前冻结，运行中不得修改。
THRESHOLDS = {
    'primary_improvement': 0.05,
    'primary_win_ratio': 0.75,
    'scenario_regression': 0.02,
    'rss_regression': 0.05,
    'rss_median_ratio': 1.05,
    'combination_improvement': 0.02,
    'output_ratio': 1.02,
    'stage_regression': 0.05,
    'stage_noise': 0.05,
    'stage_floor_seconds': 0.05,
}
# (name, mode, month_start, month_end)
SMALL_CHECKS = (
    ('normal', 'normal', None, None),
    ('check-only', 'check-only', None, None),
    ('single-month', 'normal', '2026-01', '2026-01'),
    ('cross-month', 'normal', '2025-12', '2026-01'),
    ('empty-month', 'normal', '2030-01', '2030-01'),
)
# (pipeline, size, seed, mode)
EXTENDED_SCENARIOS = (
    ('gb', 'medium', SEED, 'normal'),
    ('sk', 'medium', SEED, 'normal'),
    ('gb', 'above-threshold', SEED, 'normal'),
    ('sk', 'above-threshold', SEED, 'normal'),
    ('gb', 'medium', VALIDATION_SEED, 'normal'),
    ('sk', 'medium', VALIDATION_SEED, 'normal'),
    ('gb', 'medium', SEED, 'check-only'),
    ('sk', 'medium', SEED, 'check-only'),
)
# 脏工单集中在三月；单独用大型输入的正常月份验证完整语义，保留原有全月脏数据检查。
SEMANTIC_SCENARIO = ('gb-large-semantic', 'gb', 'above-threshold', SEED, 'normal', '2025-12', '2026-01')
IGNORED_BUNDLE_PARTS = frozenset({'.venv', '__pycache__', '.pytest_cache', '.ruff_cache'})
HERMETIC_ENV_NAMES = frozenset({'RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'CARGO_BUILD_RUSTFLAGS', 'RUSTC_WRAPPER'})


class ExperimentError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable

    def payload(self) -> dict[str, Any]:
        return {'error_code': self.code, 'message': str(self), 'retryable': self.retryable}


class BudgetExhausted(Exception):  # noqa: N818 - control-flow signal, not an error condition.
    pass


def _log(message: str) -> None:
    print(f'[{time.strftime("%H:%M:%S")}] {message}', file=sys.stderr, flush=True)


def _sha256(path: Path) -> str:
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def _save_json(path: Path, document: dict[str, Any]) -> None:
    temporary = path.with_name(f'.{path.name}.tmp')
    with temporary.open('w', encoding='utf-8') as stream:
        json.dump(document, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def environment() -> dict[str, Any]:
    cpuinfo = Path('/proc/cpuinfo').read_text(encoding='utf-8', errors='replace').splitlines()
    meminfo = Path('/proc/meminfo').read_text(encoding='ascii').splitlines()
    return {
        'system': platform.system(),
        'machine': platform.machine(),
        'cpu_model': next((line.partition(':')[2].strip() for line in cpuinfo if line.startswith('model name')), '?'),
        'logical_cpus': os.cpu_count(),
        'cpu_affinity': sorted(os.sched_getaffinity(0)),
        'memory_bytes': next((int(line.split()[1]) for line in meminfo if line.startswith('MemTotal:')), 0) * 1024,
        'python': platform.python_version(),
        'xlsxwriter': version('XlsxWriter'),
    }


def _json_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    ).hexdigest()


def protocol_identity() -> str:
    return _json_hash(
        {
            'thresholds': THRESHOLDS,
            'acceptance_resource_scenarios': ACCEPTANCE_RESOURCE_SCENARIOS,
            'seed': SEED,
            'validation_seed': VALIDATION_SEED,
            'small_checks': SMALL_CHECKS,
            'full_sk_layout': {'orders': 64, 'primary_orders': FULL_SK_PRIMARY_ORDERS, 'checks': SMALL_CHECKS},
            'extended': EXTENDED_SCENARIOS,
            'semantic_scenario': SEMANTIC_SCENARIO,
            'stage_children': STAGE_CHILDREN,
            'max_shortlist': MAX_SHORTLIST,
            'build_artifact_isolation': 'per-candidate',
        }
    )


TOOL_ROOT = Path(__file__).resolve().parents[2]
LOADED_TOOL_HASHES = {name: _sha256(TOOL_ROOT / name) for name in TOOL_FILES}


def verify_runtime(manifest: dict[str, Any]) -> None:
    """Check the executing tool files as well as the separately frozen business baseline."""
    try:
        if (
            manifest['tool_files'] != tool_inventory(manifest['files'])
            or manifest['protocol_sha256'] != protocol_identity()
        ):
            raise ValueError('Tool inventory or thresholds changed')
        for name, identity in manifest['tool_files'].items():
            path = reviewed_file(TOOL_ROOT, name)
            if _sha256(path) != identity['sha256'] or path.stat().st_size != identity['size_bytes']:
                raise ValueError('Executing tool differs from frozen tool')
            if LOADED_TOOL_HASHES[name] != identity['sha256']:
                raise ValueError('Loaded tool version differs from disk')
            if name.endswith('.py'):
                module_name = name.removesuffix('.py').replace('/', '.').removesuffix('.__init__')
                module = sys.modules.get(module_name)
                if module is not None and Path(module.__file__).resolve() != path.resolve():
                    raise ValueError('Executing module comes from another source tree')
        runtime = {
            name.removeprefix('source/'): value
            for name, value in manifest['files'].items()
            if name.startswith('source/rust/') or name == 'source/rust-toolchain.toml'
        }
        if runtime != manifest['runtime_files']:
            raise ValueError('Business source inventory changed')
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise ExperimentError(
            'IDENTITY_CHANGED', 'Frozen source, executing tools or protocol identity differs.'
        ) from error


def verify_bundle(root: Path) -> dict[str, Any]:
    """Check the extracted bundle against its manifest before anything is built from it."""
    try:
        manifest = json.loads((root / 'bundle-manifest.json').read_text(encoding='utf-8'))
        expected = manifest['files']
        present: dict[str, Path] = {}
        for path in root.rglob('*'):
            if IGNORED_BUNDLE_PARTS.intersection(path.relative_to(root).parts):
                continue
            if path.is_symlink():
                raise ExperimentError('BUNDLE_INVALID', 'Bundle contains an indirect file.')
            if path.is_file():
                present[path.relative_to(root).as_posix()] = path
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise ExperimentError('BUNDLE_INVALID', 'Bundle manifest could not be read.') from error
    if set(present) != {*expected, 'bundle-manifest.json'}:
        raise ExperimentError('BUNDLE_INVALID', 'Bundle contents differ from its manifest.')
    try:
        if manifest['schema_version'] != 1:
            raise ValueError('Unsupported bundle version')
        validate_candidates(manifest['candidates'])
        validate_combination(manifest.get('combination'), manifest['candidates'])
        for name, identity in expected.items():
            safe_relative(name)
            if _sha256(present[name]) != identity['sha256'] or present[name].stat().st_size != identity['size_bytes']:
                raise ValueError('Bundle file identity differs')
        for candidate in [*manifest['candidates'], *([manifest['combination']] if manifest.get('combination') else [])]:
            if candidate['patch_sha256'] != expected[candidate['patch']]['sha256']:
                raise ValueError('Candidate patch identity differs')
            for entry in candidate.get('payload', []):
                if expected[entry['source']] != {key: entry[key] for key in ('sha256', 'size_bytes')}:
                    raise ValueError('Fork payload identity differs')
        inventory = hashlib.sha256(json.dumps(expected, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        if inventory != manifest.get('source_inventory_sha256'):
            raise ValueError('Bundle inventory differs')
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise ExperimentError(
            'BUNDLE_INVALID', 'Bundle files or candidate identities could not be verified.'
        ) from error
    verify_runtime(manifest)
    return manifest


def prepare_tree(bundle: Path, destination: Path) -> None:
    """Copy exactly the files a Rust build and its contract tests need."""
    source = bundle / 'source'
    destination.mkdir(parents=True)
    shutil.copytree(source / 'rust', destination / 'rust')
    shutil.copy2(source / 'rust-toolchain.toml', destination / 'rust-toolchain.toml')
    contracts = source / 'tests' / 'contracts'
    if contracts.is_dir():
        shutil.copytree(contracts, destination / 'tests' / 'contracts')


def changed_files(baseline: Path, candidate: Path) -> list[str]:
    def listing(root: Path) -> dict[str, Path]:
        paths = list(root.rglob('*'))
        if any(path.is_symlink() for path in paths):
            raise ExperimentError('PATCH_SCOPE', 'Candidate trees cannot contain symbolic links.')
        return {path.relative_to(root).as_posix(): path for path in paths if path.is_file()}

    left, right = listing(baseline), listing(candidate)
    return sorted(
        name
        for name in left.keys() | right.keys()
        if name not in left or name not in right or left[name].read_bytes() != right[name].read_bytes()
    )


def assert_profile_only(baseline: Path, candidate: Path) -> None:
    """A workspace manifest candidate may change build profiles but never dependencies or features."""
    before = tomllib.loads(baseline.read_text(encoding='utf-8'))
    after = tomllib.loads(candidate.read_text(encoding='utf-8'))
    codegen_units = after.get('profile', {}).get('release', {}).get('codegen-units')
    before.pop('profile', None)
    after.pop('profile', None)
    if before != after or codegen_units != 1:
        raise ExperimentError('MANIFEST_CHANGE_NOT_ALLOWED', 'Only Cargo profiles may change, with codegen-units = 1.')


def _toml_leaves(document: dict[str, Any], prefix: tuple[str, ...] = ()) -> dict[tuple[str, ...], Any]:
    leaves = {}
    for key, value in document.items():
        if isinstance(value, dict) and value:
            leaves.update(_toml_leaves(value, (*prefix, key)))
        else:
            leaves[(*prefix, key)] = value
    return leaves


def assert_dependency_changes(baseline: Path, candidate: Path, meta: dict[str, Any]) -> None:
    for name, keys in meta.get('cargo_changes', {}).items():
        before = _toml_leaves(tomllib.loads((baseline / name).read_text(encoding='utf-8')))
        after = _toml_leaves(tomllib.loads((candidate / name).read_text(encoding='utf-8')))
        actual = {
            key
            for key in before.keys() | after.keys()
            if key not in before or key not in after or before[key] != after[key]
        }
        if actual != {tuple(key) for key in keys}:
            raise ExperimentError(
                'MANIFEST_CHANGE_NOT_ALLOWED', 'Cargo changes differ from the exact declared TOML keys.'
            )
        for key in actual:
            if key[-1] == 'path' and key in after:
                value = after[key]
                if (
                    meta.get('kind') != 'fork'
                    or key[-2] != 'rust_xlsxwriter'
                    or not isinstance(value, str)
                    or ((candidate / name).parent / value).resolve() != (candidate / VENDOR_ROOT).resolve()
                ):
                    raise ExperimentError(
                        'MANIFEST_CHANGE_NOT_ALLOWED', 'Dependency paths must use the fixed fork payload directory.'
                    )
    if 'rust/Cargo.lock' not in meta['changed_paths']:
        return
    before = tomllib.loads((baseline / 'rust/Cargo.lock').read_text(encoding='utf-8'))
    after = tomllib.loads((candidate / 'rust/Cargo.lock').read_text(encoding='utf-8'))
    left, right = before.pop('package', []), after.pop('package', [])
    if before != after:
        raise ExperimentError('LOCK_CHANGE_NOT_ALLOWED', 'Cargo.lock metadata is frozen.')

    # 完整条目比较同时覆盖 checksum、来源和传递边，区分同名多版本。
    def packages(entries: list[dict[str, Any]]) -> dict[tuple[str, str, str | None], dict[str, Any]]:
        result = {(entry['name'], entry['version'], entry.get('source')): entry for entry in entries}
        if len(result) != len(entries):
            raise ExperimentError('LOCK_CHANGE_NOT_ALLOWED', 'Duplicate Cargo.lock package identity.')
        return result

    old, new = packages(left), packages(right)
    changed = {key for key in old.keys() | new.keys() if old.get(key) != new.get(key)}
    declared = meta.get('lock_changes', {})
    for side, entries in (('before', old), ('after', new)):
        actual_entries = sorted(_json_hash(entries[key]) for key in changed if key in entries)
        if actual_entries != sorted(_json_hash(entry) for entry in declared.get(side, [])):
            raise ExperimentError(
                'LOCK_CHANGE_NOT_ALLOWED', 'Cargo.lock changes differ from declared complete package entries.'
            )


def copy_payload(root: Path, candidate: Path, meta: dict[str, Any]) -> list[str]:
    targets = []
    for entry in meta.get('payload', []):
        try:
            source = reviewed_file(root, entry['source'])
        except BundleError as error:
            raise ExperimentError('PAYLOAD_SCOPE', str(error)) from error
        target = candidate.joinpath(*safe_relative(entry['target']).parts)
        if target.exists() or target.is_symlink() or any(parent.is_symlink() for parent in target.parents):
            raise ExperimentError('PAYLOAD_CONFLICT', 'Fork payload target already exists or is indirect.')
        if _sha256(source) != entry['sha256'] or source.stat().st_size != entry['size_bytes']:
            raise ExperimentError('PAYLOAD_IDENTITY_CHANGED', 'Fork payload changed since review.')
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open('xb') as stream:
            stream.write(source.read_bytes())
        if _sha256(target) != entry['sha256'] or target.stat().st_size != entry['size_bytes']:
            raise ExperimentError('PAYLOAD_IDENTITY_CHANGED', 'Copied fork payload identity differs.')
        targets.append(entry['target'])
    return targets


def apply_candidate(
    baseline_tree: Path,
    candidate_tree: Path,
    patch: Path,
    expected: list[str],
    *,
    declaration: dict[str, Any] | None = None,
    payload_root: Path | None = None,
) -> None:
    try:
        if declaration is not None:
            validate_candidates([declaration])
            if expected != declaration['changed_paths'] or _sha256(patch) != declaration['patch_sha256']:
                raise BundleError('Candidate identity differs')
        elif any(
            name != 'rust/Cargo.toml' and not (name.startswith('rust/crates/') and name.endswith('.rs'))
            for name in expected
        ):
            raise BundleError('Dependency files require an explicit dependency declaration')
        validate_patch(patch.read_bytes(), expected)
        if changed_files(baseline_tree, candidate_tree):
            raise BundleError('Candidate must start from the frozen baseline')
        for name in expected:
            reviewed_file(candidate_tree, name)
    except BundleError as error:
        raise ExperimentError('PATCH_SCOPE', str(error)) from error
    tool = shutil.which('patch')
    if tool is None:
        raise ExperimentError('TOOL_MISSING', 'The patch tool is required to apply candidates.')
    result = subprocess.run(  # noqa: S603 - fixed tool path, explicit arguments, reviewed patch file.
        [tool, '-p1', '--batch', '--forward', '--no-backup-if-mismatch', '-d', str(candidate_tree), '-i', str(patch)],
        capture_output=True,
        check=False,
        timeout=120,
    )
    if result.returncode != 0:
        raise ExperimentError('PATCH_FAILED', 'Candidate patch did not apply cleanly.')
    if changed_files(baseline_tree, candidate_tree) != sorted(expected):
        raise ExperimentError('PATCH_SCOPE', 'Applied patch changed files outside the reviewed list.')
    if declaration is not None and declaration.get('kind') in ('dependency', 'fork'):
        assert_dependency_changes(baseline_tree, candidate_tree, declaration)
        copied = copy_payload(payload_root or patch.parent, candidate_tree, declaration)
        if changed_files(baseline_tree, candidate_tree) != sorted([*expected, *copied]):
            raise ExperimentError('PATCH_SCOPE', 'Candidate contains undeclared payload files.')
    elif 'rust/Cargo.toml' in expected:
        assert_profile_only(baseline_tree / 'rust' / 'Cargo.toml', candidate_tree / 'rust' / 'Cargo.toml')


def _build_env(target_dir: Path, commit: str, epoch: int) -> dict[str, str]:
    # 剔除会悄悄改变编译结果的环境变量，保证基线与候选只差补丁本身。
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith('CARGO_PROFILE_') and key not in HERMETIC_ENV_NAMES
    }
    env.update(CARGO_TARGET_DIR=str(target_dir), COSTING_GIT_COMMIT=commit, SOURCE_DATE_EPOCH=str(epoch))
    return env


def _run_command(arguments: list[str], *, cwd: Path, env: dict[str, str], log_path: Path, timeout: float) -> None:
    with log_path.open('wb') as log:
        try:
            completed = subprocess.run(  # noqa: S603 - tool paths come from shutil.which; no shell is involved.
                arguments, cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT, check=False, timeout=timeout
            )
        except subprocess.TimeoutExpired:
            raise ExperimentError(
                'COMMAND_TIMEOUT', f'{Path(arguments[0]).name} exceeded its timeout.', retryable=True
            ) from None
    if completed.returncode != 0:
        raise ExperimentError(
            'COMMAND_FAILED', f'{Path(arguments[0]).name} exited with status {completed.returncode}; see the build log.'
        )


def build_binary(
    tree: Path,
    target_dir: Path,
    destination: Path,
    *,
    commit: str,
    epoch: int,
    log_path: Path,
    timeout: float = BUILD_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    cargo = shutil.which('cargo')
    if cargo is None:
        raise ExperimentError('TOOL_MISSING', 'cargo is required to build candidates.')
    started = time.monotonic()
    _run_command(
        [cargo, 'build', '--release', '--locked', '--manifest-path', 'rust/Cargo.toml', '-p', 'costing-calculate'],
        cwd=tree,
        env=_build_env(target_dir, commit, epoch),
        log_path=log_path,
        timeout=timeout,
    )
    seconds = time.monotonic() - started
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(target_dir / 'release' / 'costing-calculate', destination)
    destination.chmod(0o755)
    identity = json.loads(
        subprocess.run(  # noqa: S603 - the binary was just built from the verified bundle.
            [str(destination), '--version-json'], capture_output=True, text=True, check=True, timeout=30
        ).stdout
    )
    return {
        'build_seconds': seconds,
        'binary_sha256': _sha256(destination),
        'binary_bytes': destination.stat().st_size,
        'identity': identity,
    }


def run_rust_tests(
    tree: Path,
    target_dir: Path,
    *,
    commit: str,
    epoch: int,
    log_path: Path,
    timeout: float = BUILD_TIMEOUT_SECONDS,
) -> None:
    cargo = shutil.which('cargo')
    if cargo is None:
        raise ExperimentError('TOOL_MISSING', 'cargo is required to test candidates.')
    _run_command(
        [cargo, 'test', '--locked', '--manifest-path', 'rust/Cargo.toml', '--workspace', '--all-features'],
        cwd=tree,
        env=_build_env(target_dir, commit, epoch),
        log_path=log_path,
        timeout=timeout,
    )


def summarize(report: dict[str, Any]) -> dict[str, Any]:
    """Reduce one valid paired measurement to the numbers the adoption gates use."""
    samples = report['samples']
    wall = report['summary']['external_wall_seconds']
    rss = report['summary']['peak_rss_bytes']
    baseline_output = report['summary']['baseline_max_output_bytes']
    candidate_output = report['summary']['candidate_max_output_bytes']
    stages: dict[str, dict[str, float]] = {}
    for stage in samples[0]['baseline']['stage_timings']:
        observed = [
            (pair['baseline']['stage_timings'][stage], pair['candidate']['stage_timings'].get(stage))
            for pair in samples
        ]
        usable = [(left, right) for left, right in observed if right is not None and left > 0]
        stages[stage] = {
            'baseline_median_seconds': statistics.median(left for left, _ in observed),
            'median_improvement': statistics.median((left - right) / left for left, right in usable) if usable else 0.0,
        }
    return {
        'pairs': wall['pair_count'],
        'wall_improvement': wall['paired_median_improvement'],
        'wall_wins': wall['candidate_wins'],
        'baseline_wall_median_seconds': wall['baseline_median'],
        'candidate_wall_median_seconds': wall['candidate_median'],
        'rss_improvement': rss['paired_median_improvement'],
        'baseline_rss_median_bytes': rss['baseline_median'],
        'candidate_rss_median_bytes': rss['candidate_median'],
        'rss_median_ratio': rss['candidate_median'] / rss['baseline_median'] if rss['baseline_median'] else None,
        'output_ratio': candidate_output / baseline_output if baseline_output else None,
        'stages': stages,
    }


def evaluate(
    summary: dict[str, Any],
    *,
    primary: bool,
    target_stage: str | None = None,
    target_stages: list[str] | None = None,
    purpose: str = 'screening',
    improvement_gate: float | None = None,
) -> dict[str, Any]:
    gates = THRESHOLDS
    reasons: list[str] = []
    required_pairs = 5 if purpose == 'acceptance' else 8
    if purpose not in ('screening', 'acceptance'):
        reasons.append('PURPOSE_NOT_RANKABLE')
    if not isinstance(summary.get('pairs'), int) or summary['pairs'] < required_pairs:
        reasons.append('INSUFFICIENT_PAIRS')
    for metric in ('wall_improvement', 'rss_improvement', 'rss_median_ratio'):
        if not isinstance(summary.get(metric), (int, float)) or not math.isfinite(summary[metric]):
            return {'passed': False, 'reasons': [*reasons, 'SUMMARY_INVALID'], 'investigate_stages': []}
    if primary:
        if summary['wall_improvement'] < (
            gates['primary_improvement'] if improvement_gate is None else improvement_gate
        ):
            reasons.append('PRIMARY_IMPROVEMENT_BELOW_GATE')
        if summary['wall_wins'] < math.ceil(gates['primary_win_ratio'] * summary['pairs']):
            reasons.append('PRIMARY_WINS_BELOW_GATE')
    elif summary['wall_improvement'] < -gates['scenario_regression']:
        reasons.append('SCENARIO_REGRESSION')
    if summary['rss_improvement'] < -gates['rss_regression']:
        reasons.append('MEMORY_REGRESSION')
    if summary['rss_median_ratio'] > gates['rss_median_ratio']:
        reasons.append('MEMORY_MEDIAN_REGRESSION')
    if summary['output_ratio'] is not None and summary['output_ratio'] > gates['output_ratio']:
        reasons.append('OUTPUT_SIZE_GROWTH')
    scope = set(
        candidate_stages({'target_stages': target_stages})
        if target_stages is not None
        else candidate_stages({'target_stage': target_stage})
    )
    # 子阶段已登记为目标时，重叠父阶段不能再次计为非目标回退。
    scope.update(parent for parent, children in STAGE_CHILDREN.items() if scope.intersection(children))
    investigate = sorted(
        stage
        for stage, item in summary['stages'].items()
        if stage not in scope | {'total'}
        and item['baseline_median_seconds'] >= gates['stage_floor_seconds']
        and item['median_improvement'] < -gates['stage_regression']
    )
    if investigate:
        reasons.append('STAGE_REGRESSION_NEEDS_REVIEW')
    return {'passed': not reasons, 'reasons': reasons, 'investigate_stages': investigate}


def spec_problems(spec: dict[str, Any], run: dict[str, Any], *, mode: str, audit_mode: str = 'manifest') -> list[str]:
    """Unknown Manifest fields stay unknown; an audited correctness replay verifies writer selection."""
    expected = spec['expected']
    observed = run['summary']
    problems = [
        key for key in ('run_counts', 'error_log_count', 'issue_type_counts') if observed.get(key) != expected[key]
    ]
    if audit_mode == 'manifest' and run.get('low_memory_writer') is not (
        expected['low_memory_writer'] if mode == 'normal' else False
    ):
        problems.append('low_memory_writer')
    return problems


def workbook_spec_problems(
    spec: dict[str, Any],
    path: Path,
    *,
    month_start: str | None = None,
    month_end: str | None = None,
    embedded: bool = False,
    check_resources: Any = None,
) -> list[str]:
    """Count actual saved XML cells using the existing streaming workbook reader."""
    expected = spec['expected']
    names = list(expected['sheets'])
    months = {
        month: counts
        for month, counts in expected['per_month'].items()
        if (month_start is None or month >= month_start) and (month_end is None or month <= month_end)
    }
    problems = []
    package = workbook_compare._Package(path)
    try:
        if [sheet.name for sheet in package.sheets] != names:
            return ['sheets.order']
        for index, sheet in enumerate(package.sheets):
            wanted = expected['sheets'][sheet.name]
            field_name = ('detail_rows', 'qty_rows', 'analysis_rows')[index]
            wanted_months = {
                month: (0 if embedded and index == 2 else counts[field_name]) for month, counts in months.items()
            }
            rows, last_row, max_column, month_column = 0, 1, 0, None
            actual_months: Counter[str] = Counter()
            xml_problems = []
            for number, cell in enumerate(workbook_compare._iter_xml_cells(package, sheet, xml_problems)):
                if check_resources is not None and number % 4096 == 0:
                    check_resources()
                row = int(re.search(r'\d+$', cell.coordinate).group())
                column = workbook_compare._column_index(cell.coordinate)
                max_column = max(max_column, column)
                text = cell.resolved_text if cell.resolved_text is not None else cell.lexical_value
                if row == 1 and text == '月份':
                    month_column = column
                if row > 1 and row != last_row:
                    rows += 1
                last_row = row
                if row > 1 and column == month_column:
                    display = str(text)
                    chinese_month = re.fullmatch(r'(\d{4})年(\d{1,2})期', display)
                    month = f'{chinese_month[1]}-{int(chinese_month[2]):02}' if chinese_month else display
                    actual_months[month] += 1
            if xml_problems or package.package_mismatches:
                problems.append(f'{sheet.name}.xml')
            if rows != sum(wanted_months.values()) or last_row != rows + 1:
                problems.append(f'{sheet.name}.data_rows')
            if max_column != wanted['columns']:
                problems.append(f'{sheet.name}.columns')
            if month_column is None or +actual_months != +Counter(wanted_months):
                problems.append(f'{sheet.name}.per_month')
            if index == 2 and sum(wanted_months.values()) > 0 and rows == 0:
                problems.append('analysis_sheet_empty')
    finally:
        package.close()
    return problems


def validate_saved_workbooks(request: dict[str, Any]) -> dict[str, Any]:
    """Bounded worker entry; all cell interpretation comes from workbook_compare."""
    directory = Path(request['directory'])
    paths = {side: directory / f'{side}-{request["pipeline"]}.xlsx' for side in ('baseline', 'candidate')}
    problems = [
        f'{side}.{problem}'
        for side, path in paths.items()
        for problem in workbook_spec_problems(
            request['spec'],
            path,
            month_start=request.get('month_start'),
            month_end=request.get('month_end'),
            embedded=request.get('embedded', False),
        )
    ]
    replay = None
    if request.get('force_semantic'):
        alternate = directory / 'semantic-replay.xlsx'
        try:
            # 只追加 XML 末尾空白；数据与样式完全不变，确保走完整语义比较。
            with ZipFile(paths['candidate']) as source, ZipFile(alternate, 'x', compression=ZIP_DEFLATED) as target:
                for name in source.namelist():
                    with source.open(name) as incoming, target.open(name, 'w') as outgoing:
                        shutil.copyfileobj(incoming, outgoing)
                        if name == 'xl/worksheets/sheet1.xml':
                            outgoing.write(b'\n')
            if workbook_compare.workbook_payloads_identical(paths['candidate'], alternate):
                raise ValueError('Semantic replay requires different XML bytes')
            result = workbook_compare.compare_workbooks(paths['baseline'], alternate, pipeline=request['pipeline'])
            replay = {
                'status': 'passed' if result.passed else 'failed',
                'comparison_mode': 'semantic',
                'mismatch_count': len(result.mismatches),
            }
            if not result.passed:
                problems.append('semantic_replay')
        finally:
            alternate.unlink(missing_ok=True)
    return {'fixture_problems': problems, 'semantic_replay': replay}


WORKBOOK_WORKER = (
    'import sys,json; sys.path.insert(0,sys.argv[1]); '
    'from tools.validation.autotune_experiment import validate_saved_workbooks; '
    'print(json.dumps(validate_saved_workbooks(json.loads(sys.argv[2])),allow_nan=False))'
)


def _stage_noise_ok(calibration: dict[str, Any] | None, stage: str) -> bool:
    difference = (calibration or {}).get('stage_median_differences', {}).get(stage)
    return difference is not None and abs(difference) < THRESHOLDS['stage_noise']


def _drop_workbooks(directory: Path) -> None:
    if directory.is_dir():
        for path in directory.glob('*.xlsx'):
            path.unlink(missing_ok=True)


def _correctness_scenarios() -> list[tuple[str, str, str, int, str, str | None, str | None]]:
    scenarios = [
        (f'{pipeline}-small-{name}', pipeline, 'small', SEED, mode, start, end)
        for pipeline in ('gb', 'sk')
        for name, mode, start, end in SMALL_CHECKS
    ]
    scenarios += [
        (f'{pipeline}-medium-normal', pipeline, 'medium', SEED, 'normal', None, None) for pipeline in ('gb', 'sk')
    ]
    scenarios += [
        (f'sk-full-{name}', 'sk', 'full-sk', SEED, mode, start, end) for name, mode, start, end in SMALL_CHECKS
    ]
    scenarios.append(SEMANTIC_SCENARIO)
    return scenarios


@dataclass
class Experiment:
    bundle: Path
    work: Path
    manifest: dict[str, Any]
    deadline: float
    pairs: int = 8
    calibration_pairs: int = 4
    run_timeout: float = 600.0
    memory_limit_bytes: int = 2 * 1024**3
    disk_limit_bytes: int = 2 * 1024**3
    rust_tests: bool = True
    audit_mode: str = 'manifest'
    measuring: bool = False
    fixtures: dict[tuple[str, str, int], dict[str, Any]] = field(default_factory=dict)
    calibrations: dict[tuple[Any, ...], dict[str, Any]] = field(default_factory=dict)
    report: dict[str, Any] = field(default_factory=dict)
    audited: set[tuple[Any, ...]] = field(default_factory=set)

    def remaining(self) -> float:
        seconds = self.deadline - time.monotonic()
        if seconds <= 0:
            raise BudgetExhausted
        return seconds

    def check_budget(self) -> None:
        self.remaining()

    def save(self) -> None:
        self.report['updated_utc'] = datetime.now(UTC).isoformat()
        _save_json(self.work / 'experiment-report.json', self.report)

    def check_resources(self, directory: Path) -> None:
        self.check_budget()
        rss = int(Path('/proc/self/statm').read_text(encoding='ascii').split()[1]) * os.sysconf('SC_PAGE_SIZE')
        if rss > self.memory_limit_bytes:
            raise ExperimentError('MEMORY_BUDGET_EXCEEDED', 'Workbook validation exceeded its memory budget.')
        if sum(path.stat().st_size for path in directory.rglob('*') if path.is_file()) > self.disk_limit_bytes:
            raise ExperimentError('DISK_BUDGET_EXCEEDED', 'Workbook validation exceeded its disk budget.')

    def frozen_artifacts(self) -> dict[str, str]:
        artifacts = {}
        for folder in ('trees', 'builds', 'fixtures'):
            for path in (self.work / folder).rglob('*'):
                if path.is_symlink():
                    raise ExperimentError('IDENTITY_CHANGED', 'Prepared artifacts cannot be symbolic links.')
                if path.is_file():
                    artifacts[path.relative_to(self.work).as_posix()] = _sha256(path)
        return artifacts

    def freeze(self) -> None:
        verify_bundle(self.bundle)
        for name, build in self.report['builds'].items():
            tree = self.work / 'trees' / name
            if build['source_files'] != {
                path.relative_to(tree).as_posix(): _sha256(path) for path in tree.rglob('*') if path.is_file()
            } or build['binary_sha256'] != _sha256(self.binary(name)):
                raise ExperimentError(
                    'IDENTITY_CHANGED', 'Tests or correctness checks changed a built source or binary.'
                )
        self.report['frozen'] = {
            'artifacts': self.frozen_artifacts(),
            'manifest_sha256': _json_hash(self.manifest),
            'bundle_manifest_sha256': _sha256(self.bundle / 'bundle-manifest.json'),
            'protocol_sha256': protocol_identity(),
            'environment': environment(),
        }
        self.measuring = True
        self.save()

    def verify_frozen(self) -> str:
        if verify_bundle(self.bundle) != self.manifest:
            raise ExperimentError('IDENTITY_CHANGED', 'Bundle identity changed after preparation.')
        frozen = self.report['frozen']
        if (
            self.frozen_artifacts() != frozen['artifacts']
            or _json_hash(self.manifest) != frozen['manifest_sha256']
            or _sha256(self.bundle / 'bundle-manifest.json') != frozen['bundle_manifest_sha256']
            or environment() != frozen['environment']
        ):
            raise ExperimentError(
                'IDENTITY_CHANGED', 'A prepared artifact or the frozen experiment environment changed.'
            )
        return _json_hash(frozen)

    def fixture(self, pipeline: str, size: str, seed: int) -> dict[str, Any]:
        key = (pipeline, size, seed)
        if key not in self.fixtures:
            if self.measuring:
                raise ExperimentError('PREPARATION_INCOMPLETE', 'All inputs must be generated before measurement.')
            self.check_budget()
            directory = self.work / 'fixtures' / f'{pipeline}-{size}-{seed}'
            _log(f'generating fixture {pipeline}/{size}/{seed}')
            spec = (
                build_full_sk_fixture(
                    directory, orders=FULL_SK_PRIMARY_ORDERS if size == 'full-sk-medium' else 64, seed=seed
                )
                if pipeline == 'sk' and size in ('full-sk', 'full-sk-medium')
                else build_benchmark_fixture(directory, pipeline, size, seed)  # type: ignore[arg-type]
            )
            self.fixtures[key] = {
                'input': directory / spec['files']['input'],
                'config': directory / spec['files']['config'],
                'spec': spec,
            }
        return self.fixtures[key]

    def binary(self, name: str) -> Path:
        return self.work / 'builds' / name / 'costing-calculate'

    def build(self, name: str, meta: dict[str, Any] | None) -> Path:
        if self.measuring:
            raise ExperimentError('PREPARATION_CLOSED', 'Builds must finish before measurement.')
        verify_bundle(self.bundle)
        self.check_budget()
        _log(f'building {name}')
        tree = self.work / 'trees' / name
        prepare_tree(self.bundle, tree)
        if meta is not None:
            apply_candidate(
                self.work / 'trees' / 'baseline',
                tree,
                self.bundle / meta['patch'],
                meta['changed_paths'],
                declaration=meta,
                payload_root=self.bundle,
            )
        source_identity = {
            path.relative_to(tree).as_posix(): _sha256(path) for path in tree.rglob('*') if path.is_file()
        }
        # 不同副本可能保留旧 mtime；共享产物会复用前一候选的业务代码。
        try:
            self.report['builds'][name] = build_binary(
                tree,
                self.work / 'cargo-target' / name,
                self.binary(name),
                commit=self.manifest['base_commit'],
                epoch=self.manifest['source_date_epoch'],
                log_path=self.work / 'logs' / f'build-{name}.log',
                timeout=min(BUILD_TIMEOUT_SECONDS, self.remaining()),
            )
        finally:
            self.check_budget()
        self.report['builds'][name]['source_files'] = source_identity
        if source_identity != {
            path.relative_to(tree).as_posix(): _sha256(path) for path in tree.rglob('*') if path.is_file()
        }:
            raise ExperimentError('IDENTITY_CHANGED', 'Build modified the prepared source tree.')
        self.save()
        return self.binary(name)

    def run_tests(self, name: str) -> dict[str, Any]:
        if self.measuring:
            raise ExperimentError('PREPARATION_CLOSED', 'Rust tests must finish before measurement.')
        _log(f'running Rust tests for {name}')
        started = time.monotonic()
        try:
            run_rust_tests(
                self.work / 'trees' / name,
                self.work / 'cargo-test-target' / name,
                commit=self.manifest['base_commit'],
                epoch=self.manifest['source_date_epoch'],
                log_path=self.work / 'logs' / f'test-{name}.log',
                timeout=min(BUILD_TIMEOUT_SECONDS, self.remaining()),
            )
        except ExperimentError as error:
            return {'status': 'failed', **error.payload()}
        finally:
            self.check_budget()
        return {'status': 'passed', 'seconds': time.monotonic() - started}

    def correctness(
        self,
        baseline: Path,
        candidate: Path,
        prefix: str,
        scenarios: list[tuple[str, str, str, int, str, str | None, str | None]],
        *,
        audit_mode: str = 'manifest',
        embedded: bool = False,
    ) -> dict[str, Any]:
        outcomes: dict[str, Any] = {}
        for name, pipeline, size, seed, mode, month_start, month_end in scenarios:
            self.check_budget()
            fixture = self.fixture(pipeline, size, seed)
            directory = (
                self.work / 'comparisons' / f'{prefix}-{name}-{audit_mode}-{"embedded" if embedded else "external"}'
            )
            _log(f'comparing {prefix}/{name}/{audit_mode}')
            try:
                comparison = compare_releases(
                    baseline_binary=baseline,
                    candidate_binary=candidate,
                    pipeline=pipeline,  # type: ignore[arg-type]
                    input_path=fixture['input'],
                    output_dir=directory,
                    month_start=month_start,
                    month_end=month_end,
                    config_path=None if embedded else fixture['config'],
                    mode=mode,  # type: ignore[arg-type]
                    audit_mode=audit_mode,
                    purpose='screening',
                    timeout_seconds=min(self.run_timeout, self.remaining() / 3),
                    memory_limit_bytes=self.memory_limit_bytes,
                    disk_limit_bytes=self.disk_limit_bytes,
                )
                outcome = {
                    'status': comparison['status'],
                    'mismatch_count': comparison['mismatch_count'],
                    'comparison_mode': comparison['comparison_mode'],
                    'audit_mode': audit_mode,
                }
                if comparison['mismatches']:
                    outcome['mismatches'] = comparison['mismatches'][:20]
                problems = []
                if mode == 'normal':
                    verified = run_process(
                        [
                            sys.executable,
                            '-I',
                            '-c',
                            WORKBOOK_WORKER,
                            str(TOOL_ROOT),
                            json.dumps(
                                {
                                    'spec': fixture['spec'],
                                    'directory': str(directory),
                                    'pipeline': pipeline,
                                    'month_start': month_start,
                                    'month_end': month_end,
                                    'embedded': embedded,
                                    'force_semantic': (name, pipeline, size, seed, mode, month_start, month_end)
                                    == SEMANTIC_SCENARIO
                                    and comparison['comparison_mode'] != 'semantic',
                                }
                            ),
                        ],
                        directory=directory,
                        cwd=TOOL_ROOT,
                        timeout=min(self.run_timeout, self.remaining() / 2),
                        memory_limit_bytes=self.memory_limit_bytes,
                        disk_limit_bytes=self.disk_limit_bytes,
                    )['payload']
                    problems.extend(verified['fixture_problems'])
                    outcome['semantic_replay'] = verified['semantic_replay']
                for side in ('baseline', 'candidate'):
                    if month_start is None and month_end is None and not embedded:
                        expected = fixture['spec']['expected']
                        problems.extend(
                            f'{side}.{key}'
                            for key in ('run_counts', 'error_log_count', 'issue_type_counts')
                            if comparison[side].get(key) != expected[key]
                        )
                    if audit_mode == 'manifest':
                        manifest = json.loads(
                            (directory / f'{side}-{pipeline}.manifest.json').read_text(encoding='utf-8')
                        )
                        writer = manifest['execution']['low_memory_writer']
                        wanted_writer = (
                            fixture['spec']['expected']['low_memory_writer']
                            if mode == 'normal' and month_start is None and month_end is None
                            else False
                        )
                        if type(writer) is not bool or writer != wanted_writer:
                            problems.append(f'{side}.low_memory_writer')
                if audit_mode == 'none':
                    replay = self.correctness(
                        baseline,
                        candidate,
                        f'{prefix}-audit-replay',
                        [(name, pipeline, size, seed, mode, month_start, month_end)],
                        embedded=embedded,
                    )[name]
                    outcome['audit_replay'] = replay
                    if replay['status'] != 'passed':
                        problems.append('audit_replay')
                if problems:
                    outcome.update(status='failed', fixture_expectation_mismatch=problems)
                if (
                    outcome['status'] == 'passed'
                    and audit_mode == 'manifest'
                    and month_start is None
                    and month_end is None
                    and not embedded
                ):
                    for binary in (baseline, candidate):
                        self.audited.add((str(binary), pipeline, size, seed, mode))
                self.check_resources(directory)
                _save_json(directory / 'comparison.json', comparison)
            except (ValidationError, MeasurementError, ExperimentError, OSError, ValueError, RuntimeError) as error:
                code = (
                    error.error_code
                    if isinstance(error, ValidationError)
                    else error.code
                    if isinstance(error, (ExperimentError, MeasurementError))
                    else 'VALIDATION_ERROR'
                )
                outcome = {'status': 'failed', 'error_code': code}
            finally:
                _drop_workbooks(directory)
                self.check_budget()
            directory.mkdir(parents=True, exist_ok=True)
            _save_json(directory / 'outcome.json', outcome)
            outcomes[name] = outcome
        return outcomes

    def measure(
        self,
        label: str,
        baseline: Path,
        candidate: Path,
        scenario: tuple[str, str, int, str],
        pairs: int,
        *,
        calibration: bool = False,
        audit_mode: str | None = None,
        purpose: str = 'screening',
    ) -> dict[str, Any]:
        pipeline, size, seed, mode = scenario
        audit_mode = self.audit_mode if audit_mode is None else audit_mode
        identity = self.verify_frozen()
        self.check_budget()
        fixture = self.fixture(pipeline, size, seed)
        if audit_mode == 'none' and any(
            (str(binary), pipeline, size, seed, mode) not in self.audited for binary in (baseline, candidate)
        ):
            return {'label': label, 'status': 'invalid', 'error': {'error_code': 'AUDIT_REPLAY_REQUIRED'}}
        _log(f'measuring {label} ({pairs} pairs)')
        os.sync()
        time.sleep(min(SETTLE_SECONDS, self.remaining()))
        try:
            report = measure_pairs(
                baseline_binary=baseline,
                candidate_binary=candidate,
                input_path=fixture['input'],
                output_dir=self.work / 'measurements' / label,
                pipeline=pipeline,
                config_path=fixture['config'],
                mode=mode,
                audit_mode=audit_mode,
                purpose='calibration' if calibration else purpose,
                pairs=pairs,
                timeout=self.run_timeout,
                memory_limit_bytes=self.memory_limit_bytes,
                disk_limit_bytes=self.disk_limit_bytes,
                calibration=calibration,
                deadline=self.deadline,
            )
        except MeasurementError as error:
            return {'label': label, 'status': 'invalid', 'error': error.payload()}
        finally:
            self.check_budget()
            self.verify_frozen()
        if report['status'] != 'valid':
            return {'label': label, 'status': 'invalid', 'error': report['error'], 'calibration': report['calibration']}
        expected_purpose = 'calibration' if calibration else purpose
        if (
            len(report.get('samples', [])) != pairs
            or len(report.get('warmups', [])) != 2
            or report.get('summary') != paired_statistics(report['samples'])
            or report['options'].get('purpose') != expected_purpose
            or report['options'].get('audit_mode') != audit_mode
        ):
            raise ExperimentError(
                'REPORT_IDENTITY_MISMATCH',
                'Measurement samples, statistics, purpose or audit mode differ from the frozen request.',
            )
        for side, path in (
            ('baseline', baseline),
            ('candidate', candidate),
            ('input', fixture['input']),
            ('config', fixture['config']),
        ):
            if report['artifacts'].get(side) != {'sha256': _sha256(path), 'size_bytes': path.stat().st_size}:
                raise ExperimentError(
                    'REPORT_IDENTITY_MISMATCH', 'Measurement reference artifact differs from the prepared version.'
                )
        runs = [*report['warmups'], *(pair[side] for pair in report['samples'] for side in ('baseline', 'candidate'))]
        problems = sorted(
            {
                problem
                for run in runs
                for problem in spec_problems(fixture['spec'], run, mode=mode, audit_mode=audit_mode)
            }
        )
        return {
            'label': label,
            'status': 'valid',
            'purpose': expected_purpose,
            'audit_mode': audit_mode,
            'frozen_identity_sha256': identity,
            'references': {'baseline': _sha256(baseline), 'candidate': _sha256(candidate)},
            'summary': summarize(report),
            'fixture_problems': problems,
            'calibration': report['calibration'],
        }

    def judge(
        self,
        measurement: dict[str, Any],
        *,
        primary: bool,
        target_stage: str | None = None,
        target_stages: list[str] | None = None,
        improvement_gate: float | None = None,
    ) -> dict[str, Any]:
        if measurement['status'] != 'valid':
            return {'passed': False, 'reasons': ['MEASUREMENT_INVALID'], 'investigate_stages': []}
        evaluation = evaluate(
            measurement['summary'],
            primary=primary,
            target_stage=target_stage,
            target_stages=target_stages,
            purpose=measurement.get('purpose', 'unknown'),
            improvement_gate=improvement_gate,
        )
        if measurement.get('frozen_identity_sha256') != self.verify_frozen():
            evaluation['passed'] = False
            evaluation['reasons'].append('IDENTITY_CHANGED')
        if measurement['fixture_problems']:
            evaluation['passed'] = False
            evaluation['reasons'].append('FIXTURE_EXPECTATION_MISMATCH')
        measurement['evaluation'] = evaluation
        return evaluation

    def calibrate(
        self,
        baseline: Path,
        pipeline: str,
        size: str,
        *,
        seed: int = SEED,
        mode: str = 'normal',
        audit_mode: str | None = None,
    ) -> dict[str, Any]:
        audit_mode = self.audit_mode if audit_mode is None else audit_mode
        key = (pipeline, size, seed, mode, audit_mode)
        label = (
            f'{pipeline}-{size}'
            if (seed, mode, audit_mode) == (SEED, 'normal', 'manifest')
            else f'{pipeline}-{size}-{seed}-{mode}-{audit_mode}'
        )
        if key not in self.calibrations:
            attempts = []
            for attempt in (1, 2):
                result = self.measure(
                    f'calibration-{label}-a{attempt}',
                    baseline,
                    baseline,
                    (pipeline, size, seed, mode),
                    self.calibration_pairs,
                    calibration=True,
                    audit_mode=audit_mode,
                )
                attempts.append(result)
                if result['status'] == 'valid' or result.get('error', {}).get('error_code') != 'NOISE_EXCEEDED':
                    break
            self.calibrations[key] = {
                'status': 'passed'
                if attempts[-1]['status'] == 'valid' and not attempts[-1].get('fixture_problems')
                else 'failed',
                'attempts': attempts,
            }
            self.report['calibration'][label] = self.calibrations[key]
            self.save()
        return self.calibrations[key]

    def prepare(self, baseline: Path, meta: dict[str, Any]) -> dict[str, Any]:
        name = meta['id']
        entry: dict[str, Any] = {
            'status': 'running',
            'hypothesis': meta.get('hypothesis'),
            'target_stages': candidate_stages(meta),
            'purpose': meta.get('purpose', 'screening'),
        }
        self.report['candidates'][name] = entry
        try:
            binary = self.build(name, None if name == CONTROL_ID else meta)
            if self.rust_tests and name != CONTROL_ID:
                entry['rust_tests'] = self.run_tests(name)
                if entry['rust_tests']['status'] != 'passed':
                    entry['status'] = 'tests-failed'
                    return entry
            pipeline, size = meta['primary_pipeline'], meta['primary_size']
            scenarios = _correctness_scenarios()
            if size != 'medium':
                scenarios.append((f'{pipeline}-{size}-primary', pipeline, size, SEED, 'normal', None, None))
            entry['correctness'] = self.correctness(baseline, binary, name, scenarios, audit_mode=self.audit_mode)
            embedded = [(f'{pipe}-embedded', pipe, 'small', SEED, 'normal', None, None) for pipe in ('gb', 'sk')]
            entry['embedded_correctness'] = self.correctness(baseline, binary, name, embedded, embedded=True)
            entry['status'] = (
                'prepared'
                if set(entry['correctness']) == {scenario[0] for scenario in scenarios}
                and set(entry['embedded_correctness']) == {scenario[0] for scenario in embedded}
                and all(
                    item['status'] == 'passed'
                    for group in ('correctness', 'embedded_correctness')
                    for item in entry[group].values()
                )
                else 'correctness-failed'
            )
        except ExperimentError as error:
            entry.update(status='failed', error=error.payload())
        finally:
            self.save()
        return entry

    def screen(self, baseline: Path, meta: dict[str, Any]) -> dict[str, Any]:
        name = meta['id']
        entry = self.report['candidates'][name]
        if entry['status'] != 'prepared':
            return entry
        stages = candidate_stages(meta)
        pipeline, size = meta['primary_pipeline'], meta['primary_size']
        calibration = self.calibrations[(pipeline, size, SEED, 'normal', self.audit_mode)]['attempts'][-1].get(
            'calibration'
        )
        if any(not _stage_noise_ok(calibration, stage) for stage in stages):
            entry['status'] = 'target-stage-noisy'
            return entry
        measurement = self.measure(
            f'{name}-{pipeline}-{size}-primary',
            baseline,
            self.binary(name),
            (pipeline, size, SEED, 'normal'),
            self.pairs,
            purpose=meta.get('purpose', 'screening'),
        )
        entry['primary'] = measurement
        passed = self.judge(measurement, primary=True, target_stages=stages)['passed']
        entry['status'] = (
            ('screen-passed' if passed else 'rejected') if measurement['status'] == 'valid' else 'measurement-invalid'
        )
        if meta.get('purpose') == 'diagnostic':
            entry['status'] = 'diagnostic'
        self.save()
        return entry

    def prepare_extended(self, baseline: Path, name: str, meta: dict[str, Any]) -> None:
        primary_key = (meta['primary_pipeline'], meta['primary_size'], SEED, 'normal')
        scenarios = [
            (f'{pipeline}-{size}-{seed}-{mode}', pipeline, size, seed, mode, None, None)
            for pipeline, size, seed, mode in EXTENDED_SCENARIOS
            if (pipeline, size, seed, mode) != primary_key
        ]
        boundary = [
            (f'{pipeline}-below-threshold', pipeline, 'below-threshold', SEED, 'normal', None, None)
            for pipeline in ('gb', 'sk')
        ]
        entry = self.report['candidates'][name]
        entry['required_extended_scenarios'] = [scenario[0] for scenario in [*boundary, *scenarios]]
        entry['extended_preparation'] = self.correctness(
            baseline, self.binary(name), name, [*boundary, *scenarios], audit_mode=self.audit_mode
        )
        if self.audit_mode == 'manifest':
            no_manifest = [
                (f'{pipeline}-medium-no-manifest', pipeline, 'medium', SEED, 'normal', None, None)
                for pipeline in ('gb', 'sk')
            ]
            entry['required_extended_scenarios'].extend(scenario[0] for scenario in no_manifest)
            entry['extended_preparation'].update(
                self.correctness(baseline, self.binary(name), name, no_manifest, audit_mode='none')
            )
        self.save()

    def extend(self, baseline: Path, name: str, meta: dict[str, Any]) -> dict[str, Any]:
        primary_key = (meta['primary_pipeline'], meta['primary_size'], SEED, 'normal')
        prepared = self.report['candidates'][name]['extended_preparation']
        result: dict[str, Any] = {'correctness': prepared, 'scenarios': {}, 'passed': False}
        self.report['candidates'][name]['extended'] = result
        if set(prepared) != set(self.report['candidates'][name]['required_extended_scenarios']) or not all(
            item['status'] == 'passed' for item in prepared.values()
        ):
            return result
        passed = True
        timed = [(scenario, self.audit_mode) for scenario in EXTENDED_SCENARIOS if scenario != primary_key]
        if self.audit_mode == 'manifest':
            timed += [((pipeline, 'medium', SEED, 'normal'), 'none') for pipeline in ('gb', 'sk')]
        for scenario, audit_mode in timed:
            pipeline, size, seed, mode = scenario
            label = f'{pipeline}-{size}-{seed}-{mode}-{audit_mode}'
            calibration = self.calibrate(baseline, pipeline, size, seed=seed, mode=mode, audit_mode=audit_mode)
            if calibration['status'] != 'passed' or (
                mode == 'normal'
                and any(
                    not _stage_noise_ok(calibration['attempts'][-1].get('calibration'), stage)
                    for stage in candidate_stages(meta)
                )
            ):
                result['scenarios'][label] = {'status': 'noise-invalid'}
                passed = False
                continue
            measurement = self.measure(
                f'{name}-{label}', baseline, self.binary(name), scenario, self.pairs, audit_mode=audit_mode
            )
            result['scenarios'][label] = measurement
            passed = self.judge(measurement, primary=False, target_stages=candidate_stages(meta))['passed'] and passed
            self.save()
        result['passed'] = passed
        return result

    def combine(self, baseline: Path, meta: dict[str, Any]) -> None:
        name = meta['id']
        entry = self.report['candidates'][name]
        singles = self.report['recommended']
        if not all(component in singles for component in meta['components']):
            entry['status'] = 'combination-components-unverified'
            return
        best = max(singles, key=lambda item: self.report['candidates'][item]['primary']['summary']['wall_improvement'])
        self.report['combination'] = {
            'id': name,
            'components': meta['components'],
            'best_single': best,
            'component_identities': {component: _sha256(self.binary(component)) for component in meta['components']},
        }
        self.screen(baseline, meta)
        entry['against_baseline'] = entry.get('primary')
        if entry['status'] != 'screen-passed':
            return
        scenario = (meta['primary_pipeline'], meta['primary_size'], SEED, 'normal')
        against_best = self.measure(
            f'{name}-against-best-{best}', self.binary(best), self.binary(name), scenario, self.pairs
        )
        entry['against_best'] = against_best
        if not self.judge(
            against_best,
            primary=True,
            target_stages=candidate_stages(meta),
            improvement_gate=THRESHOLDS['combination_improvement'],
        )['passed']:
            entry['status'] = 'combination-no-extra-gain'
            return
        if against_best.get('references') != {
            'baseline': _sha256(self.binary(best)),
            'candidate': _sha256(self.binary(name)),
        }:
            raise ExperimentError(
                'REPORT_IDENTITY_MISMATCH', 'Combination must compare against the frozen best single binary.'
            )
        self.prepare_extended(baseline, name, meta)
        if self.extend(baseline, name, meta)['passed']:
            entry['status'] = 'recommended-pending-target-validation'
            self.report['recommended'] = [name, best]
        else:
            entry['status'] = 'rejected-extended'
        self.save()

    def run(self, candidates: list[dict[str, Any]]) -> str:
        baseline = self.build('baseline', None)
        if self.rust_tests:
            self.report['baseline_rust_tests'] = self.run_tests('baseline')
            self.save()
            if self.report['baseline_rust_tests']['status'] != 'passed':
                return 'baseline-tests-failed'
        first = candidates[0]
        control = {
            'id': CONTROL_ID,
            'hypothesis': 'Unpatched rebuild from the same bundle; screening must reject it.',
            'target_stage': None,
            'primary_pipeline': first['primary_pipeline'],
            'primary_size': first['primary_size'],
        }
        for meta in (control, *candidates):
            self.prepare(baseline, meta)
        combination = self.manifest.get('combination')
        if combination is not None and set(combination['components']).issubset({meta['id'] for meta in candidates}):
            self.prepare(baseline, combination)
        else:
            combination = None
        # 生成所有可能入围的输入；测速开始后禁止再编译、测试或生成 fixture。
        for pipeline, size, seed, _mode in EXTENDED_SCENARIOS:
            self.fixture(pipeline, size, seed)
        for pipeline in ('gb', 'sk'):
            self.fixture(pipeline, 'below-threshold', SEED)
        self.freeze()
        for meta in (control, *candidates):
            if self.calibrate(baseline, meta['primary_pipeline'], meta['primary_size'])['status'] != 'passed':
                return 'noise-invalid'
        control_result = self.screen(baseline, control)
        if (
            control_result['status'] != 'rejected'
            or control_result['primary']['summary']['wall_improvement'] >= THRESHOLDS['primary_improvement']
        ):
            return 'control-failed'
        for meta in candidates:
            self.screen(baseline, meta)
        ranked = sorted(
            (
                (entry['primary']['summary']['wall_improvement'], name)
                for name, entry in self.report['candidates'].items()
                if name != CONTROL_ID and entry['status'] == 'screen-passed'
            ),
            reverse=True,
        )
        self.report['shortlist'] = [name for _, name in ranked[:MAX_SHORTLIST]]
        by_id = {meta['id']: meta for meta in candidates}
        for name in self.report['shortlist']:
            self.prepare_extended(baseline, name, by_id[name])
        for name in self.report['shortlist']:
            entry = self.report['candidates'][name]
            entry['extended'] = self.extend(baseline, name, by_id[name])
            entry['status'] = 'rejected-extended'
            if entry['extended']['passed']:
                entry['status'] = (
                    'recommended-pending-target-validation' if self.rust_tests else 'unverified-rust-tests'
                )
            self.save()
        self.report['recommended'] = [
            name for name, entry in self.report['candidates'].items() if entry['status'].startswith('recommended')
        ]
        if combination is not None and self.report['candidates'][combination['id']]['status'] == 'prepared':
            self.combine(baseline, combination)
        return 'completed'


def run_experiment(
    *,
    bundle_root: Path,
    work_dir: Path,
    only: list[str] | None = None,
    pairs: int = 8,
    calibration_pairs: int = 4,
    budget_seconds: float = 21600.0,
    rust_tests: bool = True,
    audit_mode: str = 'manifest',
    run_timeout: float = 600.0,
    memory_limit_bytes: int = 2 * 1024**3,
    disk_limit_bytes: int = 2 * 1024**3,
) -> dict[str, Any]:
    started = time.monotonic()
    bundle_root, work_dir = bundle_root.resolve(), work_dir.resolve()
    if work_dir == bundle_root or bundle_root in work_dir.parents or work_dir in bundle_root.parents:
        raise ExperimentError('INVALID_ARGUMENT', 'Bundle and work directories must be separate.')
    if (
        sys.platform != 'linux'
        or type(pairs) is not int
        or type(calibration_pairs) is not int
        or not 8 <= pairs <= 100
        or not 4 <= calibration_pairs <= 100
        or not math.isfinite(budget_seconds)
        or not 0 < budget_seconds <= 21600
        or not math.isfinite(run_timeout)
        or not 0 < run_timeout <= 600
        or not 0 < min(memory_limit_bytes, disk_limit_bytes) <= max(memory_limit_bytes, disk_limit_bytes) <= 2 * 1024**3
        or audit_mode not in ('manifest', 'none')
    ):
        raise ExperimentError(
            'INVALID_ARGUMENT', 'Linux, at least 8 pairs, 4 calibration pairs and bounded budgets are required.'
        )
    manifest = verify_bundle(bundle_root)
    candidates = [meta for meta in manifest['candidates'] if only is None or meta['id'] in only]
    if not candidates or (only is not None and len(candidates) != len(set(only))):
        raise ExperimentError('NO_CANDIDATES', 'Requested candidates are missing from the bundle.')
    if len({(meta['primary_pipeline'], meta['primary_size']) for meta in candidates}) != 1:
        raise ExperimentError('INVALID_ARGUMENT', 'Ranked candidates must share one predeclared primary scenario.')
    work_dir.mkdir(parents=True, exist_ok=False)
    for name in ('fixtures', 'measurements', 'comparisons', 'builds', 'trees', 'logs'):
        (work_dir / name).mkdir()
    experiment = Experiment(
        bundle=bundle_root,
        work=work_dir,
        manifest=manifest,
        deadline=started + budget_seconds,
        pairs=pairs,
        calibration_pairs=calibration_pairs,
        run_timeout=run_timeout,
        memory_limit_bytes=memory_limit_bytes,
        disk_limit_bytes=disk_limit_bytes,
        rust_tests=rust_tests,
        audit_mode=audit_mode,
    )
    experiment.report = {
        'schema_version': 1,
        'status': 'running',
        'started_utc': datetime.now(UTC).isoformat(),
        'finished_utc': None,
        'base_commit': manifest['base_commit'],
        'bundle_inventory_sha256': manifest['source_inventory_sha256'],
        'bundle_manifest_sha256': _sha256(bundle_root / 'bundle-manifest.json'),
        'protocol_sha256': protocol_identity(),
        'tool_files': manifest['tool_files'],
        'runtime_files': manifest['runtime_files'],
        'audit_mode': audit_mode,
        'purpose': 'screening',
        'thresholds': dict(THRESHOLDS),
        'limits': {
            'budget_seconds': budget_seconds,
            'pairs': pairs,
            'calibration_pairs': calibration_pairs,
            'run_timeout_seconds': run_timeout,
            'memory_limit_bytes': memory_limit_bytes,
            'disk_limit_bytes': disk_limit_bytes,
        },
        'environment': environment(),
        'builds': {},
        'calibration': {},
        'candidates': {},
        'shortlist': [],
        'recommended': [],
        'error': None,
    }
    experiment.save()
    try:
        status = experiment.run(candidates)
    except BudgetExhausted:
        status = 'budget-exhausted'
    except (ExperimentError, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        status = 'failed'
        experiment.report['error'] = (
            error.payload()
            if isinstance(error, ExperimentError)
            else ExperimentError('EXPERIMENT_FAILED', 'Experiment could not finish; see the local logs.').payload()
        )
    except KeyboardInterrupt:
        status = 'interrupted'
    if status != 'completed':
        experiment.report['recommended'] = []
        experiment.report['shortlist'] = []
    else:
        try:
            experiment.verify_frozen()
        except ExperimentError as error:
            status = 'failed'
            experiment.report.update(recommended=[], shortlist=[], error=error.payload())
    for entry in experiment.report['candidates'].values():
        if entry['status'] == 'running':
            entry['status'] = 'not-completed'
    experiment.report.update(
        status=status, finished_utc=datetime.now(UTC).isoformat(), elapsed_seconds=time.monotonic() - started
    )
    experiment.save()
    return experiment.report


def run_acceptance(
    *,
    bundle_root: Path,
    screening_report: dict[str, Any],
    candidate_id: str,
    baseline_binary: Path,
    candidate_binary: Path,
    input_path: Path,
    output_dir: Path,
    pipeline: str,
    config_path: Path | None = None,
    audit_mode: str = 'none',
    resource_scenario: str = 'standard',
    timeout: float = 600,
    memory_limit_bytes: int = 2 * 1024**3,
    disk_limit_bytes: int = 2 * 1024**3,
    budget_seconds: float = 21600,
) -> dict[str, Any]:
    """Independent Linux N=5 replay with explicit budgets bounded by the registered resource scenario."""
    scenario = ACCEPTANCE_RESOURCE_SCENARIOS.get(resource_scenario) if isinstance(resource_scenario, str) else None
    if (
        sys.platform != 'linux'
        or scenario is None
        or pipeline not in scenario['pipelines']
        or audit_mode not in ('manifest', 'none')
        or type(timeout) not in (int, float)
        or not 0 < timeout <= 600
        or type(budget_seconds) not in (int, float)
        or not 0 < budget_seconds <= 21600
        or type(memory_limit_bytes) is not int
        or type(disk_limit_bytes) is not int
        or not 0 < memory_limit_bytes <= scenario['memory_limit_bytes']
        or not 0 < disk_limit_bytes <= scenario['disk_limit_bytes']
    ):
        raise ExperimentError('INVALID_ARGUMENT', 'Acceptance requires Linux and the frozen resource budgets.')
    manifest = verify_bundle(bundle_root.resolve())
    bundle_hash = _sha256(bundle_root / 'bundle-manifest.json')
    entry = screening_report.get('candidates', {}).get(candidate_id, {})
    if (
        screening_report.get('status') != 'completed'
        or screening_report.get('purpose') != 'screening'
        or candidate_id not in screening_report.get('recommended', [])
        or entry.get('primary', {}).get('summary', {}).get('pairs', 0) < 8
        or entry.get('primary', {}).get('purpose') != 'screening'
        or not entry.get('extended', {}).get('passed')
        or screening_report.get('protocol_sha256') != manifest['protocol_sha256']
        or screening_report.get('tool_files') != manifest['tool_files']
        or screening_report.get('bundle_manifest_sha256') != bundle_hash
        or screening_report.get('runtime_files') != manifest['runtime_files']
    ):
        raise ExperimentError(
            'SCREENING_REQUIRED',
            'Acceptance requires an eligible eight-pair screening result from these frozen tools and sources.',
        )
    for name, binary in (('baseline', baseline_binary), (candidate_id, candidate_binary)):
        if screening_report.get('builds', {}).get(name, {}).get('binary_sha256') != _sha256(binary):
            raise ExperimentError('IDENTITY_CHANGED', 'Acceptance binaries differ from the screened builds.')
    meta = next(
        item
        for item in [*manifest['candidates'], *([manifest['combination']] if manifest.get('combination') else [])]
        if item['id'] == candidate_id
    )
    paths = {'baseline': baseline_binary, 'candidate': candidate_binary, 'input': input_path}
    if config_path is not None:
        paths['config'] = config_path
    identities = {name: {'sha256': _sha256(path), 'size_bytes': path.stat().st_size} for name, path in paths.items()}
    frozen_resources = {
        'protocol_sha256': protocol_identity(),
        'resource_scenario': resource_scenario,
        'limits': {
            'memory_limit_bytes': memory_limit_bytes,
            'disk_limit_bytes': disk_limit_bytes,
            'run_timeout_seconds': timeout,
            'budget_seconds': budget_seconds,
        },
    }
    resource_identity = _json_hash(frozen_resources)
    deadline = time.monotonic() + budget_seconds

    def unchanged() -> None:
        if (
            _json_hash(frozen_resources) != resource_identity
            or protocol_identity() != frozen_resources['protocol_sha256']
        ):
            raise ExperimentError('IDENTITY_CHANGED', 'Acceptance resource scenario or frozen budgets changed.')
        if (
            verify_bundle(bundle_root.resolve()) != manifest
            or _sha256(bundle_root / 'bundle-manifest.json') != bundle_hash
        ):
            raise ExperimentError('IDENTITY_CHANGED', 'Acceptance source declarations or frozen stage scope changed.')
        if identities != {
            name: {'sha256': _sha256(path), 'size_bytes': path.stat().st_size} for name, path in paths.items()
        }:
            raise ExperimentError('IDENTITY_CHANGED', 'An acceptance artifact changed during the batch.')
        if time.monotonic() >= deadline:
            raise ExperimentError('BUDGET_EXHAUSTED', 'Acceptance batch exhausted its budget.')

    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=False)
    comparisons = {}
    for audit in dict.fromkeys((audit_mode, 'manifest')):
        unchanged()
        directory = output_dir / f'correctness-{audit}'
        try:
            comparison = compare_releases(
                baseline_binary=baseline_binary,
                candidate_binary=candidate_binary,
                input_path=input_path,
                output_dir=directory,
                pipeline=pipeline,
                config_path=config_path,
                mode='normal',
                audit_mode=audit,
                purpose='acceptance',
                timeout_seconds=min(timeout, (deadline - time.monotonic()) / 3),
                memory_limit_bytes=memory_limit_bytes,
                disk_limit_bytes=disk_limit_bytes,
            )
            if audit == 'manifest':
                writers = {
                    side: json.loads((directory / f'{side}-{pipeline}.manifest.json').read_text(encoding='utf-8'))[
                        'execution'
                    ]['low_memory_writer']
                    for side in ('baseline', 'candidate')
                }
                comparison['writer_replay'] = writers
                if (
                    any(type(value) is not bool for value in writers.values())
                    or writers['baseline'] != writers['candidate']
                ):
                    comparison['status'] = 'failed'
            comparisons[audit] = comparison
        finally:
            _drop_workbooks(directory)
        unchanged()
    result: dict[str, Any] = {
        'purpose': 'acceptance',
        'audit_mode': audit_mode,
        'status': 'failed',
        'correctness': comparisons,
        'candidate_id': candidate_id,
        **frozen_resources,
        'resource_identity_sha256': resource_identity,
        'artifacts': identities,
    }
    if all(item['status'] == 'passed' for item in comparisons.values()):
        common = {
            'baseline_binary': baseline_binary,
            'candidate_binary': candidate_binary,
            'input_path': input_path,
            'pipeline': pipeline,
            'config_path': config_path,
            'audit_mode': audit_mode,
            'mode': 'normal',
            'timeout': timeout,
            'memory_limit_bytes': memory_limit_bytes,
            'disk_limit_bytes': disk_limit_bytes,
            'deadline': deadline,
        }
        calibration = measure_pairs(
            **{**common, 'candidate_binary': baseline_binary},
            output_dir=output_dir / 'calibration',
            pairs=4,
            calibration=True,
            purpose='calibration',
        )
        result['calibration'] = calibration
        unchanged()
        if calibration['status'] == 'valid' and all(
            _stage_noise_ok(calibration.get('calibration'), stage) for stage in candidate_stages(meta)
        ):
            measurement = measure_pairs(**common, output_dir=output_dir / 'acceptance', pairs=5, purpose='acceptance')
            result['measurement'] = measurement
            unchanged()
            if measurement['status'] == 'valid':
                if (
                    measurement['options'].get('purpose') != 'acceptance'
                    or measurement['options'].get('audit_mode') != audit_mode
                    or measurement.get('artifacts') != identities
                    or len(measurement['samples']) != 5
                ):
                    raise ExperimentError(
                        'REPORT_IDENTITY_MISMATCH', 'Acceptance report identities or five-pair purpose differ.'
                    )
                result['evaluation'] = evaluate(
                    summarize(measurement), primary=True, target_stages=candidate_stages(meta), purpose='acceptance'
                )
                result['status'] = 'passed' if result['evaluation']['passed'] else 'failed'
    _save_json(output_dir / 'acceptance-report.json', result)
    return result


def _overview(report: dict[str, Any]) -> dict[str, Any]:
    rows = {}
    for name, entry in report['candidates'].items():
        summary = (entry.get('primary') or {}).get('summary') or {}
        rows[name] = {
            'status': entry['status'],
            'wall_improvement': summary.get('wall_improvement'),
            'wall_wins': summary.get('wall_wins'),
            'rss_improvement': summary.get('rss_improvement'),
        }
    return {
        'status': report['status'],
        'shortlist': report['shortlist'],
        'recommended': report['recommended'],
        'candidates': rows,
        'error': report['error'],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle-root', type=Path, required=True)
    parser.add_argument('--work-dir', type=Path, required=True)
    parser.add_argument('--only', help='comma-separated candidate ids')
    parser.add_argument('--pairs', type=int, default=8)
    parser.add_argument('--calibration-pairs', type=int, default=4)
    parser.add_argument('--budget-seconds', type=float, default=21600.0)
    parser.add_argument('--run-timeout', type=float, default=600.0)
    parser.add_argument('--audit-mode', choices=('manifest', 'none'), default='manifest')
    parser.add_argument('--rust-tests', action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args()
    try:
        report = run_experiment(
            bundle_root=args.bundle_root,
            work_dir=args.work_dir,
            only=[item for item in args.only.split(',') if item] if args.only else None,
            pairs=args.pairs,
            calibration_pairs=args.calibration_pairs,
            budget_seconds=args.budget_seconds,
            rust_tests=args.rust_tests,
            audit_mode=args.audit_mode,
            run_timeout=args.run_timeout,
        )
    except (ExperimentError, OSError, ValueError, KeyError) as error:
        failure = (
            error.payload()
            if isinstance(error, ExperimentError)
            else ExperimentError('EXPERIMENT_FAILED', 'Experiment could not start or finish.').payload()
        )
        print(json.dumps({'status': 'failed', **failure}, ensure_ascii=False))
        return 2
    print(json.dumps(_overview(report), ensure_ascii=False, indent=2))
    return 0 if report['status'] == 'completed' else 2


if __name__ == '__main__':
    raise SystemExit(main())

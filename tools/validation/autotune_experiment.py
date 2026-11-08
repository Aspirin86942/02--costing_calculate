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
import shutil
import statistics
import subprocess
import sys
import time
import tomllib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tools.validation.compare_releases import ValidationError, compare_releases
from tools.validation.measure_paired_release import MeasurementError, measure_pairs
from tools.validation.prepare_colab_bundle import BundleError, validate_candidates, validate_patch
from tools.validation.synthetic_inputs import build_benchmark_fixture

SEED = 20261108
VALIDATION_SEED = 20261109
CONTROL_ID = 'control'
MAX_SHORTLIST = 2
BUILD_TIMEOUT_SECONDS = 3600.0
SETTLE_SECONDS = 3.0
# 与 docs/plans/2026-11-08-colab-offline-autotuning.md 第 6.4 节一致；开测前冻结，运行中不得修改。
THRESHOLDS = {
    'primary_improvement': 0.05,
    'primary_win_ratio': 0.75,
    'scenario_regression': 0.02,
    'rss_regression': 0.05,
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
)
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
    }


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
        for name, identity in expected.items():
            if _sha256(present[name]) != identity['sha256'] or present[name].stat().st_size != identity['size_bytes']:
                raise ValueError('Bundle file identity differs')
        for candidate in manifest['candidates']:
            if candidate['patch_sha256'] != expected[candidate['patch']]['sha256']:
                raise ValueError('Candidate patch identity differs')
        inventory = hashlib.sha256(json.dumps(expected, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        if inventory != manifest.get('source_inventory_sha256'):
            raise ValueError('Bundle inventory differs')
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise ExperimentError(
            'BUNDLE_INVALID', 'Bundle files or candidate identities could not be verified.'
        ) from error
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
        return {path.relative_to(root).as_posix(): path for path in root.rglob('*') if path.is_file()}

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


def apply_candidate(baseline_tree: Path, candidate_tree: Path, patch: Path, expected: list[str]) -> None:
    try:
        validate_patch(patch.read_bytes(), expected)
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
    if 'rust/Cargo.toml' in expected:
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
        'output_ratio': candidate_output / baseline_output if baseline_output else None,
        'stages': stages,
    }


def evaluate(summary: dict[str, Any], *, primary: bool, target_stage: str | None) -> dict[str, Any]:
    gates = THRESHOLDS
    reasons: list[str] = []
    if primary:
        if summary['wall_improvement'] < gates['primary_improvement']:
            reasons.append('PRIMARY_IMPROVEMENT_BELOW_GATE')
        if summary['wall_wins'] < math.ceil(gates['primary_win_ratio'] * summary['pairs']):
            reasons.append('PRIMARY_WINS_BELOW_GATE')
    elif summary['wall_improvement'] < -gates['scenario_regression']:
        reasons.append('SCENARIO_REGRESSION')
    if summary['rss_improvement'] < -gates['rss_regression']:
        reasons.append('MEMORY_REGRESSION')
    if summary['output_ratio'] is not None and summary['output_ratio'] > gates['output_ratio']:
        reasons.append('OUTPUT_SIZE_GROWTH')
    # 非目标阶段明显变慢必须先解释原因，因此在自动流程里按未通过处理。
    investigate = sorted(
        stage
        for stage, item in summary['stages'].items()
        if stage not in (target_stage, 'total')
        and item['baseline_median_seconds'] >= gates['stage_floor_seconds']
        and item['median_improvement'] < -gates['stage_regression']
    )
    if investigate:
        reasons.append('STAGE_REGRESSION_NEEDS_REVIEW')
    return {'passed': not reasons, 'reasons': reasons, 'investigate_stages': investigate}


def spec_problems(spec: dict[str, Any], run: dict[str, Any], *, mode: str) -> list[str]:
    """Compare one measured run with the fixture specification so generator drift is caught."""
    expected = spec['expected']
    observed = run['summary']
    problems = [key for key in ('run_counts', 'error_log_count', 'issue_type_counts') if observed[key] != expected[key]]
    if mode == 'normal' and run['low_memory_writer'] is not expected['low_memory_writer']:
        problems.append('low_memory_writer')
    return problems


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
    fixtures: dict[tuple[str, str, int], dict[str, Any]] = field(default_factory=dict)
    calibrations: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    report: dict[str, Any] = field(default_factory=dict)

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

    def fixture(self, pipeline: str, size: str, seed: int) -> dict[str, Any]:
        key = (pipeline, size, seed)
        if key not in self.fixtures:
            self.check_budget()
            directory = self.work / 'fixtures' / f'{pipeline}-{size}-{seed}'
            _log(f'generating fixture {pipeline}/{size}/{seed}')
            spec = build_benchmark_fixture(directory, pipeline, size, seed)  # type: ignore[arg-type]
            self.fixtures[key] = {
                'input': directory / spec['files']['input'],
                'config': directory / spec['files']['config'],
                'spec': spec,
            }
        return self.fixtures[key]

    def binary(self, name: str) -> Path:
        return self.work / 'builds' / name / 'costing-calculate'

    def build(self, name: str, meta: dict[str, Any] | None) -> Path:
        self.check_budget()
        _log(f'building {name}')
        tree = self.work / 'trees' / name
        prepare_tree(self.bundle, tree)
        if meta is not None:
            apply_candidate(self.work / 'trees' / 'baseline', tree, self.bundle / meta['patch'], meta['changed_paths'])
        try:
            self.report['builds'][name] = build_binary(
                tree,
                self.work / 'cargo-target',
                self.binary(name),
                commit=self.manifest['base_commit'],
                epoch=self.manifest['source_date_epoch'],
                log_path=self.work / 'logs' / f'build-{name}.log',
                timeout=min(BUILD_TIMEOUT_SECONDS, self.remaining()),
            )
        finally:
            self.check_budget()
        self.save()
        return self.binary(name)

    def run_tests(self, name: str) -> dict[str, Any]:
        _log(f'running Rust tests for {name}')
        started = time.monotonic()
        try:
            run_rust_tests(
                self.work / 'trees' / name,
                self.work / 'cargo-test-target',
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
    ) -> dict[str, Any]:
        outcomes: dict[str, Any] = {}
        for name, pipeline, size, seed, mode, month_start, month_end in scenarios:
            self.check_budget()
            fixture = self.fixture(pipeline, size, seed)
            directory = self.work / 'comparisons' / f'{prefix}-{name}'
            _log(f'comparing {prefix}/{name}')
            try:
                comparison = compare_releases(
                    baseline_binary=baseline,
                    candidate_binary=candidate,
                    pipeline=pipeline,  # type: ignore[arg-type]
                    input_path=fixture['input'],
                    output_dir=directory,
                    month_start=month_start,
                    month_end=month_end,
                    config_path=fixture['config'],
                    mode=mode,  # type: ignore[arg-type]
                    timeout_seconds=min(self.run_timeout, self.remaining() / 2),
                )
            except (ValidationError, OSError, ValueError, RuntimeError) as error:
                code = error.error_code if isinstance(error, ValidationError) else 'VALIDATION_ERROR'
                outcomes[name] = {'status': 'failed', 'error_code': code}
                continue
            finally:
                _drop_workbooks(directory)
                self.check_budget()
            _save_json(directory / 'comparison.json', comparison)
            outcome = {
                'status': comparison['status'],
                'mismatch_count': comparison['mismatch_count'],
                'comparison_mode': comparison['comparison_mode'],
            }
            if comparison['mismatches']:
                outcome['mismatches'] = comparison['mismatches'][:20]
            if month_start is None and month_end is None:
                expected = fixture['spec']['expected']
                problems = [
                    key
                    for key in ('run_counts', 'error_log_count', 'issue_type_counts')
                    if comparison['baseline'].get(key) != expected[key]
                ]
                if problems:
                    outcome.update(status='failed', fixture_expectation_mismatch=problems)
            if outcome['status'] != 'passed':
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
    ) -> dict[str, Any]:
        pipeline, size, seed, mode = scenario
        self.check_budget()
        fixture = self.fixture(pipeline, size, seed)
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
        if report['status'] != 'valid':
            return {'label': label, 'status': 'invalid', 'error': report['error'], 'calibration': report['calibration']}
        return {
            'label': label,
            'status': 'valid',
            'summary': summarize(report),
            'fixture_problems': spec_problems(fixture['spec'], report['warmups'][0], mode=mode),
            'calibration': report['calibration'],
        }

    def judge(self, measurement: dict[str, Any], *, primary: bool, target_stage: str | None) -> dict[str, Any]:
        if measurement['status'] != 'valid':
            return {'passed': False, 'reasons': ['MEASUREMENT_INVALID'], 'investigate_stages': []}
        evaluation = evaluate(measurement['summary'], primary=primary, target_stage=target_stage)
        if measurement['fixture_problems']:
            evaluation['passed'] = False
            evaluation['reasons'].append('FIXTURE_EXPECTATION_MISMATCH')
        measurement['evaluation'] = evaluation
        return evaluation

    def calibrate(self, baseline: Path, pipeline: str, size: str) -> dict[str, Any]:
        key = (pipeline, size)
        if key not in self.calibrations:
            attempts = []
            for attempt in (1, 2):
                result = self.measure(
                    f'calibration-{pipeline}-{size}-a{attempt}',
                    baseline,
                    baseline,
                    (pipeline, size, SEED, 'normal'),
                    self.calibration_pairs,
                    calibration=True,
                )
                attempts.append(result)
                if result['status'] == 'valid' or result.get('error', {}).get('error_code') != 'NOISE_EXCEEDED':
                    break
            self.calibrations[key] = {
                'status': 'passed' if attempts[-1]['status'] == 'valid' else 'failed',
                'attempts': attempts,
            }
            self.report['calibration'][f'{pipeline}-{size}'] = self.calibrations[key]
            self.save()
        return self.calibrations[key]

    def screen(self, baseline: Path, meta: dict[str, Any]) -> dict[str, Any]:
        name = meta['id']
        stage = meta.get('target_stage')
        entry: dict[str, Any] = {'status': 'running', 'hypothesis': meta.get('hypothesis'), 'target_stage': stage}
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
            entry['correctness'] = self.correctness(baseline, binary, name, scenarios)
            if any(item['status'] != 'passed' for item in entry['correctness'].values()):
                entry['status'] = 'correctness-failed'
                return entry
            calibration = self.calibrations[(pipeline, size)]['attempts'][-1].get('calibration')
            if stage is not None and not _stage_noise_ok(calibration, stage):
                entry['status'] = 'target-stage-noisy'
                return entry
            measurement = self.measure(
                f'{name}-{pipeline}-{size}-primary', baseline, binary, (pipeline, size, SEED, 'normal'), self.pairs
            )
            entry['primary'] = measurement
            passed = self.judge(measurement, primary=True, target_stage=stage)['passed']
            entry['status'] = (
                ('screen-passed' if passed else 'rejected')
                if measurement['status'] == 'valid'
                else 'measurement-invalid'
            )
        except ExperimentError as error:
            entry.update(status='failed', error=error.payload())
        finally:
            self.save()
        return entry

    def extend(self, baseline: Path, name: str, meta: dict[str, Any]) -> dict[str, Any]:
        binary = self.binary(name)
        stage = meta.get('target_stage')
        primary_key = (meta['primary_pipeline'], meta['primary_size'], SEED, 'normal')
        boundary = [
            (f'{pipeline}-below-threshold', pipeline, 'below-threshold', SEED, 'normal', None, None)
            for pipeline in ('gb', 'sk')
        ]
        result: dict[str, Any] = {}
        self.report['candidates'][name]['extended'] = result
        result['boundary_correctness'] = self.correctness(baseline, binary, name, boundary)
        passed = all(item['status'] == 'passed' for item in result['boundary_correctness'].values())
        result['scenarios'] = {}
        for pipeline, size, seed, mode in EXTENDED_SCENARIOS:
            if (pipeline, size, seed, mode) == primary_key:
                continue
            label = f'{pipeline}-{size}-{seed}-{mode}'
            scenario: dict[str, Any] = {
                'correctness': self.correctness(
                    baseline, binary, name, [(label, pipeline, size, seed, mode, None, None)]
                )[label]
            }
            if scenario['correctness']['status'] == 'passed':
                scenario['measurement'] = self.measure(
                    f'{name}-{label}', baseline, binary, (pipeline, size, seed, mode), self.pairs
                )
                self.judge(scenario['measurement'], primary=False, target_stage=stage)
            result['scenarios'][label] = scenario
            passed = passed and scenario.get('measurement', {}).get('evaluation', {}).get('passed', False)
            self.save()
        result['passed'] = passed
        return result

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
        or not 8 <= pairs <= 100
        or not 4 <= calibration_pairs <= 100
        or not math.isfinite(budget_seconds)
        or not 0 < budget_seconds <= 21600
        or not math.isfinite(run_timeout)
        or run_timeout <= 0
        or min(memory_limit_bytes, disk_limit_bytes) <= 0
    ):
        raise ExperimentError(
            'INVALID_ARGUMENT', 'Linux, at least 8 pairs, 4 calibration pairs and bounded budgets are required.'
        )
    manifest = verify_bundle(bundle_root)
    candidates = [meta for meta in manifest['candidates'] if only is None or meta['id'] in only]
    if not candidates or (only is not None and len(candidates) != len(set(only))):
        raise ExperimentError('NO_CANDIDATES', 'Requested candidates are missing from the bundle.')
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
    )
    experiment.report = {
        'schema_version': 1,
        'status': 'running',
        'started_utc': datetime.now(UTC).isoformat(),
        'finished_utc': None,
        'base_commit': manifest['base_commit'],
        'bundle_inventory_sha256': manifest['source_inventory_sha256'],
        'thresholds': THRESHOLDS,
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
    for entry in experiment.report['candidates'].values():
        if entry['status'] == 'running':
            entry['status'] = 'not-completed'
    experiment.report.update(
        status=status, finished_utc=datetime.now(UTC).isoformat(), elapsed_seconds=time.monotonic() - started
    )
    experiment.save()
    return experiment.report


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

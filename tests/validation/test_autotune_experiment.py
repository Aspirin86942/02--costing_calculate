from __future__ import annotations

import hashlib
import json
import shutil
import sys
from pathlib import Path
from typing import Any

import pytest

from tools.validation import autotune_experiment as tune
from tools.validation.measure_paired_release import paired_statistics

pytestmark = pytest.mark.skipif(sys.platform != 'linux', reason='Linux paired experiment')


def _summary(improvement: float = 0.05) -> dict[str, Any]:
    return {
        'pairs': 8,
        'wall_improvement': improvement,
        'wall_wins': 6,
        'rss_improvement': -0.05,
        'output_ratio': 1.02,
        'stages': {'xlsx_save': {'baseline_median_seconds': 0.3, 'median_improvement': improvement}},
    }


@pytest.mark.parametrize(
    ('field', 'value', 'reason'),
    [
        ('wall_improvement', 0.049999, 'PRIMARY_IMPROVEMENT_BELOW_GATE'),
        ('wall_wins', 5, 'PRIMARY_WINS_BELOW_GATE'),
        ('rss_improvement', -0.050001, 'MEMORY_REGRESSION'),
        ('output_ratio', 1.020001, 'OUTPUT_SIZE_GROWTH'),
    ],
)
def test_primary_thresholds_are_inclusive_and_reject_values_outside(field: str, value: float, reason: str) -> None:
    summary = _summary()
    assert tune.evaluate(summary, primary=True, target_stage='xlsx_save')['passed']
    summary[field] = value
    result = tune.evaluate(summary, primary=True, target_stage='xlsx_save')
    assert not result['passed']
    assert result['reasons'] == [reason]


def test_other_scenarios_and_stage_regressions_keep_frozen_gates() -> None:
    summary = _summary(-0.02)
    assert tune.evaluate(summary, primary=False, target_stage='xlsx_save')['passed']
    summary['wall_improvement'] = -0.020001
    assert 'SCENARIO_REGRESSION' in tune.evaluate(summary, primary=False, target_stage='xlsx_save')['reasons']
    summary = _summary()
    summary['stages']['ingest'] = {'baseline_median_seconds': 0.3, 'median_improvement': -0.050001}
    result = tune.evaluate(summary, primary=True, target_stage='xlsx_save')
    assert result['investigate_stages'] == ['ingest']
    assert result['reasons'] == ['STAGE_REGRESSION_NEEDS_REVIEW']
    assert tune._stage_noise_ok({'stage_median_differences': {'ingest': 0.049}}, 'ingest')
    assert not tune._stage_noise_ok({'stage_median_differences': {'ingest': 0.05}}, 'ingest')
    assert not tune._stage_noise_ok(None, 'ingest')


def test_summary_preserves_per_pair_ratios_and_maximum_output_sizes() -> None:
    pairs = []
    for left, right in ((10, 9), (100, 110), (20, 18), (200, 220)):
        pairs.append(
            {
                'baseline': {
                    'external_wall_seconds': left,
                    'peak_rss_bytes': 100,
                    'output_size_bytes': 20,
                    'stage_timings': {'ingest': left},
                },
                'candidate': {
                    'external_wall_seconds': right,
                    'peak_rss_bytes': 90,
                    'output_size_bytes': 21,
                    'stage_timings': {'ingest': right},
                },
            }
        )
    result = tune.summarize({'samples': pairs, 'summary': paired_statistics(pairs)})
    assert result['wall_improvement'] == 0
    assert result['stages']['ingest']['median_improvement'] == 0
    assert result['wall_wins'] == 2
    assert result['output_ratio'] == 1.05


def _hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


@pytest.fixture
def bundle(tmp_path: Path) -> Path:
    root = tmp_path / 'bundle'
    files = {'source/rust/Cargo.toml': b'[workspace]\n', 'source/rust-toolchain.toml': b'[toolchain]\n'}
    candidates = []
    for number in range(1, 5):
        name = f'candidate-{number}'
        patch = f'patches/{name}.patch'
        files[patch] = b'candidate patch'
        candidates.append(
            {
                'id': name,
                'patch': patch,
                'patch_sha256': _hash(files[patch]),
                'changed_paths': ['rust/crates/costing-xlsx/src/writer.rs'],
                'hypothesis': 'Reduce repeated conversion in writer',
                'primary_pipeline': 'gb',
                'primary_size': 'medium',
                'target_stage': 'xlsx_save',
            }
        )
    inventory = {}
    for name, content in files.items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        inventory[name] = {'sha256': _hash(content), 'size_bytes': len(content)}
    manifest = {
        'schema_version': 1,
        'base_commit': '1' * 40,
        'source_date_epoch': 1704067200,
        'files': inventory,
        'candidates': candidates,
        'source_inventory_sha256': _hash(json.dumps(inventory, sort_keys=True, separators=(',', ':')).encode()),
    }
    (root / 'bundle-manifest.json').write_text(json.dumps(manifest), encoding='utf-8')
    return root


@pytest.mark.parametrize('change', ['tamper', 'extra', 'patch-identity', 'symlink'])
def test_bundle_verification_rejects_changed_or_indirect_files(bundle: Path, change: str) -> None:
    assert len(tune.verify_bundle(bundle)['candidates']) == 4
    if change == 'tamper':
        (bundle / 'source/rust/Cargo.toml').write_text('changed', encoding='utf-8')
    elif change == 'extra':
        (bundle / 'unreviewed.py').write_text('print(1)', encoding='utf-8')
    elif change == 'symlink':
        (bundle / 'indirect').symlink_to(bundle / 'source/rust/Cargo.toml')
    else:
        path = bundle / 'bundle-manifest.json'
        manifest = json.loads(path.read_text(encoding='utf-8'))
        manifest['candidates'][0]['patch_sha256'] = '0' * 64
        path.write_text(json.dumps(manifest), encoding='utf-8')
    with pytest.raises(tune.ExperimentError) as failure:
        tune.verify_bundle(bundle)
    assert failure.value.code == 'BUNDLE_INVALID'


def test_cargo_profile_candidate_cannot_change_dependencies_or_codegen_units(tmp_path: Path) -> None:
    before, after = tmp_path / 'before.toml', tmp_path / 'after.toml'
    original = (
        '[workspace]\nmembers = []\n[workspace.dependencies]\nserde = "1"\n[profile.release]\ncodegen-units = 1\n'
    )
    before.write_text(original, encoding='utf-8')
    after.write_text(original + 'lto = "fat"\n', encoding='utf-8')
    tune.assert_profile_only(before, after)
    for changed in (
        original.replace('serde = "1"', 'serde = "2"'),
        original.replace('codegen-units = 1', 'codegen-units = 2'),
    ):
        after.write_text(changed, encoding='utf-8')
        with pytest.raises(tune.ExperimentError) as failure:
            tune.assert_profile_only(before, after)
        assert failure.value.code == 'MANIFEST_CHANGE_NOT_ALLOWED'


def test_patch_scope_is_checked_before_execution_and_after_application(tmp_path: Path) -> None:
    if shutil.which('patch') is None:
        pytest.skip('patch is unavailable')
    baseline, candidate = tmp_path / 'before', tmp_path / 'after'
    source = baseline / 'rust/crates/costing-xlsx/src/writer.rs'
    source.parent.mkdir(parents=True)
    source.write_text('fn before() {}\n', encoding='utf-8')
    shutil.copytree(baseline, candidate)
    relative = source.relative_to(baseline).as_posix()
    patch = tmp_path / 'writer.patch'
    content = f'--- a/{relative}\n+++ b/{relative}\n@@ -1 +1 @@\n-fn before() {{}}\n+fn after() {{}}\n'
    patch.write_text(content.replace(f'--- a/{relative}', '--- a/../../outside.rs'), encoding='utf-8')
    with pytest.raises(tune.ExperimentError) as failure:
        tune.apply_candidate(baseline, candidate, patch, [relative])
    assert failure.value.code == 'PATCH_SCOPE'
    assert (candidate / relative).read_text(encoding='utf-8') == 'fn before() {}\n'
    patch.write_text(content, encoding='utf-8')
    tune.apply_candidate(baseline, candidate, patch, [relative])
    assert (candidate / relative).read_text(encoding='utf-8') == 'fn after() {}\n'


@pytest.fixture
def stubbed(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    state: dict[str, Any] = {'built': [], 'measured': [], 'tests': [], 'control_improvement': 0.0, 'noisy': False}

    def build(experiment: tune.Experiment, name: str, meta: Any) -> Path:
        state['built'].append(name)
        if state.get('budget'):
            raise tune.BudgetExhausted
        return experiment.binary(name)

    def run_tests(experiment: tune.Experiment, name: str) -> dict[str, str]:
        state['tests'].append(name)
        return {'status': 'failed' if state.get('failed_tests') == name else 'passed'}

    def correctness(experiment: tune.Experiment, baseline: Path, candidate: Path, prefix: str, scenarios: Any) -> Any:
        state.setdefault('validated', {}).setdefault(prefix, []).extend(scenarios)
        return {
            scenario[0]: {'status': 'failed' if state.get('bad_workbook') == prefix else 'passed'}
            for scenario in scenarios
        }

    def measure(
        experiment: tune.Experiment,
        label: str,
        baseline: Path,
        candidate: Path,
        scenario: Any,
        pairs: int,
        **kwargs: Any,
    ) -> Any:
        state['measured'].append(label)
        name = candidate.parent.name
        if kwargs.get('calibration'):
            if state['noisy']:
                return {'status': 'invalid', 'error': {'error_code': 'NOISE_EXCEEDED'}}
            return {
                'status': 'valid',
                'calibration': {'stage_median_differences': {'xlsx_save': 0.0}},
                'fixture_problems': [],
            }
        improvement = (
            state['control_improvement']
            if name == 'control'
            else {'candidate-1': 0.06, 'candidate-2': 0.09, 'candidate-3': 0.07, 'candidate-4': 0.01}[name]
        )
        return {'status': 'valid', 'summary': _summary(improvement), 'fixture_problems': []}

    monkeypatch.setattr(tune.Experiment, 'build', build)
    monkeypatch.setattr(tune.Experiment, 'run_tests', run_tests)
    monkeypatch.setattr(tune.Experiment, 'correctness', correctness)
    monkeypatch.setattr(tune.Experiment, 'measure', measure)
    return state


def test_large_primary_scenario_is_compared_before_it_is_timed(bundle: Path, tmp_path: Path, stubbed: Any) -> None:
    path = bundle / 'bundle-manifest.json'
    manifest = json.loads(path.read_text(encoding='utf-8'))
    manifest['candidates'][0]['primary_size'] = 'above-threshold'
    path.write_text(json.dumps(manifest), encoding='utf-8')
    report = tune.run_experiment(bundle_root=bundle, work_dir=tmp_path / 'run', only=['candidate-1'])
    assert report['status'] == 'completed'
    assert any(
        item[1:5] == ('gb', 'above-threshold', tune.SEED, 'normal') for item in stubbed['validated']['candidate-1']
    )
    assert 'candidate-1-gb-above-threshold-primary' in stubbed['measured']


def test_default_experiment_rejects_control_and_shortlists_only_two(bundle: Path, tmp_path: Path, stubbed: Any) -> None:
    work = tmp_path / 'experiment'
    report = tune.run_experiment(bundle_root=bundle, work_dir=work)
    assert report['status'] == 'completed'
    assert report['candidates']['control']['status'] == 'rejected'
    assert report['shortlist'] == ['candidate-2', 'candidate-3']
    assert report['recommended'] == ['candidate-2', 'candidate-3']
    assert 'candidate-1' not in report['recommended']
    assert stubbed['tests'] == ['baseline', 'candidate-1', 'candidate-2', 'candidate-3', 'candidate-4']
    assert json.loads((work / 'experiment-report.json').read_text(encoding='utf-8')) == report


@pytest.mark.parametrize(
    ('setting', 'value', 'status'),
    [
        ('noisy', True, 'noise-invalid'),
        ('control_improvement', 0.08, 'control-failed'),
        ('budget', True, 'budget-exhausted'),
        ('failed_tests', 'baseline', 'baseline-tests-failed'),
    ],
)
def test_noise_control_tests_and_budget_prevent_recommendation(
    bundle: Path, tmp_path: Path, stubbed: Any, setting: str, value: Any, status: str
) -> None:
    stubbed[setting] = value
    report = tune.run_experiment(bundle_root=bundle, work_dir=tmp_path / 'run')
    assert report['status'] == status
    assert report['recommended'] == []
    assert 'candidate-1' not in stubbed['built']
    if setting == 'noisy':
        assert len(report['calibration']['gb-medium']['attempts']) == 2


def test_bad_workbook_is_not_timed_and_skipping_rust_tests_cannot_recommend(
    bundle: Path, tmp_path: Path, stubbed: Any
) -> None:
    stubbed['bad_workbook'] = 'candidate-1'
    report = tune.run_experiment(bundle_root=bundle, work_dir=tmp_path / 'run', rust_tests=False)
    assert report['candidates']['candidate-1']['status'] == 'correctness-failed'
    assert not any(label.startswith('candidate-1-') for label in stubbed['measured'])
    assert report['recommended'] == []
    assert report['candidates']['candidate-2']['status'] == 'unverified-rust-tests'


@pytest.mark.parametrize(
    'arguments',
    [
        {'pairs': 7},
        {'calibration_pairs': 3},
        {'run_timeout': 0},
        {'budget_seconds': 21601},
        {'budget_seconds': float('nan')},
    ],
)
def test_protocol_cannot_be_weakened(bundle: Path, tmp_path: Path, arguments: dict[str, Any]) -> None:
    work = tmp_path / 'run'
    with pytest.raises(tune.ExperimentError) as failure:
        tune.run_experiment(bundle_root=bundle, work_dir=work, **arguments)
    assert failure.value.code == 'INVALID_ARGUMENT'
    assert not work.exists()


def test_only_requires_all_requested_candidates_to_exist(bundle: Path, tmp_path: Path) -> None:
    with pytest.raises(tune.ExperimentError) as failure:
        tune.run_experiment(bundle_root=bundle, work_dir=tmp_path / 'run', only=['missing'])
    assert failure.value.code == 'NO_CANDIDATES'

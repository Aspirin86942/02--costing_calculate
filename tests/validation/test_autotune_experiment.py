from __future__ import annotations

import difflib
import hashlib
import json
import shutil
import sys
import time
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
        'rss_median_ratio': 1.05,
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
    files = {
        **{f'source/{name}': (tune.TOOL_ROOT / name).read_bytes() for name in tune.TOOL_FILES},
        'source/rust/Cargo.toml': b'[workspace]\n',
        'source/rust-toolchain.toml': b'[toolchain]\n',
    }
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
        'tool_files': tune.tool_inventory(inventory),
        'runtime_files': {
            name.removeprefix('source/'): value for name, value in inventory.items() if name.startswith('source/rust')
        },
        'protocol_sha256': tune.protocol_identity(),
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


def _dependency_patch(tmp_path: Path, *, extra: str = '') -> tuple[Path, Path, Path, dict[str, Any]]:
    baseline, candidate = tmp_path / 'baseline', tmp_path / 'candidate'
    manifest = '[workspace.dependencies]\nflate2 = "1.0"\n[profile.release]\ncodegen-units = 1\n'
    old = {
        'name': 'flate2',
        'version': '1.0',
        'source': 'registry+example',
        'checksum': 'a' * 64,
        'dependencies': ['zlib 0.6'],
    }
    new = {**old, 'version': '1.1', 'checksum': 'b' * 64}
    lock = (
        'version = 4\n\n[[package]]\nname = "flate2"\nversion = "1.0"\nsource = "registry+example"\nchecksum = "'
        + 'a' * 64
        + '"\ndependencies = ["zlib 0.6"]\n'
    )
    sources = {'rust/Cargo.toml': manifest, 'rust/Cargo.lock': lock}
    updated = {
        'rust/Cargo.toml': manifest.replace('"1.0"', '"1.1"') + extra,
        'rust/Cargo.lock': lock.replace('"1.0"', '"1.1"').replace('a' * 64, 'b' * 64),
    }
    patch_text = ''
    for name, content in sources.items():
        path = baseline / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding='utf-8')
        patch_text += ''.join(
            difflib.unified_diff(
                content.splitlines(True), updated[name].splitlines(True), fromfile=f'a/{name}', tofile=f'b/{name}'
            )
        )
    shutil.copytree(baseline, candidate)
    patch = tmp_path / 'dependency.patch'
    patch.write_text(patch_text, encoding='utf-8')
    meta = {
        'id': 'dependency',
        'kind': 'dependency',
        'patch': patch.name,
        'patch_sha256': _hash(patch.read_bytes()),
        'changed_paths': list(sources),
        'hypothesis': 'Reduce compression overhead',
        'primary_pipeline': 'gb',
        'primary_size': 'medium',
        'cargo_changes': {'rust/Cargo.toml': [['workspace', 'dependencies', 'flate2']]},
        'lock_changes': {'before': [old], 'after': [new]},
    }
    return baseline, candidate, patch, meta


def test_dependency_patch_validates_exact_keys_and_complete_lock_entries(tmp_path: Path) -> None:
    baseline, candidate, patch, meta = _dependency_patch(tmp_path)
    tune.apply_candidate(baseline, candidate, patch, meta['changed_paths'], declaration=meta)
    assert '"1.1"' in (candidate / 'rust/Cargo.toml').read_text()


@pytest.mark.parametrize('case', ['profile', 'checksum', 'source', 'transitive', 'same-name-version', 'undeclared-key'])
def test_dependency_rejects_undeclared_changes(tmp_path: Path, case: str) -> None:
    baseline, candidate, patch, meta = _dependency_patch(tmp_path, extra='lto = "fat"\n' if case == 'profile' else '')
    if case == 'checksum':
        meta['lock_changes']['after'][0]['checksum'] = 'c' * 64
    elif case == 'source':
        meta['lock_changes']['after'][0]['source'] = 'git+unreviewed'
    elif case == 'transitive':
        meta['lock_changes']['after'][0]['dependencies'] = ['zlib 0.7']
    elif case == 'same-name-version':
        meta['lock_changes']['before'][0]['version'] = '9.0'
    elif case == 'undeclared-key':
        meta['cargo_changes']['rust/Cargo.toml'] = [['workspace', 'dependencies', 'other']]
    with pytest.raises(tune.ExperimentError, match='Cargo'):
        tune.apply_candidate(baseline, candidate, patch, meta['changed_paths'], declaration=meta)


@pytest.mark.parametrize('field', ['default-features', 'features', 'codegen-units'])
def test_declaring_prohibited_keys_does_not_allow_them(tmp_path: Path, field: str) -> None:
    baseline, candidate, patch, meta = _dependency_patch(tmp_path)
    meta['cargo_changes']['rust/Cargo.toml'] = [['workspace', 'dependencies', 'flate2', field]]
    with pytest.raises(tune.ExperimentError):
        tune.apply_candidate(baseline, candidate, patch, meta['changed_paths'], declaration=meta)


@pytest.mark.parametrize('case', ['driver', 'measurement', 'thresholds', 'source-inventory', 'loaded-version'])
def test_executing_tools_and_business_source_are_separately_frozen(
    bundle: Path, case: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = tune.verify_bundle(bundle)
    if case in ('driver', 'measurement'):
        name = 'autotune_experiment' if case == 'driver' else 'measure_paired_release'
        manifest['tool_files'][f'tools/validation/{name}.py'] = {'sha256': '0' * 64, 'size_bytes': 1}
    elif case == 'thresholds':
        monkeypatch.setitem(tune.THRESHOLDS, 'primary_improvement', 0.01)
    elif case == 'source-inventory':
        manifest['runtime_files']['rust/Cargo.toml']['sha256'] = '0' * 64
    else:
        monkeypatch.setitem(tune.LOADED_TOOL_HASHES, 'tools/validation/autotune_experiment.py', '0' * 64)
    with pytest.raises(tune.ExperimentError) as failure:
        tune.verify_runtime(manifest)
    assert failure.value.code == 'IDENTITY_CHANGED'


@pytest.fixture
def fork_candidate(tmp_path: Path) -> tuple[Path, Path, Path, dict[str, Any]]:
    baseline, candidate = tmp_path / 'before', tmp_path / 'after'
    (baseline / 'rust').mkdir(parents=True)
    old = '[workspace.dependencies.rust_xlsxwriter]\nversion = "0.96.0"\n'
    new = old + 'path = "../vendor/rust_xlsxwriter"\n'
    (baseline / 'rust/Cargo.toml').write_text(old, encoding='utf-8')
    shutil.copytree(baseline, candidate)
    patch = tmp_path / 'fork.patch'
    patch.write_text(
        ''.join(
            difflib.unified_diff(
                old.splitlines(True), new.splitlines(True), fromfile='a/rust/Cargo.toml', tofile='b/rust/Cargo.toml'
            )
        ),
        encoding='utf-8',
    )
    payload = []
    for name, content in {
        'Cargo.toml': b'[package]\nname = "rust_xlsxwriter"\n',
        'LICENSE': b'BSD license\n',
        'src/new.rs': b'fn new() {}\n',
    }.items():
        source = tmp_path / 'payload' / name
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_bytes(content)
        payload.append(
            {
                'source': f'payload/{name}',
                'target': f'{tune.VENDOR_ROOT}/{name}',
                'sha256': _hash(content),
                'size_bytes': len(content),
            }
        )
    (tmp_path / 'payload/undeclared.rs').write_text('never copy', encoding='utf-8')
    meta = {
        'id': 'fork',
        'kind': 'fork',
        'patch': patch.name,
        'patch_sha256': _hash(patch.read_bytes()),
        'changed_paths': ['rust/Cargo.toml'],
        'hypothesis': 'Controlled writer fork',
        'primary_pipeline': 'gb',
        'primary_size': 'medium',
        'cargo_changes': {'rust/Cargo.toml': [['workspace', 'dependencies', 'rust_xlsxwriter', 'path']]},
        'fork_source': {'url': 'https://example.test/writer', 'revision': '1' * 40},
        'payload': payload,
        'licenses': [f'{tune.VENDOR_ROOT}/LICENSE'],
    }
    return baseline, candidate, patch, meta


def test_fork_copies_only_declared_files_and_preserves_license(fork_candidate: Any) -> None:
    baseline, candidate, patch, meta = fork_candidate
    tune.apply_candidate(baseline, candidate, patch, meta['changed_paths'], declaration=meta, payload_root=patch.parent)
    assert (candidate / tune.VENDOR_ROOT / 'LICENSE').read_bytes() == b'BSD license\n'
    assert (candidate / tune.VENDOR_ROOT / 'src/new.rs').is_file()
    assert not (candidate / tune.VENDOR_ROOT / 'undeclared.rs').exists()


@pytest.mark.parametrize(
    'case',
    [
        'traversal',
        'absolute',
        'backslash',
        'symlink',
        'parent-symlink',
        'conflict',
        'drift',
        'size',
        'license',
        'new-file-patch',
    ],
)
def test_fork_payload_boundaries(fork_candidate: Any, case: str) -> None:
    baseline, candidate, patch, meta = fork_candidate
    entry = meta['payload'][0]
    if case == 'traversal':
        entry['target'] = f'{tune.VENDOR_ROOT}/../outside'
    elif case == 'absolute':
        entry['target'] = '/tmp/outside'
    elif case == 'backslash':
        entry['source'] = r'payload\Cargo.toml'
    elif case == 'symlink':
        source = patch.parent / entry['source']
        original = source.read_bytes()
        source.unlink()
        (patch.parent / 'outside').write_bytes(original)
        source.symlink_to(patch.parent / 'outside')
    elif case == 'parent-symlink':
        meta['payload'][2]['source'] = 'indirect/new.rs'
        (patch.parent / 'indirect').symlink_to(patch.parent / 'payload/src', target_is_directory=True)
    elif case == 'conflict':
        meta['payload'].append(dict(entry))
    elif case == 'drift':
        (patch.parent / entry['source']).write_bytes(b'drift')
    elif case == 'size':
        entry['size_bytes'] += 1
    elif case == 'license':
        meta['licenses'] = []
    else:
        patch.write_text(patch.read_text().replace('--- a/rust/Cargo.toml', '--- /dev/null'), encoding='utf-8')
        meta['patch_sha256'] = _hash(patch.read_bytes())
    with pytest.raises((tune.ExperimentError, tune.BundleError)):
        tune.apply_candidate(
            baseline, candidate, patch, meta['changed_paths'], declaration=meta, payload_root=patch.parent
        )


def test_transitive_dependency_may_change_only_cargo_lock(tmp_path: Path) -> None:
    baseline, candidate, patch, meta = _dependency_patch(tmp_path)
    content = patch.read_text()
    patch.write_text(content[content.index('--- a/rust/Cargo.lock') :], encoding='utf-8')
    meta.update(changed_paths=['rust/Cargo.lock'], cargo_changes={}, patch_sha256=_hash(patch.read_bytes()))
    tune.apply_candidate(baseline, candidate, patch, meta['changed_paths'], declaration=meta)
    assert (baseline / 'rust/Cargo.toml').read_bytes() == (candidate / 'rust/Cargo.toml').read_bytes()


def test_legacy_apply_api_cannot_bypass_dependency_declarations(tmp_path: Path) -> None:
    baseline, candidate, patch, meta = _dependency_patch(tmp_path)
    with pytest.raises(tune.ExperimentError) as failure:
        tune.apply_candidate(baseline, candidate, patch, meta['changed_paths'])
    assert failure.value.code == 'PATCH_SCOPE'
    assert (baseline / 'rust/Cargo.lock').read_bytes() == (candidate / 'rust/Cargo.lock').read_bytes()


def test_lock_changes_distinguish_two_installed_versions_of_the_same_package(tmp_path: Path) -> None:
    baseline, candidate, patch, meta = _dependency_patch(tmp_path)
    unchanged = (
        '\n[[package]]\nname = "flate2"\nversion = "2.0"\nsource = "registry+example"\nchecksum = "' + 'c' * 64 + '"\n'
    )
    for tree in (baseline, candidate):
        path = tree / 'rust/Cargo.lock'
        path.write_text(path.read_text() + unchanged, encoding='utf-8')
    tune.apply_candidate(baseline, candidate, patch, meta['changed_paths'], declaration=meta)
    assert 'version = "2.0"' in (candidate / 'rust/Cargo.lock').read_text()


@pytest.mark.parametrize('phase', ['base', 'extended'])
def test_missing_correctness_scenario_cannot_pass(
    bundle: Path, tmp_path: Path, stubbed: Any, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    original = tune.Experiment.correctness

    def missing(
        experiment: tune.Experiment, baseline: Path, candidate: Path, prefix: str, scenarios: Any, **kwargs: Any
    ) -> Any:
        result = original(experiment, baseline, candidate, prefix, scenarios, **kwargs)
        if prefix == 'candidate-2':
            result.pop('sk-small-normal' if phase == 'base' else f'sk-medium-{tune.SEED}-check-only', None)
        return result

    monkeypatch.setattr(tune.Experiment, 'correctness', missing)
    report = tune.run_experiment(bundle_root=bundle, work_dir=tmp_path / 'run')
    assert 'candidate-2' not in report['recommended']
    assert report['candidates']['candidate-2']['status'] == (
        'correctness-failed' if phase == 'base' else 'rejected-extended'
    )


def test_stage_scope_expands_export_and_excludes_only_overlapping_parent() -> None:
    summary = _summary()
    summary['stages'].update(
        {
            stage: {'baseline_median_seconds': 0.3, 'median_improvement': -0.06}
            for stage in ('export', 'writer_populate')
        }
    )
    assert tune.evaluate(summary, primary=True, target_stages=['export'])['passed']
    result = tune.evaluate(summary, primary=True, target_stages=['xlsx_save'])
    assert result['investigate_stages'] == ['writer_populate']
    summary['rss_median_ratio'] = 1.050001
    assert 'MEMORY_MEDIAN_REGRESSION' in tune.evaluate(summary, primary=True, target_stages=['export'])['reasons']


def test_diagnostic_and_short_measurements_cannot_enter_ranking() -> None:
    assert 'PURPOSE_NOT_RANKABLE' in tune.evaluate(_summary(), primary=True, purpose='diagnostic')['reasons']
    summary = _summary()
    summary.update(pairs=5, wall_wins=4)
    assert 'INSUFFICIENT_PAIRS' in tune.evaluate(summary, primary=True)['reasons']
    assert tune.evaluate(summary, primary=True, purpose='acceptance')['passed']


def test_no_manifest_null_fields_require_audit_replay_without_assuming_false() -> None:
    spec = {'expected': {'run_counts': {}, 'error_log_count': 0, 'issue_type_counts': {}, 'low_memory_writer': True}}
    run = {'summary': {'run_counts': {}, 'error_log_count': 0, 'issue_type_counts': {}}, 'low_memory_writer': None}
    assert tune.spec_problems(spec, run, mode='normal', audit_mode='none') == []
    assert tune.spec_problems(spec, run, mode='normal') == ['low_memory_writer']
    run['low_memory_writer'] = False
    assert tune.spec_problems(spec, run, mode='check-only') == []


def _write_spec_output(
    path: Path,
    spec: dict[str, Any],
    *,
    start: str | None = None,
    end: str | None = None,
    empty_analysis: bool = False,
    wrong_month: bool = False,
    wrong_columns: bool = False,
) -> None:
    import xlsxwriter

    path.parent.mkdir(parents=True, exist_ok=True)
    with xlsxwriter.Workbook(path) as workbook:
        for index, (name, sheet_spec) in enumerate(spec['expected']['sheets'].items()):
            sheet = workbook.add_worksheet(name)
            width = sheet_spec['columns'] + bool(wrong_columns and index == 0)
            required = list(
                dict.fromkeys(
                    [
                        '月份',
                        *tune.workbook_compare.GROUP_KEYS.get(name, ()),
                        *tune.workbook_compare.NUMERIC_COLUMNS[(spec['pipeline'], name)],
                    ]
                )
            )
            sheet.write_row(0, 0, [*required, *(f'field-{col}' for col in range(len(required), width))])
            row = 1
            for month, values in spec['expected']['per_month'].items():
                if (start and month < start) or (end and month > end):
                    continue
                count = values[('detail_rows', 'qty_rows', 'analysis_rows')[index]]
                if empty_analysis and index == 2:
                    count = 0
                for _ in range(count):
                    sheet.write(row, 0, '2030-01' if wrong_month else month)
                    for key in tune.workbook_compare.GROUP_KEYS.get(name, ()):
                        if key != '月份':
                            sheet.write(row, required.index(key), f'synthetic-{row}')
                    sheet.write(row, width - 1, 1)
                    row += 1


@pytest.mark.parametrize(
    'case', ['valid', 'cross-month', 'empty-month', 'empty-analysis', 'wrong-month', 'wrong-columns']
)
def test_saved_workbook_spec_uses_actual_cells_and_months(tmp_path: Path, case: str) -> None:
    from tools.validation.synthetic_benchmark import benchmark_spec

    spec = benchmark_spec('gb', 'small')
    start, end = (
        ('2025-12', '2026-01')
        if case == 'cross-month'
        else ('2030-01', '2030-01')
        if case == 'empty-month'
        else (None, None)
    )
    path = tmp_path / 'saved.xlsx'
    _write_spec_output(
        path,
        spec,
        start=start,
        end=end,
        empty_analysis=case == 'empty-analysis',
        wrong_month=case == 'wrong-month',
        wrong_columns=case == 'wrong-columns',
    )
    problems = tune.workbook_spec_problems(spec, path, month_start=start, month_end=end)
    if case in ('valid', 'cross-month', 'empty-month'):
        assert problems == []
    else:
        assert problems
        assert any(
            (
                'analysis_sheet_empty'
                if case == 'empty-analysis'
                else 'per_month'
                if case == 'wrong-month'
                else 'columns'
            )
            in problem
            for problem in problems
        )


def test_large_semantic_scenario_uses_clean_months_and_keeps_dirty_checks() -> None:
    from tools.validation.synthetic_benchmark import benchmark_spec

    scenarios = tune._correctness_scenarios()
    assert tune.SEMANTIC_SCENARIO in scenarios
    _, pipeline, size, seed, mode, start, end = tune.SEMANTIC_SCENARIO
    spec = benchmark_spec(pipeline, size, seed)
    selected = {month: counts for month, counts in spec['expected']['per_month'].items() if start <= month <= end}
    assert mode == 'normal'
    assert sum(counts['detail_rows'] for counts in selected.values()) > 100_000
    assert max(selected) < spec['distribution']['months'][-1]
    for pipeline in ('gb', 'sk'):
        assert (f'{pipeline}-medium-normal', pipeline, 'medium', tune.SEED, 'normal', None, None) in scenarios


def test_semantic_replay_changes_xml_bytes_and_compares_every_sheet(tmp_path: Path) -> None:
    from tools.validation.synthetic_benchmark import benchmark_spec

    spec = benchmark_spec('gb', 'small')
    for side in ('baseline', 'candidate'):
        _write_spec_output(tmp_path / f'{side}-gb.xlsx', spec)
    result = tune.validate_saved_workbooks(
        {'spec': spec, 'directory': str(tmp_path), 'pipeline': 'gb', 'force_semantic': True}
    )
    assert result['semantic_replay'] == {'status': 'passed', 'comparison_mode': 'semantic', 'mismatch_count': 0}
    assert result['fixture_problems'] == []
    assert not (tmp_path / 'semantic-replay.xlsx').exists()


@pytest.fixture
def stubbed(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    state: dict[str, Any] = {'built': [], 'measured': [], 'tests': [], 'control_improvement': 0.0, 'noisy': False}

    def build(experiment: tune.Experiment, name: str, meta: Any) -> Path:
        state['built'].append(name)
        if state.get('budget'):
            raise tune.BudgetExhausted
        assert not experiment.measuring
        binary = experiment.binary(name)
        binary.parent.mkdir(parents=True, exist_ok=True)
        binary.write_text(name, encoding='utf-8')
        return binary

    def run_tests(experiment: tune.Experiment, name: str) -> dict[str, str]:
        state['tests'].append(name)
        return {'status': 'failed' if state.get('failed_tests') == name else 'passed'}

    def correctness(
        experiment: tune.Experiment, baseline: Path, candidate: Path, prefix: str, scenarios: Any, **kwargs: Any
    ) -> Any:
        state.setdefault('validated', {}).setdefault(prefix, []).extend(scenarios)
        for scenario in scenarios:
            experiment.fixture(scenario[1], scenario[2], scenario[3])
        return {
            scenario[0]: {
                'status': 'failed'
                if state.get('bad_workbook') == prefix
                or (state.get('bad_scenario') == scenario[1:5] and prefix == 'candidate-2')
                else 'passed'
            }
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
        state.setdefault('measurement_preparation', []).append(
            (list(state['built']), list(state['tests']), len(experiment.fixtures), kwargs)
        )
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
            else {'candidate-1': 0.06, 'candidate-2': 0.09, 'candidate-3': 0.07, 'candidate-4': 0.01, 'combo': 0.12}[
                name
            ]
        )
        if name == 'combo':
            improvement = (
                state.get('combo_vs_best', 0.03)
                if baseline.parent.name != 'baseline'
                else state.get('combo_vs_base', 0.12)
            )
        summary = _summary(improvement)
        if name == 'combo' and baseline.parent.name != 'baseline':
            summary['wall_wins'] = state.get('combo_wins', 6)
        return {
            'status': 'valid',
            'summary': summary,
            'fixture_problems': [],
            'purpose': kwargs.get('purpose', 'screening'),
            'frozen_identity_sha256': experiment.verify_frozen(),
            'references': {'baseline': tune._sha256(baseline), 'candidate': tune._sha256(candidate)},
        }

    def fixture(experiment: tune.Experiment, pipeline: str, size: str, seed: int) -> Any:
        key = (pipeline, size, seed)
        if key not in experiment.fixtures:
            assert not experiment.measuring
            directory = experiment.work / 'fixtures' / f'{pipeline}-{size}-{seed}'
            directory.mkdir(parents=True)
            for name in ('input', 'config'):
                (directory / name).write_text(name, encoding='utf-8')
            experiment.fixtures[key] = {'input': directory / 'input', 'config': directory / 'config'}
        return experiment.fixtures[key]

    monkeypatch.setattr(tune.Experiment, 'fixture', fixture)
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
    if setting in ('budget', 'failed_tests'):
        assert 'candidate-1' not in stubbed['built']
    else:
        assert 'candidate-1' in stubbed['built']
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


def test_every_build_test_and_fixture_precedes_the_first_measurement(
    bundle: Path, tmp_path: Path, stubbed: Any
) -> None:
    report = tune.run_experiment(bundle_root=bundle, work_dir=tmp_path / 'run')
    assert report['status'] == 'completed'
    built, tested, fixtures, _ = stubbed['measurement_preparation'][0]
    assert built == ['baseline', 'control', 'candidate-1', 'candidate-2', 'candidate-3', 'candidate-4']
    assert tested == ['baseline', 'candidate-1', 'candidate-2', 'candidate-3', 'candidate-4']
    assert fixtures == 10
    assert any(kwargs.get('audit_mode') == 'none' for _, _, _, kwargs in stubbed['measurement_preparation'])


def test_sk_check_only_failure_rejects_a_shortlisted_candidate(bundle: Path, tmp_path: Path, stubbed: Any) -> None:
    stubbed['bad_scenario'] = ('sk', 'medium', tune.SEED, 'check-only')
    report = tune.run_experiment(bundle_root=bundle, work_dir=tmp_path / 'run')
    assert report['candidates']['candidate-2']['status'] == 'rejected-extended'
    assert report['recommended'] == ['candidate-3']


def test_diagnostic_candidate_is_never_shortlisted(bundle: Path, tmp_path: Path, stubbed: Any) -> None:
    path = bundle / 'bundle-manifest.json'
    manifest = json.loads(path.read_text())
    manifest['candidates'][0]['purpose'] = 'diagnostic'
    path.write_text(json.dumps(manifest), encoding='utf-8')
    report = tune.run_experiment(bundle_root=bundle, work_dir=tmp_path / 'run', only=['candidate-1'])
    assert report['candidates']['candidate-1']['status'] == 'diagnostic'
    assert report['shortlist'] == report['recommended'] == []


def _add_combo(bundle: Path) -> None:
    path = bundle / 'bundle-manifest.json'
    manifest = json.loads(path.read_text())
    manifest['combination'] = {**manifest['candidates'][1], 'id': 'combo', 'components': ['candidate-2', 'candidate-3']}
    path.write_text(json.dumps(manifest), encoding='utf-8')


@pytest.mark.parametrize(
    ('setting', 'value', 'passed'),
    [
        (None, None, True),
        ('combo_vs_base', 0.04999, False),
        ('combo_vs_best', 0.01999, False),
        ('combo_wins', 5, False),
    ],
)
def test_combination_requires_both_baseline_and_best_single_gates(
    bundle: Path, tmp_path: Path, stubbed: Any, setting: str | None, value: Any, passed: bool
) -> None:
    _add_combo(bundle)
    if setting:
        stubbed[setting] = value
    report = tune.run_experiment(bundle_root=bundle, work_dir=tmp_path / 'run')
    assert report['status'] == 'completed'
    assert ('combo' in report['recommended']) is passed
    assert len(report['shortlist']) <= 2 and len(report['recommended']) <= 2
    assert report['combination']['best_single'] == 'candidate-2'
    entry = report['candidates']['combo']
    assert entry['against_baseline']['summary']['pairs'] == 8
    if setting != 'combo_vs_base':
        assert entry['against_best']['summary']['pairs'] == 8
        assert entry['against_best']['references']['baseline'] == tune._sha256(
            tmp_path / 'run/builds/candidate-2/costing-calculate'
        )


def test_combination_rejects_overlapping_dependency_changes_before_build(tmp_path: Path) -> None:
    from tools.validation.prepare_colab_bundle import validate_combination

    _, _, _, meta = _dependency_patch(tmp_path)
    other = {**meta, 'id': 'other'}
    combination = {**meta, 'id': 'combo', 'components': ['dependency', 'other']}
    with pytest.raises(tune.BundleError, match='conflict'):
        validate_combination(combination, [meta, other])


def test_frozen_binary_drift_invalidates_all_recommendations(
    bundle: Path, tmp_path: Path, stubbed: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = tune.Experiment.screen

    def drift(experiment: tune.Experiment, baseline: Path, meta: Any) -> Any:
        if meta['id'] == 'candidate-1':
            experiment.binary('candidate-1').write_bytes(b'changed')
        return original(experiment, baseline, meta)

    monkeypatch.setattr(tune.Experiment, 'screen', drift)
    report = tune.run_experiment(bundle_root=bundle, work_dir=tmp_path / 'run')
    assert report['status'] == 'failed'
    assert report['error']['error_code'] == 'IDENTITY_CHANGED'
    assert report['recommended'] == report['shortlist'] == []


def _prepared_driver(bundle: Path, tmp_path: Path) -> tune.Experiment:
    from tools.validation.synthetic_benchmark import benchmark_spec

    work = tmp_path / 'prepared'
    for folder in ('builds', 'trees', 'fixtures', 'comparisons', 'measurements'):
        (work / folder).mkdir(parents=True)
    experiment = tune.Experiment(
        bundle=bundle, work=work, manifest=tune.verify_bundle(bundle), deadline=time.monotonic() + 120
    )
    experiment.report = {'builds': {}, 'candidates': {}, 'calibration': {}}
    spec = benchmark_spec('gb', 'small')
    directory = work / 'fixtures/small'
    directory.mkdir()
    for filename in ('input.xlsx', 'config.toml'):
        (directory / filename).write_text(filename, encoding='utf-8')
    experiment.fixtures[('gb', 'small', tune.SEED)] = {
        'input': directory / 'input.xlsx',
        'config': directory / 'config.toml',
        'spec': spec,
    }
    for name in ('baseline', 'candidate-1'):
        path = experiment.binary(name)
        path.parent.mkdir(parents=True)
        path.write_text(name, encoding='utf-8')
    experiment.freeze()
    return experiment


def _raw_measurement(options: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
    expected = spec['expected']
    runs = {}
    for side in ('baseline', 'candidate'):
        candidate = side == 'candidate' and not options.get('calibration')
        runs[side] = {
            'external_wall_seconds': 0.9 if candidate else 1.0,
            'peak_rss_bytes': 104 if candidate else 100,
            'output_size_bytes': 101 if candidate else 100,
            'stage_timings': {'xlsx_save': 0.27 if candidate else 0.3},
            'summary': {key: expected[key] for key in ('run_counts', 'error_log_count', 'issue_type_counts')},
            'low_memory_writer': None if options['audit_mode'] == 'none' else expected['low_memory_writer'],
        }
    samples = [{**runs} for _ in range(options['pairs'])]
    artifacts = {
        side: {'sha256': tune._sha256(options[key]), 'size_bytes': options[key].stat().st_size}
        for side, key in (
            ('baseline', 'baseline_binary'),
            ('candidate', 'candidate_binary'),
            ('input', 'input_path'),
            ('config', 'config_path'),
        )
        if options.get(key) is not None
    }
    return {
        'status': 'valid',
        'samples': samples,
        'warmups': list(runs.values()),
        'summary': paired_statistics(samples),
        'options': {'audit_mode': options['audit_mode'], 'purpose': options['purpose']},
        'artifacts': artifacts,
        'calibration': {'stage_median_differences': {'xlsx_save': 0.0}} if options.get('calibration') else None,
    }


@pytest.mark.parametrize('case', ['passed', 'bad-writer', 'bad-sheet', 'memory-budget'])
def test_actual_correctness_api_replays_no_manifest_and_checks_saved_output(
    bundle: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    experiment = _prepared_driver(bundle, tmp_path)
    spec = experiment.fixtures[('gb', 'small', tune.SEED)]['spec']
    calls = []

    def compare(**options: Any) -> Any:
        calls.append(options)
        directory = options['output_dir']
        directory.mkdir(parents=True)
        for side in ('baseline', 'candidate'):
            _write_spec_output(
                directory / f'{side}-gb.xlsx', spec, empty_analysis=case == 'bad-sheet' and side == 'candidate'
            )
            if options['audit_mode'] == 'manifest':
                (directory / f'{side}-gb.manifest.json').write_text(
                    json.dumps({'execution': {'low_memory_writer': case == 'bad-writer'}}), encoding='utf-8'
                )
        return {
            'status': 'passed',
            'mismatch_count': 0,
            'mismatches': [],
            'comparison_mode': 'semantic',
            **{
                side: {key: spec['expected'][key] for key in ('run_counts', 'error_log_count', 'issue_type_counts')}
                for side in ('baseline', 'candidate')
            },
        }

    def worker(arguments: Any, **options: Any) -> Any:
        assert options['memory_limit_bytes'] == options['disk_limit_bytes'] == 2 * 1024**3
        if case == 'memory-budget':
            raise tune.MeasurementError('MEMORY_LIMIT', 'Stub budget exceeded')
        return {'payload': tune.validate_saved_workbooks(json.loads(arguments[-1]))}

    monkeypatch.setattr(tune, 'compare_releases', compare)
    monkeypatch.setattr(tune, 'run_process', worker)
    outcome = experiment.correctness(
        experiment.binary('baseline'),
        experiment.binary('candidate-1'),
        'check',
        [('small', 'gb', 'small', tune.SEED, 'normal', None, None)],
        audit_mode='none',
    )['small']
    assert [item['audit_mode'] for item in calls] == (['none'] if case == 'memory-budget' else ['none', 'manifest'])
    assert all(item['purpose'] == 'screening' and item['memory_limit_bytes'] == 2 * 1024**3 for item in calls)
    assert (outcome['status'] == 'passed') is (case == 'passed')
    assert not list((experiment.work / 'comparisons').rglob('*.xlsx'))
    if case == 'passed':
        assert outcome['audit_replay']['status'] == 'passed'
        assert (str(experiment.binary('candidate-1')), 'gb', 'small', tune.SEED, 'normal') in experiment.audited
    if case == 'memory-budget':
        assert outcome['error_code'] == 'MEMORY_LIMIT'


@pytest.mark.parametrize(
    'case',
    [
        'passed',
        'missing-replay',
        'wrong-purpose',
        'wrong-reference',
        'tool-drift',
        'short-report',
        'corrupt-statistics',
    ],
)
def test_actual_measurement_call_uses_frozen_identity_and_protocol(
    bundle: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    experiment = _prepared_driver(bundle, tmp_path)
    spec = experiment.fixtures[('gb', 'small', tune.SEED)]['spec']
    calls = []
    for name in ('baseline', 'candidate-1'):
        if case != 'missing-replay':
            experiment.audited.add((str(experiment.binary(name)), 'gb', 'small', tune.SEED, 'normal'))

    def measure(**options: Any) -> Any:
        calls.append(options)
        result = _raw_measurement(options, spec)
        if case == 'wrong-purpose':
            result['options']['purpose'] = 'diagnostic'
        elif case == 'wrong-reference':
            result['artifacts']['candidate']['sha256'] = '0' * 64
        elif case == 'tool-drift':
            monkeypatch.setitem(tune.LOADED_TOOL_HASHES, 'tools/validation/autotune_experiment.py', '0' * 64)
        elif case == 'short-report':
            result['samples'] = result['samples'][:1]
        elif case == 'corrupt-statistics':
            result['summary']['external_wall_seconds']['paired_median_improvement'] = 0.99
        return result

    monkeypatch.setattr(tune, 'measure_pairs', measure)
    monkeypatch.setattr(tune.os, 'sync', lambda: None)
    monkeypatch.setattr(tune.time, 'sleep', lambda _: None)

    def run() -> Any:
        return experiment.measure(
            'small-none',
            experiment.binary('baseline'),
            experiment.binary('candidate-1'),
            ('gb', 'small', tune.SEED, 'normal'),
            8,
            audit_mode='none',
        )

    if case in ('wrong-purpose', 'wrong-reference', 'tool-drift', 'short-report', 'corrupt-statistics'):
        with pytest.raises(tune.ExperimentError):
            run()
    else:
        report = run()
        if case == 'missing-replay':
            assert report['error']['error_code'] == 'AUDIT_REPLAY_REQUIRED'
            assert calls == []
        else:
            assert calls[0]['audit_mode'] == 'none' and calls[0]['purpose'] == 'screening'
            assert report['fixture_problems'] == []
            assert report['summary']['rss_median_ratio'] == 1.04
            assert experiment.judge(report, primary=True, target_stage='xlsx_save')['passed']


@pytest.mark.parametrize(
    'case', ['passed', 'short-screening', 'diagnostic-screening', 'wrong-binary', 'unknown-writer', 'bundle-changed']
)
def test_independent_acceptance_reuses_measure_pairs_with_five_pairs_and_audit_replay(
    bundle: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    experiment = _prepared_driver(bundle, tmp_path)
    fixture = experiment.fixtures[('gb', 'small', tune.SEED)]
    screening = {
        'status': 'completed',
        'purpose': 'screening',
        'recommended': ['candidate-1'],
        'candidates': {
            'candidate-1': {'primary': {'summary': _summary(), 'purpose': 'screening'}, 'extended': {'passed': True}}
        },
        'builds': {
            name: {'binary_sha256': tune._sha256(experiment.binary(name))} for name in ('baseline', 'candidate-1')
        },
        'bundle_manifest_sha256': tune._sha256(bundle / 'bundle-manifest.json'),
        **{key: experiment.manifest[key] for key in ('tool_files', 'runtime_files', 'protocol_sha256')},
    }
    if case == 'short-screening':
        screening['candidates']['candidate-1']['primary']['summary']['pairs'] = 5
    elif case == 'diagnostic-screening':
        screening['candidates']['candidate-1']['primary']['purpose'] = 'diagnostic'
    elif case == 'wrong-binary':
        experiment.binary('candidate-1').write_bytes(b'changed')
    elif case == 'bundle-changed':
        path = bundle / 'bundle-manifest.json'
        manifest = json.loads(path.read_text())
        manifest['candidates'][0]['hypothesis'] += ' changed'
        path.write_text(json.dumps(manifest), encoding='utf-8')
    calls = []
    comparisons = []

    def compare(**options: Any) -> Any:
        comparisons.append(options)
        options['output_dir'].mkdir()
        if options['audit_mode'] == 'manifest':
            for side in ('baseline', 'candidate'):
                (options['output_dir'] / f'{side}-gb.manifest.json').write_text(
                    json.dumps({'execution': {'low_memory_writer': None if case == 'unknown-writer' else False}}),
                    encoding='utf-8',
                )
        return {'status': 'passed'}

    def measure(**options: Any) -> Any:
        calls.append(options)
        return _raw_measurement(options, fixture['spec'])

    monkeypatch.setattr(tune, 'compare_releases', compare)
    monkeypatch.setattr(tune, 'measure_pairs', measure)

    def run() -> Any:
        return tune.run_acceptance(
            bundle_root=bundle,
            screening_report=screening,
            candidate_id='candidate-1',
            baseline_binary=experiment.binary('baseline'),
            candidate_binary=experiment.binary('candidate-1'),
            input_path=fixture['input'],
            config_path=fixture['config'],
            output_dir=tmp_path / 'acceptance',
            pipeline='gb',
        )

    if case in ('short-screening', 'diagnostic-screening', 'wrong-binary', 'bundle-changed'):
        with pytest.raises(tune.ExperimentError):
            run()
        assert not (tmp_path / 'acceptance').exists()
        assert calls == comparisons == []
    else:
        report = run()
        assert [item['audit_mode'] for item in comparisons] == ['none', 'manifest']
        assert all(item['purpose'] == 'acceptance' for item in comparisons)
        if case == 'unknown-writer':
            assert report['status'] == 'failed' and not calls
        else:
            assert [(item['pairs'], item['purpose']) for item in calls] == [(4, 'calibration'), (5, 'acceptance')]
            assert report['status'] == 'passed'
            assert report['measurement']['options']['purpose'] == 'acceptance'
            assert report['correctness']['manifest']['writer_replay'] == {'baseline': False, 'candidate': False}
            assert json.loads((tmp_path / 'acceptance/acceptance-report.json').read_text())['purpose'] == 'acceptance'

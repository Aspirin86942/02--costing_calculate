from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from tests.rust_oracle import phase0_harness
from tests.rust_oracle.benchmark_protocol import (
    AttemptState,
    ClosedBinaryLabel,
    ComparisonProfile,
    HarnessVerdict,
    NormalRunEvidence,
    RuntimeEvidence,
    build_round_plan,
)
from tests.rust_oracle.oracle_runner import CapturedNormalRun, RustNormalProcessError, RustNormalValidationError
from tests.rust_oracle.phase0_harness import (
    AppendOnlyAttemptLedger,
    BenchmarkIdentity,
    HarnessFailure,
    MetricGroupRequest,
    PairedBenchmarkRequest,
    UnverifiedPriorEvidenceClaim,
    derive_batch_id,
    run_normal_wall_group,
    validate_formal_repository_state,
)


@pytest.fixture(autouse=True)
def _trusted_repo_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(phase0_harness, 'repo_root', lambda: tmp_path)


def _runtime(pipeline: str = 'gb') -> RuntimeEvidence:
    return RuntimeEvidence(
        pipeline=pipeline,  # type: ignore[arg-type]
        output_written=True,
        request_id_present=True,
        sheet_count=3,
        error_log_count=0,
        issue_type_counts=(),
        quality_metrics=(),
        run_counts=(('reader_rows', 1),),
        stage_timings=tuple(
            (name, Decimal('0.1'))
            for name in ('ingest', 'normalize', 'split', 'fact', 'presentation', 'total', 'export')
        ),
        output_size_bytes=8,
        sheet_dimensions=('1x1', '1x1', '1x1'),
        reader_snapshot_sha256='',
    )


def _request(tmp_path: Path) -> PairedBenchmarkRequest:
    input_path = tmp_path / 'input.xlsx'
    reference = tmp_path / 'reference.exe'
    candidate = tmp_path / 'candidate.exe'
    manifest = tmp_path / 'phase0a.json'
    for path, content in ((input_path, b'input'), (reference, b'ref'), (candidate, b'candidate'), (manifest, b'{}')):
        path.write_bytes(content)
    return PairedBenchmarkRequest(
        pipeline='gb',
        input_path=input_path,
        reference_executable=reference,
        candidate_executable=candidate,
        reference_label=ClosedBinaryLabel.PHASE0A,
        candidate_label=ClosedBinaryLabel.PHASE0B,
        comparison_profile=ComparisonProfile.PHASE0B_VS_PHASE0A,
        phase0a_manifest=manifest,
        local_root=tmp_path / 'rust' / 'target' / 'perf-local',
        evidence_path=tmp_path / 'docs' / 'performance' / 'batch.json',
        attempt_ledger_root=tmp_path / 'rust' / 'target' / 'perf-local' / 'batches',
    )


def _identity() -> BenchmarkIdentity:
    return BenchmarkIdentity('3' * 8, '1' * 8, '2' * 8, 'head', '4' * 8, '5' * 8)


def _ledger(tmp_path: Path) -> AppendOnlyAttemptLedger:
    local_root = tmp_path / 'rust' / 'target' / 'perf-local'
    return AppendOnlyAttemptLedger.create(
        local_root / 'batches',
        _identity(),
        comparison_key='a' * 64,
    )


def _claim(tmp_path: Path, path: Path, sha256: str) -> UnverifiedPriorEvidenceClaim:
    return UnverifiedPriorEvidenceClaim(
        path_alias=path.relative_to(tmp_path).as_posix(),
        content_sha256=sha256,
    )


def _install_runner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    fail_role: str | None = None,
    validation_fail_role: str | None = None,
    exception_role: str | None = None,
    interrupt_role: str | None = None,
    oracle_sha: str = 'oracle',
) -> list[tuple[str, ...]]:
    commands: list[tuple[str, ...]] = []

    def fake_capture(
        executable: Path, pipeline: str, input_path: Path, output_path: Path, **kwargs: object
    ) -> CapturedNormalRun:
        role = 'reference' if executable.name.startswith('reference') else 'candidate'
        commands.append((role, pipeline, str(input_path), str(output_path), *kwargs.keys()))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b'workbook')
        if role == interrupt_role:
            raise KeyboardInterrupt('simulated process interruption')
        if role == validation_fail_role:
            raise RustNormalValidationError('invalid workbook', 'e' * 64)
        if role == exception_role:
            raise RuntimeError('unexpected adapter failure')
        if role == fail_role:
            raise RustNormalProcessError(9, 'f' * 64)
        return CapturedNormalRun(
            normal_run=NormalRunEvidence(Decimal('1.0'), None, _runtime(pipeline), oracle_sha),
            exit_code=0,
            local_unversioned_log_sha256='l' * 64,
        )

    monkeypatch.setattr(phase0_harness, 'run_rust_normal_captured', fake_capture)
    monkeypatch.setattr(phase0_harness, '_capture_identity', lambda request: _identity())
    monkeypatch.setattr(phase0_harness, 'repo_root', lambda: tmp_path)
    return commands


def _group_request(tmp_path: Path) -> MetricGroupRequest:
    ledger = _ledger(tmp_path)
    return MetricGroupRequest(
        _request(tmp_path),
        'b' * 8,
        'wall',
        build_round_plan(global_round_start=1, round_count=5),
        ledger.attempt_directory,
    )


def test_normal_wall_group_omits_check_only_and_uses_unique_outputs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    commands = _install_runner(monkeypatch, tmp_path)
    run_normal_wall_group(_group_request(tmp_path))
    paths = [command[3] for command in commands]
    assert len(paths) == len(set(paths)) == 10
    assert all('--check-only' not in command for command in commands)


def test_normal_wall_group_runs_global_ab_ba_order(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    commands = _install_runner(monkeypatch, tmp_path)
    run_normal_wall_group(_group_request(tmp_path))
    assert [item[0] for item in commands] == [
        'reference',
        'candidate',
        'candidate',
        'reference',
        'reference',
        'candidate',
        'candidate',
        'reference',
        'reference',
        'candidate',
    ]


def test_candidate_nonzero_rejects_candidate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_runner(monkeypatch, tmp_path, fail_role='candidate')
    with pytest.raises(HarnessFailure) as caught:
        run_normal_wall_group(_group_request(tmp_path))
    assert caught.value.verdict is HarnessVerdict.CANDIDATE_FAILED


def test_reference_nonzero_invalidates_whole_batch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_runner(monkeypatch, tmp_path, fail_role='reference')
    with pytest.raises(HarnessFailure) as caught:
        run_normal_wall_group(_group_request(tmp_path))
    assert caught.value.verdict is HarnessVerdict.REFERENCE_FAILED


@pytest.mark.parametrize('field', ('input_sha256', 'reference_sha256', 'candidate_sha256', 'git_head'))
def test_wall_group_rejects_input_reference_candidate_or_git_drift(
    field: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_runner(monkeypatch, tmp_path)
    values = [_identity(), replace(_identity(), **{field: 'drift'})]
    monkeypatch.setattr(
        phase0_harness,
        '_capture_identity',
        lambda request: values.pop(0) if values else replace(_identity(), **{field: 'drift'}),
    )
    with pytest.raises(HarnessFailure, match='drift'):
        run_normal_wall_group(_group_request(tmp_path))


def test_wall_group_deletes_workbooks_on_success(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_runner(monkeypatch, tmp_path)
    run_normal_wall_group(_group_request(tmp_path))
    assert not list((tmp_path / 'data').rglob('*.xlsx'))


def test_wall_group_deletes_workbooks_after_process_or_oracle_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_runner(monkeypatch, tmp_path, fail_role='candidate')
    with pytest.raises(HarnessFailure):
        run_normal_wall_group(_group_request(tmp_path))
    assert not list((tmp_path / 'data').rglob('*.xlsx'))


def test_cleanup_failure_prevents_versionable_evidence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_runner(monkeypatch, tmp_path)
    monkeypatch.setattr(
        Path,
        'unlink',
        lambda self, **kwargs: (_ for _ in ()).throw(PermissionError('locked')) if self.suffix == '.xlsx' else None,
    )
    with pytest.raises(HarnessFailure) as caught:
        run_normal_wall_group(_group_request(tmp_path))
    assert caught.value.verdict is HarnessVerdict.CLEANUP_FAILED
    assert not _request(tmp_path).evidence_path.exists()


def test_first_formal_batch_requires_clean_worktree(tmp_path: Path) -> None:
    with pytest.raises(HarnessFailure, match='clean'):
        validate_formal_repository_state((' M tests/file.py',), evidence_root=tmp_path, prior_evidence_claims=())


def test_formal_batch_rejects_non_evidence_worktree_change(tmp_path: Path) -> None:
    approved = tmp_path / 'docs' / 'prior.json'
    approved.parent.mkdir()
    approved.write_bytes(b'{}')
    claim = _claim(tmp_path, approved, phase0_harness._sha256(approved))
    with pytest.raises(HarnessFailure, match='non-evidence'):
        validate_formal_repository_state(
            ('?? src/new.py',),
            evidence_root=tmp_path / 'docs',
            prior_evidence_claims=(claim,),
            root=tmp_path,
        )


def test_later_batch_accepts_only_create_new_sanitized_prior_evidence(tmp_path: Path) -> None:
    evidence = tmp_path / 'docs' / 'prior.json'
    evidence.parent.mkdir()
    evidence.write_bytes(b'{"sanitized":true}')
    digest = phase0_harness._sha256(evidence)
    validate_formal_repository_state(
        ('?? docs/prior.json',),
        evidence_root=tmp_path / 'docs',
        prior_evidence_claims=(_claim(tmp_path, evidence, digest),),
        root=tmp_path,
    )


def test_prior_evidence_content_change_invalidates_repository_state(tmp_path: Path) -> None:
    evidence = tmp_path / 'docs' / 'prior.json'
    evidence.parent.mkdir()
    evidence.write_bytes(b'changed')
    with pytest.raises(HarnessFailure, match='content'):
        validate_formal_repository_state(
            ('?? docs/prior.json',),
            evidence_root=tmp_path / 'docs',
            prior_evidence_claims=(_claim(tmp_path, evidence, '0' * 64),),
            root=tmp_path,
        )


def test_batch_id_is_derived_and_cannot_be_supplied(tmp_path: Path) -> None:
    request = _request(tmp_path)
    assert derive_batch_id(request, _identity()) == derive_batch_id(request, _identity())
    assert 'batch_id' not in PairedBenchmarkRequest.__dataclass_fields__


def test_second_round_one_to_five_attempt_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        MetricGroupRequest(
            _request(tmp_path),
            'b' * 32,
            'wall',
            build_round_plan(global_round_start=1, round_count=5),
            tmp_path / 'attempt',
            first_group_sha256='a' * 64,
        )


def test_existing_round_record_cannot_be_overwritten(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.record_sample('wall', 1, 'reference', {'value': 1})
    with pytest.raises(HarnessFailure, match='overwrite'):
        ledger.record_sample('wall', 1, 'reference', {'value': 2})


def test_expanded_group_requires_original_first_group_sha(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    first = ledger.commit_first_group({'wall': 'one', 'pws': 'two'})
    with pytest.raises(HarnessFailure, match='first group'):
        ledger.commit_expanded_group({'wall': 'three'}, first_group_sha256='0' * 64)
    ledger.commit_expanded_group({'wall': 'three', 'pws': 'four'}, first_group_sha256=first)


def test_pws_only_resample_is_rejected(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    with pytest.raises(HarnessFailure, match='wall and pws'):
        ledger.commit_first_group({'pws': 'only'})


def test_interrupted_attempt_resumes_only_missing_samples(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    first_commands = _install_runner(monkeypatch, tmp_path, interrupt_role='candidate')
    with pytest.raises(KeyboardInterrupt):
        run_normal_wall_group(_group_request(tmp_path))
    assert [item[0] for item in first_commands] == ['reference', 'candidate']

    resumed_commands = _install_runner(monkeypatch, tmp_path)
    run_normal_wall_group(_group_request(tmp_path))
    assert resumed_commands[0][0] == 'candidate'
    assert len(resumed_commands) == 9


def test_failed_candidate_sha_cannot_be_retried(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.finish(HarnessVerdict.CANDIDATE_FAILED)
    with pytest.raises(HarnessFailure, match='candidate SHA'):
        AppendOnlyAttemptLedger.create(
            tmp_path / 'rust' / 'target' / 'perf-local' / 'batches',
            _identity(),
            comparison_key='a' * 64,
        )


def test_environment_recovery_attempt_links_previous_ledger_head(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.finish(HarnessVerdict.ENVIRONMENT_DRIFT)
    recovered = AppendOnlyAttemptLedger.create(
        tmp_path / 'rust' / 'target' / 'perf-local' / 'batches',
        _identity(),
        comparison_key='a' * 64,
    )
    assert recovered.previous_attempt_head_sha256 == ledger.head_sha256


def test_terminal_verdict_is_unique_and_cannot_change_retry_eligibility(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.finish(HarnessVerdict.CANDIDATE_FAILED, raw_log_sha256='d' * 64)
    assert ledger.state is AttemptState.FAILED
    assert ledger.terminal_verdict is HarnessVerdict.CANDIDATE_FAILED
    with pytest.raises(HarnessFailure, match='terminal'):
        ledger.finish(HarnessVerdict.ENVIRONMENT_DRIFT)
    with pytest.raises(HarnessFailure, match='terminal'):
        ledger.record_sample('wall', 2, 'reference', {'value': 2})


def test_candidate_failure_is_terminal_before_wall_runner_raises(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request = _group_request(tmp_path)
    _install_runner(monkeypatch, tmp_path, fail_role='candidate')
    with pytest.raises(HarnessFailure) as caught:
        run_normal_wall_group(request)
    assert caught.value.verdict is HarnessVerdict.CANDIDATE_FAILED
    loaded = AppendOnlyAttemptLedger.load(request.attempt_directory, _identity())
    assert loaded.terminal_verdict is HarnessVerdict.CANDIDATE_FAILED
    assert loaded.terminal_raw_log_sha256 == 'f' * 64


def test_correctness_failure_is_terminal_with_raw_log_sha(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request = _group_request(tmp_path)
    _install_runner(monkeypatch, tmp_path, validation_fail_role='candidate')
    with pytest.raises(HarnessFailure) as caught:
        run_normal_wall_group(request)
    assert caught.value.verdict is HarnessVerdict.CORRECTNESS_FAILED
    loaded = AppendOnlyAttemptLedger.load(request.attempt_directory, _identity())
    assert loaded.terminal_verdict is HarnessVerdict.CORRECTNESS_FAILED
    assert loaded.terminal_raw_log_sha256 == 'e' * 64


def test_capture_boundary_exception_maps_to_closed_terminal_verdict(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request = _group_request(tmp_path)
    _install_runner(monkeypatch, tmp_path, exception_role='candidate')
    with pytest.raises(HarnessFailure) as caught:
        run_normal_wall_group(request)
    assert caught.value.verdict is HarnessVerdict.CORRECTNESS_FAILED
    assert isinstance(caught.value.__cause__, RuntimeError)
    assert (
        AppendOnlyAttemptLedger.load(request.attempt_directory, _identity()).terminal_verdict
        is HarnessVerdict.CORRECTNESS_FAILED
    )


def test_sealed_attempt_rejects_deleted_tail_sample(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.record_sample('wall', 1, 'candidate', {'value': 1})
    ledger.finish(HarnessVerdict.CANDIDATE_FAILED)
    sample_record = next((ledger.attempt_directory / 'records').glob('*-sample.json'))
    sample_record.unlink()
    with pytest.raises(HarnessFailure, match='checkpoint|sealed|record count'):
        AppendOnlyAttemptLedger.load(ledger.attempt_directory, _identity())


def test_comparison_journal_rejects_deleted_terminal(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.finish(HarnessVerdict.CANDIDATE_FAILED)
    (ledger.attempt_directory / 'terminal.json').unlink()
    with pytest.raises(HarnessFailure, match='journal'):
        AppendOnlyAttemptLedger.load(ledger.attempt_directory, _identity())


def test_comparison_journal_rejects_tail_record_and_checkpoint_rollback(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.record_sample('wall', 1, 'candidate', {'value': 1})
    next((ledger.attempt_directory / 'records').glob('*-sample.json')).unlink()
    next((ledger.attempt_directory / 'checkpoints').glob('*.json')).unlink()
    with pytest.raises(HarnessFailure, match='journal'):
        AppendOnlyAttemptLedger.load(ledger.attempt_directory, _identity())


def test_unsealed_attempt_does_not_turn_deleted_candidate_sample_into_missing(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.record_sample('wall', 1, 'candidate', {'value': 1})
    sample_record = next((ledger.attempt_directory / 'records').glob('*-sample.json'))
    sample_record.unlink()
    with pytest.raises(HarnessFailure, match='checkpoint'):
        AppendOnlyAttemptLedger.load(ledger.attempt_directory, _identity())


def test_transient_cleanup_failure_is_closed_and_outer_retry_removes_workbook(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_runner(monkeypatch, tmp_path)
    real_remove = phase0_harness._remove_workbook
    failures_left = 1

    def transient(path: Path) -> None:
        nonlocal failures_left
        if failures_left:
            failures_left -= 1
            raise PermissionError('transient lock')
        real_remove(path)

    monkeypatch.setattr(phase0_harness, '_remove_workbook', transient)
    with pytest.raises(HarnessFailure) as caught:
        run_normal_wall_group(_group_request(tmp_path))
    assert caught.value.verdict is HarnessVerdict.ENVIRONMENT_DRIFT
    assert not list((tmp_path / 'data').rglob('*.xlsx'))


def test_persistent_cleanup_failure_preserves_primary_verdict(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_runner(monkeypatch, tmp_path, fail_role='candidate')
    real_remove = phase0_harness._remove_workbook
    calls = 0

    def fail_after_reference(path: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            real_remove(path)
            return
        raise PermissionError('persistent lock')

    monkeypatch.setattr(phase0_harness, '_remove_workbook', fail_after_reference)
    with pytest.raises(HarnessFailure) as caught:
        run_normal_wall_group(_group_request(tmp_path))
    assert caught.value.verdict is HarnessVerdict.CLEANUP_FAILED
    assert caught.value.primary_verdict is HarnessVerdict.CANDIDATE_FAILED


def test_cleanup_recovery_attempt_cleans_historical_planned_outputs_before_resume(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    ledger.record_planned_output(
        'wall',
        1,
        'reference',
        {
            'pipeline': 'gb',
            'batch_id': 'b' * 32,
            'metric': 'wall',
            'binary_sha256': '1' * 64,
            'global_round': 1,
            'role': 'reference',
            'relative_path': 'gb/.perf-runs/' + '/'.join(('b' * 32, 'wall', '1' * 64, '1', 'reference.xlsx')),
        },
    )
    monkeypatch.setattr(phase0_harness, 'repo_root', lambda: tmp_path)
    historical = phase0_harness._planned_paths(ledger.all_planned_output_payloads())[0]
    historical.write_bytes(b'partial')
    ledger.finish(HarnessVerdict.CLEANUP_FAILED, primary_verdict=HarnessVerdict.CANDIDATE_FAILED)
    recovered = AppendOnlyAttemptLedger.create(
        tmp_path / 'rust' / 'target' / 'perf-local' / 'batches',
        _identity(),
        comparison_key='a' * 64,
    )
    assert recovered.all_planned_output_payloads()
    assert recovered.cleanup_only is True
    commands = _install_runner(monkeypatch, tmp_path)
    request = MetricGroupRequest(
        _request(tmp_path),
        'b' * 8,
        'wall',
        build_round_plan(global_round_start=1, round_count=5),
        recovered.attempt_directory,
    )
    with pytest.raises(HarnessFailure) as caught:
        run_normal_wall_group(request)
    assert caught.value.verdict is HarnessVerdict.CANDIDATE_FAILED
    assert commands == []
    assert not historical.exists()
    with pytest.raises(HarnessFailure, match='candidate SHA'):
        AppendOnlyAttemptLedger.create(
            tmp_path / 'rust' / 'target' / 'perf-local' / 'batches',
            _identity(),
            comparison_key='a' * 64,
        )


@pytest.mark.parametrize('field', ('local_root', 'attempt_ledger_root'))
def test_wall_runner_requires_repository_trusted_local_roots(
    field: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    benchmark = replace(_request(tmp_path), **{field: tmp_path / 'docs' / 'performance'})
    request = MetricGroupRequest(
        benchmark,
        'b' * 8,
        'wall',
        build_round_plan(global_round_start=1, round_count=5),
        ledger.attempt_directory,
    )
    commands = _install_runner(monkeypatch, tmp_path)
    with pytest.raises(HarnessFailure, match='trusted'):
        run_normal_wall_group(request)
    assert commands == []


@pytest.mark.parametrize('comparison_key', ('../escape', 'a/b', 'C:/absolute', 'A' * 64, 'a' * 63))
def test_attempt_ledger_rejects_non_closed_comparison_key(comparison_key: str, tmp_path: Path) -> None:
    local_root = tmp_path / 'rust' / 'target' / 'perf-local'
    with pytest.raises(HarnessFailure, match='comparison_key'):
        AppendOnlyAttemptLedger.create(
            local_root / 'batches',
            _identity(),
            comparison_key=comparison_key,
        )


def test_attempt_ledger_rejects_root_outside_local_root(tmp_path: Path) -> None:
    with pytest.raises(HarnessFailure, match='local root'):
        AppendOnlyAttemptLedger.create(
            tmp_path / 'outside',
            _identity(),
            comparison_key='a' * 64,
        )


def test_attempt_ledger_rejects_raw_symlink_component(tmp_path: Path) -> None:
    local_root = tmp_path / 'rust' / 'target' / 'perf-local'
    local_root.mkdir(parents=True)
    outside = tmp_path / 'outside'
    outside.mkdir()
    linked = local_root / 'batches'
    try:
        linked.symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f'symlink creation is unavailable: {exc}')
    with pytest.raises(HarnessFailure, match='reparse|symlink'):
        AppendOnlyAttemptLedger.create(
            linked,
            _identity(),
            comparison_key='a' * 64,
        )


def test_prior_evidence_claim_content_participates_in_identity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request = _request(tmp_path)
    evidence = tmp_path / 'docs' / 'performance' / 'prior.json'
    evidence.parent.mkdir(parents=True)
    evidence.write_bytes(b'first')
    claim = _claim(tmp_path, evidence, phase0_harness._sha256(evidence))
    request = replace(request, prior_evidence_claims=(claim,))
    monkeypatch.setattr(phase0_harness, 'repo_root', lambda: tmp_path)
    monkeypatch.setattr(phase0_harness, '_run_git', lambda root, *args: 'head' if args[0] == 'rev-parse' else '')

    first = phase0_harness._capture_identity(request)
    evidence.write_bytes(b'second')
    with pytest.raises(HarnessFailure, match='prior evidence'):
        phase0_harness._capture_identity(request)
    assert first.repository_state_sha256

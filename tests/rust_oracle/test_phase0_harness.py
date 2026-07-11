from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from tests.rust_oracle import phase0_harness
from tests.rust_oracle.benchmark_protocol import (
    ClosedBinaryLabel,
    ComparisonProfile,
    HarnessVerdict,
    NormalRunEvidence,
    RuntimeEvidence,
    build_round_plan,
)
from tests.rust_oracle.oracle_runner import CapturedNormalRun, RustNormalProcessError
from tests.rust_oracle.phase0_harness import (
    AppendOnlyAttemptLedger,
    BenchmarkIdentity,
    HarnessFailure,
    MetricGroupRequest,
    PairedBenchmarkRequest,
    derive_batch_id,
    run_normal_wall_group,
    validate_formal_repository_state,
)


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
    return BenchmarkIdentity('i' * 8, 'r' * 8, 'c' * 8, 'head', 's' * 8, 'm' * 8)


def _install_runner(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, fail_role: str | None = None, oracle_sha: str = 'oracle'
) -> list[tuple[str, ...]]:
    commands: list[tuple[str, ...]] = []

    def fake_capture(
        executable: Path, pipeline: str, input_path: Path, output_path: Path, **kwargs: object
    ) -> CapturedNormalRun:
        role = 'reference' if executable.name.startswith('reference') else 'candidate'
        commands.append((role, pipeline, str(input_path), str(output_path), *kwargs.keys()))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b'workbook')
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
    ledger = AppendOnlyAttemptLedger.create(tmp_path / 'attempt-ledger', _identity(), comparison_key='wall-test')
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
        validate_formal_repository_state((' M tests/file.py',), evidence_root=tmp_path, approved_prior_evidence={})


def test_formal_batch_rejects_non_evidence_worktree_change(tmp_path: Path) -> None:
    approved = tmp_path / 'docs' / 'prior.json'
    approved.parent.mkdir()
    approved.write_bytes(b'{}')
    with pytest.raises(HarnessFailure, match='non-evidence'):
        validate_formal_repository_state(
            ('?? src/new.py',),
            evidence_root=tmp_path / 'docs',
            approved_prior_evidence={approved.resolve(): phase0_harness._sha256(approved)},
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
        approved_prior_evidence={evidence.resolve(): digest},
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
            approved_prior_evidence={evidence.resolve(): '0' * 64},
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
    ledger = AppendOnlyAttemptLedger.create(tmp_path, _identity(), comparison_key='key')
    ledger.record_sample('wall', 1, 'reference', {'value': 1})
    with pytest.raises(HarnessFailure, match='overwrite'):
        ledger.record_sample('wall', 1, 'reference', {'value': 2})


def test_expanded_group_requires_original_first_group_sha(tmp_path: Path) -> None:
    ledger = AppendOnlyAttemptLedger.create(tmp_path, _identity(), comparison_key='key')
    first = ledger.commit_first_group({'wall': 'one', 'pws': 'two'})
    with pytest.raises(HarnessFailure, match='first group'):
        ledger.commit_expanded_group({'wall': 'three'}, first_group_sha256='0' * 64)
    ledger.commit_expanded_group({'wall': 'three', 'pws': 'four'}, first_group_sha256=first)


def test_pws_only_resample_is_rejected(tmp_path: Path) -> None:
    ledger = AppendOnlyAttemptLedger.create(tmp_path, _identity(), comparison_key='key')
    with pytest.raises(HarnessFailure, match='wall and pws'):
        ledger.commit_first_group({'pws': 'only'})


def test_interrupted_attempt_resumes_only_missing_samples(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    first_commands = _install_runner(monkeypatch, tmp_path, fail_role='candidate')
    with pytest.raises(HarnessFailure):
        run_normal_wall_group(_group_request(tmp_path))
    assert [item[0] for item in first_commands] == ['reference', 'candidate']

    resumed_commands = _install_runner(monkeypatch, tmp_path)
    run_normal_wall_group(_group_request(tmp_path))
    assert resumed_commands[0][0] == 'candidate'
    assert len(resumed_commands) == 9


def test_failed_candidate_sha_cannot_be_retried(tmp_path: Path) -> None:
    ledger = AppendOnlyAttemptLedger.create(tmp_path, _identity(), comparison_key='key')
    ledger.finish(HarnessVerdict.CANDIDATE_FAILED)
    with pytest.raises(HarnessFailure, match='candidate SHA'):
        AppendOnlyAttemptLedger.create(tmp_path, _identity(), comparison_key='key')


def test_environment_recovery_attempt_links_previous_ledger_head(tmp_path: Path) -> None:
    ledger = AppendOnlyAttemptLedger.create(tmp_path, _identity(), comparison_key='key')
    ledger.finish(HarnessVerdict.ENVIRONMENT_DRIFT)
    recovered = AppendOnlyAttemptLedger.create(tmp_path, _identity(), comparison_key='key')
    assert recovered.previous_attempt_head_sha256 == ledger.head_sha256

from dataclasses import replace
from decimal import Decimal

import pytest

from tests.rust_oracle.benchmark_protocol import (
    COMPARISON_LIMITS,
    PROFILE_RULES,
    CalibrationGroup,
    CalibrationRound,
    ComparisonProfile,
    MachineEvidence,
    MetricGroup,
    MetricSample,
    NormalRunEvidence,
    PairedRound,
    Phase0AManifest,
    RuntimeEvidence,
    aggregate_output_bytes,
    approved_phase0a_output_bytes,
    assert_environment_not_drifted,
    assert_output_bytes_within_phase0a_limit,
    assert_same_batch_ratio,
    assert_same_benchmark_batch,
    build_round_plan,
    groups_have_conflicting_direction,
    merge_metric_groups,
    requires_mandatory_expansion,
    validate_calibration_group,
    validate_metric_group,
)


def _machine(fingerprint: str = 'machine') -> MachineEvidence:
    return MachineEvidence(
        windows_build='10.0.26100',
        architecture='x86_64',
        cpu_model='synthetic',
        logical_cpu_count=8,
        physical_memory_bytes=32_000_000_000,
        system_drive_media_type='SSD',
        system_drive_size_bytes=1_000_000_000_000,
        fingerprint_sha256=fingerprint,
    )


def _runtime(*, pipeline: str = 'sk', output_size_bytes: int | None = 1000) -> RuntimeEvidence:
    return RuntimeEvidence(
        pipeline=pipeline,
        output_written=True,
        request_id_present=True,
        sheet_count=3,
        error_log_count=0,
        issue_type_counts=(),
        quality_metrics=(),
        run_counts=(),
        stage_timings=(
            ('ingest', Decimal('1')),
            ('normalize', Decimal('1')),
            ('split', Decimal('1')),
            ('fact', Decimal('1')),
            ('presentation', Decimal('1')),
            ('total', Decimal('7')),
            ('export', Decimal('2')),
            ('writer_populate', Decimal('1')),
            ('xlsx_save', Decimal('1')),
        ),
        output_size_bytes=output_size_bytes,
        sheet_dimensions=('A1:C3',),
        reader_snapshot_sha256='reader',
    )


def _sample(
    role: str,
    round_number: int,
    value: str = '1',
    *,
    pipeline: str = 'sk',
    input_sha: str = 'input',
    binary_sha: str | None = None,
    git_head: str = 'head',
    repository_state_sha: str = 'state',
    machine_sha: str = 'machine',
    output_size_bytes: int | None = 1000,
) -> MetricSample:
    runtime = _runtime(pipeline=pipeline, output_size_bytes=output_size_bytes)
    return MetricSample(
        role=role,
        global_round=round_number,
        metric_value=Decimal(value),
        exit_code=0,
        input_sha256=input_sha,
        binary_sha256=binary_sha or role,
        git_head=git_head,
        repository_state_sha256=repository_state_sha,
        machine_fingerprint_sha256=machine_sha,
        local_unversioned_log_sha256=f'log-{role}-{round_number}',
        normal_run=NormalRunEvidence(
            external_wall_seconds=Decimal(value),
            peak_working_set_bytes=100,
            runtime=runtime,
            workbook_oracle_sha256='oracle',
        ),
    )


def _group(
    *,
    metric: str = 'wall',
    start: int = 1,
    batch_id: str = 'batch',
    pipeline: str = 'sk',
    reference_value: str = '1',
    candidate_value: str = '0.9',
    input_sha: str = 'input',
    reference_sha: str = 'reference',
    candidate_sha: str = 'candidate',
    git_head: str = 'head',
    repository_state_sha: str = 'state',
    machine_sha: str = 'machine',
) -> MetricGroup:
    rounds = []
    for plan in build_round_plan(global_round_start=start, round_count=5):
        rounds.append(
            PairedRound(
                plan=plan,
                reference=_sample(
                    'reference',
                    plan.global_round,
                    reference_value,
                    pipeline=pipeline,
                    input_sha=input_sha,
                    binary_sha=reference_sha,
                    git_head=git_head,
                    repository_state_sha=repository_state_sha,
                    machine_sha=machine_sha,
                ),
                candidate=_sample(
                    'candidate',
                    plan.global_round,
                    candidate_value,
                    pipeline=pipeline,
                    input_sha=input_sha,
                    binary_sha=candidate_sha,
                    git_head=git_head,
                    repository_state_sha=repository_state_sha,
                    machine_sha=machine_sha,
                ),
            )
        )
    return MetricGroup(
        batch_id=batch_id,
        pipeline=pipeline,
        metric=metric,
        global_round_start=start,
        rounds=tuple(rounds),
    )


def _replace_round_window(group: MetricGroup, *, start: int, count: int) -> MetricGroup:
    template = group.rounds[0]
    rounds = []
    for global_round in range(start, start + count):
        order = ('reference', 'candidate') if global_round % 2 else ('candidate', 'reference')
        rounds.append(
            replace(
                template,
                plan=replace(template.plan, global_round=global_round, order=order),
                reference=replace(template.reference, global_round=global_round),
                candidate=replace(template.candidate, global_round=global_round),
            )
        )
    return replace(group, global_round_start=start, rounds=tuple(rounds))


def _replace_metric_values(
    group: MetricGroup, *, reference_values: tuple[str, ...], candidate_values: tuple[str, ...]
) -> MetricGroup:
    assert len(group.rounds) == len(reference_values) == len(candidate_values)
    return replace(
        group,
        rounds=tuple(
            replace(
                paired,
                reference=replace(paired.reference, metric_value=Decimal(reference_value)),
                candidate=replace(paired.candidate, metric_value=Decimal(candidate_value)),
            )
            for paired, reference_value, candidate_value in zip(
                group.rounds, reference_values, candidate_values, strict=True
            )
        ),
    )


def _calibration_group(
    *,
    metric: str,
    pipeline: str,
    value: str = '1',
    machine_sha: str = 'machine',
    output_sizes: tuple[int | None, ...] = (1000, 1000, 1000, 1000, 1000),
) -> CalibrationGroup:
    return CalibrationGroup(
        batch_id='phase0a',
        pipeline=pipeline,
        metric=metric,
        warmup_succeeded=True,
        rounds=tuple(
            CalibrationRound(
                global_round=round_number,
                reference=_sample(
                    'reference',
                    round_number,
                    value,
                    pipeline=pipeline,
                    binary_sha='reference',
                    machine_sha=machine_sha,
                    output_size_bytes=output_size,
                ),
            )
            for round_number, output_size in enumerate(output_sizes, start=1)
        ),
    )


def _manifest(
    *,
    machine_sha: str = 'machine',
    gb_value: str = '1',
    sk_value: str = '1',
    gb_output_sizes: tuple[int | None, ...] = (1000, 1000, 1000, 1000, 1000),
    sk_output_sizes: tuple[int | None, ...] = (2000, 2000, 2000, 2000, 2000),
) -> Phase0AManifest:
    return Phase0AManifest(
        reference_exe_sha256='reference',
        fork_revision='fork',
        git_head='head',
        machine=_machine(machine_sha),
        gb_wall=_calibration_group(
            metric='wall',
            pipeline='gb',
            value=gb_value,
            machine_sha=machine_sha,
            output_sizes=gb_output_sizes,
        ),
        gb_pws=_calibration_group(
            metric='pws',
            pipeline='gb',
            value=gb_value,
            machine_sha=machine_sha,
            output_sizes=gb_output_sizes,
        ),
        sk_wall=_calibration_group(
            metric='wall',
            pipeline='sk',
            value=sk_value,
            machine_sha=machine_sha,
            output_sizes=sk_output_sizes,
        ),
        sk_pws=_calibration_group(
            metric='pws',
            pipeline='sk',
            value=sk_value,
            machine_sha=machine_sha,
            output_sizes=sk_output_sizes,
        ),
    )


def test_round_plan_uses_global_reference_candidate_order_for_rounds_one_to_ten() -> None:
    plans = build_round_plan(global_round_start=1, round_count=5) + build_round_plan(
        global_round_start=6, round_count=5
    )
    assert [plan.global_round for plan in plans] == list(range(1, 11))
    assert [plan.order for plan in plans] == [
        ('reference', 'candidate') if round_number % 2 else ('candidate', 'reference') for round_number in range(1, 11)
    ]


def test_append_group_starts_at_global_round_six() -> None:
    assert [plan.global_round for plan in build_round_plan(global_round_start=6, round_count=5)] == [6, 7, 8, 9, 10]


def test_validate_group_rejects_missing_round() -> None:
    group = _group()
    with pytest.raises(ValueError, match='rounds'):
        validate_metric_group(replace(group, rounds=group.rounds[:-1]))


def test_validate_group_rejects_duplicate_round() -> None:
    group = _group()
    duplicate = replace(group.rounds[-1], plan=group.rounds[0].plan)
    with pytest.raises(ValueError, match='rounds'):
        validate_metric_group(replace(group, rounds=group.rounds[:-1] + (duplicate,)))


def test_validate_group_rejects_rounds_two_through_six() -> None:
    with pytest.raises(ValueError, match='round window'):
        validate_metric_group(_replace_round_window(_group(), start=2, count=5))


def test_validate_group_rejects_ten_rounds_six_through_fifteen() -> None:
    with pytest.raises(ValueError, match='round window'):
        validate_metric_group(_replace_round_window(_group(), start=6, count=10))


def test_validate_group_rejects_unbalanced_order() -> None:
    group = _group()
    wrong_plan = replace(group.rounds[1].plan, order=('reference', 'candidate'))
    with pytest.raises(ValueError, match='order'):
        validate_metric_group(replace(group, rounds=(replace(group.rounds[1], plan=wrong_plan),) + group.rounds[1:]))


def test_validate_group_rejects_binary_sha_change() -> None:
    group = _group()
    changed = replace(group.rounds[-1], candidate=replace(group.rounds[-1].candidate, binary_sha256='changed'))
    with pytest.raises(ValueError, match='binary'):
        validate_metric_group(replace(group, rounds=group.rounds[:-1] + (changed,)))


def test_validate_group_rejects_input_or_git_drift() -> None:
    group = _group()
    for field, value in [('input_sha256', 'changed-input'), ('git_head', 'changed-head')]:
        changed_sample = replace(group.rounds[-1].candidate, **{field: value})
        changed_round = replace(group.rounds[-1], candidate=changed_sample)
        with pytest.raises(ValueError, match='drift'):
            validate_metric_group(replace(group, rounds=group.rounds[:-1] + (changed_round,)))


def test_same_batch_ratio_rejects_different_batch_id_n_or_round_order() -> None:
    group = _group()
    assert_same_batch_ratio(group)
    with pytest.raises(ValueError):
        assert_same_batch_ratio(replace(group, batch_id=''))
    with pytest.raises(ValueError):
        assert_same_batch_ratio(replace(group, rounds=group.rounds[:-1]))
    wrong_order = replace(group.rounds[0], plan=replace(group.rounds[0].plan, order=('candidate', 'reference')))
    with pytest.raises(ValueError):
        assert_same_batch_ratio(replace(group, rounds=(wrong_order,) + group.rounds[1:]))


def test_wall_and_pws_must_share_batch_id() -> None:
    with pytest.raises(ValueError, match='batch'):
        assert_same_benchmark_batch(_group(metric='wall'), _group(metric='pws', batch_id='other'))


def test_wall_and_pws_must_share_n_and_global_rounds() -> None:
    wall = _group(metric='wall')
    pws = _group(metric='pws')
    with pytest.raises(ValueError, match='rounds'):
        assert_same_benchmark_batch(wall, replace(pws, rounds=pws.rounds[:-1]))
    shifted = tuple(
        replace(item, plan=replace(item.plan, global_round=item.plan.global_round + 5)) for item in pws.rounds
    )
    with pytest.raises(ValueError, match='rounds'):
        assert_same_benchmark_batch(wall, replace(pws, rounds=shifted))


def test_wall_and_pws_must_share_input_and_binary_hashes() -> None:
    wall = _group(metric='wall')
    for kwargs in [
        {'input_sha': 'other-input'},
        {'reference_sha': 'other-reference'},
        {'candidate_sha': 'other-candidate'},
    ]:
        with pytest.raises(ValueError, match='hashes'):
            assert_same_benchmark_batch(wall, _group(metric='pws', **kwargs))


def test_wall_and_pws_must_share_machine_fingerprint() -> None:
    with pytest.raises(ValueError, match='machine'):
        assert_same_benchmark_batch(_group(metric='wall'), _group(metric='pws', machine_sha='other'))


def test_wall_and_pws_must_share_git_head() -> None:
    with pytest.raises(ValueError, match='Git'):
        assert_same_benchmark_batch(_group(metric='wall'), _group(metric='pws', git_head='other-head'))


def test_wall_and_pws_must_share_repository_state() -> None:
    with pytest.raises(ValueError, match='repository state'):
        assert_same_benchmark_batch(
            _group(metric='wall'),
            _group(metric='pws', repository_state_sha='other-state'),
        )


def test_calibration_group_requires_five_reference_only_rounds() -> None:
    group = _calibration_group(metric='wall', pipeline='sk')
    validate_calibration_group(group)
    with pytest.raises(ValueError, match='five'):
        validate_calibration_group(replace(group, rounds=group.rounds[:-1]))
    invalid = replace(group.rounds[-1], reference=replace(group.rounds[-1].reference, role='candidate'))
    with pytest.raises(ValueError, match='reference'):
        validate_calibration_group(replace(group, rounds=group.rounds[:-1] + (invalid,)))


def test_mandatory_expansion_is_false_outside_three_percent_boundary() -> None:
    assert not requires_mandatory_expansion(measured=Decimal('0.9699'), limit=Decimal('1.0'))
    assert not requires_mandatory_expansion(measured=Decimal('1.0301'), limit=Decimal('1.0'))


def test_mandatory_expansion_includes_exact_lower_and_upper_boundaries() -> None:
    assert requires_mandatory_expansion(measured=Decimal('0.97'), limit=Decimal('1.0'))
    assert requires_mandatory_expansion(measured=Decimal('1.03'), limit=Decimal('1.0'))


def test_mandatory_expansion_applies_when_first_group_temporarily_passes() -> None:
    assert Decimal('0.99') <= Decimal('1.0')
    assert requires_mandatory_expansion(measured=Decimal('0.99'), limit=Decimal('1.0'))


def test_mandatory_expansion_applies_when_first_group_temporarily_fails() -> None:
    assert Decimal('1.01') > Decimal('1.0')
    assert requires_mandatory_expansion(measured=Decimal('1.01'), limit=Decimal('1.0'))


def test_conflicting_five_round_groups_are_inconclusive() -> None:
    first = _group(start=1, candidate_value='0.9')
    second = _group(start=6, candidate_value='1.1')
    assert groups_have_conflicting_direction(first, second)


def test_direction_uses_ratio_of_group_medians_for_non_uniform_samples() -> None:
    first = _replace_metric_values(
        _group(start=1),
        reference_values=('1', '100', '101', '102', '103'),
        candidate_values=('99', '101', '102', '1', '1'),
    )
    second = _group(start=6, reference_value='1', candidate_value='0.9')

    # median(candidate) / median(reference) is 99/101 (<1) for the first group.
    # The median of per-pair ratios is >1, so this is a regression discriminator.
    assert not groups_have_conflicting_direction(first, second)


def test_group_join_rejects_ten_round_first_group() -> None:
    first = merge_metric_groups(_group(start=1, candidate_value='0.9'), _group(start=6, candidate_value='0.8'))
    with pytest.raises(ValueError, match='five-round'):
        groups_have_conflicting_direction(first, _group(start=6, candidate_value='0.7'))


def test_non_conflicting_groups_merge_to_global_rounds_one_through_ten() -> None:
    merged = merge_metric_groups(_group(start=1, candidate_value='0.9'), _group(start=6, candidate_value='0.8'))
    assert [item.plan.global_round for item in merged.rounds] == list(range(1, 11))
    assert merged.global_round_start == 1


def test_phase1_profile_uses_writer_populate_and_xlsx_save_from_same_samples() -> None:
    rule = PROFILE_RULES[ComparisonProfile.PHASE1_VS_PHASE0B]['sk']
    assert rule.same_sample_metrics == ('writer_populate', 'xlsx_save')
    assert set(COMPARISON_LIMITS[ComparisonProfile.PHASE1_VS_PHASE0B]['sk']) == {
        'writer_populate_ratio',
        'xlsx_save_ratio',
        'output_bytes_ratio',
    }


def test_zmij_profiles_require_four_of_five_wins_in_each_group() -> None:
    for profile in (
        ComparisonProfile.PHASE2_C_VS_A,
        ComparisonProfile.PHASE2_D_VS_B,
        ComparisonProfile.PHASE3_ZMIJ_ON_VS_OFF,
    ):
        assert PROFILE_RULES[profile]['sk'].requires_minimum_group_wins
        assert COMPARISON_LIMITS[profile]['sk']['minimum_wins'] == 4


def test_phase4_profile_uses_ingest_and_pws_from_same_batch() -> None:
    rule = PROFILE_RULES[ComparisonProfile.PHASE4_VS_PHASE3]['sk']
    assert rule.same_batch_metrics == ('ingest', 'pws')


def test_output_bytes_uses_conservative_phase0a_median_with_valid_one_byte_variation() -> None:
    manifest = _manifest(
        gb_output_sizes=(100, 101, 101, 101, 101),
        sk_output_sizes=(200, 200, 200, 200, 200),
    )
    assert approved_phase0a_output_bytes(manifest, 'gb') == 101
    assert approved_phase0a_output_bytes(manifest, 'sk') == 200
    assert_output_bytes_within_phase0a_limit(candidate_bytes=220, manifest=manifest, pipeline='sk')
    with pytest.raises(ValueError, match='110%'):
        assert_output_bytes_within_phase0a_limit(candidate_bytes=221, manifest=manifest, pipeline='sk')
    for sizes in [(None, None, None, None, None), (0, 0, 0, 0, 0)]:
        with pytest.raises(ValueError, match='Phase 0A'):
            approved_phase0a_output_bytes(_manifest(sk_output_sizes=sizes), 'sk')
    mislabeled = replace(manifest, sk_pws=replace(manifest.sk_pws, pipeline='gb'))
    with pytest.raises(ValueError, match='pipeline'):
        approved_phase0a_output_bytes(mislabeled, 'sk')
    mislabeled = replace(manifest, sk_pws=replace(manifest.sk_pws, metric='wall'))
    with pytest.raises(ValueError, match='metric'):
        approved_phase0a_output_bytes(mislabeled, 'sk')


def test_output_bytes_median_rounds_even_half_byte_upward() -> None:
    assert aggregate_output_bytes((100, 101)) == 101


@pytest.mark.parametrize('values', ((), (None,), (True,), (1.5,), ('1',), (0,), (-1,)))
def test_output_bytes_median_rejects_empty_or_invalid_sizes(values: tuple[object, ...]) -> None:
    with pytest.raises(ValueError, match='output bytes'):
        aggregate_output_bytes(values)


def test_environment_drift_rejects_changed_machine_fingerprint() -> None:
    with pytest.raises(ValueError, match='machine'):
        assert_environment_not_drifted(_group(machine_sha='changed'), _manifest())


def test_environment_drift_rejects_reference_median_over_ten_percent() -> None:
    with pytest.raises(ValueError, match='median'):
        assert_environment_not_drifted(_group(reference_value='1.1001'), _manifest(sk_value='1'))


def test_environment_drift_accepts_exactly_ten_percent() -> None:
    assert_environment_not_drifted(_group(reference_value='1.10'), _manifest(sk_value='1'))

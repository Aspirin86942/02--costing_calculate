from __future__ import annotations

import json
import subprocess
import zipfile
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from src.services.costing_service import CostingRunRequest, CostingRunResult, ServiceStatus
from tests.rust_oracle import oracle_runner
from tests.rust_oracle.benchmark_protocol import RuntimeSchema
from tests.rust_oracle.oracle_runner import (
    OracleRunSummary,
    assert_runtime_contract_matches,
    parse_runtime_payload,
    parse_rust_run_summary,
)

MISSING_STAGE = object()


def _write_minimal_xlsx(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, 'w') as archive:
        archive.writestr('[Content_Types].xml', '<Types/>')
        archive.writestr('_rels/.rels', '<Relationships/>')
        archive.writestr('xl/workbook.xml', '<workbook/>')
        archive.writestr('xl/worksheets/sheet1.xml', '<worksheet/>')


def valid_rust_check_only_payload() -> dict[str, Any]:
    return {
        'status': 'succeeded',
        'pipeline': 'gb',
        'output_written': False,
        'workbook_path': None,
        'sheet_count': 3,
        'error_log_count': 0,
        'issue_type_counts': {},
        'quality_metrics': [],
        'run_counts': {
            'reader_rows': 1,
            'detail_rows': 1,
            'qty_rows': 1,
            'qty_sheet_rows': 1,
            'quality_metric_count': 0,
            'work_order_rows': 1,
        },
        'stage_timings': {
            'stages': {
                'ingest': 1.0,
                'normalize': 2.0,
                'split': 3.0,
                'fact': 4.0,
                'presentation': 5.0,
                'total': 99.0,
            }
        },
    }


def valid_runtime_payload() -> dict[str, Any]:
    payload = valid_rust_check_only_payload()
    payload.update(
        {
            'output_written': True,
            'workbook_path': 'output.xlsx',
            'request_id': 'request-1',
            'sheet_dimensions': ['10x2', '5x3', '1x1'],
            'output_size_bytes': 123,
            'reader_snapshot_sha256': 'a' * 64,
        }
    )
    payload['stage_timings']['stages']['export'] = 0.5
    return payload


def test_base_runtime_schema_does_not_require_writer_breakdown() -> None:
    runtime = parse_runtime_payload(valid_runtime_payload(), schema=RuntimeSchema.BASE)

    assert dict(runtime.stage_timings)['export'] == 0.5
    assert 'writer_populate' not in dict(runtime.stage_timings)


def test_instrumented_runtime_schema_requires_writer_populate_xlsx_save_and_output_size() -> None:
    payload = valid_runtime_payload()
    payload['stage_timings']['stages'].update(writer_populate=0.2, xlsx_save=0.3)
    for missing in ('writer_populate', 'xlsx_save'):
        broken = json.loads(json.dumps(payload))
        broken['stage_timings']['stages'].pop(missing)
        with pytest.raises(AssertionError, match=missing):
            parse_runtime_payload(broken, schema=RuntimeSchema.INSTRUMENTED)

    with pytest.raises(AssertionError, match='output_size_bytes'):
        parse_runtime_payload({**payload, 'output_size_bytes': None}, schema=RuntimeSchema.INSTRUMENTED)

    assert parse_runtime_payload(payload, schema=RuntimeSchema.INSTRUMENTED).output_size_bytes == 123


def test_reader_instrumented_schema_requires_ingest_and_reader_contract() -> None:
    payload = valid_runtime_payload()
    payload['stage_timings']['stages'].update(writer_populate=0.2, xlsx_save=0.3)
    del payload['reader_snapshot_sha256']
    with pytest.raises(AssertionError, match='reader_snapshot_sha256'):
        parse_runtime_payload(payload, schema=RuntimeSchema.READER_INSTRUMENTED)

    payload['reader_snapshot_sha256'] = 'a' * 64
    del payload['run_counts']['reader_rows']
    with pytest.raises(AssertionError, match='reader_rows'):
        parse_runtime_payload(payload, schema=RuntimeSchema.READER_INSTRUMENTED)


@pytest.mark.parametrize('value', (float('nan'), float('inf'), -0.01))
def test_runtime_parser_rejects_non_finite_or_negative_timings(value: float) -> None:
    payload = valid_runtime_payload()
    payload['stage_timings']['stages']['ingest'] = value
    with pytest.raises(AssertionError, match='ingest'):
        parse_runtime_payload(payload, schema=RuntimeSchema.BASE)


def test_runtime_parser_rejects_unexpected_workbook_path_in_check_only() -> None:
    payload = valid_runtime_payload()
    payload['output_written'] = False
    with pytest.raises(AssertionError, match='workbook_path'):
        parse_runtime_payload(payload, schema=RuntimeSchema.BASE)


@pytest.mark.parametrize('schema', tuple(RuntimeSchema))
def test_runtime_parser_rejects_unknown_stage_for_every_schema(schema: RuntimeSchema) -> None:
    payload = valid_runtime_payload()
    if schema is not RuntimeSchema.BASE:
        payload['stage_timings']['stages'].update(writer_populate=0.2, xlsx_save=0.3)
    payload['stage_timings']['stages']['future_unknown_stage'] = 0.1

    with pytest.raises(AssertionError, match='unexpected'):
        parse_runtime_payload(payload, schema=schema)


def test_runtime_parser_rejects_string_schema() -> None:
    with pytest.raises(AssertionError, match='RuntimeSchema'):
        parse_runtime_payload(valid_runtime_payload(), schema='base')  # type: ignore[arg-type]


def test_cargo_target_directory_comes_from_metadata(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target_directory = tmp_path / 'custom-target'

    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        assert kwargs['encoding'] == 'utf-8'
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout=f'{{"target_directory": {target_directory.as_posix()!r}}}'.replace("'", '"'),
            stderr='',
        )

    monkeypatch.setattr(oracle_runner.subprocess, 'run', fake_run)

    actual = oracle_runner._cargo_target_directory('cargo', tmp_path, tmp_path / 'Cargo.toml')

    assert actual == target_directory


def test_run_python_oracle_reuses_normal_runner_request_configuration(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    configured_product_order = (('P-CONFIG', '配置产品'),)
    captured_request: CostingRunRequest | None = None

    def fake_build_request(**kwargs: object) -> CostingRunRequest:
        return CostingRunRequest(
            pipeline='gb',
            input_path=kwargs['input_file'],
            output_dir=tmp_path / 'runner-default',
            product_order=configured_product_order,
            benchmark=True,
            overwrite_confirmed=True,
        )

    def fake_run_costing_request(request: CostingRunRequest) -> CostingRunResult:
        nonlocal captured_request
        captured_request = request
        generated = tmp_path / 'generated.xlsx'
        generated.write_bytes(b'oracle')
        return CostingRunResult(
            status=ServiceStatus.SUCCEEDED,
            message='ok',
            workbook_path=generated,
        )

    monkeypatch.setattr(oracle_runner, '_build_request', fake_build_request)
    monkeypatch.setattr(oracle_runner, 'run_costing_request', fake_run_costing_request)

    output = tmp_path / 'python-oracle.xlsx'
    oracle_runner.run_python_oracle('gb', tmp_path / 'input.xlsx', output)

    assert captured_request is not None
    assert captured_request.product_order == configured_product_order
    assert captured_request.output_dir == tmp_path
    assert output.read_bytes() == b'oracle'


def test_parse_rust_run_summary_reads_runtime_contract() -> None:
    summary = parse_rust_run_summary(
        """{
            "error_log_count": 3,
            "issue_type_counts": {"MISSING_AMOUNT": 1, "NON_POSITIVE_UNIT_COST": 2},
            "quality_metrics": [
                {"category": "行数勾稽", "metric": "数量页输入行数", "value": "2", "description": "ignored"}
            ]
        }"""
    )

    assert summary == OracleRunSummary(
        error_log_count=3,
        issue_type_counts={'MISSING_AMOUNT': 1, 'NON_POSITIVE_UNIT_COST': 2},
        quality_metrics={('行数勾稽', '数量页输入行数'): '2'},
    )


def test_parse_rust_run_summary_rejects_non_json_stdout() -> None:
    with pytest.raises(AssertionError, match='valid JSON'):
        parse_rust_run_summary('not json')


def test_parse_rust_check_only_run_uses_total_not_stage_sum() -> None:
    payload = valid_rust_check_only_payload()

    result = oracle_runner.parse_rust_check_only_run(json.dumps(payload, ensure_ascii=False))

    assert result.payload_total_seconds == 99.0
    assert result.payload_total_seconds != sum(value for name, value in result.stage_timings.items() if name != 'total')


@pytest.mark.parametrize('total', (None, float('nan'), float('inf'), -1.0))
def test_parse_rust_check_only_run_rejects_invalid_total(total: float | None) -> None:
    payload = valid_rust_check_only_payload()
    if total is None:
        del payload['stage_timings']['stages']['total']
    else:
        payload['stage_timings']['stages']['total'] = total

    with pytest.raises(AssertionError, match='total'):
        oracle_runner.parse_rust_check_only_run(json.dumps(payload))


def test_parse_rust_check_only_run_rejects_export_stage() -> None:
    payload = valid_rust_check_only_payload()
    payload['stage_timings']['stages']['export'] = 0.5

    with pytest.raises(AssertionError, match='export'):
        oracle_runner.parse_rust_check_only_run(json.dumps(payload))


@pytest.mark.parametrize(
    ('field_name', 'value', 'expected_message'),
    (
        ('status', 'failed', 'status'),
        ('sheet_count', 2, 'three'),
    ),
)
def test_parse_rust_check_only_run_requires_succeeded_status_and_three_sheets(
    field_name: str,
    value: object,
    expected_message: str,
) -> None:
    payload = valid_rust_check_only_payload()
    payload[field_name] = value

    with pytest.raises(AssertionError, match=expected_message):
        oracle_runner.parse_rust_check_only_run(json.dumps(payload))


@pytest.mark.parametrize('pipeline', ([], {}), ids=('list', 'dict'))
def test_parse_rust_check_only_run_rejects_non_string_pipeline(pipeline: object) -> None:
    payload = valid_rust_check_only_payload()
    payload['pipeline'] = pipeline

    with pytest.raises(AssertionError, match='invalid Rust check-only pipeline'):
        oracle_runner.parse_rust_check_only_run(json.dumps(payload))


def test_parse_rust_check_only_run_requires_output_written_false() -> None:
    payload = valid_rust_check_only_payload()
    payload['output_written'] = True

    with pytest.raises(AssertionError, match='output_written=false'):
        oracle_runner.parse_rust_check_only_run(json.dumps(payload))


@pytest.mark.parametrize(
    ('stage_name', 'raw_value'),
    (
        ('ingest', MISSING_STAGE),
        ('normalize', float('nan')),
        ('split', float('inf')),
        ('fact', -1.0),
        ('presentation', True),
        ('total', '99.0'),
    ),
)
def test_parse_rust_check_only_run_rejects_missing_or_non_finite_required_stage(
    stage_name: str,
    raw_value: object,
) -> None:
    payload = valid_rust_check_only_payload()
    if raw_value is MISSING_STAGE:
        del payload['stage_timings']['stages'][stage_name]
    else:
        payload['stage_timings']['stages'][stage_name] = raw_value

    with pytest.raises(AssertionError, match=stage_name):
        oracle_runner.parse_rust_check_only_run(json.dumps(payload))


@pytest.mark.parametrize(
    ('count_name', 'raw_value'),
    (
        ('reader_rows', None),
        ('detail_rows', '1'),
        ('qty_rows', True),
        ('qty_sheet_rows', -1),
    ),
)
def test_parse_rust_check_only_run_rejects_missing_or_non_integer_run_count(
    count_name: str,
    raw_value: object,
) -> None:
    payload = valid_rust_check_only_payload()
    if raw_value is None:
        del payload['run_counts'][count_name]
    else:
        payload['run_counts'][count_name] = raw_value

    with pytest.raises(AssertionError, match=r'run.count'):
        oracle_runner.parse_rust_check_only_run(json.dumps(payload))


def test_run_rust_cli_release_check_only_omits_output_argument(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    executable = tmp_path / 'costing-calculate.exe'
    input_path = tmp_path / 'input.xlsx'

    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        command = args[0]
        assert command == [
            str(executable),
            'gb',
            '--input',
            str(input_path.resolve()),
            '--check-only',
            '--benchmark',
        ]
        assert '--output' not in command
        assert kwargs['cwd'] == oracle_runner.repo_root()
        assert kwargs['encoding'] == 'utf-8'
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout=json.dumps(valid_rust_check_only_payload()),
            stderr='',
        )

    monkeypatch.setattr(oracle_runner.subprocess, 'run', fake_run)

    result = oracle_runner.run_rust_cli_release_check_only(executable, 'gb', input_path)

    assert result.pipeline == 'gb'
    assert result.payload_total_seconds == 99.0


def test_run_rust_cli_release_check_only_rejects_pipeline_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    payload = valid_rust_check_only_payload()
    payload['pipeline'] = 'sk'

    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout=json.dumps(payload),
            stderr='',
        )

    monkeypatch.setattr(oracle_runner.subprocess, 'run', fake_run)

    with pytest.raises(AssertionError, match="reported pipeline 'sk', expected 'gb'"):
        oracle_runner.run_rust_cli_release_check_only(
            tmp_path / 'costing-calculate.exe',
            'gb',
            tmp_path / 'input.xlsx',
        )


def test_run_python_check_only_payload_times_only_build_call(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[str] = []
    request = object()
    prepared = SimpleNamespace(month_range=None)
    payload = SimpleNamespace(
        error_log_count=0,
        error_log_export=SimpleNamespace(empty=True, columns=[]),
        quality_metrics=(),
        stage_timings={'ingest': 1.25},
    )

    class FakePipeline:
        def build_workbook_payload(self, input_path: Path, **kwargs: object) -> SimpleNamespace:
            events.append('build-payload')
            assert input_path == tmp_path / 'input.xlsx'
            assert kwargs == {
                'standalone_cost_items': ('委外加工费',),
                'product_anomaly_scope_mode': 'configured-scope',
                'month_range': None,
                'presentation_product_order': (('P1', '产品一'),),
                'artifacts_transform': etl._filter_analysis_artifacts_by_whitelist,
                'progress_callback': None,
            }
            return payload

    class FakeEtl:
        standalone_cost_items = ('委外加工费',)
        product_anomaly_scope_mode = 'configured-scope'
        month_range = None
        product_order = (('P1', '产品一'),)

        def __init__(self) -> None:
            self.pipeline = FakePipeline()

        def _reset_last_run_state(self) -> None:
            events.append('reset-state')

        def _filter_analysis_artifacts_by_whitelist(self, artifacts: object) -> object:
            return artifacts

    etl = FakeEtl()

    def fake_build_request(**kwargs: object) -> object:
        events.append('prepare-request')
        assert kwargs['benchmark'] is True
        return request

    def fake_prepare_request(
        actual_request: object,
        *,
        validate_output_dir: bool,
    ) -> tuple[SimpleNamespace, None]:
        events.append('prepare-input')
        assert actual_request is request
        assert validate_output_dir is False
        return prepared, None

    def fake_build_etl(actual_request: object, month_range: object) -> FakeEtl:
        events.append('build-etl')
        assert actual_request is request
        assert month_range is None
        return etl

    counter_values = iter((10.0, 12.5))

    def fake_perf_counter() -> float:
        value = next(counter_values)
        events.append('timer-start' if value == 10.0 else 'timer-stop')
        return value

    def fake_quality_metric_values(metrics: object) -> dict[tuple[str, str], str]:
        events.append('build-summary')
        assert metrics == ()
        return {}

    monkeypatch.setattr(oracle_runner, '_build_request', fake_build_request)
    monkeypatch.setattr(oracle_runner.costing_service, '_prepare_request', fake_prepare_request)
    monkeypatch.setattr(oracle_runner.costing_service, '_build_etl', fake_build_etl)
    monkeypatch.setattr(oracle_runner.time, 'perf_counter', fake_perf_counter)
    monkeypatch.setattr(oracle_runner, '_quality_metric_values', fake_quality_metric_values)

    result = oracle_runner.run_python_check_only_payload('gb', tmp_path / 'input.xlsx')

    assert events == [
        'prepare-request',
        'prepare-input',
        'build-etl',
        'reset-state',
        'timer-start',
        'build-payload',
        'timer-stop',
        'build-summary',
    ]
    assert result.payload_total_seconds == 2.5
    assert result.stage_timings == {'ingest': 1.25}
    assert 'total' not in result.stage_timings


def test_run_python_check_only_payload_does_not_write_workbook(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    payload = SimpleNamespace(
        error_log_count=0,
        error_log_export=SimpleNamespace(empty=True, columns=[]),
        quality_metrics=(),
        stage_timings={},
    )

    class FakePipeline:
        def build_workbook_payload(self, input_path: Path, **kwargs: object) -> SimpleNamespace:
            return payload

    class FakeEtl:
        standalone_cost_items: tuple[str, ...] = ()
        product_anomaly_scope_mode = 'configured-scope'
        month_range = None
        product_order: tuple[tuple[str, str], ...] = ()

        def __init__(self) -> None:
            self.pipeline = FakePipeline()

        def _reset_last_run_state(self) -> None:
            return None

        def _filter_analysis_artifacts_by_whitelist(self, artifacts: object) -> object:
            return artifacts

    def fail_if_full_runner_is_called(request: object) -> None:
        pytest.fail('check-only helper must not call the workbook-writing full runner')

    monkeypatch.setattr(oracle_runner, '_build_request', lambda **kwargs: object())
    monkeypatch.setattr(
        oracle_runner.costing_service,
        '_prepare_request',
        lambda request, validate_output_dir: (SimpleNamespace(month_range=None), None),
    )
    monkeypatch.setattr(oracle_runner.costing_service, '_build_etl', lambda request, month_range: FakeEtl())
    monkeypatch.setattr(oracle_runner, 'run_costing_request', fail_if_full_runner_is_called)
    counter_values = iter((1.0, 2.0))
    monkeypatch.setattr(oracle_runner.time, 'perf_counter', lambda: next(counter_values))

    oracle_runner.run_python_check_only_payload('gb', tmp_path / 'input.xlsx')

    assert not (tmp_path / 'input_处理后.xlsx').exists()


def test_run_rust_normal_captured_returns_typed_result_without_versioned_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    executable = tmp_path / 'costing-calculate.exe'
    executable.write_bytes(b'rust-binary')
    input_path = tmp_path / 'input.xlsx'
    input_path.write_bytes(b'input-workbook')
    output_path = tmp_path / 'data' / 'processed' / 'normal-output.xlsx'
    local_log_root = tmp_path / 'rust' / 'target' / 'perf-local' / 'raw-logs'
    monkeypatch.setattr(oracle_runner, 'repo_root', lambda: tmp_path)
    counter = iter((1.0, 2.25))
    monkeypatch.setattr(oracle_runner.time, 'perf_counter', lambda: next(counter))

    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        command = args[0]
        assert command == [
            str(executable.resolve()),
            'gb',
            '--input',
            str(input_path.resolve()),
            '--output',
            str(output_path.resolve()),
            '--benchmark',
        ]
        _write_minimal_xlsx(output_path)
        payload = valid_rust_check_only_payload()
        payload['output_written'] = True
        payload['workbook_path'] = str(output_path.resolve())
        payload['stage_timings']['stages']['export'] = 0.5
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout=json.dumps(payload, ensure_ascii=False),
            stderr='',
        )

    monkeypatch.setattr(oracle_runner.subprocess, 'run', fake_run)

    result = oracle_runner.run_rust_normal_captured(
        executable,
        'gb',
        input_path,
        output_path,
        schema=RuntimeSchema.BASE,
        local_log_root=local_log_root,
    )

    assert result.normal_run.external_wall_seconds == Decimal('1.25')
    assert result.normal_run.runtime.output_size_bytes == output_path.stat().st_size
    assert output_path.exists()  # 上层 wall runner 负责在校验后立即清理。
    assert [path.name for path in local_log_root.glob('*.json')] == [f'{result.local_unversioned_log_sha256}.json']


@pytest.mark.parametrize('collision_target', ('input', 'executable'))
def test_run_rust_normal_captured_rejects_output_collisions_before_subprocess(
    collision_target: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    executable = tmp_path / 'costing-calculate.exe'
    executable.write_bytes(b'rust-binary')
    input_path = tmp_path / 'input.xlsx'
    input_path.write_bytes(b'input-workbook')
    collision_paths = {
        'input': input_path,
        'executable': executable,
    }
    subprocess_called = False

    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        nonlocal subprocess_called
        subprocess_called = True
        raise RuntimeError('subprocess must not run for an evidence path collision')

    monkeypatch.setattr(oracle_runner.subprocess, 'run', fake_run)

    monkeypatch.setattr(oracle_runner, 'repo_root', lambda: tmp_path)
    with pytest.raises(AssertionError, match='output'):
        oracle_runner.run_rust_normal_captured(
            executable,
            'gb',
            input_path,
            collision_paths[collision_target],
            schema=RuntimeSchema.BASE,
            local_log_root=tmp_path / 'rust' / 'target' / 'perf-local' / 'raw-logs',
        )

    assert subprocess_called is False
    assert input_path.read_bytes() == b'input-workbook'
    assert executable.read_bytes() == b'rust-binary'


def test_run_rust_normal_captured_maps_launch_exception_and_leaves_partial_output_for_outer_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    executable = tmp_path / 'costing-calculate.exe'
    executable.write_bytes(b'rust-binary')
    input_path = tmp_path / 'input.xlsx'
    input_path.write_bytes(b'input-workbook')
    output_path = tmp_path / 'data' / 'processed' / 'normal-output.xlsx'

    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b'partial-workbook')
        raise RuntimeError('subprocess launch failed after creating output')

    monkeypatch.setattr(oracle_runner.subprocess, 'run', fake_run)
    monkeypatch.setattr(oracle_runner, 'repo_root', lambda: tmp_path)

    with pytest.raises(oracle_runner.RustNormalProcessError) as caught:
        oracle_runner.run_rust_normal_captured(
            executable,
            'gb',
            input_path,
            output_path,
            schema=RuntimeSchema.BASE,
            local_log_root=tmp_path / 'rust' / 'target' / 'perf-local' / 'raw-logs',
        )

    assert output_path.read_bytes() == b'partial-workbook'
    assert len(caught.value.log_sha256) == 64


def test_run_rust_normal_captured_rejects_versioned_output_before_subprocess(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    executable = tmp_path / 'costing-calculate.exe'
    executable.write_bytes(b'rust')
    input_path = tmp_path / 'input.xlsx'
    input_path.write_bytes(b'input')
    monkeypatch.setattr(oracle_runner, 'repo_root', lambda: tmp_path)
    monkeypatch.setattr(
        oracle_runner.subprocess,
        'run',
        lambda *args, **kwargs: pytest.fail('invalid output must be rejected before subprocess'),
    )

    with pytest.raises(AssertionError, match='data/processed or rust/target'):
        oracle_runner.run_rust_normal_captured(
            executable,
            'gb',
            input_path,
            tmp_path / 'docs' / 'evidence.xlsx',
            schema=RuntimeSchema.BASE,
            local_log_root=tmp_path / 'rust' / 'target' / 'perf-local' / 'raw-logs',
        )


def test_run_rust_normal_captured_rejects_raw_symlink_component(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    executable = tmp_path / 'costing-calculate.exe'
    executable.write_bytes(b'rust')
    input_path = tmp_path / 'input.xlsx'
    input_path.write_bytes(b'input')
    real_directory = tmp_path / 'outside'
    real_directory.mkdir()
    link = tmp_path / 'data' / 'processed' / 'linked'
    link.parent.mkdir(parents=True)
    try:
        link.symlink_to(real_directory, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f'symlink creation is unavailable: {exc}')
    monkeypatch.setattr(oracle_runner, 'repo_root', lambda: tmp_path)

    with pytest.raises(AssertionError, match='reparse|symlink'):
        oracle_runner.run_rust_normal_captured(
            executable,
            'gb',
            input_path,
            link / 'output.xlsx',
            schema=RuntimeSchema.BASE,
            local_log_root=tmp_path / 'rust' / 'target' / 'perf-local' / 'raw-logs',
        )


def test_run_rust_normal_captured_rejects_malformed_xlsx_with_log_sha(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    executable = tmp_path / 'costing-calculate.exe'
    executable.write_bytes(b'rust')
    input_path = tmp_path / 'input.xlsx'
    input_path.write_bytes(b'input')
    output_path = tmp_path / 'data' / 'processed' / 'malformed.xlsx'
    monkeypatch.setattr(oracle_runner, 'repo_root', lambda: tmp_path)

    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b'workbook')
        payload = valid_runtime_payload()
        payload['workbook_path'] = str(output_path.resolve())
        return subprocess.CompletedProcess(args=args[0], returncode=0, stdout=json.dumps(payload), stderr='')

    monkeypatch.setattr(oracle_runner.subprocess, 'run', fake_run)
    with pytest.raises(oracle_runner.RustNormalValidationError) as caught:
        oracle_runner.run_rust_normal_captured(
            executable,
            'gb',
            input_path,
            output_path,
            schema=RuntimeSchema.BASE,
            local_log_root=tmp_path / 'rust' / 'target' / 'perf-local' / 'raw-logs',
        )
    assert len(caught.value.log_sha256) == 64


def test_runtime_contract_match_accepts_equal_summaries() -> None:
    summary = OracleRunSummary(
        error_log_count=1,
        issue_type_counts={'NON_POSITIVE_UNIT_COST': 1},
        quality_metrics={('行数勾稽', '数量页输入行数'): '1'},
    )

    assert_runtime_contract_matches(summary, summary)


def test_runtime_contract_match_allows_rust_only_quality_metric() -> None:
    expected = OracleRunSummary(
        error_log_count=1,
        issue_type_counts={'NON_POSITIVE_UNIT_COST': 1},
        quality_metrics={('行数勾稽', '数量页输入行数'): '1'},
    )
    actual = OracleRunSummary(
        error_log_count=1,
        issue_type_counts={'NON_POSITIVE_UNIT_COST': 1},
        quality_metrics={
            ('行数勾稽', '数量页输入行数'): '1',
            ('范围检查', '完工数量小于等于0行数'): '0',
        },
    )

    assert_runtime_contract_matches(expected, actual)


def test_runtime_contract_match_reports_error_log_and_issue_type_mismatches() -> None:
    expected = OracleRunSummary(
        error_log_count=2,
        issue_type_counts={'MISSING_AMOUNT': 1, 'NON_POSITIVE_UNIT_COST': 1},
        quality_metrics={('行数勾稽', '数量页输入行数'): '2'},
    )
    actual = OracleRunSummary(
        error_log_count=1,
        issue_type_counts={'MISSING_AMOUNT': 1},
        quality_metrics={('行数勾稽', '数量页输入行数'): '1'},
    )

    with pytest.raises(AssertionError, match='error_log_count mismatch') as exc_info:
        assert_runtime_contract_matches(expected, actual)

    assert 'issue_type_counts mismatch' in str(exc_info.value)
    assert 'quality_metrics mismatch' in str(exc_info.value)


def test_runtime_contract_match_reports_missing_quality_metric() -> None:
    expected = OracleRunSummary(
        error_log_count=1,
        issue_type_counts={'MISSING_AMOUNT': 1},
        quality_metrics={('行数勾稽', '数量页输入行数'): '1'},
    )
    actual = OracleRunSummary(error_log_count=1, issue_type_counts={'MISSING_AMOUNT': 1}, quality_metrics={})

    with pytest.raises(AssertionError, match='quality_metrics mismatch') as exc_info:
        assert_runtime_contract_matches(expected, actual)

    assert 'missing=' in str(exc_info.value)


def test_runtime_contract_match_reports_changed_quality_metric_value() -> None:
    expected = OracleRunSummary(
        error_log_count=1,
        issue_type_counts={'MISSING_AMOUNT': 1},
        quality_metrics={('行数勾稽', '数量页输入行数'): '1'},
    )
    actual = OracleRunSummary(
        error_log_count=1,
        issue_type_counts={'MISSING_AMOUNT': 1},
        quality_metrics={('行数勾稽', '数量页输入行数'): '2'},
    )

    with pytest.raises(AssertionError, match='quality_metrics mismatch') as exc_info:
        assert_runtime_contract_matches(expected, actual)

    assert 'values=' in str(exc_info.value)

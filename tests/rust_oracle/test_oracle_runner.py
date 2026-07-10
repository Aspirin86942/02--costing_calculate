from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from src.services.costing_service import CostingRunRequest, CostingRunResult, ServiceStatus
from tests.rust_oracle import oracle_runner
from tests.rust_oracle.oracle_runner import (
    OracleRunSummary,
    assert_runtime_contract_matches,
    parse_rust_run_summary,
)


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

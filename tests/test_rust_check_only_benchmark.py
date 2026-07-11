from __future__ import annotations

from pathlib import Path

import pytest

from tests.rust_oracle.benchmark import (
    CHECK_ONLY_ROUNDS,
    run_check_only_payload_benchmark,
    write_check_only_benchmark_result,
)
from tests.rust_oracle.oracle_runner import (
    REQUIRED_RUST_PAYLOAD_STAGES,
    build_rust_cli_release,
)
from tests.rust_oracle.repo_paths import repo_root, require_benchmark_sample


@pytest.fixture(scope='module')
def rust_release_executable() -> Path:
    return build_rust_cli_release()


@pytest.mark.parametrize('pipeline', ('gb', 'sk'))
def test_rust_check_only_is_not_slower_than_python(
    pipeline: str,
    rust_release_executable: Path,
) -> None:
    result = run_check_only_payload_benchmark(
        pipeline,
        require_benchmark_sample(pipeline),
        rust_release_executable,
    )
    result_path = repo_root() / 'rust' / 'target' / 'perf' / 'results' / f'check-only-final-{pipeline}.json'
    write_check_only_benchmark_result(result, result_path)

    assert len(result.rust_payload_total_seconds) == CHECK_ONLY_ROUNDS, result
    assert len(result.python_payload_total_seconds) == CHECK_ONLY_ROUNDS, result
    assert set(result.rust_stage_seconds) == set(REQUIRED_RUST_PAYLOAD_STAGES), result
    assert all(len(values) == CHECK_ONLY_ROUNDS for values in result.rust_stage_seconds.values()), result
    assert result.valid_pair_count == CHECK_ONLY_ROUNDS, result
    assert result.validation_passed, result
    assert (
        sum(result.rust_runtime_evidence.issue_type_counts.values()) == result.rust_runtime_evidence.error_log_count
    ), result
    assert result.rust_median_seconds <= result.python_median_seconds, result
    assert result.verdict == 'VALIDATED', result

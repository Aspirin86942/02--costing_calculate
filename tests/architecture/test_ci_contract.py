from __future__ import annotations

import re
from pathlib import Path

from tools.ci.baseline_policy import evaluate_baseline_policy

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CI_WORKFLOW = PROJECT_ROOT / '.github' / 'workflows' / 'ci.yml'
CODEOWNERS = PROJECT_ROOT / '.github' / 'CODEOWNERS'

REQUIRED_CI_COMMANDS = (
    'cargo fmt --manifest-path rust/Cargo.toml --all --check',
    ('cargo clippy --locked --manifest-path rust/Cargo.toml --workspace --all-targets --all-features -- -D warnings'),
    'cargo test --locked --manifest-path rust/Cargo.toml --workspace --all-features',
    'uv sync --frozen --extra dev',
    'uv run python -m ruff check src tests tools',
    'uv run python -m ruff format src tests tools --check',
    ('uv run python -m pytest tests -q -m "not slow and not benchmark and not meta" --basetemp .pytest-tmp/ci'),
)


def test_ci_workflow_is_pinned_and_runs_required_public_gates() -> None:
    workflow = CI_WORKFLOW.read_text(encoding='utf-8')

    for trigger in ('pull_request:', 'push:', 'workflow_dispatch:', 'workflow_call:'):
        assert trigger in workflow
    assert 'windows-latest' in workflow
    assert 'ubuntu-latest' in workflow
    for command in REQUIRED_CI_COMMANDS:
        assert command in workflow
    assert 'tools/ci/run_synthetic_e2e.py' in workflow
    assert 'tools/ci/baseline_policy.py' in workflow

    action_refs = re.findall(r'uses:\s+[^@\s]+@([^\s#]+)', workflow)
    assert action_refs
    assert all(re.fullmatch(r'[0-9a-f]{40}', ref) for ref in action_refs)


def test_codeowners_requires_review_for_frozen_baselines() -> None:
    codeowners = CODEOWNERS.read_text(encoding='utf-8')

    assert '/tests/contracts/baselines/' in codeowners
    assert '/docs/performance/baselines/' in codeowners


def test_baseline_policy_requires_label_and_structured_pr_description() -> None:
    changed_paths = ['tests/contracts/baselines/workbook_semantics.json']

    missing_label = evaluate_baseline_policy(
        changed_paths=changed_paths,
        labels=[],
        pull_request_body='',
    )
    assert any('contract-change' in error for error in missing_label)

    incomplete_body = evaluate_baseline_policy(
        changed_paths=changed_paths,
        labels=['contract-change'],
        pull_request_body='变更前行为：旧行为',
    )
    assert any('变更后行为' in error for error in incomplete_body)
    assert any('业务原因' in error for error in incomplete_body)
    assert any('受影响 Sheet 和字段' in error for error in incomplete_body)

    complete_body = '\n'.join(
        (
            '变更前行为：旧行为',
            '变更后行为：新行为',
            '业务原因：已批准的口径调整',
            '受影响 Sheet 和字段：成本计算单总表 / 本期完工金额',
        )
    )
    assert (
        evaluate_baseline_policy(
            changed_paths=changed_paths,
            labels=['contract-change'],
            pull_request_body=complete_body,
        )
        == []
    )


def test_baseline_policy_ignores_unfrozen_changes() -> None:
    assert (
        evaluate_baseline_policy(
            changed_paths=['rust/crates/costing-core/src/model.rs'],
            labels=[],
            pull_request_body='',
        )
        == []
    )

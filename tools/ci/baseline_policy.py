"""Enforce the approval contract for frozen baseline changes."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

FROZEN_BASELINE_PREFIXES = (
    'tests/contracts/baselines/',
    'docs/performance/baselines/',
)
REQUIRED_PULL_REQUEST_SECTIONS = (
    '变更前行为',
    '变更后行为',
    '业务原因',
    '受影响 Sheet 和字段',
)


def evaluate_baseline_policy(
    *,
    changed_paths: list[str],
    labels: list[str],
    pull_request_body: str,
) -> list[str]:
    """Return every policy violation for a proposed baseline change."""
    baseline_changes = [
        path.replace('\\', '/')
        for path in changed_paths
        if path.replace('\\', '/').startswith(FROZEN_BASELINE_PREFIXES)
    ]
    if not baseline_changes:
        return []

    errors: list[str] = []
    if 'contract-change' not in labels:
        errors.append('冻结 baseline 发生变化，但 Pull Request 缺少 contract-change 标签')
    for section in REQUIRED_PULL_REQUEST_SECTIONS:
        if section not in pull_request_body:
            errors.append(f'冻结 baseline 变更说明缺少“{section}”')
    return errors


def _git_changed_paths(git_executable: str, base_sha: str) -> list[str]:
    # The executable is resolved by shutil.which and base_sha is a validated 40-hex commit.
    completed = subprocess.run(  # noqa: S603
        [git_executable, 'diff', '--name-only', f'{base_sha}...HEAD'],
        check=True,
        capture_output=True,
        text=True,
        encoding='utf-8',
    )
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


def _pull_request_labels(pull_request: dict[str, Any]) -> list[str]:
    return [str(label.get('name', '')) for label in pull_request.get('labels', []) if isinstance(label, dict)]


def main() -> int:
    event_path_text = os.environ.get('GITHUB_EVENT_PATH')
    if not event_path_text:
        print('baseline policy: non-GitHub execution, nothing to enforce')
        return 0

    event_path = Path(event_path_text)
    event = json.loads(event_path.read_text(encoding='utf-8'))
    pull_request = event.get('pull_request')
    if not isinstance(pull_request, dict):
        print('baseline policy: non-pull-request event, nothing to enforce')
        return 0

    base = pull_request.get('base')
    base_sha = base.get('sha') if isinstance(base, dict) else None
    if not isinstance(base_sha, str) or not re.fullmatch(r'[0-9a-f]{40}', base_sha):
        print('baseline policy: Pull Request event is missing base.sha', file=sys.stderr)
        return 2

    git_executable = shutil.which('git')
    if git_executable is None:
        print('baseline policy: git executable is unavailable', file=sys.stderr)
        return 2

    changed_paths = _git_changed_paths(git_executable, base_sha)
    baseline_changes = [path for path in changed_paths if path.replace('\\', '/').startswith(FROZEN_BASELINE_PREFIXES)]
    errors = evaluate_baseline_policy(
        changed_paths=changed_paths,
        labels=_pull_request_labels(pull_request),
        pull_request_body=str(pull_request.get('body') or ''),
    )
    if errors:
        print('baseline policy failed:', file=sys.stderr)
        for error in errors:
            print(f'- {error}', file=sys.stderr)
        return 1

    if baseline_changes:
        print('approved frozen baseline diff:')
        # Paths come from `git diff --name-only` and are passed without a shell.
        subprocess.run(  # noqa: S603
            [
                git_executable,
                'diff',
                '--no-ext-diff',
                f'{base_sha}...HEAD',
                '--',
                *baseline_changes,
            ],
            check=True,
        )
    else:
        print('baseline policy: no frozen baseline changes')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

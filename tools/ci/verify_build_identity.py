"""Verify the release binary carries an exact, deterministic build identity."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path

EXPECTED_KEYS = {
    'name',
    'version',
    'git_commit',
    'build_timestamp',
    'rustc_version',
    'target',
    'config_schema_version',
    'run_manifest_schema_version',
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--expected-commit', required=True)
    parser.add_argument('--expected-timestamp', required=True)
    return parser.parse_args()


def _command_output(arguments: list[str]) -> str:
    result = subprocess.run(  # noqa: S603
        arguments,
        check=True,
        capture_output=True,
        text=True,
        encoding='utf-8',
    )
    return result.stdout.strip()


def _rustc_host(verbose_version: str) -> str:
    for line in verbose_version.splitlines():
        if line.startswith('host: '):
            return line.removeprefix('host: ')
    raise RuntimeError('rustc -vV did not report a host target')


def verify_identity(
    payload: dict[str, object],
    *,
    expected_commit: str,
    expected_timestamp: str,
    rustc_version: str,
    rustc_host: str,
) -> None:
    if set(payload) != EXPECTED_KEYS:
        raise RuntimeError(f'build identity keys differ: {sorted(payload)}')
    if not re.fullmatch(r'[0-9a-f]{40}', expected_commit):
        raise RuntimeError('expected commit must be a full lowercase Git SHA')

    expected = {
        'name': 'costing-calculate',
        'version': '0.2.0',
        'git_commit': expected_commit,
        'build_timestamp': expected_timestamp,
        'rustc_version': rustc_version,
        'target': rustc_host,
        'config_schema_version': 1,
        'run_manifest_schema_version': 1,
    }
    if payload != expected:
        raise RuntimeError(f'build identity differs:\nexpected={expected}\nactual={payload}')


def main() -> int:
    args = _parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        raise FileNotFoundError(binary)

    payload = json.loads(_command_output([str(binary), '--version-json']))
    if not isinstance(payload, dict):
        raise RuntimeError('version-json must be a JSON object')
    rustc_version = _command_output(['rustc', '--version'])
    rustc_host = _rustc_host(_command_output(['rustc', '-vV']))
    verify_identity(
        payload,
        expected_commit=args.expected_commit,
        expected_timestamp=args.expected_timestamp,
        rustc_version=rustc_version,
        rustc_host=rustc_host,
    )
    print('deterministic build identity verified')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

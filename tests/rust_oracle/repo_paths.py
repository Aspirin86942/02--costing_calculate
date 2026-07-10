from __future__ import annotations

import os
from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def require_benchmark_sample(pipeline: str) -> Path:
    env_names = {'gb': 'COSTING_GB_SAMPLE', 'sk': 'COSTING_SK_SAMPLE'}
    try:
        env_name = env_names[pipeline]
    except KeyError as exc:
        raise AssertionError(f'unsupported benchmark pipeline: {pipeline!r}') from exc

    configured = os.environ.get(env_name)
    if configured:
        path = Path(configured).expanduser().resolve()
        if not path.is_file() or path.suffix.lower() != '.xlsx':
            raise AssertionError(f'{env_name} must point to an existing .xlsx file: {path}')
        return path

    raw_dir = repo_root() / 'data' / 'raw' / pipeline
    candidates = sorted(path.resolve() for path in raw_dir.glob(f'{pipeline}-*.xlsx') if path.is_file())
    if len(candidates) != 1:
        raise AssertionError(
            f'{pipeline} benchmark requires exactly one sample in {raw_dir}; '
            f'found {len(candidates)}. Set {env_name} explicitly.'
        )
    return candidates[0]

"""Generate public synthetic inputs before the isolated package smoke."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from tools.validation.synthetic_inputs import build_raw_fixture  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()

    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    for pipeline in ('gb', 'sk'):
        build_raw_fixture(output / f'{pipeline}-synthetic.xlsx', pipeline, 'small')
    print('synthetic GB/SK package-smoke inputs generated')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

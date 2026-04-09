from __future__ import annotations

import argparse

from src.config.pipelines import PIPELINES
from src.etl.runner import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='成本核算 ETL 统一入口')
    parser.add_argument('pipeline', choices=sorted(PIPELINES), help='选择要运行的管线: gb 或 sk')
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return run_pipeline(PIPELINES[args.pipeline])


if __name__ == '__main__':
    raise SystemExit(main())

from __future__ import annotations

import argparse

from src.config.pipelines import PIPELINES
from src.etl.month_filter import build_month_range
from src.etl.runner import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='成本核算 ETL 统一入口')
    parser.add_argument('pipeline', choices=sorted(PIPELINES), help='选择要运行的管线: gb 或 sk')
    parser.add_argument('--month-start', dest='month_start', help='起始月份，格式 YYYY-MM')
    parser.add_argument('--month-end', dest='month_end', help='结束月份，格式 YYYY-MM')
    parser.add_argument('--check-only', action='store_true', help='只执行预检和质量校验，不写出 workbook 或 CSV')
    parser.add_argument('--benchmark', action='store_true', help='输出稳定的阶段耗时和文件规模摘要')
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        month_range = build_month_range(args.month_start, args.month_end)
    except ValueError as exc:
        parser.error(str(exc))
    return run_pipeline(
        PIPELINES[args.pipeline],
        month_range=month_range,
        check_only=args.check_only,
        benchmark=args.benchmark,
    )


if __name__ == '__main__':
    raise SystemExit(main())

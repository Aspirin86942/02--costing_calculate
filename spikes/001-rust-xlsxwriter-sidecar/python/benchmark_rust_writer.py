from __future__ import annotations

import argparse
from pathlib import Path

from sidecar_benchmark import dump_summary_json, run_benchmark


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Benchmark Python 3-sheet workbook export vs Rust sidecar export.')
    parser.add_argument('pipeline', choices=('gb', 'sk'))
    parser.add_argument('--input', type=Path, help='Input workbook path. Defaults to the first configured raw file.')
    parser.add_argument('--tmp-dir', type=Path, required=True, help='Directory for benchmark outputs.')
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--month-start', help='Start month, format YYYY-MM.')
    parser.add_argument('--month-end', help='End month, format YYYY-MM.')
    parser.add_argument('--json-output', type=Path, help='Optional path to persist benchmark JSON summary.')
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = run_benchmark(
        pipeline_name=args.pipeline,
        input_path=args.input,
        tmp_dir=args.tmp_dir,
        repeats=args.repeats,
        month_start=args.month_start,
        month_end=args.month_end,
    )
    print(dump_summary_json(summary, args.json_output))
    return 0 if summary['validation_passed'] and summary['verdict'] == 'VALIDATED' else 1


if __name__ == '__main__':
    raise SystemExit(main())

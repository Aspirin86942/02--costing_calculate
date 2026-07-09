from __future__ import annotations

import argparse
import json
from pathlib import Path

from sidecar_benchmark import build_payload_for_input, resolve_input_path
from sidecar_payload import export_sheet_models_to_payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Export Costing SheetModel payload as CSV + JSON manifest for Rust.')
    parser.add_argument('pipeline', choices=('gb', 'sk'))
    parser.add_argument('--input', type=Path, help='Input workbook path. Defaults to the first configured raw file.')
    parser.add_argument(
        '--tmp-dir', type=Path, required=True, help='Directory for manifest and intermediate CSV files.'
    )
    parser.add_argument('--output', type=Path, required=True, help='Rust workbook output path to write into manifest.')
    parser.add_argument('--month-start', help='Start month, format YYYY-MM.')
    parser.add_argument('--month-end', help='End month, format YYYY-MM.')
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    input_path = resolve_input_path(args.pipeline, args.input)
    payload = build_payload_for_input(
        pipeline_name=args.pipeline,
        input_path=input_path,
        month_start=args.month_start,
        month_end=args.month_end,
    )
    result = export_sheet_models_to_payload(payload.sheet_models, args.tmp_dir, output_path=args.output)
    print(
        json.dumps(
            {
                'input_path': str(input_path),
                'manifest_path': str(result.manifest_path),
                'intermediate_export_seconds': result.intermediate_export_seconds,
                'csv_export_methods': [
                    {
                        'sheet_name': sheet['sheet_name'],
                        'csv_export_method': sheet['csv_export_method'],
                        'intermediate_export_seconds': sheet['intermediate_export_seconds'],
                        'row_count': sheet['row_count'],
                        'column_count': sheet['column_count'],
                    }
                    for sheet in result.manifest['sheets']
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

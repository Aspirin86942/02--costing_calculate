from __future__ import annotations

import argparse
import json
from pathlib import Path

from sidecar_validation import validate_workbooks


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Validate Rust workbook against Python workbook and sidecar manifest.')
    parser.add_argument('python_workbook', type=Path)
    parser.add_argument('rust_workbook', type=Path)
    parser.add_argument('--manifest', type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    manifest = json.loads(args.manifest.read_text(encoding='utf-8'))
    report = validate_workbooks(args.python_workbook, args.rust_workbook, manifest)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

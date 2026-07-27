"""Check local links in current Markdown documents without external dependencies."""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LINK_PATTERN = re.compile(r'!?\[[^\]]*]\((<[^>]+>|[^)\s]+)(?:\s+["\'][^)]*["\'])?\)')
SKIPPED_PARTS = frozenset({'.git', '.pytest-tmp', '.venv', 'target', 'superpowers'})


def find_broken_links(root: Path = PROJECT_ROOT) -> list[str]:
    broken: list[str] = []
    for document in sorted(root.rglob('*.md')):
        relative_document = document.relative_to(root)
        if any(part in SKIPPED_PARTS for part in relative_document.parts):
            continue
        text = document.read_text(encoding='utf-8')
        for match in LINK_PATTERN.finditer(text):
            raw_target = match.group(1).strip('<>')
            if raw_target.startswith(('#', 'http://', 'https://', 'mailto:')):
                continue
            path_part = unquote(raw_target.split('#', 1)[0])
            if not path_part:
                continue
            target = (document.parent / path_part).resolve()
            if not target.exists():
                broken.append(f'{relative_document.as_posix()} -> {raw_target}')
    return broken


def main() -> int:
    broken = find_broken_links()
    if broken:
        print('Broken local Markdown links:')
        for item in broken:
            print(f'- {item}')
        return 1
    print('local Markdown links passed')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

from __future__ import annotations

from pathlib import Path

from tools.validation.check_markdown_links import find_broken_links


def test_markdown_link_check_reports_only_missing_local_targets(tmp_path: Path) -> None:
    (tmp_path / 'present.md').write_text('# Present\n', encoding='utf-8')
    (tmp_path / 'README.md').write_text(
        '\n'.join(
            (
                '[present](present.md#heading)',
                '[missing](missing.md)',
                '[web](https://example.com)',
                '[anchor](#local)',
            )
        ),
        encoding='utf-8',
    )

    assert find_broken_links(tmp_path) == ['README.md -> missing.md']

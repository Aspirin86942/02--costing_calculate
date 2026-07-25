from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BUILD_SCRIPT = PROJECT_ROOT / 'rust' / 'crates' / 'costing-cli' / 'build.rs'


def test_build_identity_never_silently_falls_back_to_unknown_or_epoch_zero() -> None:
    build_script = BUILD_SCRIPT.read_text(encoding='utf-8')

    assert '"unknown"' not in build_script
    assert '.unwrap_or(0)' not in build_script
    assert 'COSTING_GIT_COMMIT' in build_script
    assert 'SOURCE_DATE_EPOCH' in build_script

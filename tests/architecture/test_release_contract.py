from __future__ import annotations

import re
import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RELEASE_WORKFLOW = PROJECT_ROOT / '.github' / 'workflows' / 'release.yml'
PACKAGE_SCRIPT = PROJECT_ROOT / 'tools' / 'release' / 'package_windows.ps1'
SMOKE_SCRIPT = PROJECT_ROOT / 'tools' / 'release' / 'smoke_package_windows.ps1'
RELEASE_ASSETS = (
    PROJECT_ROOT / 'tools' / 'release' / 'README.md',
    PROJECT_ROOT / 'tools' / 'release' / 'run-examples.txt',
    PROJECT_ROOT / 'CHANGELOG.md',
)
RELEASE_NOTES = (
    PROJECT_ROOT / 'docs' / 'releases' / 'v0.2.0-rc.1.md',
    PROJECT_ROOT / 'docs' / 'releases' / 'v0.2.0.md',
)


def test_root_toolchain_is_exact_and_applies_to_repository_root_commands() -> None:
    root_toolchain = PROJECT_ROOT / 'rust-toolchain.toml'

    assert root_toolchain.is_file()
    assert not (PROJECT_ROOT / 'rust' / 'rust-toolchain.toml').exists()
    document = tomllib.loads(root_toolchain.read_text(encoding='utf-8'))
    assert document == {
        'toolchain': {
            'channel': '1.96.0',
            'profile': 'minimal',
            'components': ['rustfmt', 'clippy'],
        }
    }


def test_release_workflow_is_pinned_and_calls_ci_before_packaging() -> None:
    workflow = RELEASE_WORKFLOW.read_text(encoding='utf-8')

    assert 'push:' in workflow
    assert 'tags:' in workflow
    assert 'workflow_dispatch:' in workflow
    assert 'uses: ./.github/workflows/ci.yml' in workflow
    assert 'needs: quality' in workflow
    assert "ref: ${{ github.event_name == 'push' && github.ref || inputs.release_label }}" in workflow
    assert 'git rev-parse "$env:RELEASE_LABEL^{commit}"' in workflow
    assert '$tagCommit -ne $commit' in workflow
    assert 'tools/release/package_windows.ps1' in workflow
    assert 'tools/release/smoke_package_windows.ps1' in workflow
    assert 'gh release create' in workflow
    assert 'contents: write' in workflow
    assert 'SOURCE_DATE_EPOCH' in workflow
    assert 'COSTING_GIT_COMMIT' in workflow
    assert '$notes = "docs/releases/$($env:RELEASE_LABEL).md"' in workflow
    assert all(notes.is_file() for notes in RELEASE_NOTES)

    action_refs = re.findall(r'uses:\s+[^@\s]+@([^\s#]+)', workflow)
    assert action_refs
    assert all(re.fullmatch(r'[0-9a-f]{40}', ref) for ref in action_refs)


def test_packaging_scripts_and_release_assets_cover_the_frozen_layout() -> None:
    package_script = PACKAGE_SCRIPT.read_text(encoding='utf-8')
    smoke_script = SMOKE_SCRIPT.read_text(encoding='utf-8')

    for asset in RELEASE_ASSETS:
        assert asset.is_file()
    for required_path in (
        'costing-calculate.exe',
        'README.md',
        'CHANGELOG.md',
        'config/costing.default.toml',
        'config/costing.schema.json',
        'schemas/run-manifest-v1.schema.json',
        'examples/run-examples.txt',
        'SHA256SUMS',
    ):
        assert required_path in package_script
        assert required_path in smoke_script
    assert 'cargo build --release --locked' in package_script
    assert 'SOURCE_DATE_EPOCH' in package_script
    assert 'COSTING_GIT_COMMIT' in package_script
    assert '--version-json' in smoke_script
    assert '--check-only' in smoke_script
    assert '--summary-output' in smoke_script
    assert '$normalManifest.result.final_output_valid -ne $true' in smoke_script
    assert '$normalManifest.result.output_sha256' in smoke_script
    assert 'Environment.Clear()' in smoke_script

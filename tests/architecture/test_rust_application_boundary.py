from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUST_ROOT = PROJECT_ROOT / 'rust'
CLI_SRC = RUST_ROOT / 'crates' / 'costing-cli' / 'src'
CORE_ROOT = RUST_ROOT / 'crates' / 'costing-core'


def test_cli_main_only_parses_renders_and_maps_exit_status() -> None:
    main = (CLI_SRC / 'main.rs').read_text(encoding='utf-8')

    assert 'application::{execute, RunOutcome, RunRequest}' in main
    for forbidden in (
        'read_raw_workbook',
        'normalize_workbook',
        'split_detail_and_qty',
        'build_fact_bundle',
        'build_workbook_payload',
        'write_workbook',
    ):
        assert forbidden not in main


def test_costing_core_has_no_cli_config_environment_or_path_discovery_dependency() -> None:
    cargo_toml = (CORE_ROOT / 'Cargo.toml').read_text(encoding='utf-8')
    rust_sources = '\n'.join(path.read_text(encoding='utf-8') for path in (CORE_ROOT / 'src').glob('*.rs'))

    assert 'costing-cli' not in cargo_toml
    assert 'clap' not in cargo_toml
    assert 'toml' not in cargo_toml
    assert 'std::env' not in rust_sources
    assert 'Command::parse' not in rust_sources


def test_cli_uses_the_deep_core_interface_instead_of_pipeline_internals() -> None:
    run_source = (CLI_SRC / 'run.rs').read_text(encoding='utf-8')
    core_lib = (CORE_ROOT / 'src' / 'lib.rs').read_text(encoding='utf-8')

    assert 'process_workbook' in run_source
    for internal_call in (
        'costing_core::normalize',
        'costing_core::split',
        'costing_core::fact',
        'costing_core::anomaly',
        'costing_core::presentation',
    ):
        assert internal_call not in run_source
    for private_module in ('anomaly', 'fact', 'normalize', 'presentation', 'quality', 'scoring', 'split'):
        assert f'mod {private_module};' in core_lib
        assert f'pub mod {private_module};' not in core_lib

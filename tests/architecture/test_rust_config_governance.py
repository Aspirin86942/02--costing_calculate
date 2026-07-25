from __future__ import annotations

import json
import tomllib
from pathlib import Path

from src.config.pipelines import GB_PRODUCT_ORDER, SK_PRODUCT_ORDER

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUST_ROOT = PROJECT_ROOT / 'rust'
CORE_ROOT = RUST_ROOT / 'crates' / 'costing-core'
CLI_ROOT = RUST_ROOT / 'crates' / 'costing-cli'
DEFAULT_CONFIG = CLI_ROOT / 'config' / 'costing.default.toml'
CONFIG_SCHEMA = CLI_ROOT / 'config' / 'costing.schema.json'


def test_default_rust_config_matches_the_python_oracle_product_order() -> None:
    config = tomllib.loads(DEFAULT_CONFIG.read_text(encoding='utf-8'))

    assert config['schema_version'] == 1
    assert [(item['code'], item['name']) for item in config['pipelines']['gb']['product_order']] == list(
        GB_PRODUCT_ORDER
    )
    assert [(item['code'], item['name']) for item in config['pipelines']['sk']['product_order']] == list(
        SK_PRODUCT_ORDER
    )
    assert config['pipelines']['gb']['standalone_cost_items'] == ['委外加工费']
    assert config['pipelines']['sk']['standalone_cost_items'] == ['委外加工费', '软件费用']


def test_config_schema_is_closed_and_requires_both_pipelines() -> None:
    schema = json.loads(CONFIG_SCHEMA.read_text(encoding='utf-8'))

    assert schema['$schema'] == 'https://json-schema.org/draft/2020-12/schema'
    assert schema['additionalProperties'] is False
    assert schema['required'] == ['schema_version', 'pipelines']
    pipelines = schema['properties']['pipelines']
    assert pipelines['additionalProperties'] is False
    assert pipelines['required'] == ['gb', 'sk']
    assert schema['$defs']['pipeline']['additionalProperties'] is False
    assert schema['$defs']['product']['additionalProperties'] is False


def test_core_receives_owned_rules_without_parsing_or_environment_access() -> None:
    core_manifest = (CORE_ROOT / 'Cargo.toml').read_text(encoding='utf-8')
    core_source = '\n'.join(path.read_text(encoding='utf-8') for path in (CORE_ROOT / 'src').glob('*.rs'))

    assert 'toml' not in core_manifest
    assert 'sha2' not in core_manifest
    for forbidden in ('std::fs', 'std::env', '.toml', 'CliArgs'):
        assert forbidden not in core_source
    assert 'PipelineRules' in core_source

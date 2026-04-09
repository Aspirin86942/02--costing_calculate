from __future__ import annotations

from src.config.pipelines import GB_PIPELINE, PIPELINES, SK_PIPELINE


def test_pipeline_registry_exports_expected_entries() -> None:
    assert set(PIPELINES) == {'gb', 'sk'}
    assert PIPELINES['gb'] is GB_PIPELINE
    assert PIPELINES['sk'] is SK_PIPELINE


def test_pipeline_directories_follow_standard_names() -> None:
    assert GB_PIPELINE.raw_dir.name == 'gb'
    assert GB_PIPELINE.processed_dir.name == 'gb'
    assert SK_PIPELINE.raw_dir.name == 'sk'
    assert SK_PIPELINE.processed_dir.name == 'sk'


def test_pipeline_input_patterns_are_defined_per_target() -> None:
    assert GB_PIPELINE.input_patterns == (
        'GB-*成本计算单.xlsx',
        'GB-* 成本计算单.xlsx',
        'GB-*.xlsx',
    )
    assert SK_PIPELINE.input_patterns == (
        'SK-*成本计算单.xlsx',
        'SK-* 成本计算单.xlsx',
        'SK-*.xlsx',
    )


def test_sk_pipeline_product_order_preserves_business_sequence() -> None:
    expected_codes = [
        'DP.C.P0197AA',
        'DP.C.P0201AA',
        'DP.C.P0198AA',
        'DP.C.P0199AA',
        'DP.C.P0257AA',
        'DP.C.P0200AA',
        'DP.C.P0246AA',
        'DP.C.P0252AA',
    ]
    assert [code for code, _ in SK_PIPELINE.product_order] == expected_codes
    assert SK_PIPELINE.product_whitelist == set(SK_PIPELINE.product_order)

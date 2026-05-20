from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.config.pipelines import GB_PIPELINE, SK_PIPELINE, ProductOrder
from src.config.product_whitelist_store import (
    ProductWhitelistConfigError,
    ProductWhitelistStore,
    load_product_order_for_pipeline,
)


def test_missing_config_file_returns_builtin_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / 'missing' / 'product_whitelists.json'

    result = ProductWhitelistStore(config_path).load()

    assert result.exists is False
    assert result.product_orders == {
        'gb': GB_PIPELINE.product_order,
        'sk': SK_PIPELINE.product_order,
    }


def test_save_and_load_round_trips_product_orders_and_writes_json(tmp_path: Path) -> None:
    config_path = tmp_path / 'config' / 'product_whitelists.json'
    product_orders: dict[str, ProductOrder] = {
        'gb': (('GB-001', '产品甲'),),
        'sk': (('SK-001', '产品丙'),),
    }

    store = ProductWhitelistStore(config_path)
    store.save(product_orders)
    result = store.load()
    saved_text = config_path.read_text(encoding='utf-8')

    assert result.exists is True
    assert result.product_orders == product_orders
    assert json.loads(saved_text) == {
        'gb': [{'product_code': 'GB-001', 'product_name': '产品甲'}],
        'sk': [{'product_code': 'SK-001', 'product_name': '产品丙'}],
    }
    assert '\n  "gb": [' in saved_text
    assert '\n      "product_code": "GB-001",' in saved_text
    assert '产品甲' in saved_text
    assert '\\u4ea7' not in saved_text


def test_existing_config_missing_pipeline_key_returns_empty_tuple(tmp_path: Path) -> None:
    config_path = tmp_path / 'product_whitelists.json'
    config_path.write_text(
        json.dumps({'gb': [{'product_code': 'GB-001', 'product_name': '产品甲'}]}, ensure_ascii=False),
        encoding='utf-8',
    )

    result = ProductWhitelistStore(config_path).load()

    assert result.exists is True
    assert result.product_orders['gb'] == (('GB-001', '产品甲'),)
    assert result.product_orders['sk'] == ()


def test_save_rejects_unknown_pipeline_key(tmp_path: Path) -> None:
    config_path = tmp_path / 'product_whitelists.json'

    with pytest.raises(ProductWhitelistConfigError, match='未知管线'):
        ProductWhitelistStore(config_path).save({'erp': (('ERP-001', '产品甲'),)})


def test_load_rejects_unknown_pipeline_key(tmp_path: Path) -> None:
    config_path = tmp_path / 'product_whitelists.json'
    config_path.write_text(json.dumps({'erp': []}, ensure_ascii=False), encoding='utf-8')

    with pytest.raises(ProductWhitelistConfigError, match='未知管线'):
        ProductWhitelistStore(config_path).load()


def test_loading_non_object_payload_raises_chinese_error(tmp_path: Path) -> None:
    config_path = tmp_path / 'product_whitelists.json'
    config_path.write_text('[]', encoding='utf-8')

    with pytest.raises(ProductWhitelistConfigError, match='JSON 对象'):
        ProductWhitelistStore(config_path).load()


def test_save_normalizes_two_item_tuple_and_list_items(tmp_path: Path) -> None:
    config_path = tmp_path / 'product_whitelists.json'
    store = ProductWhitelistStore(config_path)

    store.save(
        {
            'gb': [(' GB-001 ', ' 产品甲 ')],
            'sk': [[' SK-001 ', ' 产品丙 ']],
        }
    )
    result = store.load()

    assert result.product_orders == {
        'gb': (('GB-001', '产品甲'),),
        'sk': (('SK-001', '产品丙'),),
    }
    assert json.loads(config_path.read_text(encoding='utf-8')) == {
        'gb': [{'product_code': 'GB-001', 'product_name': '产品甲'}],
        'sk': [{'product_code': 'SK-001', 'product_name': '产品丙'}],
    }


def test_loading_duplicate_product_pairs_raises_chinese_error(tmp_path: Path) -> None:
    config_path = tmp_path / 'product_whitelists.json'
    config_path.write_text(
        json.dumps(
            {
                'gb': [
                    {'product_code': 'GB-001', 'product_name': '产品甲'},
                    {'product_code': 'GB-001', 'product_name': '产品甲'},
                ],
            },
            ensure_ascii=False,
        ),
        encoding='utf-8',
    )

    with pytest.raises(ProductWhitelistConfigError, match='重复'):
        ProductWhitelistStore(config_path).load()


@pytest.mark.parametrize(
    'item',
    [
        {'product_code': ' ', 'product_name': '产品甲'},
        {'product_code': 'GB-001', 'product_name': ' '},
    ],
)
def test_loading_blank_product_fields_raises_chinese_error(tmp_path: Path, item: dict[str, str]) -> None:
    config_path = tmp_path / 'product_whitelists.json'
    config_path.write_text(json.dumps({'gb': [item]}, ensure_ascii=False), encoding='utf-8')

    with pytest.raises(ProductWhitelistConfigError, match='不能为空'):
        ProductWhitelistStore(config_path).load()


def test_restore_default_replaces_only_requested_pipeline(tmp_path: Path) -> None:
    config_path = tmp_path / 'product_whitelists.json'
    custom_sk: ProductOrder = (('SK-001', '产品丙'),)
    store = ProductWhitelistStore(config_path)
    store.save(
        {
            'gb': (('GB-001', '产品甲'),),
            'sk': custom_sk,
        }
    )

    store.restore_default('gb')
    result = store.load()

    assert result.exists is True
    assert result.product_orders['gb'] == GB_PIPELINE.product_order
    assert result.product_orders['sk'] == custom_sk


def test_load_product_order_for_pipeline_reads_configured_pipeline(tmp_path: Path) -> None:
    config_path = tmp_path / 'product_whitelists.json'
    expected: ProductOrder = (('SK-001', '产品丙'),)
    ProductWhitelistStore(config_path).save({'sk': expected})

    assert load_product_order_for_pipeline('sk', config_path) == expected

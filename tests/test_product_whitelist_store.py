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


def test_existing_config_missing_pipeline_key_returns_builtin_default(tmp_path: Path) -> None:
    config_path = tmp_path / 'product_whitelists.json'
    config_path.write_text(
        json.dumps({'gb': [{'product_code': 'GB-001', 'product_name': '产品甲'}]}, ensure_ascii=False),
        encoding='utf-8',
    )

    result = ProductWhitelistStore(config_path).load()

    assert result.exists is True
    assert result.product_orders['gb'] == (('GB-001', '产品甲'),)
    assert result.product_orders['sk'] == SK_PIPELINE.product_order


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


def test_loading_invalid_json_reports_line_and_column(tmp_path: Path) -> None:
    config_path = tmp_path / 'product_whitelists.json'
    config_path.write_text('{"gb": [', encoding='utf-8')

    with pytest.raises(ProductWhitelistConfigError) as exc_info:
        ProductWhitelistStore(config_path).load()

    message = str(exc_info.value)
    assert '行' in message
    assert '列' in message


def test_loading_non_utf8_bytes_raises_chinese_error(tmp_path: Path) -> None:
    config_path = tmp_path / 'product_whitelists.json'
    config_path.write_bytes(b'\xff\xfe\x00')

    with pytest.raises(ProductWhitelistConfigError, match='UTF-8'):
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


def test_partial_save_preserves_unmentioned_existing_pipeline(tmp_path: Path) -> None:
    config_path = tmp_path / 'product_whitelists.json'
    store = ProductWhitelistStore(config_path)
    old_gb: ProductOrder = (('GB-OLD', '产品甲'),)
    old_sk: ProductOrder = (('SK-OLD', '产品乙'),)
    new_sk: ProductOrder = (('SK-NEW', '产品丙'),)
    store.save({'gb': old_gb, 'sk': old_sk})

    store.save({'sk': new_sk})
    result = store.load()

    assert result.product_orders['gb'] == old_gb
    assert result.product_orders['sk'] == new_sk


def test_partial_save_uses_defaults_for_missing_file_base(tmp_path: Path) -> None:
    config_path = tmp_path / 'product_whitelists.json'
    custom_sk: ProductOrder = (('SK-NEW', '产品丙'),)

    ProductWhitelistStore(config_path).save({'sk': custom_sk})
    result = ProductWhitelistStore(config_path).load()

    assert result.product_orders['gb'] == GB_PIPELINE.product_order
    assert result.product_orders['sk'] == custom_sk


def test_save_overwrites_invalid_json_using_defaults_for_unmentioned_pipelines(tmp_path: Path) -> None:
    config_path = tmp_path / 'product_whitelists.json'
    config_path.write_text('{"gb": [', encoding='utf-8')
    custom_gb: ProductOrder = (('GB-NEW', '产品甲'),)

    ProductWhitelistStore(config_path).save({'gb': custom_gb})
    result = ProductWhitelistStore(config_path).load()

    assert result.product_orders['gb'] == custom_gb
    assert result.product_orders['sk'] == SK_PIPELINE.product_order
    assert json.loads(config_path.read_text(encoding='utf-8'))['gb'] == [
        {'product_code': 'GB-NEW', 'product_name': '产品甲'}
    ]


def test_save_overwrites_non_utf8_config_using_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / 'product_whitelists.json'
    config_path.write_bytes(b'\xff\xfe\x00')
    custom_gb: ProductOrder = (('GB-NEW', '产品甲'),)

    ProductWhitelistStore(config_path).save({'gb': custom_gb})
    result = ProductWhitelistStore(config_path).load()

    assert result.product_orders['gb'] == custom_gb
    assert result.product_orders['sk'] == SK_PIPELINE.product_order


def test_restore_default_overwrites_invalid_json(tmp_path: Path) -> None:
    config_path = tmp_path / 'product_whitelists.json'
    config_path.write_text('{"gb": [', encoding='utf-8')

    ProductWhitelistStore(config_path).restore_default('gb')
    result = ProductWhitelistStore(config_path).load()

    assert result.product_orders['gb'] == GB_PIPELINE.product_order
    assert result.product_orders['sk'] == SK_PIPELINE.product_order


def test_save_replaces_atomically_and_keeps_original_when_replace_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / 'product_whitelists.json'
    original_payload = {
        'gb': [{'product_code': 'GB-OLD', 'product_name': '产品甲'}],
        'sk': [{'product_code': 'SK-OLD', 'product_name': '产品乙'}],
    }
    config_path.write_text(json.dumps(original_payload, ensure_ascii=False, indent=2), encoding='utf-8')
    replace_calls: list[tuple[Path, Path]] = []

    def fail_replace(self: Path, target: Path) -> Path:
        replace_calls.append((self, target))
        raise OSError('replace failed')

    monkeypatch.setattr(type(config_path), 'replace', fail_replace)

    with pytest.raises(OSError, match='replace failed'):
        ProductWhitelistStore(config_path).save({'sk': (('SK-NEW', '产品丙'),)})

    assert json.loads(config_path.read_text(encoding='utf-8')) == original_payload
    assert len(replace_calls) == 1
    temp_path, target_path = replace_calls[0]
    assert target_path == config_path
    assert temp_path.parent == config_path.parent
    assert not temp_path.exists()


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


def test_restore_default_rejects_unknown_pipeline(tmp_path: Path) -> None:
    config_path = tmp_path / 'product_whitelists.json'

    with pytest.raises(ProductWhitelistConfigError, match='未知管线'):
        ProductWhitelistStore(config_path).restore_default('unknown')


def test_load_product_order_for_pipeline_reads_configured_pipeline(tmp_path: Path) -> None:
    config_path = tmp_path / 'product_whitelists.json'
    expected: ProductOrder = (('SK-001', '产品丙'),)
    ProductWhitelistStore(config_path).save({'sk': expected})

    assert load_product_order_for_pipeline('sk', config_path) == expected


def test_load_product_order_for_pipeline_rejects_unknown_pipeline(tmp_path: Path) -> None:
    config_path = tmp_path / 'product_whitelists.json'

    with pytest.raises(ProductWhitelistConfigError, match='未知管线'):
        load_product_order_for_pipeline('unknown', config_path)

"""Shared product whitelist JSON store."""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.config.pipelines import PIPELINES, ProductOrder
from src.config.settings import PROJECT_ROOT

DEFAULT_PRODUCT_WHITELIST_PATH = PROJECT_ROOT / 'config' / 'product_whitelists.json'


class ProductWhitelistConfigError(ValueError):
    """产品白名单配置错误。"""


@dataclass(frozen=True)
class ProductWhitelistStoreResult:
    exists: bool
    product_orders: dict[str, ProductOrder]


class ProductWhitelistStore:
    def __init__(self, path: Path = DEFAULT_PRODUCT_WHITELIST_PATH):
        self.path = Path(path)

    def load(self) -> ProductWhitelistStoreResult:
        if not self.path.exists():
            return ProductWhitelistStoreResult(exists=False, product_orders=_default_product_orders())

        payload = _read_json_object(self.path)
        product_orders = _normalize_payload(payload)
        return ProductWhitelistStoreResult(exists=True, product_orders=product_orders)

    def save(self, product_orders: dict[str, ProductOrder]) -> None:
        normalized_updates = _normalize_partial_payload(product_orders)
        merged_product_orders = self._load_product_orders_or_default()
        merged_product_orders.update(normalized_updates)

        self.path.parent.mkdir(parents=True, exist_ok=True)
        _write_json_atomically(self.path, _to_json_payload(merged_product_orders))

    def restore_default(self, pipeline_name: str) -> None:
        normalized_pipeline_name = _require_known_pipeline(pipeline_name)
        product_orders = self._load_product_orders_or_default()
        product_orders[normalized_pipeline_name] = PIPELINES[normalized_pipeline_name].product_order
        self.path.parent.mkdir(parents=True, exist_ok=True)
        _write_json_atomically(self.path, _to_json_payload(product_orders))

    def _load_product_orders_or_default(self) -> dict[str, ProductOrder]:
        try:
            return self.load().product_orders
        except ProductWhitelistConfigError:
            # 白名单配置是可编辑的本地偏好；保存/恢复默认时允许覆盖损坏文件，
            # 避免用户被一个坏 JSON 永久卡住。
            return _default_product_orders()


def load_product_order_for_pipeline(
    pipeline_name: str,
    path: Path = DEFAULT_PRODUCT_WHITELIST_PATH,
) -> ProductOrder:
    normalized_pipeline_name = _require_known_pipeline(pipeline_name)
    return ProductWhitelistStore(path).load().product_orders[normalized_pipeline_name]


def _default_product_orders() -> dict[str, ProductOrder]:
    return {pipeline_name: pipeline.product_order for pipeline_name, pipeline in PIPELINES.items()}


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except json.JSONDecodeError as error:
        raise ProductWhitelistConfigError(
            f'产品白名单配置不是有效 JSON: {error.msg} (行 {error.lineno}, 列 {error.colno})'
        ) from error

    if not isinstance(payload, dict):
        raise ProductWhitelistConfigError('产品白名单配置必须是 JSON 对象')
    return payload


def _normalize_payload(payload: dict[str, Any]) -> dict[str, ProductOrder]:
    if not isinstance(payload, dict):
        raise ProductWhitelistConfigError('产品白名单配置必须是 JSON 对象')

    unknown_pipeline_names = sorted(set(payload) - set(PIPELINES))
    if unknown_pipeline_names:
        raise ProductWhitelistConfigError(f'产品白名单配置包含未知管线: {", ".join(unknown_pipeline_names)}')

    product_orders = _default_product_orders()
    for pipeline_name, raw_items in payload.items():
        product_orders[pipeline_name] = _normalize_product_order(pipeline_name, raw_items)
    return product_orders


def _normalize_partial_payload(payload: dict[str, Any]) -> dict[str, ProductOrder]:
    if not isinstance(payload, dict):
        raise ProductWhitelistConfigError('产品白名单配置必须是 JSON 对象')

    unknown_pipeline_names = sorted(set(payload) - set(PIPELINES))
    if unknown_pipeline_names:
        raise ProductWhitelistConfigError(f'产品白名单配置包含未知管线: {", ".join(unknown_pipeline_names)}')

    return {
        pipeline_name: _normalize_product_order(pipeline_name, raw_items)
        for pipeline_name, raw_items in payload.items()
    }


def _normalize_product_order(pipeline_name: str, raw_items: Any) -> ProductOrder:
    if not isinstance(raw_items, list | tuple):
        raise ProductWhitelistConfigError(f'{pipeline_name} 产品白名单必须是列表')

    normalized_items: list[tuple[str, str]] = []
    seen_pairs: set[tuple[str, str]] = set()
    for item_index, raw_item in enumerate(raw_items, start=1):
        product_code, product_name = _normalize_product_item(pipeline_name, item_index, raw_item)
        pair = (product_code, product_name)
        if pair in seen_pairs:
            message = f'{pipeline_name} 产品白名单存在重复产品: {product_code} / {product_name}'
            raise ProductWhitelistConfigError(message)
        seen_pairs.add(pair)
        normalized_items.append(pair)

    return tuple(normalized_items)


def _normalize_product_item(pipeline_name: str, item_index: int, raw_item: Any) -> tuple[str, str]:
    if isinstance(raw_item, dict):
        product_code = raw_item.get('product_code')
        product_name = raw_item.get('product_name')
    elif isinstance(raw_item, list | tuple) and len(raw_item) == 2:
        product_code, product_name = raw_item
    else:
        raise ProductWhitelistConfigError(f'{pipeline_name} 产品白名单第 {item_index} 项格式错误')

    normalized_product_code = str(product_code if product_code is not None else '').strip()
    normalized_product_name = str(product_name if product_name is not None else '').strip()
    if not normalized_product_code or not normalized_product_name:
        raise ProductWhitelistConfigError(f'{pipeline_name} 产品白名单第 {item_index} 项产品编码和产品名称不能为空')

    return normalized_product_code, normalized_product_name


def _require_known_pipeline(pipeline_name: str) -> str:
    if pipeline_name not in PIPELINES:
        raise ProductWhitelistConfigError(f'未知管线: {pipeline_name}')
    return pipeline_name


def _to_json_payload(product_orders: dict[str, ProductOrder]) -> dict[str, list[dict[str, str]]]:
    return {
        pipeline_name: [
            {'product_code': product_code, 'product_name': product_name}
            for product_code, product_name in product_orders[pipeline_name]
        ]
        for pipeline_name in PIPELINES
    }


def _write_json_atomically(path: Path, payload: dict[str, list[dict[str, str]]]) -> None:
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode='w',
            encoding='utf-8',
            dir=path.parent,
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)
            json.dump(payload, temp_file, ensure_ascii=False, indent=2)
            temp_file.write('\n')

        temp_path.replace(path)
    except Exception:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
        raise

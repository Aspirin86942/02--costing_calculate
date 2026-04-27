"""Pipeline registry and whitelist definitions."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.config.settings import GB_PROCESSED_DIR, GB_RAW_DIR, SK_PROCESSED_DIR, SK_RAW_DIR

ProductOrder = tuple[tuple[str, str], ...]
PRODUCT_ANOMALY_SCOPE_LEGACY = 'legacy_single_scope'
PRODUCT_ANOMALY_SCOPE_DOC_TYPE_SPLIT = 'doc_type_split'
VALID_PRODUCT_ANOMALY_SCOPE_MODES: tuple[str, ...] = (
    PRODUCT_ANOMALY_SCOPE_LEGACY,
    PRODUCT_ANOMALY_SCOPE_DOC_TYPE_SPLIT,
)


def normalize_product_anomaly_scope_mode(
    product_anomaly_scope_mode: str | None,
    *,
    default_mode: str | None = None,
) -> str:
    """标准化并校验 product_anomaly_scope_mode。"""
    mode = default_mode if product_anomaly_scope_mode is None else product_anomaly_scope_mode
    if mode is None:
        raise ValueError('product_anomaly_scope_mode 不能为空，允许值: legacy_single_scope, doc_type_split')
    normalized_mode = str(mode).strip()
    if not normalized_mode:
        raise ValueError('product_anomaly_scope_mode 不能为空，允许值: legacy_single_scope, doc_type_split')
    if normalized_mode not in VALID_PRODUCT_ANOMALY_SCOPE_MODES:
        raise ValueError(
            f'product_anomaly_scope_mode={normalized_mode!r} 非法，允许值: legacy_single_scope, doc_type_split'
        )
    return normalized_mode


@dataclass(frozen=True)
class PipelineConfig:
    name: str
    raw_dir: Path
    processed_dir: Path
    product_order: ProductOrder = ()
    input_patterns: tuple[str, ...] = ()
    standalone_cost_items: tuple[str, ...] = ('委外加工费',)
    product_anomaly_scope_mode: str = PRODUCT_ANOMALY_SCOPE_LEGACY

    def __post_init__(self) -> None:
        normalized_mode = normalize_product_anomaly_scope_mode(self.product_anomaly_scope_mode)
        object.__setattr__(self, 'product_anomaly_scope_mode', normalized_mode)

    @property
    def product_whitelist(self) -> frozenset[tuple[str, str]]:
        return frozenset(self.product_order)


GB_PRODUCT_ORDER: ProductOrder = (
    ('GB_C.D.B0048AA', 'BMS-400W驱动器'),
    ('GB_C.D.B0040AA', 'BMS-750W驱动器'),
    ('GB_C.D.B0041AA', 'BMS-1100W驱动器'),
    ('GB_C.D.B0042AA', 'BMS-1700W驱动器'),
    ('GB_C.D.B0043AA', 'BMS-2400W驱动器'),
    ('GB_C.D.B0044AA', 'BMS-3900W驱动器'),
    ('GB_C.D.B0045AA', 'BMS-5900W驱动器'),
    ('GB_C.D.B0046AA', 'BMS-7500W驱动器'),
)

SK_PRODUCT_ORDER: ProductOrder = (
    ('DP.C.P0197AA', '动力线'),
    ('DP.C.P0201AA', '动力线'),
    ('DP.C.P0198AA', '动力线'),
    ('DP.C.P0199AA', '动力线'),
    ('DP.C.P0257AA', '动力线'),
    ('DP.C.P0200AA', '动力线'),
    ('DP.C.P0246AA', '动力抱闸线'),
    ('DP.C.P0252AA', '动力线'),
)

GB_PIPELINE = PipelineConfig(
    name='gb',
    raw_dir=GB_RAW_DIR,
    processed_dir=GB_PROCESSED_DIR,
    product_order=GB_PRODUCT_ORDER,
    product_anomaly_scope_mode=PRODUCT_ANOMALY_SCOPE_DOC_TYPE_SPLIT,
    input_patterns=(
        'GB-*成本计算单.xlsx',
        'GB-* 成本计算单.xlsx',
        'GB-*.xlsx',
    ),
)

SK_PIPELINE = PipelineConfig(
    name='sk',
    raw_dir=SK_RAW_DIR,
    processed_dir=SK_PROCESSED_DIR,
    product_order=SK_PRODUCT_ORDER,
    product_anomaly_scope_mode=PRODUCT_ANOMALY_SCOPE_LEGACY,
    input_patterns=(
        'SK-*成本计算单.xlsx',
        'SK-* 成本计算单.xlsx',
        'SK-*.xlsx',
    ),
    standalone_cost_items=('委外加工费', '软件费用'),
)

PIPELINES: dict[str, PipelineConfig] = {
    GB_PIPELINE.name: GB_PIPELINE,
    SK_PIPELINE.name: SK_PIPELINE,
}

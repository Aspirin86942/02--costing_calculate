"""Pipeline registry and whitelist definitions."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.config.settings import GB_PROCESSED_DIR, GB_RAW_DIR, SK_PROCESSED_DIR, SK_RAW_DIR

ProductOrder = tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class PipelineConfig:
    name: str
    raw_dir: Path
    processed_dir: Path
    product_order: ProductOrder = ()
    input_patterns: tuple[str, ...] = ()

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
    input_patterns=(
        'SK-*成本计算单.xlsx',
        'SK-* 成本计算单.xlsx',
        'SK-*.xlsx',
    ),
)

PIPELINES: dict[str, PipelineConfig] = {
    GB_PIPELINE.name: GB_PIPELINE,
    SK_PIPELINE.name: SK_PIPELINE,
}

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.config.pipelines import ProductOrder
from src.services.costing_service import CostingRunRequest


@dataclass(frozen=True)
class GuiFormState:
    pipeline: str
    input_path: Path | None
    output_dir: Path
    product_order: ProductOrder
    month_start: str | None = None
    month_end: str | None = None
    overwrite_confirmed: bool = False
    benchmark: bool = True

    def to_request(self) -> CostingRunRequest:
        if self.input_path is None:
            raise ValueError('缺少输入文件')
        return CostingRunRequest(
            pipeline=self.pipeline,
            input_path=self.input_path,
            output_dir=self.output_dir,
            month_start=_blank_to_none(self.month_start),
            month_end=_blank_to_none(self.month_end),
            product_order=self.product_order,
            benchmark=self.benchmark,
            overwrite_confirmed=self.overwrite_confirmed,
        )


def _blank_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None

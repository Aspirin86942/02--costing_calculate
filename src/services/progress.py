from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProgressEvent:
    percent: int
    stage: str
    message: str


ProgressCallback = Callable[[ProgressEvent], None]


def report_progress(
    callback: ProgressCallback | None,
    percent: int,
    stage: str,
    message: str,
) -> None:
    """上报进度但不允许 UI 观察能力中断 ETL 主流程。"""
    if callback is None:
        return

    try:
        callback(ProgressEvent(percent=percent, stage=stage, message=message))
    except Exception:  # noqa: BLE001
        logger.warning('Progress callback failed', exc_info=True)

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip('PySide6')

from src.gui.task_worker import ServiceWorker  # noqa: E402
from src.services.costing_service import CostingRunRequest, CostingRunResult, ServiceStatus  # noqa: E402
from src.services.progress import ProgressEvent  # noqa: E402


def test_service_worker_emits_progress_signal(tmp_path: Path) -> None:
    request = CostingRunRequest(
        pipeline='gb',
        input_path=tmp_path / 'GB-成本计算单.xlsx',
        output_dir=tmp_path,
        product_order=(('P001', '产品A'),),
    )
    events: list[ProgressEvent] = []
    results: list[CostingRunResult] = []

    def _fake_service(
        _request: CostingRunRequest,
        *,
        progress_callback: object | None = None,
    ) -> CostingRunResult:
        assert progress_callback is not None
        progress_callback(ProgressEvent(percent=45, stage='fact', message='已拆分事实表'))
        return CostingRunResult(status=ServiceStatus.SUCCEEDED, message='ok')

    worker = ServiceWorker('测试任务', request, _fake_service)
    worker.signals.progress.connect(events.append)
    worker.signals.finished.connect(results.append)

    worker.run()

    assert [(event.percent, event.stage, event.message) for event in events] == [
        (45, 'fact', '已拆分事实表')
    ]
    assert results[0].message == 'ok'

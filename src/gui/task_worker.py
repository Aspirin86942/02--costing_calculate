from __future__ import annotations

from typing import Protocol

from PySide6.QtCore import QObject, QRunnable, Signal, Slot

from src.services.costing_service import CostingRunRequest, CostingRunResult
from src.services.progress import ProgressCallback, ProgressEvent


class CostingServiceFunction(Protocol):
    def __call__(
        self,
        request: CostingRunRequest,
        *,
        progress_callback: ProgressCallback | None = None,
    ) -> CostingRunResult:
        raise NotImplementedError


class WorkerSignals(QObject):
    started = Signal(str)
    progress = Signal(object)
    finished = Signal(object)
    failed = Signal(str)


class ServiceWorker(QRunnable):
    def __init__(
        self,
        label: str,
        request: CostingRunRequest,
        function: CostingServiceFunction,
    ) -> None:
        super().__init__()
        self.label = label
        self.request = request
        self.function = function
        self.signals = WorkerSignals()

    @Slot()
    def run(self) -> None:
        self.signals.started.emit(self.label)

        def emit_progress(event: ProgressEvent) -> None:
            self.signals.progress.emit(event)

        try:
            result = self.function(self.request, progress_callback=emit_progress)
        except Exception as exc:  # noqa: BLE001
            self.signals.failed.emit(str(exc))
            return
        self.signals.finished.emit(result)

from __future__ import annotations

from collections.abc import Callable

from PySide6.QtCore import QObject, QRunnable, Signal, Slot

from src.services.costing_service import CostingRunRequest, CostingRunResult


class WorkerSignals(QObject):
    started = Signal(str)
    finished = Signal(object)
    failed = Signal(str)


class ServiceWorker(QRunnable):
    def __init__(
        self,
        label: str,
        request: CostingRunRequest,
        function: Callable[[CostingRunRequest], CostingRunResult],
    ) -> None:
        super().__init__()
        self.label = label
        self.request = request
        self.function = function
        self.signals = WorkerSignals()

    @Slot()
    def run(self) -> None:
        self.signals.started.emit(self.label)
        try:
            result = self.function(self.request)
        except Exception as exc:  # noqa: BLE001
            self.signals.failed.emit(str(exc))
            return
        self.signals.finished.emit(result)

from __future__ import annotations

import os
from pathlib import Path

import pytest

os.environ.setdefault('QT_QPA_PLATFORM', 'offscreen')

pytest.importorskip('PySide6')

from PySide6.QtWidgets import QApplication, QMessageBox  # noqa: E402

from src.gui.main_window import MainWindow  # noqa: E402
from src.services.costing_service import (  # noqa: E402
    CostingRunResult,
    ServiceStatus,
    precheck_costing_run,
    run_costing_request,
)


@pytest.fixture()
def qt_app() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


@pytest.fixture()
def main_window(qt_app: QApplication) -> MainWindow:
    window = MainWindow()
    yield window
    window.close()


def test_run_first_checks_existing_output_then_confirmed_retry(
    main_window: MainWindow,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[dict[str, object]] = []

    def capture_start_worker(label: str, function: object, *, overwrite_confirmed: bool, task_kind: str) -> None:
        calls.append(
            {
                'label': label,
                'function': function,
                'overwrite_confirmed': overwrite_confirmed,
                'task_kind': task_kind,
            }
        )

    monkeypatch.setattr(main_window, '_start_worker', capture_start_worker)

    main_window._run()

    assert len(calls) == 1
    assert calls[0]['label'] == '正在处理'
    assert calls[0]['function'] is run_costing_request
    assert calls[0]['overwrite_confirmed'] is False
    assert calls[0]['task_kind'] == 'run'

    calls.clear()
    monkeypatch.setattr(
        QMessageBox,
        'question',
        lambda *_args, **_kwargs: QMessageBox.StandardButton.Yes,
    )
    result = CostingRunResult(
        status=ServiceStatus.FAILED,
        message='输出 workbook 已存在',
        workbook_path=tmp_path / '成本计算单_处理后.xlsx',
        error_code='OUTPUT_EXISTS',
    )

    main_window._on_worker_finished(result, task_kind='run')

    assert len(calls) == 1
    assert calls[0]['label'] == '正在处理'
    assert calls[0]['function'] is run_costing_request
    assert calls[0]['overwrite_confirmed'] is True
    assert calls[0]['task_kind'] == 'run'


def test_scan_and_precheck_keep_existing_overwrite_policy(
    main_window: MainWindow,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def capture_start_worker(label: str, function: object, *, overwrite_confirmed: bool, task_kind: str) -> None:
        calls.append(
            {
                'label': label,
                'function': function,
                'overwrite_confirmed': overwrite_confirmed,
                'task_kind': task_kind,
            }
        )

    monkeypatch.setattr(main_window, '_start_worker', capture_start_worker)

    main_window._scan_products()
    main_window._precheck()

    assert calls[0]['function'] is precheck_costing_run
    assert calls[0]['overwrite_confirmed'] is True
    assert calls[0]['task_kind'] == 'scan'
    assert calls[1]['function'] is precheck_costing_run
    assert calls[1]['overwrite_confirmed'] is False
    assert calls[1]['task_kind'] == 'precheck'

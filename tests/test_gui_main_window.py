from __future__ import annotations

import os
import types
from pathlib import Path

import pytest

os.environ.setdefault('QT_QPA_PLATFORM', 'offscreen')

pytest.importorskip('PySide6')

from PySide6.QtWidgets import QApplication, QMessageBox  # noqa: E402

import src.gui.main_window as main_window_module  # noqa: E402
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


def test_run_requires_successful_precheck_before_starting_worker(
    main_window: MainWindow,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    started_workers: list[object] = []
    input_path = tmp_path / 'GB-成本计算单.xlsx'
    input_path.write_bytes(b'raw')
    main_window.input_edit.setText(str(input_path))
    main_window.output_edit.setText(str(tmp_path))
    main_window.precheck_passed = False
    monkeypatch.setattr(main_window.thread_pool, 'start', started_workers.append)

    main_window._run()

    assert started_workers == []
    assert '预检' in main_window.log_edit.toPlainText()


def test_busy_window_does_not_start_second_worker(
    main_window: MainWindow,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    started_workers: list[object] = []
    input_path = tmp_path / 'GB-成本计算单.xlsx'
    input_path.write_bytes(b'raw')
    main_window.input_edit.setText(str(input_path))
    main_window.output_edit.setText(str(tmp_path))
    main_window.busy = True
    monkeypatch.setattr(main_window.thread_pool, 'start', started_workers.append)

    main_window._scan_products()

    assert started_workers == []
    assert '任务正在运行' in main_window.log_edit.toPlainText()


def test_existing_output_confirmation_no_does_not_retry(
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
    monkeypatch.setattr(
        QMessageBox,
        'question',
        lambda *_args, **_kwargs: QMessageBox.StandardButton.No,
    )
    result = CostingRunResult(
        status=ServiceStatus.FAILED,
        message='输出 workbook 已存在',
        workbook_path=tmp_path / '成本计算单_处理后.xlsx',
        error_code='OUTPUT_EXISTS',
    )

    main_window._on_worker_finished(result, task_kind='run')

    assert calls == []
    assert '用户取消覆盖' in main_window.log_edit.toPlainText()


def test_empty_candidate_result_clears_stale_candidate_table(main_window: MainWindow) -> None:
    main_window._set_table_pairs(main_window.candidate_table, (('P001', '产品A'),))
    result = CostingRunResult(
        status=ServiceStatus.SUCCEEDED,
        message='预检通过',
        candidate_products=(),
    )

    main_window._on_worker_finished(result, task_kind='precheck')

    assert main_window.candidate_table.rowCount() == 0


def test_form_changes_clear_stale_candidates(main_window: MainWindow) -> None:
    main_window._set_table_pairs(main_window.candidate_table, (('P001', '产品A'),))
    main_window.precheck_passed = True

    main_window.month_start_edit.setText('2025-01')

    assert main_window.precheck_passed is False
    assert main_window.candidate_table.rowCount() == 0


def test_save_whitelist_logs_oserror_without_raising(
    main_window: MainWindow,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_save(_product_orders: object) -> None:
        raise OSError('磁盘只读')

    monkeypatch.setattr(main_window.whitelist_store, 'save', fail_save)

    main_window._save_whitelist()

    assert '保存失败: 磁盘只读' in main_window.log_edit.toPlainText()


def test_restore_default_whitelist_logs_oserror_without_raising(
    main_window: MainWindow,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_restore_default(_pipeline: str) -> None:
        raise OSError('配置目录不可写')

    monkeypatch.setattr(
        QMessageBox,
        'question',
        lambda *_args, **_kwargs: QMessageBox.StandardButton.Yes,
    )
    monkeypatch.setattr(main_window.whitelist_store, 'restore_default', fail_restore_default)

    main_window._restore_default_whitelist()

    assert '恢复默认失败: 配置目录不可写' in main_window.log_edit.toPlainText()


def test_open_output_dir_logs_when_xdg_open_is_missing(
    main_window: MainWindow,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def fail_popen(_args: object) -> None:
        raise AssertionError('xdg-open should not be called when it is unavailable')

    main_window.output_edit.setText(str(tmp_path))
    monkeypatch.setattr(main_window_module, 'shutil', types.SimpleNamespace(which=lambda _name: None), raising=False)
    monkeypatch.setattr(main_window_module.subprocess, 'Popen', fail_popen)

    main_window._open_output_dir()

    assert '未找到 xdg-open，无法打开输出目录' in main_window.log_edit.toPlainText()


def test_open_output_dir_logs_popen_oserror(
    main_window: MainWindow,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def fail_popen(_args: object) -> None:
        raise OSError('无法启动')

    main_window.output_edit.setText(str(tmp_path))
    monkeypatch.setattr(
        main_window_module,
        'shutil',
        types.SimpleNamespace(which=lambda _name: '/usr/bin/xdg-open'),
        raising=False,
    )
    monkeypatch.setattr(main_window_module.subprocess, 'Popen', fail_popen)

    main_window._open_output_dir()

    assert '打开输出目录失败: 无法启动' in main_window.log_edit.toPlainText()

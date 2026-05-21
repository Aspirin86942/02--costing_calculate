from __future__ import annotations

import json
import os
import types
from pathlib import Path

import pytest

os.environ.setdefault('QT_QPA_PLATFORM', 'offscreen')

pytest.importorskip('PySide6')

from PySide6.QtWidgets import QApplication, QMessageBox  # noqa: E402

import src.gui.main_window as main_window_module  # noqa: E402
from src.config.pipelines import GB_PIPELINE  # noqa: E402
from src.config.product_whitelist_store import ProductWhitelistStore  # noqa: E402
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
    monkeypatch.setattr(main_window, '_ask_confirmation', lambda *_args, **_kwargs: True)
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
    monkeypatch.setattr(main_window, '_ask_confirmation', lambda *_args, **_kwargs: False)
    main_window.status_label.setText('正在处理')
    result = CostingRunResult(
        status=ServiceStatus.FAILED,
        message='输出 workbook 已存在',
        workbook_path=tmp_path / '成本计算单_处理后.xlsx',
        error_code='OUTPUT_EXISTS',
    )

    main_window._on_worker_finished(result, task_kind='run')

    assert calls == []
    assert '用户取消覆盖' in main_window.log_edit.toPlainText()
    assert main_window.status_label.text() == result.message


def test_empty_candidate_result_clears_stale_candidate_table(main_window: MainWindow) -> None:
    main_window._set_table_pairs(main_window.candidate_table, (('P001', '产品A'),))
    result = CostingRunResult(
        status=ServiceStatus.SUCCEEDED,
        message='预检通过',
        candidate_products=(),
    )

    main_window._on_worker_finished(result, task_kind='precheck')

    assert main_window.candidate_table.rowCount() == 0


def test_failed_result_clears_stale_candidate_table(main_window: MainWindow) -> None:
    main_window._set_table_pairs(main_window.candidate_table, (('P001', '产品A'),))
    result = CostingRunResult(
        status=ServiceStatus.FAILED,
        message='处理失败',
        candidate_products=(('P002', '产品B'),),
        error_code='ETL_FAILED',
    )

    main_window._on_worker_finished(result, task_kind='run')

    assert main_window.candidate_table.rowCount() == 0


def test_worker_exception_clears_stale_candidate_table(main_window: MainWindow) -> None:
    main_window._set_table_pairs(main_window.candidate_table, (('P001', '产品A'),))

    main_window._on_worker_failed('后台异常')

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

    monkeypatch.setattr(main_window, '_ask_confirmation', lambda *_args, **_kwargs: True)
    monkeypatch.setattr(main_window.whitelist_store, 'restore_default', fail_restore_default)

    main_window._restore_default_whitelist()

    assert '恢复默认失败: 配置目录不可写' in main_window.log_edit.toPlainText()


def test_restore_default_whitelist_overwrites_invalid_json(
    main_window: MainWindow,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / 'product_whitelists.json'
    config_path.write_text('{"gb": [', encoding='utf-8')
    main_window.whitelist_store = ProductWhitelistStore(config_path)
    main_window._set_table_pairs(main_window.whitelist_table, (('GB-CUSTOM', '产品甲'),))
    monkeypatch.setattr(main_window, '_ask_confirmation', lambda *_args, **_kwargs: True)

    main_window._restore_default_whitelist()

    assert main_window._table_pairs(main_window.whitelist_table) == GB_PIPELINE.product_order
    assert json.loads(config_path.read_text(encoding='utf-8'))['gb'][0] == {
        'product_code': GB_PIPELINE.product_order[0][0],
        'product_name': GB_PIPELINE.product_order[0][1],
    }
    assert 'GB 默认白名单已恢复' in main_window.log_edit.toPlainText()


def test_save_whitelist_overwrites_invalid_json(
    main_window: MainWindow,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / 'product_whitelists.json'
    config_path.write_text('{"gb": [', encoding='utf-8')
    main_window.whitelist_store = ProductWhitelistStore(config_path)
    main_window._set_table_pairs(main_window.whitelist_table, (('GB-SAVED', '产品甲'),))

    main_window._save_whitelist()

    assert json.loads(config_path.read_text(encoding='utf-8'))['gb'] == [
        {'product_code': 'GB-SAVED', 'product_name': '产品甲'}
    ]
    assert '产品白名单已保存' in main_window.log_edit.toPlainText()


def test_confirmation_dialog_uses_light_theme_and_chinese_buttons(main_window: MainWindow) -> None:
    message_box = main_window._build_confirmation_box(
        title='覆盖确认',
        message='输出 workbook 已存在，是否覆盖？',
        yes_text='覆盖',
        no_text='取消',
        icon=QMessageBox.Icon.Warning,
    )

    assert 'QMessageBox' in message_box.styleSheet()
    assert 'background: #f8fafc;' in message_box.styleSheet()
    assert 'color: #111827;' in message_box.styleSheet()
    assert message_box.button(QMessageBox.StandardButton.Yes).text() == '覆盖'
    assert message_box.button(QMessageBox.StandardButton.No).text() == '取消'


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


def test_output_context_change_drops_stale_last_output_dir(
    main_window: MainWindow,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    old_dir = tmp_path / 'old'
    new_dir = tmp_path / 'new'
    old_dir.mkdir()
    new_dir.mkdir()
    opened_args: list[list[str]] = []
    main_window.last_output_dir = old_dir
    monkeypatch.setattr(main_window_module.shutil, 'which', lambda _name: '/usr/bin/xdg-open')
    monkeypatch.setattr(main_window_module.subprocess, 'Popen', opened_args.append)

    main_window.output_edit.setText(str(new_dir))

    assert main_window.last_output_dir is None
    main_window._open_output_dir()
    assert opened_args == [['/usr/bin/xdg-open', str(new_dir)]]


def test_stale_worker_result_is_ignored_after_form_change(
    main_window: MainWindow,
    tmp_path: Path,
) -> None:
    old_revision = getattr(main_window, 'form_revision', 0)
    old_output_dir = tmp_path / 'old-output'
    old_output_dir.mkdir()
    main_window.busy = True
    main_window.current_worker = object()
    main_window.input_edit.setText(str(tmp_path / 'changed-input.xlsx'))
    result = CostingRunResult(
        status=ServiceStatus.SUCCEEDED,
        message='预检通过',
        workbook_path=old_output_dir / 'old.xlsx',
        candidate_products=(('P001', '产品A'),),
    )

    main_window._on_worker_finished(result, task_kind='precheck', request_revision=old_revision)

    assert main_window.precheck_passed is False
    assert main_window.run_button.isEnabled() is False
    assert main_window.candidate_table.rowCount() == 0
    assert main_window.last_output_dir is None
    assert main_window.current_worker is None
    assert '忽略过期任务结果' in main_window.log_edit.toPlainText()


def test_stale_worker_exception_is_ignored_after_form_change(
    main_window: MainWindow,
    tmp_path: Path,
) -> None:
    old_revision = getattr(main_window, 'form_revision', 0)
    main_window.busy = True
    main_window.current_worker = object()
    main_window.status_label.setText('等待配置')
    main_window._set_table_pairs(main_window.candidate_table, (('P001', '产品A'),))
    main_window.last_output_dir = tmp_path / 'old-output'
    main_window.input_edit.setText(str(tmp_path / 'changed-input.xlsx'))

    main_window._on_worker_failed('old error', request_revision=old_revision)

    assert main_window.precheck_passed is False
    assert main_window.run_button.isEnabled() is False
    assert main_window.candidate_table.rowCount() == 0
    assert main_window.last_output_dir is None
    assert main_window.current_worker is None
    assert main_window.status_label.text() != '处理失败'
    assert '忽略过期任务结果' in main_window.log_edit.toPlainText()


def test_progress_widgets_initialize_and_clear(main_window: MainWindow) -> None:
    assert main_window.progress_bar.value() == 0
    assert main_window.progress_label.text() == '等待任务'

    main_window.progress_bar.setValue(70)
    main_window.progress_label.setText('已完成分析与质量校验')
    main_window._clear_conditions()

    assert main_window.progress_bar.value() == 0
    assert main_window.progress_label.text() == '等待任务'


def test_worker_progress_updates_progress_widgets(main_window: MainWindow) -> None:
    event = main_window_module.ProgressEvent(percent=45, stage='fact', message='已拆分事实表')

    main_window._on_worker_progress(event, request_revision=main_window.form_revision)

    assert main_window.progress_bar.value() == 45
    assert main_window.progress_label.text() == '已拆分事实表'
    assert '[progress] fact: 已拆分事实表' in main_window.log_edit.toPlainText()


def test_repeated_progress_stage_logs_once(main_window: MainWindow) -> None:
    first = main_window_module.ProgressEvent(percent=45, stage='fact', message='已拆分事实表')
    second = main_window_module.ProgressEvent(percent=46, stage='fact', message='仍在拆分事实表')

    main_window._on_worker_progress(first, request_revision=main_window.form_revision)
    main_window._on_worker_progress(second, request_revision=main_window.form_revision)

    assert main_window.progress_bar.value() == 46
    assert main_window.log_edit.toPlainText().count('[progress] fact:') == 1


def test_stale_worker_progress_is_ignored_after_form_change(main_window: MainWindow, tmp_path: Path) -> None:
    old_revision = main_window.form_revision
    main_window.input_edit.setText(str(tmp_path / 'changed.xlsx'))
    event = main_window_module.ProgressEvent(percent=70, stage='analysis', message='旧任务进度')

    main_window._on_worker_progress(event, request_revision=old_revision)

    assert main_window.progress_bar.value() == 0
    assert main_window.progress_label.text() == '等待任务'
    assert '旧任务进度' not in main_window.log_edit.toPlainText()


def test_result_widgets_update_kpi_labels(main_window: MainWindow, tmp_path: Path) -> None:
    workbook_path = tmp_path / 'GB-成本计算单_处理后.xlsx'
    result = CostingRunResult(
        status=ServiceStatus.SUCCEEDED,
        message='处理成功',
        workbook_path=workbook_path,
        candidate_products=(('P001', '产品A'), ('P002', '产品B')),
        error_log_count=7,
        stage_timings={'ingest': 0.1, 'analysis': 0.2},
    )

    main_window._update_result_widgets(result)

    assert main_window.error_count_label.text() == '7'
    assert main_window.candidate_count_label.text() == '2'
    assert main_window.workbook_path_label.text() == str(workbook_path)
    assert 'ingest=0.100s' in main_window.stage_label.text()


def _set_sample_kpi_labels(main_window: MainWindow, tmp_path: Path) -> None:
    workbook_path = tmp_path / 'GB-成本计算单_处理后.xlsx'
    result = CostingRunResult(
        status=ServiceStatus.SUCCEEDED,
        message='处理成功',
        workbook_path=workbook_path,
        candidate_products=(('P001', '产品A'), ('P002', '产品B')),
        error_log_count=7,
    )

    main_window._update_result_widgets(result)


def test_clear_conditions_resets_kpi_labels(main_window: MainWindow, tmp_path: Path) -> None:
    _set_sample_kpi_labels(main_window, tmp_path)

    main_window._clear_conditions()

    assert main_window.error_count_label.text() == '-'
    assert main_window.candidate_count_label.text() == '-'
    assert main_window.workbook_path_label.text() == '-'


def test_worker_exception_resets_kpi_labels(main_window: MainWindow, tmp_path: Path) -> None:
    _set_sample_kpi_labels(main_window, tmp_path)

    main_window._on_worker_failed('后台异常')

    assert main_window.error_count_label.text() == '-'
    assert main_window.candidate_count_label.text() == '-'
    assert main_window.workbook_path_label.text() == '-'


def test_failed_task_does_not_force_progress_to_complete(main_window: MainWindow) -> None:
    main_window.progress_bar.setValue(45)
    result = CostingRunResult(
        status=ServiceStatus.FAILED,
        message='处理失败',
        error_code='ETL_FAILED',
    )

    main_window._on_worker_finished(result, task_kind='run')

    assert main_window.progress_bar.value() == 45
    assert main_window.progress_label.text() == '处理失败'

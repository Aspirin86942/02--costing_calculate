from __future__ import annotations

import shutil
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Literal

from PySide6.QtCore import QThreadPool
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QComboBox,
    QFileDialog,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from src.config.pipelines import PIPELINES, ProductOrder
from src.config.product_whitelist_store import ProductWhitelistConfigError, ProductWhitelistStore
from src.config.settings import PROJECT_ROOT
from src.etl.runner import find_input_files
from src.gui.form_state import GuiFormState
from src.gui.styles import APP_STYLESHEET, STATUS_COLORS
from src.gui.task_worker import ServiceWorker
from src.gui.validators import validate_month_range, validate_month_text
from src.services.costing_service import (
    CostingRunRequest,
    CostingRunResult,
    ServiceStatus,
    precheck_costing_run,
    run_costing_request,
)

CostingServiceFunction = Callable[[CostingRunRequest], CostingRunResult]
TaskKind = Literal['scan', 'precheck', 'run']


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle('成本核算分析工具')
        self.setMinimumSize(1180, 760)
        self.setStyleSheet(APP_STYLESHEET)

        self.thread_pool = QThreadPool.globalInstance()
        self.whitelist_store = ProductWhitelistStore()
        self.form_revision = 0
        self.precheck_passed = False
        self.busy = False
        self.last_output_dir: Path | None = None
        self.current_worker: ServiceWorker | None = None
        self.whitelist_action_buttons: list[QPushButton] = []

        self.pipeline_combo = QComboBox()
        self.pipeline_combo.addItems(['gb', 'sk'])

        self.input_edit = QLineEdit()
        self.output_edit = QLineEdit()
        self.month_start_edit = QLineEdit()
        self.month_end_edit = QLineEdit()

        self.status_label = QLabel('等待配置')
        self.status_label.setObjectName('StatusLabel')
        self.stage_label = QLabel('-')
        self.summary_label = QLabel('尚未运行')
        self.summary_label.setWordWrap(True)

        self.log_edit = QTextEdit()
        self.log_edit.setReadOnly(True)

        self.whitelist_table = QTableWidget(0, 2)
        self.candidate_table = QTableWidget(0, 2)

        self.scan_button = QPushButton('扫描产品')
        self.precheck_button = QPushButton('预检')
        self.run_button = QPushButton('开始处理')
        self.run_button.setObjectName('PrimaryButton')
        self.open_output_button = QPushButton('打开输出目录')
        self.clear_button = QPushButton('清空条件')
        self.exit_button = QPushButton('退出')
        self.add_candidate_button = QPushButton('加入白名单')

        self._build_ui()
        self._connect_signals()
        self._load_pipeline_defaults()
        self._set_busy(False)

    def _build_ui(self) -> None:
        title = QLabel('成本核算分析工具')
        title.setObjectName('TitleLabel')
        subtitle = QLabel('金蝶 ERP 成本计算单处理')
        subtitle.setObjectName('SubtitleLabel')

        config_group = QGroupBox('输入配置')
        config_layout = QFormLayout(config_group)
        config_layout.addRow('管线', self.pipeline_combo)
        config_layout.addRow(
            '输入文件',
            self._path_row(
                self.input_edit,
                '选择文件',
                self._choose_input_file,
                second_text='自动查找',
                second_slot=self._auto_find_input,
            ),
        )
        config_layout.addRow('输出目录', self._path_row(self.output_edit, '选择目录', self._choose_output_dir))
        config_layout.addRow('开始月份', self.month_start_edit)
        config_layout.addRow('结束月份', self.month_end_edit)
        self.month_start_edit.setPlaceholderText('YYYY-MM，可留空')
        self.month_end_edit.setPlaceholderText('YYYY-MM，可留空')

        whitelist_group = QGroupBox('产品白名单池')
        whitelist_layout = QVBoxLayout(whitelist_group)
        self._setup_table(self.whitelist_table, editable=True)
        whitelist_layout.addWidget(self.whitelist_table)
        whitelist_layout.addLayout(self._whitelist_buttons())

        candidate_group = QGroupBox('候选产品')
        candidate_layout = QVBoxLayout(candidate_group)
        self._setup_table(self.candidate_table, editable=False)
        candidate_layout.addWidget(self.candidate_table)
        self.add_candidate_button.clicked.connect(self._add_selected_candidates)
        candidate_layout.addWidget(self.add_candidate_button)

        left = QVBoxLayout()
        left.addWidget(title)
        left.addWidget(subtitle)
        left.addWidget(config_group)
        left.addWidget(whitelist_group, stretch=1)
        left.addWidget(candidate_group, stretch=1)

        status_group = QGroupBox('任务状态')
        status_layout = QFormLayout(status_group)
        status_layout.addRow('当前状态', self.status_label)
        status_layout.addRow('当前阶段', self.stage_label)
        status_layout.addRow('结果摘要', self.summary_label)

        log_group = QGroupBox('日志')
        log_layout = QVBoxLayout(log_group)
        log_layout.addWidget(self.log_edit)

        button_layout = QHBoxLayout()
        for button in (
            self.scan_button,
            self.precheck_button,
            self.run_button,
            self.open_output_button,
            self.clear_button,
            self.exit_button,
        ):
            button_layout.addWidget(button)

        right = QVBoxLayout()
        right.addWidget(status_group)
        right.addWidget(log_group, stretch=1)
        right.addLayout(button_layout)

        root_layout = QGridLayout()
        root_layout.addLayout(left, 0, 0)
        root_layout.addLayout(right, 0, 1)
        root_layout.setColumnStretch(0, 2)
        root_layout.setColumnStretch(1, 3)

        root = QWidget()
        root.setLayout(root_layout)
        self.setCentralWidget(root)

    def _path_row(
        self,
        edit: QLineEdit,
        text: str,
        slot: Callable[[], None],
        *,
        second_text: str | None = None,
        second_slot: Callable[[], None] | None = None,
    ) -> QWidget:
        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(edit)

        button = QPushButton(text)
        button.clicked.connect(slot)
        layout.addWidget(button)

        if second_text is not None and second_slot is not None:
            second = QPushButton(second_text)
            second.clicked.connect(second_slot)
            layout.addWidget(second)

        return widget

    def _setup_table(self, table: QTableWidget, *, editable: bool) -> None:
        table.setColumnCount(2)
        table.setHorizontalHeaderLabels(['产品编码', '产品名称'])
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        table.verticalHeader().setVisible(False)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        if editable:
            table.setEditTriggers(
                QAbstractItemView.EditTrigger.DoubleClicked | QAbstractItemView.EditTrigger.EditKeyPressed
            )
        else:
            table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)

    def _whitelist_buttons(self) -> QHBoxLayout:
        layout = QHBoxLayout()
        actions: tuple[tuple[str, Callable[[], None]], ...] = (
            ('新增', self._add_blank_whitelist_row),
            ('删除', self._delete_selected_whitelist_rows),
            ('上移', lambda: self._move_selected_whitelist_row(-1)),
            ('下移', lambda: self._move_selected_whitelist_row(1)),
            ('保存', self._save_whitelist),
            ('恢复默认', self._restore_default_whitelist),
        )
        for text, slot in actions:
            button = QPushButton(text)
            button.clicked.connect(slot)
            layout.addWidget(button)
            self.whitelist_action_buttons.append(button)
        return layout

    def _connect_signals(self) -> None:
        self.pipeline_combo.currentTextChanged.connect(self._on_pipeline_changed)
        self.input_edit.textChanged.connect(self._invalidate_precheck)
        self.output_edit.textChanged.connect(self._invalidate_precheck)
        self.month_start_edit.textChanged.connect(self._invalidate_precheck)
        self.month_end_edit.textChanged.connect(self._invalidate_precheck)
        self.whitelist_table.itemChanged.connect(self._invalidate_precheck)

        self.scan_button.clicked.connect(self._scan_products)
        self.precheck_button.clicked.connect(self._precheck)
        self.run_button.clicked.connect(self._run)
        self.open_output_button.clicked.connect(self._open_output_dir)
        self.clear_button.clicked.connect(self._clear_conditions)
        self.exit_button.clicked.connect(self._quit_application)

    def _on_pipeline_changed(self) -> None:
        self._load_pipeline_defaults()
        self.candidate_table.setRowCount(0)
        self._invalidate_precheck()

    def _load_pipeline_defaults(self) -> None:
        pipeline = self.pipeline_combo.currentText()
        if pipeline not in PIPELINES:
            return

        self.output_edit.setText(str(PIPELINES[pipeline].processed_dir))
        try:
            product_orders = self.whitelist_store.load().product_orders[pipeline]
        except ProductWhitelistConfigError as exc:
            self._append_log(f'产品白名单配置错误: {exc}')
            product_orders = PIPELINES[pipeline].product_order

        self._set_table_pairs(self.whitelist_table, product_orders)
        self.precheck_passed = False
        self._refresh_buttons()

    def _choose_input_file(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, '选择成本计算单', str(PROJECT_ROOT), 'Excel Workbook (*.xlsx)')
        if path:
            self.input_edit.setText(path)
            self._append_log(f'已选择输入文件: {path}')

    def _choose_output_dir(self) -> None:
        path = QFileDialog.getExistingDirectory(self, '选择输出目录', self.output_edit.text())
        if path:
            self.output_edit.setText(path)
            self._append_log(f'已选择输出目录: {path}')

    def _auto_find_input(self) -> None:
        config = PIPELINES[self.pipeline_combo.currentText()]
        files = find_input_files(config)
        if not files:
            self._append_log(f'未在 {config.raw_dir} 找到输入文件')
            return

        self.input_edit.setText(str(files[0]))
        self._append_log(f'自动查找到输入文件: {files[0]}')

    def _scan_products(self) -> None:
        self._start_worker(
            '正在扫描产品',
            precheck_costing_run,
            overwrite_confirmed=True,
            task_kind='scan',
        )

    def _precheck(self) -> None:
        self._start_worker(
            '正在预检',
            precheck_costing_run,
            overwrite_confirmed=False,
            task_kind='precheck',
        )

    def _run(self) -> None:
        self._start_worker(
            '正在处理',
            run_costing_request,
            overwrite_confirmed=False,
            task_kind='run',
        )

    def _start_worker(
        self,
        label: str,
        function: CostingServiceFunction,
        *,
        overwrite_confirmed: bool,
        task_kind: TaskKind,
    ) -> None:
        if self.busy or self.current_worker is not None:
            self._append_log('任务正在运行，请等待当前任务完成')
            self._refresh_buttons()
            return
        if task_kind == 'run' and not overwrite_confirmed and not self.precheck_passed:
            self._append_log('请先完成预检，通过后再开始处理')
            self._refresh_buttons()
            return

        validation_message = self._validate_form()
        if validation_message is not None:
            self.precheck_passed = False
            self._set_status('配置错误', 'failed')
            self._append_log(validation_message)
            self._refresh_buttons()
            return

        try:
            request = self._state(overwrite_confirmed=overwrite_confirmed).to_request()
        except ValueError as exc:
            self.precheck_passed = False
            self._set_status('配置错误', 'failed')
            self._append_log(str(exc))
            self._refresh_buttons()
            return

        self._set_busy(True)
        request_revision = self.form_revision
        worker = ServiceWorker(label, request, function)
        worker.signals.started.connect(self._on_worker_started)
        worker.signals.finished.connect(
            lambda result: self._on_worker_finished(
                result,
                task_kind=task_kind,
                request_revision=request_revision,
            )
        )
        worker.signals.failed.connect(
            lambda message: self._on_worker_failed(message, request_revision=request_revision)
        )
        self.current_worker = worker
        self.thread_pool.start(worker)

    def _validate_form(self) -> str | None:
        if not self.input_edit.text().strip():
            return '缺少输入文件'
        if not self.output_edit.text().strip():
            return '缺少输出目录'

        month_start = self.month_start_edit.text().strip()
        month_end = self.month_end_edit.text().strip()
        return (
            validate_month_text(month_start)
            or validate_month_text(month_end)
            or validate_month_range(month_start, month_end)
        )

    def _state(self, *, overwrite_confirmed: bool = False) -> GuiFormState:
        input_text = self.input_edit.text().strip()
        output_text = self.output_edit.text().strip()
        if not output_text:
            raise ValueError('缺少输出目录')
        return GuiFormState(
            pipeline=self.pipeline_combo.currentText(),
            input_path=Path(input_text) if input_text else None,
            output_dir=Path(output_text),
            month_start=self.month_start_edit.text().strip() or None,
            month_end=self.month_end_edit.text().strip() or None,
            product_order=self._table_pairs(self.whitelist_table),
            overwrite_confirmed=overwrite_confirmed,
        )

    def _on_worker_started(self, label: str) -> None:
        self._set_status(label, 'busy')
        self.stage_label.setText('-')
        self._append_log(label)

    def _on_worker_finished(
        self,
        result: CostingRunResult,
        *,
        task_kind: TaskKind,
        request_revision: int | None = None,
    ) -> None:
        self.current_worker = None
        self._set_busy(False)
        if self._is_stale_request(request_revision):
            self._ignore_stale_worker_result()
            return

        self._update_result_widgets(result)
        self._append_result_log(result, task_kind=task_kind)

        if result.workbook_path is not None:
            self.last_output_dir = result.workbook_path.parent

        if result.status == ServiceStatus.SUCCEEDED:
            self._set_table_pairs(self.candidate_table, result.candidate_products)
            self.precheck_passed = task_kind in {'precheck', 'run'}
            self._set_status(result.message, 'success')
        else:
            self._set_table_pairs(self.candidate_table, ())
            self.precheck_passed = False
            self._set_status(result.message, 'failed')
            self._refresh_buttons()
            if result.error_code == 'OUTPUT_EXISTS':
                self._confirm_overwrite_and_retry(result, task_kind)
            return

        self._refresh_buttons()

    def _on_worker_failed(self, message: str, request_revision: int | None = None) -> None:
        self.current_worker = None
        self._set_busy(False)
        if self._is_stale_request(request_revision):
            self._ignore_stale_worker_result()
            return

        self.precheck_passed = False
        self._set_table_pairs(self.candidate_table, ())
        self._set_status('处理失败', 'failed')
        self.stage_label.setText('-')
        self.summary_label.setText('任务异常终止')
        self._append_log(message)
        self._refresh_buttons()

    def _confirm_overwrite_and_retry(self, result: CostingRunResult, task_kind: TaskKind) -> None:
        reply = QMessageBox.question(
            self,
            '覆盖确认',
            f'{result.message}\n\n是否允许覆盖后继续？',
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            self._append_log('用户取消覆盖。')
            return

        if task_kind == 'run':
            self._start_worker('正在处理', run_costing_request, overwrite_confirmed=True, task_kind='run')
            return

        self._start_worker('正在预检', precheck_costing_run, overwrite_confirmed=True, task_kind='precheck')

    def _update_result_widgets(self, result: CostingRunResult) -> None:
        timings_text = self._format_stage_timings(result.stage_timings)
        self.stage_label.setText(timings_text or '-')
        self.summary_label.setText(self._format_summary(result))

    def _format_summary(self, result: CostingRunResult) -> str:
        workbook = str(result.workbook_path) if result.workbook_path is not None else '-'
        candidate_count = len(result.candidate_products)
        return (
            f'{result.message}\n'
            f'error_log 行数: {result.error_log_count}\n'
            f'候选产品: {candidate_count}\n'
            f'workbook: {workbook}'
        )

    def _append_result_log(self, result: CostingRunResult, *, task_kind: TaskKind) -> None:
        self._append_log(f'[{task_kind}] {result.message}')
        if result.error_code:
            self._append_log(f'error_code={result.error_code}')
        self._append_log(f'error_log_count={result.error_log_count}')
        if result.workbook_path is not None:
            self._append_log(f'workbook_path={result.workbook_path}')
        if result.candidate_products:
            self._append_log(f'candidate_products={len(result.candidate_products)}')
        timings_text = self._format_stage_timings(result.stage_timings)
        if timings_text:
            self._append_log(f'stage_timings={timings_text}')
        if result.technical_detail:
            self._append_log(f'technical_detail={result.technical_detail}')

    def _format_stage_timings(self, stage_timings: dict[str, float] | None) -> str:
        if not stage_timings:
            return ''
        return ', '.join(f'{stage}={seconds:.3f}s' for stage, seconds in stage_timings.items())

    def _is_stale_request(self, request_revision: int | None) -> bool:
        return request_revision is not None and request_revision != self.form_revision

    def _ignore_stale_worker_result(self) -> None:
        self.precheck_passed = False
        self.last_output_dir = None
        self._set_table_pairs(self.candidate_table, ())
        self._append_log('表单配置已变更，已忽略过期任务结果')
        self._refresh_buttons()

    def _set_busy(self, busy: bool) -> None:
        self.busy = busy
        self._refresh_buttons()

    def _refresh_buttons(self) -> None:
        has_input = bool(self.input_edit.text().strip())
        self.scan_button.setEnabled(not self.busy and has_input)
        self.precheck_button.setEnabled(not self.busy and has_input)
        self.run_button.setEnabled(not self.busy and has_input and self.precheck_passed)
        self.open_output_button.setEnabled(not self.busy)
        self.clear_button.setEnabled(not self.busy)
        self.exit_button.setEnabled(not self.busy)
        self.add_candidate_button.setEnabled(not self.busy)
        for button in self.whitelist_action_buttons:
            button.setEnabled(not self.busy)

    def _invalidate_precheck(self, *_args: object) -> None:
        self.form_revision += 1
        self.precheck_passed = False
        self.last_output_dir = None
        self.candidate_table.setRowCount(0)
        self._refresh_buttons()

    def _set_status(self, text: str, status: str) -> None:
        self.status_label.setText(text)
        self.status_label.setStyleSheet(f'color: {STATUS_COLORS.get(status, STATUS_COLORS["idle"])};')

    def _append_log(self, text: str) -> None:
        self.log_edit.append(text)

    def _set_table_pairs(self, table: QTableWidget, pairs: ProductOrder) -> None:
        previous_block_state = table.blockSignals(True)
        try:
            table.setRowCount(0)
            for code, name in pairs:
                row = table.rowCount()
                table.insertRow(row)
                table.setItem(row, 0, QTableWidgetItem(str(code)))
                table.setItem(row, 1, QTableWidgetItem(str(name)))
        finally:
            table.blockSignals(previous_block_state)

    def _table_pairs(self, table: QTableWidget) -> ProductOrder:
        pairs: list[tuple[str, str]] = []
        for row in range(table.rowCount()):
            code_item = table.item(row, 0)
            name_item = table.item(row, 1)
            code = '' if code_item is None else code_item.text().strip()
            name = '' if name_item is None else name_item.text().strip()
            if code or name:
                pairs.append((code, name))
        return tuple(pairs)

    def _add_blank_whitelist_row(self) -> None:
        row = self.whitelist_table.rowCount()
        self.whitelist_table.insertRow(row)
        self.whitelist_table.setItem(row, 0, QTableWidgetItem(''))
        self.whitelist_table.setItem(row, 1, QTableWidgetItem(''))
        self.whitelist_table.selectRow(row)
        self._invalidate_precheck()

    def _delete_selected_whitelist_rows(self) -> None:
        selected_rows = sorted({index.row() for index in self.whitelist_table.selectedIndexes()}, reverse=True)
        for row in selected_rows:
            self.whitelist_table.removeRow(row)
        if selected_rows:
            self._invalidate_precheck()

    def _move_selected_whitelist_row(self, delta: int) -> None:
        selected_rows = sorted({index.row() for index in self.whitelist_table.selectedIndexes()})
        if len(selected_rows) != 1:
            return

        row = selected_rows[0]
        target = row + delta
        if target < 0 or target >= self.whitelist_table.rowCount():
            return

        pairs = list(self._table_pairs(self.whitelist_table))
        pairs[row], pairs[target] = pairs[target], pairs[row]
        self._set_table_pairs(self.whitelist_table, tuple(pairs))
        self.whitelist_table.selectRow(target)
        self._invalidate_precheck()

    def _add_selected_candidates(self) -> None:
        existing = set(self._table_pairs(self.whitelist_table))
        added = 0
        selected_rows = sorted({index.row() for index in self.candidate_table.selectedIndexes()})
        for row in selected_rows:
            code_item = self.candidate_table.item(row, 0)
            name_item = self.candidate_table.item(row, 1)
            if code_item is None or name_item is None:
                continue

            code = code_item.text().strip()
            name = name_item.text().strip()
            if not code or not name or (code, name) in existing:
                continue

            target = self.whitelist_table.rowCount()
            self.whitelist_table.insertRow(target)
            self.whitelist_table.setItem(target, 0, QTableWidgetItem(code))
            self.whitelist_table.setItem(target, 1, QTableWidgetItem(name))
            existing.add((code, name))
            added += 1

        if added:
            self._append_log(f'已加入白名单: {added} 个产品')
            self._invalidate_precheck()

    def _save_whitelist(self) -> None:
        pipeline = self.pipeline_combo.currentText()
        try:
            self.whitelist_store.save({pipeline: self._table_pairs(self.whitelist_table)})
        except (ProductWhitelistConfigError, OSError) as exc:
            self._append_log(f'保存失败: {exc}')
            return

        self._append_log('产品白名单已保存')
        self._invalidate_precheck()

    def _restore_default_whitelist(self) -> None:
        pipeline = self.pipeline_combo.currentText()
        reply = QMessageBox.question(
            self,
            '恢复默认',
            f'确认恢复 {pipeline.upper()} 默认白名单？',
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        try:
            self.whitelist_store.restore_default(pipeline)
        except (ProductWhitelistConfigError, OSError) as exc:
            self._append_log(f'恢复默认失败: {exc}')
            return

        self._load_pipeline_defaults()
        self._append_log(f'{pipeline.upper()} 默认白名单已恢复')
        self._invalidate_precheck()

    def _clear_conditions(self) -> None:
        self.input_edit.clear()
        self.month_start_edit.clear()
        self.month_end_edit.clear()
        self.candidate_table.setRowCount(0)
        self.precheck_passed = False
        self.last_output_dir = None
        self.stage_label.setText('-')
        self.summary_label.setText('尚未运行')
        self._set_status('等待配置', 'idle')
        self._append_log('已清空输入与月份条件')
        self._refresh_buttons()

    def _open_output_dir(self) -> None:
        output_text = self.output_edit.text().strip()
        path = self.last_output_dir or (Path(output_text) if output_text else None)
        if path is None:
            self._append_log('缺少输出目录')
            return
        if not path.exists() or not path.is_dir():
            self._append_log(f'输出目录不存在: {path}')
            return

        opener = shutil.which('xdg-open')
        if opener is None:
            self._append_log('未找到 xdg-open，无法打开输出目录')
            return

        try:
            subprocess.Popen([opener, str(path)])  # noqa: S603
        except OSError as exc:
            self._append_log(f'打开输出目录失败: {exc}')
            return
        self._append_log(f'已请求打开输出目录: {path}')

    def _quit_application(self) -> None:
        app = QApplication.instance()
        if app is None:
            self.close()
            return
        app.quit()

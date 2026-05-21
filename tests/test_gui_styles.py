from __future__ import annotations

from src.gui.styles import APP_STYLESHEET, MESSAGE_BOX_STYLESHEET


def test_stylesheet_uses_muted_slate_main_window_theme() -> None:
    assert 'QMainWindow {' in APP_STYLESHEET
    assert 'background: #1E222B;' in APP_STYLESHEET
    assert 'color: #E5E7EB;' in APP_STYLESHEET
    assert 'QWidget#MainContentContainer' in APP_STYLESHEET


def test_stylesheet_pins_dark_control_text_and_selection_colors() -> None:
    assert 'QLineEdit,' in APP_STYLESHEET
    assert 'QComboBox {' in APP_STYLESHEET
    assert 'background: #1F2430;' in APP_STYLESHEET
    assert 'color: #E5E7EB;' in APP_STYLESHEET
    assert 'selection-background-color: #2B6CB0;' in APP_STYLESHEET
    assert 'selection-color: #FFFFFF;' in APP_STYLESHEET


def test_stylesheet_pins_table_selection_and_zebra_colors() -> None:
    assert 'QTableWidget {' in APP_STYLESHEET
    assert 'alternate-background-color: #2C313C;' in APP_STYLESHEET
    assert 'QTableWidget::item:selected {' in APP_STYLESHEET
    assert 'background: #2B6CB0;' in APP_STYLESHEET
    assert 'color: #FFFFFF;' in APP_STYLESHEET


def test_stylesheet_styles_log_terminal_and_progress_bar() -> None:
    assert 'QTextEdit#LogTerminal {' in APP_STYLESHEET
    assert 'background: #181A1F;' in APP_STYLESHEET
    assert 'font-family: Consolas, "Fira Code", monospace;' in APP_STYLESHEET
    assert 'QProgressBar#TaskProgressBar {' in APP_STYLESHEET
    assert 'QProgressBar#TaskProgressBar::chunk {' in APP_STYLESHEET


def test_message_box_stylesheet_remains_light_for_confirmation_dialogs() -> None:
    assert 'QMessageBox {' in MESSAGE_BOX_STYLESHEET
    assert 'background: #f8fafc;' in MESSAGE_BOX_STYLESHEET
    assert 'color: #111827;' in MESSAGE_BOX_STYLESHEET

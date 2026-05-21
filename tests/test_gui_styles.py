from __future__ import annotations

from src.gui.styles import APP_STYLESHEET


def test_stylesheet_pins_control_text_colors_for_dark_system_themes() -> None:
    assert 'QLineEdit,' in APP_STYLESHEET
    assert 'QComboBox {' in APP_STYLESHEET
    assert 'color: #111827;' in APP_STYLESHEET
    assert 'selection-color: #ffffff;' in APP_STYLESHEET


def test_stylesheet_pins_combobox_popup_selection_colors() -> None:
    assert 'QComboBox QAbstractItemView {' in APP_STYLESHEET
    assert 'QComboBox QAbstractItemView::item:selected {' in APP_STYLESHEET
    assert 'selection-background-color: #2563eb;' in APP_STYLESHEET
    assert 'selection-color: #ffffff;' in APP_STYLESHEET


def test_stylesheet_pins_scrollbar_colors_for_dark_system_themes() -> None:
    assert 'QScrollBar:vertical {' in APP_STYLESHEET
    assert 'QScrollBar::handle:vertical {' in APP_STYLESHEET
    assert 'background: #f1f5f9;' in APP_STYLESHEET
    assert 'background: #94a3b8;' in APP_STYLESHEET
    assert 'QTextEdit QScrollBar:vertical' in APP_STYLESHEET
    assert 'QTextEdit QScrollBar::handle:vertical' in APP_STYLESHEET

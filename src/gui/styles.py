STATUS_COLORS = {
    'idle': '#52606d',
    'busy': '#1d4ed8',
    'success': '#047857',
    'failed': '#b91c1c',
}

APP_STYLESHEET = """
QMainWindow {
    background: #f4f6f8;
}
QWidget {
    color: #1f2933;
    font-size: 13px;
}
QLabel#TitleLabel {
    font-size: 22px;
    font-weight: 700;
    color: #1f2933;
}
QLabel#SubtitleLabel {
    color: #52606d;
}
QLabel#StatusLabel {
    font-weight: 600;
}
QGroupBox {
    border: 1px solid #d9e2ec;
    border-radius: 6px;
    margin-top: 10px;
    background: #ffffff;
}
QGroupBox::title {
    subcontrol-origin: margin;
    left: 10px;
    padding: 0 4px;
    color: #334e68;
    font-weight: 600;
}
QLineEdit,
QComboBox {
    min-height: 28px;
    padding: 2px 6px;
    border: 1px solid #cbd5e1;
    border-radius: 4px;
    background: #ffffff;
    color: #111827;
    selection-background-color: #2563eb;
    selection-color: #ffffff;
}
QLineEdit:focus,
QComboBox:focus {
    border-color: #2563eb;
}
QComboBox QAbstractItemView {
    border: 1px solid #cbd5e1;
    background: #ffffff;
    color: #111827;
    selection-background-color: #2563eb;
    selection-color: #ffffff;
    outline: 0;
}
QComboBox QAbstractItemView::item {
    min-height: 28px;
    padding: 4px 8px;
}
QComboBox QAbstractItemView::item:selected {
    background: #2563eb;
    color: #ffffff;
}
QPushButton {
    min-height: 30px;
    padding: 4px 10px;
    border: 1px solid #cbd5e1;
    border-radius: 4px;
    background: #ffffff;
    color: #111827;
}
QPushButton:hover {
    background: #f1f5f9;
}
QPushButton:disabled {
    color: #94a3b8;
    background: #f8fafc;
}
QPushButton#PrimaryButton {
    background: #2563eb;
    color: #ffffff;
    border: 0;
}
QPushButton#PrimaryButton:hover {
    background: #1d4ed8;
}
QTableWidget {
    gridline-color: #e2e8f0;
    background: #ffffff;
    color: #111827;
    selection-background-color: #2563eb;
    selection-color: #ffffff;
}
QHeaderView::section {
    padding: 5px;
    border: 0;
    border-right: 1px solid #e2e8f0;
    border-bottom: 1px solid #e2e8f0;
    background: #f8fafc;
    color: #111827;
    font-weight: 600;
}
QTextEdit {
    background: #111827;
    color: #e5e7eb;
    border: 1px solid #1f2937;
    border-radius: 4px;
    font-family: monospace;
}
"""

MESSAGE_BOX_STYLESHEET = """
QMessageBox {
    background: #f8fafc;
    color: #111827;
}
QMessageBox QLabel {
    color: #111827;
    font-size: 13px;
}
QMessageBox QPushButton {
    min-width: 72px;
    min-height: 30px;
    padding: 4px 12px;
    border: 1px solid #cbd5e1;
    border-radius: 4px;
    background: #ffffff;
    color: #111827;
}
QMessageBox QPushButton:hover {
    background: #f1f5f9;
}
QMessageBox QPushButton:default {
    border-color: #2563eb;
}
"""

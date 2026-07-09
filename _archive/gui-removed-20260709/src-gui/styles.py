STATUS_COLORS = {
    'idle': '#A0AEC0',
    'busy': '#63B3ED',
    'success': '#48BB78',
    'failed': '#E53E3E',
}

APP_STYLESHEET = """
QMainWindow {
    background: #1E222B;
}
QWidget {
    color: #E5E7EB;
    font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
    font-size: 13px;
}
QWidget#MainContentContainer,
QWidget#ProgressArea,
QWidget#BottomActionBar {
    background: transparent;
}
QWidget#LeftPanel,
QWidget#RightPanel {
    background: transparent;
}
QLabel#TitleLabel {
    font-size: 22px;
    font-weight: 700;
    color: #F7FAFC;
}
QLabel#SubtitleLabel,
QLabel#ProgressLabel,
QLabel#KpiTitle {
    color: #A0AEC0;
}
QLabel#StatusLabel,
QLabel#KpiValue {
    font-weight: 700;
    color: #E5E7EB;
}
QLabel#KpiValue {
    font-size: 20px;
}
QLabel#KpiPathValue {
    color: #E5E7EB;
}
QWidget#KpiCard {
    border: 1px solid #3E4451;
    border-radius: 6px;
    background: #252932;
}
QGroupBox {
    border: 1px solid #3E4451;
    border-radius: 6px;
    margin-top: 10px;
    background: #252932;
}
QGroupBox::title {
    subcontrol-origin: margin;
    left: 10px;
    padding: 0 4px;
    color: #A0AEC0;
    font-weight: 600;
}
QLineEdit,
QComboBox {
    min-height: 30px;
    padding: 3px 8px;
    border: 1px solid #3E4451;
    border-radius: 4px;
    background: #1F2430;
    color: #E5E7EB;
    selection-background-color: #2B6CB0;
    selection-color: #FFFFFF;
}
QLineEdit:focus,
QComboBox:focus {
    border-color: #3182CE;
}
QComboBox QAbstractItemView {
    border: 1px solid #3E4451;
    background: #1F2430;
    color: #E5E7EB;
    selection-background-color: #2B6CB0;
    selection-color: #FFFFFF;
    outline: 0;
}
QComboBox QAbstractItemView::item {
    min-height: 28px;
    padding: 4px 8px;
}
QComboBox QAbstractItemView::item:selected {
    background: #2B6CB0;
    color: #FFFFFF;
}
QPushButton {
    min-height: 32px;
    padding: 4px 12px;
    border: 1px solid #4A5568;
    border-radius: 4px;
    background: #2D3748;
    color: #E5E7EB;
}
QPushButton:hover {
    background: #3A465A;
}
QPushButton:disabled {
    color: #718096;
    background: #252932;
    border-color: #3E4451;
}
QPushButton#PrimaryButton {
    background: #2B6CB0;
    color: #FFFFFF;
    border: 1px solid #2B6CB0;
    font-weight: 700;
}
QPushButton#PrimaryButton:hover {
    background: #3182CE;
    border-color: #3182CE;
}
QTableWidget {
    gridline-color: #3E4451;
    background: #252932;
    alternate-background-color: #2C313C;
    color: #E5E7EB;
    selection-background-color: #2B6CB0;
    selection-color: #FFFFFF;
    border: 1px solid #3E4451;
    border-radius: 4px;
}
QTableWidget::item:selected {
    background: #2B6CB0;
    color: #FFFFFF;
}
QHeaderView::section {
    padding: 6px;
    border: 0;
    border-right: 1px solid #3E4451;
    border-bottom: 1px solid #3E4451;
    background: #1F2430;
    color: #E5E7EB;
    font-weight: 600;
}
QTextEdit#LogTerminal {
    background: #181A1F;
    color: #E5E7EB;
    border: 1px solid #3E4451;
    border-radius: 4px;
    padding: 10px;
    font-family: Consolas, "Fira Code", monospace;
}
QProgressBar#TaskProgressBar {
    min-height: 14px;
    border: 1px solid #3E4451;
    border-radius: 4px;
    background: #1F2430;
    color: #E5E7EB;
    text-align: center;
}
QProgressBar#TaskProgressBar::chunk {
    border-radius: 3px;
    background: #2B6CB0;
}
QScrollBar:vertical {
    width: 14px;
    margin: 0;
    border: 1px solid #3E4451;
    border-radius: 4px;
    background: #1F2430;
}
QScrollBar::handle:vertical {
    min-height: 24px;
    margin: 2px;
    border-radius: 5px;
    background: #4A5568;
}
QScrollBar::handle:vertical:hover {
    background: #718096;
}
QScrollBar::add-line:vertical,
QScrollBar::sub-line:vertical {
    height: 0;
    border: 0;
    background: transparent;
}
QScrollBar::add-page:vertical,
QScrollBar::sub-page:vertical {
    background: transparent;
}
QScrollBar:horizontal {
    height: 14px;
    margin: 0;
    border: 1px solid #3E4451;
    border-radius: 4px;
    background: #1F2430;
}
QScrollBar::handle:horizontal {
    min-width: 24px;
    margin: 2px;
    border-radius: 5px;
    background: #4A5568;
}
QScrollBar::handle:horizontal:hover {
    background: #718096;
}
QScrollBar::add-line:horizontal,
QScrollBar::sub-line:horizontal {
    width: 0;
    border: 0;
    background: transparent;
}
QScrollBar::add-page:horizontal,
QScrollBar::sub-page:horizontal {
    background: transparent;
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

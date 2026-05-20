from __future__ import annotations

from src.etl.month_filter import build_month_range
from src.gui.form_state import GuiFormState


def validate_month_text(value: str) -> str | None:
    stripped = value.strip()
    if not stripped:
        return None
    try:
        build_month_range(stripped, None)
    except ValueError:
        return '月份必须是 YYYY-MM 格式'
    return None


def can_start_processing(state: GuiFormState, *, precheck_passed: bool, busy: bool) -> bool:
    return state.input_path is not None and precheck_passed and not busy

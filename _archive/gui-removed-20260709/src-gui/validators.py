from __future__ import annotations

from src.etl.month_filter import build_month_range
from src.gui.form_state import GuiFormState


def validate_month_text(value: str | None) -> str | None:
    normalized = _blank_to_none(value)
    if normalized is None:
        return None
    try:
        build_month_range(normalized, None)
    except ValueError:
        return '月份必须是 YYYY-MM 格式'
    return None


def validate_month_range(month_start: str | None, month_end: str | None) -> str | None:
    try:
        build_month_range(_blank_to_none(month_start), _blank_to_none(month_end))
    except ValueError as exc:
        message = str(exc)
        if '不能晚于' in message:
            return '开始月份不能晚于结束月份'
        return '月份必须是 YYYY-MM 格式'
    return None


def can_start_processing(state: GuiFormState, *, precheck_passed: bool, busy: bool) -> bool:
    return state.input_path is not None and precheck_passed and not busy


def _blank_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None

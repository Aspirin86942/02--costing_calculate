from __future__ import annotations

from pathlib import Path

import pytest

from src.gui.form_state import GuiFormState
from src.gui.validators import can_start_processing, validate_month_range, validate_month_text


def test_form_state_builds_service_request(tmp_path: Path) -> None:
    input_path = tmp_path / 'GB-成本计算单.xlsx'
    input_path.write_bytes(b'raw')
    state = GuiFormState(
        pipeline='gb',
        input_path=input_path,
        output_dir=tmp_path / 'processed',
        month_start='2025-01',
        month_end='2025-03',
        product_order=(('GB_C.D.B0040AA', 'BMS-750W驱动器'),),
        overwrite_confirmed=True,
    )

    request = state.to_request()

    assert request.pipeline == 'gb'
    assert request.input_path == input_path
    assert request.output_dir == tmp_path / 'processed'
    assert request.month_start == '2025-01'
    assert request.month_end == '2025-03'
    assert request.product_order == (('GB_C.D.B0040AA', 'BMS-750W驱动器'),)


def test_can_start_processing_requires_input_and_successful_precheck(tmp_path: Path) -> None:
    state = GuiFormState(pipeline='gb', input_path=None, output_dir=tmp_path, product_order=())

    assert can_start_processing(state, precheck_passed=False, busy=False) is False

    state = GuiFormState(
        pipeline='gb',
        input_path=tmp_path / 'GB-成本计算单.xlsx',
        output_dir=tmp_path,
        product_order=(('P001', '产品A'),),
    )
    assert can_start_processing(state, precheck_passed=True, busy=False) is True
    assert can_start_processing(state, precheck_passed=True, busy=True) is False


def test_validate_month_text_accepts_blank_and_yyyy_mm() -> None:
    assert validate_month_text('') is None
    assert validate_month_text(None) is None
    assert validate_month_text('2025-01') is None
    assert validate_month_text('2025/01') == '月份必须是 YYYY-MM 格式'


def test_validate_month_range_returns_gui_messages_for_invalid_ranges() -> None:
    assert validate_month_range('2025-03', '2025-01') == '开始月份不能晚于结束月份'
    assert validate_month_range('2025-01', '2025-03') is None
    assert validate_month_range('', None) is None
    assert validate_month_range('2025/01', '2025-03') == '月份必须是 YYYY-MM 格式'


def test_form_state_normalizes_blank_months_and_passes_benchmark(tmp_path: Path) -> None:
    input_path = tmp_path / 'GB-成本计算单.xlsx'
    input_path.write_bytes(b'raw')
    state = GuiFormState(
        pipeline='gb',
        input_path=input_path,
        output_dir=tmp_path,
        month_start='  ',
        month_end='\t',
        product_order=(('P001', '产品A'),),
        benchmark=False,
    )

    request = state.to_request()

    assert request.month_start is None
    assert request.month_end is None
    assert request.benchmark is False


def test_form_state_requires_input_path(tmp_path: Path) -> None:
    state = GuiFormState(pipeline='gb', input_path=None, output_dir=tmp_path, product_order=())

    with pytest.raises(ValueError, match='缺少输入文件'):
        state.to_request()

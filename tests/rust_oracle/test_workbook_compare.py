from __future__ import annotations

from tests.rust_oracle.workbook_compare import values_equal


def test_numeric_strings_are_not_equal_to_numbers() -> None:
    assert not values_equal('00123', 123)
    assert not values_equal('2025', 2025)


def test_numbers_use_decimal_tolerance() -> None:
    assert values_equal(1, 1.0000001)

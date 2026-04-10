from __future__ import annotations

import pytest

from main import main


def test_main_requires_pipeline_argument() -> None:
    with pytest.raises(SystemExit) as exc_info:
        main([])
    assert exc_info.value.code == 2


def test_main_rejects_invalid_pipeline() -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(['bad'])
    assert exc_info.value.code == 2

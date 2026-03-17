"""读取原始 workbook。"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def load_raw_workbook(input_path: Path, *, skip_rows: int) -> pd.DataFrame:
    """读取双层表头 workbook。

    为什么单独拆出来：
    后续 ETL 只关心 DataFrame，不需要知道底层 Excel 读取参数，
    这样既方便测试，也方便后续引入 profile 或观测。
    """
    return pd.read_excel(input_path, header=[0, 1], skiprows=skip_rows, engine='openpyxl')

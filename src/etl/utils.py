"""通用工具函数"""

import re

import pandas as pd


def clean_column_name(col_tuple: tuple | str) -> str:
    """清理并扁平化双层表头

    Args:
        col_tuple: 列名元组 (上层，下层) 或字符串

    Returns:
        清洗后的列名字符串
    """
    if isinstance(col_tuple, str):
        return col_tuple.strip().replace(' ', '').replace('\n', '')

    c0 = str(col_tuple[0]).strip()
    c1 = str(col_tuple[1]).strip()

    # 清除 Unnamed 和换行符/空格
    if 'Unnamed' in c0:
        c0 = ''
    if 'Unnamed' in c1:
        c1 = ''
    c0 = c0.replace(' ', '').replace('\n', '')
    c1 = c1.replace(' ', '').replace('\n', '')

    if c0 and c1 and c0 != c1:
        return f'{c0}{c1}'
    elif c1:
        return c1
    else:
        return c0


def format_period_col(df: pd.DataFrame) -> pd.DataFrame:
    """统一生成月份列

    Args:
        df: 输入 DataFrame

    Returns:
        添加'月份'列的 DataFrame
    """
    if '年期' in df.columns:

        def _fmt(val) -> str:
            if pd.isna(val):
                return val
            match = re.search(r'(\d+)年\s*(\d+)\s*期', str(val))
            if match:
                return f'{match.group(1)}年{int(match.group(2)):02d}期'
            return val

        # 插入到第二列
        if '月份' not in df.columns:
            df.insert(1, '月份', df['年期'].apply(_fmt))
    return df

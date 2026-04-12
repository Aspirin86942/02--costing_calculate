"""error_log 契约与错误拼装。"""

from __future__ import annotations

import pandas as pd
import polars as pl

ERROR_LOG_COLUMNS = [
    'row_id',
    'cost_bucket',
    'product_code',
    'product_name',
    'period',
    'issue_type',
    'field_name',
    'original_value',
    'lhs',
    'rhs',
    'diff',
    'reason',
    'action',
    'retryable',
]
STABLE_ISSUE_TYPES = [
    'UNMAPPED_COST_ITEM',
    'MISSING_AMOUNT',
    'DUPLICATE_WORK_ORDER_KEY',
    'MOH_BREAKDOWN_MISMATCH',
    'TOTAL_COST_MISMATCH',
    'NON_POSITIVE_UNIT_COST',
]
LEGACY_ISSUE_TYPES = ['MISSING_QTY', 'PRICE_MISMATCH']


def empty_error_log() -> pd.DataFrame:
    """返回固定列序的空 error_log。"""
    return pd.DataFrame(columns=ERROR_LOG_COLUMNS)


def empty_error_log_polars() -> pl.DataFrame:
    """返回固定列序的空 Polars error_log。"""
    return pl.DataFrame(
        schema={
            'row_id': pl.String,
            'cost_bucket': pl.String,
            'product_code': pl.String,
            'product_name': pl.String,
            'period': pl.String,
            'issue_type': pl.String,
            'field_name': pl.String,
            'original_value': pl.Object,
            'lhs': pl.Object,
            'rhs': pl.Object,
            'diff': pl.Object,
            'reason': pl.String,
            'action': pl.String,
            'retryable': pl.Boolean,
        }
    ).select(ERROR_LOG_COLUMNS)


def build_error_frame(
    data: pd.DataFrame,
    issue_type: str,
    field_name: str,
    reason: str,
    action: str,
    *,
    original_column: str | None = None,
    lhs_column: str | None = None,
    rhs_column: str | None = None,
    diff_column: str | None = None,
    row_id_fields: list[str] | None = None,
) -> pd.DataFrame:
    """把一批异常行投影成统一 error_log 格式。"""
    if data.empty:
        return empty_error_log()

    frame = pd.DataFrame(index=data.index)
    frame['product_code'] = data.get('product_code')
    frame['product_name'] = data.get('product_name')
    frame['period'] = data.get('period')
    frame['cost_bucket'] = data.get('cost_bucket')
    frame['issue_type'] = issue_type
    frame['field_name'] = field_name
    frame['reason'] = reason
    frame['action'] = action
    frame['retryable'] = False
    frame['original_value'] = data[original_column] if original_column else None
    frame['lhs'] = data[lhs_column] if lhs_column else None
    frame['rhs'] = data[rhs_column] if rhs_column else None
    frame['diff'] = data[diff_column] if diff_column else None

    row_parts: list[pd.Series] = []
    for field in row_id_fields or ['period', 'product_code', 'order_no', 'order_line', 'cost_bucket']:
        if field in data.columns:
            row_parts.append(data[field].map(normalize_key_value))
    if row_parts:
        row_id = row_parts[0].copy()
        for part in row_parts[1:]:
            row_id = row_id + '|' + part
        frame['row_id'] = row_id
    else:
        frame['row_id'] = pd.Series('', index=data.index)

    return frame[ERROR_LOG_COLUMNS].reset_index(drop=True)


def concat_error_logs(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """合并多批 error_log。"""
    valid_frames = [frame for frame in frames if not frame.empty]
    if not valid_frames:
        return empty_error_log()
    return pd.concat(valid_frames, ignore_index=True)


def append_reason(reason_series: pd.Series, mask: pd.Series, reason: str) -> pd.Series:
    """向复核原因列追加文本说明。"""
    updated = reason_series.copy()
    target = mask.fillna(False)
    if not target.any():
        return updated
    non_empty = target & updated.ne('')
    empty = target & updated.eq('')
    updated.loc[non_empty] = updated.loc[non_empty] + ';' + reason
    updated.loc[empty] = reason
    return updated


def normalize_key_value(value: object) -> str:
    """统一 row_id 字段的文本表现，避免 1 / 1.0 被当成两个键。"""
    if value is None or pd.isna(value):
        return ''
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).strip()
    if text.endswith('.0') and text[:-2].isdigit():
        return text[:-2]
    return text

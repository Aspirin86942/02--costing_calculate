"""Synthetic raw-workbook fixtures for performance harness checks."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import xlsxwriter

from tests.rust_oracle.benchmark_protocol import PipelineName

LOW_MEMORY_QUANTITY_ROWS = 140_000
# 当前 SK 数量 Sheet 的闭合输出契约为 36 列；真实 smoke 还会直接复核 OOXML dimension。
LOW_MEMORY_OUTPUT_COLUMNS_LOWER_BOUND = 36

RAW_COLUMNS = (
    '年期',
    '成本中心名称',
    '产品编码',
    '产品名称',
    '规格型号',
    '工单编号',
    '工单行号',
    '基本单位',
    '成本项目名称',
    '本期完工数量',
    '本期完工单位成本',
    '本期完工金额',
)


def build_raw_fixture(path: Path, pipeline: PipelineName, size: Literal['small', 'low-memory']) -> None:
    """Create one legal raw input Sheet containing synthetic-only costing rows."""
    if pipeline not in ('gb', 'sk'):
        raise ValueError('pipeline must be gb or sk')
    if size not in ('small', 'low-memory'):
        raise ValueError('size must be small or low-memory')
    if path.exists():
        raise FileExistsError(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    workbook = xlsxwriter.Workbook(path, {'constant_memory': True, 'tmpdir': str(path.parent)})
    workbook.set_properties(
        {
            'title': 'Synthetic costing fixture',
            'subject': 'Automated performance validation',
            'author': 'Synthetic fixture builder',
            'company': 'Synthetic data only',
        }
    )
    worksheet = workbook.add_worksheet('SyntheticRawInput')
    try:
        worksheet.write_row(0, 0, ('Synthetic fixture',))
        worksheet.write_row(1, 0, ('Generated data only',))
        worksheet.write_row(2, 0, RAW_COLUMNS)
        worksheet.write_row(3, 0, ('',) * len(RAW_COLUMNS))

        row_index = 4
        quantity_rows = 2 if size == 'small' else LOW_MEMORY_QUANTITY_ROWS
        for order_index in range(1, quantity_rows + 1):
            worksheet.write_row(row_index, 0, _quantity_row(pipeline, order_index))
            row_index += 1

        # 常数个明细足以覆盖类别契约，同时避免把 100k 数量 fixture 放大到 600k 行。
        for cost_item, unit_cost, amount in _detail_costs(pipeline):
            worksheet.write_row(row_index, 0, _detail_row(pipeline, cost_item, unit_cost, amount))
            row_index += 1
    finally:
        workbook.close()


def _quantity_row(pipeline: PipelineName, order_index: int) -> tuple[object, ...]:
    total_amount = 170 if pipeline == 'sk' else 160
    return (
        '2026年1期',
        'Synthetic Center',
        f'SYN-{pipeline.upper()}-PRODUCT',
        'Synthetic Product Outside Whitelist',
        'SYN-MODEL',
        f'SYN-{pipeline.upper()}-{order_index:06d}',
        1,
        'PCS',
        None,
        10,
        total_amount // 10,
        total_amount,
    )


def _detail_costs(pipeline: PipelineName) -> tuple[tuple[str, int, int], ...]:
    base: tuple[tuple[str, int, int], ...] = (
        ('直接材料', 10, 100),
        ('直接人工', 2, 20),
        ('制造费用-人工', 3, 30),
        ('委外加工费', 1, 10),
    )
    return (*base, ('软件费用', 1, 10)) if pipeline == 'sk' else base


def _detail_row(pipeline: PipelineName, cost_item: str, unit_cost: int, amount: int) -> tuple[object, ...]:
    return (
        '2026年1期',
        'Synthetic Center',
        f'SYN-{pipeline.upper()}-PRODUCT',
        'Synthetic Product Outside Whitelist',
        'SYN-MODEL',
        f'SYN-{pipeline.upper()}-000001',
        1,
        'PCS',
        cost_item,
        None,
        unit_cost,
        amount,
    )

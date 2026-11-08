"""Deterministic synthetic data and specifications; all costing remains in Rust."""

from __future__ import annotations

import hashlib
import json
import random
from collections.abc import Iterator
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import xlsxwriter

from tools.validation.synthetic_inputs import RAW_COLUMNS, BenchmarkSize
from tools.validation.types import PipelineName

BENCHMARK_SIZES = ('small', 'medium', 'below-threshold', 'above-threshold')
DEFAULT_SEED = 20261108
CELL_SLOT_THRESHOLD = 5_000_000
BENCHMARK_COLUMNS = (
    *RAW_COLUMNS,
    '生产类型',
    '单据类型',
    '供应商编码',
    '供应商名称',
    '子项物料编码',
    '子项物料名称',
)
MONTHS = ('2025-12', '2026-01', '2026-02', '2026-03')
DIRTY_CASES = (
    'missing-detail-amount',
    'zero-labor',
    'negative-material',
    'total-mismatch',
    'unmapped-item',
    'unclassified-overhead',
    'duplicate-quantity',
    'zero-quantity',
    'negative-quantity',
    'missing-quantity',
    'missing-total',
    'fractional-text',
    'zero-total',
    'negative-total',
)
SHEET_NAMES = ('成本计算单总表', '成本计算单数量聚合维度', '成本分析工单维度')
# 固定输入列投影的当前 Rust 契约；实际 workbook 维度必须独立复核。
OUTPUT_COLUMNS = {'gb': (19, 36, 35), 'sk': (19, 38, 37)}
BASE_ITEMS = (
    '直接材料',
    '直接人工',
    '制造费用_其他',
    '制造费用-人工',
    '制造费用_机物料及低耗',
    '制造费用_折旧',
    '制造费用_水电费',
    '委外加工费',
)


def _cost_items(pipeline: PipelineName) -> tuple[str, ...]:
    return (*BASE_ITEMS, '软件费用') if pipeline == 'sk' else BASE_ITEMS


def _products(pipeline: PipelineName) -> tuple[tuple[str, str], ...]:
    prefix = f'SYN-{pipeline.upper()}'
    return (
        (f'{prefix}-002', '合成产品乙'),
        (f'{prefix}-001', '合成产品甲'),
        (f'{prefix}-OUT', '合成白名单外产品'),
        (f'{prefix}-002', '合成同编码不同名称产品'),
    )


def synthetic_config() -> str:
    """Return a complete v1 TOML config with only synthetic product identities."""
    lines = ['schema_version = 1', '']
    for pipeline in ('gb', 'sk'):
        items = ['委外加工费', '软件费用'] if pipeline == 'sk' else ['委外加工费']
        lines.extend(
            (
                f'[pipelines.{pipeline}]',
                f'input_pattern = "{pipeline}-synthetic-*.xlsx"',
                f'standalone_cost_items = {json.dumps(items, ensure_ascii=False)}',
                '',
            )
        )
        for index, (code, name) in enumerate(_products(pipeline)[:2], 1):
            lines.extend(
                (
                    f'[[pipelines.{pipeline}.product_order]]',
                    f'code = "{code}"',
                    f'name = "{name}"',
                    f'display_order = {index * 10}',
                    '',
                )
            )
    return '\n'.join(lines)


def benchmark_spec(
    pipeline: PipelineName,
    size: BenchmarkSize = 'small',
    seed: int = DEFAULT_SEED,
) -> dict[str, Any]:
    """Calculate fixture expectations without generating or processing a workbook."""
    if pipeline not in ('gb', 'sk') or size not in BENCHMARK_SIZES:
        raise ValueError('invalid synthetic benchmark pipeline or size')
    if type(seed) is not int or not 0 <= seed < 2**64:
        raise ValueError('seed must be an unsigned 64-bit integer')
    item_count = len(_cost_items(pipeline))
    detail_width = OUTPUT_COLUMNS[pipeline][0]
    if size == 'small':
        base_orders = 64
    elif size == 'medium':
        base_orders = 50_000 // item_count
    else:
        # 每工单完整成本项；两条额外脏明细也计入阈值，表头不参与 writer 判定。
        below_orders = (CELL_SLOT_THRESHOLD - 1 - 2 * detail_width) // (item_count * detail_width)
        total_orders = below_orders + (size == 'above-threshold')
        base_orders = total_orders - len(DIRTY_CASES)
    total_orders = base_orders + len(DIRTY_CASES)
    detail_rows = total_orders * item_count + 2
    qty_input_rows = total_orders + 1
    qty_rows = total_orders - 3  # 三个无效数量、一个缺总额，加一条重复数量。
    unique_orders = total_orders - 4
    in_pool = sum(_residue_count(base_orders, index, 4) for index in (0, 1)) + len(DIRTY_CASES) - 4
    run_counts = {
        'reader_rows': detail_rows + qty_input_rows,
        'detail_rows': detail_rows,
        'qty_rows': qty_rows,
        'qty_sheet_rows': qty_rows,
        'work_order_rows': unique_orders,
        'quality_metric_count': 10,
    }
    issues = {
        'DUPLICATE_WORK_ORDER_KEY': 2,
        'MISSING_AMOUNT': 1,
        'MOH_BREAKDOWN_MISMATCH': 1,
        'NON_POSITIVE_UNIT_COST': 21,
        'TOTAL_COST_MISMATCH': 2,
        'UNMAPPED_COST_ITEM': 1,
    }
    sheets = {}
    for name, rows, columns in zip(
        SHEET_NAMES, (detail_rows, qty_rows, in_pool), OUTPUT_COLUMNS[pipeline], strict=True
    ):
        sheets[name] = {
            'data_rows': rows,
            'columns': columns,
            'xlsx_rows_including_header': rows + 1,
            'cell_slots': rows * columns,
            'low_memory': rows * columns >= CELL_SLOT_THRESHOLD,
        }
    per_month = {}
    for month_index, month in enumerate(MONTHS):
        # 每 32 个正常工单覆盖四产品 × 两生产类型 × 四月份。
        count = sum(_residue_count(base_orders, residue, 32) for residue in range(month_index * 8, month_index * 8 + 8))
        analysis_count = sum(
            _residue_count(base_orders, residue, 32)
            for residue in range(month_index * 8, month_index * 8 + 8)
            if residue % 4 in (0, 1)
        )
        dirty = month == MONTHS[-1]
        per_month[month] = {
            'detail_rows': count * item_count + (len(DIRTY_CASES) * item_count + 2 if dirty else 0),
            'qty_rows': count + (len(DIRTY_CASES) - 3 if dirty else 0),
            'analysis_rows': analysis_count + (len(DIRTY_CASES) - 4 if dirty else 0),
        }
    return {
        'schema_version': 1,
        'generator_version': 1,
        'pipeline': pipeline,
        'size': size,
        'seed': seed,
        'files': {'input': f'{pipeline}-synthetic-{size}.xlsx', 'config': 'config.toml'},
        'distribution': {
            'base_orders': base_orders,
            'dirty_orders': len(DIRTY_CASES),
            'cost_items_per_order': item_count,
            'cost_items': list(_cost_items(pipeline)),
            'months': list(MONTHS),
            'production_types': ['正常生产', '返工生产'],
            'product_cycle': [{'code': code, 'name': name} for code, name in _products(pipeline)],
            'whitelist': [{'code': code, 'name': name} for code, name in _products(pipeline)[:2]],
            'dirty_cases': list(DIRTY_CASES),
            'text_policy': 'Four repeating synthetic products; unique orders; Chinese labels and whitespace models.',
            'missing_detail_amounts': 1,
            'missing_detail_amount_rate': 1 / detail_rows,
            'missing_quantity_values': 1,
            'missing_quantity_total_amounts': 1,
        },
        'expected': {
            'run_counts': run_counts,
            'error_log_count': sum(issues.values()),
            'issue_type_counts': issues,
            'quantity_input_rows': qty_input_rows,
            'filtered_invalid_qty_count': 3,
            'filtered_missing_total_amount_count': 1,
            'duplicate_work_order_row_count': 2,
            'analysis_rows': in_pool,
            'sheets': sheets,
            'per_month': per_month,
            'low_memory_writer': any(sheet['low_memory'] for sheet in sheets.values()),
        },
        'threshold': {
            'cell_slots': CELL_SLOT_THRESHOLD,
            'comparison': '>=',
            'header_included': False,
            'target_sheet': SHEET_NAMES[0],
        },
        'runtime_verification': 'required: compare saved workbook dimensions, counts and Manifest against expected',
    }


def _residue_count(count: int, residue: int, cycle: int) -> int:
    return max(0, (count - 1 - residue) // cycle + 1)


def build_benchmark_fixture(
    output_dir: Path,
    pipeline: PipelineName,
    size: BenchmarkSize = 'small',
    seed: int = DEFAULT_SEED,
) -> dict[str, Any]:
    """Write input, complete config and spec without overwriting any existing artifact."""
    spec = benchmark_spec(pipeline, size, seed)
    output_dir = Path(output_dir)
    paths = {key: output_dir / value for key, value in spec['files'].items()}
    paths['spec'] = output_dir / 'spec.json'
    if any(path.exists() for path in paths.values()):
        raise FileExistsError('synthetic benchmark artifact already exists')
    output_dir.mkdir(parents=True, exist_ok=True)
    with paths['config'].open('x', encoding='utf-8', newline='\n') as stream:
        stream.write(synthetic_config())
    with paths['input'].open('xb') as stream:
        workbook = xlsxwriter.Workbook(
            stream,
            {
                'constant_memory': True,
                'tmpdir': str(output_dir),
                'strings_to_formulas': False,
                'strings_to_urls': False,
            },
        )
        workbook.set_properties(
            {
                'title': 'Synthetic costing benchmark',
                'author': 'Synthetic fixture builder',
                'created': datetime(2000, 1, 1),
            }
        )
        try:
            sheet = workbook.add_worksheet('SyntheticRawInput')
            sheet.write_row(0, 0, ('Synthetic benchmark; generated data only',))
            sheet.write_row(1, 0, ('Fixed seed; no real business identities',))
            sheet.write_row(2, 0, BENCHMARK_COLUMNS)
            sheet.write_row(3, 0, ('',) * len(BENCHMARK_COLUMNS))
            for row_index, row in enumerate(_benchmark_rows(spec), 4):
                sheet.write_row(row_index, 0, row)
        finally:
            workbook.close()
    spec['sha256'] = {key: _sha256(path) for key, path in paths.items() if key != 'spec'}
    with paths['spec'].open('x', encoding='utf-8', newline='\n') as stream:
        json.dump(spec, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write('\n')
    return spec


def _sha256(path: Path) -> str:
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def _benchmark_rows(spec: dict[str, Any]) -> Iterator[tuple[object, ...]]:
    pipeline = spec['pipeline']
    rng = random.Random(spec['seed'])  # noqa: S311 - fixed-seed synthetic data, not security.
    base_orders = spec['distribution']['base_orders']
    for index in range(base_orders + len(DIRTY_CASES)):
        case = '' if index < base_orders else DIRTY_CASES[index - base_orders]
        product = _products(pipeline)[index % 4] if not case else _products(pipeline)[0]
        month = MONTHS[(index // 8) % 4] if not case else MONTHS[-1]
        doc_type = '汇报入库-返工生产' if not case and (index // 4) % 2 else '汇报入库-普通生产'
        quantity: int | str | None = rng.randrange(5, 16)
        amounts: list[int | None] = [rng.randrange(10, 31) for _ in _cost_items(pipeline)]
        amounts[0] *= 5
        if not case and index % 37 == 36:
            amounts[0] *= 40  # 固定分布的离群工单，实际执行异常评分路径。
        total = sum(amounts)
        if case == 'missing-detail-amount':
            amounts[0] = None
        elif case == 'zero-labor':
            total -= amounts[1]
            amounts[1] = 0
        elif case == 'negative-material':
            total -= 2 * amounts[0]
            amounts[0] = -amounts[0]
        elif case == 'total-mismatch':
            total += 1
        elif case == 'unclassified-overhead':
            total += 1
        elif case == 'zero-quantity':
            quantity = 0
        elif case == 'negative-quantity':
            quantity = -1
        elif case == 'missing-quantity':
            quantity = None
        elif case == 'missing-total':
            total = None
        elif case == 'fractional-text':
            quantity = '10.5'
        elif case == 'zero-total':
            amounts = [0] * len(amounts)
            total = 0
        elif case == 'negative-total':
            amounts = [-amount for amount in amounts]
            total = sum(amounts)
        # 负材料案例的总额仍取固定正数，保留单项负数同时只触发一个单位成本问题。
        if case == 'negative-material' and total <= 0:
            amounts[0] = -1
            total = sum(amounts)
        order = f'SYN-{pipeline.upper()}-{index:07d}'
        center = '集成车间' if index % 7 == 0 else '合成成本中心'
        common = (f'{month[:4]}年{int(month[-2:])}期', center, *product, '  合成规格  ', order, 1, 'PCS')
        extras = ('返工生产' if '返工' in doc_type else '正常生产', doc_type)
        vendor = (None, None) if center == '集成车间' else ('SYN-VENDOR-01', '合成供应商')
        qty_row = (*common, None, quantity, _unit_cost(total, quantity), total, *extras, *vendor, None, None)
        yield qty_row
        if case == 'duplicate-quantity':
            yield qty_row
        for item, amount in zip(_cost_items(pipeline), amounts, strict=True):
            child = ('SYN-MATERIAL-01', '合成子项物料') if item == '直接材料' else (None, None)
            yield (*common, item, None, _unit_cost(amount, quantity), amount, *extras, *vendor, *child)
        if case in ('unmapped-item', 'unclassified-overhead'):
            item = '合成未映射成本' if case == 'unmapped-item' else '制造费用_合成未分类'
            yield (*common, item, None, 1, 1, *extras, *vendor, None, None)


def _unit_cost(amount: int | None, quantity: int | str | None) -> Decimal | None:
    if amount is None or quantity is None or Decimal(quantity) == 0:
        return None
    return Decimal(amount) / Decimal(quantity)

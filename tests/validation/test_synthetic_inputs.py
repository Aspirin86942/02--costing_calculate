import hashlib
import json
import sys
import tomllib
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from zipfile import ZipFile

import pytest
from openpyxl import load_workbook

from tools.validation import synthetic_inputs
from tools.validation.synthetic_benchmark import (
    BENCHMARK_COLUMNS,
    BENCHMARK_SIZES,
    CELL_SLOT_THRESHOLD,
    FULL_SK_HEADERS,
    build_full_sk_fixture,
    synthetic_config,
)
from tools.validation.synthetic_inputs import RAW_COLUMNS, benchmark_spec, build_benchmark_fixture, build_raw_fixture


def test_synthetic_inputs_cover_both_pipeline_specific_cost_items(tmp_path: Path) -> None:
    for pipeline, expected_item in (('gb', '委外加工费'), ('sk', '软件费用')):
        path = tmp_path / f'{pipeline}.xlsx'

        build_raw_fixture(path, pipeline, 'small')

        workbook = load_workbook(path, read_only=True, data_only=True)
        try:
            sheet = workbook.active
            assert tuple(cell.value for cell in sheet[3]) == RAW_COLUMNS
            values = {row[8].value for row in sheet.iter_rows(min_row=5)}
            assert expected_item in values
        finally:
            workbook.close()


def test_synthetic_config_is_complete_and_contains_only_synthetic_products() -> None:
    config = tomllib.loads(synthetic_config())

    assert config['schema_version'] == 1
    assert set(config['pipelines']) == {'gb', 'sk'}
    for pipeline, rules in config['pipelines'].items():
        assert rules['standalone_cost_items'] == (['委外加工费'] if pipeline == 'gb' else ['委外加工费', '软件费用'])
        assert [product['display_order'] for product in rules['product_order']] == [10, 20]
        assert [product['code'] for product in rules['product_order']] == [
            f'SYN-{pipeline.upper()}-002',
            f'SYN-{pipeline.upper()}-001',
        ]
        assert all(product['name'].startswith('合成') for product in rules['product_order'])


@pytest.mark.parametrize('pipeline', ['gb', 'sk'])
def test_benchmark_input_contains_linked_costs_and_dirty_boundaries(tmp_path: Path, pipeline: str) -> None:
    spec = build_benchmark_fixture(tmp_path, pipeline)
    workbook = load_workbook(tmp_path / spec['files']['input'], read_only=True, data_only=True)
    try:
        sheet = workbook.active
        assert tuple(cell.value for cell in sheet[3]) == BENCHMARK_COLUMNS
        rows = [dict(zip(BENCHMARK_COLUMNS, row, strict=True)) for row in sheet.iter_rows(min_row=5, values_only=True)]
    finally:
        workbook.close()
    assert len(rows) == spec['expected']['run_counts']['reader_rows']
    quantities = [row for row in rows if row['成本项目名称'] is None]
    details = [row for row in rows if row['成本项目名称'] is not None]
    assert len(quantities) == spec['expected']['quantity_input_rows']
    selected_products = {(product['code'], product['name']) for product in spec['distribution']['whitelist']}
    analysis_keys = {
        (row['年期'], row['产品编码'], row['工单编号'], str(row['工单行号']))
        for row in quantities
        if row['本期完工数量'] is not None
        and Decimal(str(row['本期完工数量'])) > 0
        and row['本期完工金额'] is not None
        and (row['产品编码'], row['产品名称']) in selected_products
    }
    assert spec['expected']['run_counts']['work_order_rows'] == len(analysis_keys)
    assert len(details) == spec['expected']['run_counts']['detail_rows']
    detail_items = defaultdict(set)
    for row in details:
        detail_items[row['工单编号']].add(row['成本项目名称'])
        if row['成本项目名称'] == '直接材料':
            assert row['子项物料编码'].startswith('SYN-')
    expected_items = set(spec['distribution']['cost_items'])
    assert all(expected_items <= detail_items[row['工单编号']] for row in quantities)
    assert {row['生产类型'] for row in rows} == {'正常生产', '返工生产'}
    assert {row['年期'] for row in rows} == {'2025年12期', '2026年1期', '2026年2期', '2026年3期'}
    assert {0, -1, None, '10.5'} <= {row['本期完工数量'] for row in quantities}
    assert any(row['本期完工金额'] is None for row in details)
    assert any(row['本期完工金额'] < 0 for row in details if row['本期完工金额'] is not None)
    assert any(row['本期完工金额'] == 0 for row in details)
    pool = {(row['产品编码'], row['产品名称']) for row in quantities}
    assert len(pool) == 4
    assert sum(code == f'SYN-{pipeline.upper()}-002' for code, _ in pool) == 2
    assert json.loads((tmp_path / 'spec.json').read_text(encoding='utf-8')) == spec


def test_benchmark_artifact_hashes_are_reproducible_and_seed_changes_input(tmp_path: Path) -> None:
    first = build_benchmark_fixture(tmp_path / 'first', 'gb', seed=7)
    second = build_benchmark_fixture(tmp_path / 'second', 'gb', seed=7)
    other_seed = build_benchmark_fixture(tmp_path / 'other', 'gb', seed=8)

    for name in (first['files']['input'], 'config.toml', 'spec.json'):
        first_bytes = (tmp_path / 'first' / name).read_bytes()
        assert first_bytes == (tmp_path / 'second' / name).read_bytes()
    assert first['sha256'] == second['sha256']
    with ZipFile(tmp_path / 'first' / first['files']['input']) as archive:
        assert {entry.date_time for entry in archive.infolist()} == {(1980, 1, 31, 0, 0, 0)}
        core_properties = archive.read('docProps/core.xml').decode('utf-8')
        assert core_properties.count('2000-01-01T00:00:00Z') == 2
    assert first['sha256']['input'] != other_seed['sha256']['input']
    assert first['sha256']['config'] == other_seed['sha256']['config']
    assert first['expected'] == other_seed['expected']
    assert (
        hashlib.sha256((tmp_path / 'first' / first['files']['input']).read_bytes()).hexdigest()
        == first['sha256']['input']
    )


@pytest.mark.parametrize('existing_name', ['gb-synthetic-small.xlsx', 'config.toml', 'spec.json'])
def test_benchmark_refuses_existing_artifacts_without_writing(tmp_path: Path, existing_name: str) -> None:
    (tmp_path / existing_name).write_bytes(b'keep-existing')

    with pytest.raises(FileExistsError):
        build_benchmark_fixture(tmp_path, 'gb')

    assert list(tmp_path.iterdir()) == [tmp_path / existing_name]
    assert (tmp_path / existing_name).read_bytes() == b'keep-existing'


@pytest.mark.parametrize('pipeline', ['gb', 'sk'])
def test_benchmark_specs_cover_medium_and_exact_threshold_sides(pipeline: str) -> None:
    medium = benchmark_spec(pipeline, 'medium')
    assert 50_000 <= medium['expected']['run_counts']['detail_rows'] < 50_200
    below = benchmark_spec(pipeline, 'below-threshold')
    above = benchmark_spec(pipeline, 'above-threshold')
    target = below['threshold']['target_sheet']
    below_shape = below['expected']['sheets'][target]
    above_shape = above['expected']['sheets'][target]
    assert below_shape['cell_slots'] < CELL_SLOT_THRESHOLD < above_shape['cell_slots']
    assert CELL_SLOT_THRESHOLD - below_shape['cell_slots'] < below_shape['columns'] * 10
    assert above_shape['data_rows'] - below_shape['data_rows'] == below['distribution']['cost_items_per_order']
    assert below['expected']['low_memory_writer'] is False
    assert above['expected']['low_memory_writer'] is True
    assert all(not sheet['low_memory'] for sheet in below['expected']['sheets'].values())
    for size in BENCHMARK_SIZES:
        spec = benchmark_spec(pipeline, size)
        assert spec['expected']['run_counts']['reader_rows'] + 4 <= 1_048_576
        assert (
            sum(month['detail_rows'] for month in spec['expected']['per_month'].values())
            == spec['expected']['run_counts']['detail_rows']
        )
        assert (
            sum(month['analysis_rows'] for month in spec['expected']['per_month'].values())
            == spec['expected']['analysis_rows']
        )
        assert spec['runtime_verification'].startswith('required:')


@pytest.mark.parametrize('declared_a1', [True, False])
def test_full_sk_fixture_preserves_width_material_totals_and_real_coordinates(
    tmp_path: Path,
    declared_a1: bool,
) -> None:
    spec = build_full_sk_fixture(tmp_path, orders=4, declared_a1=declared_a1)
    path = tmp_path / spec['files']['input']
    assert len(FULL_SK_HEADERS) == 58
    with ZipFile(path) as archive:
        assert 'xl/sharedStrings.xml' in archive.namelist()
        xml = archive.read('xl/worksheets/sheet1.xml')
        assert (b'<dimension ref="A1"' in xml) is declared_a1
        assert b'r="BF53"' not in xml  # 最后一行仅有合计；宽度来自真实数据行。
        assert b'r="BF52"' in xml
    workbook = load_workbook(path, read_only=True, data_only=True)
    try:
        sheet = workbook.active
        sheet.reset_dimensions()
        headers = list(sheet.iter_rows(min_row=3, max_row=4, max_col=58, values_only=True))
        assert list(zip(*headers, strict=True)) == list(FULL_SK_HEADERS)
        rows = list(sheet.iter_rows(min_row=5, max_col=58, values_only=True))
        assert len(rows) == 49
        assert [rows[index][46] for index in (0, 1, 2, 3)] == [100, 40, 25, 35]
        assert rows[0][34] == 999
        assert rows[0][43] == 10
        assert rows[-1][0] == '合计'
    finally:
        workbook.close()
    expected = spec['expected']
    assert expected['run_counts']['reader_rows'] == 49
    assert expected['run_counts']['work_order_rows'] == 2
    assert expected['issue_type_counts'] == {'MATERIAL_DETAIL_MISMATCH': 4}
    assert json.loads((tmp_path / 'spec.json').read_text(encoding='utf-8')) == spec


def test_full_sk_fixture_is_reproducible_and_refuses_existing_artifacts(tmp_path: Path) -> None:
    first = build_full_sk_fixture(tmp_path / 'first', orders=4, seed=7)
    second = build_full_sk_fixture(tmp_path / 'second', orders=4, seed=7)
    assert first == second
    original = (tmp_path / 'first' / first['files']['input']).read_bytes()
    with pytest.raises(FileExistsError):
        build_full_sk_fixture(tmp_path / 'first', orders=4, seed=8)
    assert (tmp_path / 'first' / first['files']['input']).read_bytes() == original


@pytest.mark.parametrize(
    'options',
    [
        {'orders': True},
        {'orders': 3},
        {'orders': 50_001},
        {'seed': -1},
        {'seed': True},
        {'declared_a1': 'yes'},
    ],
)
def test_full_sk_fixture_rejects_invalid_options_before_writing(tmp_path: Path, options: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        build_full_sk_fixture(tmp_path, **options)
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('seed', [-1, 2**64, True])
def test_benchmark_rejects_invalid_seeds_before_writing(tmp_path: Path, seed: int) -> None:
    with pytest.raises(ValueError):
        build_benchmark_fixture(tmp_path, 'gb', seed=seed)
    assert not list(tmp_path.iterdir())


def test_synthetic_cli_invalid_arguments_are_structured_and_redacted(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(sys, 'argv', ['synthetic_inputs', '--pipeline', '/private/value'])

    assert synthetic_inputs.main() == 2
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload['error_code'] == 'SYNTHETIC_INPUT_ERROR'
    assert payload['message']
    assert payload['retryable'] is False
    assert '/private/' not in captured.out
    assert captured.err == ''

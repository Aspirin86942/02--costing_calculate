"""测试主 ETL 输出与基础行为。"""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
from openpyxl import load_workbook

from src.analytics.pq_analysis import AnalysisArtifacts, FlatSheet, ProductAnomalySection
from src.etl.costing_etl import CostingWorkbookETL


def _build_header_map(worksheet, header_row: int = 1) -> dict[str, int]:
    return {
        worksheet.cell(header_row, col_idx).value: col_idx
        for col_idx in range(1, worksheet.max_column + 1)
        if worksheet.cell(header_row, col_idx).value is not None
    }


def _find_title_row(worksheet, title: str) -> int:
    for row_idx in range(1, worksheet.max_row + 1):
        if worksheet.cell(row_idx, 1).value == title:
            return row_idx
    raise AssertionError(f'未找到标题行: {title}')


def _rgb_suffix(color) -> str | None:
    rgb = getattr(color, 'rgb', None)
    if rgb is None:
        return None
    return rgb[-6:]


class TestCostingWorkbookETL:
    """测试 CostingWorkbookETL 类。"""

    def test_etl_initialization(self) -> None:
        """测试 ETL 初始化。"""
        etl = CostingWorkbookETL(skip_rows=2)
        assert etl.skip_rows == 2
        assert hasattr(etl, 'process_file')
        assert hasattr(etl, 'FILL_COLS')
        assert hasattr(etl, 'DETAIL_COLS')
        assert hasattr(etl, 'QTY_COLS')

    def test_process_file_not_found(self) -> None:
        """测试文件不存在时返回 False。"""
        etl = CostingWorkbookETL(skip_rows=2)
        assert etl.process_file(Path('missing.xlsx'), Path('output.xlsx')) is False

    def test_auto_rename_columns(self) -> None:
        """测试列名自动识别。"""
        etl = CostingWorkbookETL(skip_rows=2)
        df = pd.DataFrame(columns=['物料编码', '成本项目', '其他列'])

        col_map = etl._auto_rename_columns(df)

        assert col_map['物料编码'] == '子项物料编码'
        assert col_map['成本项目'] == '成本项目名称'

    def test_remove_total_rows(self) -> None:
        """测试剔除合计行。"""
        etl = CostingWorkbookETL(skip_rows=2)
        df = pd.DataFrame({'年期': ['2024年01期', '合计', '2024年03期'], '数据': [1, 2, 3]})

        result = etl._remove_total_rows(df)

        assert len(result) == 2
        assert '合计' not in result['年期'].tolist()

    def test_forward_fill_with_rules_skip_vendor_for_integrated_workshop(self) -> None:
        """测试集成车间下供应商字段不向下填充。"""
        etl = CostingWorkbookETL(skip_rows=2)
        df_raw = pd.DataFrame(
            {
                '成本中心名称': ['集成车间', None],
                '产品编码': ['P001', None],
                '供应商编码': ['V001', None],
                '供应商名称': ['供应商A', None],
            }
        )

        result = etl._forward_fill_with_rules(df_raw)

        assert result.loc[1, '成本中心名称'] == '集成车间'
        assert result.loc[1, '产品编码'] == 'P001'
        assert pd.isna(result.loc[1, '供应商编码'])
        assert pd.isna(result.loc[1, '供应商名称'])

    def test_filter_fact_df_for_analysis_uses_whitelist_order(self) -> None:
        """测试分析白名单过滤和顺序。"""
        etl = CostingWorkbookETL(skip_rows=2)
        fact_df = pd.DataFrame(
            [
                {
                    'period': '2025-01',
                    'product_code': 'GB_C.D.B0041AA',
                    'product_name': 'BMS-1100W驱动器',
                    'cost_bucket': 'direct_material',
                    'amount': 220,
                    'qty': 20,
                    'price': 11,
                },
                {
                    'period': '2025-01',
                    'product_code': 'GB_C.D.B0040AA',
                    'product_name': 'BMS-750W驱动器',
                    'cost_bucket': 'direct_material',
                    'amount': 100,
                    'qty': 10,
                    'price': 10,
                },
                {
                    'period': '2025-01',
                    'product_code': 'P001',
                    'product_name': '产品A',
                    'cost_bucket': 'direct_material',
                    'amount': 100,
                    'qty': 10,
                    'price': 10,
                },
            ]
        )

        result = etl._filter_fact_df_for_analysis(fact_df)

        assert result['product_code'].tolist() == ['GB_C.D.B0040AA', 'GB_C.D.B0041AA']


def test_process_file_writes_v3_analysis_sheets(tmp_path) -> None:
    """测试 process_file 会输出 v3 相关 sheet 与基础样式。"""
    etl = CostingWorkbookETL(skip_rows=2)
    input_path = tmp_path / 'input.xlsx'
    output_path = tmp_path / 'output.xlsx'

    df_raw = pd.DataFrame({'子项物料编码': ['MAT-001'], '成本项目名称': ['直接材料'], '年期': ['2025年1期']})
    df_detail = pd.DataFrame(
        [
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'GB_C.D.B0040AA',
                '产品名称': 'BMS-750W驱动器',
                '规格型号': 'S-01',
                '工单编号': 'WO-001',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '直接材料',
                '本期完工单位成本': 10,
                '本期完工金额': 100,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'GB_C.D.B0040AA',
                '产品名称': 'BMS-750W驱动器',
                '规格型号': 'S-01',
                '工单编号': 'WO-001',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '直接人工',
                '本期完工单位成本': 2,
                '本期完工金额': 20,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'GB_C.D.B0040AA',
                '产品名称': 'BMS-750W驱动器',
                '规格型号': 'S-01',
                '工单编号': 'WO-001',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '制造费用-人工',
                '本期完工单位成本': 3,
                '本期完工金额': 30,
            },
        ]
    )
    df_qty = pd.DataFrame(
        [
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'GB_C.D.B0040AA',
                '产品名称': 'BMS-750W驱动器',
                '规格型号': 'S-01',
                '工单编号': 'WO-001',
                '工单行号': 1,
                '基本单位': 'PCS',
                '本期完工数量': 10,
                '本期完工金额': 150,
            }
        ]
    )

    with (
        patch('src.etl.costing_etl.pd.read_excel', return_value=df_raw),
        patch.object(CostingWorkbookETL, '_split_sheets', return_value=(df_detail, df_qty)),
    ):
        assert etl.process_file(input_path, output_path) is True

    xls = pd.ExcelFile(output_path, engine='openpyxl')
    expected_sheets = {
        '成本明细',
        '产品数量统计',
        '直接材料_价量比',
        '直接人工_价量比',
        '制造费用_价量比',
        '按工单按产品异常值分析',
        '按产品异常值分析',
        '数据质量校验',
        'error_log',
    }
    assert expected_sheets.issubset(set(xls.sheet_names))

    wb = load_workbook(output_path)
    ws_detail = wb['成本明细']
    detail_headers = _build_header_map(ws_detail)
    assert ws_detail.freeze_panes == 'A2'
    assert ws_detail.cell(2, detail_headers['本期完工单位成本']).number_format == '#,##0.00'
    assert ws_detail.cell(2, detail_headers['本期完工金额']).number_format == '#,##0.00'
    assert isinstance(ws_detail.cell(2, detail_headers['本期完工单位成本']).value, (int, float))

    ws_qty = wb['产品数量统计']
    qty_headers = _build_header_map(ws_qty)
    qty_decimal_columns = [
        '本期完工金额',
        '本期完工直接材料合计完工金额',
        '本期完工直接人工合计完工金额',
        '本期完工制造费用合计完工金额',
        '本期完工制造费用_其他合计完工金额',
        '本期完工制造费用_人工合计完工金额',
        '本期完工制造费用_机物料及低耗合计完工金额',
        '本期完工制造费用_折旧合计完工金额',
        '本期完工制造费用_水电费合计完工金额',
        '直接材料单位完工金额',
        '直接人工单位完工金额',
        '制造费用单位完工金额',
        '制造费用_其他单位完工成本',
        '制造费用_人工单位完工成本',
        '制造费用_机物料及低耗单位完工成本',
        '制造费用_折旧单位完工成本',
        '制造费用_水电费单位完工成本',
    ]
    assert ws_qty.freeze_panes == 'A2'
    for column_name in qty_decimal_columns:
        cell = ws_qty.cell(2, qty_headers[column_name])
        assert cell.number_format == '#,##0.00'
        assert isinstance(cell.value, (int, float))

    ws_price = wb['直接材料_价量比']
    assert ws_price['A1'].value == '直接材料完工金额'
    assert ws_price.freeze_panes == 'C3'
    assert ws_price.auto_filter.ref is not None
    for title in ['直接材料完工金额', '直接材料完工数量', '直接材料完工单价']:
        title_row = _find_title_row(ws_price, title)
        header_row = title_row + 1
        data_row = header_row + 1
        month_col = _build_header_map(ws_price, header_row)['2025年01期']
        cell = ws_price.cell(data_row, month_col)
        assert cell.number_format == '#,##0.00'
        assert isinstance(cell.value, (int, float))

    ws_work_order = wb['按工单按产品异常值分析']
    assert ws_work_order['A1'].value == '月份'
    assert ws_work_order['I2'].value == 10
    assert ws_work_order.freeze_panes == 'A2'
    assert ws_work_order.auto_filter.ref is not None

    ws_product = wb['按产品异常值分析']
    assert ws_product['A1'].value == '四、按单个产品异常值分析'
    assert ws_product['A3'].value == '产品编码'
    assert ws_product['A4'].value == 'GB_C.D.B0040AA'
    assert ws_product.freeze_panes == 'A6'

    ws_quality = wb['数据质量校验']
    assert ws_quality['A1'].value == '检查类别'
    assert ws_quality.freeze_panes == 'A2'


def test_process_file_highlights_work_order_value_and_flag_cells(tmp_path) -> None:
    """测试工单异常页会同步高亮值列和标记列。"""
    etl = CostingWorkbookETL(skip_rows=2)
    input_path = tmp_path / 'input.xlsx'
    output_path = tmp_path / 'output.xlsx'

    df_raw = pd.DataFrame({'子项物料编码': ['MAT-001'], '成本项目名称': ['直接材料'], '年期': ['2025年1期']})
    df_detail = pd.DataFrame(
        [
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'GB_C.D.B0040AA',
                '产品名称': 'BMS-750W驱动器',
                '规格型号': 'S-01',
                '工单编号': 'WO-001',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '直接材料',
                '本期完工金额': 100,
            }
        ]
    )
    df_qty = pd.DataFrame(
        [
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'GB_C.D.B0040AA',
                '产品名称': 'BMS-750W驱动器',
                '规格型号': 'S-01',
                '工单编号': 'WO-001',
                '工单行号': 1,
                '基本单位': 'PCS',
                '本期完工数量': 10,
                '本期完工金额': 100,
            }
        ]
    )
    work_order_df = pd.DataFrame(
        [
            {
                '月份': '2025年01期',
                '成本中心': '中心A',
                '产品编码': 'GB_C.D.B0040AA',
                '产品名称': 'BMS-750W驱动器',
                '规格型号': 'S-01',
                '工单编号': 'WO-001',
                '工单行': '1',
                '基本单位': 'PCS',
                '本期完工数量': 10,
                '直接材料单位完工成本': 18.0,
                '直接人工单位完工成本': 2.0,
                '制造费用单位完工成本': 3.0,
                '制造费用_其他单位完工成本': 0.0,
                '制造费用_人工单位完工成本': 30.0,
                '制造费用_机物料及低耗单位完工成本': 0.0,
                '制造费用_折旧单位完工成本': 0.0,
                '制造费用_水电费单位完工成本': 0.0,
                '直接材料异常标记': '关注',
                '直接人工异常标记': '正常',
                '制造费用异常标记': '正常',
                '制造费用_其他异常标记': '正常',
                '制造费用_人工异常标记': '高度可疑',
                '制造费用_机物料及低耗异常标记': '正常',
                '制造费用_折旧异常标记': '正常',
                '制造费用_水电费异常标记': '正常',
            }
        ]
    )
    work_order_column_types = {
        '月份': 'text',
        '成本中心': 'text',
        '产品编码': 'text',
        '产品名称': 'text',
        '规格型号': 'text',
        '工单编号': 'text',
        '工单行': 'text',
        '基本单位': 'text',
        '本期完工数量': 'qty',
        '直接材料单位完工成本': 'price',
        '直接人工单位完工成本': 'price',
        '制造费用单位完工成本': 'price',
        '制造费用_其他单位完工成本': 'price',
        '制造费用_人工单位完工成本': 'price',
        '制造费用_机物料及低耗单位完工成本': 'price',
        '制造费用_折旧单位完工成本': 'price',
        '制造费用_水电费单位完工成本': 'price',
        '直接材料异常标记': 'text',
        '直接人工异常标记': 'text',
        '制造费用异常标记': 'text',
        '制造费用_其他异常标记': 'text',
        '制造费用_人工异常标记': 'text',
        '制造费用_机物料及低耗异常标记': 'text',
        '制造费用_折旧异常标记': 'text',
        '制造费用_水电费异常标记': 'text',
    }
    artifacts = AnalysisArtifacts(
        fact_df=pd.DataFrame(
            [
                {
                    'period': '2025-01',
                    'product_code': 'GB_C.D.B0040AA',
                    'product_name': 'BMS-750W驱动器',
                    'cost_bucket': 'direct_material',
                    'amount': 100,
                    'qty': 10,
                    'price': 10,
                }
            ]
        ),
        qty_sheet_df=df_qty.copy(),
        work_order_sheet=FlatSheet(data=work_order_df, column_types=work_order_column_types),
        product_anomaly_sections=[
            ProductAnomalySection(
                product_code='GB_C.D.B0040AA',
                product_name='BMS-750W驱动器',
                data=pd.DataFrame([{'月份': '2025年01期', '总成本': 100.0, '完工数量': 10, '单位成本': 10.0}]),
                column_types={'月份': 'text', '总成本': 'amount', '完工数量': 'qty', '单位成本': 'price'},
                amount_columns=['总成本'],
                outlier_cells=set(),
            )
        ],
        quality_sheet=FlatSheet(
            data=pd.DataFrame([{'检查类别': '行数勾稽', '指标': '样例', '数值': '1', '说明': '测试'}]),
            column_types={'检查类别': 'text', '指标': 'text', '数值': 'text', '说明': 'text'},
        ),
        error_log=pd.DataFrame(),
    )

    with (
        patch('src.etl.costing_etl.pd.read_excel', return_value=df_raw),
        patch.object(CostingWorkbookETL, '_split_sheets', return_value=(df_detail, df_qty)),
        patch('src.etl.costing_etl.build_report_artifacts', return_value=artifacts),
    ):
        assert etl.process_file(input_path, output_path) is True

    wb = load_workbook(output_path)
    ws_work_order = wb['按工单按产品异常值分析']
    headers = _build_header_map(ws_work_order)

    dm_value = ws_work_order.cell(2, headers['直接材料单位完工成本'])
    dm_flag = ws_work_order.cell(2, headers['直接材料异常标记'])
    moh_labor_value = ws_work_order.cell(2, headers['制造费用_人工单位完工成本'])
    moh_labor_flag = ws_work_order.cell(2, headers['制造费用_人工异常标记'])

    assert _rgb_suffix(dm_value.fill.fgColor) == 'DDEBF7'
    assert _rgb_suffix(dm_flag.fill.fgColor) == 'DDEBF7'
    assert _rgb_suffix(moh_labor_value.fill.fgColor) == '4472C4'
    assert _rgb_suffix(moh_labor_flag.fill.fgColor) == '4472C4'
    assert _rgb_suffix(moh_labor_value.font.color) == 'FFFFFF'
    assert _rgb_suffix(moh_labor_flag.font.color) == 'FFFFFF'

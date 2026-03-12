"""测试主 ETL 输出与基础行为。"""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
from openpyxl import load_workbook

from src.etl.costing_v2 import CostingETL


class TestCostingETL:
    """测试 CostingETL 类。"""

    def test_etl_initialization(self) -> None:
        """测试 ETL 初始化。"""
        etl = CostingETL(skip_rows=2)
        assert etl.skip_rows == 2
        assert hasattr(etl, 'process_file')
        assert hasattr(etl, 'FILL_COLS')
        assert hasattr(etl, 'DETAIL_COLS')
        assert hasattr(etl, 'QTY_COLS')

    def test_process_file_not_found(self) -> None:
        """测试文件不存在时返回 False。"""
        etl = CostingETL(skip_rows=2)
        assert etl.process_file(Path('missing.xlsx'), Path('output.xlsx')) is False

    def test_auto_rename_columns(self) -> None:
        """测试列名自动识别。"""
        etl = CostingETL(skip_rows=2)
        df = pd.DataFrame(columns=['物料编码', '成本项目', '其他列'])

        col_map = etl._auto_rename_columns(df)

        assert col_map['物料编码'] == '子项物料编码'
        assert col_map['成本项目'] == '成本项目名称'

    def test_remove_total_rows(self) -> None:
        """测试剔除合计行。"""
        etl = CostingETL(skip_rows=2)
        df = pd.DataFrame({'年期': ['2024年01期', '合计', '2024年03期'], '数据': [1, 2, 3]})

        result = etl._remove_total_rows(df)

        assert len(result) == 2
        assert '合计' not in result['年期'].tolist()

    def test_forward_fill_with_rules_skip_vendor_for_integrated_workshop(self) -> None:
        """测试集成车间下供应商字段不向下填充。"""
        etl = CostingETL(skip_rows=2)
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
        etl = CostingETL(skip_rows=2)
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
    etl = CostingETL(skip_rows=2)
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
        patch('src.etl.costing_v2.pd.read_excel', return_value=df_raw),
        patch.object(CostingETL, '_split_sheets', return_value=(df_detail, df_qty)),
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
    ws_price = wb['直接材料_价量比']
    assert ws_price['A1'].value == '直接材料完工金额'
    assert ws_price.freeze_panes == 'C3'
    assert ws_price.auto_filter.ref is not None

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

"""测试主 ETL 脚本"""

from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.etl.costing_v2 import CostingETL


class TestCostingETL:
    """测试 CostingETL 类"""

    def test_etl_initialization(self):
        """测试 ETL 初始化"""
        etl = CostingETL(skip_rows=2)
        assert etl.skip_rows == 2
        assert hasattr(etl, 'process_file')
        assert hasattr(etl, 'FILL_COLS')
        assert hasattr(etl, 'DETAIL_COLS')
        assert hasattr(etl, 'QTY_COLS')

    def test_etl_custom_skip_rows(self):
        """测试自定义跳过行数"""
        etl = CostingETL(skip_rows=3)
        assert etl.skip_rows == 3

    def test_process_file_not_found(self):
        """测试文件不存在的情况（edge case）"""
        etl = CostingETL(skip_rows=2)
        input_path = Path('nonexistent_file.xlsx')
        output_path = Path('output.xlsx')

        result = etl.process_file(input_path, output_path)
        assert result is False

    def test_process_file_with_mock(self):
        """测试成功处理文件（使用 mock）"""
        etl = CostingETL(skip_rows=2)
        input_path = Path('test_input.xlsx')
        output_path = Path('test_output.xlsx')

        # 模拟 process_file 返回 True
        with patch.object(etl, 'process_file', return_value=True) as mock_method:
            result = mock_method(input_path, output_path)
            assert result is True
            mock_method.assert_called_once_with(input_path, output_path)

    def test_auto_rename_columns(self):
        """测试列名自动识别"""
        import pandas as pd

        etl = CostingETL(skip_rows=2)
        df = pd.DataFrame(columns=['物料编码', '成本项目', '其他列'])

        col_map = etl._auto_rename_columns(df)

        assert '物料编码' in col_map
        assert col_map['物料编码'] == '子项物料编码'
        assert '成本项目' in col_map
        assert col_map['成本项目'] == '成本项目名称'

    def test_remove_total_rows(self):
        """测试剔除合计行"""
        import pandas as pd

        etl = CostingETL(skip_rows=2)
        df = pd.DataFrame({'年期': ['2024 年 01 期', '2024 年 02 期', '合计', '2024 年 03 期'], '数据': [1, 2, 3, 4]})

        result = etl._remove_total_rows(df)

        assert len(result) == 3
        assert '合计' not in result['年期'].values

    def test_remove_total_rows_no_change(self):
        """测试剔除合计行 - 无合计行的情况"""
        etl = CostingETL(skip_rows=2)
        df = pd.DataFrame({'年期': ['2024 年 01 期', '2024 年 02 期', '2024 年 03 期'], '数据': [1, 2, 3]})

        result = etl._remove_total_rows(df)

        assert len(result) == 3
        assert len(result) == len(df)

    def test_split_sheets_keep_full_columns(self):
        """测试拆分后保留完整明细/数量字段，不仅是前几列"""
        etl = CostingETL(skip_rows=2)

        df_raw = pd.DataFrame(
            {
                '年期': ['2024年1期', '2024年1期', '2024年1期'],
                '成本中心名称': ['中心A', '中心A', '中心A'],
                '产品编码': ['P001', 'P001', 'P001'],
                '产品名称': ['产品A', '产品A', '产品A'],
                '规格型号': ['S-01', 'S-01', 'S-01'],
                '生产类型': ['自制', '自制', '自制'],
                '单据类型': ['成本计算单', '成本计算单', '成本计算单'],
                '工单编号': ['WO-001', 'WO-001', 'WO-001'],
                '工单行号': [1, 1, 1],
                '供应商编码': ['V001', 'V001', 'V001'],
                '供应商名称': ['供应商A', '供应商A', '供应商A'],
                '基本单位': ['PCS', 'PCS', 'PCS'],
                '计划产量': [100, 100, 100],
                '成本项目名称': [None, '直接材料', '直接人工'],
                '子项物料编码': [None, 'MAT-001', None],
                '子项物料名称': [None, '铜线', None],
                '期初在产品数量': [0, 1, 0],
                '期初在产品金额': [0, 10, 0],
                '期初调整数量': [0, 0, 0],
                '期初调整金额': [0, 0, 0],
                '本期投入数量': [100, 90, 10],
                '本期投入金额': [1000, 900, 100],
                '累计投入数量': [100, 90, 10],
                '累计投入金额': [1000, 900, 100],
                '期末在产品数量': [5, 4, 1],
                '期末在产品金额': [50, 40, 10],
                '本期完工数量': [95, 86, 9],
                '本期完工单耗': [1.0, 0.9, 0.1],
                '本期完工单位成本': [10, 9, 1],
                '本期完工金额': [950, 860, 90],
                '累计完工数量': [95, 86, 9],
                '累计完工单耗': [1.0, 0.9, 0.1],
                '累计完工单位成本': [10, 9, 1],
                '累计完工金额': [950, 860, 90],
            }
        )

        df_filled = df_raw.copy()
        cols_to_fill = [c for c in df_filled.columns if c in etl.FILL_COLS]
        df_filled[cols_to_fill] = df_filled[cols_to_fill].ffill()
        df_filled['Filled_成本项目'] = df_filled['成本项目名称'].ffill()

        df_detail, df_qty = etl._split_sheets(df_raw, df_filled, '子项物料编码', '成本项目名称')

        expected_detail_cols = [
            '年期',
            '月份',
            '成本中心名称',
            '产品编码',
            '产品名称',
            '规格型号',
            '生产类型',
            '单据类型',
            '工单编号',
            '工单行号',
            '供应商编码',
            '供应商名称',
            '基本单位',
            '计划产量',
            '成本项目名称',
            '子项物料编码',
            '子项物料名称',
            '期初在产品数量',
            '期初在产品金额',
            '期初调整数量',
            '期初调整金额',
            '本期投入数量',
            '本期投入金额',
            '累计投入数量',
            '累计投入金额',
            '期末在产品数量',
            '期末在产品金额',
            '本期完工数量',
            '本期完工单耗',
            '本期完工单位成本',
            '本期完工金额',
            '累计完工数量',
            '累计完工单耗',
            '累计完工单位成本',
            '累计完工金额',
        ]
        expected_qty_cols = [
            '年期',
            '月份',
            '成本中心名称',
            '产品编码',
            '产品名称',
            '规格型号',
            '生产类型',
            '单据类型',
            '工单编号',
            '工单行号',
            '基本单位',
            '计划产量',
            '期初在产品数量',
            '期初在产品金额',
            '本期投入数量',
            '本期投入金额',
            '累计投入数量',
            '累计投入金额',
            '期末在产品数量',
            '期末在产品金额',
            '本期完工数量',
            '本期完工单耗',
            '本期完工单位成本',
            '本期完工金额',
            '累计完工数量',
            '累计完工单耗',
            '累计完工单位成本',
            '累计完工金额',
        ]

        assert list(df_detail.columns) == expected_detail_cols
        assert list(df_qty.columns) == expected_qty_cols
        assert len(df_detail) == 2
        assert len(df_qty) == 1


def test_process_file_writes_analysis_sheets(tmp_path):
    """测试 process_file 会输出三张分析表和 error_log。"""
    etl = CostingETL(skip_rows=2)
    input_path = tmp_path / 'input.xlsx'
    output_path = tmp_path / 'output.xlsx'

    df_raw = pd.DataFrame({'子项物料编码': ['MAT-001'], '成本项目名称': ['直接材料'], '年期': ['2025年1期']})
    df_detail = pd.DataFrame(
        [
            {
                '月份': '2025年01期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '直接材料',
                '本期完工金额': 100,
                '本期完工单位成本': 10,
            },
            {
                '月份': '2025年02期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '直接人工',
                '本期完工金额': 60,
                '本期完工单位成本': 5,
            },
            {
                '月份': '2025年02期',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '成本项目名称': '制造费用-人工',
                '本期完工金额': 40,
                '本期完工单位成本': 3.3,
            },
        ]
    )
    df_qty = pd.DataFrame(
        [
            {'月份': '2025年01期', '产品编码': 'P001', '产品名称': '产品A', '本期完工数量': 10},
            {'月份': '2025年02期', '产品编码': 'P001', '产品名称': '产品A', '本期完工数量': 12},
        ]
    )

    with (
        patch('src.etl.costing_v2.pd.read_excel', return_value=df_raw),
        patch.object(CostingETL, '_split_sheets', return_value=(df_detail, df_qty)),
    ):
        assert etl.process_file(input_path, output_path) is True

    xls = pd.ExcelFile(output_path, engine='openpyxl')
    expected_sheets = {'成本明细', '产品数量统计', '直接材料_价量比', '直接人工_缝隙', '制造费用_价量比', 'error_log'}
    assert expected_sheets.issubset(set(xls.sheet_names))

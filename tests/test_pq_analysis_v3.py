"""测试 V3 分析数据逻辑。"""

from decimal import Decimal

import pandas as pd

from src.analytics.pq_analysis import (
    QTY_CHECK_STATUS,
    QTY_DM_AMOUNT,
    QTY_DM_UNIT_COST,
    QTY_MOH_LABOR_AMOUNT,
    QTY_MOH_MATCH,
    QTY_TOTAL_MATCH,
    build_report_artifacts,
)


def _build_base_detail_df() -> pd.DataFrame:
    return pd.DataFrame(
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


def test_build_report_artifacts_enriches_qty_sheet() -> None:
    """测试数量页补强金额、单位成本和校验字段。"""
    df_detail = _build_base_detail_df()
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

    artifacts = build_report_artifacts(df_detail, df_qty)
    row = artifacts.qty_sheet_df.iloc[0]

    assert row[QTY_DM_AMOUNT] == Decimal('100')
    assert row[QTY_MOH_LABOR_AMOUNT] == Decimal('30')
    assert row[QTY_DM_UNIT_COST] == Decimal('10')
    assert row[QTY_MOH_MATCH] == '是'
    assert row[QTY_TOTAL_MATCH] == '是'
    assert row[QTY_CHECK_STATUS] == '通过'


def test_build_report_artifacts_flags_mismatch_and_non_positive_qty() -> None:
    """测试金额勾稽失败和数量无效会进入 error_log。"""
    df_detail = _build_base_detail_df()
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
                '本期完工数量': 0,
                '本期完工金额': 999,
            }
        ]
    )

    artifacts = build_report_artifacts(df_detail, df_qty)
    row = artifacts.qty_sheet_df.iloc[0]

    assert row[QTY_TOTAL_MATCH] == '否'
    assert row[QTY_CHECK_STATUS] == '需复核'
    assert {'INVALID_COMPLETED_QTY', 'TOTAL_COST_MISMATCH'}.issubset(set(artifacts.error_log['issue_type']))


def test_build_report_artifacts_uses_product_level_modified_zscore() -> None:
    """测试 Modified Z-score 按产品总体计算，而不是按月份拆池。"""
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
                '月份': '2025年02期',
                '成本中心名称': '中心A',
                '产品编码': 'GB_C.D.B0040AA',
                '产品名称': 'BMS-750W驱动器',
                '规格型号': 'S-01',
                '工单编号': 'WO-002',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '直接材料',
                '本期完工金额': 110,
            },
            {
                '月份': '2025年03期',
                '成本中心名称': '中心A',
                '产品编码': 'GB_C.D.B0040AA',
                '产品名称': 'BMS-750W驱动器',
                '规格型号': 'S-01',
                '工单编号': 'WO-003',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '直接材料',
                '本期完工金额': 500,
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
                '本期完工金额': 100,
            },
            {
                '月份': '2025年02期',
                '成本中心名称': '中心A',
                '产品编码': 'GB_C.D.B0040AA',
                '产品名称': 'BMS-750W驱动器',
                '规格型号': 'S-01',
                '工单编号': 'WO-002',
                '工单行号': 1,
                '基本单位': 'PCS',
                '本期完工数量': 10,
                '本期完工金额': 110,
            },
            {
                '月份': '2025年03期',
                '成本中心名称': '中心A',
                '产品编码': 'GB_C.D.B0040AA',
                '产品名称': 'BMS-750W驱动器',
                '规格型号': 'S-01',
                '工单编号': 'WO-003',
                '工单行号': 1,
                '基本单位': 'PCS',
                '本期完工数量': 10,
                '本期完工金额': 500,
            },
        ]
    )

    artifacts = build_report_artifacts(df_detail, df_qty)
    anomaly_df = artifacts.work_order_sheet.data
    suspicious_row = anomaly_df.loc[anomaly_df['工单编号'] == 'WO-003'].iloc[0]

    assert suspicious_row['Modified Z-score_总单位完工成本'] is not None
    assert suspicious_row['总成本异常标记'] == '高度可疑'
    assert suspicious_row['异常等级'] == '高度可疑'
    assert suspicious_row['异常主要来源'] == '总成本异常'

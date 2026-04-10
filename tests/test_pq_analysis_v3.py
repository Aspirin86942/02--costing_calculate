"""测试 V3 分析数据逻辑。"""

from decimal import Decimal

import pandas as pd

from src.analytics.fact_builder import (
    QTY_CHECK_STATUS,
    QTY_DM_AMOUNT,
    QTY_DM_UNIT_COST,
    QTY_MOH_LABOR_AMOUNT,
    QTY_MOH_MATCH,
    QTY_OUTSOURCE_AMOUNT,
    QTY_OUTSOURCE_UNIT_COST,
    QTY_TOTAL_MATCH,
)
from src.analytics.qty_enricher import build_report_artifacts


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
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'GB_C.D.B0040AA',
                '产品名称': 'BMS-750W驱动器',
                '规格型号': 'S-01',
                '工单编号': 'WO-001',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '委外加工费',
                '本期完工金额': 15,
            },
        ]
    )


def _build_base_qty_df(*, total_amount: int) -> pd.DataFrame:
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
                '本期完工数量': 10,
                '本期完工金额': total_amount,
            }
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
                '本期完工金额': 165,
            }
        ]
    )

    artifacts = build_report_artifacts(df_detail, df_qty)
    row = artifacts.qty_sheet_df.iloc[0]
    work_order_row = artifacts.work_order_sheet.data.iloc[0]

    assert row[QTY_DM_AMOUNT] == Decimal('100')
    assert row[QTY_MOH_LABOR_AMOUNT] == Decimal('30')
    assert row[QTY_OUTSOURCE_AMOUNT] == Decimal('15')
    assert row[QTY_DM_UNIT_COST] == Decimal('10')
    assert row[QTY_OUTSOURCE_UNIT_COST] == Decimal('1.5')
    assert row[QTY_MOH_MATCH] == '是'
    assert row[QTY_TOTAL_MATCH] == '是'
    assert row[QTY_CHECK_STATUS] == '通过'
    assert work_order_row['委外加工费合计完工金额'] == Decimal('15')
    assert work_order_row['委外加工费单位完工成本'] == Decimal('1.5')
    assert '委外加工费异常标记' not in artifacts.work_order_sheet.data.columns
    assert 'log_委外加工费单位完工成本' not in artifacts.work_order_sheet.data.columns
    assert 'Modified Z-score_委外加工费' not in artifacts.work_order_sheet.data.columns
    assert not artifacts.error_log['issue_type'].isin(['EXCLUDED_COST_ITEM', 'UNMAPPED_COST_ITEM']).any()
    assert '完工数量是否有效' not in artifacts.qty_sheet_df.columns
    assert '完工数量是否小于等于0' not in artifacts.qty_sheet_df.columns
    assert '是否存在空值' not in artifacts.qty_sheet_df.columns


def test_build_report_artifacts_filters_out_invalid_qty_rows() -> None:
    """测试数量无效的工单不会出现在数量页与工单异常页。"""
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
    quality_metrics = {metric.metric: metric.value for metric in artifacts.quality_metrics}

    assert artifacts.qty_sheet_df.empty
    assert artifacts.work_order_sheet.data.empty
    assert artifacts.error_log.empty
    assert quality_metrics['产品数量统计输出行数'] == '0'
    assert quality_metrics['因完工数量无效被过滤行数'] == '1'
    assert quality_metrics['因总完工成本为空被过滤行数'] == '0'


def test_build_report_artifacts_filters_out_missing_total_amount_rows() -> None:
    """测试总完工成本为空的工单不会出现在数量页与工单异常页。"""
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
                '本期完工金额': None,
            }
        ]
    )

    artifacts = build_report_artifacts(df_detail, df_qty)
    quality_metrics = {metric.metric: metric.value for metric in artifacts.quality_metrics}

    assert artifacts.qty_sheet_df.empty
    assert artifacts.work_order_sheet.data.empty
    assert artifacts.error_log.empty
    assert quality_metrics['产品数量统计输出行数'] == '0'
    assert quality_metrics['因完工数量无效被过滤行数'] == '0'
    assert quality_metrics['因总完工成本为空被过滤行数'] == '1'


def test_build_report_artifacts_keeps_total_cost_mismatch_for_retained_rows() -> None:
    """测试保留行上的总成本勾稽失败仍进入 error_log。"""
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
                '本期完工金额': 999,
            }
        ]
    )

    artifacts = build_report_artifacts(df_detail, df_qty)
    row = artifacts.qty_sheet_df.iloc[0]

    assert row[QTY_TOTAL_MATCH] == '否'
    assert row[QTY_CHECK_STATUS] == '需复核'
    assert 'TOTAL_COST_MISMATCH' in set(artifacts.error_log['issue_type'])
    assert 'INVALID_COMPLETED_QTY' not in set(artifacts.error_log['issue_type'])
    assert 'MISSING_REQUIRED_VALUE' not in set(artifacts.error_log['issue_type'])


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


def test_build_report_artifacts_supports_software_fee_as_sk_standalone_item() -> None:
    """测试 SK 下软件费用作为独立成本项参与展示和总成本勾稽。"""
    df_detail = pd.concat(
        [
            _build_base_detail_df(),
            pd.DataFrame(
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
                        '成本项目名称': '软件费用',
                        '本期完工金额': 5,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    df_qty = _build_base_qty_df(total_amount=170)

    artifacts = build_report_artifacts(df_detail, df_qty, standalone_cost_items=('委外加工费', '软件费用'))
    qty_row = artifacts.qty_sheet_df.iloc[0]
    work_order_row = artifacts.work_order_sheet.data.iloc[0]

    assert qty_row['本期完工软件费用合计完工金额'] == Decimal('5')
    assert qty_row['软件费用单位完工成本'] == Decimal('0.5')
    assert qty_row['直接材料+直接人工+制造费用+委外加工费+软件费用是否等于总完工成本'] == '是'
    assert qty_row[QTY_CHECK_STATUS] == '通过'
    assert work_order_row['软件费用合计完工金额'] == Decimal('5')
    assert work_order_row['软件费用单位完工成本'] == Decimal('0.5')
    assert '软件费用异常标记' not in artifacts.work_order_sheet.data.columns
    assert 'log_软件费用单位完工成本' not in artifacts.work_order_sheet.data.columns
    assert 'Modified Z-score_软件费用' not in artifacts.work_order_sheet.data.columns
    assert not artifacts.error_log['issue_type'].eq('UNMAPPED_COST_ITEM').any()
    assert not artifacts.error_log['issue_type'].eq('TOTAL_COST_MISMATCH').any()


def test_build_report_artifacts_keeps_software_fee_unmapped_for_gb_default() -> None:
    """测试 GB 默认配置下软件费用仍记为未映射成本项。"""
    df_detail = pd.concat(
        [
            _build_base_detail_df(),
            pd.DataFrame(
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
                        '成本项目名称': '软件费用',
                        '本期完工金额': 5,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    df_qty = _build_base_qty_df(total_amount=165)

    artifacts = build_report_artifacts(df_detail, df_qty, standalone_cost_items=('委外加工费',))

    assert artifacts.error_log['issue_type'].eq('UNMAPPED_COST_ITEM').any()
    assert '本期完工软件费用合计完工金额' not in artifacts.qty_sheet_df.columns
    assert '软件费用单位完工成本' not in artifacts.qty_sheet_df.columns

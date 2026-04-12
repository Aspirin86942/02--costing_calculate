"""测试 V3 分析数据逻辑。"""

from decimal import Decimal

import pandas as pd
import polars as pl

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


def test_build_report_artifacts_exposes_fact_bundle_and_preserves_contracts() -> None:
    artifacts = build_report_artifacts(_build_base_detail_df(), _build_base_qty_df(total_amount=999))

    assert artifacts.fact_bundle is not None
    assert artifacts.fact_bundle.qty_fact.height == 1
    qty_row = artifacts.qty_sheet_df.iloc[0]
    assert qty_row['本期完工直接材料合计完工金额'] == Decimal('100')
    assert 'TOTAL_COST_MISMATCH' in set(artifacts.error_log['issue_type'])


def test_build_report_artifacts_preserves_non_terminating_decimal_division_precision() -> None:
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
                '本期完工数量': 3,
                '本期完工金额': 100,
            }
        ]
    )

    artifacts = build_report_artifacts(df_detail, df_qty)
    qty_row = artifacts.qty_sheet_df.iloc[0]
    expected = Decimal('100') / Decimal('3')

    assert qty_row[QTY_DM_UNIT_COST] == expected


def test_build_report_artifacts_preserves_small_positive_quantity() -> None:
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
                '本期完工金额': '0.01',
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
                '本期完工数量': '0.00005',
                '本期完工金额': '0.01',
            }
        ]
    )

    artifacts = build_report_artifacts(df_detail, df_qty)
    quality_metrics = {metric.metric: metric.value for metric in artifacts.quality_metrics}

    assert len(artifacts.qty_sheet_df) == 1
    assert quality_metrics['因完工数量无效被过滤行数'] == '0'


def test_build_report_artifacts_keeps_pandas_polars_compatibility_on_precision_fields() -> None:
    detail_pd = pd.DataFrame(
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
                '成本项目名称': '委外加工费',
                '本期完工金额': 5,
            },
        ]
    )
    qty_pd = pd.DataFrame(
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
                '本期完工数量': 3,
                '本期完工金额': 105,
            }
        ]
    )

    artifacts_pd = build_report_artifacts(detail_pd, qty_pd)
    artifacts_pl = build_report_artifacts(
        pl.DataFrame(detail_pd.to_dict(orient='list')),
        pl.DataFrame(qty_pd.to_dict(orient='list')),
    )
    qty_row_pd = artifacts_pd.qty_sheet_df.iloc[0]
    qty_row_pl = artifacts_pl.qty_sheet_df.iloc[0]

    for column in [QTY_DM_AMOUNT, QTY_DM_UNIT_COST, QTY_OUTSOURCE_AMOUNT, QTY_OUTSOURCE_UNIT_COST, QTY_TOTAL_MATCH]:
        assert qty_row_pd[column] == qty_row_pl[column]


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


def test_build_report_artifacts_normalizes_spaced_standalone_cost_items() -> None:
    """测试带前后空格的独立成本项在识别、聚合和勾稽时使用统一口径。"""
    df_detail = _build_base_detail_df().copy()
    df_detail.loc[df_detail['成本项目名称'] == '委外加工费', '成本项目名称'] = ' 委外加工费 '
    df_detail = pd.concat(
        [
            df_detail,
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
                        '成本项目名称': ' 软件费用 ',
                        '本期完工金额': 5,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    df_qty = _build_base_qty_df(total_amount=170)

    artifacts = build_report_artifacts(df_detail, df_qty, standalone_cost_items=(' 委外加工费 ', ' 软件费用 '))
    qty_row = artifacts.qty_sheet_df.iloc[0]
    work_order_row = artifacts.work_order_sheet.data.iloc[0]

    assert qty_row[QTY_OUTSOURCE_AMOUNT] == Decimal('15')
    assert qty_row['本期完工软件费用合计完工金额'] == Decimal('5')
    assert qty_row['软件费用单位完工成本'] == Decimal('0.5')
    assert qty_row['直接材料+直接人工+制造费用+委外加工费+软件费用是否等于总完工成本'] == '是'
    assert qty_row[QTY_CHECK_STATUS] == '通过'
    assert work_order_row['委外加工费合计完工金额'] == Decimal('15')
    assert work_order_row['软件费用合计完工金额'] == Decimal('5')
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


def test_build_report_artifacts_supports_software_fee_only_standalone_output_columns() -> None:
    """测试仅配置软件费用时，工单异常页不要求委外列存在。"""
    df_detail = pd.concat(
        [
            _build_base_detail_df().loc[lambda df: df['成本项目名称'] != '委外加工费'],
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
    df_qty = _build_base_qty_df(total_amount=155)

    artifacts = build_report_artifacts(df_detail, df_qty, standalone_cost_items=('软件费用',))
    work_order_columns = set(artifacts.work_order_sheet.data.columns)

    assert '软件费用合计完工金额' in work_order_columns
    assert '软件费用单位完工成本' in work_order_columns
    assert '委外加工费合计完工金额' not in work_order_columns
    assert '委外加工费单位完工成本' not in work_order_columns


def test_build_report_artifacts_supports_empty_standalone_output_columns() -> None:
    """测试空 standalone 配置下工单异常页可正常构建且不输出 standalone 列。"""
    df_detail = _build_base_detail_df().loc[lambda df: df['成本项目名称'] != '委外加工费']
    df_qty = _build_base_qty_df(total_amount=150)

    artifacts = build_report_artifacts(df_detail, df_qty, standalone_cost_items=())
    work_order_columns = set(artifacts.work_order_sheet.data.columns)

    assert '委外加工费合计完工金额' not in work_order_columns
    assert '委外加工费单位完工成本' not in work_order_columns
    assert '软件费用合计完工金额' not in work_order_columns
    assert '软件费用单位完工成本' not in work_order_columns

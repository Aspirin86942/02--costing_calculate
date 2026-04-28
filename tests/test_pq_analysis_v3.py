"""测试 V3 分析数据逻辑。"""

from decimal import Decimal

import pandas as pd
import polars as pl
import pytest

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


def test_build_report_artifacts_passes_scope_mode_to_product_anomaly_sections(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_build_product_anomaly_sections(summary_df: pd.DataFrame, *, scope_mode: str = 'legacy_single_scope'):
        captured['scope_mode'] = scope_mode
        captured['summary_columns'] = tuple(summary_df.columns)
        return []

    monkeypatch.setattr(
        'src.analytics.qty_enricher.build_product_anomaly_sections',
        _fake_build_product_anomaly_sections,
    )

    artifacts = build_report_artifacts(
        _build_base_detail_df(),
        _build_base_qty_df(total_amount=165),
        product_anomaly_scope_mode='doc_type_split',
    )

    assert captured['scope_mode'] == 'doc_type_split'
    assert 'product_code' in captured['summary_columns']
    assert artifacts.product_anomaly_sections == []


def test_build_report_artifacts_rejects_invalid_scope_mode() -> None:
    with pytest.raises(ValueError, match='product_anomaly_scope_mode'):
        build_report_artifacts(
            _build_base_detail_df(),
            _build_base_qty_df(total_amount=165),
            product_anomaly_scope_mode='bad',
        )


def test_build_report_artifacts_doc_type_split_rejects_missing_doc_type_values() -> None:
    with pytest.raises(ValueError, match='doc_type'):
        build_report_artifacts(
            _build_base_detail_df(),
            _build_base_qty_df(total_amount=165),
            product_anomaly_scope_mode='doc_type_split',
        )


def test_build_report_artifacts_doc_type_split_builds_labeled_sections() -> None:
    df_detail = pd.DataFrame(
        [
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
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
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-002',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '直接材料',
                '本期完工金额': 50,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-003',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '直接材料',
                '本期完工金额': 20,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-004',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '直接材料',
                '本期完工金额': 7,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-005',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '直接材料',
                '本期完工金额': 3,
            },
        ]
    )
    df_qty = pd.DataFrame(
        [
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-001',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '汇报入库-普通生产',
                '本期完工数量': 10,
                '本期完工金额': 100,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-002',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '直接入库-普通生产',
                '本期完工数量': 5,
                '本期完工金额': 50,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-003',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '汇报入库-返工生产',
                '本期完工数量': 2,
                '本期完工金额': 20,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-004',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '普通委外订单',
                '本期完工数量': 1,
                '本期完工金额': 7,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-005',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '未知类型',
                '本期完工数量': 1,
                '本期完工金额': 3,
            },
        ]
    )

    artifacts = build_report_artifacts(
        df_detail,
        df_qty,
        product_anomaly_scope_mode='doc_type_split',
    )

    assert artifacts.fact_bundle is not None
    work_order_fact = artifacts.fact_bundle.work_order_fact
    assert 'doc_type' in work_order_fact.columns
    assert set(work_order_fact['doc_type'].to_list()) == {
        '汇报入库-普通生产',
        '直接入库-普通生产',
        '汇报入库-返工生产',
        '普通委外订单',
        '未知类型',
    }

    assert [section.section_label for section in artifacts.product_anomaly_sections] == ['全部', '正常生产', '返工生产']
    section_by_label = {section.section_label: section for section in artifacts.product_anomaly_sections}
    assert section_by_label['全部'].data['总成本'].tolist() == [Decimal('180')]
    assert section_by_label['正常生产'].data['总成本'].tolist() == [Decimal('150')]
    assert section_by_label['返工生产'].data['总成本'].tolist() == [Decimal('20')]


def test_build_report_artifacts_work_order_sheet_adds_production_scope_column() -> None:
    df_detail = pd.DataFrame(
        [
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-N1',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '直接材料',
                '本期完工金额': 100,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-U1',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '直接材料',
                '本期完工金额': 300,
            },
        ]
    )
    df_qty = pd.DataFrame(
        [
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-N1',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '汇报入库-普通生产',
                '本期完工数量': 10,
                '本期完工金额': 100,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-U1',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '未知类型',
                '本期完工数量': 10,
                '本期完工金额': 300,
            },
        ]
    )

    artifacts = build_report_artifacts(df_detail, df_qty, product_anomaly_scope_mode='doc_type_split')
    anomaly_df = artifacts.work_order_sheet.data

    assert anomaly_df.columns.tolist()[7:10] == ['生产类型', '基本单位', '本期完工数量']
    assert anomaly_df.loc[anomaly_df['工单编号'] == 'WO-N1', '生产类型'].iloc[0] == '正常生产'
    assert anomaly_df.loc[anomaly_df['工单编号'] == 'WO-U1', '生产类型'].iloc[0] == '未归类'


def test_build_report_artifacts_marks_unknown_doc_type_as_not_analyzable() -> None:
    detail_rows: list[dict[str, object]] = []
    order_cost_map = {
        'WO-N1': {'直接材料': 100, '直接人工': 30, '制造费用_其他': 10, '制造费用-人工': 20, '制造费用_机物料及低耗': 5, '制造费用_折旧': 8, '制造费用_水电费': 7},
        'WO-N2': {'直接材料': 120, '直接人工': 35, '制造费用_其他': 12, '制造费用-人工': 22, '制造费用_机物料及低耗': 6, '制造费用_折旧': 9, '制造费用_水电费': 8},
        'WO-U1': {'直接材料': 500, '直接人工': 60, '制造费用_其他': 40, '制造费用-人工': 50, '制造费用_机物料及低耗': 20, '制造费用_折旧': 15, '制造费用_水电费': 15},
    }
    for order_no, cost_item_map in order_cost_map.items():
        for cost_item, amount in cost_item_map.items():
            detail_rows.append(
                {
                    '月份': '2025年01期',
                    '成本中心名称': '中心A',
                    '产品编码': 'P001',
                    '产品名称': '产品A',
                    '规格型号': 'S-01',
                    '工单编号': order_no,
                    '工单行号': 1,
                    '基本单位': 'PCS',
                    '成本项目名称': cost_item,
                    '本期完工金额': amount,
                }
            )
    df_detail = pd.DataFrame(detail_rows)
    df_qty = pd.DataFrame(
        [
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-N1',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '汇报入库-普通生产',
                '本期完工数量': 10,
                '本期完工金额': 180,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-N2',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '汇报入库-返工生产',
                '本期完工数量': 10,
                '本期完工金额': 212,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-U1',
                '工单行号': 1,
                '基本单位': 'PCS',
                '单据类型': '普通委外订单',
                '本期完工数量': 10,
                '本期完工金额': 700,
            },
        ]
    )

    artifacts = build_report_artifacts(df_detail, df_qty, product_anomaly_scope_mode='doc_type_split')
    anomaly_df = artifacts.work_order_sheet.data
    row = anomaly_df.loc[anomaly_df['工单编号'] == 'WO-U1'].iloc[0]

    assert row['生产类型'] == '未归类'
    assert row['是否可参与分析'] == '否'
    assert row['异常等级'] == ''
    assert row['异常主要来源'] == ''
    assert row['Modified Z-score_总单位完工成本'] is None
    assert row['复核原因'] == '单据类型未归类，不参与正常生产/返工生产异常池'


def test_build_report_artifacts_marks_missing_doc_type_column_as_not_analyzable() -> None:
    df_detail = pd.DataFrame(
        [
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-M1',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '直接材料',
                '本期完工金额': 100,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-M1',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '直接人工',
                '本期完工金额': 30,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-M1',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '制造费用_其他',
                '本期完工金额': 10,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-M1',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '制造费用-人工',
                '本期完工金额': 20,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-M1',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '制造费用_机物料及低耗',
                '本期完工金额': 5,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-M1',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '制造费用_折旧',
                '本期完工金额': 8,
            },
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-M1',
                '工单行号': 1,
                '基本单位': 'PCS',
                '成本项目名称': '制造费用_水电费',
                '本期完工金额': 7,
            },
        ]
    )
    df_qty = pd.DataFrame(
        [
            {
                '月份': '2025年01期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '规格型号': 'S-01',
                '工单编号': 'WO-M1',
                '工单行号': 1,
                '基本单位': 'PCS',
                '本期完工数量': 10,
                '本期完工金额': 180,
            }
        ]
    )

    artifacts = build_report_artifacts(df_detail, df_qty)
    anomaly_df = artifacts.work_order_sheet.data
    row = anomaly_df.loc[anomaly_df['工单编号'] == 'WO-M1'].iloc[0]

    assert row['生产类型'] == '未归类'
    assert row['是否可参与分析'] == '否'
    assert row['异常等级'] == ''
    assert row['异常主要来源'] == ''
    assert row['Modified Z-score_总单位完工成本'] is None
    assert row['复核原因'] == '单据类型未归类，不参与正常生产/返工生产异常池'


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


def test_build_report_artifacts_fact_bundle_preserves_non_terminating_decimal_division_precision() -> None:
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
                '本期完工金额': '100',
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
                '本期完工数量': '3',
                '本期完工金额': '100',
            }
        ]
    )

    artifacts = build_report_artifacts(df_detail, df_qty)
    assert artifacts.fact_bundle is not None
    qty_row = artifacts.fact_bundle.qty_fact.select([QTY_DM_UNIT_COST]).to_dicts()[0]

    assert qty_row[QTY_DM_UNIT_COST] == (Decimal('100') / Decimal('3'))


def test_build_report_artifacts_fact_bundle_preserves_decimal_division_for_fractional_values() -> None:
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
                '本期完工金额': '0.1',
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
                '本期完工数量': '0.03',
                '本期完工金额': '0.1',
            }
        ]
    )

    artifacts = build_report_artifacts(df_detail, df_qty)
    assert artifacts.fact_bundle is not None
    qty_row = artifacts.fact_bundle.qty_fact.select([QTY_DM_UNIT_COST]).to_dicts()[0]

    assert qty_row[QTY_DM_UNIT_COST] == (Decimal('0.1') / Decimal('0.03'))


def test_build_report_artifacts_fact_bundle_division_columns_keep_polars_decimal_dtype() -> None:
    artifacts = build_report_artifacts(_build_base_detail_df(), _build_base_qty_df(total_amount=165))

    assert artifacts.fact_bundle is not None
    qty_schema = artifacts.fact_bundle.qty_fact.schema
    work_order_schema = artifacts.fact_bundle.work_order_fact.schema

    assert qty_schema[QTY_DM_UNIT_COST] == pl.Decimal(38, 28)
    assert qty_schema[QTY_OUTSOURCE_UNIT_COST] == pl.Decimal(38, 28)
    assert work_order_schema['dm_unit_cost'] == pl.Decimal(38, 28)


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


def test_build_report_artifacts_preserves_tiny_positive_quantity_with_legacy_semantics() -> None:
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
                '本期完工数量': '0.0000000000000000001',
                '本期完工金额': '0.01',
            }
        ]
    )

    artifacts = build_report_artifacts(df_detail, df_qty)
    quality_metrics = {metric.metric: metric.value for metric in artifacts.quality_metrics}

    assert len(artifacts.qty_sheet_df) == 1
    assert quality_metrics['产品数量统计输出行数'] == '1'
    assert quality_metrics['因完工数量无效被过滤行数'] == '0'


def test_build_report_artifacts_preserves_high_scale_amount_and_qty_precision() -> None:
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
                '本期完工金额': '0.0000000000000000002',
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
                '本期完工数量': '0.0000000000000000001',
                '本期完工金额': '0.0000000000000000002',
            }
        ]
    )

    artifacts = build_report_artifacts(df_detail, df_qty)
    qty_row = artifacts.qty_sheet_df.iloc[0]

    assert qty_row[QTY_DM_AMOUNT] == Decimal('0.0000000000000000002')
    assert qty_row[QTY_DM_UNIT_COST] == Decimal('2')


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
                '单据类型': '汇报入库-普通生产',
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
                '单据类型': '汇报入库-普通生产',
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
                '单据类型': '汇报入库-普通生产',
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

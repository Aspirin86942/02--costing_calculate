"""测试 ETL 阶段 pipeline 契约。"""

from __future__ import annotations

import pandas as pd

from src.analytics.contracts import ResolvedColumns, SplitResult
from src.etl.costing_etl import CostingWorkbookETL


def test_pipeline_resolve_columns_returns_resolved_columns_contract() -> None:
    etl = CostingWorkbookETL(skip_rows=2)
    df = pd.DataFrame(columns=['物料编码', '成本项目', '其他列'])

    resolved = etl.pipeline.resolve_columns(df)

    assert isinstance(resolved, ResolvedColumns)
    assert resolved.child_material_column == '子项物料编码'
    assert resolved.cost_item_column == '成本项目名称'
    assert resolved.rename_map == {'物料编码': '子项物料编码', '成本项目': '成本项目名称'}


def test_pipeline_split_sheets_returns_split_result_and_keeps_month_column() -> None:
    etl = CostingWorkbookETL(skip_rows=2)
    df_raw = pd.DataFrame(
        [
            {
                '年期': '2025年1期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '工单编号': 'WO-001',
                '工单行号': 1,
                '子项物料编码': '',
                '成本项目名称': '',
                '本期完工数量': 10,
                '本期完工金额': 100,
            },
            {
                '年期': '2025年1期',
                '成本中心名称': '中心A',
                '产品编码': 'P001',
                '产品名称': '产品A',
                '工单编号': 'WO-001',
                '工单行号': 1,
                '子项物料编码': 'MAT-001',
                '成本项目名称': '直接材料',
                '本期完工数量': 10,
                '本期完工金额': 100,
            },
        ]
    )
    df_filled = df_raw.copy()
    df_filled['Filled_成本项目'] = df_filled['成本项目名称'].replace('', pd.NA).ffill()

    split_result = etl.pipeline.split_sheets(df_raw, df_filled)

    assert isinstance(split_result, SplitResult)
    assert split_result.qty_df.columns.tolist()[0:2] == ['年期', '月份']
    assert split_result.detail_df.columns.tolist()[0:2] == ['年期', '月份']
    assert split_result.qty_df.iloc[0]['工单编号'] == 'WO-001'
    assert split_result.detail_df.iloc[0]['成本项目名称'] == '直接材料'

"""测试 ETL 阶段 pipeline 契约。"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import polars as pl
from openpyxl import Workbook

from src.analytics.contracts import (
    AnalysisArtifacts,
    ConditionalFormatRule,
    FactBundle,
    FlatSheet,
    NormalizedCostFrame,
    RawWorkbookFrame,
    ResolvedColumns,
    SheetModel,
    SplitResult,
    WorkbookPayload,
)
from src.etl.costing_etl import CostingWorkbookETL
from src.etl.stages.reader import load_raw_workbook
from src.etl.stages.workbook_ingestor import WorkbookIngestor


def test_polars_pipeline_contract_objects_are_constructible() -> None:
    raw = RawWorkbookFrame(
        sheet_name='成本计算单',
        header_rows=(('年期', '产品编码'), ('', '')),
        frame=pl.DataFrame({'column_0': ['2025年1期'], 'column_1': ['P001']}),
    )
    normalized = NormalizedCostFrame(
        frame=pl.DataFrame({'年期': ['2025-01'], '产品编码': ['P001']}),
        key_columns=('年期', '产品编码'),
    )
    model = SheetModel(
        sheet_name='成本明细',
        columns=('年期', '产品编码'),
        rows_factory=lambda: iter([('2025-01', 'P001')]),
        column_types={'年期': 'text', '产品编码': 'text'},
        number_formats={},
        freeze_panes='A2',
        auto_filter=True,
        fixed_width=15.0,
        conditional_formats=(
            ConditionalFormatRule(
                target_range='A2:A1048576',
                formula='=$B2="高度可疑"',
                format_key='suspicious',
            ),
        ),
    )
    payload = WorkbookPayload(sheet_models=(model,), quality_metrics=(), error_log_count=0, stage_timings={})

    assert raw.sheet_name == '成本计算单'
    assert normalized.key_columns == ('年期', '产品编码')
    assert list(model.rows_factory()) == [('2025-01', 'P001')]
    assert payload.sheet_models[0].sheet_name == '成本明细'


def test_fact_bundle_is_constructible_and_exposes_expected_frames() -> None:
    bundle = FactBundle(
        detail_fact=pl.DataFrame({'detail_id': ['D-001']}),
        qty_fact=pl.DataFrame({'qty_id': ['Q-001']}),
        work_order_fact=pl.DataFrame({'work_order_id': ['WO-001']}),
        product_summary_fact=pl.DataFrame({'product_id': ['P-001']}),
        error_fact=pl.DataFrame({'error_code': ['MISSING_AMOUNT']}),
    )

    assert bundle.detail_fact.columns == ['detail_id']
    assert bundle.qty_fact.item(0, 'qty_id') == 'Q-001'
    assert bundle.work_order_fact.item(0, 'work_order_id') == 'WO-001'
    assert bundle.product_summary_fact.item(0, 'product_id') == 'P-001'
    assert bundle.error_fact.item(0, 'error_code') == 'MISSING_AMOUNT'


def test_analysis_artifacts_remains_constructible_without_fact_bundle() -> None:
    artifacts = AnalysisArtifacts(
        fact_df=pd.DataFrame({'产品编码': ['P001']}),
        qty_sheet_df=pd.DataFrame({'产品编码': ['P001']}),
        work_order_sheet=FlatSheet(
            data=pd.DataFrame({'工单编号': ['WO-001']}),
            column_types={'工单编号': 'text'},
        ),
        product_anomaly_sections=[],
        quality_metrics=(),
        error_log=pd.DataFrame({'错误码': ['MISSING_AMOUNT']}),
    )

    assert artifacts.fact_bundle is None
    assert artifacts.work_order_sheet.column_types == {'工单编号': 'text'}
    assert artifacts.error_log.iloc[0]['错误码'] == 'MISSING_AMOUNT'


def test_analysis_artifacts_accepts_fact_bundle_without_breaking_existing_fields() -> None:
    bundle = FactBundle(
        detail_fact=pl.DataFrame({'detail_id': ['D-001']}),
        qty_fact=pl.DataFrame({'qty_id': ['Q-001']}),
        work_order_fact=pl.DataFrame({'work_order_id': ['WO-001']}),
        product_summary_fact=pl.DataFrame({'product_id': ['P-001']}),
        error_fact=pl.DataFrame({'error_code': ['MISSING_AMOUNT']}),
    )
    artifacts = AnalysisArtifacts(
        fact_df=pd.DataFrame({'产品编码': ['P001']}),
        qty_sheet_df=pd.DataFrame({'产品编码': ['P001']}),
        work_order_sheet=FlatSheet(
            data=pd.DataFrame({'工单编号': ['WO-001']}),
            column_types={'工单编号': 'text'},
        ),
        product_anomaly_sections=[],
        quality_metrics=(),
        error_log=pd.DataFrame({'错误码': ['MISSING_AMOUNT']}),
        fact_bundle=bundle,
    )

    assert artifacts.fact_bundle is bundle
    assert artifacts.fact_df.iloc[0]['产品编码'] == 'P001'
    assert artifacts.fact_bundle.error_fact.item(0, 'error_code') == 'MISSING_AMOUNT'


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


def test_workbook_ingestor_falls_back_once_when_fast_reader_fails(tmp_path: Path) -> None:
    ingestor = WorkbookIngestor()
    fallback = RawWorkbookFrame(
        sheet_name='Sheet1',
        header_rows=(('年期', '产品编码'), ('', '')),
        frame=pl.DataFrame({'column_0': ['2025年1期'], 'column_1': ['P001']}),
    )

    with (
        patch.object(ingestor, '_load_with_calamine', side_effect=RuntimeError('boom')),
        patch.object(ingestor, '_load_with_openpyxl', return_value=fallback) as fallback_mock,
    ):
        result = ingestor.load(tmp_path / 'input.xlsx', skip_rows=2)

    assert result.sheet_name == 'Sheet1'
    fallback_mock.assert_called_once()


def test_load_raw_workbook_delegates_to_workbook_ingestor(tmp_path: Path) -> None:
    raw = RawWorkbookFrame(
        sheet_name='Sheet1',
        header_rows=(('年期', '产品编码'), ('', '')),
        frame=pl.DataFrame({'column_0': ['2025年1期'], 'column_1': ['P001']}),
    )

    with patch.object(WorkbookIngestor, 'load', return_value=raw) as load_mock:
        result = load_raw_workbook(tmp_path / 'input.xlsx', skip_rows=2)

    assert result is raw
    load_mock.assert_called_once_with(tmp_path / 'input.xlsx', skip_rows=2)


def test_workbook_ingestor_calamine_fast_path_preserves_contract(tmp_path: Path) -> None:
    workbook_path = tmp_path / 'fast.xlsx'
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = 'FastSheet'
    worksheet.append(['metadata'])
    worksheet.append(['metadata2'])
    worksheet.append(['年期', '产品编码'])
    worksheet.append(['', '产品名称'])
    worksheet.append(['2026年3期', 'P002'])
    workbook.save(workbook_path)

    ingestor = WorkbookIngestor()
    result = ingestor.load(workbook_path, skip_rows=2)

    assert result.sheet_name == 'FastSheet'
    assert result.header_rows == (('年期', '产品编码'), ('', '产品名称'))
    assert result.frame.columns == ['column_0', 'column_1']
    assert result.frame.row(0) == ('2026年3期', 'P002')


def test_workbook_ingestor_openpyxl_fallback_preserves_sheet_name_and_headers(tmp_path: Path) -> None:
    workbook_path = tmp_path / 'fallback.xlsx'
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = 'FallbackSheet'
    worksheet.append(['metadata'])
    worksheet.append(['unused'])
    worksheet.append(['主表头', '产品编码'])
    worksheet.append(['次表头', '产品名称'])
    worksheet.append(['2025年1期', 'P001'])
    workbook.save(workbook_path)

    ingestor = WorkbookIngestor()
    with patch.object(ingestor, '_load_with_calamine', side_effect=RuntimeError('boom')):
        result = ingestor.load(workbook_path, skip_rows=2)

    assert result.sheet_name == 'FallbackSheet'
    assert result.header_rows == (('主表头', '产品编码'), ('次表头', '产品名称'))

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.analytics.contracts import QualityMetric
from src.analytics.summary import build_summary_payload, write_summary_json


def test_build_summary_payload_counts_error_and_anomaly_fields(tmp_path: Path) -> None:
    payload = build_summary_payload(
        pipeline_name='gb',
        input_path=tmp_path / 'input.xlsx',
        output_path=tmp_path / 'output.xlsx',
        error_log_path=tmp_path / 'error.csv',
        error_log_count=3,
        quality_metrics=(
            QualityMetric('行数勾稽', '产品数量统计输出行数', '2', '仅保留有效工单'),
            QualityMetric('分析覆盖率', '可参与分析占比', '50.00%', '白名单工单覆盖率'),
        ),
        error_log_frame=pd.DataFrame(
            [
                {'issue_type': 'MISSING_AMOUNT'},
                {'issue_type': 'MISSING_AMOUNT'},
                {'issue_type': 'TOTAL_COST_MISMATCH'},
            ]
        ),
        work_order_sheet_frame=pd.DataFrame(
            [
                {'异常等级': '关注', '异常主要来源': '材料异常'},
                {'异常等级': '高度可疑', '异常主要来源': '总成本异常'},
                {'异常等级': '正常', '异常主要来源': ''},
            ]
        ),
        month_filter_summary=None,
    )

    assert payload['pipeline'] == 'gb'
    assert payload['error_log_count'] == 3
    assert payload['quality_metrics']['产品数量统计输出行数']['value'] == '2'
    assert payload['issue_type_counts'] == {'MISSING_AMOUNT': 2, 'TOTAL_COST_MISMATCH': 1}
    assert payload['anomaly_level_counts'] == {'关注': 1, '高度可疑': 1, '正常': 1}
    assert payload['anomaly_source_counts'] == {'材料异常': 1, '总成本异常': 1}


def test_write_summary_json_uses_utf8(tmp_path: Path) -> None:
    output_path = tmp_path / 'summary.json'
    write_summary_json(output_path, {'pipeline': 'gb', '中文': '可读'})

    loaded = json.loads(output_path.read_text(encoding='utf-8'))

    assert loaded == {'pipeline': 'gb', '中文': '可读'}

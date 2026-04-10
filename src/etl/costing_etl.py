"""
ETL pipeline for costing workbooks.
Excel原始成本计算单 -> 清洗双层表头 -> 规则填充 -> 拆成成本明细/数量页
-> 生成价量分析/异常分析/校验表 -> 写回一个新Excel
"""

import logging
import sys
from pathlib import Path

import pandas as pd

try:
    from src.analytics.contracts import FlatSheet, ProductAnomalySection, QualityMetric
    from src.analytics.qty_enricher import build_report_artifacts
    from src.analytics.table_rendering import render_tables
    from src.config.pipelines import GB_PIPELINE
    from src.config.settings import GB_PROCESSED_DIR, GB_RAW_DIR, ensure_directories
    from src.etl.pipeline import CostingEtlPipeline
    from src.etl.utils import clean_column_name
    from src.excel.workbook_writer import CostingWorkbookWriter
except ModuleNotFoundError:
    # 直接执行 src/etl/costing_etl.py 时，解释器搜索路径不含项目根目录，补齐后重试导入。
    project_root = Path(__file__).resolve().parents[2]
    project_root_str = str(project_root)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)
    from src.analytics.contracts import FlatSheet, ProductAnomalySection, QualityMetric
    from src.analytics.qty_enricher import build_report_artifacts
    from src.analytics.table_rendering import render_tables
    from src.config.pipelines import GB_PIPELINE
    from src.config.settings import GB_PROCESSED_DIR, GB_RAW_DIR, ensure_directories
    from src.etl.pipeline import CostingEtlPipeline
    from src.etl.utils import clean_column_name
    from src.excel.workbook_writer import CostingWorkbookWriter

# 当前模块创建一个 logger, 后面所有日志都走这个 logger
logger = logging.getLogger(__name__)


# 预定义列名常量
COL_PERIOD = '年期'
COL_MONTH = '月份'
COL_COST_CENTER = '成本中心名称'
COL_PRODUCT_CODE = '产品编码'
COL_PRODUCT_NAME = '产品名称'
COL_SPEC = '规格型号'
COL_ORDER_NO = '工单编号'
COL_ORDER_LINE = '工单行号'
COL_VENDOR_CODE = '供应商编码'
COL_VENDOR_NAME = '供应商名称'
COL_UNIT = '基本单位'
COL_PLAN_QTY = '计划产量'
COL_PRODUCTION_TYPE = '生产类型'
COL_DOC_TYPE = '单据类型'
COL_COST_ITEM = '成本项目名称'
COL_CHILD_MATERIAL = '子项物料编码'
COL_CHILD_MATERIAL_NAME = '子项物料名称'
COL_FILLED_COST_ITEM = 'Filled_成本项目'  # 填充后的成本项目列，因为成本项目原始生成的项目可能是空值
COL_OPENING_WIP_QTY = '期初在产品数量'
COL_OPENING_WIP_AMOUNT = '期初在产品金额'
COL_OPENING_ADJUST_QTY = '期初调整数量'
COL_OPENING_ADJUST_AMOUNT = '期初调整金额'
COL_CURRENT_INPUT_QTY = '本期投入数量'
COL_CURRENT_INPUT_AMOUNT = '本期投入金额'
COL_CUMULATIVE_INPUT_QTY = '累计投入数量'
COL_CUMULATIVE_INPUT_AMOUNT = '累计投入金额'
COL_ENDING_WIP_QTY = '期末在产品数量'
COL_ENDING_WIP_AMOUNT = '期末在产品金额'
COL_CURRENT_COMPLETED_QTY = '本期完工数量'
COL_CURRENT_COMPLETED_CONSUMPTION = '本期完工单耗'
COL_CURRENT_COMPLETED_UNIT_COST = '本期完工单位成本'
COL_CURRENT_COMPLETED_AMOUNT = '本期完工金额'
COL_CUMULATIVE_COMPLETED_QTY = '累计完工数量'
COL_CUMULATIVE_COMPLETED_CONSUMPTION = '累计完工单耗'
COL_CUMULATIVE_COMPLETED_UNIT_COST = '累计完工单位成本'
COL_CUMULATIVE_COMPLETED_AMOUNT = '累计完工金额'
INTEGRATED_WORKSHOP_NAME = '集成车间'  # 供应商字段不再向下填充


# 分析用的产品白名单和顺序，其他产品会被过滤掉
class CostingWorkbookETL:
    """
    Process a costing workbook into detail/quantity sheets.
    负责读 Excel->清洗->拆表->调分析模块->写 Excel
    """

    # 类变量:允许向下填充, 供应商编码和供应商名称在后面处理
    FILL_COLS = [
        COL_PERIOD,
        COL_COST_CENTER,
        COL_PRODUCT_CODE,
        COL_PRODUCT_NAME,
        COL_SPEC,
        COL_ORDER_NO,
        COL_ORDER_LINE,
        COL_VENDOR_CODE,
        COL_VENDOR_NAME,
        COL_UNIT,
        COL_PLAN_QTY,
        COL_PRODUCTION_TYPE,
        COL_DOC_TYPE,
    ]

    # 类变量:成本明细表保留物料相关列和成本项目列
    DETAIL_COLS = [
        COL_PERIOD,
        COL_MONTH,
        COL_COST_CENTER,
        COL_PRODUCT_CODE,
        COL_PRODUCT_NAME,
        COL_SPEC,
        COL_PRODUCTION_TYPE,
        COL_DOC_TYPE,
        COL_ORDER_NO,
        COL_ORDER_LINE,
        COL_VENDOR_CODE,
        COL_VENDOR_NAME,
        COL_UNIT,
        COL_PLAN_QTY,
        COL_COST_ITEM,
        COL_CHILD_MATERIAL,
        COL_CHILD_MATERIAL_NAME,
        COL_OPENING_WIP_QTY,
        COL_OPENING_WIP_AMOUNT,
        COL_OPENING_ADJUST_QTY,
        COL_OPENING_ADJUST_AMOUNT,
        COL_CURRENT_INPUT_QTY,
        COL_CURRENT_INPUT_AMOUNT,
        COL_CUMULATIVE_INPUT_QTY,
        COL_CUMULATIVE_INPUT_AMOUNT,
        COL_ENDING_WIP_QTY,
        COL_ENDING_WIP_AMOUNT,
        COL_CURRENT_COMPLETED_QTY,
        COL_CURRENT_COMPLETED_CONSUMPTION,
        COL_CURRENT_COMPLETED_UNIT_COST,
        COL_CURRENT_COMPLETED_AMOUNT,
        COL_CUMULATIVE_COMPLETED_QTY,
        COL_CUMULATIVE_COMPLETED_CONSUMPTION,
        COL_CUMULATIVE_COMPLETED_UNIT_COST,
        COL_CUMULATIVE_COMPLETED_AMOUNT,
    ]

    # 类变量:产品数量统计表保留工单层级的列，物料相关列和成本项目列都不保留
    QTY_COLS = [
        COL_PERIOD,
        COL_MONTH,
        COL_COST_CENTER,
        COL_PRODUCT_CODE,
        COL_PRODUCT_NAME,
        COL_SPEC,
        COL_PRODUCTION_TYPE,
        COL_DOC_TYPE,
        COL_ORDER_NO,
        COL_ORDER_LINE,
        COL_UNIT,
        COL_PLAN_QTY,
        COL_OPENING_WIP_QTY,
        COL_OPENING_WIP_AMOUNT,
        COL_CURRENT_INPUT_QTY,
        COL_CURRENT_INPUT_AMOUNT,
        COL_CUMULATIVE_INPUT_QTY,
        COL_CUMULATIVE_INPUT_AMOUNT,
        COL_ENDING_WIP_QTY,
        COL_ENDING_WIP_AMOUNT,
        COL_CURRENT_COMPLETED_QTY,
        COL_CURRENT_COMPLETED_CONSUMPTION,
        COL_CURRENT_COMPLETED_UNIT_COST,
        COL_CURRENT_COMPLETED_AMOUNT,
        COL_CUMULATIVE_COMPLETED_QTY,
        COL_CUMULATIVE_COMPLETED_CONSUMPTION,
        COL_CUMULATIVE_COMPLETED_UNIT_COST,
        COL_CUMULATIVE_COMPLETED_AMOUNT,
    ]

    def __init__(
        self,
        skip_rows: int = 2,
        *,
        product_order: tuple[tuple[str, str], ...] | None = None,
    ):
        # Excel原始数据通常有两行表头，默认跳过前两行
        self.skip_rows = skip_rows
        base_order = GB_PIPELINE.product_order if product_order is None else product_order
        normalized_order = tuple((str(code), str(name)) for code, name in base_order)
        self.product_order: tuple[tuple[str, str], ...] = normalized_order
        self.product_whitelist = frozenset(normalized_order)
        self._product_order_index: dict[tuple[str, str], int] = {
            pair: idx for idx, pair in enumerate(self.product_order)
        }
        self.workbook_writer = CostingWorkbookWriter()
        self.pipeline = CostingEtlPipeline(
            skip_rows=skip_rows,
            fill_columns=self.FILL_COLS,
            detail_columns=self.DETAIL_COLS,
            qty_columns=self.QTY_COLS,
            period_column=COL_PERIOD,
            cost_center_column=COL_COST_CENTER,
            child_material_column=COL_CHILD_MATERIAL,
            cost_item_column=COL_COST_ITEM,
            filled_cost_item_column=COL_FILLED_COST_ITEM,
            order_number_column=COL_ORDER_NO,
            vendor_columns=[COL_VENDOR_CODE, COL_VENDOR_NAME],
            integrated_workshop_name=INTEGRATED_WORKSHOP_NAME,
            logger=logger,
        )
        self.last_quality_metrics: tuple[QualityMetric, ...] = ()
        self.last_error_log_count: int = 0
        ensure_directories()

    def _log_quality_metrics(self, quality_metrics: tuple[QualityMetric, ...]) -> None:
        """将质量指标结果写入日志，避免继续输出到 Excel。"""
        for metric in quality_metrics:
            logger.info(
                'Quality metric | category=%s | metric=%s | value=%s | description=%s',
                metric.category,
                metric.metric,
                metric.value,
                metric.description,
            )

    def _load_raw_dataframe(self, input_path: Path) -> pd.DataFrame:
        """读取原始 workbook。"""
        return self.pipeline.load_raw_dataframe(input_path)

    def _resolve_columns(self, df: pd.DataFrame):
        """生成关键列契约。"""
        return self.pipeline.resolve_columns(df)

    def _auto_rename_columns(self, df: pd.DataFrame) -> dict[str, str]:
        """Infer key columns when source headers vary."""
        return self.pipeline.infer_rename_map(df)

    def _remove_total_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop summary rows containing '合计'."""
        return self.pipeline.remove_total_rows(df)

    def _forward_fill_with_rules(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """按业务规则执行向下填充。"""
        return self.pipeline.forward_fill_with_rules(df_raw)

    def _filter_fact_df_for_analysis(self, fact_df: pd.DataFrame) -> pd.DataFrame:
        """按白名单过滤分析数据，仅输出目标产品。"""
        return self._filter_dataframe_by_whitelist(
            fact_df,
            code_col='product_code',
            name_col='product_name',
            sort_cols=['period', 'cost_bucket'],
        )

    def _filter_dataframe_by_whitelist(
        self,
        df: pd.DataFrame,
        *,
        code_col: str,
        name_col: str,
        sort_cols: list[str] | None = None,
    ) -> pd.DataFrame:
        """按产品白名单过滤任意分析 DataFrame。"""
        actual_required_cols = {code_col, name_col}
        if not actual_required_cols.issubset(df.columns):
            missing_cols = sorted(actual_required_cols - set(df.columns))
            logger.warning('Skip analysis whitelist filter: missing columns=%s', missing_cols)
            return df

        if not self.product_order:
            logger.info('Skipping whitelist filter because no product order provided.')
            return df

        if df.empty:
            return df

        product_pairs = pd.MultiIndex.from_frame(df[[code_col, name_col]].astype(str))
        matched_mask = product_pairs.isin(self.product_whitelist)
        filtered_df = df.loc[matched_mask].copy()
        order_map = self._product_order_index
        filtered_pairs = pd.MultiIndex.from_frame(filtered_df[[code_col, name_col]].astype(str))
        filtered_df['_order_idx'] = filtered_pairs.map(order_map)
        order_cols = ['_order_idx']
        if sort_cols:
            order_cols.extend([col for col in sort_cols if col in filtered_df.columns])
        filtered_df = filtered_df.sort_values(order_cols).drop(columns=['_order_idx'])

        logger.info(
            'Analysis product whitelist filter applied: rows %s -> %s, products %s -> %s',
            len(df),
            len(filtered_df),
            df[[code_col, name_col]].drop_duplicates().shape[0],
            filtered_df[[code_col, name_col]].drop_duplicates().shape[0],
        )
        return filtered_df

    def _filter_product_anomaly_sections(
        self,
        sections: list[ProductAnomalySection],
    ) -> list[ProductAnomalySection]:
        """按白名单和既定顺序过滤兼容摘要分段。"""
        if not self.product_order:
            return sections
        filtered_sections = [
            section
            for section in sections
            if (str(section.product_code), str(section.product_name)) in self.product_whitelist
        ]
        return sorted(
            filtered_sections,
            key=lambda section: self._product_order_index[(str(section.product_code), str(section.product_name))],
        )

    def _split_sheets(
        self,
        df_raw: pd.DataFrame,
        df_filled: pd.DataFrame,
        target_mat: str,
        target_item: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Split source rows into detail and quantity sheets."""
        split_result = self.pipeline.split_sheets(df_raw, df_filled)
        return split_result.detail_df, split_result.qty_df

    def process_file(self, input_path: Path, output_path: Path) -> bool:
        """Read one workbook and write split output workbook."""
        try:
            self.last_quality_metrics = ()
            self.last_error_log_count = 0
            logger.info('Processing file: %s', input_path)
            df_raw = self._load_raw_dataframe(input_path)
            logger.info('Loaded rows=%s, cols=%s', len(df_raw), len(df_raw.columns))

            df_raw.columns = [clean_column_name(c) for c in df_raw.columns]
            resolved_columns = self._resolve_columns(df_raw)
            if resolved_columns.rename_map:
                df_raw.rename(columns=resolved_columns.rename_map, inplace=True)

            target_mat = resolved_columns.child_material_column
            target_item = resolved_columns.cost_item_column

            if target_mat not in df_raw.columns:
                logger.error("Missing required column '%s'; columns=%s", target_mat, df_raw.columns.tolist())
                return False

            df_raw = self._remove_total_rows(df_raw)
            df_filled = self._forward_fill_with_rules(df_raw)

            if target_item in df_filled.columns:
                df_filled[COL_FILLED_COST_ITEM] = df_filled[target_item].ffill()
            else:
                df_filled[COL_FILLED_COST_ITEM] = None

            df_detail, df_qty = self._split_sheets(df_raw, df_filled, target_mat, target_item)
            artifacts = build_report_artifacts(df_detail, df_qty)
            analysis_fact_df = self._filter_fact_df_for_analysis(artifacts.fact_df)
            analysis_tables = render_tables(analysis_fact_df)
            filtered_work_order_sheet = FlatSheet(
                data=self._filter_dataframe_by_whitelist(
                    artifacts.work_order_sheet.data,
                    code_col='产品编码',
                    name_col='产品名称',
                    sort_cols=['月份', '工单编号', '工单行'],
                ),
                column_types=artifacts.work_order_sheet.column_types,
            )
            product_anomaly_sections = self._filter_product_anomaly_sections(artifacts.product_anomaly_sections)
            error_log = artifacts.error_log.copy()
            self.last_quality_metrics = artifacts.quality_metrics
            self.last_error_log_count = len(error_log)
            self._log_quality_metrics(self.last_quality_metrics)
            logger.info('Quality issue count | error_log_rows=%s', self.last_error_log_count)

            self.workbook_writer.write_workbook(
                output_path,
                detail_df=df_detail,
                qty_sheet_df=artifacts.qty_sheet_df,
                analysis_tables=analysis_tables,
                work_order_sheet=filtered_work_order_sheet,
                product_anomaly_sections=product_anomaly_sections,
                error_log=error_log,
            )

            logger.info(
                'Output saved: %s (detail=%s, qty=%s)', output_path, len(df_detail), len(artifacts.qty_sheet_df)
            )
            if not error_log.empty:
                logger.warning('Detected %s data quality issues, check sheet error_log', len(error_log))
            return True
        except Exception as exc:
            logger.error('Processing failed: %s', exc, exc_info=True)
            return False


def _find_input_files() -> list[Path]:
    """Match GB costing files; allow filename variants with/without a space."""
    patterns = [
        'GB-*成本计算单*.xlsx',
        'GB-* 成本计算单*.xlsx',
        'GB-*.xlsx',
    ]

    matched: list[Path] = []
    seen: set[Path] = set()
    for pattern in patterns:
        for path in sorted(GB_RAW_DIR.glob(pattern)):
            if path not in seen:
                seen.add(path)
                matched.append(path)
    return matched


def main() -> None:
    """Entry point for GB costing ETL."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    etl = CostingWorkbookETL(skip_rows=2)
    input_files = _find_input_files()

    if not input_files:
        logger.warning('No GB costing file found under %s', GB_RAW_DIR)
        return

    input_file = input_files[0]
    logger.info('Matched input file: %s', input_file.name)
    output_file = GB_PROCESSED_DIR / f'{input_file.stem}_处理后.xlsx'

    if etl.process_file(input_file, output_file):
        print('处理成功')
    else:
        print('处理失败')


if __name__ == '__main__':
    raise SystemExit('Use `python main.py gb` or `python main.py sk` instead.')

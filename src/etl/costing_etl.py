"""
ETL pipeline for costing workbooks.
Excel原始成本计算单 -> 清洗双层表头 -> 规则填充 -> 拆成成本明细/数量页
-> 生成价量分析/异常分析/校验表 -> 写回一个新Excel
"""

import logging
import sys
from pathlib import Path
from time import perf_counter

import pandas as pd

try:
    from src.analytics.contracts import ProductAnomalySection, QualityMetric, SheetModel, WorkbookPayload
    from src.config.pipelines import GB_PIPELINE
    from src.config.settings import GB_PROCESSED_DIR, GB_RAW_DIR, ensure_directories
    from src.etl.pipeline import CostingEtlPipeline
    from src.excel.workbook_writer import CostingWorkbookWriter
except ModuleNotFoundError:
    # 直接执行 src/etl/costing_etl.py 时，解释器搜索路径不含项目根目录，补齐后重试导入。
    project_root = Path(__file__).resolve().parents[2]
    project_root_str = str(project_root)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)
    from src.analytics.contracts import ProductAnomalySection, QualityMetric, SheetModel, WorkbookPayload
    from src.config.pipelines import GB_PIPELINE
    from src.config.settings import GB_PROCESSED_DIR, GB_RAW_DIR, ensure_directories
    from src.etl.pipeline import CostingEtlPipeline
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
        standalone_cost_items: tuple[str, ...] | None = None,
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
        base_standalone_items = (
            GB_PIPELINE.standalone_cost_items if standalone_cost_items is None else standalone_cost_items
        )
        normalized_items: list[str] = []
        for item in base_standalone_items:
            normalized = str(item).strip()
            if normalized:
                normalized_items.append(normalized)
        self.standalone_cost_items = tuple(normalized_items)
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

    def _filter_sheet_model_by_whitelist(
        self,
        model: SheetModel,
        *,
        code_col: str,
        name_col: str,
        sort_cols: tuple[str, ...] = (),
    ) -> SheetModel:
        """按白名单过滤展示层 SheetModel。"""
        if not self.product_order:
            return model

        header_map = {column_name: idx for idx, column_name in enumerate(model.columns)}
        if code_col not in header_map or name_col not in header_map:
            missing_cols = [column for column in (code_col, name_col) if column not in header_map]
            logger.warning('Skip analysis whitelist filter: missing columns=%s', missing_cols)
            return model

        rows = tuple(model.rows_factory())
        if not rows:
            return model

        code_idx = header_map[code_col]
        name_idx = header_map[name_col]
        filtered_rows = [
            row
            for row in rows
            if (str(row[code_idx]), str(row[name_idx])) in self.product_whitelist
        ]
        if not filtered_rows:
            return SheetModel(
                sheet_name=model.sheet_name,
                columns=model.columns,
                rows_factory=lambda: iter(()),
                column_types=model.column_types,
                number_formats=model.number_formats,
                freeze_panes=model.freeze_panes,
                auto_filter=model.auto_filter,
                fixed_width=model.fixed_width,
                conditional_formats=model.conditional_formats,
            )

        order_map = self._product_order_index

        def _sort_key(row: tuple[object, ...]) -> tuple[object, ...]:
            key: list[object] = [order_map[(str(row[code_idx]), str(row[name_idx]))]]
            for column_name in sort_cols:
                if column_name in header_map:
                    value = row[header_map[column_name]]
                    key.append('' if value is None else str(value))
            return tuple(key)

        filtered_rows.sort(key=_sort_key)
        logger.info(
            'Analysis product whitelist filter applied: rows %s -> %s, products %s -> %s',
            len(rows),
            len(filtered_rows),
            len({(str(row[code_idx]), str(row[name_idx])) for row in rows}),
            len({(str(row[code_idx]), str(row[name_idx])) for row in filtered_rows}),
        )
        frozen_rows = tuple(filtered_rows)
        return SheetModel(
            sheet_name=model.sheet_name,
            columns=model.columns,
            rows_factory=lambda rows=frozen_rows: iter(rows),
            column_types=model.column_types,
            number_formats=model.number_formats,
            freeze_panes=model.freeze_panes,
            auto_filter=model.auto_filter,
            fixed_width=model.fixed_width,
            conditional_formats=model.conditional_formats,
        )

    def _filter_workbook_payload_by_whitelist(self, payload: WorkbookPayload) -> WorkbookPayload:
        """只对白名单相关分析页应用过滤与排序。"""
        filtered_models: list[SheetModel] = []
        for model in payload.sheet_models:
            if model.sheet_name in {'直接材料_价量比', '直接人工_价量比', '制造费用_价量比'}:
                filtered_models.append(
                    self._filter_sheet_model_by_whitelist(
                        model,
                        code_col='产品编码',
                        name_col='产品名称',
                        sort_cols=('月份',),
                    )
                )
                continue
            if model.sheet_name == '按工单按产品异常值分析':
                filtered_models.append(
                    self._filter_sheet_model_by_whitelist(
                        model,
                        code_col='产品编码',
                        name_col='产品名称',
                        sort_cols=('月份', '工单编号', '工单行'),
                    )
                )
                continue
            if model.sheet_name == '按产品异常值分析':
                filtered_models.append(
                    self._filter_sheet_model_by_whitelist(
                        model,
                        code_col='产品编码',
                        name_col='产品名称',
                        sort_cols=('月份',),
                    )
                )
                continue
            filtered_models.append(model)

        return WorkbookPayload(
            sheet_models=tuple(filtered_models),
            quality_metrics=payload.quality_metrics,
            error_log_count=payload.error_log_count,
            stage_timings=payload.stage_timings,
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
            total_start = perf_counter()
            self.last_quality_metrics = ()
            self.last_error_log_count = 0
            logger.info('Processing file: %s', input_path)

            payload = self.pipeline.build_workbook_payload(
                input_path,
                standalone_cost_items=self.standalone_cost_items,
            )
            filtered_payload = self._filter_workbook_payload_by_whitelist(payload)
            self.last_quality_metrics = filtered_payload.quality_metrics
            self.last_error_log_count = filtered_payload.error_log_count
            self._log_quality_metrics(self.last_quality_metrics)
            logger.info('Quality issue count | error_log_rows=%s', self.last_error_log_count)
            for stage_name, seconds in filtered_payload.stage_timings.items():
                logger.info('Timing | stage=%s | seconds=%.3f', stage_name, seconds)

            export_start = perf_counter()
            self.workbook_writer.write_workbook_from_models(
                output_path,
                sheet_models=filtered_payload.sheet_models,
            )
            logger.info('Timing | stage=export | seconds=%.3f', perf_counter() - export_start)
            logger.info('Timing | stage=total | seconds=%.3f', perf_counter() - total_start)
            logger.info('Output saved: %s', output_path)
            if self.last_error_log_count > 0:
                logger.warning('Detected %s data quality issues, check sheet error_log', self.last_error_log_count)
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

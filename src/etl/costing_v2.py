"""ETL pipeline for costing workbooks."""

import logging
import sys
from pathlib import Path

import pandas as pd

try:
    from src.config.settings import GB_PROCESSED_DIR, GB_RAW_DIR, ensure_directories
    from src.etl.utils import clean_column_name, format_period_col
except ModuleNotFoundError:
    # 直接执行 src/etl/costing_v2.py 时，解释器搜索路径不含项目根目录，补齐后重试导入。
    project_root = Path(__file__).resolve().parents[2]
    project_root_str = str(project_root)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)
    from src.config.settings import GB_PROCESSED_DIR, GB_RAW_DIR, ensure_directories
    from src.etl.utils import clean_column_name, format_period_col

logger = logging.getLogger(__name__)


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
COL_FILLED_COST_ITEM = 'Filled_成本项目'
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
COL_CUMULATIVE_COMPLETED_QTY_1_11 = '累计完工1-11数量'
COL_CUMULATIVE_COMPLETED_CONSUMPTION = '累计完工单耗'
COL_CUMULATIVE_COMPLETED_UNIT_COST = '累计完工单位成本'
COL_CUMULATIVE_COMPLETED_AMOUNT = '累计完工金额'


class CostingETL:
    """Process a costing workbook into detail/quantity sheets."""

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
        COL_CUMULATIVE_COMPLETED_QTY_1_11,
        COL_CUMULATIVE_COMPLETED_QTY,
        COL_CUMULATIVE_COMPLETED_CONSUMPTION,
        COL_CUMULATIVE_COMPLETED_UNIT_COST,
        COL_CUMULATIVE_COMPLETED_AMOUNT,
    ]

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

    def __init__(self, skip_rows: int = 2):
        self.skip_rows = skip_rows
        ensure_directories()

    def _auto_rename_columns(self, df: pd.DataFrame) -> dict[str, str]:
        """Infer key columns when source headers vary."""
        col_map: dict[str, str] = {}

        if COL_CHILD_MATERIAL not in df.columns:
            possible = [c for c in df.columns if '物料编码' in c or '子件' in c]
            if possible:
                col_map[possible[0]] = COL_CHILD_MATERIAL
                logger.info('Column rename: %s -> %s', possible[0], COL_CHILD_MATERIAL)

        if COL_COST_ITEM not in df.columns:
            possible = [c for c in df.columns if '成本项目' in c or '费用项目' in c]
            if possible:
                col_map[possible[0]] = COL_COST_ITEM
                logger.info('Column rename: %s -> %s', possible[0], COL_COST_ITEM)

        return col_map

    def _remove_total_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop summary rows containing '合计'."""
        initial_rows = len(df)
        cols_to_check = [c for c in df.columns[:3] if c in [COL_PERIOD, COL_COST_CENTER]]
        if not cols_to_check:
            return df

        mask_keep = pd.Series([True] * len(df), index=df.index)
        for col in cols_to_check:
            is_total = df[col].astype(str).str.contains('合计', na=False)
            mask_keep &= ~is_total

        result = df[mask_keep].copy()
        removed = initial_rows - len(result)
        if removed > 0:
            logger.info('Removed total rows: %s', removed)
        return result

    def _split_sheets(
        self,
        df_raw: pd.DataFrame,
        df_filled: pd.DataFrame,
        target_mat: str,
        target_item: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Split source rows into detail and quantity sheets."""
        cond_no_material = df_raw[target_mat].isna() | (df_raw[target_mat].astype(str).str.strip() == '')

        if target_item in df_raw.columns:
            cond_no_cost_item = df_raw[target_item].isna() | (df_raw[target_item].astype(str).str.strip() == '')
        else:
            cond_no_cost_item = True

        if COL_ORDER_NO in df_filled.columns:
            cond_has_order = df_filled[COL_ORDER_NO].notna()
        else:
            cond_has_order = True

        df_qty = df_filled[cond_no_material & cond_no_cost_item & cond_has_order].copy()
        df_qty = format_period_col(df_qty)
        qty_cols_final = [c for c in self.QTY_COLS if c in df_qty.columns]
        if qty_cols_final:
            df_qty = df_qty[qty_cols_final]

        cond_is_material = df_filled[target_mat].notna() & (df_filled[target_mat].astype(str).str.strip() != '')

        if target_item in df_raw.columns:
            cond_is_expense = (
                df_raw[target_mat].isna()
                & df_raw[target_item].notna()
                & (df_raw[target_item].astype(str).str.strip() != '直接材料')
            )
        else:
            cond_is_expense = False

        df_detail = df_filled[cond_is_material | cond_is_expense].copy()
        if COL_FILLED_COST_ITEM in df_detail.columns and target_item in df_detail.columns:
            df_detail[target_item] = df_detail[COL_FILLED_COST_ITEM]

        df_detail = format_period_col(df_detail)
        detail_cols_final = [c for c in self.DETAIL_COLS if c in df_detail.columns]
        if detail_cols_final:
            df_detail = df_detail[detail_cols_final]

        return df_detail, df_qty

    def process_file(self, input_path: Path, output_path: Path) -> bool:
        """Read one workbook and write split output workbook."""
        try:
            logger.info('Processing file: %s', input_path)
            df_raw = pd.read_excel(input_path, header=[0, 1], skiprows=self.skip_rows, engine='openpyxl')
            logger.info('Loaded rows=%s, cols=%s', len(df_raw), len(df_raw.columns))

            df_raw.columns = [clean_column_name(c) for c in df_raw.columns]
            col_map = self._auto_rename_columns(df_raw)
            if col_map:
                df_raw.rename(columns=col_map, inplace=True)

            target_mat = COL_CHILD_MATERIAL
            target_item = COL_COST_ITEM

            if target_mat not in df_raw.columns:
                logger.error("Missing required column '%s'; columns=%s", target_mat, df_raw.columns.tolist())
                return False

            df_raw = self._remove_total_rows(df_raw)
            df_filled = df_raw.copy()

            cols_to_fill = [c for c in df_filled.columns if c in self.FILL_COLS]
            if cols_to_fill:
                df_filled[cols_to_fill] = df_filled[cols_to_fill].ffill()

            if target_item in df_filled.columns:
                df_filled[COL_FILLED_COST_ITEM] = df_filled[target_item].ffill()
            else:
                df_filled[COL_FILLED_COST_ITEM] = None

            df_detail, df_qty = self._split_sheets(df_raw, df_filled, target_mat, target_item)

            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df_detail.to_excel(writer, sheet_name='成本明细', index=False)
                df_qty.to_excel(writer, sheet_name='产品数量统计', index=False)

            logger.info('Output saved: %s (detail=%s, qty=%s)', output_path, len(df_detail), len(df_qty))
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

    etl = CostingETL(skip_rows=2)
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
    main()

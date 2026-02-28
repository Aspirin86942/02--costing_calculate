import pandas as pd
import re
import os
import sys
import logging

# ================= 配置区 =================
# 如果表头上面有几行废话（比如报表标题），请设置跳过的行数
# 您现在的需求是删除一开始的两行，所以这里设为 2
SKIP_ROWS = 2  
# =========================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_column_name(col_tuple: tuple) -> str:
    """清理并扁平化双层表头"""
    c0 = str(col_tuple[0]).strip()
    c1 = str(col_tuple[1]).strip()
    
    # 清除 Unnamed 和换行符/空格
    if 'Unnamed' in c0: c0 = ''
    if 'Unnamed' in c1: c1 = ''
    c0 = c0.replace(' ', '').replace('\n', '')
    c1 = c1.replace(' ', '').replace('\n', '')

    if c0 and c1 and c0 != c1:
        return f"{c0}{c1}"
    elif c1:
        return c1
    else:
        return c0

def process_cost_audit_sheet(input_file: str, output_file: str) -> None:
    if not os.path.exists(input_file):
        logger.error(f"文件未找到: {input_file}")
        return

    logger.info(f"读取 Excel: {input_file}")
    logger.info(f"配置: 跳过前 {SKIP_ROWS} 行读取表头...")

    try:
        # --- 关键修改：skiprows=SKIP_ROWS ---
        # 意思：跳过前2行，把第3、4行作为双层表头([0,1])
        df = pd.read_excel(
            input_file, 
            header=[0, 1], 
            skiprows=SKIP_ROWS, 
            engine='openpyxl'
        )
    except Exception as e:
        logger.critical(f"读取失败: {e}")
        return

    # 扁平化列名
    raw_columns = [clean_column_name(col) for col in df.columns]
    df.columns = [c.strip() for c in raw_columns]
    
    # 打印列名供核对
    logger.info(f"识别到的列名(前5个): {df.columns.tolist()[:5]} ...") 

    # --- 核心列检查 ---
    target_col = '子项物料编码'
    if target_col not in df.columns:
        # 模糊搜索容错
        possible = [c for c in df.columns if '物料编码' in c or '子件' in c]
        if possible:
            logger.warning(f"自动修正列名: {possible[0]} -> {target_col}")
            df.rename(columns={possible[0]: target_col}, inplace=True)
        else:
            logger.critical(f"仍然找不到 '{target_col}'。请检查 SKIP_ROWS 是否应该改为 0, 1 或 3？")
            logger.critical(f"当前读取到的所有列名: {df.columns.tolist()}")
            return

    # 3. 维度填充 (Forward Fill)
    fill_cols = [
        '年期', '成本中心名称', '产品编码', '产品名称', '规格型号', 
        '工单编号', '工单行号', '供应商编码', '供应商名称', 
        '基本单位', '计划产量'
    ]
    existing = [c for c in fill_cols if c in df.columns]
    df[existing] = df[existing].ffill()

    # 4. 成本项目名称处理
    if '成本项目名称' in df.columns:
        df['原始_成本项目名称'] = df['成本项目名称']
        df['成本项目名称'] = df['成本项目名称'].ffill()
    else:
        df['原始_成本项目名称'] = None

    # 5. 筛选逻辑
    has_material = df[target_col].notna() & (df[target_col].astype(str).str.strip() != '')
    
    if '原始_成本项目名称' in df.columns:
        is_expense_row = (
            (df[target_col].isna()) & 
            (df['原始_成本项目名称'].notna()) & 
            (df['原始_成本项目名称'] != '直接材料')
        )
    else:
        is_expense_row = pd.Series([False] * len(df))

    df_processed = df[has_material | is_expense_row].copy()

    # 6. 生成月份
    if '年期' in df_processed.columns:
        def format_period(val):
            if pd.isna(val): return val
            match = re.search(r'(\d+)年(\d+)期', str(val))
            if match:
                y, m = match.groups()
                return f"{y}年{int(m):02d}期"
            return val
        df_processed.insert(1, '月份', df_processed['年期'].apply(format_period))

    # 7. 导出
    try:
        df_processed.to_excel(output_file, index=False)
        logger.info(f"处理完成! 行数: {len(df)} -> {len(df_processed)}")
        logger.info(f"文件保存至: {output_file}")
    except Exception as e:
        logger.error(f"保存失败: {e}")

if __name__ == '__main__':
    # 请确认此处文件名是否正确
    SOURCE = r"D:\03- Program\02- special\02- costing_calculate\成本计算单_20251-11月.xlsx"
    TARGET = r"D:\03- Program\02- special\02- costing_calculate\数控-成本计算单_处理后11.xlsx"
    
    process_cost_audit_sheet(SOURCE, TARGET)
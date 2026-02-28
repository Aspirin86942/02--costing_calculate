import pandas as pd
import re
import os
import sys
import logging

# ================= 配置区 =================
# 原始文件路径 (输入)
SOURCE_FILE = r"D:\03- Program\02- special\02- costing_calculate\data\raw\gb\GB-成本计算单_2026011910255936_100160.xlsx"

# 输出文件路径
TARGET_FILE = r"D:\03- Program\02- special\02- costing_calculate\data\processed\gb\GB-成本计算单_自动拆分版.xlsx"

# 表头处理配置 (根据您的文件，前2行是废话)
SKIP_ROWS = 2
# =========================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_column_name(col_tuple: tuple) -> str:
    """清理并扁平化双层表头"""
    c0 = str(col_tuple[0]).strip()
    c1 = str(col_tuple[1]).strip()
    
    if 'Unnamed' in c0: c0 = ''
    if 'Unnamed' in c1: c1 = ''
    # 强制去空格
    c0 = c0.replace(' ', '').replace('\n', '')
    c1 = c1.replace(' ', '').replace('\n', '')

    if c0 and c1 and c0 != c1:
        return f"{c0}{c1}"
    elif c1:
        return c1
    else:
        return c0

def format_period_col(df: pd.DataFrame) -> pd.DataFrame:
    """统一生成月份列"""
    if '年期' in df.columns:
        def _fmt(val):
            if pd.isna(val): return val
            match = re.search(r'(\d+)年(\d+)期', str(val))
            if match:
                return f"{match.group(1)}年{int(match.group(2)):02d}期"
            return val
        
        # 插入到第二列
        if '月份' not in df.columns:
            df.insert(1, '月份', df['年期'].apply(_fmt))
    return df

def process_costing_data(input_path, output_path):
    if not os.path.exists(input_path):
        logger.error(f"文件不存在: {input_path}")
        return

    logger.info(f"开始读取: {input_path} (跳过前 {SKIP_ROWS} 行)")
    
    try:
        # 1. 读取原始数据
        df_raw = pd.read_excel(input_path, header=[0, 1], skiprows=SKIP_ROWS, engine='openpyxl')
    except Exception as e:
        logger.critical(f"读取失败: {e}")
        return

    # 2. 清洗表头
    df_raw.columns = [clean_column_name(c) for c in df_raw.columns]
    
    # 3. 关键列名定位 (自动修正)
    col_map = {}
    
    # 寻找【子项物料编码】
    target_mat = '子项物料编码'
    if target_mat not in df_raw.columns:
        possible = [c for c in df_raw.columns if '物料编码' in c or '子件' in c]
        if possible:
            col_map[possible[0]] = target_mat
            logger.info(f"列名映射: {possible[0]} -> {target_mat}")
    
    # 寻找【成本项目名称】
    target_item = '成本项目名称'
    if target_item not in df_raw.columns:
         possible = [c for c in df_raw.columns if '成本项目' in c or '费用项目' in c]
         if possible:
             col_map[possible[0]] = target_item
    
    # 应用重命名
    if col_map:
        df_raw.rename(columns=col_map, inplace=True)
        
    # 再次检查关键列是否存在，若不存在则无法继续
    if target_mat not in df_raw.columns:
        logger.critical(f"无法识别 '{target_mat}' 列，请检查源文件表头。")
        logger.info(f"当前列名: {df_raw.columns.tolist()}")
        return

    # ================= [关键改动点2] 数据清洗：在填充前剔除合计行 =================
    # 目的：防止 'ffill' 把上一行的工单信息填充到最后的合计行里
    initial_rows = len(df_raw)
    
    # 检查前几列（通常是年期或成本中心）是否包含“合计”
    cols_to_check = [c for c in df_raw.columns[:3] if c in ['年期', '成本中心名称']]
    mask_keep = pd.Series([True] * len(df_raw))
    
    for col in cols_to_check:
        is_total = df_raw[col].astype(str).str.contains('合计', na=False)
        mask_keep = mask_keep & (~is_total)
        
    df_raw = df_raw[mask_keep].copy()
    
    if len(df_raw) < initial_rows:
        logger.info(f"已剔除包含'合计'的汇总行: {initial_rows - len(df_raw)} 行")
    # =======================================================================


    # ================= 核心分支处理 =================

    # 准备工作：先对原始数据做一次基础的填充，用于提取公共信息
    # 注意：为了区分“表头行”和“明细行”，我们在填充前先备份一下原始状态
    df_filled = df_raw.copy()
    
    fill_cols = [
        '年期', '成本中心名称', '产品编码', '产品名称', '规格型号', 
        '工单编号', '工单行号', '供应商编码', '供应商名称', 
        '基本单位', '计划产量',
        # 可能存在的其他维度
        '生产类型', '单据类型'
    ]
    # 仅填充存在的列
    cols_to_fill = [c for c in df_filled.columns if c in fill_cols]
    df_filled[cols_to_fill] = df_filled[cols_to_fill].ffill()

    # 特殊：成本项目名称也需要填充给明细行用，但原始的空值对于判断Header很重要
    # 所以我们增加一列 'Filled_成本项目'
    if target_item in df_filled.columns:
        df_filled['Filled_成本项目'] = df_filled[target_item].ffill()
    else:
        df_filled['Filled_成本项目'] = None

    # --- 生成 Sheet 2: 产品数量统计 (Quantity) ---
    logger.info("正在生成 Sheet2 (产品数量统计)...")
    
    # 逻辑：提取“工单头行”
    # 特征：有工单号，但【子项物料编码】为空，且【成本项目名称】为空(或者不是具体的费用)
    # 大多数报表里，Header行的“成本项目名称”是空的，或者是“直接材料”的汇总
    # 我们可以用 '子项物料编码' 为空 AND ('成本项目名称' 为空) 来定位 Header
    
    # 判断标准：
    # 1. 物料编码必须为空 (不是材料行)
    cond_no_material = df_raw[target_mat].isna() | (df_raw[target_mat].astype(str).str.strip() == '')
    
    # 2. 成本项目必须为空 (排除 '直接人工' 等费用行)
    # 注意：这里要用未填充的原始 df_raw 来判断空值
    if target_item in df_raw.columns:
        cond_no_cost_item = df_raw[target_item].isna() | (df_raw[target_item].astype(str).str.strip() == '')
    else:
        cond_no_cost_item = True # 如果没有这列，默认符合条件

    # 3. 必须有工单编号 (防止空行)
    if '工单编号' in df_filled.columns:
        cond_has_order = df_filled['工单编号'].notna()
    else:
        cond_has_order = True

    # 筛选 Sheet2 数据
    # 使用 df_filled 因为我们需要填充好的“产品名称”、“年期”等信息
    df_qty = df_filled[cond_no_material & cond_no_cost_item & cond_has_order].copy()
    
    # 格式化 Sheet2
    df_qty = format_period_col(df_qty)
    
    # 选择 Sheet2 输出列 (参考您的需求)
    qty_cols_target = [
        '年期', '月份', '成本中心名称', '产品编码', '产品名称', '规格型号', 
        '生产类型', '单据类型', '工单编号', '工单行号', '基本单位', 
        '计划产量', 
        '期初在产品数量', '期初在产品金额', 
        '本期投入数量', '本期投入金额', 
        '累计投入数量', '累计投入金额',
        '期末在产品数量', '期末在产品金额',
        '本期完工数量', '本期完工单耗', '本期完工单位成本', '本期完工金额',
        '累计完工数量', '累计完工单耗', '累计完工单位成本', '累计完工金额'
    ]
    # 只保留存在的列
    qty_cols_final = [c for c in qty_cols_target if c in df_qty.columns]
    df_qty = df_qty[qty_cols_final]


    # --- 生成 Sheet 1: 成本明细 (Details) ---
    logger.info("正在生成 Sheet1 (成本明细)...")
    
    # 逻辑：
    # 1. 具体的物料行 (物料编码不为空)
    cond_is_material = df_filled[target_mat].notna() & (df_filled[target_mat].astype(str).str.strip() != '')
    
    # 2. 具体的费用行 (物料为空，但成本项目不为空，且不等于 '直接材料' 这种汇总头)
    # 使用 'Filled_成本项目' 确保每一行都有项目归属，
    # 但排除掉上面已经提取过的 Header (即那些原始成本项目为空的行)
    # 实际上，只要 'Filled_成本项目' 有值，且不是 Header 行即可。
    # 更好的方法：排除掉刚才选进 Sheet2 的行，剩下的就是 Sheet1 的行 (除了垃圾空行)
    
    # 简单粗暴逻辑：保留 (有物料) OR (是费用)
    # 费用行定义：原始 Cost Item 有值，且不等于 '直接材料' (通常 '直接材料' 是汇总行，只有金额没有数量)
    if target_item in df_raw.columns:
        cond_is_expense = (
            df_raw[target_mat].isna() & 
            df_raw[target_item].notna() & 
            (df_raw[target_item] != '直接材料')
        )
    else:
        cond_is_expense = False
        
    df_detail = df_filled[cond_is_material | cond_is_expense].copy()
    
    # 使用填充后的成本项目名称覆盖回去
    if 'Filled_成本项目' in df_detail.columns and target_item in df_detail.columns:
        df_detail[target_item] = df_detail['Filled_成本项目']
        
    df_detail = format_period_col(df_detail)
     
    # 定义 Sheet1 输出顺序
    detail_cols_target = [
        '年期', '月份', '成本中心名称', '产品编码', '产品名称', '规格型号', 
        '生产类型', '单据类型',
        '工单编号', '工单行号', '供应商编码', '供应商名称', '基本单位', '计划产量', 
        '成本项目名称', '子项物料编码', '子项物料名称', 
        '期初在产品数量', '期初在产品金额', '期初调整数量', '期初调整金额', 
        '本期投入数量', '本期投入金额', '累计投入数量', '累计投入金额', 
        '期末在产品数量', '期末在产品金额', 
        '本期完工数量', '本期完工单耗', '本期完工单位成本', '本期完工金额', 
        '累计完工1-11数量', '累计完工单耗', '累计完工单位成本', '累计完工金额'
    ]
    detail_cols_final = [c for c in detail_cols_target if c in df_detail.columns]
    df_detail = df_detail[detail_cols_final]

    # ================= 写入文件 =================
    logger.info(f"写入 Excel: {output_path}")
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df_detail.to_excel(writer, sheet_name='成本明细', index=False)
            df_qty.to_excel(writer, sheet_name='产品数量统计', index=False)
        logger.info(f"处理成功！\n- 成本明细: {len(df_detail)} 行\n- 数量统计: {len(df_qty)} 行")
    except Exception as e:
        logger.error(f"写入失败: {e}")

if __name__ == '__main__':
    process_costing_data(SOURCE_FILE, TARGET_FILE)
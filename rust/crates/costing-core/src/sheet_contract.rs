pub const DETAIL_SHEET_COLUMNS: &[&str] = &[
    "年期",
    "月份",
    "成本中心名称",
    "产品编码",
    "产品名称",
    "规格型号",
    "生产类型",
    "单据类型",
    "工单编号",
    "工单行号",
    "供应商编码",
    "供应商名称",
    "基本单位",
    "计划产量",
    "成本项目名称",
    "子项物料编码",
    "子项物料名称",
    "期初在产品数量",
    "期初在产品金额",
    "期初调整数量",
    "期初调整金额",
    "本期投入数量",
    "本期投入金额",
    "累计投入数量",
    "累计投入金额",
    "期末在产品数量",
    "期末在产品金额",
    "本期完工数量",
    "本期完工单耗",
    "本期完工单位成本",
    "本期完工金额",
    "累计完工数量",
    "累计完工单耗",
    "累计完工单位成本",
    "累计完工金额",
];

pub const QTY_SHEET_BASE_COLUMNS: &[&str] = &[
    "年期",
    "月份",
    "成本中心名称",
    "产品编码",
    "产品名称",
    "规格型号",
    "生产类型",
    "单据类型",
    "工单编号",
    "工单行号",
    "基本单位",
    "计划产量",
    "期初在产品数量",
    "期初在产品金额",
    "本期投入数量",
    "本期投入金额",
    "累计投入数量",
    "累计投入金额",
    "期末在产品数量",
    "期末在产品金额",
    "本期完工数量",
    "本期完工单耗",
    "本期完工单位成本",
    "本期完工金额",
    "累计完工数量",
    "累计完工单耗",
    "累计完工单位成本",
    "累计完工金额",
];

pub fn detail_sheet_columns(source_columns: &[String]) -> Vec<String> {
    visible_contract_columns(source_columns, DETAIL_SHEET_COLUMNS)
}

pub fn qty_sheet_base_columns(source_columns: &[String]) -> Vec<String> {
    visible_contract_columns(source_columns, QTY_SHEET_BASE_COLUMNS)
}

fn visible_contract_columns(source_columns: &[String], contract_columns: &[&str]) -> Vec<String> {
    contract_columns
        .iter()
        .filter(|column| source_columns.iter().any(|source| source == **column))
        .map(|column| (*column).to_string())
        .collect()
}

use std::collections::BTreeMap;

use rust_decimal::Decimal;

use crate::model::{CellValue, FactBundle, SheetModel, TableRow};
use crate::pipeline::PipelineConfig;
use crate::scoring::{
    decimal_ln, grade_score, modified_z_score, resolve_effective_log_mad, weighted_mad,
    weighted_median,
};

const ZERO: Decimal = Decimal::ZERO;
const NORMAL_SCOPE: &str = "正常生产";
const REWORK_SCOPE: &str = "返工生产";
const UNKNOWN_SCOPE: &str = "未归类";

const BASE_WORK_ORDER_COLUMNS: &[&str] = &[
    "月份",
    "成本中心",
    "产品编码",
    "产品名称",
    "规格型号",
    "工单编号",
    "工单行",
    "生产类型",
    "基本单位",
    "本期完工数量",
    "总完工成本",
    "直接材料合计完工金额",
    "直接人工合计完工金额",
    "制造费用合计完工金额",
    "制造费用_其他合计完工金额",
    "制造费用_人工合计完工金额",
    "制造费用_机物料及低耗合计完工金额",
    "制造费用_折旧合计完工金额",
    "制造费用_水电费合计完工金额",
    "总单位完工成本",
    "直接材料单位完工成本",
    "直接人工单位完工成本",
    "制造费用单位完工成本",
    "制造费用_其他单位完工成本",
    "制造费用_人工单位完工成本",
    "制造费用_机物料及低耗单位完工成本",
    "制造费用_折旧单位完工成本",
    "制造费用_水电费单位完工成本",
    "是否可参与分析",
    "异常等级",
    "异常主要来源",
    "异常明细解释",
    "复核原因",
];

#[derive(Clone, Copy)]
struct Metric {
    key: &'static str,
    display_name: &'static str,
    source_label: &'static str,
    explanation_label: &'static str,
}

const ANOMALY_METRICS: &[Metric] = &[
    Metric {
        key: "total_unit_cost",
        display_name: "总单位完工成本",
        source_label: "总成本异常",
        explanation_label: "总成本",
    },
    Metric {
        key: "dm_unit_cost",
        display_name: "直接材料单位完工成本",
        source_label: "材料异常",
        explanation_label: "直接材料",
    },
    Metric {
        key: "dl_unit_cost",
        display_name: "直接人工单位完工成本",
        source_label: "人工异常",
        explanation_label: "直接人工",
    },
    Metric {
        key: "moh_unit_cost",
        display_name: "制造费用单位完工成本",
        source_label: "制造费用异常",
        explanation_label: "制造费用",
    },
    Metric {
        key: "moh_other_unit_cost",
        display_name: "制造费用_其他单位完工成本",
        source_label: "其他异常",
        explanation_label: "制造费用_其他",
    },
    Metric {
        key: "moh_labor_unit_cost",
        display_name: "制造费用_人工单位完工成本",
        source_label: "制造费用人工异常",
        explanation_label: "制造费用_人工",
    },
    Metric {
        key: "moh_consumables_unit_cost",
        display_name: "制造费用_机物料及低耗单位完工成本",
        source_label: "机物料及低耗异常",
        explanation_label: "制造费用_机物料及低耗",
    },
    Metric {
        key: "moh_depreciation_unit_cost",
        display_name: "制造费用_折旧单位完工成本",
        source_label: "折旧异常",
        explanation_label: "制造费用_折旧",
    },
    Metric {
        key: "moh_utilities_unit_cost",
        display_name: "制造费用_水电费单位完工成本",
        source_label: "水电费异常",
        explanation_label: "制造费用_水电费",
    },
];

#[derive(Default)]
struct MetricAudit {
    flag: String,
    score: Option<Decimal>,
    current_log: Option<Decimal>,
    center_log: Option<Decimal>,
    raw_mad: Option<Decimal>,
    effective_mad: Option<Decimal>,
    sample_size: usize,
}

struct AnomalyRow<'a> {
    source: &'a TableRow,
    numbers: BTreeMap<String, Decimal>,
    production_scope: String,
    can_analyze: bool,
    reasons: Vec<String>,
    audits: BTreeMap<&'static str, MetricAudit>,
    anomaly_level: String,
    anomaly_source: String,
    detail_explanation: String,
}

pub fn build_work_order_anomaly_sheet(bundle: &FactBundle, config: &PipelineConfig) -> SheetModel {
    let columns = work_order_columns(config);
    let mut rows = analysis_work_order_rows(bundle, config)
        .into_iter()
        .map(|row| build_anomaly_row(row, config))
        .collect::<Vec<_>>();
    score_rows(&mut rows);

    SheetModel {
        sheet_name: "成本分析工单维度".to_string(),
        rows: rows
            .iter()
            .map(|row| {
                columns
                    .iter()
                    .map(|column| map_work_order_value(row, column, config))
                    .collect()
            })
            .collect(),
        column_types: build_column_types(&columns),
        number_formats: build_number_formats(&columns),
        columns,
        freeze_panes: Some("A2".to_string()),
        auto_filter: true,
        fixed_width: Some(15.0),
    }
}

fn analysis_work_order_rows<'a>(
    bundle: &'a FactBundle,
    config: &PipelineConfig,
) -> Vec<&'a TableRow> {
    if config.product_order.is_empty() {
        return bundle.work_order_fact.iter().collect();
    }

    let mut rows = bundle
        .work_order_fact
        .iter()
        .filter_map(|row| {
            let product_code = text_any(row, &["product_code", "产品编码"]);
            let product_name = text_any(row, &["product_name", "产品名称"]);
            // 产品编码可能复用，必须按编码和名称精确匹配，避免错误产品进入异常池。
            config
                .product_order
                .iter()
                .position(|(code, name)| *code == product_code && *name == product_name)
                .map(|order_index| (order_index, row))
        })
        .collect::<Vec<_>>();
    // Python 展示契约在白名单顺序内继续按月份、工单号和数值工单行排序。
    rows.sort_by(|(left_order, left_row), (right_order, right_row)| {
        left_order
            .cmp(right_order)
            .then_with(|| {
                compare_text_field(left_row, right_row, &["period_display", "月份", "period"])
            })
            .then_with(|| compare_text_field(left_row, right_row, &["order_no", "工单编号"]))
            .then_with(|| compare_order_line(left_row, right_row))
    });
    rows.into_iter().map(|(_, row)| row).collect()
}

fn compare_text_field(left: &TableRow, right: &TableRow, keys: &[&str]) -> std::cmp::Ordering {
    text_any(left, keys)
        .trim()
        .cmp(text_any(right, keys).trim())
}

fn compare_order_line(left: &TableRow, right: &TableRow) -> std::cmp::Ordering {
    let left_text = text_any(left, &["order_line", "工单行", "工单行号"]);
    let right_text = text_any(right, &["order_line", "工单行", "工单行号"]);
    match (
        left_text.trim().parse::<Decimal>(),
        right_text.trim().parse::<Decimal>(),
    ) {
        (Ok(left_number), Ok(right_number)) => left_number.cmp(&right_number),
        (Ok(_), Err(_)) => std::cmp::Ordering::Less,
        (Err(_), Ok(_)) => std::cmp::Ordering::Greater,
        (Err(_), Err(_)) => left_text.trim().cmp(right_text.trim()),
    }
}

fn work_order_columns(config: &PipelineConfig) -> Vec<String> {
    let mut columns = BASE_WORK_ORDER_COLUMNS
        .iter()
        .map(|value| (*value).to_string())
        .collect::<Vec<_>>();
    for item in config.standalone_cost_items {
        let meta = standalone_meta(item);
        insert_before(&mut columns, "总单位完工成本", meta.amount_column);
        insert_before(&mut columns, "是否可参与分析", meta.unit_column);
    }
    columns
}

fn insert_before(columns: &mut Vec<String>, marker: &str, value: &str) {
    if columns.iter().any(|column| column == value) {
        return;
    }
    let index = columns
        .iter()
        .position(|column| column == marker)
        .unwrap_or(columns.len());
    columns.insert(index, value.to_string());
}

fn build_anomaly_row<'a>(row: &'a TableRow, config: &PipelineConfig) -> AnomalyRow<'a> {
    let completed_qty = decimal(row, "completed_qty").unwrap_or(ZERO);
    let completed_total = decimal(row, "completed_amount_total").unwrap_or(ZERO);
    let mut numbers = BTreeMap::from([
        ("completed_qty".to_string(), completed_qty),
        ("completed_amount_total".to_string(), completed_total),
        (
            "dm_amount".to_string(),
            decimal(row, "dm_amount").unwrap_or(ZERO),
        ),
        (
            "dl_amount".to_string(),
            decimal(row, "dl_amount").unwrap_or(ZERO),
        ),
        (
            "moh_amount".to_string(),
            decimal(row, "moh_amount").unwrap_or(ZERO),
        ),
        (
            "moh_other_amount".to_string(),
            decimal(row, "moh_other_amount").unwrap_or(ZERO),
        ),
        (
            "moh_labor_amount".to_string(),
            decimal(row, "moh_labor_amount").unwrap_or(ZERO),
        ),
        (
            "moh_consumables_amount".to_string(),
            decimal(row, "moh_consumables_amount").unwrap_or(ZERO),
        ),
        (
            "moh_depreciation_amount".to_string(),
            decimal(row, "moh_depreciation_amount").unwrap_or(ZERO),
        ),
        (
            "moh_utilities_amount".to_string(),
            decimal(row, "moh_utilities_amount").unwrap_or(ZERO),
        ),
    ]);

    insert_unit_costs(&mut numbers, completed_qty);
    for item in config.standalone_cost_items {
        let meta = standalone_meta(item);
        let amount = decimal(row, meta.amount_key).unwrap_or(ZERO);
        numbers.insert(meta.amount_key.to_string(), amount);
        if let Some(unit_cost) = safe_divide(amount, completed_qty) {
            numbers.insert(meta.unit_key.to_string(), unit_cost);
        }
    }

    let production_scope = map_doc_type_to_scope(&text_any(row, &["doc_type", "单据类型"]));
    let mut reasons = Vec::new();
    if production_scope == UNKNOWN_SCOPE {
        reasons.push("单据类型未归类，不参与正常生产/返工生产异常池".to_string());
    }
    let total_unit_cost = numbers.get("total_unit_cost").copied().unwrap_or(ZERO);
    let can_analyze = completed_qty > ZERO
        && total_unit_cost > ZERO
        && matches!(production_scope.as_str(), NORMAL_SCOPE | REWORK_SCOPE);

    AnomalyRow {
        source: row,
        numbers,
        production_scope,
        can_analyze,
        reasons,
        audits: BTreeMap::new(),
        anomaly_level: "正常".to_string(),
        anomaly_source: String::new(),
        detail_explanation: String::new(),
    }
}

fn insert_unit_costs(numbers: &mut BTreeMap<String, Decimal>, completed_qty: Decimal) {
    let amount_to_unit = [
        ("completed_amount_total", "total_unit_cost"),
        ("dm_amount", "dm_unit_cost"),
        ("dl_amount", "dl_unit_cost"),
        ("moh_amount", "moh_unit_cost"),
        ("moh_other_amount", "moh_other_unit_cost"),
        ("moh_labor_amount", "moh_labor_unit_cost"),
        ("moh_consumables_amount", "moh_consumables_unit_cost"),
        ("moh_depreciation_amount", "moh_depreciation_unit_cost"),
        ("moh_utilities_amount", "moh_utilities_unit_cost"),
    ];
    for (amount_key, unit_key) in amount_to_unit {
        if let Some(unit_cost) = safe_divide(
            numbers.get(amount_key).copied().unwrap_or(ZERO),
            completed_qty,
        ) {
            numbers.insert(unit_key.to_string(), unit_cost);
        }
    }
}

fn score_rows(rows: &mut [AnomalyRow<'_>]) {
    for metric in ANOMALY_METRICS {
        append_non_positive_reasons(rows, *metric);
        let mut groups: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for (index, row) in rows.iter().enumerate() {
            if !row.can_analyze || !positive_number(row, metric.key) {
                continue;
            }
            groups.entry(group_key(row)).or_default().push(index);
        }

        for indexes in groups.values() {
            let mut valid = Vec::new();
            for index in indexes {
                let Some(value) = rows[*index].numbers.get(metric.key).copied() else {
                    continue;
                };
                let Some(weight) = rows[*index].numbers.get("completed_qty").copied() else {
                    continue;
                };
                let Some(log_value) = decimal_ln(value) else {
                    push_score_reason(
                        &mut rows[*index],
                        *metric,
                        "无法计算log，不参与 Modified Z-score",
                    );
                    continue;
                };
                let Some(log_decimal) = decimal_from_f64(log_value) else {
                    push_score_reason(
                        &mut rows[*index],
                        *metric,
                        "log值无法转换为Decimal，不参与 Modified Z-score",
                    );
                    continue;
                };
                valid.push((*index, log_decimal, weight));
            }
            if valid.len() < 3 {
                continue;
            }

            let weighted_values = valid
                .iter()
                .map(|(_, log_decimal, weight)| (*log_decimal, *weight))
                .collect::<Vec<_>>();
            let Some(center_decimal) = weighted_median(&weighted_values) else {
                for (index, _, _) in &valid {
                    push_score_reason(
                        &mut rows[*index],
                        *metric,
                        "异常池中心值缺失，不参与 Modified Z-score",
                    );
                }
                continue;
            };
            let Some(raw_mad_decimal) = weighted_mad(&weighted_values, center_decimal) else {
                for (index, _, _) in &valid {
                    push_score_reason(
                        &mut rows[*index],
                        *metric,
                        "异常池MAD缺失，不参与 Modified Z-score",
                    );
                }
                continue;
            };
            let Some(effective_mad) = resolve_effective_log_mad(Some(raw_mad_decimal)) else {
                for (index, _, _) in &valid {
                    push_score_reason(
                        &mut rows[*index],
                        *metric,
                        "有效MAD缺失，不参与 Modified Z-score",
                    );
                }
                continue;
            };

            for (index, current_log, _) in valid {
                let Some(score) = modified_z_score(current_log, center_decimal, effective_mad)
                else {
                    push_score_reason(
                        &mut rows[index],
                        *metric,
                        "有效MAD小于等于0，不参与 Modified Z-score",
                    );
                    continue;
                };
                let flag = grade_score(Some(score)).to_string();
                rows[index].audits.insert(
                    metric.key,
                    MetricAudit {
                        flag,
                        score: Some(score),
                        current_log: Some(current_log),
                        center_log: Some(center_decimal),
                        raw_mad: Some(raw_mad_decimal),
                        effective_mad: Some(effective_mad),
                        sample_size: indexes.len(),
                    },
                );
            }
        }
    }

    for row in rows {
        finalize_row_anomaly(row);
    }
}

fn push_score_reason(row: &mut AnomalyRow<'_>, metric: Metric, reason: &str) {
    row.reasons
        .push(format!("{}{}", metric.display_name, reason));
}

fn append_non_positive_reasons(rows: &mut [AnomalyRow<'_>], metric: Metric) {
    for row in rows {
        if !positive_number(row, metric.key) {
            row.reasons
                .push(format!("{}小于等于0或为空", metric.display_name));
        }
    }
}

fn finalize_row_anomaly(row: &mut AnomalyRow<'_>) {
    let mut severity_rank = 0;
    let mut highest_score: Option<Decimal> = None;
    let mut highest_source = String::new();
    let mut overall_level = "正常".to_string();

    for metric in ANOMALY_METRICS {
        let audit = row.audits.entry(metric.key).or_default();
        let rank = severity_rank_for(&audit.flag);
        let Some(score_abs) = audit.score.map(decimal_abs) else {
            continue;
        };
        if rank > severity_rank
            || (rank == severity_rank
                && highest_score
                    .map(|current| score_abs > current)
                    .unwrap_or(true))
        {
            severity_rank = rank;
            highest_score = Some(score_abs);
            overall_level = audit.flag.clone();
            highest_source = metric.source_label.to_string();
        } else if rank == severity_rank
            && rank > 0
            && highest_score == Some(score_abs)
            && !highest_source.is_empty()
            && highest_source != metric.source_label
            && highest_source != "总成本异常"
            && metric.source_label != "总成本异常"
        {
            highest_source = "多项同时异常".to_string();
        }
    }

    if severity_rank <= 0 {
        highest_source.clear();
    }
    if row.production_scope == UNKNOWN_SCOPE {
        overall_level.clear();
        highest_source.clear();
    }

    row.anomaly_level = overall_level;
    row.anomaly_source = highest_source;
    row.detail_explanation = build_detail_explanation(row);
}

fn build_detail_explanation(row: &AnomalyRow<'_>) -> String {
    let mut parts = Vec::new();
    for metric in ANOMALY_METRICS {
        let Some(audit) = row.audits.get(metric.key) else {
            continue;
        };
        if !matches!(audit.flag.as_str(), "关注" | "高度可疑") {
            continue;
        }
        let current_value = row.numbers.get(metric.key).copied().unwrap_or(ZERO);
        let score = audit.score.unwrap_or(ZERO);
        let current_log = audit.current_log.unwrap_or(ZERO);
        let center_log = audit.center_log.unwrap_or(ZERO);
        let log_delta = current_log - center_log;
        let baseline_value = decimal_to_f64(center_log).map(f64::exp).unwrap_or(0.0);
        let relative_delta = decimal_to_f64(log_delta).map(f64::exp_m1).unwrap_or(0.0);
        let raw_mad = audit.raw_mad.unwrap_or(ZERO);
        let effective_mad = audit.effective_mad.unwrap_or(ZERO);
        parts.push(format!(
            "{}: {}, 当前值={}, 当前log={}, 基准值={:.2}, 基准log={}, log偏离={}, 相对偏离={}, score={}, 有效工单数={}, 原始MAD={}, 有效MAD={}",
            metric.explanation_label,
            audit.flag,
            format_decimal_fixed(current_value, 2),
            format_decimal_fixed(current_log, 4),
            baseline_value,
            format_decimal_fixed(center_log, 4),
            format_decimal_fixed(log_delta, 4),
            format_percent(relative_delta),
            format_decimal_fixed(score, 2),
            audit.sample_size,
            format_decimal_fixed(raw_mad, 4),
            format_decimal_fixed(effective_mad, 4)
        ));
    }
    parts.join("; ")
}

fn format_decimal_fixed(value: Decimal, digits: usize) -> String {
    // 展示层沿用 Python f-string 的舍入口径；评分与阈值判断仍全部使用 Decimal。
    let number = decimal_to_f64(value).unwrap_or(0.0);
    format!("{number:.digits$}")
}

fn format_percent(value: f64) -> String {
    format!("{:.2}%", value * 100.0)
}

fn map_work_order_value(row: &AnomalyRow<'_>, column: &str, config: &PipelineConfig) -> CellValue {
    match column {
        "月份" => value_any(&row.source, &["period_display", "月份", "年期"]),
        "成本中心" => value_any(&row.source, &["cost_center", "成本中心名称"]),
        "产品编码" => value_any(&row.source, &["product_code", "产品编码"]),
        "产品名称" => value_any(&row.source, &["product_name", "产品名称"]),
        "规格型号" => value_any(&row.source, &["spec", "规格型号"]),
        "工单编号" => value_any(&row.source, &["order_no", "工单编号"]),
        "工单行" => value_any(&row.source, &["order_line", "工单行号"]),
        "生产类型" => CellValue::Text(row.production_scope.clone()),
        "基本单位" => value_any(&row.source, &["unit", "基本单位"]),
        "本期完工数量" => decimal_value(row, "completed_qty"),
        "总完工成本" => decimal_value(row, "completed_amount_total"),
        "直接材料合计完工金额" => decimal_value(row, "dm_amount"),
        "直接人工合计完工金额" => decimal_value(row, "dl_amount"),
        "制造费用合计完工金额" => decimal_value(row, "moh_amount"),
        "制造费用_其他合计完工金额" => decimal_value(row, "moh_other_amount"),
        "制造费用_人工合计完工金额" => decimal_value(row, "moh_labor_amount"),
        "制造费用_机物料及低耗合计完工金额" => {
            decimal_value(row, "moh_consumables_amount")
        }
        "制造费用_折旧合计完工金额" => decimal_value(row, "moh_depreciation_amount"),
        "制造费用_水电费合计完工金额" => decimal_value(row, "moh_utilities_amount"),
        "总单位完工成本" => decimal_value(row, "total_unit_cost"),
        "直接材料单位完工成本" => decimal_value(row, "dm_unit_cost"),
        "直接人工单位完工成本" => decimal_value(row, "dl_unit_cost"),
        "制造费用单位完工成本" => decimal_value(row, "moh_unit_cost"),
        "制造费用_其他单位完工成本" => decimal_value(row, "moh_other_unit_cost"),
        "制造费用_人工单位完工成本" => decimal_value(row, "moh_labor_unit_cost"),
        "制造费用_机物料及低耗单位完工成本" => {
            decimal_value(row, "moh_consumables_unit_cost")
        }
        "制造费用_折旧单位完工成本" => decimal_value(row, "moh_depreciation_unit_cost"),
        "制造费用_水电费单位完工成本" => decimal_value(row, "moh_utilities_unit_cost"),
        "是否可参与分析" => {
            CellValue::Text(if row.can_analyze { "是" } else { "否" }.to_string())
        }
        "异常等级" => CellValue::Text(row.anomaly_level.clone()),
        "异常主要来源" => CellValue::Text(row.anomaly_source.clone()),
        "异常明细解释" => CellValue::Text(row.detail_explanation.clone()),
        "复核原因" => CellValue::Text(row.reasons.join(";")),
        other => standalone_display_value(row, other, config),
    }
}

fn standalone_display_value(
    row: &AnomalyRow<'_>,
    column: &str,
    config: &PipelineConfig,
) -> CellValue {
    for item in config.standalone_cost_items {
        let meta = standalone_meta(item);
        if column == meta.amount_column {
            return decimal_value(row, meta.amount_key);
        }
        if column == meta.unit_column {
            return decimal_value(row, meta.unit_key);
        }
    }
    CellValue::Blank
}

fn build_column_types(columns: &[String]) -> BTreeMap<String, String> {
    columns
        .iter()
        .map(|column| {
            let metric_type = if column == "成本中心" {
                "text"
            } else if column == "本期完工数量" {
                "qty"
            } else if column.contains("单位完工成本") {
                "price"
            } else if column.contains("金额") || column.contains("成本") {
                "amount"
            } else {
                "text"
            };
            (column.clone(), metric_type.to_string())
        })
        .collect()
}

fn build_number_formats(columns: &[String]) -> BTreeMap<String, String> {
    build_column_types(columns)
        .into_iter()
        .filter_map(|(column, metric_type)| {
            if matches!(metric_type.as_str(), "amount" | "price" | "qty") {
                Some((column, "#,##0.00".to_string()))
            } else {
                None
            }
        })
        .collect()
}

fn group_key(row: &AnomalyRow<'_>) -> String {
    format!(
        "{}|{}|{}",
        text_any(&row.source, &["product_code", "产品编码"]),
        text_any(&row.source, &["product_name", "产品名称"]),
        row.production_scope
    )
}

fn positive_number(row: &AnomalyRow<'_>, key: &str) -> bool {
    row.numbers
        .get(key)
        .map(|value| *value > ZERO)
        .unwrap_or(false)
}

fn decimal_value(row: &AnomalyRow<'_>, key: &str) -> CellValue {
    row.numbers
        .get(key)
        .copied()
        .map(CellValue::Decimal)
        .unwrap_or(CellValue::Blank)
}

fn value_any(row: &TableRow, keys: &[&str]) -> CellValue {
    keys.iter()
        .find_map(|key| row.values.get(*key).cloned())
        .unwrap_or(CellValue::Blank)
}

fn text_any(row: &TableRow, keys: &[&str]) -> String {
    keys.iter()
        .find_map(|key| row.values.get(*key).map(cell_to_text))
        .unwrap_or_default()
}

fn decimal(row: &TableRow, key: &str) -> Option<Decimal> {
    row.values.get(key).and_then(cell_to_decimal)
}

fn cell_to_decimal(value: &CellValue) -> Option<Decimal> {
    match value {
        CellValue::Decimal(value) => Some(*value),
        CellValue::Text(value) => value.trim().parse().ok(),
        CellValue::Blank | CellValue::DateLike(_) => None,
    }
}

fn cell_to_text(value: &CellValue) -> String {
    match value {
        CellValue::Blank => String::new(),
        CellValue::Text(value) | CellValue::DateLike(value) => value.clone(),
        CellValue::Decimal(value) => value.normalize().to_string(),
    }
}

fn safe_divide(numerator: Decimal, denominator: Decimal) -> Option<Decimal> {
    if denominator == ZERO {
        None
    } else {
        numerator.checked_div(denominator)
    }
}

fn decimal_from_f64(value: f64) -> Option<Decimal> {
    if value.is_finite() {
        format!("{value:.17}").parse().ok()
    } else {
        None
    }
}

fn decimal_to_f64(value: Decimal) -> Option<f64> {
    value.to_string().parse().ok()
}

fn decimal_abs(value: Decimal) -> Decimal {
    if value < ZERO {
        -value
    } else {
        value
    }
}

fn severity_rank_for(value: &str) -> i32 {
    match value {
        "高度可疑" => 2,
        "关注" => 1,
        "正常" => 0,
        _ => -1,
    }
}

fn map_doc_type_to_scope(value: &str) -> String {
    match value.trim() {
        "汇报入库-普通生产" | "直接入库-普通生产" => NORMAL_SCOPE.to_string(),
        "汇报入库-返工生产" => REWORK_SCOPE.to_string(),
        _ => UNKNOWN_SCOPE.to_string(),
    }
}

struct StandaloneMeta {
    amount_key: &'static str,
    unit_key: &'static str,
    amount_column: &'static str,
    unit_column: &'static str,
}

fn standalone_meta(item: &str) -> StandaloneMeta {
    match item.trim() {
        "软件费用" => StandaloneMeta {
            amount_key: "software_amount",
            unit_key: "software_unit_cost",
            amount_column: "软件费用合计完工金额",
            unit_column: "软件费用单位完工成本",
        },
        _ => StandaloneMeta {
            amount_key: "outsource_amount",
            unit_key: "outsource_unit_cost",
            amount_column: "委外加工费合计完工金额",
            unit_column: "委外加工费单位完工成本",
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::str::FromStr;

    use rust_decimal::Decimal;

    use crate::model::{CellValue, ErrorIssue, FactBundle, TableRow};
    use crate::pipeline::{PipelineConfig, PipelineName};

    use super::*;

    const TEST_PRODUCT_ORDER: &[(&str, &str)] = &[("P1", "产品"), ("P-NEAR-MAD", "近零MAD产品")];

    fn test_config(name: PipelineName) -> PipelineConfig {
        PipelineConfig {
            product_order: TEST_PRODUCT_ORDER,
            ..PipelineConfig::for_name(name)
        }
    }

    fn row(
        order_no: &str,
        unit_cost: i64,
        doc_type: &str,
        extra: &[(&str, CellValue)],
    ) -> TableRow {
        let mut values = BTreeMap::from([
            (
                "月份".to_string(),
                CellValue::Text("2025年01期".to_string()),
            ),
            ("产品编码".to_string(), CellValue::Text("P1".to_string())),
            ("产品名称".to_string(), CellValue::Text("产品".to_string())),
            (
                "工单编号".to_string(),
                CellValue::Text(order_no.to_string()),
            ),
            ("工单行号".to_string(), CellValue::Text("1".to_string())),
            (
                "单据类型".to_string(),
                CellValue::Text(doc_type.to_string()),
            ),
            (
                "completed_qty".to_string(),
                CellValue::Decimal(Decimal::new(1, 0)),
            ),
            (
                "completed_amount_total".to_string(),
                CellValue::Decimal(Decimal::new(unit_cost, 0)),
            ),
            (
                "dm_amount".to_string(),
                CellValue::Decimal(Decimal::new(unit_cost, 0)),
            ),
            ("dl_amount".to_string(), CellValue::Decimal(Decimal::ZERO)),
            ("moh_amount".to_string(), CellValue::Decimal(Decimal::ZERO)),
        ]);
        for (key, value) in extra {
            values.insert((*key).to_string(), value.clone());
        }
        TableRow { values }
    }

    fn bundle(rows: Vec<TableRow>) -> FactBundle {
        FactBundle {
            detail_columns: Vec::new(),
            detail_fact: vec![],
            qty_columns: Vec::new(),
            qty_input_row_count: 0,
            filtered_invalid_qty_count: 0,
            filtered_missing_total_amount_count: 0,
            qty_fact: vec![],
            work_order_fact: rows,
            error_issues: Vec::<ErrorIssue>::new(),
        }
    }

    fn decimal_row(
        order_no: &str,
        product_code: &str,
        product_name: &str,
        qty: &str,
        unit_cost: &str,
        doc_type: &str,
    ) -> TableRow {
        let qty = Decimal::from_str(qty).unwrap();
        let unit_cost = Decimal::from_str(unit_cost).unwrap();
        let total_amount = qty * unit_cost;
        let mut values = row(order_no, 1, doc_type, &[]).values;
        values.insert(
            "产品编码".to_string(),
            CellValue::Text(product_code.to_string()),
        );
        values.insert(
            "产品名称".to_string(),
            CellValue::Text(product_name.to_string()),
        );
        values.insert("completed_qty".to_string(), CellValue::Decimal(qty));
        values.insert(
            "completed_amount_total".to_string(),
            CellValue::Decimal(total_amount),
        );
        values.insert("dm_amount".to_string(), CellValue::Decimal(total_amount));
        TableRow { values }
    }

    fn column_index(sheet: &SheetModel, column: &str) -> usize {
        sheet
            .columns
            .iter()
            .position(|value| value == column)
            .unwrap()
    }

    #[test]
    fn work_order_sheet_contains_required_audit_columns() {
        let sheet = build_work_order_anomaly_sheet(
            &bundle(vec![row("WO1", 100, "汇报入库-普通生产", &[])]),
            &test_config(PipelineName::Gb),
        );

        assert_eq!(sheet.sheet_name, "成本分析工单维度");
        assert!(sheet.columns.contains(&"异常等级".to_string()));
        assert!(sheet.columns.contains(&"异常主要来源".to_string()));
        assert!(sheet.columns.contains(&"异常明细解释".to_string()));
        assert!(sheet.columns.contains(&"复核原因".to_string()));
        assert_eq!(sheet.freeze_panes, Some("A2".to_string()));
        assert!(sheet.auto_filter);
        assert_eq!(sheet.fixed_width, Some(15.0));
        assert_eq!(sheet.column_types["成本中心"], "text");
        assert!(!sheet.number_formats.contains_key("成本中心"));
    }

    #[test]
    fn analysis_sheet_filters_exact_product_pairs_and_keeps_whitelist_order() {
        const PRODUCT_ORDER: &[(&str, &str)] = &[("P2", "产品二"), ("P1", "产品一")];
        let config = PipelineConfig {
            product_order: PRODUCT_ORDER,
            ..PipelineConfig::for_name(PipelineName::Gb)
        };
        let rows = vec![
            row(
                "WO-P1",
                100,
                "汇报入库-普通生产",
                &[
                    ("产品编码", CellValue::Text("P1".to_string())),
                    ("产品名称", CellValue::Text("产品一".to_string())),
                ],
            ),
            row(
                "WO-WRONG-NAME",
                100,
                "汇报入库-普通生产",
                &[
                    ("产品编码", CellValue::Text("P1".to_string())),
                    ("产品名称", CellValue::Text("名称不匹配".to_string())),
                ],
            ),
            row(
                "WO-P2",
                100,
                "汇报入库-普通生产",
                &[
                    ("产品编码", CellValue::Text("P2".to_string())),
                    ("产品名称", CellValue::Text("产品二".to_string())),
                ],
            ),
            row(
                "WO-NOT-LISTED",
                100,
                "汇报入库-普通生产",
                &[
                    ("产品编码", CellValue::Text("P3".to_string())),
                    ("产品名称", CellValue::Text("产品三".to_string())),
                ],
            ),
        ];

        let sheet = build_work_order_anomaly_sheet(&bundle(rows), &config);
        let product_code_idx = column_index(&sheet, "产品编码");

        assert_eq!(sheet.rows.len(), 2);
        assert_eq!(
            sheet.rows[0][product_code_idx],
            CellValue::Text("P2".to_string())
        );
        assert_eq!(
            sheet.rows[1][product_code_idx],
            CellValue::Text("P1".to_string())
        );
    }

    #[test]
    fn analysis_sheet_sorts_each_product_by_month_order_and_numeric_order_line() {
        let rows = vec![
            row(
                "WO-B",
                100,
                "汇报入库-普通生产",
                &[
                    ("月份", CellValue::Text("2025年02期".to_string())),
                    ("工单行号", CellValue::Text("1".to_string())),
                ],
            ),
            row(
                "WO-A",
                100,
                "汇报入库-普通生产",
                &[
                    ("月份", CellValue::Text("2025年01期".to_string())),
                    ("工单行号", CellValue::Text("10".to_string())),
                ],
            ),
            row(
                "WO-A",
                100,
                "汇报入库-普通生产",
                &[
                    ("月份", CellValue::Text("2025年01期".to_string())),
                    ("工单行号", CellValue::Text("2".to_string())),
                ],
            ),
        ];

        let sheet = build_work_order_anomaly_sheet(&bundle(rows), &test_config(PipelineName::Gb));
        let month_index = column_index(&sheet, "月份");
        let order_index = column_index(&sheet, "工单编号");
        let order_line_index = column_index(&sheet, "工单行");

        assert_eq!(
            sheet
                .rows
                .iter()
                .map(|row| (
                    row[month_index].clone(),
                    row[order_index].clone(),
                    row[order_line_index].clone(),
                ))
                .collect::<Vec<_>>(),
            vec![
                (
                    CellValue::Text("2025年01期".to_string()),
                    CellValue::Text("WO-A".to_string()),
                    CellValue::Text("2".to_string()),
                ),
                (
                    CellValue::Text("2025年01期".to_string()),
                    CellValue::Text("WO-A".to_string()),
                    CellValue::Text("10".to_string()),
                ),
                (
                    CellValue::Text("2025年02期".to_string()),
                    CellValue::Text("WO-B".to_string()),
                    CellValue::Text("1".to_string()),
                ),
            ]
        );
    }

    #[test]
    fn grades_attention_and_suspicious_by_product_scope() {
        let sheet = build_work_order_anomaly_sheet(
            &bundle(vec![
                row("WO100", 100, "汇报入库-普通生产", &[]),
                row("WO101", 101, "汇报入库-普通生产", &[]),
                row("WO102", 102, "汇报入库-普通生产", &[]),
                row("WO103", 103, "汇报入库-普通生产", &[]),
                row("WO106", 106, "汇报入库-普通生产", &[]),
                row("WO115", 115, "汇报入库-普通生产", &[]),
                row("WO130", 130, "汇报入库-普通生产", &[]),
            ]),
            &test_config(PipelineName::Gb),
        );
        let level_idx = column_index(&sheet, "异常等级");
        let source_idx = column_index(&sheet, "异常主要来源");
        let detail_idx = column_index(&sheet, "异常明细解释");

        assert_eq!(
            sheet.rows[5][level_idx],
            CellValue::Text("关注".to_string())
        );
        assert_eq!(
            sheet.rows[6][level_idx],
            CellValue::Text("高度可疑".to_string())
        );
        assert_eq!(
            sheet.rows[6][source_idx],
            CellValue::Text("总成本异常".to_string())
        );
        let CellValue::Text(detail) = &sheet.rows[6][detail_idx] else {
            panic!("detail explanation should be text");
        };
        assert!(detail.contains("总成本:"));
        assert!(detail.contains("当前值=130.00"));
        assert!(detail.contains("基准值=103.00"));
        assert!(detail.contains("log偏离="));
        assert!(detail.contains("相对偏离="));
        assert!(detail.contains("score="));
    }

    #[test]
    fn unknown_doc_type_is_not_analyzable() {
        let sheet = build_work_order_anomaly_sheet(
            &bundle(vec![row("WO1", 100, "其他入库", &[])]),
            &test_config(PipelineName::Gb),
        );
        let can_analyze_idx = column_index(&sheet, "是否可参与分析");
        let level_idx = column_index(&sheet, "异常等级");
        let reason_idx = column_index(&sheet, "复核原因");

        assert_eq!(
            sheet.rows[0][can_analyze_idx],
            CellValue::Text("否".to_string())
        );
        assert_eq!(sheet.rows[0][level_idx], CellValue::Text(String::new()));
        let CellValue::Text(reason) = &sheet.rows[0][reason_idx] else {
            panic!("reason should be text");
        };
        assert_eq!(
            reason,
            "单据类型未归类，不参与正常生产/返工生产异常池;直接人工单位完工成本小于等于0或为空;制造费用单位完工成本小于等于0或为空;制造费用_其他单位完工成本小于等于0或为空;制造费用_人工单位完工成本小于等于0或为空;制造费用_机物料及低耗单位完工成本小于等于0或为空;制造费用_折旧单位完工成本小于等于0或为空;制造费用_水电费单位完工成本小于等于0或为空"
        );
    }

    #[test]
    fn sk_standalone_software_columns_are_visible_without_anomaly_flags() {
        let sheet = build_work_order_anomaly_sheet(
            &bundle(vec![row(
                "WO1",
                100,
                "汇报入库-普通生产",
                &[("software_amount", CellValue::Decimal(Decimal::new(5, 0)))],
            )]),
            &test_config(PipelineName::Sk),
        );

        assert!(sheet.columns.contains(&"软件费用合计完工金额".to_string()));
        assert!(sheet.columns.contains(&"软件费用单位完工成本".to_string()));
        assert!(!sheet.columns.contains(&"软件费用异常标记".to_string()));
        assert!(!sheet
            .columns
            .contains(&"Modified Z-score_软件费用".to_string()));
    }

    #[test]
    fn scores_normal_and_rework_in_separate_pools() {
        let sheet = build_work_order_anomaly_sheet(
            &bundle(vec![
                row("WO-N1", 100, "汇报入库-普通生产", &[]),
                row("WO-N2", 105, "汇报入库-普通生产", &[]),
                row("WO-N3", 500, "汇报入库-普通生产", &[]),
                row("WO-R1", 200, "汇报入库-返工生产", &[]),
                row("WO-R2", 210, "汇报入库-返工生产", &[]),
                row("WO-R3", 500, "汇报入库-返工生产", &[]),
            ]),
            &test_config(PipelineName::Gb),
        );
        let level_idx = column_index(&sheet, "异常等级");
        let scope_idx = column_index(&sheet, "生产类型");

        assert_eq!(
            sheet.rows[2][level_idx],
            CellValue::Text("高度可疑".to_string())
        );
        assert_eq!(
            sheet.rows[5][level_idx],
            CellValue::Text("高度可疑".to_string())
        );
        assert_eq!(
            sheet.rows[5][scope_idx],
            CellValue::Text("返工生产".to_string())
        );
    }

    #[test]
    fn equal_decimal_scores_mark_multiple_non_total_sources() {
        let config = test_config(PipelineName::Gb);
        let source = row("WO-TIE", 100, "汇报入库-普通生产", &[]);
        let mut anomaly_row = build_anomaly_row(&source, &config);
        for metric_key in ["dm_unit_cost", "dl_unit_cost"] {
            anomaly_row.audits.insert(
                metric_key,
                MetricAudit {
                    flag: "关注".to_string(),
                    score: Some(Decimal::new(30, 1)),
                    ..MetricAudit::default()
                },
            );
        }

        finalize_row_anomaly(&mut anomaly_row);

        assert_eq!(anomaly_row.anomaly_level, "关注");
        assert_eq!(anomaly_row.anomaly_source, "多项同时异常");
    }

    #[test]
    fn near_zero_mad_uses_minimum_dispersion() {
        let sheet = build_work_order_anomaly_sheet(
            &bundle(vec![
                decimal_row(
                    "WO-R-CENTER-1",
                    "P-NEAR-MAD",
                    "近零MAD产品",
                    "100",
                    "100.0000000",
                    "汇报入库-返工生产",
                ),
                decimal_row(
                    "WO-R-CENTER-2",
                    "P-NEAR-MAD",
                    "近零MAD产品",
                    "710",
                    "100.0000008",
                    "汇报入库-返工生产",
                ),
                decimal_row(
                    "WO-R-CLOSE",
                    "P-NEAR-MAD",
                    "近零MAD产品",
                    "100",
                    "100.01",
                    "汇报入库-返工生产",
                ),
                decimal_row(
                    "WO-R-FAR",
                    "P-NEAR-MAD",
                    "近零MAD产品",
                    "100",
                    "120",
                    "汇报入库-返工生产",
                ),
                decimal_row(
                    "WO-R-EXTREME",
                    "P-NEAR-MAD",
                    "近零MAD产品",
                    "100",
                    "180",
                    "汇报入库-返工生产",
                ),
            ]),
            &test_config(PipelineName::Gb),
        );
        let level_idx = column_index(&sheet, "异常等级");
        let detail_idx = column_index(&sheet, "异常明细解释");

        assert_eq!(
            sheet.rows[2][level_idx],
            CellValue::Text("正常".to_string())
        );
        assert_eq!(sheet.rows[2][detail_idx], CellValue::Text(String::new()));
        assert_eq!(
            sheet.rows[3][level_idx],
            CellValue::Text("高度可疑".to_string())
        );
        assert_eq!(
            sheet.rows[4][level_idx],
            CellValue::Text("高度可疑".to_string())
        );
        let CellValue::Text(detail) = &sheet.rows[3][detail_idx] else {
            panic!("detail explanation should be text");
        };
        assert!(detail.contains("score="));
    }
}

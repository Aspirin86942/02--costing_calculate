use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;

use crate::error::CostingError;
use crate::model::{CellValue, ErrorIssue, FactBundle, IndexedFactRow, SplitResult};
use crate::pipeline::PipelineConfig;
use crate::sheet_contract::qty_sheet_base_columns;
use crate::table::{ColumnId, ColumnSchema, IndexedRow};

const ZERO: Decimal = Decimal::ZERO;
const DM_AMOUNT_KEY: &str = "dm_amount";
const DL_AMOUNT_KEY: &str = "dl_amount";
const MOH_AMOUNT_KEY: &str = "moh_amount";
const MOH_OTHER_AMOUNT_KEY: &str = "moh_other_amount";
const MOH_LABOR_AMOUNT_KEY: &str = "moh_labor_amount";
const MOH_CONSUMABLES_AMOUNT_KEY: &str = "moh_consumables_amount";
const MOH_DEPRECIATION_AMOUNT_KEY: &str = "moh_depreciation_amount";
const MOH_UTILITIES_AMOUNT_KEY: &str = "moh_utilities_amount";
const COMPLETED_QTY_KEY: &str = "completed_qty";
const COMPLETED_TOTAL_KEY: &str = "completed_amount_total";
const QTY_DM_AMOUNT: &str = "本期完工直接材料合计完工金额";
const QTY_DL_AMOUNT: &str = "本期完工直接人工合计完工金额";
const QTY_MOH_AMOUNT: &str = "本期完工制造费用合计完工金额";
const QTY_MOH_OTHER_AMOUNT: &str = "本期完工制造费用_其他合计完工金额";
const QTY_MOH_LABOR_AMOUNT: &str = "本期完工制造费用_人工合计完工金额";
const QTY_MOH_CONSUMABLES_AMOUNT: &str = "本期完工制造费用_机物料及低耗合计完工金额";
const QTY_MOH_DEPRECIATION_AMOUNT: &str = "本期完工制造费用_折旧合计完工金额";
const QTY_MOH_UTILITIES_AMOUNT: &str = "本期完工制造费用_水电费合计完工金额";
const QTY_DM_UNIT_COST: &str = "直接材料单位完工金额";
const QTY_DL_UNIT_COST: &str = "直接人工单位完工金额";
const QTY_MOH_UNIT_COST: &str = "制造费用单位完工金额";
const QTY_MOH_OTHER_UNIT_COST: &str = "制造费用_其他单位完工成本";
const QTY_MOH_LABOR_UNIT_COST: &str = "制造费用_人工单位完工成本";
const QTY_MOH_CONSUMABLES_UNIT_COST: &str = "制造费用_机物料及低耗单位完工成本";
const QTY_MOH_DEPRECIATION_UNIT_COST: &str = "制造费用_折旧单位完工成本";
const QTY_MOH_UTILITIES_UNIT_COST: &str = "制造费用_水电费单位完工成本";
const QTY_OUTSOURCE_UNIT_COST: &str = "委外加工费单位完工成本";
const QTY_SOFTWARE_UNIT_COST: &str = "软件费用单位完工成本";
const QTY_MOH_MATCH: &str = "制造费用明细项合计是否等于制造费用合计";
const QTY_CHECK_STATUS: &str = "数据校验状态";
const QTY_CHECK_REASON: &str = "异常原因说明";
const NON_POSITIVE_UNIT_COST_METRICS: &[(&str, &str)] = &[
    (COMPLETED_TOTAL_KEY, "总单位完工成本"),
    (DM_AMOUNT_KEY, "直接材料单位完工成本"),
    (DL_AMOUNT_KEY, "直接人工单位完工成本"),
    (MOH_AMOUNT_KEY, "制造费用单位完工成本"),
    (MOH_OTHER_AMOUNT_KEY, "制造费用_其他单位完工成本"),
    (MOH_LABOR_AMOUNT_KEY, "制造费用_人工单位完工成本"),
    (
        MOH_CONSUMABLES_AMOUNT_KEY,
        "制造费用_机物料及低耗单位完工成本",
    ),
    (MOH_DEPRECIATION_AMOUNT_KEY, "制造费用_折旧单位完工成本"),
    (MOH_UTILITIES_AMOUNT_KEY, "制造费用_水电费单位完工成本"),
];
const REQUIRED_DETAIL_COLUMNS: &[&str] = &[
    "产品编码",
    "产品名称",
    "工单编号",
    "工单行号",
    "成本项目名称",
    "本期完工金额",
];
const REQUIRED_QTY_COLUMNS: &[&str] = &[
    "产品编码",
    "产品名称",
    "工单编号",
    "工单行号",
    "本期完工数量",
    "本期完工金额",
];

#[derive(Debug, Clone, Copy)]
struct WorkOrderColumns {
    month_or_period: Option<ColumnId>,
    product_code: ColumnId,
    work_order_number: ColumnId,
    work_order_line: ColumnId,
}

impl WorkOrderColumns {
    fn resolve(schema: &ColumnSchema) -> Result<Self, CostingError> {
        Ok(Self {
            month_or_period: schema.optional("月份").or_else(|| schema.optional("年期")),
            product_code: schema.require("产品编码")?,
            work_order_number: schema.require("工单编号")?,
            work_order_line: schema.require("工单行号")?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct DetailFactColumns {
    key: WorkOrderColumns,
    cost_item: ColumnId,
    completed_amount: ColumnId,
}

impl DetailFactColumns {
    fn resolve(schema: &ColumnSchema) -> Result<Self, CostingError> {
        validate_required_columns(schema, REQUIRED_DETAIL_COLUMNS, "成本明细")?;
        Ok(Self {
            key: WorkOrderColumns::resolve(schema)?,
            cost_item: schema.require("成本项目名称")?,
            completed_amount: schema.require("本期完工金额")?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct QtyFactColumns {
    key: WorkOrderColumns,
    completed_qty: ColumnId,
    completed_amount: ColumnId,
}

impl QtyFactColumns {
    fn resolve(schema: &ColumnSchema) -> Result<Self, CostingError> {
        validate_required_columns(schema, REQUIRED_QTY_COLUMNS, "产品数量统计")?;
        Ok(Self {
            key: WorkOrderColumns::resolve(schema)?,
            completed_qty: schema.require("本期完工数量")?,
            completed_amount: schema.require("本期完工金额")?,
        })
    }
}

pub fn build_fact_bundle(
    split: SplitResult,
    config: &PipelineConfig,
) -> Result<FactBundle, CostingError> {
    let (schema, detail_display_columns, detail_rows, qty_display_columns, qty_source_rows) =
        split.into_parts();
    let detail_columns = DetailFactColumns::resolve(&schema)?;
    let qty_columns = QtyFactColumns::resolve(&schema)?;

    let mut amount_by_key: BTreeMap<String, BTreeMap<String, Decimal>> = BTreeMap::new();
    let mut qty_rows_by_key: BTreeMap<String, usize> = BTreeMap::new();
    let mut error_issues = Vec::new();

    for row in &detail_rows {
        let key = work_order_key(row, &detail_columns.key)?;
        let cost_item = cell_to_text(row.get(detail_columns.cost_item)?);
        let amount = cell_to_decimal(row.get(detail_columns.completed_amount)?);
        let buckets = bucket_names(&cost_item, config.standalone_cost_items);
        if buckets.is_empty() {
            if !cost_item.trim().is_empty() {
                error_issues.push(error_issue(
                    key,
                    "UNMAPPED_COST_ITEM",
                    "成本项目名称",
                    cost_item,
                    "成本项目未映射到直接材料/直接人工/制造费用",
                    "该行已从分析数据中排除",
                ));
            }
            continue;
        }

        if amount.is_none() {
            error_issues.push(error_issue(
                key.clone(),
                "MISSING_AMOUNT",
                "本期完工金额",
                cell_to_text(row.get(detail_columns.completed_amount)?),
                "成本明细金额为空，已按 0 参与汇总",
                "金额置为 0 后继续计算",
            ));
        }

        for bucket in buckets {
            *amount_by_key
                .entry(key.clone())
                .or_default()
                .entry(bucket)
                .or_default() += amount.unwrap_or(ZERO);
        }
    }

    let qty_input_row_count = qty_source_rows.len();
    let mut filtered_invalid_qty_count = 0usize;
    let mut filtered_missing_total_amount_count = 0usize;
    let mut valid_qty_rows = Vec::with_capacity(qty_source_rows.len());
    for row in qty_source_rows {
        let completed_qty = cell_to_decimal(row.get(qty_columns.completed_qty)?);
        if !completed_qty.is_some_and(|value| value > ZERO) {
            filtered_invalid_qty_count += 1;
            continue;
        }
        if cell_to_decimal(row.get(qty_columns.completed_amount)?).is_none() {
            filtered_missing_total_amount_count += 1;
            continue;
        }
        valid_qty_rows.push(row);
    }

    for qty_row in &valid_qty_rows {
        let key = work_order_key(qty_row, &qty_columns.key)?;
        *qty_rows_by_key.entry(key).or_default() += 1;
    }

    let mut duplicate_work_order_row_count = 0usize;
    for qty_row in &valid_qty_rows {
        let key = work_order_key(qty_row, &qty_columns.key)?;
        let count = qty_rows_by_key[&key];
        if count > 1 {
            duplicate_work_order_row_count += 1;
            error_issues.push(error_issue(
                key,
                "DUPLICATE_WORK_ORDER_KEY",
                "工单主键",
                count.to_string(),
                "数量页存在重复工单主键",
                "数量页原样保留，异常分析按首条记录去重",
            ));
        }
    }

    let mut qty_rows = Vec::with_capacity(valid_qty_rows.len());
    for qty_row in valid_qty_rows {
        let key = work_order_key(&qty_row, &qty_columns.key)?;
        let amounts = amount_by_key.get(&key).cloned().unwrap_or_default();
        let completed_qty =
            cell_to_decimal(qty_row.get(qty_columns.completed_qty)?).unwrap_or(ZERO);
        let completed_total =
            cell_to_decimal(qty_row.get(qty_columns.completed_amount)?).unwrap_or(ZERO);
        let mut fact_row = IndexedFactRow::new(qty_row);

        for (bucket, amount) in amounts {
            fact_row.insert_derived(bucket, CellValue::Decimal(amount));
        }
        fact_row.insert_derived(
            COMPLETED_QTY_KEY.to_string(),
            CellValue::Decimal(completed_qty),
        );
        fact_row.insert_derived(
            COMPLETED_TOTAL_KEY.to_string(),
            CellValue::Decimal(completed_total),
        );

        // 在 fact 阶段就产出审计问题，避免后续写表阶段再分叉业务口径。
        let moh_sum = moh_component_sum(&fact_row, &schema)?;
        let moh_total = decimal_named(&fact_row, &schema, MOH_AMOUNT_KEY)?;
        if moh_sum != moh_total {
            error_issues.push(error_issue(
                key.clone(),
                "MOH_BREAKDOWN_MISMATCH",
                "制造费用",
                format!("明细合计={};制造费用={}", moh_sum, moh_total),
                "制造费用明细项合计不等于制造费用合计",
                "保留结果并标记需复核",
            ));
        }

        let derived_total = total_amount_from_row(&fact_row, &schema, config)?;
        if derived_total != completed_total {
            error_issues.push(error_issue(
                key,
                "TOTAL_COST_MISMATCH",
                "总完工成本",
                format!("计算值={};数量页={}", derived_total, completed_total),
                &format!(
                    "{}不等于数量页总完工成本",
                    total_expression(config.standalone_cost_items)
                ),
                "保留结果并标记需复核",
            ));
        }

        qty_rows.push(fact_row);
    }

    let mut seen_work_order_keys = BTreeSet::new();
    let mut work_order_rows = Vec::new();
    for row in &qty_rows {
        let key = work_order_key(&row.source, &qty_columns.key)?;
        if seen_work_order_keys.insert(key) {
            work_order_rows.push(row.clone());
        }
    }
    append_non_positive_unit_cost_issues(
        &schema,
        &qty_columns.key,
        &work_order_rows,
        &mut error_issues,
    )?;

    Ok(FactBundle {
        schema,
        detail_display_columns,
        detail_rows,
        qty_display_columns,
        qty_input_row_count,
        filtered_invalid_qty_count,
        filtered_missing_total_amount_count,
        qty_rows,
        work_order_rows,
        duplicate_work_order_row_count,
        error_issues,
    })
}

fn append_non_positive_unit_cost_issues(
    schema: &ColumnSchema,
    key_columns: &WorkOrderColumns,
    work_order_rows: &[IndexedFactRow],
    error_issues: &mut Vec<ErrorIssue>,
) -> Result<(), CostingError> {
    for row in work_order_rows {
        let completed_qty = decimal_named(row, schema, COMPLETED_QTY_KEY)?;
        if completed_qty <= ZERO {
            continue;
        }
        let row_id = work_order_key(&row.source, key_columns)?;
        for (amount_key, field_name) in NON_POSITIVE_UNIT_COST_METRICS {
            let Some(unit_cost) =
                safe_divide(decimal_named(row, schema, amount_key)?, completed_qty)
            else {
                continue;
            };
            if unit_cost <= ZERO {
                error_issues.push(error_issue(
                    row_id.clone(),
                    "NON_POSITIVE_UNIT_COST",
                    field_name,
                    unit_cost.normalize().to_string(),
                    "单位成本小于等于 0，不参与 log 与 Modified Z-score",
                    "保留在异常分析页并标记复核原因",
                ));
            }
        }
    }
    Ok(())
}

fn error_issue(
    row_id: String,
    issue_type: &str,
    field_name: &str,
    original_value: impl Into<String>,
    reason: &str,
    action: &str,
) -> ErrorIssue {
    ErrorIssue {
        row_id,
        issue_type: issue_type.to_string(),
        field_name: field_name.to_string(),
        original_value: original_value.into(),
        reason: reason.to_string(),
        action: action.to_string(),
        retryable: false,
    }
}

pub fn qty_sheet_columns(source_columns: &[String], config: &PipelineConfig) -> Vec<String> {
    let mut columns = qty_sheet_base_columns(source_columns);
    append_column(&mut columns, QTY_DM_AMOUNT);
    append_column(&mut columns, QTY_DL_AMOUNT);
    append_column(&mut columns, QTY_MOH_AMOUNT);
    append_column(&mut columns, QTY_MOH_OTHER_AMOUNT);
    append_column(&mut columns, QTY_MOH_LABOR_AMOUNT);
    append_column(&mut columns, QTY_MOH_CONSUMABLES_AMOUNT);
    append_column(&mut columns, QTY_MOH_DEPRECIATION_AMOUNT);
    append_column(&mut columns, QTY_MOH_UTILITIES_AMOUNT);
    for item in config.standalone_cost_items {
        append_column(&mut columns, &format!("本期完工{item}合计完工金额"));
    }
    append_column(&mut columns, QTY_DM_UNIT_COST);
    append_column(&mut columns, QTY_DL_UNIT_COST);
    append_column(&mut columns, QTY_MOH_UNIT_COST);
    append_column(&mut columns, QTY_MOH_OTHER_UNIT_COST);
    append_column(&mut columns, QTY_MOH_LABOR_UNIT_COST);
    append_column(&mut columns, QTY_MOH_CONSUMABLES_UNIT_COST);
    append_column(&mut columns, QTY_MOH_DEPRECIATION_UNIT_COST);
    append_column(&mut columns, QTY_MOH_UTILITIES_UNIT_COST);
    for item in config.standalone_cost_items {
        append_column(&mut columns, standalone_unit_cost_column(item));
    }
    append_column(&mut columns, QTY_MOH_MATCH);
    append_column(
        &mut columns,
        &total_match_column(config.standalone_cost_items),
    );
    append_column(&mut columns, QTY_CHECK_STATUS);
    append_column(&mut columns, QTY_CHECK_REASON);
    columns
}

fn append_column(columns: &mut Vec<String>, column: &str) {
    if !columns.iter().any(|value| value == column) {
        columns.push(column.to_string());
    }
}

pub(crate) fn build_qty_sheet_rows(
    rows: Vec<IndexedFactRow>,
    schema: &ColumnSchema,
    config: &PipelineConfig,
) -> Result<Vec<IndexedFactRow>, CostingError> {
    rows.into_iter()
        .map(|mut row| {
            let completed_qty = decimal_named(&row, schema, COMPLETED_QTY_KEY)?;
            let dm = decimal_named(&row, schema, DM_AMOUNT_KEY)?;
            let dl = decimal_named(&row, schema, DL_AMOUNT_KEY)?;
            let moh = decimal_named(&row, schema, MOH_AMOUNT_KEY)?;
            let moh_other = decimal_named(&row, schema, MOH_OTHER_AMOUNT_KEY)?;
            let moh_labor = decimal_named(&row, schema, MOH_LABOR_AMOUNT_KEY)?;
            let moh_consumables = decimal_named(&row, schema, MOH_CONSUMABLES_AMOUNT_KEY)?;
            let moh_depreciation = decimal_named(&row, schema, MOH_DEPRECIATION_AMOUNT_KEY)?;
            let moh_utilities = decimal_named(&row, schema, MOH_UTILITIES_AMOUNT_KEY)?;

            row.insert_derived(QTY_DM_AMOUNT, CellValue::Decimal(dm));
            row.insert_derived(QTY_DL_AMOUNT, CellValue::Decimal(dl));
            row.insert_derived(QTY_MOH_AMOUNT, CellValue::Decimal(moh));
            row.insert_derived(
                QTY_MOH_OTHER_AMOUNT.to_string(),
                CellValue::Decimal(moh_other),
            );
            row.insert_derived(
                QTY_MOH_LABOR_AMOUNT.to_string(),
                CellValue::Decimal(moh_labor),
            );
            row.insert_derived(
                QTY_MOH_CONSUMABLES_AMOUNT.to_string(),
                CellValue::Decimal(moh_consumables),
            );
            row.insert_derived(
                QTY_MOH_DEPRECIATION_AMOUNT.to_string(),
                CellValue::Decimal(moh_depreciation),
            );
            row.insert_derived(
                QTY_MOH_UTILITIES_AMOUNT.to_string(),
                CellValue::Decimal(moh_utilities),
            );
            row.insert_derived(
                QTY_DM_UNIT_COST.to_string(),
                decimal_or_blank(safe_divide(dm, completed_qty)),
            );
            row.insert_derived(
                QTY_DL_UNIT_COST.to_string(),
                decimal_or_blank(safe_divide(dl, completed_qty)),
            );
            row.insert_derived(
                QTY_MOH_UNIT_COST.to_string(),
                decimal_or_blank(safe_divide(moh, completed_qty)),
            );
            row.insert_derived(
                QTY_MOH_OTHER_UNIT_COST.to_string(),
                decimal_or_blank(safe_divide(moh_other, completed_qty)),
            );
            row.insert_derived(
                QTY_MOH_LABOR_UNIT_COST.to_string(),
                decimal_or_blank(safe_divide(moh_labor, completed_qty)),
            );
            row.insert_derived(
                QTY_MOH_CONSUMABLES_UNIT_COST.to_string(),
                decimal_or_blank(safe_divide(moh_consumables, completed_qty)),
            );
            row.insert_derived(
                QTY_MOH_DEPRECIATION_UNIT_COST.to_string(),
                decimal_or_blank(safe_divide(moh_depreciation, completed_qty)),
            );
            row.insert_derived(
                QTY_MOH_UTILITIES_UNIT_COST.to_string(),
                decimal_or_blank(safe_divide(moh_utilities, completed_qty)),
            );

            let mut derived_total = dm + dl + moh;
            for item in config.standalone_cost_items {
                let key = standalone_key(item);
                let amount = decimal_named(&row, schema, &key)?;
                row.insert_derived(
                    format!("本期完工{item}合计完工金额"),
                    CellValue::Decimal(amount),
                );
                let unit_cost_column = standalone_unit_cost_column(item);
                row.insert_derived(
                    unit_cost_column.to_string(),
                    decimal_or_blank(safe_divide(amount, completed_qty)),
                );
                derived_total += amount;
            }

            let moh_match = if moh_other
                + moh_labor
                + moh_consumables
                + moh_depreciation
                + moh_utilities
                == moh
            {
                "是"
            } else {
                "否"
            };
            row.insert_derived(
                QTY_MOH_MATCH.to_string(),
                CellValue::Text(moh_match.to_string()),
            );

            let completed_total = decimal_named(&row, schema, COMPLETED_TOTAL_KEY)?;
            let total_match = if derived_total == completed_total {
                "是"
            } else {
                "否"
            };
            row.insert_derived(
                total_match_column(config.standalone_cost_items),
                CellValue::Text(total_match.to_string()),
            );

            let reason = build_check_reason(moh_match, total_match, config.standalone_cost_items);
            row.insert_derived(
                QTY_CHECK_STATUS.to_string(),
                CellValue::Text(
                    if reason.is_empty() {
                        "通过"
                    } else {
                        "需复核"
                    }
                    .to_string(),
                ),
            );
            row.insert_derived(QTY_CHECK_REASON.to_string(), CellValue::Text(reason));
            Ok(row)
        })
        .collect()
}

fn build_check_reason(moh_match: &str, total_match: &str, standalone_items: &[&str]) -> String {
    let total_mismatch_reason = format!("{}与总完工成本不一致", total_expression(standalone_items));
    match (moh_match, total_match) {
        ("否", "否") => format!("制造费用明细与合计不一致;{total_mismatch_reason}"),
        ("否", _) => "制造费用明细与合计不一致".to_string(),
        (_, "否") => total_mismatch_reason,
        _ => String::new(),
    }
}

fn total_expression(standalone_items: &[&str]) -> String {
    let mut parts = vec!["直接材料", "直接人工", "制造费用"];
    parts.extend(standalone_items.iter().copied());
    parts.join("+")
}

fn total_amount_from_row(
    row: &IndexedFactRow,
    schema: &ColumnSchema,
    config: &PipelineConfig,
) -> Result<Decimal, CostingError> {
    let mut total = decimal_named(row, schema, DM_AMOUNT_KEY)?
        + decimal_named(row, schema, DL_AMOUNT_KEY)?
        + decimal_named(row, schema, MOH_AMOUNT_KEY)?;
    for item in config.standalone_cost_items {
        total += decimal_named(row, schema, &standalone_key(item))?;
    }
    Ok(total)
}

fn moh_component_sum(row: &IndexedFactRow, schema: &ColumnSchema) -> Result<Decimal, CostingError> {
    Ok(decimal_named(row, schema, MOH_OTHER_AMOUNT_KEY)?
        + decimal_named(row, schema, MOH_LABOR_AMOUNT_KEY)?
        + decimal_named(row, schema, MOH_CONSUMABLES_AMOUNT_KEY)?
        + decimal_named(row, schema, MOH_DEPRECIATION_AMOUNT_KEY)?
        + decimal_named(row, schema, MOH_UTILITIES_AMOUNT_KEY)?)
}

fn work_order_key(row: &IndexedRow, columns: &WorkOrderColumns) -> Result<String, CostingError> {
    let period = columns
        .month_or_period
        .map(|id| row.get(id).map(normalize_key_value))
        .transpose()?
        .unwrap_or_default();
    Ok([
        period,
        normalize_key_value(row.get(columns.product_code)?),
        normalize_key_value(row.get(columns.work_order_number)?),
        normalize_key_value(row.get(columns.work_order_line)?),
    ]
    .join("|"))
}

fn normalize_key_value(value: &CellValue) -> String {
    let normalized = cell_to_text(value).trim().to_string();
    if let Some(integer) = normalized.strip_suffix(".0") {
        if !integer.is_empty() && integer.chars().all(|character| character.is_ascii_digit()) {
            return integer.to_string();
        }
    }
    normalized
}

fn bucket_names(cost_item: &str, standalone_items: &[&str]) -> Vec<String> {
    let normalized = cost_item.trim();
    let mut buckets = match normalized {
        "直接材料" => vec![DM_AMOUNT_KEY.to_string()],
        "直接人工" => vec![DL_AMOUNT_KEY.to_string()],
        value if value.starts_with("制造费用") => {
            // 制造费用明细同时参与制造费用总额和明细勾稽，保持 Python fact_builder 的双口径。
            let mut buckets = vec![MOH_AMOUNT_KEY.to_string()];
            if let Some(component_key) = moh_component_key(value) {
                buckets.push(component_key.to_string());
            }
            buckets
        }
        _ => Vec::new(),
    };

    if standalone_items
        .iter()
        .any(|item| item.trim() == normalized)
    {
        buckets.push(standalone_key(normalized));
    }

    buckets
}

fn standalone_key(item: &str) -> String {
    match item.trim() {
        "委外加工费" => "outsource_amount".to_string(),
        "软件费用" => "software_amount".to_string(),
        other => format!("standalone:{other}"),
    }
}

fn moh_component_key(item: &str) -> Option<&'static str> {
    match item.trim() {
        "制造费用_其他" => Some(MOH_OTHER_AMOUNT_KEY),
        "制造费用-人工" => Some(MOH_LABOR_AMOUNT_KEY),
        "制造费用_机物料及低耗" => Some(MOH_CONSUMABLES_AMOUNT_KEY),
        "制造费用_折旧" => Some(MOH_DEPRECIATION_AMOUNT_KEY),
        "制造费用_水电费" => Some(MOH_UTILITIES_AMOUNT_KEY),
        _ => None,
    }
}

fn standalone_unit_cost_column(item: &str) -> &'static str {
    match item.trim() {
        "委外加工费" => QTY_OUTSOURCE_UNIT_COST,
        "软件费用" => QTY_SOFTWARE_UNIT_COST,
        _ => "独立成本项单位完工成本",
    }
}

fn total_match_column(items: &[&str]) -> String {
    format!("{}是否等于总完工成本", total_expression(items))
}

fn decimal_named(
    row: &IndexedFactRow,
    schema: &ColumnSchema,
    column: &str,
) -> Result<Decimal, CostingError> {
    Ok(row
        .get_named(schema, column)?
        .and_then(cell_to_decimal)
        .unwrap_or(ZERO))
}

fn decimal_or_blank(value: Option<Decimal>) -> CellValue {
    value.map(CellValue::Decimal).unwrap_or(CellValue::Blank)
}

fn safe_divide(numerator: Decimal, denominator: Decimal) -> Option<Decimal> {
    if denominator == ZERO {
        None
    } else {
        numerator.checked_div(denominator)
    }
}

fn cell_to_text(value: &CellValue) -> String {
    match value {
        CellValue::Blank => String::new(),
        CellValue::Text(value) | CellValue::DateLike(value) => value.clone(),
        CellValue::Decimal(value) => value.normalize().to_string(),
    }
}

fn cell_to_decimal(value: &CellValue) -> Option<Decimal> {
    match value {
        CellValue::Decimal(value) => Some(*value),
        CellValue::Text(value) => value.trim().parse().ok(),
        CellValue::Blank | CellValue::DateLike(_) => None,
    }
}

fn validate_required_columns(
    schema: &ColumnSchema,
    required_columns: &[&str],
    dataset_name: &str,
) -> Result<(), CostingError> {
    let missing = required_columns
        .iter()
        .filter(|column| schema.optional(column).is_none())
        .copied()
        .collect::<BTreeSet<_>>();

    if missing.is_empty() {
        return Ok(());
    }

    Err(CostingError::invalid_input(format!(
        "{dataset_name}缺少必要字段: {}",
        missing.into_iter().collect::<Vec<_>>().join(", ")
    )))
}

#[cfg(test)]
mod tests {
    use crate::anomaly::build_work_order_anomaly_sheet;
    use crate::error::ErrorCode;
    use crate::model::{CellValue, SplitResult};
    use crate::pipeline::{PipelineConfig, PipelineName};
    use crate::table::IndexedTable;

    use super::*;

    type NamedTestRow = BTreeMap<String, CellValue>;

    fn row(values: &[(&str, CellValue)]) -> NamedTestRow {
        values
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect()
    }

    fn split_result(detail_rows: Vec<NamedTestRow>, qty_rows: Vec<NamedTestRow>) -> SplitResult {
        let mut columns = [
            "月份",
            "产品编码",
            "产品名称",
            "工单编号",
            "工单行号",
            "成本项目名称",
            "本期完工数量",
            "本期完工金额",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
        for column in detail_rows.iter().chain(&qty_rows).flat_map(BTreeMap::keys) {
            if !columns.contains(column) {
                columns.push(column.clone());
            }
        }
        split_result_with_columns(columns, detail_rows, qty_rows)
    }

    fn split_result_with_columns(
        columns: Vec<String>,
        detail_rows: Vec<NamedTestRow>,
        qty_rows: Vec<NamedTestRow>,
    ) -> SplitResult {
        let detail_len = detail_rows.len();
        let positional = detail_rows
            .into_iter()
            .chain(qty_rows)
            .map(|mut named| {
                columns
                    .iter()
                    .map(|column| named.remove(column).unwrap_or(CellValue::Blank))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let table = IndexedTable::from_raw(columns.clone(), positional).unwrap();
        let (schema, _, mut rows) = table.into_parts();
        let qty_rows = rows.split_off(detail_len);
        let detail_display_columns = schema.display_order_for(&columns).unwrap();
        SplitResult {
            qty_display_columns: detail_display_columns.clone(),
            schema,
            detail_display_columns,
            detail_rows: rows,
            qty_rows,
        }
    }

    fn fact_value<'a>(
        schema: &ColumnSchema,
        row: &'a IndexedFactRow,
        column: &str,
    ) -> Option<&'a CellValue> {
        row.get_named(schema, column).unwrap()
    }

    fn bundle_columns(bundle: &FactBundle) -> Vec<String> {
        bundle
            .qty_display_columns
            .iter()
            .map(|id| bundle.schema.name(*id).unwrap().to_string())
            .collect()
    }

    #[test]
    fn indexed_fact_row_reads_derived_value_before_same_named_source_value() {
        let table = IndexedTable::from_raw(
            vec!["metric".to_string()],
            vec![vec![CellValue::Text("source".to_string())]],
        )
        .unwrap();
        let (schema, _, mut rows) = table.into_parts();
        let mut row = IndexedFactRow::new(rows.pop().unwrap());
        row.insert_derived("metric", CellValue::Text("derived".to_string()));

        assert_eq!(
            row.get_named(&schema, "metric").unwrap(),
            Some(&CellValue::Text("derived".to_string()))
        );
    }

    #[test]
    fn indexed_fact_row_keeps_source_schema_unchanged_when_derived_values_are_inserted() {
        let table = IndexedTable::from_raw(
            vec!["source".to_string()],
            vec![vec![CellValue::Text("value".to_string())]],
        )
        .unwrap();
        let (schema, _, mut rows) = table.into_parts();
        let mut row = IndexedFactRow::new(rows.pop().unwrap());

        row.insert_derived("derived", CellValue::Decimal(Decimal::ONE));

        assert_eq!(schema.len(), 1);
        assert_eq!(schema.optional("derived"), None);
        assert_eq!(
            row.get_named(&schema, "derived").unwrap(),
            Some(&CellValue::Decimal(Decimal::ONE))
        );
    }

    #[test]
    fn indexed_fact_row_takes_derived_value_without_rebuilding_source_map() {
        let table = IndexedTable::from_raw(
            vec!["source".to_string()],
            vec![vec![CellValue::Text("value".to_string())]],
        )
        .unwrap();
        let (schema, _, mut rows) = table.into_parts();
        let source = schema.require("source").unwrap();
        let mut row = IndexedFactRow::new(rows.pop().unwrap());
        row.insert_derived("derived", CellValue::Decimal(Decimal::ONE));

        assert_eq!(
            row.take_named(&schema, "derived").unwrap(),
            Some(CellValue::Decimal(Decimal::ONE))
        );
        assert_eq!(row.get_named(&schema, "derived").unwrap(), None);
        assert_eq!(
            row.get_named(&schema, "source").unwrap(),
            Some(&CellValue::Text("value".to_string()))
        );
        assert_eq!(
            row.source.get(source).unwrap(),
            &CellValue::Text("value".to_string())
        );
    }

    #[test]
    fn gb_quantity_sheet_includes_outsource_total_match() {
        let detail = vec![
            row(&[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("产品编码", CellValue::Text("P1".to_string())),
                ("产品名称", CellValue::Text("产品".to_string())),
                ("工单编号", CellValue::Text("WO1".to_string())),
                ("工单行号", CellValue::Text("1".to_string())),
                ("成本项目名称", CellValue::Text("直接材料".to_string())),
                ("本期完工金额", CellValue::Decimal(Decimal::new(100, 0))),
            ]),
            row(&[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("产品编码", CellValue::Text("P1".to_string())),
                ("产品名称", CellValue::Text("产品".to_string())),
                ("工单编号", CellValue::Text("WO1".to_string())),
                ("工单行号", CellValue::Text("1".to_string())),
                ("成本项目名称", CellValue::Text("委外加工费".to_string())),
                ("本期完工金额", CellValue::Decimal(Decimal::new(5, 0))),
            ]),
        ];
        let qty = vec![row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("本期完工数量", CellValue::Decimal(Decimal::new(10, 0))),
            ("本期完工金额", CellValue::Decimal(Decimal::new(105, 0))),
        ])];
        let config = PipelineConfig::for_name(PipelineName::Gb);
        let bundle = build_fact_bundle(split_result(detail, qty), &config).unwrap();
        let sheet = build_qty_sheet_rows(bundle.qty_rows.clone(), &bundle.schema, &config).unwrap();
        assert_eq!(
            fact_value(&bundle.schema, &sheet[0], "本期完工委外加工费合计完工金额"),
            Some(&CellValue::Decimal(Decimal::new(5, 0)))
        );
        assert_eq!(
            fact_value(
                &bundle.schema,
                &sheet[0],
                "直接材料+直接人工+制造费用+委外加工费是否等于总完工成本"
            ),
            Some(&CellValue::Text("是".to_string()))
        );
    }

    #[test]
    fn work_order_keys_trim_text_and_normalize_integer_suffixes_before_joining() {
        let detail = vec![row(&[
            ("月份", CellValue::Text(" 2025年01期 ".to_string())),
            ("产品编码", CellValue::Text(" P1 ".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text(" WO1 ".to_string())),
            ("工单行号", CellValue::Text(" 1.0 ".to_string())),
            ("成本项目名称", CellValue::Text("直接材料".to_string())),
            ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
        ])];
        let qty = vec![row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("本期完工数量", CellValue::Decimal(Decimal::new(2, 0))),
            ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
        ])];

        let bundle = build_fact_bundle(
            split_result(detail, qty),
            &PipelineConfig::for_name(PipelineName::Gb),
        )
        .unwrap();

        assert_eq!(
            fact_value(&bundle.schema, &bundle.qty_rows[0], DM_AMOUNT_KEY),
            Some(&CellValue::Decimal(Decimal::new(10, 0)))
        );
        assert!(!bundle
            .error_issues
            .iter()
            .any(|issue| issue.issue_type == "TOTAL_COST_MISMATCH"));
    }

    #[test]
    fn overflowing_unit_costs_become_blank_without_panicking() {
        const PRODUCT_ORDER: &[(&str, &str)] = &[("P1", "产品")];
        let amount = Decimal::new(9_999_999_999, 0);
        let tiny_qty = Decimal::new(1, 28);
        let detail = vec![row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("成本项目名称", CellValue::Text("直接材料".to_string())),
            ("本期完工金额", CellValue::Decimal(amount)),
        ])];
        let qty = vec![row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("单据类型", CellValue::Text("汇报入库-普通生产".to_string())),
            ("本期完工数量", CellValue::Decimal(tiny_qty)),
            ("本期完工金额", CellValue::Decimal(amount)),
        ])];
        let config = PipelineConfig {
            product_order: PRODUCT_ORDER,
            ..PipelineConfig::for_name(PipelineName::Gb)
        };

        let bundle = build_fact_bundle(split_result(detail, qty), &config).unwrap();
        let anomaly_sheet = build_work_order_anomaly_sheet(&bundle, &config).unwrap();
        let qty_sheet =
            build_qty_sheet_rows(bundle.qty_rows.clone(), &bundle.schema, &config).unwrap();
        let total_unit_index = anomaly_sheet
            .columns
            .iter()
            .position(|column| column == "总单位完工成本")
            .unwrap();

        assert_eq!(
            fact_value(&bundle.schema, &qty_sheet[0], QTY_DM_UNIT_COST),
            Some(&CellValue::Blank)
        );
        assert_eq!(anomaly_sheet.rows[0][total_unit_index], CellValue::Blank);
        assert!(!bundle.error_issues.iter().any(|issue| {
            issue.issue_type == "NON_POSITIVE_UNIT_COST"
                && matches!(
                    issue.field_name.as_str(),
                    "总单位完工成本" | "直接材料单位完工成本"
                )
        }));
    }

    #[test]
    fn sk_quantity_sheet_includes_software_fee() {
        let detail = vec![row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("成本项目名称", CellValue::Text("软件费用".to_string())),
            ("本期完工金额", CellValue::Decimal(Decimal::new(7, 0))),
        ])];
        let qty = vec![row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("本期完工数量", CellValue::Decimal(Decimal::new(1, 0))),
            ("本期完工金额", CellValue::Decimal(Decimal::new(7, 0))),
        ])];
        let config = PipelineConfig::for_name(PipelineName::Sk);
        let bundle = build_fact_bundle(split_result(detail, qty), &config).unwrap();
        let sheet = build_qty_sheet_rows(bundle.qty_rows.clone(), &bundle.schema, &config).unwrap();
        assert_eq!(
            fact_value(&bundle.schema, &sheet[0], "本期完工软件费用合计完工金额"),
            Some(&CellValue::Decimal(Decimal::new(7, 0)))
        );
    }

    #[test]
    fn fact_bundle_records_missing_amount_and_duplicate_work_order_key() {
        let detail = vec![row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("成本项目名称", CellValue::Text("直接材料".to_string())),
            ("本期完工金额", CellValue::Blank),
        ])];
        let qty = vec![
            row(&[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("产品编码", CellValue::Text("P1".to_string())),
                ("产品名称", CellValue::Text("产品".to_string())),
                ("工单编号", CellValue::Text("WO1".to_string())),
                ("工单行号", CellValue::Text("1".to_string())),
                ("本期完工数量", CellValue::Decimal(Decimal::new(1, 0))),
                ("本期完工金额", CellValue::Decimal(Decimal::new(0, 0))),
            ]),
            row(&[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("产品编码", CellValue::Text("P1".to_string())),
                ("产品名称", CellValue::Text("产品".to_string())),
                ("工单编号", CellValue::Text("WO1".to_string())),
                ("工单行号", CellValue::Text("1".to_string())),
                ("本期完工数量", CellValue::Decimal(Decimal::new(2, 0))),
                ("本期完工金额", CellValue::Decimal(Decimal::new(0, 0))),
            ]),
        ];

        let bundle = build_fact_bundle(
            split_result(detail, qty),
            &PipelineConfig::for_name(PipelineName::Gb),
        )
        .unwrap();

        assert!(bundle
            .error_issues
            .iter()
            .any(|issue| issue.issue_type == "MISSING_AMOUNT"));
        assert!(bundle
            .error_issues
            .iter()
            .any(|issue| issue.issue_type == "DUPLICATE_WORK_ORDER_KEY"));
    }

    #[test]
    fn fact_bundle_records_non_positive_unit_cost_audit_issues() {
        let detail = vec![row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("成本项目名称", CellValue::Text("直接材料".to_string())),
            ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
        ])];
        let qty = vec![row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("本期完工数量", CellValue::Decimal(Decimal::new(2, 0))),
            ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
        ])];

        let bundle = build_fact_bundle(
            split_result(detail, qty),
            &PipelineConfig::for_name(PipelineName::Gb),
        )
        .unwrap();

        let issues = bundle
            .error_issues
            .iter()
            .filter(|issue| issue.issue_type == "NON_POSITIVE_UNIT_COST")
            .collect::<Vec<_>>();
        assert_eq!(issues.len(), 7);
        assert!(issues.iter().any(|issue| {
            issue.field_name == "直接人工单位完工成本"
                && issue.row_id == "2025年01期|P1|WO1|1"
                && issue.original_value == "0"
                && issue.reason == "单位成本小于等于 0，不参与 log 与 Modified Z-score"
                && issue.action == "保留在异常分析页并标记复核原因"
        }));
        assert!(!issues
            .iter()
            .any(|issue| issue.field_name == "委外加工费单位完工成本"));
    }

    #[test]
    fn qty_sheet_marks_moh_and_total_mismatch() {
        let detail = vec![
            row(&[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("产品编码", CellValue::Text("P1".to_string())),
                ("产品名称", CellValue::Text("产品".to_string())),
                ("工单编号", CellValue::Text("WO1".to_string())),
                ("工单行号", CellValue::Text("1".to_string())),
                ("成本项目名称", CellValue::Text("制造费用".to_string())),
                ("本期完工金额", CellValue::Decimal(Decimal::new(20, 0))),
            ]),
            row(&[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("产品编码", CellValue::Text("P1".to_string())),
                ("产品名称", CellValue::Text("产品".to_string())),
                ("工单编号", CellValue::Text("WO1".to_string())),
                ("工单行号", CellValue::Text("1".to_string())),
                ("成本项目名称", CellValue::Text("制造费用-人工".to_string())),
                ("本期完工金额", CellValue::Decimal(Decimal::new(5, 0))),
            ]),
        ];
        let qty = vec![row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("本期完工数量", CellValue::Decimal(Decimal::new(1, 0))),
            ("本期完工金额", CellValue::Decimal(Decimal::new(99, 0))),
        ])];
        let config = PipelineConfig::for_name(PipelineName::Gb);
        let bundle = build_fact_bundle(split_result(detail, qty), &config).unwrap();
        let sheet = build_qty_sheet_rows(bundle.qty_rows.clone(), &bundle.schema, &config).unwrap();
        let columns = qty_sheet_columns(&bundle_columns(&bundle), &config);

        assert!(columns.contains(&"制造费用_人工单位完工成本".to_string()));
        assert_eq!(
            fact_value(&bundle.schema, &sheet[0], "制造费用_人工单位完工成本"),
            Some(&CellValue::Decimal(Decimal::new(5, 0)))
        );

        assert_eq!(
            fact_value(
                &bundle.schema,
                &sheet[0],
                "制造费用明细项合计是否等于制造费用合计"
            ),
            Some(&CellValue::Text("否".to_string()))
        );
        assert_eq!(
            fact_value(
                &bundle.schema,
                &sheet[0],
                "直接材料+直接人工+制造费用+委外加工费是否等于总完工成本"
            ),
            Some(&CellValue::Text("否".to_string()))
        );
        assert!(bundle
            .error_issues
            .iter()
            .any(|issue| issue.issue_type == "MOH_BREAKDOWN_MISMATCH"));
        assert!(bundle
            .error_issues
            .iter()
            .any(|issue| issue.issue_type == "TOTAL_COST_MISMATCH"));
    }

    #[test]
    fn filters_invalid_qty_rows_before_fact_and_sheet_output() {
        let detail = vec![row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("成本项目名称", CellValue::Text("直接材料".to_string())),
            ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
        ])];
        let qty = vec![
            row(&[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("产品编码", CellValue::Text("P1".to_string())),
                ("产品名称", CellValue::Text("产品".to_string())),
                ("工单编号", CellValue::Text("WO1".to_string())),
                ("工单行号", CellValue::Text("1".to_string())),
                ("本期完工数量", CellValue::Decimal(Decimal::new(2, 0))),
                ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
            ]),
            row(&[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("产品编码", CellValue::Text("P1".to_string())),
                ("产品名称", CellValue::Text("产品".to_string())),
                ("工单编号", CellValue::Text("WO-ZERO".to_string())),
                ("工单行号", CellValue::Text("1".to_string())),
                ("本期完工数量", CellValue::Decimal(Decimal::new(0, 0))),
                ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
            ]),
            row(&[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("产品编码", CellValue::Text("P1".to_string())),
                ("产品名称", CellValue::Text("产品".to_string())),
                ("工单编号", CellValue::Text("WO-MISSING".to_string())),
                ("工单行号", CellValue::Text("1".to_string())),
                ("本期完工数量", CellValue::Decimal(Decimal::new(1, 0))),
                ("本期完工金额", CellValue::Blank),
            ]),
        ];
        let config = PipelineConfig::for_name(PipelineName::Gb);

        let bundle = build_fact_bundle(split_result(detail, qty), &config).unwrap();
        assert_eq!(bundle.qty_rows.len(), 1);
        assert_eq!(bundle.work_order_rows.len(), 1);
        assert_eq!(
            fact_value(&bundle.schema, &bundle.qty_rows[0], "工单编号"),
            Some(&CellValue::Text("WO1".to_string()))
        );
        let sheet = build_qty_sheet_rows(bundle.qty_rows.clone(), &bundle.schema, &config).unwrap();
        assert_eq!(sheet.len(), 1);
    }

    #[test]
    fn gb_software_fee_is_unmapped_not_standalone() {
        let detail = vec![row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("成本项目名称", CellValue::Text("软件费用".to_string())),
            ("本期完工金额", CellValue::Decimal(Decimal::new(7, 0))),
        ])];
        let qty = vec![row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("本期完工数量", CellValue::Decimal(Decimal::new(1, 0))),
            ("本期完工金额", CellValue::Decimal(Decimal::new(0, 0))),
        ])];
        let config = PipelineConfig::for_name(PipelineName::Gb);

        let bundle = build_fact_bundle(split_result(detail, qty), &config).unwrap();
        let sheet = build_qty_sheet_rows(bundle.qty_rows.clone(), &bundle.schema, &config).unwrap();

        assert!(bundle
            .error_issues
            .iter()
            .any(|issue| issue.issue_type == "UNMAPPED_COST_ITEM"));
        assert_eq!(
            fact_value(
                &bundle.schema,
                &bundle.work_order_rows[0],
                "software_amount"
            ),
            None
        );
        assert_eq!(
            fact_value(&bundle.schema, &sheet[0], "本期完工软件费用合计完工金额"),
            None
        );
    }

    #[test]
    fn qty_fact_keeps_duplicates_but_work_order_fact_keeps_first() {
        let detail = vec![row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("成本项目名称", CellValue::Text("直接材料".to_string())),
            ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
        ])];
        let qty = vec![
            row(&[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("产品编码", CellValue::Text("P1".to_string())),
                ("产品名称", CellValue::Text("产品".to_string())),
                ("工单编号", CellValue::Text("WO1".to_string())),
                ("工单行号", CellValue::Text("1".to_string())),
                ("本期完工数量", CellValue::Decimal(Decimal::new(1, 0))),
                ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
            ]),
            row(&[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("产品编码", CellValue::Text("P1".to_string())),
                ("产品名称", CellValue::Text("产品".to_string())),
                ("工单编号", CellValue::Text("WO1".to_string())),
                ("工单行号", CellValue::Text("1".to_string())),
                ("本期完工数量", CellValue::Decimal(Decimal::new(2, 0))),
                ("本期完工金额", CellValue::Decimal(Decimal::new(20, 0))),
            ]),
        ];

        let bundle = build_fact_bundle(
            split_result(detail, qty),
            &PipelineConfig::for_name(PipelineName::Gb),
        )
        .unwrap();

        assert_eq!(bundle.qty_rows.len(), 2);
        assert_eq!(bundle.work_order_rows.len(), 1);
        assert_eq!(
            bundle
                .error_issues
                .iter()
                .filter(|issue| issue.issue_type == "DUPLICATE_WORK_ORDER_KEY")
                .count(),
            2
        );
        assert_eq!(
            fact_value(&bundle.schema, &bundle.work_order_rows[0], "本期完工数量"),
            Some(&CellValue::Decimal(Decimal::new(1, 0)))
        );
        assert!(bundle
            .error_issues
            .iter()
            .any(|issue| issue.issue_type == "DUPLICATE_WORK_ORDER_KEY"));
    }

    #[test]
    fn missing_required_columns_returns_error() {
        let detail = vec![row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("成本项目名称", CellValue::Text("直接材料".to_string())),
        ])];
        let qty = vec![row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("本期完工数量", CellValue::Decimal(Decimal::new(1, 0))),
            ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
        ])];

        let error = build_fact_bundle(
            split_result_with_columns(
                [
                    "月份",
                    "产品编码",
                    "产品名称",
                    "工单编号",
                    "工单行号",
                    "成本项目名称",
                    "本期完工数量",
                ]
                .into_iter()
                .map(str::to_string)
                .collect(),
                detail,
                qty,
            ),
            &PipelineConfig::for_name(PipelineName::Gb),
        )
        .unwrap_err();

        assert_eq!(error.code(), ErrorCode::InvalidInput);
        assert!(error.message().contains("本期完工金额"));
    }

    #[test]
    fn empty_rows_still_validate_required_schema_columns() {
        let error = build_fact_bundle(
            split_result_with_columns(vec!["产品编码".to_string()], vec![], vec![]),
            &PipelineConfig::for_name(PipelineName::Gb),
        )
        .unwrap_err();

        assert_eq!(error.code(), ErrorCode::InvalidInput);
        assert!(error.message().contains("缺少必要字段"));
        assert!(error.message().contains("本期完工金额"));
    }

    #[test]
    fn fact_bundle_keeps_qty_filter_audit_counts() {
        let detail = vec![row(&[
            ("月份", CellValue::Text("2025年01期".to_string())),
            ("产品编码", CellValue::Text("P1".to_string())),
            ("产品名称", CellValue::Text("产品".to_string())),
            ("工单编号", CellValue::Text("WO1".to_string())),
            ("工单行号", CellValue::Text("1".to_string())),
            ("成本项目名称", CellValue::Text("直接材料".to_string())),
            ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
        ])];
        let qty = vec![
            row(&[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("产品编码", CellValue::Text("P1".to_string())),
                ("产品名称", CellValue::Text("产品".to_string())),
                ("工单编号", CellValue::Text("WO1".to_string())),
                ("工单行号", CellValue::Text("1".to_string())),
                ("本期完工数量", CellValue::Decimal(Decimal::new(1, 0))),
                ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
            ]),
            row(&[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("产品编码", CellValue::Text("P1".to_string())),
                ("产品名称", CellValue::Text("产品".to_string())),
                ("工单编号", CellValue::Text("WO-ZERO".to_string())),
                ("工单行号", CellValue::Text("1".to_string())),
                ("本期完工数量", CellValue::Decimal(Decimal::ZERO)),
                ("本期完工金额", CellValue::Decimal(Decimal::new(10, 0))),
            ]),
            row(&[
                ("月份", CellValue::Text("2025年01期".to_string())),
                ("产品编码", CellValue::Text("P1".to_string())),
                ("产品名称", CellValue::Text("产品".to_string())),
                ("工单编号", CellValue::Text("WO-MISSING".to_string())),
                ("工单行号", CellValue::Text("1".to_string())),
                ("本期完工数量", CellValue::Decimal(Decimal::ONE)),
                ("本期完工金额", CellValue::Blank),
            ]),
        ];

        let bundle = build_fact_bundle(
            split_result(detail, qty),
            &PipelineConfig::for_name(PipelineName::Gb),
        )
        .unwrap();

        assert_eq!(bundle.qty_input_row_count, 3);
        assert_eq!(bundle.filtered_invalid_qty_count, 1);
        assert_eq!(bundle.filtered_missing_total_amount_count, 1);
        assert_eq!(bundle.qty_rows.len(), 1);
    }
}

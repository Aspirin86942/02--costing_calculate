use std::collections::{BTreeSet, HashMap, HashSet};

use rust_decimal::Decimal;

use crate::error::CostingError;
use crate::model::{CellValue, CostAmounts, ErrorIssue, FactBundle, QtyFactRow, SplitResult};
use crate::pipeline::PipelineRules;
use crate::sheet_contract::qty_sheet_base_columns;
use crate::table::{ColumnId, ColumnSchema, IndexedRow};

const ZERO: Decimal = Decimal::ZERO;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MohComponent {
    Other,
    Labor,
    Consumables,
    Depreciation,
    Utilities,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CostClassification {
    DirectMaterial,
    DirectLabor,
    ManufacturingOverhead(Option<MohComponent>),
    Standalone(usize),
    Unmapped,
}

impl CostAmounts {
    fn add(&mut self, classification: CostClassification, amount: Decimal) {
        match classification {
            CostClassification::DirectMaterial => self.direct_material += amount,
            CostClassification::DirectLabor => self.direct_labor += amount,
            CostClassification::ManufacturingOverhead(component) => {
                // 制造费用一行同时进入总额和可识别细项，保留既有双口径勾稽语义。
                self.manufacturing_overhead += amount;
                match component {
                    Some(MohComponent::Other) => self.moh_other += amount,
                    Some(MohComponent::Labor) => self.moh_labor += amount,
                    Some(MohComponent::Consumables) => self.moh_consumables += amount,
                    Some(MohComponent::Depreciation) => self.moh_depreciation += amount,
                    Some(MohComponent::Utilities) => self.moh_utilities += amount,
                    None => {}
                }
            }
            CostClassification::Standalone(index) => {
                // index 只由同一配置 slice 的 position 产生，因此这里可直接定位稳定槽位。
                self.standalone[index] += amount;
            }
            CostClassification::Unmapped => {}
        }
    }
}

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

#[derive(Debug)]
struct PreparedQtyRow {
    source: IndexedRow,
    work_order_key: String,
    completed_qty: Decimal,
    completed_total: Decimal,
}

#[derive(Debug)]
struct ReconciliationAudit {
    moh_component_sum: Decimal,
    derived_total: Decimal,
    moh_matches: bool,
    total_matches: bool,
    check_reason: String,
}

#[derive(Debug, Clone, Copy)]
enum UnitCostAmount {
    CompletedTotal,
    DirectMaterial,
    DirectLabor,
    ManufacturingOverhead,
    MohOther,
    MohLabor,
    MohConsumables,
    MohDepreciation,
    MohUtilities,
}

impl UnitCostAmount {
    fn value(self, row: &QtyFactRow) -> Decimal {
        match self {
            Self::CompletedTotal => row.completed_total,
            Self::DirectMaterial => row.amounts.direct_material,
            Self::DirectLabor => row.amounts.direct_labor,
            Self::ManufacturingOverhead => row.amounts.manufacturing_overhead,
            Self::MohOther => row.amounts.moh_other,
            Self::MohLabor => row.amounts.moh_labor,
            Self::MohConsumables => row.amounts.moh_consumables,
            Self::MohDepreciation => row.amounts.moh_depreciation,
            Self::MohUtilities => row.amounts.moh_utilities,
        }
    }
}

const NON_POSITIVE_UNIT_COST_METRICS: &[(UnitCostAmount, &str)] = &[
    (UnitCostAmount::CompletedTotal, "总单位完工成本"),
    (UnitCostAmount::DirectMaterial, "直接材料单位完工成本"),
    (UnitCostAmount::DirectLabor, "直接人工单位完工成本"),
    (
        UnitCostAmount::ManufacturingOverhead,
        "制造费用单位完工成本",
    ),
    (UnitCostAmount::MohOther, "制造费用_其他单位完工成本"),
    (UnitCostAmount::MohLabor, "制造费用_人工单位完工成本"),
    (
        UnitCostAmount::MohConsumables,
        "制造费用_机物料及低耗单位完工成本",
    ),
    (UnitCostAmount::MohDepreciation, "制造费用_折旧单位完工成本"),
    (UnitCostAmount::MohUtilities, "制造费用_水电费单位完工成本"),
];

pub fn build_fact_bundle(
    split: SplitResult,
    config: &PipelineRules,
) -> Result<FactBundle, CostingError> {
    let (schema, detail_display_columns, detail_rows, qty_display_columns, qty_source_rows) =
        split.into_parts();
    let detail_columns = DetailFactColumns::resolve(&schema)?;
    let qty_columns = QtyFactColumns::resolve(&schema)?;
    let mut error_issues = Vec::new();
    let amounts_by_key = aggregate_detail_rows_in_input_order(
        &detail_rows,
        &detail_columns,
        config,
        &mut error_issues,
    )?;

    let qty_input_row_count = qty_source_rows.len();
    let mut prepared_rows = Vec::with_capacity(qty_input_row_count);
    let mut qty_key_counts: HashMap<String, usize> = HashMap::new();
    let mut filtered_invalid_qty_count = 0usize;
    let mut filtered_missing_total_amount_count = 0usize;
    for source in qty_source_rows {
        let completed_qty = cell_to_decimal(source.get(qty_columns.completed_qty)?);
        let completed_total = cell_to_decimal(source.get(qty_columns.completed_amount)?);
        let (completed_qty, completed_total) = match (completed_qty, completed_total) {
            (Some(qty), Some(total)) if qty > ZERO => (qty, total),
            (Some(qty), None) if qty > ZERO => {
                filtered_missing_total_amount_count += 1;
                continue;
            }
            _ => {
                filtered_invalid_qty_count += 1;
                continue;
            }
        };
        let work_order_key = work_order_key(&source, &qty_columns.key)?;
        *qty_key_counts.entry(work_order_key.clone()).or_default() += 1;
        prepared_rows.push(PreparedQtyRow {
            source,
            work_order_key,
            completed_qty,
            completed_total,
        });
    }

    let mut duplicate_work_order_row_count = 0usize;
    for row in &prepared_rows {
        let count = qty_key_counts
            .get(&row.work_order_key)
            .copied()
            .unwrap_or(0);
        if count > 1 {
            duplicate_work_order_row_count += 1;
            error_issues.push(duplicate_work_order_issue(&row.work_order_key, count));
        }
    }

    let mut qty_rows = Vec::with_capacity(prepared_rows.len());
    let mut unique_work_order_indices = Vec::new();
    let mut seen_work_orders = HashSet::new();
    for prepared in prepared_rows {
        let amounts = amounts_by_key
            .get(&prepared.work_order_key)
            .cloned()
            .unwrap_or_else(|| CostAmounts::new(config.standalone_cost_items.len()));
        let audit = calculate_reconciliation(&amounts, prepared.completed_total, config);
        append_reconciliation_issues_in_current_order(
            &mut error_issues,
            &prepared.work_order_key,
            &amounts,
            prepared.completed_total,
            &audit,
            config,
        );
        let index = qty_rows.len();
        let is_first = seen_work_orders.insert(prepared.work_order_key.clone());
        qty_rows.push(build_qty_fact_row(prepared, amounts, audit));
        if is_first {
            unique_work_order_indices.push(index);
        }
    }

    append_non_positive_unit_cost_issues(&qty_rows, &unique_work_order_indices, &mut error_issues);

    Ok(FactBundle {
        schema,
        detail_display_columns,
        detail_rows,
        qty_display_columns,
        qty_rows,
        unique_work_order_indices,
        qty_input_row_count,
        filtered_invalid_qty_count,
        filtered_missing_total_amount_count,
        duplicate_work_order_row_count,
        error_issues,
    })
}

fn aggregate_detail_rows_in_input_order(
    rows: &[IndexedRow],
    columns: &DetailFactColumns,
    config: &PipelineRules,
    error_issues: &mut Vec<ErrorIssue>,
) -> Result<HashMap<String, CostAmounts>, CostingError> {
    let mut amounts_by_key = HashMap::new();
    for row in rows {
        let key = work_order_key(row, &columns.key)?;
        let cost_item = cell_to_text(row.get(columns.cost_item)?);
        let amount_cell = row.get(columns.completed_amount)?;
        let amount = cell_to_decimal(amount_cell);
        let classification = classify_cost_item(&cost_item, &config.standalone_cost_items);
        if classification == CostClassification::Unmapped {
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
                cell_to_text(amount_cell),
                "成本明细金额为空，已按 0 参与汇总",
                "金额置为 0 后继续计算",
            ));
        }
        amounts_by_key
            .entry(key)
            .or_insert_with(|| CostAmounts::new(config.standalone_cost_items.len()))
            .add(classification, amount.unwrap_or(ZERO));
    }
    Ok(amounts_by_key)
}

fn duplicate_work_order_issue(work_order_key: &str, count: usize) -> ErrorIssue {
    error_issue(
        work_order_key.to_string(),
        "DUPLICATE_WORK_ORDER_KEY",
        "工单主键",
        count.to_string(),
        "数量页存在重复工单主键",
        "数量页原样保留，异常分析按首条记录去重",
    )
}

fn calculate_reconciliation(
    amounts: &CostAmounts,
    completed_total: Decimal,
    config: &PipelineRules,
) -> ReconciliationAudit {
    let moh_component_sum = amounts.moh_component_sum();
    let derived_total = amounts.direct_material
        + amounts.direct_labor
        + amounts.manufacturing_overhead
        + amounts.standalone.iter().copied().sum::<Decimal>();
    let moh_matches = moh_component_sum == amounts.manufacturing_overhead;
    let total_matches = derived_total == completed_total;
    ReconciliationAudit {
        moh_component_sum,
        derived_total,
        moh_matches,
        total_matches,
        check_reason: build_check_reason(moh_matches, total_matches, &config.standalone_cost_items),
    }
}

fn append_reconciliation_issues_in_current_order(
    error_issues: &mut Vec<ErrorIssue>,
    work_order_key: &str,
    amounts: &CostAmounts,
    completed_total: Decimal,
    audit: &ReconciliationAudit,
    config: &PipelineRules,
) {
    if !audit.moh_matches {
        error_issues.push(error_issue(
            work_order_key.to_string(),
            "MOH_BREAKDOWN_MISMATCH",
            "制造费用",
            format!(
                "明细合计={};制造费用={}",
                audit.moh_component_sum, amounts.manufacturing_overhead
            ),
            "制造费用明细项合计不等于制造费用合计",
            "保留结果并标记需复核",
        ));
    }
    if !audit.total_matches {
        error_issues.push(error_issue(
            work_order_key.to_string(),
            "TOTAL_COST_MISMATCH",
            "总完工成本",
            format!("计算值={};数量页={}", audit.derived_total, completed_total),
            &format!(
                "{}不等于数量页总完工成本",
                total_expression(&config.standalone_cost_items)
            ),
            "保留结果并标记需复核",
        ));
    }
}

fn build_qty_fact_row(
    prepared: PreparedQtyRow,
    amounts: CostAmounts,
    audit: ReconciliationAudit,
) -> QtyFactRow {
    QtyFactRow {
        source: prepared.source,
        work_order_key: prepared.work_order_key,
        completed_qty: prepared.completed_qty,
        completed_total: prepared.completed_total,
        amounts,
        moh_matches: audit.moh_matches,
        total_matches: audit.total_matches,
        check_reason: audit.check_reason,
    }
}

fn append_non_positive_unit_cost_issues(
    qty_rows: &[QtyFactRow],
    unique_work_order_indices: &[usize],
    error_issues: &mut Vec<ErrorIssue>,
) {
    for index in unique_work_order_indices {
        let row = &qty_rows[*index];
        for (amount, field_name) in NON_POSITIVE_UNIT_COST_METRICS {
            let Some(unit_cost) = safe_divide(amount.value(row), row.completed_qty) else {
                continue;
            };
            if unit_cost <= ZERO {
                error_issues.push(error_issue(
                    row.work_order_key.clone(),
                    "NON_POSITIVE_UNIT_COST",
                    field_name,
                    unit_cost.normalize().to_string(),
                    "单位成本小于等于 0，不参与 log 与 Modified Z-score",
                    "保留在异常分析页并标记复核原因",
                ));
            }
        }
    }
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

pub fn qty_sheet_columns(source_columns: &[String], config: &PipelineRules) -> Vec<String> {
    let mut columns = qty_sheet_base_columns(source_columns);
    append_column(&mut columns, QTY_DM_AMOUNT);
    append_column(&mut columns, QTY_DL_AMOUNT);
    append_column(&mut columns, QTY_MOH_AMOUNT);
    append_column(&mut columns, QTY_MOH_OTHER_AMOUNT);
    append_column(&mut columns, QTY_MOH_LABOR_AMOUNT);
    append_column(&mut columns, QTY_MOH_CONSUMABLES_AMOUNT);
    append_column(&mut columns, QTY_MOH_DEPRECIATION_AMOUNT);
    append_column(&mut columns, QTY_MOH_UTILITIES_AMOUNT);
    for item in &config.standalone_cost_items {
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
    for item in &config.standalone_cost_items {
        append_column(&mut columns, standalone_unit_cost_column(item));
    }
    append_column(&mut columns, QTY_MOH_MATCH);
    append_column(
        &mut columns,
        &total_match_column(&config.standalone_cost_items),
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

fn build_check_reason(
    moh_matches: bool,
    total_matches: bool,
    standalone_items: &[String],
) -> String {
    let total_mismatch_reason = format!("{}与总完工成本不一致", total_expression(standalone_items));
    match (moh_matches, total_matches) {
        (false, false) => format!("制造费用明细与合计不一致;{total_mismatch_reason}"),
        (false, true) => "制造费用明细与合计不一致".to_string(),
        (true, false) => total_mismatch_reason,
        (true, true) => String::new(),
    }
}

fn total_expression(standalone_items: &[String]) -> String {
    let mut parts = vec![
        "直接材料".to_string(),
        "直接人工".to_string(),
        "制造费用".to_string(),
    ];
    parts.extend(standalone_items.iter().cloned());
    parts.join("+")
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

fn classify_cost_item<T: AsRef<str>>(
    cost_item: &str,
    standalone_items: &[T],
) -> CostClassification {
    let normalized = cost_item.trim();
    match normalized {
        "直接材料" => CostClassification::DirectMaterial,
        "直接人工" => CostClassification::DirectLabor,
        value if value.starts_with("制造费用") => {
            let component = match value {
                "制造费用_其他" => Some(MohComponent::Other),
                "制造费用-人工" => Some(MohComponent::Labor),
                "制造费用_机物料及低耗" => Some(MohComponent::Consumables),
                "制造费用_折旧" => Some(MohComponent::Depreciation),
                "制造费用_水电费" => Some(MohComponent::Utilities),
                _ => None,
            };
            CostClassification::ManufacturingOverhead(component)
        }
        value => standalone_items
            .iter()
            .position(|item| item.as_ref().trim() == value)
            .map(CostClassification::Standalone)
            .unwrap_or(CostClassification::Unmapped),
    }
}

fn standalone_unit_cost_column(item: &str) -> &'static str {
    match item.trim() {
        "委外加工费" => QTY_OUTSOURCE_UNIT_COST,
        "软件费用" => QTY_SOFTWARE_UNIT_COST,
        _ => "独立成本项单位完工成本",
    }
}

fn total_match_column(items: &[String]) -> String {
    format!("{}是否等于总完工成本", total_expression(items))
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
#[path = "fact_tests.rs"]
mod tests;

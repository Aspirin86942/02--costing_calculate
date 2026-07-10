use std::collections::BTreeMap;

use crate::error::CostingError;
use crate::model::{CellValue, MonthRange, NormalizedCostFrame, RawWorkbook};
use crate::pipeline::PipelineConfig;
use crate::table::{ColumnId, ColumnSchema, DerivedColumnPosition, IndexedRow, IndexedTable};

const PERIOD_COLUMN: &str = "年期";
const MONTH_COLUMN: &str = "月份";
const COST_CENTER_COLUMN: &str = "成本中心名称";
const CHILD_MATERIAL_COLUMN: &str = "子项物料编码";
const COST_ITEM_COLUMN: &str = "成本项目名称";
const FILLED_COST_ITEM_COLUMN: &str = "Filled_成本项目";
const INTEGRATED_WORKSHOP_NAME: &str = "集成车间";
const FILL_COLUMNS: &[&str] = &[
    PERIOD_COLUMN,
    COST_CENTER_COLUMN,
    "产品编码",
    "产品名称",
    "规格型号",
    "工单编号",
    "工单行号",
    "供应商编码",
    "供应商名称",
    "基本单位",
    "计划产量",
    "生产类型",
    "单据类型",
];
const VENDOR_COLUMNS: &[&str] = &["供应商编码", "供应商名称"];
const KEY_COLUMNS: &[&str] = &[MONTH_COLUMN, "产品编码", "工单编号", "工单行号"];

#[derive(Debug, Clone, Copy)]
struct ResolvedFillColumn {
    id: ColumnId,
    is_vendor: bool,
}

struct NormalizeColumns {
    period: Option<ColumnId>,
    month: Option<ColumnId>,
    cost_center: Option<ColumnId>,
    cost_item: Option<ColumnId>,
    total_row_columns: [Option<ColumnId>; 3],
    fill_columns: Vec<ResolvedFillColumn>,
}

impl NormalizeColumns {
    fn resolve(schema: &ColumnSchema) -> Self {
        Self {
            period: schema.optional(PERIOD_COLUMN),
            month: schema.optional(MONTH_COLUMN),
            cost_center: schema.optional(COST_CENTER_COLUMN),
            cost_item: schema.optional(COST_ITEM_COLUMN),
            total_row_columns: [
                schema.optional(PERIOD_COLUMN),
                schema.optional(MONTH_COLUMN),
                schema.optional(COST_CENTER_COLUMN),
            ],
            fill_columns: FILL_COLUMNS
                .iter()
                .filter_map(|name| {
                    schema.optional(name).map(|id| ResolvedFillColumn {
                        id,
                        is_vendor: VENDOR_COLUMNS.contains(name),
                    })
                })
                .collect(),
        }
    }
}

pub fn build_month_range(
    month_start: Option<&str>,
    month_end: Option<&str>,
) -> Result<Option<MonthRange>, CostingError> {
    if month_start.is_none() && month_end.is_none() {
        return Ok(None);
    }

    Ok(Some(normalize_month_range(MonthRange {
        start: month_start.map(str::to_string),
        end: month_end.map(str::to_string),
    })?))
}

pub fn normalize_workbook(
    raw: RawWorkbook,
    _config: &PipelineConfig,
    month_range: Option<MonthRange>,
) -> Result<NormalizedCostFrame, CostingError> {
    let normalized_range = month_range.map(normalize_month_range).transpose()?;
    let mut source_names = flatten_headers(&raw.header_rows);
    normalize_key_column_names(&mut source_names);
    let mut table = IndexedTable::from_raw(source_names, raw.rows)?;
    let columns = NormalizeColumns::resolve(table.schema());

    table.try_retain_rows(|row| Ok(!is_total_row(row, &columns)?))?;
    forward_fill_with_rules(&mut table, &columns)?;

    let month_id = if let Some(period_id) = columns.period {
        let values = derive_month_values(table.rows(), period_id)?;
        Some(table.ensure_or_reuse_derived_column(
            MONTH_COLUMN,
            DerivedColumnPosition::AfterFirstSourceName(PERIOD_COLUMN),
            values,
        )?)
    } else {
        columns.month
    };

    let filled_values = derive_filled_cost_item_values(table.rows(), columns.cost_item)?;
    table.ensure_or_reuse_derived_column(
        FILLED_COST_ITEM_COLUMN,
        DerivedColumnPosition::End,
        filled_values,
    )?;

    if let Some(range) = normalized_range.as_ref() {
        // 月份过滤统一走 YYYY-MM 键，避免展示格式差异影响边界命中。
        table.try_retain_rows(|row| month_in_range(row, month_id, columns.period, range))?;
    }

    Ok(NormalizedCostFrame::new(
        table,
        KEY_COLUMNS.iter().map(|name| (*name).to_string()).collect(),
    ))
}

pub fn flatten_headers(header_rows: &[Vec<String>; 2]) -> Vec<String> {
    let width = header_rows[0].len().max(header_rows[1].len());
    (0..width)
        .map(|index| {
            let top = header_rows[0].get(index).map(String::as_str).unwrap_or("");
            let second = header_rows[1].get(index).map(String::as_str).unwrap_or("");
            let top = clean_header_token(top);
            let second = clean_header_token(second);
            let merged = if !top.is_empty() && !second.is_empty() && top != second {
                format!("{top}{second}")
            } else if !second.is_empty() {
                second
            } else {
                top
            };
            if merged.is_empty() {
                format!("column_{index}")
            } else {
                merged
            }
        })
        .collect()
}

fn clean_header_token(value: &str) -> String {
    let token = value.trim().replace(' ', "").replace('\n', "");
    if token.is_empty()
        || token.to_ascii_lowercase().starts_with("unnamed")
        || matches!(token.as_str(), "None" | "nan" | "NaN")
    {
        String::new()
    } else {
        token
    }
}

fn normalize_key_column_names(columns: &mut [String]) {
    let rename_map = infer_rename_map(columns);
    for column in columns.iter_mut() {
        if let Some(target) = rename_map.get(column.as_str()) {
            *column = target.clone();
        }
    }
}

fn infer_rename_map(columns: &[String]) -> BTreeMap<String, String> {
    let mut rename_map = BTreeMap::new();

    if !columns.iter().any(|column| column == CHILD_MATERIAL_COLUMN) {
        if let Some(candidate) = columns
            .iter()
            .find(|column| column.contains("物料编码") || column.contains("子件"))
        {
            rename_map.insert(candidate.clone(), CHILD_MATERIAL_COLUMN.to_string());
        }
    }

    if !columns.iter().any(|column| column == COST_ITEM_COLUMN) {
        if let Some(candidate) = columns
            .iter()
            .find(|column| column.contains("成本项目") || column.contains("费用项目"))
        {
            rename_map.insert(candidate.clone(), COST_ITEM_COLUMN.to_string());
        }
    }

    rename_map
}

fn is_total_row(row: &IndexedRow, columns: &NormalizeColumns) -> Result<bool, CostingError> {
    for id in columns.total_row_columns.iter().flatten().copied() {
        if cell_text(row.get(id)?).contains("合计") {
            return Ok(true);
        }
    }
    Ok(false)
}

fn forward_fill_with_rules(
    table: &mut IndexedTable,
    columns: &NormalizeColumns,
) -> Result<(), CostingError> {
    let mut last_values = vec![None; columns.fill_columns.len()];
    table.try_update_rows(|row| {
        for (index, column) in columns.fill_columns.iter().enumerate() {
            let current = row.get(column.id)?.clone();
            let integrated_row = columns
                .cost_center
                .map(|id| {
                    row.get(id)
                        .map(|value| cell_text(value) == INTEGRATED_WORKSHOP_NAME)
                })
                .transpose()?
                .unwrap_or(false);

            if is_blank_like(&current) {
                if column.is_vendor && integrated_row {
                    continue;
                }
                if let Some(previous) = last_values[index].clone() {
                    row.replace(column.id, previous)?;
                }
                continue;
            }

            if column.is_vendor && integrated_row {
                // 集成车间行不能成为供应商向下填充的种子，避免跨工单串值。
                continue;
            }
            last_values[index] = Some(current);
        }
        Ok(())
    })
}

fn derive_month_values(
    rows: &[IndexedRow],
    period: ColumnId,
) -> Result<Vec<CellValue>, CostingError> {
    rows.iter()
        .map(|row| row.get(period).map(format_period_value))
        .collect()
}

fn derive_filled_cost_item_values(
    rows: &[IndexedRow],
    cost_item: Option<ColumnId>,
) -> Result<Vec<CellValue>, CostingError> {
    let mut last_cost_item: Option<CellValue> = None;
    let mut values = Vec::with_capacity(rows.len());
    for row in rows {
        let current = cost_item
            .map(|id| row.get(id).cloned())
            .transpose()?
            .unwrap_or(CellValue::Blank);
        if !is_blank_like(&current) {
            last_cost_item = Some(current.clone());
            values.push(current);
        } else {
            values.push(last_cost_item.clone().unwrap_or(CellValue::Blank));
        }
    }
    Ok(values)
}

fn format_period_value(value: &CellValue) -> CellValue {
    let text = cell_text(value);
    if text.is_empty() {
        return CellValue::Blank;
    }

    if text.contains('年') && text.contains('期') {
        if let Some(period_key) = normalize_period_key_from_text(&text) {
            let year = &period_key[0..4];
            let month = &period_key[5..7];
            return CellValue::Text(format!("{year}年{month}期"));
        }
    }

    CellValue::Text(text)
}

fn month_in_range(
    row: &IndexedRow,
    month: Option<ColumnId>,
    period: Option<ColumnId>,
    range: &MonthRange,
) -> Result<bool, CostingError> {
    let normalized = month
        .map(|id| row.get(id).map(normalize_period_key))
        .transpose()?
        .flatten()
        .or(period
            .map(|id| row.get(id).map(normalize_period_key))
            .transpose()?
            .flatten());

    Ok(match normalized {
        None => false,
        Some(period) => {
            let after_start = range
                .start
                .as_ref()
                .map(|start| period.as_str() >= start.as_str())
                .unwrap_or(true);
            let before_end = range
                .end
                .as_ref()
                .map(|end| period.as_str() <= end.as_str())
                .unwrap_or(true);
            after_start && before_end
        }
    })
}

fn normalize_month_range(range: MonthRange) -> Result<MonthRange, CostingError> {
    let start = normalize_cli_month(range.start.as_deref(), "month_start")?;
    let end = normalize_cli_month(range.end.as_deref(), "month_end")?;
    if let (Some(start), Some(end)) = (&start, &end) {
        if start > end {
            return Err(CostingError::invalid_input(format!(
                "month_start={start} 不能晚于 month_end={end}"
            )));
        }
    }
    Ok(MonthRange { start, end })
}

fn normalize_cli_month(
    value: Option<&str>,
    field_name: &str,
) -> Result<Option<String>, CostingError> {
    let Some(value) = value else {
        return Ok(None);
    };

    let trimmed = value.trim();
    let Some((year, month)) = trimmed.split_once('-') else {
        return Err(CostingError::invalid_input(format!(
            "{field_name} 必须是 YYYY-MM 格式，收到: {value:?}"
        )));
    };
    if year.len() != 4
        || month.len() != 2
        || !year.chars().all(|ch| ch.is_ascii_digit())
        || !month.chars().all(|ch| ch.is_ascii_digit())
    {
        return Err(CostingError::invalid_input(format!(
            "{field_name} 必须是 YYYY-MM 格式，收到: {value:?}"
        )));
    }

    let month_number: u32 = month.parse().map_err(|_| {
        CostingError::invalid_input(format!("{field_name} 必须是 YYYY-MM 格式，收到: {value:?}"))
    })?;
    if !(1..=12).contains(&month_number) {
        return Err(CostingError::invalid_input(format!(
            "{field_name} 必须是 YYYY-MM 格式，收到: {value:?}"
        )));
    }

    Ok(Some(format!("{year}-{month_number:02}")))
}

fn normalize_period_key(value: &CellValue) -> Option<String> {
    normalize_period_key_from_text(&cell_text(value))
}

fn normalize_period_key_from_text(value: &str) -> Option<String> {
    let digits: String = value.chars().filter(|ch| ch.is_ascii_digit()).collect();
    if digits.len() < 5 {
        return None;
    }

    let year = &digits[0..4];
    let month_end = digits.len().min(6);
    let month: u32 = digits[4..month_end].parse().ok()?;
    if !(1..=12).contains(&month) {
        return None;
    }

    Some(format!("{year}-{month:02}"))
}

fn is_blank_like(value: &CellValue) -> bool {
    match value {
        CellValue::Blank => true,
        CellValue::Text(text) | CellValue::DateLike(text) => text.trim().is_empty(),
        CellValue::Decimal(_) => false,
    }
}

fn cell_text(value: &CellValue) -> String {
    match value {
        CellValue::Blank => String::new(),
        CellValue::Text(value) | CellValue::DateLike(value) => value.trim().to_string(),
        CellValue::Decimal(value) => value.normalize().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::{PipelineConfig, PipelineName};

    fn raw_table(columns: &[&str], rows: Vec<Vec<CellValue>>) -> RawWorkbook {
        RawWorkbook {
            sheet_name: "成本计算单".to_string(),
            header_rows: [
                vec![String::new(); columns.len()],
                columns.iter().map(|name| (*name).to_string()).collect(),
            ],
            rows,
        }
    }

    fn value(frame: &NormalizedCostFrame, row_index: usize, column: &str) -> CellValue {
        let id = frame.table.schema().require(column).unwrap();
        frame.table.rows()[row_index].get(id).unwrap().clone()
    }

    fn raw_with_vendor_rows() -> RawWorkbook {
        RawWorkbook {
            sheet_name: "成本计算单".to_string(),
            header_rows: [
                vec![
                    "年期".to_string(),
                    "成本中心名称".to_string(),
                    "产品编码".to_string(),
                    "工单编号".to_string(),
                    "供应商编码".to_string(),
                ],
                vec![
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                ],
            ],
            rows: vec![
                vec![
                    CellValue::Text("2025年01期".to_string()),
                    CellValue::Text("普通车间".to_string()),
                    CellValue::Text("P1".to_string()),
                    CellValue::Text("WO-1".to_string()),
                    CellValue::Text("V001".to_string()),
                ],
                vec![
                    CellValue::Blank,
                    CellValue::Text("集成车间".to_string()),
                    CellValue::Blank,
                    CellValue::Blank,
                    CellValue::Blank,
                ],
            ],
        }
    }

    #[test]
    fn forward_fill_skips_vendor_columns_for_integrated_workshop() {
        let config = PipelineConfig::for_name(PipelineName::Gb);
        let normalized = normalize_workbook(raw_with_vendor_rows(), &config, None).unwrap();
        assert_eq!(
            value(&normalized, 1, "产品编码"),
            CellValue::Text("P1".to_string())
        );
        assert_eq!(value(&normalized, 1, "供应商编码"), CellValue::Blank);
    }

    #[test]
    fn integrated_workshop_rows_do_not_seed_vendor_forward_fill() {
        let config = PipelineConfig::for_name(PipelineName::Gb);
        let raw = RawWorkbook {
            sheet_name: "成本计算单".to_string(),
            header_rows: [
                vec![
                    "年期".to_string(),
                    "成本中心名称".to_string(),
                    "产品编码".to_string(),
                    "工单编号".to_string(),
                    "供应商编码".to_string(),
                ],
                vec![
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                ],
            ],
            rows: vec![
                vec![
                    CellValue::Text("2025年01期".to_string()),
                    CellValue::Text("普通车间".to_string()),
                    CellValue::Text("P1".to_string()),
                    CellValue::Text("WO-1".to_string()),
                    CellValue::Text("V001".to_string()),
                ],
                vec![
                    CellValue::Blank,
                    CellValue::Text("集成车间".to_string()),
                    CellValue::Text("P2".to_string()),
                    CellValue::Text("WO-2".to_string()),
                    CellValue::Text("V999".to_string()),
                ],
                vec![
                    CellValue::Blank,
                    CellValue::Text("普通车间".to_string()),
                    CellValue::Text("P3".to_string()),
                    CellValue::Text("WO-3".to_string()),
                    CellValue::Blank,
                ],
            ],
        };

        let normalized = normalize_workbook(raw, &config, None).unwrap();
        assert_eq!(
            value(&normalized, 2, "供应商编码"),
            CellValue::Text("V001".to_string())
        );
    }

    #[test]
    fn removes_total_rows() {
        let config = PipelineConfig::for_name(PipelineName::Gb);
        let mut raw = raw_with_vendor_rows();
        raw.rows.push(vec![
            CellValue::Text("合计".to_string()),
            CellValue::Text("普通车间".to_string()),
            CellValue::Blank,
            CellValue::Blank,
            CellValue::Blank,
        ]);
        let normalized = normalize_workbook(raw, &config, None).unwrap();
        assert_eq!(normalized.row_count(), 2);
    }

    #[test]
    fn flatten_headers_merges_two_level_tokens() {
        let headers = [
            vec!["成本项目".to_string(), "供应商".to_string()],
            vec!["名称".to_string(), "编码".to_string()],
        ];

        assert_eq!(
            flatten_headers(&headers),
            vec!["成本项目名称".to_string(), "供应商编码".to_string()]
        );
    }

    #[test]
    fn normalizes_alias_columns_like_python_infer_rename_map() {
        let config = PipelineConfig::for_name(PipelineName::Gb);
        let raw = RawWorkbook {
            sheet_name: "成本计算单".to_string(),
            header_rows: [
                vec![
                    "年期".to_string(),
                    "物料编码".to_string(),
                    "费用项目".to_string(),
                    "工单编号".to_string(),
                ],
                vec![
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                    "".to_string(),
                ],
            ],
            rows: vec![vec![
                CellValue::Text("2025年01期".to_string()),
                CellValue::Text("MAT-1".to_string()),
                CellValue::Text("制造费用".to_string()),
                CellValue::Text("WO-1".to_string()),
            ]],
        };

        let normalized = normalize_workbook(raw, &config, None).unwrap();

        assert!(normalized.table.schema().optional("子项物料编码").is_some());
        assert!(normalized.table.schema().optional("成本项目名称").is_some());
        assert_eq!(
            value(&normalized, 0, "子项物料编码"),
            CellValue::Text("MAT-1".to_string())
        );
        assert_eq!(
            value(&normalized, 0, "成本项目名称"),
            CellValue::Text("制造费用".to_string())
        );
    }

    #[test]
    fn always_adds_blank_filled_cost_item_when_cost_item_column_missing() {
        let config = PipelineConfig::for_name(PipelineName::Gb);
        let raw = RawWorkbook {
            sheet_name: "成本计算单".to_string(),
            header_rows: [
                vec!["年期".to_string(), "工单编号".to_string()],
                vec!["".to_string(), "".to_string()],
            ],
            rows: vec![vec![
                CellValue::Text("2025年01期".to_string()),
                CellValue::Text("WO-1".to_string()),
            ]],
        };

        let normalized = normalize_workbook(raw, &config, None).unwrap();

        assert!(normalized
            .table
            .schema()
            .optional(FILLED_COST_ITEM_COLUMN)
            .is_some());
        assert_eq!(
            value(&normalized, 0, FILLED_COST_ITEM_COLUMN),
            CellValue::Blank
        );
    }

    #[test]
    fn key_columns_match_python_contract_even_if_some_columns_are_missing() {
        let config = PipelineConfig::for_name(PipelineName::Gb);
        let normalized = normalize_workbook(raw_with_vendor_rows(), &config, None).unwrap();

        assert_eq!(
            normalized.key_columns(),
            vec![
                "月份".to_string(),
                "产品编码".to_string(),
                "工单编号".to_string(),
                "工单行号".to_string()
            ]
        );
    }

    #[test]
    fn adds_month_column_after_period_column() {
        let config = PipelineConfig::for_name(PipelineName::Gb);
        let normalized = normalize_workbook(raw_with_vendor_rows(), &config, None).unwrap();
        let (schema, display, rows) = normalized.into_table().into_parts();
        assert_eq!(schema.name(display[1]).unwrap(), "月份");
        let month = schema.require("月份").unwrap();
        assert_eq!(
            rows[0].get(month).unwrap(),
            &CellValue::Text("2025年01期".to_string())
        );
    }

    #[test]
    fn zero_pads_single_digit_month_display_like_python() {
        let config = PipelineConfig::for_name(PipelineName::Gb);
        let raw = RawWorkbook {
            sheet_name: "成本计算单".to_string(),
            header_rows: [
                vec!["年期".to_string(), "工单编号".to_string()],
                vec!["".to_string(), "".to_string()],
            ],
            rows: vec![vec![
                CellValue::Text("2025年7期".to_string()),
                CellValue::Text("WO-1".to_string()),
            ]],
        };

        let normalized = normalize_workbook(raw, &config, None).unwrap();

        assert_eq!(
            value(&normalized, 0, "月份"),
            CellValue::Text("2025年07期".to_string())
        );
    }

    #[test]
    fn filters_rows_by_strict_month_range() {
        let config = PipelineConfig::for_name(PipelineName::Gb);
        let raw = RawWorkbook {
            sheet_name: "成本计算单".to_string(),
            header_rows: [
                vec!["年期".to_string(), "工单编号".to_string()],
                vec!["".to_string(), "".to_string()],
            ],
            rows: vec![
                vec![
                    CellValue::Text("2025年01期".to_string()),
                    CellValue::Text("WO-1".to_string()),
                ],
                vec![
                    CellValue::Text("2025年02期".to_string()),
                    CellValue::Text("WO-2".to_string()),
                ],
            ],
        };

        let normalized = normalize_workbook(
            raw,
            &config,
            Some(MonthRange {
                start: Some("2025-02".to_string()),
                end: Some("2025-02".to_string()),
            }),
        )
        .unwrap();

        assert_eq!(normalized.row_count(), 1);
        assert_eq!(
            value(&normalized, 0, "工单编号"),
            CellValue::Text("WO-2".to_string())
        );
    }

    #[test]
    fn build_month_range_rejects_non_strict_cli_value() {
        let error = build_month_range(Some("2025年01期"), None).unwrap_err();
        assert_eq!(error.code(), crate::error::ErrorCode::InvalidInput);
    }

    #[test]
    fn build_month_range_rejects_non_ascii_value_without_panicking() {
        // 7 个 UTF-8 字节且第 4 字节位于中文字符内部，可覆盖历史字节切片 panic。
        let error = build_month_range(Some("123中a"), None).unwrap_err();
        assert_eq!(error.code(), crate::error::ErrorCode::InvalidInput);
    }

    #[test]
    fn fills_cost_item_from_previous_non_blank_row() {
        let frame = normalize_workbook(
            raw_table(
                &["成本项目名称"],
                vec![
                    vec![CellValue::Text("直接人工".to_string())],
                    vec![CellValue::Blank],
                ],
            ),
            &PipelineConfig::for_name(PipelineName::Gb),
            None,
        )
        .unwrap();
        let (schema, _, rows) = frame.into_table().into_parts();
        let filled = schema.require(FILLED_COST_ITEM_COLUMN).unwrap();

        assert_eq!(
            rows[1].get(filled).unwrap(),
            &CellValue::Text("直接人工".to_string())
        );
    }

    #[test]
    fn alias_collision_uses_last_physical_column() {
        let frame = normalize_workbook(
            raw_table(
                &["物料编码", "物料编码"],
                vec![vec![
                    CellValue::Text("FIRST".to_string()),
                    CellValue::Text("LAST".to_string()),
                ]],
            ),
            &PipelineConfig::for_name(PipelineName::Gb),
            None,
        )
        .unwrap();
        let (schema, _, rows) = frame.into_table().into_parts();
        let material = schema.require(CHILD_MATERIAL_COLUMN).unwrap();

        assert_eq!(
            rows[0].get(material).unwrap(),
            &CellValue::Text("LAST".to_string())
        );
    }

    #[test]
    fn existing_month_column_reuses_last_slot_without_moving_display_position() {
        let frame = normalize_workbook(
            raw_table(
                &["年期", "月份", "月份"],
                vec![vec![
                    CellValue::Text("2025年07期".to_string()),
                    CellValue::Text("first".to_string()),
                    CellValue::Text("last".to_string()),
                ]],
            ),
            &PipelineConfig::for_name(PipelineName::Gb),
            None,
        )
        .unwrap();
        let (schema, display, rows) = frame.into_table().into_parts();
        let display_names = display
            .iter()
            .map(|id| schema.name(*id).unwrap())
            .collect::<Vec<_>>();
        let month = schema.require(MONTH_COLUMN).unwrap();

        assert_eq!(display_names[..3], ["年期", "月份", "月份"]);
        assert_eq!(schema.len(), 4);
        assert_eq!(
            rows[0].get(month).unwrap(),
            &CellValue::Text("2025年07期".to_string())
        );
    }

    #[test]
    fn missing_period_column_does_not_add_or_overwrite_month() {
        let frame = normalize_workbook(
            raw_table(
                &["月份"],
                vec![vec![CellValue::Text("manual-month".to_string())]],
            ),
            &PipelineConfig::for_name(PipelineName::Gb),
            None,
        )
        .unwrap();
        let (schema, display, rows) = frame.into_table().into_parts();
        let month = schema.require(MONTH_COLUMN).unwrap();

        assert_eq!(schema.len(), 2);
        assert_eq!(schema.name(display[0]).unwrap(), MONTH_COLUMN);
        assert_eq!(
            rows[0].get(month).unwrap(),
            &CellValue::Text("manual-month".to_string())
        );
    }

    #[test]
    fn duplicate_period_column_inserts_month_after_first_name_and_reads_last_slot() {
        let frame = normalize_workbook(
            raw_table(
                &["年期", "年期", "成本中心名称"],
                vec![vec![
                    CellValue::Text("2025年01期".to_string()),
                    CellValue::Text("2025年02期".to_string()),
                    CellValue::Text("车间".to_string()),
                ]],
            ),
            &PipelineConfig::for_name(PipelineName::Gb),
            None,
        )
        .unwrap();
        let (schema, display, rows) = frame.into_table().into_parts();
        let display_names = display
            .iter()
            .map(|id| schema.name(*id).unwrap())
            .collect::<Vec<_>>();
        let month = schema.require(MONTH_COLUMN).unwrap();

        assert_eq!(display_names[..3], ["年期", "月份", "年期"]);
        assert_eq!(
            rows[0].get(month).unwrap(),
            &CellValue::Text("2025年02期".to_string())
        );
        assert_eq!(schema.len(), 5);
    }

    #[test]
    fn existing_filled_cost_item_reuses_last_slot_without_appending() {
        let frame = normalize_workbook(
            raw_table(
                &[
                    COST_ITEM_COLUMN,
                    FILLED_COST_ITEM_COLUMN,
                    FILLED_COST_ITEM_COLUMN,
                ],
                vec![vec![
                    CellValue::Text("直接人工".to_string()),
                    CellValue::Text("first".to_string()),
                    CellValue::Text("last".to_string()),
                ]],
            ),
            &PipelineConfig::for_name(PipelineName::Gb),
            None,
        )
        .unwrap();
        let (schema, display, rows) = frame.into_table().into_parts();
        let display_names = display
            .iter()
            .map(|id| schema.name(*id).unwrap())
            .collect::<Vec<_>>();
        let filled = schema.require(FILLED_COST_ITEM_COLUMN).unwrap();

        assert_eq!(
            display_names,
            [
                COST_ITEM_COLUMN,
                FILLED_COST_ITEM_COLUMN,
                FILLED_COST_ITEM_COLUMN
            ]
        );
        assert_eq!(schema.len(), 3);
        assert_eq!(
            rows[0].get(filled).unwrap(),
            &CellValue::Text("直接人工".to_string())
        );
    }
}

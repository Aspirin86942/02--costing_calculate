use std::collections::BTreeMap;

use crate::error::CostingError;
use crate::model::{CellValue, MonthRange, NormalizedCostFrame, RawWorkbook, TableRow};
use crate::pipeline::PipelineConfig;

const PERIOD_COLUMN: &str = "年期";
const MONTH_COLUMN: &str = "月份";
const COST_CENTER_COLUMN: &str = "成本中心名称";
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
const KEY_COLUMN_CANDIDATES: &[&str] = &[MONTH_COLUMN, "产品编码", "工单编号", "工单行号"];

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
    let normalized_range = match month_range {
        Some(range) => Some(normalize_month_range(range)?),
        None => None,
    };

    let mut columns = flatten_headers(&raw.header_rows);
    let mut rows = rows_to_maps(&columns, raw.rows);

    rows.retain(|row| !is_total_row(row));
    forward_fill_with_rules(&mut rows);
    insert_month_column(&mut columns, &mut rows);
    insert_filled_cost_item_column(&mut columns, &mut rows);

    if let Some(range) = normalized_range.as_ref() {
        // 月份过滤统一走 YYYY-MM 键，避免展示格式差异影响边界命中。
        rows.retain(|row| month_in_range(row, range));
    }

    let key_columns = KEY_COLUMN_CANDIDATES
        .iter()
        .filter(|column| columns.iter().any(|existing| existing == **column))
        .map(|column| (*column).to_string())
        .collect();

    Ok(NormalizedCostFrame {
        columns,
        rows,
        key_columns,
    })
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

fn rows_to_maps(columns: &[String], rows: Vec<Vec<CellValue>>) -> Vec<TableRow> {
    rows.into_iter()
        .map(|row| {
            let values = columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    (
                        column.clone(),
                        row.get(index).cloned().unwrap_or(CellValue::Blank),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            TableRow { values }
        })
        .collect()
}

fn is_total_row(row: &TableRow) -> bool {
    [PERIOD_COLUMN, MONTH_COLUMN, COST_CENTER_COLUMN]
        .iter()
        .filter_map(|column| row.values.get(*column))
        .any(|value| cell_text(value).contains("合计"))
}

fn forward_fill_with_rules(rows: &mut [TableRow]) {
    let mut last_values: BTreeMap<String, CellValue> = BTreeMap::new();

    for row in rows.iter_mut() {
        for column in FILL_COLUMNS {
            if !row.values.contains_key(*column) {
                continue;
            }

            let key = (*column).to_string();
            let current = row.values.get(*column).cloned().unwrap_or(CellValue::Blank);
            let is_vendor_column = VENDOR_COLUMNS.contains(column);
            let integrated_row = row
                .values
                .get(COST_CENTER_COLUMN)
                .map(|value| cell_text(value) == INTEGRATED_WORKSHOP_NAME)
                .unwrap_or(false);

            if is_blank_like(&current) {
                if is_vendor_column && integrated_row {
                    continue;
                }
                if let Some(previous) = last_values.get(*column).cloned() {
                    row.values.insert(key, previous);
                }
                continue;
            }

            if is_vendor_column && integrated_row {
                // 集成车间行不能成为供应商向下填充的种子，避免跨工单串值。
                continue;
            }

            last_values.insert(key, current);
        }
    }
}

fn insert_month_column(columns: &mut Vec<String>, rows: &mut [TableRow]) {
    let Some(period_index) = columns.iter().position(|column| column == PERIOD_COLUMN) else {
        return;
    };

    if !columns.iter().any(|column| column == MONTH_COLUMN) {
        columns.insert(period_index + 1, MONTH_COLUMN.to_string());
    }

    for row in rows.iter_mut() {
        let month_value = row
            .values
            .get(PERIOD_COLUMN)
            .map(format_period_value)
            .unwrap_or(CellValue::Blank);
        row.values.insert(MONTH_COLUMN.to_string(), month_value);
    }
}

fn insert_filled_cost_item_column(columns: &mut Vec<String>, rows: &mut [TableRow]) {
    if !columns.iter().any(|column| column == COST_ITEM_COLUMN) {
        return;
    }
    if !columns
        .iter()
        .any(|column| column == FILLED_COST_ITEM_COLUMN)
    {
        columns.push(FILLED_COST_ITEM_COLUMN.to_string());
    }

    let mut last_cost_item: Option<CellValue> = None;
    for row in rows.iter_mut() {
        let current = row
            .values
            .get(COST_ITEM_COLUMN)
            .cloned()
            .unwrap_or(CellValue::Blank);
        if is_blank_like(&current) {
            row.values.insert(
                FILLED_COST_ITEM_COLUMN.to_string(),
                last_cost_item.clone().unwrap_or(CellValue::Blank),
            );
        } else {
            last_cost_item = Some(current.clone());
            row.values
                .insert(FILLED_COST_ITEM_COLUMN.to_string(), current);
        }
    }
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

fn month_in_range(row: &TableRow, range: &MonthRange) -> bool {
    let normalized = row
        .values
        .get(MONTH_COLUMN)
        .and_then(normalize_period_key)
        .or_else(|| row.values.get(PERIOD_COLUMN).and_then(normalize_period_key));

    match normalized {
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
    }
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
    if trimmed.len() != 7 || &trimmed[4..5] != "-" {
        return Err(CostingError::invalid_input(format!(
            "{field_name} 必须是 YYYY-MM 格式，收到: {value:?}"
        )));
    }

    let year = &trimmed[0..4];
    let month = &trimmed[5..7];
    if !year.chars().all(|ch| ch.is_ascii_digit()) || !month.chars().all(|ch| ch.is_ascii_digit()) {
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
    if digits.len() < 6 {
        return None;
    }

    let year = &digits[0..4];
    let month: u32 = digits[4..6].parse().ok()?;
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
            normalized.rows[1].values["产品编码"],
            CellValue::Text("P1".to_string())
        );
        assert_eq!(normalized.rows[1].values["供应商编码"], CellValue::Blank);
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
            normalized.rows[2].values["供应商编码"],
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
        assert_eq!(normalized.rows.len(), 2);
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
    fn adds_month_column_after_period_column() {
        let config = PipelineConfig::for_name(PipelineName::Gb);
        let normalized = normalize_workbook(raw_with_vendor_rows(), &config, None).unwrap();
        assert_eq!(normalized.columns[1], "月份");
        assert_eq!(
            normalized.rows[0].values["月份"],
            CellValue::Text("2025年01期".to_string())
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

        assert_eq!(normalized.rows.len(), 1);
        assert_eq!(
            normalized.rows[0].values["工单编号"],
            CellValue::Text("WO-2".to_string())
        );
    }

    #[test]
    fn build_month_range_rejects_non_strict_cli_value() {
        let error = build_month_range(Some("2025年01期"), None).unwrap_err();
        assert_eq!(error.code(), crate::error::ErrorCode::InvalidInput);
    }
}

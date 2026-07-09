use std::path::Path;

use calamine::{open_workbook_auto, Data, Reader};
use costing_core::model::{CellValue, RawWorkbook};
use rust_decimal::Decimal;

const HEADER_ANCHOR: &str = "年期";
const HEADER_HINTS: &[&str] = &["成本中心名称", "产品编码", "工单编号", "成本项目名称"];

#[derive(Debug, thiserror::Error)]
pub enum CostingXlsxError {
    #[error("{0}")]
    Message(String),
    #[error("xlsx read error: {0}")]
    Calamine(#[from] calamine::Error),
}

pub fn read_raw_workbook(path: &Path) -> Result<RawWorkbook, CostingXlsxError> {
    let mut workbook = open_workbook_auto(path)?;
    let sheet_name = workbook
        .sheet_names()
        .first()
        .cloned()
        .ok_or_else(|| CostingXlsxError::Message("workbook has no sheets".to_string()))?;
    let range = workbook.worksheet_range(&sheet_name)?;
    let rows: Vec<Vec<Data>> = range.rows().map(|row| row.to_vec()).collect();
    if rows.len() < 2 {
        return Err(CostingXlsxError::Message(
            "workbook must contain two header rows".to_string(),
        ));
    }
    let header_start = find_header_start(&rows);
    let max_width = rows
        .iter()
        .skip(header_start)
        .map(Vec::len)
        .max()
        .unwrap_or(0);
    let header_rows = [
        normalize_header_row(rows.get(header_start).expect("checked len"), max_width),
        normalize_header_row(rows.get(header_start + 1).expect("checked len"), max_width),
    ];
    let data_rows = rows
        .iter()
        .skip(header_start + 2)
        .map(|row| normalize_data_row(row, max_width))
        .collect();
    Ok(RawWorkbook {
        sheet_name,
        header_rows,
        rows: data_rows,
    })
}

fn find_header_start(rows: &[Vec<Data>]) -> usize {
    rows.windows(2)
        .position(|pair| is_header_pair(&pair[0], &pair[1]))
        .unwrap_or(0)
}

fn is_header_pair(top: &[Data], bottom: &[Data]) -> bool {
    let top_tokens = normalize_header_row(top, top.len());
    let mut tokens = top_tokens.clone();
    tokens.extend(normalize_header_row(bottom, bottom.len()));

    let has_anchor = top_tokens.iter().any(|token| token == HEADER_ANCHOR);
    let has_hint = HEADER_HINTS
        .iter()
        .any(|hint| tokens.iter().any(|token| token == hint));
    has_anchor && has_hint
}

fn normalize_header_row(row: &[Data], width: usize) -> Vec<String> {
    (0..width)
        .map(|idx| match row.get(idx).unwrap_or(&Data::Empty) {
            Data::String(value) => value.trim().to_string(),
            Data::Float(value) => float_text(*value),
            Data::Int(value) => value.to_string(),
            Data::Bool(value) => value.to_string(),
            Data::DateTime(value) => value.to_string(),
            Data::DateTimeIso(value) => value.trim().to_string(),
            Data::DurationIso(value) => value.trim().to_string(),
            Data::Error(value) => format!("{value:?}"),
            Data::Empty => String::new(),
        })
        .collect()
}

fn normalize_data_row(row: &[Data], width: usize) -> Vec<CellValue> {
    (0..width)
        .map(|idx| match row.get(idx).unwrap_or(&Data::Empty) {
            Data::Empty => CellValue::Blank,
            Data::String(value) => {
                let text = value.trim().to_string();
                if text.is_empty() {
                    CellValue::Blank
                } else {
                    CellValue::Text(text)
                }
            }
            Data::Float(value) => float_cell_value(*value),
            Data::Int(value) => CellValue::Decimal(Decimal::from(*value)),
            Data::Bool(value) => CellValue::Text(value.to_string()),
            Data::DateTime(value) => CellValue::DateLike(value.to_string()),
            Data::DateTimeIso(value) => CellValue::DateLike(value.clone()),
            Data::DurationIso(value) => CellValue::Text(value.clone()),
            Data::Error(value) => CellValue::Text(format!("{value:?}")),
        })
        .collect()
}

fn float_text(value: f64) -> String {
    if value.is_finite() && value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

fn float_cell_value(value: f64) -> CellValue {
    if !value.is_finite() {
        return CellValue::Text(value.to_string());
    }
    let text = float_text(value);
    Decimal::from_str_exact(&text)
        .or_else(|_| Decimal::from_scientific(&text))
        .map(CellValue::Decimal)
        .unwrap_or(CellValue::Text(text))
}

#[cfg(test)]
mod tests {
    use std::process;
    use std::time::{SystemTime, UNIX_EPOCH};

    use costing_core::model::{CellValue, ReaderSnapshot};
    use rust_decimal::Decimal;
    use rust_xlsxwriter::{ExcelDateTime, Format, Workbook};

    use crate::snapshot::build_reader_snapshot;

    use super::*;

    fn unique_temp_path(stem: &str) -> std::path::PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "costing-xlsx-{stem}-pid{}-{timestamp}.xlsx",
            process::id()
        ))
    }

    fn write_reader_fixture(path: &std::path::Path) {
        let mut workbook = Workbook::new();

        let primary = workbook.add_worksheet();
        primary.set_name("成本计算单").unwrap();
        primary.write_string(0, 0, "项目").unwrap();
        primary.write_string(0, 1, "金额").unwrap();
        primary.write_string(0, 2, "日期").unwrap();
        primary.write_string(1, 0, "").unwrap();
        primary.write_string(1, 1, "").unwrap();
        primary.write_string(1, 2, "").unwrap();
        primary.write_string(2, 0, "首行").unwrap();
        primary.write_number(2, 1, 0.1).unwrap();
        let date_format = Format::new().set_num_format("yyyy-mm-dd");
        primary
            .write_datetime_with_format(
                2,
                2,
                ExcelDateTime::from_ymd(2025, 1, 2).unwrap(),
                &date_format,
            )
            .unwrap();
        primary.write_string(3, 0, "次行").unwrap();
        primary.write_number(3, 1, 12.34).unwrap();

        let secondary = workbook.add_worksheet();
        secondary.set_name("备用表").unwrap();
        secondary.write_string(0, 0, "should not be read").unwrap();

        workbook.save(path).unwrap();
    }

    #[test]
    fn reads_numeric_and_date_values_from_the_first_sheet() {
        let path = unique_temp_path("first-sheet");
        write_reader_fixture(&path);

        let raw = read_raw_workbook(&path).unwrap();

        assert_eq!(raw.sheet_name, "成本计算单");
        assert_eq!(raw.header_rows[0], vec!["项目", "金额", "日期"]);
        assert_eq!(raw.header_rows[1], vec!["", "", ""]);
        assert_eq!(raw.rows[0][0], CellValue::Text("首行".to_string()));
        assert_eq!(raw.rows[0][1], CellValue::Decimal(Decimal::new(1, 1)));
        assert!(matches!(raw.rows[0][2], CellValue::DateLike(ref text) if !text.is_empty()));
        assert_eq!(raw.rows[1][0], CellValue::Text("次行".to_string()));
        assert_eq!(raw.rows[1][1], CellValue::Decimal(Decimal::new(1234, 2)));
        assert_eq!(raw.rows[1][2], CellValue::Blank);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn snapshot_counts_blank_cells_from_reader_rows() {
        let path = unique_temp_path("snapshot-blanks");
        write_reader_fixture(&path);

        let raw = read_raw_workbook(&path).unwrap();
        let snapshot: ReaderSnapshot = build_reader_snapshot(&raw);

        assert_eq!(snapshot.sheet_name, "成本计算单");
        assert_eq!(snapshot.row_count, 2);
        assert_eq!(snapshot.column_count, 3);
        assert_eq!(snapshot.headers, vec!["项目", "金额", "日期"]);
        assert_eq!(snapshot.null_counts["日期"], 1);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn skips_metadata_rows_before_header_pair() {
        let path = unique_temp_path("metadata-header");
        let mut workbook = Workbook::new();
        let worksheet = workbook.add_worksheet();
        worksheet.set_name("成本计算单").unwrap();
        worksheet
            .write_string(0, 0, "核算体系:财务会计核算体系")
            .unwrap();
        worksheet.write_string(1, 0, "币别:人民币").unwrap();
        worksheet.write_string(2, 0, "年期").unwrap();
        worksheet.write_string(2, 1, "成本中心名称").unwrap();
        worksheet.write_string(2, 2, "产品编码").unwrap();
        worksheet.write_string(3, 0, "年期").unwrap();
        worksheet.write_string(3, 1, "成本中心名称").unwrap();
        worksheet.write_string(3, 2, "产品编码").unwrap();
        worksheet.write_string(4, 0, "2025年07期").unwrap();
        worksheet.write_string(4, 1, "集成车间").unwrap();
        worksheet.write_string(4, 2, "GB_C.D.B0048AA").unwrap();
        workbook.save(&path).unwrap();

        let raw = read_raw_workbook(&path).unwrap();

        assert_eq!(raw.header_rows[0][0], "年期");
        assert_eq!(raw.header_rows[1][2], "产品编码");
        assert_eq!(raw.rows.len(), 1);
        assert_eq!(raw.rows[0][0], CellValue::Text("2025年07期".to_string()));
        let _ = std::fs::remove_file(path);
    }
}

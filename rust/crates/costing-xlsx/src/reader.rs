use std::path::Path;

use calamine::{open_workbook_auto, Data, Reader};
use costing_core::model::{CellValue, RawWorkbook};
use rust_decimal::Decimal;

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
        return Err(CostingXlsxError::Message("workbook must contain two header rows".to_string()));
    }
    let max_width = rows.iter().map(Vec::len).max().unwrap_or(0);
    let header_rows = [
        normalize_header_row(rows.first().expect("checked len"), max_width),
        normalize_header_row(rows.get(1).expect("checked len"), max_width),
    ];
    let data_rows = rows
        .iter()
        .skip(2)
        .map(|row| normalize_data_row(row, max_width))
        .collect();
    Ok(RawWorkbook {
        sheet_name,
        header_rows,
        rows: data_rows,
    })
}

fn normalize_header_row(row: &[Data], width: usize) -> Vec<String> {
    (0..width)
        .map(|idx| match row.get(idx).unwrap_or(&Data::Empty) {
            Data::String(value) => value.trim().to_string(),
            Data::Float(value) => trim_numeric_string(*value),
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
            Data::Float(value) => Decimal::from_f64_retain(*value)
                .map(CellValue::Decimal)
                .unwrap_or_else(|| CellValue::Text(value.to_string())),
            Data::Int(value) => CellValue::Decimal(Decimal::from(*value)),
            Data::Bool(value) => CellValue::Text(value.to_string()),
            Data::DateTime(value) => CellValue::DateLike(value.to_string()),
            Data::DateTimeIso(value) => CellValue::DateLike(value.clone()),
            Data::DurationIso(value) => CellValue::Text(value.clone()),
            Data::Error(value) => CellValue::Text(format!("{value:?}")),
        })
        .collect()
}

fn trim_numeric_string(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use costing_core::model::CellValue;
    use rust_xlsxwriter::Workbook;

    use super::*;

    #[test]
    fn reads_two_header_rows_and_data_values() {
        let path = std::env::temp_dir().join("costing-reader-two-headers.xlsx");
        let mut workbook = Workbook::new();
        let worksheet = workbook.add_worksheet();
        worksheet.set_name("成本计算单").unwrap();
        worksheet.write_string(0, 0, "年期").unwrap();
        worksheet.write_string(0, 1, "产品").unwrap();
        worksheet.write_string(1, 0, "").unwrap();
        worksheet.write_string(1, 1, "产品编码").unwrap();
        worksheet.write_string(2, 0, "2025年01期").unwrap();
        worksheet.write_string(2, 1, "GB_C.D.B0040AA").unwrap();
        workbook.save(&path).unwrap();

        let raw = read_raw_workbook(&path).unwrap();

        assert_eq!(raw.sheet_name, "成本计算单");
        assert_eq!(raw.header_rows[0][0], "年期");
        assert_eq!(raw.header_rows[1][1], "产品编码");
        assert_eq!(raw.rows[0][0], CellValue::Text("2025年01期".to_string()));
        assert_eq!(raw.rows[0][1], CellValue::Text("GB_C.D.B0040AA".to_string()));
        let _ = std::fs::remove_file(path);
    }
}

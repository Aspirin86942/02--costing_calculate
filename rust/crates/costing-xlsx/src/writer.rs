use std::path::Path;

use costing_core::model::{CellValue, WorkbookPayload};
use rust_decimal::prelude::ToPrimitive;
use rust_xlsxwriter::{Color, Format, FormatAlign, FormatBorder, Workbook, Worksheet};

use crate::reader::CostingXlsxError;

const PRODUCT_DIMENSION_SHEET: &str = "成本分析产品维度";

pub fn write_workbook(path: &Path, payload: &WorkbookPayload) -> Result<(), CostingXlsxError> {
    let mut workbook = Workbook::new();
    for sheet in &payload.sheet_models {
        if sheet.sheet_name == PRODUCT_DIMENSION_SHEET {
            return Err(CostingXlsxError::Message(format!(
                "{PRODUCT_DIMENSION_SHEET} 不属于 Rust 默认 workbook 契约"
            )));
        }

        let worksheet = workbook.add_worksheet();
        worksheet
            .set_name(&sheet.sheet_name)
            .map_err(|error| CostingXlsxError::Message(error.to_string()))?;

        let header_format = Format::new()
            .set_bold()
            .set_background_color(Color::RGB(0xD9E1F2))
            .set_border(FormatBorder::Thin);
        let text_format = Format::new().set_align(FormatAlign::Left);

        write_header_row(worksheet, &sheet.columns, sheet.fixed_width, &header_format)?;
        write_data_rows(
            worksheet,
            &sheet.columns,
            &sheet.rows,
            &sheet.number_formats,
            &text_format,
        )?;

        if sheet.auto_filter && !sheet.columns.is_empty() {
            let last_row = sheet.rows.len() as u32;
            let last_col = (sheet.columns.len() - 1) as u16;
            worksheet
                .autofilter(0, 0, last_row, last_col)
                .map_err(|error| CostingXlsxError::Message(error.to_string()))?;
        }
        if let Some(freeze_panes) = &sheet.freeze_panes {
            let (row, col) = parse_freeze_panes(freeze_panes)?;
            worksheet
                .set_freeze_panes(row, col)
                .map_err(|error| CostingXlsxError::Message(error.to_string()))?;
        }
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| CostingXlsxError::Message(error.to_string()))?;
    }
    workbook
        .save(path)
        .map_err(|error| CostingXlsxError::Message(error.to_string()))
}

fn write_header_row(
    worksheet: &mut Worksheet,
    columns: &[String],
    fixed_width: Option<f64>,
    header_format: &Format,
) -> Result<(), CostingXlsxError> {
    for (col_idx, column) in columns.iter().enumerate() {
        let col_idx = col_idx as u16;
        worksheet
            .write_string_with_format(0, col_idx, column, header_format)
            .map_err(|error| CostingXlsxError::Message(error.to_string()))?;
        if let Some(width) = fixed_width {
            worksheet
                .set_column_width(col_idx, width)
                .map_err(|error| CostingXlsxError::Message(error.to_string()))?;
        }
    }
    Ok(())
}

fn write_data_rows(
    worksheet: &mut Worksheet,
    columns: &[String],
    rows: &[Vec<CellValue>],
    number_formats: &std::collections::BTreeMap<String, String>,
    text_format: &Format,
) -> Result<(), CostingXlsxError> {
    for (row_idx, row) in rows.iter().enumerate() {
        let excel_row = (row_idx + 1) as u32;
        for (col_idx, value) in row.iter().enumerate() {
            let Some(column_name) = columns.get(col_idx) else {
                continue;
            };
            let excel_col = col_idx as u16;
            let number_format = number_formats
                .get(column_name)
                .map(|format| Format::new().set_num_format(format));
            match (value, number_format.as_ref()) {
                (CellValue::Blank, _) => {}
                (CellValue::Decimal(value), Some(format)) => {
                    worksheet
                        .write_number_with_format(
                            excel_row,
                            excel_col,
                            decimal_to_f64(value)?,
                            format,
                        )
                        .map_err(|error| CostingXlsxError::Message(error.to_string()))?;
                }
                (CellValue::Decimal(value), None) => {
                    worksheet
                        .write_number(excel_row, excel_col, decimal_to_f64(value)?)
                        .map_err(|error| CostingXlsxError::Message(error.to_string()))?;
                }
                (CellValue::Text(value) | CellValue::DateLike(value), _) => {
                    worksheet
                        .write_string_with_format(excel_row, excel_col, value, text_format)
                        .map_err(|error| CostingXlsxError::Message(error.to_string()))?;
                }
            }
        }
    }
    Ok(())
}

fn decimal_to_f64(value: &rust_decimal::Decimal) -> Result<f64, CostingXlsxError> {
    value.to_f64().ok_or_else(|| {
        CostingXlsxError::Message(format!("decimal value cannot be written to xlsx: {value}"))
    })
}

fn parse_freeze_panes(token: &str) -> Result<(u32, u16), CostingXlsxError> {
    match token.trim().to_ascii_uppercase().as_str() {
        "A2" => Ok((1, 0)),
        other => Err(CostingXlsxError::Message(format!(
            "unsupported freeze panes token: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::process;
    use std::time::{SystemTime, UNIX_EPOCH};

    use calamine::{open_workbook_auto, Reader};
    use costing_core::model::{
        CellValue, QualityMetric, SheetModel, StageTimings, WorkbookPayload,
    };
    use rust_decimal::Decimal;

    use super::*;

    fn unique_temp_path(stem: &str) -> std::path::PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "costing-writer-{stem}-pid{}-{timestamp}.xlsx",
            process::id()
        ))
    }

    fn sheet(sheet_name: &str) -> SheetModel {
        SheetModel {
            sheet_name: sheet_name.to_string(),
            columns: vec!["月份".to_string(), "本期完工金额".to_string()],
            rows: vec![vec![
                CellValue::Text("2025年01期".to_string()),
                CellValue::Decimal(Decimal::new(125, 1)),
            ]],
            column_types: BTreeMap::from([
                ("月份".to_string(), "text".to_string()),
                ("本期完工金额".to_string(), "text".to_string()),
            ]),
            number_formats: BTreeMap::from([("本期完工金额".to_string(), "#,##0.00".to_string())]),
            freeze_panes: Some("A2".to_string()),
            auto_filter: true,
            fixed_width: Some(15.0),
        }
    }

    fn payload(sheet_models: Vec<SheetModel>) -> WorkbookPayload {
        WorkbookPayload {
            sheet_models,
            quality_metrics: vec![QualityMetric {
                category: "行数勾稽".to_string(),
                metric: "产品数量统计输出行数".to_string(),
                value: "1".to_string(),
                description: "test".to_string(),
            }],
            error_log_count: 0,
            error_log: Vec::new(),
            stage_timings: StageTimings::default(),
        }
    }

    #[test]
    fn writes_three_sheet_workbook() {
        let output = unique_temp_path("three-sheet");
        let payload = payload(vec![
            sheet("成本计算单总表"),
            sheet("成本计算单数量聚合维度"),
            sheet("成本分析工单维度"),
        ]);

        write_workbook(&output, &payload).unwrap();

        let workbook = open_workbook_auto(&output).unwrap();
        assert_eq!(
            workbook.sheet_names(),
            &[
                "成本计算单总表".to_string(),
                "成本计算单数量聚合维度".to_string(),
                "成本分析工单维度".to_string(),
            ]
        );
        let _ = std::fs::remove_file(output);
    }

    #[test]
    fn rejects_product_dimension_sheet() {
        let output = unique_temp_path("product-dimension");
        let payload = payload(vec![sheet("成本分析产品维度")]);

        let error = write_workbook(&output, &payload).unwrap_err();

        assert!(error.to_string().contains("成本分析产品维度"));
        assert!(!output.exists());
    }
}

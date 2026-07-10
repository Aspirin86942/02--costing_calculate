use std::fs::OpenOptions;
use std::path::Path;

use costing_core::model::{CellValue, WorkbookPayload};
use rust_decimal::prelude::ToPrimitive;
use rust_xlsxwriter::{Color, Format, FormatAlign, FormatBorder, Workbook, Worksheet};

use crate::reader::CostingXlsxError;

const DEFAULT_SHEET_NAMES: [&str; 3] = [
    "成本计算单总表",
    "成本计算单数量聚合维度",
    "成本分析工单维度",
];

pub fn write_workbook(path: &Path, payload: &WorkbookPayload) -> Result<(), CostingXlsxError> {
    validate_default_sheet_contract(payload)?;

    let mut workbook = Workbook::new();
    for sheet in &payload.sheet_models {
        let worksheet = workbook.add_worksheet();
        worksheet
            .set_name(&sheet.sheet_name)
            .map_err(|error| CostingXlsxError::Message(error.to_string()))?;

        let header_format = Format::new()
            .set_bold()
            .set_background_color(Color::RGB(0xD9E1F2))
            .set_border(FormatBorder::Thin)
            .set_align(FormatAlign::Center)
            .set_align(FormatAlign::VerticalCenter);
        let text_format = Format::new()
            .set_align(FormatAlign::Left)
            .set_align(FormatAlign::VerticalCenter);

        write_header_row(
            worksheet,
            &sheet.columns,
            &sheet.number_formats,
            sheet.fixed_width,
            &header_format,
            &text_format,
        )?;
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
    // 在真正写出时原子创建目标文件，避免前置 exists 检查与保存之间的并发覆盖竞态。
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                CostingXlsxError::OutputExists(path.to_path_buf())
            } else {
                CostingXlsxError::Message(error.to_string())
            }
        })?;
    if let Err(error) = workbook.save_to_writer(file) {
        let cleanup_error = std::fs::remove_file(path).err();
        let cleanup_detail = cleanup_error
            .map(|cleanup_error| format!("; 清理未完成输出失败: {cleanup_error}"))
            .unwrap_or_default();
        return Err(CostingXlsxError::Message(format!(
            "{error}{cleanup_detail}"
        )));
    }
    Ok(())
}

fn validate_default_sheet_contract(payload: &WorkbookPayload) -> Result<(), CostingXlsxError> {
    let actual = payload
        .sheet_models
        .iter()
        .map(|sheet| sheet.sheet_name.as_str())
        .collect::<Vec<_>>();
    if actual.as_slice() == DEFAULT_SHEET_NAMES.as_slice() {
        return Ok(());
    }

    Err(CostingXlsxError::Message(format!(
        "Rust 默认 workbook 只允许按顺序输出: {}; 实际: {}",
        DEFAULT_SHEET_NAMES.join(", "),
        actual.join(", ")
    )))
}

fn write_header_row(
    worksheet: &mut Worksheet,
    columns: &[String],
    number_formats: &std::collections::BTreeMap<String, String>,
    fixed_width: Option<f64>,
    header_format: &Format,
    text_format: &Format,
) -> Result<(), CostingXlsxError> {
    for (col_idx, column) in columns.iter().enumerate() {
        let col_idx = col_idx as u16;
        worksheet
            .write_string_with_format(0, col_idx, column, header_format)
            .map_err(|error| CostingXlsxError::Message(error.to_string()))?;
        if let Some(width) = fixed_width {
            worksheet
                .set_column_width(col_idx, normalized_column_width(width))
                .map_err(|error| CostingXlsxError::Message(error.to_string()))?;
        }
        let column_format = number_formats
            .get(column)
            .map(|format| numeric_format(format))
            .unwrap_or_else(|| text_format.clone());
        worksheet
            .set_column_format(col_idx, &column_format)
            .map_err(|error| CostingXlsxError::Message(error.to_string()))?;
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
                .map(|format| numeric_format(format));
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

fn numeric_format(number_format: &str) -> Format {
    Format::new()
        .set_num_format(number_format)
        .set_align(FormatAlign::Right)
        .set_align(FormatAlign::VerticalCenter)
}

fn normalized_column_width(width: f64) -> f64 {
    // 与 Python xlsxwriter 的固定 15 列宽换算一致，使 openpyxl/OOXML 语义值保持 15.0。
    if width == 15.0 {
        14.3
    } else {
        width
    }
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
    use std::sync::{Arc, Barrier};
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
    fn refuses_to_replace_existing_output() {
        let output = unique_temp_path("existing-output");
        let original = b"existing workbook bytes";
        std::fs::write(&output, original).unwrap();
        let payload = payload(vec![
            sheet("成本计算单总表"),
            sheet("成本计算单数量聚合维度"),
            sheet("成本分析工单维度"),
        ]);

        let error = write_workbook(&output, &payload).unwrap_err();

        assert!(matches!(
            error,
            CostingXlsxError::OutputExists(ref path) if path == &output
        ));
        assert_eq!(std::fs::read(&output).unwrap(), original);
        let _ = std::fs::remove_file(output);
    }

    #[test]
    fn concurrent_writers_allow_only_one_output() {
        let output = Arc::new(unique_temp_path("concurrent-output"));
        let barrier = Arc::new(Barrier::new(2));
        let handles = (0..2)
            .map(|_| {
                let output = Arc::clone(&output);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let payload = payload(vec![
                        sheet("成本计算单总表"),
                        sheet("成本计算单数量聚合维度"),
                        sheet("成本分析工单维度"),
                    ]);
                    barrier.wait();
                    match write_workbook(&output, &payload) {
                        Ok(()) => "written",
                        Err(CostingXlsxError::OutputExists(_)) => "exists",
                        Err(error) => panic!("unexpected writer error: {error}"),
                    }
                })
            })
            .collect::<Vec<_>>();

        let mut outcomes = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        outcomes.sort_unstable();

        assert_eq!(outcomes, ["exists", "written"]);
        let workbook = open_workbook_auto(output.as_ref()).unwrap();
        assert_eq!(workbook.sheet_names().len(), 3);
        let _ = std::fs::remove_file(output.as_ref());
    }

    #[test]
    fn rejects_product_dimension_sheet() {
        let output = unique_temp_path("product-dimension");
        let payload = payload(vec![sheet("成本分析产品维度")]);

        let error = write_workbook(&output, &payload).unwrap_err();

        assert!(error.to_string().contains("成本分析产品维度"));
        assert!(!output.exists());
    }

    #[test]
    fn rejects_extra_non_default_sheet() {
        let output = unique_temp_path("extra-sheet");
        let payload = payload(vec![
            sheet("成本计算单总表"),
            sheet("成本计算单数量聚合维度"),
            sheet("成本分析工单维度"),
            sheet("调试输出"),
        ]);

        let error = write_workbook(&output, &payload).unwrap_err();

        assert!(error.to_string().contains("默认 workbook"));
        assert!(!output.exists());
    }
}

use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::Parser;
use rust_xlsxwriter::{Format, Workbook};
use serde::Deserialize;

const PRODUCT_DIMENSION_SHEET: &str = "成本分析产品维度";
const DEFAULT_SHEETS: [&str; 3] = [
    "成本计算单总表",
    "成本计算单数量聚合维度",
    "成本分析工单维度",
];

#[derive(Parser)]
struct Args {
    #[arg(long)]
    manifest: PathBuf,
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Deserialize)]
struct Manifest {
    workbook: WorkbookConfig,
    sheets: Vec<SheetManifest>,
}

#[derive(Deserialize)]
struct WorkbookConfig {
    output_path: Option<PathBuf>,
}

#[derive(Deserialize)]
struct SheetManifest {
    sheet_name: String,
    csv_path: PathBuf,
    columns: Vec<String>,
    #[serde(default)]
    column_types: HashMap<String, String>,
    #[serde(default)]
    number_formats: HashMap<String, String>,
    #[serde(default)]
    write_types: HashMap<String, String>,
    freeze_panes: Option<String>,
    #[serde(default)]
    auto_filter: bool,
    fixed_width: Option<f64>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let manifest = read_manifest(&args.manifest)?;
    let output_path = args
        .output
        .or(manifest.workbook.output_path.clone())
        .context("output path is required")?;
    write_workbook(&manifest, &output_path)?;
    Ok(())
}

fn read_manifest(path: &PathBuf) -> Result<Manifest> {
    let file = File::open(path).with_context(|| format!("open manifest: {}", path.display()))?;
    serde_json::from_reader(file).with_context(|| format!("parse manifest: {}", path.display()))
}

fn write_workbook(manifest: &Manifest, output_path: &PathBuf) -> Result<()> {
    let mut workbook = Workbook::new();
    for sheet_manifest in &manifest.sheets {
        validate_sheet_name(&sheet_manifest.sheet_name)?;
        write_sheet(&mut workbook, sheet_manifest)?;
    }

    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output directory: {}", parent.display()))?;
    }
    workbook
        .save(output_path)
        .with_context(|| format!("save workbook: {}", output_path.display()))?;
    Ok(())
}

fn validate_sheet_name(sheet_name: &str) -> Result<()> {
    if sheet_name == PRODUCT_DIMENSION_SHEET {
        bail!("{PRODUCT_DIMENSION_SHEET} must not be written by the Rust sidecar spike");
    }
    if !DEFAULT_SHEETS.contains(&sheet_name) {
        bail!("unsupported sidecar sheet: {sheet_name}");
    }
    Ok(())
}

fn write_sheet(workbook: &mut Workbook, sheet_manifest: &SheetManifest) -> Result<()> {
    let worksheet = workbook.add_worksheet();
    worksheet
        .set_name(&sheet_manifest.sheet_name)
        .with_context(|| format!("set sheet name: {}", sheet_manifest.sheet_name))?;

    let column_formats = build_column_formats(sheet_manifest);
    for (column_index, column_name) in sheet_manifest.columns.iter().enumerate() {
        worksheet.write_string(0, column_index as u16, column_name)?;
        if let Some(width) = sheet_manifest.fixed_width {
            worksheet.set_column_width(column_index as u16, width)?;
        }
    }

    let mut reader = csv::Reader::from_path(&sheet_manifest.csv_path)
        .with_context(|| format!("open csv: {}", sheet_manifest.csv_path.display()))?;
    let csv_headers = reader.headers()?.clone();
    let header_indexes = build_header_indexes(&csv_headers, &sheet_manifest.columns)?;
    let mut row_index = 1_u32;

    for record in reader.records() {
        let record = record?;
        for (column_index, column_name) in sheet_manifest.columns.iter().enumerate() {
            let csv_index = header_indexes[column_index];
            let raw_value = record.get(csv_index).unwrap_or("");
            if raw_value.is_empty() {
                continue;
            }

            let format = column_formats[column_index].as_ref();
            if should_write_number(sheet_manifest, column_name) {
                if is_blank_number_value(raw_value) {
                    continue;
                }
                match raw_value.parse::<f64>() {
                    Ok(number) => {
                        if let Some(format) = format {
                            worksheet.write_number_with_format(
                                row_index,
                                column_index as u16,
                                number,
                                format,
                            )?;
                        } else {
                            worksheet.write_number(row_index, column_index as u16, number)?;
                        }
                    }
                    Err(_) => {
                        bail!(
                            "failed to parse number: sheet={} row={} col={} value={:?}",
                            sheet_manifest.sheet_name,
                            row_index + 1,
                            column_name,
                            raw_value
                        );
                    }
                }
            } else if let Some(format) = format {
                worksheet.write_string_with_format(
                    row_index,
                    column_index as u16,
                    raw_value,
                    format,
                )?;
            } else {
                worksheet.write_string(row_index, column_index as u16, raw_value)?;
            }
        }
        row_index += 1;
    }

    if let Some(freeze_panes) = &sheet_manifest.freeze_panes {
        let (row, col) = parse_freeze_panes(freeze_panes)?;
        worksheet.set_freeze_panes(row, col)?;
    }
    if sheet_manifest.auto_filter && !sheet_manifest.columns.is_empty() {
        let last_row = row_index.saturating_sub(1);
        let last_col = (sheet_manifest.columns.len() - 1) as u16;
        worksheet.autofilter(0, 0, last_row, last_col)?;
    }

    Ok(())
}

fn build_column_formats(sheet_manifest: &SheetManifest) -> Vec<Option<Format>> {
    sheet_manifest
        .columns
        .iter()
        .map(|column_name| {
            sheet_manifest
                .number_formats
                .get(column_name)
                .map(|number_format| Format::new().set_num_format(number_format))
        })
        .collect()
}

fn build_header_indexes(csv_headers: &csv::StringRecord, columns: &[String]) -> Result<Vec<usize>> {
    columns
        .iter()
        .map(|column_name| {
            csv_headers
                .iter()
                .position(|header| header == column_name)
                .with_context(|| format!("csv header missing: {column_name}"))
        })
        .collect()
}

fn should_write_number(sheet_manifest: &SheetManifest, column_name: &str) -> bool {
    match sheet_manifest
        .write_types
        .get(column_name)
        .map(String::as_str)
    {
        Some("number") => return true,
        Some("text") => return false,
        _ => {}
    }
    if sheet_manifest.number_formats.contains_key(column_name) {
        return true;
    }
    !matches!(
        sheet_manifest
            .column_types
            .get(column_name)
            .map(String::as_str),
        None | Some("text") | Some("string") | Some("date") | Some("datetime")
    )
}

fn is_blank_number_value(raw_value: &str) -> bool {
    raw_value.trim() == "-"
}

fn parse_freeze_panes(token: &str) -> Result<(u32, u16)> {
    let trimmed = token.trim().to_ascii_uppercase();
    let mut letters = String::new();
    let mut digits = String::new();
    for character in trimmed.chars() {
        if character.is_ascii_alphabetic() && digits.is_empty() {
            letters.push(character);
        } else if character.is_ascii_digit() {
            digits.push(character);
        } else {
            bail!("invalid freeze panes token: {token}");
        }
    }
    if letters.is_empty() || digits.is_empty() {
        bail!("invalid freeze panes token: {token}");
    }

    let row_number: u32 = digits.parse()?;
    let mut column_number = 0_u32;
    for character in letters.chars() {
        column_number = column_number * 26 + (character as u32 - 'A' as u32 + 1);
    }
    Ok((
        row_number.saturating_sub(1),
        column_number.saturating_sub(1) as u16,
    ))
}

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_xlsxwriter::{ExcelDateTime, Format, Workbook};

#[test]
fn check_only_benchmark_omits_writer_breakdown_and_output_size() {
    let input = unique_temp_path("check-only-input.xlsx");
    write_minimal_input_workbook(&input);

    let output = Command::new(locate_costing_binary())
        .args([
            "gb",
            "--input",
            input.to_str().unwrap(),
            "--check-only",
            "--benchmark",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let payload: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let stages = payload["stage_timings"]["stages"].as_object().unwrap();
    assert!(!payload["request_id"].as_str().unwrap().is_empty());
    assert_eq!(payload["output_written"], false);
    assert!(payload["workbook_path"].is_null());
    assert!(payload["output_size_bytes"].is_null());
    assert_eq!(
        stages.keys().map(String::as_str).collect::<Vec<_>>(),
        [
            "fact",
            "ingest",
            "normalize",
            "presentation",
            "split",
            "total"
        ]
    );
    assert!(stages["total"].as_f64().unwrap().is_finite());
    assert!(stages
        .values()
        .all(|seconds| seconds.as_f64().unwrap().is_finite() && seconds.as_f64().unwrap() >= 0.0));

    let _ = std::fs::remove_file(input);
}

#[test]
fn normal_benchmark_reports_writer_breakdown_and_output_size() {
    let input = unique_temp_path("normal-input.xlsx");
    let workbook = unique_temp_path("normal-output.xlsx");
    write_minimal_input_workbook(&input);

    let output = Command::new(locate_costing_binary())
        .args([
            "gb",
            "--input",
            input.to_str().unwrap(),
            "--output",
            workbook.to_str().unwrap(),
            "--benchmark",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let payload: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let stages = payload["stage_timings"]["stages"].as_object().unwrap();
    assert!(!payload["request_id"].as_str().unwrap().is_empty());
    assert_eq!(payload["output_written"], true);
    assert_eq!(
        payload["output_size_bytes"].as_u64().unwrap(),
        std::fs::metadata(&workbook).unwrap().len()
    );
    assert!(payload["output_size_bytes"].as_u64().unwrap() > 0);
    assert_eq!(
        stages.keys().map(String::as_str).collect::<Vec<_>>(),
        [
            "export",
            "fact",
            "ingest",
            "normalize",
            "presentation",
            "split",
            "total",
            "writer_populate",
            "xlsx_save",
        ]
    );
    assert!(stages["total"].as_f64().unwrap().is_finite());
    assert!(stages["export"].as_f64().unwrap().is_finite());
    assert!(stages["writer_populate"].as_f64().unwrap().is_finite());
    assert!(stages["xlsx_save"].as_f64().unwrap().is_finite());
    assert!(stages
        .values()
        .all(|seconds| seconds.as_f64().unwrap().is_finite() && seconds.as_f64().unwrap() >= 0.0));

    let _ = std::fs::remove_file(input);
    let _ = std::fs::remove_file(workbook);
}

fn locate_costing_binary() -> PathBuf {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_costing_calculate") {
        return PathBuf::from(path);
    }
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_costing-calculate") {
        return PathBuf::from(path);
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut binary = manifest_dir;
    binary.push("../..");
    binary.push("target");
    binary.push("debug");
    binary.push("costing-calculate");
    if cfg!(windows) {
        binary.set_extension("exe");
    }
    binary
}

fn unique_temp_path(suffix: &str) -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "costing-cli-pid{}-{now}-{suffix}",
        std::process::id()
    ))
}

fn write_minimal_input_workbook(path: &Path) {
    let mut workbook = Workbook::new();
    let sheet = workbook.add_worksheet();
    sheet.set_name("成本计算单").unwrap();
    sheet.write_string(0, 0, "年期").unwrap();
    sheet.write_string(0, 1, "产品编码").unwrap();
    sheet.write_string(0, 2, "产品名称").unwrap();
    sheet.write_string(0, 3, "工单编号").unwrap();
    sheet.write_string(0, 4, "工单行号").unwrap();
    sheet.write_string(0, 5, "本期完工数量").unwrap();
    sheet.write_string(0, 6, "本期完工金额").unwrap();
    sheet.write_string(0, 7, "成本项目名称").unwrap();
    sheet.write_string(0, 8, "日期").unwrap();
    for column in 0..=8 {
        sheet.write_string(1, column, "").unwrap();
    }
    sheet.write_string(2, 0, "2025年01期").unwrap();
    sheet.write_string(2, 1, "P1").unwrap();
    sheet.write_string(2, 2, "产品").unwrap();
    sheet.write_string(2, 3, "WO-1").unwrap();
    sheet.write_string(2, 4, "1").unwrap();
    sheet.write_number(2, 5, 1).unwrap();
    sheet.write_number(2, 6, 10).unwrap();
    sheet.write_string(2, 7, "").unwrap();
    let date_format = Format::new().set_num_format("yyyy-mm-dd");
    sheet
        .write_datetime_with_format(
            2,
            8,
            ExcelDateTime::from_ymd(2025, 1, 2).unwrap(),
            &date_format,
        )
        .unwrap();
    workbook.save(path).unwrap();
}

use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_xlsxwriter::{ExcelDateTime, Format, Workbook};
use serde_json::Value;

#[test]
fn cli_failure_json_has_stable_fields() {
    let binary_path = locate_costing_binary();
    let output = Command::new(&binary_path)
        .args([
            "gb",
            "--input",
            "does-not-exist-costing-cli-test.xlsx",
            "--check-only",
        ])
        .output()
        .expect("run costing-calculate binary");
    assert!(!output.status.success());

    assert_error_json(&output.stderr, "FILE_NOT_FOUND");
}

#[test]
fn missing_arguments_use_stable_json_error_model() {
    let output = Command::new(locate_costing_binary())
        .output()
        .expect("run costing-calculate binary");

    assert!(!output.status.success());
    let payload = error_json(&output.stderr);
    assert_eq!(payload["code"], "INVALID_INPUT");
    assert!(payload["request_id"].is_null());
    assert!(payload["details"].is_null());
}

#[test]
fn accepted_arguments_fail_with_request_id_and_closed_stage() {
    let binary_path = locate_costing_binary();
    let output = Command::new(&binary_path)
        .args([
            "gb",
            "--input",
            "does-not-exist-contextual-costing-cli-test.xlsx",
            "--check-only",
        ])
        .output()
        .expect("run costing-calculate binary");

    assert!(!output.status.success());
    let payload = error_json(&output.stderr);
    assert_eq!(payload["code"], "FILE_NOT_FOUND");
    assert!(payload["request_id"]
        .as_str()
        .is_some_and(|request_id| request_id.starts_with("costing-")));
    assert_eq!(payload["details"]["stage"], "ValidateCliRequest");
    assert_eq!(
        payload["details"]["path"],
        "does-not-exist-contextual-costing-cli-test.xlsx"
    );
    assert!(payload["details"]["io_kind"].is_null());
    assert!(payload["details"]["raw_os_error"].is_null());
}

#[test]
fn existing_output_reports_the_failing_output_path() {
    let root = unique_temp_dir("existing-output-context");
    std::fs::create_dir_all(&root).unwrap();
    let input = root.join("input.xlsx");
    let output_path = root.join("already-exists.xlsx");
    write_minimal_input_workbook(&input);
    let original_output = b"existing output";
    std::fs::write(&output_path, original_output).unwrap();

    let output = Command::new(locate_costing_binary())
        .args([
            "gb",
            "--input",
            input.to_str().unwrap(),
            "--output",
            output_path.to_str().unwrap(),
        ])
        .output()
        .expect("run costing-calculate binary");

    assert!(!output.status.success());
    let payload = error_json(&output.stderr);
    assert_eq!(payload["code"], "OUTPUT_EXISTS");
    assert_eq!(payload["details"]["stage"], "CreateFinalOutput");
    assert_eq!(
        payload["details"]["path"],
        output_path.display().to_string()
    );
    assert_eq!(payload["details"]["io_kind"], "AlreadyExists");
    assert!(payload["details"]["raw_os_error"].is_number());
    assert_eq!(std::fs::read(&output_path).unwrap(), original_output);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn unknown_pipeline_uses_stable_json_error_model() {
    let output = Command::new(locate_costing_binary())
        .args(["unknown", "--input", "input.xlsx", "--check-only"])
        .output()
        .expect("run costing-calculate binary");

    assert!(!output.status.success());
    assert_error_json(&output.stderr, "INVALID_INPUT");
}

#[test]
fn help_describes_automatic_input_and_output_paths() {
    let output = Command::new(locate_costing_binary())
        .arg("--help")
        .output()
        .expect("run costing-calculate binary");

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("data/raw/<pipeline>"));
    assert!(stdout.contains("data/processed/<pipeline>"));
}

#[test]
fn damaged_xlsx_is_classified_as_file_not_readable() {
    let input =
        std::env::temp_dir().join(format!("costing-cli-damaged-{}.xlsx", std::process::id()));
    std::fs::write(&input, b"not an xlsx archive").unwrap();

    let output = Command::new(locate_costing_binary())
        .args(["gb", "--input", input.to_str().unwrap(), "--check-only"])
        .output()
        .expect("run costing-calculate binary");

    let _ = std::fs::remove_file(input);
    assert!(!output.status.success());
    assert_error_json(&output.stderr, "FILE_NOT_READABLE");
}

#[test]
fn omitted_input_uses_the_only_pipeline_workbook() {
    let root = unique_temp_dir("auto-input");
    let raw_dir = root.join("data/raw/gb");
    std::fs::create_dir_all(&raw_dir).unwrap();
    std::fs::write(raw_dir.join("gb-sample.xlsx"), b"not an xlsx archive").unwrap();

    let output = Command::new(locate_costing_binary())
        .args(["gb", "--check-only"])
        .current_dir(&root)
        .output()
        .expect("run costing-calculate binary");

    let _ = std::fs::remove_dir_all(root);
    assert!(!output.status.success());
    assert_error_json(&output.stderr, "FILE_NOT_READABLE");
}

#[test]
fn omitted_input_reports_when_no_pipeline_workbook_exists() {
    let root = unique_temp_dir("auto-input-empty");
    std::fs::create_dir_all(root.join("data/raw/gb")).unwrap();

    let output = Command::new(locate_costing_binary())
        .args(["gb", "--check-only"])
        .current_dir(&root)
        .output()
        .expect("run costing-calculate binary");

    let _ = std::fs::remove_dir_all(root);
    assert!(!output.status.success());
    assert_error_json(&output.stderr, "FILE_NOT_FOUND");
}

#[test]
fn omitted_input_rejects_multiple_pipeline_workbooks() {
    let root = unique_temp_dir("auto-input-multiple");
    let raw_dir = root.join("data/raw/gb");
    std::fs::create_dir_all(&raw_dir).unwrap();
    std::fs::write(raw_dir.join("GB-first.xlsx"), b"first").unwrap();
    std::fs::write(raw_dir.join("gb-second.xlsx"), b"second").unwrap();

    let output = Command::new(locate_costing_binary())
        .args(["gb", "--check-only"])
        .current_dir(&root)
        .output()
        .expect("run costing-calculate binary");

    let _ = std::fs::remove_dir_all(root);
    assert!(!output.status.success());
    let payload = error_json(&output.stderr);
    assert_eq!(payload["code"], "INVALID_INPUT");
    assert!(payload["message"].as_str().unwrap().contains("检测到多个"));
    assert!(payload["message"].as_str().unwrap().contains("--input"));
}

fn assert_error_json(stderr: &[u8], expected_code: &str) {
    let payload = error_json(stderr);
    assert_eq!(payload["status"], "failed");
    assert_eq!(payload["code"], expected_code);
    assert!(payload["message"].is_string());
    assert!(payload["retryable"].is_boolean());
}

fn error_json(stderr: &[u8]) -> Value {
    serde_json::from_slice(stderr).expect("stderr must contain only JSON")
}

fn unique_temp_dir(suffix: &str) -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "costing-cli-{suffix}-pid{}-{now}",
        std::process::id()
    ))
}

fn write_minimal_input_workbook(path: &std::path::Path) {
    let mut workbook = Workbook::new();
    let sheet = workbook.add_worksheet();
    sheet.set_name("成本计算单").unwrap();
    for (column, header) in [
        "年期",
        "产品编码",
        "产品名称",
        "工单编号",
        "工单行号",
        "本期完工数量",
        "本期完工金额",
        "成本项目名称",
        "日期",
    ]
    .into_iter()
    .enumerate()
    {
        sheet.write_string(0, column as u16, header).unwrap();
        sheet.write_string(1, column as u16, "").unwrap();
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

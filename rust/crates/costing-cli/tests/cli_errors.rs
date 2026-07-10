use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

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
    assert_error_json(&output.stderr, "INVALID_INPUT");
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

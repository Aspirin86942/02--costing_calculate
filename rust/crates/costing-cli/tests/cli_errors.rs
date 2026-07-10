use std::path::PathBuf;
use std::process::Command;

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

fn assert_error_json(stderr: &[u8], expected_code: &str) {
    let payload: Value = serde_json::from_slice(stderr).expect("stderr must contain only JSON");
    assert_eq!(payload["status"], "failed");
    assert_eq!(payload["code"], expected_code);
    assert!(payload["message"].is_string());
    assert!(payload["retryable"].is_boolean());
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

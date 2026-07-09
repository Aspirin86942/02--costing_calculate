use std::path::PathBuf;
use std::process::Command;

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

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("\"status\""));
    assert!(stderr.contains("\"code\""));
    assert!(stderr.contains("\"message\""));
    assert!(stderr.contains("\"retryable\""));
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

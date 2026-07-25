use std::process::Command;

#[test]
fn version_json_reports_deterministic_build_identity_without_a_pipeline() {
    let output = Command::new(env!("CARGO_BIN_EXE_costing-calculate"))
        .arg("--version-json")
        .output()
        .expect("run costing-calculate --version-json");

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse version JSON");
    assert_eq!(value["name"], "costing-calculate");
    assert_eq!(value["version"], "0.2.0");
    assert_eq!(value["config_schema_version"], 1);
    assert_eq!(value["run_manifest_schema_version"], 1);
    assert!(value["target"]
        .as_str()
        .is_some_and(|value| value.contains('-')));
    assert!(value["git_commit"].as_str().is_some_and(
        |value| value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
    ));
    assert!(value["build_timestamp"]
        .as_str()
        .is_some_and(|value| value.ends_with('Z')));
    assert!(value["rustc_version"]
        .as_str()
        .is_some_and(|value| value.starts_with("rustc ")));
}

#[test]
fn standard_version_flag_reports_v020() {
    let output = Command::new(env!("CARGO_BIN_EXE_costing-calculate"))
        .arg("--version")
        .output()
        .expect("run costing-calculate --version");

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_eq!(
        String::from_utf8(output.stdout).expect("version output is UTF-8"),
        "costing-calculate 0.2.0\n"
    );
}

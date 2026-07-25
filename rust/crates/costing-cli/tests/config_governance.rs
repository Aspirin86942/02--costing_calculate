use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

use calamine::{open_workbook_auto, Data, Range, Reader};
use rust_xlsxwriter::Workbook;

const DEFAULT_CONFIG: &str = include_str!("../config/costing.default.toml");
const DEFAULT_GOLDEN: &str = include_str!("../config/costing.default.golden.json");

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_costing-calculate")
}

fn run(cwd: &Path, arguments: &[&str]) -> Output {
    Command::new(binary())
        .args(arguments)
        .current_dir(cwd)
        .output()
        .expect("run costing-calculate")
}

fn json_stdout(output: &Output) -> serde_json::Value {
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    serde_json::from_slice(&output.stdout).expect("parse stdout JSON")
}

fn json_stderr(output: &Output) -> serde_json::Value {
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    serde_json::from_slice(&output.stderr).expect("parse stderr JSON")
}

fn temp_root(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "costing-config-{label}-{}-{nanos}",
        std::process::id()
    ));
    fs::create_dir_all(&root).expect("create temporary test root");
    root
}

fn write_config(root: &Path, name: &str, contents: &str) -> PathBuf {
    let path = root.join(name);
    fs::write(&path, contents).expect("write test config");
    path
}

fn full_sha(value: &serde_json::Value) -> &str {
    let value = value.as_str().expect("SHA-256 is text");
    assert_eq!(value.len(), 64);
    assert!(value.bytes().all(|byte| byte.is_ascii_hexdigit()));
    value
}

fn write_minimal_input(path: &Path) {
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
    ]
    .into_iter()
    .enumerate()
    {
        sheet.write_string(0, column as u16, header).unwrap();
        sheet.write_string(1, column as u16, "").unwrap();
    }
    for (column, value) in ["2026年1期", "P1", "产品", "WO-1", "1"]
        .into_iter()
        .enumerate()
    {
        sheet.write_string(2, column as u16, value).unwrap();
    }
    sheet.write_number(2, 5, 1).unwrap();
    sheet.write_number(2, 6, 10).unwrap();
    sheet.write_string(2, 7, "").unwrap();
    workbook.save(path).unwrap();
}

fn workbook_content(path: &Path) -> Vec<(String, Range<Data>)> {
    let mut workbook = open_workbook_auto(path).expect("open generated workbook");
    workbook
        .sheet_names()
        .to_vec()
        .into_iter()
        .map(|name| {
            let range = workbook
                .worksheet_range(&name)
                .expect("read generated worksheet");
            (name, range)
        })
        .collect()
}

#[test]
fn validate_config_does_not_require_or_read_a_workbook() {
    let root = temp_root("validate-default");

    let value = json_stdout(&run(&root, &["gb", "--validate-config"]));

    assert_eq!(value["status"], "valid");
    assert_eq!(value["pipeline"], "gb");
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["source"], "embedded-default");
    assert!(value["source_sha256"].is_null());
    full_sha(&value["effective_sha256"]);
    assert!(!root.join("data").exists());
    fs::remove_dir_all(root).expect("remove temporary test root");
}

#[test]
fn embedded_effective_hashes_match_the_reviewed_golden() {
    let root = temp_root("golden");
    let golden: serde_json::Value = serde_json::from_str(DEFAULT_GOLDEN).unwrap();

    for pipeline in ["gb", "sk"] {
        let value = json_stdout(&run(&root, &[pipeline, "--validate-config"]));
        assert_eq!(value["schema_version"], golden["schema_version"]);
        assert_eq!(
            value["effective_sha256"],
            golden["effective_sha256"][pipeline]
        );
    }
    fs::remove_dir_all(root).expect("remove temporary test root");
}

#[test]
fn print_effective_config_marks_external_and_sealed_fields() {
    let root = temp_root("print-external");
    let config_path = write_config(&root, "costing.toml", DEFAULT_CONFIG);

    let value = json_stdout(&run(
        &root,
        &[
            "sk",
            "--config",
            config_path.to_str().expect("UTF-8 config path"),
            "--print-effective-config",
        ],
    ));

    assert_eq!(value["status"], "valid");
    assert_eq!(value["pipeline"], "sk");
    assert_eq!(value["source"], "external");
    full_sha(&value["effective_sha256"]);
    full_sha(&value["source_sha256"]);
    assert_eq!(value["config"]["input_pattern"]["source"], "external");
    assert_eq!(value["config"]["product_order"]["source"], "external");
    assert_eq!(value["config"]["standalone_cost_items"]["source"], "sealed");
    assert_eq!(
        value["config"]["standalone_cost_items"]["value"],
        serde_json::json!(["委外加工费", "软件费用"])
    );
    assert!(value
        .to_string()
        .find(config_path.to_str().unwrap())
        .is_none());
    fs::remove_dir_all(root).expect("remove temporary test root");
}

#[test]
fn version_json_cannot_bypass_a_requested_config_diagnostic() {
    let root = temp_root("version-conflict");
    let missing = root.join("missing.toml");

    let output = run(
        &root,
        &[
            "gb",
            "--config",
            missing.to_str().unwrap(),
            "--validate-config",
            "--version-json",
        ],
    );

    assert!(!output.status.success());
    let error: serde_json::Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["code"], "INVALID_INPUT");
    assert!(error["message"]
        .as_str()
        .unwrap()
        .contains("--version-json"));
    assert!(error["message"]
        .as_str()
        .unwrap()
        .contains("--validate-config"));
    fs::remove_dir_all(root).expect("remove temporary test root");
}

#[test]
fn equivalent_external_config_has_the_embedded_semantic_hash() {
    let root = temp_root("equivalent-hash");
    let reordered = DEFAULT_CONFIG
        .replacen(
            "input_pattern = \"gb-*.xlsx\"\nstandalone_cost_items = [\"委外加工费\"]",
            "# Equivalent comment and key order\nstandalone_cost_items = [\"委外加工费\"]\ninput_pattern = \"gb-*.xlsx\"",
            1,
        )
        .replacen(
            "input_pattern = \"sk-*.xlsx\"\nstandalone_cost_items = [\"委外加工费\", \"软件费用\"]",
            "standalone_cost_items = [\"委外加工费\", \"软件费用\"]\ninput_pattern = \"sk-*.xlsx\"",
            1,
        );
    let config_path = write_config(&root, "equivalent.toml", &reordered);

    let embedded = json_stdout(&run(&root, &["gb", "--print-effective-config"]));
    let external = json_stdout(&run(
        &root,
        &[
            "gb",
            "--config",
            config_path.to_str().unwrap(),
            "--print-effective-config",
        ],
    ));

    assert_eq!(embedded["effective_sha256"], external["effective_sha256"]);
    assert!(embedded["source_sha256"].is_null());
    full_sha(&external["source_sha256"]);
    fs::remove_dir_all(root).expect("remove temporary test root");
}

#[test]
fn unknown_fields_fail_during_config_parse_before_input_discovery() {
    let root = temp_root("unknown-field");
    let config_path = write_config(
        &root,
        "unknown.toml",
        &format!("{DEFAULT_CONFIG}\nunexpected_field = true\n"),
    );

    let error = json_stderr(&run(
        &root,
        &["gb", "--config", config_path.to_str().unwrap()],
    ));

    assert_eq!(error["code"], "INVALID_CONFIG");
    assert_eq!(error["details"]["stage"], "ParseConfig");
    assert_eq!(error["details"]["path"], "unknown.toml");
    assert!(!error.to_string().contains(root.to_str().unwrap()));
    assert!(error["message"]
        .as_str()
        .unwrap()
        .contains("unexpected_field"));
    fs::remove_dir_all(root).expect("remove temporary test root");
}

#[test]
fn non_utf8_and_incomplete_external_configs_fail_closed() {
    let root = temp_root("decode");
    let non_utf8_path = root.join("non-utf8.toml");
    fs::write(&non_utf8_path, [0xff, 0xfe]).unwrap();
    let incomplete_path = write_config(
        &root,
        "incomplete.toml",
        "schema_version = 1\n[pipelines.gb]\ninput_pattern = \"gb-*.xlsx\"\n",
    );

    let non_utf8 = json_stderr(&run(
        &root,
        &[
            "gb",
            "--config",
            non_utf8_path.to_str().unwrap(),
            "--validate-config",
        ],
    ));
    assert_eq!(non_utf8["code"], "INVALID_CONFIG");
    assert_eq!(non_utf8["details"]["stage"], "LoadConfig");

    let incomplete = json_stderr(&run(
        &root,
        &[
            "gb",
            "--config",
            incomplete_path.to_str().unwrap(),
            "--validate-config",
        ],
    ));
    assert_eq!(incomplete["code"], "INVALID_CONFIG");
    assert_eq!(incomplete["details"]["stage"], "ParseConfig");
    assert!(incomplete["message"]
        .as_str()
        .unwrap()
        .contains("pipelines"));
    fs::remove_dir_all(root).expect("remove temporary test root");
}

#[test]
fn sealed_cost_items_and_unsafe_input_patterns_fail_validation() {
    let root = temp_root("sealed");
    let sealed_path = write_config(
        &root,
        "sealed.toml",
        &DEFAULT_CONFIG.replacen(
            "standalone_cost_items = [\"委外加工费\"]",
            "standalone_cost_items = [\"软件费用\"]",
            1,
        ),
    );
    let unsafe_path = write_config(
        &root,
        "unsafe.toml",
        &DEFAULT_CONFIG.replacen(
            "input_pattern = \"gb-*.xlsx\"",
            "input_pattern = \"../*.xlsx\"",
            1,
        ),
    );

    for (path, expected_path) in [
        (&sealed_path, "pipelines.gb.standalone_cost_items"),
        (&unsafe_path, "pipelines.gb.input_pattern"),
    ] {
        let error = json_stderr(&run(
            &root,
            &[
                "gb",
                "--config",
                path.to_str().unwrap(),
                "--validate-config",
            ],
        ));
        assert_eq!(error["code"], "INVALID_CONFIG");
        assert_eq!(error["details"]["stage"], "ValidateConfig");
        assert!(error["message"].as_str().unwrap().contains(expected_path));
    }
    fs::remove_dir_all(root).expect("remove temporary test root");
}

#[test]
fn equivalent_external_config_produces_identical_workbook_content() {
    let root = temp_root("equivalent-workbook");
    let input_path = root.join("gb-input.xlsx");
    let embedded_output = root.join("embedded.xlsx");
    let external_output = root.join("external.xlsx");
    let config_path = write_config(&root, "costing.toml", DEFAULT_CONFIG);
    write_minimal_input(&input_path);

    let common = ["gb", "--input", input_path.to_str().unwrap(), "--output"];
    let embedded = run(
        &root,
        &[
            common[0],
            common[1],
            common[2],
            common[3],
            embedded_output.to_str().unwrap(),
        ],
    );
    json_stdout(&embedded);
    let external = run(
        &root,
        &[
            common[0],
            common[1],
            common[2],
            common[3],
            external_output.to_str().unwrap(),
            "--config",
            config_path.to_str().unwrap(),
        ],
    );
    json_stdout(&external);

    assert_eq!(
        workbook_content(&embedded_output),
        workbook_content(&external_output)
    );
    fs::remove_dir_all(root).expect("remove temporary test root");
}

#[test]
fn external_input_pattern_controls_default_discovery() {
    let root = temp_root("input-pattern");
    let raw_dir = root.join("data/raw/gb");
    fs::create_dir_all(&raw_dir).unwrap();
    write_minimal_input(&raw_dir.join("gb-approved-2026.xlsx"));
    fs::write(raw_dir.join("gb-ignored.xlsx"), b"not a workbook").unwrap();
    let config = DEFAULT_CONFIG.replacen(
        "input_pattern = \"gb-*.xlsx\"",
        "input_pattern = \"gb-approved-*.xlsx\"",
        1,
    );
    let config_path = write_config(&root, "pattern.toml", &config);

    let value = json_stdout(&run(
        &root,
        &[
            "gb",
            "--config",
            config_path.to_str().unwrap(),
            "--check-only",
        ],
    ));

    assert_eq!(value["pipeline"], "gb");
    assert_eq!(value["status"], "succeeded");
    fs::remove_dir_all(root).expect("remove temporary test root");
}

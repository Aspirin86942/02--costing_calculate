use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{self, Command};
use std::time::{SystemTime, UNIX_EPOCH};

use rust_xlsxwriter::Workbook;
use serde_json::Value;
use sha2::{Digest, Sha256};

#[test]
fn check_only_summary_is_atomic_and_keeps_stdout_run_summary_compatible() {
    let root = unique_root("check-only");
    std::fs::create_dir(&root).unwrap();
    let input = root.join("input.xlsx");
    let summary = root.join("summary.json");
    write_minimal_input(&input);

    let output = Command::new(locate_costing_binary())
        .current_dir(&root)
        .args([
            "gb",
            "--input",
            input.to_str().unwrap(),
            "--check-only",
            "--summary-output",
            summary.to_str().unwrap(),
        ])
        .output()
        .expect("run costing-calculate");

    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        stdout
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "error_log_count".to_string(),
            "issue_type_counts".to_string(),
            "output_size_bytes".to_string(),
            "output_written".to_string(),
            "pipeline".to_string(),
            "quality_metrics".to_string(),
            "request_id".to_string(),
            "run_counts".to_string(),
            "sheet_count".to_string(),
            "stage_timings".to_string(),
            "status".to_string(),
            "workbook_path".to_string(),
        ])
    );
    assert_eq!(stdout["output_written"], false);

    let manifest: Value = serde_json::from_slice(&std::fs::read(&summary).unwrap()).unwrap();
    assert_eq!(manifest["schema_version"], 1);
    assert_eq!(manifest["status"], "succeeded");
    assert_eq!(manifest["request_id"], stdout["request_id"]);
    assert_eq!(manifest["application"]["name"], "costing-calculate");
    assert_eq!(manifest["application"]["version"], "0.2.0");
    assert_eq!(manifest["execution"]["pipeline"], "gb");
    assert_eq!(manifest["execution"]["mode"], "check-only");
    assert_eq!(manifest["execution"]["low_memory_writer"], false);
    assert_eq!(manifest["input"]["file_name"], "input.xlsx");
    assert_eq!(manifest["input"]["sha256"], sha256_file(&input));
    assert_eq!(manifest["input"]["selected_sheet"], "成本计算单");
    assert_eq!(manifest["config"]["schema_version"], 1);
    assert!(manifest["config"]["effective_sha256"]
        .as_str()
        .is_some_and(|hash| hash.len() == 64));
    assert_eq!(manifest["result"]["output_written"], false);
    assert!(manifest["result"]["workbook_path"].is_null());
    assert!(manifest["result"]["output_sha256"].is_null());
    assert_eq!(
        manifest["quality"]["error_log_count"],
        stdout["error_log_count"]
    );
    assert_eq!(manifest["run_counts"], stdout["run_counts"]);
    assert_eq!(manifest["stage_timings"], stdout["stage_timings"]);
    assert_eq!(manifest["warnings"], serde_json::json!([]));
    assert_no_publish_temps(&root);

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn existing_summary_is_rejected_before_a_damaged_workbook_is_read() {
    let root = unique_root("existing-summary");
    std::fs::create_dir(&root).unwrap();
    let input = root.join("damaged.xlsx");
    let workbook_output = root.join("new-output.xlsx");
    let summary = root.join("summary.json");
    std::fs::write(&input, b"not an xlsx workbook").unwrap();
    std::fs::write(&summary, b"competitor summary").unwrap();
    let original_sha256 = sha256_file(&summary);

    let output = Command::new(locate_costing_binary())
        .current_dir(&root)
        .args([
            "gb",
            "--input",
            input.to_str().unwrap(),
            "--output",
            workbook_output.to_str().unwrap(),
            "--summary-output",
            summary.to_str().unwrap(),
        ])
        .output()
        .expect("run costing-calculate");

    assert!(!output.status.success());
    let failure: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(failure["code"], "OUTPUT_EXISTS");
    assert_eq!(failure["details"]["stage"], "CheckSummaryOutput");
    assert_eq!(sha256_file(&summary), original_sha256);
    assert!(!workbook_output.exists());
    assert_no_publish_temps(&root);

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn damaged_workbook_writes_a_failure_manifest_without_business_rows() {
    let root = unique_root("failure");
    std::fs::create_dir(&root).unwrap();
    let input = root.join("damaged.xlsx");
    let summary = root.join("failure.json");
    std::fs::write(&input, b"not an xlsx workbook").unwrap();

    let output = Command::new(locate_costing_binary())
        .current_dir(&root)
        .args([
            "gb",
            "--input",
            input.to_str().unwrap(),
            "--check-only",
            "--summary-output",
            summary.to_str().unwrap(),
        ])
        .output()
        .expect("run costing-calculate");

    assert!(!output.status.success());
    let failure: Value = serde_json::from_slice(&output.stderr).unwrap();
    let manifest: Value = serde_json::from_slice(&std::fs::read(&summary).unwrap()).unwrap();
    assert_eq!(manifest["schema_version"], 1);
    assert_eq!(manifest["status"], "failed");
    assert_eq!(manifest["request_id"], failure["request_id"]);
    assert_eq!(manifest["application"]["name"], "costing-calculate");
    assert_eq!(manifest["execution"]["pipeline"], "gb");
    assert_eq!(manifest["execution"]["mode"], "check-only");
    assert_eq!(manifest["code"], "FILE_NOT_READABLE");
    assert_eq!(manifest["stage"], "IngestWorkbook");
    assert_eq!(manifest["input"]["file_name"], "damaged.xlsx");
    assert_eq!(manifest["config"]["schema_version"], 1);
    assert_eq!(manifest["final_output_valid"], false);
    assert!(manifest.get("business_rows").is_none());
    assert_no_publish_temps(&root);

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn normal_run_manifest_hashes_only_the_published_workbook() {
    let root = unique_root("normal");
    std::fs::create_dir(&root).unwrap();
    let input = root.join("input.xlsx");
    let workbook_output = root.join("output.xlsx");
    let summary = root.join("summary.json");
    write_minimal_input(&input);

    let output = Command::new(locate_costing_binary())
        .current_dir(&root)
        .args([
            "sk",
            "--input",
            input.to_str().unwrap(),
            "--output",
            workbook_output.to_str().unwrap(),
            "--summary-output",
            summary.to_str().unwrap(),
        ])
        .output()
        .expect("run costing-calculate");

    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
    let manifest: Value = serde_json::from_slice(&std::fs::read(&summary).unwrap()).unwrap();
    assert_eq!(manifest["status"], "succeeded");
    assert_eq!(manifest["result"]["output_written"], true);
    assert_eq!(manifest["result"]["final_output_valid"], true);
    assert_eq!(
        manifest["result"]["output_size_bytes"],
        std::fs::metadata(&workbook_output).unwrap().len()
    );
    assert_eq!(
        manifest["result"]["output_sha256"],
        sha256_file(&workbook_output)
    );
    assert_eq!(manifest["input"]["sha256"], sha256_file(&input));
    assert_eq!(manifest["result"]["sheet_count"], 3);
    assert_eq!(
        manifest["result"]["sheet_names"],
        serde_json::json!([
            "成本计算单总表",
            "成本计算单数量聚合维度",
            "成本分析工单维度"
        ])
    );
    assert_eq!(
        stdout["workbook_path"],
        workbook_output.display().to_string()
    );
    assert_eq!(&std::fs::read(&workbook_output).unwrap()[..2], b"PK");
    assert_no_publish_temps(&root);

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn redacted_manifest_uses_relative_paths_inside_cwd_and_basename_outside() {
    let root = unique_root("redaction");
    let external_root = unique_root("redaction-external");
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(&external_root).unwrap();
    let input = root.join("input.xlsx");
    let config = root.join("costing.toml");
    let workbook_output = external_root.join("outside-output.xlsx");
    let summary = root.join("summary.json");
    write_minimal_input(&input);
    std::fs::write(
        &config,
        include_str!("../config/costing.default.toml").as_bytes(),
    )
    .unwrap();

    let output = Command::new(locate_costing_binary())
        .current_dir(&root)
        .args([
            "gb",
            "--input",
            input.to_str().unwrap(),
            "--output",
            workbook_output.to_str().unwrap(),
            "--config",
            config.to_str().unwrap(),
            "--summary-output",
            summary.to_str().unwrap(),
            "--redact-paths",
        ])
        .output()
        .expect("run costing-calculate");

    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
    let bytes = std::fs::read(&summary).unwrap();
    let text = String::from_utf8(bytes.clone()).unwrap();
    let manifest: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(manifest["input"]["path"], "input.xlsx");
    assert_eq!(manifest["config"]["path"], "costing.toml");
    assert_eq!(manifest["result"]["workbook_path"], "outside-output.xlsx");
    assert_eq!(stdout["workbook_path"], "outside-output.xlsx");
    assert_eq!(manifest["input"]["sha256"], sha256_file(&input));
    assert_eq!(
        manifest["result"]["output_sha256"],
        sha256_file(&workbook_output)
    );
    assert!(!text.contains(&root.display().to_string()));
    assert!(!text.contains(&external_root.display().to_string()));
    assert_no_publish_temps(&root);
    assert_no_publish_temps(&external_root);

    std::fs::remove_dir_all(root).unwrap();
    std::fs::remove_dir_all(external_root).unwrap();
}

#[test]
fn redacted_failure_manifest_and_stderr_do_not_leak_external_parent_paths() {
    let root = unique_root("redacted-failure");
    let external_root = unique_root("private-user-directory");
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(&external_root).unwrap();
    let missing_input = external_root.join("missing.xlsx");
    let summary = root.join("failure.json");

    let output = Command::new(locate_costing_binary())
        .current_dir(&root)
        .args([
            "gb",
            "--input",
            missing_input.to_str().unwrap(),
            "--check-only",
            "--summary-output",
            summary.to_str().unwrap(),
            "--redact-paths",
        ])
        .output()
        .expect("run costing-calculate");

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    let manifest_text = std::fs::read_to_string(&summary).unwrap();
    let failure: Value = serde_json::from_str(&stderr).unwrap();
    let manifest: Value = serde_json::from_str(&manifest_text).unwrap();
    assert_eq!(failure["details"]["path"], "missing.xlsx");
    assert_eq!(manifest["input"]["path"], "missing.xlsx");
    assert!(!stderr.contains(&external_root.display().to_string()));
    assert!(!manifest_text.contains(&external_root.display().to_string()));

    std::fs::remove_dir_all(root).unwrap();
    std::fs::remove_dir_all(external_root).unwrap();
}

fn sha256_file(path: &Path) -> String {
    let bytes = std::fs::read(path).unwrap();
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn assert_no_publish_temps(root: &Path) {
    let names = std::fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(".costing-publish-"))
        .collect::<Vec<_>>();
    assert_eq!(names, Vec::<String>::new());
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
    sheet.write_string(2, 0, "2025年01期").unwrap();
    sheet.write_string(2, 1, "P1").unwrap();
    sheet.write_string(2, 2, "产品").unwrap();
    sheet.write_string(2, 3, "WO-1").unwrap();
    sheet.write_string(2, 4, "1").unwrap();
    sheet.write_number(2, 5, 1).unwrap();
    sheet.write_number(2, 6, 10).unwrap();
    sheet.write_string(2, 7, "").unwrap();
    workbook.save(path).unwrap();
}

fn unique_root(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("costing-manifest-{name}-{}-{nanos}", process::id()))
}

fn locate_costing_binary() -> PathBuf {
    for key in [
        "CARGO_BIN_EXE_costing_calculate",
        "CARGO_BIN_EXE_costing-calculate",
    ] {
        if let Ok(path) = std::env::var(key) {
            return PathBuf::from(path);
        }
    }
    let mut binary = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    binary.push("../..");
    binary.push("target");
    binary.push("debug");
    binary.push("costing-calculate");
    if cfg!(windows) {
        binary.set_extension("exe");
    }
    binary
}

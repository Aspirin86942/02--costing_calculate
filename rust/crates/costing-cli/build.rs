use std::env;
use std::path::Path;
use std::process::Command;

use chrono::{DateTime, SecondsFormat, Utc};

fn command_output(program: &str, current_dir: &Path, arguments: &[&str]) -> Option<String> {
    let output = Command::new(program)
        .current_dir(current_dir)
        .args(arguments)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|value| !value.is_empty())
}

fn source_epoch(manifest_dir: &Path) -> i64 {
    match env::var("SOURCE_DATE_EPOCH") {
        Ok(value) => value
            .parse::<i64>()
            .expect("SOURCE_DATE_EPOCH must contain Unix seconds"),
        Err(env::VarError::NotPresent) => {
            command_output("git", manifest_dir, &["show", "-s", "--format=%ct", "HEAD"])
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(0)
        }
        Err(env::VarError::NotUnicode(_)) => {
            panic!("SOURCE_DATE_EPOCH must be valid Unicode")
        }
    }
}

fn watch_git_identity(manifest_dir: &Path) {
    let Some(head_path) = command_output("git", manifest_dir, &["rev-parse", "--git-path", "HEAD"])
    else {
        return;
    };
    println!("cargo:rerun-if-changed={head_path}");

    let Some(symbolic_ref) =
        command_output("git", manifest_dir, &["symbolic-ref", "-q", "HEAD"])
    else {
        return;
    };
    if let Some(ref_path) = command_output(
        "git",
        manifest_dir,
        &["rev-parse", "--git-path", &symbolic_ref],
    ) {
        println!("cargo:rerun-if-changed={ref_path}");
    }
}

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let manifest_dir = Path::new(&manifest_dir);
    let git_commit = command_output("git", manifest_dir, &["rev-parse", "HEAD"])
        .unwrap_or_else(|| "unknown".into());
    let epoch = source_epoch(manifest_dir);
    let build_timestamp = DateTime::<Utc>::from_timestamp(epoch, 0)
        .expect("SOURCE_DATE_EPOCH must be in the supported timestamp range")
        .to_rfc3339_opts(SecondsFormat::Secs, true);
    let rustc = env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    let rustc_version = command_output(&rustc, manifest_dir, &["--version"])
        .unwrap_or_else(|| "rustc unknown".to_string());
    let target = env::var("TARGET").expect("TARGET");

    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
    watch_git_identity(manifest_dir);
    println!("cargo:rustc-env=COSTING_GIT_COMMIT={git_commit}");
    println!("cargo:rustc-env=COSTING_BUILD_TIMESTAMP={build_timestamp}");
    println!("cargo:rustc-env=COSTING_RUSTC_VERSION={rustc_version}");
    println!("cargo:rustc-env=COSTING_BUILD_TARGET={target}");
}

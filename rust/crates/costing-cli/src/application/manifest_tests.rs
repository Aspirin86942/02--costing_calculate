use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

use super::*;

#[test]
fn path_redaction_keeps_only_relative_or_basename() {
    let cwd = unique_root("redaction");
    let inside = cwd.join("data/input.xlsx");
    let outside = cwd
        .parent()
        .unwrap()
        .join("private-parent")
        .join("output.xlsx");

    assert_eq!(
        present_path(&inside, &cwd, true),
        Path::new("data").join("input.xlsx").display().to_string()
    );
    assert_eq!(present_path(&outside, &cwd, true), "output.xlsx");
}

#[test]
fn manifest_publish_race_preserves_competitor_and_cleans_staging() {
    let root = unique_root("race");
    std::fs::create_dir_all(&root).unwrap();
    let output = root.join("summary.json");
    let manifest = sample_failure_manifest();

    let error = publish_manifest_with_hook(&output, "manifest-race", &manifest, |_, final_path| {
        std::fs::write(final_path, b"competitor")
    })
    .unwrap_err();

    assert_eq!(error.code(), ErrorCode::OutputExists);
    assert_eq!(std::fs::read(&output).unwrap(), b"competitor");
    assert_no_publish_temps(&root);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn manifest_staging_failure_leaves_no_final_or_temp_file() {
    let root = unique_root("staging-failure");
    std::fs::create_dir_all(&root).unwrap();
    let output = root.join("summary.json");
    let manifest = sample_failure_manifest();

    let error = publish_manifest_with_hook(&output, "manifest-failure", &manifest, |_, _| {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "injected manifest failure",
        ))
    })
    .unwrap_err();

    assert_eq!(error.code(), ErrorCode::OutputNotWritable);
    assert!(!output.exists());
    assert_no_publish_temps(&root);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn atomic_flush_failure_keeps_a_distinct_summary_stage() {
    let output = PathBuf::from("summary.json");
    let error = map_atomic_error(
        "manifest-flush",
        AtomicFileError {
            stage: AtomicFileStage::Flush,
            final_path: output,
            staging_path: Some(PathBuf::from(".costing-publish-test.tmp")),
            final_published: false,
            cleanup_error: None,
            source: io::Error::new(io::ErrorKind::StorageFull, "flush failed"),
        },
    );

    assert_eq!(
        error.context().unwrap().details.stage,
        ErrorStage::FlushSummaryTempFile
    );
}

fn sample_failure_manifest() -> RunManifestV1 {
    RunManifestV1::Failed(FailureRunManifestV1 {
        schema_version: RUN_MANIFEST_SCHEMA_VERSION,
        status: FailureManifestStatus::Failed,
        request_id: "sample".to_string(),
        application: application_identity(),
        execution: ManifestExecution {
            pipeline: "gb".to_string(),
            mode: ManifestExecutionMode::CheckOnly,
            started_at: "2026-07-25T00:00:00.000Z".to_string(),
            finished_at: "2026-07-25T00:00:01.000Z".to_string(),
            duration_ms: 1_000,
            low_memory_writer: false,
        },
        code: ErrorCode::InvalidInput,
        stage: ErrorStage::ValidateCliRequest,
        message: "sample failure".to_string(),
        retryable: false,
        input: KnownManifestInput::default(),
        filter: ManifestFilter {
            month_start: None,
            month_end: None,
        },
        config: None,
        final_output_valid: false,
        final_output: None,
        warnings: Vec::new(),
    })
}

fn assert_no_publish_temps(root: &Path) {
    let names = std::fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(".costing-publish-"))
        .collect::<Vec<_>>();
    assert!(names.is_empty(), "{names:?}");
}

fn unique_root(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("costing-manifest-{name}-{}-{nanos}", process::id()))
}

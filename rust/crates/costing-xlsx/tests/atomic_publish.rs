use std::io::Write;
use std::path::PathBuf;
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

use costing_xlsx::atomic_file::{AtomicFile, AtomicFileStage};

fn unique_root(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("costing-atomic-{name}-{}-{nanos}", process::id()))
}

#[test]
fn completed_staging_file_is_published_without_exposing_a_partial_final_file() {
    let root = unique_root("success");
    let final_path = root.join("result.xlsx");
    let expected = b"complete workbook bytes";

    let mut staged = AtomicFile::create(&final_path, "request/with:path").unwrap();
    let staging_path = staged.staging_path().to_path_buf();
    assert_eq!(staging_path.parent(), Some(root.as_path()));
    assert!(staging_path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .starts_with(".costing-publish-request_with_path-"));
    assert!(!final_path.exists());

    staged.writer().write_all(expected).unwrap();
    let published = staged.publish().unwrap();

    assert_eq!(published.metadata().unwrap().len(), expected.len() as u64);
    assert_eq!(std::fs::read(&final_path).unwrap(), expected);
    assert!(!staging_path.exists());
    std::fs::remove_file(final_path).unwrap();
    std::fs::remove_dir(root).unwrap();
}

#[test]
fn publish_race_never_overwrites_the_competing_file_and_cleans_staging() {
    let root = unique_root("race");
    let final_path = root.join("result.xlsx");
    let competitor = b"competitor bytes";

    let mut staged = AtomicFile::create(&final_path, "race").unwrap();
    let staging_path = staged.staging_path().to_path_buf();
    staged.writer().write_all(b"candidate bytes").unwrap();
    std::fs::write(&final_path, competitor).unwrap();

    let error = staged.publish().unwrap_err();

    assert_eq!(error.stage, AtomicFileStage::Publish);
    assert_eq!(error.source.kind(), std::io::ErrorKind::AlreadyExists);
    assert!(!error.final_published);
    assert_eq!(std::fs::read(&final_path).unwrap(), competitor);
    assert!(!staging_path.exists());
    std::fs::remove_file(final_path).unwrap();
    std::fs::remove_dir(root).unwrap();
}

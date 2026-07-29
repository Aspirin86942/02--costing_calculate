use std::collections::BTreeMap;
use std::error::Error;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::process;
use std::sync::{Arc, Barrier};
use std::time::{SystemTime, UNIX_EPOCH};

use calamine::{open_workbook_auto, Reader};
use costing_core::error::{ErrorStage, IoKindCode};
use costing_core::model::{CellValue, QualityMetric, SheetModel, StageTimings, WorkbookPayload};
use rust_decimal::Decimal;

use super::*;

fn writer_context() -> WriterContext {
    WriterContext {
        request_id: "writer-test-request".to_string(),
    }
}

fn writer_io_error(raw_os_error: i32) -> WriterError {
    WriterError {
        context: ErrorContext::new(
            "writer-test-request",
            ErrorStage::SaveWorkbook,
            Some(std::path::PathBuf::from("output.xlsx")),
        ),
        low_memory_writer: false,
        primary: WriterPrimaryError::Io(std::io::Error::from_raw_os_error(raw_os_error)),
    }
}

fn unique_temp_path(stem: &str) -> std::path::PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "costing-writer-{stem}-pid{}-{timestamp}.xlsx",
        process::id()
    ))
}

fn unique_temp_dir(stem: &str) -> std::path::PathBuf {
    let path = unique_temp_path(stem).with_extension("");
    std::fs::create_dir(&path).unwrap();
    path
}

fn assert_no_temporary_artifacts(parent: &Path) {
    let artifacts = std::fs::read_dir(parent)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(".costing-publish-") || name.starts_with(".costing-tmp-"))
        .collect::<Vec<_>>();
    assert_eq!(artifacts, Vec::<String>::new());
}

fn sheet(sheet_name: &str) -> SheetModel {
    SheetModel {
        sheet_name: sheet_name.to_string(),
        columns: vec!["月份".to_string(), "本期完工金额".to_string()],
        rows: vec![vec![
            CellValue::Text("2025年01期".to_string().into()),
            CellValue::Decimal(Decimal::new(125, 1)),
        ]],
        column_types: BTreeMap::from([
            ("月份".to_string(), "text".to_string()),
            ("本期完工金额".to_string(), "text".to_string()),
        ]),
        number_formats: BTreeMap::from([("本期完工金额".to_string(), "#,##0.00".to_string())]),
        freeze_panes: Some("A2".to_string()),
        auto_filter: true,
        fixed_width: Some(15.0),
    }
}

fn payload(sheet_models: Vec<SheetModel>) -> WorkbookPayload {
    WorkbookPayload {
        sheet_models,
        quality_metrics: vec![QualityMetric {
            category: "行数勾稽".to_string(),
            metric: "产品数量统计输出行数".to_string(),
            value: "1".to_string(),
            description: "test".to_string(),
        }],
        error_log_count: 0,
        error_log: Vec::new(),
        stage_timings: StageTimings::default(),
    }
}

#[cfg(feature = "low-memory")]
#[test]
fn low_memory_threshold_uses_saturating_cell_slots() {
    assert!(!use_low_memory_for_shape(0, LOW_MEMORY_CELL_SLOT_THRESHOLD));
    assert!(!use_low_memory_for_shape(
        1,
        LOW_MEMORY_CELL_SLOT_THRESHOLD - 1
    ));
    assert!(use_low_memory_for_shape(1, LOW_MEMORY_CELL_SLOT_THRESHOLD));
    assert!(use_low_memory_for_shape(usize::MAX, 2));
}

#[cfg(feature = "low-memory")]
#[test]
fn temp_workspace_is_created_and_removed_below_output_parent() {
    let parent = unique_temp_path("workspace-parent");
    std::fs::create_dir(&parent).unwrap();

    let workspace = TempWorkspace::create(&parent, "request/with:path").unwrap();
    let workspace_path = workspace.path().to_path_buf();

    assert_eq!(workspace_path.parent(), Some(parent.as_path()));
    assert!(workspace_path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .starts_with(".costing-tmp-request_with_path-"));
    workspace.close().unwrap();
    assert!(!workspace_path.exists());
    std::fs::remove_dir(parent).unwrap();
}

#[test]
fn write_workbook_reports_populate_save_and_output_size() {
    let output = unique_temp_path("three-sheet");
    let payload = payload(vec![
        sheet("成本计算单总表"),
        sheet("成本计算单数量聚合维度"),
        sheet("成本分析工单维度"),
    ]);

    let report = write_workbook(&writer_context(), &output, &payload).unwrap();

    assert!(report.writer_populate_seconds.is_finite());
    assert!(report.writer_populate_seconds >= 0.0);
    assert!(report.xlsx_save_seconds.is_finite());
    assert!(report.xlsx_save_seconds >= 0.0);
    let output_size_bytes = std::fs::metadata(&output).unwrap().len();
    assert!(output_size_bytes > 0);
    assert_eq!(report.output_size_bytes, output_size_bytes);

    let workbook = open_workbook_auto(&output).unwrap();
    assert_eq!(
        workbook.sheet_names(),
        &[
            "成本计算单总表".to_string(),
            "成本计算单数量聚合维度".to_string(),
            "成本分析工单维度".to_string(),
        ]
    );
    let _ = std::fs::remove_file(output);
}

#[test]
fn refuses_to_replace_existing_output() {
    let output = unique_temp_path("existing-output");
    let original = b"existing workbook bytes";
    std::fs::write(&output, original).unwrap();
    let payload = payload(vec![
        sheet("成本计算单总表"),
        sheet("成本计算单数量聚合维度"),
        sheet("成本分析工单维度"),
    ]);

    let error = write_workbook(&writer_context(), &output, &payload).unwrap_err();

    assert!(matches!(
        error.primary,
        WriterPrimaryError::Io(ref source) if source.kind() == ErrorKind::AlreadyExists
    ));
    assert_eq!(std::fs::read(&output).unwrap(), original);
    let _ = std::fs::remove_file(output);
}

#[test]
fn concurrent_writers_allow_only_one_output() {
    let output = Arc::new(unique_temp_path("concurrent-output"));
    let barrier = Arc::new(Barrier::new(2));
    let handles = (0..2)
        .map(|_| {
            let output = Arc::clone(&output);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let payload = payload(vec![
                    sheet("成本计算单总表"),
                    sheet("成本计算单数量聚合维度"),
                    sheet("成本分析工单维度"),
                ]);
                barrier.wait();
                match write_workbook(&writer_context(), &output, &payload) {
                    Ok(_) => "written",
                    Err(WriterError {
                        primary: WriterPrimaryError::Io(source),
                        ..
                    }) if source.kind() == ErrorKind::AlreadyExists => "exists",
                    Err(error) => panic!("unexpected writer error: {error}"),
                }
            })
        })
        .collect::<Vec<_>>();

    let mut outcomes = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    outcomes.sort_unstable();

    assert_eq!(outcomes, ["exists", "written"]);
    let workbook = open_workbook_auto(output.as_ref()).unwrap();
    assert_eq!(workbook.sheet_names().len(), 3);
    let _ = std::fs::remove_file(output.as_ref());
}

#[cfg(feature = "low-memory")]
#[test]
fn staging_write_failure_never_exposes_final_output_in_either_writer_mode() {
    for force_low_memory in [false, true] {
        let parent = unique_temp_dir(if force_low_memory {
            "atomic-write-low"
        } else {
            "atomic-write-standard"
        });
        let output = parent.join("output.xlsx");
        let payload = payload(vec![
            sheet("成本计算单总表"),
            sheet("成本计算单数量聚合维度"),
            sheet("成本分析工单维度"),
        ]);
        let control = WriterTestControl {
            force_low_memory,
            fault: WriterTestFault::FailAfterStagingWrite,
        };

        let error = write_workbook_with_test_control(&writer_context(), &output, &payload, control)
            .unwrap_err();

        assert_eq!(error.context.details.stage, ErrorStage::SaveWorkbook);
        assert_eq!(error.low_memory_writer, force_low_memory);
        assert!(!error.context.details.final_output_valid);
        assert!(!output.exists());
        assert_no_temporary_artifacts(&parent);
        std::fs::remove_dir(parent).unwrap();
    }
}

#[cfg(feature = "low-memory")]
#[test]
fn publish_race_preserves_competitor_in_either_writer_mode() {
    for force_low_memory in [false, true] {
        let parent = unique_temp_dir(if force_low_memory {
            "atomic-race-low"
        } else {
            "atomic-race-standard"
        });
        let output = parent.join("output.xlsx");
        let payload = payload(vec![
            sheet("成本计算单总表"),
            sheet("成本计算单数量聚合维度"),
            sheet("成本分析工单维度"),
        ]);
        let control = WriterTestControl {
            force_low_memory,
            fault: WriterTestFault::CompeteBeforePublish,
        };

        let error = write_workbook_with_test_control(&writer_context(), &output, &payload, control)
            .unwrap_err();

        assert_eq!(error.context.details.stage, ErrorStage::PublishWorkbook);
        assert_eq!(error.low_memory_writer, force_low_memory);
        assert!(matches!(
            error.primary,
            WriterPrimaryError::Io(ref source) if source.kind() == ErrorKind::AlreadyExists
        ));
        assert!(!error.context.details.final_output_valid);
        assert_eq!(std::fs::read(&output).unwrap(), b"competing workbook");
        assert_no_temporary_artifacts(&parent);
        std::fs::remove_file(output).unwrap();
        std::fs::remove_dir(parent).unwrap();
    }
}

#[cfg(feature = "low-memory")]
#[test]
fn catchable_interruption_cleans_staging_in_either_writer_mode() {
    for force_low_memory in [false, true] {
        let parent = unique_temp_dir(if force_low_memory {
            "interrupt-low"
        } else {
            "interrupt-standard"
        });
        let output = parent.join("output.xlsx");
        let payload = payload(vec![
            sheet("成本计算单总表"),
            sheet("成本计算单数量聚合维度"),
            sheet("成本分析工单维度"),
        ]);
        let control = WriterTestControl {
            force_low_memory,
            fault: WriterTestFault::InterruptAfterStagingWrite,
        };

        let error = write_workbook_with_test_control(&writer_context(), &output, &payload, control)
            .unwrap_err();

        assert_eq!(error.context.details.stage, ErrorStage::SaveWorkbook);
        assert_eq!(error.low_memory_writer, force_low_memory);
        assert!(matches!(
            error.primary,
            WriterPrimaryError::Io(ref source) if source.kind() == ErrorKind::Interrupted
        ));
        assert!(!error.context.details.final_output_valid);
        assert!(!output.exists());
        assert_no_temporary_artifacts(&parent);
        std::fs::remove_dir(parent).unwrap();
    }
}

#[cfg(feature = "low-memory")]
#[test]
fn post_publish_failure_reports_a_valid_output_in_either_writer_mode() {
    for force_low_memory in [false, true] {
        let parent = unique_temp_dir(if force_low_memory {
            "post-publish-low"
        } else {
            "post-publish-standard"
        });
        let output = parent.join("output.xlsx");
        let payload = payload(vec![
            sheet("成本计算单总表"),
            sheet("成本计算单数量聚合维度"),
            sheet("成本分析工单维度"),
        ]);
        let control = WriterTestControl {
            force_low_memory,
            fault: WriterTestFault::FailAfterPublish,
        };

        let error = write_workbook_with_test_control(&writer_context(), &output, &payload, control)
            .unwrap_err();

        assert_eq!(error.context.details.stage, ErrorStage::ReadOutputMetadata);
        assert_eq!(error.low_memory_writer, force_low_memory);
        assert!(error.context.details.final_output_valid);
        let workbook = open_workbook_auto(&output).unwrap();
        assert_eq!(workbook.sheet_names().len(), 3);
        assert_no_temporary_artifacts(&parent);
        std::fs::remove_file(output).unwrap();
        std::fs::remove_dir(parent).unwrap();
    }
}

#[test]
fn rejects_product_dimension_sheet() {
    let output = unique_temp_path("product-dimension");
    let payload = payload(vec![sheet("成本分析产品维度")]);

    let error = write_workbook(&writer_context(), &output, &payload).unwrap_err();

    assert!(error.to_string().contains("成本分析产品维度"));
    assert!(!output.exists());
}

#[test]
fn rejects_extra_non_default_sheet() {
    let output = unique_temp_path("extra-sheet");
    let payload = payload(vec![
        sheet("成本计算单总表"),
        sheet("成本计算单数量聚合维度"),
        sheet("成本分析工单维度"),
        sheet("调试输出"),
    ]);

    let error = write_workbook(&writer_context(), &output, &payload).unwrap_err();

    assert!(error.to_string().contains("默认 workbook"));
    assert!(!output.exists());
}

#[test]
fn cleanup_failure_does_not_replace_primary_error() {
    let output = unique_temp_path("cleanup-primary");
    let raw_os_error = if cfg!(windows) { 112 } else { 28 };
    let error = writer_io_error(raw_os_error);

    let error = merge_cleanup_failure(
        error,
        ErrorStage::RemovePartialOutput,
        output.clone(),
        std::io::Error::new(ErrorKind::PermissionDenied, "cleanup denied"),
    );

    let WriterPrimaryError::Io(primary) = &error.primary else {
        panic!("expected original I/O primary error")
    };
    assert_eq!(primary.kind(), ErrorKind::StorageFull);
    assert_eq!(primary.raw_os_error(), Some(raw_os_error));
    assert_eq!(
        error
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .raw_os_error(),
        Some(raw_os_error)
    );
    assert_eq!(error.context.details.cleanup_failures.len(), 1);
    let cleanup = &error.context.details.cleanup_failures[0];
    assert_eq!(cleanup.stage, ErrorStage::RemovePartialOutput);
    assert_eq!(cleanup.path.as_deref(), Some(output.as_path()));
    assert_eq!(cleanup.io_meta.kind, IoKindCode::PermissionDenied);
}

#[test]
fn atomic_flush_failure_keeps_a_distinct_workbook_stage() {
    let output = PathBuf::from("output.xlsx");
    let error = atomic_file_error(
        &writer_context(),
        &output,
        AtomicFileError {
            stage: AtomicFileStage::Flush,
            final_path: output.clone(),
            staging_path: Some(PathBuf::from(".costing-publish-test.tmp")),
            final_published: false,
            cleanup_error: None,
            source: std::io::Error::new(ErrorKind::StorageFull, "flush failed"),
        },
    );

    assert_eq!(
        error.context.details.stage,
        ErrorStage::FlushWorkbookTempFile
    );
    assert!(!error.context.details.final_output_valid);
}

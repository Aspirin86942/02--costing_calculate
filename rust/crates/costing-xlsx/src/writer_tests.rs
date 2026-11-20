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

#[cfg(feature = "low-memory")]
#[test]
fn row_compression_matches_serial_xml_and_replays_after_metadata_changes() {
    use std::io::{Cursor, Read};
    use zip::ZipArchive;

    fn worksheet_xml(expected_last_row: Option<u32>, change_view: bool) -> Vec<u8> {
        let dir = tempfile::tempdir().unwrap();
        let mut book = Workbook::new();
        book.set_tempdir(dir.path()).unwrap();
        book.set_zip_compression_level(5);
        let sheet = book.add_worksheet_with_low_memory().unwrap();
        sheet.write_string(0, 0, "标题").unwrap();
        if let Some(last) = expected_last_row {
            sheet.start_row_compression(1, last, 5);
        }
        for row in 1..=1500 {
            sheet.write_number(row, 0, row).unwrap();
        }
        if change_view {
            sheet.set_screen_gridlines(false);
        }
        let extract = |bytes: Vec<u8>| {
            let mut zip = ZipArchive::new(Cursor::new(bytes.as_slice())).unwrap();
            let mut sheet = zip.by_name("xl/worksheets/sheet1.xml").unwrap();
            // A central-directory reader alone accepts a missing streaming descriptor.
            // Excel also checks the local header when opening a workbook.
            let header = sheet.header_start() as usize;
            let flags = u16::from_le_bytes(bytes[header + 6..header + 8].try_into().unwrap());
            let fields = if flags & 8 == 0 {
                header + 14
            } else {
                let end = sheet.data_start().unwrap() as usize + sheet.compressed_size() as usize;
                assert_eq!(&bytes[end..end + 4], b"PK\x07\x08");
                end + 4
            };
            let sizes = [
                sheet.crc32(),
                sheet.compressed_size() as u32,
                sheet.size() as u32,
            ];
            for (field, expected) in bytes[fields..fields + 12].chunks_exact(4).zip(sizes) {
                assert_eq!(u32::from_le_bytes(field.try_into().unwrap()), expected);
            }
            let mut xml = Vec::new();
            sheet.read_to_end(&mut xml).unwrap();
            xml
        };
        let first = extract(book.save_to_buffer().unwrap());
        assert_eq!(first, extract(book.save_to_buffer().unwrap()));
        first
    }
    let expected = worksheet_xml(None, false);
    assert_eq!(worksheet_xml(Some(1500), false), expected);
    assert_eq!(worksheet_xml(Some(1501), false), expected);
    assert_eq!(worksheet_xml(Some(1500), true), worksheet_xml(None, true));
}

#[cfg(feature = "low-memory")]
#[test]
fn low_memory_saves_final_data_and_format_only_rows_once() {
    use std::io::{Cursor, Read};
    use zip::ZipArchive;

    let dir = tempfile::tempdir().unwrap();
    let mut book = Workbook::new();
    book.set_tempdir(dir.path()).unwrap();
    let sheet = book.add_worksheet_with_low_memory().unwrap();
    sheet.write_number(0, 0, 1).unwrap();
    sheet.write_number(1, 0, 2.5).unwrap();
    sheet.set_row_format(1, &Format::new().set_bold()).unwrap();
    sheet.set_row_height(3, 23).unwrap();
    for _ in 0..2 {
        let mut xml = String::new();
        let mut package = ZipArchive::new(Cursor::new(book.save_to_buffer().unwrap())).unwrap();
        package
            .by_name("xl/worksheets/sheet1.xml")
            .unwrap()
            .read_to_string(&mut xml)
            .unwrap();
        assert_eq!(xml.matches(r#"<row r="2""#).count(), 1);
        assert_eq!(xml.matches(r#"<row r="4""#).count(), 1);
        assert!(xml.contains("<v>2.5</v>"));
    }
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

#[test]
fn detail_coordinates_preserve_sparse_cells_in_both_writer_modes() {
    use std::io::Read;
    use zip::ZipArchive;

    let mut sheets: Vec<_> = DEFAULT_SHEET_NAMES.iter().map(|name| sheet(name)).collect();
    let detail = &mut sheets[0];
    detail.columns = (0..28).map(|col| format!("Col{col}")).collect();
    detail.number_formats = BTreeMap::from([("Col1".into(), "#,##0.00".into())]);
    let mut values = vec![CellValue::Blank; 28];
    values[1] = CellValue::Decimal(Decimal::new(125, 1));
    values[2] = CellValue::Text("0012".into());
    values[25] = CellValue::Text("Z".into());
    values[26] = CellValue::Decimal(Decimal::ONE);
    values[27] = CellValue::Text("AB".into());
    detail.rows = vec![values.clone(), vec![CellValue::Blank; 28], values];
    let payload = payload(sheets);

    for &force_low_memory in if cfg!(feature = "low-memory") {
        &[false, true][..]
    } else {
        &[false][..]
    } {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("compact.xlsx");
        let control = WriterTestControl {
            force_low_memory,
            ..Default::default()
        };
        write_workbook_with_test_control(&writer_context(), &path, &payload, control).unwrap();
        let mut archive = ZipArchive::new(std::fs::File::open(&path).unwrap()).unwrap();
        let mut xml = String::new();
        archive
            .by_name("xl/worksheets/sheet1.xml")
            .unwrap()
            .read_to_string(&mut xml)
            .unwrap();
        for row in [2, 4] {
            assert!(xml.contains(&format!(r#"<c r="B{row}" s=""#)));
            assert!(xml.contains(&format!(r#"<c r="Z{row}""#)));
            for column in ["C", "AA", "AB"] {
                assert!(!xml.contains(&format!(r#"<c r="{column}{row}""#)));
            }
        }
        xml.clear();
        archive
            .by_name("xl/worksheets/sheet2.xml")
            .unwrap()
            .read_to_string(&mut xml)
            .unwrap();
        assert!(xml.contains(r#"<c r="A2""#));
        let mut parsed = open_workbook_auto(&path).unwrap();
        let detail = parsed.worksheet_range(DEFAULT_SHEET_NAMES[0]).unwrap();
        for row in [1, 3] {
            assert_eq!(
                detail.get_value((row, 1)),
                Some(&calamine::Data::Float(12.5))
            );
            assert_eq!(
                detail.get_value((row, 2)),
                Some(&calamine::Data::String("0012".into()))
            );
            assert_eq!(
                detail.get_value((row, 27)),
                Some(&calamine::Data::String("AB".into()))
            );
            assert_eq!(detail.get_value((row, 3)), Some(&calamine::Data::Empty));
        }
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
fn xlsx_io_wrappers_preserve_the_original_os_error() {
    let raw_os_error = if cfg!(windows) { 112 } else { 28 };
    for error in [
        XlsxError::IoError(std::io::Error::from_raw_os_error(raw_os_error)),
        XlsxError::TempFileError(
            PathBuf::from("writer-temp"),
            std::io::Error::from_raw_os_error(raw_os_error),
        ),
        XlsxError::ZipError(zip::result::ZipError::Io(
            std::io::Error::from_raw_os_error(raw_os_error),
        )),
    ] {
        let mut source = Some(&error as &(dyn Error + 'static));
        let mut original_io = None;
        while let Some(current) = source {
            if let Some(io_error) = current.downcast_ref::<std::io::Error>() {
                original_io = Some(io_error);
                break;
            }
            source = current.source();
        }
        assert_eq!(
            original_io
                .expect("original I/O error in source chain")
                .raw_os_error(),
            Some(raw_os_error)
        );
        let WriterPrimaryError::Io(source) =
            primary_from_xlsx_error(CostingXlsxError::Writer(error))
        else {
            panic!("expected original I/O error");
        };
        assert_eq!(source.kind(), ErrorKind::StorageFull);
        assert_eq!(source.raw_os_error(), Some(raw_os_error));
    }
    assert!(matches!(
        primary_from_xlsx_error(CostingXlsxError::Writer(XlsxError::ZipError(
            zip::result::ZipError::UnsupportedArchive("unsupported test option"),
        ))),
        WriterPrimaryError::Xlsx(_)
    ));
}

#[cfg(feature = "low-memory")]
#[test]
fn low_memory_factory_failure_keeps_initialization_stage_and_directory() {
    let directory = unique_temp_dir("factory-missing");
    let output = directory.with_extension("xlsx");
    let mut workbook = Workbook::new();
    workbook.set_tempdir(&directory).unwrap();
    std::fs::remove_dir(&directory).unwrap();

    let error = populate_sheet(
        &writer_context(),
        &output,
        &mut workbook,
        &sheet("成本计算单总表"),
        true,
        1,
    )
    .unwrap_err();

    assert_eq!(
        error.context.details.stage,
        ErrorStage::InitializeLowMemoryTempWriter
    );
    assert_eq!(
        error.context.details.path.as_deref(),
        Some(directory.as_path())
    );
    assert!(
        matches!(error.primary, WriterPrimaryError::Io(ref source) if source.kind() == ErrorKind::NotFound)
    );
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

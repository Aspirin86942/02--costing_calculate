use std::collections::BTreeSet;

use costing_calculate::application::{RunManifestV1, RUN_MANIFEST_SCHEMA_VERSION};
use costing_calculate::build_info::BuildInfo;
use costing_core::error::ErrorStage;
use costing_core::ErrorCode;
use serde_json::Value;

const SCHEMA: &str = include_str!("../config/run-manifest.schema.json");
const SUCCESS_GOLDEN: &str = include_str!("../config/run-manifest.success.golden.json");
const FAILURE_GOLDEN: &str = include_str!("../config/run-manifest.failure.golden.json");

#[test]
fn success_and_failure_goldens_round_trip_through_the_closed_v1_model() {
    for golden in [SUCCESS_GOLDEN, FAILURE_GOLDEN] {
        let expected: Value = serde_json::from_str(golden).unwrap();
        let typed: RunManifestV1 = serde_json::from_str(golden).unwrap();
        assert_eq!(serde_json::to_value(typed).unwrap(), expected);
    }

    let mut unknown_top_level: Value = serde_json::from_str(SUCCESS_GOLDEN).unwrap();
    unknown_top_level["unexpected"] = Value::Bool(true);
    assert!(serde_json::from_value::<RunManifestV1>(unknown_top_level).is_err());

    let mut unknown_nested: Value = serde_json::from_str(SUCCESS_GOLDEN).unwrap();
    unknown_nested["application"]["secret_path"] = Value::String("forbidden".to_string());
    assert!(serde_json::from_value::<RunManifestV1>(unknown_nested).is_err());

    let mut unknown_status: Value = serde_json::from_str(SUCCESS_GOLDEN).unwrap();
    unknown_status["status"] = Value::String("completed".to_string());
    assert!(serde_json::from_value::<RunManifestV1>(unknown_status).is_err());
}

#[test]
fn published_schema_is_closed_and_tracks_the_runtime_error_vocabulary() {
    let schema: Value = serde_json::from_str(SCHEMA).unwrap();
    assert_eq!(
        schema["$schema"],
        "https://json-schema.org/draft/2020-12/schema"
    );
    assert_eq!(schema["title"], "RunManifestV1");
    assert_eq!(schema["oneOf"].as_array().unwrap().len(), 2);
    assert_closed_objects(&schema, "$");

    let schema_codes = string_set(&schema["$defs"]["failure"]["properties"]["code"]["enum"]);
    let runtime_codes = [
        ErrorCode::InvalidInput,
        ErrorCode::InvalidConfig,
        ErrorCode::FileNotFound,
        ErrorCode::FileNotReadable,
        ErrorCode::UnsupportedFileType,
        ErrorCode::OutputExists,
        ErrorCode::OutputNotWritable,
        ErrorCode::InsufficientDiskSpace,
        ErrorCode::TempCleanupFailed,
        ErrorCode::ReaderMismatch,
        ErrorCode::EtlMismatch,
        ErrorCode::AnalysisMismatch,
        ErrorCode::WorkbookMismatch,
        ErrorCode::PerformanceRegression,
        ErrorCode::InternalError,
    ]
    .into_iter()
    .map(|code| {
        serde_json::to_value(code)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string()
    })
    .collect::<BTreeSet<_>>();
    assert_eq!(schema_codes, runtime_codes);

    let schema_stages = string_set(&schema["$defs"]["failure"]["properties"]["stage"]["enum"]);
    let runtime_stages = [
        ErrorStage::ValidateCliRequest,
        ErrorStage::LoadConfig,
        ErrorStage::ParseConfig,
        ErrorStage::ValidateConfig,
        ErrorStage::ResolveCliPaths,
        ErrorStage::IngestWorkbook,
        ErrorStage::Normalize,
        ErrorStage::Split,
        ErrorStage::BuildFact,
        ErrorStage::BuildPresentation,
        ErrorStage::PrepareOutputDirectory,
        ErrorStage::CheckDiskSpace,
        ErrorStage::CreateTempWorkspace,
        ErrorStage::CreateWorkbookTempFile,
        ErrorStage::PlanSheet,
        ErrorStage::InitializeLowMemoryTempWriter,
        ErrorStage::PopulateWorkbook,
        ErrorStage::CreateFinalOutput,
        ErrorStage::SaveWorkbook,
        ErrorStage::SyncWorkbookTempFile,
        ErrorStage::PublishWorkbook,
        ErrorStage::CleanupWorkbookTempFile,
        ErrorStage::RemovePartialOutput,
        ErrorStage::CleanupTempWorkspace,
        ErrorStage::ReadOutputMetadata,
        ErrorStage::CheckSummaryOutput,
        ErrorStage::PrepareSummaryDirectory,
        ErrorStage::CreateSummaryTempFile,
        ErrorStage::WriteSummary,
        ErrorStage::SyncSummaryTempFile,
        ErrorStage::PublishSummary,
        ErrorStage::CleanupSummaryTempFile,
        ErrorStage::HashInput,
        ErrorStage::HashOutput,
        ErrorStage::BuildManifest,
    ]
    .into_iter()
    .map(|stage| {
        serde_json::to_value(stage)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string()
    })
    .collect::<BTreeSet<_>>();
    assert_eq!(schema_stages, runtime_stages);
}

#[test]
fn build_info_and_manifest_export_the_same_schema_version() {
    assert_eq!(RUN_MANIFEST_SCHEMA_VERSION, 1);
    assert_eq!(
        BuildInfo::current().run_manifest_schema_version,
        RUN_MANIFEST_SCHEMA_VERSION
    );
}

fn string_set(value: &Value) -> BTreeSet<String> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item.as_str().unwrap().to_string())
        .collect()
}

fn assert_closed_objects(value: &Value, path: &str) {
    match value {
        Value::Object(object) => {
            if object.get("type").and_then(Value::as_str) == Some("object")
                && object.contains_key("properties")
            {
                assert!(
                    object.contains_key("additionalProperties"),
                    "object schema at {path} must declare additionalProperties"
                );
            }
            for (key, nested) in object {
                assert_closed_objects(nested, &format!("{path}/{key}"));
            }
        }
        Value::Array(items) => {
            for (index, nested) in items.iter().enumerate() {
                assert_closed_objects(nested, &format!("{path}/{index}"));
            }
        }
        _ => {}
    }
}

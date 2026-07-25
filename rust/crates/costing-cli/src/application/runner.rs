use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use costing_core::error::{ErrorContext, ErrorStage};
use costing_core::model::ErrorSummary;
use costing_core::{CostingError, ErrorCode};

use super::manifest::{
    preflight_summary_output, publish_manifest, redact_failure_paths, redact_run_summary, RunAudit,
    RunManifestV1,
};
use super::{RunOperation, RunOutcome, RunRequest};

#[must_use]
pub fn execute(request: RunRequest) -> RunOutcome {
    execute_with_publisher(request, &publish_manifest)
}

fn execute_with_publisher(
    request: RunRequest,
    publisher: &impl Fn(&Path, &str, &RunManifestV1) -> Result<(), CostingError>,
) -> RunOutcome {
    let request_id = new_request_id();
    let summary_output = request.summary_output.clone();
    let redact_paths = request.redact_paths;
    let mut audit = RunAudit::new(&request, summary_output.is_some());
    if let Some(path) = summary_output.as_deref() {
        if let Err(error) = preflight_summary_output(path, &request_id) {
            let mut failure = failure_from_error(&error);
            if redact_paths {
                redact_failure_paths(&mut failure, current_dir());
            }
            return RunOutcome::Failed(failure);
        }
    }
    let loaded = match crate::config::load_configuration(request.config.as_deref(), &request_id) {
        Ok(loaded) => loaded,
        Err(error) => {
            return failed_outcome(
                failure_from_error(&error),
                &request_id,
                summary_output.as_deref(),
                None,
                &audit,
                redact_paths,
                publisher,
            )
        }
    };
    let effective = match loaded.for_pipeline(request.pipeline) {
        Ok(effective) => effective,
        Err(error) => {
            let error = error.with_context(ErrorContext::new(
                &request_id,
                ErrorStage::ValidateConfig,
                request.config.clone(),
            ));
            return failed_outcome(
                failure_from_error(&error),
                &request_id,
                summary_output.as_deref(),
                None,
                &audit,
                redact_paths,
                publisher,
            );
        }
    };
    match request.operation {
        RunOperation::ValidateConfig => RunOutcome::ConfigValidated(effective.validation_record()),
        RunOperation::PrintEffectiveConfig => RunOutcome::EffectiveConfig(effective.document),
        RunOperation::Execute => {
            let effective_document = effective.document.clone();
            match crate::run::run_with_audit(
                request,
                effective.rules,
                effective.input_pattern,
                request_id.clone(),
                &mut audit,
            ) {
                Ok(mut summary) => {
                    if let Some(path) = summary_output.as_deref() {
                        let manifest = match audit.build_success(
                            &request_id,
                            &effective_document,
                            &summary,
                            redact_paths,
                        ) {
                            Ok(manifest) => manifest,
                            Err(error) => {
                                let error = error.with_context(ErrorContext::new(
                                    &request_id,
                                    ErrorStage::BuildManifest,
                                    Some(path.to_path_buf()),
                                ));
                                let mut failure = failure_from_error(&error);
                                audit.enrich_failure(&mut failure, redact_paths);
                                return RunOutcome::Failed(failure);
                            }
                        };
                        if let Err(error) = publisher(path, &request_id, &manifest) {
                            let mut failure = failure_from_error(&error);
                            audit.enrich_failure(&mut failure, redact_paths);
                            return RunOutcome::Failed(failure);
                        }
                    }
                    if redact_paths {
                        redact_run_summary(&mut summary, current_dir());
                    }
                    RunOutcome::Succeeded(summary)
                }
                Err(error) => failed_outcome(
                    failure_from_anyhow(&error),
                    &request_id,
                    summary_output.as_deref(),
                    Some(&effective_document),
                    &audit,
                    redact_paths,
                    publisher,
                ),
            }
        }
    }
}

fn failed_outcome(
    mut failure: ErrorSummary,
    request_id: &str,
    summary_output: Option<&Path>,
    config: Option<&crate::config::EffectiveConfigDocument>,
    audit: &RunAudit,
    redact_paths: bool,
    publisher: &impl Fn(&Path, &str, &RunManifestV1) -> Result<(), CostingError>,
) -> RunOutcome {
    audit.enrich_failure(&mut failure, redact_paths);
    if let Some(path) = summary_output {
        let manifest = audit.build_failure(request_id, config, &failure, redact_paths);
        if let Err(manifest_error) = publisher(path, request_id, &manifest) {
            let mut manifest_failure = failure_from_error(&manifest_error);
            if redact_paths {
                redact_failure_paths(&mut manifest_failure, current_dir());
            }
            failure.message = format!(
                "{}; 失败 Manifest 写出失败: {}",
                failure.message, manifest_failure.message
            );
            if let (Some(details), Some(manifest_details)) = (
                failure.details.as_mut(),
                manifest_error.context().map(|context| &context.details),
            ) {
                details
                    .cleanup_failures
                    .extend(manifest_details.cleanup_failures.clone());
            }
        }
    }
    RunOutcome::Failed(failure)
}

fn failure_from_anyhow(error: &anyhow::Error) -> ErrorSummary {
    error
        .downcast_ref::<CostingError>()
        .map(ErrorSummary::from_error)
        .unwrap_or_else(|| ErrorSummary {
            status: "failed".to_string(),
            code: ErrorCode::InternalError,
            message: error.to_string(),
            retryable: false,
            request_id: None,
            details: None,
        })
}

fn failure_from_error(error: &CostingError) -> ErrorSummary {
    ErrorSummary::from_error(error)
}

fn new_request_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("costing-{}-{nanos}", std::process::id())
}

fn current_dir() -> &'static Path {
    Path::new(".")
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::process;
    use std::time::{SystemTime, UNIX_EPOCH};

    use calamine::{open_workbook_auto, Reader};
    use costing_core::{ErrorCode, PipelineName};
    use rust_xlsxwriter::Workbook;

    use super::*;
    use crate::application::{RunManifestV1, RunOperation};

    #[test]
    fn manifest_failure_after_workbook_publish_keeps_a_valid_hashed_workbook() {
        let root = unique_root("publish-failure");
        std::fs::create_dir(&root).unwrap();
        let input = root.join("input.xlsx");
        let output = root.join("output.xlsx");
        let summary = root.join("summary.json");
        write_minimal_input(&input);
        let request = RunRequest {
            pipeline: PipelineName::Gb,
            input: Some(input.clone()),
            output: Some(output.clone()),
            month_start: None,
            month_end: None,
            check_only: false,
            benchmark: false,
            summary_output: Some(summary.clone()),
            redact_paths: false,
            config: None,
            operation: RunOperation::Execute,
        };
        let failing_publisher = |path: &Path, request_id: &str, manifest: &RunManifestV1| {
            assert!(matches!(manifest, RunManifestV1::Succeeded(_)));
            let source =
                std::io::Error::new(std::io::ErrorKind::PermissionDenied, "injected failure");
            Err(CostingError::io_with_source(
                ErrorCode::OutputNotWritable,
                "injected manifest failure",
                source,
            )
            .with_context(ErrorContext::new(
                request_id,
                ErrorStage::WriteSummary,
                Some(path.to_path_buf()),
            )))
        };

        let outcome = execute_with_publisher(request, &failing_publisher);

        let RunOutcome::Failed(failure) = outcome else {
            panic!("expected manifest publication failure");
        };
        assert_eq!(failure.code, ErrorCode::OutputNotWritable);
        let failure_json = serde_json::to_value(&failure).unwrap();
        assert_eq!(failure_json["details"]["final_output_valid"], true);
        assert_eq!(
            failure_json["details"]["final_output_path"],
            output.display().to_string()
        );
        let details = failure.details.expect("structured failure details");
        assert_eq!(details.stage, ErrorStage::WriteSummary);
        assert!(details.final_output_valid);
        let final_output = details.final_output.expect("valid output identity");
        assert_eq!(final_output.final_output_path, output);
        let (_, expected_sha256) = super::super::manifest::sha256_file(&output).unwrap();
        assert_eq!(
            final_output.final_output_sha256.as_deref(),
            Some(expected_sha256.as_str())
        );
        assert!(!summary.exists());
        let workbook = open_workbook_auto(&output).expect("published workbook opens");
        assert_eq!(workbook.sheet_names().len(), 3);
        std::fs::remove_dir_all(root).unwrap();
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
        std::env::temp_dir().join(format!("costing-runner-{name}-{}-{nanos}", process::id()))
    }
}

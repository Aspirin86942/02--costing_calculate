//! Versioned run evidence and atomic sidecar publication.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::Instant;

use chrono::{DateTime, SecondsFormat, Utc};
use costing_core::error::{
    CleanupFailureMeta, ErrorContext, ErrorDetails, ErrorStage, FinalOutputMeta, IoFailureMeta,
};
use costing_core::model::{ErrorSummary, QualityMetric};
use costing_core::{CostingError, ErrorCode, RunSummary, StageTimings};
use costing_xlsx::atomic_file::{AtomicFile, AtomicFileError, AtomicFileStage};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};

use crate::build_info::BuildInfo;
use crate::config::{ConfigSource, EffectiveConfigDocument};

use super::request::RunRequest;

/// Only run-manifest schema version accepted by this executable.
pub const RUN_MANIFEST_SCHEMA_VERSION: u32 = 1;

fn deserialize_manifest_schema_version<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let version = u32::deserialize(deserializer)?;
    if version != RUN_MANIFEST_SCHEMA_VERSION {
        return Err(serde::de::Error::custom(format!(
            "unsupported run manifest schema_version {version}; expected {RUN_MANIFEST_SCHEMA_VERSION}"
        )));
    }
    Ok(version)
}

/// Successful or failed version-one audit record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RunManifestV1 {
    /// Complete evidence for a successful run.
    Succeeded(SuccessRunManifestV1),
    /// Bounded evidence for a failed run.
    Failed(FailureRunManifestV1),
}

/// Closed success status vocabulary for schema v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SuccessManifestStatus {
    #[serde(rename = "succeeded")]
    Succeeded,
}

/// Closed failure status vocabulary for schema v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailureManifestStatus {
    #[serde(rename = "failed")]
    Failed,
}

/// Reproducible executable identity recorded in every manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestApplication {
    pub name: String,
    pub version: String,
    pub git_commit: String,
    pub build_timestamp: String,
    pub rustc_version: String,
    pub target: String,
}

/// User-visible execution mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ManifestExecutionMode {
    Normal,
    CheckOnly,
}

/// Timing and writer-mode identity for one execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestExecution {
    pub pipeline: String,
    pub mode: ManifestExecutionMode,
    pub started_at: String,
    pub finished_at: String,
    pub duration_ms: u64,
    pub low_memory_writer: bool,
}

/// Fully known input identity for a successful run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestInput {
    pub path: String,
    pub file_name: String,
    pub size_bytes: u64,
    pub sha256: String,
    pub selected_sheet: String,
    pub reader_rows: usize,
}

/// Optional month filter applied to the run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestFilter {
    pub month_start: Option<String>,
    pub month_end: Option<String>,
}

/// Validated configuration identity and semantic fingerprint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestConfig {
    pub schema_version: u32,
    pub source: ConfigSource,
    pub effective_sha256: String,
    pub source_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// Published workbook identity and stable output contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestResult {
    pub output_written: bool,
    pub workbook_path: Option<String>,
    pub output_size_bytes: Option<u64>,
    pub output_sha256: Option<String>,
    pub sheet_count: usize,
    pub sheet_names: Vec<String>,
    pub final_output_valid: bool,
}

/// Quality and audit aggregates that contain no business detail rows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestQuality {
    pub error_log_count: usize,
    pub issue_type_counts: BTreeMap<String, usize>,
    pub quality_metrics: Vec<QualityMetric>,
}

/// Version-one manifest for a successful run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuccessRunManifestV1 {
    #[serde(deserialize_with = "deserialize_manifest_schema_version")]
    pub schema_version: u32,
    pub status: SuccessManifestStatus,
    pub request_id: String,
    pub application: ManifestApplication,
    pub execution: ManifestExecution,
    pub input: ManifestInput,
    pub filter: ManifestFilter,
    pub config: ManifestConfig,
    pub result: ManifestResult,
    pub quality: ManifestQuality,
    pub run_counts: BTreeMap<String, usize>,
    pub stage_timings: StageTimings,
    pub warnings: Vec<String>,
}

/// Input identity fields known before or at the point of failure.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KnownManifestInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_sheet: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reader_rows: Option<usize>,
}

/// Identity of a workbook that remains valid after a later failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidFinalOutput {
    pub path: String,
    pub size_bytes: u64,
    pub sha256: String,
}

/// Version-one manifest for a failed run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailureRunManifestV1 {
    #[serde(deserialize_with = "deserialize_manifest_schema_version")]
    pub schema_version: u32,
    pub status: FailureManifestStatus,
    pub request_id: String,
    pub application: ManifestApplication,
    pub execution: ManifestExecution,
    pub code: ErrorCode,
    pub stage: ErrorStage,
    pub message: String,
    pub retryable: bool,
    pub input: KnownManifestInput,
    pub filter: ManifestFilter,
    pub config: Option<ManifestConfig>,
    pub final_output_valid: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_output: Option<ValidFinalOutput>,
    pub warnings: Vec<String>,
}

/// Mutable, bounded evidence captured while one request crosses the application boundary.
#[derive(Debug)]
pub(crate) struct RunAudit {
    enabled: bool,
    cwd: PathBuf,
    pipeline: String,
    mode: ManifestExecutionMode,
    started_at: DateTime<Utc>,
    started: Instant,
    month_start: Option<String>,
    month_end: Option<String>,
    config_path: Option<PathBuf>,
    input_path: Option<PathBuf>,
    input_file_name: Option<String>,
    input_size_bytes: Option<u64>,
    input_sha256: Option<String>,
    selected_sheet: Option<String>,
    reader_rows: Option<usize>,
    output_path: Option<PathBuf>,
    output_size_bytes: Option<u64>,
    output_sha256: Option<String>,
    sheet_names: Vec<String>,
    low_memory_writer: bool,
    final_output_valid: bool,
    warnings: Vec<String>,
}

impl RunAudit {
    pub(crate) fn new(request: &RunRequest, enabled: bool, cwd: PathBuf) -> Self {
        Self {
            enabled,
            cwd,
            pipeline: request.pipeline.as_str().to_string(),
            mode: if request.check_only {
                ManifestExecutionMode::CheckOnly
            } else {
                ManifestExecutionMode::Normal
            },
            started_at: Utc::now(),
            started: Instant::now(),
            month_start: request.month_start.clone(),
            month_end: request.month_end.clone(),
            config_path: request.config.clone(),
            input_path: request.input.clone(),
            input_file_name: request
                .input
                .as_deref()
                .and_then(Path::file_name)
                .map(|name| name.to_string_lossy().into_owned()),
            input_size_bytes: None,
            input_sha256: None,
            selected_sheet: None,
            reader_rows: None,
            output_path: request.output.clone(),
            output_size_bytes: None,
            output_sha256: None,
            sheet_names: Vec::new(),
            low_memory_writer: false,
            final_output_valid: false,
            warnings: Vec::new(),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn cwd(&self) -> &Path {
        &self.cwd
    }

    pub(crate) fn record_resolved_paths(&mut self, input: &Path, output: Option<&Path>) {
        self.input_path = Some(input.to_path_buf());
        self.input_file_name = input
            .file_name()
            .map(|name| name.to_string_lossy().into_owned());
        self.output_path = output.map(Path::to_path_buf);
    }

    pub(crate) fn record_input(
        &mut self,
        size_bytes: u64,
        sha256: String,
        selected_sheet: String,
        reader_rows: usize,
    ) {
        self.input_size_bytes = Some(size_bytes);
        self.input_sha256 = Some(sha256);
        self.selected_sheet = Some(selected_sheet);
        self.reader_rows = Some(reader_rows);
    }

    pub(crate) fn record_reader_identity(&mut self, selected_sheet: String, reader_rows: usize) {
        self.selected_sheet = Some(selected_sheet);
        self.reader_rows = Some(reader_rows);
    }

    pub(crate) fn record_sheet_names(&mut self, sheet_names: Vec<String>) {
        self.sheet_names = sheet_names;
    }

    pub(crate) fn record_output(
        &mut self,
        size_bytes: u64,
        sha256: String,
        low_memory_writer: bool,
    ) {
        self.output_size_bytes = Some(size_bytes);
        self.output_sha256 = Some(sha256);
        self.low_memory_writer = low_memory_writer;
        self.final_output_valid = true;
    }

    pub(crate) fn mark_output_published(&mut self, size_bytes: u64, low_memory_writer: bool) {
        self.output_size_bytes = Some(size_bytes);
        self.low_memory_writer = low_memory_writer;
        self.final_output_valid = true;
    }

    pub(crate) fn record_writer_failure(
        &mut self,
        low_memory_writer: bool,
        final_output_valid: bool,
    ) {
        self.low_memory_writer = low_memory_writer;
        self.final_output_valid |= final_output_valid;
    }

    pub(crate) fn warn(&mut self, warning: impl Into<String>) {
        self.warnings.push(warning.into());
    }

    pub(crate) fn build_success(
        &self,
        request_id: &str,
        config: &EffectiveConfigDocument,
        summary: &RunSummary,
        redact_paths: bool,
    ) -> Result<RunManifestV1, CostingError> {
        let input_path = required(&self.input_path, "input.path")?;
        let input_file_name = required(&self.input_file_name, "input.file_name")?;
        let input_size_bytes = required(&self.input_size_bytes, "input.size_bytes")?;
        let input_sha256 = required(&self.input_sha256, "input.sha256")?;
        let selected_sheet = required(&self.selected_sheet, "input.selected_sheet")?;
        let reader_rows = required(&self.reader_rows, "input.reader_rows")?;
        if summary.output_written && !self.final_output_valid {
            return Err(CostingError::internal(
                "cannot build successful manifest before workbook publication",
            ));
        }
        Ok(RunManifestV1::Succeeded(SuccessRunManifestV1 {
            schema_version: RUN_MANIFEST_SCHEMA_VERSION,
            status: SuccessManifestStatus::Succeeded,
            request_id: request_id.to_string(),
            application: application_identity(),
            execution: self.execution_identity(),
            input: ManifestInput {
                path: present_path(input_path, &self.cwd, redact_paths),
                file_name: input_file_name.clone(),
                size_bytes: *input_size_bytes,
                sha256: input_sha256.clone(),
                selected_sheet: selected_sheet.clone(),
                reader_rows: *reader_rows,
            },
            filter: self.filter_identity(),
            config: self.config_identity(config, redact_paths),
            result: ManifestResult {
                output_written: summary.output_written,
                workbook_path: self
                    .output_path
                    .as_deref()
                    .filter(|_| summary.output_written)
                    .map(|path| present_path(path, &self.cwd, redact_paths)),
                output_size_bytes: self.output_size_bytes,
                output_sha256: self.output_sha256.clone(),
                sheet_count: summary.sheet_count,
                sheet_names: self.sheet_names.clone(),
                final_output_valid: self.final_output_valid,
            },
            quality: ManifestQuality {
                error_log_count: summary.error_log_count,
                issue_type_counts: summary.issue_type_counts.clone(),
                quality_metrics: summary.quality_metrics.clone(),
            },
            run_counts: summary.run_counts.clone(),
            stage_timings: summary.stage_timings.clone(),
            warnings: self.warnings.clone(),
        }))
    }

    pub(crate) fn build_failure(
        &self,
        request_id: &str,
        config: Option<&EffectiveConfigDocument>,
        failure: &ErrorSummary,
        redact_paths: bool,
    ) -> RunManifestV1 {
        let stage = failure
            .details
            .as_ref()
            .map(|details| details.stage)
            .unwrap_or(ErrorStage::ValidateCliRequest);
        let input = KnownManifestInput {
            path: self
                .input_path
                .as_deref()
                .map(|path| present_path(path, &self.cwd, redact_paths)),
            file_name: self.input_file_name.clone(),
            size_bytes: self.input_size_bytes,
            sha256: self.input_sha256.clone(),
            selected_sheet: self.selected_sheet.clone(),
            reader_rows: self.reader_rows,
        };
        let error_reports_valid_output = failure
            .details
            .as_ref()
            .is_some_and(|details| details.final_output_valid);
        let final_output_valid = self.final_output_valid || error_reports_valid_output;
        let final_output = match (
            final_output_valid,
            self.output_path.as_deref(),
            self.output_size_bytes,
            self.output_sha256.as_deref(),
        ) {
            (true, Some(path), Some(size_bytes), Some(sha256)) => Some(ValidFinalOutput {
                path: present_path(path, &self.cwd, redact_paths),
                size_bytes,
                sha256: sha256.to_string(),
            }),
            _ => None,
        };
        RunManifestV1::Failed(FailureRunManifestV1 {
            schema_version: RUN_MANIFEST_SCHEMA_VERSION,
            status: FailureManifestStatus::Failed,
            request_id: request_id.to_string(),
            application: application_identity(),
            execution: self.execution_identity(),
            code: failure.code,
            stage,
            message: failure.message.clone(),
            retryable: failure.retryable,
            input,
            filter: self.filter_identity(),
            config: config.map(|config| self.config_identity(config, redact_paths)),
            final_output_valid,
            final_output,
            warnings: self.warnings.clone(),
        })
    }

    pub(crate) fn enrich_failure(&self, failure: &mut ErrorSummary, redact_paths: bool) {
        let final_output_valid = self.final_output_valid
            || failure
                .details
                .as_ref()
                .is_some_and(|details| details.final_output_valid);
        if final_output_valid {
            let details = failure
                .details
                .get_or_insert_with(|| ErrorDetails::new(ErrorStage::BuildManifest, None));
            details.final_output_valid = true;
            details.final_output = self.output_path.as_deref().map(|path| {
                Box::new(FinalOutputMeta {
                    final_output_path: PathBuf::from(present_path(path, &self.cwd, redact_paths)),
                    final_output_sha256: self.output_sha256.clone(),
                })
            });
        }
        if redact_paths {
            for path in [
                self.input_path.as_deref(),
                self.output_path.as_deref(),
                self.config_path.as_deref(),
            ]
            .into_iter()
            .flatten()
            {
                redact_path_in_message(&mut failure.message, path, &self.cwd);
            }
            redact_failure_paths(failure, &self.cwd);
        }
    }

    fn execution_identity(&self) -> ManifestExecution {
        let finished_at = Utc::now();
        let duration_ms = self.started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        ManifestExecution {
            pipeline: self.pipeline.clone(),
            mode: self.mode,
            started_at: rfc3339(self.started_at),
            finished_at: rfc3339(finished_at),
            duration_ms,
            low_memory_writer: self.low_memory_writer,
        }
    }

    fn filter_identity(&self) -> ManifestFilter {
        ManifestFilter {
            month_start: self.month_start.clone(),
            month_end: self.month_end.clone(),
        }
    }

    fn config_identity(
        &self,
        config: &EffectiveConfigDocument,
        redact_paths: bool,
    ) -> ManifestConfig {
        ManifestConfig {
            schema_version: config.schema_version,
            source: config.source,
            effective_sha256: config.effective_sha256.clone(),
            source_sha256: config.source_sha256.clone(),
            path: self
                .config_path
                .as_deref()
                .map(|path| present_path(path, &self.cwd, redact_paths)),
        }
    }
}

pub(crate) fn redact_run_summary(summary: &mut RunSummary, cwd: &Path) {
    summary.workbook_path = summary.workbook_path.as_deref().map(|path| {
        let path = Path::new(path);
        present_path(path, cwd, true)
    });
}

pub(crate) fn redact_failure_paths(failure: &mut ErrorSummary, cwd: &Path) {
    let Some(details) = failure.details.as_mut() else {
        return;
    };
    let diagnostic_paths = std::iter::once(details.path.as_deref())
        .chain(std::iter::once(
            details
                .final_output
                .as_deref()
                .map(|output| output.final_output_path.as_path()),
        ))
        .chain(
            details
                .cleanup_failures
                .iter()
                .map(|cleanup| cleanup.path.as_deref()),
        )
        .flatten()
        .map(Path::to_path_buf)
        .collect::<Vec<_>>();
    for path in &diagnostic_paths {
        redact_path_in_message(&mut failure.message, path, cwd);
    }
    details.path = details
        .path
        .as_deref()
        .map(|path| PathBuf::from(present_path(path, cwd, true)));
    if let Some(output) = details.final_output.as_mut() {
        output.final_output_path =
            PathBuf::from(present_path(&output.final_output_path, cwd, true));
    }
    for cleanup in &mut details.cleanup_failures {
        cleanup.path = cleanup
            .path
            .as_deref()
            .map(|path| PathBuf::from(present_path(path, cwd, true)));
    }
}

fn redact_path_in_message(message: &mut String, path: &Path, cwd: &Path) {
    let replacement = present_path(path, cwd, true);
    let absolute = absolute_lexical(path, cwd);
    let mut candidates = [absolute.display().to_string(), path.display().to_string()];
    candidates.sort_by_key(|candidate| std::cmp::Reverse(candidate.len()));
    for candidate in candidates {
        if !candidate.is_empty() && candidate != replacement {
            *message = message.replace(&candidate, &replacement);
        }
    }
}

pub(crate) fn sha256_file(path: &Path) -> io::Result<(u64, String)> {
    let file = File::open(path)?;
    let size_bytes = file.metadata()?.len();
    let mut reader = BufReader::new(file);
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok((size_bytes, hex_digest(digest)))
}

pub(crate) fn sha256_bytes(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    hex_digest(digest)
}

fn hex_digest(digest: Sha256) -> String {
    let digest = digest.finalize();
    let mut sha256 = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut sha256, "{byte:02x}").expect("writing to String cannot fail");
    }
    sha256
}

pub(crate) fn publish_manifest(
    path: &Path,
    request_id: &str,
    manifest: &RunManifestV1,
) -> Result<(), CostingError> {
    publish_manifest_with_hook(path, request_id, manifest, |_, _| Ok(()))
}

pub(crate) fn preflight_summary_output(path: &Path, request_id: &str) -> Result<(), CostingError> {
    match path.try_exists() {
        Ok(false) => Ok(()),
        Ok(true) => Err(CostingError::io(
            ErrorCode::OutputExists,
            format!("运行 Manifest 已存在: {}", path.display()),
            path.to_path_buf(),
        )
        .with_context(ErrorContext::new(
            request_id,
            ErrorStage::CheckSummaryOutput,
            Some(path.to_path_buf()),
        ))),
        Err(source) => Err(CostingError::io_with_source(
            ErrorCode::OutputNotWritable,
            format!("无法检查运行 Manifest 路径 {}: {source}", path.display()),
            source,
        )
        .with_context(ErrorContext::new(
            request_id,
            ErrorStage::CheckSummaryOutput,
            Some(path.to_path_buf()),
        ))),
    }
}

fn publish_manifest_with_hook(
    path: &Path,
    request_id: &str,
    manifest: &RunManifestV1,
    before_publish: impl FnOnce(&Path, &Path) -> io::Result<()>,
) -> Result<(), CostingError> {
    let mut bytes = serde_json::to_vec_pretty(manifest).map_err(|error| {
        CostingError::internal(format!("运行 Manifest 序列化失败: {error}")).with_context(
            ErrorContext::new(
                request_id,
                ErrorStage::BuildManifest,
                Some(path.to_path_buf()),
            ),
        )
    })?;
    bytes.push(b'\n');
    let mut atomic = AtomicFile::create(path, request_id)
        .map_err(|error| map_atomic_error(request_id, error))?;
    let staging_path = atomic.staging_path().to_path_buf();
    if let Err(source) = atomic.writer().write_all(&bytes) {
        return Err(write_failure(
            atomic,
            path,
            request_id,
            ErrorStage::WriteSummary,
            source,
        ));
    }
    if let Err(source) = before_publish(&staging_path, path) {
        return Err(write_failure(
            atomic,
            path,
            request_id,
            ErrorStage::WriteSummary,
            source,
        ));
    }
    atomic
        .publish()
        .map(|_| ())
        .map_err(|error| map_atomic_error(request_id, error))
}

fn write_failure(
    atomic: AtomicFile,
    final_path: &Path,
    request_id: &str,
    stage: ErrorStage,
    source: io::Error,
) -> CostingError {
    let cleanup = atomic.discard().err();
    let mut context = ErrorContext::new(request_id, stage, Some(final_path.to_path_buf()));
    context.details.io_meta = Some(IoFailureMeta::from(&source));
    if let Some(cleanup) = cleanup {
        let message = cleanup.to_string();
        context.details.cleanup_failures.push(CleanupFailureMeta {
            stage: ErrorStage::CleanupSummaryTempFile,
            path: cleanup.staging_path,
            io_meta: IoFailureMeta::from(&cleanup.source),
            message,
        });
    }
    CostingError::io_with_source(
        ErrorCode::OutputNotWritable,
        format!("写出运行 Manifest 失败: {source}"),
        source,
    )
    .with_context(context)
}

fn map_atomic_error(request_id: &str, error: AtomicFileError) -> CostingError {
    let AtomicFileError {
        stage,
        final_path,
        staging_path,
        cleanup_error,
        source,
        ..
    } = error;
    let error_stage = match stage {
        AtomicFileStage::CheckTarget => ErrorStage::CheckSummaryOutput,
        AtomicFileStage::PrepareParent => ErrorStage::PrepareSummaryDirectory,
        AtomicFileStage::CreateStaging => ErrorStage::CreateSummaryTempFile,
        AtomicFileStage::Flush => ErrorStage::FlushSummaryTempFile,
        AtomicFileStage::Sync => ErrorStage::SyncSummaryTempFile,
        AtomicFileStage::Publish => ErrorStage::PublishSummary,
        AtomicFileStage::Cleanup => ErrorStage::CleanupSummaryTempFile,
    };
    let code = if source.kind() == io::ErrorKind::AlreadyExists
        && matches!(
            error_stage,
            ErrorStage::CheckSummaryOutput | ErrorStage::PublishSummary
        ) {
        ErrorCode::OutputExists
    } else {
        ErrorCode::OutputNotWritable
    };
    let mut context = ErrorContext::new(request_id, error_stage, Some(final_path.clone()));
    context.details.io_meta = Some(IoFailureMeta::from(&source));
    if let Some(cleanup_error) = cleanup_error {
        context.details.cleanup_failures.push(CleanupFailureMeta {
            stage: ErrorStage::CleanupSummaryTempFile,
            path: staging_path,
            io_meta: IoFailureMeta::from(&cleanup_error),
            message: cleanup_error.to_string(),
        });
    }
    CostingError::io_with_source(
        code,
        if code == ErrorCode::OutputExists {
            format!("运行 Manifest 已存在: {}", final_path.display())
        } else {
            format!("原子发布运行 Manifest 失败: {source}")
        },
        source,
    )
    .with_context(context)
}

fn application_identity() -> ManifestApplication {
    let build = BuildInfo::current();
    ManifestApplication {
        name: build.name.to_string(),
        version: build.version.to_string(),
        git_commit: build.git_commit.to_string(),
        build_timestamp: build.build_timestamp.to_string(),
        rustc_version: build.rustc_version.to_string(),
        target: build.target.to_string(),
    }
}

fn required<'a, T>(value: &'a Option<T>, field: &str) -> Result<&'a T, CostingError> {
    value
        .as_ref()
        .ok_or_else(|| CostingError::internal(format!("运行 Manifest 缺少必需字段 {field}")))
}

fn rfc3339(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn present_path(path: &Path, cwd: &Path, redact: bool) -> String {
    if !redact {
        return path.display().to_string();
    }
    let cwd = absolute_lexical(cwd, cwd);
    let absolute = absolute_lexical(path, &cwd);
    if let Ok(relative) = absolute.strip_prefix(&cwd) {
        let rendered = relative.display().to_string();
        return if rendered.is_empty() {
            ".".to_string()
        } else {
            rendered
        };
    }
    path.file_name()
        .or_else(|| absolute.file_name())
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "redacted".to_string())
}

fn absolute_lexical(path: &Path, cwd: &Path) -> PathBuf {
    let candidate = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };
    if let Ok(canonical) = candidate.canonicalize() {
        return canonical;
    }
    let mut normalized = PathBuf::new();
    for component in candidate.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            _ => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

#[cfg(test)]
#[path = "manifest_tests.rs"]
mod tests;

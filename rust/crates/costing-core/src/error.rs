use std::path::{Path, PathBuf};

use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    InvalidInput,
    FileNotFound,
    FileNotReadable,
    UnsupportedFileType,
    OutputExists,
    OutputNotWritable,
    InsufficientDiskSpace,
    TempCleanupFailed,
    ReaderMismatch,
    EtlMismatch,
    AnalysisMismatch,
    WorkbookMismatch,
    PerformanceRegression,
    InternalError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ErrorStage {
    ValidateCliRequest,
    ResolveCliPaths,
    IngestWorkbook,
    Normalize,
    Split,
    BuildFact,
    BuildPresentation,
    PrepareOutputDirectory,
    CheckDiskSpace,
    CreateTempWorkspace,
    PlanSheet,
    InitializeLowMemoryTempWriter,
    PopulateWorkbook,
    CreateFinalOutput,
    SaveWorkbook,
    RemovePartialOutput,
    CleanupTempWorkspace,
    ReadOutputMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum IoKindCode {
    Interrupted,
    WouldBlock,
    TimedOut,
    AlreadyExists,
    PermissionDenied,
    InvalidInput,
    InvalidData,
    NotFound,
    StorageFull,
    Other,
}

impl From<std::io::ErrorKind> for IoKindCode {
    fn from(kind: std::io::ErrorKind) -> Self {
        match kind {
            std::io::ErrorKind::Interrupted => Self::Interrupted,
            std::io::ErrorKind::WouldBlock => Self::WouldBlock,
            std::io::ErrorKind::TimedOut => Self::TimedOut,
            std::io::ErrorKind::AlreadyExists => Self::AlreadyExists,
            std::io::ErrorKind::PermissionDenied => Self::PermissionDenied,
            std::io::ErrorKind::InvalidInput => Self::InvalidInput,
            std::io::ErrorKind::InvalidData => Self::InvalidData,
            std::io::ErrorKind::NotFound => Self::NotFound,
            std::io::ErrorKind::StorageFull => Self::StorageFull,
            _ => Self::Other,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct IoFailureMeta {
    #[serde(rename = "io_kind")]
    pub kind: IoKindCode,
    pub raw_os_error: Option<i32>,
}

impl From<&std::io::Error> for IoFailureMeta {
    fn from(error: &std::io::Error) -> Self {
        Self {
            kind: error.kind().into(),
            raw_os_error: error.raw_os_error(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CleanupFailureMeta {
    pub stage: ErrorStage,
    pub path: Option<PathBuf>,
    #[serde(flatten)]
    pub io_meta: IoFailureMeta,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ErrorDetails {
    pub stage: ErrorStage,
    pub path: Option<PathBuf>,
    #[serde(flatten)]
    pub io_meta: Option<IoFailureMeta>,
    pub final_output_valid: bool,
    pub partial_output_removed: Option<bool>,
    pub cleanup_failures: Vec<CleanupFailureMeta>,
}

impl ErrorDetails {
    pub fn new(stage: ErrorStage, path: Option<PathBuf>) -> Self {
        Self {
            stage,
            path,
            io_meta: None,
            final_output_valid: false,
            partial_output_removed: None,
            cleanup_failures: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorContext {
    pub request_id: String,
    pub details: ErrorDetails,
}

impl ErrorContext {
    pub fn new(request_id: impl Into<String>, stage: ErrorStage, path: Option<PathBuf>) -> Self {
        Self {
            request_id: request_id.into(),
            details: ErrorDetails::new(stage, path),
        }
    }
}

#[derive(Debug, Error)]
pub enum CostingError {
    #[error("{message}")]
    User {
        code: ErrorCode,
        message: String,
        retryable: bool,
    },
    #[error("{message}")]
    Io {
        code: ErrorCode,
        message: String,
        path: PathBuf,
        retryable: bool,
    },
    #[error("{message}")]
    Internal { code: ErrorCode, message: String },
    #[error("{message}")]
    IoSource {
        code: ErrorCode,
        message: String,
        retryable: bool,
        io_meta: IoFailureMeta,
        #[source]
        source: std::io::Error,
    },
    #[error("{message}")]
    Writer {
        code: ErrorCode,
        message: String,
        retryable: bool,
    },
    #[error("{source}")]
    Contextual {
        context: ErrorContext,
        #[source]
        source: Box<CostingError>,
    },
}

impl CostingError {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::User { code, .. }
            | Self::Io { code, .. }
            | Self::Internal { code, .. }
            | Self::IoSource { code, .. }
            | Self::Writer { code, .. } => *code,
            Self::Contextual { source, .. } => source.code(),
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::User { message, .. }
            | Self::Io { message, .. }
            | Self::Internal { message, .. }
            | Self::IoSource { message, .. }
            | Self::Writer { message, .. } => message,
            Self::Contextual { source, .. } => source.message(),
        }
    }

    pub fn retryable(&self) -> bool {
        match self {
            Self::User { retryable, .. }
            | Self::Io { retryable, .. }
            | Self::IoSource { retryable, .. }
            | Self::Writer { retryable, .. } => *retryable,
            Self::Internal { .. } => false,
            Self::Contextual { source, .. } => source.retryable(),
        }
    }

    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::User {
            code: ErrorCode::InvalidInput,
            message: message.into(),
            retryable: false,
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            code: ErrorCode::InternalError,
            message: message.into(),
        }
    }

    pub fn io(code: ErrorCode, message: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self::Io {
            code,
            message: message.into(),
            path: path.into(),
            retryable: false,
        }
    }

    pub fn io_with_source(
        code: ErrorCode,
        message: impl Into<String>,
        source: std::io::Error,
    ) -> Self {
        let io_meta = IoFailureMeta::from(&source);
        let retryable = retryable_io(&source);
        Self::IoSource {
            code,
            message: message.into(),
            retryable,
            io_meta,
            source,
        }
    }

    pub fn context(&self) -> Option<&ErrorContext> {
        match self {
            Self::Contextual { context, .. } => Some(context),
            _ => None,
        }
    }

    pub fn path(&self) -> Option<&Path> {
        match self {
            Self::Io { path, .. } => Some(path.as_path()),
            Self::Contextual { context, source } => {
                context.details.path.as_deref().or_else(|| source.path())
            }
            _ => None,
        }
    }

    pub fn io_meta(&self) -> Option<IoFailureMeta> {
        match self {
            Self::IoSource { io_meta, .. } => Some(*io_meta),
            Self::Contextual { source, .. } => source.io_meta(),
            _ => None,
        }
    }

    pub fn with_context(self, mut context: ErrorContext) -> Self {
        if matches!(self, Self::Contextual { .. }) {
            return self;
        }
        if context.details.io_meta.is_none() {
            context.details.io_meta = self.io_meta();
        }
        Self::Contextual {
            context,
            source: Box::new(self),
        }
    }
}

fn retryable_io(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::TimedOut
    ) || matches!(error.raw_os_error(), Some(32 | 33 | 39 | 112))
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io::ErrorKind;
    use std::path::PathBuf;

    use super::{CostingError, ErrorCode, ErrorContext, ErrorStage};

    #[test]
    fn contextual_io_error_preserves_source_chain_and_delegates() {
        let contextual = CostingError::io_with_source(
            ErrorCode::OutputNotWritable,
            "write failed",
            std::io::Error::from_raw_os_error(112),
        )
        .with_context(ErrorContext::new(
            "costing-test-1",
            ErrorStage::SaveWorkbook,
            Some(PathBuf::from("output.xlsx")),
        ));

        assert_eq!(contextual.code(), ErrorCode::OutputNotWritable);
        assert!(contextual.retryable());
        assert_eq!(contextual.context().unwrap().request_id, "costing-test-1");

        let inner = contextual.source().expect("contextual source");
        let io_error = inner
            .source()
            .expect("I/O source")
            .downcast_ref::<std::io::Error>()
            .expect("original std::io::Error");
        assert_eq!(io_error.kind(), ErrorKind::StorageFull);
        assert_eq!(io_error.raw_os_error(), Some(112));
    }

    #[test]
    fn timed_out_io_error_is_retryable_without_raw_os_error() {
        let error = CostingError::io_with_source(
            ErrorCode::OutputNotWritable,
            "timed out",
            std::io::Error::new(ErrorKind::TimedOut, "timed out"),
        );

        assert!(error.retryable());
        let io_error = error
            .source()
            .expect("I/O source")
            .downcast_ref::<std::io::Error>()
            .expect("original std::io::Error");
        assert_eq!(io_error.kind(), ErrorKind::TimedOut);
        assert_eq!(io_error.raw_os_error(), None);
    }

    #[test]
    fn with_context_leaves_already_contextual_error_unchanged() {
        let original =
            CostingError::internal("already contextual").with_context(ErrorContext::new(
                "original-request",
                ErrorStage::PopulateWorkbook,
                Some(PathBuf::from("original.xlsx")),
            ));

        let result = original.with_context(ErrorContext::new(
            "replacement-request",
            ErrorStage::SaveWorkbook,
            Some(PathBuf::from("replacement.xlsx")),
        ));

        let context = result.context().expect("existing context");
        assert_eq!(context.request_id, "original-request");
        assert_eq!(context.details.stage, ErrorStage::PopulateWorkbook);
        assert_eq!(context.details.path, Some(PathBuf::from("original.xlsx")));
    }

    #[test]
    fn error_path_prefers_outer_context_and_falls_back_to_source() {
        let direct = CostingError::io(
            ErrorCode::OutputExists,
            "output exists",
            PathBuf::from("source-output.xlsx"),
        );
        assert_eq!(
            direct.path(),
            Some(PathBuf::from("source-output.xlsx").as_path())
        );

        let inherited = direct.with_context(ErrorContext::new(
            "inherited-request",
            ErrorStage::ValidateCliRequest,
            None,
        ));
        assert_eq!(
            inherited.path(),
            Some(PathBuf::from("source-output.xlsx").as_path())
        );

        let outer = CostingError::io(
            ErrorCode::OutputExists,
            "output exists",
            PathBuf::from("source-output.xlsx"),
        )
        .with_context(ErrorContext::new(
            "outer-request",
            ErrorStage::ValidateCliRequest,
            Some(PathBuf::from("outer-output.xlsx")),
        ));
        assert_eq!(
            outer.path(),
            Some(PathBuf::from("outer-output.xlsx").as_path())
        );
    }
}

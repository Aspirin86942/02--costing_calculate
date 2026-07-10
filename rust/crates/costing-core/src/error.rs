use std::path::PathBuf;

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
    ReaderMismatch,
    EtlMismatch,
    AnalysisMismatch,
    WorkbookMismatch,
    PerformanceRegression,
    InternalError,
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
}

impl CostingError {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::User { code, .. } | Self::Io { code, .. } | Self::Internal { code, .. } => *code,
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::User { message, .. }
            | Self::Io { message, .. }
            | Self::Internal { message, .. } => message,
        }
    }

    pub fn retryable(&self) -> bool {
        match self {
            Self::User { retryable, .. } | Self::Io { retryable, .. } => *retryable,
            Self::Internal { .. } => false,
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
}

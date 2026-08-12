//! Same-directory, durable, no-overwrite file publication.

use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use tempfile::{Builder, NamedTempFile};

/// Stable stage at which atomic file publication failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtomicFileStage {
    /// The final path was checked before staging work began.
    CheckTarget,
    /// The final path's parent directory was prepared.
    PrepareParent,
    /// The same-directory staging file was created.
    CreateStaging,
    /// Buffered staging bytes were flushed.
    Flush,
    /// Staging file content was synchronized to the filesystem.
    Sync,
    /// The staging file was published without replacing the final path.
    Publish,
    /// A staging file was explicitly discarded after another failure.
    Cleanup,
}

/// Failure produced by an atomic file operation.
#[derive(Debug, thiserror::Error)]
#[error("atomic file {stage:?} failed for {}: {source}", final_path.display())]
pub struct AtomicFileError {
    /// Failed operation.
    pub stage: AtomicFileStage,
    /// Intended final path.
    pub final_path: PathBuf,
    /// Same-directory staging path, when one had been created.
    pub staging_path: Option<PathBuf>,
    /// Whether the complete final file was already published.
    pub final_published: bool,
    /// Secondary failure while cleaning the staging file.
    pub cleanup_error: Option<io::Error>,
    /// Primary filesystem failure.
    #[source]
    pub source: io::Error,
}

/// Unique same-directory file that becomes visible only through no-overwrite publication.
pub struct AtomicFile {
    inner: NamedTempFile,
    final_path: PathBuf,
}

impl AtomicFile {
    /// Create a unique staging file beside `final_path`.
    ///
    /// The method rejects an already existing final path, while `publish`
    /// performs the authoritative no-overwrite check again to close the race.
    pub fn create(final_path: &Path, request_id: &str) -> Result<Self, AtomicFileError> {
        if final_path
            .try_exists()
            .map_err(|source| error(AtomicFileStage::CheckTarget, final_path, None, source))?
        {
            return Err(error(
                AtomicFileStage::CheckTarget,
                final_path,
                None,
                io::Error::new(io::ErrorKind::AlreadyExists, "final path already exists"),
            ));
        }

        let parent = final_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)
            .map_err(|source| error(AtomicFileStage::PrepareParent, final_path, None, source))?;
        let prefix = format!(".costing-publish-{}-", sanitize_request_id(request_id));
        let inner = Builder::new()
            .prefix(&prefix)
            .suffix(".tmp")
            .tempfile_in(parent)
            .map_err(|source| error(AtomicFileStage::CreateStaging, final_path, None, source))?;
        Ok(Self {
            inner,
            final_path: final_path.to_path_buf(),
        })
    }

    /// Return the path of the currently private staging file.
    #[must_use]
    pub fn staging_path(&self) -> &Path {
        self.inner.path()
    }

    /// Return the staging file writer.
    pub fn writer(&mut self) -> &mut File {
        self.inner.as_file_mut()
    }

    /// Flush, synchronize and publish the complete file without replacement.
    pub fn publish(mut self) -> Result<File, AtomicFileError> {
        if let Err(source) = self.inner.as_file_mut().flush() {
            return Err(self.fail_and_cleanup(AtomicFileStage::Flush, source));
        }
        if let Err(source) = self.inner.as_file().sync_all() {
            return Err(self.fail_and_cleanup(AtomicFileStage::Sync, source));
        }

        let staging_path = self.inner.path().to_path_buf();
        match self.inner.persist_noclobber(&self.final_path) {
            Ok(file) => Ok(file),
            Err(persist_error) => {
                let source = persist_error.error;
                let cleanup_error = persist_error.file.close().err();
                Err(AtomicFileError {
                    stage: AtomicFileStage::Publish,
                    final_path: self.final_path,
                    staging_path: Some(staging_path),
                    final_published: false,
                    cleanup_error,
                    source,
                })
            }
        }
    }

    /// Explicitly discard the staging file and report cleanup failure.
    pub fn discard(self) -> Result<(), AtomicFileError> {
        let staging_path = self.inner.path().to_path_buf();
        self.inner.close().map_err(|source| AtomicFileError {
            stage: AtomicFileStage::Cleanup,
            final_path: self.final_path,
            staging_path: Some(staging_path),
            final_published: false,
            cleanup_error: None,
            source,
        })
    }

    fn fail_and_cleanup(self, stage: AtomicFileStage, source: io::Error) -> AtomicFileError {
        let staging_path = self.inner.path().to_path_buf();
        let cleanup_error = self.inner.close().err();
        AtomicFileError {
            stage,
            final_path: self.final_path,
            staging_path: Some(staging_path),
            final_published: false,
            cleanup_error,
            source,
        }
    }
}

fn sanitize_request_id(request_id: &str) -> String {
    let sanitized = request_id
        .chars()
        .take(48)
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "request".to_string()
    } else {
        sanitized
    }
}

fn error(
    stage: AtomicFileStage,
    final_path: &Path,
    staging_path: Option<PathBuf>,
    source: io::Error,
) -> AtomicFileError {
    AtomicFileError {
        stage,
        final_path: final_path.to_path_buf(),
        staging_path,
        final_published: false,
        cleanup_error: None,
        source,
    }
}

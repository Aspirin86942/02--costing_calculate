use std::fmt::Write;
use std::path::{Path, PathBuf};

use costing_core::error::{ErrorContext, ErrorStage};
use costing_core::{CostingError, ErrorCode, PipelineName, PipelineRules};
use sha2::{Digest, Sha256};

use super::{
    validate_file_config, ConfigSource, EffectiveConfigDocument, EffectiveConfigView,
    EffectiveConfiguration, FieldSource, FileConfigV1, SemanticConfigView, SourcedValue,
    DEFAULT_CONFIG,
};

#[derive(Debug)]
pub(crate) struct LoadedConfiguration {
    file: FileConfigV1,
    source: ConfigSource,
    source_sha256: Option<String>,
}

impl LoadedConfiguration {
    pub fn for_pipeline(
        &self,
        pipeline: PipelineName,
    ) -> Result<EffectiveConfiguration, CostingError> {
        let configured = self.file.pipeline(pipeline);
        let semantic = SemanticConfigView {
            schema_version: self.file.schema_version,
            pipeline,
            input_pattern: &configured.input_pattern,
            standalone_cost_items: &configured.standalone_cost_items,
            product_order: &configured.product_order,
        };
        let canonical_json = serde_json::to_vec(&semantic)
            .map_err(|error| CostingError::internal(format!("有效配置序列化失败: {error}")))?;
        let effective_sha256 = sha256_hex(&canonical_json);
        let maintainable_source = match self.source {
            ConfigSource::EmbeddedDefault => FieldSource::EmbeddedDefault,
            ConfigSource::External => FieldSource::External,
        };
        let document = EffectiveConfigDocument {
            status: "valid",
            pipeline,
            schema_version: self.file.schema_version,
            source: self.source,
            effective_sha256,
            source_sha256: self.source_sha256.clone(),
            config: EffectiveConfigView {
                input_pattern: SourcedValue {
                    value: configured.input_pattern.clone(),
                    source: maintainable_source,
                },
                standalone_cost_items: SourcedValue {
                    value: configured.standalone_cost_items.clone(),
                    source: FieldSource::Sealed,
                },
                product_order: SourcedValue {
                    value: configured.product_order.clone(),
                    source: maintainable_source,
                },
            },
        };
        let rules = PipelineRules {
            name: pipeline,
            product_order: configured
                .product_order
                .iter()
                .map(|item| (item.code.clone(), item.name.clone()))
                .collect(),
            standalone_cost_items: configured.standalone_cost_items.clone(),
        };
        Ok(EffectiveConfiguration {
            input_pattern: configured.input_pattern.clone(),
            rules,
            document,
        })
    }
}

pub(crate) fn load_configuration(
    path: Option<&Path>,
    request_id: &str,
) -> Result<LoadedConfiguration, CostingError> {
    let (bytes, source, error_path) = match path {
        Some(path) => {
            let bytes = std::fs::read(path).map_err(|error| {
                contextual(
                    CostingError::io_with_source(
                        ErrorCode::InvalidConfig,
                        format!("无法读取配置文件: {error}"),
                        error,
                    ),
                    request_id,
                    ErrorStage::LoadConfig,
                    Some(path.to_path_buf()),
                )
            })?;
            (bytes, ConfigSource::External, Some(path.to_path_buf()))
        }
        None => (
            DEFAULT_CONFIG.as_bytes().to_vec(),
            ConfigSource::EmbeddedDefault,
            None,
        ),
    };
    let text = std::str::from_utf8(&bytes).map_err(|error| {
        contextual(
            CostingError::invalid_config(format!("配置文件不是合法 UTF-8: {error}")),
            request_id,
            ErrorStage::LoadConfig,
            error_path.clone(),
        )
    })?;
    let file: FileConfigV1 = toml::from_str(text).map_err(|error| {
        contextual(
            CostingError::invalid_config(format!("TOML 配置解析失败: {error}")),
            request_id,
            ErrorStage::ParseConfig,
            error_path.clone(),
        )
    })?;
    validate_file_config(&file).map_err(|message| {
        contextual(
            CostingError::invalid_config(format!("配置校验失败: {message}")),
            request_id,
            ErrorStage::ValidateConfig,
            error_path.clone(),
        )
    })?;
    let source_sha256 = (source == ConfigSource::External).then(|| sha256_hex(&bytes));
    Ok(LoadedConfiguration {
        file,
        source,
        source_sha256,
    })
}

fn contextual(
    error: CostingError,
    request_id: &str,
    stage: ErrorStage,
    path: Option<PathBuf>,
) -> CostingError {
    let diagnostic_path = path.and_then(|path| path.file_name().map(PathBuf::from));
    error.with_context(ErrorContext::new(request_id, stage, diagnostic_path))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_hash_excludes_source_and_raw_toml_layout() {
        let embedded = load_configuration(None, "embedded").unwrap();
        let mut text = DEFAULT_CONFIG.replacen(
            "input_pattern = \"gb-*.xlsx\"\nstandalone_cost_items = [\"委外加工费\"]",
            "standalone_cost_items = [\"委外加工费\"]\ninput_pattern = \"gb-*.xlsx\"",
            1,
        );
        text.push_str("\n# semantically irrelevant comment\n");
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "costing-config-hash-{}-{nanos}.toml",
            std::process::id()
        ));
        std::fs::write(&path, text).unwrap();
        let external = load_configuration(Some(&path), "external").unwrap();

        let embedded = embedded.for_pipeline(PipelineName::Gb).unwrap();
        let external = external.for_pipeline(PipelineName::Gb).unwrap();
        assert_eq!(
            embedded.document.effective_sha256,
            external.document.effective_sha256
        );
        assert_eq!(embedded.document.source_sha256, None);
        assert_ne!(external.document.source_sha256, None);
        let _ = std::fs::remove_file(path);
    }
}

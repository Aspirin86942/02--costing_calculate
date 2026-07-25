use costing_core::{PipelineName, PipelineRules};
use serde::{Deserialize, Serialize};

/// Configuration schema version supported by this executable.
pub const CONFIG_SCHEMA_VERSION: u32 = 1;

/// Complete version-one configuration file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfigV1 {
    /// Schema version declared by the file.
    pub schema_version: u32,
    /// Rules declared for every supported production pipeline.
    pub pipelines: FilePipelinesV1,
}

impl FileConfigV1 {
    /// Return the file rules for a selected pipeline.
    pub fn pipeline(&self, name: PipelineName) -> &FilePipelineV1 {
        match name {
            PipelineName::Gb => &self.pipelines.gb,
            PipelineName::Sk => &self.pipelines.sk,
        }
    }
}

/// Required GB and SK sections in a version-one configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilePipelinesV1 {
    /// GB configuration.
    pub gb: FilePipelineV1,
    /// SK configuration.
    pub sk: FilePipelineV1,
}

/// Externally maintainable and explicitly sealed fields for one pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilePipelineV1 {
    /// Safe basename glob used only by application-layer input discovery.
    pub input_pattern: String,
    /// Explicit declaration of the pipeline's sealed standalone-cost contract.
    pub standalone_cost_items: Vec<String>,
    /// Ordered product identities shown on the analysis sheet.
    pub product_order: Vec<ProductOrderEntryV1>,
}

/// One product identity and its explicit display position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProductOrderEntryV1 {
    /// Exact product code.
    pub code: String,
    /// Exact product name.
    pub name: String,
    /// Strictly increasing order value matching the array position.
    pub display_order: u32,
}

/// Origin of the complete loaded configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ConfigSource {
    /// Reviewed configuration embedded at compile time.
    EmbeddedDefault,
    /// Complete replacement configuration loaded from a user-supplied file.
    External,
}

/// Origin or governance class of an effective field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum FieldSource {
    /// Value maintained by the embedded default.
    EmbeddedDefault,
    /// Value maintained by the external configuration.
    External,
    /// Value declared in the file but constrained to the frozen contract.
    Sealed,
}

/// A diagnostic value paired with its source classification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourcedValue<T> {
    /// Effective semantic value.
    pub value: T,
    /// Source or governance class of the value.
    pub source: FieldSource,
}

/// Complete effective view for one selected pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EffectiveConfigView {
    /// Application-layer input discovery glob and its source.
    pub input_pattern: SourcedValue<String>,
    /// Frozen standalone-cost items and their sealed classification.
    pub standalone_cost_items: SourcedValue<Vec<String>>,
    /// Product display rules and their source.
    pub product_order: SourcedValue<Vec<ProductOrderEntryV1>>,
}

/// Reader-facing effective configuration diagnostic document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EffectiveConfigDocument {
    /// Stable success status.
    pub status: &'static str,
    /// Selected pipeline.
    pub pipeline: PipelineName,
    /// Validated schema version.
    pub schema_version: u32,
    /// Origin of the complete configuration.
    pub source: ConfigSource,
    /// SHA-256 of canonical semantic values for the selected pipeline.
    pub effective_sha256: String,
    /// SHA-256 of external source bytes, or `None` for embedded defaults.
    pub source_sha256: Option<String>,
    /// Effective values and their field-level sources.
    pub config: EffectiveConfigView,
}

/// Compact successful result emitted by `--validate-config`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConfigValidationRecord {
    /// Stable success status.
    pub status: &'static str,
    /// Selected pipeline.
    pub pipeline: PipelineName,
    /// Validated schema version.
    pub schema_version: u32,
    /// Origin of the complete configuration.
    pub source: ConfigSource,
    /// SHA-256 of canonical semantic values for the selected pipeline.
    pub effective_sha256: String,
    /// SHA-256 of external source bytes, or `None` for embedded defaults.
    pub source_sha256: Option<String>,
}

#[derive(Debug)]
pub(crate) struct EffectiveConfiguration {
    pub input_pattern: String,
    pub rules: PipelineRules,
    pub document: EffectiveConfigDocument,
}

impl EffectiveConfiguration {
    pub fn validation_record(&self) -> ConfigValidationRecord {
        ConfigValidationRecord {
            status: self.document.status,
            pipeline: self.document.pipeline,
            schema_version: self.document.schema_version,
            source: self.document.source,
            effective_sha256: self.document.effective_sha256.clone(),
            source_sha256: self.document.source_sha256.clone(),
        }
    }
}

#[derive(Serialize)]
pub(crate) struct SemanticConfigView<'a> {
    pub schema_version: u32,
    pub pipeline: PipelineName,
    pub input_pattern: &'a str,
    pub standalone_cost_items: &'a [String],
    pub product_order: &'a [ProductOrderEntryV1],
}

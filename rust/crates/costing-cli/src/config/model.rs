use costing_core::{PipelineName, PipelineRules};
use serde::{Deserialize, Serialize};

pub const CONFIG_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfigV1 {
    pub schema_version: u32,
    pub pipelines: FilePipelinesV1,
}

impl FileConfigV1 {
    pub fn pipeline(&self, name: PipelineName) -> &FilePipelineV1 {
        match name {
            PipelineName::Gb => &self.pipelines.gb,
            PipelineName::Sk => &self.pipelines.sk,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilePipelinesV1 {
    pub gb: FilePipelineV1,
    pub sk: FilePipelineV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilePipelineV1 {
    pub input_pattern: String,
    pub standalone_cost_items: Vec<String>,
    pub product_order: Vec<ProductOrderEntryV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProductOrderEntryV1 {
    pub code: String,
    pub name: String,
    pub display_order: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ConfigSource {
    EmbeddedDefault,
    External,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum FieldSource {
    EmbeddedDefault,
    External,
    Sealed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourcedValue<T> {
    pub value: T,
    pub source: FieldSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EffectiveConfigView {
    pub input_pattern: SourcedValue<String>,
    pub standalone_cost_items: SourcedValue<Vec<String>>,
    pub product_order: SourcedValue<Vec<ProductOrderEntryV1>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EffectiveConfigDocument {
    pub status: &'static str,
    pub pipeline: PipelineName,
    pub schema_version: u32,
    pub source: ConfigSource,
    pub effective_sha256: String,
    pub source_sha256: Option<String>,
    pub config: EffectiveConfigView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConfigValidationRecord {
    pub status: &'static str,
    pub pipeline: PipelineName,
    pub schema_version: u32,
    pub source: ConfigSource,
    pub effective_sha256: String,
    pub source_sha256: Option<String>,
}

#[derive(Debug)]
pub(crate) struct EffectiveConfiguration {
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

//! Versioned configuration loading, validation and diagnostic models.

mod loader;
mod model;
mod validation;

/// Complete reviewed configuration embedded into the executable.
pub const DEFAULT_CONFIG: &str = include_str!("../../config/costing.default.toml");

pub use model::{
    ConfigSource, ConfigValidationRecord, EffectiveConfigDocument, EffectiveConfigView,
    FieldSource, FileConfigV1, FilePipelineV1, FilePipelinesV1, ProductOrderEntryV1, SourcedValue,
    CONFIG_SCHEMA_VERSION,
};

pub(crate) use loader::load_configuration;
pub(crate) use model::{EffectiveConfiguration, SemanticConfigView};
pub(crate) use validation::{input_pattern_matches, validate_file_config};

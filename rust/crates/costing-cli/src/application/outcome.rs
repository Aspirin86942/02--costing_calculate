use costing_core::model::ErrorSummary;
use costing_core::RunSummary;

use crate::config::{ConfigValidationRecord, EffectiveConfigDocument};

pub type RunRecord = RunSummary;
pub type FailureRecord = ErrorSummary;

#[derive(Debug, Clone, PartialEq)]
pub enum RunOutcome {
    Succeeded(RunRecord),
    ConfigValidated(ConfigValidationRecord),
    EffectiveConfig(EffectiveConfigDocument),
    Failed(FailureRecord),
}

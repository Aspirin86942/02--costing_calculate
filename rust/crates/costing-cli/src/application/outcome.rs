use costing_core::model::ErrorSummary;
use costing_core::RunSummary;

pub type RunRecord = RunSummary;
pub type FailureRecord = ErrorSummary;

#[derive(Debug, Clone, PartialEq)]
pub enum RunOutcome {
    Succeeded(RunRecord),
    Failed(FailureRecord),
}

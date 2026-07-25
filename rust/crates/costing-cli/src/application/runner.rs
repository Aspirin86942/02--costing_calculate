use costing_core::model::ErrorSummary;
use costing_core::{CostingError, ErrorCode};

use super::{RunOutcome, RunRequest};

#[must_use]
pub fn execute(request: RunRequest) -> RunOutcome {
    match crate::run::run(request) {
        Ok(summary) => RunOutcome::Succeeded(summary),
        Err(error) => {
            let failure = error
                .downcast_ref::<CostingError>()
                .map(ErrorSummary::from_error)
                .unwrap_or_else(|| ErrorSummary {
                    status: "failed".to_string(),
                    code: ErrorCode::InternalError,
                    message: error.to_string(),
                    retryable: false,
                    request_id: None,
                    details: None,
                });
            RunOutcome::Failed(failure)
        }
    }
}

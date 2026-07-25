use std::time::{SystemTime, UNIX_EPOCH};

use costing_core::error::{ErrorContext, ErrorStage};
use costing_core::model::ErrorSummary;
use costing_core::{CostingError, ErrorCode};

use super::{RunOperation, RunOutcome, RunRequest};

#[must_use]
pub fn execute(request: RunRequest) -> RunOutcome {
    let request_id = new_request_id();
    let loaded = match crate::config::load_configuration(request.config.as_deref(), &request_id) {
        Ok(loaded) => loaded,
        Err(error) => return RunOutcome::Failed(failure_from_error(&error)),
    };
    let effective = match loaded.for_pipeline(request.pipeline) {
        Ok(effective) => effective,
        Err(error) => {
            let error = error.with_context(ErrorContext::new(
                &request_id,
                ErrorStage::ValidateConfig,
                request.config.clone(),
            ));
            return RunOutcome::Failed(failure_from_error(&error));
        }
    };
    match request.operation {
        RunOperation::ValidateConfig => RunOutcome::ConfigValidated(effective.validation_record()),
        RunOperation::PrintEffectiveConfig => RunOutcome::EffectiveConfig(effective.document),
        RunOperation::Execute => {
            match crate::run::run(
                request,
                effective.rules,
                effective.input_pattern,
                request_id,
            ) {
                Ok(summary) => RunOutcome::Succeeded(summary),
                Err(error) => RunOutcome::Failed(failure_from_anyhow(&error)),
            }
        }
    }
}

fn failure_from_anyhow(error: &anyhow::Error) -> ErrorSummary {
    error
        .downcast_ref::<CostingError>()
        .map(ErrorSummary::from_error)
        .unwrap_or_else(|| ErrorSummary {
            status: "failed".to_string(),
            code: ErrorCode::InternalError,
            message: error.to_string(),
            retryable: false,
            request_id: None,
            details: None,
        })
}

fn failure_from_error(error: &CostingError) -> ErrorSummary {
    ErrorSummary::from_error(error)
}

fn new_request_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("costing-{}-{nanos}", std::process::id())
}

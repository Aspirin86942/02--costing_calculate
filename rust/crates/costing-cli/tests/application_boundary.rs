use std::path::PathBuf;

use costing_calculate::application::{execute, RunOutcome, RunRequest};
use costing_core::error::ErrorStage;
use costing_core::{ErrorCode, PipelineName};

#[test]
fn execute_returns_a_typed_failure_for_an_invalid_request() {
    let missing_input = PathBuf::from("definitely-missing-costing-input.xlsx");
    let request = RunRequest {
        pipeline: PipelineName::Gb,
        input: Some(missing_input.clone()),
        output: None,
        month_start: None,
        month_end: None,
        check_only: true,
        benchmark: false,
    };

    let outcome = execute(request);

    let RunOutcome::Failed(failure) = outcome else {
        panic!("missing input must return RunOutcome::Failed");
    };
    assert_eq!(failure.code, ErrorCode::FileNotFound);
    assert!(failure.request_id.is_some());
    let details = failure.details.expect("application failure details");
    assert_eq!(details.stage, ErrorStage::ValidateCliRequest);
    assert_eq!(details.path, Some(missing_input));
}

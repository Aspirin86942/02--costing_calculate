use std::collections::BTreeMap;

use costing_core::{model::QualityMetric, RunSummary, StageTimings};
use costing_oracle_tests::{compare_runtime_contract, OracleRuntimeContract};

fn summary(
    error_log_count: usize,
    issue_type_counts: &[(&str, usize)],
    quality_metrics: &[(&str, &str, &str)],
) -> RunSummary {
    RunSummary {
        status: "success".to_owned(),
        pipeline: "gb".to_owned(),
        output_written: false,
        workbook_path: None,
        sheet_count: 3,
        error_log_count,
        issue_type_counts: issue_type_counts
            .iter()
            .map(|(issue_type, count)| ((*issue_type).to_owned(), *count))
            .collect(),
        quality_metrics: quality_metrics
            .iter()
            .map(|(category, metric, value)| QualityMetric {
                category: (*category).to_owned(),
                metric: (*metric).to_owned(),
                value: (*value).to_owned(),
                description: format!("{category}:{metric}"),
            })
            .collect(),
        run_counts: BTreeMap::from([("reader_rows".to_owned(), 10)]),
        stage_timings: StageTimings {
            stages: BTreeMap::from([("reader".to_owned(), 0.1)]),
        },
    }
}

fn expected_contract() -> OracleRuntimeContract {
    OracleRuntimeContract {
        error_log_count: 2,
        issue_type_counts: BTreeMap::from([
            ("MISSING_AMOUNT".to_owned(), 1),
            ("TOTAL_COST_MISMATCH".to_owned(), 1),
        ]),
        quality_metrics: BTreeMap::from([(
            ("coverage".to_owned(), "analysis_rate".to_owned()),
            "0.95".to_owned(),
        )]),
    }
}

#[test]
fn snapshot_is_built_from_a_complete_run_summary() {
    let actual = OracleRuntimeContract::try_from(&summary(
        2,
        &[("MISSING_AMOUNT", 1), ("TOTAL_COST_MISMATCH", 1)],
        &[("coverage", "analysis_rate", "0.95")],
    ))
    .expect("valid summary should build an oracle snapshot");

    assert_eq!(actual, expected_contract());
}

#[test]
fn comparison_allows_rust_only_quality_metrics() {
    let actual = OracleRuntimeContract::try_from(&summary(
        2,
        &[("MISSING_AMOUNT", 1), ("TOTAL_COST_MISMATCH", 1)],
        &[
            ("coverage", "analysis_rate", "0.95"),
            ("rust", "extra_metric", "1"),
        ],
    ))
    .expect("valid summary should build an oracle snapshot");

    assert!(compare_runtime_contract(&expected_contract(), &actual).passed());
}

#[test]
fn comparison_reports_error_count_and_issue_type_mismatches() {
    let actual = OracleRuntimeContract {
        error_log_count: 3,
        issue_type_counts: BTreeMap::from([("MISSING_AMOUNT".to_owned(), 3)]),
        quality_metrics: expected_contract().quality_metrics,
    };

    let comparison = compare_runtime_contract(&expected_contract(), &actual);

    assert!(!comparison.passed());
    assert!(comparison
        .errors
        .iter()
        .any(|error| error.contains("error_log_count")));
    assert!(comparison
        .errors
        .iter()
        .any(|error| error.contains("issue_type_counts")));
}

#[test]
fn comparison_reports_missing_and_mismatched_required_quality_metrics() {
    let actual = OracleRuntimeContract {
        error_log_count: 2,
        issue_type_counts: expected_contract().issue_type_counts,
        quality_metrics: BTreeMap::from([(
            ("coverage".to_owned(), "analysis_rate".to_owned()),
            "0.90".to_owned(),
        )]),
    };
    let expected = OracleRuntimeContract {
        quality_metrics: BTreeMap::from([
            (
                ("coverage".to_owned(), "analysis_rate".to_owned()),
                "0.95".to_owned(),
            ),
            (
                ("null_rate".to_owned(), "amount".to_owned()),
                "0".to_owned(),
            ),
        ]),
        ..expected_contract()
    };

    let comparison = compare_runtime_contract(&expected, &actual);

    assert!(!comparison.passed());
    assert!(comparison
        .errors
        .iter()
        .any(|error| error.contains("mismatched required quality metric")));
    assert!(comparison
        .errors
        .iter()
        .any(|error| error.contains("missing required quality metric")));
}

#[test]
fn snapshot_rejects_issue_counts_that_do_not_match_error_count() {
    let error = OracleRuntimeContract::try_from(&summary(
        2,
        &[("MISSING_AMOUNT", 1)],
        &[("coverage", "analysis_rate", "0.95")],
    ))
    .expect_err("inconsistent issue counts must be rejected");

    assert!(error.contains("issue_type_counts sum"));
}

#[test]
fn snapshot_rejects_duplicate_quality_metrics() {
    let error = OracleRuntimeContract::try_from(&summary(
        2,
        &[("MISSING_AMOUNT", 1), ("TOTAL_COST_MISMATCH", 1)],
        &[
            ("coverage", "analysis_rate", "0.95"),
            ("coverage", "analysis_rate", "0.95"),
        ],
    ))
    .expect_err("duplicate quality metric keys must be rejected");

    assert!(error.contains("duplicate quality metric"));
}

//! Oracle contract support for comparing Python and Rust runtime summaries.

use std::collections::BTreeMap;

use costing_core::RunSummary;

pub type QualityMetricKey = (String, String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleRuntimeContract {
    pub error_log_count: usize,
    pub issue_type_counts: BTreeMap<String, usize>,
    pub quality_metrics: BTreeMap<QualityMetricKey, String>,
}

impl TryFrom<&RunSummary> for OracleRuntimeContract {
    type Error = String;

    fn try_from(summary: &RunSummary) -> Result<Self, Self::Error> {
        let issue_count_sum =
            summary
                .issue_type_counts
                .values()
                .try_fold(0_usize, |sum, count| {
                    sum.checked_add(*count)
                        .ok_or_else(|| "issue_type_counts sum overflowed usize".to_owned())
                })?;
        if issue_count_sum != summary.error_log_count {
            return Err(format!(
                "issue_type_counts sum ({issue_count_sum}) does not equal error_log_count ({})",
                summary.error_log_count
            ));
        }

        let mut quality_metrics = BTreeMap::new();
        for metric in &summary.quality_metrics {
            let key = (metric.category.clone(), metric.metric.clone());
            // 同一指标键会掩盖跨运行时差异，必须在快照边界拒绝。
            if quality_metrics
                .insert(key.clone(), metric.value.clone())
                .is_some()
            {
                return Err(format!(
                    "duplicate quality metric for category {:?} and metric {:?}",
                    key.0, key.1
                ));
            }
        }

        Ok(Self {
            error_log_count: summary.error_log_count,
            issue_type_counts: summary.issue_type_counts.clone(),
            quality_metrics,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeContractComparison {
    pub errors: Vec<String>,
}

impl RuntimeContractComparison {
    pub fn passed(&self) -> bool {
        self.errors.is_empty()
    }
}

pub fn compare_runtime_contract(
    expected: &OracleRuntimeContract,
    actual: &OracleRuntimeContract,
) -> RuntimeContractComparison {
    let mut errors = Vec::new();

    if expected.error_log_count != actual.error_log_count {
        errors.push(format!(
            "error_log_count mismatch: expected {}, actual {}",
            expected.error_log_count, actual.error_log_count
        ));
    }

    if expected.issue_type_counts != actual.issue_type_counts {
        errors.push(format!(
            "issue_type_counts mismatch: expected {:?}, actual {:?}",
            expected.issue_type_counts, actual.issue_type_counts
        ));
    }

    for (key, expected_value) in &expected.quality_metrics {
        match actual.quality_metrics.get(key) {
            None => errors.push(format!(
                "missing required quality metric: category {:?}, metric {:?}",
                key.0, key.1
            )),
            Some(actual_value) if actual_value != expected_value => errors.push(format!(
                "mismatched required quality metric: category {:?}, metric {:?}, expected {:?}, actual {:?}",
                key.0, key.1, expected_value, actual_value
            )),
            Some(_) => {}
        }
    }

    RuntimeContractComparison { errors }
}

use crate::model::{FactBundle, QualityMetric};

pub fn build_quality_metrics(bundle: &FactBundle) -> Vec<QualityMetric> {
    vec![
        QualityMetric {
            category: "行数勾稽".to_string(),
            metric: "成本明细行数".to_string(),
            value: bundle.detail_fact.len().to_string(),
            description: "Rust detail fact rows".to_string(),
        },
        QualityMetric {
            category: "行数勾稽".to_string(),
            metric: "数量页行数".to_string(),
            value: bundle.qty_fact.len().to_string(),
            description: "Rust qty fact rows".to_string(),
        },
    ]
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::model::{FactBundle, TableRow};

    use super::*;

    #[test]
    fn quality_metrics_report_fact_row_counts() {
        let bundle = FactBundle {
            detail_columns: Vec::new(),
            detail_fact: vec![TableRow {
                values: BTreeMap::new(),
            }],
            qty_columns: Vec::new(),
            qty_fact: vec![
                TableRow {
                    values: BTreeMap::new(),
                },
                TableRow {
                    values: BTreeMap::new(),
                },
            ],
            work_order_fact: Vec::new(),
            error_issues: Vec::new(),
        };

        let metrics = build_quality_metrics(&bundle);
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0].value, "1");
        assert_eq!(metrics[1].value, "2");
    }
}

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

pub fn weighted_median(values: &[(Decimal, Decimal)]) -> Option<Decimal> {
    let mut valid = values
        .iter()
        .copied()
        .filter(|(_, weight)| *weight > Decimal::ZERO)
        .collect::<Vec<_>>();
    if valid.is_empty() {
        return None;
    }

    valid.sort_by(|left, right| left.0.cmp(&right.0));
    let total_weight = valid.iter().map(|(_, weight)| *weight).sum::<Decimal>();
    let midpoint = total_weight / Decimal::new(2, 0);
    let mut cumulative = Decimal::ZERO;
    for (value, weight) in valid {
        cumulative += weight;
        // Python uses np.searchsorted(..., side="right"), so exact half falls to the right value.
        if cumulative > midpoint {
            return Some(value);
        }
    }
    None
}

pub fn weighted_mad(values: &[(Decimal, Decimal)], median: Decimal) -> Option<Decimal> {
    let deviations = values
        .iter()
        .map(|(value, weight)| (decimal_abs(*value - median), *weight))
        .collect::<Vec<_>>();
    weighted_median(&deviations)
}

pub fn grade_score(score: Option<Decimal>) -> &'static str {
    let Some(score) = score else {
        return "";
    };
    let abs_score = decimal_abs(score);
    if abs_score > Decimal::new(35, 1) {
        "高度可疑"
    } else if abs_score > Decimal::new(25, 1) {
        "关注"
    } else {
        "正常"
    }
}

pub fn decimal_ln(value: Decimal) -> Option<f64> {
    value.to_f64().filter(|number| *number > 0.0).map(f64::ln)
}

fn decimal_abs(value: Decimal) -> Decimal {
    if value < Decimal::ZERO {
        -value
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    #[test]
    fn weighted_median_uses_weights() {
        let values = vec![
            (Decimal::new(1, 0), Decimal::new(1, 0)),
            (Decimal::new(10, 0), Decimal::new(10, 0)),
            (Decimal::new(100, 0), Decimal::new(1, 0)),
        ];

        assert_eq!(weighted_median(&values), Some(Decimal::new(10, 0)));
    }

    #[test]
    fn weighted_median_uses_python_right_cutoff_on_exact_half() {
        let values = vec![
            (Decimal::new(1, 0), Decimal::new(1, 0)),
            (Decimal::new(2, 0), Decimal::new(1, 0)),
        ];

        assert_eq!(weighted_median(&values), Some(Decimal::new(2, 0)));
    }

    #[test]
    fn weighted_mad_uses_weighted_absolute_deviations() {
        let values = vec![
            (Decimal::new(1, 0), Decimal::new(1, 0)),
            (Decimal::new(10, 0), Decimal::new(10, 0)),
            (Decimal::new(100, 0), Decimal::new(1, 0)),
        ];

        assert_eq!(
            weighted_mad(&values, Decimal::new(10, 0)),
            Some(Decimal::ZERO)
        );
    }

    #[test]
    fn weighted_median_ignores_non_positive_weights() {
        let values = vec![
            (Decimal::new(1, 0), Decimal::new(0, 0)),
            (Decimal::new(2, 0), Decimal::new(-1, 0)),
            (Decimal::new(3, 0), Decimal::new(1, 0)),
        ];

        assert_eq!(weighted_median(&values), Some(Decimal::new(3, 0)));
    }

    #[test]
    fn grade_score_matches_contract_thresholds() {
        assert_eq!(grade_score(None), "");
        assert_eq!(grade_score(Some(Decimal::new(25, 1))), "正常");
        assert_eq!(grade_score(Some(Decimal::new(26, 1))), "关注");
        assert_eq!(grade_score(Some(Decimal::new(35, 1))), "关注");
        assert_eq!(grade_score(Some(Decimal::new(36, 1))), "高度可疑");
        assert_eq!(grade_score(Some(Decimal::new(-36, 1))), "高度可疑");
    }

    #[test]
    fn decimal_ln_only_accepts_positive_values() {
        assert_eq!(decimal_ln(Decimal::ZERO), None);
        assert_eq!(decimal_ln(Decimal::new(-1, 0)), None);
        assert_eq!(decimal_ln(Decimal::new(1, 0)), Some(0.0));
    }
}

use std::str::FromStr;

use serde::Serialize;

use crate::error::CostingError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PipelineName {
    Gb,
    Sk,
}

impl PipelineName {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Gb => "gb",
            Self::Sk => "sk",
        }
    }
}

impl FromStr for PipelineName {
    type Err = CostingError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "gb" => Ok(Self::Gb),
            "sk" => Ok(Self::Sk),
            other => Err(CostingError::invalid_input(format!("未知管线: {other}"))),
        }
    }
}

/// Validated domain rules consumed by the costing pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PipelineRules {
    /// Pipeline identity.
    pub name: PipelineName,
    /// Ordered exact product-code and product-name pairs.
    pub product_order: Vec<(String, String)>,
    /// Ordered standalone cost items admitted by the frozen business contract.
    pub standalone_cost_items: Vec<String>,
}

#[cfg(test)]
impl PipelineRules {
    pub(crate) fn for_name(name: PipelineName) -> Self {
        match name {
            PipelineName::Gb => Self {
                name,
                product_order: Vec::new(),
                standalone_cost_items: vec!["委外加工费".to_string()],
            },
            PipelineName::Sk => Self {
                name,
                product_order: Vec::new(),
                standalone_cost_items: vec!["委外加工费".to_string(), "软件费用".to_string()],
            },
        }
    }
}

#[cfg(test)]
pub(crate) fn owned_product_order(values: &[(&str, &str)]) -> Vec<(String, String)> {
    values
        .iter()
        .map(|(code, name)| ((*code).to_string(), (*name).to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rules_keep_pipeline_identity_and_sealed_standalone_items() {
        let gb = PipelineRules::for_name(PipelineName::Gb);
        assert_eq!(gb.name, PipelineName::Gb);
        assert_eq!(gb.standalone_cost_items, ["委外加工费"]);

        let sk = PipelineRules::for_name(PipelineName::Sk);
        assert_eq!(sk.name, PipelineName::Sk);
        assert_eq!(sk.standalone_cost_items, ["委外加工费", "软件费用"]);
    }
}

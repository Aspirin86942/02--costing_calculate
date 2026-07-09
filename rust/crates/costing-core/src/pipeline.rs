use std::str::FromStr;

use serde::Serialize;

use crate::error::CostingError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PipelineConfig {
    pub name: PipelineName,
    pub product_order: &'static [(&'static str, &'static str)],
    pub standalone_cost_items: &'static [&'static str],
}

pub const GB_PRODUCT_ORDER: &[(&str, &str)] = &[
    ("GB_C.D.B0048AA", "BMS-400W驱动器"),
    ("GB_C.D.B0040AA", "BMS-750W驱动器"),
    ("GB_C.D.B0041AA", "BMS-1100W驱动器"),
    ("GB_C.D.B0042AA", "BMS-1700W驱动器"),
    ("GB_C.D.B0043AA", "BMS-2400W驱动器"),
    ("GB_C.D.B0044AA", "BMS-3900W驱动器"),
    ("GB_C.D.B0045AA", "BMS-5900W驱动器"),
    ("GB_C.D.B0046AA", "BMS-7500W驱动器"),
];

pub const SK_PRODUCT_ORDER: &[(&str, &str)] = &[
    ("DP.C.P0197AA", "动力线"),
    ("DP.C.P0201AA", "动力线"),
    ("DP.C.P0198AA", "动力线"),
    ("DP.C.P0199AA", "动力线"),
    ("DP.C.P0257AA", "动力线"),
    ("DP.C.P0200AA", "动力线"),
    ("DP.C.P0246AA", "动力抱闸线"),
    ("DP.C.P0252AA", "动力线"),
];

impl PipelineConfig {
    pub fn for_name(name: PipelineName) -> Self {
        match name {
            PipelineName::Gb => Self {
                name,
                product_order: GB_PRODUCT_ORDER,
                standalone_cost_items: &["委外加工费"],
            },
            PipelineName::Sk => Self {
                name,
                product_order: SK_PRODUCT_ORDER,
                standalone_cost_items: &["委外加工费", "软件费用"],
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gb_and_sk_configs_match_python_contract() {
        let gb = PipelineConfig::for_name(PipelineName::Gb);
        assert_eq!(gb.standalone_cost_items, ["委外加工费"]);
        assert_eq!(gb.product_order, GB_PRODUCT_ORDER);

        let sk = PipelineConfig::for_name(PipelineName::Sk);
        assert_eq!(sk.standalone_cost_items, ["委外加工费", "软件费用"]);
        assert_eq!(sk.product_order, SK_PRODUCT_ORDER);
    }
}

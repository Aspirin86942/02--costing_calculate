use std::collections::HashSet;
use std::path::Path;

use costing_core::PipelineName;

use super::{FileConfigV1, FilePipelineV1, CONFIG_SCHEMA_VERSION};

// 独立成本项直接参与 GB/SK 总成本勾稽；保持冻结集合和顺序可防止普通外部配置悄然改变核算口径。
const GB_STANDALONE_ITEMS: &[&str] = &["委外加工费"];
const SK_STANDALONE_ITEMS: &[&str] = &["委外加工费", "软件费用"];

pub(crate) fn validate_file_config(config: &FileConfigV1) -> Result<(), String> {
    if config.schema_version != CONFIG_SCHEMA_VERSION {
        return Err(format!(
            "schema_version: unsupported value {}; expected {CONFIG_SCHEMA_VERSION}",
            config.schema_version
        ));
    }
    validate_pipeline(PipelineName::Gb, &config.pipelines.gb, GB_STANDALONE_ITEMS)?;
    validate_pipeline(PipelineName::Sk, &config.pipelines.sk, SK_STANDALONE_ITEMS)?;
    Ok(())
}

fn validate_pipeline(
    name: PipelineName,
    pipeline: &FilePipelineV1,
    sealed_standalone_items: &[&str],
) -> Result<(), String> {
    let path = format!("pipelines.{}", name.as_str());
    validate_input_pattern(name, &pipeline.input_pattern)
        .map_err(|message| format!("{path}.input_pattern: {message}"))?;

    if !pipeline
        .standalone_cost_items
        .iter()
        .map(String::as_str)
        .eq(sealed_standalone_items.iter().copied())
    {
        return Err(format!(
            "{path}.standalone_cost_items: must exactly equal {:?}",
            sealed_standalone_items
        ));
    }
    if pipeline.product_order.is_empty() {
        return Err(format!("{path}.product_order: must not be empty"));
    }

    let mut codes = HashSet::new();
    let mut pairs = HashSet::new();
    let mut display_orders = HashSet::new();
    for (index, product) in pipeline.product_order.iter().enumerate() {
        let item_path = format!("{path}.product_order[{index}]");
        validate_trimmed_text(&product.code, &format!("{item_path}.code"))?;
        validate_trimmed_text(&product.name, &format!("{item_path}.name"))?;
        if !codes.insert(product.code.as_str()) {
            return Err(format!(
                "{item_path}.code: duplicate product code {:?}",
                product.code
            ));
        }
        if !pairs.insert((product.code.as_str(), product.name.as_str())) {
            return Err(format!(
                "{item_path}: duplicate product code/name pair {:?}/{:?}",
                product.code, product.name
            ));
        }
        if !display_orders.insert(product.display_order) {
            return Err(format!(
                "{item_path}.display_order: duplicate value {}",
                product.display_order
            ));
        }
    }
    for (index, pair) in pipeline.product_order.windows(2).enumerate() {
        if pair[0].display_order >= pair[1].display_order {
            return Err(format!(
                "{path}.product_order[{}].display_order: array order must be strictly increasing",
                index + 1
            ));
        }
    }
    Ok(())
}

fn validate_trimmed_text(value: &str, path: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{path}: must not be blank"));
    }
    if value != value.trim() {
        return Err(format!("{path}: surrounding whitespace is not allowed"));
    }
    Ok(())
}

fn validate_input_pattern(name: PipelineName, pattern: &str) -> Result<(), String> {
    validate_trimmed_text(pattern, "value")?;
    let expected_prefix = format!("{}-", name.as_str());
    if !pattern.starts_with(&expected_prefix) {
        return Err(format!("must start with {expected_prefix:?}"));
    }
    if !pattern.to_ascii_lowercase().ends_with(".xlsx") {
        return Err("must end with .xlsx".to_string());
    }
    if Path::new(pattern).is_absolute()
        || pattern.contains(['/', '\\'])
        || pattern.contains("..")
        || pattern.contains(':')
    {
        return Err(
            "must be a relative basename glob without separators, drive prefixes, or '..'"
                .to_string(),
        );
    }
    if pattern
        .chars()
        .any(|character| character.is_control() || matches!(character, '[' | ']' | '{' | '}'))
    {
        return Err("contains an unsupported glob character".to_string());
    }
    Ok(())
}

pub(crate) fn input_pattern_matches(pattern: &str, file_name: &str) -> bool {
    let pattern = pattern.to_ascii_lowercase().chars().collect::<Vec<_>>();
    let value = file_name.to_ascii_lowercase().chars().collect::<Vec<_>>();
    let (mut pattern_index, mut value_index) = (0usize, 0usize);
    let (mut star_index, mut star_value_index) = (None, 0usize);

    while value_index < value.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == '?' || pattern[pattern_index] == value[value_index])
        {
            pattern_index += 1;
            value_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == '*' {
            star_index = Some(pattern_index);
            pattern_index += 1;
            star_value_index = value_index;
        } else if let Some(star) = star_index {
            pattern_index = star + 1;
            star_value_index += 1;
            value_index = star_value_index;
        } else {
            return false;
        }
    }
    while pattern_index < pattern.len() && pattern[pattern_index] == '*' {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DEFAULT_CONFIG;

    fn defaults() -> FileConfigV1 {
        toml::from_str(DEFAULT_CONFIG).unwrap()
    }

    #[test]
    fn default_config_is_valid() {
        validate_file_config(&defaults()).unwrap();
    }

    #[test]
    fn duplicate_code_pair_and_display_order_are_rejected() {
        let mut duplicate_code = defaults();
        duplicate_code.pipelines.gb.product_order[1].code =
            duplicate_code.pipelines.gb.product_order[0].code.clone();
        assert!(validate_file_config(&duplicate_code)
            .unwrap_err()
            .contains("duplicate product code"));

        let mut duplicate_order = defaults();
        duplicate_order.pipelines.sk.product_order[1].display_order =
            duplicate_order.pipelines.sk.product_order[0].display_order;
        assert!(validate_file_config(&duplicate_order)
            .unwrap_err()
            .contains("duplicate value"));
    }

    #[test]
    fn blank_or_out_of_order_products_are_rejected() {
        let mut blank = defaults();
        blank.pipelines.gb.product_order[0].name = " \t".to_string();
        assert!(validate_file_config(&blank)
            .unwrap_err()
            .contains("must not be blank"));

        let mut out_of_order = defaults();
        out_of_order.pipelines.gb.product_order.swap(0, 1);
        assert!(validate_file_config(&out_of_order)
            .unwrap_err()
            .contains("strictly increasing"));
    }

    #[test]
    fn wildcard_matching_is_basename_only_and_ascii_case_insensitive() {
        assert!(input_pattern_matches("gb-*.xlsx", "GB-2026-06.XLSX"));
        assert!(input_pattern_matches("sk-??.xlsx", "sk-06.xlsx"));
        assert!(!input_pattern_matches("sk-??.xlsx", "sk-006.xlsx"));
        assert!(!input_pattern_matches("gb-*.xlsx", "sk-2026.xlsx"));
    }
}

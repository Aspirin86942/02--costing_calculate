use std::path::{Path, PathBuf};

use costing_core::model::MonthRange;
use costing_core::{CostingError, ErrorCode};

use crate::application::RunRequest;
use crate::config::input_pattern_matches;

#[derive(Debug, PartialEq, Eq)]
pub(super) struct ResolvedCliPaths {
    pub(super) input: PathBuf,
    pub(super) output: Option<PathBuf>,
}

pub(super) fn resolve_cli_paths(
    args: &RunRequest,
    base_dir: &Path,
    month_range: Option<&MonthRange>,
    input_pattern: &str,
) -> Result<ResolvedCliPaths, CostingError> {
    let pipeline = args.pipeline.as_str();
    let input = match &args.input {
        Some(input) => input.clone(),
        None => discover_default_input(base_dir, pipeline, input_pattern)?,
    };
    let output = match (&args.output, args.check_only) {
        (Some(output), _) => Some(output.clone()),
        (None, true) => None,
        (None, false) => Some(default_output_path(
            base_dir,
            pipeline,
            &input,
            month_range,
        )?),
    };
    Ok(ResolvedCliPaths { input, output })
}

fn discover_default_input(
    base_dir: &Path,
    pipeline: &str,
    input_pattern: &str,
) -> Result<PathBuf, CostingError> {
    let raw_dir = base_dir.join("data").join("raw").join(pipeline);
    let entries = std::fs::read_dir(&raw_dir).map_err(|error| {
        let code = if error.kind() == std::io::ErrorKind::NotFound {
            ErrorCode::FileNotFound
        } else {
            ErrorCode::FileNotReadable
        };
        CostingError::io(
            code,
            format!("无法读取默认输入目录 {}: {error}", raw_dir.display()),
            raw_dir.clone(),
        )
    })?;
    let mut candidates = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            CostingError::io(
                ErrorCode::FileNotReadable,
                format!("读取默认输入目录项失败: {error}"),
                raw_dir.clone(),
            )
        })?;
        let path = entry.path();
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if !input_pattern_matches(input_pattern, &file_name) {
            continue;
        }
        let metadata = entry.metadata().map_err(|error| {
            CostingError::io(
                ErrorCode::FileNotReadable,
                format!("读取默认输入文件元数据失败 {}: {error}", path.display()),
                path.clone(),
            )
        })?;
        if !metadata.is_file() {
            continue;
        }
        candidates.push(path);
    }
    candidates.sort();
    match candidates.as_slice() {
        [input] => Ok(input.clone()),
        [] => Err(CostingError::io(
            ErrorCode::FileNotFound,
            format!(
                "未在默认输入目录 {} 找到匹配 {input_pattern:?} 的文件",
                raw_dir.display(),
            ),
            raw_dir,
        )),
        _ => {
            let candidate_text = candidates
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ");
            Err(CostingError::invalid_input(format!(
                "检测到多个 {pipeline} 输入文件，请使用 --input 明确指定: {candidate_text}"
            )))
        }
    }
}

fn default_output_path(
    base_dir: &Path,
    pipeline: &str,
    input: &Path,
    month_range: Option<&MonthRange>,
) -> Result<PathBuf, CostingError> {
    let stem = input
        .file_stem()
        .ok_or_else(|| CostingError::invalid_input("输入文件名缺少有效主文件名"))?;
    let mut file_name = stem.to_os_string();
    file_name.push("_处理后");
    if let Some(suffix) = month_output_suffix(month_range) {
        file_name.push("_");
        file_name.push(suffix);
    }
    file_name.push(".xlsx");
    Ok(base_dir
        .join("data")
        .join("processed")
        .join(pipeline)
        .join(file_name))
}

pub(super) fn month_output_suffix(month_range: Option<&MonthRange>) -> Option<String> {
    let month_range = month_range?;
    match (&month_range.start, &month_range.end) {
        (Some(start), Some(end)) => Some(format!("{start}_{end}")),
        (Some(start), None) => Some(format!("from_{start}")),
        (None, Some(end)) => Some(format!("to_{end}")),
        (None, None) => None,
    }
}

#[cfg(test)]
pub(super) fn validate_cli_request(args: &RunRequest) -> Result<(), CostingError> {
    let cwd = std::env::current_dir().map_err(|source| {
        CostingError::io_with_source(
            ErrorCode::InvalidInput,
            format!("无法获取当前工作目录: {source}"),
            source,
        )
    })?;
    validate_cli_request_from(args, &cwd)
}

pub(super) fn validate_cli_request_from(args: &RunRequest, cwd: &Path) -> Result<(), CostingError> {
    let input = args
        .input
        .as_ref()
        .ok_or_else(|| CostingError::invalid_input("缺少输入文件路径"))?;
    if !input.exists() {
        return Err(CostingError::Io {
            code: ErrorCode::FileNotFound,
            message: format!("输入文件不存在: {}", input.display()),
            path: input.clone(),
            retryable: false,
        });
    }
    if !input.is_file() {
        return Err(CostingError::Io {
            code: ErrorCode::InvalidInput,
            message: format!("输入路径不是文件: {}", input.display()),
            path: input.clone(),
            retryable: false,
        });
    }
    if input
        .extension()
        .and_then(|value| value.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
        != Some("xlsx")
    {
        return Err(CostingError::Io {
            code: ErrorCode::UnsupportedFileType,
            message: "输入文件必须是 .xlsx 格式".to_string(),
            path: input.clone(),
            retryable: false,
        });
    }
    if !args.check_only && args.output.is_none() {
        return Err(CostingError::invalid_input(
            "非 check-only 运行必须提供 --output",
        ));
    }
    if !args.check_only {
        let output = args.output.as_ref().expect("checked output above");
        if paths_resolve_to_same_file(input, output, cwd) {
            return Err(CostingError::invalid_input(
                "输入文件与输出文件不能是同一文件",
            ));
        }
        match output.try_exists() {
            Ok(true) => {
                return Err(CostingError::io(
                    ErrorCode::OutputExists,
                    format!("输出 workbook 已存在: {}", output.display()),
                    output.clone(),
                ));
            }
            Ok(false) => {}
            Err(source) => {
                return Err(CostingError::io(
                    ErrorCode::OutputNotWritable,
                    format!("无法检查输出 workbook 路径 {}: {source}", output.display()),
                    output.clone(),
                ));
            }
        }
    }
    if let Some(summary_output) = args.summary_output.as_ref() {
        if let Some(output) = args.output.as_ref() {
            if paths_resolve_to_same_file(output, summary_output, cwd) {
                return Err(CostingError::invalid_input(
                    "workbook 输出与运行 Manifest 不能指向同一文件",
                ));
            }
        }
        match summary_output.try_exists() {
            Ok(true) => {
                return Err(CostingError::io(
                    ErrorCode::OutputExists,
                    format!("运行 Manifest 已存在: {}", summary_output.display()),
                    summary_output.clone(),
                ));
            }
            Ok(false) => {}
            Err(source) => {
                return Err(CostingError::io(
                    ErrorCode::OutputNotWritable,
                    format!(
                        "无法检查运行 Manifest 路径 {}: {source}",
                        summary_output.display()
                    ),
                    summary_output.clone(),
                ));
            }
        }
    }
    Ok(())
}

fn paths_resolve_to_same_file(input: &Path, output: &Path, cwd: &Path) -> bool {
    match (input.canonicalize(), output.canonicalize()) {
        (Ok(input), Ok(output)) => input == output,
        _ => normalize_comparison_path(input, cwd) == normalize_comparison_path(output, cwd),
    }
}

fn normalize_comparison_path(path: &Path, cwd: &Path) -> PathBuf {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            _ => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

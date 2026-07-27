# Contract Baselines

本目录保存 Rust 正式实现的冻结契约数据。

## 规则

- 纯重构不得修改 baseline。
- 只有明确批准的业务口径变化才允许更新 baseline。
- 更新时必须同时说明变更前行为、变更后行为、业务原因及受影响的 Sheet 和字段。
- baseline 不由 Python 业务实现生成；代码、Rust 契约测试和实际 CLI 输出共同校验这些文件。

## 当前基线

- `baselines/workbook_semantics.json`
  - 冻结默认三张 Sheet 的顺序、列序、冻结窗格、筛选范围、数字格式、列宽和异常高亮位置。
- `baselines/error_log_contract.json`
  - 冻结运行时 `error_log` 数据契约；CLI 不单独写出 CSV，但运行汇总与质量计数仍依赖该契约。

## 跨版本验证

使用统一验证命令，让基线 binary 和候选 binary 处理同一份输入：

```powershell
uv run python -m tools.validation.compare_releases `
  --baseline-binary <baseline.exe> `
  --candidate-binary <candidate.exe> `
  --pipeline gb `
  --input <workbook.xlsx> `
  --output-dir <empty-directory> `
  --report <report.json>
```

需要比较月份过滤时，追加 `--month-start YYYY-MM` 和/或 `--month-end YYYY-MM`。

验证器会比较 Sheet 顺序、行列、单元格类型和值、样式、数字格式、条件格式和 OOXML 包结构；数值容差固定为单元格 `1e-9`、列累计 `1e-8`。报告不包含单元格值或敏感路径。

缺少真实样本不等于通过。正式验收必须让 GB、SK 两条管线都实际执行，并把脱敏结果写入 `docs/changes/`。

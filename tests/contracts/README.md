# Contract Baselines

本目录用于冻结“当前代码真实输出”的 contract baseline。

## 规则

- 纯重构不得修改 baseline。
- 只有业务口径明确变化时，才允许更新 baseline，并且必须在变更说明里写清差异。
- README 不是 contract 真值；baseline 只能来自当前代码真实输出。

## 当前基线

- `baselines/workbook_semantics.json`
  - 该基线冻结默认 3 张 workbook Sheet 的顺序、列序、freeze panes、auto filter、number format、column width 和工单异常高亮位置。
- `baselines/error_log_contract.json`
  - 冻结运行时 `error_log` 数据契约；当前 CLI 不再写出 CSV，但内存汇总与质量计数仍依赖该契约。

## 生成方式

运行：

```bash
uv run python -m tests.contracts.generate_baselines
```

该命令会使用当前代码路径生成 workbook 语义快照，而不是比对二进制文件。

## Rust oracle

`tests/test_full_rust_cli_oracle.py` 使用当前 Python service 路径生成 oracle workbook，再调用 Rust CLI 生成同一输入的 workbook，并通过 `tests/rust_oracle/workbook_compare.py` 比对 sheet 顺序、行列形状、freeze panes、auto filter 和单元格值。

在 linked worktree 没有 `data/raw` 样本时，可通过环境变量指定本机样本：

```bash
COSTING_GB_SAMPLE=... COSTING_SK_SAMPLE=... uv run python -m pytest tests/test_full_rust_cli_oracle.py -q --basetemp .pytest-tmp/rust-oracle
```

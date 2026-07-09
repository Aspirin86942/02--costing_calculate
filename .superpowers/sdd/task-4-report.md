# Task 4 Report

## Status

DONE_WITH_CONCERNS

## Summary

- 实现了 Rust 侧 `NormalizedCostFrame` / `SplitResult` / `MonthRange` 模型。
- 新增 `normalize.rs`，覆盖表头压平、汇总行移除、按业务规则向下填充、月份列生成、严格月份区间过滤、`Filled_成本项目` 补列。
- 新增 `split.rs`，按 Python 参考掩码拆分数量行和明细行，并在明细行回填 `Filled_成本项目`。
- 在 CLI `run` 中接入 `normalize_workbook`、`split_detail_and_qty` 和严格 `YYYY-MM` month range 校验。
- 保留并通过了现有 Rust 测试，同时新增 Task 4 相关测试。

## RED Evidence

### 1. Normalize 红灯

Command:

```powershell
cargo test --manifest-path rust/Cargo.toml -p costing-core normalize::tests
```

Result:

- FAIL
- 失败点符合 brief 预期：`normalizer missing`
- 具体失败测试：
  - `normalize::tests::forward_fill_skips_vendor_columns_for_integrated_workshop`
  - `normalize::tests::removes_total_rows`

Key output:

```text
called `Result::unwrap()` on an `Err` value: User { code: InvalidInput, message: "normalizer missing", retryable: false }
```

## GREEN Evidence

### 1. Normalize 绿灯

Command:

```powershell
cargo test --manifest-path rust/Cargo.toml -p costing-core normalize::tests
```

Result:

- PASS
- `7 passed; 0 failed`
- 覆盖：
  - 集成车间供应商列不继承
  - 集成车间供应商值不作为后续行 fill seed
  - 合计行移除
  - 双层表头压平
  - `月份` 列生成
  - 严格 month range 过滤
  - 非严格 CLI month 值拒绝

### 2. 全量 Rust 测试

Command:

```powershell
cargo test --manifest-path rust/Cargo.toml
```

Result:

- PASS
- `costing-cli` tests: `7 passed; 0 failed`
- `costing-core` tests: `10 passed; 0 failed`
- `costing-xlsx` tests: `3 passed; 0 failed`

### 3. CLI 样本验证

Command:

```powershell
cargo run --manifest-path rust/Cargo.toml -p costing-calculate -- gb --input "D:\python_program\02--costing_calculate\data\raw\gb\gb-成本计算单_2026070916484310_100160.xlsx" --check-only --benchmark
```

Result:

- Exit `0`
- 返回 JSON summary
- 关键字段：

```json
{
  "status": "succeeded",
  "pipeline": "gb",
  "output_written": false,
  "stage_timings": {
    "stages": {
      "detail_rows": 0.0,
      "ingest": 0.0,
      "qty_rows": 57910.0,
      "reader_rows": 57910.0
    }
  }
}
```

## Commands Run

```powershell
Get-Content .superpowers/sdd/task-4-brief.md
cargo test --manifest-path rust/Cargo.toml -p costing-core normalize::tests
cargo fmt --manifest-path rust/Cargo.toml --all
cargo test --manifest-path rust/Cargo.toml
cargo run --manifest-path rust/Cargo.toml -p costing-calculate -- gb --input "D:\python_program\02--costing_calculate\data\raw\gb\gb-成本计算单_2026070916484310_100160.xlsx" --check-only --benchmark
uv run python -c "from openpyxl import load_workbook; ..."
```

## Files Changed

- `rust/crates/costing-core/src/model.rs`
- `rust/crates/costing-core/src/lib.rs`
- `rust/crates/costing-core/src/normalize.rs`
- `rust/crates/costing-core/src/split.rs`
- `rust/crates/costing-cli/src/run.rs`
- `.superpowers/sdd/task-4-report.md`

## Dependency Decision

- 查了已有库：是
- 用了外部库：否
- 需要审批新依赖：否
- 原因：Task 4 所需逻辑可以直接基于现有 `rust_decimal`、`serde` 和标准库完成

## Concerns

1. **brief 示例代码与 Python 参考存在冲突**
   - brief 中 `flatten_headers` 在上下表头都非空且不相等时取第二行；
   - Python 参考 `_flatten_headers` 会拼接两层表头，形成诸如 `成本项目名称`、`供应商编码` 这类稳定列名。
   - 为了保持与 Python 参考一致并支撑 split 规则，本次实现采用了 **拼接两层表头** 的行为。

2. **真实 GB 样本上的 `detail_rows=0` 暴露了 reader 前置限制**
   - 真实样本前两行是元信息，真正表头在第 3/4 行。
   - 当前 `rust/crates/costing-xlsx/src/reader.rs` 固定把前两行当作 header，这会让后续 normalize/split 基于错误 header 工作。
   - 该问题超出 Task 4 write scope，本次未改 reader，只在报告中保留证据。

3. **brief 示例的 `key_columns` 与 Python 参考也不完全一致**
   - brief 示例偏向 `["月份", "产品编码"]`
   - Python 参考是 `("月份", "产品编码", "工单编号", "工单行号")`
   - 本次实现采用“按 Python 契约候选列、仅保留实际存在列”的折中方式。

## Non-Task Changes Avoided

- 未修改 Python 源码和 Python 测试
- 未引入新依赖
- 未提交 `rust/target/`
- 未触碰 README、AGENTS、docs、`.gitignore`、`src/`

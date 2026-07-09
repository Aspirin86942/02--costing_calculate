# Rust Rewrite Validation

## Status

Rust CLI is the current primary entrypoint for the default GB/SK costing ETL path. The commands on this page are ongoing validation gates; Python retirement requires separate approval.
When `data/raw` samples are absent, set `COSTING_GB_SAMPLE` and `COSTING_SK_SAMPLE` to prove validation on real workbooks; default `skip` results are not evidence for GB/SK validation.

## Required Commands

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --manifest-path rust/Cargo.toml
uv run python -m pytest tests/test_full_rust_cli_oracle.py tests/test_full_rust_cli_benchmark.py -q --basetemp .pytest-tmp/full-rust-final
uv run python -m pytest tests -q --basetemp .pytest-tmp/python-regression
uv run python -m ruff check src tests
uv run python -m ruff format src tests --check
```

## Manual Check

- Open one GB Rust output workbook in Excel or WPS.
- Open one SK Rust output workbook in Excel or WPS.
- Confirm the workbook contains exactly 3 sheets.
- Confirm `成本分析产品维度` is absent.
- Confirm filters and frozen panes are visible on all sheets.

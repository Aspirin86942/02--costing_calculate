# Rust Rewrite Validation

## Status

Rust CLI is the validated replacement target for the default GB/SK costing ETL path after the full parity suite passes.

## Required Commands

```powershell
cargo fmt --all --check
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

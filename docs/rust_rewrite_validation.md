# Rust Rewrite Validation

## Status

Rust CLI is the current primary entrypoint for the default GB/SK costing ETL path. The commands on this page are ongoing validation gates; Python retirement requires separate approval.
When `data/raw` samples are absent, set `COSTING_GB_SAMPLE` and `COSTING_SK_SAMPLE` to prove validation on real workbooks; default `skip` results are not evidence for GB/SK validation.

## Last Local Validation Snapshot: 2026-07-10

The recorded GB/SK verdict is `VALIDATED`. This is a manually preserved summary of completed local runs, not a repository-contained raw log. Exact code version, samples, reproduction commands, and evidence limitations are recorded in [`evidence/2026-07-10-rust-validation.md`](evidence/2026-07-10-rust-validation.md).

| Gate | Result |
|---|---:|
| Rust workspace tests | 85 passed |
| Python regression suite | 265 passed, 4 skipped |
| Ruff check and format check | passed |
| GB oracle target | passed; 42.59s recorded |
| SK oracle target | passed; 369.58s recorded |

The latest same-machine benchmark used three sequential repeats per pipeline. Runtime-contract and workbook comparisons passed in every repeat.

| Pipeline | Python oracle median | Rust release median | Speedup | Verdict |
|---|---:|---:|---:|---|
| GB | 12.143s | 3.507s | 3.46x | `VALIDATED` |
| SK | 112.923s | 39.582s | 2.85x | `VALIDATED` |

These timings describe that same-machine run only; they are not a permanent performance guarantee. Excel/WPS visual inspection remains a separate manual check and is not marked complete here.

## Required Commands

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --manifest-path rust/Cargo.toml
uv run python -m pytest tests/test_full_rust_cli_oracle.py::test_rust_gb_workbook_matches_python_oracle -q --basetemp .pytest-tmp/rust-oracle-gb
uv run python -m pytest tests/test_full_rust_cli_oracle.py::test_rust_sk_workbook_matches_python_oracle -q --basetemp .pytest-tmp/rust-oracle-sk
uv run python -m pytest tests/test_full_rust_cli_benchmark.py -q --basetemp .pytest-tmp/rust-benchmark
uv run python -m pytest tests -q --basetemp .pytest-tmp/python-regression
uv run python -m ruff check src tests
uv run python -m ruff format src tests --check
```

## Manual Check

- Run the Rust CLI commands from the root README with explicit `--output` paths.
- Open the generated GB Rust output workbook in Excel or WPS.
- Open the generated SK Rust output workbook in Excel or WPS.
- Confirm the workbook contains exactly 3 sheets.
- Confirm `成本分析产品维度` is absent.
- Confirm filters and frozen panes are visible on all sheets.

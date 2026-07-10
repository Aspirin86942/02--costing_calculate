# Python Retirement After Rust Validation

The Rust CLI was validated for GB and SK on 2026-07-10; see [`rust_rewrite_validation.md`](rust_rewrite_validation.md) for the recorded snapshot and evidence limits. This document lists Python code that remains in place until a separate retirement change is reviewed and approved; validation alone is not deletion approval.

## Keep Until Retirement Is Approved

- `main.py`
- `src/etl/`
- `src/analytics/`
- `src/excel/`
- `src/services/costing_service.py`
- `tests/contracts/`
- `tests/rust_oracle/`

## Product Dimension Retirement

Rust does not implement `成本分析产品维度`.

In a separately approved retirement change, remove the Python legacy product-dimension helpers:

- `src/analytics/table_rendering.py` product anomaly section helpers
- `src/excel/product_anomaly_writer.py`
- Tests that only protect the retired product-dimension sheet

## Removal Rule

Do not delete any Python oracle code in the same commit that validates Rust. Deletion requires a separate review after Rust validation evidence is attached.

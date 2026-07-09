# Python Retirement After Rust Validation

This document lists Python code that may be removed only after Rust CLI is validated for GB and SK.

## Keep Until Rust Is Validated

- `main.py`
- `src/etl/`
- `src/analytics/`
- `src/excel/`
- `src/services/costing_service.py`
- `tests/contracts/`
- `tests/rust_oracle/`

## Product Dimension Retirement

Rust does not implement `成本分析产品维度`.

After Rust validation, remove the Python legacy product-dimension helpers in a separate change:

- `src/analytics/table_rendering.py` product anomaly section helpers
- `src/excel/product_anomaly_writer.py`
- Tests that only protect the retired product-dimension sheet

## Removal Rule

Do not delete any Python oracle code in the same commit that validates Rust. Deletion requires a separate review after Rust validation evidence is attached.

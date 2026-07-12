# Rust Performance Validation — 2026-07-12

## Scope

- Production implementation commit: `f298649`.
- Oracle semantic-comparison commit: `514e6b5`.
- Controlled `rust_xlsxwriter` fork: `816cd47bc2faa84ab1ac2fbb3320a4699a454b22`.
- Environment: one Windows machine, `rustc 1.96.0`, release profile with `codegen-units = 1`.
- Inputs: approved local GB/SK ERP workbooks; filenames and paths are intentionally omitted.
- Method: independent normal-mode process per round, external wall-clock and Peak Working Set sampling, N=5 median.

## Final Results

| Pipeline | Wall rounds | Wall median | PWS median | Output bytes |
|---|---|---:|---:|---:|
| GB | 2.509, 2.585, 2.475, 2.409, 2.443s | 2.475s | 357,191,680 | 3,808,077 |
| SK | 19.584, 19.665, 19.994, 20.066, 19.883s | 19.883s | 1,461,714,944 | 43,611,044 |

Acceptance limits:

- SK wall median `<= 20.0s`;
- SK PWS median `<= 2,147,483,648` bytes;
- SK output `<= 48,658,823` bytes;
- GB wall `<= 3.2554s`, PWS `<= 375,700,685` bytes, output `<= 4,194,321` bytes.

All limits passed.

## Correctness

- Rust workspace: 168 tests passed.
- Python contracts: 7 tests passed.
- Workbook comparator: 39 focused tests passed.
- Current Python-oracle GB and SK workbooks both passed the full ZIP/XML, style, value, business-total and runtime-contract comparison with zero mismatches.
- SK every round: 3 sheets, 425,459 detail rows, 11,239 quantity rows, 201,815 error-log entries.
- GB every round: 3 sheets and 20,515 error-log entries.
- SK issue types: `MISSING_AMOUNT=168424`, `NON_POSITIVE_UNIT_COST=33391`.
- GB issue types: `MISSING_AMOUNT=17193`, `NON_POSITIVE_UNIT_COST=3322`.
- Final SK workbook and the fully oracle-validated reference had identical ZIP member names and identical decompressed member hashes except the expected `docProps/core.xml` timestamp.
- Python XlsxWriter shared-string cells and Rust inline-string cells are compared as equivalent OOXML text encodings while each package's relationship/content-type/index chain remains strictly validated.
- Numeric XML lexemes use a `1e-9` absolute tolerance per cell and per work-order group; whole-column totals use `1e-8`. Measured GB/SK serialization-tail maxima were `1e-10` per cell/group and `2.509e-9` per column total; a `1e-7` difference remains a tested failure.
- Output directories and system `%TEMP%` had no `.costing-tmp-*` residue after the runs.

## Evidence Limits

Raw logs, binaries, workbook outputs and PWS samples remain under ignored `rust/target/perf-local/` and are not repository artifacts. The numbers above are a same-machine validation snapshot, not a cross-machine SLA. The frozen Phase 0A and fork dependency JSON files remain under `docs/performance/` for audit only.

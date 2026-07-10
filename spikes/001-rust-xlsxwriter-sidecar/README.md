# 001: rust_xlsxwriter sidecar

> Compatibility note (2026-07-10): this is a historical spike report. Its Python benchmark command was updated from the retired conda environment to the current uv-managed `.venv`; the measurements remain the 2026-07-09 spike results.

## Question

Can `rust_xlsxwriter` replace Python `xlsxwriter` export for the 3-sheet default workbook and reduce
`sidecar_export_seconds` to `<= 5s` while achieving at least 40% speedup over the Python 3-sheet export baseline?

## Baseline

Benchmark date: 2026-07-09

Command:

```powershell
uv run python spikes/001-rust-xlsxwriter-sidecar/python/benchmark_rust_writer.py gb `
  --input 'D:\python_program\02--costing_calculate\data\raw\gb\gb-成本计算单_2026070916484310_100160.xlsx' `
  --tmp-dir .pytest-tmp/phase1-benchmark-final2 `
  --repeats 3 `
  --json-output .pytest-tmp/phase1-benchmark-final2/summary.json
```

- `python_3sheet_total_seconds`: 17.005
- `python_3sheet_export_seconds`: 13.515
- `python_3sheet_payload_seconds`: 3.491
- `input rows`: 54,752 rows in `成本计算单总表`
- `output workbook size`: 4,853,323 bytes for the median Python export run
- `sheet count`: 3

## Approach

- Phase 0 changed the default workbook contract to 3 sheets.
- Python exports manifest + CSV for the 3 default sheets.
- CSV export prefers Polars `source_frame.write_csv()`.
- The manifest records `column_types`, `number_formats`, `source_dtypes`, and explicit `write_types`.
- Rust reads the manifest + CSV and writes xlsx via `rust_xlsxwriter`.
- `sidecar_export_seconds = intermediate_export_seconds + rust_export_seconds`.
- The production `main.py` path is unchanged; this is a spike-only sidecar.

## Results

| Run | Python 3-sheet export | Intermediate export | Rust export | Sidecar export | Speedup |
|---|---:|---:|---:|---:|---:|
| 1 | 13.513 | 0.022 | 1.176 | 1.199 | 11.28x |
| 2 | 13.517 | 0.021 | 1.144 | 1.164 | 11.61x |
| 3 | 13.515 | 0.022 | 1.164 | 1.186 | 11.40x |
| median | 13.515 | 0.022 | 1.164 | 1.186 | 11.40x |

Additional totals:

- `payload_build_seconds`: 3.491
- `sidecar_total_seconds`: 4.676
- `total_speedup`: 3.64x
- `rust_output_size_bytes`: 3,821,176

## CSV Export

| Sheet | Method | Seconds | Rows | Columns |
|---|---|---:|---:|---:|
| 成本计算单总表 | polars | 0.014 | 54,752 | 21 |
| 成本计算单数量聚合维度 | polars | 0.003 | 1,124 | 38 |
| 成本分析工单维度 | polars | 0.003 | 396 | 35 |

## Validation

- `openpyxl load`: passed
- `sheet count`: 3
- `sheet names`: matched
- `product dimension absent`: passed
- `row counts`: matched
- `column counts`: matched
- `headers`: matched
- `checked cells`: 1,206,458 per Rust run
- `sample values`: full manifest-column scan passed
- `number formats`: matched for manifest number-format columns
- `freeze panes`: matched
- `auto filter`: matched exact expected ranges
- `Excel/WPS manual open`: not performed in this automated spike run

Validation was run for all 3 Rust sidecar outputs. All 3 reports returned:

```json
{
  "passed": true,
  "sheet_count": 3,
  "number_format_matched": true,
  "shape_matched": true,
  "auto_filter_matched": true,
  "error_count": 0
}
```

## Verdict

`VALIDATED`

Reason:

- `sidecar_export_seconds = 1.186s`, below the 5.000s absolute target.
- `sidecar_export_seconds <= python_3sheet_export_seconds * 0.60`.
- Workbook validation passed for all 3 sidecar runs.

## Recommendation

Adopt the Rust writer idea for a separate productionization plan.

Required production work if adopted:

- Add an explicit optional writer mode, for example `python main.py gb --writer rust`.
- Keep the Python writer as fallback.
- Define where the Rust binary is built, stored, versioned, and deployed.
- Preserve this manifest validation as a production safety gate.
- Add clear failure fallback if Rust exits non-zero.
- Keep the 3-sheet workbook contract as the default behavior.

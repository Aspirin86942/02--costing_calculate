# Rust Validation Evidence Snapshot - 2026-07-10

## Scope And Provenance

- Code under test: Git `HEAD` `7afebde32033d32eee19ac21555d13517122c055`.
- Environment: one Windows machine; Python oracle and Rust release runs executed sequentially, with no concurrent pytest, uv, or Cargo workload.
- Source: results recorded from the completed 2026-07-10 interactive validation session. The SK standalone oracle result was supplied after that run completed.
- Evidence limitation: raw stdout, temporary comparison workbooks, and benchmark JSON were not committed and the temporary directories were cleaned. This file is an operator-recorded snapshot, not self-contained raw proof. Use the reproduction commands below when fresh evidence is required.

## Inputs

- GB: `data/raw/gb/gb-成本计算单_2026070916484310_100160.xlsx`
- SK: `data/raw/sk/sk-成本计算单_2026041311461807_3592191.xlsx`

ERP exports are sensitive. The input workbooks remain local and are not documentation artifacts.

## Automated Gates

| Gate | Recorded result |
|---|---:|
| Rust workspace tests | 85 passed |
| Python regression suite | 265 passed, 4 skipped |
| Ruff check and format check | passed |
| GB oracle target | passed; 42.59s recorded |
| SK oracle target | passed; 369.58s recorded |

The original console summaries also contained deselected/skipped counts determined by that session's test selection and sample visibility. The exact original invocation was not retained, so those incidental collection counts are intentionally not treated as acceptance evidence here.

## Benchmark Method

The official `tests/rust_oracle/benchmark.py::run_same_machine_benchmark` harness ran three repeats per pipeline. Each repeat ran the Python oracle first and the Rust release binary second, then compared runtime contracts and workbook semantics. The table reports medians across the three repeats.

| Pipeline | Python oracle median | Rust release median | Speedup | Validation failures | Verdict |
|---|---:|---:|---:|---:|---|
| GB | 12.143s | 3.507s | 3.46x | 0 | `VALIDATED` |
| SK | 112.923s | 39.582s | 2.85x | 0 | `VALIDATED` |

The speedups are measurements from this machine and run, not fixed service-level guarantees.

## Reproduction

Automated gates:

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

The pytest benchmark gate proves the verdict but does not print median fields. To print a fresh `BenchmarkResult`, call the harness directly for each local sample:

```powershell
uv run python -c "from dataclasses import asdict; from pathlib import Path; from tempfile import TemporaryDirectory; import json; from tests.rust_oracle.benchmark import run_same_machine_benchmark as run; tmp=TemporaryDirectory(); result=run('gb', Path(r'data/raw/gb/gb-成本计算单_2026070916484310_100160.xlsx'), Path(tmp.name), repeats=3); print(json.dumps(asdict(result), ensure_ascii=False, indent=2))"
uv run python -c "from dataclasses import asdict; from pathlib import Path; from tempfile import TemporaryDirectory; import json; from tests.rust_oracle.benchmark import run_same_machine_benchmark as run; tmp=TemporaryDirectory(); result=run('sk', Path(r'data/raw/sk/sk-成本计算单_2026041311461807_3592191.xlsx'), Path(tmp.name), repeats=3); print(json.dumps(asdict(result), ensure_ascii=False, indent=2))"
```

Manual Excel/WPS inspection was not recorded as complete in this snapshot.

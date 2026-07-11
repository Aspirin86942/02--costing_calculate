# Rust 输出 Phase 4–5 Reader A/B 与最终发布实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在完整刻画 Calamine 读取语义后删除唯一的 `Range<Data> -> Vec<Vec<Data>>` 中间整表副本，以同批 A/B 决定保留或回退，并让最终同一 EXE SHA 同时通过 SK `<=20.0s`、PWS `<=2.0 GiB`、workbook/runtime/error 契约、PE imports 和干净 Windows 单 EXE smoke。

**Architecture:** Phase 4 先以直接构造的 Calamine variant 测试冻结 header/data 规范化，再让 header 扫描与 data normalize 直接借用 `Range<Data>`；这只删除 Calamine `Data` 的中间副本，最终 `RawWorkbook.rows` 仍保持拥有数据。Phase 4/3 和 Phase 4/0A 使用 Phase 0H harness 新采同批证据；不达保留条件即显式 revert。Phase 5 不再改业务算法，只对保留候选执行全量验证、发布文档同步、PE/clean-Windows gate 和最终 sanitized manifest。

**Tech Stack:** Rust 2021、Calamine 0.26.1、Phase 3 selected writer features、Python Phase 0H harness、pytest/Ruff、MSVC/dumpbin、可选 llvm-readobj、干净 Windows 10/11 x64。

## Global Constraints

- Phase 4 是强制 A/B；即使 Phase 3 已满足最终 wall/PWS，也必须制作并测量 reader candidate。
- 公开接口保持 `read_raw_workbook(path: &Path) -> Result<RawWorkbook, CostingXlsxError>` 不变。
- 不改变第一 Sheet 选择、双层表头识别、尾部空白列、String whitespace、Decimal/date/error 的现有语义。
- Header 的 `String/DateTimeIso/DurationIso` trim；Data 非空 String、`DateTimeIso`、`DurationIso` 保留原始首尾空格；纯空白 String 转 `Blank`。
- `DateTime` 继续转 `DateLike(value.to_string())`；`DateTimeIso` 转 `DateLike`；`DurationIso` 转 `Text`。
- 有限 Float 继续通过现有文本路径转 Decimal；整数 Float 去 `.0`；科学计数法可解析时转 Decimal；NaN/正负 infinity 转 Text。
- `Data::Error` 继续用 Debug 文本，`Data::Empty` 继续 Blank/空 header。
- 不把本阶段描述为“零复制 reader”：最终 `Vec<Vec<CellValue>>`、String clone 和 workbook payload 均保留。
- Phase 4/3 的内部收益分母必须是同批重新运行的 Phase 3 EXE；不得读取历史 Phase 3 JSON 相除。
- Phase 4 不通过保留门槛时，先提交 rejected evidence，再 `git revert` reader code commit。
- Phase 5 只使用 Phase 4 保留 SHA或 Phase 3 回退 SHA；正式 gate 通过后不得 rebuild 未验证的新 EXE。
- clean Windows 不可用时最终状态为 `BLOCKED_CLEAN_WINDOWS_REQUIRED`，不能用开发机替代。

---

## File Structure

### Reader implementation

- Modify: `rust/crates/costing-xlsx/src/reader.rs`
- Modify if needed for snapshot-only tests: `rust/crates/costing-xlsx/src/snapshot.rs`

### Performance and release evidence

- Modify: `tests/rust_oracle/benchmark_protocol.py`
- Modify: `tests/rust_oracle/phase0_harness.py`
- Modify: `tests/rust_oracle/test_phase0_harness.py`
- Modify: `tests/rust_oracle/run_clean_windows_smoke.ps1`
- Modify: `tests/rust_oracle/verify_pe_imports.ps1`
- Modify: `tests/rust_oracle/test_windows_release_scripts.py`
- Generate through sanitizer: `docs/performance/runs/phase4/*.json`
- Generate through sanitizer: `docs/performance/releases/2026-07-11-windows-x64-final.json`

### Final command documentation

- Modify only after feature decision: `README.md`
- Modify only after feature decision: `AGENTS.md`
- Modify: `docs/performance/README.md`

### Do not modify

- `src/` Python business/oracle implementation、business contracts/baselines、writer algorithm、fact/normalize/presentation logic。

## Task 1: Freeze Every Current Reader Variant and Boundary

**Files:**
- Modify tests in place: `rust/crates/costing-xlsx/src/reader.rs`
- Modify only if snapshot helper assertions need exposure: `rust/crates/costing-xlsx/src/snapshot.rs`

**Interfaces:**
- Consumes: current pre-optimization reader implementation。
- Produces: characterization tests that must pass before and after direct-range normalization。

- [ ] **Step 1: Add direct header-variant tests without changing implementation**

Add exact tests:

```text
reader::tests::header_string_datetime_iso_and_duration_iso_are_trimmed
reader::tests::header_float_int_bool_datetime_error_and_empty_match_snapshot
reader::tests::range_with_fewer_than_two_rows_is_rejected
reader::tests::unrecognized_adjacent_rows_are_rejected
reader::tests::first_matching_header_pair_wins_after_metadata_rows
```

Construct `calamine::Range<Data>` directly in test helpers so every enum variant is covered without depending on XLSX writer coercion.

- [ ] **Step 2: Add direct data-variant tests**

Add:

```text
reader::tests::data_nonblank_string_preserves_whitespace
reader::tests::data_whitespace_only_string_becomes_blank
reader::tests::data_datetime_iso_and_duration_iso_preserve_source_text
reader::tests::data_float_int_bool_datetime_error_and_empty_match_snapshot
reader::tests::data_integer_float_scientific_nan_and_infinities_match_snapshot
reader::tests::trailing_blank_columns_and_rows_match_snapshot
reader::tests::chinese_text_is_preserved_exactly
```

Expected values must be explicit `CellValue` variants and exact strings/Decimals, not broad pattern-only assertions.

- [ ] **Step 3: Add an immutable before-copy snapshot fixture**

Add `reader::tests::pre_copy_normalizer_snapshot_is_complete` with a small matrix containing all variants, metadata rows, two header rows, nonempty rows and trailing blanks. Serialize or compare a deterministic tuple containing:

```text
sheet_name
both normalized header rows
all CellValue variants and values
row_count
column_count
null_counts
```

The expected tuple is literal test data and contains no ERP values.

- [ ] **Step 4: Run characterization tests and commit separately**

```powershell
cargo test --locked --manifest-path rust/Cargo.toml -p costing-xlsx --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features reader::tests
cargo fmt --manifest-path rust/Cargo.toml --all --check
git add -- rust/crates/costing-xlsx/src/reader.rs rust/crates/costing-xlsx/src/snapshot.rs
git diff --cached --name-only
git diff --cached --check
git commit -m "test(xlsx): characterize calamine reader semantics"
```

Before staging, omit `snapshot.rs` if unchanged. Expected: all new tests pass against the existing copy-based reader.

## Task 2: Normalize the Calamine Range Without the Intermediate Row Copy

**Files:**
- Modify: `rust/crates/costing-xlsx/src/reader.rs`

**Interfaces:**
- Produces: `normalize_range_directly()` and `find_header_start_in_range()`。
- Removes exactly: `range.rows().map(|row| row.to_vec()).collect::<Vec<Vec<Data>>>()`。

- [ ] **Step 1: Add RED implementation-shape and parity tests**

Add exact tests:

```text
reader::tests::direct_range_normalizer_preserves_all_calamine_variants
reader::tests::direct_range_normalizer_preserves_trailing_blanks
reader::tests::direct_range_normalizer_matches_pre_copy_snapshot
reader::tests::direct_range_normalizer_preserves_reader_row_count
reader::tests::direct_header_scan_uses_adjacent_borrowed_rows
```

The first four call the new helper and fail to compile before implementation. The last test uses a synthetic range with two possible pairs and asserts the first valid adjacent pair.

- [ ] **Step 2: Implement direct borrowed traversal**

Use this internal boundary:

```rust
fn normalize_range_directly(
    sheet_name: String,
    range: calamine::Range<calamine::Data>,
) -> Result<RawWorkbook, CostingXlsxError>;

fn find_header_start_in_range(
    range: &calamine::Range<calamine::Data>,
) -> Option<usize>;
```

`Range` is rectangular in Calamine 0.26.1, so the normalized width is `range.width()`. Header scanning keeps only the previous borrowed row:

```rust
fn find_header_start_in_range(range: &Range<Data>) -> Option<usize> {
    let mut rows = range.rows();
    let mut top = rows.next()?;
    for (index, bottom) in rows.enumerate() {
        if is_header_pair(top, bottom) {
            return Some(index);
        }
        top = bottom;
    }
    None
}
```

After finding the pair, normalize headers and data directly from fresh `range.rows()` iterators. Do not collect `Data` rows or clone the Range.

- [ ] **Step 3: Prove the removed allocation shape**

```powershell
if (rg -n "Vec<Vec<Data>>|row\.to_vec\(\)|rows\(\).*collect" rust/crates/costing-xlsx/src/reader.rs) {
    throw 'reader still contains the intermediate Data row copy'
}
```

Expected: no matches. This textual guard complements semantic tests; it is not a performance claim.

- [ ] **Step 4: Run Rust and Python regressions**

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --locked --manifest-path rust/Cargo.toml -p costing-xlsx --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
cargo test --locked --manifest-path rust/Cargo.toml --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
uv run python -m pytest tests/rust_oracle -q --basetemp .pytest-tmp/rust-oracle
uv run python -m pytest tests/contracts tests/architecture -q --basetemp .pytest-tmp/contracts
```

Expected: all pass and before/after snapshots are exact.

- [ ] **Step 5: Commit only the direct-range implementation**

```powershell
git add -- rust/crates/costing-xlsx/src/reader.rs
git diff --cached --name-only
git diff --cached --check
git commit -m "perf(xlsx): normalize calamine range without row copy"
```

Record this commit SHA as `$Phase4CodeCommit` for the deterministic keep/revert step.

## Task 3: Run the Mandatory Phase 4/3 Same-Batch A/B

**Files:**
- Modify: `tests/rust_oracle/benchmark_protocol.py`
- Modify: `tests/rust_oracle/phase0_harness.py`
- Modify: `tests/rust_oracle/test_phase0_harness.py`
- Generate: `docs/performance/runs/phase4/*.json`

**Interfaces:**
- Consumes: exact Phase 3 EXE SHA, exact Phase 4 candidate SHA, Phase 3 selected feature set。
- Produces: keep/revert verdict with internal and common gates。

- [ ] **Step 1: Add RED Phase 4 profile tests**

Add:

```text
test_phase4_profile_requires_same_batch_phase3_denominator
test_phase4_profile_requires_reader_instrumented_candidate_schema
test_phase4_profile_accepts_ingest_or_pws_ten_percent_gain
test_phase4_profile_rejects_sk_wall_above_phase3
test_phase4_profile_rejects_gb_ingest_or_pws_regression_above_five_percent
test_phase4_rejected_evidence_records_revert_required
test_phase4_decision_cli_consumes_only_same_run_artifacts
```

- [ ] **Step 2: Implement the Phase 4 profile and decision CLI, then commit tooling**

Implement the already-approved `phase4-vs-phase3`/`phase4-vs-phase0a` profile rows in `benchmark_protocol.py` and `decide-phase4` in `phase0_harness.py`. The decision command requires SK/GB internal plus SK/GB external artifacts, revalidates their profile/binary/input/evidence SHAs, and accepts no thresholds.

```powershell
uv run python -m pytest tests/rust_oracle/test_phase0_harness.py tests/rust_oracle/test_benchmark_protocol.py -q --basetemp .pytest-tmp/phase4-profile -k "phase4"
git add -- tests/rust_oracle/benchmark_protocol.py tests/rust_oracle/phase0_harness.py tests/rust_oracle/test_phase0_harness.py
git diff --cached --check
git commit -m "test(perf): implement phase4 reader decision gate"
```

Expected: all Step 1 RED tests are GREEN before building the candidate.

- [ ] **Step 3: Build Phase 4 with the exact Phase 3 feature set**

Read the closed `selected_feature_set` from Phase 3 evidence and map it mechanically:

```text
low-memory-default   -> --features low-memory
low-memory-zlib      -> --features low-memory,zlib
low-memory-zmij      -> --features low-memory,zmij
low-memory-zlib-zmij -> --features low-memory,zlib,zmij
```

Build to `rust/target/perf-builds/phase4/reader-{selected_label}` with release, locked, MSVC, `--no-default-features`. Do not rebuild the Phase 3 reference; verify its existing SHA against Phase 3 evidence.

Resolve the closed mapping and build with this PowerShell decision:

```powershell
$Phase3Decision = Get-Content -Raw -Encoding UTF8 docs/performance/runs/phase3/decision.json | ConvertFrom-Json
switch ($Phase3Decision.selected_feature_set) {
    'low-memory-default'   { $SelectedFeatures = 'low-memory'; $SelectedLabel = 'low-memory-default' }
    'low-memory-zlib'      { $SelectedFeatures = 'low-memory,zlib'; $SelectedLabel = 'low-memory-zlib' }
    'low-memory-zmij'      { $SelectedFeatures = 'low-memory,zmij'; $SelectedLabel = 'low-memory-zmij' }
    'low-memory-zlib-zmij' { $SelectedFeatures = 'low-memory,zlib,zmij'; $SelectedLabel = 'low-memory-zlib-zmij' }
    default { throw 'unclosed Phase 3 feature set' }
}
$Phase3Exe = "rust/target/perf-builds/phase3/$SelectedLabel/x86_64-pc-windows-msvc/release/costing-calculate.exe"
$Phase4Target = "rust/target/perf-builds/phase4/reader-$SelectedLabel"
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir $Phase4Target --no-default-features --features $SelectedFeatures
$Phase4Exe = "$Phase4Target/x86_64-pc-windows-msvc/release/costing-calculate.exe"
if ((Get-FileHash -Algorithm SHA256 -LiteralPath $Phase3Exe).Hash.ToLowerInvariant() -ne $Phase3Decision.selected_exe_sha256) {
    throw 'Phase 3 EXE SHA drifted'
}
```

- [ ] **Step 4: Run GB/SK Phase4/3 internal and GB/SK common comparisons**

Use `phase4-vs-phase3` for the same-batch internal comparison and `phase4-vs-phase0a` for common gates. Execute all four batches with the same environment inputs and exact executable paths. Evidence destinations are:

```text
docs/performance/runs/phase4/sk-internal.json
docs/performance/runs/phase4/gb-internal.json
docs/performance/runs/phase4/gb-external.json
docs/performance/runs/phase4/sk-external.json
```

The harness must apply mandatory N=10 expansion to wall/PWS together and cannot load historical Phase 3 timings as denominator.

Run:

```powershell
$Phase0AExe = 'rust/target/perf-builds/phase0a/reference/x86_64-pc-windows-msvc/release/costing-calculate.exe'
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline sk --input "$env:COSTING_SK_SAMPLE" --reference-executable $Phase3Exe --candidate-executable $Phase4Exe --reference-label phase3 --candidate-label phase4 --comparison-profile phase4-vs-phase3 --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/sk/.perf-runs --evidence-path docs/performance/runs/phase4/sk-internal.json
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline gb --input "$env:COSTING_GB_SAMPLE" --reference-executable $Phase3Exe --candidate-executable $Phase4Exe --reference-label phase3 --candidate-label phase4 --comparison-profile phase4-vs-phase3 --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/gb/.perf-runs --evidence-path docs/performance/runs/phase4/gb-internal.json
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline gb --input "$env:COSTING_GB_SAMPLE" --reference-executable $Phase0AExe --candidate-executable $Phase4Exe --reference-label phase0a --candidate-label phase4 --comparison-profile phase4-vs-phase0a --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/gb/.perf-runs --evidence-path docs/performance/runs/phase4/gb-external.json
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline sk --input "$env:COSTING_SK_SAMPLE" --reference-executable $Phase0AExe --candidate-executable $Phase4Exe --reference-label phase0a --candidate-label phase4 --comparison-profile phase4-vs-phase0a --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/sk/.perf-runs --evidence-path docs/performance/runs/phase4/sk-external.json
```

- [ ] **Step 5: Apply the exact keep predicate**

Keep only when all are true:

```text
(SK ingest Phase4 / same-batch Phase3 <= 0.90
 or SK PWS Phase4 / same-batch Phase3 <= 0.90)
and SK wall Phase4 / same-batch Phase3 <= 1.00
and GB ingest Phase4 / same-batch Phase3 <= 1.05
and GB PWS Phase4 / same-batch Phase3 <= 1.05
and GB wall/PWS Phase4 / same-batch Phase0A <= 1.05
and GB/SK bytes Phase4 / approved Phase0A <= 1.10
and all correctness/runtime/reader/workbook contracts pass
```

Generate the closed decision without hand-editing JSON:

```powershell
uv run python -m tests.rust_oracle.phase0_harness decide-phase4 --sk-internal docs/performance/runs/phase4/sk-internal.json --gb-internal docs/performance/runs/phase4/gb-internal.json --gb-external docs/performance/runs/phase4/gb-external.json --sk-external docs/performance/runs/phase4/sk-external.json --phase3-decision docs/performance/runs/phase3/decision.json --output docs/performance/runs/phase4/decision.json
```

The command revalidates batch/profile/binary hashes and writes exact fields `selected_label`、`selected_feature_set`、`selected_exe_sha256`、`selected_code_commit`、`final_candidate_exe_sha256`、`decision_verdict` and sorted `required_evidence_sha256`. `decision_verdict` is either `KEEP_PHASE4` or `REVERT_TO_PHASE3`; the command accepts no numeric thresholds.

- [ ] **Step 6: Commit evidence only, then keep or revert**

```powershell
git add -- docs/performance/runs/phase4
uv run python -m tests.rust_oracle.evidence scan --root docs/performance --staged
git diff --cached --check
git commit -m "docs(perf): decide phase4 reader optimization"
```

If verdict is rejected, run:

```powershell
$Phase4CodeCommit = (git log -1 --format=%H --fixed-strings --grep='perf(xlsx): normalize calamine range without row copy').Trim()
if ($Phase4CodeCommit -notmatch '^[0-9a-f]{40}$') { throw 'Phase 4 code commit is missing' }
git revert --no-edit $Phase4CodeCommit
```

Expected: final candidate label in the Phase 4 decision artifact is exactly `phase4-reader` when kept or `phase3-writer` when reverted. Do not manually edit the evidence after generation.

## Task 4: Freeze the Final Candidate Command and Run Full Verification

**Files:**
- Modify: `tests/rust_oracle/phase0_harness.py`
- Modify: `tests/rust_oracle/test_phase0_harness.py`
- No production algorithm changes。

**Interfaces:**
- Produces: final candidate EXE path/SHA/feature set closure and a release-command emitter。

```python
@dataclass(frozen=True)
class ReleaseIdentity:
    selected_label: Literal['low-memory-default', 'low-memory-zlib', 'low-memory-zmij', 'low-memory-zlib-zmij']
    cargo_features: tuple[str, ...]
    candidate_source: Literal['phase3', 'phase4']
    candidate_exe_sha256: str
    candidate_code_commit: str

def resolve_release_identity(decision_path: Path) -> ReleaseIdentity:
    decision = load_closed_phase4_decision(decision_path)
    assert_sha256(decision.final_candidate_exe, decision.final_candidate_exe_sha256)
    return ReleaseIdentity.from_closed_decision(decision)

def render_release_build_command(identity: ReleaseIdentity) -> str:
    return render_locked_msvc_build(
        target_dir=f'rust/target/release-{identity.selected_label}',
        features=identity.cargo_features,
    )

def validate_release_documents(identity: ReleaseIdentity, documents: tuple[Path, ...]) -> None:
    expected = parse_cargo_command(render_release_build_command(identity))
    for document in documents:
        for command in parse_formal_release_commands(document):
            assert_required_release_flags(command, expected)
```

- [ ] **Step 1: Add RED release-identity tests**

Add:

```text
test_release_identity_matches_phase4_keep_or_revert_decision
test_release_command_uses_locked_msvc_independent_target_and_exact_features
test_release_gate_rejects_binary_sha_change_after_measurement
test_release_gate_rejects_unclosed_feature_label
test_release_doc_parser_rejects_build_or_run_missing_required_flags
test_release_doc_parser_accepts_exact_selected_feature_command
```

- [ ] **Step 2: Select without rebuilding the measured candidate**

Implement the release-identity resolver, `print-release-command`, and `validate-release-docs` subcommands. If Phase 4 was kept, final candidate is the measured Phase 4 EXE. If reverted, final candidate is the measured Phase 3 EXE. Verify file SHA against its sanitized evidence. The emitter prints the exact future reproducible build command for documentation, while Phase 5 gates execute the already measured binary. The docs validator parses every formal `cargo build/run --release` code block and returns nonzero if `--locked`、MSVC target、independent target-dir、`--no-default-features` or exact selected features are missing.

- [ ] **Step 3: Run the full repository verification using the selected features**

Set `$SelectedFeatures` from the closed mapping, not free text, then run:

```powershell
cargo test --release --locked --manifest-path rust/Cargo.toml -p costing-xlsx --target x86_64-pc-windows-msvc --target-dir rust/target/test-final --no-default-features --features $SelectedFeatures
cargo test --release --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/test-final --no-default-features --features $SelectedFeatures
cargo test --locked --manifest-path rust/Cargo.toml --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
cargo fmt --manifest-path rust/Cargo.toml --all --check
uv run python -m pytest tests/rust_oracle -q --basetemp .pytest-tmp/rust-oracle
uv run python -m pytest tests/contracts tests/architecture -q --basetemp .pytest-tmp/contracts
uv run python -m ruff check src tests
uv run python -m ruff format src tests --check
```

Expected: all exit 0. These test binaries are not substitutes for the already measured final release EXE.

- [ ] **Step 4: Commit release-identity tooling**

```powershell
git add -- tests/rust_oracle/phase0_harness.py tests/rust_oracle/test_phase0_harness.py
git diff --cached --name-only
git diff --cached --check
git commit -m "test(release): freeze final candidate identity"
```

## Task 5: Run Final Phase 5 Wall/PWS/Correctness Gates

**Files:**
- Generate sanitized local components: `rust/target/perf-local/final/gb.json` and `rust/target/perf-local/final/sk.json`
- No production-code changes。

**Interfaces:**
- Consumes: fixed Phase 0A reference and fixed final candidate SHA。
- Produces: final GB/SK metric, stage, bytes and correctness verdicts。

- [ ] **Step 1: Run final paired batches once per pipeline**

Use `phase5-vs-phase0a` with the fixed executable paths. The harness performs 1 warm-up, initial global rounds 1–5 for wall and PWS, mandatory rounds 6–10 when any time/PWS gate is within 3%, runtime/OOXML oracle after every normal run, and environment-drift validation against the approved Phase 0A calibration.

Resolve the final candidate from `phase4/decision.json`, reconstruct its known target path, and assert its SHA. Then run:

```powershell
$Phase4Decision = Get-Content -Raw -Encoding UTF8 docs/performance/runs/phase4/decision.json | ConvertFrom-Json
switch ($Phase4Decision.selected_feature_set) {
    'low-memory-default'   { $SelectedFeatures = 'low-memory'; $SelectedLabel = 'low-memory-default' }
    'low-memory-zlib'      { $SelectedFeatures = 'low-memory,zlib'; $SelectedLabel = 'low-memory-zlib' }
    'low-memory-zmij'      { $SelectedFeatures = 'low-memory,zmij'; $SelectedLabel = 'low-memory-zmij' }
    'low-memory-zlib-zmij' { $SelectedFeatures = 'low-memory,zlib,zmij'; $SelectedLabel = 'low-memory-zlib-zmij' }
    default { throw 'unclosed final feature set' }
}
switch ($Phase4Decision.decision_verdict) {
    'KEEP_PHASE4' { $FinalExe = "rust/target/perf-builds/phase4/reader-$SelectedLabel/x86_64-pc-windows-msvc/release/costing-calculate.exe" }
    'REVERT_TO_PHASE3' { $FinalExe = "rust/target/perf-builds/phase3/$SelectedLabel/x86_64-pc-windows-msvc/release/costing-calculate.exe" }
    default { throw 'Phase 4 has no releasable verdict' }
}
$FinalSha = (Get-FileHash -Algorithm SHA256 -LiteralPath $FinalExe).Hash.ToLowerInvariant()
if ($FinalSha -ne $Phase4Decision.final_candidate_exe_sha256) { throw 'final candidate SHA drifted' }
$Phase0AExe = 'rust/target/perf-builds/phase0a/reference/x86_64-pc-windows-msvc/release/costing-calculate.exe'
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline gb --input "$env:COSTING_GB_SAMPLE" --reference-executable $Phase0AExe --candidate-executable $FinalExe --reference-label phase0a --candidate-label phase5 --comparison-profile phase5-vs-phase0a --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/gb/.perf-runs --evidence-path rust/target/perf-local/final/gb.json
uv run python -m tests.rust_oracle.phase0_harness paired --pipeline sk --input "$env:COSTING_SK_SAMPLE" --reference-executable $Phase0AExe --candidate-executable $FinalExe --reference-label phase0a --candidate-label phase5 --comparison-profile phase5-vs-phase0a --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --local-root data/processed/sk/.perf-runs --evidence-path rust/target/perf-local/final/sk.json
```

- [ ] **Step 2: Require the complete final matrix**

```text
SK external wall N-round median <= 20.0 seconds
SK normal PWS N-round median <= 2,147,483,648 bytes
GB wall candidate / same-batch Phase0A <= 1.05
GB PWS candidate / same-batch Phase0A <= 1.05
SK/GB output bytes / approved Phase0A bytes <= 1.10
three Sheet order/name/dimensions/values/storage/style/sharedStrings = pass
runtime counts/quality/error_log semantics = pass
all prior non-target/internal stage gates remain VALIDATED in their original same-batch evidence chain
every formal run succeeds; no missing/duplicate/unbalanced round
```

The Phase 5/0A batch computes only final common gates that Phase 0A can support. It does not invent writer-substage denominators absent from `RuntimeSchema.BASE`. The final manifest includes raw metric values, medians, ratios, exact EXE SHA, input SHA aliases, selected feature set and verdicts, plus SHA-bound closure references to Phase1/0B、Phase2 feature edges、Phase3 feature on/off and Phase4/3 evidence. It contains no workbook, path, ERP basename or raw output.

- [ ] **Step 3: Stop on the first complete pass**

If all gates pass, do not profile or add speculative optimizations. If a gate fails, mark the release evidence rejected and return to the specific owning phase; do not loosen thresholds or take a second unplanned sample batch.

## Task 6: Run PE Imports and Clean-Windows Single-EXE Gate on the Same SHA

**Files:**
- Modify if final gaps are found: `tests/rust_oracle/verify_pe_imports.ps1`
- Modify if final gaps are found: `tests/rust_oracle/run_clean_windows_smoke.ps1`
- Modify: `tests/rust_oracle/test_windows_release_scripts.py`
- Generate sanitized local components: `rust/target/perf-local/final/pe.json` and `rust/target/perf-local/final/smoke.json`。

**Interfaces:**
- Consumes: the exact Phase 5 performance-tested EXE SHA。
- Produces: PE normal/delay imports and clean-host smoke sections of the same final manifest。

- [ ] **Step 1: Verify PE imports locally**

Run `dumpbin /DEPENDENTS` and `/IMPORTS`; use `llvm-readobj --coff-imports` when installed. Require:

```text
no import basename matching (?i)(zlib|libz|deflate).*\.dll
no project-private DLL
no new non-Windows/non-approved Microsoft runtime relative to Phase 0A
release staging directory contains exactly one EXE and no DLL
```

Raw output stays in `rust/target/perf-local/final/`; sanitizer stores only tool/version aliases, parsed basenames, EXE SHA, verdict and local-log SHA.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tests/rust_oracle/verify_pe_imports.ps1 -CandidateExecutable $FinalExe -Phase0AExecutable $Phase0AExe -LocalLogRoot rust/target/perf-local/final/pe-raw -LocalResultPath rust/target/perf-local/final/pe-parsed-raw.json
uv run python -m tests.rust_oracle.evidence pe-imports --local-result rust/target/perf-local/final/pe-parsed-raw.json --candidate-executable $FinalExe --output rust/target/perf-local/final/pe.json
```

- [ ] **Step 2: Run the sanitized fixture on clean Windows 10/11 x64**

The clean host must not have Rust, Cargo, Python or zlib installed. The bundle has only the exact candidate EXE and sanitized workbook fixture. Set process-local TEMP/TMP/TMPDIR to a confirmed-nonexistent canary outside output. Run normal mode and prove:

```text
exit code = 0
CLI allowlist summary says three Sheets and output_written=true
workbook package has exactly the approved three Sheet names
TEMP/TMP/TMPDIR canary path was never created
output directory has no .costing-tmp-* residue
no missing DLL dialog/error
```

Delete output in `finally`. If the clean host is unavailable, record `BLOCKED_CLEAN_WINDOWS_REQUIRED` locally and do not create a validated final manifest.

On the clean host, run the separately provisioned verifier once with each one-input bundle:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File C:\verification\run_clean_windows_smoke.ps1 -Pipeline sk -ExpectedWriterMode Standard -CandidateExecutable C:\smoke-bundle\costing-calculate.exe -SanitizedInput C:\smoke-bundle\sanitized-sk-small.xlsx -OutputRoot C:\smoke-output -LocalResultPath C:\verification-results\smoke-standard.json
powershell -NoProfile -ExecutionPolicy Bypass -File C:\verification\run_clean_windows_smoke.ps1 -Pipeline sk -ExpectedWriterMode LowMemory -CandidateExecutable C:\smoke-bundle\costing-calculate.exe -SanitizedInput C:\smoke-bundle\sanitized-sk-low-memory.xlsx -OutputRoot C:\smoke-output -LocalResultPath C:\verification-results\smoke-low-memory.json
```

After returning the allowlisted result and raw-log SHA to the development worktree, run:

```powershell
uv run python -m tests.rust_oracle.evidence smoke --standard-result rust/target/perf-local/final/smoke-standard-returned.json --low-memory-result rust/target/perf-local/final/smoke-low-memory-returned.json --candidate-executable $FinalExe --output rust/target/perf-local/final/smoke.json
```

`EvidenceSanitizer.build_smoke_summary()` verifies `candidate_exe_sha256 == $FinalSha` and rebuilds the closed local component.

- [ ] **Step 3: Re-run release-script tests and merge sanitized evidence**

```powershell
uv run python -m pytest tests/rust_oracle/test_windows_release_scripts.py tests/rust_oracle/test_evidence.py -q --basetemp .pytest-tmp/final-release
uv run python -m tests.rust_oracle.evidence scan --root docs/performance --staged
```

The final versioned manifest is written only after the performance batch, PE gate and clean-host smoke all refer to the same 64-character EXE SHA.

## Task 7: Synchronize Final Commands Without Reopening the Decision

**Files:**
- Modify: `README.md`
- Modify: `AGENTS.md`
- Modify: `docs/performance/README.md`

**Interfaces:**
- Consumes: closed final feature label and reproducible command emitted by Task 4。
- Produces: exact user/build/test commands; no new architecture decisions。

- [ ] **Step 1: Invoke `doc-updater` for minimum command changes**

Explicitly assign only these edits:

- replace generic release build/run examples with `--locked --target x86_64-pc-windows-msvc --no-default-features` and the exact selected features；
- document independent `--target-dir` for formal builds；
- state Phase 0A/final manifest locations and no-raw-evidence rule；
- keep Python as oracle/legacy and keep all current business rules unchanged。

- [ ] **Step 2: Run command consistency checks**

```powershell
$ReleaseCommand = uv run python -m tests.rust_oracle.phase0_harness print-release-command --decision docs/performance/runs/phase4/decision.json
rg -n --fixed-strings $ReleaseCommand README.md AGENTS.md docs/performance/README.md
uv run python -m tests.rust_oracle.phase0_harness validate-release-docs --decision docs/performance/runs/phase4/decision.json --documents README.md AGENTS.md docs/performance/README.md
```

Expected: the exact emitted build command appears where documented and the validator exits 0；`rg` is only a human-readable aid. Run a `doc_reviewer` read-only review after edits; the reviewer must not modify files.

- [ ] **Step 3: Commit documentation separately**

```powershell
git add -- README.md AGENTS.md docs/performance/README.md
git diff --cached --name-only
git diff --cached --check
git commit -m "docs(rust): publish final release command"
```

## Task 8: Commit the Final Sanitized Release Evidence

**Files:**
- Create: `docs/performance/releases/2026-07-11-windows-x64-final.json`

- [ ] **Step 1: Build the final manifest from the four closed local components**

```powershell
uv run python -m tests.rust_oracle.phase0_harness finalize-release --gb-evidence rust/target/perf-local/final/gb.json --sk-evidence rust/target/perf-local/final/sk.json --pe-evidence rust/target/perf-local/final/pe.json --smoke-evidence rust/target/perf-local/final/smoke.json --phase0a-manifest docs/performance/baselines/2026-07-11-windows-x64-phase0a.json --phase2-decision docs/performance/runs/phase2/decision.json --phase3-decision docs/performance/runs/phase3/decision.json --phase4-decision docs/performance/runs/phase4/decision.json --evidence-root docs/performance/runs --output docs/performance/releases/2026-07-11-windows-x64-final.json
```

The command follows each decision's sorted `required_evidence_sha256`, verifies the full Phase1→4 same-batch closure, and rejects any EXE SHA mismatch, non-validated component, missing gate, unknown field or existing destination. It writes through `EvidenceSanitizer`, not by copying component dictionaries.

- [ ] **Step 2: Scan the complete evidence tree and staged final file**

```powershell
git add -- docs/performance/releases/2026-07-11-windows-x64-final.json
uv run python -m tests.rust_oracle.evidence scan --root docs/performance --staged
git diff --cached --name-only
git diff --cached --check
```

Expected: one staged path, no absolute path/username/hostname/ERP basename/expected/actual/stdout/stderr/raw command, and all sections bind the same final EXE SHA.

- [ ] **Step 3: Verify the user-owned main-workspace change is untouched**

From the main workspace:

```powershell
git -C D:\python_program\02--costing_calculate diff -- rust/crates/costing-core/src/model.rs
git -C D:\python_program\02--costing_calculate status --short
```

Expected: the original three user-owned `#[cfg(test)]` additions remain unstaged; execution worktree commits never include them.

- [ ] **Step 4: Commit and stop**

```powershell
git commit -m "docs(perf): attest final windows release"
```

Do not create a PR unless the user separately asks. Do not rebuild the candidate after this commit and present it as the tested binary.

## Pseudocode Draft

```rust
// 目标：直接借用 Calamine Range 扫描双层表头并规范化数据，删除 Data 行的中间整表复制。
// 输入：第一张 Sheet 的名称与拥有的 Range<Data>。
// 输出：与旧 reader 完全相同的 RawWorkbook；表头不足或不可识别时返回原有错误。

fn normalize_range_directly(
    sheet_name: String,
    range: Range<Data>,
) -> Result<RawWorkbook, CostingXlsxError> {
    if range.height() < 2 {
        return Err(CostingXlsxError::Message(
            "workbook must contain two header rows".to_string(),
        ));
    }

    let header_start = find_header_start_in_range(&range).ok_or_else(|| {
        CostingXlsxError::Message("未找到可识别的成本计算单双层表头".to_string())
    })?;
    let width = range.width();

    let mut rows = range.rows();
    let top = rows
        .nth(header_start)
        .ok_or_else(|| CostingXlsxError::Message("missing top header row".to_string()))?;
    let bottom = rows
        .next()
        .ok_or_else(|| CostingXlsxError::Message("missing bottom header row".to_string()))?;
    let header_rows = [
        normalize_header_row(top, width),
        normalize_header_row(bottom, width),
    ];

    // 只构造业务需要的 CellValue 结果；不再克隆 Calamine Data 行。
    let normalized_rows = range
        .rows()
        .skip(header_start + 2)
        .map(|row| normalize_data_row(row, width))
        .collect();

    Ok(RawWorkbook {
        sheet_name,
        header_rows,
        rows: normalized_rows,
    })
}
```

## Phase 4–5 Exit Checklist

- [ ] Every Calamine header/data variant and whitespace boundary is frozen by explicit tests.
- [ ] `Vec<Vec<Data>>` and `row.to_vec()` are absent from the reader.
- [ ] Phase 4/3 uses same-batch global AB/BA evidence and cannot consume historical denominators.
- [ ] Phase 4 is either retained by the exact predicate or explicitly reverted after rejected evidence is committed.
- [ ] Final repository tests, fmt, pytest and Ruff all pass with the exact selected feature set.
- [ ] Final SK wall and PWS, GB regressions, bytes, non-target stages and correctness all pass.
- [ ] PE imports and clean-Windows smoke bind the same performance-tested EXE SHA.
- [ ] TEMP/TMP/TMPDIR canary is never created and no `.costing-tmp-*` remains.
- [ ] README/AGENTS commands match the closed final decision and were independently reviewed.
- [ ] Final evidence passes full-tree and staged sensitive scans.
- [ ] No PR was created and no unverified rebuilt EXE is represented as the tested release.

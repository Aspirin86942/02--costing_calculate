# Rust 输出 Phase -1D 依赖安全与可恢复错误实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在不升级 rust_xlsxwriter、不提交上游 PR 的前提下，用精确 revision 受控 fork 消除 Standard Sheet 的系统临时文件、将 LowMemory 临时 I/O 全部改为可恢复 `Result`，并让原始 `io::Error` 与结构化上下文到达 CLI JSON。

**Architecture:** fork 在 `Worksheet` 中引入延迟创建的 `DeferredFileWriter`：它仅在 memory-mode 首次写入时创建 tempfile，捕获上游 XML helper 的首个真实 I/O 错误，并在 row/flush/rewind/copy 边界返回 `XlsxError::IoError`。costing 仓库通过 `CostingError::Contextual -> IoSource -> std::io::Error` 保留 source chain，通过 `ErrorContext` 保留 request/stage/output/cleanup 状态。

**Tech Stack:** Rust 2021、rust_xlsxwriter 0.96.0、GitHub CLI、Cargo Git dependency、`thiserror`、`serde`、Windows/MSVC、pytest。

## Global Constraints

- 上游仓库只能是 `https://github.com/jmcnamara/rust_xlsxwriter.git`。
- 上游审计起点只能是 `9134de25afadaee955d0f821862338e3d046a338`，它必须仍由 tag `v0.96.0` 解析得到。
- crates.io 0.96.0 checksum 必须是 `dd1746025420e17b5d62528b930e550e016e857038794d74e169018126ef3d14`。
- 受控 fork 只能是 `https://github.com/Aspirin86942/rust_xlsxwriter.git`，依赖必须锁定完整 40 位 commit SHA。
- 不创建、不提交、不等待 PR；不合并其他上游提交，不自动同步。
- fork 生产源码 diff 默认只允许 `src/worksheet.rs`、`src/workbook.rs`、`src/packager.rs`；只有 row-start/cell-body/row-end fault test 证明 deferred sink 无法恢复时，才允许对 `src/xmlwriter.rs` 增加最窄 fallible row-tag 接口并单独审查。可增加对应 tests 和必要 Cargo metadata。
- Standard Sheet 在 `constant_memory` feature 打开时也不得创建、打开或探测任何 tempfile。
- 四个 ConstantMemory/LowMemory public factory 构造时不得创建 tempfile，不得 `unwrap/expect` I/O。
- 可恢复临时 I/O 不得 panic；必须保留原始 `ErrorKind/raw_os_error()`。
- 非 I/O XLSX 错误只承诺闭合分类和 `ErrorContext`，不承诺穿过 `costing-core` 保留依赖错误类型。
- 实施使用总控计划创建的 clean worktree；不接触主工作区未提交 `model.rs`。

---

## File Structure

### External fork checkout

- Modify: `D:\python_program\rust_xlsxwriter-costing-fork\src\worksheet.rs` — delayed writer、fallible row flush、test-only fault points。
- Modify: `D:\python_program\rust_xlsxwriter-costing-fork\src\workbook.rs` — 四个 memory factory 只传 mode/tempdir。
- Modify: `D:\python_program\rust_xlsxwriter-costing-fork\src\packager.rs` — `flush_last_row/rewind/copy` 使用 `?`。
- Modify only if the named fallback gate fires: `D:\python_program\rust_xlsxwriter-costing-fork\src\xmlwriter.rs` — narrow fallible row-tag method only。
- Modify: `D:\python_program\rust_xlsxwriter-costing-fork\src\worksheet\tests.rs` — create/write/flush/rewind/read 故障注入。
- Modify: `D:\python_program\rust_xlsxwriter-costing-fork\src\workbook\tests.rs` — Standard/TEMP canary、四 factory、空/单/多行 round-trip。

### Costing repository

- Modify: `rust/Cargo.toml`、`rust/Cargo.lock`、`rust/crates/costing-xlsx/Cargo.toml`、`rust/crates/costing-cli/Cargo.toml` — fork pin 与精确 feature forwarding。
- Modify: `rust/crates/costing-core/src/error.rs` — `ErrorStage`、`ErrorContext`、I/O metadata、`Contextual/IoSource/Writer`。
- Modify: `rust/crates/costing-core/src/model.rs` — `ErrorSummary.request_id/details`。
- Modify: `rust/crates/costing-xlsx/src/reader.rs` — `CostingXlsxError::Writer(XlsxError)` source-bearing variant。
- Modify: `rust/crates/costing-xlsx/src/writer.rs` — `WriterError`、safe output ownership/cleanup；本阶段仍强制 Standard。
- Modify: `rust/crates/costing-cli/src/run.rs` — request id、stage context、writer error mapping。
- Modify: `rust/crates/costing-cli/src/main.rs` — 直接序列化扩展 `ErrorSummary`。
- Modify: `rust/crates/costing-cli/tests/cli_errors.rs` 和上述 Rust 文件内单元测试。
- Create: `tests/rust_oracle/evidence.py` — 先落地 dependency evidence 的闭合 sanitizer，Phase 0H 再扩展。
- Create: `tests/rust_oracle/test_evidence.py` — dependency manifest 脱敏和值域测试。
- Create at runtime: `docs/performance/dependencies/2026-07-11-rust-xlsxwriter-0.96.0.json` — 只能由 sanitizer 生成。

### Do not modify

- `src/` Python 生产路径、`tests/contracts/baselines/`、旧 specs/plans、`_archive/`。
- fork 的其他生产源码；禁止复制或重写整套 XML helper。`src/xmlwriter.rs` 仅受上述命名 fallback gate 控制。

### Task 1: Create the Controlled Public Fork Checkout Without a PR

**Files:**
- Create checkout: `D:\python_program\rust_xlsxwriter-costing-fork`
- Preserve: `LICENSE_MIT`、`LICENSE_Apache2.0`

**Interfaces:**
- Consumes: official upstream SHA `9134de25afadaee955d0f821862338e3d046a338`。
- Produces: local branch `costing-fallible-temp-io-v0.96.0` based exactly on that SHA, with `origin` pointing to the approved public fork and `upstream` pointing to the official repository.

- [ ] **Step 1: Verify GitHub identity and create only the fork repository if absent**

```powershell
$ExpectedOwner = 'Aspirin86942'
$ForkRepo = 'Aspirin86942/rust_xlsxwriter'
$ActualOwner = (gh api user --jq .login).Trim()
if ($ActualOwner -ne $ExpectedOwner) {
    throw "GitHub login must be $ExpectedOwner, got $ActualOwner"
}
gh repo view $ForkRepo --json nameWithOwner 2>$null
if ($LASTEXITCODE -ne 0) {
    gh repo fork jmcnamara/rust_xlsxwriter --clone=false --remote=false
}
if ((gh repo view $ForkRepo --json nameWithOwner --jq .nameWithOwner).Trim() -ne $ForkRepo) {
    throw 'controlled fork repository is unavailable'
}
```

Expected: public fork exists. Do not run `gh pr create`.

- [ ] **Step 2: Clone to the exact external checkout and pin the audit base**

```powershell
$ForkCheckout = 'D:\python_program\rust_xlsxwriter-costing-fork'
$UpstreamBase = '9134de25afadaee955d0f821862338e3d046a338'
if (Test-Path -LiteralPath $ForkCheckout) {
    throw "fork checkout already exists; inspect it before continuing: $ForkCheckout"
}
git clone https://github.com/Aspirin86942/rust_xlsxwriter.git $ForkCheckout
git -C $ForkCheckout remote add upstream https://github.com/jmcnamara/rust_xlsxwriter.git
git -C $ForkCheckout fetch upstream refs/tags/v0.96.0:refs/tags/v0.96.0
$ResolvedTag = (git -C $ForkCheckout rev-parse 'refs/tags/v0.96.0^{commit}').Trim()
if ($ResolvedTag -ne $UpstreamBase) { throw "v0.96.0 moved to $ResolvedTag" }
git -C $ForkCheckout switch -c costing-fallible-temp-io-v0.96.0 $UpstreamBase
```

Expected: `git -C $ForkCheckout status --short` is empty and `HEAD` equals the fixed base SHA.

- [ ] **Step 3: Verify license and package provenance**

```powershell
$Required = @('LICENSE_MIT', 'LICENSE_Apache2.0', 'Cargo.toml', 'src/worksheet.rs', 'src/workbook.rs', 'src/packager.rs')
foreach ($Name in $Required) {
    if (-not (Test-Path -LiteralPath (Join-Path $ForkCheckout $Name))) { throw "missing $Name" }
}
$Vcs = Get-Content -LiteralPath "$env:USERPROFILE\.cargo\registry\src\index.crates.io-1949cf8c6b5b557f\rust_xlsxwriter-0.96.0\.cargo_vcs_info.json" -Raw | ConvertFrom-Json
if ($Vcs.git.sha1 -ne $UpstreamBase) { throw 'crates.io vcs SHA mismatch' }
$ExpectedChecksum = 'dd1746025420e17b5d62528b930e550e016e857038794d74e169018126ef3d14'
$LockText = Get-Content -Raw -Encoding UTF8 'rust/Cargo.lock'
$LockMatch = [regex]::Match($LockText, '(?ms)\[\[package\]\]\s+name = "rust_xlsxwriter"\s+version = "0\.96\.0"\s+source = "registry\+[^\r\n]+"\s+checksum = "([0-9a-f]{64})"')
if (-not $LockMatch.Success -or $LockMatch.Groups[1].Value -ne $ExpectedChecksum) {
    throw 'Cargo.lock crates.io checksum mismatch'
}
cargo fetch --locked --manifest-path rust/Cargo.toml
$Archives = @(Get-ChildItem -Path "$env:USERPROFILE\.cargo\registry\cache" -Recurse -Filter 'rust_xlsxwriter-0.96.0.crate')
if ($Archives.Count -lt 1) { throw 'cached rust_xlsxwriter 0.96.0 crate archive is missing' }
foreach ($Archive in $Archives) {
    $ArchiveChecksum = (Get-FileHash -Algorithm SHA256 -LiteralPath $Archive.FullName).Hash.ToLowerInvariant()
    if ($ArchiveChecksum -ne $ExpectedChecksum) { throw "crates.io archive checksum mismatch: $($Archive.FullName)" }
}
```

Expected: both upstream licenses remain present, crates.io provenance points at the exact base, and both lock entry and cached `.crate` archive equal the approved checksum. Task 6 repeats these checks into the ignored provenance log before dependency replacement can be attested.

### Task 2: Make rust_xlsxwriter Temporary I/O Lazy and Recoverable

**Files:**
- Modify: `D:\python_program\rust_xlsxwriter-costing-fork\src\worksheet.rs`
- Modify: `D:\python_program\rust_xlsxwriter-costing-fork\src\workbook.rs`
- Modify: `D:\python_program\rust_xlsxwriter-costing-fork\src\packager.rs`
- Modify: `D:\python_program\rust_xlsxwriter-costing-fork\src\worksheet\tests.rs`
- Modify: `D:\python_program\rust_xlsxwriter-costing-fork\src\workbook\tests.rs`

**Interfaces:**
- Consumes: existing public write methods returning `Result<&mut Worksheet, XlsxError>` and existing `XlsxError::IoError(std::io::Error)`.
- Produces: `DeferredFileWriter::{set_tempdir,take_write_error,flush_result,rewind_result}`; `insert_cell/flush_to_row/flush_data_row/flush_last_row -> Result<_, XlsxError>`; unchanged public workbook API.

- [ ] **Step 1: Add failing factory and fault-injection tests**

Add test-only cases with these exact names:

Add tests with these exact names:

```text
standard_worksheet_never_initializes_temp_writer
all_memory_factories_defer_tempfile_creation
add_constant_memory_create_failure_is_recoverable
add_low_memory_create_failure_is_recoverable
new_constant_memory_create_failure_is_recoverable
new_low_memory_create_failure_is_recoverable
row_start_write_failure_returns_original_io_error
cell_body_write_failure_returns_original_io_error
row_end_write_failure_returns_original_io_error
flush_failure_returns_original_io_error
rewind_failure_returns_original_io_error
packager_copy_failure_returns_original_io_error
removed_tempdir_after_set_tempdir_returns_io_error
empty_single_and_multi_row_low_memory_sheets_round_trip
```

The first test asserts `file_writer.is_initialized() == false` before and after Standard writes. The second calls each of the four memory factories and makes the same assertion before any cell write. Each four-factory test injects `Create`, performs its first real write/save, wraps the call with `catch_unwind`, asserts no panic, then asserts `XlsxError::IoError` with `ErrorKind::StorageFull` and raw OS 112.

The row-start/cell-body/row-end tests arm a test-only `NextWrite` immediately before that exact XML boundary. The copy test arms `ReadDuringCopy` and proves the failure is returned by the `std::io::copy` path. The TOCTOU test calls `set_tempdir()`, removes that already-validated directory, then proves the first LowMemory I/O returns `IoError` without panic. Empty/single/multi tests save and reopen separate workbooks.

The shared assertion for deterministic injected error 112 is below. The real TOCTOU test instead asserts `IoError`, non-panicking behavior and preservation of the OS-provided kind/raw code without requiring that platform-dependent code to equal 112.

```rust
#[test]
fn assert_storage_full(error: XlsxError) {
    let XlsxError::IoError(source) = error else { panic!("expected IoError") };
    assert_eq!(source.kind(), std::io::ErrorKind::StorageFull);
    assert_eq!(source.raw_os_error(), Some(112));
}
```

Add an isolated child-process test that sets `TEMP/TMP/TMPDIR` to a nonexistent canary, writes one Standard sheet and one LowMemory sheet with a controlled tempdir, then asserts the canary was never created.

Run:

```powershell
$ForkCheckout = 'D:\python_program\rust_xlsxwriter-costing-fork'
cargo test --manifest-path "$ForkCheckout\Cargo.toml" --target x86_64-pc-windows-msvc --target-dir D:\python_program\02--costing_calculate\rust\target\fork-test-msvc --no-default-features --features constant_memory
cargo test --release --manifest-path "$ForkCheckout\Cargo.toml" --target x86_64-pc-windows-msvc --target-dir D:\python_program\02--costing_calculate\rust\target\fork-test-release-msvc --no-default-features --features constant_memory
```

Expected: RED because delayed writer/fault-point APIs do not exist yet.

- [ ] **Step 2: Introduce the delayed writer without changing XML helpers**

In `worksheet.rs`, replace the eager `BufWriter<File>` field with this responsibility-equivalent type:

```rust
#[cfg(feature = "constant_memory")]
pub(crate) struct DeferredFileWriter {
    writer: Option<BufWriter<File>>,
    tempdir: Option<PathBuf>,
    first_write_error: Option<std::io::Error>,
    #[cfg(test)]
    failure_point: Option<TempIoFailurePoint>,
}

#[cfg(test)]
enum TempIoFailurePoint {
    Create,
    NextWrite,
    Flush,
    Rewind,
    ReadDuringCopy,
}

#[cfg(feature = "constant_memory")]
impl DeferredFileWriter {
    fn new() -> Self {
        Self { writer: None, tempdir: None, first_write_error: None, #[cfg(test)] failure_point: None }
    }

    pub(crate) fn set_tempdir(&mut self, tempdir: Option<PathBuf>) {
        self.tempdir = tempdir;
    }

    fn ensure_writer(&mut self) -> std::io::Result<&mut BufWriter<File>> {
        if self.writer.is_none() {
            #[cfg(test)]
            if self.failure_point == Some(TempIoFailurePoint::Create) {
                return Err(std::io::Error::from_raw_os_error(112));
            }
            let directory = self.tempdir.clone().unwrap_or_else(std::env::temp_dir);
            self.writer = Some(BufWriter::new(tempfile_in(directory)?));
        }
        Ok(self.writer.as_mut().expect("writer initialized above"))
    }

    fn remember(&mut self, error: std::io::Error) {
        if self.first_write_error.is_none() { self.first_write_error = Some(error); }
    }

    pub(crate) fn take_write_error(&mut self) -> Option<std::io::Error> {
        self.first_write_error.take()
    }

    pub(crate) fn flush_result(&mut self) -> std::io::Result<()> {
        if let Some(error) = self.take_write_error() { return Err(error); }
        #[cfg(test)]
        if self.failure_point == Some(TempIoFailurePoint::Flush) {
            return Err(std::io::Error::from_raw_os_error(112));
        }
        self.ensure_writer()?.flush()
    }

    pub(crate) fn rewind_result(&mut self) -> std::io::Result<()> {
        self.flush_result()?;
        #[cfg(test)]
        if self.failure_point == Some(TempIoFailurePoint::Rewind) {
            return Err(std::io::Error::from_raw_os_error(112));
        }
        self.ensure_writer()?.seek(SeekFrom::Start(0)).map(|_| ())
    }
}
```

Implement `Write` so existing `xmlwriter`/cell helpers never panic, but the first real error is retained:

```rust
impl Write for DeferredFileWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.first_write_error.is_some() { return Ok(bytes.len()); }
        #[cfg(test)]
        if self.failure_point == Some(TempIoFailurePoint::NextWrite) {
            self.remember(std::io::Error::from_raw_os_error(112));
            return Ok(bytes.len());
        }
        match self.ensure_writer().and_then(|writer| writer.write_all(bytes)) {
            Ok(()) => Ok(bytes.len()),
            Err(error) => { self.remember(error); Ok(bytes.len()) }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.first_write_error.is_some() { return Ok(()); }
        match self.ensure_writer().and_then(BufWriter::flush) {
            Ok(()) => Ok(()),
            Err(error) => { self.remember(error); Ok(()) }
        }
    }
}
```

Implement `Read` by delegating to `writer.get_mut().read`, with the test-only `ReadDuringCopy` fault returning raw OS error 112. `Worksheet::new()` initializes only `DeferredFileWriter::new()` and must remove `tempfile_in(std::env::temp_dir()).unwrap()`.

- [ ] **Step 3: Propagate row-flush failures through existing public write Results**

Change the exact internal signatures:

```rust
fn insert_cell(&mut self, row: RowNum, col: ColNum, cell: CellType) -> Result<(), XlsxError>
fn flush_to_row(&mut self, next_row: RowNum) -> Result<(), XlsxError>
fn flush_data_row(&mut self, next_row: RowNum) -> Result<(), XlsxError>
pub(crate) fn flush_last_row(&mut self) -> Result<(), XlsxError>
```

At the end of every `flush_data_row` path:

```rust
if let Some(error) = self.file_writer.take_write_error() {
    return Err(XlsxError::IoError(error));
}
Ok(())
```

Add `?` to the eight existing `insert_cell` call sites in `store_number`, `store_string`, `store_rich_string`, `store_formula`, `store_array_formula`, `store_blank`, `store_boolean`, and `store_embedded_image`. Add `?` to all `flush_to_row`/`flush_data_row` loops. Do not change the public write method signatures.

- [ ] **Step 3a: Audit every temp XML expect path and apply the narrow fallback only if proven necessary**

```powershell
$ForkCheckout = 'D:\python_program\rust_xlsxwriter-costing-fork'
rg -n "expect\(XML_WRITE_ERROR\)|\.unwrap\(\)" "$ForkCheckout\src\worksheet.rs" "$ForkCheckout\src\packager.rs" "$ForkCheckout\src\xmlwriter.rs"
foreach ($TestName in @('row_start_write_failure_returns_original_io_error', 'cell_body_write_failure_returns_original_io_error', 'row_end_write_failure_returns_original_io_error')) {
    cargo test --manifest-path "$ForkCheckout\Cargo.toml" --target x86_64-pc-windows-msvc --target-dir D:\python_program\02--costing_calculate\rust\target\fork-test-msvc --no-default-features --features constant_memory $TestName
    if ($LASTEXITCODE -ne 0) { throw "fault gate failed: $TestName" }
}
```

Expected: all three faults return the original I/O error through the deferred sink. If and only if one named test still panics because its row tag never reaches the sink boundary, permit a smallest `src/xmlwriter.rs` method that returns `io::Result<()>` for that exact tag. Record `xmlwriter_fallback_used=true` and the triggering test name in dependency evidence, add only that file to the diff allowlist, and rerun all three tests. Do not generalize or rewrite XML helpers.

- [ ] **Step 4: Make all four factories construction-only**

For each of the four public factory methods in `workbook.rs`, remove `BufWriter::new(tempfile_in(tempdir).unwrap())` and use the same state-only configuration:

```rust
worksheet.use_constant_memory = true;
worksheet.use_inline_strings = use_inline_strings;
worksheet.file_writer.set_tempdir(self.tempdir.clone());
```

The exact `use_inline_strings` values remain as upstream: `true` for constant-memory and `false` for low-memory. `Workbook::set_tempdir()` keeps its controlled-directory writability probe.

- [ ] **Step 5: Make packaging fallible**

Replace the constant-memory section in `packager.rs` with:

```rust
worksheet.flush_last_row()?;
worksheet.file_writer.rewind_result()?;
std::io::copy(&mut worksheet.file_writer, &mut self.zip)?;
```

Expected: empty sheets create their tempfile at `rewind_result()`, and write/flush/rewind/read errors all convert through existing `From<std::io::Error> for XlsxError`.

- [ ] **Step 6: Run fork tests and audit the narrow diff**

```powershell
$ForkCheckout = 'D:\python_program\rust_xlsxwriter-costing-fork'
$Base = '9134de25afadaee955d0f821862338e3d046a338'
cargo fmt --manifest-path "$ForkCheckout\Cargo.toml" --all --check
cargo test --manifest-path "$ForkCheckout\Cargo.toml" --target x86_64-pc-windows-msvc --target-dir D:\python_program\02--costing_calculate\rust\target\fork-test-msvc --no-default-features
cargo test --manifest-path "$ForkCheckout\Cargo.toml" --target x86_64-pc-windows-msvc --target-dir D:\python_program\02--costing_calculate\rust\target\fork-test-msvc --no-default-features --features constant_memory
cargo test --release --manifest-path "$ForkCheckout\Cargo.toml" --target x86_64-pc-windows-msvc --target-dir D:\python_program\02--costing_calculate\rust\target\fork-test-release-msvc --no-default-features --features constant_memory
git -C $ForkCheckout diff --check $Base --
$Changed = @(git -C $ForkCheckout diff --name-only $Base --)
$RequiredChanged = @('src/worksheet.rs', 'src/workbook.rs', 'src/packager.rs', 'src/worksheet/tests.rs', 'src/workbook/tests.rs')
$AllowedChanged = $RequiredChanged + @('src/xmlwriter.rs')
if (Compare-Object $Changed ($Changed | Where-Object { $_ -in $AllowedChanged })) { throw 'fork diff contains an unapproved path' }
foreach ($Path in $RequiredChanged) { if ($Path -notin $Changed) { throw "missing required fork diff: $Path" } }
```

Expected production-source diff is the three required files and, only when Step 3a recorded the named fallback trigger, `src/xmlwriter.rs`; remaining paths are the two corresponding tests.

- [ ] **Step 7: Commit and push the fork branch, without a PR**

```powershell
$ForkCheckout = 'D:\python_program\rust_xlsxwriter-costing-fork'
$Base = '9134de25afadaee955d0f821862338e3d046a338'
$ForkPaths = @('src/worksheet.rs', 'src/workbook.rs', 'src/packager.rs', 'src/worksheet/tests.rs', 'src/workbook/tests.rs')
git -C $ForkCheckout diff --quiet $Base -- src/xmlwriter.rs
if ($LASTEXITCODE -eq 1) { $ForkPaths += 'src/xmlwriter.rs' }
elseif ($LASTEXITCODE -ne 0) { throw 'cannot inspect xmlwriter fallback diff' }
git -C $ForkCheckout add -- $ForkPaths
git -C $ForkCheckout diff --cached --check
git -C $ForkCheckout commit -m "fix: make constant-memory temp IO recoverable"
git -C $ForkCheckout push -u origin costing-fallible-temp-io-v0.96.0
$ForkRevision = (git -C $ForkCheckout rev-parse HEAD).Trim()
if ($ForkRevision -notmatch '^[0-9a-f]{40}$') { throw 'fork revision must be a full SHA' }
$Prs = gh pr list --repo jmcnamara/rust_xlsxwriter --head Aspirin86942:costing-fallible-temp-io-v0.96.0 --state all --json url | ConvertFrom-Json
if (@($Prs).Count -ne 0) { throw 'an upstream PR exists for the controlled branch' }
```

Expected: branch is public at the approved fork and the read-only PR query returns an empty list. Do not invoke any PR creation command.

### Task 3: Pin the Fork and Forward Exact Features

**Files:**
- Modify: `rust/Cargo.toml`
- Modify: `rust/Cargo.lock`
- Modify: `rust/crates/costing-xlsx/Cargo.toml`
- Modify: `rust/crates/costing-cli/Cargo.toml`

**Interfaces:**
- Consumes: full `$ForkRevision` produced by Task 2.
- Produces: `costing-calculate` features `low-memory`, `zlib`, and `zmij`; exactly one Git-sourced rust_xlsxwriter revision in metadata/lock.

- [ ] **Step 1: Write a failing feature/source assertion**

Add a temporary command assertion before editing manifests:

```powershell
$Metadata = cargo metadata --locked --manifest-path rust/Cargo.toml --format-version 1 | ConvertFrom-Json
$Xlsx = @($Metadata.packages | Where-Object name -eq 'rust_xlsxwriter')
if ($Xlsx.Count -ne 1 -or $Xlsx[0].source -notlike 'git+https://github.com/Aspirin86942/rust_xlsxwriter.git*') {
    throw 'rust_xlsxwriter is not yet pinned to the controlled fork'
}
```

Expected: FAIL against the current registry dependency.

- [ ] **Step 2: Apply the exact workspace and crate feature declarations**

Set the workspace dependency to the runtime-generated full SHA. Immediately before the patch, obtain it with:

```powershell
$ForkCheckout = 'D:\python_program\rust_xlsxwriter-costing-fork'
$ForkRevision = (git -C $ForkCheckout rev-parse HEAD).Trim()
if ($ForkRevision -notmatch '^[0-9a-f]{40}$') { throw 'fork revision must be a full SHA' }
```

Use `apply_patch` to set the workspace dependency fields to exact Git URL `https://github.com/Aspirin86942/rust_xlsxwriter.git`, `default-features = false`, and `rev` equal to the 40 characters currently held in `$ForkRevision`. Also add workspace `tempfile = "3"` and `windows-sys = { version = "0.61", features = ["Win32_Storage_FileSystem"] }`. A branch, symbolic value or short SHA is invalid.

After patching, parse `rust/Cargo.toml` and assert its `rev` value equals `$ForkRevision` before running Cargo.

In `costing-xlsx/Cargo.toml`:

```toml
[features]
default = []
low-memory = ["rust_xlsxwriter/constant_memory", "dep:tempfile", "dep:windows-sys"]
zlib = ["rust_xlsxwriter/zlib"]
zmij = ["rust_xlsxwriter/zmij"]

[dependencies]
tempfile = { workspace = true, optional = true }
windows-sys = { workspace = true, optional = true }
```

In `costing-cli/Cargo.toml`:

```toml
[features]
default = []
low-memory = ["costing-xlsx/low-memory"]
zlib = ["costing-xlsx/zlib"]
zmij = ["costing-xlsx/zmij"]

[dev-dependencies]
rust_xlsxwriter.workspace = true
```

- [ ] **Step 3: Lock and verify every feature graph**

```powershell
cargo update --manifest-path rust/Cargo.toml -p rust_xlsxwriter
cargo test --locked --manifest-path rust/Cargo.toml --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
foreach ($Features in @('low-memory', 'zlib', 'zmij', 'zlib,zmij', 'low-memory,zlib,zmij')) {
    cargo check --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir "rust/target/feature-check/$($Features.Replace(',','-'))" --no-default-features --features $Features
    if ($LASTEXITCODE -ne 0) { throw "feature check failed: $Features" }
}
$Metadata = cargo metadata --locked --manifest-path rust/Cargo.toml --format-version 1 | ConvertFrom-Json
$Xlsx = @($Metadata.packages | Where-Object name -eq 'rust_xlsxwriter')
if ($Xlsx.Count -ne 1 -or $Xlsx[0].source -notmatch '#[0-9a-f]{40}$') { throw 'fork source is not uniquely pinned' }
```

Expected: all commands exit 0 and metadata has one rust_xlsxwriter package at the fork revision.

- [ ] **Step 4: Commit only dependency wiring**

```powershell
git add -- rust/Cargo.toml rust/Cargo.lock rust/crates/costing-xlsx/Cargo.toml rust/crates/costing-cli/Cargo.toml
git diff --cached --name-only
git commit -m "build(xlsx): pin recoverable writer fork"
```

### Task 4: Add Core Error Context and Stable CLI Serialization

**Files:**
- Modify: `rust/crates/costing-core/src/error.rs`
- Modify: `rust/crates/costing-core/src/model.rs`
- Modify: `rust/crates/costing-cli/src/run.rs`
- Modify: `rust/crates/costing-cli/src/main.rs`
- Modify: `rust/crates/costing-cli/tests/cli_errors.rs`

**Interfaces:**
- Produces: `ErrorStage`, `IoKindCode`, `IoFailureMeta`, `CleanupFailureMeta`, `ErrorDetails`, `ErrorContext`, `CostingError::{IoSource,Writer,Contextual}`, `CostingError::with_context`, `ErrorSummary::from_error`.

- [ ] **Step 1: Add failing serialization and delegation tests**

Add tests that assert:

```rust
let contextual = CostingError::io_with_source(
    ErrorCode::OutputNotWritable,
    "write failed",
    std::io::Error::from_raw_os_error(112),
).with_context(ErrorContext::new(
    "costing-test-1",
    ErrorStage::SaveWorkbook,
    Some(PathBuf::from("output.xlsx")),
));
assert_eq!(contextual.code(), ErrorCode::OutputNotWritable);
assert!(contextual.retryable());
assert_eq!(contextual.context().unwrap().request_id, "costing-test-1");
let summary = ErrorSummary::from_error(&contextual);
assert_eq!(summary.request_id.as_deref(), Some("costing-test-1"));
assert_eq!(summary.details.as_ref().unwrap().io_meta.as_ref().unwrap().kind, IoKindCode::StorageFull);
assert_eq!(summary.details.as_ref().unwrap().io_meta.as_ref().unwrap().raw_os_error, Some(112));
```

Also walk `std::error::Error::source()` from the outer contextual error to the same underlying `std::io::Error`, asserting `ErrorKind::StorageFull` and raw OS 112. Add a separate `TimedOut` source test proving retryability can come from `ErrorKind` without a Windows raw code. Assert `with_context` returns an already contextual error unchanged and CLI parse failures serialize `request_id=null`, `details=null`.

Run:

```powershell
cargo test --locked --manifest-path rust/Cargo.toml -p costing-core --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features error
cargo test --locked --manifest-path rust/Cargo.toml -p costing-calculate --test cli_errors --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
```

Expected: RED because the new types and fields do not exist.

- [ ] **Step 2: Implement the transport types in `costing-core`**

Use closed serializable enums. Add stable `ErrorCode::InsufficientDiskSpace` and `ErrorCode::TempCleanupFailed`. `ErrorStage` includes exactly the approved stages from `ValidateCliRequest` through `ReadOutputMetadata`; `IoKindCode` includes `Interrupted`, `WouldBlock`, `TimedOut`, `AlreadyExists`, `PermissionDenied`, `InvalidInput`, `InvalidData`, `NotFound`, `StorageFull`, and `Other`.

Implement the core ownership shape:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ErrorDetails {
    pub stage: ErrorStage,
    pub path: Option<PathBuf>,
    #[serde(flatten)]
    pub io_meta: Option<IoFailureMeta>,
    pub final_output_valid: bool,
    pub partial_output_removed: Option<bool>,
    pub cleanup_failures: Vec<CleanupFailureMeta>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorContext {
    pub request_id: String,
    pub details: ErrorDetails,
}

#[derive(Debug, Error)]
pub enum CostingError {
    #[error("{message}")]
    IoSource {
        code: ErrorCode,
        message: String,
        retryable: bool,
        io_meta: IoFailureMeta,
        #[source]
        source: std::io::Error,
    },
    #[error("{message}")]
    Writer { code: ErrorCode, message: String, retryable: bool },
    #[error("{source}")]
    Contextual { context: ErrorContext, #[source] source: Box<CostingError> },
}
```

`code/message/retryable` recursively delegate through `Contextual.source`. `context()` returns the outer context. `with_context()` does not double-wrap. `io_with_source()` calculates `IoFailureMeta` before moving the original error; retryability is exactly `Interrupted/WouldBlock/TimedOut` or raw OS `{32,33,39,112}`.

- [ ] **Step 3: Extend `ErrorSummary` without parsing text**

Add fields:

```rust
pub request_id: Option<String>,
pub details: Option<ErrorDetails>,
```

Implement `ErrorSummary::from_error(error: &CostingError)` by cloning `error.context()` and using the recursive accessors. Never inspect `message()` to recover stage or I/O metadata.

In `main.rs`, CLI parse errors explicitly use `None/None`; runtime errors call `ErrorSummary::from_error`. Move request-id creation before `build_month_range` and path validation. Generate without a new dependency:

```rust
fn new_request_id() -> String {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    format!("costing-{}-{nanos}", std::process::id())
}
```

At each `run.rs` stage boundary, attach the corresponding `ErrorContext`; `with_context` must leave writer-created contexts unchanged.

- [ ] **Step 4: Run focused and workspace tests**

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --locked --manifest-path rust/Cargo.toml -p costing-core --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
cargo test --locked --manifest-path rust/Cargo.toml -p costing-calculate --test cli_errors --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
cargo test --locked --manifest-path rust/Cargo.toml --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
```

Expected: parse errors have null context; every error after args acceptance has a request id and closed stage; all existing codes remain unchanged.

- [ ] **Step 5: Commit the core error transport**

```powershell
git add -- rust/crates/costing-core/src/error.rs rust/crates/costing-core/src/model.rs rust/crates/costing-cli/src/run.rs rust/crates/costing-cli/src/main.rs rust/crates/costing-cli/tests/cli_errors.rs
git diff --cached --name-only
git commit -m "feat(errors): preserve writer failure context"
```

### Task 5: Replace Writer Stringification With Safe Output Ownership

**Files:**
- Modify: `rust/crates/costing-xlsx/src/reader.rs`
- Modify: `rust/crates/costing-xlsx/src/writer.rs`
- Modify: `rust/crates/costing-cli/src/run.rs`

**Interfaces:**
- Produces: `WriterContext`, `WriterPrimaryError`, `WriterError`, `OutputArtifactState`; `write_workbook(&WriterContext, &Path, &WorkbookPayload) -> Result<(), WriterError>`.

- [ ] **Step 1: Add failing source/cleanup/ownership tests**

Add tests named:

```text
writer_io_error_reaches_cli_with_same_raw_os_error
cleanup_failure_does_not_replace_primary_error
not_created_never_deletes_existing_path
created_by_current_run_removes_partial_output
completed_output_is_not_deleted_by_secondary_cleanup_failure
```

The first test constructs `XlsxError::IoError(std::io::Error::from_raw_os_error(112))`, routes it through `WriterError` and `map_xlsx_write_error`, and walks the standard source chain to assert both `ErrorKind::StorageFull` and raw OS 112；JSON `details.io_meta` must report the same values. The renamed secondary-cleanup test covers only ownership/error merging in Phase -1D；real `TempWorkspace` cleanup remains in Phase 3.

Run the focused packages; expect RED because writer errors are still converted to `Message(error.to_string())`.

- [ ] **Step 2: Add source-bearing writer errors and explicit artifact ownership**

In `CostingXlsxError`, add:

```rust
#[error("xlsx writer error: {0}")]
Writer(#[source] rust_xlsxwriter::XlsxError),
```

Remove `CostingXlsxError::OutputExists(PathBuf)` after adapting its tests. Output existence is determined only from the original `create_new(true)` `std::io::Error` with `ErrorKind::AlreadyExists`, so this path cannot bypass source preservation.

In `writer.rs`, add:

```rust
pub struct WriterContext { pub request_id: String }

#[derive(Debug, thiserror::Error)]
pub enum WriterPrimaryError {
    #[error("{0}")]
    Io(#[source] std::io::Error),
    #[error("{0}")]
    Xlsx(#[source] CostingXlsxError),
    #[error("{0}")]
    Contract(String),
}

#[derive(Debug, thiserror::Error)]
#[error("{primary}")]
pub struct WriterError {
    pub context: ErrorContext,
    #[source]
    pub primary: WriterPrimaryError,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutputArtifactState { NotCreated, CreatedByCurrentRun, CompletedByCurrentRun }
```

All rust_xlsxwriter calls use `CostingXlsxError::Writer` or `WriterPrimaryError::Xlsx`; remove every `.map_err(|error| Message(error.to_string()))`. Parent-directory and final-file I/O use `WriterPrimaryError::Io`.

`write_workbook` keeps Standard sheets in Phase -1D, populates before `create_new(true)`, saves through a mutable final file, flushes/closes it, verifies nonzero metadata, and deletes only `CreatedByCurrentRun` on failure. Cleanup errors append `CleanupFailureMeta` to `context.details.cleanup_failures` without changing the primary code/source.

- [ ] **Step 3: Map writer errors structurally at the CLI boundary**

`map_xlsx_write_error` matches:

```rust
WriterPrimaryError::Io(source)
| WriterPrimaryError::Xlsx(CostingXlsxError::Writer(XlsxError::IoError(source)))
```

It computes metadata from `&source`, moves that same source into `CostingError::IoSource`, and wraps the result in `CostingError::Contextual`. `OutputExists` maps to `OUTPUT_EXISTS`; non-I/O XLSX/contract errors map to `CostingError::Writer` with `retryable=false`.

- [ ] **Step 4: Run all Phase -1D project tests**

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --locked --manifest-path rust/Cargo.toml -p costing-xlsx --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
cargo test --locked --manifest-path rust/Cargo.toml -p costing-calculate --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
cargo test --locked --manifest-path rust/Cargo.toml -p costing-xlsx --target x86_64-pc-windows-msvc --target-dir rust/target/test-low-memory --no-default-features --features low-memory
cargo test --locked --manifest-path rust/Cargo.toml --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
```

Expected: source/error-context tests and existing concurrency/no-overwrite tests all pass.

- [ ] **Step 5: Commit only the project adapter**

```powershell
git add -- rust/crates/costing-xlsx/src/reader.rs rust/crates/costing-xlsx/src/writer.rs rust/crates/costing-cli/src/run.rs
git diff --cached --name-only
git commit -m "fix(xlsx): preserve recoverable writer IO errors"
```

### Task 6: Generate Sanitized Dependency Provenance and Close Phase -1D

**Files:**
- Create: `tests/rust_oracle/evidence.py`
- Create: `tests/rust_oracle/test_evidence.py`
- Create by command: `docs/performance/dependencies/2026-07-11-rust-xlsxwriter-0.96.0.json`

**Interfaces:**
- Produces: `DependencyEvidence` and `EvidenceSanitizer.write_dependency_manifest`; Phase 0H extends the same module for benchmark/release evidence.

- [ ] **Step 1: Add failing dependency evidence tests**

```python
def test_dependency_manifest_rejects_moving_or_sensitive_values(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        EvidenceSanitizer.write_dependency_manifest(
            tmp_path / 'bad.json',
            dependency_evidence(fork_revision='main'),
        )
    with pytest.raises(ValueError):
        EvidenceSanitizer.write_dependency_manifest(
            tmp_path / 'bad-path.json',
            dependency_evidence(local_unversioned_log_sha256=r'C:\Users\secret\log.txt'),
        )

def test_dependency_manifest_has_no_pr_and_exact_provenance(tmp_path: Path) -> None:
    output = tmp_path / 'dependency.json'
    EvidenceSanitizer.write_dependency_manifest(output, dependency_evidence())
    payload = json.loads(output.read_text(encoding='utf-8'))
    assert payload['upstream_pr_url'] is None
    assert payload['upstream_base_revision'] == '9134de25afadaee955d0f821862338e3d046a338'
```

Expected: RED because the sanitizer does not exist.

- [ ] **Step 2: Implement a closed dependency schema**

```python
@dataclass(frozen=True)
class DependencyEvidence:
    upstream_url: Literal['https://github.com/jmcnamara/rust_xlsxwriter.git']
    upstream_tag: Literal['v0.96.0']
    upstream_base_revision: str
    crates_io_checksum: str
    pre_pin_costing_commit: str
    fork_url: Literal['https://github.com/Aspirin86942/rust_xlsxwriter.git']
    fork_revision: str
    allowed_diff_files: tuple[str, ...]
    xmlwriter_fallback_used: bool
    xmlwriter_fallback_trigger_test: str | None
    diff_sha256: str
    local_unversioned_log_sha256: str
    upstream_pr_url: None
    verdict: Literal['VALIDATED']
```

`EvidenceSanitizer` validates 40/64-character lowercase hashes, exact URLs/tag/checksum, the five mandatory fork source/test paths, and optional `src/xmlwriter.rs` only when `xmlwriter_fallback_used=true` with one of the three named row fault tests. It requires the read-only PR query to be empty, serializes `upstream_pr_url=None`, and rejects drive/UNC/user/canary/stdout/stderr patterns in every string. It builds a fresh dictionary explicitly; it does not call `dataclasses.asdict`.

Add `test_rust_xlsxwriter_revision_consistency_rejects_each_of_five_mismatches` with temp fixtures for fork HEAD, workspace manifest, Cargo metadata source, Cargo.lock source and dependency manifest. Add checksum tests that reject a changed lock checksum or cached archive hash.

- [ ] **Step 3: Capture raw provenance locally and generate the manifest**

```powershell
$ForkCheckout = 'D:\python_program\rust_xlsxwriter-costing-fork'
$DependencyCommit = (git log -1 --format=%H --fixed-strings --grep='build(xlsx): pin recoverable writer fork').Trim()
if ($DependencyCommit -notmatch '^[0-9a-f]{40}$') { throw 'dependency wiring commit is missing' }
$PrePinCommit = (git rev-parse "$DependencyCommit^").Trim()
uv run python -m tests.rust_oracle.evidence dependency `
    --fork-checkout $ForkCheckout `
    --cargo-manifest rust/Cargo.toml `
    --cargo-lock rust/Cargo.lock `
    --pre-pin-commit $PrePinCommit `
    --local-log-root rust/target/perf/local-logs `
    --output docs/performance/dependencies/2026-07-11-rust-xlsxwriter-0.96.0.json
```

The Python CLI itself invokes the fixed read-only Git/gh/Cargo commands with `subprocess.run(..., text=True, encoding='utf-8', check=True)`. It reads the registry checksum from `git show <pre-pin-commit>:rust/Cargo.lock`, rehashes every cached `.crate` archive, and repeats tag/archive/no-PR checks after the current lock has moved to Git. It writes raw output under the ignored local-log root using explicit UTF-8, hashes that log, and writes only the closed manifest. No PowerShell text pipeline writes repository files.

- [ ] **Step 4: Run Phase -1D final gates**

```powershell
uv run python -m pytest tests/rust_oracle/test_evidence.py -q --basetemp .pytest-tmp/evidence
uv run python -m tests.rust_oracle.evidence verify-dependency --fork-checkout D:\python_program\rust_xlsxwriter-costing-fork --cargo-manifest rust/Cargo.toml --cargo-lock rust/Cargo.lock --pre-pin-commit $PrePinCommit --dependency-manifest docs/performance/dependencies/2026-07-11-rust-xlsxwriter-0.96.0.json
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo test --locked --manifest-path rust/Cargo.toml --target x86_64-pc-windows-msvc --target-dir rust/target/test-msvc --no-default-features
cargo test --locked --manifest-path rust/Cargo.toml -p costing-xlsx --target x86_64-pc-windows-msvc --target-dir rust/target/test-low-memory --no-default-features --features low-memory
uv run python -m pytest tests/contracts tests/architecture -q --basetemp .pytest-tmp/contracts
cargo test --release --manifest-path D:\python_program\rust_xlsxwriter-costing-fork\Cargo.toml --target x86_64-pc-windows-msvc --target-dir rust/target/fork-test-release-msvc --no-default-features --features constant_memory
git diff --check
```

Expected: all pass; fork tests have already proved create/write/flush/rewind/read failures return Result; manifest revision equals Cargo manifest and lock.

- [ ] **Step 5: Commit the dependency evidence boundary**

```powershell
git add -- tests/rust_oracle/evidence.py tests/rust_oracle/test_evidence.py docs/performance/dependencies/2026-07-11-rust-xlsxwriter-0.96.0.json
git diff --cached --name-only
git diff --cached --check
git commit -m "test(xlsx): attest recoverable writer dependency"
```

## Phase -1D Exit Checklist

- [ ] Standard Sheet never initializes a tempfile with `constant_memory` enabled.
- [ ] All four memory factories defer creation and each returns recoverable create failure on first real I/O without panic.
- [ ] Controlled LowMemory succeeds while system TEMP/TMP/TMPDIR points to a nonexistent canary.
- [ ] Removed/custom tempdir TOCTOU and row-start/cell-body/row-end/flush/rewind/copy failures return `XlsxError::IoError` with the original kind/OS code.
- [ ] Project JSON preserves request id, stage, output state, cleanup list, `io_kind`, and `raw_os_error`.
- [ ] Existing output cannot be deleted by a losing concurrent writer.
- [ ] fork HEAD, workspace manifest, Cargo metadata, lockfile and dependency manifest pass the single five-way revision gate.
- [ ] registry lock checksum and cached `.crate` archive both match the approved crates.io checksum.
- [ ] `upstream_pr_url` is null and no PR exists.
- [ ] Only after every item passes may execution open the Phase 0H plan.

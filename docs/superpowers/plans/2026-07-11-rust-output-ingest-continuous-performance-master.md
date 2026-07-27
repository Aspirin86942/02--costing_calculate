# Rust 输出与读取持续性能优化总控实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在三张 Sheet、runtime、quality、error-log 和 workbook 语义完全一致的前提下，把 SK 正常模式 wall-clock 中位数降到 `<=20.0s`，并把 Peak Working Set 中位数限制在 `<=2.0 GiB`。

**Architecture:** 实施严格分为四个可独立验收的阻塞阶段：先用精确 revision 受控 fork 消除 LowMemory 的 `%TEMP%` 和 panic P0，再建立 normal wall/PWS、workbook oracle 和脱敏证据工具链，然后优化 writer/feature/low-memory，最后做 Reader 去副本与 Windows 单 EXE 发布门禁。后一阶段只能消费前一阶段已提交且已验证的产物。

**Tech Stack:** Rust 2021、`rust_xlsxwriter 0.96.0` 精确 Git fork、Calamine 0.26、`rust_decimal`、Python 3.11、pytest、PowerShell、Windows/MSVC、OOXML/ZIP、`dumpbin`、可选 `llvm-readobj`。

## Global Constraints

- 已批准规格固定为 `docs/superpowers/specs/2026-07-11-rust-output-ingest-continuous-performance-design.md`；子计划不得重新解释门槛。
- 决策优先级固定为 Correctness > Maintainability > Observability > Performance。
- SK normal-mode external wall-clock 正式 `N` 轮中位数必须 `<=20.0s`。
- SK normal-mode Peak Working Set 正式 `N` 轮中位数必须 `<=2.0 GiB`。
- GB wall/PWS 相对同批固定 Phase 0A reference 回退不得超过 5%。
- GB/SK output bytes 相对已批准 Phase 0A manifest 增长不得超过 10%。
- `N=5`；任一 time/PWS 门槛进入 `abs(measured / limit - 1) <= 0.03` 时，必须追加 global round 6–10，时间与 PWS 两套证据都扩为 `N=10`。
- 奇数 global round 固定 reference → candidate，偶数 global round 固定 candidate → reference。
- 所有内部性能比的分子与分母必须来自同 pipeline、同 input SHA、同 `N`、同批 AB/BA 和固定 binary SHA。
- 金额、数量和单位成本继续使用 `Decimal`；不得以 `float/f64` 作为业务比较容差。
- 不改变三张 Sheet、列顺序、样式、白名单顺序、GB/SK 独立成本项、Modified Z-score 或错误日志语义。
- 不使用系统 `%TEMP%`；LowMemory 只能使用最终输出目录下的 `.costing-tmp-<request_id>-<random>`。
- 不创建、不提交、不等待上游 PR；受控 fork 不自动同步上游。
- 上游基线固定为 `9134de25afadaee955d0f821862338e3d046a338`，crates.io checksum 固定为 `dd1746025420e17b5d62528b930e550e016e857038794d74e169018126ef3d14`。
- Cargo manifest、`Cargo.lock` 和依赖证据中的 fork revision 必须是同一个完整 40 位小写 SHA。
- 所有 Rust 性能构建固定 `--target x86_64-pc-windows-msvc --no-default-features --target-dir rust/target/...`。
- 所有待版本化 evidence 必须经 `EvidenceSanitizer`；原始命令、绝对路径、ERP basename、stdout/stderr 和 mismatch 真实值只能进入本地 ignored 日志。
- 每个代码任务使用 red-green-refactor，验证通过后独立 Conventional Commit。
- 每次只暂存任务列出的精确 pathspec；禁止 `git add -A` 或顺带提交用户修改。

---

## Starting Workspace State

计划编写时主工作区基线为 `bf4b59c`，并且存在用户所有的未暂存修改：

```text
rust/crates/costing-core/src/model.rs
```

该差异只为 `SplitResult::{schema,detail_rows,qty_rows}` 增加 `#[cfg(test)]`。实施不得修改、删除、暂存或提交这三行。执行必须从“包含本计划文档的 main HEAD”创建独立 worktree，使该未提交差异留在主工作区。

## Plan Set and Dependency Order

| 顺序 | 子计划 | 可独立验收产物 | 下一阶段阻塞条件 |
|---:|---|---|---|
| 1 | `2026-07-11-rust-output-phase-1d-dependency-safety.md` | 精确 fork、可恢复临时 I/O、项目错误上下文和依赖证据 | 任一 temp I/O panic、source 丢失或 revision 不一致 |
| 2 | `2026-07-11-rust-output-phase-0h-benchmark-foundation.md` | normal wall/PWS `N=5/10`、OOXML oracle、EvidenceSanitizer | 扩样、轮次、oracle、cleanup 或脱敏测试失败 |
| 3 | `2026-07-11-rust-output-phase-0a-3-writer-optimization.md` | 已批 Phase 0A manifest、观测、ColumnWritePlan、feature 决策和自适应 LowMemory | Phase 0A 未经用户确认，或任一阶段门禁未通过 |
| 4 | `2026-07-11-rust-output-phase-4-5-reader-release.md` | Reader 去副本取舍、最终 wall/PWS/workbook 门禁和 Windows 单 EXE 证据 | 任一最终硬门槛或发布 smoke 失败 |

四份子计划必须按表格顺序执行，不得并行修改共享文件。可并行的只有只读审查、性能证据复核和 PE 输出复核。

## File Ownership Map

- 子计划 1 拥有：外部 fork 的 `src/worksheet.rs`、`src/workbook.rs`、`src/packager.rs`、条件式最窄 `src/xmlwriter.rs` fallback 及对应测试；当前仓库的 Cargo manifests/lock、`costing-core/src/error.rs`、`costing-core/src/model.rs`、`costing-xlsx/src/{reader,writer}.rs`、`costing-cli/src/{run,main}.rs`；并创建 dependency-only `tests/rust_oracle/evidence.py`、`test_evidence.py` 和 dependency manifest。
- 子计划 2 按顺序扩展同一 evidence 模块，并拥有 `tests/rust_oracle/` 下的 benchmark protocol、normal runner、PWS 脚本、workbook OOXML 解析和对应 pytest；子计划 1/2 不得并行改 evidence 文件。
- 子计划 3 拥有：`costing-xlsx` 的 planner/temp workspace/writer 热路径、CLI 输出观测、Phase 0A/2/3 性能决策证据。
- 子计划 4 拥有：`costing-xlsx/src/reader.rs`、Reader 回归测试、Windows 发布验证脚本、最终 evidence 和必要 README/AGENTS 命令同步。

## Task 0: Create the Isolated Execution Worktree

**Files:**
- Preserve unchanged in main workspace: `rust/crates/costing-core/src/model.rs`
- Create at execution time: `D:\python_program\02--costing_calculate\.worktrees\rust-output-ingest-performance`

**Interfaces:**
- Consumes: 包含本计划集的 main HEAD。
- Produces: 分支 `perf/rust-output-ingest-continuous` 的干净 worktree；后续所有实施提交只发生在该 worktree。

Precondition: approved spec status line and all five plan documents have already been committed together on main. If any remains modified/untracked, stop before Step 1；do not treat in-progress planning files as an allowed implementation-worktree state.

- [ ] **Step 1: Verify the main-workspace ownership boundary**

Run from `D:\python_program\02--costing_calculate`:

```powershell
$status = @(git status --short)
$allowed = @(' M rust/crates/costing-core/src/model.rs')
if (Compare-Object $status $allowed) {
    $status
    throw 'main workspace contains changes other than the user-owned model.rs diff'
}
git diff -- rust/crates/costing-core/src/model.rs
```

Expected: 只显示三个 `#[cfg(test)]` 增加；不执行 stash、checkout、reset 或 commit。

- [ ] **Step 2: Use the required worktree skill**

Invoke `superpowers:using-git-worktrees` and request exactly:

```text
repository = D:\python_program\02--costing_calculate
worktree = D:\python_program\02--costing_calculate\.worktrees\rust-output-ingest-performance
branch = perf/rust-output-ingest-continuous
base = current main HEAD containing the approved plan set
```

Expected: `.worktrees/` 已被 `.gitignore` 忽略，新 worktree 状态为 clean，主工作区的 `model.rs` 差异仍存在。

- [ ] **Step 3: Verify both workspaces after creation**

```powershell
$main = 'D:\python_program\02--costing_calculate'
$worktree = 'D:\python_program\02--costing_calculate\.worktrees\rust-output-ingest-performance'
if (git -C $worktree status --porcelain) { throw 'execution worktree must start clean' }
if ((git -C $main status --short) -ne ' M rust/crates/costing-core/src/model.rs') {
    throw 'user-owned main-workspace change was altered'
}
git -C $worktree log -1 --oneline
```

Expected: worktree HEAD 是本计划集提交，main 仍只有用户 `model.rs` 差异。

## Cross-Plan Commit and Review Rules

- fork 仓库和 costing 仓库使用独立 commit；两者不得使用同一 `git add` 命令。
- fork 提交只 push 到 `Aspirin86942/rust_xlsxwriter`，禁止 `gh pr create`、`git request-pull` 或任何 PR API。
- 每个代码 commit 后先做只读 code review；ETL/workbook/evidence 改动再分别做 data/security review。
- README/AGENTS/证据说明的修改与 `doc_reviewer` 只读审查分开；reviewer 不直接改文档。
- 每次 commit 前必须运行 `git diff --cached --name-only` 并与当前任务文件清单逐项相等。
- 真实 ERP workbook、raw benchmark logs、EXE 和 `.costing-tmp-*` 只能留在 ignored `rust/target/` 或仓库外。

## Global Stop Rules

1. Phase -1D 任一故障注入仍 panic，或无法读取原始 `ErrorKind/raw_os_error`：立即停止，不进入 Phase 0H。
2. Phase 0H 的轮次、扩样、oracle、finally cleanup 或 staged-evidence scan 任一测试失败：不采 Phase 0A。
3. Phase 0A manifest 未经用户明确确认和提交：不开始 Phase 0B/1/2/3。
4. 任一性能候选 workbook/runtime/quality/error-log 不一致：先回退或修正 correctness，不讨论性能收益。
5. Phase 4 是强制 A/B；若不达保留条件，用显式 `git revert` 保留审计链，再以 Phase 3 候选进入 Phase 5。
6. Phase 5 全部门槛首次同时满足即停止；不继续猜测性优化。
7. 若 Windows Sandbox/等价干净 Windows 10/11 x64 不可用，发布验收状态是 `BLOCKED_CLEAN_WINDOWS_REQUIRED`，不得用开发机 smoke 代替。

## Plan Self-Review Checklist

- [ ] 四份子计划已全部完成，且与本总控顺序一致。
- [ ] 两个 P0 在 Phase -1D 内有故障注入和项目端到端错误测试。
- [ ] 强制 global round 6–10、same-batch 分母和 `INCONCLUSIVE` 均有纯单元测试。
- [ ] OOXML value/style/sharedStrings 比较不再使用全局 epsilon 或每列样式集合。
- [ ] 任一版本化 evidence 都无法绕过 sanitizer 和 staged scan。
- [ ] Phase 0B、1、2、3、4 每个性能决策都使用同批新采分母。
- [ ] 最终 Windows 证据同时包含 PE imports、TEMP canary、三张 Sheet、无临时目录残留和单 EXE 成功退出。
- [ ] 用户主工作区 `model.rs` 修改从未进入任何实施 commit。

## Execution Handoff

完整计划集以本文档为入口。推荐使用 `superpowers:subagent-driven-development`：每个任务使用新实现 agent，然后做规格符合性和代码质量两阶段审查。若改用 inline 执行，必须使用 `superpowers:executing-plans`，并在每份子计划结束时停下检查阻塞条件。

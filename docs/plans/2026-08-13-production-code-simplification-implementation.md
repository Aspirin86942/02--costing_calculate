# 生产代码深度简化 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 对 Rust 全部生产代码做激进深度简化(11 批),行为与性能双重不变,契约 baseline 零 diff。

**Architecture:** 模块分批(方案 1)。每批:理解先行(Chesterton's Fence)→ 产出简化清单 → 逐项改 + 每项快速测试 → 全部门禁 → 独立 commit → 直推 main。第 1 批为检查点,用户认可模式后其余批次不停顿。热路径批(4/9/10)加配对基准护栏;全部完成后 N=5 硬门禁 + 收尾记录。

**Tech Stack:** Rust workspace(costing-cli/core/xlsx/oracle-tests)、cargo fmt/clippy/test、Python pytest 契约、`tools/validation/measure_paired_release.ps1`(护栏)、`tools/validation/measure_release.ps1`(N=5 门禁)。

**说明:** 简化改动本身在执行时按清单逐项发现,本计划给出的是精确流程、门禁命令、清单模板与提交格式——所有可预写的机械步骤均为具体内容,无占位符。

## Global Constraints

- **永不动**:`application::execute(RunRequest) -> RunOutcome`、`process_workbook` 签名与语义、CLI 参数/JSON/错误码/`retryable`、Sheet 名称/顺序/字段/样式、`RunManifestV1`、config TOML/schema 字段、error_log 类别、Decimal 语义、异常阈值(2.5/3.5)、白名单顺序、crate 依赖方向、5,000,000 cell slots 阈值。
- **禁止**:修改任何测试文件;新增生产依赖;修改 `tests/contracts/baselines/`(必须零 diff)。
- **每批 diff 只含该批生产文件**——`git diff --stat` 中不得出现测试文件、其他模块或无关文件。
- **优化禁止**:疑似瓶颈只记录进候选清单,不与简化混 commit。
- **不确定就跳过**:任何「不确定行为是否完全一致」的简化,跳过并记录,绝不猜。
- **提交格式**:Conventional Commits,`refactor(<crate>): simplify <module>`,正文列简化类别与门禁结果,结尾:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
- **推送**:每批本地门禁全绿后直推 main(用户批准,admin 豁免)。
- **性能阈值**(`docs/performance/README.md`):GB wall ≤ 3.2554s / PWS ≤ 375,700,685 B / output ≤ 4,194,321 B;SK wall ≤ 20.0s / PWS ≤ 2,147,483,648 B / output ≤ 48,658,823 B。
- **样本**:`data/raw/sk/sk-成本计算单_2026041311461807_3592191.xlsx`、`data/raw/gb/gb-成本计算单_2026062418012916_576938.xlsx`(已确认存在);若执行时缺失,护栏降级并说明原因与风险。

## 文件结构(批次)

| 任务 | 模块 | 护栏 |
|---|---|---|
| Task 1 | `rust/crates/costing-core/src/presentation.rs` | —(检查点)|
| Task 2 | `rust/crates/costing-core/src/normalize.rs` | — |
| Task 3 | `rust/crates/costing-core/src/table.rs` | — |
| Task 4 | `rust/crates/costing-core/src/fact.rs` | SK check-only 8 对 |
| Task 5 | `rust/crates/costing-core/src/anomaly.rs` | — |
| Task 6 | `rust/crates/costing-core/src/{quality,scoring,split,pipeline,process,model,sheet_contract,timing,lib,error}.rs` | — |
| Task 7 | `rust/crates/costing-cli/src/application/manifest.rs` | — |
| Task 8 | `rust/crates/costing-cli/src/{run,run_paths,args,main,lib,build_info}.rs` + `application/{runner,request,outcome,mod}.rs` + `config/*.rs` | — |
| Task 9 | `rust/crates/costing-xlsx/src/writer.rs` | SK normal 8 对 |
| Task 10 | `rust/crates/costing-xlsx/src/{reader,snapshot,atomic_file,lib}.rs` | SK normal 8 对 |
| Task 11 | `rust/crates/costing-oracle-tests/src/lib.rs` | —(大概率不动)|

## 每批通用步骤模板(Task 1-11 共用)

- [ ] **Step 1 理解先行**:通读本批全部文件;`rg -n "<模块名>" rust/crates/<crate>/src/*_tests.rs` 定位覆盖测试并读之;读 `docs/contracts/workbook.md` 相关章节;`git log --follow --oneline -3 -- <文件>` + 对可疑代码 `git blame`(Chesterton's Fence)。
- [ ] **Step 2 产出简化清单**:写入 `rust/target/perf-local/simplify-inventory-batchNN.md`(本地文件,不提交),模板:

```markdown
# 简化清单 — 批次 NN:<模块>
| # | 位置(文件:行) | 类型 | 改法 | 行为影响 | 备注 |
|---|---|---|---|---|---|
| 1 | presentation.rs:123 | 嵌套 | 提取卫语句 | 无 | |
类型取值:嵌套/长函数/命名/死代码/重复/多余抽象/重排/文件拆分
行为影响:无 | 不确定→跳过并记入候选备注
```

- [ ] **Step 3 逐项修改**:一次只改清单中的一项;每项后运行快速反馈:
  `cargo test --manifest-path rust/Cargo.toml -p costing-core --lib`(cli/xlsx 批用对应 crate 名)
  失败→立即回退该项并重想。
- [ ] **Step 4 全部门禁**(全部通过才可提交):

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo clippy --locked --manifest-path rust/Cargo.toml --workspace --all-targets --all-features -- -D warnings
cargo test --locked --manifest-path rust/Cargo.toml --workspace --all-features
uv run python -m pytest tests -q -m "not slow and not benchmark" --basetemp .pytest-tmp/python-validation
```

- [ ] **Step 5 基线零 diff 检查**:`git status --porcelain tests/contracts/baselines/` 输出必须为空;`git diff --stat` 只含本批文件。
- [ ] **Step 6 提交推送**:

```powershell
git add rust/crates/<crate>/src/<files>
git commit -m "refactor(<crate>): simplify <module>" -m "简化类别:... 门禁:fmt/clippy/test/pytest 全绿;baseline 零 diff" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push
```

- [ ] **Step 7 批次摘要**:报告清单条目数、门禁结果、diff 统计、跳过项。

## Task 0:预检、基线二进制与护栏准备

- [ ] **Step 1 干净树全门禁**:在尚未改任何代码的 HEAD 上跑 Step 4 的四条命令,确认全绿(若某条本来就不绿,先停下报告,不得带病开批)。
- [ ] **Step 2 构建并保存基线二进制**:

```powershell
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate
Copy-Item rust/target/release/costing-calculate.exe rust/target/perf-local/simplify-baseline.exe
```

- [ ] **Step 3 记录基线身份**:写入 `rust/target/perf-local/simplify-baseline.txt`(本地,不提交):当前 `git rev-parse HEAD`、基线 exe 的 SHA-256。
- [ ] **Step 4 确认样本**:检查两个样本文件存在(路径见 Global Constraints);缺失则后续护栏批降级并在摘要中说明。

## Task 1:批次 1 — presentation.rs(检查点批)

**Files:**
- Modify: `rust/crates/costing-core/src/presentation.rs`
- 覆盖测试(定位后只读):`rg -n "presentation" rust/crates/costing-core/src/*_tests.rs` 命中的文件
- 契约参考(只读):`docs/contracts/workbook.md`

- [ ] **Step 1-7**:执行通用模板,commit 信息:`refactor(core): simplify presentation`
- [ ] **Step 8 检查点暂停**:向用户提交:完整简化清单、前后统计(文件行数、函数数)、3-5 个关键前后对比片段、门禁结果、跳过项与原因。**用户认可模式后才继续 Task 2**;若用户要求调整深度或风格,先修订本计划对应处再继续。

## Task 2-3、5-8:常规批次

各任务执行通用模板,提交信息分别为:
- Task 2 `refactor(core): simplify normalize`
- Task 3 `refactor(core): simplify table`
- Task 5 `refactor(core): simplify anomaly`
- Task 6 `refactor(core): simplify core helper modules`(多文件一批,正文列文件清单)
- Task 7 `refactor(cli): simplify manifest`
- Task 8 `refactor(cli): simplify run and config modules`

## Task 4:批次 4 — fact.rs(护栏批)

**Files:** Modify: `rust/crates/costing-core/src/fact.rs`

- [ ] **Step 1-7**:执行通用模板,commit `refactor(core): simplify fact`
- [ ] **Step 8 配对护栏(SK check-only,8 对)**:先 `cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate`,再:

```powershell
.\tools\validation\measure_paired_release.ps1 `
  -BaselineBinary .\rust\target\perf-local\simplify-baseline.exe `
  -CandidateBinary .\rust\target\release\costing-calculate.exe `
  -Pipeline sk `
  -InputPath .\data\raw\sk\sk-成本计算单_2026041311461807_3592191.xlsx `
  -Mode check-only `
  -Pairs 8 `
  -OutputDirectory .\.pytest-tmp\perf-simplify-b4 `
  -ReportPath .\.pytest-tmp\perf-simplify-b4.json
```

- [ ] **Step 9 判读**:报告 `status=valid`;`summary.external_wall_seconds.paired_median_relative_delta` 与 `peak_working_set_bytes` 中位数不得显著回退(≥ +5% 视为疑似回退,停止并把该批 diff 作为怀疑对象排查);把报告摘要附进批次摘要。脚本 exit 0 才继续;`invalid_reason` 非空时报给用户并停止。

## Task 9-10:writer/reader 护栏批

同 Task 4 结构,差异:`-Mode normal`、OutputDirectory/ReportPath 用 `perf-simplify-b9` / `perf-simplify-b10`,commit 分别为 `refactor(xlsx): simplify writer`、`refactor(xlsx): simplify reader and atomic publish`。判读同 Task 4 Step 9(normal 模式额外看 `output_size_bytes` 与 `candidate_max_output_bytes` ≤ SK 上限 48,658,823 B)。

## Task 11:oracle-tests

- [ ] 通读 `rust/crates/costing-oracle-tests/src/lib.rs`(89 行),按同一标准评估。
- [ ] 无可简化项→本任务以「无需改动」结论完成,不提交;有→按模板走,commit `refactor(oracle): simplify contract checks`。

## Task 12:最终验收

- [ ] **全部门禁复跑**:通用模板 Step 4 四条命令,全绿。
- [ ] **输出目录前置检查**:`.pytest-tmp/perf-final-gb`、`.pytest-tmp/perf-final-sk` 必须事先不存在;存在则换名或删除(仅限 `.pytest-tmp/` 下)。
- [ ] **baseline 终检**:`git status --porcelain tests/contracts/baselines/` 为空。
- [ ] **GB N=5 硬门禁**:

```powershell
.\tools\validation\measure_release.ps1 -BinaryPath .\rust\target\release\costing-calculate.exe -Pipeline gb -InputPath .\data\raw\gb\gb-成本计算单_2026062418012916_576938.xlsx -OutputDirectory .\.pytest-tmp\perf-final-gb -Iterations 5 -ReportPath .\.pytest-tmp\perf-final-gb.json
```

- [ ] **SK N=5 硬门禁**:同参数,`-Pipeline sk`、`-InputPath .\data\raw\sk\sk-成本计算单_2026041311461807_3592191.xlsx`、`perf-final-sk` 路径。
- [ ] **阈值对照**:GB wall ≤ 3.2554s / PWS ≤ 375,700,685 B / output ≤ 4,194,321 B;SK wall ≤ 20.0s / PWS ≤ 2,147,483,648 B / output ≤ 48,658,823 B。任何一项不过→停止,报告,不得继续。

## Task 13:收尾

- [ ] **优化候选清单**:汇总各批次清单中的跳过/发现项,整理为候选清单(位置、证据、预期收益)交付用户,由其决定是否另开优化阶段。
- [ ] **变更记录**:新建 `docs/changes/2026-08-13-production-code-simplification.md`,记录:各批实际改动摘要与 commit、门禁结果、护栏与 N=5 报告摘要(脱敏)、跳过项、候选清单、剩余风险。
- [ ] **计划状态**:把 `docs/plans/2026-08-13-production-code-simplification.md` 状态改为 Completed,并在 `docs/plans/README.md` 当前文件列表加一行。
- [ ] **提交推送**:

```powershell
git add docs/changes/2026-08-13-production-code-simplification.md docs/plans/2026-08-13-production-code-simplification.md docs/plans/README.md
git commit -m "docs(changes): record production code simplification results" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push
```

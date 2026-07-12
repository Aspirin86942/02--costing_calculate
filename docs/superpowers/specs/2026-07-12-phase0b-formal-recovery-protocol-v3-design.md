# Phase 0B Formal Recovery Protocol v3 设计

**状态：** 设计已于 2026-07-12 获用户批准；允许编写并提交本规格和实施计划，正式 GB/SK 仍须在实现、测试和审查全部通过后按门禁执行
**日期：** 2026-07-12
**适用仓库：** `D:\python_program\02--costing_calculate`
**上位规格：** `docs/superpowers/specs/2026-07-11-rust-output-ingest-continuous-performance-design.md`
**被恢复阶段：** Phase 0B instrumented vs Phase 0A reference

在 protocol v3 实现、测试、审查和代码提交完成前，`docs/performance/README.md` 描述的 protocol 2 仍是唯一当前运行契约；本规格不得被直接当作可执行 CLI 授权。

## 1. 目标

在不修改、不删除、不重放 protocol v2 封存 attempt 的前提下，为一个已经精确证明的 benchmark harness 证据构建缺陷建立全新的 protocol v3 正式比较身份，重新采集 GB/SK 的同批 wall/PWS 证据，并在两条 pipeline 都 `VALIDATED` 后恢复 Phase 1→5。

本次恢复的 Phase 0B v3 closed profile 固定为：

```text
GB instrumented wall / same-batch Phase 0A reference wall <= 1.02
SK instrumented wall / same-batch Phase 0A reference wall <= 1.02
Phase 0B PWS 没有 direct closed gate，但仍须 fresh 采集、参与全局临界扩样、
same-batch/environment validation 和 direction diagnostic
GB/SK output bytes relative to approved Phase 0A manifest <= 1.10
GB/SK correctness、runtime、dimensions、oracle 和全部正式轮次通过
```

上位规格的最终门槛保持不变：

```text
SK normal external wall median <= 20.0s
SK normal PWS median <= 2.0 GiB
GB final wall/PWS relative to same-batch Phase 0A reference <= 1.05
```

最终门槛不能替代或放宽本次 Phase 0B 的 `wall_ratio <= 1.02` instrumentation gate；Phase 0B 也不能提前用后续阶段的 SK `20s/2GiB` 作为通过条件。

本设计只解决 Phase 0B 正式证据链的恢复，不实现 Phase 1 writer、Phase 2 feature、Phase 3 LowMemory、Phase 4 Reader 或 Phase 5 release gate。

## 2. 已封存事实

### 2.1 固定二进制和基线

```text
Phase 0A reference EXE SHA:
f75f7ee17cc222765537f6bbe02f90e76cd041c55c8990b0261788e6fa63db56

Phase 0B instrumented EXE SHA:
d06470e4e7c9e6dc8f54efc9d26d996d3cbbbddec04cb7dffef6e6869802b629

Approved GB input SHA:
6aa5e3e7fdc547ebaaef968eb5b95d4d630c4ec9915184f94346f60687b8e7ee

Approved Phase 0A manifest SHA:
17faa1f08a0601fac52186ab64a4ebb4519312a259c59b7d64e69c748bab56df

Sealed v1 terminal SHA:
d42940dfc48f208834efa103de6a08663e75d1ee09dd6804d4e2416ad90af71f
```

恢复实现不得重建或替换这两个 EXE。正式 preflight 必须重新计算并精确匹配上述 SHA。

### 2.2 封存的 GB v2 attempt

```text
protocol_version: 2
pipeline: gb
comparison_profile: phase0b-vs-phase0a
comparison_key:
09d6bb93ab04dda277e97f19dc8a270be91f2f8898a42f25d1d5bd745bdf0fd7
attempt: attempt-0001
terminal_verdict: INCOMPLETE_EVIDENCE
terminal_sha256:
f515c305518093e9aa0ac90fa0b82520874fcd7006db16946b45921fd9b2a57b
```

封存 ledger 含：

```text
planned-output: 20
sample: 20
first-group: 1
cleanup-complete: 1
evidence-prepared: 0
evidence-committed: 0
versioned evidence: 0
```

20 个正式 sample 的 runtime `sheet_dimensions` 全为空；其余结构可由现有 ledger reader 读取。失败发生在实际 workbook 已清理后构建 paired evidence 时，不能从已删除 workbook 补写 v2 sample。

### 2.3 已完成修复

```text
c5dddfd fix(perf): capture formal sheet dimensions
f481ac8 fix(perf): audit dimension validation failures
```

修复后 wall/PWS 会在删除 workbook 前从实际文件验证并补齐 dimensions；wall dimensions enrichment 失败也会登记、清理 raw log，并把 SHA 写入 terminal。修复不赋予修改 v2 ledger 或重跑同一 v2 comparison 的权限。

## 3. 决策优先级和不变量

继续使用上位规格的优先级：

1. Correctness；
2. Maintainability；
3. Observability；
4. Performance。

恢复协议必须满足：

- v1/v2 只读，v3 单写；
- 不复用任何 v2 wall/PWS 数值；
- 不把 v2/v3 sample 拼接为同一个 `N`；
- 不允许操作者传入任意 protocol version、parent comparison key、terminal path、terminal SHA 或 recovery reason；
- GB v3 必须从闭合授权表自动派生唯一 parent；
- SK v3 没有 recovery parent，只能在 GB v3 evidence 已验证并提交到当前 `HEAD` 后执行；
- 任一正式 v3 terminal 都不得通过新 attempt 重新采样；
- v3 在启动每个正式子进程前写 durable `sample-started`；只有尚未写 `sample-started` 的计划步骤可以续跑；
- 已有 `sample-started` 但没有对应 `sample` 时必须清理并封存，禁止重启该 sample；
- 已完成的 `sample` 只能在同一未封存 attempt 内只读复用，不能删除或替换；
- `CLEANUP_FAILED` 只允许纯清理后继，不允许启动 reference/candidate；
- SK comparison、batch、ledger 和 evidence 必须显式绑定已提交 GB v3 artifact、marker 和 evidence commit；
- 所有版本化 artifact 继续经过 `EvidenceSanitizer` 和 staged scan。

## 4. 范围与非目标

### 4.1 范围

- version 3 comparison/batch/ledger/evidence identity；
- legacy multi-read + v3 single-write 迁移；
- 当前 GB v2 失败形态的闭合 recovery authorization；
- v3 fresh GB/SK formal flow；
- v3 evidence schema 和 artifact basename；
- GB evidence commit-before-SK gate；
- terminal、cleanup-only 和 no-resample 规则；
- 单元测试、sanitizer 回归、README/上位规格状态同步；
- 正式执行前后的 SHA、clean worktree 和 evidence 门禁。

### 4.2 非目标

- 不修改 v1/v2 ledger、terminal、records、journal 或 checkpoint；
- 不从 v2 sample 生成 v3 sample；
- 不只补跑 workbook dimensions attestation；
- 不开放通用 retry/recovery CLI；
- 不改变 Phase 0A manifest；
- 不重建 reference/candidate EXE；
- 不提前进入 Phase 1；
- 不改变业务 Rust 生产代码；
- 不创建 PR；
- 不放宽同批、扩样、correctness、脱敏或 cleanup 规则。

## 5. 协议版本和兼容边界

固定常量概念为：

```python
CURRENT_PAIRED_PROTOCOL_VERSION = 3
CURRENT_BENCHMARK_SCHEMA_VERSION = 3
READABLE_LEDGER_PROTOCOL_VERSIONS = (1, 2, 3)
READABLE_BENCHMARK_SCHEMA_VERSIONS = (1, 2, 3)
```

兼容行为：

| 输入 | reader/rebuilder | create/append/publish |
|---|---|---|
| v1 ledger/evidence | 允许只读审计 | 拒绝 |
| v2 ledger/evidence | 允许只读审计 | 拒绝 |
| v3 ledger/evidence | 允许 | 仅当前规则允许 |

当前 `PAIRED_PROTOCOL_VERSION` 不能继续同时承担“当前写版本”和“所有历史可读版本”的判断。实现必须拆分 current-write 判断与 legacy-read 判断；否则把常量从 2 改为 3 会错误拒绝 v2 parent 或把 v2 当 v3 重建。

`paired` CLI 仍不提供 protocol selector。实现完成后它固定创建 v3；v1/v2 只能通过内部 typed reader 审计。

`BenchmarkIdentity` 当前六个字段不直接增加 v3-only provenance：

```text
input_sha256
reference_sha256
candidate_sha256
git_head
repository_state_sha256
machine_fingerprint_sha256
```

legacy v1/v2 reader 使用 exact six-field payload；v3 writer 也使用这六个运行身份字段，并把 comparison key、recovery/upstream provenance 作为独立 metadata exact keys。实现必须提供按 protocol exact-key dispatch 的 identity serializer，不能用带可选 v3 字段的 `asdict()` 去严格比较 v2 metadata。`derive_v2_comparison_key()` 保留为只读审计纯函数，新增独立 `derive_v3_comparison_key()`；不把现有函数放宽为接受任意版本。

legacy parent 读取必须拆成纯读取的 `parse_and_validate_ledger_snapshot()`；任何 current v3 repair/publication recovery 另走显式函数。parent eligibility 不得调用具备 checkpoint/journal 写能力的 loader。

## 6. 闭合恢复授权

### 6.1 类型

```python
class RecoveryReason(StrEnum):
    MISSING_FORMAL_SHEET_DIMENSIONS = "MISSING_FORMAL_SHEET_DIMENSIONS"


@dataclass(frozen=True)
class ApprovedRecoveryParent:
    pipeline: Literal["gb"]
    comparison_profile: Literal[ComparisonProfile.PHASE0B_VS_PHASE0A]
    reference_label: Literal[ClosedBinaryLabel.PHASE0A]
    candidate_label: Literal[ClosedBinaryLabel.PHASE0B]
    input_sha256: str
    reference_sha256: str
    candidate_sha256: str
    parent_protocol_version: Literal[2]
    parent_comparison_key: str
    parent_attempt: Literal[1]
    parent_terminal_sha256: str
    parent_comparison_tree_sha256: str
    parent_journal_head_sha256: str
    parent_inventory_entry_count: Literal[134]
    reason: Literal[RecoveryReason.MISSING_FORMAL_SHEET_DIMENSIONS]
```

授权表只包含以下一个精确对象：

```python
ApprovedRecoveryParent(
    pipeline='gb',
    comparison_profile=ComparisonProfile.PHASE0B_VS_PHASE0A,
    reference_label=ClosedBinaryLabel.PHASE0A,
    candidate_label=ClosedBinaryLabel.PHASE0B,
    input_sha256='6aa5e3e7fdc547ebaaef968eb5b95d4d630c4ec9915184f94346f60687b8e7ee',
    reference_sha256='f75f7ee17cc222765537f6bbe02f90e76cd041c55c8990b0261788e6fa63db56',
    candidate_sha256='d06470e4e7c9e6dc8f54efc9d26d996d3cbbbddec04cb7dffef6e6869802b629',
    parent_protocol_version=2,
    parent_comparison_key='09d6bb93ab04dda277e97f19dc8a270be91f2f8898a42f25d1d5bd745bdf0fd7',
    parent_attempt=1,
    parent_terminal_sha256='f515c305518093e9aa0ac90fa0b82520874fcd7006db16946b45921fd9b2a57b',
    parent_comparison_tree_sha256='8e961515bcac3afad271bb75eac9e439fdb18d1e8ba07b0fef7e210838796ccb',
    parent_journal_head_sha256='ae10e9d441ecebee9ba6cfb93a799f14a9085c75560103fedc9df6ff56b92c85',
    parent_inventory_entry_count=134,
    reason=RecoveryReason.MISSING_FORMAL_SHEET_DIMENSIONS,
)
```

新增任何 recovery case 都必须修改代码、测试、规格并重新审查；不能通过命令行或任意 JSON 动态加入。

### 6.2 自动匹配

GB v3 preflight 使用请求中的 pipeline/profile/labels/input/reference/candidate SHA 精确查找授权。零个或多个匹配都 fail closed。调用者不能选择 parent。

SK 不查询授权表；其 `recovery_parent=None`，并从固定 GB identity 自动派生 `UpstreamGateProvenance`。调用者不能提供 artifact、marker、commit 或 provenance。

### 6.3 Parent 资格验证

GB 采样前必须只读验证：

```text
canonical parent path 位于 ignored local ledger root
comparison directory tree SHA == 授权值
comparison journal head SHA == 授权值
inventory entry count == 134
comparison directory 只含 attempt-0001 和 journal 两个一级目录
不存在额外 attempt、未知文件、symlink 或 Windows reparse point
metadata protocol_version == 2
comparison key == 授权值
attempt == attempt-0001
terminal verdict == INCOMPLETE_EVIDENCE
terminal file SHA == 授权值
ledger seal/checkpoint/journal 全部有效
record count == 42
planned-output == 20
sample == 20
first-group == 1
cleanup-complete == 1
evidence-prepared == 0
evidence-committed == 0
所有 sample sheet_dimensions == ()
所有 sample 的 metric/round/role 形成完整 wall/PWS global round 1–5
所有 20 个 sample 通过 exact v2 sample parser：input/binary/Git/repository/machine identity、
output bytes、role/order、oracle pair 和 runtime 类型全部有效
对应 v2 versioned evidence 不存在
```

以下任一情况拒绝恢复：

- terminal SHA 或 ledger head 漂移；
- comparison tree、journal head、inventory 或 reparse 状态漂移；
- terminal verdict 为 candidate/correctness/reference/environment/cleanup/sensitive failure；
- sample/round/order/role 不完整；
- 已存在 evidence-prepared/evidence-committed；
- 已存在 versioned v2 evidence；
- dimensions 已完整或只有部分为空；
- parent identity 与当前固定 input/reference/candidate 不一致。

恢复资格只证明“可以创建新的 v3 comparison”，不证明 v2 性能结果有效；v2 metric values 不进入任何 v3 verdict 或 evidence。

## 7. Protocol v3 identity

### 7.1 Recovery provenance

```python
@dataclass(frozen=True)
class RecoveryProvenance:
    parent_protocol_version: Literal[2]
    parent_comparison_key: str
    parent_attempt: Literal[1]
    parent_terminal_sha256: str
    parent_comparison_tree_sha256: str
    parent_journal_head_sha256: str
    parent_inventory_entry_count: Literal[134]
    reason: RecoveryReason
```

```python
@dataclass(frozen=True)
class UpstreamGateProvenance:
    pipeline: Literal['gb']
    protocol_version: Literal[3]
    schema_version: Literal[3]
    comparison_key: str
    artifact_basename: str
    artifact_sha256: str
    marker_basename: str
    marker_sha256: str
    validated_commit_sha: str
```

当前 Phase 0B GB 使用完整 recovery provenance、`upstream_gate_provenance=None`；当前 Phase 0B SK 使用 `recovery_provenance=None`、完整 upstream gate provenance。后续普通 v3 comparison 的 recovery provenance 必须为 `None`；其 profile-specific upstream 依赖由对应阶段规格决定，不能由调用者自由提供。

SK upstream 对象由 harness 自动执行以下闭合步骤构建：

1. 根据固定 GB identity 派生唯一 GB v3 comparison key、artifact basename 和 marker basename；
2. typed read artifact，要求 schema/protocol/pipeline/verdict 为 `3/3/gb/VALIDATED`；
3. 重建 marker 并与磁盘内容相等；
4. 要求当前 `HEAD` 是单父提交，且该提交 diff exact paths 只有 GB artifact 和 marker；
5. 读取 `HEAD:<fixed-path>` Git blob，要求 blob SHA-256 与工作区文件内容完全一致；
6. 把当前 40 位小写 `HEAD` 作为 `validated_commit_sha`。

任一条件失败都在 SK 子进程启动前按第 11.1 节的闭合表映射：artifact/marker/commit shape 或 tracked-state 不完整为 `INCOMPLETE_EVIDENCE`，当前磁盘内容、Git blob 或 `HEAD` 相对已派生值漂移为 `ENVIRONMENT_DRIFT`。该对象必须进入 SK comparison key、batch ID、ledger metadata 和 schema v3 evidence，使发布后的 SK artifact 可以独立证明其 GB 前置链。

### 7.2 Comparison key

v3 comparison key 的 canonical JSON exact key set 固定为：

```json
{
  "protocol_version": 3,
  "pipeline": "gb|sk",
  "comparison_profile": "phase0b-vs-phase0a",
  "reference_label": "phase0a",
  "candidate_label": "phase0b",
  "phase0a_manifest_sha256": "...",
  "input_sha256": "...",
  "reference_sha256": "...",
  "candidate_sha256": "...",
  "recovery_provenance": {
    "parent_protocol_version": 2,
    "parent_comparison_key": "...",
    "parent_attempt": 1,
    "parent_terminal_sha256": "...",
    "parent_comparison_tree_sha256": "...",
    "parent_journal_head_sha256": "...",
    "parent_inventory_entry_count": 134,
    "reason": "MISSING_FORMAL_SHEET_DIMENSIONS"
  },
  "upstream_gate_provenance": null
}
```

顶层字段名只能是上述 11 个键，其中 recovery 对象只能包含上述 8 个键；upstream 对象只能包含 `UpstreamGateProvenance` 的 9 个字段。`profile`、label 和 reason 使用 enum `.value`。SHA 必须为 64 位小写十六进制，commit 为 40 位小写十六进制；`parent_attempt` 和 inventory count 必须是非 bool 的固定正整数。GB 的 upstream 和 SK 的 recovery 分别以 JSON `null` 参与 hash，不能省略键。修改 parent tree/journal/inventory 或任一 GB artifact/marker/commit provenance都必须改变 comparison key。序列化固定为：

```python
json.dumps(
    payload,
    ensure_ascii=False,
    sort_keys=True,
    separators=(',', ':'),
).encode('utf-8')
```

禁止 unknown key、不同字段别名、隐式 enum repr 或 `bool` 充当整数。

repository state、machine fingerprint 和 prior evidence claims fingerprint 继续进入六字段 runtime `BenchmarkIdentity` 与每轮不变性检查；Phase 0A manifest SHA 作为 comparison key、batch 和 ledger metadata 的独立 exact key，不塞进 legacy identity dataclass。comparison key 负责稳定比较对象；ledger identity 负责实际执行环境完整性。

### 7.3 Batch ID 和 artifact name

batch ID canonical JSON exact key set 固定为：

```json
{
  "protocol_version": 3,
  "comparison_key": "<64 lowercase hex>",
  "profile": "<closed profile value>",
  "pipeline": "gb|sk",
  "phase0a_manifest_sha256": "...",
  "input_sha256": "...",
  "reference_sha256": "...",
  "candidate_sha256": "...",
  "git_head": "<40 lowercase hex>",
  "repository_state_sha256": "...",
  "machine_fingerprint_sha256": "...",
  "recovery_provenance": null,
  "upstream_gate_provenance": null
}
```

顶层 exact key set 为上述 13 个键；GB/SK 按前述规则填完整对象或 JSON `null`。使用与 comparison key 相同的 canonical JSON 规则。prior evidence claims 的路径/内容 fingerprint 已进入 `repository_state_sha256`，不得再以顺序不确定的原始对象重复加入 payload。调用者不能提供 batch ID，unknown key 和 bool-as-int 同样拒绝。

artifact basename 固定为：

```text
benchmark-v3-<comparison_key前16位>.json
```

目录固定为：

```text
docs/performance/runs/phase0b-v3/
```

传入的 evidence path basename、parent directory 和派生 identity 任一不匹配都在启动子进程前拒绝。

## 8. Ledger v3

### 8.1 Metadata

v3 metadata 增加：

```json
{
  "protocol_version": 3,
  "comparison_key": "...",
  "phase0a_manifest_sha256": "...",
  "recovery_provenance": null,
  "upstream_gate_provenance": null
}
```

GB 为完整 recovery/空 upstream，SK 为空 recovery/完整 upstream。metadata、records、checkpoint、journal、terminal 都继续使用 canonical JSON 和 hash chain。

每个正式子进程启动前，在 `planned-output` 后追加 durable `sample-started` record。payload exact key set 为：

```json
{
  "batch_id": "<64 lowercase hex>",
  "metric": "wall|pws",
  "global_round": 1,
  "role": "reference|candidate",
  "order": ["reference", "candidate"],
  "input_sha256": "<64 lowercase hex>",
  "binary_sha256": "<64 lowercase hex>",
  "planned_output_record_sha256": "<64 lowercase hex>"
}
```

`sample-started` 必须 flush、checkpoint 和 journal anchor 完成后才可调用 reference/candidate。成功 `sample` payload 增加 `sample_started_record_sha256`，精确引用对应 started record。

### 8.2 Create-once

当前授权的 Phase 0B v3 comparison 使用闭合状态表：

| 当前状态 | 唯一允许行为 |
|---|---|
| comparison 不存在 | 创建 `attempt-0001` |
| 未封存，planned-output 存在但尚无 `sample-started` | 允许从未启动步骤继续 |
| 未封存，`sample-started` 有对应 `sample` | 只读复用已完成 sample |
| 未封存，`sample-started` 无对应 `sample` | 清理并封存 `INCOMPLETE_EVIDENCE`；不得再次调用子进程 |
| `cleanup-complete` 但尚无 `evidence-prepared` | 只从 sealed sample records 确定性重建 typed evidence；零子进程 |
| `evidence-prepared` 但未 committed | 只恢复 typed readback/marker/publication；不得采样 |
| terminal=`CLEANUP_FAILED` | 只创建 cleanup-only successor |
| 其他失败 terminal | 拒绝，不创建 attempt |
| `EVIDENCE_COMMITTED` | terminal-equivalent sealed success；typed readback 现有 artifact 后返回，不创建 attempt |
| 额外 attempt、unknown file 或不合法组合 | fail closed |

`terminal.json` 是失败封存；`EVIDENCE_COMMITTED` 是成功封存。二者都禁止新的采样 attempt。terminal 为 `CLEANUP_FAILED` 时，successor 不得记录 sample/planned-output/sample-started、不得启动子进程，只能清理历史 planned paths 并再次封存。`ENVIRONMENT_DRIFT` 不自动获得新采样 attempt；需要新的设计批准。

`KeyboardInterrupt`、`SystemExit`、进程被终止或 capture boundary 意外退出发生在 `sample-started` 后时，必须先清理再封存 `INCOMPLETE_EVIDENCE`（已有结构化 process error 时仍按 reference/candidate/correctness 映射）；不能只向外重抛并留下可重试 started sample。若进程崩溃导致当前进程来不及写 terminal，下一次 open 检测 started-without-sample 后只执行清理和封存，零子进程。

实现不得因为旧 `AppendOnlyAttemptLedger.create()` 支持某些 v2 environment recovery 而把该能力无条件带入 v3 formal recovery。

### 8.3 Prepared publication

采样、gate 和真实 cleanup 全部成功后，发布顺序固定为：

1. append `cleanup-complete`；
2. 从 ledger state 构建 schema v3 typed artifact 到内存；
3. 完成 typed rebuild 和 sanitizer；
4. 根据 artifact bytes 确定性派生 marker basename/content/SHA；
5. append durable `evidence-prepared`，payload 保存 exact `artifact_basename`、`artifact_sha256`、canonical sanitized `artifact_content`、`marker_basename` 和 `marker_sha256`；
6. marker-last publication；
7. append `evidence-committed`。

`evidence-prepared` 前的 schema、typed rebuild 或 sanitizer 失败封存 `SENSITIVE_EVIDENCE`，不得恢复 publication。`evidence-prepared` 后的 OSError、`KeyboardInterrupt`、`SystemExit` 或进程崩溃属于固定 bytes 的 publication boundary：保留 prepared record，不写失败 terminal；下一次只能重建并发布同一 bytes，零 sample、零子进程。恢复规则固定为：

| 磁盘状态 | 行为 |
|---|---|
| artifact/marker 都不存在 | 发布 artifact，再 marker |
| artifact 存在且 bytes/SHA 精确匹配，marker 不存在 | 只发布派生 marker |
| 两者都存在且内容精确匹配 | typed readback/staged scan 后 append `evidence-committed` |
| marker 单独存在 | `INCOMPLETE_EVIDENCE`，不得覆盖 |
| 任一现有内容不匹配 | `SENSITIVE_EVIDENCE`，不得覆盖 |

prepared bytes 的 deterministic typed rebuild/sanitizer/staged scan 在恢复时再次执行；失败则删除本批可确认所有权的 partial artifact/marker，封存 `SENSITIVE_EVIDENCE`。OSError 或进程级中断保留 prepared 状态以恢复同一 bytes；它不允许改变 artifact 或重新采样。publication recovery 不重新做性能 gate，也不从磁盘任意 JSON 重建内容。

`evidence-committed` record exact payload 同时绑定 `artifact_sha256` 和 `marker_sha256`。committed 后 artifact/marker 必须同时存在且逐字节匹配；缺失或漂移 fail closed，不能重新发布不一致内容。

### 8.4 v2 immutability

调用任何 v2 ledger loader 前，先对整个 canonical parent comparison directory（包括 comparison-level `journal/` 和所有 attempts）计算 tree hash。先拒绝 symlink/reparse point，再把 root 下所有 entry 按 relative POSIX path 排序：directory entry 为 `{"path":...,"kind":"directory"}`，file entry 为 `{"path":...,"kind":"file","size":...,"sha256":...}`。列表使用 `ensure_ascii=True, sort_keys=True, separators=(',', ':')` 编码为 UTF-8 后取 SHA-256。

授权固定要求：

```text
tree_sha256 = 8e961515bcac3afad271bb75eac9e439fdb18d1e8ba07b0fef7e210838796ccb
journal_head_sha256 = ae10e9d441ecebee9ba6cfb93a799f14a9085c75560103fedc9df6ff56b92c85
inventory_entry_count = 134
```

只读验证成功或失败后都必须在 `finally` 重新计算并比较同一 tree hash。tree hash 覆盖：

- 所有相对路径；
- 文件类型；
- 每个文件内容 SHA；
- metadata、terminal、journal、checkpoint 和 records。

当前 tree 必须先等于授权 digest；只证明 before/after 相同但不等于授权值仍然失败。v3 创建过程不得调用 v2 ledger 的 `_append()`、`finish()`、repair、checkpoint 补写或 journal 修复。不能在 loader 之后才取“before”快照，也不能只 hash `attempt-0001/`。测试除 tree hash 外还 monkeypatch 所有 write/repair 入口为立即失败，证明成功和每一种失败 preflight 都零写入 v2 parent。

## 9. Fresh sampling flow

### 9.1 GB

```text
preflight fixed SHA and state-aware repository gate
→ validate closed v2 parent
→ derive unique v3 comparison/batch identity
→ create v3 attempt-0001
→ 对每个 sample：planned-output → durable sample-started → child process → sample
→ fresh wall global round 1–5
→ fresh PWS global round 1–5
→ mandatory paired expansion to global round 6–10 when any time/PWS gate is critical
→ validate all samples/dimensions/oracles/same-batch/closed gates
→ cleanup-complete
→ durable evidence-prepared
→ marker-last sanitized schema v3 publication
→ evidence-committed
→ typed readback + staged scan
→ evidence-only commit
```

### 9.2 SK

SK preflight 除固定 SHA/输入外，必须：

- 派生预期 GB v3 comparison key/artifact/marker；
- typed read GB schema/protocol/pipeline/verdict；
- marker 内容与 artifact 重建值一致；
- artifact 和 marker 都被当前 `HEAD` 跟踪；
- 当前 `HEAD` 是只包含该 GB artifact/marker 的 evidence-only 单父提交；
- 自动构建的 `UpstreamGateProvenance` 进入 SK comparison/batch/ledger/evidence；
- repository clean；
- 新建 SK attempt 时不存在当前 SK v3 comparison directory/evidence；已有 v3 state 时只能按第 8.2/8.3 节恢复，绝不创建新采样 attempt。

随后全新运行 SK wall/PWS，扩样和 publication 规则与 GB 相同。GB/SK 不属于同一个 benchmark batch；每条 pipeline 内的 reference/candidate 必须同批，SK 通过已提交 GB evidence 建立执行顺序和审计依赖。

### 9.3 禁止跨批复用

v3 verdict/evidence 不得读取 parent v2 的：

- metric values；
- medians；
- direction diagnostics；
- output-size observations；
- runtime timings。

parent 只贡献闭合 provenance。v3 output bytes 来自 v3 的 `N` 轮元数据中位数，固定 limit 来自 approved Phase 0A manifest；其他 ratio 的分子/分母来自 v3 同批 reference/candidate。

## 10. Evidence schema v3

### 10.1 类型

`BenchmarkManifestEvidence` 支持 schema 1/2/3 read；builder/publisher 只写 3。v3 增加：

```python
@dataclass(frozen=True)
class RecoveryProvenanceEvidence:
    parent_protocol_version: Literal[2]
    parent_comparison_key: str
    parent_attempt: Literal[1]
    parent_terminal_sha256: str
    parent_comparison_tree_sha256: str
    parent_journal_head_sha256: str
    parent_inventory_entry_count: Literal[134]
    reason: Literal["MISSING_FORMAL_SHEET_DIMENSIONS"]


@dataclass(frozen=True)
class UpstreamGateProvenanceEvidence:
    pipeline: Literal['gb']
    protocol_version: Literal[3]
    schema_version: Literal[3]
    comparison_key: str
    artifact_basename: str
    artifact_sha256: str
    marker_basename: str
    marker_sha256: str
    validated_commit_sha: str
```

```json
{
  "schema_version": 3,
  "protocol_version": 3,
  "recovery_provenance": null,
  "upstream_gate_provenance": null
}
```

provenance 适用规则固定为：

- 当前 `PHASE0B_VS_PHASE0A + gb`：必须是闭合授权匹配出的完整对象；
- 当前 `PHASE0B_VS_PHASE0A + gb`：upstream 必须为 `null`；
- 当前 `PHASE0B_VS_PHASE0A + sk`：recovery 必须为 `null`，upstream 必须为自动派生的完整对象；
- Phase 1–5 和其他后续非恢复 v3 comparison：recovery 必须为 `null`；upstream 由对应 profile 规格闭合定义，未定义时必须为 `null`；
- 不允许调用者自行提供非空 provenance。

reader 对 extra/missing/duplicate key、错误类型、非小写 SHA、未知 reason 全部拒绝。

### 10.2 Sanitizer

provenance 只含 enum、计数和 SHA，不含本地 path。v3 继续拒绝：

- 绝对路径、UNC、用户名、hostname；
- ERP basename/stem；
- raw stdout/stderr/command；
- `expected=`/`actual=` 和真实 cell value；
- 任意未闭合 reason/message。

v3 artifact 和 marker 继续 marker-last publication；失败或扫描失败删除本批待提交 artifact，并保持 local ledger terminal 可审计。

## 11. Verdict 和错误模型

### 11.1 Preflight

preflight 顺序固定为：

1. 解析闭合 CLI，校验 trusted roots，并只采集不依赖 worktree status 的 static comparison inputs：pipeline/profile/labels、input/reference/candidate/manifest SHA；
2. 用 static inputs 和自动 provenance 派生 comparison key，只读定位 canonical v3 comparison directory，并以 `strict_identity=False` 的纯 snapshot parser 解析 ledger state；
3. comparison 不存在的新 attempt 要求 strict clean worktree，且目标 artifact/marker 都不存在；
4. `evidence-prepared`/`EVIDENCE_COMMITTED` 状态允许的 dirty path 只能是 prepared payload 派生的 exact artifact 和 marker；
5. 允许的 artifact/marker 必须是 untracked 或与当前 ledger state 一致的预期状态，内容逐字节/SHA 匹配；任何其他 staged/modified/untracked path 都拒绝；
6. publication recovery 直接进入第 8.3 节，零 parent 重验以外的采样动作、零子进程；
7. 只有新采样 attempt 才在 clean-state gate 后采集完整六字段 `BenchmarkIdentity`；existing cleanup/prepared/committed state 使用 ledger 中已封存 identity，并逐项核对当前 input/executable/Git HEAD/machine；
8. existing publication state 的 repository-status 比较从当前 status 中只扣除 exact allowlisted artifact/marker，再要求归一化结果等于 ledger 创建时的 repository state；不能把允许的 evidence path 计入新 identity。

不能在读取 ledger state 前无条件执行 clean-worktree 拒绝，否则 artifact link 与 marker-last 之间的合法 prepared recovery 永远不可达。

state-aware repository policy 固定为：

| Ledger 状态 | 允许的 repository/evidence shape |
|---|---|
| comparison 不存在 | worktree 完全 clean，artifact/marker 不存在 |
| 正在采样且没有 started-without-sample | 除 ignored local ledger/runtime 外完全 clean，artifact/marker 不存在 |
| started-without-sample | 不允许 evidence；只清理并封 terminal |
| cleanup-complete、尚未 prepared | worktree clean，artifact/marker 不存在；只重建 evidence，零子进程 |
| evidence-prepared | 只允许 artifact 不存在、artifact-only、或 artifact+marker；现有 bytes 必须与 prepared record 精确匹配 |
| EVIDENCE_COMMITTED | artifact+marker 必须同时存在并精确匹配 committed/prepared SHA；只 typed readback |
| 失败 terminal | 不允许 evidence recovery；返回原 terminal |

prepared/committed 状态下允许的 Git status path exact 等于派生的 artifact/marker path，且不能出现 staged modification；任何其他 modified/staged/untracked path 都是 `ENVIRONMENT_DRIFT`。marker-only 违反 marker-last，返回 `INCOMPLETE_EVIDENCE`。新建 SK attempt 的“不存在 comparison/evidence”不得套用到 publication recovery。

映射固定为：

| 场景 | verdict / exit | 副作用 |
|---|---|---|
| CLI 出现 protocol/parent/reason/terminal 等禁止参数 | usage / exit 5 | 无 attempt、零子进程、零 parent 写入 |
| parent path/layout/terminal/records/count/hash/shape 不一致 | `INCOMPLETE_EVIDENCE` | 同上 |
| GB v3 artifact/marker/evidence commit shape 或 tracked state 与闭合预期不一致 | `INCOMPLETE_EVIDENCE` | 同上 |
| 当前 input/reference/candidate/manifest/v1-v2 terminal/machine/repository identity 漂移 | `ENVIRONMENT_DRIFT` | 同上 |
| 新采样 attempt 的 worktree 非 clean | `ENVIRONMENT_DRIFT` | 同上 |
| prepared/committed recovery 出现 exact allowlist 外的 dirty path | `ENVIRONMENT_DRIFT` | 零子进程、保留 prepared state |
| prepared artifact/marker 内容与 ledger 派生 bytes 不匹配 | `SENSITIVE_EVIDENCE` | 零子进程、不得覆盖 |

尚未创建 attempt 的 preflight 失败不生成 terminal。不得执行 reference/candidate，并必须在所有返回/异常路径证明 parent comparison tree hash 未变化。

### 11.2 Attempt 内失败

进入 attempt 后的状态序列固定为：

1. 验证 N=5 completeness、correctness、reference、environment 和 sample identity；
2. 计算任一 time/PWS 指标是否触发 mandatory expansion；
3. 如触发，wall/PWS 都完整采集 global round 6–10；
4. 验证最终 N=5/10 same-batch、完整性、dimensions 和 oracle；
5. 评价全部 combined direct/composite/stage/output closed gates，明确失败优先；
6. 只有全部 closed gates 通过后，评价 active direct metric near-boundary direction conflict；
7. 构建 typed evidence 到内存；
8. cleanup；
9. cleanup 成功后记录 `cleanup-complete`；
10. sanitizer/marker-last publication；
11. `evidence-committed` 和 `VALIDATED`。

mandatory expansion 是采样状态转换，不是 terminal verdict；不得先用 N=5 closed-gate 失败跳过本应强制执行的临界扩样。

dimensions 缺失在 evidence build 前必须由 sample validation 返回 `INCOMPLETE_EVIDENCE`；不得再次允许 20 个空 dimensions sample 走到 publisher。

### 11.3 Cleanup

- 主失败、cleanup 成功：保留主 verdict；
- 主失败、cleanup 失败：`CLEANUP_FAILED`，原 verdict 写入 `primary_verdict`；
- raw log SHA 保留在 terminal，实际日志按 cleanup policy 删除；
- workbook、PWS stdout/stderr/result/driver artifacts 和 raw logs 全部登记后再进入可能失败的 enrichment/validation；
- cleanup-only successor 不能创建新 evidence 或新 sample。

## 12. 实现文件边界

### 修改

- `tests/rust_oracle/benchmark_protocol.py`
  - current/legacy version 常量；
  - `RecoveryReason`、`RecoveryProvenance`、`UpstreamGateProvenance`；
  - v2 legacy key 和 v3 key 派生；
  - v3 identity validation。
- `tests/rust_oracle/phase0_harness.py`
  - 闭合授权表；
  - v2 parent read-only eligibility；
  - ledger v3 create/load/terminal/cleanup-only；
  - GB/SK v3 runner gate。
- `tests/rust_oracle/evidence.py`
  - schema 3 typed reader/builder；
  - v3 basename、recovery/upstream provenance 和 sanitizer。
- `tests/rust_oracle/test_benchmark_protocol.py`
- `tests/rust_oracle/test_phase0_harness.py`
- `tests/rust_oracle/test_evidence.py`
- `docs/performance/README.md`
- `docs/superpowers/specs/2026-07-11-rust-output-ingest-continuous-performance-design.md`
  - 仅同步当前实施状态和 protocol v3 链接，不重写原始门槛。
- `docs/superpowers/plans/2026-07-11-rust-output-phase-0a-3-writer-optimization.md`
  - 仅把 Phase 0B handoff 指向 v3 evidence；不改变 Phase 1 门槛。

### 正式成功时才创建

- `docs/performance/runs/phase0b-v3/benchmark-v3-<gb-key-prefix>.json`
- 对应 batch marker；
- GB evidence-only commit；
- 随后 SK artifact/marker 和单独 evidence-only commit。

### 禁止修改

- `docs/performance/baselines/2026-07-11-windows-x64-phase0a.json`
- `docs/performance/dependencies/2026-07-11-rust-xlsxwriter-0.96.0.json`
- `rust/target/perf-local/batches/09d6.../attempt-0001/**`
- `rust/crates/**` 生产代码；
- 主工作区用户所有的 `rust/crates/costing-core/src/model.rs` 差异。

## 13. 测试设计

### 13.1 Identity 和授权

- v3 key 与 v2 key 不同；
- GB key 改任一 parent 字段都变化；
- SK `null` provenance 稳定参与 key；
- CLI 不能指定 protocol/parent/reason/terminal SHA；
- 非授权 identity 在子进程调用前失败；
- 授权表不能出现重复匹配。

### 13.2 Parent eligibility

- 精确 synthetic v2 parent 通过；
- terminal SHA 漂移拒绝；
- tree digest、journal head 或 entry count 漂移拒绝；
- append journal、增加 attempt-0002、unknown file、symlink/reparse point 都在 v3 create 前拒绝；
- terminal verdict 非 `INCOMPLETE_EVIDENCE` 拒绝；
- 41/43 records 拒绝；
- sample role/round/metric 重复或缺失拒绝；
- evidence-prepared/committed 拒绝；
- versioned v2 evidence 存在拒绝；
- dimensions 全空是唯一允许形态；部分空、全完整均拒绝；
- 成功/每一种失败 preflight 前后 entire comparison tree hash 相同；
- monkeypatch write/repair 入口证明 parent reader 没有隐藏写路径。

### 13.3 Ledger v3

- v1/v2 load read-only；
- v1/v2 create/append/finish 拒绝；
- v3 create 写 protocol/provenance；
- planned-output 后未 started 的步骤可续跑；
- sample-started 有 sample 时只读复用；
- sample-started 无 sample 时清理并封存，第二次运行零子进程；
- reference/candidate 启动后 `KeyboardInterrupt`/`SystemExit` 不能留下可重试 sample；
- terminal 后拒绝新采样 attempt；
- cleanup-only successor 不允许 benchmark records；
- environment terminal 不自动创建新采样 attempt。
- `EVIDENCE_COMMITTED` 作为成功封存只能 typed readback，不能采样。
- cleanup-complete/prepared/committed 三种 publication state 均零子进程；
- artifact-only crash window 只补 marker，artifact+marker crash window只补 committed record；
- marker-only、bytes 漂移和 allowlist 外 dirty path fail closed；
- deterministic sanitizer/staged-scan failure 封存 `SENSITIVE_EVIDENCE`；
- prepared 后 OSError/interruption 保留相同 bytes，恢复时不能改变 artifact。

### 13.4 Fresh sampling

- v3 不读取 v2 metric values；
- 每个子进程前 durable `sample-started`，started-without-sample 不得重启；
- wall/PWS 各自 fresh global round 1–5；
- 任一临界指标触发两套 global round 6–10；
- AB/BA 全局奇偶不重置；
- 所有 wall/PWS sample 在 ledger 中保存实际 dimensions；
- reference/candidate oracle mismatch fail closed；
- dimensions mismatch 清理 workbook/raw log 并封存 SHA；
- GB 非 `VALIDATED` 时 SK runner 未被调用。
- SK key/batch/ledger/evidence 均绑定 exact GB artifact/marker/evidence-only commit。

### 13.5 Evidence

- schema 1/2 readback 不变；
- builder/publisher 拒绝写 schema 1/2；
- schema 3 GB provenance 必填；
- schema 3 GB recovery 必填且 upstream 为 null；
- schema 3 SK recovery 为 null 且 upstream 必填；
- 修改 upstream artifact/marker/commit 任一字段都会改变 SK key 并使 typed rebuild 失败；
- basename/parent directory/key 不匹配拒绝；
- v3 artifact marker-last、typed rebuild 一致；
- evidence-prepared 同时绑定 artifact/marker basename 和 SHA，evidence-committed 同时绑定两个 SHA；
- provenance 字段 canary 和未知值被 sanitizer 拒绝；
- scan failure 删除本批 artifact。

### 13.6 Formal preflight

- 固定 EXE/manifest/v1/v2 SHA 全匹配；
- worktree dirty 时不创建 attempt；
- GB evidence 未 tracked/marker 不一致时 SK 不运行；
- 已存在 comparison 时按第 8.2 节状态表处理，任何状态都不得创建新的采样 attempt；
- 正式执行命令不包含 `--check-only`；
- 不执行 `cargo build`。

## 14. 伪代码草案

### 14.1 Parent 资格

```python
# [伪代码草案]
# 目标：只允许当前已批准的 dimensions evidence-construction 缺陷进入 v3。
# 输入：当前请求 identity、闭合授权表、canonical v2 ledger root。
# 输出：RecoveryProvenance；失败时零写入、零子进程。

def authorize_v3_recovery(identity: BenchmarkIdentity) -> RecoveryProvenance:
    approved = match_exactly_one_approved_recovery(identity)
    comparison = canonical_comparison_path(approved.parent_comparison_key)
    reject_reparse_points_and_unknown_inventory(comparison)
    before = hash_comparison_tree(comparison)
    require(before.sha256 == approved.parent_comparison_tree_sha256)
    require(before.entry_count == approved.parent_inventory_entry_count)
    require(before.journal_head_sha256 == approved.parent_journal_head_sha256)
    try:
        parent = parse_and_validate_ledger_snapshot(
            comparison / 'attempt-0001',
            expected_protocol_version=2,
            repair=False,
        )
        validate_terminal_sha(parent, approved.parent_terminal_sha256)
        require(parent.terminal_verdict == INCOMPLETE_EVIDENCE)
        require(record_kind_counts(parent) == {
            "planned-output": 20,
            "sample": 20,
            "first-group": 1,
            "cleanup-complete": 1,
        })
        require(no_prepared_or_committed_evidence(parent))
        require(complete_wall_and_pws_rounds_1_to_5(parent))
        samples = tuple(parse_exact_v2_sample(payload) for payload in parent.sample_payloads)
        require(all(sample.normal_run.runtime.sheet_dimensions == () for sample in samples))
        require(all_sample_identities_orders_oracles_and_output_bytes(samples, parent.identity))
        require(no_versioned_v2_evidence(parent.comparison_key))
        return RecoveryProvenance.from_approved(approved)
    finally:
        # 为什么在 loader 前取快照并在 finally 比较：成功和失败读取都必须证明零写入，
        # comparison-level journal 也属于不可变 parent，而不只是 attempt 目录。
        require(hash_comparison_tree(comparison) == before)
```

### 14.2 Fresh v3 batch

```python
# [伪代码草案]
# 目标：用新协议身份重新采集整批证据，不读取 v2 性能值。
# 输入：固定 pipeline/input/reference/candidate、批准 baseline、可选 recovery provenance。
# 输出：VALIDATED schema v3 evidence，或不可重采的 terminal verdict。

def run_protocol_v3_batch(request: PairedBenchmarkRequest) -> PairedBenchmarkResult:
    static = capture_static_comparison_inputs(request)
    recovery = authorize_v3_recovery(static) if request.pipeline == "gb" else None
    upstream = None

    if request.pipeline == "sk":
        upstream = derive_committed_validated_gb_v3_provenance(static)

    comparison_key = derive_v3_comparison_key(static, recovery, upstream)
    state = inspect_v3_state_before_repository_gate(request, comparison_key)
    apply_state_aware_repository_policy(state, request)

    if state.is_evidence_committed:
        return typed_readback_committed_evidence(state, request)
    if state.is_evidence_prepared:
        return recover_prepared_publication(state, request)
    if state.is_cleanup_complete:
        return prepare_and_publish_from_sealed_samples(state, request)
    if state.has_terminal:
        raise terminal_failure(state)

    identity = capture_full_identity_after_state_gate(request, state)

    attempt = open_or_create_v3_attempt_once(
        state=state,
        identity=identity,
        comparison_key=comparison_key,
        recovery_provenance=recovery,
        upstream_gate_provenance=upstream,
    )

    try:
        wall = capture_fresh_group_with_durable_started(
            metric="wall", global_round_start=1, round_count=5
        )
        pws = capture_fresh_group_with_durable_started(
            metric="pws", global_round_start=1, round_count=5
        )

        if requires_mandatory_expansion(wall, pws):
            wall += capture_fresh_group_with_durable_started(
                metric="wall", global_round_start=6, round_count=5
            )
            pws += capture_fresh_group_with_durable_started(
                metric="pws", global_round_start=6, round_count=5
            )

        validate_same_batch(wall, pws)
        validate_complete_actual_dimensions(wall, pws)
        validate_oracles(wall, pws)
        evaluate_protocol_v3_closed_gates(wall, pws)  # 失败统一 raise HarnessFailure
    except (HarnessFailure, KeyboardInterrupt, SystemExit) as error:
        primary = normalize_outer_formal_failure(error)
        cleanup = cleanup_all_registered_artifacts()
        return seal_with_cleanup_priority(attempt, primary, cleanup)

    # 所有采样和 gate 成功仍必须先清理；不能把 mark_cleanup_complete 当作清理动作。
    cleanup = cleanup_all_registered_artifacts()
    if not cleanup.succeeded:
        return seal_with_cleanup_priority(
            attempt,
            HarnessFailure(CLEANUP_FAILED, 'formal artifact cleanup failed'),
            cleanup,
        )

    attempt.mark_cleanup_complete()
    return prepare_and_publish_from_sealed_samples(attempt, request)


def prepare_and_publish_from_sealed_samples(attempt, request):
    try:
        artifact = build_and_sanitize_schema_v3_evidence_from_ledger(attempt)
        marker = derive_batch_marker(artifact)
        attempt.prepare_evidence(
            artifact_basename=artifact.file_name,
            artifact_sha256=sha256(artifact.content),
            artifact_content=artifact.content,
            marker_basename=marker.file_name,
            marker_sha256=sha256(marker.content),
        )
    except HarnessFailure as error:
        return seal_terminal(attempt, SENSITIVE_EVIDENCE, cause=error)

    try:
        publish_and_verify_marker_last(artifact, marker)
        typed_readback_and_staged_scan(artifact, marker)
    except DeterministicEvidenceFailure as error:
        remove_owned_partial_publication(artifact, marker)
        return seal_terminal(attempt, SENSITIVE_EVIDENCE, cause=error)
    except (OSError, KeyboardInterrupt, SystemExit) as interruption:
        # 性能样本和待发布 bytes 已固定；保留 prepared 状态，下一次零子进程恢复。
        raise PreparedPublicationPending() from interruption

    attempt.mark_evidence_committed(
        artifact_sha256=sha256(artifact.content),
        marker_sha256=sha256(marker.content),
    )
    return typed_validated_result(attempt, artifact)
```

### 14.3 Durable sample start

```python
# [伪代码草案]
# 目标：子进程一旦启动，对应 sample 就不能因中断或崩溃被选择性重试。
# 输入：闭合 round plan、role、planned-output record、capture callback。
# 输出：唯一 sample，或清理后不可重采的 terminal failure。

def capture_one_formal_sample(attempt, plan, role, planned_output, capture):
    started = attempt.record_sample_started(
        batch_id=attempt.batch_id,
        metric=planned_output.metric,
        global_round=plan.global_round,
        role=role,
        order=plan.order,
        input_sha256=attempt.identity.input_sha256,
        binary_sha256=binary_sha_for_role(attempt.identity, role),
        planned_output_record_sha256=planned_output.record_sha256,
    )
    # record_sample_started 已完成 record/checkpoint/journal durable append 后才能启动进程。
    try:
        # capture adapter 自己负责在异常时终止并回收子进程，但不负责 cleanup/terminal。
        captured = capture()
        sample = validate_and_build_sample(captured)
        attempt.record_sample(sample, sample_started_record_sha256=started.sha256)
        return sample
    except (KeyboardInterrupt, SystemExit) as interruption:
        raise HarnessFailure(INCOMPLETE_EVIDENCE, 'formal sample was interrupted') from interruption
    except HarnessFailure:
        raise
    except Exception as error:
        # 唯一 cleanup/terminal owner 是外层 run_protocol_v3_batch。
        raise map_unclassified_started_capture_failure(role, error) from error
```

open/resume 时若看到 started-without-sample，不再调用本函数的 `capture`；只执行清理和 terminal seal。

### 14.4 Terminal 后行为

```python
# [伪代码草案]
# 目标：终态不重采；仅 cleanup failure 允许无子进程的清理后继。

def open_v3_successor(previous: AppendOnlyAttemptLedger):
    require(previous.protocol_version == 3)
    require(previous.terminal_verdict == CLEANUP_FAILED)
    successor = create_cleanup_only_attempt(previous)
    prohibit(successor.record_sample)
    prohibit(successor.record_planned_output)
    prohibit(successor.record_sample_started)
    prohibit(run_reference_or_candidate)
    return successor
```

## 15. 验证命令

实现阶段至少运行：

```powershell
uv run python -m pytest tests/rust_oracle/test_benchmark_protocol.py -q --basetemp C:\costing-v3-protocol
uv run python -m pytest tests/rust_oracle/test_phase0_harness.py -q --basetemp C:\costing-v3-harness
uv run python -m pytest tests/rust_oracle/test_evidence.py -q --basetemp C:\costing-v3-evidence
uv run python -m pytest tests/rust_oracle -q --basetemp C:\costing-v3-full
uv run python -m ruff check tests/rust_oracle
uv run python -m ruff format tests/rust_oracle --check
uv run python -m tests.rust_oracle.evidence scan --root docs/performance
git diff --check
```

测试临时根使用短绝对路径，避免 Windows MAX_PATH 噪声；每次使用前必须确认路径不存在或属于本轮测试，测试后验证再清理。

正式 GB/SK 命令不写入本规格。实施计划必须先给出精确 preflight、唯一 identity、停止条件和 evidence-only commit 步骤；代码、测试和审查未完成前不得运行。

## 16. 验收

本恢复阶段完成必须同时证明：

- v2 parent tree hash 未变化；
- v3 implementation/full tests/Ruff/format/sanitizer 全通过；
- Python、data、security、doc review 无未解决 P0/P1；
- 固定 EXE、Phase 0A、v1/v2 terminal SHA 未漂移；
- GB v3 是 fresh N=5/10 同批证据并 `VALIDATED`；
- GB schema v3 artifact/marker 已 typed validation、staged scan 并单独提交；
- SK v3 在 GB commit 后执行并 `VALIDATED`；
- SK artifact/marker 单独提交；
- GB/SK instrumented wall / same-batch Phase 0A reference wall 均 `<=1.02`；
- Phase 0B PWS fresh N=5/10、same-batch、environment 和 direction diagnostic 全部有效，但不误设 direct gate；
- GB/SK workbook、runtime、quality、error-log 和 output bytes `<=1.10` 全通过；
- 没有真实 workbook/raw log/绝对路径/ERP basename 进入 Git；
- 当前 worktree clean；
- 主工作区用户 `model.rs` 差异未被触碰；
- Phase 1 handoff 明确指向冻结的 Phase 0B candidate EXE SHA 和已提交 v3 evidence。

任一项缺失都不得宣称 Phase 0B 完成，也不得进入 Phase 1。

## 17. 后续流程

1. 提交本规格；
2. 做 spec 自审和只读 doc/Python feasibility review；
3. 使用 `superpowers:writing-plans` 生成逐步实施计划；
4. 计划提交后再按 TDD 实现；
5. 代码、测试和审查通过后执行正式 GB v3；
6. GB evidence-only commit 后执行正式 SK v3；
7. 两条 pipeline 均通过后恢复原计划 Phase 1→5；
8. 正式 GB/SK 仍受本规格和上位规格全部门禁约束。

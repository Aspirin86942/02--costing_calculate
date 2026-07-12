# Phase 0H Paired Direction Protocol v2 设计

**状态：** 用户已选择方案 2；本文件是书面复核稿，用户确认后才进入 implementation plan。

**日期：** 2026-07-12

**关联设计：** `docs/superpowers/specs/2026-07-11-rust-output-ingest-continuous-performance-design.md`

## 1. 目标

在不修改 Phase 0A 批准基线、不重建 reference/candidate EXE、不改写旧 attempt 的前提下，引入不可选择的 paired benchmark protocol v2，解决旧规则在强制扩样后把“已经远离门槛的 N=10 combined median”仍一律判为 `INCONCLUSIVE` 的问题。

v2 保留以下原规则：

- 第一组固定 global round 1–5；
- 一旦任一受控 time/PWS 指标进入门槛 ±3% 临界区，必须同时追加 wall/PWS global round 6–10；
- 不得根据首组通过或失败选择是否扩样；
- reference/candidate 继续全局 AB/BA 交替；
- correctness、输入/二进制/Git/机器身份、清理和敏感证据规则不变；
- 最多 N=10，不允许第三组或操作者重采；
- 所有门禁仍由已关闭的 `COMPARISON_LIMITS` 决定。

v2 只修改扩样后的“方向冲突如何裁决”，并让协议版本进入完整审计身份。

## 2. 已封存事实

旧 protocol v1 的 GB Phase 0B attempt 4 已完成 40 个正式样本并封存：

```text
comparison_key:              b6a98530a04b060b6d9739d804f1b928682b55e58ec7fae03bd779fb9b526149
attempt_number:              4
terminal_verdict:            INCONCLUSIVE
terminal_sha256:             d42940dfc48f208834efa103de6a08663e75d1ee09dd6804d4e2416ad90af71f
ledger_record_head_sha256:   907de0c12b9c4c5a76cbc7ba895e9f4302f3e4c5e63cfcea91e6a5d05fc37540
checkpoint_head_sha256:      9d85b477940d62c00c57f3a6b6de7fd75bed7b9c5ee42f7eaed0e259a00ffef2
```

脱敏指标：

| 指标 | round 1–5 | round 6–10 | combined N=10 |
|---|---:|---:|---:|
| external wall ratio | 1.023791 | 0.983364 | 0.940918 |
| PWS ratio | 1.000011 | 0.999943 | 0.999742 |

Phase 0B profile 只有 `wall_ratio <= 1.02`，没有 PWS closed limit。旧规则因为 wall 和 PWS 两组都发生方向反转，在 combined gate 之前终止。

这些记录只能读取，不能删除、覆盖、追加、迁移到 v2 或追认为通过。

## 3. 非目标

本设计不做以下事项：

- 不修改、替换或重新批准 Phase 0A manifest；
- 不重建两份固定 EXE；
- 不复用 v1 attempt 4 的任何正式样本；
- 不把 v1 `INCONCLUSIVE` 填入 v2 的 safe retry 列表；
- 不改变 3% 临界区、10% output bytes、20 秒、2.0 GiB 或其他门槛；
- 不新增 warm-up、CPU affinity、process priority、休眠或人工机器状态判断；
- 不修改 workbook/runtime/correctness oracle；
- 不允许 CLI 操作者选择 v1/v2；
- 不以 no-op candidate commit 换 SHA 重采；
- 不创建 PR。

## 4. 协议身份

### 4.1 固定版本

新增关闭常量：

```python
PAIRED_PROTOCOL_VERSION: Final = 2
```

正式 `paired` CLI 只能运行当前版本 2。不得增加 `--protocol-version` 参数，也不得从环境变量读取版本，避免操作者根据结果选择协议。

### 4.2 Batch ID

`derive_batch_id()` 的 canonical payload 在既有扁平 identity 字段基础上增加
`protocol_version`；下面的 `existing_identity_fields` 只是说明占位，不是待新增的嵌套字段：

```json
{
  "protocol_version": 2,
  "profile": "...",
  "pipeline": "...",
  "existing_identity_fields": "remain flattened exactly as v1"
}
```

因此同一输入、binary SHA 和机器身份在 v1/v2 下仍产生不同 batch ID。

### 4.3 Comparison key

抽取单一 `derive_comparison_key()`，canonical payload 必须包含：

```text
protocol_version
pipeline
comparison_profile
reference_label
candidate_label
input_sha256
reference_sha256
candidate_sha256
```

protocol version 不得只依赖 Git HEAD 间接区分。v2 comparison key 必须与已封存 v1 key 不同，因此 v2 从 `attempt-0001` 开始。

### 4.4 Ledger metadata

新 attempt metadata 显式保存 `protocol_version=2`。Loader 行为：

- 旧 metadata 缺少该字段时，只按 `protocol_version=1` 读取；
- 新 metadata 必须精确为整数 2，拒绝 `bool`、字符串、未知整数；
- comparison directory 中所有 attempt 的 protocol version 必须一致；
- v1 loader 兼容只用于读取和审计，正式 runner 不再创建 v1 attempt。

`BatchAttempt` 同步携带 protocol version，避免 evidence 构建时从全局常量猜测。

## 5. v2 方向冲突裁决

### 5.1 Direct metric gate

方向冲突只对当前 profile/pipeline 中直接存在的 wall/PWS closed limit 生效：

| Metric | Direct ratio gate | Direct absolute gate |
|---|---|---|
| wall | `wall_ratio` | `wall_seconds` |
| pws | `pws_ratio` | `pws_bytes` |

以下 composite/stage gate 不把 wall/PWS 自动视为 direct metric gate：

- `ingest_or_pws_ratio`；
- `writer_populate_or_export_ratio`；
- `ingest_ratio`、`writer_populate_ratio`、`xlsx_save_ratio`；
- `output_bytes_ratio`。

这些门禁仍由既有 `_evaluate_closed_profile()` 评价，但不参与方向冲突 veto。

同一 resolved profile/pipeline limits entry 下，每个 metric 最多允许一个 direct gate；若该 entry 同时配置 ratio 和 absolute direct gate，profile table validation 必须 fail closed。不同 pipeline 各自使用不同 direct gate 不构成冲突。

### 5.2 Combined value

扩样完成后先验证两个五轮组可连接，再无条件形成全局 N=10 group。这里的“无条件”只表示不在 merge helper 内提前按方向失败；所有 identity、轮号、batch、AB/BA 和样本完整性检查仍必须通过。

组合值定义：

```text
ratio gate:
  combined_value = candidate_N10_median / reference_N10_median

absolute gate:
  combined_value = candidate_N10_median
```

归一化值：

```text
normalized = combined_value / direct_limit
near_boundary = abs(normalized - 1) <= 0.03
```

不得用第一组、第二组或历史 manifest 的有利值替代 combined N=10。

### 5.3 裁决表

| 任一 combined closed gate 是否明确失败 | 当前 metric 是否方向冲突 | 是否 direct gate | combined 是否仍在 ±3% | 裁决 |
|---|---|---|---|---|
| 是 | 任意 | 任意 | 任意 | `CANDIDATE_FAILED` |
| 否 | 否 | 任意 | 任意 | `VALIDATED` |
| 否 | 是 | 否 | 不适用 | 只记录 diagnostic，`VALIDATED` |
| 否 | 是 | 是 | 是 | `INCONCLUSIVE` |
| 否 | 是 | 是 | 否 | `VALIDATED` |

因此：

- 必须先评价全部 N=10 combined closed gates；任何明确失败优先返回 `CANDIDATE_FAILED`，不能被另一指标的临界方向冲突改写；
- 只有全部 closed gates 已通过，方向 diagnostic 才能把原本的 `VALIDATED` 收窄为 `INCONCLUSIVE`；
- 只有“direct gated metric 方向冲突且 N=10 仍靠近门槛”才是不确定；
- Phase 0B 的 PWS 冲突只记录，不否决，因为该 profile 没有 PWS direct gate；
- 不增加第 11 轮或任何额外样本。

### 5.4 多指标

先构建两套 combined group，并一次性执行现有 closed profile gate。只要 direct、composite 或 stage gate 中任一项明确失败，就先返回 `CANDIDATE_FAILED`。仅当全部 closed gates 通过后，若 wall/PWS 任一 active direct metric 满足“冲突且 near boundary”，才返回 `INCONCLUSIVE`；否则返回 `VALIDATED`。

## 6. Direction diagnostics

新增关闭的 evidence 结构，只有 N=10 时恰好包含 wall、PWS 两项：

```python
@dataclass(frozen=True)
class DirectionDiagnosticEvidence:
    metric: Literal['wall', 'pws']
    first_group_ratio: Decimal
    second_group_ratio: Decimal
    combined_ratio: Decimal
    directions_conflict: bool
    direct_gate: Literal['none', 'ratio', 'absolute']
    direct_limit: Decimal | None
    normalized_to_limit: Decimal | None
    near_boundary: bool | None
```

约束：

- N=5 evidence 的 diagnostics 必须为空；
- N=10 evidence 必须按 wall、PWS 固定顺序各一项；
- `direct_gate='none'` 时 limit、normalized、near_boundary 必须全为 `None`；
- direct gate 存在时 `direct_limit`、`normalized_to_limit` 必须非空、正数且有限，`near_boundary` 必须是按公式重算的布尔值；
- `directions_conflict` 必须由 `(first_group_ratio - 1) * (second_group_ratio - 1) < 0` 计算，不能由调用者任意填写；任一 ratio 恰好等于 1 时均不算冲突；
- evidence 不保存绝对路径、命令 stdout/stderr 或 ERP 值。

## 7. Evidence schema v2

### 7.1 双读单写

Benchmark evidence schema 升级为 2：

```text
schema v1: 继续严格读取和原样 rebuild，仅用于历史审计
schema v2: 当前正式 runner 唯一允许写出的 schema
```

reader/rebuilder 可以接受 v1，以纯函数方式验证旧 artifact；正式 writer/publisher 必须拒绝创建或发布 schema/protocol v1。v1 rebuild 不得写回历史路径，也不得生成新的 artifact。

v2 在 v1 字段基础上增加：

```json
{
  "schema_version": 2,
  "protocol_version": 2,
  "direction_diagnostics": []
}
```

Builder/reader 必须继续对两个 schema 使用 exact-key validation：

- v1 禁止出现 v2 字段；
- v2 缺字段或包含未知字段均拒绝；
- schema 2 与 protocol 2 必须一一对应；
- 未知 schema/protocol、`bool` 整数别名和字符串数字全部 fail closed。

### 7.2 Artifact filename

v2 benchmark artifact basename 直接绑定完整 comparison identity：

```text
benchmark-v2-<comparison_key[:16]>.json
```

`comparison_key` 已覆盖 protocol、pipeline、profile、input、reference/candidate label 与 SHA。该命名既保留短文件名，又避免不同 input/reference 共用同一 candidate 时发生 basename 碰撞，并使 protocol identity 在文件名中可见。新增一个纯 `expected_benchmark_artifact_name()` 作为 builder、测试和正式命令的唯一事实来源；`--evidence-path` 仍要求传入完整、精确的目标路径，不把接口放宽为任意目录。

正式 PowerShell 命令先用单行 `uv run python -c` 调用该 helper 计算 basename，再组成 `docs/performance/runs/<phase>/...` 路径。不得继续使用与 typed artifact 不匹配的固定 `gb.json`/`sk.json` basename。

### 7.3 Attempt count

v2 comparison identity 是新身份，因此 evidence 中：

```text
attempt_count = v2 identity 内的 attempt number
```

第一次 v2 运行是 1，不是把 v1 attempt 4 变成 5。`prior_safe_verdicts` 不包含 v1 `INCONCLUSIVE`。

## 8. 旧数据和审计链

v1 attempt 4 保持：

- 原 comparison directory；
- 原 metadata、records、checkpoints、journal、terminal；
- 原 terminal SHA；
- 原 `INCONCLUSIVE` 语义。

实现和运行期间禁止：

- 移动或复制 v1 samples 到 v2；
- 修改 v1 terminal；
- 删除旧 comparison directory；
- 把 v1 terminal 当作可恢复 attempt；
- 在 v2 evidence 中声称 v1 已通过。

设计文档记录的 terminal SHA 是 v1→v2 的审计依据；v2 evidence 通过 schema/protocol version 明确表明它采用新协议。

## 9. 错误模型

| 条件 | 结果 |
|---|---|
| v2 metadata/schema/protocol 不一致 | `INCOMPLETE_EVIDENCE` |
| v1 metadata 被当作 v2 resume | `INCOMPLETE_EVIDENCE` |
| 其他 closed gates 全部通过，且 direct metric 冲突、combined 仍在 ±3% | `INCONCLUSIVE` |
| combined closed gate 明确失败 | `CANDIDATE_FAILED` |
| inactive metric 方向冲突 | 仅 diagnostic，不改变 verdict |
| 样本/轮号/AB-BA/identity 不完整 | `INCOMPLETE_EVIDENCE` |
| reference/机器漂移 | 既有 `REFERENCE_FAILED` / `ENVIRONMENT_DRIFT` |
| workbook correctness 失败 | `CORRECTNESS_FAILED`，停止性能判定 |
| typed evidence schema/path 在发布前不一致 | `INCOMPLETE_EVIDENCE` |
| evidence 写入、重建校验或 staged 敏感扫描失败 | `SENSITIVE_EVIDENCE`；随后仍执行既有 cleanup |

## 10. 测试契约

### 10.1 Protocol identity

- v1/v2 对相同输入和 binary 产生不同 batch ID；
- v1/v2 comparison key 不同；
- v2 comparison 从 attempt 1 开始；
- v1 metadata 仍可读取，未知版本拒绝；
- CLI 不存在 protocol version 选择参数。

### 10.2 Direction decision

- active wall conflict + combined decisive pass → 继续并通过；
- active wall conflict + combined near boundary → `INCONCLUSIVE`；
- active wall conflict + combined decisive fail → `CANDIDATE_FAILED`；
- active wall near-boundary conflict + PWS/direct/composite/stage 任一明确失败 → `CANDIDATE_FAILED`；
- inactive PWS conflict 在 Phase 0B 只记录；
- PWS direct gate profile 的 near-boundary conflict → `INCONCLUSIVE`；
- 全部 closed gates 通过后，wall/PWS 都 active 时任一不确定即 `INCONCLUSIVE`；
- first 或 second ratio 恰好等于 1 时不构成方向冲突；
- no conflict 行为与既有 gate 相同；
- 不得产生 round 11 或第三组。

### 10.3 Evidence

- v1 exact payload round-trip 不变；
- v1 拒绝 v2 extra keys；
- reader/rebuilder 可以重建 v1，但正式 writer/publisher 拒绝新建或发布 v1；
- v1 rebuild 不写回历史路径、不生成新 artifact；
- v2 exact payload round-trip；
- v2 diagnostics 固定顺序、计算一致；
- N=5 diagnostics 为空，N=10 恰好两项；
- v2 artifact filename 包含 protocol identity；
- staged evidence scanner 和 batch marker 接受 v2，并继续拒绝敏感路径、未知字段和 orphan artifact。

### 10.4 历史保护

测试使用临时 v1 ledger 构造已封存 `INCONCLUSIVE`，随后运行 v2：

- v1 terminal bytes/SHA 不变；
- v2 写入不同 comparison directory；
- v2 attempt number 为 1；
- v2 不读取 v1 sample payload。

## 11. 运行顺序和停止条件

实现通过测试和独立审查后：

1. 重新确认 worktree clean；
2. 重新确认 approved manifest、reference EXE、candidate EXE SHA 不变；
3. 重新确认 v1 attempt 4 terminal SHA 不变；
4. 计算 v2 GB typed evidence basename；
5. 只运行一次 v2 GB paired batch；
6. GB 非 `VALIDATED`：封存并停止，不运行 SK；
7. GB `VALIDATED`：计算 v2 SK basename，只运行一次 SK；
8. SK 非 `VALIDATED`：封存并停止；
9. GB/SK 均 `VALIDATED`：执行 staged evidence scan，代码提交与 evidence 提交继续分离；
10. 不创建 PR。

没有任何条件允许相同 protocol version + comparison identity + candidate SHA 因 `INCONCLUSIVE` 或 `CANDIDATE_FAILED` 再采。

## 12. 伪代码草案

### 12.1 输入

- `request`: pipeline、profile、固定 binary、Phase 0A manifest、evidence 目标；
- `identity`: input/reference/candidate/Git/repository/machine hashes；
- `wall_first`, `pws_first`: global round 1–5；
- `wall_second`, `pws_second`: 仅在 mandatory expansion 后存在的 round 6–10；
- `limits`: 当前 profile/pipeline 的关闭门槛。

### 12.2 输出

- 成功：v2 `PairedBenchmarkResult` 和 schema v2 sanitized evidence；
- 临界方向冲突：`INCONCLUSIVE`，不发布 evidence；
- 明确门禁失败：`CANDIDATE_FAILED`，不发布 evidence；
- 结构/身份/清理失败：既有结构化 verdict；
- 不存在自动重试或降级结果。

### 12.3 主流程

```python
# [伪代码草案]
PAIRED_PROTOCOL_VERSION = 2


def run_paired_normal_batch_v2(request):
    # 版本固定在代码中，避免操作者按结果选择规则。
    identity = capture_and_validate_identity(request)
    batch_id = derive_batch_id(PAIRED_PROTOCOL_VERSION, request, identity)
    comparison_key = derive_comparison_key(PAIRED_PROTOCOL_VERSION, request, identity)
    ledger = create_v2_ledger(comparison_key, protocol_version=2, identity=identity)

    wall_first = capture_wall(rounds=range(1, 6), ledger=ledger)
    pws_first = capture_pws(rounds=range(1, 6), ledger=ledger)
    validate_same_batch_and_phase0a_drift(wall_first, pws_first)
    first_sha = ledger.commit_first_group(wall_first, pws_first)

    if not requires_joint_expansion(request, wall_first, pws_first):
        # N=5 没有两个子组，因此没有方向 diagnostic。
        evaluate_closed_profile(request, wall_first, pws_first)
        return publish_v2_evidence(direction_diagnostics=())

    # 触发后无条件追加，轮号延续为 6–10。
    wall_second = capture_wall(rounds=range(6, 11), ledger=ledger, first_sha=first_sha)
    pws_second = capture_pws(rounds=range(6, 11), ledger=ledger, first_sha=first_sha)
    ledger.commit_expanded_group(wall_second, pws_second, first_sha=first_sha)

    # join 只验证结构并合并，不能在这里沿用 v1 的一律冲突 veto。
    wall = join_metric_groups(wall_first, wall_second)
    pws = join_metric_groups(pws_first, pws_second)
    diagnostics = build_direction_diagnostics(request, wall_first, wall_second, pws_first, pws_second)

    # 明确失败优先；任何 direct/composite/stage gate 失败都不能被冲突改写。
    evaluate_closed_profile(request, wall, pws)

    for diagnostic in diagnostics:
        if (
            diagnostic.directions_conflict
            and diagnostic.direct_gate != "none"
            and diagnostic.near_boundary
        ):
            # 只有 active direct metric 在 N=10 后仍靠近门槛才无法判定。
            raise HarnessFailure("INCONCLUSIVE", "v2 active metric remains direction-conflicted near limit")

    # closed gates 全部通过且不存在 active near-boundary conflict 才发布。
    return publish_v2_evidence(direction_diagnostics=diagnostics)
```

### 12.4 Evidence schema

```python
# [伪代码草案]
def rebuild_benchmark_manifest_payload(value):
    if value.schema_version == 1:
        require_protocol_v1_without_v2_fields(value)
        return build_exact_v1_payload(value)

    if value.schema_version == 2:
        require(value.protocol_version == 2)
        validate_direction_diagnostics(value.rounds, value.direction_diagnostics)
        return build_exact_v2_payload(value)

    raise ValueError("unknown benchmark evidence schema")


def build_benchmark_manifest_for_publication(value):
    # v1 只允许历史 reader/rebuilder 验证，正式发布永远单写 v2。
    if value.schema_version != 2 or value.protocol_version != 2:
        raise ValueError("formal publisher accepts protocol v2 evidence only")
    return rebuild_benchmark_manifest_payload(value)
```

### 12.5 边界条件

```python
# [伪代码草案]
def direct_metric_gate(metric, limits):
    ratio_key, absolute_key = {
        "wall": ("wall_ratio", "wall_seconds"),
        "pws": ("pws_ratio", "pws_bytes"),
    }[metric]

    present = [key for key in (ratio_key, absolute_key) if key in limits]
    if len(present) > 1:
        raise RuntimeError("one metric cannot have two direct direction gates")
    if not present:
        return None
    return present[0], limits[present[0]]
```

## 13. 实现文件边界

预计只修改 benchmark/evidence 支撑层及测试：

- `tests/rust_oracle/benchmark_protocol.py`；
- `tests/rust_oracle/phase0_harness.py`；
- `tests/rust_oracle/evidence.py`；
- `tests/rust_oracle/test_benchmark_protocol.py`；
- `tests/rust_oracle/test_phase0_harness.py`；
- `tests/rust_oracle/test_evidence.py`；
- 原持续性能设计第 10.5 节的 v2 修订说明；
- 后续独立 implementation plan。

不修改 Rust、Cargo、批准 baseline 或生成中的 Phase 0B evidence。

## 14. 验收

设计实施完成必须同时满足：

- protocol version 在 batch/comparison/ledger/evidence/artifact filename 五个边界显式闭合；
- v1 只读兼容和 terminal SHA 保护测试通过；
- v2 裁决表全部有 RED→GREEN 测试；
- benchmark evidence v1/v2 exact schema 测试通过；
- 完整 `tests/rust_oracle`、Ruff、format、diff/EOL 检查通过；
- Python reviewer 与 doc/evidence reviewer 无 Critical/Important；
- 两份固定 EXE 和 approved manifest SHA 不变；
- v2 GB 只运行一次；GB 通过后 v2 SK 只运行一次；
- 成功 evidence 通过 staged sanitizer 后独立提交；
- 不创建 PR。

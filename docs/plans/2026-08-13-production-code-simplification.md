# 生产代码深度简化计划

- 状态：Completed（实际结果见 `../changes/2026-08-13-production-code-simplification.md`）
- 日期：2026-08-13
- 依据：用户指令「简化整个项目深度简化」+ `code-simplification` skill（用户级已安装）

## 目标

对 Rust 全部生产代码做激进深度简化：降低复杂度、提升可读性，同时保持行为完全不变。不是追求更少行数，而是让新成员更快理解每个模块。完成后实际结果记录到 `../changes/`。

## 范围

**做**：Rust 4 个 crate 的全部生产代码，按模块分批。

**不做**：

- 测试文件（`*_tests.rs`）——它们是「行为未变」的证明，动测试会破坏验证能力
- Python 验证工具（`tests/`、`tools/`）
- 文档（`docs/`，CLAUDE.md 的调整已单独完成）
- 性能优化——简化时发现的瓶颈候选只记录，不与简化改动混在同一 commit（见「简化与优化的边界」）

## 永不动（硬红线）

- 稳定接口：`application::execute(RunRequest) -> RunOutcome`、`process_workbook` 签名与语义
- CLI 参数、默认输入发现、默认输出命名、禁止覆盖、同路径拒绝、check-only 行为
- 成功/失败 JSON、错误码、`retryable`
- 三张 Sheet 的名称、顺序、字段顺序、值、样式和勾稽
- `RunManifestV1` 字段、路径脱敏、SHA-256、原子发布
- config TOML/schema 字段
- error_log 类别、Decimal 语义、异常阈值（2.5/3.5）、白名单顺序
- crate 依赖方向（CLI → core/xlsx；xlsx → core 模型；core 无外部依赖）
- 不新增生产依赖
- `tests/contracts/baselines/` 一个字节不变

## 激进深度允许的操作

- 提取/合并辅助函数、卫语句替换深嵌套
- 内部标识符重命名（不碰对外 JSON 键、字段名、错误码文本）
- 删死代码、去重复逻辑、移除多余抽象层
- 模块内函数/结构重排
- crate 内文件合并/拆分

**原则**：任何「不确定行为是否完全一致」的简化，跳过并记录，绝不猜。

## 简化与优化的边界

- 简化保持行为与性能双重不变；优化不在本计划内（仓库规则：没有可复现瓶颈就不优化）
- 简化过程中发现的疑似瓶颈：记录位置、证据（阶段耗时/分配观察）与预期收益，不顺手修改
- 全部批次完成后，将候选清单交付用户，由其决定是否另开优化阶段（走「可复现瓶颈 → 跨版本正确性 → 8 对交错配对 → changes/ 记录」流程）

## 批次计划

| 批 | 模块 | 备注 |
|---|---|---|
| 1 | core: presentation.rs (924 行) | 检查点批 |
| 2 | core: normalize.rs (842) | |
| 3 | core: table.rs (813) | |
| 4 | core: fact.rs (614) | 热路径，加性能护栏 |
| 5 | core: anomaly.rs (733) | |
| 6 | core: quality/scoring/split/pipeline/process/model/sheet_contract/timing 小文件 | 打包一批 |
| 7 | cli: manifest.rs (794) | |
| 8 | cli: run/run_paths/application/config/args 其余 | 打包一批 |
| 9 | xlsx: writer.rs (698) | 热路径，加性能护栏 |
| 10 | xlsx: reader/snapshot/atomic_file | 热路径，加性能护栏 |
| 11 | costing-oracle-tests (89 行) | 大概率不动 |

顺序理由：core 业务模块复杂度最高、模式可复制性最好；cli 编排层其次；xlsx 性能敏感区最后，用前面批次跑顺的手感去做。

## 每批执行流程

1. 理解先行（Chesterton's Fence）：读模块 + 其测试 + `docs/contracts/workbook.md` + git blame
2. 产出简化清单（每项：位置、改法、理由、是否碰行为）
3. 逐项改，每项后跑 `cargo test -p <crate>` 快速反馈
4. 批次全部门禁：
   - `cargo fmt --manifest-path rust/Cargo.toml --all --check`
   - `cargo clippy --locked --manifest-path rust/Cargo.toml --workspace --all-targets --all-features -- -D warnings`
   - `cargo test --locked --manifest-path rust/Cargo.toml --workspace --all-features`
   - `uv run python -m pytest tests -q -m "not slow and not benchmark" --basetemp .pytest-tmp/python-validation`
5. 热路径批（4/9/10）额外：用 `tools/validation/measure_paired_release.ps1` 做基线/候选至少 8 对交错配对（固定双边预热、奇偶反序、独立输出、PWS 轮询）
6. 全部批次完成后：用 `tools/validation/measure_release.ps1` 对 GB/SK 各跑 N=5 最终硬门禁，对照 `docs/performance/README.md` 当前阈值（GB wall ≤ 3.2554s / PWS ≤ 375,700,685 B；SK wall ≤ 20.0s / PWS ≤ 2,147,483,648 B）

## 检查点与提交策略

- 第 1 批（presentation.rs）完成后暂停：展示简化清单、前后统计、关键前后对比片段、门禁结果；用户认可模式后，后续批次不停顿，每批只报告摘要
- 一个模块一个 commit，Conventional Commits 风格（如 `refactor(core): simplify presentation`）
- 本地门禁全绿后直推 main（用户批准，利用 admin 豁免）
- 每批独立 commit → 任何问题 `git revert` 单批回滚
- 批次门禁不过：批内修复；修不动则整批放弃（逐项回退），不硬推

## 性能护栏前提

- 需要本机 `data/raw/gb`、`data/raw/sk` 真实样本（或相应环境变量），`measure_paired_release.ps1` / `measure_release.ps1` 以 normal mode 跑真实输入
- 执行时先检查；缺失则护栏降级为「跳过基准并说明原因与风险」（符合 AGENTS.md 门禁降级规则）

## 验收条件

- 全部 11 批完成，每批门禁全绿
- `tests/contracts/baselines/` 无任何 diff（纯重构不得修改 baseline）
- 热路径批次交错基准无回退；最终 SK normal-mode N=5 达到 `docs/performance/README.md` 门禁
- 每个 commit 的 diff 干净、可审，无无关改动混入
- 交付简化中发现的优化候选清单（含证据），供用户决定后续是否另开优化阶段

## 风险

- **行为漂移**：激进深度下的最大风险。缓解：每项后跑测试、契约门禁、不确定就跳过
- **性能回退**：热路径简化可能退化（如去掉的「冗余」实际是优化）。缓解：护栏批次交错基准 + 最终 N=5
- **过度简化**：skill 的 Maintain Balance 原则——不为行数简化、不删有意义抽象
- **main 直推无 CI**：利用 admin 豁免绕过双平台 CI。缓解：本地门禁完整覆盖 fmt/clippy/test/pytest；若后续发现平台差异，补跑 CI

## 验证方式

- 每批：fmt + clippy + cargo test + pytest 契约
- 热路径批：交错 A/B 基准
- 最终：SK normal-mode N=5 + 全部门禁 + baseline diff 检查

# Rust 性能与内存门禁

## 硬门禁

正式比较统一使用 release profile、真实输入、normal mode、独立进程和 N=5 中位数。
以下门禁来自 Windows 基线，性能脚本仍通过 PowerShell 7 在 Windows 执行。Linux 使用独立的配对测量和峰值 RSS（进程实际占用物理内存的峰值）口径；最终 GB/SK 真实数据采纳仍需本机验收。

| Pipeline | Wall 中位数 | Peak Working Set 中位数 | 最大输出大小 |
|---|---:|---:|---:|
| GB | `<= 3.2554s` | `<= 375,700,685 bytes` | `<= 4,194,321 bytes` |
| SK | `<= 20.0s` | `<= 2,147,483,648 bytes` | `<= 48,658,823 bytes` |

同时必须满足：

- workbook、运行摘要、质量、error log 和 CLI 契约与冻结基线一致；
- 单元格 `1e-9`、列累计 `1e-8` 数值容差不变；
- 输出路径每轮独立且事先不存在；
- 真实输入、workbook、绝对路径和主机信息不进入版本库。

## 当前实现边界

- release profile 固定 `codegen-units = 1`。
- Calamine `0.38.0` 读取 workbook。
- reader 对有限、无小数且安全落在 `i64` 范围内的浮点值直接构造 `Decimal`；
  其他值保留字符串解析回退。
- `CellValue::Text` / `DateLike` 使用 `Arc<str>`，clone 时共享文本分配；
  不启用全局或按列文本驻留池。
- 默认启用 `low-memory`；单张 Sheet 达到 `5,000,000` cell slots 时切换。
- low-memory 临时目录位于最终输出目录，禁止回退系统临时目录（如 Windows `%TEMP%` 或 Linux `/tmp`）。
- writer 预计算列行为并跳过空白单元格。
- `rust_xlsxwriter 0.102.0` 使用 `rust/vendor/rust_xlsxwriter/` 受控源码，保留必要 I/O 错误传播；来源与文件哈希见[依赖清单](dependencies/2026-11-08-rust-xlsxwriter-0.102.0.json)。
- 读写共用 ZIP `9.4.2`，配套 quick-xml `0.42.0`、flate2 `1.1.11`、zlib-rs `0.6.8`；以 `rust/Cargo.lock` 为准。
- 普通工作簿沿用 ZIP 后端默认压缩等级 6；工作簿含 low-memory Sheet 时显式设置等级 5。历史降低等级的实验未满足输出大小门禁。

## 复测顺序

先通过正确性：

```powershell
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo clippy --locked --manifest-path rust/Cargo.toml --workspace --all-targets --all-features -- -D warnings
cargo test --locked --manifest-path rust/Cargo.toml --workspace --all-features
uv run python -m pytest tests -q -m "not slow and not benchmark" --basetemp .pytest-tmp/python-validation
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate
uv run python tools/ci/run_synthetic_e2e.py --binary rust/target/release/costing-calculate.exe
```

再完成基线/候选真实 workbook 比较，使用
`tools/validation/measure_paired_release.ps1` 做至少 8 对交错配对，最后运行 N=5。

`tools/validation/measure_paired_release.ps1` 固定双边预热、奇偶反序、独立输出、PWS 轮询、
阶段耗时、业务摘要、临时残留和输入/二进制哈希复核；
`tools/validation/measure_release.ps1` 用于最终 N=5 硬门禁。

```powershell
.\tools\validation\measure_release.ps1 `
  -BinaryPath .\rust\target\release\costing-calculate.exe `
  -Pipeline gb `
  -InputPath <gb-real.xlsx> `
  -OutputDirectory .\.pytest-tmp\perf-gb `
  -Iterations 5 `
  -ReportPath .\.pytest-tmp\perf-gb.json

.\tools\validation\measure_release.ps1 `
  -BinaryPath .\rust\target\release\costing-calculate.exe `
  -Pipeline sk `
  -InputPath <sk-real.xlsx> `
  -OutputDirectory .\.pytest-tmp\perf-sk `
  -Iterations 5 `
  -ReportPath .\.pytest-tmp\perf-sk.json
```

脚本输出不含输入路径，但输出 workbook 和原始本地报告仍只保存在忽略目录。

## Linux / Colab 离线调优

同一套 Python 验证工具在 Linux 本机和 Colab 免费 CPU 环境运行，业务计算始终调用 Rust 程序。
[notebook（交互式笔记本）](../../tools/validation/colab_autotune.ipynb)负责环境准备、顺序调用和报告下载；
[实验驱动](../../tools/validation/autotune_experiment.py)构建基线、未改动对照和最多 6 个独立候选。
每个候选先比较结果，再执行正式测量；入围最多 2 项。允许预登记最多 1 个组合，只有组件均通过完整保护时才测量；组合同时比较原基线和最佳单项。

候选支持源码、profile（编译配置）、精确依赖变更及受控 fork 源码载荷。依赖候选必须列出实际改变的 Cargo 键和完整锁包条目；fork 按逐文件哈希、固定来源和许可证校验。业务源码、实际执行的验证工具与门槛分别冻结。

GB/SK 合成集覆盖小型正确性、中型处理、低于/高于 `5,000,000` cell slots 的边界、月份过滤、空结果和独立验证种子。
样本使用合成产品配置，分析页非空，并检查预期运行计数、质量计数和低内存标记。
跨版本比较同时验证 workbook、业务摘要和 Manifest；时间与构建身份单独记录。实际输出还核验 Sheet 行列数、月份分布、分析页非空和阈值两侧的切换。大型正常月份场景额外改变 XML 末尾空白后逐单元格比较，避免只覆盖字节相同路径；原全月脏数据场景保留。

### 准备允许文件包

先冻结本地源码清单、基础提交、固定构建时间及候选补丁，再从仓库根目录运行：

```sh
uv run python -m tools.validation.prepare_colab_bundle \
  --candidate-manifest <candidates.json> \
  --baseline-metadata <baseline.json> \
  --output-dir <new-bundle-directory>
```

输出 `colab-source.zip` 和 `bundle-review.json`。包只收集允许的源码、配置、验证工具和补丁；
不会包含 `data/raw/`、`data/processed/`、`.claude/`、Git 元数据、编译产物或真实运行报告。
默认配置和测试基线含业务标识，仍须在本机审阅，再决定上传范围。生成工具只准备文件。
Notebook 通过本机审阅记录中的独立 SHA-256 核对上传包，再安全解压；真实 ERP 输入输出始终留在本地。

本地使用已审阅并解压的包时，进入其 `source` 目录，把 Python 环境放在包外：

```sh
export UV_PROJECT_ENVIRONMENT=/absolute/path/to/autotune-venv
export PYTHONDONTWRITEBYTECODE=1
uv sync --frozen --extra dev
uv run --frozen python -m tools.validation.autotune_experiment \
  --bundle-root /absolute/path/to/extracted-bundle \
  --work-dir /absolute/path/to/new-experiment \
  --rust-tests
```

Rust `1.96.0`、锁定依赖、默认功能与 `codegen-units = 1` 保持固定。各候选使用独立源码和 binary（可执行文件）目录，
构建缓存共享；每次构建后立即复制程序并记录哈希。编译和样本生成完成后才进入对应测速阶段。
`--no-rust-tests` 仅用于诊断；跳过 Rust 测试的结果不会成为推荐项。

### 配对测量与判定

[Linux 测量工具](../../tools/validation/measure_paired_release.py)通过轻量辅助进程对实际 CLI 使用 `wait4`，
避免父 Python 进程占用及先前子进程峰值污染统计；输出文件独立捕获，超时、内存、磁盘超限均使测量无效。
原始数据及报告格式见 [Linux 测量 schema](../../tools/validation/measure_paired_release.linux.schema.json)。

- 基线对基线先校准 4 对：端到端配对差中位数的绝对值小于 1%，各对绝对差的中位数不超过 2%；目标阶段小于 5%。
- 正式测量至少 8 对，双边预热，奇偶反序；先逐对计算改善率，再取中位数。噪声校准最多尝试两次，未通过就终止排名。
- 主场景改善至少 5%，至少胜出 6/8 对；其他场景回退不超过 2%。内存同时满足逐对峰值 RSS 改善中位数不低于 −5%，以及候选峰值中位数/基线峰值中位数不超过 1.05；合成最大输出不超过基线的 102%。
- 目标阶段范围开测前冻结，`export` 自动包含 `writer_populate`、`xlsx_save`；范围外基线中位数达到 50ms 的阶段回退超过 5% 时拒绝候选。
- 组合对原基线满足 5% 门槛，对最佳单项额外改善至少 2%、胜出至少 6/8 对；完整保护集也须通过。
- 未改动对照异常通过、正确性失败、资源超限、身份变化或预算耗尽时，不产生相应推荐。
- 报告逐对原子保存，成功验证的 workbook 默认清理，保留摘要、哈希与大小；`--keep-outputs` 可保留工作簿。

单独验证基线噪声：

```sh
uv run python -m tools.validation.measure_paired_release \
  --baseline-binary <baseline> --candidate-binary <baseline> \
  --pipeline gb --input <local-input.xlsx> \
  --output-dir <new-calibration-directory> \
  --pairs 4 --calibration --purpose calibration --target-stage xlsx_save
```

比较候选时把 `--candidate-binary` 换成候选程序，使用 `--pairs 8 --purpose screening` 并移除 `--calibration`。
有合成配置时，两边统一通过 `--config` 传入；`--mode check-only` 用于只计算场景。
`--resume` 只允许同一会话内、相同程序/输入/配置及选项的未完成测量恢复；整个实验在新会话中建立新目录并重新校准。

`measure_paired_release`、`compare_releases` 和实验驱动均支持 `--audit-mode manifest|none`，默认 `manifest`。`none` 实际省略 `--summary-output`，在计时外验证程序、输入、配置、业务摘要及产物；无法从运行中观测的 Manifest 字段保留 `null`。实验驱动额外重放审计模式，两种模式分别校准和计时。

配对与比较报告记录 `purpose`（用途）：`screening` 初筛、`acceptance` 验收、`diagnostic` 诊断、`calibration` 校准。诊断结果不能进入推荐；切换用途或审计模式不能恢复原批次。比较器的 CLI 和完整工作簿比较子进程均有超时、内存及磁盘上限。

最终 Linux 独立 N=5 使用 `autotune_experiment.run_acceptance`：要求已有 ≥8 对合格入围证据及相同源码包、工具和二进制身份，先验证真实输入和审计重放，再单独校准及每侧五次测量；主场景仍须改善 ≥5%、胜出 ≥4/5。Windows 继续使用原 PowerShell 门禁。

Colab 默认预算为 21,600 秒，只处理合成数据，手动下载报告，不自动使用 Google Drive 或付费资源。
云端筛选通过后仍需目标电脑真实 GB/SK 正确性、性能和 N=5 复测；缺少真实 SK 时，整体采纳状态保持未验证。
前一轮工具与候选结论见[离线调优实施记录](../changes/2026-11-08-colab-autotuning-tooling.md)，当前读写、依赖候选与验收状态见[Excel I/O 优化记录](../changes/2026-11-08-xlsx-io-optimization.md)。完整门槛见[Excel I/O 方案](../plans/2026-11-08-xlsx-io-optimization.md)。
2026-11-08 的 Linux 本地首轮中，64 KiB 缓冲候选改善 -0.4598%，完整 LTO（链接时优化）候选改善 +2.2797%，
均为 8 对的逐对改善率中位数，均未达到 5% 门槛；首轮入围 0 项，生产版本保留原实现。

本轮 Excel I/O 优化实验中，借用读取、zlib-rs 补丁升级及待激活组合通过构建和基础正确性验收；两个独立候选的 22 项规模/种子/模式保护比较、真实 GB 的带/不带 Manifest 共 4 项比较均为零差异，输出大小也满足 102% 上限。两个 CPU 集合各两次基线自比校准均未达到噪声门槛，因而没有候选 8 对或 N=5 速度数据，该优化任务结束时未产生性能采纳结论。不能把校准自比偏差作为优化收益。

随后实施的[上游维护更新](../changes/2026-11-08-upstream-refresh.md)将正式读写依赖更新到本页当前版本，保留业务和工作簿契约；维护更新的正确性验收与速度收益分别记录。

## 2026-07-28/29 采纳结论

本轮多候选实验与正式采纳已经完成：

- reader 安全整数快路径已在提交 `26f29e3` 采纳；
- `CellValue::Text` / `DateLike` 使用 `Arc<str>` 的 T3-A 已在提交 `6d0f274` 采纳；
- `zmij`、ZIP Level 4、Thin LTO、forward-fill 和有界按列文本驻留均被拒绝；
- 最终采纳栈相对原始基线的 SK normal 8 对中，wall 改善 `6.1896%`、
  PWS 下降 `24.9597%`，两项均赢 `8/8`；
- 正式组合分支的 GB/SK N=5、真实 workbook、单月过滤和 Windows 包 smoke 均通过。

完整脱敏证据见
[`../changes/2026-07-28-sk-performance-experiments.md`](../changes/2026-07-28-sk-performance-experiments.md)。
架构取舍见
[`../decisions/2026-07-29-cell-value-arc-text.md`](../decisions/2026-07-29-cell-value-arc-text.md)。

## 优化规则

- 没有可复现瓶颈就不优化。
- 优化必须先通过跨版本正确性，再做交错配对。
- 正式优化实验至少 8 对，报告中同时记录 wall、PWS、输出大小和环境限制。
- 无论采用或拒绝，都在 `docs/changes/` 留下脱敏结果。
- 不得通过改用 dev profile、放宽阈值、放宽容差或更新基线来获得通过。

## 历史证据

- v0.2.0 N=5：[`../changes/2026-07-12-rust-performance-validation.md`](../changes/2026-07-12-rust-performance-validation.md)
- ZIP 压缩拒绝实验：[`../changes/2026-07-25-v0.2.0-m6b-zip-compression.md`](../changes/2026-07-25-v0.2.0-m6b-zip-compression.md)
- 2026-07-28 多候选 SK 优化实验：[`../changes/2026-07-28-sk-performance-experiments.md`](../changes/2026-07-28-sk-performance-experiments.md)
- 冻结 JSON：`docs/performance/baselines/`
- 依赖来源：`docs/performance/dependencies/`

`docs/superpowers/` 中的历史协议全程只读，不恢复为当前默认流程。

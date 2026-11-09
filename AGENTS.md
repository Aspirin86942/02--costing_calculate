# Costing Calculate 项目规则

本文件是项目级提示词。当前用户明确指令优先；实际代码、测试和配置输出优于文档。

## 项目边界

- Rust 是唯一正式业务实现。新增或修复业务能力默认写入 `rust/`。
- 支持 Windows 和 Linux；Linux 使用原生 Bash 和无 `.exe` 后缀的可执行文件，公共验证不得强制依赖 PowerShell。Windows 发布包仍按现有 ZIP 流程验收。
- Python 只用于工作簿比较、合成输入和发布验收，不得重新建立第二套业务管线或生产入口。
- 不新增生产依赖，除非用户明确批准。
- `data/raw/` 中的 ERP 输入、`data/processed/` 输出和本地验收产物都视为敏感数据，不得提交。

## Rust 模块与依赖

- `rust/crates/costing-cli`：参数、配置、路径、运行编排、控制台 JSON 和 `RunManifestV1`。
- `rust/crates/costing-core`：全部内存业务计算；公开入口是 `process_workbook`，必要领域类型和错误类型除外。
- `rust/crates/costing-xlsx`：Excel 读取、标准/low-memory 写出和原子发布。
- `rust/crates/costing-oracle-tests`：独立运行契约验证。
- 依赖方向固定为：CLI 可依赖 core/xlsx；xlsx 可依赖 core 模型；core 禁止依赖 CLI、路径发现、环境变量或 Excel 实现。
- 保持 `application::execute(RunRequest) -> RunOutcome` 稳定。CLI 不得重新逐个编排 core 内部步骤。
- 不为整理而新增 crate、空 trait、转发层或重复模型；优先深模块和小接口。

详细说明见 `docs/architecture.md`。

## 兼容边界

除明确批准的业务变更外，必须保持：

- CLI 参数、默认输入发现、默认输出命名、禁止覆盖、同路径拒绝和 check-only 行为；
- 成功/失败 JSON、错误码和 `retryable`；
- 三张 Sheet 的名称、顺序、字段顺序、值、样式和勾稽；
- GB/SK 独立成本项、产品白名单顺序、异常阈值和 Decimal 语义；
- `RunManifestV1` 字段、路径脱敏、SHA-256 和原子发布；
- `5,000,000` cell slots 的 low-memory 触发条件与输出结果。

业务或 workbook 变更前必须先读 `docs/contracts/workbook.md` 和 `tests/contracts/baselines/`。

## 高频业务不变量

- Sheet 顺序固定：`成本计算单总表`、`成本计算单数量聚合维度`、`成本分析工单维度`；不得新增产品维度 Sheet。
- `集成车间`行不得向下填充供应商编码和供应商名称。
- 产品池按“产品编码 + 产品名称”精确匹配，并保持配置顺序。
- 金额使用 Decimal 语义；缺失的本期完工金额在分析中按 0，并记录 `MISSING_AMOUNT`。
- 数量聚合只保留完工数量大于 0 且总完工成本非空的工单。
- `委外加工费`是 GB/SK 独立成本项；`软件费用`只在 SK 作为独立成本项。二者不属于制造费用，但参与各自管线总成本勾稽。
- 制造费用明细勾稽不包含独立成本项。
- 异常池按同产品、同生产类型、同成本指标构建；只有大于 0 的单位成本参与对数与 Modified Z-score。
- 异常阈值固定：`|score| <= 2.5` 正常，`2.5 < |score| <= 3.5` 关注，`|score| > 3.5` 高度可疑。
- 独立成本项不参与异常等级和主要来源判断。

完整字段、错误码和 Manifest 口径以 `docs/contracts/workbook.md` 为当前事实。

## 文档治理

- `docs/superpowers/` 只读：禁止新增、修改、移动或删除。
- 新计划写入 `docs/plans/`，并明确 `Proposed / In Progress / Completed / Superseded`。
- 已经发生的变更和验证写入 `docs/changes/`。
- 重要取舍写入 `docs/decisions/`。
- 每次实施完成后，把最终口径同步到代码、测试、根 README、AGENTS、`docs/README.md`、配置/schema 和当前契约文档；过程文档不能代替当前事实。
- 其他文档只链接权威事实，不复制整段业务规则。

## 开发与验证

正式 Rust 命令统一使用仓库根 `rust-toolchain.toml` 和 release profile：

```powershell
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate
cargo fmt --manifest-path rust/Cargo.toml --all --check
cargo clippy --locked --manifest-path rust/Cargo.toml --workspace --all-targets --all-features -- -D warnings
cargo test --locked --manifest-path rust/Cargo.toml --workspace --all-features
```

Python 验证工具环境（Linux / Bash）：

```sh
uv sync --frozen --extra dev
mkdir -p .pytest-tmp
uv run python -m ruff check tests tools
uv run python -m ruff format tests tools --check
uv run python -m tools.validation.check_markdown_links
uv run python -m pytest tests -q -m "not slow and not benchmark" --basetemp .pytest-tmp/python-validation
uv run python tools/ci/run_synthetic_e2e.py --binary rust/target/release/costing-calculate
```

Windows 使用 `rust/target/release/costing-calculate.exe`，并用 `New-Item -ItemType Directory -Force .pytest-tmp` 创建测试临时目录。非 Windows 环境未安装 `pwsh` 时，仅跳过 Windows 性能脚本自检；Windows 门禁仍必须运行该自检。

GitHub CI（`.github/workflows/ci.yml`）不再随 push / PR 自动运行，只能在 Actions 页面手动运行，并由 `release.yml` 在发布前复用；提交前以上面的本地命令为准。决策见 `docs/decisions/2026-11-08-manual-ci.md`。

跨版本真实 workbook：

```powershell
uv run python -m tools.validation.compare_releases `
  --baseline-binary <baseline.exe> `
  --candidate-binary <candidate.exe> `
  --pipeline gb `
  --input <workbook.xlsx> `
  --output-dir <empty-directory> `
  --report <report.json>
```

数值容差固定为单元格 `1e-9`、列累计 `1e-8`，不得放宽。

## 性能与发布

- release profile 固定 `codegen-units = 1`。
- 默认启用 `low-memory`；临时目录必须位于最终输出目录，禁止回退系统临时目录（如 Windows `%TEMP%` 或 Linux `/tmp`）。
- `CellValue::Text` / `DateLike` 使用 `Arc<str>`，但不建立全局或按列文本驻留池。
- reader 的整数快路径只接受有限、无小数且安全落在 `i64` 范围内的值；其余值必须保留原字符串解析回退。
- `rust_xlsxwriter` 使用 `rust/vendor/rust_xlsxwriter/` 中的受控源码，精确版本为 0.102.0；上游 revision、发布包校验和、必要补丁与完整文件哈希见 `docs/performance/dependencies/2026-11-08-rust-xlsxwriter-0.102.0.json`。保留来源、许可证、CHANGELOG 编译输入及原始 I/O 错误链。
- 性能门禁见 `docs/performance/README.md`。只有测出瓶颈才优化；优化实验使用至少 8 对交错配对并记录到 `docs/changes/`。
- Linux / Colab 的实验入口为 `tools/validation/autotune_experiment.py` 和 `colab_autotune.ipynb`；配对测量使用 `measure_paired_release.py`，Linux 峰值 RSS 与 Windows PWS 分别判定。源码包用 `prepare_colab_bundle.py` 按允许清单生成，上传前审阅内嵌业务配置，真实 ERP 数据留在本地。依赖/fork 候选须精确声明 Cargo、锁包及源码载荷；实际执行工具与业务源码分别冻结。
- 配对与比较支持 `--audit-mode manifest|none`，用途通过 `--purpose` 区分；日常无 Manifest 与审计模式分别校准。Linux 独立 N=5 入口为 `autotune_experiment.run_acceptance`，要求已有合格的 8 对入围证据；诊断结果不能推荐。
- Windows ZIP 必须从干净提交构建，并通过无 Rust/Python child `PATH` 的完整 smoke。
- `RunManifestV1` schema 仍是 V1；应用版本升级不得顺带改 schema。
- 推送正式标签、GitHub Release 或其他外部资产前必须获得明确确认。

## 工作方式

- 先用 `rg` / `rg --files` 定位，再读最小相关内容。
- 保留用户现有改动；不做无关重构、全局格式化或兼容层。
- 文本修改用 `apply_patch`；保持 UTF-8 和现有 CRLF/LF。
- 每改必验；不能运行的门禁必须说明原因和风险。
- 禁止未经授权的 `reset --hard`、`checkout --`、`clean`、强推、递归删除或范围外写入。
- 涉及架构、跨文件、核心业务或调用链时，优先使用 Codebase Memory：先 `list_projects`，再 `search_graph`，需要时 `trace_path`。工具不可用或索引过期时必须标明 state、root、所需 action 和错误，再降级到 Git、Cargo、`rg` 和直接读取。

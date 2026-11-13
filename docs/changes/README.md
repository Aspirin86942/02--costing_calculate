# 实际变更

本目录记录已经落地的实现、验证证据和发布说明。未来目标应写入 `../plans/`，尚未决定的取舍应写入 `../decisions/`。

## 写入规则

- 使用 `YYYY-MM-DD-<slug>.md`，发布说明可保留版本号文件名。
- 至少记录范围、实际结果、验证、剩余风险和回滚边界。
- 性能实验无论采用或拒绝都记录结果，不把实验提案写成已采用。
- 记录完成后，把最终行为同步到当前事实文档和相应契约。

## 当前记录

- [`2026-11-12-streaming-reader.md`](2026-11-12-streaming-reader.md)：XLSX 逐单元格读取 V2/V3 正式筛选与阶段内存诊断；两批均因阶段回退拒绝，生产读取器未变。

- [`2026-11-12-physical-address-capture.md`](2026-11-12-physical-address-capture.md)：物理地址采集链路验证、两批 64 次检测与原始程序交叉检查；本批未捕获真实失配地址。

- [`2026-11-12-runtime-repair.md`](2026-11-12-runtime-repair.md)：分配器与页策略修复对照、基本整数读写失配和修复后复验入口；修复仍受运行环境阻塞。

- [`2026-11-12-baseline-content-localization.md`](2026-11-12-baseline-content-localization.md)：写出前的文字变化定位、Rust/C++ 最小复现与逐单元格读取评估；具体运行层根因未确定。

- [`2026-11-12-full-sk-benchmark.md`](2026-11-12-full-sk-benchmark.md)：完整 SK 新编译与计时校准通过；B1 六对有效数据及两处原始基线内容异常，未形成性能采纳。

- [`2026-11-11-memory-optimization.md`](2026-11-11-memory-optimization.md)：行容量与读写候选、完整 SK 合成保护、独立构建和资源预算验收。

- [`2026-11-09-full-sk-erp-format.md`](2026-11-09-full-sk-erp-format.md)：完整 58 列 SK ERP 格式、材料汇总口径、分析页计数及全量验收。

- [`2026-11-08-upstream-refresh.md`](2026-11-08-upstream-refresh.md)：Excel 读写最新稳定上游、受控本地源码、接口适配及兼容验证。

- [`2026-11-08-xlsx-io-optimization.md`](2026-11-08-xlsx-io-optimization.md)：Excel 读写、压缩与发布的隔离诊断、候选筛选及验收记录。

- [`2026-11-08-disable-automatic-ci.md`](2026-11-08-disable-automatic-ci.md)：关闭 GitHub CI 自动运行，并记录首次远端运行中 Windows 测试失败的原因。
- [`2026-11-08-colab-autotuning-tooling.md`](2026-11-08-colab-autotuning-tooling.md)：离线调优工具、允许文件包、合成验证与本地候选筛选。
- [`2026-11-08-linux-environment.md`](2026-11-08-linux-environment.md)：Linux 原生构建、公共验证适配与 GB / SK 隔离运行验收。
- [`2026-07-12-rust-performance-validation.md`](2026-07-12-rust-performance-validation.md)：Rust 主路径 N=5 验收证据。
- [`2026-07-25-v0.2.0-m0-baseline.md`](2026-07-25-v0.2.0-m0-baseline.md)：v0.2.0 冻结基线。
- [`2026-07-25-v0.2.0-m3-config-governance.md`](2026-07-25-v0.2.0-m3-config-governance.md)：配置治理退出证据。
- [`2026-07-25-v0.2.0-m4-manifest-atomicity.md`](2026-07-25-v0.2.0-m4-manifest-atomicity.md)：Manifest 与原子发布证据。
- [`2026-07-25-v0.2.0-m5-rc-packaging.md`](2026-07-25-v0.2.0-m5-rc-packaging.md)：Windows RC 打包与回滚证据。
- [`2026-07-25-v0.2.0-m6a-cell-text.md`](2026-07-25-v0.2.0-m6a-cell-text.md)：`cell_text` 优化采用证据。
- [`2026-07-25-v0.2.0-m6b-zip-compression.md`](2026-07-25-v0.2.0-m6b-zip-compression.md)：ZIP 压缩实验拒绝证据。
- [`2026-07-25-v0.2.0-final-dod.md`](2026-07-25-v0.2.0-final-dod.md)：v0.2.0 最终完成标准。
- [`2026-07-25-github-main-protection.md`](2026-07-25-github-main-protection.md)：远端主分支保护证据。
- [`releases/v0.2.0-rc.1.md`](releases/v0.2.0-rc.1.md) 与 [`releases/v0.2.0.md`](releases/v0.2.0.md)：版本发布说明。
- [`2026-07-27-documentation-reorganization.md`](2026-07-27-documentation-reorganization.md)：本次文档目录整理记录。
- [`2026-07-27-v0.3.0-cleanup-baseline.md`](2026-07-27-v0.3.0-cleanup-baseline.md)：v0.3.0 全项目整理前冻结基线。
- [`2026-07-27-v0.3.0-rust-architecture.md`](2026-07-27-v0.3.0-rust-architecture.md)：Rust 深模块接口与依赖边界整理。
- [`2026-07-27-v0.3.0-python-retirement.md`](2026-07-27-v0.3.0-python-retirement.md)：Python 业务实现退役与验证工具收敛。
- [`2026-07-27-v0.3.0-governance.md`](2026-07-27-v0.3.0-governance.md)：版本、当前事实文档、项目提示词、CI 和发布流程统一。
- [`2026-07-27-v0.3.0-rc-validation.md`](2026-07-27-v0.3.0-rc-validation.md)：真实 GB/SK、N=5 性能和 Windows RC 包最终验收。
- [`2026-07-28-sk-performance-experiments.md`](2026-07-28-sk-performance-experiments.md)：SK paired 基线、候选采纳/拒绝和最终验证证据。
- [`releases/v0.3.0-rc.1.md`](releases/v0.3.0-rc.1.md) 与 [`releases/v0.3.0.md`](releases/v0.3.0.md)：v0.3.0 发布候选和正式版说明。

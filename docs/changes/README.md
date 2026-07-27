# 实际变更

本目录记录已经落地的实现、验证证据和发布说明。未来目标应写入 `../plans/`，尚未决定的取舍应写入 `../decisions/`。

## 写入规则

- 使用 `YYYY-MM-DD-<slug>.md`，发布说明可保留版本号文件名。
- 至少记录范围、实际结果、验证、剩余风险和回滚边界。
- 性能实验无论采用或拒绝都记录结果，不把实验提案写成已采用。
- 记录完成后，把最终行为同步到当前事实文档和相应契约。

## 当前记录

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
- [`releases/v0.3.0-rc.1.md`](releases/v0.3.0-rc.1.md) 与 [`releases/v0.3.0.md`](releases/v0.3.0.md)：v0.3.0 发布候选和正式版说明。

# 重要决策

本目录保存需要长期解释“为什么这样做”的重要决策。当前代码、测试与事实文档仍是行为真值；决策落地后必须同步这些来源。

## 写入规则

- 使用 `YYYY-MM-DD-<slug>.md`。
- 至少包含状态、日期、背景、决策、影响和被否决或未选择的方案。
- 被新决策取代时保留原文件，标记 `Superseded` 并链接后继记录。
- 依赖、安全、兼容性、数据口径、发布与文档治理等跨期取舍优先记录。

## 当前记录

- [`2026-07-25-toml-sha2-review.md`](2026-07-25-toml-sha2-review.md)：`toml` / `sha2` 生产依赖批准。
- [`2026-07-27-documentation-lifecycle.md`](2026-07-27-documentation-lifecycle.md)：文档分层、只读档案和事实同步规则。
- [`2026-07-27-python-validation-only.md`](2026-07-27-python-validation-only.md)：退役 Python 业务实现，仅保留验证工具。
- [`2026-07-27-rust-deep-module-seams.md`](2026-07-27-rust-deep-module-seams.md)：Rust 四 crate 的深模块接口与依赖方向。
- [`2026-07-29-cell-value-arc-text.md`](2026-07-29-cell-value-arc-text.md)：reader 整数快路径、`Arc<str>` 文本表示和不采用驻留池的取舍。

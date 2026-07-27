# Python 退役计划

- 状态：Completed
- 完成日期：2026-07-27
- 批准来源：v0.3.0 全项目整理计划

Rust GB/SK 正式路径通过冻结基线、真实 workbook 和契约验证后，Python 业务实现已在独立阶段退役。验证能力先提取为独立工具，随后才删除旧入口、业务实现、只保护旧实现的测试和 Phase 0/meta 脚手架。

最终边界：

- Rust 是唯一正式业务实现。
- Python 只保留工作簿比较、合成 GB/SK 输入和发布验收工具。
- 当前分支不保留第二套业务实现；退役代码通过 Git 基线标签恢复。

实际文件范围、验证结果和恢复边界见 [`../changes/2026-07-27-v0.3.0-python-retirement.md`](../changes/2026-07-27-v0.3.0-python-retirement.md)。

# Linux 环境适配

状态：Completed

## 结果

Linux 可以从源码构建并运行 GB / SK CLI，日常公共验证使用 Bash、Rust 和项目 uv 环境。

- CI 矩阵分别指定 Windows `.exe` 和 Linux 原生可执行文件路径，版本身份检查与合成端到端命令使用各平台默认 shell。
- Markdown 链接校验通过 `python -m tools.validation.check_markdown_links` 启动，避免脚本目录中的 `types.py` 遮蔽 Python 标准库。
- pytest 显式将仓库根目录加入导入路径，直接启动 pytest 时也能导入 `tools`。
- 非 Windows 环境缺少 PowerShell 7 时，Windows 性能脚本自检报告跳过；Windows 继续强制执行该自检。
- README、项目规则和当前验证文档提供 Linux 路径、运行命令及测试临时目录的准备步骤。

## 验证证据

环境：Debian GNU/Linux 13.7、`x86_64-unknown-linux-gnu`、Rust `1.96.0`、Python `3.13.18`。
实施起点为提交 `11a4001`；业务源码与该提交相同。

| 验收项 | 状态 | 证据 |
|---|---|---|
| Rust 全功能测试 | 通过 | `cargo test --locked --manifest-path rust/Cargo.toml --workspace --all-features`：212 项通过 |
| Rust 格式与静态检查 | 通过 | `cargo fmt --manifest-path rust/Cargo.toml --all --check`；全工作区、全目标、全功能 clippy，`-D warnings` |
| Python 公共测试 | 通过 | `uv run python -m pytest tests -q -m "not slow and not benchmark" --basetemp .pytest-tmp/linux-validation`：66 项通过，1 项跳过 |
| PowerShell 性能脚本自检 | 未验证 | 当前 Linux 未安装 `pwsh`，测试明确报告跳过 |
| Python lint / 格式 | 通过 | `ruff check tests tools`；`ruff format tests tools --check`，24 个文件符合格式 |
| Linux release 构建 | 通过 | `cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate` |
| 构建身份 | 通过 | `verify_build_identity.py` 验证完整提交 SHA、固定时间 `2024-01-01T00:00:00Z`、编译器和 Linux target |
| GB / SK 合成端到端 | 通过 | `run_synthetic_e2e.py`：配置、默认输入发现、check-only、三张 Sheet、默认中文输出名、禁止覆盖 |
| GB 真实样本 | 通过 | Linux 默认输入发现、正式输出与 check-only 一致；完整字段契约、产品白名单及顺序、明细与数量行数、工单唯一性、SHA-256、禁止覆盖和临时清理均通过独立检查 |
| Linux 隔离运行 | 通过 | 子进程 `PATH=/nonexistent`，GB / SK 含中文与空格的输入、输出和 Manifest 路径均可用 |
| Manifest 与文件保护 | 通过 | SHA-256 匹配、路径脱敏、workbook / Manifest 拒绝覆盖、同输入输出路径拒绝、无临时残留 |
| 当前 Markdown 链接与 diff 空白 | 通过 | `uv run python -m tools.validation.check_markdown_links`；`git diff --check` |

隔离运行的本地合成验收报告：`.pytest-tmp/linux-runtime-smoke.json`。
GB 真实样本的本地验收报告：`.pytest-tmp/gb-real-validation.json`；输入与输出保存在 Git 忽略目录。
真实样本核对中，`work_order_rows` 是产品白名单筛选前的工单数，分析页行数按配置中的产品编码和名称精确筛选后独立核对。
验收二进制：`rust/target/release/costing-calculate`，SHA-256：

```text
0e9c71fa678b49709edc04ecb23e32e451023807e4598f41c8a26266d47b9452
```

## 适用范围与回滚

当前 Linux 实测覆盖 Debian 13 x86_64。GNU/Linux 二进制依赖兼容的系统运行库，其他环境应在目标系统构建或先确认 glibc 兼容性。
Windows 发布包、SK 真实 workbook、Excel / WPS 视觉复核和正式性能门禁在本轮为未验证；性能基线继续使用已有 Windows 证据。
远端 GitHub Actions 首次运行（推送 `60503d1` 后）：Ubuntu 通过；Windows 有 4 项 Colab notebook 测试因平台差异失败，其后的构建、构建身份和合成端到端步骤被跳过，尚未验证。原因与处理见 [关闭 GitHub CI 自动运行](2026-11-08-disable-automatic-ci.md)。

本次可按差异恢复 CI、pytest 配置、平台条件和文档；恢复后重新运行受影响的公共验证命令。

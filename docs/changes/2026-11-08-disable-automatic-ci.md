# 关闭 GitHub CI 自动运行

状态：Completed

决策：[GitHub CI 改为手动运行](../decisions/2026-11-08-manual-ci.md)

## 起因

提交 `60503d1` 推送到 `main` 后，GitHub 运行 `CI`（run `42482893523`）：

| 任务 | 结果 |
|---|---|
| `Public gates (ubuntu-latest)` | 通过 |
| `Public gates (windows-latest)` | `Test public Python suites` 失败：4 失败、145 通过、45 跳过 |

Windows 任务在该步骤失败后，`Build release binary`、`Verify deterministic build identity` 和 `Run synthetic GB/SK end-to-end smokes` 被跳过，这三步在 Windows 上尚未验证。

维护者认为自动 CI 没有必要，要求取消。

## 失败原因

4 项失败都在 `tests/validation/test_colab_notebook.py`，是测试对平台的假设，不涉及 Rust 程序或 workbook 结果。notebook 只在 Colab（Linux）运行，其安全解压和 rustup 下载逻辑没有改动。

| 测试 | Windows 现象 | 处理 |
|---|---|---|
| `test_safe_extract_rejects_unsafe_paths_before_writing[source\\file.txt]` | `zipfile` 在 Windows 把条目名中的 `\` 转成 `/`，路径变合法，未抛出 `ValueError` | 该参数只在非 Windows 运行 |
| `test_rustup_official_download_is_verified_before_execution[True-*]`（3 项） | Windows 文件没有 Unix 可执行权限位，`st_mode & S_IXUSR` 为 0 | 只在非 Windows 检查可执行权限位 |

在 Linux 上把 `os.sep` 设为 `\` 后，`ZipInfo('source\\file.txt').filename` 变为 `source/file.txt`，复现了第一项的原因。

## 修改

- `.github/workflows/ci.yml`：删除 `push` 和 `pull_request` 触发，保留 `workflow_dispatch` 与 `workflow_call`；删除只在 PR 事件运行的冻结基线策略步骤。`release.yml` 未改，仍通过 `uses: ./.github/workflows/ci.yml` 在打包前复用。
- `tests/architecture/test_ci_contract.py`：契约改为“只允许手动运行和被复用，不得出现 `push` / `pull_request` 触发”。
- `tests/validation/test_colab_notebook.py`：按上表处理两处 Windows 假设。
- `AGENTS.md`、`docs/README.md`、`docs/rust_rewrite_validation.md`、两个索引和 `2026-11-08-linux-environment.md` 中“远端运行未验证”的旧说法已同步。
- Rust 源码、`release.yml`、`tools/ci/`、CODEOWNERS 和分支保护没有改动。

## 验证

| 项目 | 结果 | 证据 |
|---|---|---|
| 工作流解析 | 通过 | PyYAML 解析 `ci.yml`：触发只有 `workflow_call`、`workflow_dispatch`，共 15 步，无基线策略步骤；`release.yml` 的 `quality` 仍复用 `ci.yml`，`package` 仍 `needs: quality` |
| Python 公共测试 | 通过 | `uv run python -m pytest tests -q -m "not slow and not benchmark"`：193 通过，0 失败，1 跳过（未安装 `pwsh`，Windows 性能脚本自检跳过） |
| Lint 与格式 | 通过 | `ruff check tests tools`；`ruff format tests tools --check`，33 个文件符合格式 |
| Windows 修复 | 未验证 | 当前环境是 Linux。需在 Windows 本机运行 `uv run python -m pytest tests/validation/test_colab_notebook.py -q`，或推送后在 Actions 页面手动运行 CI |
| Windows 构建、构建身份、合成端到端 | 未验证 | 上次运行中被跳过 |

## 远端状态与后续

- 推送本次修改后，`main` 上不再产生自动 CI。
- `main` 分支保护仍要求 `Public gates (windows-latest)` 和 `Public gates (ubuntu-latest)`（2026-11-08 读取）。管理员直接推送不受影响；通过 PR 合并前需先在 GitHub 设置中移除这两项必需检查。

## 回滚

恢复 `ci.yml` 的 `main` push、PR 触发和 PR 专用基线策略步骤，并同步 `tests/architecture/test_ci_contract.py`。完整说明见决策记录的“恢复”一节。

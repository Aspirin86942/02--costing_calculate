# GitHub Release 标签与已发布附件复验

2026-11-21，`v0.3.1-rc.2` 第一次手动打包执行 checkout 时，远端尚无这个标签。标签创建后，手动任务重跑成功。同一标签的自动发布任务被取消，GitHub 将取消操作也列为错误注记。随后直接针对标签提交 `1b0cdfa` 做独立复验，Windows、Ubuntu 和 Windows 打包三项均通过。[首次打包日志](https://github.com/Aspirin86942/02--costing_calculate/actions/runs/44855559450/attempts/1) · [自动发布记录](https://github.com/Aspirin86942/02--costing_calculate/actions/runs/44855823749) · [标签复验结果](https://github.com/Aspirin86942/02--costing_calculate/actions/runs/44860756507)

`release.yml` 按发布标签串行调度。手动打包前查询完整的 `refs/tags/` 引用，遇到 GitHub 返回 404 时最多等待 60 秒。其他 HTTP 错误直接报告。标签推送按事件的提交打包，并与标签解析出的提交核对。

复验已发布的版本时，工作流从 GitHub 下载常规 ZIP、同名 SHA-256 文件及已提供的 PGO 包，逐包检查构建提交、文件哈希和 GB/SK 运行结果。已经发布的附件由复验路径处理。缺失附件、校验不符或启动失败都会使检查失败。[发布工作流](../../.github/workflows/release.yml) · [Windows 验收脚本](../../tools/release/smoke_package_windows.ps1)

本地执行了工作流中的 PowerShell 步骤，覆盖标签延迟可见、同名分支、权限错误、草稿、单包和双包发布，以及 PGO 附件缺失和 ZIP 损坏等 13 个场景。两个工作流文件均通过 actionlint 检查。

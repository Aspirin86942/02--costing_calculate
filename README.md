# Costing Calculate

Costing Calculate 是 GB / SK 成本核算工作簿处理工具。正式实现是 Rust CLI；发布包是自包含 Windows ZIP，运行时不需要安装 Rust 或 Python。

## 安装

### 使用 Windows 发布包

1. 下载同一版本的 ZIP 和 `.zip.sha256`。
2. 校验 ZIP：

```powershell
$expected = (Get-Content .\costing-calculate-v0.3.0-windows-x86_64.zip.sha256).Split()[0]
$actual = (Get-FileHash .\costing-calculate-v0.3.0-windows-x86_64.zip -Algorithm SHA256).Hash
$expected -eq $actual
```

3. 解压后保留整个目录，不要混用不同版本的可执行文件、配置、schema 和校验文件。
4. 查看版本与帮助：

```powershell
.\costing-calculate.exe --version-json
.\costing-calculate.exe --help
```

### 从源码构建

仓库根目录的 `rust-toolchain.toml` 固定 Rust `1.96.0`：

```powershell
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate
```

生成文件位于 `rust\target\release\costing-calculate.exe`。

## 运行

处理 GB：

```powershell
.\costing-calculate.exe gb --input C:\data\gb-input.xlsx --output C:\data\gb-result.xlsx
```

处理 SK：

```powershell
.\costing-calculate.exe sk --input C:\data\sk-input.xlsx --output C:\data\sk-result.xlsx
```

只检查、不写 workbook：

```powershell
.\costing-calculate.exe gb --input C:\data\gb-input.xlsx --check-only --benchmark
```

按月份过滤：

```powershell
.\costing-calculate.exe sk `
  --input C:\data\sk-input.xlsx `
  --output C:\data\sk-result.xlsx `
  --month-start 2026-01 `
  --month-end 2026-06
```

同时写出可审计 Manifest：

```powershell
.\costing-calculate.exe gb `
  --input C:\data\gb-input.xlsx `
  --output C:\data\gb-result.xlsx `
  --summary-output C:\data\gb-run-manifest.json `
  --redact-paths
```

校验或查看配置，不读取 workbook：

```powershell
.\costing-calculate.exe gb --config .\config\costing.default.toml --validate-config
.\costing-calculate.exe gb --config .\config\costing.default.toml --print-effective-config
```

## 默认输入与输出

省略 `--input` 时，程序扫描：

- GB：`data/raw/gb/gb-*.xlsx`
- SK：`data/raw/sk/sk-*.xlsx`

恰好一个匹配文件时自动使用；没有文件时报 `FILE_NOT_FOUND`；多个文件时报 `INVALID_INPUT`，此时必须显式传入 `--input`。

非 `--check-only` 模式省略 `--output` 时，结果写到：

- GB：`data/processed/gb/<输入名>_处理后.xlsx`
- SK：`data/processed/sk/<输入名>_处理后.xlsx`

月份过滤会在 `.xlsx` 前加入 `_YYYY-MM_YYYY-MM`、`_from_YYYY-MM` 或 `_to_YYYY-MM`。程序不会覆盖已有 workbook 或 Manifest，也拒绝输入和输出指向同一文件。

## 输出内容

正常运行只生成一个处理后 workbook，固定包含三张 Sheet：

1. `成本计算单总表`
2. `成本计算单数量聚合维度`
3. `成本分析工单维度`

质量摘要、异常数量和阶段耗时写到控制台 JSON。只有显式使用 `--summary-output` 时才写 `RunManifestV1`；schema 版本仍为 V1。

完整业务口径见 [`docs/contracts/workbook.md`](docs/contracts/workbook.md)。

## 性能与内存

当前实现对安全整数使用 reader 快路径，并以 `Arc<str>` 保存内部文本 cell，
降低大工作簿处理时的重复 clone 成本和峰值内存；不启用文本驻留池。
这些优化不改变 workbook、CLI 或 Manifest 契约。当前门禁、复测命令和实测结果见
[`docs/performance/README.md`](docs/performance/README.md)。

## 常见错误

| 错误码 | 含义 | 处理方式 |
|---|---|---|
| `FILE_NOT_FOUND` | 输入文件不存在或默认目录没有匹配文件 | 检查路径，或显式传入 `--input` |
| `INVALID_INPUT` | 参数、月份、默认文件数量或输入输出关系不合法 | 查看错误中的 `message` 后修正参数 |
| `INVALID_CONFIG` | 配置不符合 schema 或修改了封闭字段 | 先运行 `--validate-config` |
| `FILE_NOT_READABLE` | workbook 损坏、被占用或无读取权限 | 关闭占用程序并检查文件 |
| `OUTPUT_EXISTS` | workbook 或 Manifest 已存在 | 换新路径；程序不会覆盖 |
| `OUTPUT_NOT_WRITABLE` | 输出目录不可写 | 检查目录权限和剩余空间 |

失败 JSON 会包含稳定的错误码、`message` 和 `retryable`。不要依靠错误文本做自动化分支，优先使用错误码。

## 发布包

维护者从干净提交构建 Windows ZIP：

```powershell
.\tools\release\package_windows.ps1 -ReleaseLabel v0.3.0-rc.1 -OutputDirectory dist
```

验收包：

```powershell
.\tools\release\smoke_package_windows.ps1 `
  -ArchivePath .\dist\costing-calculate-v0.3.0-rc.1-windows-x86_64.zip `
  -ChecksumPath .\dist\costing-calculate-v0.3.0-rc.1-windows-x86_64.zip.sha256 `
  -GbInput <gb-synthetic.xlsx> `
  -SkInput <sk-synthetic.xlsx> `
  -ExpectedReleaseLabel v0.3.0-rc.1 `
  -ExpectedCommit <full-git-sha>
```

该 smoke 会在 child `PATH` 中移除 Rust 和 Python，再验证哈希、固定目录、help、version、配置、GB/SK check-only、正式 workbook 和 Manifest。

开发、架构、验证与文档导航见 [`docs/README.md`](docs/README.md)。

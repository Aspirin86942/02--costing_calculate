# Costing Calculate

Costing Calculate 是 GB / SK 成本核算工作簿处理工具。正式实现是 Rust CLI，支持 Windows 和 Linux。Windows 提供自包含 ZIP；Linux 可从源码构建，生成的程序运行时不需要 Rust 或 Python。

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

仓库根目录的 `rust-toolchain.toml` 固定 Rust `1.96.0`。Linux 构建环境需要 Rust 工具链和 C 链接器（用于生成可执行文件）；在仓库根目录执行：

```sh
cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate
```

生成文件：

- Linux：`rust/target/release/costing-calculate`
- Windows：`rust/target/release/costing-calculate.exe`

Linux 当前验证环境为 Debian 13、`x86_64-unknown-linux-gnu`。建议在实际运行的 Linux 环境构建，或确认目标环境的 glibc（系统运行库）与构建环境兼容。

## 运行

### Linux

在仓库根目录执行。Linux 文件名区分大小写；含空格的路径要加引号。

```sh
./rust/target/release/costing-calculate --version-json
./rust/target/release/costing-calculate --help

# 处理 GB / SK
./rust/target/release/costing-calculate gb --input ./data/raw/gb/gb-input.xlsx --output ./data/processed/gb/gb-result.xlsx
./rust/target/release/costing-calculate sk --input ./data/raw/sk/sk-input.xlsx --output ./data/processed/sk/sk-result.xlsx

# 只检查并输出耗时
./rust/target/release/costing-calculate gb --input ./data/raw/gb/gb-input.xlsx --check-only --benchmark

# 校验内嵌默认配置
./rust/target/release/costing-calculate gb --validate-config
```

其余参数与下方示例一致。Linux 使用 `/` 路径分隔符，多行命令使用 `\` 续行。开发验证见 [`docs/rust_rewrite_validation.md`](docs/rust_rewrite_validation.md)。

### Windows

下面的命令在解压后的发布包目录运行。

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

完整 SK ERP 导出支持前置元数据、58 列双层表头及错误声明为 `A1` 的工作表范围。SK 工单材料金额采用 ERP 直接材料汇总，子项明细保留供核查，差异在数量页和分析页提示复核；输入规则和金额口径见[工作簿契约](docs/contracts/workbook.md)。

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

Excel 读取使用 calamine `0.38.0`，写出使用 rust_xlsxwriter `0.102.0` 受控源码；ZIP/压缩依赖同步更新并由锁文件固定。
上游来源、必要错误处理修补和新旧输出验证见[依赖更新记录](docs/changes/2026-11-08-upstream-refresh.md)。

离线调优工具可在本机 Linux 或 Colab CPU 环境运行：自动构建候选、生成合成数据、比较结果并配对测速。
入口是 [`colab_autotune.ipynb`](tools/validation/colab_autotune.ipynb)，本地命令与验收条件见
[离线调优说明](docs/performance/README.md#linux--colab-离线调优)。源码包包含内嵌业务配置，上传前须审阅；真实 ERP 数据留在本地。
读写与压缩的阶段开销、候选判定及剩余验收见[当前优化记录](docs/changes/2026-11-08-xlsx-io-optimization.md)。

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

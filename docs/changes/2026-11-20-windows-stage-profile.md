# Windows 0.3.1 全流程耗时与 XLSX 体积实测

2026-11-20 在 Windows 原生 release 程序上测量，提交为 `e1c8d2a`，Rust 业务源码与 Linux 0.3.1 构建提交 `1f6ae90` 相同。本次使用默认配置和既有三张表，显式指定输入。工作簿、输入身份和逐次报告留在本机 Git 忽略目录。后续候选见[跨平台性能优化方案](../plans/2026-11-20-cross-platform-performance.md)。

## 正常写出的 N=5 基线

使用现有 Windows 脚本，各场景串行启动五个独立进程，取外部耗时和峰值工作集（PWS）的中位数，以及五次输出中的最大字节数。完整 SK 复用同日、相同二进制和输入的实测记录。[测量脚本](../../tools/validation/measure_release.ps1) · [现行硬门禁](../performance/README.md)

| 输入 | 输入行数 | 端到端中位数 | 峰值内存中位数 | 最大输出字节 | 此批门禁 |
| --- | ---: | ---: | ---: | ---: | --- |
| GB | 61,000 | 0.854 秒 | 358.19 MiB | 4,684,486 | 大小超限 |
| 常规 SK | 467,420 | 2.878 秒 | 719.05 MiB | 43,589,014 | 通过 |
| 完整 SK | 912,921 | 7.274 秒 | 1,754.26 MiB | 75,372,863 | 大小超限 |

GB 的五次 PWS 为 357.26 至 364.18 MiB，中位数距 358.30 MiB 上限约 0.11 MiB。完整 SK 另有五次审计写出，耗时中位数为 7.231 秒，PWS 中位数为 1.761 GiB、最高 1.775 GiB。全量普通与审计十次的受约束 ZIP 成员一致，审计的输入、输出哈希均核对通过。常规 SK 输入哈希与 7 月冻结的 SK 相同，当前 GB 和完整 SK 均与各自管线的 7 月输入不同。绝对大小门禁仍按原值报告。[冻结样本清单](../performance/baselines/2026-07-11-windows-x64-phase0a.json) · [完整 SK 行数参照](2026-11-09-full-sk-erp-format.md)

## 固定的 Windows 对照程序

已将生成上述报告的可执行文件复制到本机 `dist/baselines/windows-x64-0.3.1-e1c8d2a/`，SHA-256 为 `178d628019d2bb5531b7b61432d887bd598ab1767dbc1c73443a2c994f57570b`。同目录的 `baseline.json` 保存构建身份、三份输入与内嵌配置的哈希、N=5 原始值和门禁结果。重新构建候选时，Cargo 输出与这份对照程序分开存放。

用同一份对照程序在完整 SK 普通写出上校准。双方各预热一次，再交错运行四对。端到端逐对差的中位数为 +0.1115%，逐对绝对差的中位数为 0.3052%，目标阶段 `writer_populate` 的差为 −0.3023%。首批通过预定的 1% / 2% / 5% 噪声检查，含预热在内的十份工作簿均通过内容核对。三项数据描述本批运行波动，候选收益需与对照程序同批测量。本地逐次记录保存在 `.pytest-tmp/windows-031-calibration-0fsftjy0/`。[Windows 配对脚本](../../tools/validation/measure_paired_release.ps1) · [校准规则](../performance/README.md)

## 阶段耗时诊断

同一程序对每个输入再做三次普通写出，带 `--benchmark --redact-paths`，省略审计参数。各次输出均检查三表顺序，再与本组三次中的首份逐成员核对。表内各列分别取中位数，单位为秒。CLI 的 `total` 计时在导出前结束，端到端取外部时钟。`writer_populate` 和 `xlsx_save` 是 `export` 的子阶段，低内存填表期间后台压缩可以同时运行。[计时代码](../../rust/crates/costing-cli/src/run.rs)

| 输入 | 端到端 | 读取 | 规范化 | 拆分 | fact 聚合 | 展示表 | 导出 | 其中填表 | 其中保存 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| GB | 0.851 | 0.217 | 0.018 | 0.005 | 0.012 | 0.028 | 0.541 | 0.093 | 0.426 |
| 常规 SK | 2.894 | 0.520 | 0.143 | 0.047 | 0.142 | 0.206 | 1.794 | 1.575 | 0.174 |
| 完整 SK | 6.175 | 1.650 | 0.356 | 0.109 | 0.341 | 0.494 | 3.126 | 2.825 | 0.226 |

完整 SK 的读取加导出约占该批端到端时间的 77%。N=3 与上表 N=5 是同版本的独立批次，两组中位数的差说明了跨批比较的局限。测量记录了阶段耗时，CPU、分配和队列等待的占比仍需采样。

## 固定第一张表 XML 的压缩实验

按 ZIP 目录读取各成员的压缩字节数。第一张表占 GB 整包的 93.76%、常规 SK 的 94.19%、完整 SK 的 96.11%。取 GB 和完整 SK 的全部 `xl/worksheets/sheet1.xml`，流式使用验证环境 Python zlib（`1.3.1.zlib-ng`）生成 5 级、9 级 raw DEFLATE 数据，并对解压后的 XML 核对 SHA-256。估计整包字节数等于原包字节数减去原首表压缩字节数，再加上实测的新首表压缩字节数。

| 输入 | 原包字节 | 5 级估计整包 | 9 级估计整包 | 现行上限 | 9 级距上限 |
| --- | ---: | ---: | ---: | ---: | ---: |
| GB | 4,684,486 | 4,666,796 | 4,563,589 | 4,194,321 | 超 369,268 字节 |
| 完整 SK | 75,372,863 | 75,372,284 | 74,180,768 | 48,658,823 | 超 25,521,945 字节 |

9 级实验对应的完整 SK 估计体积较原包小 1.58%，达到原门槛需要小 35.44%。这组数值只描述固定 XML 和上述验证后端的压缩空间。Rust 写出速度、其它 XML 表示的体积及 Linux 稳定性均需独立验证。[Linux 同版本验收](releases/v0.3.1.md)

## 本地复测

在 PowerShell 将环境变量 `COSTING_SK_FULL` 指向完整 SK 工作簿，从仓库根目录运行。两个输出目录独立，阶段明细从第一条 CLI 命令的控制台 JSON 读取。

```powershell
$run = Join-Path '.pytest-tmp' ([guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $run | Out-Null
$binary = '.\rust\target\release\costing-calculate.exe'
& $binary sk --input $env:COSTING_SK_FULL --output (Join-Path $run 'stage.xlsx') --benchmark --redact-paths
& .\tools\validation\measure_release.ps1 -BinaryPath $binary -Pipeline sk -InputPath $env:COSTING_SK_FULL -OutputDirectory (Join-Path $run 'n5') -Iterations 5 -ReportPath (Join-Path $run 'n5.json')
```

本地 `.pytest-tmp/performance-plan-dinzubjs/` 保存三组逐次阶段数据、GB/常规 SK 的 N=5 和固定 XML 压缩结果，完整 SK 的 N=5 与审计复测保存在 `.pytest-tmp/full-sk-windows-eiy6ikys/`。本轮工作到诊断为止，生产版本仍为 0.3.1。

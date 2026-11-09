# Excel 上游依赖更新与适配记录

- 状态：Completed（Linux 本机维护升级已完成；跨平台和样本缺口见验证表）。
- 计划：[更新方案](../plans/2026-11-08-upstream-refresh.md)。
- 授权：用户要求将此前选定的读写上游更新到当前最新稳定版本，并完成适配。
- 工作材料：`.pytest-tmp/upstream/20261108-latest/`；升级前文件与原有未提交改动已备份。
- CodeGraph 状态：unindexed；当前仓库查询失败，按磁盘、Cargo 和测试回退，不创建索引。

## 依赖与来源

实际 `cargo search`、`cargo info` 及锁文件核对：

| 依赖 | 升级前 | 当前版本 |
|---|---|---|
| rust_xlsxwriter | 0.96.0 受控 Git fork | 0.102.0 受控本地源码 |
| calamine | 0.36.0 | 0.38.0 |
| zip | 7.2.0、8.6.0 | 9.4.2 |
| quick-xml | 0.41.0 | 0.42.0 |
| flate2 | 1.1.9 | 1.1.11 |
| zlib-rs | 0.6.6 | 0.6.8 |

上游写出版本的 revision 为 `d850973d3836905db3a04022b093b3d5575ee85e`，发布包 SHA-256 为 `6ca3d0e879a578c1313c2800ab0c6c029d38eed18098675e50341a30d32372ed`。正式受控源码位于 `rust/vendor/rust_xlsxwriter/`，原许可证、来源和 [PATCHES.md](../../rust/vendor/rust_xlsxwriter/PATCHES.md) 一并保留。

`lock-changes.json` 保存全部锁包前后差异。仅按目标更新 Excel 依赖；ZIP 9 带来的必要传递变化一并锁定，未整体更新其余业务依赖。核心计算、金额规则和应用版本不变。

## 已实施适配

- 使用上游原生 Result 工作表工厂、惰性临时文件和 `set_zip_compression_level`。普通压缩等级保持 6，低内存保持 5。
- 移植仍必要的行写入、flush、rewind、copy 失败传播；不改变已有 XML 并行和共享字符串顺序。
- 在 xlsx 适配器集中解开普通、临时文件和 ZIP 包装的原始 I/O 错误，保留 ErrorKind、OS 码和错误来源链；新工作表创建失败保持初始化阶段及实际临时目录。
- 本地受控 crate 排除在父 workspace 成员外；正式业务仍只有原四个 crate。
- 原源码包工具补齐 CHANGELOG 编译输入、README、修补说明和许可证；这些文件同样参与运行源码身份校验。

## 验证

| 项目 | 状态 | 证据 |
|---|---|---|
| 最新版与锁树 | 通过 | 六项目标版本已锁定，原两套 ZIP 合并为 9.4.2 |
| 受控补丁审阅 | 通过 | 四个生产源文件和一项来源链测试与上游有差异，其余 71 个 src 文件保持原字节 |
| I/O 故障与来源链 | 通过 | `fork-checks/commands.json` 与 `validation.stdout.txt`：十项隔离断言通过，覆盖实际写入、flush/rewind/copy 失败及四种原始 I/O 错误来源链 |
| 适配器定向回归 | 通过 | 全目标/全功能 check 及 costing-xlsx 测试 26 项通过 |
| 源码包定向回归 | 通过 | 108 项 Python 工具/候选测试通过 |
| 完整 Rust / release | 通过 | fmt/clippy、完整 Rust 测试 214 项、release 构建及 GB/SK 合成端到端通过；关闭低内存分支另有 19 项测试通过 |
| 完整 Python | 通过 | Ruff/格式检查，370 项通过、1 项 PowerShell 平台跳过 |
| 新旧初步比较 | 通过 | 小型 GB/SK × 两种审计模式、真实 GB × 两种审计模式，共 6 组完整零差异 |
| 边界与模式矩阵 | 通过 | `compatibility/summary.json`：25 个主场景及两次无审计模式的审计重放全部零差异；覆盖 GB/SK 阈值上下、独立种子、月份、check-only 与内嵌配置；大型 XML 字节不同的完整语义重放也为零差异 |
| 源码包实际构建 | 通过 | 最终包解压后 offline/locked Cargo check 通过；147 个运行源码文件、14 个执行工具与当前工作区哈希一致，归档 CRC 通过 |
| 来源与程序身份 | 通过 | 六项目标锁包、四个业务 workspace 成员、83 个受控源码文件及实际 release 程序哈希核对通过 |
| Windows / 真实 SK | 未验证 | 当前没有 Windows 实际运行环境和真实 SK 输入 |

## 最终产物与身份

- 本机程序：`rust/target/release/costing-calculate`；SHA-256 为 `6d0b3c43e378483154f0374bdafb5ab8104510b67ede659f72c8c87be9a915a0`。
- 应用版本仍为 `0.3.0`，配置和 Manifest schema 均为 V1；构建提交身份为 `747c0ca5e15403d4c79aeff4bf4a7e5bbc356345`。本次源码尚未提交，由锁文件、受控源码清单及程序哈希识别升级后的实际版本。
- 83 个受控文件的库存 SHA-256：`cdd96207f8b100fbc0e0eecaf7ce832fc82a94e7158a11fba53489bc38ab9d2a`；逐文件哈希见[来源清单](../performance/dependencies/2026-11-08-rust-xlsxwriter-0.102.0.json)。
- 本地源码验证包：`.pytest-tmp/upstream/20261108-latest/package-final/bundle/colab-source.zip`；SHA-256 为 `cc63aebf81fbd2665890299bbd7e7eb8b48cd03c4056174380eb0f926db2b346`，大小为 `919,101 bytes`。
- 源码包检查证据：工作材料下的 `package-final/result.json`、`package-final/build.log` 和 `source-delivery-check.json`；最终依赖图与全部兼容结果汇总见 `dependency-graph-check.json`、`delivery-verification.json`。
- 升级前程序保留在 `.pytest-tmp/optimization/20261108-xlsx-v2/baseline/costing-calculate`；SHA-256 为 `3a5bc445e84fb1ac87a53ea1b42184a797fc9ee287bd32259b5d7e1f3bb197bc`。本次升级前文件副本位于工作材料下的 `before/`，原有未提交差异保存在 `preexisting.patch`。

本次按用户要求做依赖维护升级，尚无新的稳定速度数据，不宣称提速。真实 ERP、输出和原始比较材料留在本地忽略目录；没有提交、推送、发布或上传，也不触发 CI。

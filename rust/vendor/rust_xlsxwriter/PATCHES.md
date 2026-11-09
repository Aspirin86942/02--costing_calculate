# rust_xlsxwriter 0.102.0 受控源码

## 上游来源

- 发布包：crates.io `rust_xlsxwriter` `0.102.0`。
- 上游仓库：<https://github.com/jmcnamara/rust_xlsxwriter>。
- 发布包 `.cargo_vcs_info.json` 记录的 revision：`d850973d3836905db3a04022b093b3d5575ee85e`。
- 原始 `.crate` 发布包 SHA-256：`6ca3d0e879a578c1313c2800ab0c6c029d38eed18098675e50341a30d32372ed`。
- 移植参考：旧受控 fork 的实际 revision `816cd47bc2faa84ab1ac2fbb3320a4699a454b22`。历史来源记录中的 `f4e4e99f` 不是此次比较的最终旧版本。

这里保留的 `.cargo_vcs_info.json` 是原发布包的来源记录；上述发布包哈希不代表修补后的目录哈希。

## 本地修补

生产源码仅修改以下四个文件：

1. `src/worksheet.rs`
   - 用私有 `TempFileWriter` 包装原来的 `BufWriter<File>`。
   - XML 辅助函数原本通过 `expect` 处理写入结果，因此包装器暂存行内第一个原始 `std::io::Error`，在行写入结束时返回该对象。发生错误后，该行剩余写入不会覆盖第一个错误。
   - `insert_cell`、`flush_to_row`、`flush_data_row` 和 `flush_last_row` 通过 `Result` 向调用者传播错误，覆盖有单元格数据和只有行属性的分支。
   - 保存前的 flush 和 rewind 都返回实际 I/O 错误。
2. `src/workbook.rs`
   - 已有临时文件工厂构造上述包装器。保留上游的创建时机、指定临时目录处理、四个返回 `Result` 的 worksheet 工厂，以及 `TempFileError(PathBuf, io::Error)`。
3. `src/packager.rs`
   - 最后一行写入和 rewind 改为 `?` 传播错误。
   - 保留原有 `BufReader` 和 `std::io::copy(...)?` 路径。
4. `src/error.rs`
   - `Error::source()` 返回 `IoError`、`TempFileError` 和 `ZipError` 包含的错误对象，使调用者能沿错误来源链检查 `ErrorKind`、`raw_os_error()` 和更深层的来源。
   - 保留上游错误变体及 `From` 转换。

`src/error/tests.rs` 同步了来源链的断言。其余 71 个 `src/` 文件与原发布包字节一致。

保留上游按 Sheet 并行生成 XML、共享字符串次序和 XML 写出代码。保留上游压缩 API、压缩等级默认值、ZIP 后端功能选择及全部公开 features；本地修补没有增加并行压缩或其他性能优化。

上游已实现普通 worksheet 不创建临时文件，以及低内存工厂返回创建错误；这里直接使用这些实现。`Worksheet::file_writer()` 中仍保留上游对内部状态的 `expect`：只有已经成功创建临时文件的低内存 worksheet 才会调用它，该断言不处理 I/O 返回值。

## 文件载荷与删减

目录包含 76 个原上游 `src/` 文件，以及以下七个根文件，共 83 个文件：

- `Cargo.toml`：从上游 `Cargo.toml.orig` 精简。
- `README.md`：原文保留。
- `LICENSE_MIT`、`LICENSE_Apache2.0`：原文保留。
- `CHANGELOG.md`：原文保留；`src/changelog.rs` 通过 `include_str!("../CHANGELOG.md")` 引用，是编译必需文件。
- `.cargo_vcs_info.json`：原文保留。
- `PATCHES.md`：本来源与修补说明。

`Cargo.toml` 中的全部 13 个正常/可选依赖及 13 个 features 定义与上游完全一致，包括 `constant_memory`、`zlib`、`zmij` 和 `test-resave`。

删去未复制的 examples、benches、外部集成测试目标、开发依赖及上游维护命令。未复制构建产物、缓存、锁文件、测试数据和独立文档目录。保留源码内的文档模块及 27 个 `src/*/tests.rs` 文件，因为上游源码通过无条件 `mod tests;` 声明引用这些路径。

该最小分发的 `[lib]` 设置 `test = false`、`doctest = false`，不自动运行依赖外部测试数据和开发依赖的上游测试套件。显式执行上游单元测试需要另行准备完整上游测试环境；项目自身的集成回归由父 workspace 执行。

## 本地验证

- 通过：用 TOML 解析器逐对象比较上游和本目录的依赖、features，均完全一致。
- 通过：源码目录清单、允许修改的五个源文件、原许可证/README/CHANGELOG/来源文件的字节一致性，以及所有编译时包含文件路径检查。
- 通过：对实际修改的五个 Rust 文件执行 `rustfmt --edition 2021 --config skip_children=true --check`。
- 通过：使用 Rust 1.96.0 和最终源码中的 `TempFileWriter`、`XlsxError` 实现，以及本机 `zip 9.4.2` 的原始 `result.rs`，运行十项隔离断言：立即写入失败、缓冲 flush 失败、rewind 前 flush 失败、非可定位文件的 rewind 失败、copy 期间读取失败、原字节往返及再次保存后追加，以及直接 I/O、临时文件、ZIP 包装、ZIP 转换为 I/O 后的四种错误来源链。
- 上述失败路径使用 Linux 实际文件错误 `EBADF`、`ESPIPE`，来源链检查同时确认 `ENOSPC` 对应的原始操作系统错误码和 `StorageFull`。隔离验证只生成目录内临时产物，结束后已清理。
- 主项目完整构建、workbook 契约、原子发布、故障回归和平台验证以[集成记录](../../../docs/changes/2026-11-08-upstream-refresh.md)为准；本最小分发不自动运行上游完整测试套件。

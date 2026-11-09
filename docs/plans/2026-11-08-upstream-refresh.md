# Excel 上游依赖更新与适配

- 状态：Completed（Linux 本机维护升级已完成；结果见[实施记录](../changes/2026-11-08-upstream-refresh.md)）。
- 授权：用户要求按此前意向更新上游、完成适配并升级到最新稳定版本。
- 基础提交：`747c0ca5e15403d4c79aeff4bf4a7e5bbc356345`；保留此前未提交的验证工具和文档改动。
- 最新版本核对：本轮实际 `cargo search/info`，2026-11-08。
- 本地材料：`.pytest-tmp/upstream/20261108-latest/`。

## 范围与方式

更新 Excel 读写链路：rust_xlsxwriter 0.102.0、calamine 0.38.0、zip 9.4.2、quick-xml 0.42.0、flate2 1.1.11、zlib-rs 0.6.8。保持 Rust 1.96.0、release、codegen-units=1；按目标更新锁包，不整体升级无关业务依赖。

受控写出源码放在 `rust/vendor/rust_xlsxwriter/`，保留上游来源、许可证和必要修补，使用精确版本和本地路径。沿用上游已实现的惰性临时文件和 Result 工厂；移植仍必要的写/flush/rewind/copy 错误传播及原始错误链。根项目适配新压缩设置 API、可失败的工作表创建和 TempFileError/ZIP I/O 分类。同步已有源码包工具，使本地构建和 Colab 包均包含完整的必需源码与许可证。

普通工作簿压缩等级保持 6，含低内存 Sheet 时保持 5；阈值、共享字符串顺序、三张 Sheet、金额与业务规则、错误码、Manifest V1、禁止覆盖、原子发布及清理语义保持。借用读取与并行压缩等优化实验不在本次升级中自动采纳。

## 完成条件

1. 最新版本和来源可核对；Cargo.lock 固定目标版本，受控源码完整可构建。
2. 所有调用、错误映射、故障测试和源码打包接入新依赖；保留原始 ErrorKind、OS 错误和外部阶段。
3. Rust fmt/clippy/workspace tests、Python 受影响测试及必要全套回归、release 构建与 GB/SK 合成端到端通过。
4. 新旧程序在真实 GB、GB/SK 合成正确性、阈值上下、独立种子和两类审计模式下完整比较；完整语义比较不降为抽样。
5. 更新来源记录、事实文档和实际变更记录。没有稳定速度数据就不宣称提速；真实 SK、Windows/Excel/WPS 缺口明确保留。

Git 提交/推送、远端 fork 发布、Release 和云端上传不在本次范围内。自动 CI 保持关闭。

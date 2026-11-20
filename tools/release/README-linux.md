# Costing Calculate（Linux x86-64）

在解压目录中校验文件，再确认版本与构建提交。

```sh
sha256sum -c SHA256SUMS
./costing-calculate --version-json
./costing-calculate --help
```

GB 或 SK 各运行一次处理一份输入。将示例路径替换为实际的 Excel 路径。

```sh
./costing-calculate gb --check-only --input ./gb-input.xlsx
./costing-calculate sk --input ./sk-input.xlsx --output ./sk-result.xlsx
```

命令行使用内嵌默认配置。需要调整产品白名单时，可复制 `config/costing.default.toml`，编辑后通过 `--config` 指定副本。需要审计记录时可附加 `--summary-output ./run.json`。工作簿和审计记录均须使用新的输出文件名。

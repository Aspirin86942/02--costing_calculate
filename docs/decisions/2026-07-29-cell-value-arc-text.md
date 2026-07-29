# Reader 整数快路径与 Arc 文本表示

- 状态：Accepted
- 日期：2026-07-29
- 范围：`costing-core` 内存模型与 `costing-xlsx` 读取适配
- 证据：[`../changes/2026-07-28-sk-performance-experiments.md`](../changes/2026-07-28-sk-performance-experiments.md)

## 背景

真实 SK 输入包含 `467,420` 行、约 `14,022,600` 个 cell。原实现把
`CellValue::Text` 和 `DateLike` 保存为独立 `String`，而 normalize、
split、fact 和 presentation 会在表投影或派生时 clone 大量 cell。
同时，reader 对所有浮点数都经过字符串格式化再解析为 `Decimal`。

聚合 census 和独立候选实验表明：

- `CellValue` 从 `32 bytes` 降到 `24 bytes` 可以直接降低整表行内容量；
- 仅把文本改为 `Arc<str>`，不做驻留，已经使真实 SK PWS 下降约 `25%`；
- 有界按列驻留虽再节省约 `4%` PWS，却使 wall 回退约 `2.5%`；
- 有限、整数且安全落在 `i64` 范围内的浮点值可以直接构造等价 `Decimal`。

## 决策

1. `CellValue::Text` 和 `CellValue::DateLike` 使用 `Arc<str>`。
2. clone `CellValue` 时共享文本分配；相等性仍按文本内容，JSON 序列化形状不变。
3. 只启用现有 `serde` 的 `rc` feature，不新增生产依赖或版本，不修改 `Cargo.lock`。
4. reader 对有限、无小数且满足 `i64::MIN <= value < i64::MAX as f64`
   的值使用 `Decimal::from(value as i64)`。
5. `i64::MAX as f64` 因舍入等于 `2^63`，上界必须保持严格小于；其他浮点值继续走原有
   String → Decimal 回退路径。
6. 不启用全局或按列文本驻留池，不 trim、不规范化、不改变源文本内容。
7. workbook、CLI、错误码、`RunManifestV1`、三张 Sheet 和 Decimal 输出边界保持不变。

## 影响

- 真实 SK normal 相对冻结基线的 8 对结果：wall 改善 `6.1896%`、
  PWS 下降 `24.9597%`，两项均赢 `8/8`。
- 读取、normalize、split 和表投影中的文本 clone 变成 Arc 引用计数操作。
- reader 初次读取每个源文本时仍各自创建 Arc；本决策不承诺跨 cell 驻留。
- `serde/rc` 是编译 feature 变化，不改变运行时部署依赖。
- 序列化 golden、真实 GB/SK package fast path、单月过滤、N=5 和 Windows 包 smoke
  已验证通过。

## 未选择的方案

- 有界按列文本驻留：PWS 继续下降，但 wall、ingest 和 total 回退并增加实现复杂度。
- 无界全局驻留：高基数列可能无限增长，不满足容量和所有权边界。
- compact/small-string 第三方类型：需要新增生产依赖，当前收益没有证明其必要性。
- 替换 Calamine 或实现流式 XLSX reader：显著扩大范围和风险。
- 继续保留 `String`：正确但放弃已经稳定复现的内存和端到端收益。

## 回滚

reader 快路径和 Arc 表示分别位于提交 `26f29e3` 与 `6d0f274`。回滚必须同时重跑
Rust/Python、真实 GB/SK、N=5 和 Windows 包 smoke；不得只根据二进制能启动判断安全。

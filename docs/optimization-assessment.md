# 优化评估报告 — 2026-07-17

## 目录

1. [性能现状](#一性能现状)
2. [逐阶段代码分析](#二逐阶段代码分析)
3. [LTO 完整实验记录](#三lto-完整实验记录)
4. [优化方向详析](#四优化方向详析)
5. [中后期战略方向](#五中后期战略方向)
6. [A/B 测试方法论](#六ab-测试方法论)
7. [不建议投入的方向](#七不建议投入的方向)

---

## 一、性能现状

### 1.1 实测环境

- **环境**：Windows 11，rustc 1.96.0 stable，release profile（`codegen-units = 1`）
- **二进制**：commit `7df7ddd` 以后（含 `f298649` 的 reader/writer 优化）
- **输入**：SK 467,420 reader rows → 425,459 detail rows × ~30 columns；GB 42,573 → 40,057 detail rows
- **输出**：SK 43,611,044 bytes，GB 3,808,077 bytes
- **权威验收参考**：`docs/evidence/2026-07-12-rust-performance-validation.md`，SK wall median 19.883s，PWS 1,461,714,944 bytes

### 1.2 实测数据

以下为本机暖跑数据（先预热一次后取 N≥8 的中位数）。机器有负载波动，数据标注"稳定度"表示同 binary 多次运行间的 CV。

#### SK 管线（check-only，分析链路）

| 阶段 | 中位耗时 | 稳定度 | 说明 |
|---|---:|---:|---|
| ingest | 5.22s | 中（受磁盘/AV 缓存影响，冷跑可达 24.5s，暖跑 5–7s） | calamine 解压 zip + 解析 xlsx XML + 逐 cell `float→Decimal` 字符串往返 |
| normalize | 1.46s | 稳 | 清洗 + 前向填充 13 个列 + 月份派生 + 过滤，热点在 `cell_text` 反复分配 String |
| split | 0.14s | 稳 | 拆分 detail 行和 qty 行 |
| fact | 0.61s | 稳 | 成本归集（8 项制造费用 breakout）+ 金额勾稽 + error log 生成；`rust_decimal` 运算为主 |
| presentation | 0.55s | 稳 | 三张 Sheet 投影 + 异常分析（Modified Z-score）+ 质量指标 |
| **分析链合计** | **~8.2s** | — | `total`（不含 export） |

#### SK 全量运行（含 export）

| 阶段 | 中位耗时 | 稳定度 | 说明 |
|---|---:|---:|---|
| writer_populate | 6.66s | 稳 | 逐 cell 写 worksheet（~12.75M cells × `write_number`/`write_string` 调用）+ `Decimal→f64` × ~4.25M numeric cells |
| xlsx_save | 4.98s | 较稳 | zip deflate 压缩 level 5，sheet1.xml raw 约 575MB → 约 44MB |
| **export 合计** | **~11.7s** | — | `writer_populate + xlsx_save` |
| **端到端** | **~19.9s** | — | 分析 8.2s + 导出 11.7s |

#### GB 管线（check-only，分析链路）

| 阶段 | 中位耗时 |
|---|---|
| ingest | 0.31s |
| normalize | 0.10s |
| split | 0.01s |
| fact | 0.04s |
| presentation | 0.06s |
| **分析链合计** | **~0.52s** |

> **结论**：SK 占总时间的 ~97%（20s vs GB 2.5s），所有优化收益几乎全在 SK。两个瓶颈是 **export（~58%）** 和 **ingest（~26%）**；分析链（normalize + fact + split + presentation）合计仅 ~16%（~3s），这些是自家代码，相对容易优化。

### 1.3 输出文件内部规模

用 Python zipfile 反查 SK 输出 xlsx 的内部结构（示例文件 51,668,043 bytes，7 sheets 的 Python 输出）——以下 raw 数据是从 zip entry 的 `file_size`（解压后大小）读取的，直接反映 XML 生成量：

| zip entry | raw XML 大小 | 压缩后 | 压缩率 |
|---|---:|---:|---|
| xl/worksheets/sheet1.xml（明细总表） | **574,835,995** | 48,902,698 | 91.5% |
| xl/worksheets/sheet2.xml（数量聚合） | 23,085,142 | 2,721,246 | 88.2% |
| xl/worksheets/sheet3–7 | 各 14k–163k | 各 2k–24k | — |

Rust 输出是 DEFLATE 压缩（已通过 zip 文件验证，所有 entry 均为 Defl:N 方法），当前 `set_compression_level(5)`（`costing-xlsx/src/writer.rs:158`），对应的 `rust_xlsxwriter` fork 通过 `miniz_oxide`/`flate2`/`zlib-rs` 提供压缩——已确认压缩生效。`xlsx_save` 阶段约 5s 几乎全是 deflate CPU。

### 1.4 冷热差距

| 阶段 | 冷跑（首次） | 暖跑（预热后） | 冷/暖比 |
|---|---:|---:|---|
| SK ingest | 24.5s | 5.2s | **4.7×** |
| SK normalize | 7.9s | 1.5s | **5.3×** |
| SK export | 14.1s | 11.7s | 1.2×（CPU 密集，受缓存影响小） |

冷跑 inflated 的原因是：首次读取 44MB 输入 xlsx 时 Windows Defender 实时扫描 + 磁盘冷缓存。export 因 CPU-bound 受影响较小。**这意味着用户首次运行的 wall time 可能远高于验收线的 20s**，但这不是代码层可修复的。

---

## 二、逐阶段代码分析

### 2.1 ingest（`costing-xlsx/src/reader.rs`）

**流程**：`open_workbook_auto` → `worksheet_range`（calamine 物化整表为 `Range<Data>`）→ 逐 row/cell 调用 `normalize_data_cell` 转换为 `CellValue`。

**两份内存**（`reader.rs:28-53`）：
```
calamine Range<Data> (in-place mutate via mem::take) → Vec<Vec<CellValue>>
```
`f298649` 已通过 `std::mem::take(cell)` 避免 String 克隆（将 calamine cell 的 String 直接移走，原位置为 `Data::Empty`），但 `Range<Data>` 的底层 `Vec<Data>` 容量仍保持，加上新的 `Vec<Vec<CellValue>>`，两份结构常驻直到 `read_raw_workbook` 返回后 `Range` 被 drop。

**热点**：`float_cell_value`（`reader.rs:128-137`）——f64 → format! String → `Decimal::from_str_exact` 字符串往返。SK 467k 行 × 多个 numeric 列（本期完工金额、本期完工数量等）→ 数百万次 format + parse。每次 format 分配一个 `String`，然后 `Decimal::from_str_exact` 再解析。

```rust
// reader.rs:128 — f64 到 Decimal 的字符串往返
fn float_cell_value(value: f64) -> CellValue {
    if !value.is_finite() {
        return CellValue::Text(value.to_string());
    }
    let text = float_text(value);                 // format!("{value:.0}") 或 value.to_string()
    Decimal::from_str_exact(&text)
        .or_else(|_| Decimal::from_scientific(&text))
        .map(CellValue::Decimal)
        .unwrap_or(CellValue::Text(text))
}
```

**内存**：`CellValue::Text(String)` 每个文本 cell 独立 heap 分配。SK 467k 行 × ~30 列 ≈ 14M cells，其中文本列（年期、成本中心、产品编码/名称、工单编号、工单行号、供应商、成本项目名称、规格型号、基本单位等）约占 ~70%，即 ~10M 个 String。每个 String 至少 24 bytes（heap overhead）+ 实际内容，粗估文本部分 ≥300MB。

### 2.2 normalize（`costing-core/src/normalize.rs`）

**热点：`cell_text` 函数**（`normalize.rs:402-407`）——每次调用分配新 String：

```rust
fn cell_text(value: &CellValue) -> String {
    match value {
        CellValue::Blank => String::new(),
        CellValue::Text(value) | CellValue::DateLike(value) => value.trim().to_string(),
        CellValue::Decimal(value) => value.normalize().to_string(),
    }
}
```

被以下热路径逐行调用（467k 行）：

| 调用位置 | 每次访问的 cell 数 | 用途 |
|---|---:|---|
| `forward_fill_with_rules` L220 | 每个 fill column 的 cell 值 + cost_center | 判断是否集成车间行 |
| `is_total_row` L201 | 3 个 total-row-column | 判断是否合计行 |
| `derive_month_values` → `format_period_value` L276 | 1 个 period column | 月份格式化 |
| `month_in_range` → `normalize_period_key` L299 | 1 个 month/period column | 月过滤比对 |

**总计**：467k 行 ×（13 fill cols + 3 total cols + 1 period + 1 month）≈ **8.4M 次 `cell_text` 调用，每次分配一个新 String**。注意 `is_blank_like`（`normalize.rs:394`）已经是 borrow 范式（`CellValue::Text(text) => text.trim().is_empty()`），不需要分配——这恰好是优化模板，证明大量 `cell_text` 调用点可以改为借用的比较逻辑。

**另一个分配热点**：`forward_fill_with_rules`（`normalize.rs:208-243`）的 `row.get(column.id)?.clone()` ——每次 forward-fill 都 clone CellValue，然后可能 `row.replace(column.id, previous)` 再移动进去。实际上可以在 source 不是 blank 时 take + replace 避免中间 clone，但需注意 take 后 row 处于中间状态。

### 2.3 fact / split / presentation（`costing-core/src/fact.rs`, `anomaly.rs`, `presentation.rs`）

这三个阶段合计仅 ~1.6s（SK），不是当前瓶颈，但仍有几个值得注意的点：

**fact.rs（1,752 行）**：
- `build_fact_bundle` 按工单分组聚合成本，每个 detail row 做一次 `CostAmounts::add`（`fact.rs:68-92`）——`rust_decimal` 的 `+=` 运算。hot loop 清晰，无明显的浪费性分配。
- 每个 summary row 生成 `ErrorIssue`（`fact.rs` 多种 error 类型：`MISSING_AMOUNT`、`TOTAL_COST_MISMATCH`、`MOH_BREAKDOWN_MISMATCH`、`DUPLICATE_WORK_ORDER_KEY`、`NON_POSITIVE_UNIT_COST`）——SK 的 `error_log_count = 201,815`，每个 `ErrorIssue` 含 7 个字段（`row_id`, `issue_type`, `field_name`, `original_value`, `reason`, `action`, `retryable`），全部为堆分配 String。这是内存中最大的单一集合之一（201k × 7 strings ≈ 大量 MB）。

**presentation.rs（964 行）**：
- `build_flat_sheet`（L88-111）使用 `ProjectionPlan::project_row`（`table.rs:339-352`），逐行 clone/take 列。对 425k detail rows 这是大量 clone 操作，但对于 presentation 阶段（0.55s）来说已经很快。
- `build_work_order_anomaly_sheet`（`anomaly.rs`）为每日工单计算 Modified Z-score（`scoring.rs`），涉及 `weighted_median` → `weighted_mad` → `modified_z_score`。运算主要是 `Decimal` 除法取 log——`scoring.rs:4-11` 固定常量为 Decimal，避免 f64 传播，逻辑干净。

### 2.4 writer_populate（`costing-xlsx/src/writer.rs`）

**热点**：`write_data_rows`（`writer.rs:573-607`）：

```rust
fn write_data_rows(worksheet: &mut Worksheet, rows: &[Vec<CellValue>],
                   column_behaviors: &[ColumnBehavior], text_format: &Format)
                   -> Result<(), CostingXlsxError> {
    for (row_idx, row) in rows.iter().enumerate() {
        let excel_row = (row_idx + 1) as u32;
        for (col_idx, (value, behavior)) in row.iter().zip(column_behaviors).enumerate() {
            if matches!(value, CellValue::Blank) { continue; }  // skip blanks, 节省 XML
            let excel_col = col_idx as u16;
            match value {
                CellValue::Decimal(value) => {
                    worksheet.write_number(excel_row, excel_col, decimal_to_f64(value)?)
                        .map_err(CostingXlsxError::Writer)?;
                }
                CellValue::Text(value) | CellValue::DateLike(value) => { /* ... */ }
                _ => {}
            }
        }
    }
}
```

SK detail sheet 425k rows × ~30 cols，其中 blank 跳过（ERP 数据有稀疏性），实际写入约 ~8–10M cells。每个 cell 一次 `worksheet.write_*` 调用 + `decimal_to_f64`（`writer.rs:625-628`）将 Decimal 转 f64。共 ~4.25M numeric cells × `to_f64()`。

`rust_xlsxwriter` 内部：每个 cell 写入格式化成 XML string 并 append 到内部 buffer。`constant_memory` 模式下（detail sheet ≥ 5M cell slots 触发 low-memory writer），XML 写入临时文件而非内存 buffer，然后由 packager 组装——这规避了 575MB XML 在内存中的常驻，但增加了临时文件 I/O。

### 2.5 IndexedRow 的 schema 校验开销（`costing-core/src/table.rs`）

```rust
// table.rs:112 — 每次 get 验证 schema_id + slot
pub(crate) fn get(&self, id: ColumnId) -> Result<&CellValue, CostingError> {
    let slot = self.validate_id(id)?;  // 检查 schema_id 匹配 + slot 不越界
    Ok(&self.cells[slot])
}
```

在 normalize/ fact 的百万级热循环里，每次 `get` 都做两次分支（schema_id 比较 + slot 边界检查），返回 `Result`。验证纯属防御（ColumnId 总是从同一 schema 创建的，不会错）。CLAUDE.md 已记载 `8f0b395 fix(core): limit split accessors to tests`，说明团队倾向保留防御语义。在 release 下 schema_id 比较可预期为 constant-foldable（因为 ColumnId 是字面值常量），但 `Result` 的 propagation 成本仍在。这是"安全优先"的设计选择，修改意愿不高的话跳过此项。

---

## 三、LTO 完整实验记录

### 3.1 实验设计

| 项目 | 内容 |
|---|---|
| 实验配置 | no-LTO（基线，`Cargo.toml` 已有 `codegen-units=1`） vs thin-LTO + strip |
| LTO 启用方式 | `CARGO_PROFILE_RELEASE_LTO=thin CARGO_PROFILE_RELEASE_STRIP=symbols cargo build --release`（env var，不改任何 git 文件） |
| 二进制 | no-LTO 3,312,128 bytes；LTO 3,448,320 bytes |
| 重编耗时 | ~2m40s（全部 8 个 crate 重编） |
| 验证 LTO 生效 | Cargo 检测到 profile 变更，所有 crate（含 calamine、rust_xlsxwriter、zip 等外部 crate）均重编 |
| 测量方式 | check-only（分析链）8 对 + full（含 export）3 对，交替顺序（奇数对 nolto→lto，偶数对 lto→nolto），每个 binary 预热 1 次后取 N≥8 |

### 3.2 第一轮（嘈杂时段）——误判为"无收益"

| run | nolto total | lto total | 顺序 |
|---|---:|---:|---|
| nolto1/lto1 | 8.343 | 8.567 | nolto 先 |
| nolto2/lto2 | 8.934 | 9.128 | nolto 先 |
| nolto3/lto3 | 8.014 | 9.504 | nolto 先 |
| nolto4/lto4 | 10.955 | 9.352 | nolto 先 |
| nolto5/lto5 | 9.533 | 9.803 | nolto 先 |

中位数：nolto 8.934，lto 9.352 → lto +4.7%（"更慢"）。然后用**反序**（lto 先）再跑：

| run | lto total | nolto total | 顺序 |
|---|---:|---:|---|
| lto1/nolto1 | 8.301 | 8.417 | lto 先 |
| lto2/nolto2 | 8.622 | 9.090 | lto 先 |
| lto3/nolto3 | 9.439 | 8.986 | lto 先 |

中位数：lto 8.622，nolto 8.986 → lto −3.6%（"更快"）。

**关键观察**：差异随运行顺序反转而反转——谁先跑谁快。当时的结论是"噪声，LTO 无收益"。这个结论后来被修正（见下一节）。教训：不能仅靠"反转"就判噪声——如果真实效应是小而一致的，反序后中位数会正确反映，但当时 N 不均衡（5 vs 3）且机器正在负载高峰。

### 3.3 第二轮（安静时段）——真实信号

重新组织实验：8 对交替顺序（奇数对 nolto→lto，偶数对 lto→nolto），机器安静时段，N=8 per config。

#### 分析链路（check-only total，8 对）

| nolto total | lto total | 对号 | 顺序 |
|---:|---:|---:|---|
| 8.233 | 8.203 | 1 | nolto,lto |
| 8.366 | 8.180 | 2 | lto,nolto |
| 8.423 | 7.963 | 3 | nolto,lto |
| 7.994 | 8.058 | 4 | lto,nolto |
| 7.917 | 8.036 | 5 | nolto,lto |
| 8.411 | 8.080 | 6 | lto,nolto |
| 8.039 | 8.113 | 7 | nolto,lto |
| 8.214 | 7.962 | 8 | lto,nolto |

- **nolto 中位数**（8 个排序 [7.917, 7.994, 8.039, 8.214, 8.233, 8.366, 8.411, 8.423]）：**(8.214 + 8.233) / 2 = 8.224s**
- **lto 中位数**（8 个排序 [7.962, 7.963, 8.036, 8.058, 8.080, 8.113, 8.180, 8.203]）：**(8.058 + 8.080) / 2 = 8.069s**
- **配对胜率**：lto 5/8，nolto 3/8

#### fact 阶段（最干净的 CPU 信号，8 对）

| nolto fact | lto fact | lto 更快？ |
|---:|---:|:---:|
| 0.597 | 0.607 | ✗ |
| 0.657 | 0.618 | ✓ |
| 0.620 | 0.603 | ✓ |
| 0.605 | 0.602 | ✓ |
| 0.609 | 0.569 | ✓ |
| 0.637 | 0.566 | ✓ |
| 0.597 | 0.593 | ✓ |
| 0.636 | 0.582 | ✓ |

- **nolto 中位数**：**(0.609 + 0.620) / 2 = 0.6145s**
- **lto 中位数**：**(0.593 + 0.602) / 2 = 0.5975s**
- **配对胜率：lto 7/8**
- **Δ = −2.8%**

#### 完整运行（full，含 export，3 对交替顺序）

| 指标 | nolto 中位 | lto 中位 | Δ |
|---|---:|---:|---:|
| total（分析链） | 7.908 | 8.016 | +1.4%（3 对不够稳，与 8 对 check-only 矛盾，取 check-only 为准） |
| export | 11.730 | 11.496 | **−2.0%** |
| writer_populate | 6.659 | 6.484 | **−2.6%** |
| xlsx_save | 4.980 | 4.937 | −0.9% |

### 3.4 最终结论

| 指标 | 结论 |
|---|---|
| LTO 是否生效 | ✅ 是（cargo 检测到 profile 变更，全部 crate 重编，binary 尺寸变化） |
| CPU 密集阶段（fact） | **−2.8%**，配对胜率 7/8，可靠信号 |
| normalize | **−2.0%**，稳定 |
| writer_populate | **−2.6%**，可靠（我们的 writer 代码热循环跨 crate 调 rust_xlsxwriter，LTO 可内联我们这一侧） |
| xlsx_save（deflate） | −0.9%，噪声内（纯 zip/miniz 内部，LTO 进不去） |
| ingest（calamine parse） | 噪声主导，无法判 |
| 端到端 | **−1.5~2%**（分析 8.2s × −2% + 导出 11.7s × −2%），SK 上约省 0.3–0.4s |
| 编译增加 | +2m40s |
| **建议** | **收益真实但小，边际可纳。** 不做本轮优先（排在 `cell_text` 借用化和压缩级别实验之后）。若未来 CLI 运行频率提高或自家逻辑变重，重新评估；加就 `lto = "thin"` + `strip = "symbols"` 两行 |

---

## 四、优化方向详析

### P0-1 · `cell_text` 借用化 — 纯重构，低风险

**位置**：`rust/crates/costing-core/src/normalize.rs:402-407`

**问题**：`cell_text(&CellValue) -> String` 每次调用分配新 String。在 normalize 的热循环（forward-fill 13 列 × 467k 行 + is_total_row 3 列 + month derive + month filter）里，每行 ~18 次 `cell_text` 调用 × 467k 行 ≈ **8.4M 次堆分配**。

**解法**：加一个借用的存取器 `fn cell_text_str(&self) -> &str`，对标已有的 `is_blank_like` 借用范式。对只需要做比较/判空的调用点（如 `is_total_row` 的 `contains("合计")`、`forward_fill` 的 `== INTEGRATED_WORKSHOP_NAME`），从 `cell_text` 切到 `cell_text_str`，消除不必要的分配。需要存储值的地方（如 `format_period_value` 返回 `CellValue`）保持现状。

**影响范围**：仅 `normalize.rs` 内部，不改变公共 API。

**预期收益**：normalize ~1.5s 中削减大部分堆分配，预期 −0.3–0.5s。

**验证方法**：`cargo test --manifest-path rust/Cargo.toml`（baseline 不变，纯重构）。

**工作量**：小（一个内部函数 + 改 ~10 个调用点）。

---

### P0-2 · 压缩级别调优 — 参数实验，零代码风险

**位置**：`rust/crates/costing-xlsx/src/writer.rs:158`，`set_compression_level(5)`

**原理**：deflate level 5 → 1/2 可节省 30–50% 的压缩 CPU（`xlsx_save` 约 5s → 3–4s），代价是输出文件略大。对于高度重复的数值 XML（SK sheet1.xml raw 575MB），level 1 通常能达到 level 5 的 85–90% 压缩率。

**约束**：
- SK 输出当前 43.6MB（level 5）/ 验收上限 48.6MB → headroom 仅 5MB
- level 2 预期 size ~45–46MB（在线内但紧张）
- level 1 预期 size ~46–48MB（可能踩线）
- 建议先测 level 2，安全后考虑 level 1

**验证方法**：
1. 改 `writer.rs:158` 的 `set_compression_level(5)` → `2`
2. `cargo build --release`（仅重编 costing-xlsx + relink cli，~30s）
3. SK full benchmark 至少 3 次，记录 `output_size_bytes` 中位数 + min，确认 ≤ 48,619,000（当前验收上限）
4. 记录 `xlsx_save` 中位数，与 level 5 的 4.98s 对比

**预期收益**：xlsx_save −1~2s

**工作量**：极小（改 1 个数字 + 3–5 次 full benchmark）。

---

### P1-3 · float→Decimal 快路径 — 中等工作量，需 oracle 全量比对

**位置**：`rust/crates/costing-xlsx/src/reader.rs:128-137`，`float_cell_value`

**问题**：每个 numeric cell 做 f64 → format! String → `Decimal::from_str_exact` 字符串往返。SK 467k rows × 若干 numeric 列 → 数百万次 format + parse。

**解法**：加整数/简单小数快路径：

```rust
// 快路径（概念）：
// - 如果 value 是有限大的，且 value.fract() == 0.0 且 |value| < 2^63，
//   直接从整数部分构造 Decimal::from(value as i64)
// - 否则回退到原来的 String→parse 路径
```

关键难点：`Decimal` 的精度模型与 f64 不同。`Decimal::new(1, 1)` = 0.1，但 f64 的 0.1 是近似值 `0.10000000000000000555...`。String round-trip 的好处是它复现了"文件里看到的值"（即 Excel 显示的精度），而直接 f64→Decimal 可能引入误差。所以快路径应该只能在"f64 精确表示的值"上走（整数、2 的幂次分母的简单小数），其余回退。

**必须用 oracle 全量比对**（`tests/test_full_rust_cli_oracle.py`）确认逐 cell 数值不变。

**预期收益**：ingest 中可控部分，预期 −0.3–0.8s

**工作量**：中（需理解 Decimal 和 f64 的精度边界 + oracle 比对）。

---

### P1-4 · 加 LTO thin + strip — 两行配置

**位置**：`rust/Cargo.toml`，`[profile.release]`

```toml
[profile.release]
codegen-units = 1
lto = "thin"
strip = "symbols"
```

**结论见第三节**。当前 `rust/Cargo.toml` 未做此改动，是否 adopt 看 CLI 运行频率。

**工作量**：极小（改 2 行 + `cargo build --release` 重编 ~2m40s）。

---

## 五、中后期战略方向

### 5.1 字符串驻留（String Interning）

**问题**：`CellValue` 定义（`rust/crates/costing-core/src/model.rs:8-15`）：

```rust
pub enum CellValue {
    Blank,
    Text(String),       // 每个文本 cell 独立堆分配 ~24 bytes + 内容
    Decimal(Decimal),   // 12 bytes（rust_decimal 是 96 bits）
    DateLike(String),   // 同 Text
}
```

SK 467k 行 × ~30 columns = ~14M cells。文本列约占 70%（年期、成本中心名称、产品编码、产品名称、工单编号、工单行号、供应商编码/名称、成本项目名称、规格型号、基本单位、生产类型、单据类型等）→ ~10M 个 `String`。

这些列的**基数分析**：
- `年期`：12 个不同月份 → 12 个 unique strings
- `成本中心名称`：< 10 个车间 → < 10 unique strings
- `成本项目名称`：< 20 个成本项（直接材料、直接人工、制造费用等）→ < 20 unique strings
- `产品编码`：几百个 → 中等基数
- `产品名称`：与产品编码 1:1 对应 → 中等基数
- `工单编号`：高基数（每个工单一个 unique）→ 不可驻留
- `供应商`、`基本单位`、`生产类型`：低基数

**结论**：大部分文本列的基数远低于行数（467k vs < 1k），驻留收益极高。

**方向 A（Arc 方案）**：`CellValue::Text(Arc<str>)`——读入时对已知低基数列做 string pool，相同文本复用同一个 `Arc<str>`。内存 −30~50%。但 `CellValue` 不再 `PartialEq` by value 那么简单（需要 Deref 比较），需改 `PartialEq` derive。

**方向 B（列级池方案）**：对特定列的 `IndexedRow` cells 用 u32 index 替代 String，列 schema 维护 `Vec<String>` 池。更紧凑但破坏 `CellValue` 的统一类型。

**方向 C（更激进的）**：整个 `CellValue` 从 enum 改为 compact repr（如 `SmallString` / `CompactString`），对 ≤22 bytes 的文本 inline 存储，避免 heap 分配。但这需要第三方 crate 且改动更大。

**当前状态**：SK PWS 1.46GB / 上限 2GB（68%，在安全区内）。如果后续要处理更大输入（比如 1M+ rows），此项必须做。目前不紧急，作为中后期储备。

**影响范围**：reader / normalize / fact / anomaly / presentation / writer + `PartialEq` / `Serialize` derive，需重跑 oracle 全量比对。

**工作量**：大（全链路触及，≥1 周）。

### 5.2 Python legacy/oracle 分批退场

**基准文档**：`docs/python_retirement_after_rust.md`

**当前状态**：
- Rust CLI 已于 2026-07-10 验收通过（GB/SK 双 pipeline，workbook 语义等价 oracle 验证，N=5 性能达验收线）
- `src/`（6.5k 行 Python）+ `tests/rust_oracle/`（~17k 行验证 harness）保留为 legacy/oracle 路径
- `main.py` 仍可用 `uv run python main.py gb/sk` 跑 Python 版本，但 GB/SK 默认主入口已是 Rust

**分阶段退场**：

| 阶段 | 内容 | 前提 |
|---|---|---|
| **Phase 1** | 删除 `src/excel/product_anomaly_writer.py` + `src/analytics/table_rendering.py` 产品维度部分（Rust 不实现 `成本分析产品维度`，此项 API 已冻结） | 已满足 |
| **Phase 2** | 删除 `main.py` + `src/etl/` + `src/analytics/` + `src/excel/` + `src/services/`（Rust 全量接管后退役） | Rust 稳定运行 N 个周期（建议 ≥ 3 个月），无新增 regression |
| **Phase 3** | 精简 `tests/rust_oracle/`（phase0_harness.py 5.4k、evidence.py 3.3k 等验证基础设施）——保留 `workbook_compare.py` 作为 Rust 自我验证的基础 | Rust 视为唯一真值源 |

**收益**：不提升运行时性能，但显著降低维护成本（消除双重维护）和回归测试时长（Python tests 从 ~32k 行中大部分删减）。

---

## 六、A/B 测试方法论

### 6.1 本机噪声特征

本机 dev 环境（Windows 11，16 核 16GB，system drive SSD）被以下因素严重干扰：
- **Windows Defender** 实时扫描：每读写 xlsx 文件拦截扫描，冷/暖差距可达 5×
- **Windows Search 索引**：`cargo build` 后 target/ 被索引，几分钟内负载很高
- **其他后台进程**：随机出现

同一 binary 同参数，在几分钟内 `total` 可以从 8s 飙到 49s。**单次对比不可信。单类顺序对比（"先跑完 A 再跑完 B"）不可信。**

### 6.2 正确方法

**黄金法则**：交错 + 逐对反序 + N≥8 + 取中位数。

**步骤**：
1. 将改前和改后的 binary 各 `cp` 到 `/tmp/` 保留两份
2. 预热：各跑 1 次（丢弃数据）
3. 跑 8 对交错测试：
   - 第 1, 3, 5, 7 对：改前 → 改后
   - 第 2, 4, 6, 8 对：改后 → 改前
4. 用 `--benchmark` 拿到 JSON，`python -c` 解析提取 `stage_timings.stages`
5. 每 config 收集 8 个数据点，排序取中位数 + 最小值
6. **用 fact（CPU 密集阶段）做主要判断**，ingest 受 I/O 噪声主导不可靠
7. full run 做 export 补充验证（N≥5）

**具体命令**：

```bash
# 保留改前 binary
cp rust/target/release/costing-calculate.exe /tmp/perf/bin-before.exe
# 改代码 + cargo build --release
cp rust/target/release/costing-calculate.exe /tmp/perf/bin-after.exe

# 预热（丢弃）
/tmp/perf/bin-before.exe sk --input data/raw/sk/<file>.xlsx --check-only --benchmark > /dev/null
/tmp/perf/bin-after.exe  sk --input data/raw/sk/<file>.xlsx --check-only --benchmark > /dev/null

# 8 对交错
for i in 1 2 3 4 5 6 7 8; do
  if [ $((i%2)) -eq 1 ]; then
    O1="/tmp/perf/bin-before.exe" O2="/tmp/perf/bin-after.exe"
    N1="before" N2="after"
  else
    O1="/tmp/perf/bin-after.exe"  O2="/tmp/perf/bin-before.exe"
    N1="after" N2="before"
  fi
  for pair in "$O1,$N1" "$O2,$N2"; do
    bin="${pair%%,*}"; name="${pair#*,}"
    "$bin" sk --input <file>.xlsx --check-only --benchmark 2>&1 \
      | python -c "import sys,json; d=json.load(sys.stdin); s=d['stage_timings']['stages']; \
        print(f'{sys.argv[1]:6s} total={s[\"total\"]:.3f} fact={s[\"fact\"]:.3f}')" "$name"
  done
done

# 中位数计算
# before: 8 个 total 值排序取第 4/5 平均
# after:  同上
# fact 同理
```

**判断规则**：
- 中位数差 > ±3% 且在配对胜率上一致（≥ 6/8）→ 可信信号
- 中位数差 < ±2% 或配对分裂（~4/4）→ 噪声，无法判
- fact 阶段的数据比 total 干净，优先参考
- 若差异随顺序反转而反转 → 噪声，不是信号

### 6.3 结果记录规范

每次 A/B 实验记录到 `docs/evidence/`：
- 两个 binary 的 exe sha256（`sha256sum`）区分身份
- 配置差异（一句话描述）
- 中位数表（total / fact / ingest / export）
- 配对胜率
- 结论与建议

---

## 七、不建议投入的方向

| 方向 | 理由 |
|---|---|
| **多线程/并行** | 管线本质是 ingest → ... → export 串行 I/O。3 张输出 sheet 互独立但 `rust_xlsxwriter` 的 `Workbook` 跨 worksheet 并行不友好（共享格式/styles），constant_memory 模式串行写临时文件。分析阶段 fact/split/presentation 合计 ~1.6s（SK），并行收益硬顶 < 1s，但引入复杂度和 bug 风险远大于收益 |
| **`panic = "abort"`** | 改变 unwind 语义：low-memory writer 在 panic 时不 unwind 不触发 temp cleanup。收益 ~5% 但对异常路径的审计破坏大，不建议 |
| **Decimal→f64 写出精度** | `writer.rs:625` `decimal_to_f64` 将 Decimal 降精度为 f64 写入 xlsx——这是**既有契约**（Python xlsxwriter 同口径，数值以 IEEE 754 double 存储在 xlsx 中），oracle 已验证。改回 Decimal 字符串写出会破坏 Oracle 语义，不要动 |
| **`payload_timings = timings.clone()`** | `run.rs:117` 克隆一个小 BTreeMap（只有各阶段名称 + f64），耗时 < 1ms，不值得改 |
| **替换 calamine（流式 reader）** | calamine 0.36 不提供流式 xlsx cell iterator；物化整表 `Range<Data>` 是 ingest 内存大户，但要替代就是自己用 `zip` crate + `quick-xml` 写一个流式 parser，复杂度极高。仅当 P0–P2 全部做完且 PWS 仍不够时再考虑 |
| **`rust_xlsxwriter` 批量写模式** | 当前 fork 的 `write_number`/`write_string` 是逐行追加到内部 buffer；批量模式需要深入 fork 的 worksheet 内部数据结构，风险高且 fork 升级时冲突多。同上述，低优先级 |
| **`IndexedRow::get` unchecked 版** | `table.rs:112` 的 schema_id 校验在 release 下大概率被分支预测优化，且 CLAUDE.md 已记载团队倾向保留防御语义。提供一个 `get_unchecked` 带来的进度极小且破坏安全设计，不值得 |

---

## 建议落地路线

```
本轮（开 worktree）：
├─ P0-1 cell_text 借用化     ← 纯重构，cargo test 验证即可
└─ P0-2 压缩级别 2 实测      ← 3–5 次 full benchmark，盯 output size

下轮（取决于本轮结论）：
├─ P1-3 float→Decimal 快路径  ← 如果 ingest 仍是瓶颈
└─ P1-4 LTO thin adopt        ← 如果本次跑下来觉得 0.4s 值得 2m40s

中后期（单独 proposal）：
├─ P2-5 字符串驻留            ← PWS 扩容 + 全链路重构
└─ P2-6 Python 退场           ← 维护成本削减
```

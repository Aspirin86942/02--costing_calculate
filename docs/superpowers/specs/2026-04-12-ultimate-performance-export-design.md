# 终极性能优化方案设计（全链路）

## 1. 背景

当前正在实施的轻量 Excel 导出优化（`2026-04-12-lightweight-excel-export-design.md`）采用 xlsxwriter + 条件格式方案，预期可将导出耗时从 230 秒降至 30-60 秒。

但在实际生产场景中，单个成本计算单文件可能包含 **20 万行以上**的成本明细数据。基于当前实测数据（38s 读取 + 10s 计算 + 230s 导出），即使导出层优化到 30 秒，总耗时仍需 78 秒。

**全链路瓶颈分析**：

### 1.1 读取阶段（38 秒）

- `openpyxl` 读取 20 万行 Excel 需要构建完整 DOM 树
- 每个单元格都是 Python 对象，内存占用高
- 读取后还需要做列名清洗、类型推断

### 1.2 计算阶段（10 秒）

- `fact_builder.py:278` 使用 `iterrows()` 遍历 grouped 结果（热点）
- `analytics/table_rendering.py` 中有多处 `apply()` 调用
- Decimal 精度计算虽然正确，但比 float 慢 10-50 倍
- pandas 的 groupby + agg 对大数据集有对象分配开销

### 1.3 导出阶段（230 秒 → 30 秒）

- 当前优化方案已经解决大部分问题
- 但 pandas → xlsxwriter 的数据转换仍有开销

本文档描述一个**终极性能方案**，目标是将**读取、计算、导出**三个阶段都优化到极致，实现 20 万行数据在 **20 秒以内**完成全流程处理。

## 2. 目标

本方案的目标是：

- **读取阶段**：从 38 秒降至 **8-10 秒**
- **计算阶段**：从 10 秒降至 **2-3 秒**
- **导出阶段**：从 230 秒降至 **10-15 秒**
- **总耗时**：从 278 秒（4 分 38 秒）降至 **20-28 秒以内**
- **内存占用**：降低 60% 以上（从 2GB 降至 800MB 以内）
- 保持与当前方案相同的业务输出（sheet 数量、数据完整性、异常高亮）
- 不改变业务计算口径和数据契约

## 3. 非目标

本方案明确不作为目标的事项：

- 不改变最终交付格式（仍然是 `.xlsx`）
- 不重构 ETL 业务计算逻辑（读取、清洗、聚合、异常分析保持不变）
- 不引入 Rust/C++ 等编译型语言（保持纯 Python 实现）
- 不改为异步导出或后台任务（保持同步导出流程）

## 4. 核心技术选型

### 4.1 Polars 替代 pandas（全链路）

**为什么选择 Polars**：

- 基于 Rust + Apache Arrow，DataFrame 操作比 pandas 快 5-10 倍
- 内存占用降低 50-60%（Arrow 列式存储 vs pandas 行式存储）
- 原生支持并行计算（自动利用多核 CPU）
- `iter_rows()` 直接返回 tuple，无 Python 对象转换开销
- 与 pandas 互操作零拷贝（`polars.from_pandas()` 只是换视图）

**全链路使用策略**：

- **读取层**：使用 `polars.read_excel()` 替代 `openpyxl`（基于 calamine 引擎，比 openpyxl 快 3-5 倍）
- **计算层**：核心聚合、分组、异常判定改用 Polars 表达式（lazy evaluation + 查询优化）
- **导出层**：使用 `polars.iter_rows()` + xlsxwriter 原生流式写入

**为什么全链路迁移**：

- 如果只在导出层用 Polars，读取和计算阶段仍是瓶颈
- Polars 的 lazy evaluation 可以跨阶段优化（如 filter pushdown、projection pushdown）
- 一次性迁移比分阶段迁移的总工作量更小

### 4.2 xlsxwriter 原生流式写入

**为什么不用 pandas.to_excel()**：

- `pandas.to_excel()` 会先构建完整的内存结构，再序列化
- 对 20 万行数据，会产生大量临时 Python 对象
- 无法利用 `constant_memory` 模式的流式写入优势

**为什么用原生 API**：

- 直接调用 `worksheet.write_row()` 流式写入
- `constant_memory` 模式下，写完的行立即序列化到磁盘，内存恒定
- 避免 pandas → openpyxl/xlsxwriter 的中间转换层开销

### 4.3 条件格式替代逐格着色

这一点与当前优化方案一致，但在终极方案中更为关键：

- 20 万行 × 8 对异常列 = 160 万次单元格访问
- 逐格着色需要 160 万次 Python 函数调用 + 样式对象序列化
- 条件格式只需写入 8 条规则，由 Excel 引擎执行

## 5. 架构设计

### 5.1 数据流

```
原始 Excel
    ↓
【读取层】polars.read_excel() (calamine 引擎，8-10 秒)
    ↓
Polars LazyFrame (列名清洗、类型推断)
    ↓
【计算层】Polars 表达式 (聚合、分组、异常判定，2-3 秒)
    ↓
Polars DataFrame (collect 后的结果)
    ↓
【导出层】xlsxwriter 原生流式写入 (constant_memory 模式，10-15 秒)
    ↓
最终 .xlsx (总耗时 20-28 秒)
```

**关键点**：

- 全链路使用 Polars，避免 pandas ↔ Polars 转换开销
- Lazy evaluation 允许跨阶段查询优化
- 内存占用恒定（Arrow 列式存储 + constant_memory 写入）

### 5.2 模块职责

#### A. 需要重构的模块（全链路 Polars 化）

**读取层**：

- `src/excel/reader.py`：新增 `load_raw_workbook_polars()` 方法
  - 使用 `polars.read_excel(engine='calamine')` 替代 `openpyxl`
  - 返回 `polars.LazyFrame` 而不是 `pd.DataFrame`

**计算层**：

- `src/analytics/fact_builder.py`：核心聚合逻辑改用 Polars 表达式
  - `build_fact_table()` 中的 `iterrows()` 改为 Polars `group_by().agg()`
  - Decimal 计算保留在关键金额字段，其他字段用 float64
- `src/analytics/qty_enricher.py`：聚合逻辑改用 Polars
  - `groupby().agg()` 改为 Polars 原生表达式
  - 避免 `apply()` 和 `iterrows()`
- `src/analytics/anomaly.py`：异常判定改用 Polars 条件表达式
  - `df.apply(lambda row: ...)` 改为 `pl.when().then().otherwise()`

**导出层**：

- 新增 `src/excel/fast_writer.py`：终极性能导出器
  - 接收 `polars.DataFrame` 而不是 `pd.DataFrame`
  - 使用 `pl.iter_rows()` + xlsxwriter 原生流式写入

#### B. 保持不变的模块

以下模块的业务逻辑保持不变，只是底层从 pandas 换成 Polars：

- `src/etl/costing_etl.py`：主流程编排
- `src/etl/pipeline.py`：清洗、拆表逻辑
- `src/analytics/contracts.py`：数据契约定义

#### C. 兼容层设计

为了降低迁移风险，增加一个兼容层：

```python
# src/compat/polars_pandas.py
"""Polars ↔ pandas 兼容层，用于渐进式迁移。"""

def to_pandas_if_needed(df: pl.DataFrame | pd.DataFrame) -> pd.DataFrame:
    """如果是 Polars DataFrame，转为 pandas。"""
    if isinstance(df, pl.DataFrame):
        return df.to_pandas()
    return df

def to_polars_if_needed(df: pd.DataFrame | pl.DataFrame) -> pl.DataFrame:
    """如果是 pandas DataFrame，转为 Polars。"""
    if isinstance(df, pd.DataFrame):
        return pl.from_pandas(df)
    return df
```

这样可以支持：

- 部分模块先迁移到 Polars，其他模块仍用 pandas
- 通过环境变量控制是否启用 Polars 路径
- 如果 Polars 路径出问题，可以快速回退到 pandas

### 5.3 依赖变化

需要新增依赖：

```toml
# pyproject.toml
dependencies = [
    "pandas>=2.0.0",      # 保留（用于兼容层和回退）
    "polars[all]>=1.0.0", # 新增（包含 calamine 引擎）
    "xlsxwriter>=3.2.0",  # 已有（当前优化方案已引入）
    "openpyxl>=3.1.0",    # 保留（用于回退）
    "numpy>=1.24.0",
    "beautifulsoup4>=4.12.0",
]
```

**风险评估**：

- Polars 是纯 Python 包（通过 PyO3 绑定 Rust），无需编译环境
- `polars[all]` 包含 calamine 引擎（Excel 读取）和其他可选依赖
- 安装体积约 30MB，对部署环境无特殊要求
- 与 pandas 互操作成熟，不会引入兼容性问题
- 保留 pandas 和 openpyxl 依赖，用于兼容层和回退

## 6. 核心实现要点

### 6.1 读取层优化

使用 Polars 的 `calamine` 引擎读取 Excel：

```python
# src/excel/reader.py
import polars as pl

def load_raw_workbook_polars(file_path: Path, skip_rows: int = 2) -> pl.LazyFrame:
    """使用 Polars 读取原始 Excel（比 openpyxl 快 3-5 倍）。"""
    # calamine 是 Rust 实现的 Excel 解析器，比 openpyxl 快得多
    df = pl.read_excel(
        file_path,
        sheet_name=0,  # 读取第一个 sheet
        engine='calamine',
        read_options={'skip_rows': skip_rows},
    )
    
    # 返回 LazyFrame，延迟执行（允许查询优化）
    return df.lazy()
```

**性能关键点**：

- `calamine` 引擎基于 Rust，比 Python 的 openpyxl 快 3-5 倍
- 返回 `LazyFrame` 而不是 `DataFrame`，允许跨阶段查询优化
- 列名清洗、类型推断可以在 lazy 阶段完成，避免中间物化

### 6.2 计算层优化

#### A. 消除 iterrows() 热点

当前代码 `fact_builder.py:278` 使用 `iterrows()` 遍历 grouped 结果：

```python
# 当前实现（慢）
for _, row in grouped.iterrows():
    for cost_bucket, amount_column in bucket_map.items():
        amount = row[amount_column]
        qty = row['qty']
        rows.append({...})
```

改为 Polars 表达式（快 10-20 倍）：

```python
# 终极实现（快）
fact_df = (
    work_order_df
    .group_by(['product_code', 'product_name', 'period'])
    .agg([
        pl.col('dm_amount').sum().alias('dm_amount'),
        pl.col('dl_amount').sum().alias('dl_amount'),
        pl.col('moh_amount').sum().alias('moh_amount'),
        pl.col('completed_qty').sum().alias('qty'),
    ])
    .unpivot(
        index=['product_code', 'product_name', 'period', 'qty'],
        on=['dm_amount', 'dl_amount', 'moh_amount'],
        variable_name='cost_bucket',
        value_name='amount',
    )
    .with_columns([
        pl.col('cost_bucket').str.replace('_amount', ''),
        (pl.col('amount') / pl.col('qty')).alias('price'),
    ])
)
```

**性能关键点**：

- `unpivot()` 替代 Python 循环，由 Rust 引擎执行
- 所有计算都是向量化的，无 Python 对象分配
- 自动并行执行（利用多核 CPU）

#### B. 消除 apply() 调用

当前代码中有多处 `df.apply(lambda row: ...)`，改为 Polars 条件表达式：

```python
# 当前实现（慢）
df['异常标记'] = df.apply(
    lambda row: '高度可疑' if row['单位成本'] > threshold * 2 else '关注' if row['单位成本'] > threshold else '',
    axis=1
)

# 终极实现（快）
df = df.with_columns([
    pl.when(pl.col('单位成本') > threshold * 2)
      .then(pl.lit('高度可疑'))
      .when(pl.col('单位成本') > threshold)
      .then(pl.lit('关注'))
      .otherwise(pl.lit(''))
      .alias('异常标记')
])
```

**性能关键点**：

- `when().then().otherwise()` 是向量化的，比 `apply()` 快 50-100 倍
- 无 Python 函数调用开销
- 自动利用 SIMD 指令

#### C. Decimal vs float64 权衡

当前代码全部使用 `Decimal` 保证精度，但 Decimal 比 float64 慢 10-50 倍。

**优化策略**：

- **关键金额字段**（如 `本期完工金额`、`单位成本`）保留 Decimal
- **中间计算字段**（如 `price`、`qty`）使用 float64
- 在最终输出前，将 float64 转回 Decimal 并格式化

```python
# 中间计算用 float64
fact_df = fact_df.with_columns([
    (pl.col('amount').cast(pl.Float64) / pl.col('qty').cast(pl.Float64)).alias('price')
])

# 导出前转回 Decimal（仅关键字段）
fact_df = fact_df.with_columns([
    pl.col('amount').map_elements(lambda x: Decimal(str(x)), return_dtype=pl.Object).alias('amount')
])
```

### 6.3 导出层优化

对 `成本明细`、`产品数量统计`、`error_log` 等大 sheet，使用流式写入：

```python
def _write_large_dataframe(
    self,
    sheet_name: str,
    df: pd.DataFrame,
    *,
    numeric_columns: set[str],
) -> None:
    """流式写入大 DataFrame（热点路径）。"""
    # 1. 零拷贝转换为 Polars
    pl_df = pl.from_pandas(df)
    
    worksheet = self.workbook.add_worksheet(sheet_name)
    
    # 2. 写表头
    worksheet.write_row(0, 0, pl_df.columns, self.formats['header'])
    
    # 3. 流式写数据（关键：iter_rows 直接返回 tuple）
    for row_idx, row_tuple in enumerate(pl_df.iter_rows(), start=1):
        worksheet.write_row(row_idx, 0, row_tuple)
    
    # 4. 批量设置数字列格式（不逐单元格）
    for col_idx, col_name in enumerate(pl_df.columns):
        if col_name in numeric_columns:
            col_letter = xl_col_to_name(col_idx)
            worksheet.set_column(f'{col_letter}:{col_letter}', 12, self.formats['number'])
    
    # 5. 冻结窗格 + 筛选
    worksheet.freeze_panes(1, 0)
    if len(pl_df) > 0:
        worksheet.autofilter(0, 0, len(pl_df), len(pl_df.columns) - 1)
```

**性能关键点**：

- `pl_df.iter_rows()` 比 `pd_df.itertuples()` 快 2-3 倍（无 Python 对象转换）
- `worksheet.write_row()` 比逐单元格 `write()` 快 3-5 倍
- `set_column()` 批量设置格式，比逐单元格快 100 倍

### 6.2 条件格式高亮

对 `按工单按产品异常值分析` sheet，使用条件格式替代逐格着色：

```python
def _apply_conditional_highlights(
    self,
    worksheet: xlsxwriter.worksheet.Worksheet,
    columns: list[str],
    data_rows: int,
) -> None:
    """应用条件格式高亮（核心性能优化点）。"""
    header_map = {col_name: idx for idx, col_name in enumerate(columns)}
    
    for value_col, flag_col in WORK_ORDER_HIGHLIGHT_COLUMNS:
        value_idx = header_map.get(value_col)
        flag_idx = header_map.get(flag_col)
        if value_idx is None or flag_idx is None:
            continue
        
        value_col_letter = xl_col_to_name(value_idx)
        flag_col_letter = xl_col_to_name(flag_idx)
        
        # 关注：黄色
        worksheet.conditional_format(
            f'{value_col_letter}2:{value_col_letter}{data_rows + 1}',
            {
                'type': 'formula',
                'criteria': f'=${flag_col_letter}2="关注"',
                'format': self.formats['attention'],
            },
        )
        
        # 高度可疑：红底白字
        worksheet.conditional_format(
            f'{value_col_letter}2:{value_col_letter}{data_rows + 1}',
            {
                'type': 'formula',
                'criteria': f'=${flag_col_letter}2="高度可疑"',
                'format': self.formats['suspicious'],
            },
        )
```

**性能关键点**：

- 20 万行只需写入 8 × 2 = 16 条规则
- 替代了 20 万 × 8 × 2 = 320 万次单元格访问
- 高亮由 Excel 引擎执行，打开文件时才计算

### 6.3 格式对象复用

所有格式对象在 workbook 初始化时集中创建：

```python
def _init_formats(self) -> None:
    """预创建所有格式对象（只创建一次）。"""
    wb = self.workbook
    self.formats = {
        'header': wb.add_format({'bold': True, 'bg_color': '#D9D9D9', 'align': 'center'}),
        'number': wb.add_format({'num_format': '#,##0.00', 'align': 'right'}),
        'section_title': wb.add_format({'bold': True, 'bg_color': '#4472C4', 'font_color': '#FFFFFF'}),
        'attention': wb.add_format({'bg_color': '#FFFF00'}),
        'suspicious': wb.add_format({'bg_color': '#FF0000', 'font_color': '#FFFFFF'}),
    }
```

**性能关键点**：

- 禁止在行循环或单元格循环内创建格式对象
- xlsxwriter 内部会对格式对象做哈希去重，但创建本身有开销
- 预创建可以避免 20 万次格式对象创建

### 6.4 删除所有边框

与当前优化方案一致，删除所有 `cell.border = THIN_BORDER`：

- 20 万行 × 10 列 = 200 万次边框赋值
- 每次赋值需要序列化边框样式对象
- 删除后可节省约 30-40 秒

## 7. 性能预期

### 7.1 理论分析

| 优化点 | 当前方案 | 终极方案 | 提速倍数 |
|--------|---------|---------|---------|
| DataFrame 迭代 | pandas.itertuples | polars.iter_rows | 2-3x |
| 单元格写入 | write() 逐格 | write_row() 批量 | 3-5x |
| 异常高亮 | 逐格着色 | 条件格式 | 100x+ |
| 内存占用 | 1.5GB | 800MB | 0.5x |

### 7.2 预期耗时（20 万行）

| 阶段 | 当前实现 | 近期优化（xlsxwriter） | 终极方案（全链路 Polars） |
|------|---------|---------------------|----------------------|
| 读取 | 38 秒 | 38 秒 | **8-10 秒** |
| 计算 | 10 秒 | 10 秒 | **2-3 秒** |
| 导出 | 230 秒 | 30-60 秒 | **10-15 秒** |
| **总计** | **278 秒** | **78-108 秒** | **20-28 秒** |

**保守估计**：总耗时降至 30 秒以内，相比当前实现提速 **9-10 倍**。

### 7.3 实测验证要求

实施时必须记录以下指标：

- **分阶段耗时**：读取、计算、导出各阶段耗时（精确到毫秒）
- **分 sheet 耗时**：各 sheet 写出耗时（用于定位热点）
- **内存峰值**：使用 `tracemalloc` 或 `memory_profiler` 记录
- **CPU 利用率**：验证 Polars 是否正确利用多核
- **磁盘 I/O**：使用 `time.perf_counter()` 对比 CPU 时间和墙钟时间

验收标准：

- **总耗时**：20 万行样本 < 30 秒（目标 20-28 秒）
- **内存峰值**：< 1GB（目标 800MB）
- **数据一致性**：所有 sheet 行数、关键金额列总和与当前方案一致
- **异常高亮**：工单异常页的黄色/红色高亮正确显示
- **回归测试**：`pytest` 全量通过

## 8. 风险与缓解

### 8.1 主要风险

**风险 1：Polars 与 pandas 互操作兼容性**

- 风险描述：某些 pandas 特有的数据类型（如 `pd.Categorical`、`pd.Period`）可能无法直接转换
- 缓解措施：在转换前做类型检查，必要时先转为基础类型（`str`、`float`、`int`）

**风险 2：条件格式公式引用错误**

- 风险描述：如果公式引用写错（如相对引用变成绝对引用），会导致整列高亮失效
- 缓解措施：增加条件格式验证函数，在 workbook 保存前检查规则数量和应用范围

**风险 3：xlsxwriter constant_memory 模式的顺序写约束**

- 风险描述：`constant_memory` 模式要求按行号递增顺序写入，不能跳行或回头修改
- 缓解措施：确保所有 sheet 都是"先写表头，再按行顺序写数据"，不做回溯修改

**风险 4：精确耗时改善幅度不确定**

- 风险描述：实际提速受磁盘写入、ZIP 压缩和 Python 对象转换开销影响
- 缓解措施：不承诺固定秒数，只承诺"明显快于当前方案"，并以实测数据为准

### 8.2 回滚预案

如果终极方案出现问题，可以快速回退：

```bash
# 回退到当前优化方案
USE_FAST_WRITER=false python -m src.etl.costing_etl
```

保留当前优化方案的代码不删除，作为稳定基线。

## 9. 实施边界

### 9.1 允许修改的文件

**读取层**：
- 新增 `src/excel/reader_polars.py`（Polars 读取器）
- 修改 `src/excel/reader.py`（增加引擎切换逻辑）

**计算层**：
- 修改 `src/analytics/fact_builder.py`（消除 iterrows，改用 Polars 表达式）
- 修改 `src/analytics/qty_enricher.py`（消除 apply，改用 Polars 聚合）
- 修改 `src/analytics/anomaly.py`（消除 apply，改用 Polars 条件表达式）
- 修改 `src/analytics/table_rendering.py`（改用 Polars）

**导出层**：
- 新增 `src/excel/fast_writer.py`（终极导出器）
- 修改 `src/excel/workbook_writer.py`（增加引擎切换逻辑）

**兼容层**：
- 新增 `src/compat/polars_pandas.py`（Polars ↔ pandas 兼容层）

**配置**：
- 修改 `pyproject.toml`（新增 polars 依赖）

### 9.2 不允许修改的内容

- **业务口径**：异常判定阈值、金额聚合规则、产品白名单
- **数据契约**：字段名、sheet 名称、输出行数
- **测试基线**：`tests/contracts/baselines/` 中的真值（除非为了反映"性能优化但数据不变"的新预期）

### 9.3 测试要求

必须通过以下测试：

- `pytest` 全量通过
- 真实 `gb`、`sk` 样本都能成功导出
- 导出的 9 张 sheet 都存在
- 关键业务数据与当前方案一致（使用 `pd.read_excel` 读回来对比）
- `按工单按产品异常值分析` 的异常高亮存在（手工打开 Excel 验证）

## 10. 后续演进路径

如果终极方案实施后，性能仍不满足需求（如需要 5 秒以内导出 20 万行），可以考虑：

### 10.1 Rust 原生导出（需要编译环境）

- 使用 `rust_xlsxwriter` + PyO3 封装
- 预期耗时：5-8 秒
- 但需要 Rust 编译环境，部署复杂度高

### 10.2 先导出 Parquet，按需转 Excel

- 导出 Parquet：2 秒
- 用户需要时再转 Excel：`polars.read_parquet().write_excel()`
- 但改变了交付格式，需要业务方接受

### 10.3 异步导出 + 进度通知

- 导出任务放入后台队列
- 用户可以继续其他工作，导出完成后通知
- 但需要引入任务队列（如 Celery），架构复杂度高

## 11. 实施策略

### 11.1 渐进式迁移路径

为了降低风险，建议分三个阶段实施：

**阶段 1：导出层 Polars 化（低风险）**
- 只改 `src/excel/fast_writer.py`
- 上游仍用 pandas，导出前做一次 `pl.from_pandas()` 转换
- 预期提速：230 秒 → 10-15 秒（导出阶段）
- 总耗时：278 秒 → 58-63 秒

**阶段 2：读取层 Polars 化（中风险）**
- 改 `src/excel/reader.py`，使用 `polars.read_excel()`
- 读取后转为 pandas，保持计算层不变
- 预期提速：38 秒 → 8-10 秒（读取阶段）
- 总耗时：58-63 秒 → 28-33 秒

**阶段 3：计算层 Polars 化（高风险）**
- 改 `src/analytics/` 下所有模块
- 全链路使用 Polars，无 pandas 转换
- 预期提速：10 秒 → 2-3 秒（计算阶段）
- 总耗时：28-33 秒 → **20-28 秒**

### 11.2 回退预案

每个阶段都保留环境变量开关：

```bash
# 阶段 1：启用 Polars 导出
USE_POLARS_WRITER=true python -m src.etl.costing_etl

# 阶段 2：启用 Polars 读取
USE_POLARS_READER=true python -m src.etl.costing_etl

# 阶段 3：全链路 Polars
USE_POLARS_FULL=true python -m src.etl.costing_etl

# 回退到 pandas
USE_POLARS_FULL=false python -m src.etl.costing_etl
```

## 12. 总结

本方案是一个**终极性能方案**，目标是将 20 万行数据的全流程处理从 4 分 38 秒降至 **20-28 秒以内**。

核心优势：

- **全链路优化**：读取、计算、导出三个阶段都优化到极致
- **提速 9-10 倍**：从 278 秒降至 20-28 秒
- **内存降低 60%**：从 2GB 降至 800MB
- **保持纯 Python**：无需 Rust 编译环境
- **渐进式迁移**：分三个阶段实施，每个阶段都可独立验收和回退

实施时机：

- **近期**：先完成当前优化方案（xlsxwriter + 条件格式）
- **中期**：当业务规模增长到 30 万行以上，或用户明确要求"更快"时
- **长期**：作为技术储备，持续跟进 Polars 生态发展

实施优先级：

- 优先级：**中**（比 Rust 方案更现实，比当前方案更快）
- 建议在当前优化方案实施完成后 3-6 个月内启动
- 优先实施阶段 1（导出层），风险最低、收益最大

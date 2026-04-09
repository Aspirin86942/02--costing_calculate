# 双管线入口与质量日志化设计

## 1. 背景

当前仓库已经存在 `gb` 相关处理流程，且目录层面已经出现了数控数据入口的改动迹象，但 CLI 入口、测试契约和文档仍然围绕单一 `gb` 管线组织，仓库处于“目录已分叉、入口未分叉”的半完成状态。

同时，现有实现还有两处会阻碍新增 `sk` 管线：

- 产品白名单被写死在 [`src/etl/costing_etl.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/etl/costing_etl.py) 的全局常量中，只适用于 `gb`。
- `数据质量校验` 以独立 Sheet 写入 Excel，导致工作簿输出臃肿，不利于日常查看。

本设计用于把仓库收敛成一个明确、可维护的双管线入口，并同步调整质量校验的输出形态。

## 2. 目标

- 新增 `sk` 管线，流程与 `gb` 保持一致。
- 仓库只保留一个正式入口 [`main.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/main.py)。
- 运行时必须显式指定管线，允许值仅为 `gb` 或 `sk`。
- `gb` 与 `sk` 共享同一套 ETL 核心流程，但使用各自独立的产品白名单。
- 移除 Excel 中的 `数据质量校验` Sheet，改为控制台摘要 + 同目录日志文件。
- 全仓统一使用 `sk` 命名，不再保留 `shukong` 目录或文档表述。
- 更新测试、README、AGENTS，使仓库使用方式与实现保持一致。

## 3. 非目标

- 不在本次设计中引入第三条及以上管线。
- 不将白名单外置为可编辑配置文件。
- 不改造现有单个工作簿的 ETL 业务规则。
- 不把 `error_log` 从 Excel 中移除；本次仅移除 `数据质量校验` Sheet。
- 不把“每次处理第一份匹配文件”的行为扩展为批量多文件处理。

## 4. 推荐方案

推荐采用“统一 runner + 管线配置”的方案。

### 4.1 方案概述

- `main.py` 负责解析命令行参数并选择管线。
- 新增统一运行层，负责文件匹配、输出路径生成、日志落地和调用 ETL。
- [`src/etl/costing_etl.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/etl/costing_etl.py) 仅保留单个工作簿 ETL 核心能力，不再作为 CLI 入口。
- `gb` 与 `sk` 的差异集中在“管线配置对象”中，而不是散落在多个入口文件或全局常量里。

### 4.2 选择理由

- 可维护性最好：未来若调整匹配规则、日志格式、输出文件名，只需改统一 runner 或配置层。
- 差异边界清晰：`gb/sk` 只在目录、文件模式、白名单等配置上分叉。
- 能避免复制出第二份 `costing_etl` 入口，减少后续双改风险。

## 5. 目标结构

### 5.1 入口层

新增 [`main.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/main.py) 作为仓库唯一正式入口。

职责：

- 解析必填位置参数 `pipeline`。
- 限制参数值为 `gb` 或 `sk`。
- 将参数交给统一 runner 执行。
- 对用户提供稳定、明确的命令行用法。

建议运行方式：

```bash
python main.py gb
python main.py sk
```

### 5.2 运行层

新增运行层模块，位置定为 [`src/etl/runner.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/etl/runner.py)。

职责：

- 根据管线配置查找输入文件。
- 保持“只处理第一份匹配文件”的现有行为。
- 生成输出 Excel 路径和对应日志路径。
- 调用 `CostingWorkbookETL` 处理单个文件。
- 打印控制台摘要日志。
- 写出完整质量日志文件。

### 5.3 ETL 核心层

保留 [`src/etl/costing_etl.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/etl/costing_etl.py) 作为核心处理模块，但移除其 CLI 入口职责。

职责：

- 加载原始工作簿。
- 完成列识别、清洗、拆分、分析和 Excel 写出。
- 接收管线级配置，例如产品白名单。
- 返回处理结果及质量摘要所需数据。

不再承担：

- 自行查找 `gb` 输入目录。
- 自行决定输出目录。
- 自行作为 `python -m` 入口执行。

## 6. 管线配置设计

新增管线配置对象，用于集中描述 `gb` 与 `sk` 的差异。建议字段至少包括：

- `name`: 管线名，值为 `gb` 或 `sk`
- `raw_dir`: 原始输入目录
- `processed_dir`: 输出目录
- `input_patterns`: 文件匹配模式列表
- `product_order`: 产品白名单及展示顺序
- `quality_log_name`: 质量日志标识或默认文件后缀

### 6.1 `gb` 配置

- 输入目录：`data/raw/gb`
- 输出目录：`data/processed/gb`
- 文件模式：沿用现有 `GB-*成本计算单.xlsx` 相关模式
- 产品白名单：沿用当前 `gb` 版本白名单顺序

### 6.2 `sk` 配置

- 输入目录：`data/raw/sk`
- 输出目录：`data/processed/sk`
- 文件模式：镜像 `gb` 规则，使用 `SK-*成本计算单.xlsx` 相关模式
- 产品白名单：新增为 `sk` 专属白名单，内容与 `gb` 分离

### 6.3 命名统一

仓库中所有目录、文档和测试统一改为 `sk`，不再保留 `shukong` 命名。

## 7. 产品白名单设计

### 7.1 问题

当前白名单通过 `ANALYSIS_PRODUCT_ORDER` 和 `ANALYSIS_PRODUCT_WHITELIST` 全局常量定义，只能服务于单一 `gb` 管线。

### 7.2 设计

将白名单从全局常量改为管线级配置，并在 `CostingWorkbookETL` 初始化时注入。

ETL 内部保留以下行为，但基于注入数据运行：

- 白名单过滤仍然使用 `产品编码 + 产品名称` 双字段精确匹配。
- 分析页产品展示顺序仍然严格遵循对应管线的 `product_order`。
- 过滤逻辑同时作用于事实表、工单异常分析页和产品异常摘要页。

### 7.3 结果

- `gb` 与 `sk` 共用同一套白名单算法。
- `gb` 与 `sk` 各自维护独立产品范围，不互相污染。
- 后续如果仅调整 `sk` 白名单，不需要动 `gb` 业务逻辑。

## 8. 数据质量输出设计

### 8.1 问题

当前 `数据质量校验` 通过 [`src/analytics/quality.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/analytics/quality.py) 生成 `FlatSheet`，再由 [`src/excel/workbook_writer.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/excel/workbook_writer.py) 写入 Excel。该方式会使输出工作簿冗长。

### 8.2 设计目标

- 不再将 `数据质量校验` 写入 Excel。
- 仍然保留质量校验的审计价值。
- 同时满足“命令行快速查看”和“落盘追溯”的需要。

### 8.3 输出形式

质量校验改为两层输出：

- 控制台摘要：
  - 当前管线名
  - 输入文件路径
  - 输出文件路径
  - 输入行数、输出行数
  - 被过滤行数
  - 主键重复数
  - 分析覆盖率
  - `error_log` 条数摘要
- 日志文件完整摘要：
  - 文件路径与处理时间
  - 所有质量指标
  - 固定格式的质量摘要块

日志文件与输出 Excel 同目录、同 stem，扩展名为 `.log`，示例：

- `xxx_处理后.xlsx`
- `xxx_处理后.log`

### 8.4 指标内容

保留当前 `quality_sheet` 的指标语义，包括但不限于：

- 行数勾稽
- 关键字段空值率
- 工单主键重复数
- 分析覆盖率
- 过滤原因计数

### 8.5 Excel 输出调整

移除 `数据质量校验` Sheet 后，默认输出 Sheet 数量由 9 张调整为 8 张，保留：

- `成本明细`
- `产品数量统计`
- 三大类价量分析 Sheet
- `按工单按产品异常值分析`
- `按产品异常值分析`
- `error_log`

`error_log` 继续保留在 Excel 中，因为它是逐行异常明细，不等同于质量摘要。

## 9. CLI 契约

### 9.1 正式入口

仓库唯一正式入口为 [`main.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/main.py)。

### 9.2 参数规则

- 必须显式传入一个位置参数。
- 允许值仅为 `gb` 或 `sk`。
- 缺少参数时应报错并输出用法。
- 非法参数时应报错并显示允许值。

### 9.3 运行行为

- 每次执行只处理第一份匹配输入文件。
- 若未找到匹配文件，输出明确日志并以失败结束，不创建任何输出文件。
- 成功时生成 Excel 与日志文件。
- 失败时输出明确失败信息，不允许静默失败。

### 9.4 旧入口处理

[`src/etl/costing_etl.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/etl/costing_etl.py) 不再保留 CLI 入口契约。

影响：

- 删除或改写旧的脚本模式兼容测试。
- 用户文档不再推荐 `python -m src.etl.costing_etl`。

## 10. 模块改动范围

### 10.1 预计新增

- [`main.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/main.py)
- [`src/etl/runner.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/etl/runner.py)
- [`src/config/pipelines.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/config/pipelines.py)

### 10.2 预计修改

- [`src/etl/costing_etl.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/etl/costing_etl.py)
- [`src/config/settings.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/config/settings.py)
- [`src/analytics/contracts.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/analytics/contracts.py)
- [`src/analytics/quality.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/analytics/quality.py)
- [`src/excel/workbook_writer.py`](D:/03-%20Program/02-%20special/02-%20costing_calculate/src/excel/workbook_writer.py)
- README、AGENTS 和相关测试

## 11. 测试策略

### 11.1 CLI 测试

新增或重写以下测试：

- `main.py` 缺少参数时报错
- `main.py` 非法参数时报错
- `main.py gb` 选择 `gb` 配置
- `main.py sk` 选择 `sk` 配置

### 11.2 文件匹配测试

- `gb` 使用 `GB-*` 模式匹配并保持现有顺序和去重规则
- `sk` 使用 `SK-*` 模式匹配并保持同样顺序和去重规则

### 11.3 白名单测试

- `gb` 仍按 `gb` 白名单过滤
- `sk` 使用自己的白名单
- 两条管线的白名单排序互不影响

### 11.4 Workbook 契约测试

- 从 contract baseline 中移除 `数据质量校验` Sheet
- 保留 `error_log` Sheet 契约
- 保证其余分析 Sheet 语义不变

### 11.5 日志测试

- 控制台包含质量摘要
- 成功处理后生成对应 `.log` 文件
- 日志文件中包含关键质量指标

## 12. 文档更新要求

必须同步更新以下文档：

- [`AGENTS.md`](D:/03-%20Program/02-%20special/02-%20costing_calculate/AGENTS.md)
- [`README.md`](D:/03-%20Program/02-%20special/02-%20costing_calculate/README.md)

更新内容至少包括：

- 目录统一为 `gb/sk`
- 唯一入口改为 `python main.py gb|sk`
- `数据质量校验` 不再写入 Excel
- 质量校验改为控制台摘要 + 同目录日志文件
- 输出 Sheet 数量和名称变化

## 13. 风险与约束

- 旧的 CLI 测试和入口兼容测试将失效，必须同步调整，否则测试会系统性失败。
- 若 `sk` 白名单未及时补齐，`sk` 分析页可能被全部过滤为空。
- 若仅移除 Excel Sheet 但未提供结构化日志，审计可读性会退化。
- 仓库当前存在未提交改动，本次实现阶段需要避免覆盖用户已有修改。

## 14. 验收标准

满足以下条件则视为本设计落地成功：

- 可以通过 `python main.py gb` 和 `python main.py sk` 分别运行两条管线。
- 旧入口不再作为官方使用方式。
- `gb` 与 `sk` 使用独立白名单，且都能正常过滤与排序分析页。
- 输出 Excel 不再包含 `数据质量校验` Sheet。
- 每次处理均输出控制台质量摘要，并在输出目录生成对应 `.log` 文件。
- README 与 AGENTS 的使用说明与实际行为一致。
- 测试覆盖新入口、双管线、白名单和日志输出。

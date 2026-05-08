# 02_data_pipeline（V2）

## 模块为什么存在
数据层的核心职责是把外部行情源转换为可研究、可回测、可复用的标准输入。当前主入口为 `src/qsys/utils/build_real_feature_store.py`。

## 在主流程中的位置
Data 阶段：负责从 AkShare 获取日线数据并标准化，输出后续 Panel/Feature 的基础分区文件。

## 核心协作关系
- `_fetch_symbol_universe`：构建股票池。
- `_safe_fetch_daily`：带重试的数据抓取。
- `_normalize_daily_frame`：字段标准化、衍生收益/波动/流动性列、`is_tradable`。
- `build_real_feature_store`：按 `trade_date=.../data.parquet` 落盘。

## 输入/输出
- 输入：AkShare 数据、symbols、日期过滤、重试参数。
- 输出：parquet 分区数据（研究层统一输入）。

## 设计选择
- 采用“先标准化再分区写入”的简单可追踪流程。
- 使用 `REQUIRED_COLUMNS` 强约束输出字段。

## MVP 与技术债
- MVP：日期过滤+分区写入具备“轻量增量”能力，但不是完整增量调度系统。
- 技术债：缺 raw zone、缺数据质量审计日志、缺 schema version 管理。

## sqlite metadata / incremental update 状态
- sqlite metadata：README 提及，但在当前 `src/qsys` 主链路未见强耦合实现，判定为“文档提及但待验证”。
- incremental update：当前更多是“部分实现”（通过日期过滤和分区增写），不是完整可编排框架。

## 相关测试
- `tests/utils/test_build_real_feature_store.py`

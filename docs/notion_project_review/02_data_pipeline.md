# 02 Data Pipeline

## 数据源与获取
- 真实数据脚本：`qsys/utils/build_real_feature_store.py`，通过 AkShare `stock_zh_a_spot_em` + `stock_zh_a_daily`。
- 含重试：`_safe_fetch_daily(retries, retry_wait)`。

## 标准化与存储
- `_normalize_daily_frame` 统一到字段：`trade_date`, `ts_code`, OHLCV, amount/turnover, ret/vol/fwd_ret, `is_tradable`。
- 输出按 `trade_date=YYYY-MM-DD/data.parquet` 分区。

## parquet / sqlite / metadata
- parquet：已实现并主路径。
- sqlite metadata：README 声明存在历史能力，但当前 `src/qsys` 主流程中未见活跃 sqlite 注册逻辑（需进一步核验 legacy notebook）。

## 数据质量
- 缺失列补 NA、数值强转、日期校验。
- `is_tradable` 基于 open/close/volume/amount 可用性。

## 技术债与改进
- 技术债：数据接入在 util 脚本，不在独立 data ingestion package。
- 建议：增加 schema version、数据完整性审计、增量 watermark。

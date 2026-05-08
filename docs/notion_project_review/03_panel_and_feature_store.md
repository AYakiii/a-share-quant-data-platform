# 03 Panel and Feature Store

## MultiIndex 设计
- 统一索引为 `MultiIndex [date, asset]`，在 `features/compute.py::_sorted_panel`、`signals/transforms.py::_ensure_multiindex`、`universe/eligibility.py::_validate_multiindex` 等处显式校验。

## Panel
- `DailyPanelReader` 从 parquet 根目录读取并过滤日期/股票，`load_daily_panel` 为便捷入口。

## Feature Store
- 两条路径：
  1) 研究内生路径：`features/store.py::materialize_and_store_features`（可结合 `FeatureRegistry`）。
  2) 实盘数据构建路径：`utils/build_real_feature_store.py`（直接生成 v1 字段）。

## 已实现特征
- `ret_1d/5d/20d`, `vol_20d`, `turnover_5d/20d`, `amount_20d`, `market_cap`, `fwd_ret_5d/20d`。

## anti-lookahead
- 特征中 forward return 通过 `shift(-k)`，用于评估标签，不应参与当日可交易信号。
- 组合和约束逻辑均声明 same-date 数据。

## 局限/技术债
- feature store schema 版本治理较弱。
- ingestion 与 feature materialization 边界重叠。

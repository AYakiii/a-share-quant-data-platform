# 03_panel_and_feature_store
## MultiIndex 设计
- 多处强校验 `MultiIndex [date, asset]`：`_sorted_panel`、`_ensure_multiindex`、`_validate_multiindex`。

## Panel 构建
- `qsys.data.panel.daily_panel.DailyPanelReader` 从分区 parquet 加载并标准化索引。

## Feature store 设计
- 特征定义：`BaseFeature`、`FunctionFeature`、`FeatureRegistry`。
- 物化：`compute_features` + `store.materialize_and_store_features`。
- 分区：按 `trade_date` 写 parquet（`write_feature_store`）。
- 加载：`load_feature_store_frame`。

## anti-lookahead
- `fwd_ret_5d`/`fwd_ret_20d` 通过 `shift(-5/-20)` 构造标签；组合构建使用 same-date 信息（`build_top_n_portfolio` 注释和实现）。

## 已实现特征
`ret_1d/5d/20d`、`vol_20d`、`turnover_5d/20d`、`amount_20d`、`market_cap`、`fwd_ret_5d/20d`。

## 局限与技术债
- 特征版本管理较轻（缺 registry version pinning）。
- 缺训练/验证切分协议与特征可用性时间戳。

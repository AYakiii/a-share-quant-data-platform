# 01 Architecture Map

## 架构主链
Data (`qsys/utils/build_real_feature_store.py`) → Panel (`qsys/data/panel/daily_panel.py`) → Feature (`qsys/features/compute.py`, `qsys/features/store.py`) → Signal (`qsys/signals/engine.py`) → Portfolio/Backtest (`qsys/backtest/*`, `qsys/rebalance/*`) → Diagnostics (`qsys/research/*`)。

## 关键模块与职责
- `qsys/data`: 读取日频面板，`DailyPanelReader`/`load_daily_panel`。
- `qsys/features`: 特征定义注册与物化，`FeatureRegistry`, `default_feature_registry`, `materialize_and_store_features`。
- `qsys/signals`: 横截面变换与组合，`winsorize_cross_section`, `zscore_cross_section`, `rank_cross_section`, `SignalEngine`。
- `qsys/backtest`: strict top-N 组合与回测，`build_top_n_portfolio`, `run_backtest_from_signal`。
- `qsys/rebalance`: buffered top-N 与基准、诊断。
- `qsys/research`: IC/quantile/decay/exposure/constraint impact 等。

## 实现状态
- 已实现：模块化研究链路完整可跑。
- 部分实现：reporting、benchmark 归因、风险归因。
- 尚未实现：完整优化器 + 风险模型。

## 架构优劣
- 优点：函数粒度清晰、test 覆盖较多。
- 局限：MVP 与长期架构边界未完全制度化；同类功能分散在 `backtest` 与 `rebalance` 双栈。

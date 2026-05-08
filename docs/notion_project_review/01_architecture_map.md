# 01_architecture_map
## 架构总览
研究主链路：Data → Panel → Feature → Signal → Portfolio → Backtest → Diagnostics → Constraints/Report。

## 目录与职责
- `src/qsys/data/panel/daily_panel.py`：`DailyPanelReader` / `load_daily_panel`，读取按 `trade_date=.../data.parquet` 分区。
- `src/qsys/features/compute.py`：`default_feature_registry`、`compute_features`。
- `src/qsys/features/store.py`：`materialize_and_store_features`，特征落盘。
- `src/qsys/signals/engine.py`：`SignalEngine`、`demo_alpha_signal`。
- `src/qsys/backtest/*`：组合、执行对齐、成本、指标、模拟器。
- `src/qsys/rebalance/*`：buffered policy、对比、报告。
- `src/qsys/research/*`：IC/quantile/decay/exposure/turnover/correlation。

## 依赖关系
- Signal 依赖 Feature store frame：`load_feature_store_frame`。
- Backtest 依赖 signal/returns 对齐：`align_next_day_returns`、`align_weights_and_returns`。
- Diagnostics 依赖 signal + forward returns（如 `daily_rank_ic`、`quantile_spread`）。

## 实现分层
- 已实现：模块化研究流水线、CLI 示例脚本（`src/qsys/utils/*`）。
- 部分实现：风险控制仅约束级、benchmark/report 偏工具化。
- 尚未实现：统一配置中心、任务编排、完整 model registry。

## 架构优点与局限
优点：模块小、可替换性好、测试覆盖主要行为。  
局限：多个入口脚本重复拼装；配置分散在 argparse/dataclass；研究与报告耦合弱。

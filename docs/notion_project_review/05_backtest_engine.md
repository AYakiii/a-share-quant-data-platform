# 05 Backtest Engine

## strict_top_n 主栈
- `build_top_n_portfolio`：支持 long_only/long_short、liquidity filter、max_single_weight、size-aware scaling、group cap。
- `run_backtest_from_signal` / `run_backtest_from_weights`：处理调仓频率、收益对齐、成本与指标。

## buffered_top_n 与 benchmark
- `qsys/rebalance/policies.py::BufferedTopNPolicyConfig` + `build_buffered_top_n_weights`。
- `qsys/rebalance/backtest.py::run_buffered_topn_backtest`。
- benchmark: `rebalance/benchmarks.py`, `rebalance/index_benchmarks.py`。

## 执行与成本
- execution 支持 `next_close`（`next_open` 请求会回退说明）。
- cost = turnover * (transaction_cost_bps + slippage_bps)。

## 局限
- 成本模型线性且简化。
- 无真实成交仿真、无冲击成本曲线。

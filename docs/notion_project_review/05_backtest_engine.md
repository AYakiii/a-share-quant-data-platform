# 05_backtest_engine（V2）

## 模块为什么存在
回测层负责把信号映射到组合与收益曲线，是研究结论可验证的核心执行层。

## 在主流程中的位置
Signal → Portfolio → Backtest：
- 组合构建：`build_top_n_portfolio`。
- 执行对齐：`align_next_day_returns`、`align_weights_and_returns`。
- 成本：`compute_turnover`、`compute_daily_cost`。
- 绩效：`summarize_metrics`。

## 输入/输出
- 输入：signal、asset returns、`BacktestConfig`。
- 输出：net/gross returns、turnover、cost、summary。

## 设计选择
- `BacktestConfig` 集中配置再平衡频率、执行方式、成本参数。
- 规则型组合（top_n）简化了实验门槛，适合 MVP 快速研究。

## MVP 与技术债
- MVP：执行假设简化，成本模型线性。
- 技术债：`execution=next_open` 仍 fallback 行为需长期策略定义；未纳入冲击成本/容量限制。

## 相关测试
- `tests/backtest/test_portfolio.py`
- `tests/backtest/test_portfolio_constraints.py`
- `tests/backtest/test_simulator_metrics.py`

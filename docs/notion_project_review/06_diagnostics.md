# 06_diagnostics
## 指标实现
- IC/Rank IC：`qsys.research.ic.daily_ic` / `daily_rank_ic` / `ic_summary`。
- quantile return：`quantile_mean_forward_returns`、`quantile_spread`。
- decay：`qsys.research.decay.decay_analysis`。
- turnover：`signal_autocorrelation`、`top_n_turnover`。
- exposure：`size_exposure_daily`、`group_exposure_daily`、`compute_portfolio_exposure`。

## 解释与风险
- Rank IC 高不代表可交易收益稳定；需联动 turnover 与成本。
- quantile spread 对分组边界敏感，样本小易误读。
- exposure 结果需区分“信号暴露”与“组合暴露”。

## 弱点
- 缺统一 diagnostics report schema。
- 缺显著性/稳健性统计（bootstrap、子区间一致性）。

# PROJECT_QUERY_GUIDE（V3）

> 目的：给 GPT/Codex 与人类读者一个“问题驱动”的检索入口。

## Q1. 我想理解 data ingestion，先看哪里？
- 文档建议：`02_data_pipeline.md`、`deep_review_evidence_pack.md` Data pipeline 章节
- 代码路径：`src/qsys/utils/build_real_feature_store.py`
- 关键函数：`build_real_feature_store`, `_safe_fetch_daily`, `_normalize_daily_frame`
- 相关测试：`tests/utils/test_build_real_feature_store.py`
- 可问 GPT/Codex：
  1) `REQUIRED_COLUMNS` 与下游模块依赖关系是什么？
  2) `start_date/end_date` 当前增量语义是什么？

## Q2. 我想理解 feature 生成，先看哪里？
- 文档建议：`03_panel_and_feature_store.md` + evidence pack feature 章节
- 代码路径：`src/qsys/features/compute.py`, `store.py`, `registry.py`
- 关键函数：`default_feature_registry`, `compute_features`, `materialize_and_store_features`
- 相关测试：`tests/features/test_feature_compute.py`
- 可问 GPT/Codex：
  1) 每个特征的窗口与标签泄露风险点？
  2) 如何添加新特征并保持与现有索引规范一致？

## Q3. 我想理解 signal 如何变成组合权重，先看哪里？
- 文档建议：`04_signal_engine.md`, `05_backtest_engine.md`, `07_constraints_and_risk_control.md`
- 代码路径：`src/qsys/signals/*`, `src/qsys/backtest/portfolio.py`
- 关键函数：`SignalEngine.build_transformed_signals`, `linear_combine`, `build_top_n_portfolio`
- 相关测试：`tests/signals/test_engine.py`, `tests/backtest/test_portfolio_constraints.py`
- 可问 GPT/Codex：
  1) 从 signal 到 top_n 的每一步归一化如何进行？
  2) liquidity filter 与 group cap 在何时生效？

## Q4. 我想理解 backtest assumptions，先看哪里？
- 文档建议：`05_backtest_engine.md`, `09_tests_and_reliability.md`
- 代码路径：`src/qsys/backtest/simulator.py`, `execution.py`, `cost.py`, `metrics.py`
- 关键函数：`run_backtest_from_signal`, `align_next_day_returns`, `compute_daily_cost`, `summarize_metrics`
- 相关测试：`tests/backtest/test_simulator_metrics.py`
- 可问 GPT/Codex：
  1) `execution` 的 fallback 对结果可能影响多大？
  2) 成本模型是否对 turnover 过于线性？

## Q5. 我想理解 diagnostics，先看哪里？
- 文档建议：`06_diagnostics.md` + evidence pack diagnostics
- 代码路径：`src/qsys/research/*`
- 关键函数：`daily_rank_ic`, `quantile_spread`, `decay_analysis`, `top_n_turnover`, `exposure_summary`
- 相关测试：`tests/research/test_ic.py`, `tests/research/test_quantile_or_corr.py`
- 可问 GPT/Codex：
  1) 哪些指标最容易被误读？
  2) 指标如何联动解释（IC + turnover + quantile spread）？

## Q6. 我想理解 risk control，先看哪里？
- 文档建议：`07_constraints_and_risk_control.md`, `08_model_review.md`
- 代码路径：`src/qsys/backtest/portfolio.py`, `src/qsys/risk/exposure.py`, `src/qsys/research/constraint_impact.py`
- 关键函数：`build_top_n_portfolio`, `build_risk_exposure_matrix`, `compare_constraint_impact`
- 相关测试：`tests/research/test_constraint_impact.py`, `tests/risk/test_exposure.py`
- 可问 GPT/Codex：
  1) 当前哪些是组合约束，哪些可视为风险控制雏形？
  2) 与 full risk model 的差距最关键是哪几项？

## Q7. 我想排查 test failure，先看哪里？
- 文档建议：`09_tests_and_reliability.md`, `10_next_development_roadmap.md`
- 代码路径：失败对应模块 + 测试文件
- 当前已知：`tests/signals/test_engine.py::test_demo_alpha_combination_behavior`
- 可问 GPT/Codex：
  1) 测试期望是否与 `demo_alpha_signal` 当前实现一致？
  2) 若不一致，建议先改测试还是先改实现？理由是什么？

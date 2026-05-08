# deep_review_evidence_pack（V3）

> 目的：提供“可追溯到代码”的证据包，支撑后续长文系统手册编写。  
> 范围：仅依据当前仓库 `README.md`、`src/qsys/**`、`tests/**`、`src/qsys/utils/**`。

---

## 1) Data pipeline

**状态**：已实现（MVP），部分能力待验证（metadata/incremental orchestration）。

- 主要文件
  - `src/qsys/utils/build_real_feature_store.py`
- 关键函数/脚本
  - `build_real_feature_store`
  - `_fetch_symbol_universe`
  - `_safe_fetch_daily`
  - `_normalize_daily_frame`
  - `main`
- 输入结构
  - AkShare 股票列表/日线数据；CLI 参数 `--feature-root --start-date --end-date --limit --retries --retry-wait --request-sleep`。
- 输出结构
  - `trade_date=YYYY-MM-DD/data.parquet` 分区文件。
  - 标准列由 `REQUIRED_COLUMNS` 定义（含 `ret_1d/5d/20d`, `vol_20d`, `fwd_ret_5d/20d`, `is_tradable`）。
- 前后连接
  - 前：外部行情源（AkShare）
  - 后：`qsys.data.panel.daily_panel` 与 `qsys.signals.engine.load_feature_store_frame`
- 隐含假设
  - AkShare 字段稳定；`ts_code` 与 symbol 互转规则有效。
- MVP 简化
  - 日期过滤 + 分区写入 ≈ 轻量增量，不是完整调度增量框架。
- technical debt
  - 缺 raw/clean 分层、缺数据质量审计日志、缺 schema version。
- 已有测试
  - `tests/utils/test_build_real_feature_store.py`
- 缺失测试
  - 网络异常稳定性（长时重试、接口字段突变）
  - 大规模 symbol 集合性能测试
- 需人工判断问题
  - README 提到 sqlite metadata/incremental update 在主路径是否完整接线？需代码外流程核查。

---

## 2) Daily panel

**状态**：已实现。

- 主要文件
  - `src/qsys/data/panel/daily_panel.py`
- 关键函数/类
  - `DailyPanelConfig`
  - `DailyPanelReader`
  - `load_daily_panel`
- 输入结构
  - 按日分区 parquet
- 输出结构
  - `MultiIndex [date, asset]` panel
- 前后连接
  - 前：Data pipeline 分区
  - 后：Feature 计算/Signal 加载
- 隐含假设
  - 分区目录规范稳定；字段命名可映射 `trade_date->date`, `ts_code->asset`。
- MVP 简化
  - 未包含复杂 schema 演进治理。
- technical debt
  - 无统一 panel schema registry。
- 已有测试
  - `tests/data/test_daily_panel.py`
- 缺失测试
  - 异常分区（缺列/脏值/混合 schema）容错基线。
- 需人工判断
  - 多数据源并行时字段冲突处理规则。

---

## 3) Feature store

**状态**：已实现 v1。

- 主要文件
  - `src/qsys/features/base.py`
  - `src/qsys/features/registry.py`
  - `src/qsys/features/compute.py`
  - `src/qsys/features/store.py`
- 关键函数/类
  - `BaseFeature`, `FunctionFeature`, `FeatureRegistry`
  - `default_feature_registry`, `compute_features`
  - `materialize_features`, `write_feature_store`, `materialize_and_store_features`
- 输入结构
  - `MultiIndex [date, asset]` panel
- 输出结构
  - feature frame + 按 `trade_date` 分区写盘
- 前后连接
  - 前：Daily panel
  - 后：Signal engine
- 隐含假设
  - 特征在当前索引排序与分组逻辑下无跨资产污染。
- MVP 简化
  - registry/version governance 轻量。
- technical debt
  - 缺统一“可用时间戳（feature availability）”协议。
- 已有测试
  - `tests/features/test_feature_compute.py`
- 缺失测试
  - 特征版本迁移兼容测试；分区级回归快照。
- 需人工判断
  - 特征定义与标签窗口是否应统一集中配置。

---

## 4) Signal engine

**状态**：已实现 v1，存在行为一致性待确认点。

- 主要文件
  - `src/qsys/signals/transforms.py`
  - `src/qsys/signals/combine.py`
  - `src/qsys/signals/engine.py`
- 关键函数/类
  - `winsorize_cross_section`, `zscore_cross_section`, `rank_cross_section`
  - `linear_combine`
  - `SignalEngine`, `demo_alpha_signal`, `load_feature_store_frame`
- 输入结构
  - feature frame（含 `ret_20d`, `vol_20d` 等）
- 输出结构
  - signal series（`MultiIndex [date, asset]`）
- 前后连接
  - 前：Feature store
  - 后：Portfolio/backtest
- 隐含假设
  - 同日横截面可直接比较，分布可被 zscore/rank 有效标准化。
- MVP 简化
  - 线性组合为主；demo alpha 规则化。
- technical debt
  - 参数治理与信号版本管理不完整。
- 已有测试
  - `tests/signals/test_transforms.py`
  - `tests/signals/test_engine.py`
- 缺失测试
  - 多参数 recipe 稳定性与边界条件矩阵。
- 需人工判断
  - `test_demo_alpha_combination_behavior` 失败应归因于“测试预期”还是“实现逻辑”。

---

## 5) Backtest engine

**状态**：已实现 MVP。

- 主要文件
  - `src/qsys/backtest/portfolio.py`
  - `src/qsys/backtest/execution.py`
  - `src/qsys/backtest/cost.py`
  - `src/qsys/backtest/metrics.py`
  - `src/qsys/backtest/simulator.py`
- 关键函数/类
  - `build_top_n_portfolio`
  - `align_next_day_returns`, `align_weights_and_returns`
  - `compute_turnover`, `compute_daily_cost`
  - `summarize_metrics`
  - `BacktestConfig`, `run_backtest_from_signal`, `run_backtest_from_weights`
- 输入结构
  - signal + asset return + config
- 输出结构
  - net/gross returns, turnover, cost, summary
- 前后连接
  - 前：Signal
  - 后：Diagnostics / benchmark compare
- 隐含假设
  - 执行时点近似、成本线性可接受。
- MVP 简化
  - `next_open` fallback 说明存在，执行模型简化。
- technical debt
  - 容量与冲击成本未纳入。
- 已有测试
  - `tests/backtest/test_portfolio.py`
  - `tests/backtest/test_portfolio_constraints.py`
  - `tests/backtest/test_simulator_metrics.py`
- 缺失测试
  - 长区间回放一致性快照；交易成本敏感性回归。
- 需人工判断
  - 执行假设是否满足目标研究场景。

---

## 6) Diagnostics

**状态**：已实现 v1，框架分散。

- 主要文件
  - `src/qsys/research/ic.py`, `quantiles.py`, `decay.py`, `turnover.py`, `exposure.py`, `correlation.py`
  - `src/qsys/research/signal_quality/*`
- 关键函数
  - `daily_ic`, `daily_rank_ic`, `ic_summary`
  - `quantile_mean_forward_returns`, `quantile_spread`
  - `decay_analysis`, `top_n_turnover`
  - `size_exposure_daily`, `group_exposure_daily`
- 输入/输出
  - 输入：signal、fwd returns、features
  - 输出：IC/Rank IC、quantile、decay、turnover、exposure
- 前后连接
  - 前：Backtest / signal outputs
  - 后：模型迭代与约束设计
- 隐含假设
  - forward return 标签足够代表预测目标。
- MVP 简化
  - 统计显著性与统一报告 schema 有限。
- technical debt
  - 指标分散在多模块，统一报告管道弱。
- 已有测试
  - `tests/research/test_ic.py`
  - `tests/research/test_quantile_or_corr.py`
  - `tests/research/test_exposure.py`
- 缺失测试
  - 子样本稳定性、显著性鲁棒性。
- 需人工判断
  - 指标阈值/及格线定义。

---

## 7) Constraints / risk control

**状态**：部分实现（constraints 有，full risk model 未实现）。

- 主要文件
  - `src/qsys/backtest/portfolio.py`
  - `src/qsys/research/constraint_impact.py`
  - `src/qsys/universe/eligibility.py`
  - `src/qsys/risk/exposure.py`
- 关键函数/类
  - `build_top_n_portfolio`（liquidity / cap / group cap / size scaling）
  - `compare_constraint_impact`
  - `build_eligibility_mask`
  - `build_risk_exposure_matrix`
- 输入/输出
  - 输入：signal、liquidity、market_cap、group labels
  - 输出：约束后权重、约束影响分析
- 前后连接
  - 前：Signal
  - 后：Backtest/diagnostics
- 隐含假设
  - same-date 数据可用，且 group labels 质量可靠。
- MVP 简化
  - 约束逻辑偏组合层，不是风险预算优化。
- technical debt
  - 缺协方差驱动 risk model 与优化器。
- 已有测试
  - `tests/backtest/test_portfolio_constraints.py`
  - `tests/research/test_constraint_impact.py`
  - `tests/universe/test_eligibility.py`
- 缺失测试
  - 风险暴露目标约束回归（因当前未实现 full risk model）。
- 需人工判断
  - 风险控制目标是“约束优先”还是“风险预算优先”。

---

## 8) Rebalance / benchmark / report workflow

**状态**：部分实现。

- 主要文件
  - `src/qsys/rebalance/policies.py`, `backtest.py`, `benchmarks.py`, `index_benchmarks.py`, `diagnostics.py`
  - `src/qsys/utils/compare_rebalance_policies_from_feature_store.py`
  - `src/qsys/utils/report_rebalance_policy_comparison.py`
- 关键函数/类
  - `BufferedTopNPolicyConfig`, `build_buffered_top_n_weights`
  - `run_buffered_topn_backtest`
  - `build_equal_weight_benchmark`
  - `generate_report`
- 输入/输出
  - 输入：signal/returns/benchmark sources
  - 输出：策略比较、基准比较、报告图表
- 前后连接
  - 前：Signal + Backtest
  - 后：研究决策与策略复盘
- 隐含假设
  - buffered 机制可降低无效换手。
- MVP 简化
  - report schema 仍工具化，不是统一报告系统。
- technical debt
  - benchmark 配置与版本化治理不足。
- 已有测试
  - `tests/rebalance/test_buffered_top_n_backtest.py`
  - `tests/rebalance/test_report_rebalance_policy_comparison.py`
  - `tests/rebalance/test_index_benchmarks.py`
- 缺失测试
  - 报告产物一致性快照（图表/表格）。
- 需人工判断
  - benchmark 选取标准和治理流程。

---

## 9) Tests and reliability

**状态**：模块化测试较完整，但端到端与真实网络波动覆盖有限。

- 关键证据
  - 定向命令：`PYTHONPATH=src pytest -q tests/signals/test_engine.py tests/backtest/test_portfolio.py tests/data/test_daily_panel.py`
  - 结果：backtest/data 通过，signal 有 1 个失败 `test_demo_alpha_combination_behavior`（已在 docs 记录）。
- 已覆盖
  - 多数核心函数行为与输入校验。
- 缺失
  - 大规模性能、真实 API 长期稳定性、E2E 回归快照。
- 需人工判断
  - 失败测试应修预期还是修实现。

---

## 10) Current model assumptions

**状态**：已实现基础规则模型，仍属 MVP 研究基线。

- 主要假设
  - momentum（`ret_20d` rank）+ volatility penalty（`vol_20d` zscore）可产生可研究 alpha。
  - 横截面标准化在日频环境稳定有效。
  - 组合约束可在一定程度替代完整风险模型。
- 证据文件
  - `src/qsys/signals/engine.py`（`demo_alpha_signal`）
  - `src/qsys/backtest/portfolio.py`（约束）
  - `src/qsys/research/*`（诊断）
- 风险
  - 线性惩罚可能不足以应对 regime shift；风险控制尚未系统化。
- 需人工判断
  - base model 的下一阶段是先“稳健验证”还是直接“复杂化扩展”。

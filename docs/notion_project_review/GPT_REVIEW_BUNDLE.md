# GPT_REVIEW_BUNDLE（Whole-Project Review Context）

> 用途：给 GPT/Codex 提供“全项目理解上下文”，用于在新增模块后产出 **whole-project impact report** 并更新系统手册。  
> 约束：仅基于当前仓库可见证据，不推测未实现功能。

---

## 1) 项目目的与当前阶段

### 项目目的
构建一个面向 A-share 日频/低频研究的量化研究平台，核心价值是“研究流程可复现”，而非生产交易系统。主链路：Data → Panel → Feature → Signal → Portfolio → Backtest → Diagnostics → Constraints → Report。

### 当前阶段判定
- **Implemented**：Feature Store v1、Signal Engine v1、Backtest MVP、Diagnostics v1。
- **Partially implemented**：Benchmark/Rebalance/Report workflow、Risk control 的系统化层。
- **Not implemented**：production OMS/EMS、full risk model + optimizer。
- **Requires verification**：README 中 sqlite metadata / incremental update 在主路径接线程度。

---

## 2) 端到端工作流

`Data → Panel → Feature → Signal → Portfolio → Backtest → Diagnostics → Constraints → Report`

- Data：`src/qsys/utils/build_real_feature_store.py`
- Panel：`src/qsys/data/panel/daily_panel.py`
- Feature：`src/qsys/features/{base,registry,compute,store}.py`
- Signal：`src/qsys/signals/{transforms,combine,engine}.py`
- Portfolio/Backtest：`src/qsys/backtest/*.py`
- Diagnostics：`src/qsys/research/*.py` + `src/qsys/research/signal_quality/*`
- Constraints/Risk：`src/qsys/backtest/portfolio.py`, `src/qsys/risk/exposure.py`, `src/qsys/research/constraint_impact.py`
- Report/Rebalance：`src/qsys/rebalance/*.py`, `src/qsys/utils/report_rebalance_policy_comparison.py`

---

## 3) Major module map（全局）

1. data_ingestion_pipeline  
2. daily_panel  
3. feature_store  
4. signal_engine  
5. backtest_engine  
6. diagnostics  
7. constraints_risk_control  
8. rebalance_benchmark_report  
9. utility_scripts  
10. test_system

---

## 4) Module-by-module evidence

### A. data_ingestion_pipeline
- main files
  - `src/qsys/utils/build_real_feature_store.py`
- key functions/classes
  - `build_real_feature_store`, `_fetch_symbol_universe`, `_safe_fetch_daily`, `_normalize_daily_frame`, `main`
- inputs
  - AkShare symbol/daily data；CLI 参数（`--feature-root`, `--start-date`, `--end-date`, `--limit`, `--retries` ...）
- outputs
  - parquet 分区：`trade_date=YYYY-MM-DD/data.parquet`
- upstream dependencies
  - AkShare API
- downstream consumers
  - `qsys.data.panel.daily_panel`, `qsys.signals.engine.load_feature_store_frame`
- current assumptions
  - 外部字段稳定；symbol ↔ `ts_code` 映射有效
- MVP simplifications
  - 日期过滤 + 分区写入 ≠ 完整增量调度
- technical debt
  - 缺 raw zone / quality audit / schema version
- existing tests
  - `tests/utils/test_build_real_feature_store.py`
- missing tests
  - 网络抖动长期稳定性、字段突变兼容

### B. daily_panel
- main files
  - `src/qsys/data/panel/daily_panel.py`
- key functions/classes
  - `DailyPanelConfig`, `DailyPanelReader`, `load_daily_panel`
- inputs
  - feature-store style parquet partitions
- outputs
  - panel（`MultiIndex [date, asset]`）
- upstream dependencies
  - data_ingestion_pipeline outputs
- downstream consumers
  - feature compute / signal loading
- current assumptions
  - 分区路径与字段可规范映射
- MVP simplifications
  - schema 演进管理较轻
- technical debt
  - 缺 panel schema registry
- existing tests
  - `tests/data/test_daily_panel.py`
- missing tests
  - 异常分区容错（缺列/类型漂移）

### C. feature_store
- main files
  - `src/qsys/features/base.py`, `registry.py`, `compute.py`, `store.py`
- key functions/classes
  - `BaseFeature`, `FunctionFeature`, `FeatureRegistry`, `default_feature_registry`, `compute_features`, `materialize_and_store_features`
- inputs
  - panel（`MultiIndex [date, asset]`）
- outputs
  - 特征 frame + 分区写盘
- upstream dependencies
  - daily_panel
- downstream consumers
  - signal_engine
- current assumptions
  - 分组/排序逻辑保证特征正确对齐
- MVP simplifications
  - 版本治理简化
- technical debt
  - 缺 feature availability 协议
- existing tests
  - `tests/features/test_feature_compute.py`
- missing tests
  - 特征版本迁移与快照回归

### D. signal_engine
- main files
  - `src/qsys/signals/transforms.py`, `combine.py`, `engine.py`
- key functions/classes
  - `winsorize_cross_section`, `zscore_cross_section`, `rank_cross_section`, `linear_combine`, `SignalEngine`, `demo_alpha_signal`, `load_feature_store_frame`
- inputs
  - feature frame + recipes + weights
- outputs
  - signal series（`MultiIndex [date, asset]`）
- upstream dependencies
  - feature_store
- downstream consumers
  - backtest/portfolio
- current assumptions
  - 横截面可比；线性组合有效
- MVP simplifications
  - demo 信号规则化，参数管理轻量
- technical debt
  - signal registry 与参数治理不足
- existing tests
  - `tests/signals/test_transforms.py`, `tests/signals/test_engine.py`
- missing tests
  - recipe 参数网格行为边界

### E. backtest_engine
- main files
  - `src/qsys/backtest/portfolio.py`, `execution.py`, `cost.py`, `metrics.py`, `simulator.py`
- key functions/classes
  - `build_top_n_portfolio`, `align_next_day_returns`, `compute_turnover`, `compute_daily_cost`, `summarize_metrics`, `BacktestConfig`, `run_backtest_from_signal`
- inputs
  - signal / asset returns / config
- outputs
  - returns, turnover, cost, summary
- upstream dependencies
  - signal_engine
- downstream consumers
  - diagnostics / report
- current assumptions
  - 执行假设近似；成本线性
- MVP simplifications
  - `next_open` fallback 行为；无冲击成本
- technical debt
  - 容量/冲击/真实执行差异
- existing tests
  - `tests/backtest/test_portfolio.py`, `tests/backtest/test_portfolio_constraints.py`, `tests/backtest/test_simulator_metrics.py`
- missing tests
  - 长周期 E2E 回归快照

### F. diagnostics
- main files
  - `src/qsys/research/ic.py`, `quantiles.py`, `decay.py`, `turnover.py`, `exposure.py`, `correlation.py`, `signal_quality/*`
- key functions/classes
  - `daily_rank_ic`, `ic_summary`, `quantile_spread`, `decay_analysis`, `top_n_turnover`, `exposure_summary`, `compute_ic_by_date`
- inputs
  - signal, forward returns, features
- outputs
  - IC/quantile/decay/turnover/exposure metrics
- upstream dependencies
  - backtest or signal outputs
- downstream consumers
  - model iteration / risk-control refinement
- current assumptions
  - 标签与指标口径足以评价信号
- MVP simplifications
  - 显著性检验与统一报告 schema 不完整
- technical debt
  - 指标分散，统一输出弱
- existing tests
  - `tests/research/test_ic.py`, `tests/research/test_quantile_or_corr.py`, `tests/research/test_exposure.py`
- missing tests
  - 子样本稳定性与统计鲁棒性

### G. constraints_risk_control
- main files
  - `src/qsys/backtest/portfolio.py`, `src/qsys/research/constraint_impact.py`, `src/qsys/universe/eligibility.py`, `src/qsys/risk/exposure.py`
- key functions/classes
  - `build_top_n_portfolio`, `compare_constraint_impact`, `build_eligibility_mask`, `build_risk_exposure_matrix`
- inputs
  - signal / liquidity / market_cap / group labels
- outputs
  - constrained weights / impact diagnostics / exposure matrix
- upstream dependencies
  - signal_engine
- downstream consumers
  - backtest + diagnostics
- current assumptions
  - same-date 数据可用；group labels 可依赖
- MVP simplifications
  - 约束主导，不是 full risk optimizer
- technical debt
  - 缺风险预算与协方差优化
- existing tests
  - `tests/research/test_constraint_impact.py`, `tests/universe/test_eligibility.py`, `tests/risk/test_exposure.py`
- missing tests
  - 目标风险暴露约束（因模块尚未实现）

### H. rebalance_benchmark_report
- main files
  - `src/qsys/rebalance/policies.py`, `backtest.py`, `benchmarks.py`, `index_benchmarks.py`, `diagnostics.py`
  - `src/qsys/utils/compare_rebalance_policies_from_feature_store.py`
  - `src/qsys/utils/report_rebalance_policy_comparison.py`
- key functions/classes
  - `BufferedTopNPolicyConfig`, `build_buffered_top_n_weights`, `run_buffered_topn_backtest`, `build_equal_weight_benchmark`, `generate_report`
- inputs
  - signal/returns/benchmark data
- outputs
  - policy comparison, benchmark curves, plots/reports
- upstream dependencies
  - backtest/signal outputs
- downstream consumers
  - research decision / review docs
- current assumptions
  - buffered policy 有助降低噪声换手
- MVP simplifications
  - report schema 未统一
- technical debt
  - benchmark 配置治理和版本化不足
- existing tests
  - `tests/rebalance/test_buffered_top_n_backtest.py`, `tests/rebalance/test_index_benchmarks.py`, `tests/rebalance/test_report_rebalance_policy_comparison.py`
- missing tests
  - 图表与报告产物快照回归

### I. utility_scripts
- main files
  - `src/qsys/utils/*.py`, `run_demo.py`
- key functions/classes
  - 多个 `main()` 入口（feature build / signal grid / rebalance compare / report）
- inputs
  - CLI args + feature_root
- outputs
  - 中间产物、比较结果、示例执行
- upstream dependencies
  - all modules
- downstream consumers
  - 人工研究工作流
- current assumptions
  - 用户按文档顺序执行脚本
- MVP simplifications
  - 入口分散，命令面分裂
- technical debt
  - 缺统一 CLI 门面
- existing tests
  - `tests/utils/test_run_signal_sanity_grid.py`, `tests/utils/test_run_buffered_rebalance_from_feature_store_script.py`
- missing tests
  - 跨脚本一致性契约

### J. test_system
- main files
  - `tests/**`
- key functions/classes
  - pytest suites
- inputs
  - 模块函数与样例数据
- outputs
  - 行为回归信号
- upstream dependencies
  - source modules
- downstream consumers
  - 开发决策与文档更新
- current assumptions
  - 单元/行为测试足以覆盖核心逻辑
- MVP simplifications
  - 真实网络+性能覆盖有限
- technical debt
  - 缺 E2E snapshot 基准
- existing tests
  - 全模块已有基础覆盖
- missing tests
  - 大规模数据与长期回放稳定性

---

## 5) 当前模型假设

- 动量/波动规则模型可作为 baseline：`demo_alpha_signal` 使用 `rank(ret_20d) - 0.5*zscore(vol_20d)`。
- 横截面标准化（rank/zscore）对日频研究有效。
- 线性信号组合可作为第一阶段策略表达。

**状态**：Implemented（baseline） + Requires verification（稳健性/样本外）。

---

## 6) 当前 risk-control 假设

- 组合约束（liquidity、single-name cap、group cap、size-aware scaling）可在 MVP 阶段提供风险约束。
- 但该层不等于 full risk model（缺协方差优化/风险预算）。

**状态**：Partially implemented + Technical debt。

---

## 7) 当前可靠性问题

- 已记录失败：`tests/signals/test_engine.py::test_demo_alpha_combination_behavior`。
- 结论：目前应先判定“测试预期 vs 实现逻辑”哪一方需调整，避免未经确认更改 signal baseline。
- 网络与外部 API 波动（AkShare/Notion）属于操作层风险，不应与核心研究逻辑混淆。

---

## 8) 最新 phase 变更（文档/工具层）

- 新增/扩展 `docs/notion_project_review/**`（包括 evidence/query/manual）。
- `scripts/sync_project_review_to_notion.py` 已支持 `child-pages`/`inline`、重试、batch、`--start-from`。
- 本阶段不涉及 `src/qsys/**` 或 `tests/**` 逻辑修改。

---

## 9) GPT 应从 whole-project 角度回答的问题

1. 新模块加入后，是否改变 Data→Report 的依赖方向或接口契约？
2. 新模块是否引入 lookahead 风险或标签口径不一致？
3. 新模块对已有 diagnostics 指标解释是否造成冲突？
4. 新模块是否要求更新 constraints/risk-control 假设？
5. 新模块是否应新增测试，优先级如何（unit/behavior/e2e）？
6. 新模块是否暴露了已有 technical debt 的“必须偿还点”？
7. 新模块是否改变 `project_map.json` 中模块状态（implemented/partially/not implemented/requires verification）？

---

## 10) GPT review 后应更新的文档

必更：
- `docs/notion_project_review/project_map.json`
- `docs/notion_project_review/01_architecture_map.md`
- `docs/notion_project_review/10_next_development_roadmap.md`
- `docs/notion_project_review/09_tests_and_reliability.md`

按影响选择更新：
- `02_data_pipeline.md`（数据接入变化）
- `03_panel_and_feature_store.md`（特征层变化）
- `04_signal_engine.md`（信号层变化）
- `05_backtest_engine.md`（回测口径变化）
- `06_diagnostics.md`（指标层变化）
- `07_constraints_and_risk_control.md`（约束/风控变化）
- `08_model_review.md`（模型假设变化）
- `PROJECT_SYSTEM_MANUAL_DRAFT.md`（长文系统手册增量修订）
- `deep_review_evidence_pack.md`（证据重采样）
- `PROJECT_QUERY_GUIDE.md`（查询入口更新）

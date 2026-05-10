# PROJECT_SYSTEM_MANUAL｜Quant Research Platform 全项目系统手册

## 0. 阅读方式与证据边界

本手册基于当前上传的项目材料包生成，目标不是复述 README，也不是扩写 Codex 报告，而是把当前仓库解释成一个可以长期维护、查询和复盘的 quant research platform。手册采用的主线是：

`Data → Panel → Feature → Signal → Portfolio → Backtest → Diagnostics → Constraints → Report`

本次实际读取并交叉检查的材料包括：

- `README.md`
- `requirements.txt`
- `docs/notion_project_review/*.md`
- `docs/notion_project_review/project_map.json`
- `docs/notion_project_review/GPT_REVIEW_BUNDLE.md`
- `docs/notion_project_review/deep_review_evidence_pack.md`
- `docs/notion_project_review/PROJECT_QUERY_GUIDE.md`
- `docs/notion_project_review/PROJECT_SYSTEM_MANUAL_DRAFT.md`
- `src/qsys/**`
- `tests/**`
- `scripts/sync_project_review_to_notion.py`

同时需要明确几个证据边界。

第一，材料包中没有 `docs/notion_project_review/phases/` 和 `docs/notion_project_review/deep_reviews/` 目录。因此，所有关于 phase log 或 deep review 历史的判断，只能依据当前 review docs、`GPT_REVIEW_BUNDLE.md`、`deep_review_evidence_pack.md` 与源码，而不能依据不存在的分阶段日志。

第二，材料包中没有 README 中提到的 `run_demo.py` 和 `A_share_Analytical_DWH.ipynb`。这可能是打包范围没有包含，也可能是 README 已经过时。基于当前仓库快照，只能标记为【文档需更新 / 需进一步验证】，不能默认这些入口仍然可运行。

第三，本次审查环境并不等价于你的本地开发环境。当前环境中 `pyarrow`、`fastparquet`、`akshare`、`SQLAlchemy` 未安装，而 `requirements.txt` 声明了这些依赖。因此，涉及 parquet、AkShare 或 sqlite/SQLAlchemy 的测试失败，不能直接判断为代码 bug，必须标记为【本地标准环境需复现】。不过，部分不依赖这些包的单元测试已经可以运行，并暴露出几个真实的行为一致性问题。

第四，本手册不把 Codex 报告当作唯一事实来源。`GPT_REVIEW_BUNDLE.md` 和 `deep_review_evidence_pack.md` 很适合作为索引，但最终判断以 `src/qsys/**`、`tests/**`、README、`project_map.json` 之间的交叉证据为准。如果文档与代码不一致，本手册会明确标注【不一致 / 需进一步验证】。

第五，Notion 在当前项目中应被理解为阅读层和同步层，不应被理解为 source of truth。正式记录应留在 GitHub repo 中，特别是 `docs/notion_project_review/PROJECT_SYSTEM_MANUAL.md` 这一类可版本控制的 Markdown 文档。

---

## 1. 项目当前阶段判断

### 1.1 一句话定位

当前项目是一个面向 A 股日频/低频研究的 modular quant research platform。它的核心价值不是某一个已经被证明可以实盘赚钱的策略，而是把“数据接入、panel 标准化、feature store、signal 生成、组合构建、回测、诊断、约束影响分析、报告输出”组织成一条可解释、可复盘、可继续演进的研究链路。

更准确地说，它现在是【已实现：research infrastructure + MVP strategy layer + diagnostics layer + constraints prototype + report/review workflow 雏形】；不是【尚未实现：production trading system / full risk model / mature ML alpha platform / experiment registry】。

### 1.2 当前工程阶段

README 用 “Current status: V1 complete” 描述项目阶段，但这个说法需要加限定。当前项目可以说是 **V1 research infrastructure 基本成型**，但不能说完整策略系统、完整风险系统或生产交易系统已经完成。

当前阶段可以拆成几层：

| 层级 | 当前判断 | 说明 |
|---|---|---|
| Research infrastructure | 【已实现】 | `src/qsys/data`、`features`、`signals`、`backtest`、`research` 等模块已经形成主链路。 |
| MVP strategy layer | 【部分实现】 | 有 demo alpha、Top-N portfolio、buffered rebalance、成本扣减，但仍是规则型研究基线。 |
| Diagnostics layer | 【已实现 v1】 | IC、Rank IC、quantile、turnover、decay、exposure、conditioned IC 等基础诊断存在。 |
| Risk-control prototype | 【部分实现】 | 已有 liquidity filter、single-name cap、group cap、eligibility、risk exposure matrix，但不是 full risk model。 |
| Report/review workflow | 【部分实现】 | 有 rebalance comparison report、Notion review docs、sync script，但 report schema 未统一。 |
| Production trading system | 【尚未实现】 | 没有 OMS/EMS、订单执行、实时风控、权限、监控、灾备。 |
| Full risk model | 【尚未实现】 | 没有协方差模型、风险预算、tracking error optimizer、多因子风险归因。 |
| Experiment registry | 【尚未实现 / 需进一步验证】 | 有部分 metadata 记录，但没有统一实验 registry、参数版本、artifact lineage。 |

### 1.3 已实现能力

当前仓库已经实现了一个研究系统最重要的骨架：

- 【已实现】从 AkShare 或已有 parquet 分区读取数据，并输出 `trade_date=YYYY-MM-DD/data.parquet` 风格的 feature store。
- 【已实现】通过 `DailyPanelReader` / `load_daily_panel` 将日频数据标准化成 `MultiIndex [date, asset]`。
- 【已实现】通过 `FeatureRegistry`、`FunctionFeature`、`compute_features` 计算收益、波动、流动性、市值、forward return 标签。
- 【已实现】通过 `SignalEngine`、`rank_cross_section`、`zscore_cross_section`、`linear_combine` 把 feature 转成 signal。
- 【已实现】通过 `build_top_n_portfolio` 把 signal 转成组合权重。
- 【已实现】通过 `run_backtest_from_signal` / `run_backtest_from_weights` 做简化回测、执行对齐、成本扣减和 summary metrics。
- 【已实现】通过 `src/qsys/research/*` 做 IC、Rank IC、quantile spread、turnover、decay、exposure、correlation 等诊断。
- 【部分实现】通过 `src/qsys/rebalance/*` 做 buffered Top-N、equal-weight benchmark、index benchmark、trade diagnostics 和 report 输出。
- 【部分实现】通过 `docs/notion_project_review/*.md` 和 `scripts/sync_project_review_to_notion.py` 形成 review / Notion 同步系统。

### 1.4 部分实现能力

部分能力已经有代码，但还不能视为成熟系统：

- 【部分实现】Data governance：`build_real_feature_store.py` 可以抓取和写入 feature store；`features/store.py` 可以记录 minimal sqlite metadata；但 raw/clean/feature 分层、schema contract、数据质量审计、增量调度仍不完整。
- 【部分实现】Benchmark/report：有 strict vs buffered、equal-weight、CSI300/CSI500/上证指数等比较入口；但 benchmark 数据来源治理、统一 report schema、结果 artifact version 还没成体系。
- 【部分实现】Risk-control：目前更多是 portfolio constraints 和 exposure diagnostics，不是 full risk model。
- 【部分实现】Volatility treatment：`demo_alpha_signal` 把 `vol_20d` 作为线性惩罚项；`risk/exposure.py` 又把波动作为 risk exposure state；但二者还没有统一的模型定义。
- 【部分实现】Testing：模块测试不少，但当前测试体系存在依赖环境、测试收集、行为预期冲突与 index alignment 问题。

### 1.5 尚未实现能力

这些能力不能因为 README 或文档中出现过相关词汇就认为已经实现：

- 【尚未实现】完整 ML predictive model。当前没有训练/验证/测试拆分、模型训练管线、模型持久化、超参搜索、特征重要性、预测校准等 ML pipeline。
- 【尚未实现】完整 risk model。当前没有 Barra-like exposure + factor covariance + specific risk + optimizer。
- 【尚未实现】production trading。没有实盘执行、订单管理、风控拦截、实时监控、日志审计、失败重试、交易权限。
- 【尚未实现】统一 experiment registry。没有统一记录 run config、数据版本、代码 commit、feature version、signal version、回测结果、报告 artifact 的中心表。
- 【尚未实现】正式 data quality framework。当前更像工程可用的 MVP，而不是严格数据治理系统。

### 1.6 与完整 production system 的差距

完整 production quant system 至少需要四类能力：数据可信、研究可复现、交易可执行、风险可控制。当前项目在“研究可复现”上已经有清晰基础，在“数据可信”和“风险可控制”上有 MVP 雏形，在“交易可执行”上基本没有展开。

所以它最合理的阶段定义是：

> 当前项目是一个已形成主链路的研究型量化平台 v1，而不是可直接实盘部署的交易系统。下一阶段不应直接跳复杂模型，而应先修复可靠性问题、固定实验协议、统一 report schema，并明确 volatility / constraints / risk model 的边界。

---

## 2. 全链路系统视角

项目主链路可以理解为一条逐步增加抽象层级的研究生产线。

`Data` 解决“原始数据怎么进入系统”；`Panel` 解决“数据如何统一成可研究的横截面/时间序列面板”；`Feature` 解决“可重复使用的研究变量如何生成”；`Signal` 解决“feature 如何变成可排序的 alpha score”；`Portfolio` 解决“score 如何变成权重”；`Backtest` 解决“权重在执行假设和成本假设下会产生什么收益路径”；`Diagnostics` 解决“结果是否有稳定结构，而不是偶然噪声”；`Constraints` 解决“组合是否满足基本可交易性和风险边界”；`Report` 解决“实验结果如何被长期记录、比较和复盘”。

| 层 | 为什么存在 | 输入 | 输出 | 关键文件 | 当前状态 |
|---|---|---|---|---|---|
| Data | 将外部行情源转为本地可回放数据 | AkShare symbol universe、daily OHLCV-like fields、日期参数 | `trade_date=YYYY-MM-DD/data.parquet` | `src/qsys/utils/build_real_feature_store.py` | 【已实现 MVP】 |
| Panel | 统一研究索引和字段口径 | parquet partitions | `MultiIndex [date, asset]` panel | `src/qsys/data/panel/daily_panel.py` | 【已实现】 |
| Feature | 生成可复用研究变量与 forward labels | panel | feature frame / feature store partitions | `src/qsys/features/*` | 【已实现 v1】 |
| Signal | 把 feature 转成可排序 score | feature frame、recipes、weights | `combined_signal` / signal series | `src/qsys/signals/*` | 【已实现 v1，有行为冲突需决策】 |
| Portfolio | 把 signal 转成权重 | signal、Top-N 参数、约束输入 | target weights | `src/qsys/backtest/portfolio.py` | 【已实现 MVP，有 index alignment bug】 |
| Backtest | 对齐未来收益、扣成本、汇总绩效 | weights/signal、asset returns、config | returns、turnover、cost、summary | `src/qsys/backtest/*` | 【已实现 MVP】 |
| Diagnostics | 检查预测性、稳定性、暴露、换手 | signal、forward returns、features、weights | IC/RankIC、quantile、decay、turnover、exposure | `src/qsys/research/*` | 【已实现 v1】 |
| Constraints | 做可交易性与组合层约束 | liquidity、market_cap、group labels、eligibility | constrained weights、impact diagnostics | `portfolio.py`, `universe/eligibility.py`, `risk/exposure.py`, `constraint_impact.py` | 【部分实现】 |
| Report | 输出实验比较与长期复盘材料 | backtest outputs、benchmark outputs、diagnostics | CSV/PNG/report docs/Notion sync | `src/qsys/utils/report_*`, `scripts/sync_*`, `docs/notion_project_review/*` | 【部分实现】 |

这条链路当前最重要的工程假设是 `MultiIndex [date, asset]`。几乎所有核心函数都依赖这个索引语义：横截面 rank/zscore 按 date 分组，forward returns 按 asset shift，backtest 按 date 聚合收益，exposure 也是 date 内横截面相关。也就是说，这个项目的“系统合同”并不只是列名，而是 **索引、时点、字段、label 对齐方式共同构成的协议**。

---

## 3. 端到端运行示例

下面用一条具体叙事说明当前系统如何从数据走到报告。

### 3.1 数据进入：AkShare → feature store partition

真实数据入口是：

```bash
PYTHONPATH=src python -m qsys.utils.build_real_feature_store \
  --feature-root data/processed/feature_store/v1 \
  --start-date 2020-01-01 \
  --limit 300
```

对应文件是 `src/qsys/utils/build_real_feature_store.py`。它做几件事：

1. `_fetch_symbol_universe(limit=...)` 通过 AkShare 获取 A 股股票列表；
2. `_safe_fetch_daily(symbol, retries, retry_wait)` 对单个 symbol 获取日线；
3. `_normalize_daily_frame(raw, symbol)` 将字段统一为 `trade_date`, `ts_code`, `open`, `high`, `low`, `close`, `volume`, `amount`, `turnover`, `outstanding_share` 等；
4. 在同一函数中计算 `ret_1d`, `ret_5d`, `ret_20d`, `vol_20d`, `amount_20d`, `turnover_5d`, `turnover_20d`, `market_cap`, `fwd_ret_5d`, `fwd_ret_20d`, `is_tradable`；
5. 按 `trade_date=YYYY-MM-DD/data.parquet` 写出。

这里有一个重要的设计现状：`build_real_feature_store.py` 直接输出的是 feature-store-like 数据，而不是纯 raw daily bars。它一边做 data ingestion，一边做 feature/label 计算。因此它是【已实现 MVP】，但不是严格分层的数据仓库架构。

### 3.2 Panel 构造：partition → MultiIndex frame

如果从标准 daily bars parquet 出发，`src/qsys/data/panel/daily_panel.py` 中的 `load_daily_panel` 会读取 `dataset_root/trade_date=YYYY-MM-DD/data.parquet`，把 `trade_date` 改成 `date`，把 `ts_code` 改成 `asset`，并设置索引为：

```python
MultiIndex([date, asset])
```

`DailyPanelReader._normalize` 也会把 `vol` 映射成 `volume`，并补齐 `open/high/low/close/volume/amount/adj_factor/market_cap/is_tradable` 等 expected columns。这个 panel 层的存在，是为了让下游 feature、signal、backtest 都不必关心原始数据源字段如何变化。

### 3.3 Feature store 生成：panel → feature frame → partitions

Feature 层有两条相关路径。

第一条是更标准的研究路径：

- `src/qsys/features/base.py` 定义 `BaseFeature` 抽象；
- `src/qsys/features/registry.py` 定义 `FeatureRegistry`；
- `src/qsys/features/compute.py` 中 `default_feature_registry()` 注册默认特征；
- `compute_features(panel, feature_names)` 计算选定特征；
- `src/qsys/features/store.py` 中 `write_feature_store()` 按日期写回 feature store；
- `record_feature_metadata()` 写入 sqlite sidecar metadata。

第二条是 `build_real_feature_store.py` 的快捷真实数据路径，它直接抓 AkShare 并生成相同/相近字段。这条路径更适合 Colab 和快速真实数据实验，但分层较弱。

当前默认 feature 包括：

- `ret_1d`, `ret_5d`, `ret_20d`
- `vol_20d`
- `turnover_5d`, `turnover_20d`
- `amount_20d`
- `market_cap`
- `fwd_ret_5d`, `fwd_ret_20d`

其中 `fwd_ret_5d` 和 `fwd_ret_20d` 的本质是 label，不是交易当下可见的 feature。它们通过 per-asset `shift(-5)` / `shift(-20)` 构造，反映 date t 的 signal 对未来 5/20 日收益的预测目标。因此，后续必须明确：feature store 是否允许 label columns 与 explanatory feature columns 混放。如果允许，需要在 schema 中标记 role；如果不允许，应拆成 `feature_store` 与 `label_store`。

### 3.4 Signal 计算：feature → score

Signal 入口在 `src/qsys/signals/engine.py`。

最典型 demo 是：

```python
demo_alpha_signal(features)
```

它实际执行：

```text
rank(ret_20d) - 0.5 * zscore(vol_20d)
```

这里的 `rank(ret_20d)` 是同一交易日横截面 rank，`zscore(vol_20d)` 是同一交易日横截面标准化波动率。`-0.5` 是一个固定线性波动惩罚系数。

这个设计的优点是非常可解释：偏向近期动量高、波动不过高的股票。问题是，波动率并不一定总是单调负面：在不同 regime、不同市值/流动性分组、不同趋势阶段下，高波动可能代表风险，也可能代表机会。因此当前 `-0.5*zscore(vol_20d)` 应标记为【MVP 假设】，不能视为已验证的通用 volatility model。

### 3.5 Portfolio weights：signal → target weights

Portfolio 层由 `src/qsys/backtest/portfolio.py` 的 `build_top_n_portfolio` 实现。它接受 `signal: pd.Series`，要求索引是 `MultiIndex [date, asset]`。核心逻辑是每个 date 横截面选 Top-N，然后生成等权或 long-short 权重。

它还支持几个约束参数：

- `long_only`
- `bottom_n`
- `max_single_weight`
- `liquidity` + `min_liquidity`
- `market_cap` + `size_aware_scaling`
- `group_labels` + `group_cap`

这说明当前 portfolio 层不仅是简单权重生成器，也承担了部分 constraints/risk-control prototype 的职责。不过，这些仍然是规则约束，不是优化器或风险预算模型。

本次测试发现，这个函数在使用 liquidity filter 时存在 index alignment 问题：`tests/backtest/test_portfolio_constraints.py::test_portfolio_constraints_contract` 会触发 `pandas.errors.IndexingError: Unalignable boolean Series provided as indexer`。同样问题会传导到 `tests/research/test_constraint_impact.py`。因此，Portfolio constraints 当前必须标记为【部分实现 / technical debt】，不能写成完全可靠。

### 3.6 Backtest：weights + returns → net return / cost / summary

Backtest MVP 在 `src/qsys/backtest/*`：

- `execution.py`：`align_next_day_returns`, `align_weights_and_returns`
- `cost.py`：`compute_turnover`, `compute_daily_cost`
- `metrics.py`：`summarize_metrics`
- `simulator.py`：`BacktestConfig`, `run_backtest_from_weights`, `run_backtest_from_signal`

核心流程是：

1. `run_backtest_from_signal` 调用 `build_top_n_portfolio` 得到 target weights；
2. `align_next_day_returns` 对 asset returns 做 per-asset `shift(-1)`，让 date t 的权重对应下一期收益；
3. `compute_turnover` 计算 `sum(abs(w_t - w_{t-1}))`；
4. `compute_daily_cost` 用 turnover × bps rate 计算成本；
5. `gross_return - cost` 得到 `strategy_return`；
6. `summarize_metrics` 输出 cumulative return、annual return、annual vol、Sharpe、max drawdown、turnover。

这里必须注意：`execution='next_open'` 当前只是接受参数并 warning，然后 fallback 到 `next_close`。所以执行模型是【MVP 假设】，不能被解释为真实 next-open execution backtest。

### 3.7 Diagnostics：signal/backtest → quality feedback

Diagnostics 不是为了证明策略一定赚钱，而是为了分解信号质量和组合行为。当前项目里有两组 diagnostics。

第一组是基础 research diagnostics：

- `src/qsys/research/ic.py`：`daily_ic`, `daily_rank_ic`, `ic_summary`
- `src/qsys/research/quantiles.py`：`quantile_mean_forward_returns`, `quantile_spread`
- `src/qsys/research/decay.py`：`decay_analysis`
- `src/qsys/research/turnover.py`：`signal_autocorrelation`, `top_n_turnover`
- `src/qsys/research/exposure.py`：size/group/feature exposure
- `src/qsys/research/correlation.py`：pairwise signal correlation

第二组是 signal quality MVP diagnostics：

- `src/qsys/research/signal_quality/align.py`
- `src/qsys/research/signal_quality/ic.py`
- `src/qsys/research/signal_quality/quantile.py`
- `src/qsys/research/signal_quality/conditioned_ic.py`
- `src/qsys/utils/run_signal_quality_mvp.py`
- `src/qsys/utils/run_signal_sanity_grid.py`

这些模块让你可以问：这个 signal 在 5 日/20 日 horizon 上有没有 Rank IC？分组收益是否单调？top-minus-bottom 是否为正？在高波动/低流动性/不同 size bucket 中信号是否稳定？

### 3.8 Report / review 输出

Report 层当前有两个方向。

第一，实验 report：`src/qsys/utils/report_rebalance_policy_comparison.py` 可以读取 rebalance comparison outputs，生成：

- `summary_metrics.csv`
- `policy_diff_metrics.csv`
- `cumulative_net_return.png`
- `turnover.png`
- 如果有 market benchmark，则生成 `market_benchmark_metrics.csv`、`buffered_excess_return_vs_benchmarks.csv`、`market_benchmark_comparison.png`

第二，项目 review / Notion 同步：`docs/notion_project_review/*.md` 保存系统文档；`scripts/sync_project_review_to_notion.py` 支持 child-pages 和 inline 两种同步模式。但当前 sync script 不会清空/覆盖旧内容，`README_SYNC.md` 也明确提示会产生重复页面或重复 block。因此 Notion 只能作为阅读层，GitHub docs 才是正式记录。

---

## 4. 模块级系统手册

### 4.1 Data ingestion

Data ingestion 的核心文件是 `src/qsys/utils/build_real_feature_store.py`。它存在的原因是把外部 A 股日线数据转化成项目内部可以直接研究的 parquet 分区数据。它的输入是 AkShare 的 symbol universe 和日线行情，输出是 `feature_root/trade_date=YYYY-MM-DD/data.parquet`。

关键函数包括 `_fetch_symbol_universe`、`_safe_fetch_daily`、`_normalize_daily_frame`、`build_real_feature_store` 和 `main`。`REQUIRED_COLUMNS` 定义了输出 schema，其中既包括行情字段，也包括计算后的 return、volatility、liquidity、market_cap、forward returns 和 `is_tradable`。

当前状态是【已实现 MVP】。它已经可以支持真实数据实验，但有几个重要边界：

- 【MVP 假设】AkShare schema 基本稳定；如果字段名变化，只靠当前 rename_map 不一定足够。
- 【technical debt】没有 raw-zone persistence，抓取后直接进入 normalized/feature-like 输出。
- 【technical debt】没有系统化 data quality report、缺失值报告、异常值报告、symbol-level 失败记录。
- 【需进一步验证】README 提到 sqlite metadata，但 `build_real_feature_store.py` 本身没有写 sqlite；metadata 写入存在于 `src/qsys/features/store.py` 的 `record_feature_metadata`，但两条路径没有完全统一。
- 【本地标准环境需复现】当前审查环境没有 `akshare`，所以 AkShare 相关真实抓取无法运行验证。

相关测试是 `tests/utils/test_build_real_feature_store.py`。由于当前环境缺少 parquet engine 与 AkShare，不能把相关测试失败直接归因于代码。

后续演化方向：把 Data 层拆成 raw/bronze、standardized/silver、feature/gold 三层；补 schema contract；补 data quality report；把失败 symbol、抓取时间、数据源版本、字段映射、缺失比例写入 metadata。

### 4.2 Daily panel

Daily panel 的核心文件是 `src/qsys/data/panel/daily_panel.py`。它的作用是统一所有研究模块的数据入口，让下游只处理 `MultiIndex [date, asset]`，而不需要关心 parquet 文件路径、原始字段名和缺失列补齐。

关键类和函数：

- `DailyPanelConfig`
- `DailyPanelReader`
- `DailyPanelReader.load`
- `DailyPanelReader._resolve_trade_dates`
- `DailyPanelReader._normalize`
- `load_daily_panel`

输入是 `dataset_root/trade_date=YYYY-MM-DD/data.parquet`；输出是 index 为 `date, asset` 的 DataFrame。`_normalize` 会把 `trade_date` → `date`，`ts_code` → `asset`，`vol` → `volume`，并补齐 `_EXPECTED_PANEL_COLUMNS`。

当前状态是【已实现】。它的工程边界比较清晰，但仍有技术债：

- 【technical debt】没有正式 panel schema registry。
- 【technical debt】异常分区、缺列分区、混合 schema 分区的容错和告警还不系统。
- 【本地环境问题】`tests/data/test_daily_panel.py` 在当前环境因缺少 `pyarrow`/`fastparquet` 失败，失败发生在测试写 parquet 阶段，不是 panel reader 业务逻辑本身。

后续演化方向：建立 panel schema validation；增加 partition-level audit；明确 `asset` 标识符规范；支持缓存读取和大规模增量读取。

### 4.3 Feature store

Feature store 涉及 `src/qsys/features/base.py`、`registry.py`、`compute.py`、`store.py`。它存在的原因是把可复用研究变量从数据读取逻辑中独立出来，让 signal 和 diagnostics 可以基于稳定特征集合运行。

核心对象：

- `BaseFeature`
- `FunctionFeature`
- `FeatureRegistry`
- `default_feature_registry`
- `compute_features`
- `FeatureStoreConfig`
- `materialize_features`
- `write_feature_store`
- `record_feature_metadata`
- `materialize_and_store_features`

`default_feature_registry` 中默认特征包括 return、volatility、turnover、amount、market cap、forward returns。`compute_features` 会检查 `MultiIndex [date, asset]`，对 panel 排序，并按 feature_names 逐一计算。

当前状态是【已实现 v1】。优势是模块化、轻量、容易加新特征；风险是 feature governance 还弱。

关键技术债：

- 【technical debt】缺 feature version pinning。现在 feature 名称和实现函数之间缺少长期版本契约。
- 【technical debt】缺 feature availability timestamp。比如 `ret_20d` 在 date t 是否已完全可用、何时可用于交易，应该被显式声明。
- 【需人工决策】`fwd_ret_5d` / `fwd_ret_20d` 是 label，不是当下可见 feature。是否继续与 feature 混放，需要 schema 级别决策。
- 【需进一步验证】sqlite metadata 存在于 `features/store.py`，但与 `build_real_feature_store.py` 的真实数据构建主路径未完全统一。

后续演化方向：建立 `feature_registry.yaml` 或类似配置，记录 feature name、version、required columns、availability lag、role(feature/label/exposure)、owner、test case。

### 4.4 Signal engine

Signal engine 的核心文件是 `src/qsys/signals/engine.py`、`transforms.py`、`combine.py`。它解决的问题是：feature 本身不等于交易信号，必须经过横截面标准化、rank、winsorize、neutralization 或线性组合，才变成可排序 score。

核心函数：

- `load_feature_store_frame`
- `SignalEngine.apply_transform`
- `SignalEngine.build_transformed_signals`
- `SignalEngine.combine`
- `demo_alpha_signal`
- `winsorize_cross_section`
- `zscore_cross_section`
- `rank_cross_section`
- `neutralize_by_size`
- `neutralize_by_group`
- `linear_combine`

当前 demo alpha 是 `rank(ret_20d) - 0.5*zscore(vol_20d)`。它不是完整 predictive model，而是一个可解释的 scoring rule。`rank_cross_section` 默认 percentile rank；`zscore_cross_section` 在每个 date 内做标准化；`linear_combine` 用固定权重组合信号。

当前状态是【已实现 v1】，但有一个必须优先处理的行为一致性问题。

本次可运行的定向测试：

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src pytest -q --import-mode=importlib \
  tests/signals/test_engine.py \
  tests/signals/test_transforms.py \
  tests/research/test_ic.py \
  tests/research/test_quantile_or_corr.py \
  tests/research/test_exposure.py \
  tests/risk/test_exposure.py \
  tests/universe/test_eligibility.py \
  tests/backtest/test_portfolio.py \
  tests/backtest/test_simulator_metrics.py
```

结果是：22 passed, 1 failed。失败是：

```text
tests/signals/test_engine.py::test_demo_alpha_combination_behavior
assert 0.5 > 1.0
```

这个失败的含义不是“信号引擎整体不可用”，而是 `demo_alpha_signal` 的业务预期与当前公式存在冲突。测试注释认为 2024-01-02 的 B 应该因为更高 return rank 而超过 A，但当前公式中 B 的 volatility z-score 更高，`-0.5*zscore(vol)` 的惩罚使 B 得分低于 A。这个结果从数学上是可解释的，所以这里更像【需人工决策】：到底是测试预期应更新，还是 `-0.5` 的惩罚尺度/变换方式应调整。

后续演化方向：在复杂 ML 之前，应先把 signal recipe、参数、方向、处理 NaN/极值、测试样例意图固定下来。否则复杂模型只会把一个不稳定的信号协议放大。

### 4.5 Portfolio construction

Portfolio construction 的核心文件是 `src/qsys/backtest/portfolio.py`。它把 signal 映射成 target weights，是 Signal 和 Backtest 之间的关键边界。

核心函数：

- `_normalize_weights`
- `_apply_group_cap_long_only`
- `build_top_n_portfolio`

`build_top_n_portfolio` 当前支持：

- long-only Top-N；
- long-short bottom leg；
- `max_single_weight` 单票上限；
- `liquidity` + `min_liquidity` 流动性过滤；
- `market_cap` + `size_aware_scaling` 规模感知缩放；
- `group_labels` + `group_cap` 分组上限。

当前状态是【已实现 MVP / 存在 technical debt】。

重要测试发现：

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src pytest -vv -s --import-mode=importlib \
  tests/backtest/test_portfolio_constraints.py
```

失败信息为：

```text
pandas.errors.IndexingError: Unalignable boolean Series provided as indexer
```

问题位置在 `src/qsys/backtest/portfolio.py` 的 liquidity filter 附近：

```python
g = g[g.droplevel("date").isin(keep_assets)]
```

这里 boolean Series 的 index 是 asset-level，而 `g` 的 index 是 MultiIndex `[date, asset]`，导致 pandas alignment 失败。这个问题也影响 `tests/research/test_constraint_impact.py`，因为 constraint impact 会调用 constrained `build_top_n_portfolio`。

因此，Portfolio constraints 不能被写成完全可靠。更准确说：基础 Top-N 已经可用；带 liquidity filter / size scaling / group cap 的约束路径需要修复 index alignment，并重新确认约束顺序和归一化语义。

后续演化方向：将 portfolio construction 与 constraints application 拆成更清晰的步骤：先生成 candidate weights，再 apply eligibility/liquidity/cap/group constraints，再 normalize，再输出 constraints impact log。

### 4.6 Backtest engine

Backtest engine 的核心文件包括：

- `src/qsys/backtest/execution.py`
- `src/qsys/backtest/cost.py`
- `src/qsys/backtest/metrics.py`
- `src/qsys/backtest/simulator.py`

它存在的原因是把 target weights 转换成可评估的收益路径。当前 `BacktestConfig` 管理 `top_n`、`long_only`、`bottom_n`、`rebalance`、`execution`、`transaction_cost_bps`、`slippage_bps`。

实现逻辑是标准 MVP：权重乘下一期收益，扣 turnover-based linear cost，再汇总收益、波动、Sharpe、回撤。

当前状态是【已实现 MVP】。本次定向测试中 `tests/backtest/test_portfolio.py` 和 `tests/backtest/test_simulator_metrics.py` 可以通过，说明基础 Top-N 和 metrics/backtest 主路径有基本回归保护。

但有几个必须标记的限制：

- 【MVP 假设】`next_open` 当前 fallback 到 `next_close`，不能解释为真实 next-open execution。
- 【MVP 假设】交易成本是线性的 turnover × bps，不含 market impact、capacity、涨跌停、停牌、成交量约束。
- 【technical debt】`simulator._rebalance_dates` 的 weekly/monthly 使用 period head；而 `rebalance/backtest.py` 和 `benchmarks.py` 使用 period tail。这是一个潜在语义不一致：weekly rebalance 到底是周初还是周末，需要统一。
- 【需进一步验证】真实 A 股环境下停牌、涨跌停、ST、退市、复权、分红等处理尚未形成完整执行假设。

后续演化方向：先明确 execution convention，再做成本敏感性分析和 liquidity-aware execution。不要在执行假设未定时过早解释收益曲线。

### 4.7 Rebalance policy

Rebalance 相关文件在 `src/qsys/rebalance/*`。这是一个独立于基础 backtest simulator 的策略执行层扩展，核心是 buffered Top-N。

关键文件：

- `src/qsys/rebalance/policies.py`
- `src/qsys/rebalance/backtest.py`
- `src/qsys/rebalance/costs.py`
- `src/qsys/rebalance/diagnostics.py`
- `src/qsys/rebalance/benchmarks.py`
- `src/qsys/rebalance/index_benchmarks.py`

`BufferedTopNPolicyConfig` 定义 `target_n`、`buy_rank`、`sell_rank`、`min_holding_n`、`max_holding_n`、`rebalance`、`min_trade_weight`、`max_single_weight`、`cost_bps` 等参数。`build_buffered_top_n_weights` 的核心思想是：不是 signal rank 一变就交易，而是设置 buy zone、sell zone 和 buffer zone，从而降低不必要换手。

当前状态是【部分实现】。它已经有比较完整的规则和测试文件，例如：

- `tests/rebalance/test_buffered_top_n_policy.py`
- `tests/rebalance/test_buffered_top_n_backtest.py`
- `tests/rebalance/test_buffered_rebalance_diagnostics.py`
- `tests/rebalance/test_report_rebalance_policy_comparison.py`

但它也引入了新的系统边界问题：基础 `backtest/simulator.py` 和独立 `rebalance/backtest.py` 对 rebalance date 的定义不一致。前者 weekly/monthly 取 period head，后者取 period tail。这个不是普通代码风格问题，而是会影响收益对齐和交易时点解释的系统假设问题。

后续演化方向：将 rebalance convention 写成全项目统一协议，并在 report 中输出：rebalance frequency、rebalance date rule、return alignment、cost model。

### 4.8 Diagnostics / signal quality

Diagnostics 层已经相当丰富，核心文件在 `src/qsys/research/*` 和 `src/qsys/research/signal_quality/*`。

当前实现包括：

- IC / Rank IC：`daily_ic`, `daily_rank_ic`, `compute_ic_by_date`, `summarize_ic`
- Quantile return：`quantile_mean_forward_returns`, `assign_quantiles_by_date`, `compute_quantile_forward_returns`, `compute_quantile_spread`
- Decay：`decay_analysis`
- Turnover/persistence：`signal_autocorrelation`, `top_n_turnover`
- Exposure：`size_exposure_daily`, `group_exposure_daily`, `signal_feature_correlation_daily`, `exposure_summary`
- Risk exposure matrix：`build_risk_exposure_matrix`
- Conditioned IC：`assign_condition_buckets`, `compute_conditioned_rank_ic`
- Portfolio exposure：`compute_portfolio_exposure`, `summarize_exposure_stability`

当前状态是【已实现 v1】。它的价值很高，因为它让项目从“跑收益曲线”变成“拆解信号质量”。但它还不是完整 research report system，原因是：

- 【technical debt】指标分散，缺统一 diagnostics report schema。
- 【technical debt】没有固定输出一个完整 tearsheet 或 experiment artifact。
- 【MVP 假设】ICIR、positive_rate、top-minus-bottom 等指标的阈值还没有项目内标准。
- 【需进一步验证】conditioned IC 的 bucket 需要足够样本，否则很容易得出不稳定结论。

后续演化方向：建立统一 `diagnostics_report` schema，至少包含 signal summary、label coverage、IC by horizon、quantile summary、turnover、exposure、conditioned IC、portfolio metrics、benchmark comparison、warnings。

### 4.9 Constraints / risk control

当前 constraints/risk-control 相关模块分散在：

- `src/qsys/backtest/portfolio.py`
- `src/qsys/universe/eligibility.py`
- `src/qsys/risk/exposure.py`
- `src/qsys/research/constraint_impact.py`
- `src/qsys/research/portfolio_exposure.py`

`universe/eligibility.py` 可以根据 required columns、`is_tradable`、`amount_20d`、`turnover_20d`、`market_cap` 生成 eligibility mask；`risk/exposure.py` 可以生成 `vol_20d_z`、`liquidity_z`、`size_z`；`portfolio.py` 可以施加组合约束；`constraint_impact.py` 可以比较 constrained vs unconstrained 的结果。

当前状态是【部分实现】。它已经比简单 Top-N 更进一步，但仍不是 full risk model。

关键边界：

- portfolio constraints 是“规则过滤/限制”；
- risk exposure matrix 是“暴露观测/诊断”；
- full risk model 应包括 factor exposure、factor covariance、specific risk、risk contribution、tracking error、optimizer；
- 当前项目还没有 optimizer 和风险预算目标函数。

此外，因为 `build_top_n_portfolio` 的 liquidity filter 存在 index alignment 问题，constraint impact 当前也应标记为【technical debt / 需修复后复测】。

后续演化方向：先把 constraints 层稳定下来，再把 volatility、liquidity、size 从单纯 penalty/filters 扩展为 risk states 和 portfolio exposure controls。

### 4.10 Report / Notion review

Report 层目前包括实验输出和文档同步两块。

实验输出由 `src/qsys/utils/report_rebalance_policy_comparison.py` 负责，能生成 summary CSV 和 PNG 图。它说明项目已经开始从“脚本跑结果”走向“实验报告”。但当前 report 还不是统一系统：不同 utils 输出不同 schema，缺少 run metadata、输入数据版本、参数、代码版本、警告信息的统一记录。

Notion review 系统由 `docs/notion_project_review/*.md` 和 `scripts/sync_project_review_to_notion.py` 组成。`README_SYNC.md` 明确写出 child-pages 和 inline 两种模式，也明确警告重复同步风险。这个系统的价值在于阅读和复盘，不在于替代 GitHub。

当前状态是【部分实现】。后续应把 `PROJECT_SYSTEM_MANUAL.md` 放在 GitHub docs 中作为主手册，Notion 只是同步阅读层。

### 4.11 Tests / reliability

当前测试覆盖面较广，目录包括：

- `tests/data/*`
- `tests/features/*`
- `tests/signals/*`
- `tests/backtest/*`
- `tests/rebalance/*`
- `tests/research/*`
- `tests/risk/*`
- `tests/universe/*`
- `tests/utils/*`

这说明项目已经有比较好的模块回归意识。但本次审查发现，可靠性并不等于“测试文件多”。目前至少有五类问题：

1. 【本地环境问题】当前审查环境缺 `pyarrow`/`fastparquet`，parquet 测试无法代表代码真实状态。
2. 【本地环境问题】当前审查环境缺 `akshare`、`SQLAlchemy`，真实数据抓取和部分 metadata 路径无法验证。
3. 【测试收集问题】同时收集 `tests/research/test_exposure.py` 与 `tests/risk/test_exposure.py` 时，在默认 import mode 下出现 `import file mismatch`。使用 `--import-mode=importlib` 可以绕过这类同名模块收集问题。
4. 【行为预期冲突】`test_demo_alpha_combination_behavior` 与 `demo_alpha_signal` 数学结果冲突。
5. 【代码行为问题】`test_portfolio_constraints.py` 和 `test_constraint_impact.py` 暴露 liquidity filter 的 MultiIndex boolean alignment bug。

因此，测试体系应先做可靠性清理，再扩大覆盖。

---

## 5. 当前模型本质与模型假设

### 5.1 当前系统里的 “model” 分成哪几层

当前项目中的 “model” 不是单一机器学习模型，而是一组规则、假设和反馈机制组合在一起的研究模型。可以拆成五层。

第一层是 **signal scoring model**。当前代表是 `demo_alpha_signal(features)`：

```text
score = rank(ret_20d) - 0.5 * zscore(vol_20d)
```

它是一个横截面打分模型，用于排序股票。它不是训练出来的 predictive model，而是人为设定的 scoring rule。

第二层是 **portfolio construction model**。`build_top_n_portfolio` 把 score 转成权重。Top-N、long-only、long-short、max weight、liquidity filter、size scaling、group cap 都属于这一层。

第三层是 **backtest execution assumption**。`align_next_day_returns`、`compute_turnover`、`compute_daily_cost` 共同定义“如果按这个权重交易，在简化执行和成本假设下会发生什么”。这不是市场真实执行模型，而是 MVP research accounting model。

第四层是 **diagnostics feedback loop**。IC、Rank IC、quantile、turnover、exposure、conditioned IC 用于判断 signal 是否值得改、portfolio 是否过度换手、收益是否来自某类暴露。

第五层是 **constraints / risk-control layer**。eligibility、liquidity、single-name cap、group cap、risk exposure matrix 共同构成早期 risk-control prototype。

### 5.2 当前是否是完整 predictive model

【尚未实现】当前不是完整 predictive model。原因是：

- 没有训练/验证/测试切分；
- 没有 model fit/predict 接口；
- 没有模型参数学习过程；
- 没有 out-of-sample evaluation 协议；
- 没有 model artifact 保存；
- 没有超参搜索或模型选择框架。

当前 demo alpha 可以被称为 scoring model 或 rule-based alpha baseline，但不能称为完整预测模型。

### 5.3 当前是否是完整 ML model

【尚未实现】当前不是完整 ML model。虽然项目未来可以接入 Ridge、Lasso、Tree、XGBoost、NN 等模型，但现在的核心系统仍是规则信号 + 组合构建 + 回测诊断。

这并不是缺点。相反，在基础数据协议、label 协议、回测执行、诊断 schema 还没有完全稳定之前，直接上复杂 ML 可能会制造更难解释的问题。复杂模型可能提高拟合能力，但也会放大数据泄漏、label 混乱、样本不稳定和交易成本误判。

### 5.4 当前是否是完整 risk model

【尚未实现】当前不是完整 risk model。它有 risk-related components，但没有完整 risk model 的核心结构：

- factor exposure matrix；
- factor covariance；
- specific risk；
- risk contribution；
- tracking error；
- optimizer constraints；
- risk budget objective。

`src/qsys/risk/exposure.py` 中的 `build_risk_exposure_matrix` 更准确地说是 risk exposure diagnostics，不是 risk model。

### 5.5 当前 demo alpha 的本质

当前 demo alpha 的本质是：

> 一个可解释、横截面、规则型、MVP 的 signal scoring baseline。

它的优点是简单透明：动量正向、波动惩罚负向。它的局限也非常明确：

- 固定 `-0.5` 没有经过稳定性验证；
- volatility 的作用被简化为线性负效应；
- 没有区分不同 market regime；
- 没有区分 signal layer 的 volatility penalty 和 risk-control layer 的 volatility exposure；
- 当前测试预期与公式结果发生冲突。

### 5.6 Base model 下一步应该怎么定义

下一阶段的 base model 不应直接定义成复杂 ML。更合理的是先定义一个 **可验证的 base model protocol**：

1. 明确输入：哪些 columns 是 feature，哪些是 label，哪些是 exposure，哪些是 eligibility。
2. 明确 scoring：例如 `score = rank(ret_20d)`、`score = rank(ret_20d) - k*zscore(vol_20d)`、或分组/条件化版本。
3. 明确 label：`fwd_ret_5d` 与 `fwd_ret_20d` 的构造方式、对齐方式、是否含成本。
4. 明确 portfolio：Top-N、strict vs buffered、rebalance convention。
5. 明确 diagnostics：必须输出 IC、Rank IC、quantile spread、turnover、exposure、benchmark comparison。
6. 明确 acceptance criteria：什么情况下认为 base model 比 baseline 有进步。

在这个协议稳定之后，再考虑 nonlinear extension，比如 volatility condition、regime state、Ridge/Lasso、tree-based model 或 ranking model。

---

## 6. Constraints vs Risk Control

### 6.1 当前已经实现的 constraints / eligibility / exposure diagnostics

当前项目已经实现了若干 constraints 和 exposure diagnostics。

在 `src/qsys/universe/eligibility.py` 中，`build_eligibility_mask` 可以基于以下条件生成可交易样本：

- required columns not null；
- `is_tradable == True`；
- `amount_20d >= min_amount_20d`；
- `turnover_20d >= min_turnover_20d`；
- `market_cap >= min_market_cap`。

在 `src/qsys/backtest/portfolio.py` 中，`build_top_n_portfolio` 支持：

- liquidity filter；
- max single-name cap；
- size-aware scaling；
- group cap；
- long-only / long-short。

在 `src/qsys/risk/exposure.py` 中，`build_risk_exposure_matrix` 可以输出：

- `vol_20d_z`
- `liquidity_z`
- `size_z`

在 `src/qsys/research/portfolio_exposure.py` 中，可以计算 portfolio exposure 并总结 exposure stability。

这些能力意味着项目已经进入 risk-control prototype 阶段，不再只是裸 signal 回测。

### 6.2 为什么这还不是 full risk model

这些 constraints 不能等同于 full risk model。核心区别是：

- Constraints 是规则：不买低流动性股票、单票不超过 x%、行业不超过 y%。
- Risk model 是度量：组合对哪些风险因子暴露多少，这些因子的协方差是多少，组合 tracking error 和 risk contribution 是多少。
- Optimizer 是决策：在 alpha、风险、成本、约束之间求最优权重。

当前项目有 constraints 和 exposure diagnostics，但没有 factor covariance，也没有 optimizer，因此不是 full risk-control system。

### 6.3 Volatility 当前属于哪一层

Volatility 当前同时出现在三个层面，因此必须避免概念混乱。

第一，Signal 层：`demo_alpha_signal` 使用 `-0.5*zscore(vol_20d)`。这表示 volatility 被当作 alpha penalty，即高波动降低 score。

第二，Risk exposure 层：`build_risk_exposure_matrix` 输出 `vol_20d_z`。这表示 volatility 被当作一种可观测风险状态或暴露变量。

第三，Diagnostics 层：`conditioned_ic.py` 可以基于 `vol_20d_z_bucket` 做 conditioned Rank IC。这表示 volatility 可用于分析 signal 在不同风险状态下是否稳定。

当前还没有 Portfolio optimizer 层把 volatility 作为风险预算约束，也没有 regime model 把 volatility 作为状态切换变量。因此，固定 linear volatility penalty 只能标记为【MVP 假设】。

### 6.4 未来 volatility 应该如何演化

未来至少有四种处理方式：

1. **Penalty**：继续作为 score 中的惩罚项，但参数不应固定拍脑袋，应做 grid / conditioned analysis。
2. **Condition**：不直接惩罚，而是判断 signal 在高/中/低 volatility bucket 下是否有效。
3. **Risk exposure**：作为 portfolio exposure，控制组合整体波动暴露不要过高。
4. **Regime state**：作为市场或个股状态变量，决定不同 regime 下使用不同 signal 或权重规则。

对当前项目来说，最合理下一步不是马上改成 GARCH 或复杂 volatility model，而是先用 `run_signal_sanity_grid.py` / conditioned IC / exposure diagnostics 验证 volatility 到底更适合当 penalty、condition、risk exposure 还是 regime state。

---

## 7. Diagnostics 解释框架

Diagnostics 的正确用法是“组合判断”，不是单指标崇拜。当前项目中的诊断指标可以按问题分组。

### 7.1 IC / Rank IC

IC 衡量 signal 与 forward return 的横截面相关性。`daily_ic` 用 Pearson，`daily_rank_ic` / `compute_ic_by_date(method="spearman")` 用 Spearman Rank IC。

Rank IC 更适合当前项目，因为 signal 本质上用于横截面排序，而不一定要求 score 与收益线性相关。

但要注意：

- Rank IC 高不等于净收益稳定。
- IC 是 signal 层指标，不自动包含 portfolio construction、rebalance、交易成本。
- IC 受样本数量、横截面大小、极端值、行业/市值暴露影响。
- 多个 horizon 的 IC 需要一起看，否则可能误判信号持有周期。

### 7.2 ICIR

ICIR 通常是 mean IC / std IC，表示 IC 序列的稳定性。当前 `summarize_ic` 使用 `std(ddof=1)` 并输出 t-stat。

ICIR 的价值在于看 signal 是否稳定，而不是偶尔几天很强。但它也有局限：

- 样本天数太少时不可靠。
- IC 分布非正态时 t-stat 解释要谨慎。
- 如果 signal 在某些 regime 有效、某些 regime 失效，整体 ICIR 会掩盖结构。

### 7.3 Quantile return / top-minus-bottom

Quantile diagnostics 把股票按 signal 分组，看高分组未来收益是否高于低分组。`compute_quantile_spread` 输出 top-minus-bottom 和 top-minus-universe。

它的优点是直观：如果 Q5 长期高于 Q1，说明排序可能有经济意义。但要注意：

- quantile spread 可能受样本大小影响；
- qcut 在重复值多、样本少时会不稳定；
- 分组收益没有自动扣交易成本；
- Top-minus-bottom 正，不代表 Top-N portfolio after-cost 一定赚钱；
- quantile 边界变化会影响结果。

### 7.4 Turnover

Turnover 既可以指 signal 的 top-N membership turnover，也可以指 portfolio weights turnover。当前项目两者都有相关逻辑。

正确解读是：turnover 必须和 cost 一起看。一个 signal 预测性强但 turnover 很高，可能在扣成本后失效。Buffered rebalance 的意义就在这里：不是改变 signal 本身，而是减少由微小 rank 波动导致的无效交易。

### 7.5 Decay

Decay analysis 比较不同 forward return horizon 下的 IC/Rank IC。如果 5 日有效、20 日无效，说明信号偏短周期；如果 20 日更稳定，说明持有期可以更长。

Decay 不能单独决定 rebalance frequency，因为 rebalance 还受交易成本、持仓稳定性、市场冲击影响。

### 7.6 Exposure

Exposure diagnostics 用来回答：signal 到底是在预测收益，还是只是暴露在某个已知风险/风格上？

当前项目可以做 size exposure、group exposure、signal-feature correlation、portfolio exposure。这里必须区分：

- signal exposure：score 与某个 feature/risk variable 的关系；
- portfolio exposure：权重加权后组合对某个变量的暴露；
- return attribution：最终收益由哪些暴露贡献。

当前项目已有前两类雏形，但 attribution 仍不完整。

### 7.7 Conditioned IC

Conditioned IC 是非常重要的下一阶段工具。它回答：signal 在不同 volatility/liquidity/size bucket 下是否仍然有效。

这对 volatility 尤其关键。如果高波动 bucket 中 Rank IC 更强，那么简单 volatility penalty 可能错误；如果高波动 bucket 中 Rank IC 方向反转，那么 volatility 应作为 risk gate 或 regime state。

### 7.8 Benchmark comparison

Benchmark comparison 是策略层诊断，不是 signal 层诊断。当前项目已有 equal-weight、CSI300、CSI500、上证指数比较雏形。

正确使用方式是：

- signal diagnostics 看预测质量；
- portfolio/backtest 看可交易收益；
- benchmark comparison 看是否超越简单替代方案；
- turnover/cost 看收益是否被交易拖垮；
- exposure 看收益是否只是某种风格暴露。

---

## 8. 技术债与可靠性问题

### 8.1 P0 高优先级 technical debt

**1. Portfolio constraints 的 index alignment bug**

`tests/backtest/test_portfolio_constraints.py` 和 `tests/research/test_constraint_impact.py` 暴露了 liquidity filter 的 MultiIndex boolean alignment 问题。这会影响 constrained portfolio 和 constraint impact analysis，属于高优先级 technical debt。

影响范围：`build_top_n_portfolio`、constraint impact、后续 risk-control prototype。

后续动作：先修复/确认 liquidity filter 对 MultiIndex 的筛选方式，再复测 constraints 和 constraint_impact。不要在这个问题未清理前解释 constrained backtest 结果。

**2. Signal demo 行为预期冲突**

`test_demo_alpha_combination_behavior` 失败，说明 `rank(ret_20d) - 0.5*zscore(vol_20d)` 的数学结果与测试注释预期不一致。

影响范围：demo alpha、volatility penalty 解释、后续 base model 定义。

后续动作：人工决定是改测试预期，还是调整 signal formula/penalty scaling。这个决策应记录在 phase log 或系统手册更新中。

**3. Rebalance semantics 不一致**

`src/qsys/backtest/simulator.py` 的 `_rebalance_dates` weekly/monthly 使用 period head；`src/qsys/rebalance/backtest.py` 和 `benchmarks.py` 使用 period tail。

影响范围：strict backtest、buffered backtest、benchmark comparison、report 解读。

后续动作：统一 weekly/monthly rebalance 到底是周初、周末、还是下一个交易日执行，并在所有 report 中写明。

**4. 测试命令与当前环境不一致**

README 写 `PYTHONPATH=src pytest -q`，但当前审查环境直接运行存在超时/收集问题；禁用第三方插件并使用 `--import-mode=importlib` 后可运行部分测试。同时缺少 parquet / AkShare / SQLAlchemy 依赖。

影响范围：可靠性判断、CI 配置、开发者复现。

后续动作：本地标准环境中安装 `requirements.txt`，用一致命令复现；考虑在 pytest 配置中固定 import mode 或重命名同名测试文件。

### 8.2 P1 中优先级 technical debt

**1. Feature / label schema 混放**

`fwd_ret_5d`、`fwd_ret_20d` 是 label，但存在于 feature store 输出中。研究阶段可以接受，但必须显式标记 role，否则未来 ML 或 signal recipe 很容易误用 label 造成 leakage。

**2. Feature availability 缺失**

当前 feature 没有统一声明何时可用。日频交易中，收盘后生成特征、下一交易日开盘/收盘执行，这些时点必须被写入协议。

**3. Report schema 分散**

`report_rebalance_policy_comparison.py` 能生成多个 CSV/PNG，但没有统一 metadata。未来需要把 run config、feature root、date range、signal formula、rebalance rule、cost bps、benchmark source 都写入一个 manifest。

**4. Data quality 弱**

AkShare schema drift、缺失值、异常价格、停牌、复权、涨跌停等都需要 data quality contract。

**5. Notion sync duplication**

`sync_project_review_to_notion.py` 明确不会覆盖/清空旧内容。child-pages 每次创建新页面，inline 每次 append。这个不是致命问题，但如果 Notion 被误当 source of truth，会造成知识库混乱。

### 8.3 P2 低优先级但应记录的 technical debt

- 缺统一 CLI command surface，现在 utils 脚本较分散。
- 缺 performance / scale regression tests。
- 缺大型真实样本 snapshot tests。
- 缺 benchmark 数据源版本治理。
- 缺图表输出视觉回归或 artifact check。
- README 中部分 future work 已经部分实现，应更新措辞。

---

## 9. 已过时或需要修订的文档

### 9.1 README.md

README 的总体定位仍然成立：这是 research-oriented A-share systematic trading framework，并且明确不是 production live trading。这个定位应保留。

但以下内容需要更新或加限定：

- `Current status: V1 complete` 应改为更精确的表述：`V1 research infrastructure largely implemented; strategy/risk/report layers remain MVP/partial`。
- Data Layer 中写到 `sqlite metadata`，但真实数据入口 `build_real_feature_store.py` 不写 sqlite；metadata 逻辑在 `features/store.py` 中，主路径接线需进一步验证。
- Repository Structure 里提到 `run_demo.py`，当前材料包中没有该文件，标记为【文档需更新 / 需进一步验证】。
- Legacy Notebook 提到 `A_share_Analytical_DWH.ipynb`，当前材料包中没有该 notebook，标记为【需进一步验证】。
- Future Work 中 `benchmark comparison`、`report / tearsheet generation`、`richer exposure controls` 已经部分实现，应从 Future Work 改成 `partially implemented / needs standardization`。
- Testing 部分只写 `PYTHONPATH=src pytest -q` 不够。应补充依赖前提、parquet engine、pytest import mode 或 CI 环境。

### 9.2 docs/notion_project_review/*.md

`00_index.md` 到 `12_project_tracking_log_template.md` 很适合作为模块化 review docs，但多数文件偏短，适合作为入口，不足以替代系统手册。

`PROJECT_SYSTEM_MANUAL_DRAFT.md` 已经抓住主线，但仍是草案，缺少本次测试发现、具体文件路径全量化、状态标签和过时文档检查。因此应由本文件取代或升级。

`deep_review_evidence_pack.md` 很有价值，应作为证据包保留，而不是被系统手册取代。它适合给 Codex/GPT 快速定位代码证据。

`PROJECT_QUERY_GUIDE.md` 应继续保留，并在本手册第 11 章中扩展成更具体的“问题 → 查询路径”。

`README_SYNC.md` 仍然成立，特别是关于 Notion 重复同步的警告。但应补一句：GitHub docs 是 source of truth，Notion 是同步阅读层。

### 9.3 project_map.json

`project_map.json` 是结构化索引，很有用。它列出了 module、main_files、key functions、status、limitations、technical debt 等。但是它不应代替人工系统手册，因为：

- 它不能充分解释模块之间的边界和假设；
- 它不能记录本次测试中发现的新问题；
- 它对代码行为的描述需要持续校验。

建议后续每次大 phase 后同时更新 `project_map.json` 和 `PROJECT_SYSTEM_MANUAL.md`。

---

## 10. 需人工决策的问题

这些问题不能由代码自动决定，必须由项目 owner 做研究取舍。

### 10.1 Volatility 是 penalty、condition、risk exposure 还是 regime state？

当前公式把 volatility 当 penalty，但数学直觉上 volatility 不一定单调负面。下一阶段应通过 conditioned IC 和 grid diagnostics 判断它的角色。

建议决策顺序：先 condition，再决定是否 penalty；先 exposure diagnostics，再决定是否 portfolio constraint；最后才考虑 regime model。

### 10.2 demo alpha 应改测试还是改实现？

`test_demo_alpha_combination_behavior` 失败不是简单 bug。当前公式确实会让高波动股票被惩罚。如果测试期望是“return rank 的优势应压过 volatility penalty”，那就要改公式尺度；如果公式本来就是强惩罚波动，那就要改测试注释和预期。

### 10.3 weekly rebalance 用周初还是周末？

`backtest/simulator.py` 与 `rebalance/backtest.py` 当前不一致。这个会影响收益解释。需要决定：

- 周初调仓：更像拿上周信息在本周开始执行；
- 周末调仓：更像一周结束后形成新组合；
- 下一交易日执行：需要更明确的 signal date / execution date 分离。

### 10.4 constraints 是否拆成独立 layer？

目前 constraints 混在 portfolio construction 中。为了长期维护，建议拆成独立 layer：`eligibility → candidate selection → raw weights → constraints → normalized weights → constraint log`。

### 10.5 feature store 是否允许 label columns？

如果允许，必须加 role 标记，防止 leakage。如果不允许，应拆分 label store。这个决策会影响未来 ML pipeline。

### 10.6 下一阶段先做 base model、risk control 还是 diagnostics report schema？

我的判断是：

1. 先修 P0 reliability；
2. 再固定 base model protocol；
3. 同步做 diagnostics/report schema；
4. risk-control module 作为 P3；
5. complex ML/nonlinear extension 放后。

原因是，当前最危险的不是模型不够复杂，而是协议和解释边界还没有完全固定。

---

## 11. 如何查询系统细节

这一章是给后续维护用的。每次想查一个问题，不要只搜 README，要从“入口文档 → 源码 → 测试 → 运行脚本”四步走。

### 11.1 我想查 feature 如何生成

推荐阅读：

- `docs/notion_project_review/03_panel_and_feature_store.md`
- `docs/notion_project_review/deep_review_evidence_pack.md` 的 Feature store 部分

源码路径：

- `src/qsys/features/base.py`
- `src/qsys/features/registry.py`
- `src/qsys/features/compute.py`
- `src/qsys/features/store.py`
- `src/qsys/utils/build_real_feature_store.py`

关键函数：

- `default_feature_registry`
- `compute_features`
- `materialize_features`
- `write_feature_store`
- `_normalize_daily_frame`

相关测试：

- `tests/features/test_feature_compute.py`
- `tests/utils/test_build_real_feature_store.py`

应该继续问 GPT/Codex 的问题：

> 请检查 `default_feature_registry` 中每个 feature 的 required columns、计算窗口、index 对齐和是否存在 look-ahead 风险。

### 11.2 我想查 fwd_ret_5d / fwd_ret_20d 是如何构造的

源码路径：

- `src/qsys/features/compute.py`
- `src/qsys/utils/build_real_feature_store.py`

关键逻辑：

- `close.shift(-5) / close - 1`
- `close.shift(-20) / close - 1`
- 都是 per-asset forward label。

相关测试：

- `tests/features/test_feature_compute.py`
- `tests/research/signal_quality/test_signal_quality_mvp.py`

应该继续问：

> 请确认 `fwd_ret_5d` 和 `fwd_ret_20d` 在 feature store 中作为 label 使用时，signal date、label horizon、execution date 是否一致，是否需要从 feature columns 中拆出 label role。

### 11.3 我想查 signal 如何生成

推荐阅读：

- `docs/notion_project_review/04_signal_engine.md`
- `docs/notion_project_review/08_model_review.md`

源码路径：

- `src/qsys/signals/engine.py`
- `src/qsys/signals/transforms.py`
- `src/qsys/signals/combine.py`

关键函数：

- `SignalEngine.build_transformed_signals`
- `rank_cross_section`
- `zscore_cross_section`
- `linear_combine`
- `demo_alpha_signal`

相关测试：

- `tests/signals/test_engine.py`
- `tests/signals/test_transforms.py`

应该继续问：

> 请逐步打印 `demo_alpha_signal` 在一个 2-date × 2-asset toy example 上的 rank、zscore、combined score，并判断测试预期是否合理。

### 11.4 我想查 signal 如何进入 portfolio weights

源码路径：

- `src/qsys/backtest/portfolio.py`
- `src/qsys/backtest/simulator.py`
- `src/qsys/rebalance/policies.py`

关键函数：

- `build_top_n_portfolio`
- `run_backtest_from_signal`
- `build_buffered_top_n_weights`

相关测试：

- `tests/backtest/test_portfolio.py`
- `tests/backtest/test_portfolio_constraints.py`
- `tests/rebalance/test_buffered_top_n_policy.py`

应该继续问：

> 请检查 `build_top_n_portfolio` 的约束顺序、MultiIndex 对齐、归一化逻辑，并判断 liquidity filter bug 的最小修复方案。

### 11.5 我想查 backtest execution assumption

源码路径：

- `src/qsys/backtest/execution.py`
- `src/qsys/backtest/simulator.py`
- `src/qsys/rebalance/backtest.py`

关键函数：

- `align_next_day_returns`
- `align_weights_and_returns`
- `run_backtest_from_weights`
- `run_buffered_topn_backtest`

相关测试：

- `tests/backtest/test_simulator_metrics.py`
- `tests/rebalance/test_buffered_top_n_backtest.py`

应该继续问：

> 请比较 `backtest/simulator.py` 与 `rebalance/backtest.py` 的收益对齐和 rebalance date 语义是否一致，并输出需要统一的项目协议。

### 11.6 我想查 volatility penalty 的实现和含义

源码路径：

- `src/qsys/signals/engine.py`
- `src/qsys/risk/exposure.py`
- `src/qsys/research/signal_quality/conditioned_ic.py`
- `src/qsys/utils/run_phase14b_risk_diagnostics.py`
- `src/qsys/utils/run_signal_sanity_grid.py`

关键函数：

- `demo_alpha_signal`
- `build_risk_exposure_matrix`
- `assign_condition_buckets`
- `compute_conditioned_rank_ic`

相关测试：

- `tests/signals/test_engine.py`
- `tests/risk/test_exposure.py`
- `tests/research/signal_quality/test_conditioned_ic.py`

应该继续问：

> 请设计一个 volatility role diagnostic：比较 volatility 作为 penalty、condition、risk exposure 时对 Rank IC、quantile spread、turnover 和 portfolio exposure 的影响。

### 11.7 我想查 constraints 和 full risk model 的区别

推荐阅读：

- `docs/notion_project_review/07_constraints_and_risk_control.md`
- 本手册第 6 章

源码路径：

- `src/qsys/backtest/portfolio.py`
- `src/qsys/universe/eligibility.py`
- `src/qsys/risk/exposure.py`
- `src/qsys/research/constraint_impact.py`
- `src/qsys/research/portfolio_exposure.py`

应该继续问：

> 请基于当前 constraints 代码，设计 risk model v1 的最小接口，但不要直接引入 optimizer；先定义 exposure、risk state、portfolio diagnostics。

### 11.8 我想查某个测试失败的影响

查询路径：

1. 先看失败测试文件，例如 `tests/signals/test_engine.py`。
2. 再看对应源码，例如 `src/qsys/signals/engine.py`。
3. 判断失败属于：测试预期问题、代码行为问题、环境依赖问题、还是收集配置问题。
4. 再判断影响哪个系统假设。

本次已发现：

- `test_demo_alpha_combination_behavior`：行为预期冲突，影响 signal baseline 和 volatility penalty 解释。
- `test_portfolio_constraints_contract`：代码行为问题，影响 constraints 和 constraint impact。
- `test_load_daily_panel_contract`：当前环境缺 parquet engine，需本地标准环境复现。
- `import file mismatch`：测试收集配置问题，可考虑 importlib mode 或重命名同名测试文件。

### 11.9 我想查 report / benchmark workflow

源码路径：

- `src/qsys/rebalance/benchmarks.py`
- `src/qsys/rebalance/index_benchmarks.py`
- `src/qsys/utils/compare_rebalance_policies_from_feature_store.py`
- `src/qsys/utils/report_rebalance_policy_comparison.py`
- `src/qsys/utils/build_market_index_benchmarks.py`

相关测试：

- `tests/rebalance/test_rebalance_benchmarks.py`
- `tests/rebalance/test_index_benchmarks.py`
- `tests/rebalance/test_report_rebalance_policy_comparison.py`
- `tests/rebalance/test_compare_rebalance_policies_from_feature_store_script.py`

应该继续问：

> 请检查当前 report 输出文件是否足够支持长期实验复盘，并设计一个 `run_manifest.json` schema。

---

## 12. 下一阶段开发优先级

### P0：必须先处理，否则影响后续判断

P0 不是继续加功能，而是清理会扭曲后续研究结论的问题。

1. 修复或确认 `build_top_n_portfolio` 的 liquidity filter index alignment bug。
2. 处理 `demo_alpha_signal` 测试预期冲突，明确 volatility penalty 的当前定义。
3. 统一 rebalance date convention：周初、周末、还是 signal date / execution date 分离。
4. 建立标准本地测试命令，安装 `requirements.txt` 后复现完整测试；处理同名测试模块的 collection issue。

为什么是 P0：这些问题会直接影响 signal 解释、constraints 可靠性、回测收益对齐和测试可信度。不处理这些，后面跑更复杂模型会没有地基。

### P1：研究协议和实验标准化

P1 是把当前研究系统从“能跑”变成“可长期复盘”。

需要定义：

- feature role：feature / label / exposure / eligibility；
- signal recipe schema；
- label horizon；
- rebalance convention；
- execution assumption；
- cost assumption；
- benchmark source；
- sample universe；
- date range；
- run config。

为什么是 P1：没有协议，结果不可比较；不可比较，项目就无法积累。

### P2：report / diagnostics schema

P2 是建立统一 diagnostics/report 输出。

建议最小 schema 包括：

- `run_manifest.json`
- `signal_quality_summary.csv`
- `ic_by_date.csv`
- `ic_summary_by_horizon.csv`
- `quantile_return.csv`
- `quantile_spread_summary.csv`
- `turnover_summary.csv`
- `exposure_summary.csv`
- `conditioned_ic_summary.csv`
- `portfolio_metrics.csv`
- `benchmark_comparison.csv`
- `warnings.md`

为什么是 P2：当前 diagnostics 已经不少，但分散。统一 schema 后，项目 owner 才能长期横向比较不同 signal、不同参数和不同阶段。

### P3：risk-control module

P3 才是从 constraints 走向 risk-control module。

建议不要一开始就做 optimizer，而是先做：

- exposure matrix 标准化；
- portfolio exposure diagnostics；
- volatility/liquidity/size bucket conditioned IC；
- constraints impact report；
- risk gate prototype。

为什么是 P3：当前 constraints 还存在 bug，risk model 需要建立在可靠 weights 和 diagnostics 上。

### P4：data governance

P4 是补数据治理。

包括：

- raw/standardized/feature 分层；
- schema version；
- data quality report；
- source metadata；
- failed symbol log；
- parquet partition audit；
- incremental build policy。

为什么是 P4：数据治理重要，但在当前阶段，先修复研究链路关键行为更急。治理可以和实验协议并行推进，但不应阻塞 P0/P1。

### P5：更复杂模型或 nonlinear extension

P5 才是复杂模型扩展。

可能方向：

- volatility nonlinear penalty；
- volatility-conditioned signal；
- Ridge/Lasso base predictive model；
- tree-based ranking model；
- regime-aware model；
- ensemble signal。

为什么放在 P5：复杂模型之前必须先稳定 data/label/signal/backtest/diagnostics 协议。否则模型复杂度只会让错误更难发现。

---

## 13. 最终结论

当前项目的真实状态是：它已经不是一个零散 notebook，也不只是一个 feature 工具包，而是一个已经形成主链路的 A 股日频/低频量化研究平台。它具备清晰的 Data → Panel → Feature → Signal → Portfolio → Backtest → Diagnostics → Constraints → Report 架构，并且已经有相当多的模块化代码和测试支撑。

项目最重要的优势有三个。

第一，它的结构是可解释的。`MultiIndex [date, asset]` 作为核心协议，把横截面信号、forward return label、组合权重、回测收益和风险暴露连接在一起。

第二，它是可复盘的。当前已经有 review docs、evidence pack、project_map、Notion sync、report script，说明项目不只是写代码，也在主动管理长期知识。

第三，它是可继续演进的。Feature registry、SignalEngine、BacktestConfig、BufferedTopNPolicyConfig、risk exposure matrix 都为后续扩展留下了接口。

但当前最危险的技术债也很明确。

第一，constraints 路径存在真实 index alignment bug，会影响 constrained portfolio 和 constraint impact analysis。

第二，demo alpha 的 volatility penalty 与测试预期冲突，说明 base signal 的业务定义还没有完全稳定。

第三，rebalance semantics 在不同模块中不一致，可能导致 strict/backtest/rebalance/benchmark 的结果解释口径不统一。

第四，文档中 “V1 complete”、“sqlite metadata”、“run_demo.py”、“notebook”、“Future Work” 等描述需要更新或加限定，否则项目 owner 后续查询会被旧文档误导。

下一阶段最合理方向不是直接上复杂 ML，也不是继续堆功能，而是先完成四件事：修 P0 reliability、固定 base model protocol、统一 diagnostics/report schema、明确 volatility 与 risk-control 的边界。完成这些后，项目才能更稳地走向 risk model v1、nonlinear signal、ML model 或更系统化的实验平台。

一句话总结：

> 这个项目当前已经具备可解释、可复盘、可继续演进、可长期维护的研究平台骨架；但它距离完整生产系统还有明显距离。下一阶段的关键不是“让模型更复杂”，而是“让每一个研究结论更可信”。

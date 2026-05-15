# A-Share Quant Data Platform

A research-oriented A-share systematic trading framework built around a modular pipeline:

Data → Panel → Feature → Signal → Backtest → Diagnostics → Constraints

This project focuses on daily-frequency / low-frequency research workflows, not production live trading.

---

## Overview

This repository started from an A-share data engineering pipeline and was extended into a research-oriented trading system.

### Data Layer
- AkShare-based ingestion
- parquet storage
- sqlite metadata
- legacy notebook pipeline

### Research Layer
- panel abstraction
- feature store
- signal engine
- diagnostics
- exposure analysis

### Portfolio / Strategy Layer
- portfolio construction
- execution simulation
- transaction cost modeling
- portfolio constraints
- constraint impact analysis

---

## Project Status

Current status: V1 complete

### Included in V1
- standardized research panel access
- feature store v1
- signal engine v1
- backtest MVP
- diagnostics v1
- exposure analysis v1
- portfolio constraints v1
- constraint impact analysis v1

### Not included in V1
- live trading / OMS / EMS
- high-frequency / order book research
- large-scale ML alpha platform
- full benchmark attribution / optimizer stack

---

## Repository Structure

    .
    ├─ A_share_Analytical_DWH.ipynb
    ├─ run_demo.py (deprecated for real-data workflow)
    ├─ requirements.txt
    ├─ src/
    │  └─ qsys/
    │     ├─ data/
    │     ├─ features/
    │     ├─ signals/
    │     ├─ backtest/
    │     ├─ research/
    │     └─ utils/
    └─ tests/

---

## Key Modules

### src/qsys/data/
- panel access layer

### src/qsys/features/
- feature abstractions
- feature registry
- feature materialization / store

### src/qsys/signals/
- cross-sectional transforms
- signal combination
- demo alpha construction

### src/qsys/backtest/
- portfolio construction
- execution alignment
- transaction cost
- simulator
- summary metrics

### src/qsys/research/
- IC / Rank IC
- quantile analysis
- persistence / turnover
- decay analysis
- signal correlation
- exposure analysis
- constraint impact analysis

### src/qsys/utils/
- CLI / example entrypoints
- synthetic demo data generator
- real-data feature store builder for Colab/local research

---

## Quickstart

### 1. Install dependencies

    pip install -r requirements.txt

### 2. Build real feature store (recommended)

    PYTHONPATH=src python -m qsys.utils.build_real_feature_store \
      --feature-root data/processed/feature_store/v1 \
      --start-date 2020-01-01 \
      --limit 300

This creates:

    data/processed/feature_store/v1/

This path is for real/processed feature-store data.

---

### 3. Run research/backtest with explicit feature root

    PYTHONPATH=src python -m qsys.utils.signal_engine_example \
      --feature-root data/processed/feature_store/v1

    PYTHONPATH=src python -m qsys.utils.research_diagnostics_example \
      --feature-root data/processed/feature_store/v1

    PYTHONPATH=src python -m qsys.utils.backtest_example \
      --feature-root data/processed/feature_store/v1

    PYTHONPATH=src python -m qsys.utils.constraint_impact_example \
      --feature-root data/processed/feature_store/v1

---

## Colab + real data

Use these notebooks in order:

1. `01_build_real_feature_store.ipynb`
2. `02_signal_research.ipynb`
3. `03_backtest.ipynb`

Real-data feature builder output schema includes:

- `trade_date`, `ts_code`
- `open`, `high`, `low`, `close`
- `volume`, `amount`, `turnover`, `outstanding_share`
- `ret_1d`, `ret_5d`, `ret_20d`
- `vol_20d`, `amount_20d`, `turnover_5d`, `turnover_20d`
- `market_cap`
- `fwd_ret_5d`, `fwd_ret_20d`
- `is_tradable`

All research/backtest example entrypoints now require `--feature-root` explicitly to avoid fragile implicit paths in Colab.

---

## Example Workflow

1. Load feature-store data  
2. Construct signal  
   Baseline candidate: rank(ret_20d)  
   Experimental variant: rank(ret_20d) - 0.5 * zscore(vol_20d)  
3. Run diagnostics  
   - IC / Rank IC  
   - quantile spread  
   - exposure analysis  
4. Construct portfolio  
5. Run backtest  
6. Evaluate constraint impact  

---

## Synthetic Demo Note (optional)

This repository does not include full market datasets.

If you only need a smoke test:

    PYTHONPATH=src python -m qsys.utils.generate_synthetic_feature_store
    python run_demo.py --mode all

Default synthetic output path:

    data/sample/feature_store/v1/

This is for:
- smoke testing
- architecture validation
- demo usage

It does NOT represent real trading performance.

## Universe sample builder (configurable)

Build an index-derived universe sample with configurable sample size (`--n`), output directory, and name:

Quick smoke test (`n=50`):

    PYTHONPATH=src python -m qsys.utils.build_universe_sample \
      --index-list 000300 000905 000852 \
      --n 50 \
      --seed 42 \
      --output-dir data/universe \
      --name csi_smoke

Small research sample (`n=100`):

    PYTHONPATH=src python -m qsys.utils.build_universe_sample \
      --index-list 000300 000905 000852 \
      --n 100 \
      --seed 42 \
      --output-dir data/universe \
      --name csi_small

Broader research sample (`n=300` or `n=500`):

    PYTHONPATH=src python -m qsys.utils.build_universe_sample \
      --index-list 000300 000905 000852 \
      --n 300 \
      --seed 42 \
      --output-dir data/universe \
      --name csi_large_mid

    PYTHONPATH=src python -m qsys.utils.build_universe_sample \
      --index-list 000300 000905 000852 \
      --n 500 \
      --seed 42 \
      --output-dir data/universe \
      --name csi_large

## CSI500 point-in-time 成分快照（BaoStock）

    PYTHONPATH=src python -m qsys.utils.build_baostock_index_members \
      --start-date 2018-01-01 \
      --end-date 2025-12-31 \
      --output-root data/raw/index_constituents/baostock \
      --freq ME

该数据层用于 point-in-time CSI500 universe；回测时应使用
`load_index_members_asof(as_of_date=...)` 获取不晚于回测日期的最近一期快照，
避免未来函数。

---

## Legacy Notebook

A_share_Analytical_DWH.ipynb is kept as the original pipeline.

Current system:

    src/qsys/

---

## Testing

    PYTHONPATH=src pytest -q

---

## Future Work

- integrate real standardized panel outputs  
- improve execution assumptions  
- benchmark comparison  
- richer exposure controls  
- report / tearsheet generation  
- broader strategy research support  

---

## Summary

This project is a research-oriented A-share systematic trading framework focused on:

- modular architecture  
- reproducible research  
- signal diagnostics  
- portfolio behavior analysis  

It aims to answer:

- Does a signal have predictive power?  
- How stable is the alpha?  
- What exposures drive the signal?  
- How do constraints affect performance?  

## Buffered Rebalance Policy

Strict Top-N rebalancing can create excessive turnover when small cross-sectional rank moves cause large portfolio changes. The buffered policy separates signal ranking from trade execution and is designed for daily/low-frequency multi-factor research.

### Core idea

- **Signal** decides which assets are attractive.
- **Rebalance policy** decides whether the portfolio should actually trade.
- **Portfolio weights** decide how much to hold.
- **Cost model** evaluates the trading drag from turnover.

### Buffered Top-N rules

Default behavior in this repository:

- buy new asset if `rank <= buy_rank`
- sell existing holding only if `rank > sell_rank`
- keep assets in the buffer zone `(buy_rank, sell_rank]`
- do not force buys when holdings remain above `min_holding_n`
- trim holdings if count exceeds `max_holding_n`
- skip tiny weight changes below `min_trade_weight`
- charge transaction cost with `turnover * cost_bps / 10000`

### Module layout

```text
src/qsys/rebalance/
- policies.py
- costs.py
- backtest.py
- diagnostics.py

src/qsys/utils/
- buffered_rebalance_example.py
- run_buffered_rebalance_from_feature_store.py
- compare_rebalance_policies_from_feature_store.py
```

### Synthetic demo

```bash
PYTHONPATH=src python src/qsys/utils/buffered_rebalance_example.py
```

### Run from feature store

```bash
PYTHONPATH=src python src/qsys/utils/run_buffered_rebalance_from_feature_store.py \
  --feature-root data/processed/feature_store/v1 \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --target-n 50 \
  --buy-rank 50 \
  --sell-rank 100 \
  --rebalance weekly \
  --cost-bps 20
```

### Compare strict vs buffered policies

```bash
PYTHONPATH=src python src/qsys/utils/compare_rebalance_policies_from_feature_store.py \
  --feature-root data/processed/feature_store/v1 \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --target-n 50 \
  --rebalance weekly \
  --cost-bps 20 \
  --output-dir outputs/rebalance_experiments \
  --run-name strict_vs_buffered_2024
```

Output files:

- `comparison.csv`
- `strict_daily_returns.csv`
- `buffered_daily_returns.csv`
- `strict_turnover.csv`
- `buffered_turnover.csv`
- `strict_trades.csv`
- `buffered_trades.csv`
- `strict_weights.csv`
- `buffered_weights.csv`

### Diagnostics

- `summarize_trades`: daily action counts, turnover, and rank summaries.
- `holding_period_summary`: holding-segment duration statistics.
- `analyze_trade_forward_returns`: ex-post buy/sell forward return stats by horizon.
- `rank_migration_matrix`: transition counts of held-asset rank buckets over time.

### Interpretation

Buffered policy is not automatically better than strict Top-N. Evaluate with after-cost return, turnover reduction, total cost reduction, holding-period stability, buy/sell forward-return diagnostics, and robustness across parameter settings.


## Phase 18A-18 Colab controlled recovery (P0/P1 selected sources)

Use the raw coverage runner with explicit selected APIs for controlled recovery:

    PYTHONPATH=src python -m qsys.utils.run_factor_lake_raw_coverage_ingest \
      --families market_price,margin_leverage,financial_fundamental,event_ownership \
      --include-disabled \
      --api-names stock_zh_a_hist,stock_individual_info_em,stock_margin_detail_szse,stock_financial_analysis_indicator,stock_gpzy_pledge_ratio_detail_em \
      --max-workers 2

Default policy keeps disabled sources skipped unless `--include-disabled` is set.

Notes for Phase 18A-18 recovery:
- `raw_ingest_catalog.csv` records the **actual run result** (e.g., success/empty/failed/skipped).
- `raw_source_acquisition_checklist.csv` records the **default acquisition policy view** (`获取` / `暂停获取` / `排除`) and keeps base schema:
  `api_name,source_family,acquisition_status`.
- For disabled heavy/detail sources (for example `stock_gpzy_pledge_ratio_detail_em`, `stock_jgdy_detail_em`), checklist may remain `暂停获取` even if an include-disabled probe was run.

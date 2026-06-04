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
- source-specific resilient page checkpoints for selected heavy raw-detail sources, including `stock_jgdy_detail_em` under `_operation_review/stock_jgdy_detail_em_pages/since_date=YYYYMMDD/`

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


## Phase 18A-19 official raw lake acquisition runner

Default mode is **formal full-market/full-period Raw Data Lake acquisition**.
When symbol/date/industry/concept filters are omitted, the runner expands via acquisition-universe defaults.

    PYTHONPATH=src python -m qsys.utils.run_factor_lake_raw_ingest \
      --output-root outputs/factor_lake_raw \
      --start-date 20100101 \
      --end-date 20101231 \
      --families market_price,index_market,margin_leverage,industry_concept,financial_fundamental,event_ownership,corporate_action,trading_attention,disclosure_ir

Targeted filters are supported for debug/recovery (not default semantics):

    PYTHONPATH=src python -m qsys.utils.run_factor_lake_raw_ingest \
      --symbols 000001,000002 \
      --trade-dates 20100104,20100105 \
      --api-names stock_zh_a_hist

Default policy keeps disabled sources skipped unless `--include-disabled` is set.

Admission-control tuning is configured explicitly at the API level when needed:

    PYTHONPATH=src python -m qsys.utils.run_factor_lake_raw_ingest \
      --api-inflight-limits stock_margin_detail_szse=2

The same compact form is accepted by the preheat CLI. In the Colab console, set `API_INFLIGHT_LIMITS` to a Python dict such as `{"stock_margin_detail_szse": 2}`; use `{}` for no API-specific cap. Active API limits are written to `_operation_review/admission_control_manifest.json` for audit and are intentionally excluded from hybrid checkpoint fingerprints so existing staging remains resumable.

Notes for Phase 18A-18 recovery:
- `raw_ingest_catalog.csv` records the **actual run result** (e.g., success/empty/failed/skipped).
- `raw_source_acquisition_checklist.csv` records the **default acquisition policy view** (`获取` / `暂停获取` / `排除`) and keeps base schema:
  `api_name,source_family,acquisition_status`.
- For disabled heavy/detail sources (for example `stock_gpzy_pledge_ratio_detail_em`, `stock_jgdy_detail_em`), checklist may remain `暂停获取` even if an include-disabled probe was run.
- Default queue restored for Colab-verified sources: `stock_margin_detail_szse`, `stock_industry_clf_hist_sw`, and canonical `trading_attention/stock_jgdy_tj_em` (while duplicate `disclosure_ir/stock_jgdy_tj_em` remains paused to avoid duplicate default ingestion).

## Controlled Raw Lake compact and Drive promotion workflow

This workflow consolidates the validated post-acquisition Raw Lake flow into reusable modules and human-gated CLI commands. It is limited to local Raw Factor/Data Lake acquisition outputs and promotion of compact Raw parquet assets; it does not create normalized panels, feature stores, signals, backtests, or models.

Preheat report-date modes:

```bash
# Existing explicit mode remains supported.
PYTHONPATH=src python -m qsys.utils.run_raw_lake_preheat \
  --output-root outputs/raw_acquisition_local/wave_20220101_20241231 \
  --start-date 20220101 \
  --end-date 20241231 \
  --report-dates 20220331,20220630,20220930,20221231

# Optional automatic mode derives 0331/0630/0930/1231 dates inside the window.
PYTHONPATH=src python -m qsys.utils.run_raw_lake_preheat \
  --output-root outputs/raw_acquisition_local/wave_20220101_20241231 \
  --start-date 20220101 \
  --end-date 20241231 \
  --auto-quarter-end-report-dates
```

`--report-dates` and `--auto-quarter-end-report-dates` are mutually exclusive, and omitting both preserves the previous no-default behavior.

Prepare is local-only for compact parquet assets. It scans already-landed local ingest Raw parquet files under `<output-root>/data/raw/akshare`, writes compact package assets under `<package-root>/raw/akshare`, classifies compact buckets from physical Raw lineage, writes local QA artifacts, and reads Drive only to produce `drive_collision_plan.csv` and `READY_FOR_PROMOTION.json`. Empty staging is rejected before manifest/readiness files are written. Acquisition windows are inferred from an output-root name containing `YYYYMMDD_YYYYMMDD`, or supplied explicitly with `--start-date` and `--end-date`; unknown or inverted windows are rejected. The prepared Drive DWH root, Raw root, catalog root, collision-plan path, collision-plan SHA-256, and action counts are recorded in `READY_FOR_PROMOTION.json` for operator review. It never writes Drive Raw parquet, never deletes Drive files, and fails if the Drive root is unavailable.

```bash
PYTHONPATH=src python -m qsys.utils.raw_lake_compact_cli prepare \
  --output-root outputs/raw_acquisition_local/wave_20220101_20241231 \
  --drive-dwh-root /content/gdrive/MyDrive/a_share_quant_data
```

Promote is the only command that writes Drive Raw assets. Canonical Drive Raw storage is `<drive-dwh-root>/raw/akshare/...`; the workflow must never create `<drive-dwh-root>/data/raw/akshare/...`. Promotion requires exact human confirmation, refuses a different Drive DWH root than the one reviewed during `prepare`, validates the reviewed local collision-plan SHA-256, validates the local package before the first Drive write, immediately rebuilds the collision plan, refuses changed target path sets and every non-identical Raw or catalog artifact overwrite, copies only new files, skips byte-identical files, reopens all promoted parquet files from Drive, and verifies rows, columns, and SHA-256. Buckets classified as `scope` or `snapshot` require explicit operator opt-in via `--allow-reviewed-bucket-kinds`.

```bash
PYTHONPATH=src python -m qsys.utils.raw_lake_compact_cli promote \
  --package-root outputs/raw_acquisition_compact/<PROMOTION_NAME> \
  --drive-dwh-root /content/gdrive/MyDrive/a_share_quant_data \
  --confirm-promotion <PROMOTION_NAME>
```

Audit is independent and read-only. It loads Drive promotion artifacts from `catalog/promotions/<PROMOTION_NAME>`, reopens every promoted Drive parquet, verifies rows, columns, and SHA-256, and prints a bucket summary without writes or deletions.

```bash
PYTHONPATH=src python -m qsys.utils.raw_lake_compact_cli audit \
  --promotion-name <PROMOTION_NAME> \
  --drive-dwh-root /content/gdrive/MyDrive/a_share_quant_data
```

Raw compact is inventory-driven and lineage-driven: it uses generic partition keys such as `snapshot`, `year`, `trade_date`, `report_date`, `date`, `start_date`/`end_date`, and `since_date`. It intentionally avoids API-name-specific compact rules, parquet-body business-date repartitioning, normalization, silent deduplication, row deletion, and column deletion. Failed-task backlog counts are task-level rows from `raw_ingest_catalog.csv`, not API-level recovery summaries.

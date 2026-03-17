# A-share Quant Data Platform

Personal research project for building an A-share quantitative data platform.

## Part 1
- AkShare data ingestion
- incremental pipeline
- parquet storage
- standardized dataset
- feature layer

## New minimal panel layer
- Thin research panel reader at `src/qsys/data/panel/daily_panel.py`
- Reads existing standardized parquet layout as-is: `data/standardized/market/daily_bars/trade_date=YYYY-MM-DD/data.parquet`
- Supports filters: `start_date`, `end_date`, `symbols`, `columns`
- Returns normalized pandas DataFrame indexed by `[date, asset]`

Panel example:

```bash
PYTHONPATH=src python -m qsys.utils.panel_example --start-date 2024-01-01 --end-date 2024-01-31 --symbols 000001.SZ
```

## Feature Store v1 (minimal)
- Feature interfaces in `src/qsys/features/`
- Compute features on top of panel API only (no ingestion/notebook changes)
- Materialized outputs are written to `data/processed/feature_store/v1/trade_date=YYYY-MM-DD/data.parquet`
- Minimal metadata sidecar stored in sqlite: `data/processed/feature_store/metadata.db`

Feature Store example:

```bash
PYTHONPATH=src python -m qsys.utils.feature_store_example \
  --features ret_1d ret_5d vol_20d turnover_20d amount_20d fwd_ret_5d \
  --start-date 2024-01-01 --end-date 2024-03-31 --symbols 000001.SZ
```

## Signal Engine v1 (minimal)
- Cross-sectional transforms in `src/qsys/signals/transforms.py`
  - `winsorize_cross_section`
  - `zscore_cross_section`
  - `rank_cross_section`
  - optional: `neutralize_by_size`, `neutralize_by_group`
- Combination helper in `src/qsys/signals/combine.py`
  - `linear_combine(signals, weights)`
- Thin engine in `src/qsys/signals/engine.py`
  - transform feature columns
  - combine transformed signals
  - demo alpha: `rank(ret_20d) - 0.5 * zscore(vol_20d)`

Signal Engine example:

```bash
PYTHONPATH=src python -m qsys.utils.signal_engine_example \
  --feature-root data/processed/feature_store/v1 \
  --start-date 2024-01-01 --end-date 2024-03-31 --symbols 000001.SZ
```

## Backtest Engine MVP
- Portfolio layer (`src/qsys/backtest/portfolio.py`)
  - top-N equal-weight long-only, optional long-short
- Execution layer (`src/qsys/backtest/execution.py`)
  - signal at date `t` mapped to realized return at `t+1` (next-day close fallback)
- Cost layer (`src/qsys/backtest/cost.py`)
  - fixed transaction cost bps + slippage bps based on turnover
- Simulator (`src/qsys/backtest/simulator.py`)
  - daily/weekly/monthly rebalance
  - returns, turnover, costs, holdings weights output
- Metrics (`src/qsys/backtest/metrics.py`)
  - cumulative return / annual return / annual vol / sharpe / max drawdown / turnover
- Metrics are computed on **net** strategy returns after costs in simulator output
- Turnover definition (gross): `sum(abs(w_t - w_{t-1}))` across assets (no extra scaling)
- If long-short is used, construction is explicit: long leg sums to `+1`, short leg sums to `-1` when both legs are fully populated

Backtest demo:

```bash
PYTHONPATH=src python -m qsys.utils.backtest_example \
  --feature-root data/processed/feature_store/v1 \
  --start-date 2024-01-01 --end-date 2024-06-30 \
  --top-n 20 --rebalance weekly
```

- Portfolio Constraints v1 (in `build_top_n_portfolio`)
  - `max_single_weight` cap
  - liquidity filter (same-date metric only, no look-ahead)
  - optional size-aware scaling by same-date `market_cap`
  - optional long-only `group_cap` when group labels are available
  - weights are normalized after constraints (`sum(long)=1` in long-only; in long-short, `sum(long)=+1`, `sum(short)=-1`)

Portfolio constraints demo:

```bash
PYTHONPATH=src python -m qsys.utils.portfolio_constraints_example \
  --feature-root data/processed/feature_store/v1 \
  --start-date 2024-01-01 --end-date 2024-06-30 --top-n 20 \
  --max-single-weight 0.10 --min-liquidity 1000000 --size-aware-scaling
```

## Research Diagnostics v1
- Diagnostics package in `src/qsys/research/`:
  - `ic.py`: daily IC / daily Rank IC (cross-sectional by date)
  - `quantiles.py`: cross-sectional quantile mean forward returns and spread
  - `turnover.py`: signal autocorrelation and top-N membership turnover
  - `decay.py`: IC decay summary across multiple forward-return horizons
  - `correlation.py`: pairwise signal correlation on strictly intersected observations
- Exposure analysis (`src/qsys/research/exposure.py`):
  - size exposure (signal vs log market cap)
  - optional group exposure (if group/industry column exists)
  - signal-feature correlation (e.g. vs ret_20d, vol_20d)
- Alignment policy:
  - IC/RankIC and quantile functions require identical `[date, asset]` index between signal and labels
  - no implicit reindexing is performed

Diagnostics demo:

```bash
PYTHONPATH=src python -m qsys.utils.research_diagnostics_example \
  --feature-root data/processed/feature_store/v1 \
  --start-date 2024-01-01 --end-date 2024-06-30 --symbols 000001.SZ
```

Exposure analysis demo:

```bash
PYTHONPATH=src python -m qsys.utils.exposure_analysis_example \
  --feature-root data/processed/feature_store/v1 \
  --start-date 2024-01-01 --end-date 2024-06-30 --symbols 000001.SZ
```


## Constraint Impact Analysis v1
- Compare unconstrained vs constrained portfolios using the same signal input
- Output includes:
  - summary DataFrame (`return_diff`, `sharpe_diff`, `turnover_diff`, `ic_diff`, exposure diffs)
  - per-date comparison DataFrame (return/turnover/IC differences)
- Implemented in `src/qsys/research/constraint_impact.py`

Constraint impact demo:

```bash
PYTHONPATH=src python -m qsys.utils.constraint_impact_example \
  --feature-root data/processed/feature_store/v1 \
  --start-date 2024-01-01 --end-date 2024-06-30 \
  --top-n 20 --max-single-weight 0.10 --min-liquidity 1000000 --size-aware-scaling
```

## Future work
- factor layer
- backtest engine extensions
- news / text intelligence layer

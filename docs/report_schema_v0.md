# Report Schema v0

## Purpose

Report Schema v0 is a lightweight run-artifact contract for research experiments.
It exists to:
- make experiments reproducible,
- make runs comparable,
- record assumptions explicitly,
- prevent hidden changes in signal, portfolio, cost, benchmark, or sample.

This schema is intentionally simple and revisable.

---

## Standard artifact layout for one experiment run

Each run should save artifacts under one run directory, for example:

```text
runs/<run_id>/
  run_manifest.json
  signal_quality_report.csv
  portfolio_summary.csv
  benchmark_comparison.csv
  exposure_summary.csv
  warnings.md
```

Required files:
- `run_manifest.json`
- `signal_quality_report.csv`
- `portfolio_summary.csv`
- `benchmark_comparison.csv`
- `exposure_summary.csv`
- `warnings.md`

---

## `run_manifest.json` fields

Minimum fields to record per run:

- `run_id`: unique run identifier string.
- `created_at`: run creation timestamp (ISO-8601).
- `code_commit`: git commit hash used for the run.
- `feature_root`: data root path used for feature loading.
- `data_range`: explicit sample date range (start/end).
- `universe`: universe definition used by the run.
- `feature_roles`: mapping of fields to roles (feature/label/filter/etc.).
- `label_horizon`: label horizon setting (for example `fwd_ret_5d`, `fwd_ret_20d`).
- `signal_recipe`: signal construction definition (inputs/transforms/weights).
- `portfolio_rule`: portfolio construction rule definition.
- `rebalance_rule`: rebalance frequency and date convention.
- `execution_assumption`: return alignment / execution assumption used.
- `cost_model`: cost formula and parameters.
- `benchmark`: benchmark definition used for comparison.
- `diagnostics_requested`: diagnostics requested for this run.
- `known_limitations`: known caveats for this run configuration.
- `warnings`: warning tags/messages generated during run.

Example skeleton:

```json
{
  "run_id": "2026-05-08_weekly_baseline_v0",
  "created_at": "2026-05-08T12:00:00Z",
  "code_commit": "<git_sha>",
  "feature_root": "data/processed/feature_store/v1",
  "data_range": {"start": "2020-01-01", "end": "2025-12-31"},
  "universe": "eligible_A_share_is_tradable",
  "feature_roles": {
    "ret_20d": "feature",
    "vol_20d": "diagnostic_or_condition",
    "fwd_ret_5d": "label",
    "fwd_ret_20d": "label"
  },
  "label_horizon": ["fwd_ret_5d", "fwd_ret_20d"],
  "signal_recipe": "rank(ret_20d)",
  "portfolio_rule": "top_n_long_only",
  "rebalance_rule": "weekly_period_end_available_date",
  "execution_assumption": "next_close_realized_returns",
  "cost_model": "turnover_times_bps",
  "benchmark": ["equal_weight", "CSI300"],
  "diagnostics_requested": ["rank_ic", "quantile_spread", "exposure_summary"],
  "known_limitations": ["research_level_execution_model"],
  "warnings": []
}
```

---

## `signal_quality_report.csv` expected columns

- `signal_name`
- `horizon`
- `mean_rank_ic`
- `median_rank_ic`
- `ic_std`
- `icir`
- `t_stat`
- `positive_rate`
- `n_dates`
- `quantile_spread`
- `top_minus_bottom`
- `notes`

Notes:
- `horizon` should match labels declared in `run_manifest.json`.
- `notes` should capture caveats such as unstable periods or missing bins.

---

## `portfolio_summary.csv` expected columns

- `strategy_name`
- `total_return`
- `annualized_return`
- `annualized_vol`
- `sharpe`
- `max_drawdown`
- `average_turnover`
- `total_cost`
- `n_rebalance_dates`
- `notes`

Notes:
- `strategy_name` should be consistent with manifest `signal_recipe` + `portfolio_rule` naming.
- `n_rebalance_dates` should reflect the applied rebalance rule (not just number of data dates).

---

## `benchmark_comparison.csv` expected columns

- `strategy_name`
- `benchmark_name`
- `total_return`
- `excess_return`
- `tracking_difference`
- `correlation`
- `notes`

Notes:
- `benchmark_name` must match entries declared in manifest `benchmark`.
- `tracking_difference` should document exact definition used in notes if not standardized yet.

---

## `exposure_summary.csv` expected columns

- `strategy_name`
- `exposure_name`
- `mean_exposure`
- `max_abs_exposure`
- `exposure_volatility`
- `notes`

Notes:
- `exposure_name` examples: `size_z`, `liquidity_z`, `vol_20d_z`.
- Keep sign conventions explicit in notes when needed.

---

## `warnings.md` purpose

`warnings.md` is a human-readable run warning log.
It should record:
- missing data warnings,
- small sample warnings,
- unstable IC warnings,
- high turnover warnings,
- benchmark mismatch warnings,
- unsupported execution assumption warnings.

Warnings should be concise and actionable.

---

## Optional mapping from current scripts (no refactor in this phase)

Current utilities already produce partial outputs that can map into this schema:

- `src/qsys/utils/compare_rebalance_policies_from_feature_store.py`
  - emits `comparison.csv` and per-policy daily/turnover/trade/weight CSVs.
- `src/qsys/utils/report_rebalance_policy_comparison.py`
  - builds summary metrics and benchmark-relative tables from saved CSV outputs.

These outputs partially cover `portfolio_summary.csv` and `benchmark_comparison.csv`, but they do not yet enforce a full `run_manifest.json` + unified run directory contract.

---

## What this schema does not solve yet

- It does not validate alpha.
- It does not implement a full experiment registry.
- It does not implement production trading.
- It does not implement full risk attribution.
- It does not guarantee economic significance.

This schema is a consistency layer, not a proof of strategy quality.

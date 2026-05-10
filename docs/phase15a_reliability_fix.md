# Phase 15A-1 Reliability Fix Note

Date: 2026-05-08

## Scope
P0 reliability fixes only (no feature additions, no file deletion/archive).

## 1) `src/qsys/data/` existence and README/docs consistency

- In this tracked snapshot, `README.md` references `src/qsys/data/` in structure and module descriptions.
- Current tracked files under `src/qsys/` do not include a `data/` subtree.
- This is an inconsistency between documentation and currently tracked code and should be resolved in a follow-up docs pass.

## 2) Liquidity filter MultiIndex alignment bug in `build_top_n_portfolio`

Issue:
- The prior liquidity filter sliced same-date liquidity and matched by asset-level index membership.
- This could be fragile under MultiIndex ordering/misalignment conditions.

Fix:
- Reindex liquidity directly to the current date-group MultiIndex (`liquidity.reindex(group.index)`) and apply boolean keep mask on aligned rows.
- Missing/non-numeric liquidity now safely falls out of the filtered universe.

Reliability impact:
- Prevents unintended keep/drop decisions due to date/asset alignment ambiguity.

## 3) Added test coverage for constrained portfolio + liquidity filter

- Added test with intentionally unsorted liquidity MultiIndex to verify correct date+asset alignment and deterministic filtering.

## 4) `demo_alpha_signal` behavior investigation and interpretation

Formula under test:
- `alpha = rank(ret_20d) - 0.5 * zscore(vol_20d)`.

Toy example (2024-01-02):
- `ret_20d`: A=0.1, B=0.2 -> rank pct: A=0.5, B=1.0
- `vol_20d`: A=0.5, B=1.0 -> zscore: A=-1, B=+1
- alpha: A=0.5 - 0.5*(-1)=1.0; B=1.0 - 0.5*(+1)=0.5

Conclusion:
- The formula is mathematically consistent as a volatility-penalized rank signal.
- Updated test expectation to reflect this consistent interpretation (A > B for that day).

## 5) Rebalance date convention unification (`simulator` vs `rebalance`)

Previous mismatch:
- `src/qsys/backtest/simulator.py` used period-start available dates for weekly/monthly rebalance.
- `src/qsys/rebalance/backtest.py` used period-end available dates.

Change made:
- Unified simulator convention to **period-end available date** (tail(1)) to match rebalance module.
- Added test asserting equality of weekly/monthly rebalance date sets across modules.

## 6) Duplicate test basename conflict mitigation

- Renamed:
  - `tests/risk/test_exposure.py` -> `tests/risk/test_risk_exposure.py`
  - `tests/research/test_exposure.py` -> `tests/research/test_research_exposure.py`

Reason:
- Avoid potential pytest import/collection ambiguity from duplicate test module basenames.

## 7) Validation

- Ran focused pytest suites for updated backtest/signal/risk/research/rebalance tests.

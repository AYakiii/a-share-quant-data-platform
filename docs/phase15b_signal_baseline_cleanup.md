# Phase 15B-2 Signal Baseline Cleanup

Date: 2026-05-08

## Scope
Documentation + minimal signal API/test semantics cleanup only.
No model expansion, no ML baseline, no optimizer work.

## What was reviewed
Searched references to `demo_alpha_signal` across:
- `src/`
- `tests/`
- `README.md`
- `docs/`

## Decisions
1. Keep `demo_alpha_signal` for backward compatibility in examples/tests.
2. Mark `demo_alpha_signal` as **experimental** (not official baseline).
3. Introduce a cleaner baseline candidate:
   - `baseline_momentum_signal(features) = cross-sectional rank(ret_20d)`
4. Keep volatility-penalty interpretation explicitly experimental and non-validated.

## Code/Test updates
- Added `baseline_momentum_signal` in `qsys.signals.engine`.
- Exported `baseline_momentum_signal` in `qsys.signals.__init__`.
- Added deterministic test for baseline rank behavior.
- Renamed demo-alpha behavior test to explicitly indicate experimental semantics.

## Documentation updates
- README signal example now distinguishes:
  - baseline candidate: `rank(ret_20d)`
  - experimental variant: `rank(ret_20d) - 0.5*zscore(vol_20d)`
- Research contract updated to clarify:
  - baseline candidate is simple ret_20d rank,
  - `vol_20d` is currently treated as exposure/conditioning/diagnostic role,
  - volatility penalty must not be promoted without diagnostics.

## Non-goals confirmed
- No deletion/archive.
- No new ML model.
- No risk optimizer.
- No promotion of experimental signal to official production strategy.

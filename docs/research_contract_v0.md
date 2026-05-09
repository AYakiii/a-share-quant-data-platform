# Research Contract v0

This contract is a lightweight guardrail for research consistency. It is not meant to restrict exploration; it is meant to prevent hidden assumptions from silently changing conclusions.

## 1. Hard Contracts

- Research data is expected to use **MultiIndex `[date, asset]`** unless explicitly documented otherwise.
- `fwd_ret_5d` and `fwd_ret_20d` are **labels** for evaluation; they are not tradable features.
- Signal construction must avoid look-ahead bias (no future information in feature/signal generation at decision time).
- Core research experiments should record at least:
  - sample range,
  - signal recipe,
  - portfolio construction rule,
  - transaction cost assumption,
  - benchmark definition.
- Changes to these contracts should be treated as methodology changes, not minor formatting changes.

## 2. Current Conventions

- Weekly/monthly rebalance currently uses **period-end available date**.
- Transaction cost is currently modeled as **turnover × bps** (with configured cost/slippage bps).
- Current backtest is a **research-level return alignment framework**, not a real execution simulator.
- Current conventions may evolve after reliability checks and comparability validation.

## 3. Experimental Assumptions

- Experimental signals, transformations, or penalties are not validated conclusions by default.
- Simple cross-sectional `rank(ret_20d)` is the current baseline candidate for comparison.
- Current volatility penalty usage is a research hypothesis and is **not validated** as a universal improvement.
- Any volatility penalty coefficient must be tested and compared before promotion to broader usage.
- Buffered rebalance is a research policy option and should not be assumed superior without evidence.

## 4. Known Non-Decisions

- The project has not decided whether `vol_20d` should primarily be:
  - an alpha penalty,
  - a risk exposure,
  - a conditioning variable,
  - or a regime state.
- Until that decision is made, treat `vol_20d` primarily as an exposure/conditioning/diagnostic variable.
- No ML model is currently promoted to an official baseline.
- No full risk model / optimizer stack is implemented yet.
- Production/live execution is outside current project scope.

## 5. How to Revise This Contract

- This contract records current research conventions, not permanent truth.
- Revisions should:
  - state the reason clearly,
  - preserve historical comparability where possible,
  - explicitly avoid introducing look-ahead bias,
  - be recorded in a phase note or commit history.
- Prefer small, explicit revisions over large implicit changes.

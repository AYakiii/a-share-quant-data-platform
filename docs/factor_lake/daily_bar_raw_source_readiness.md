# Daily Bar Raw Source Readiness Note (Phase 18A patch)

## Observed endpoint behavior in Colab
- `stock_zh_a_hist` may fail intermittently with `RemoteDisconnected` / connection-aborted behavior.
- `stock_zh_a_daily` succeeded for `000001` in 2024Q1 and returned usable raw columns:
  - `date, open, high, low, close, volume, amount, outstanding_share, turnover`

## Patch decision
For `daily_bar_raw`, the source capability registry now records:
- primary candidate: `stock_zh_a_hist`
- fallback candidate: `stock_zh_a_daily`

A probe case for `stock_zh_a_daily` was added (`20240101` to `20240331`) so `run_factor_lake_probe.py` can validate this fallback path explicitly.

## Adapter readiness update
A new raw adapter helper was added:
- `fetch_stock_zh_a_daily(symbol, start_date, end_date, adjust="")`

It accepts a 6-digit code (e.g. `000001`, `600000`) and converts to AkShare exchange-prefixed code (`sz000001`, `sh600000`) when needed. Raw columns are preserved as returned.

## Scope reminder
This is a **raw-source readiness patch only**.
- no normalized panel changes
- no factor/signal/backtest logic
- no full-market ingest

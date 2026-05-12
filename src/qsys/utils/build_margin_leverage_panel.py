"""Build normalized margin-leverage panel partitions from raw SSE/SZSE sources."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import pandas as pd

from qsys.data.sources.akshare_margin import fetch_stock_margin_detail_sse, fetch_stock_margin_detail_szse
from qsys.reporting.artifacts import write_warnings


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        x = v.strip()
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _load_symbols(symbols: list[str] | None, symbols_file: str | Path | None) -> list[str]:
    merged = list(symbols or [])
    if symbols_file:
        fp = Path(symbols_file)
        merged.extend([x.strip() for x in fp.read_text(encoding="utf-8").splitlines() if x.strip()])
    out = _dedupe_keep_order(merged)
    if not out:
        raise ValueError("No symbols provided. Please pass --symbols and/or --symbols-file.")
    return out


def _normalize_symbol(s: str) -> str:
    x = str(s).strip().lower()
    if x.startswith(("sh", "sz", "bj")) and len(x) == 8:
        return x
    digits = "".join(ch for ch in x if ch.isdigit())
    if len(digits) != 6:
        raise ValueError(f"Unsupported symbol format: {s}")
    if digits.startswith(("60", "68")):
        return f"sh{digits}"
    if digits.startswith(("00", "30")):
        return f"sz{digits}"
    return f"bj{digits}"


def _symbol_exchange(symbol: str) -> str:
    s = _normalize_symbol(symbol)
    if s.startswith("sh"):
        return "SSE"
    if s.startswith("sz"):
        return "SZSE"
    return "OTHER"


def _pick_col(df: pd.DataFrame, choices: list[str]) -> str | None:
    for c in choices:
        if c in df.columns:
            return c
    return None


def _normalize_raw_margin(raw: pd.DataFrame, date_str: str, symbols: list[str]) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["date", "asset", "financing_balance", "financing_buy_amount", "margin_total_balance"])

    symbol_col = _pick_col(raw, ["ts_code", "证券代码", "股票代码", "标的证券代码", "code"])
    date_col = _pick_col(raw, ["trade_date", "信用交易日期", "交易日期", "date", "日期"])
    fb_col = _pick_col(raw, ["financing_balance", "融资余额", "融资余额(元)"])
    buy_col = _pick_col(raw, ["financing_buy_amount", "融资买入额", "融资买入额(元)"])
    total_col = _pick_col(raw, ["margin_total_balance", "融资融券余额", "融资融券余额(元)"])

    if symbol_col is None or fb_col is None or buy_col is None or total_col is None:
        return pd.DataFrame()

    out = pd.DataFrame()
    out["asset"] = raw[symbol_col].astype(str).map(_normalize_symbol)
    if date_col is None:
        out["date"] = pd.to_datetime(date_str)
    else:
        out["date"] = pd.to_datetime(raw[date_col], errors="coerce")
        out["date"] = out["date"].fillna(pd.to_datetime(date_str))

    out["financing_balance"] = pd.to_numeric(raw[fb_col], errors="coerce")
    out["financing_buy_amount"] = pd.to_numeric(raw[buy_col], errors="coerce")
    out["margin_total_balance"] = pd.to_numeric(raw[total_col], errors="coerce")

    optional_map = {
        "short_balance": ["short_balance", "融券余额"],
        "short_sell_amount": ["short_sell_amount", "融券卖出量", "融券卖出额"],
        "margin_eligible": ["margin_eligible", "是否两融标的", "margin_eligibility"],
    }
    for out_col, candidates in optional_map.items():
        c = _pick_col(raw, candidates)
        if c is not None:
            out[out_col] = pd.to_numeric(raw[c], errors="coerce")

    out = out[out["asset"].isin(set(symbols))].dropna(subset=["date", "asset"])
    out = out.drop_duplicates(subset=["date", "asset"], keep="last")
    return out


def build_margin_leverage_panel(
    *,
    symbols: list[str] | None = None,
    symbols_file: str | Path | None = None,
    start_date: str,
    end_date: str,
    output_root: str | Path = "data/processed/margin_panel/v1",
    output_dir: str | Path = "outputs/margin_panel",
    run_name: str | None = None,
    retries: int = 2,
    retry_wait: float = 1.0,
    request_sleep: float = 0.5,
    skip_failed_symbols: bool = True,
    show_progress: bool = False,
    progress_every: int = 1,
    include_calendar_days: bool = False,
) -> dict[str, Path]:
    selected_symbols = [_normalize_symbol(s) for s in _load_symbols(symbols, symbols_file)]
    symbols_by_exchange: dict[str, list[str]] = {
        "SSE": [s for s in selected_symbols if _symbol_exchange(s) == "SSE"],
        "SZSE": [s for s in selected_symbols if _symbol_exchange(s) == "SZSE"],
    }
    run_id = run_name or f"margin_panel_{start_date}_{end_date}"
    art_dir = Path(output_dir) / run_id
    art_dir.mkdir(parents=True, exist_ok=True)
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)

    warnings: list[str] = []
    date_freq = "D" if include_calendar_days else "B"
    dates = pd.date_range(start=start_date, end=end_date, freq=date_freq)
    frames: list[pd.DataFrame] = []
    started_at = time.perf_counter()
    empty_response_counts: dict[str, int] = {"SSE": 0, "SZSE": 0}
    failed_counts: dict[str, int] = {"SSE": 0, "SZSE": 0}
    cache: dict[tuple[str, str], pd.DataFrame] = {}
    n_fetch_requests_attempted = 0

    for market_name, fetcher in [("SSE", fetch_stock_margin_detail_sse), ("SZSE", fetch_stock_margin_detail_szse)]:
        exchange_symbols = symbols_by_exchange.get(market_name, [])
        if not exchange_symbols:
            continue
        total_dates = len(dates)
        symbol_set = set(exchange_symbols)
        for date_idx, d in enumerate(dates, start=1):
            ds = d.strftime("%Y%m%d")
            key = (market_name, ds)
            if key not in cache:
                if show_progress and ((date_idx == 1) or (date_idx == total_dates) or (date_idx % max(1, int(progress_every)) == 0)):
                    print(f"[{market_name} {date_idx}/{total_dates}] START {ds}", flush=True)
                n_fetch_requests_attempted += 1
                fetch_started = time.perf_counter()
                got = None
                last_err = None
                for attempt in range(1, retries + 1):
                    try:
                        got = fetcher(ds).raw
                        break
                    except Exception as exc:  # noqa: BLE001
                        last_err = exc
                        if attempt < retries:
                            time.sleep(retry_wait * attempt)
                if got is None:
                    failed_counts[market_name] = failed_counts.get(market_name, 0) + 1
                    cache[key] = pd.DataFrame()
                    if show_progress and ((date_idx == 1) or (date_idx == total_dates) or (date_idx % max(1, int(progress_every)) == 0)):
                        total_elapsed = time.perf_counter() - started_at
                        elapsed = time.perf_counter() - fetch_started
                        print(
                            f"[{market_name} {date_idx}/{total_dates}] FAIL {ds} reason=fetch_error "
                            f"rows_raw=0 rows_selected=0 elapsed={elapsed:.1f}s total_elapsed={total_elapsed:.1f}s",
                            flush=True,
                        )
                    continue
                cache[key] = got if isinstance(got, pd.DataFrame) else pd.DataFrame(got)
                if request_sleep > 0:
                    time.sleep(request_sleep)

            got = cache[key]
            if got.empty:
                empty_response_counts[market_name] = empty_response_counts.get(market_name, 0) + 1
                if show_progress and ((date_idx == 1) or (date_idx == total_dates) or (date_idx % max(1, int(progress_every)) == 0)):
                    total_elapsed = time.perf_counter() - started_at
                    print(
                        f"[{market_name} {date_idx}/{total_dates}] FAIL {ds} reason=empty "
                        f"rows_raw=0 rows_selected=0 elapsed=0.0s total_elapsed={total_elapsed:.1f}s",
                        flush=True,
                    )
                continue
            normalized = _normalize_raw_margin(got, d.strftime("%Y-%m-%d"), exchange_symbols)
            selected_rows = int(len(normalized))
            if show_progress and ((date_idx == 1) or (date_idx == total_dates) or (date_idx % max(1, int(progress_every)) == 0)):
                total_elapsed = time.perf_counter() - started_at
                print(
                    f"[{market_name} {date_idx}/{total_dates}] OK {ds} rows_raw={int(len(got))} "
                    f"rows_selected={selected_rows} elapsed=0.0s total_elapsed={total_elapsed:.1f}s",
                    flush=True,
                )
            if not normalized.empty:
                normalized = normalized[normalized["asset"].isin(symbol_set)]
                frames.append(normalized)

    if frames:
        all_days = pd.concat(frames, ignore_index=True)
        for trade_date, day_df in all_days.groupby("date", sort=True):
            per_day = day_df.groupby(["date", "asset"], as_index=False).last()
            if per_day.empty:
                continue
            part_dir = root / f"trade_date={pd.Timestamp(trade_date).strftime('%Y-%m-%d')}"
            part_dir.mkdir(parents=True, exist_ok=True)
            out_df = per_day.rename(columns={"date": "trade_date", "asset": "ts_code"})
            out_df.to_parquet(part_dir / "data.parquet", index=False)

    if not frames:
        raise ValueError("No margin panel rows loaded for requested symbols/date range")

    panel = pd.concat(frames, ignore_index=True)
    present_assets = set(panel["asset"].unique().tolist())
    per_symbol_rows = panel.groupby("asset").size().to_dict()
    total_symbols = len(selected_symbols)
    symbols_with_data = sorted([s for s, n in per_symbol_rows.items() if int(n) > 0])
    symbols_without_data = [s for s in selected_symbols if s not in set(symbols_with_data)]
    missing = [s for s in selected_symbols if s not in present_assets]
    if missing:
        msg = "Symbols with no margin data in date range: " + ", ".join(missing)
        if skip_failed_symbols:
            warnings.append(msg)
        else:
            raise ValueError(msg)

    symbols_fp = art_dir / "symbols.txt"
    symbols_fp.write_text("\n".join(selected_symbols) + "\n", encoding="utf-8")

    quality = panel.groupby("asset").agg(n_rows=("asset", "size"), n_dates=("date", "nunique")).reset_index()
    quality_fp = art_dir / "data_quality_summary.csv"
    quality.to_csv(quality_fp, index=False)

    manifest = {
        "phase": "18A-2",
        "start_date": start_date,
        "end_date": end_date,
        "n_selected_symbols": len(selected_symbols),
        "selected_symbols": selected_symbols,
        "n_loaded_rows": int(len(panel)),
        "output_root": str(root),
        "output_dir": str(art_dir),
        "retries": retries,
        "retry_wait": retry_wait,
        "request_sleep": request_sleep,
        "skip_failed_symbols": skip_failed_symbols,
    }
    manifest_fp = art_dir / "panel_manifest.json"
    manifest_fp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for exchange, count in empty_response_counts.items():
        if count > 0:
            warnings.append(f"{exchange} empty responses skipped: {count} dates")
    for exchange, count in failed_counts.items():
        if count > 0:
            warnings.append(f"{exchange} fetch failures: {count} dates")
    n_fetch_requests_failed = int(sum(failed_counts.values()))
    n_empty_exchange_dates = int(sum(empty_response_counts.values()))
    manifest["fetch_strategy"] = "exchange_date_first"
    manifest["n_fetch_requests_attempted"] = int(n_fetch_requests_attempted)
    manifest["n_fetch_requests_failed"] = n_fetch_requests_failed
    manifest["n_empty_exchange_dates"] = n_empty_exchange_dates
    manifest["symbols_with_data"] = symbols_with_data
    manifest["symbols_without_data"] = symbols_without_data
    warnings_fp = write_warnings(art_dir, warnings)
    if show_progress:
        failed_count = sum(1 for s in selected_symbols if int(per_symbol_rows.get(s, 0)) == 0)
        fetched_count = total_symbols - failed_count
        elapsed_s = time.perf_counter() - started_at
        mins, secs = divmod(int(elapsed_s), 60)
        print(
            f"Done: fetched={fetched_count} failed={failed_count} rows={int(len(panel))} elapsed={mins}m{secs:02d}s",
            flush=True,
        )
    return {"panel_root": root, "panel_manifest": manifest_fp, "warnings": warnings_fp, "symbols": symbols_fp, "data_quality": quality_fp}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build normalized margin leverage panel")
    p.add_argument("--symbols", nargs="*", default=None)
    p.add_argument("--symbols-file", default=None)
    p.add_argument("--start-date", required=True)
    p.add_argument("--end-date", required=True)
    p.add_argument("--output-root", default="data/processed/margin_panel/v1")
    p.add_argument("--output-dir", default="outputs/margin_panel")
    p.add_argument("--run-name", default=None)
    p.add_argument("--retries", type=int, default=2)
    p.add_argument("--retry-wait", type=float, default=1.0)
    p.add_argument("--request-sleep", type=float, default=0.5)
    p.add_argument("--skip-failed-symbols", type=lambda x: str(x).lower() != "false", default=True)
    p.add_argument("--show-progress", action="store_true")
    p.add_argument("--progress-every", type=int, default=1)
    p.add_argument("--include-calendar-days", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    out = build_margin_leverage_panel(
        symbols=args.symbols,
        symbols_file=args.symbols_file,
        start_date=args.start_date,
        end_date=args.end_date,
        output_root=args.output_root,
        output_dir=args.output_dir,
        run_name=args.run_name,
        retries=args.retries,
        retry_wait=args.retry_wait,
        request_sleep=args.request_sleep,
        skip_failed_symbols=args.skip_failed_symbols,
        show_progress=args.show_progress,
        progress_every=args.progress_every,
        include_calendar_days=args.include_calendar_days,
    )
    print({k: str(v) for k, v in out.items()})


if __name__ == "__main__":
    main()

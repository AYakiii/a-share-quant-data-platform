"""Build Feature Store v1 partitions from real A-share daily data via AkShare."""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Iterable

import pandas as pd

REQUIRED_COLUMNS: list[str] = [
    "trade_date",
    "ts_code",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "turnover",
    "outstanding_share",
    "ret_1d",
    "ret_5d",
    "ret_20d",
    "vol_20d",
    "amount_20d",
    "turnover_5d",
    "turnover_20d",
    "market_cap",
    "fwd_ret_5d",
    "fwd_ret_20d",
    "is_tradable",
]


def _ts_code_to_ak_symbol(ts_code: str) -> str:
    code, exchange = ts_code.split(".")
    exchange = exchange.upper()
    if exchange == "SH":
        return f"sh{code}"
    if exchange == "SZ":
        return f"sz{code}"
    if exchange == "BJ":
        return f"bj{code}"
    raise ValueError(f"Unsupported exchange in ts_code: {ts_code}")


def _ak_spot_to_ts_code(symbol: str) -> str:
    prefix = symbol[:2].lower()
    code = symbol[2:]
    exchange = {"sh": "SH", "sz": "SZ", "bj": "BJ"}.get(prefix)
    if exchange is None:
        raise ValueError(f"Unsupported AkShare symbol: {symbol}")
    return f"{code}.{exchange}"


def _fetch_symbol_universe(limit: int | None = None) -> list[str]:
    import akshare as ak

    spot = ak.stock_zh_a_spot_em()
    if "代码" not in spot.columns:
        raise ValueError("stock_zh_a_spot_em result missing 代码 column")

    codes = spot["代码"].astype(str).str.zfill(6)
    filtered = codes[codes.str[:2].isin(["00", "30", "60", "68", "83", "87", "43"])]

    symbols = []
    for code in filtered.tolist():
        if code.startswith(("60", "68")):
            symbols.append(f"sh{code}")
        elif code.startswith(("00", "30")):
            symbols.append(f"sz{code}")
        else:
            symbols.append(f"bj{code}")

    if limit is not None:
        return symbols[:limit]
    return symbols


def _to_hist_symbol(symbol: str) -> str:
    s = str(symbol).strip().lower()
    if len(s) >= 8 and s[:2] in {"sh", "sz", "bj"}:
        return s[2:8]
    return s


def _safe_fetch_daily(symbol: str, retries: int, retry_wait: float) -> pd.DataFrame:
    import akshare as ak

    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            df = ak.stock_zh_a_daily(symbol=symbol, adjust="")
            if df is not None and not df.empty:
                return df

            hist_symbol = _to_hist_symbol(symbol)
            try:
                hist = ak.stock_zh_a_hist(symbol=hist_symbol, period="daily", adjust="")
            except TypeError:
                hist = ak.stock_zh_a_hist(symbol=hist_symbol, period="daily")

            if hist is not None and not hist.empty:
                return hist
            return pd.DataFrame()
        except Exception as exc:  # pragma: no cover - network/runtime dependent
            last_error = exc
            if attempt < retries:
                time.sleep(retry_wait * attempt)

    raise RuntimeError(f"Failed to fetch {symbol} after {retries} retries") from last_error


def _normalize_daily_frame(raw: pd.DataFrame, symbol: str) -> pd.DataFrame:
    df = raw.copy()
    rename_map = {
        "date": "trade_date",
        "日期": "trade_date",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "成交量": "volume",
        "成交额": "amount",
        "换手率": "turnover",
        "代码": "ts_code",
        "code": "ts_code",
        "股票代码": "ts_code",
    }
    df = df.rename(columns=rename_map)

    if "trade_date" not in df.columns:
        raise ValueError(f"{symbol} missing trade_date/date column")

    if "ts_code" not in df.columns:
        df["ts_code"] = _ak_spot_to_ts_code(symbol)
    else:
        ts = df["ts_code"].astype(str).str.strip()
        if not ts.str.contains(r"\.").any():
            ex = _ak_spot_to_ts_code(symbol).split(".", 1)[1]
            ts = ts.str.zfill(6) + f".{ex}"
        df["ts_code"] = ts

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df = df.dropna(subset=["trade_date"]).sort_values("trade_date")

    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "turnover",
        "outstanding_share",
    ]
    for col in numeric_cols:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors="coerce")

    close = df["close"]
    ret_1d = close.pct_change(1)
    df["ret_1d"] = ret_1d
    df["ret_5d"] = close.pct_change(5)
    df["ret_20d"] = close.pct_change(20)
    df["vol_20d"] = ret_1d.rolling(20, min_periods=20).std()
    df["amount_20d"] = df["amount"].rolling(20, min_periods=20).mean()
    df["turnover_5d"] = df["turnover"].rolling(5, min_periods=5).mean()
    df["turnover_20d"] = df["turnover"].rolling(20, min_periods=20).mean()
    df["market_cap"] = df["close"] * df["outstanding_share"]
    df["fwd_ret_5d"] = close.shift(-5) / close - 1
    df["fwd_ret_20d"] = close.shift(-20) / close - 1
    df["is_tradable"] = (
        df["close"].notna()
        & df["open"].notna()
        & (df["volume"].fillna(0) > 0)
        & (df["amount"].fillna(0) > 0)
    )

    df["trade_date"] = df["trade_date"].dt.strftime("%Y-%m-%d")
    return df[REQUIRED_COLUMNS].copy()


def build_real_feature_store(
    feature_root: str | Path,
    symbols: Iterable[str] | None = None,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    retries: int = 3,
    retry_wait: float = 1.0,
    request_sleep: float = 0.1,
    limit: int | None = None,
    skip_failed_symbols: bool = False,
    verbose: bool = False,
) -> Path:
    """Fetch A-share daily bars from AkShare and write Feature Store v1 partitions."""

    target_symbols = list(symbols) if symbols else _fetch_symbol_universe(limit=limit)
    if not target_symbols:
        raise ValueError("No symbols provided or discovered")

    frames: list[pd.DataFrame] = []
    skipped: list[tuple[str, str]] = []
    start_dt = pd.to_datetime(start_date) if start_date else None
    end_dt = pd.to_datetime(end_date) if end_date else None

    for idx, symbol in enumerate(target_symbols, start=1):
        try:
            raw = _safe_fetch_daily(symbol=symbol, retries=retries, retry_wait=retry_wait)
            if verbose:
                print(f"[DEBUG] symbol={symbol} raw_shape={raw.shape} raw_cols={list(raw.columns)}")
                if not raw.empty:
                    dt_col = "date" if "date" in raw.columns else ("日期" if "日期" in raw.columns else None)
                    if dt_col:
                        dt = pd.to_datetime(raw[dt_col], errors="coerce")
                        if len(dt.dropna()):
                            print(f"[DEBUG] symbol={symbol} raw_date_min={dt.min()} raw_date_max={dt.max()}")

            if raw.empty:
                reason = "raw_empty"
                if skip_failed_symbols:
                    skipped.append((symbol, reason))
                    if verbose:
                        print(f"[WARN] skip symbol={symbol} reason={reason}")
                    continue
                raise ValueError(f"No data fetched for symbol {symbol}")

            norm = _normalize_daily_frame(raw, symbol)
            if verbose:
                print(f"[DEBUG] symbol={symbol} norm_shape_before_filter={norm.shape}")

            norm_dt = norm.copy()
            norm_dt["trade_date"] = pd.to_datetime(norm_dt["trade_date"], errors="coerce")
            norm_dt = norm_dt.dropna(subset=["trade_date"])
            if start_dt is not None:
                norm_dt = norm_dt[norm_dt["trade_date"] >= start_dt]
            if end_dt is not None:
                norm_dt = norm_dt[norm_dt["trade_date"] <= end_dt]
            norm = norm_dt.copy()
            norm["trade_date"] = norm["trade_date"].dt.strftime("%Y-%m-%d")

            if verbose:
                print(f"[DEBUG] symbol={symbol} norm_shape_after_filter={norm.shape}")

        except Exception as exc:  # pragma: no cover - network/runtime dependent
            if skip_failed_symbols:
                skipped.append((symbol, f"fetch_or_normalize_error:{exc}"))
                if verbose:
                    print(f"[WARN] skip symbol={symbol} reason={exc}")
                continue
            raise

        if not norm.empty:
            frames.append(norm)
        else:
            reason = "empty_after_date_filter"
            if skip_failed_symbols:
                skipped.append((symbol, reason))
                if verbose:
                    print(f"[WARN] skip symbol={symbol} reason={reason}")
            elif verbose:
                print(f"[WARN] symbol={symbol} produced no rows after date filter")

        if request_sleep > 0:
            time.sleep(request_sleep)
        if idx % 100 == 0:
            print(f"Fetched {idx}/{len(target_symbols)} symbols")

    if verbose and skipped:
        print(f"[INFO] skipped_symbols={len(skipped)}")
        for sym, reason in skipped[:20]:
            print(f"[INFO] skipped {sym}: {reason}")

    if not frames:
        raise ValueError("No data fetched from AkShare for requested symbols/date range")

    all_data = pd.concat(frames, ignore_index=True)
    all_data = all_data.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)

    root = Path(feature_root)
    root.mkdir(parents=True, exist_ok=True)

    for trade_date, group in all_data.groupby("trade_date", sort=True):
        part_dir = root / f"trade_date={trade_date}"
        part_dir.mkdir(parents=True, exist_ok=True)
        group.to_parquet(part_dir / "data.parquet", index=False)

    return root


def main() -> None:
    parser = argparse.ArgumentParser(description="Build real Feature Store v1 from AkShare data")
    parser.add_argument(
        "--feature-root",
        required=True,
        help="Output root, e.g. data/processed/feature_store/v1",
    )
    parser.add_argument("--symbols", nargs="*", default=None, help="AkShare symbols, e.g. sh600000 sz000001")
    parser.add_argument("--start-date", default=None, help="Inclusive date filter: YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="Inclusive date filter: YYYY-MM-DD")
    parser.add_argument("--limit", type=int, default=None, help="Limit symbol count for quick runs")
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--retry-wait", type=float, default=1.0)
    parser.add_argument("--request-sleep", type=float, default=0.1)
    parser.add_argument("--skip-failed-symbols", action="store_true", help="Skip symbols that fail fetch/normalize")
    parser.add_argument("--verbose", action="store_true", help="Print per-symbol diagnostics")
    args = parser.parse_args()

    root = build_real_feature_store(
        feature_root=args.feature_root,
        symbols=args.symbols,
        start_date=args.start_date,
        end_date=args.end_date,
        retries=args.retries,
        retry_wait=args.retry_wait,
        request_sleep=args.request_sleep,
        limit=args.limit,
        skip_failed_symbols=args.skip_failed_symbols,
        verbose=args.verbose,
    )
    print(f"Feature store built at: {root}")


if __name__ == "__main__":
    main()

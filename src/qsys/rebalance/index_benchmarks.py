"""Market index benchmark loaders and converters."""

from __future__ import annotations

import time

import pandas as pd


def normalize_index_price_frame(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {"日期": "date", "收盘": "close", "date": "date", "close": "close"}
    out = df.rename(columns=rename_map).copy()
    if "date" not in out.columns or "close" not in out.columns:
        raise ValueError("index price frame must contain date and close columns")
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out = out.dropna(subset=["date", "close"]).sort_values("date", kind="mergesort")
    return out[["date", "close"]].reset_index(drop=True)


def build_index_return_curve(price_df: pd.DataFrame, policy: str) -> pd.DataFrame:
    px = normalize_index_price_frame(price_df)
    gross = px["close"].pct_change().fillna(0.0)
    net = gross.copy()
    cum = (1.0 + net).cumprod() - 1.0
    return pd.DataFrame(
        {
            "date": px["date"],
            "gross_return": gross,
            "net_return": net,
            "cumulative_net_return": cum,
            "policy": policy,
        }
    )


def load_akshare_index_benchmark_curve(
    name: str,
    primary_symbol: str,
    fallback_symbol: str,
    start_date: str,
    end_date: str,
    retries: int = 5,
    sleep: float = 3.0,
) -> pd.DataFrame:
    import akshare as ak

    sd = pd.to_datetime(start_date).strftime("%Y%m%d")
    ed = pd.to_datetime(end_date).strftime("%Y%m%d")
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)

    last_err: Exception | None = None
    for _ in range(max(retries, 1)):
        try:
            df = ak.index_zh_a_hist(symbol=primary_symbol, period="daily", start_date=sd, end_date=ed)
            return build_index_return_curve(df, policy=name)
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(float(sleep))

    fb = ak.stock_zh_index_daily(symbol=fallback_symbol)
    fb = normalize_index_price_frame(fb)
    fb = fb[(fb["date"] >= start_dt) & (fb["date"] <= end_dt)]
    if fb.empty and last_err is not None:
        raise RuntimeError(f"Failed to load index data for {name}") from last_err
    return build_index_return_curve(fb, policy=name)


def load_default_market_benchmark_curves(
    start_date: str,
    end_date: str,
    retries: int = 5,
    sleep: float = 3.0,
) -> dict[str, pd.DataFrame]:
    return {
        "CSI300": load_akshare_index_benchmark_curve("CSI300", "000300", "sh000300", start_date, end_date, retries, sleep),
        "CSI500": load_akshare_index_benchmark_curve("CSI500", "000905", "sh000905", start_date, end_date, retries, sleep),
        "SHANGHAI_COMPOSITE": load_akshare_index_benchmark_curve("SHANGHAI_COMPOSITE", "000001", "sh000001", start_date, end_date, retries, sleep),
    }

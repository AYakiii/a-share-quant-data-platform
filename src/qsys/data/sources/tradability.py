from __future__ import annotations

from pathlib import Path

import pandas as pd


def build_tradability_mask_v0_from_daily(raw_root: Path) -> pd.DataFrame:
    daily_root = raw_root / "stock_zh_a_daily" / "v1"
    files = sorted(daily_root.glob("symbol=*/start_date=*_end_date=*/data.parquet"))
    if not files:
        raise FileNotFoundError(f"No stock_zh_a_daily parquet found under: {daily_root}")
    frames = [pd.read_parquet(fp) for fp in files]
    daily = pd.concat(frames, ignore_index=True)
    rename_map = {"日期": "trade_date", "date": "trade_date", "代码": "stock_code", "symbol": "stock_code", "开盘": "open", "最高": "high", "最低": "low", "收盘": "close", "成交量": "volume", "成交额": "amount"}
    daily = daily.rename(columns={k: v for k, v in rename_map.items() if k in daily.columns})
    required = ["trade_date", "stock_code", "open", "high", "low", "close", "volume", "amount"]
    miss = [c for c in required if c not in daily.columns]
    if miss:
        raise ValueError(f"Missing required columns from stock_zh_a_daily: {miss}")
    daily["trade_date"] = pd.to_datetime(daily["trade_date"], errors="coerce")
    daily["stock_code"] = daily["stock_code"].astype(str).str.extract(r"(\d+)", expand=False).fillna("").str.zfill(6)
    out = daily[["trade_date", "stock_code", "open", "high", "low", "close", "volume", "amount"]].copy()
    out["bar_exists"] = True
    ohlc_notna = out[["open", "high", "low", "close"]].notna().all(axis=1)
    high_ok = out["high"] >= out[["open", "close", "low"]].max(axis=1)
    low_ok = out["low"] <= out[["open", "close", "high"]].min(axis=1)
    out["valid_ohlc"] = ohlc_notna & high_ok & low_ok
    out["positive_volume"] = out["volume"] > 0
    out["positive_amount"] = out["amount"] > 0
    out["tradable_proxy"] = out["bar_exists"] & out["valid_ohlc"] & out["positive_volume"] & out["positive_amount"]
    return out[["trade_date", "stock_code", "bar_exists", "valid_ohlc", "positive_volume", "positive_amount", "tradable_proxy"]]

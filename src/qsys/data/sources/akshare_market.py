"""AkShare raw adapters for market price/volume data."""

from __future__ import annotations

import pandas as pd

from qsys.data.sources.base import SourceFetchResult, build_source_metadata


def to_akshare_stock_symbol(symbol: str) -> str:
    """Convert 6-digit A-share code to AkShare symbol format, if needed."""

    if symbol.startswith(("sh", "sz", "bj")):
        return symbol
    if len(symbol) != 6 or not symbol.isdigit():
        return symbol
    if symbol.startswith(("5", "6", "9")):
        return f"sh{symbol}"
    if symbol.startswith(("0", "1", "2", "3")):
        return f"sz{symbol}"
    return f"bj{symbol}"


def fetch_stock_zh_a_hist(
    symbol: str,
    start_date: str,
    end_date: str,
    period: str = "daily",
    adjust: str = "qfq",
) -> SourceFetchResult:
    """Fetch raw stock daily bars from AkShare without factorization."""

    import akshare as ak  # lazy import for test monkeypatching

    raw = ak.stock_zh_a_hist(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        period=period,
        adjust=adjust,
    )
    if not isinstance(raw, pd.DataFrame):
        raw = pd.DataFrame(raw)

    params = {
        "symbol": symbol,
        "start_date": start_date,
        "end_date": end_date,
        "period": period,
        "adjust": adjust,
    }
    meta = build_source_metadata(
        api_name="stock_zh_a_hist",
        source_family="market_price_volume",
        request_params=params,
        raw=raw,
    )
    return SourceFetchResult(api_name="stock_zh_a_hist", source_family="market_price_volume", raw=raw, metadata=meta)


def fetch_stock_zh_a_daily(symbol: str, start_date: str, end_date: str, adjust: str = "") -> SourceFetchResult:
    """Fetch raw stock daily bars via stock_zh_a_daily with symbol conversion fallback."""

    import akshare as ak

    ak_symbol = to_akshare_stock_symbol(symbol)
    raw = ak.stock_zh_a_daily(symbol=ak_symbol, start_date=start_date, end_date=end_date, adjust=adjust)
    if not isinstance(raw, pd.DataFrame):
        raw = pd.DataFrame(raw)

    params = {"symbol": symbol, "ak_symbol": ak_symbol, "start_date": start_date, "end_date": end_date, "adjust": adjust}
    meta = build_source_metadata(
        api_name="stock_zh_a_daily",
        source_family="market_price_volume",
        request_params=params,
        raw=raw,
    )
    return SourceFetchResult(api_name="stock_zh_a_daily", source_family="market_price_volume", raw=raw, metadata=meta)

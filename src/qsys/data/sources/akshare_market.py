"""AkShare raw adapters for market price/volume data."""

from __future__ import annotations

import pandas as pd

from qsys.data.sources.base import SourceFetchResult, build_source_metadata


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

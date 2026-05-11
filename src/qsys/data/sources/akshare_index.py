"""AkShare raw adapters for index history data."""

from __future__ import annotations

import pandas as pd

from qsys.data.sources.base import SourceFetchResult, build_source_metadata


def fetch_stock_zh_index_hist_csindex(symbol: str, start_date: str, end_date: str) -> SourceFetchResult:
    """Fetch raw CSI index history from AkShare."""

    import akshare as ak

    raw = ak.stock_zh_index_hist_csindex(symbol=symbol, start_date=start_date, end_date=end_date)
    if not isinstance(raw, pd.DataFrame):
        raw = pd.DataFrame(raw)

    params = {"symbol": symbol, "start_date": start_date, "end_date": end_date}
    meta = build_source_metadata(
        api_name="stock_zh_index_hist_csindex",
        source_family="market_index_regime",
        request_params=params,
        raw=raw,
    )
    return SourceFetchResult(
        api_name="stock_zh_index_hist_csindex",
        source_family="market_index_regime",
        raw=raw,
        metadata=meta,
    )

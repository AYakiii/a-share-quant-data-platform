"""AkShare raw adapters for disclosure calendar sources."""

from __future__ import annotations

import pandas as pd

from qsys.data.sources.base import SourceFetchResult, build_source_metadata


def fetch_stock_yysj_em(symbol: str = "沪深A股", date: str | None = None) -> SourceFetchResult:
    """Fetch raw disclosure-calendar data from AkShare."""

    import akshare as ak

    kwargs = {"symbol": symbol}
    if date is not None:
        kwargs["date"] = date

    raw = ak.stock_yysj_em(**kwargs)
    if not isinstance(raw, pd.DataFrame):
        raw = pd.DataFrame(raw)

    meta = build_source_metadata(
        api_name="stock_yysj_em",
        source_family="fundamental_disclosure",
        request_params=kwargs,
        raw=raw,
        notes="Availability-calendar source; raw adapter only.",
    )
    return SourceFetchResult(api_name="stock_yysj_em", source_family="fundamental_disclosure", raw=raw, metadata=meta)

"""AkShare raw adapters for margin-trading sources."""

from __future__ import annotations

import pandas as pd

from qsys.data.sources.base import SourceFetchResult, build_source_metadata


def fetch_stock_margin_detail_sse(date: str) -> SourceFetchResult:
    """Fetch SSE margin detail raw data."""

    import akshare as ak

    raw = ak.stock_margin_detail_sse(date=date)
    if not isinstance(raw, pd.DataFrame):
        raw = pd.DataFrame(raw)

    params = {"date": date}
    meta = build_source_metadata(
        api_name="stock_margin_detail_sse",
        source_family="margin_leverage",
        request_params=params,
        raw=raw,
    )
    return SourceFetchResult(api_name="stock_margin_detail_sse", source_family="margin_leverage", raw=raw, metadata=meta)


def fetch_stock_margin_detail_szse(date: str) -> SourceFetchResult:
    """Fetch SZSE margin detail raw data and annotate trade_date when raw date is absent."""

    import akshare as ak

    raw = ak.stock_margin_detail_szse(date=date)
    if not isinstance(raw, pd.DataFrame):
        raw = pd.DataFrame(raw)

    normalized_columns: list[str] = []
    notes: str | None = None
    if "trade_date" not in raw.columns and "信用交易日期" not in raw.columns:
        raw = raw.copy()
        raw["trade_date"] = date
        normalized_columns.append("trade_date")
        notes = "trade_date injected from input date because SZSE raw output had no date column."

    params = {"date": date}
    meta = build_source_metadata(
        api_name="stock_margin_detail_szse",
        source_family="margin_leverage",
        request_params=params,
        raw=raw,
        normalized_columns=normalized_columns,
        notes=notes,
    )
    return SourceFetchResult(api_name="stock_margin_detail_szse", source_family="margin_leverage", raw=raw, metadata=meta)

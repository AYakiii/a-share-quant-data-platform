"""AkShare raw adapters for corporate actions and unlock sources."""

from __future__ import annotations

import pandas as pd

from qsys.data.sources.base import SourceFetchResult, build_source_metadata


def _to_df(raw: object) -> pd.DataFrame:
    return raw if isinstance(raw, pd.DataFrame) else pd.DataFrame(raw)


def _result(api_name: str, raw: object, request_params: dict[str, object], notes: str | None = None) -> SourceFetchResult:
    df = _to_df(raw)
    meta = build_source_metadata(
        api_name=api_name,
        source_family="corporate_action",
        request_params=request_params,
        raw=df,
        notes=notes,
    )
    return SourceFetchResult(api_name, "corporate_action", df, meta)


def fetch_stock_fhps_em() -> SourceFetchResult:
    import akshare as ak

    return _result("stock_fhps_em", ak.stock_fhps_em(), {})


def fetch_stock_history_dividend() -> SourceFetchResult:
    import akshare as ak

    return _result("stock_history_dividend", ak.stock_history_dividend(), {})


def fetch_stock_history_dividend_detail(symbol: str) -> SourceFetchResult:
    import akshare as ak

    return _result("stock_history_dividend_detail", ak.stock_history_dividend_detail(symbol=symbol), {"symbol": symbol})


def fetch_stock_restricted_release_queue_em() -> SourceFetchResult:
    import akshare as ak

    return _result("stock_restricted_release_queue_em", ak.stock_restricted_release_queue_em(), {})


def fetch_stock_restricted_release_summary_em() -> SourceFetchResult:
    import akshare as ak

    return _result("stock_restricted_release_summary_em", ak.stock_restricted_release_summary_em(), {})


def fetch_stock_restricted_release_detail_em() -> SourceFetchResult:
    import akshare as ak

    return _result(
        "stock_restricted_release_detail_em",
        ak.stock_restricted_release_detail_em(),
        {},
        notes="Post-event fields like 解禁后20日涨跌幅 must be treated as label/outcome only.",
    )

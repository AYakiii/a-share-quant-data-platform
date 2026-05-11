"""AkShare raw adapters for industry/theme sources."""

from __future__ import annotations

import pandas as pd

from qsys.data.sources.base import SourceFetchResult, build_source_metadata


def _to_df(raw: object) -> pd.DataFrame:
    return raw if isinstance(raw, pd.DataFrame) else pd.DataFrame(raw)


def fetch_stock_industry_clf_hist_sw() -> SourceFetchResult:
    import akshare as ak

    raw = _to_df(ak.stock_industry_clf_hist_sw())
    params: dict[str, str] = {}
    meta = build_source_metadata(
        api_name="stock_industry_clf_hist_sw",
        source_family="industry",
        request_params=params,
        raw=raw,
    )
    return SourceFetchResult("stock_industry_clf_hist_sw", "industry", raw, meta)


def fetch_index_component_sw(symbol: str) -> SourceFetchResult:
    import akshare as ak

    raw = _to_df(ak.index_component_sw(symbol=symbol))
    params = {"symbol": symbol}
    meta = build_source_metadata(
        api_name="index_component_sw",
        source_family="industry",
        request_params=params,
        raw=raw,
        notes="最新权重 is latest snapshot weight, not historical weight series.",
    )
    return SourceFetchResult("index_component_sw", "industry", raw, meta)


def fetch_index_hist_sw(symbol: str, period: str = "day") -> SourceFetchResult:
    import akshare as ak

    raw = _to_df(ak.index_hist_sw(symbol=symbol, period=period))
    params = {"symbol": symbol, "period": period}
    meta = build_source_metadata(
        api_name="index_hist_sw",
        source_family="industry",
        request_params=params,
        raw=raw,
    )
    return SourceFetchResult("index_hist_sw", "industry", raw, meta)


def fetch_stock_industry_change_cninfo(
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> SourceFetchResult:
    import akshare as ak

    kwargs = {"symbol": symbol}
    if start_date is not None:
        kwargs["start_date"] = start_date
    if end_date is not None:
        kwargs["end_date"] = end_date

    raw = _to_df(ak.stock_industry_change_cninfo(**kwargs))
    meta = build_source_metadata(
        api_name="stock_industry_change_cninfo",
        source_family="industry",
        request_params=kwargs,
        raw=raw,
    )
    return SourceFetchResult("stock_industry_change_cninfo", "industry", raw, meta)


def fetch_sw_index_first_info() -> SourceFetchResult:
    import akshare as ak

    raw = _to_df(ak.sw_index_first_info())
    meta = build_source_metadata(
        api_name="sw_index_first_info",
        source_family="industry_valuation",
        request_params={},
        raw=raw,
        notes="Snapshot-like source; preserve raw output as-is without inferred historical timeline.",
    )
    return SourceFetchResult("sw_index_first_info", "industry_valuation", raw, meta)


def fetch_sw_index_second_info() -> SourceFetchResult:
    import akshare as ak

    raw = _to_df(ak.sw_index_second_info())
    meta = build_source_metadata(
        api_name="sw_index_second_info",
        source_family="industry_valuation",
        request_params={},
        raw=raw,
        notes="Snapshot-like source; preserve raw output as-is without inferred historical timeline.",
    )
    return SourceFetchResult("sw_index_second_info", "industry_valuation", raw, meta)


def fetch_sw_index_third_info() -> SourceFetchResult:
    import akshare as ak

    raw = _to_df(ak.sw_index_third_info())
    meta = build_source_metadata(
        api_name="sw_index_third_info",
        source_family="industry_valuation",
        request_params={},
        raw=raw,
        notes="Snapshot-like source; preserve raw output as-is without inferred historical timeline.",
    )
    return SourceFetchResult("sw_index_third_info", "industry_valuation", raw, meta)


def fetch_stock_board_industry_index_ths(symbol: str, start_date: str, end_date: str) -> SourceFetchResult:
    import akshare as ak

    raw = _to_df(ak.stock_board_industry_index_ths(symbol=symbol, start_date=start_date, end_date=end_date))
    params = {"symbol": symbol, "start_date": start_date, "end_date": end_date}
    meta = build_source_metadata(
        api_name="stock_board_industry_index_ths",
        source_family="ths_industry_theme",
        request_params=params,
        raw=raw,
    )
    return SourceFetchResult("stock_board_industry_index_ths", "ths_industry_theme", raw, meta)


def fetch_stock_board_concept_index_ths(symbol: str, start_date: str, end_date: str) -> SourceFetchResult:
    import akshare as ak

    raw = _to_df(ak.stock_board_concept_index_ths(symbol=symbol, start_date=start_date, end_date=end_date))
    params = {"symbol": symbol, "start_date": start_date, "end_date": end_date}
    meta = build_source_metadata(
        api_name="stock_board_concept_index_ths",
        source_family="ths_concept_theme",
        request_params=params,
        raw=raw,
    )
    return SourceFetchResult("stock_board_concept_index_ths", "ths_concept_theme", raw, meta)


def fetch_stock_board_concept_summary_ths() -> SourceFetchResult:
    import akshare as ak

    raw = _to_df(ak.stock_board_concept_summary_ths())
    meta = build_source_metadata(
        api_name="stock_board_concept_summary_ths",
        source_family="theme_event",
        request_params={},
        raw=raw,
        notes="Theme event summary source; not concept membership source.",
    )
    return SourceFetchResult("stock_board_concept_summary_ths", "theme_event", raw, meta)

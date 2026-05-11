"""AkShare raw adapters for block trade, LHB, and event-attention sources."""

from __future__ import annotations

import pandas as pd

from qsys.data.sources.base import SourceFetchResult, build_source_metadata


def _to_df(raw: object) -> pd.DataFrame:
    return raw if isinstance(raw, pd.DataFrame) else pd.DataFrame(raw)


def _result(api_name: str, source_family: str, raw: object, request_params: dict[str, object], notes: str | None = None) -> SourceFetchResult:
    df = _to_df(raw)
    meta = build_source_metadata(
        api_name=api_name,
        source_family=source_family,
        request_params=request_params,
        raw=df,
        notes=notes,
    )
    return SourceFetchResult(api_name, source_family, df, meta)


def fetch_stock_dzjy_sctj() -> SourceFetchResult:
    import akshare as ak

    return _result("stock_dzjy_sctj", "block_trade", ak.stock_dzjy_sctj(), {})


def fetch_stock_dzjy_mrmx(date: str | None = None) -> SourceFetchResult:
    import akshare as ak

    kwargs: dict[str, object] = {}
    if date is not None:
        kwargs["date"] = date
    return _result("stock_dzjy_mrmx", "block_trade", ak.stock_dzjy_mrmx(**kwargs), kwargs)


def fetch_stock_dzjy_mrtj(date: str | None = None) -> SourceFetchResult:
    import akshare as ak

    kwargs: dict[str, object] = {}
    if date is not None:
        kwargs["date"] = date
    return _result("stock_dzjy_mrtj", "block_trade", ak.stock_dzjy_mrtj(**kwargs), kwargs)


def fetch_stock_dzjy_hyyybtj() -> SourceFetchResult:
    import akshare as ak

    return _result("stock_dzjy_hyyybtj", "block_trade", ak.stock_dzjy_hyyybtj(), {})


def fetch_stock_lhb_detail_em(start_date: str | None = None, end_date: str | None = None) -> SourceFetchResult:
    import akshare as ak

    kwargs: dict[str, object] = {}
    if start_date is not None:
        kwargs["start_date"] = start_date
    if end_date is not None:
        kwargs["end_date"] = end_date
    return _result(
        "stock_lhb_detail_em",
        "trading_attention",
        ak.stock_lhb_detail_em(**kwargs),
        kwargs,
        notes="Post-event fields 上榜后1日/2日/5日/10日 must not be used as signal inputs.",
    )


def fetch_stock_lhb_stock_statistic_em(start_date: str | None = None, end_date: str | None = None) -> SourceFetchResult:
    import akshare as ak

    kwargs: dict[str, object] = {}
    if start_date is not None:
        kwargs["start_date"] = start_date
    if end_date is not None:
        kwargs["end_date"] = end_date
    return _result("stock_lhb_stock_statistic_em", "trading_attention", ak.stock_lhb_stock_statistic_em(**kwargs), kwargs)


def fetch_stock_lhb_jgmmtj_em(start_date: str | None = None, end_date: str | None = None) -> SourceFetchResult:
    import akshare as ak

    kwargs: dict[str, object] = {}
    if start_date is not None:
        kwargs["start_date"] = start_date
    if end_date is not None:
        kwargs["end_date"] = end_date
    return _result("stock_lhb_jgmmtj_em", "trading_attention", ak.stock_lhb_jgmmtj_em(**kwargs), kwargs)


def fetch_stock_lhb_hyyyb_em(start_date: str | None = None, end_date: str | None = None) -> SourceFetchResult:
    import akshare as ak

    kwargs: dict[str, object] = {}
    if start_date is not None:
        kwargs["start_date"] = start_date
    if end_date is not None:
        kwargs["end_date"] = end_date
    return _result("stock_lhb_hyyyb_em", "trading_attention", ak.stock_lhb_hyyyb_em(**kwargs), kwargs)


def fetch_stock_lhb_yybph_em(start_date: str | None = None, end_date: str | None = None) -> SourceFetchResult:
    import akshare as ak

    kwargs: dict[str, object] = {}
    if start_date is not None:
        kwargs["start_date"] = start_date
    if end_date is not None:
        kwargs["end_date"] = end_date
    return _result("stock_lhb_yybph_em", "trading_attention", ak.stock_lhb_yybph_em(**kwargs), kwargs)


def fetch_stock_jgdy_tj_em() -> SourceFetchResult:
    import akshare as ak

    return _result("stock_jgdy_tj_em", "institution_attention", ak.stock_jgdy_tj_em(), {})


def fetch_stock_yjyg_em(date: str) -> SourceFetchResult:
    import akshare as ak

    return _result("stock_yjyg_em", "fundamental_event", ak.stock_yjyg_em(date=date), {"date": date})

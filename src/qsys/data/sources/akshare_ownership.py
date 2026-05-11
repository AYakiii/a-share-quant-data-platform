"""AkShare raw adapters for ownership/governance sources."""

from __future__ import annotations

import pandas as pd

from qsys.data.sources.base import SourceFetchResult, build_source_metadata


def _to_df(raw: object) -> pd.DataFrame:
    return raw if isinstance(raw, pd.DataFrame) else pd.DataFrame(raw)


def _result(api_name: str, source_family: str, raw: object, request_params: dict[str, object]) -> SourceFetchResult:
    df = _to_df(raw)
    meta = build_source_metadata(api_name=api_name, source_family=source_family, request_params=request_params, raw=df)
    return SourceFetchResult(api_name, source_family, df, meta)


def fetch_stock_zh_a_gdhs() -> SourceFetchResult:
    import akshare as ak

    return _result("stock_zh_a_gdhs", "ownership_structure", ak.stock_zh_a_gdhs(), {})


def fetch_stock_zh_a_gdhs_detail_em(symbol: str) -> SourceFetchResult:
    import akshare as ak

    return _result("stock_zh_a_gdhs_detail_em", "ownership_structure", ak.stock_zh_a_gdhs_detail_em(symbol=symbol), {"symbol": symbol})


def fetch_stock_gdfx_free_holding_analyse_em() -> SourceFetchResult:
    import akshare as ak

    return _result("stock_gdfx_free_holding_analyse_em", "ownership_structure", ak.stock_gdfx_free_holding_analyse_em(), {})


def fetch_stock_gdfx_holding_analyse_em() -> SourceFetchResult:
    import akshare as ak

    return _result("stock_gdfx_holding_analyse_em", "ownership_structure", ak.stock_gdfx_holding_analyse_em(), {})


def fetch_stock_gpzy_pledge_ratio_em() -> SourceFetchResult:
    import akshare as ak

    return _result("stock_gpzy_pledge_ratio_em", "ownership_governance_risk", ak.stock_gpzy_pledge_ratio_em(), {})


def fetch_stock_gpzy_pledge_ratio_detail_em() -> SourceFetchResult:
    import akshare as ak

    return _result("stock_gpzy_pledge_ratio_detail_em", "ownership_governance_risk", ak.stock_gpzy_pledge_ratio_detail_em(), {})


def fetch_stock_gpzy_industry_data_em() -> SourceFetchResult:
    import akshare as ak

    return _result("stock_gpzy_industry_data_em", "ownership_governance_risk", ak.stock_gpzy_industry_data_em(), {})


def fetch_stock_gpzy_profile_em() -> SourceFetchResult:
    import akshare as ak

    return _result("stock_gpzy_profile_em", "ownership_governance_risk", ak.stock_gpzy_profile_em(), {})

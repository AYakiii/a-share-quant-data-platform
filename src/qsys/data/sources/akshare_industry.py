"""AkShare raw adapters for industry/theme sources."""

from __future__ import annotations

import os
import time
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests

from qsys.data.sources.base import SourceFetchResult, build_source_metadata


SW_INDUSTRY_RESCUE_URL = "https://www.swsresearch.com/swindex/pdf/SwClass2021/StockClassifyUse_stock.xls"




_RETRYABLE_HTTP_CODES = {429, 508, 500, 502, 503, 504}
_SW_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Referer": "https://www.swsresearch.com/",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}


def _download_with_retry(source_url: str, timeout: float, verify: bool, retries: int = 3, backoff_sec: float = 1.0) -> requests.Response:
    last_exc: Exception | None = None
    for idx in range(retries):
        try:
            resp = requests.get(source_url, timeout=timeout, verify=verify, headers=_SW_HEADERS)
            status_code = getattr(resp, "status_code", 200)
            if status_code in _RETRYABLE_HTTP_CODES:
                raise requests.exceptions.HTTPError(f"{status_code} retryable", response=resp)
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status in _RETRYABLE_HTTP_CODES and idx < retries - 1:
                time.sleep(backoff_sec * (2 ** idx))
                last_exc = exc
                continue
            raise
        except Exception as exc:
            last_exc = exc
            if idx < retries - 1:
                time.sleep(backoff_sec * (2 ** idx))
                continue
            raise
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("unreachable")
def _normalize_sw_industry_history(df: pd.DataFrame) -> pd.DataFrame:
    out = df.rename(
        columns={
            "股票代码": "stock_code",
            "计入日期": "effective_date",
            "行业代码": "industry_code",
            "更新日期": "source_update_time",
        }
    ).copy()
    out["stock_code"] = out["stock_code"].astype(str).str.extract(r"(\d+)", expand=False).fillna("").str.zfill(6)
    out["effective_date"] = pd.to_datetime(out["effective_date"], errors="coerce")
    out["source_update_time"] = pd.to_datetime(out["source_update_time"], errors="coerce")
    out["industry_code"] = out["industry_code"].astype(str).str.extract(r"(\d+)", expand=False).fillna("").str.zfill(6)
    out["industry_l1_code"] = out["industry_code"].str[:2]
    out["industry_l2_code"] = out["industry_code"].str[:4]
    out["industry_l3_code"] = out["industry_code"].str[:6]
    return out


def fetch_sw_industry_membership_rescue(source_url: str = SW_INDUSTRY_RESCUE_URL, timeout: float = 30.0, local_file: str | Path | None = None) -> SourceFetchResult:
    ssl_verify = True
    manual_review_required = False
    rescue_reason = ""
    source_mode = "remote_download"
    local_file_path = ""
    local_candidate = Path(local_file) if local_file else (Path(os.environ["QSYS_SW_INDUSTRY_MEMBERSHIP_XLS"]) if os.environ.get("QSYS_SW_INDUSTRY_MEMBERSHIP_XLS") else None)
    try:
        resp = _download_with_retry(source_url=source_url, timeout=timeout, verify=True)
    except requests.exceptions.SSLError:
        resp = _download_with_retry(source_url=source_url, timeout=timeout, verify=False)
        raw = pd.read_excel(BytesIO(resp.content))
        ssl_verify = False
        manual_review_required = True
        rescue_reason = "SSL certificate verification failed in Colab"
    except Exception:
        if local_candidate is None:
            raise
        raw = pd.read_excel(local_candidate)
        source_mode = "local_file_fallback"
        local_file_path = str(local_candidate)
        manual_review_required = True
        rescue_reason = "Remote SW Excel download failed; local file fallback used"
    else:
        raw = pd.read_excel(BytesIO(resp.content))
    normalized = _normalize_sw_industry_history(raw)
    meta = build_source_metadata(
        api_name="sw_industry_membership_rescue",
        source_family="industry",
        request_params={"source_url": source_url},
        raw=normalized,
        notes="Direct SW Excel rescue path; source_update_time is metadata-only and PIT risk is medium.",
    )
    meta.update(
        {
            "ssl_verify": ssl_verify,
            "manual_review_required": manual_review_required,
            "rescue_reason": rescue_reason,
            "source_url": source_url,
            "pit_risk": "medium",
            "metadata_only_fields": ["source_update_time"],
            "source_mode": source_mode,
            "local_file_path": local_file_path,
        }
    )
    return SourceFetchResult("sw_industry_membership_rescue", "industry", normalized, meta)


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

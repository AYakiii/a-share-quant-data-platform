from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

import pandas as pd

from qsys.data.sources.akshare_market import fetch_stock_zh_a_daily, fetch_stock_zh_a_hist
from qsys.data.sources.akshare_margin import fetch_stock_margin_detail_sse, fetch_stock_margin_detail_szse


@dataclass(frozen=True)
class FetchPartition:
    values: dict[str, str]


@dataclass(frozen=True)
class SourceSpec:
    source_name: str
    source_version: str
    partition_keys: tuple[str, ...]
    fetch_mode: str
    build_fetch_plan: Callable[..., Iterable[FetchPartition]]
    fetch_partition: Callable[[FetchPartition], pd.DataFrame | dict[str, Any]]
    build_raw_partition_path: Callable[[Path, FetchPartition], Path]
    schema_hints: dict[str, str]
    empty_result_allowed: bool = True
    provider: str | None = None
    source_family: str | None = None
    priority: str | None = None
    acquisition_status: str = "enabled"
    manual_review_required: bool = False
    disabled_reason: str | None = None
    empty_policy: str = "warn"
    expected_grain: str | None = None


def build_margin_detail_fetch_plan(*, start_date: str, end_date: str, include_calendar_days: bool, exchanges: str) -> list[FetchPartition]:
    freq = "D" if include_calendar_days else "B"
    dates = pd.date_range(start=start_date, end=end_date, freq=freq)
    ex_arg = (exchanges or "both").lower().strip()
    if ex_arg == "both":
        used = ["SSE", "SZSE"]
    elif ex_arg in {"sse", "szse"}:
        used = [ex_arg.upper()]
    else:
        raise ValueError("--exchanges must be one of: sse, szse, both")
    return [FetchPartition(values={"exchange": ex, "trade_date": d.strftime("%Y-%m-%d")}) for ex in used for d in dates]


def build_stock_zh_a_daily_fetch_plan(*, symbols: str | list[str], start_date: str, end_date: str, **_: Any) -> list[FetchPartition]:
    symbol_list = symbols.split(",") if isinstance(symbols, str) else symbols
    cleaned = [s.strip() for s in symbol_list if s and s.strip()]
    if not cleaned:
        raise ValueError("--symbols is required for stock_zh_a_daily")
    return [FetchPartition(values={"symbol": symbol, "start_date": start_date, "end_date": end_date}) for symbol in cleaned]


def _fetch_margin_partition(partition: FetchPartition) -> pd.DataFrame:
    ex = partition.values["exchange"]
    ds = partition.values["trade_date"].replace("-", "")
    if ex == "SSE":
        return fetch_stock_margin_detail_sse(ds).raw
    if ex == "SZSE":
        return fetch_stock_margin_detail_szse(ds).raw
    raise ValueError(f"Unsupported exchange: {ex}")


def _detect_date_col(df: pd.DataFrame) -> str | None:
    for c in ["date", "日期", "trade_date"]:
        if c in df.columns:
            return c
    return None


def _fetch_stock_zh_a_daily_partition(partition: FetchPartition) -> dict[str, Any]:
    symbol = partition.values["symbol"]
    start_date = partition.values["start_date"]
    end_date = partition.values["end_date"]
    primary_err = ""
    fallback_err = ""
    original_symbol = symbol
    try:
        data = fetch_stock_zh_a_hist(symbol=symbol, start_date=start_date, end_date=end_date, adjust="").raw
        actual = "stock_zh_a_hist"
        fallback_from = ""
    except Exception as exc:
        primary_err = f"{type(exc).__name__}: {exc}"
        try:
            data = fetch_stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date).raw
            actual = "stock_zh_a_daily"
            fallback_from = "stock_zh_a_hist"
        except Exception as exc2:
            fallback_err = f"{type(exc2).__name__}: {exc2}"
            raise RuntimeError(f"primary_error={primary_err}; fallback_error={fallback_err}")

    df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
    date_col = _detect_date_col(df)
    rows_before = int(len(df))
    min_before = max_before = None
    rows_after = rows_before
    min_after = max_after = None
    if date_col and not df.empty:
        dt = pd.to_datetime(df[date_col], errors="coerce")
        min_before = str(dt.min()) if dt.notna().any() else None
        max_before = str(dt.max()) if dt.notna().any() else None
        mask = (dt >= pd.to_datetime(start_date)) & (dt <= pd.to_datetime(end_date))
        df = df.loc[mask].copy()
        rows_after = int(len(df))
        dta = pd.to_datetime(df[date_col], errors="coerce") if not df.empty else pd.Series(dtype="datetime64[ns]")
        min_after = str(dta.min()) if (not df.empty and dta.notna().any()) else None
        max_after = str(dta.max()) if (not df.empty and dta.notna().any()) else None

    return {
        "data": df,
        "requested_api_name": "stock_zh_a_hist",
        "actual_api_name": actual,
        "fallback_from": fallback_from,
        "primary_error": primary_err,
        "fallback_error": fallback_err,
        "original_symbol": original_symbol,
        "akshare_symbol": symbol,
        "rows_before_filter": rows_before,
        "rows_after_filter": rows_after,
        "min_date_before": min_before,
        "max_date_before": max_before,
        "min_date_after": min_after,
        "max_date_after": max_after,
    }


def _margin_partition_path(raw_root: Path, partition: FetchPartition) -> Path:
    return raw_root / "margin_detail" / "v1" / f"exchange={partition.values['exchange']}" / f"trade_date={partition.values['trade_date']}" / "data.parquet"

SOURCE_SPECS: dict[str, SourceSpec] = {
    MARGIN_DETAIL_SPEC.source_name: MARGIN_DETAIL_SPEC,
    STOCK_ZH_A_DAILY_SPEC.source_name: STOCK_ZH_A_DAILY_SPEC,
}


def _stock_zh_a_daily_partition_path(raw_root: Path, partition: FetchPartition) -> Path:
    return raw_root / "stock_zh_a_daily" / "v1" / f"symbol={partition.values['symbol']}" / f"start_date={partition.values['start_date']}_end_date={partition.values['end_date']}" / "data.parquet"


MARGIN_DETAIL_SPEC = SourceSpec("margin_detail", "v1", ("exchange", "trade_date"), "exchange_date", build_margin_detail_fetch_plan, _fetch_margin_partition, _margin_partition_path, {"trade_date": "date", "exchange": "category"}, True, "akshare", "margin", "P1", "enabled", False, None, "allow", "exchange-trade_date detail rows")

STOCK_ZH_A_DAILY_SPEC = SourceSpec("stock_zh_a_daily", "v1", ("symbol", "start_date", "end_date"), "symbol_date_range", build_stock_zh_a_daily_fetch_plan, _fetch_stock_zh_a_daily_partition, _stock_zh_a_daily_partition_path, {"symbol": "category", "date": "date"}, False, "akshare", "market_price", "P0", "enabled", False, None, "warn", "asset-date daily bars")

SOURCE_SPECS: dict[str, SourceSpec] = {MARGIN_DETAIL_SPEC.source_name: MARGIN_DETAIL_SPEC, STOCK_ZH_A_DAILY_SPEC.source_name: STOCK_ZH_A_DAILY_SPEC}

def _stock_zh_a_daily_partition_path(raw_root: Path, partition: FetchPartition) -> Path:
    return raw_root / "stock_zh_a_daily" / "v1" / f"symbol={partition.values['symbol']}" / f"start_date={partition.values['start_date']}_end_date={partition.values['end_date']}" / "data.parquet"


MARGIN_DETAIL_SPEC = SourceSpec("margin_detail", "v1", ("exchange", "trade_date"), "exchange_date", build_margin_detail_fetch_plan, _fetch_margin_partition, _margin_partition_path, {"trade_date": "date", "exchange": "category"}, True, "akshare", "margin", "P1", "enabled", False, None, "allow", "exchange-trade_date detail rows")

STOCK_ZH_A_DAILY_SPEC = SourceSpec("stock_zh_a_daily", "v1", ("symbol", "start_date", "end_date"), "symbol_date_range", build_stock_zh_a_daily_fetch_plan, _fetch_stock_zh_a_daily_partition, _stock_zh_a_daily_partition_path, {"symbol": "category", "date": "date"}, False, "akshare", "market_price", "P0", "enabled", False, None, "warn", "asset-date daily bars")

SOURCE_SPECS: dict[str, SourceSpec] = {MARGIN_DETAIL_SPEC.source_name: MARGIN_DETAIL_SPEC, STOCK_ZH_A_DAILY_SPEC.source_name: STOCK_ZH_A_DAILY_SPEC}

def get_source_spec(source_name: str) -> SourceSpec:
    try:
        return SOURCE_SPECS[source_name]
    except KeyError as exc:
        raise ValueError(f"Unsupported source: {source_name}") from exc

def list_source_specs() -> list[str]:
    return sorted(SOURCE_SPECS.keys())

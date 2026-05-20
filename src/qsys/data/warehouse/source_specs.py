from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

import pandas as pd

from qsys.data.sources.akshare_market import fetch_stock_zh_a_daily
from qsys.data.sources.akshare_margin import fetch_stock_margin_detail_sse, fetch_stock_margin_detail_szse


@dataclass(frozen=True)
class FetchPartition:
    """Generic raw partition descriptor."""

    values: dict[str, str]


@dataclass(frozen=True)
class SourceSpec:
    """Configuration contract for a raw source in warehouse mode."""

    source_name: str
    source_version: str
    partition_keys: tuple[str, ...]
    fetch_mode: str
    build_fetch_plan: Callable[..., Iterable[FetchPartition]]
    fetch_partition: Callable[[FetchPartition], pd.DataFrame]
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
    return [
        FetchPartition(values={"exchange": ex, "trade_date": d.strftime("%Y-%m-%d")})
        for ex in used
        for d in dates
    ]


def build_stock_zh_a_daily_fetch_plan(*, symbols: str | list[str], start_date: str, end_date: str, **_: Any) -> list[FetchPartition]:
    symbol_list = symbols.split(",") if isinstance(symbols, str) else symbols
    cleaned = [s.strip() for s in symbol_list if s and s.strip()]
    if not cleaned:
        raise ValueError("--symbols is required for stock_zh_a_daily")
    return [
        FetchPartition(values={"symbol": symbol, "start_date": start_date, "end_date": end_date})
        for symbol in cleaned
    ]


def _fetch_margin_partition(partition: FetchPartition) -> pd.DataFrame:
    ex = partition.values["exchange"]
    ds = partition.values["trade_date"].replace("-", "")
    if ex == "SSE":
        return fetch_stock_margin_detail_sse(ds).raw
    if ex == "SZSE":
        return fetch_stock_margin_detail_szse(ds).raw
    raise ValueError(f"Unsupported exchange: {ex}")


def _fetch_stock_zh_a_daily_partition(partition: FetchPartition) -> pd.DataFrame:
    return fetch_stock_zh_a_daily(
        symbol=partition.values["symbol"],
        start_date=partition.values["start_date"],
        end_date=partition.values["end_date"],
    ).raw


def _margin_partition_path(raw_root: Path, partition: FetchPartition) -> Path:
    return (
        raw_root
        / "margin_detail"
        / "v1"
        / f"exchange={partition.values['exchange']}"
        / f"trade_date={partition.values['trade_date']}"
        / "data.parquet"
    )


def _stock_zh_a_daily_partition_path(raw_root: Path, partition: FetchPartition) -> Path:
    return (
        raw_root
        / "stock_zh_a_daily"
        / "v1"
        / f"symbol={partition.values['symbol']}"
        / f"start_date={partition.values['start_date']}_end_date={partition.values['end_date']}"
        / "data.parquet"
    )


MARGIN_DETAIL_SPEC = SourceSpec(
    source_name="margin_detail",
    source_version="v1",
    partition_keys=("exchange", "trade_date"),
    fetch_mode="exchange_date",
    build_fetch_plan=build_margin_detail_fetch_plan,
    fetch_partition=_fetch_margin_partition,
    build_raw_partition_path=_margin_partition_path,
    schema_hints={"trade_date": "date", "exchange": "category"},
    empty_result_allowed=True,
    provider="akshare",
    source_family="margin",
    priority="P1",
    acquisition_status="enabled",
    empty_policy="allow",
    expected_grain="exchange-trade_date detail rows",
)

STOCK_ZH_A_DAILY_SPEC = SourceSpec(
    source_name="stock_zh_a_daily",
    source_version="v1",
    partition_keys=("symbol", "start_date", "end_date"),
    fetch_mode="symbol_date_range",
    build_fetch_plan=build_stock_zh_a_daily_fetch_plan,
    fetch_partition=_fetch_stock_zh_a_daily_partition,
    build_raw_partition_path=_stock_zh_a_daily_partition_path,
    schema_hints={"symbol": "category", "date": "date"},
    empty_result_allowed=False,
    provider="akshare",
    source_family="market_price",
    priority="P0",
    acquisition_status="enabled",
    empty_policy="warn",
    expected_grain="asset-date daily bars",
)

SOURCE_SPECS: dict[str, SourceSpec] = {
    MARGIN_DETAIL_SPEC.source_name: MARGIN_DETAIL_SPEC,
    STOCK_ZH_A_DAILY_SPEC.source_name: STOCK_ZH_A_DAILY_SPEC,
}


def get_source_spec(source_name: str) -> SourceSpec:
    try:
        return SOURCE_SPECS[source_name]
    except KeyError as exc:
        raise ValueError(f"Unsupported source: {source_name}") from exc


def list_source_specs() -> list[str]:
    return sorted(SOURCE_SPECS.keys())

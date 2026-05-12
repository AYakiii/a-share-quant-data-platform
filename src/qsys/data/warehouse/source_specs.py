from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

import pandas as pd

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


def _fetch_margin_partition(partition: FetchPartition) -> pd.DataFrame:
    ex = partition.values["exchange"]
    ds = partition.values["trade_date"].replace("-", "")
    if ex == "SSE":
        return fetch_stock_margin_detail_sse(ds).raw
    if ex == "SZSE":
        return fetch_stock_margin_detail_szse(ds).raw
    raise ValueError(f"Unsupported exchange: {ex}")


def _margin_partition_path(raw_root: Path, partition: FetchPartition) -> Path:
    return (
        raw_root
        / "margin_detail"
        / "v1"
        / f"exchange={partition.values['exchange']}"
        / f"trade_date={partition.values['trade_date']}"
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
)


def get_source_spec(source_name: str) -> SourceSpec:
    if source_name == "margin_detail":
        return MARGIN_DETAIL_SPEC
    raise ValueError(f"Unsupported source: {source_name}")

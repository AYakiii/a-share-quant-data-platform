"""Calendar-aware request planning for Tushare raw acquisition."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

import pandas as pd


class CalendarQueryClient(Protocol):
    """Minimal client protocol used to fetch provider calendar data."""

    def query(self, api_name: str, **params: Any) -> pd.DataFrame: ...


def calendar_days(start_date: str, end_date: str) -> list[str]:
    """Return inclusive natural dates formatted as YYYYMMDD."""
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    days: list[str] = []
    cur = start
    while cur <= end:
        days.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)
    return days


@dataclass(frozen=True)
class CalendarPlan:
    """Resolved provider calendar plan and audit metadata."""

    trade_dates: list[str]
    calendar_days: list[str]
    date_source: str
    calendar_source: str
    cache_path: str | None
    skipped_non_trading_days_count: int


class TushareCalendarPlanner:
    """Fetch and cache Tushare trade_cal for local-only request planning."""

    def __init__(self, output_root: str | Path, client: CalendarQueryClient | None = None) -> None:
        self.output_root = Path(output_root)
        self.client = client

    def cache_path(self, start_date: str, end_date: str) -> Path:
        """Return local-only cache path for a trade_cal request window."""
        return self.output_root / "artifacts" / "tushare_raw_acquisition" / "calendar" / f"trade_cal_{start_date}_{end_date}.csv"

    def plan(self, start_date: str, end_date: str, *, calendar_mode: str = "trading_days") -> CalendarPlan:
        """Resolve request dates for the requested calendar mode."""
        natural_days = calendar_days(start_date, end_date)
        if calendar_mode == "calendar_days":
            return CalendarPlan(natural_days, natural_days, "calendar_days", "calendar_range", None, 0)
        if calendar_mode == "manual":
            return CalendarPlan(natural_days, natural_days, "manual", "manual", None, 0)
        if calendar_mode != "trading_days":
            raise ValueError(f"unsupported calendar_mode: {calendar_mode}")

        path = self.cache_path(start_date, end_date)
        if path.exists():
            cal = pd.read_csv(path, dtype={"cal_date": str})
            date_source = "tushare_trade_cal_cache"
        else:
            if self.client is None:
                raise RuntimeError("Tushare trade_cal client is required to plan trading_days")
            cal = self.client.query("trade_cal", start_date=start_date, end_date=end_date)
            path.parent.mkdir(parents=True, exist_ok=True)
            cal.to_csv(path, index=False)
            date_source = "tushare_trade_cal"
        if "cal_date" not in cal.columns or "is_open" not in cal.columns:
            raise ValueError("trade_cal response must include cal_date and is_open")
        cal = cal.copy()
        cal["cal_date"] = cal["cal_date"].astype(str)
        trading = sorted(cal.loc[cal["is_open"].astype(int) == 1, "cal_date"].tolist())
        return CalendarPlan(trading, natural_days, date_source, "trade_cal", str(path), max(0, len(natural_days) - len(trading)))

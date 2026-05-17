from __future__ import annotations

from datetime import datetime, timedelta


def _norm(values: list[str] | None) -> list[str]:
    return [v.strip() for v in (values or []) if v and v.strip()]


def load_stock_symbols(symbols: list[str] | None = None) -> list[str]:
    vals = _norm(symbols)
    return vals if vals else ["000001", "000002", "000004"]


def load_index_symbols(index_symbols: list[str] | None = None) -> list[str]:
    vals = _norm(index_symbols)
    return vals if vals else ["000300", "000905", "000852"]


def build_trade_dates(start_date: str, end_date: str, trade_dates: list[str] | None = None) -> list[str]:
    vals = _norm(trade_dates)
    if vals:
        return vals
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    out: list[str] = []
    cur = start
    while cur <= end:
        out.append(cur.strftime("%Y%m%d"))
        cur = cur + timedelta(days=1)
    return out


def build_report_dates(start_date: str, end_date: str, report_dates: list[str] | None = None) -> list[str]:
    vals = _norm(report_dates)
    if vals:
        return vals
    s, e = int(start_date[:4]), int(end_date[:4])
    q = ["0331", "0630", "0930", "1231"]
    return [f"{y}{m}" for y in range(s, e + 1) for m in q if start_date <= f"{y}{m}" <= end_date]


def load_industry_names(industry_names: list[str] | None = None) -> list[str]:
    vals = _norm(industry_names)
    return vals if vals else ["半导体", "银行", "医药生物"]


def load_concept_names(concept_names: list[str] | None = None) -> list[str]:
    vals = _norm(concept_names)
    return vals if vals else ["AI PC", "算力", "新能源车"]

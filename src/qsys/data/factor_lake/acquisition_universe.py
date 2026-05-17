from __future__ import annotations

from pathlib import Path
import pandas as pd

DEFAULT_UNIVERSE_ROOT = Path("config/factor_sources/acquisition_universe")


def _norm(values: list[str] | None) -> list[str]:
    return [v.strip() for v in (values or []) if v and v.strip()]


def _load_required_csv(path: Path, column: str) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Missing acquisition universe file: {path}")
    df = pd.read_csv(path)
    if column not in df.columns:
        raise ValueError(f"Universe file {path} missing required column: {column}")
    out = [str(v).strip() for v in df[column].tolist() if str(v).strip()]
    if not out:
        raise ValueError(f"Universe file {path} has no valid values in column: {column}")
    return out


def load_stock_symbols(symbols: list[str] | None = None, universe_root: str | Path = DEFAULT_UNIVERSE_ROOT) -> list[str]:
    vals = _norm(symbols)
    return vals if vals else _load_required_csv(Path(universe_root) / "stock_symbols.csv", "symbol")


def load_index_symbols(index_symbols: list[str] | None = None, universe_root: str | Path = DEFAULT_UNIVERSE_ROOT) -> list[str]:
    vals = _norm(index_symbols)
    return vals if vals else _load_required_csv(Path(universe_root) / "index_symbols.csv", "index_symbol")


def build_trade_dates(start_date: str, end_date: str, trade_dates: list[str] | None = None, universe_root: str | Path = DEFAULT_UNIVERSE_ROOT) -> list[str]:
    vals = _norm(trade_dates)
    if vals:
        return vals
    dates = _load_required_csv(Path(universe_root) / "trading_calendar.csv", "trade_date")
    return [d for d in dates if start_date <= d <= end_date]


def build_report_dates(start_date: str, end_date: str, report_dates: list[str] | None = None) -> list[str]:
    vals = _norm(report_dates)
    if vals:
        return vals
    s, e = int(start_date[:4]), int(end_date[:4])
    q = ["0331", "0630", "0930", "1231"]
    return [f"{y}{m}" for y in range(s, e + 1) for m in q if start_date <= f"{y}{m}" <= end_date]


def load_industry_names(industry_names: list[str] | None = None, universe_root: str | Path = DEFAULT_UNIVERSE_ROOT) -> list[str]:
    vals = _norm(industry_names)
    return vals if vals else _load_required_csv(Path(universe_root) / "industry_names.csv", "industry_name")


def load_concept_names(concept_names: list[str] | None = None, universe_root: str | Path = DEFAULT_UNIVERSE_ROOT) -> list[str]:
    vals = _norm(concept_names)
    return vals if vals else _load_required_csv(Path(universe_root) / "concept_names.csv", "concept_name")

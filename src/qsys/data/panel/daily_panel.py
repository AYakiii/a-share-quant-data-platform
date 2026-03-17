"""Panel reader for standardized daily bars parquet dataset."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd


_EXPECTED_PANEL_COLUMNS: list[str] = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "adj_factor",
    "market_cap",
    "is_tradable",
]


@dataclass(frozen=True)
class DailyPanelConfig:
    """Configuration for reading daily panel data."""

    dataset_root: Path = Path("data/standardized/market/daily_bars")


class DailyPanelReader:
    """Read standardized daily bars and normalize output for research usage.

    The expected source layout is unchanged from the notebook pipeline:
    ``dataset_root/trade_date=YYYY-MM-DD/data.parquet``.
    """

    def __init__(self, config: DailyPanelConfig | None = None) -> None:
        self.config = config or DailyPanelConfig()

    def load(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        symbols: Sequence[str] | None = None,
        columns: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """Load panel data with optional date/symbol/column filters.

        Returns a DataFrame indexed by ``[date, asset]``.
        """

        dates = self._resolve_trade_dates(start_date=start_date, end_date=end_date)
        if not dates:
            return self._empty_panel(columns=columns)

        frames: list[pd.DataFrame] = []
        symbol_set = set(symbols) if symbols else None

        for date_str in dates:
            fp = self.config.dataset_root / f"trade_date={date_str}" / "data.parquet"
            if not fp.exists():
                continue

            df = pd.read_parquet(fp)
            if df.empty:
                continue

            if symbol_set is not None and "ts_code" in df.columns:
                df = df[df["ts_code"].isin(symbol_set)]
                if df.empty:
                    continue

            frames.append(df)

        if not frames:
            return self._empty_panel(columns=columns)

        combined = pd.concat(frames, ignore_index=True)
        normalized = self._normalize(combined)

        if columns is not None:
            unknown = [c for c in columns if c not in _EXPECTED_PANEL_COLUMNS]
            if unknown:
                raise ValueError(f"Unknown panel columns requested: {unknown}")
            normalized = normalized[list(columns)]

        return normalized.sort_index()

    def _resolve_trade_dates(self, *, start_date: str | None, end_date: str | None) -> list[str]:
        pattern = self.config.dataset_root.glob("trade_date=*/data.parquet")
        dates = sorted({p.parent.name.split("=", 1)[1] for p in pattern})

        if start_date:
            dates = [d for d in dates if d >= start_date]
        if end_date:
            dates = [d for d in dates if d <= end_date]
        return dates

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()

        if "trade_date" not in out.columns or "ts_code" not in out.columns:
            raise ValueError("Input dataset must contain trade_date and ts_code columns")

        out = out.rename(columns={"trade_date": "date", "ts_code": "asset", "vol": "volume"})
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out.dropna(subset=["date", "asset"]).drop_duplicates(subset=["date", "asset"], keep="last")

        if "is_tradable" not in out.columns:
            out["is_tradable"] = out["close"].notna() if "close" in out.columns else pd.Series(False, index=out.index)

        for col in _EXPECTED_PANEL_COLUMNS:
            if col not in out.columns:
                out[col] = pd.NA

        out = out[["date", "asset"] + _EXPECTED_PANEL_COLUMNS]
        out = out.set_index(["date", "asset"])
        return out

    def _empty_panel(self, columns: Sequence[str] | None = None) -> pd.DataFrame:
        target_cols = list(columns) if columns is not None else _EXPECTED_PANEL_COLUMNS
        empty = pd.DataFrame(columns=target_cols)
        empty.index = pd.MultiIndex.from_arrays([[], []], names=["date", "asset"])
        return empty


def load_daily_panel(
    *,
    dataset_root: str | Path = Path("data/standardized/market/daily_bars"),
    start_date: str | None = None,
    end_date: str | None = None,
    symbols: Iterable[str] | None = None,
    columns: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Convenience function to load normalized daily panel data."""

    reader = DailyPanelReader(DailyPanelConfig(dataset_root=Path(dataset_root)))
    return reader.load(
        start_date=start_date,
        end_date=end_date,
        symbols=list(symbols) if symbols is not None else None,
        columns=columns,
    )

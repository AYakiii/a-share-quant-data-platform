"""Minimal signal engine for transforming and combining feature signals."""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Sequence

import pandas as pd

from qsys.signals.combine import linear_combine
from qsys.signals.transforms import rank_cross_section, winsorize_cross_section, zscore_cross_section

TransformFunc = Callable[[pd.Series], pd.Series]


def load_feature_store_frame(
    feature_root: str | Path = Path("data/processed/feature_store/v1"),
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    symbols: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Load materialized feature-store partitions to MultiIndex [date, asset]."""

    root = Path(feature_root)
    files = sorted(root.glob("trade_date=*/data.parquet"))
    if not files:
        return pd.DataFrame(index=pd.MultiIndex.from_arrays([[], []], names=["date", "asset"]))

    frames: list[pd.DataFrame] = []
    for fp in files:
        trade_date = fp.parent.name.split("=", 1)[1]
        if start_date and trade_date < start_date:
            continue
        if end_date and trade_date > end_date:
            continue
        df = pd.read_parquet(fp)
        if df.empty:
            continue
        frames.append(df)

    if not frames:
        return pd.DataFrame(index=pd.MultiIndex.from_arrays([[], []], names=["date", "asset"]))

    out = pd.concat(frames, ignore_index=True)
    out = out.rename(columns={"trade_date": "date", "ts_code": "asset"})
    if "asset" not in out.columns or "date" not in out.columns:
        raise ValueError("Feature store files must include date and asset columns")

    if symbols is not None:
        out = out[out["asset"].isin(set(symbols))]

    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date", "asset"]).set_index(["date", "asset"]).sort_index()
    return out


class SignalEngine:
    """Thin signal engine to transform feature columns and combine them."""

    def __init__(self) -> None:
        self._transforms: dict[str, Callable[..., pd.Series]] = {
            "winsorize": winsorize_cross_section,
            "zscore": zscore_cross_section,
            "rank": rank_cross_section,
        }

    def apply_transform(self, series: pd.Series, transform_name: str, **kwargs) -> pd.Series:
        """Apply one named transform to a signal series."""

        if transform_name not in self._transforms:
            raise KeyError(f"Unknown transform: {transform_name}")
        return self._transforms[transform_name](series, **kwargs)

    def build_transformed_signals(
        self,
        features: pd.DataFrame,
        recipes: dict[str, list[dict]],
    ) -> dict[str, pd.Series]:
        """Build transformed signals from feature columns.

        recipes format:
        {
          "sig_name": [
            {"column": "ret_20d"},
            {"transform": "rank", "params": {"pct": True}}
          ]
        }
        """

        signals: dict[str, pd.Series] = {}

        for signal_name, steps in recipes.items():
            if not steps or "column" not in steps[0]:
                raise ValueError(f"Recipe for {signal_name} must start with a column step")

            col = steps[0]["column"]
            if col not in features.columns:
                raise KeyError(f"Feature column not found: {col}")

            s = features[col].copy()
            s.name = signal_name

            for step in steps[1:]:
                tname = step.get("transform")
                params = step.get("params", {})
                s = self.apply_transform(s, tname, **params)
                s.name = signal_name

            signals[signal_name] = s

        return signals

    def combine(self, signals: dict[str, pd.Series], weights: dict[str, float]) -> pd.Series:
        """Combine transformed signals into a final alpha series."""

        return linear_combine(signals, weights)


def demo_alpha_signal(features: pd.DataFrame) -> pd.Series:
    """Experimental demo alpha: rank(ret_20d) - 0.5 * zscore(vol_20d).

    This helper is retained for experiments/examples and is not the official baseline.
    """

    engine = SignalEngine()
    signals = engine.build_transformed_signals(
        features,
        recipes={
            "rank_ret_20d": [
                {"column": "ret_20d"},
                {"transform": "rank", "params": {"pct": True}},
            ],
            "z_vol_20d": [
                {"column": "vol_20d"},
                {"transform": "zscore", "params": {}},
            ],
        },
    )
    return engine.combine(signals, weights={"rank_ret_20d": 1.0, "z_vol_20d": -0.5})


def baseline_momentum_signal(features: pd.DataFrame) -> pd.Series:
    """Baseline momentum candidate: cross-sectional rank(ret_20d) by date."""

    if "ret_20d" not in features.columns:
        raise KeyError("Feature column not found: ret_20d")
    return rank_cross_section(features["ret_20d"], pct=True).rename("baseline_momentum_signal")

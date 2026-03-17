"""Feature definitions and computation helpers for Feature Store v1."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Sequence

import pandas as pd

from qsys.features.base import BaseFeature
from qsys.features.registry import FeatureRegistry


@dataclass(frozen=True)
class FunctionFeature(BaseFeature):
    """A feature backed by a callable implementation."""

    fn: Callable[[pd.DataFrame], pd.Series | pd.DataFrame]

    def compute(self, panel: pd.DataFrame) -> pd.Series | pd.DataFrame:
        return self.fn(panel)


def _require_columns(panel: pd.DataFrame, columns: Sequence[str]) -> None:
    missing = [c for c in columns if c not in panel.columns]
    if missing:
        raise ValueError(f"Panel missing required columns: {missing}")


def _sorted_panel(panel: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(panel.index, pd.MultiIndex) or panel.index.names != ["date", "asset"]:
        raise ValueError("Panel must be indexed by MultiIndex [date, asset]")
    return panel.sort_index(level=[1, 0]).copy()


def _daily_returns(panel: pd.DataFrame) -> pd.Series:
    _require_columns(panel, ["close"])
    p = _sorted_panel(panel)
    return p.groupby(level="asset")["close"].pct_change(1)


def default_feature_registry() -> FeatureRegistry:
    """Build default feature registry for Feature Store v1."""

    reg = FeatureRegistry()

    reg.register(FunctionFeature("ret_1d", ("close",), lambda p: _daily_returns(p).rename("ret_1d")))
    reg.register(
        FunctionFeature(
            "ret_5d",
            ("close",),
            lambda p: _sorted_panel(p).groupby(level="asset")["close"].pct_change(5).rename("ret_5d"),
        )
    )
    reg.register(
        FunctionFeature(
            "ret_20d",
            ("close",),
            lambda p: _sorted_panel(p).groupby(level="asset")["close"].pct_change(20).rename("ret_20d"),
        )
    )
    reg.register(
        FunctionFeature(
            "vol_20d",
            ("close",),
            lambda p: _daily_returns(p)
            .groupby(level="asset")
            .rolling(20, min_periods=20)
            .std()
            .droplevel(0)
            .rename("vol_20d"),
        )
    )

    def _turnover_roll(p: pd.DataFrame, window: int, name: str) -> pd.Series:
        s = _sorted_panel(p)
        source_col = "turnover" if "turnover" in s.columns else "amount"
        _require_columns(s, [source_col])
        return (
            s.groupby(level="asset")[source_col]
            .rolling(window, min_periods=window)
            .mean()
            .droplevel(0)
            .rename(name)
        )

    reg.register(FunctionFeature("turnover_5d", ("amount",), lambda p: _turnover_roll(p, 5, "turnover_5d")))
    reg.register(FunctionFeature("turnover_20d", ("amount",), lambda p: _turnover_roll(p, 20, "turnover_20d")))
    reg.register(
        FunctionFeature(
            "amount_20d",
            ("amount",),
            lambda p: _sorted_panel(p)
            .groupby(level="asset")["amount"]
            .rolling(20, min_periods=20)
            .mean()
            .droplevel(0)
            .rename("amount_20d"),
        )
    )

    def _market_cap(p: pd.DataFrame) -> pd.Series:
        s = _sorted_panel(p)
        if "market_cap" in s.columns:
            return s["market_cap"].rename("market_cap")
        return pd.Series(pd.NA, index=s.index, name="market_cap")

    reg.register(FunctionFeature("market_cap", tuple(), _market_cap))

    reg.register(
        FunctionFeature(
            "fwd_ret_5d",
            ("close",),
            lambda p: (
                _sorted_panel(p).groupby(level="asset")["close"].shift(-5)
                / _sorted_panel(p)["close"]
                - 1
            ).rename("fwd_ret_5d"),
        )
    )
    reg.register(
        FunctionFeature(
            "fwd_ret_20d",
            ("close",),
            lambda p: (
                _sorted_panel(p).groupby(level="asset")["close"].shift(-20)
                / _sorted_panel(p)["close"]
                - 1
            ).rename("fwd_ret_20d"),
        )
    )

    return reg


def compute_features(
    panel: pd.DataFrame,
    feature_names: Sequence[str],
    *,
    registry: FeatureRegistry | None = None,
) -> pd.DataFrame:
    """Compute selected features from a normalized panel."""

    reg = registry or default_feature_registry()
    base = _sorted_panel(panel)
    out = pd.DataFrame(index=base.index)

    for name in feature_names:
        feature = reg.get(name)
        _require_columns(base, list(feature.required_columns))
        value = feature.compute(base)

        if isinstance(value, pd.Series):
            out[name] = value.reindex(base.index)
        else:
            for col in value.columns:
                out[col] = value[col].reindex(base.index)

    out.index = pd.MultiIndex.from_tuples(out.index, names=["date", "asset"])
    return out.sort_index()

"""Deterministic candidate margin-leverage factor builder (Phase 17P)."""

from __future__ import annotations

import pandas as pd

REQUIRED_COLUMNS = ["financing_balance", "financing_buy_amount", "margin_total_balance"]
FORBIDDEN_FIELDS = {"fwd_ret_5d", "fwd_ret_20d", "解禁后20日涨跌幅", "上榜后1日", "上榜后2日", "上榜后5日", "上榜后10日"}

DEFAULT_WINDOWS: dict[str, list[int]] = {
    "change": [5, 20],
    "mean": [5, 20],
    "shock": [5, 20],
}


def _ensure_index(panel: pd.DataFrame, date_level: str, asset_level: str) -> pd.DataFrame:
    if isinstance(panel.index, pd.MultiIndex) and list(panel.index.names) == [date_level, asset_level]:
        return panel
    if date_level in panel.columns and asset_level in panel.columns:
        return panel.set_index([date_level, asset_level])
    raise ValueError(f"panel must be MultiIndex ['{date_level}', '{asset_level}'] or contain these columns")


def _numeric_required(panel: pd.DataFrame) -> pd.DataFrame:
    out = panel.copy()
    for c in REQUIRED_COLUMNS:
        if c not in out.columns:
            raise ValueError(f"Missing required columns: {c}")
        try:
            out[c] = pd.to_numeric(out[c], errors="raise")
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"Required column must be numeric: {c}") from e
    return out


def build_margin_leverage_factors(
    panel: pd.DataFrame,
    windows: dict[str, list[int]] | None = None,
    date_level: str = "date",
    asset_level: str = "asset",
) -> pd.DataFrame:
    """Build candidate margin/leverage factors from normalized margin panel input."""

    w = DEFAULT_WINDOWS if windows is None else windows
    frame = _numeric_required(_ensure_index(panel, date_level=date_level, asset_level=asset_level))
    out = pd.DataFrame(index=frame.index)

    g = frame.groupby(level=asset_level, sort=False)

    financing_balance = frame["financing_balance"]
    financing_buy = frame["financing_buy_amount"]
    margin_total = frame["margin_total_balance"]

    out["financing_balance"] = financing_balance
    out["margin_total_balance"] = margin_total

    for x in w.get("change", []):
        out[f"financing_balance_chg_{x}d"] = financing_balance / g["financing_balance"].shift(x) - 1.0
        out[f"margin_total_balance_chg_{x}d"] = margin_total / g["margin_total_balance"].shift(x) - 1.0

    for x in w.get("mean", []):
        out[f"financing_buy_mean_{x}d"] = financing_buy.groupby(level=asset_level, sort=False).transform(
            lambda s: s.rolling(x, min_periods=x).mean()
        )

    if 5 in w.get("shock", []) and 20 in w.get("shock", []):
        denom = out["financing_buy_mean_20d"].where(out["financing_buy_mean_20d"] > 0)
        out["financing_buy_shock_5d_vs_20d"] = out["financing_buy_mean_5d"] / denom - 1.0

    if "financing_repay_amount" in frame.columns:
        repay = pd.to_numeric(frame["financing_repay_amount"], errors="coerce")
        out["financing_net_buy"] = financing_buy - repay
        for x in w.get("mean", []):
            out[f"financing_net_buy_mean_{x}d"] = out["financing_net_buy"].groupby(level=asset_level, sort=False).transform(
                lambda s: s.rolling(x, min_periods=x).mean()
            )

    if "short_sell_volume" in frame.columns:
        short_sell = pd.to_numeric(frame["short_sell_volume"], errors="coerce")
        for x in w.get("mean", []):
            out[f"short_sell_mean_{x}d"] = short_sell.groupby(level=asset_level, sort=False).transform(
                lambda s: s.rolling(x, min_periods=x).mean()
            )
        if 5 in w.get("shock", []) and 20 in w.get("shock", []):
            denom = out["short_sell_mean_20d"].where(out["short_sell_mean_20d"] > 0)
            out["short_sell_shock_5d_vs_20d"] = out["short_sell_mean_5d"] / denom - 1.0

    if "short_balance" in frame.columns:
        short_balance = pd.to_numeric(frame["short_balance"], errors="coerce")
        for x in w.get("change", []):
            out[f"short_balance_chg_{x}d"] = short_balance / short_balance.groupby(level=asset_level, sort=False).shift(x) - 1.0

    if "margin_eligibility" in frame.columns:
        eligibility = pd.to_numeric(frame["margin_eligibility"], errors="coerce")
        out["margin_eligibility_dummy"] = (eligibility > 0).astype(float)

    bad = sorted(FORBIDDEN_FIELDS.intersection(set(out.columns)))
    if bad:
        raise ValueError(f"Output contains forbidden fields: {bad}")

    return out

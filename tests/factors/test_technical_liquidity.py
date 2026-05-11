from __future__ import annotations

import pandas as pd
import pytest

from qsys.factors.technical_liquidity import build_technical_liquidity_factors


def _make_panel(n_dates: int = 90) -> pd.DataFrame:
    dates = pd.date_range("2025-01-01", periods=n_dates, freq="D")
    assets = ["000001.SZ", "600000.SH"]
    idx = pd.MultiIndex.from_product([dates, assets], names=["date", "asset"])

    close_vals = []
    high_vals = []
    low_vals = []
    amount_vals = []
    turnover_vals = []

    for d_i, _ in enumerate(dates):
        for a_i, _ in enumerate(assets):
            base = 100 + d_i + 10 * a_i
            close = float(base)
            high = close + 2
            low = close - 2
            amount = float(1000 + 20 * d_i + 30 * a_i)
            turnover = float(0.02 + 0.0005 * d_i + 0.001 * a_i)
            close_vals.append(close)
            high_vals.append(high)
            low_vals.append(low)
            amount_vals.append(amount)
            turnover_vals.append(turnover)

    panel = pd.DataFrame(
        {
            "close": close_vals,
            "high": high_vals,
            "low": low_vals,
            "amount": amount_vals,
            "turnover": turnover_vals,
        },
        index=idx,
    )
    return panel


def test_output_index_equals_input_index() -> None:
    panel = _make_panel()
    out = build_technical_liquidity_factors(panel)
    assert out.index.equals(panel.index)


def test_expected_factor_columns_present() -> None:
    panel = _make_panel()
    out = build_technical_liquidity_factors(panel)
    expected = {
        "ret_5d", "ret_20d", "ret_60d", "momentum_20d", "momentum_60d", "reversal_5d", "reversal_20d",
        "realized_vol_20d", "realized_vol_60d", "downside_vol_20d", "downside_vol_60d",
        "max_drawdown_20d", "max_drawdown_60d", "amount_mean_20d", "amount_mean_60d", "turnover_mean_20d",
        "turnover_mean_60d", "amount_shock_5d_vs_20d", "turnover_shock_5d_vs_20d", "amihud_illiquidity_20d",
        "amihud_illiquidity_60d", "high_low_range_20d", "high_low_range_60d", "close_to_high_20d", "close_to_high_60d",
    }
    assert expected.issubset(out.columns)


def test_missing_required_columns_raises() -> None:
    panel = _make_panel().drop(columns=["turnover"])
    with pytest.raises(ValueError, match="Missing required columns"):
        build_technical_liquidity_factors(panel)


def test_ret_5d_definition_for_one_asset() -> None:
    panel = _make_panel()
    out = build_technical_liquidity_factors(panel)
    asset = "000001.SZ"
    g = panel.xs(asset, level="asset")
    expected = g["close"] / g["close"].shift(5) - 1.0
    got = out.xs(asset, level="asset")["ret_5d"]
    pd.testing.assert_series_equal(got, expected, check_names=False)


def test_reversal_5d_is_negative_ret_5d() -> None:
    panel = _make_panel()
    out = build_technical_liquidity_factors(panel)
    pd.testing.assert_series_equal(out["reversal_5d"], -out["ret_5d"], check_names=False)


def test_amount_shock_definition() -> None:
    panel = _make_panel()
    out = build_technical_liquidity_factors(panel)
    expected = out["amount_mean_20d"].copy()
    # reconstruct 5d mean directly for assertion
    amount_5 = panel["amount"].groupby(level="asset", sort=False).transform(lambda s: s.rolling(5, min_periods=5).mean())
    expected = amount_5 / out["amount_mean_20d"] - 1.0
    pd.testing.assert_series_equal(out["amount_shock_5d_vs_20d"], expected, check_names=False)


def test_non_positive_amount_produces_nan_in_amihud() -> None:
    panel = _make_panel()
    panel.loc[(slice(None), "000001.SZ"), "amount"] = -1.0
    out = build_technical_liquidity_factors(panel)
    s = out.xs("000001.SZ", level="asset")["amihud_illiquidity_20d"]
    assert s.isna().all()


def test_initial_rows_before_full_window_are_nan() -> None:
    panel = _make_panel()
    out = build_technical_liquidity_factors(panel)
    s = out.xs("000001.SZ", level="asset")["ret_60d"]
    assert s.iloc[:60].isna().all()


def test_multiple_assets_computed_independently() -> None:
    panel = _make_panel()
    out = build_technical_liquidity_factors(panel)

    for asset in ["000001.SZ", "600000.SH"]:
        g = panel.xs(asset, level="asset")
        expected = g["close"] / g["close"].shift(5) - 1.0
        got = out.xs(asset, level="asset")["ret_5d"]
        pd.testing.assert_series_equal(got, expected, check_names=False)


def test_output_does_not_include_forbidden_label_fields() -> None:
    panel = _make_panel()
    panel["fwd_ret_5d"] = 0.0
    panel["上榜后1日"] = 0.0
    out = build_technical_liquidity_factors(panel)
    forbidden = {"fwd_ret_5d", "fwd_ret_20d", "解禁后20日涨跌幅", "上榜后1日", "上榜后2日", "上榜后5日", "上榜后10日"}
    assert forbidden.isdisjoint(out.columns)

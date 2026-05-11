from __future__ import annotations

import pandas as pd
import pytest

from qsys.factors.margin_leverage import build_margin_leverage_factors


def _panel(n_dates: int = 40) -> pd.DataFrame:
    dates = pd.date_range("2025-01-01", periods=n_dates, freq="D")
    assets = ["A", "B"]
    idx = pd.MultiIndex.from_product([dates, assets], names=["date", "asset"])
    rows = []
    for di, d in enumerate(dates):
        for ai, a in enumerate(assets):
            rows.append(
                {
                    "date": d,
                    "asset": a,
                    "financing_balance": 1000 + di * 10 + ai * 50,
                    "financing_buy_amount": 100 + di * 2 + ai,
                    "margin_total_balance": 2000 + di * 15 + ai * 40,
                    "financing_repay_amount": 50 + di + ai,
                    "short_sell_volume": 30 + di + ai,
                    "short_balance": 400 + di * 3 + ai,
                    "margin_eligibility": 1,
                }
            )
    return pd.DataFrame(rows).set_index(["date", "asset"])


def test_output_index_equals_input_index() -> None:
    p = _panel()
    out = build_margin_leverage_factors(p)
    assert out.index.equals(p.index)


def test_expected_columns_present() -> None:
    p = _panel()
    out = build_margin_leverage_factors(p)
    expected = {
        "financing_balance", "margin_total_balance", "financing_balance_chg_5d", "financing_balance_chg_20d",
        "margin_total_balance_chg_5d", "margin_total_balance_chg_20d", "financing_buy_mean_5d", "financing_buy_mean_20d",
        "financing_buy_shock_5d_vs_20d",
    }
    assert expected.issubset(out.columns)


def test_missing_required_columns_raises() -> None:
    p = _panel().drop(columns=["financing_buy_amount"])
    with pytest.raises(ValueError):
        build_margin_leverage_factors(p)


def test_financing_balance_chg_5d_definition() -> None:
    p = _panel()
    out = build_margin_leverage_factors(p)
    g = p.xs("A", level="asset")
    expected = g["financing_balance"] / g["financing_balance"].shift(5) - 1.0
    got = out.xs("A", level="asset")["financing_balance_chg_5d"]
    pd.testing.assert_series_equal(got, expected, check_names=False)


def test_financing_buy_mean_5d_definition() -> None:
    p = _panel()
    out = build_margin_leverage_factors(p)
    expected = p["financing_buy_amount"].groupby(level="asset").transform(lambda s: s.rolling(5, min_periods=5).mean())
    pd.testing.assert_series_equal(out["financing_buy_mean_5d"], expected, check_names=False)


def test_financing_buy_shock_5d_vs_20d_definition() -> None:
    p = _panel()
    out = build_margin_leverage_factors(p)
    denom = out["financing_buy_mean_20d"].where(out["financing_buy_mean_20d"] > 0)
    expected = out["financing_buy_mean_5d"] / denom - 1.0
    pd.testing.assert_series_equal(out["financing_buy_shock_5d_vs_20d"], expected, check_names=False)


def test_optional_net_buy_factors_presence() -> None:
    p = _panel()
    out = build_margin_leverage_factors(p)
    assert {"financing_net_buy", "financing_net_buy_mean_5d", "financing_net_buy_mean_20d"}.issubset(out.columns)

    p2 = p.drop(columns=["financing_repay_amount"])
    out2 = build_margin_leverage_factors(p2)
    assert "financing_net_buy" not in out2.columns


def test_optional_short_factors_presence() -> None:
    p = _panel()
    out = build_margin_leverage_factors(p)
    assert {"short_sell_mean_5d", "short_sell_mean_20d", "short_sell_shock_5d_vs_20d", "short_balance_chg_5d", "short_balance_chg_20d"}.issubset(out.columns)

    p2 = p.drop(columns=["short_sell_volume", "short_balance"])
    out2 = build_margin_leverage_factors(p2)
    assert "short_sell_mean_5d" not in out2.columns
    assert "short_balance_chg_5d" not in out2.columns


def test_multiple_assets_computed_independently() -> None:
    p = _panel()
    out = build_margin_leverage_factors(p)
    for asset in ["A", "B"]:
        g = p.xs(asset, level="asset")
        expected = g["financing_balance"] / g["financing_balance"].shift(5) - 1.0
        got = out.xs(asset, level="asset")["financing_balance_chg_5d"]
        pd.testing.assert_series_equal(got, expected, check_names=False)


def test_no_label_or_post_event_fields_in_output() -> None:
    p = _panel()
    p["fwd_ret_5d"] = 0.0
    out = build_margin_leverage_factors(p)
    forbidden = {"fwd_ret_5d", "fwd_ret_20d", "解禁后20日涨跌幅", "上榜后1日", "上榜后2日", "上榜后5日", "上榜后10日"}
    assert forbidden.isdisjoint(out.columns)

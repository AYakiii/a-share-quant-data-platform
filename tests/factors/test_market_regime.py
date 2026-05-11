from __future__ import annotations

import pandas as pd

from qsys.factors.market_regime import build_market_regime_factors


def _date_panel(n: int = 140, with_valuation: bool = True) -> pd.DataFrame:
    d = pd.date_range("2024-01-01", periods=n, freq="D")
    df = pd.DataFrame(index=d)
    df["close"] = [3000 + i * 3 for i in range(n)]
    df["high"] = df["close"] + 20
    df["amount"] = [1_000_000 + i * 1000 for i in range(n)]
    if with_valuation:
        df["valuation_pe_ttm"] = [12 + (i % 20) * 0.1 for i in range(n)]
    return df


def _multi_panel(n: int = 140) -> pd.DataFrame:
    d = pd.date_range("2024-01-01", periods=n, freq="D")
    idx = pd.MultiIndex.from_product([d, ["CSI300", "CSI500"]], names=["date", "index"])
    rows = []
    for i, dt in enumerate(d):
        rows.append({"date": dt, "index": "CSI300", "close": 3000 + i * 2, "high": 3010 + i * 2, "amount": 1_000_000 + i * 1000, "valuation_pe_ttm": 12 + (i % 10) * 0.1})
        rows.append({"date": dt, "index": "CSI500", "close": 5000 + i * 3, "high": 5020 + i * 3, "amount": 2_000_000 + i * 1200, "valuation_pe_ttm": 20 + (i % 10) * 0.2})
    return pd.DataFrame(rows).set_index(["date", "index"]).reindex(idx)


def test_dateindex_input_works() -> None:
    out = build_market_regime_factors(_date_panel())
    assert not out.empty


def test_multiindex_input_works() -> None:
    out = build_market_regime_factors(_multi_panel(), index_level="index")
    assert not out.empty


def test_output_index_equals_input_index() -> None:
    p = _multi_panel()
    out = build_market_regime_factors(p, index_level="index")
    assert out.index.equals(p.index)


def test_expected_columns_present() -> None:
    out = build_market_regime_factors(_date_panel())
    expected = {"index_ret_5d", "index_ret_20d", "index_ret_60d", "index_momentum_20d", "index_momentum_60d", "index_realized_vol_20d", "index_max_drawdown_20d", "index_close_to_high_20d", "index_amount_mean_20d", "index_amount_shock_5d_vs_20d", "index_pe_ttm", "index_pe_ttm_z_60d", "index_pe_ttm_pct_rank_60d"}
    assert expected.issubset(out.columns)


def test_index_ret_5d_formula() -> None:
    p = _date_panel()
    out = build_market_regime_factors(p)
    exp = p["close"] / p["close"].shift(5) - 1.0
    pd.testing.assert_series_equal(out["index_ret_5d"], exp, check_names=False)


def test_realized_vol_20d_values_after_window() -> None:
    out = build_market_regime_factors(_date_panel())
    assert out["index_realized_vol_20d"].iloc[25:].notna().any()


def test_amount_shock_formula_when_amount_exists() -> None:
    out = build_market_regime_factors(_date_panel())
    p = _date_panel()
    m5 = p["amount"].rolling(5, min_periods=5).mean()
    m20 = p["amount"].rolling(20, min_periods=20).mean()
    exp = m5 / m20.where(m20 > 0) - 1.0
    pd.testing.assert_series_equal(out["index_amount_shock_5d_vs_20d"], exp, check_names=False)


def test_valuation_columns_conditional() -> None:
    out = build_market_regime_factors(_date_panel(with_valuation=False))
    assert "index_pe_ttm" not in out.columns
    assert "index_pe_ttm_z_60d" not in out.columns
    assert "index_pe_ttm_pct_rank_60d" not in out.columns


def test_multiple_indices_independent() -> None:
    p = _multi_panel()
    out = build_market_regime_factors(p, index_level="index")
    for k in ["CSI300", "CSI500"]:
        s = p.xs(k, level="index")["close"]
        exp = s / s.shift(5) - 1.0
        got = out.xs(k, level="index")["index_ret_5d"]
        pd.testing.assert_series_equal(got, exp, check_names=False)


def test_no_label_or_signal_columns() -> None:
    out = build_market_regime_factors(_date_panel())
    forbidden = {"fwd_ret_5d", "fwd_ret_20d", "signal", "position"}
    assert forbidden.isdisjoint(out.columns)

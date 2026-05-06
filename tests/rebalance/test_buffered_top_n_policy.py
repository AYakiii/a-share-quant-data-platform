from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

from qsys.rebalance.policies import BufferedTopNPolicyConfig, build_buffered_top_n_weights


def _mk_signal(rows: list[tuple[str, float, int, bool]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["asset", "score", "rank", "is_tradable"]).set_index("asset")


def test_initial_portfolio_buys_top_target_n_names() -> None:
    sig = _mk_signal([
        ("A", 10.0, 1, True),
        ("B", 9.0, 2, True),
        ("C", 8.0, 3, True),
        ("D", 7.0, 4, True),
    ])
    prev = pd.Series(dtype=float)
    cfg = BufferedTopNPolicyConfig(target_n=3, buy_rank=3, sell_rank=5, min_holding_n=2, max_holding_n=5)

    w, _ = build_buffered_top_n_weights(sig, prev, cfg)

    assert list(w.index) == ["A", "B", "C"]
    assert float(w.sum()) == pytest.approx(1.0)


def test_held_asset_in_buffer_zone_is_kept() -> None:
    sig = _mk_signal([("A", 5.0, 8, True), ("B", 4.0, 2, True)])
    prev = pd.Series({"A": 1.0})
    cfg = BufferedTopNPolicyConfig(target_n=1, buy_rank=5, sell_rank=10, min_holding_n=1, max_holding_n=3, min_trade_weight=0.0)

    w, log = build_buffered_top_n_weights(sig, prev, cfg)

    assert "A" in w.index
    assert log.loc[log["asset"] == "A", "reason"].iloc[0] == "in_buffer_zone"


def test_held_asset_rank_above_sell_rank_sold_if_tradable() -> None:
    sig = _mk_signal([("A", 5.0, 12, True), ("B", 6.0, 1, True)])
    prev = pd.Series({"A": 1.0})
    cfg = BufferedTopNPolicyConfig(target_n=1, buy_rank=5, sell_rank=10, min_holding_n=1, max_holding_n=3, min_trade_weight=0.0)

    w, log = build_buffered_top_n_weights(sig, prev, cfg)

    assert "A" not in w.index
    assert log.loc[log["asset"] == "A", "action"].iloc[0] == "sell"


def test_held_asset_rank_above_sell_rank_not_tradable_kept() -> None:
    sig = _mk_signal([("A", 5.0, 12, False)])
    prev = pd.Series({"A": 1.0})
    cfg = BufferedTopNPolicyConfig(target_n=1, buy_rank=5, sell_rank=10, min_holding_n=1, max_holding_n=3)

    w, log = build_buffered_top_n_weights(sig, prev, cfg)

    assert "A" in w.index
    assert log.loc[log["asset"] == "A", "reason"].iloc[0] in {"not_tradable", "min_trade_filter"}


def test_missing_signal_held_asset_kept() -> None:
    sig = _mk_signal([("B", 6.0, 1, True)])
    prev = pd.Series({"A": 1.0})
    cfg = BufferedTopNPolicyConfig(target_n=1, buy_rank=5, sell_rank=10, min_holding_n=1, max_holding_n=3, min_trade_weight=0.0)

    w, log = build_buffered_top_n_weights(sig, prev, cfg)
    assert "A" in w.index
    assert log.loc[log["asset"] == "A", "reason"].iloc[0] == "missing_signal"


def test_no_forced_buy_when_kept_holdings_ge_min_holding_n() -> None:
    sig = _mk_signal([("A", 9.0, 8, True), ("B", 8.0, 9, True), ("C", 7.0, 1, True)])
    prev = pd.Series({"A": 0.5, "B": 0.5})
    cfg = BufferedTopNPolicyConfig(target_n=3, buy_rank=2, sell_rank=10, min_holding_n=2, max_holding_n=5, min_trade_weight=0.0)

    w, _ = build_buffered_top_n_weights(sig, prev, cfg)
    assert set(w.index) == {"A", "B"}


def test_force_buy_when_kept_holdings_less_than_min_holding_n() -> None:
    sig = _mk_signal([("A", 9.0, 12, True), ("B", 8.0, 1, True), ("C", 7.0, 2, True)])
    prev = pd.Series({"A": 1.0})
    cfg = BufferedTopNPolicyConfig(target_n=2, buy_rank=2, sell_rank=10, min_holding_n=1, max_holding_n=5, min_trade_weight=0.0)

    w, _ = build_buffered_top_n_weights(sig, prev, cfg)
    assert set(w.index) == {"B", "C"}


def test_max_holding_n_trims_worst_ranked_held_assets() -> None:
    sig = _mk_signal([("A", 9.0, 1, True), ("B", 8.0, 2, True), ("C", 7.0, 3, True), ("D", 6.0, 4, True)])
    prev = pd.Series({"A": 0.25, "B": 0.25, "C": 0.25, "D": 0.25})
    cfg = BufferedTopNPolicyConfig(target_n=3, buy_rank=5, sell_rank=10, min_holding_n=2, max_holding_n=3, min_trade_weight=0.0)

    w, log = build_buffered_top_n_weights(sig, prev, cfg)
    assert set(w.index) == {"A", "B", "C"}
    assert log.loc[log["asset"] == "D", "reason"].iloc[0] == "above_max_holding_n"


def test_min_trade_weight_prevents_tiny_changes() -> None:
    sig = _mk_signal([("A", 9.0, 1, True), ("B", 8.0, 2, True)])
    prev = pd.Series({"A": 0.5, "B": 0.5})
    cfg = BufferedTopNPolicyConfig(target_n=2, buy_rank=2, sell_rank=5, min_holding_n=2, max_holding_n=3, min_trade_weight=0.01)

    w, log = build_buffered_top_n_weights(sig, prev, cfg)
    assert w["A"] == pytest.approx(0.5)
    assert log.loc[log["asset"] == "A", "reason"].iloc[0] == "min_trade_filter"


def test_trade_log_required_columns_and_actions() -> None:
    sig = _mk_signal([("A", 9.0, 1, True), ("B", 8.0, 20, True), ("C", 7.0, 2, True), ("D", 6.0, 3, True)])
    prev = pd.Series({"A": 0.7, "B": 0.3})
    cfg = BufferedTopNPolicyConfig(target_n=3, buy_rank=3, sell_rank=10, min_holding_n=2, max_holding_n=5, min_trade_weight=0.0)

    _, log = build_buffered_top_n_weights(sig, prev, cfg)
    required_cols = ["asset", "prev_weight", "target_weight", "trade_weight", "action", "reason", "rank", "score", "is_tradable"]
    assert all(c in log.columns for c in required_cols)
    assert set(log["action"]).issubset({"buy", "sell", "keep", "add", "trim"})

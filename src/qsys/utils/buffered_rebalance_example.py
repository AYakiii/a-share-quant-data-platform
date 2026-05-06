"""Synthetic demo for buffered Top-N rebalance module.

This script uses fully synthetic data and does not require external data files.
It demonstrates buffer-zone holding, forced sell, non-tradable handling,
turnover, and transaction costs in the independent buffered backtest.
"""

from __future__ import annotations

import pandas as pd

from qsys.rebalance import BufferedTopNPolicyConfig, run_buffered_topn_backtest


def make_synthetic_signal_df() -> pd.DataFrame:
    """Construct synthetic signal panel with changing ranks and tradability."""

    dates = pd.to_datetime(
        [
            "2024-01-01",
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
            "2024-01-08",
            "2024-01-09",
            "2024-01-10",
        ]
    )
    assets = ["A", "B", "C", "D", "E", "F", "G", "H"]

    # Scores by date (higher is better), intentionally designed so:
    # - C moves from buy zone to buffer zone
    # - A crosses above sell_rank later
    # - H becomes non-tradable on a rebalance week
    score_map: dict[pd.Timestamp, dict[str, float]] = {
        pd.Timestamp("2024-01-01"): {"A": 0.90, "B": 0.80, "C": 0.70, "D": 0.60, "E": 0.50, "F": 0.40, "G": 0.30, "H": 0.20},
        pd.Timestamp("2024-01-02"): {"A": 0.88, "B": 0.79, "C": 0.69, "D": 0.61, "E": 0.52, "F": 0.41, "G": 0.31, "H": 0.21},
        pd.Timestamp("2024-01-03"): {"A": 0.87, "B": 0.78, "C": 0.62, "D": 0.75, "E": 0.58, "F": 0.42, "G": 0.32, "H": 0.22},
        pd.Timestamp("2024-01-04"): {"A": 0.84, "B": 0.77, "C": 0.60, "D": 0.76, "E": 0.59, "F": 0.43, "G": 0.33, "H": 0.23},
        pd.Timestamp("2024-01-05"): {"A": 0.40, "B": 0.85, "C": 0.70, "D": 0.80, "E": 0.68, "F": 0.64, "G": 0.34, "H": 0.24},
        pd.Timestamp("2024-01-08"): {"A": 0.38, "B": 0.83, "C": 0.69, "D": 0.82, "E": 0.67, "F": 0.65, "G": 0.35, "H": 0.66},
        pd.Timestamp("2024-01-09"): {"A": 0.37, "B": 0.82, "C": 0.68, "D": 0.81, "E": 0.66, "F": 0.63, "G": 0.36, "H": 0.72},
        pd.Timestamp("2024-01-10"): {"A": 0.36, "B": 0.81, "C": 0.67, "D": 0.79, "E": 0.65, "F": 0.62, "G": 0.37, "H": 0.73},
    }

    rows: list[dict[str, object]] = []
    for d in dates:
        day_scores = score_map[d]
        day_rank = pd.Series(day_scores).rank(method="first", ascending=False)
        for a in assets:
            tradable = not (a == "H" and d in {pd.Timestamp("2024-01-09"), pd.Timestamp("2024-01-10")})
            rows.append(
                {
                    "date": d,
                    "asset": a,
                    "score": float(day_scores[a]),
                    "rank": int(day_rank.loc[a]),
                    "is_tradable": bool(tradable),
                }
            )

    return pd.DataFrame(rows).set_index(["date", "asset"]).sort_index()


def make_synthetic_returns_df(signal_df: pd.DataFrame) -> pd.DataFrame:
    """Construct deterministic daily returns for all date-assets."""

    idx = signal_df.index
    assets = idx.get_level_values("asset")
    dates = idx.get_level_values("date")

    asset_bias = {
        "A": -0.0010,
        "B": 0.0012,
        "C": 0.0008,
        "D": 0.0015,
        "E": 0.0006,
        "F": 0.0004,
        "G": 0.0002,
        "H": 0.0010,
    }
    day_pattern = {0: -0.0005, 1: 0.0000, 2: 0.0004, 3: -0.0002, 4: 0.0003}

    vals = []
    for d, a in zip(dates, assets):
        vals.append(asset_bias[str(a)] + day_pattern[pd.Timestamp(d).dayofweek])

    return pd.DataFrame({"ret_1d": vals}, index=idx).sort_index()


def main() -> None:
    signal_df = make_synthetic_signal_df()
    returns_df = make_synthetic_returns_df(signal_df)

    config = BufferedTopNPolicyConfig(
        target_n=3,
        buy_rank=3,
        sell_rank=5,
        min_holding_n=2,
        max_holding_n=4,
        rebalance="weekly",
        min_trade_weight=0.001,
        max_single_weight=0.5,
        cost_bps=20.0,
    )

    result = run_buffered_topn_backtest(signal_df, returns_df, config=config, return_col="ret_1d")

    print("=== Summary ===")
    print(result["summary"])
    print("\n=== Daily Returns ===")
    print(result["daily_returns"].head(10))
    print("\n=== Turnover ===")
    print(result["turnover"])
    print("\n=== Trades ===")
    print(result["trades"].head(30))
    print("\n=== Weights ===")
    print(result["weights"].head(30))


if __name__ == "__main__":
    main()

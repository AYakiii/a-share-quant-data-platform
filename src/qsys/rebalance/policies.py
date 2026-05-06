"""Rebalance policy definitions."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class BufferedTopNPolicyConfig:
    """Configuration for buffered top-N rebalance policy."""

    target_n: int = 50
    buy_rank: int = 50
    sell_rank: int = 100
    min_holding_n: int = 45
    max_holding_n: int = 60
    rebalance: str = "weekly"
    min_trade_weight: float = 0.003
    max_single_weight: float = 0.025
    require_tradable_for_buy: bool = True
    require_tradable_for_sell: bool = True
    cost_bps: float = 20.0

    def __post_init__(self) -> None:
        if self.target_n <= 0:
            raise ValueError("target_n must be > 0")
        if self.buy_rank <= 0:
            raise ValueError("buy_rank must be > 0")
        if self.sell_rank < self.buy_rank:
            raise ValueError("sell_rank must be >= buy_rank")
        if self.min_holding_n <= 0:
            raise ValueError("min_holding_n must be > 0")
        if self.max_holding_n < self.min_holding_n:
            raise ValueError("max_holding_n must be >= min_holding_n")
        if not (self.min_holding_n <= self.target_n <= self.max_holding_n):
            raise ValueError("target_n must satisfy min_holding_n <= target_n <= max_holding_n")
        if self.min_trade_weight < 0:
            raise ValueError("min_trade_weight must be >= 0")
        if self.max_single_weight <= 0:
            raise ValueError("max_single_weight must be > 0")
        if self.rebalance not in {"daily", "weekly", "monthly"}:
            raise ValueError("rebalance must be one of {'daily', 'weekly', 'monthly'}")
        if self.cost_bps < 0:
            raise ValueError("cost_bps must be >= 0")


def _normalize_with_cap(assets: list[object], cap: float) -> pd.Series:
    if not assets:
        return pd.Series(dtype=float, name="target_weight")

    w = pd.Series(1.0 / len(assets), index=pd.Index(assets), dtype=float)
    w = w.clip(upper=float(cap))
    s = float(w[w > 0].sum())
    if s > 0:
        w = w / s
    w.name = "target_weight"
    return w.sort_index()


def build_buffered_top_n_weights(
    today_signal: pd.DataFrame,
    prev_weights: pd.Series,
    config: BufferedTopNPolicyConfig,
) -> tuple[pd.Series, pd.DataFrame]:
    """Build buffered top-N target weights and a per-asset trade log.

    Parameters
    ----------
    today_signal:
        DataFrame indexed by asset with required columns: score, rank, is_tradable.
    prev_weights:
        Series indexed by asset of previous executed target weights.
    config:
        BufferedTopNPolicyConfig controlling buy/sell/holding behavior.
    """

    required = {"score", "rank", "is_tradable"}
    missing = required.difference(today_signal.columns)
    if missing:
        raise ValueError(f"today_signal missing required columns: {sorted(missing)}")

    sig = today_signal.copy()
    sig = sig.sort_values(["rank", "score"], ascending=[True, False], kind="mergesort")

    prev = prev_weights.copy().astype(float)
    held_assets = list(prev[prev > 0].index)
    held_set = set(held_assets)

    kept_assets: list[object] = []
    reasons: dict[object, str] = {}
    sold_assets: set[object] = set()

    for a in held_assets:
        if a not in sig.index:
            kept_assets.append(a)
            reasons[a] = "missing_signal"
            continue

        row = sig.loc[a]
        rank = float(row["rank"])
        tradable = bool(row["is_tradable"])

        if rank > config.sell_rank:
            if config.require_tradable_for_sell and not tradable:
                kept_assets.append(a)
                reasons[a] = "not_tradable"
            else:
                sold_assets.add(a)
                reasons[a] = "sell_rank_exceeded"
            continue

        kept_assets.append(a)
        if rank <= config.buy_rank:
            reasons[a] = "still_in_buy_zone"
        else:
            reasons[a] = "in_buffer_zone"

    keep_set = set(kept_assets)

    buy_pool = sig[~sig.index.isin(keep_set)]
    buy_pool = buy_pool[buy_pool["rank"] <= config.buy_rank]
    if config.require_tradable_for_buy:
        buy_pool = buy_pool[buy_pool["is_tradable"].astype(bool)]

    buy_candidates = list(buy_pool.index)

    selected_assets = list(kept_assets)
    if len(held_assets) == 0:
        selected_assets.extend(buy_candidates[: config.target_n])
    elif len(kept_assets) < config.min_holding_n:
        need = max(config.target_n - len(kept_assets), 0)
        selected_assets.extend(buy_candidates[:need])

    selected_assets = list(dict.fromkeys(selected_assets))

    if len(selected_assets) > config.max_holding_n:
        selected_frame = sig.reindex(selected_assets)
        selected_frame = selected_frame.assign(_asset=pd.Index(selected_assets))
        selected_frame["_is_held"] = selected_frame["_asset"].isin(held_set)
        selected_frame = selected_frame.sort_values(["rank", "score", "_asset"], ascending=[True, False, True], kind="mergesort")
        keep_after_trim = list(selected_frame.head(config.max_holding_n)["_asset"])
        trimmed = set(selected_assets).difference(keep_after_trim)
        for a in trimmed:
            reasons[a] = "above_max_holding_n"
            sold_assets.add(a)
        selected_assets = keep_after_trim

    target = _normalize_with_cap(selected_assets, config.max_single_weight)

    all_assets = prev.index.union(target.index)
    aligned_prev = prev.reindex(all_assets).fillna(0.0)
    aligned_target = target.reindex(all_assets).fillna(0.0)

    small_change = (aligned_target - aligned_prev).abs() < float(config.min_trade_weight)
    if small_change.any():
        aligned_target.loc[small_change] = aligned_prev.loc[small_change]
        for a in all_assets[small_change.values]:
            reasons[a] = "min_trade_filter"

    pos_sum = float(aligned_target[aligned_target > 0].sum())
    if pos_sum > 0:
        aligned_target = aligned_target / pos_sum

    target_weights = aligned_target[aligned_target > 0].copy()
    target_weights.name = "target_weight"

    log_assets = prev.index.union(sig.index).union(target_weights.index)
    rows: list[dict[str, object]] = []
    for a in log_assets:
        prev_w = float(prev.reindex([a]).fillna(0.0).iloc[0])
        tgt_w = float(target_weights.reindex([a]).fillna(0.0).iloc[0])
        trade_w = tgt_w - prev_w

        if prev_w == 0 and tgt_w > 0:
            action = "buy"
        elif prev_w > 0 and tgt_w == 0:
            action = "sell"
        elif prev_w > 0 and abs(tgt_w - prev_w) < 1e-12:
            action = "keep"
        elif prev_w > 0 and tgt_w > prev_w:
            action = "add"
        elif prev_w > 0 and 0 < tgt_w < prev_w:
            action = "trim"
        elif prev_w == 0 and tgt_w == 0:
            action = "keep"
        else:
            action = "keep"

        if a in sig.index:
            rank = sig.loc[a, "rank"]
            score = sig.loc[a, "score"]
            tradable = sig.loc[a, "is_tradable"]
        else:
            rank = pd.NA
            score = pd.NA
            tradable = pd.NA

        rows.append(
            {
                "asset": a,
                "prev_weight": prev_w,
                "target_weight": tgt_w,
                "trade_weight": trade_w,
                "action": action,
                "reason": reasons.get(a, ""),
                "rank": rank,
                "score": score,
                "is_tradable": tradable,
            }
        )

    trade_log = pd.DataFrame(rows)
    trade_log = trade_log.sort_values("asset", kind="mergesort").reset_index(drop=True)

    return target_weights.sort_index(), trade_log

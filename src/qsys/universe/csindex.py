"""CSI/Index universe sampling helpers."""

from __future__ import annotations

import numpy as np
import pandas as pd


def to_ak_symbol(code: str) -> str:
    c = str(code).strip().lower()
    if c.startswith(("sh", "sz", "bj")) and len(c) == 8:
        return c
    digits = "".join(ch for ch in c if ch.isdigit())
    if len(digits) != 6:
        raise ValueError(f"Unsupported symbol/code format: {code}")
    if digits.startswith(("60", "68")):
        return f"sh{digits}"
    if digits.startswith(("00", "30")):
        return f"sz{digits}"
    return f"bj{digits}"


def fetch_index_components(index_symbol: str) -> pd.DataFrame:
    import akshare as ak

    return ak.index_stock_cons(symbol=index_symbol)


def normalize_component_codes(df: pd.DataFrame) -> pd.Series:
    for col in ["品种代码", "成分券代码", "代码", "code"]:
        if col in df.columns:
            s = df[col].astype(str).str.strip()
            s = s[s != ""].map(to_ak_symbol)
            return s
    raise ValueError("index component dataframe missing security-code column")


def build_universe_sample(index_list: list[str], n: int, seed: int) -> tuple[list[str], pd.DataFrame]:
    if n <= 0:
        raise ValueError("n must be a positive integer")

    rows: list[pd.DataFrame] = []
    for idx in index_list:
        comp = fetch_index_components(idx).copy()
        codes = normalize_component_codes(comp)
        rows.append(pd.DataFrame({"index_symbol": idx, "symbol": codes.values}))

    if not rows:
        raise ValueError("index_list cannot be empty")

    merged = pd.concat(rows, ignore_index=True).dropna(subset=["symbol"])
    merged["symbol"] = merged["symbol"].astype(str)
    merged = merged.drop_duplicates(subset=["symbol"]).sort_values("symbol").reset_index(drop=True)

    total = len(merged)
    k = min(n, total)
    rng = np.random.default_rng(seed)
    picked_idx = rng.choice(total, size=k, replace=False)
    sampled = merged.iloc[np.sort(picked_idx)].reset_index(drop=True)

    meta = sampled.copy()
    meta.insert(0, "sample_rank", range(1, len(meta) + 1))
    meta["requested_n"] = int(n)
    meta["actual_n"] = int(k)
    meta["seed"] = int(seed)

    return sampled["symbol"].tolist(), meta

"""Factor output validation, summary, and persistence contract."""

from __future__ import annotations

from pathlib import Path
import json
from typing import Any

import numpy as np
import pandas as pd

DEFAULT_FORBIDDEN_FIELDS = [
    "fwd_ret_5d",
    "fwd_ret_20d",
    "解禁后20日涨跌幅",
    "上榜后1日",
    "上榜后2日",
    "上榜后5日",
    "上榜后10日",
]

DEFAULT_RAW_INPUT_COLUMNS = ["open", "high", "low", "close", "volume", "amount", "turnover"]


def validate_factor_output(
    factors: pd.DataFrame,
    date_level: str = "date",
    asset_level: str = "asset",
    forbidden_fields: list[str] | None = None,
    raw_input_columns: list[str] | None = None,
    allow_all_nan_columns: bool = False,
) -> list[str]:
    """Validate factor output contract and return human-readable messages."""

    msgs: list[str] = []
    forbidden = DEFAULT_FORBIDDEN_FIELDS if forbidden_fields is None else forbidden_fields
    raw_cols = DEFAULT_RAW_INPUT_COLUMNS if raw_input_columns is None else raw_input_columns

    if not isinstance(factors, pd.DataFrame):
        return ["factors must be a pandas DataFrame"]

    if not isinstance(factors.index, pd.MultiIndex):
        msgs.append("index must be MultiIndex")
        return msgs

    idx_names = list(factors.index.names)
    if date_level not in idx_names or asset_level not in idx_names:
        msgs.append(f"index must contain levels '{date_level}' and '{asset_level}'")

    if factors.index.has_duplicates:
        msgs.append("index must not contain duplicate entries")

    for c in forbidden:
        if c in factors.columns:
            msgs.append(f"forbidden field present in factor output: {c}")

    for c in raw_cols:
        if c in factors.columns:
            msgs.append(f"raw input column should not appear in factor output: {c}")

    for c in factors.columns:
        s = factors[c]
        if not pd.api.types.is_numeric_dtype(s):
            msgs.append(f"non-numeric factor column: {c}")
            continue

        vals = s.to_numpy(dtype=float, copy=False)
        n_inf = int(np.isinf(vals).sum())
        if n_inf > 0:
            msgs.append(f"factor column contains inf/-inf values: {c} n_inf={n_inf}")

        if not allow_all_nan_columns and s.isna().all():
            msgs.append(f"factor column is all-NaN: {c}")

    return msgs


def summarize_factor_output(factors: pd.DataFrame) -> pd.DataFrame:
    """Summarize numeric factor columns."""

    if not isinstance(factors, pd.DataFrame):
        raise ValueError("factors must be a pandas DataFrame")

    rows: list[dict[str, float | str | int]] = []
    for c in factors.columns:
        s = factors[c]
        if not pd.api.types.is_numeric_dtype(s):
            continue
        vals = pd.to_numeric(s, errors="coerce")
        n_total = int(len(vals))
        n_non_null = int(vals.notna().sum())
        coverage = float(n_non_null / n_total) if n_total else float("nan")
        n_inf = int(np.isinf(vals.to_numpy(dtype=float, copy=False)).sum())
        finite = vals.replace([np.inf, -np.inf], np.nan)
        rows.append(
            {
                "factor_name": c,
                "n_total": n_total,
                "n_non_null": n_non_null,
                "coverage": coverage,
                "n_inf": n_inf,
                "mean": float(finite.mean()) if n_non_null else float("nan"),
                "std": float(finite.std(ddof=1)) if n_non_null > 1 else float("nan"),
                "min": float(finite.min()) if n_non_null else float("nan"),
                "max": float(finite.max()) if n_non_null else float("nan"),
            }
        )
    return pd.DataFrame(rows)


def write_factor_output(
    factors: pd.DataFrame,
    output_root: str | Path,
    dataset_name: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Path]:
    """Write factors, metadata, and summary with deterministic naming."""

    root = Path(output_root) / f"dataset_name={dataset_name}"
    root.mkdir(parents=True, exist_ok=True)

    factors_fp = root / "factors.csv"
    meta_fp = root / "metadata.json"
    summary_fp = root / "summary.csv"

    factors.to_csv(factors_fp, encoding="utf-8")
    summary = summarize_factor_output(factors)
    summary.to_csv(summary_fp, index=False, encoding="utf-8")

    meta: dict[str, Any] = {
        "dataset_name": dataset_name,
        "row_count": int(len(factors)),
        "column_count": int(factors.shape[1]),
        "factor_columns": [str(c) for c in factors.columns],
        "index_names": list(factors.index.names),
    }
    if metadata:
        meta.update(metadata)

    meta_fp.write_text(json.dumps(meta, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    return {"factors": factors_fp, "metadata": meta_fp, "summary": summary_fp}

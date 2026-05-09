"""BaoStock-based index member snapshot helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

STANDARD_COLUMNS = [
    "index_name",
    "index_code",
    "snapshot_date",
    "asset",
    "asset_name",
    "is_member",
    "source",
    "ingested_at",
]


def normalize_baostock_code(code: str) -> str:
    """Normalize BaoStock code like ``sh.600000`` to ``600000.SH``."""

    c = str(code).strip().lower()
    if c.startswith("sh.") and len(c) == 9 and c[3:].isdigit():
        return f"{c[3:]}.SH"
    if c.startswith("sz.") and len(c) == 9 and c[3:].isdigit():
        return f"{c[3:]}.SZ"
    if c.startswith("bj.") and len(c) == 9 and c[3:].isdigit():
        return f"{c[3:]}.BJ"
    raise ValueError(f"Unsupported BaoStock code format: {code}")


def baostock_result_to_dataframe(rs: Any) -> pd.DataFrame:
    """Convert BaoStock ResultData to DataFrame with robust fallbacks."""

    if str(getattr(rs, "error_code", "0")) != "0":
        raise RuntimeError(f"BaoStock query failed: error_code={rs.error_code}, error_msg={getattr(rs, 'error_msg', '')}")

    if hasattr(rs, "get_data"):
        df = rs.get_data()
        if isinstance(df, pd.DataFrame):
            return df

    fields = list(getattr(rs, "fields", []))
    rows: list[list[str]] = []
    while rs.next():
        rows.append(rs.get_row_data())
    return pd.DataFrame(rows, columns=fields if fields else None)


def fetch_csi500_members(date: str) -> pd.DataFrame:
    """Fetch CSI500 members snapshot from BaoStock for a given date."""

    import baostock as bs

    rs = bs.query_zz500_stocks(date=date)
    raw = baostock_result_to_dataframe(rs)

    if raw.empty:
        return pd.DataFrame(columns=STANDARD_COLUMNS)

    if "code" not in raw.columns:
        raise ValueError("BaoStock result missing 'code' column")

    asset_name = raw["code_name"] if "code_name" in raw.columns else pd.Series([pd.NA] * len(raw))

    out = pd.DataFrame(
        {
            "index_name": "csi500",
            "index_code": "000905.SH",
            "snapshot_date": pd.to_datetime(date),
            "asset": raw["code"].map(normalize_baostock_code),
            "asset_name": asset_name,
            "is_member": 1,
            "source": "baostock",
            "ingested_at": datetime.now(timezone.utc),
        }
    )
    return out[STANDARD_COLUMNS].copy()

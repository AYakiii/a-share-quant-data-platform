"""BaoStock CSI500 constituent snapshot helpers."""

from __future__ import annotations

from datetime import datetime, timezone

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
    """Normalize BaoStock code (e.g. sh.600000) to internal format (600000.SH)."""

    c = str(code).strip().lower()
    if c.startswith("sh.") and len(c) == 9 and c[3:].isdigit():
        return f"{c[3:]}.SH"
    if c.startswith("sz.") and len(c) == 9 and c[3:].isdigit():
        return f"{c[3:]}.SZ"
    if c.startswith("bj.") and len(c) == 9 and c[3:].isdigit():
        return f"{c[3:]}.BJ"
    raise ValueError(f"Unknown BaoStock code format: {code}")


def baostock_result_to_dataframe(rs) -> pd.DataFrame:
    """Convert BaoStock ResultData into a pandas DataFrame."""

    err_code = str(getattr(rs, "error_code", "0"))
    if err_code != "0":
        err_msg = str(getattr(rs, "error_msg", ""))
        raise RuntimeError(f"BaoStock query failed: error_code={err_code}, error_msg={err_msg}")

    if hasattr(rs, "get_data"):
        df = rs.get_data()
        if isinstance(df, pd.DataFrame):
            return df
        return pd.DataFrame(df)

    fields = list(getattr(rs, "fields", []))
    rows: list[list[str]] = []
    while True:
        ok = rs.next()
        if not ok:
            break
        rows.append(list(rs.get_row_data()))
    return pd.DataFrame(rows, columns=fields if fields else None)


def fetch_csi500_members(date: str) -> pd.DataFrame:
    """Fetch one CSI500 snapshot for a given date using BaoStock query_zz500_stocks."""

    import baostock as bs

    rs = bs.query_zz500_stocks(date=date)
    raw = baostock_result_to_dataframe(rs)
    if raw.empty:
        return pd.DataFrame(columns=STANDARD_COLUMNS)

    if "code" not in raw.columns:
        raise ValueError("BaoStock query_zz500_stocks result missing 'code' column")

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
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    return out[STANDARD_COLUMNS]

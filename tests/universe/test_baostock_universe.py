from __future__ import annotations

import types

import pandas as pd
import pytest

from qsys.universe.baostock import (
    fetch_csi500_members,
    normalize_baostock_code,
)
from qsys.universe.index_members import load_index_member_snapshots, load_index_members_asof


def test_normalize_baostock_code() -> None:
    assert normalize_baostock_code("sh.600000") == "600000.SH"
    assert normalize_baostock_code("sz.000001") == "000001.SZ"
    assert normalize_baostock_code("sh.688001") == "688001.SH"
    assert normalize_baostock_code("sz.300001") == "300001.SZ"
    with pytest.raises(ValueError):
        normalize_baostock_code("600000")


def test_fetch_csi500_members_with_mock(monkeypatch) -> None:
    class _MockRS:
        error_code = "0"
        error_msg = ""

        def get_data(self):
            return pd.DataFrame(
                {
                    "code": ["sh.600000", "sz.000001"],
                    "code_name": ["浦发银行", "平安银行"],
                }
            )

    fake_bs = types.SimpleNamespace(query_zz500_stocks=lambda date: _MockRS())
    monkeypatch.setitem(__import__("sys").modules, "baostock", fake_bs)

    out = fetch_csi500_members("2025-01-31")
    assert list(out.columns) == [
        "index_name", "index_code", "snapshot_date", "asset", "asset_name", "is_member", "source", "ingested_at"
    ]
    assert out["asset"].tolist() == ["600000.SH", "000001.SZ"]
    assert (out["index_name"] == "csi500").all()
    assert (out["index_code"] == "000905.SH").all()
    assert (out["is_member"] == 1).all()
    assert (out["source"] == "baostock").all()


def test_load_index_members_asof_no_lookahead(tmp_path) -> None:
    root = tmp_path / "raw" / "index_constituents" / "baostock" / "index_name=csi500"
    d1 = pd.Timestamp("2025-01-31")
    d2 = pd.Timestamp("2025-02-28")
    d3 = pd.Timestamp("2025-03-31")
    cols = ["index_name", "index_code", "snapshot_date", "asset", "asset_name", "is_member", "source", "ingested_at"]

    for d, asset in [(d1, "000001.SZ"), (d2, "600000.SH"), (d3, "300750.SZ")]:
        part = root / f"year={d.year}"
        part.mkdir(parents=True, exist_ok=True)
        fp = part / "data.parquet"
        old = pd.read_parquet(fp) if fp.exists() else pd.DataFrame(columns=cols)
        row = pd.DataFrame([["csi500", "000905.SH", d, asset, "x", 1, "baostock", "now"]], columns=cols)
        pd.concat([old, row], ignore_index=True).to_parquet(fp, index=False)

    out = load_index_members_asof(tmp_path / "raw" / "index_constituents" / "baostock", as_of_date="2025-03-15")
    assert (out["snapshot_date"] == pd.Timestamp("2025-02-28")).all()
    assert "300750.SZ" not in out["asset"].tolist()


def test_load_index_member_snapshots_missing_path(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        load_index_member_snapshots(tmp_path / "not_exists")

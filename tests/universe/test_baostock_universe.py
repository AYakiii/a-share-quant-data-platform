from __future__ import annotations

import types

import pandas as pd
import pytest

from qsys.universe.baostock import fetch_csi500_members, normalize_baostock_code
from qsys.universe.index_members import load_index_member_snapshots, load_index_members_asof


def test_normalize_baostock_code() -> None:
    assert normalize_baostock_code(" sh.600000 ") == "600000.SH"
    assert normalize_baostock_code("sz.000001") == "000001.SZ"
    assert normalize_baostock_code("sh.688001") == "688001.SH"
    assert normalize_baostock_code("sz.300001") == "300001.SZ"
    with pytest.raises(ValueError):
        normalize_baostock_code("000001")


class _MockRS:
    error_code = "0"
    error_msg = ""

    def get_data(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "code": ["sh.600000", "sz.000001"],
                "code_name": ["浦发银行", "平安银行"],
            }
        )


def test_fetch_csi500_members_with_mock(monkeypatch) -> None:
    mock_bs = types.SimpleNamespace(query_zz500_stocks=lambda date: _MockRS())
    monkeypatch.setitem(__import__("sys").modules, "baostock", mock_bs)

    df = fetch_csi500_members("2025-01-31")
    expected_cols = {
        "index_name", "index_code", "snapshot_date", "asset", "asset_name", "is_member", "source", "ingested_at"
    }
    assert expected_cols.issubset(df.columns)
    assert df["asset"].tolist() == ["600000.SH", "000001.SZ"]
    assert set(df["index_name"]) == {"csi500"}
    assert set(df["index_code"]) == {"000905.SH"}
    assert set(df["is_member"]) == {1}
    assert set(df["source"]) == {"baostock"}


def test_load_index_members_asof_no_lookahead(tmp_path) -> None:
    base = tmp_path / "index_constituents" / "baostock" / "index_name=csi500"
    rows = []
    for d in ["2025-01-31", "2025-02-28", "2025-03-31"]:
        rows.append(
            pd.DataFrame(
                {
                    "index_name": ["csi500"],
                    "index_code": ["000905.SH"],
                    "snapshot_date": [pd.Timestamp(d)],
                    "asset": ["600000.SH"],
                    "asset_name": ["x"],
                    "is_member": [1],
                    "source": ["baostock"],
                    "ingested_at": [pd.Timestamp("2025-04-01", tz="UTC")],
                }
            )
        )
    full = pd.concat(rows, ignore_index=True)
    for year, g in full.groupby(full["snapshot_date"].dt.year):
        part = base / f"year={year}"
        part.mkdir(parents=True, exist_ok=True)
        g.to_parquet(part / "data.parquet", index=False)

    out = load_index_members_asof(root=tmp_path / "index_constituents" / "baostock", as_of_date="2025-03-15")
    assert out["snapshot_date"].nunique() == 1
    assert out["snapshot_date"].iloc[0] == pd.Timestamp("2025-02-28")


def test_load_index_member_snapshots_missing_path(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        load_index_member_snapshots(root=tmp_path / "missing")

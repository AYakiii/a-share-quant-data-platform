from __future__ import annotations

import types

import pandas as pd
import pytest

from qsys.universe.baostock import fetch_csi500_members, normalize_baostock_code
from qsys.universe.index_members import apply_pit_index_universe_mask, load_index_member_snapshots, load_index_members_asof


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


def test_apply_pit_index_universe_mask_filters_by_asof_snapshot(tmp_path) -> None:
    base = tmp_path / "index_constituents" / "baostock" / "index_name=csi500" / "year=2024"
    base.mkdir(parents=True, exist_ok=True)
    snaps = pd.DataFrame(
        {
            "index_name": ["csi500", "csi500", "csi500", "csi500"],
            "index_code": ["000905.SH"] * 4,
            "snapshot_date": pd.to_datetime(["2024-01-31", "2024-01-31", "2024-02-29", "2024-02-29"]),
            "asset": ["A", "B", "B", "C"],
            "asset_name": ["a", "b", "b", "c"],
            "is_member": [1, 1, 1, 1],
            "source": ["baostock"] * 4,
            "ingested_at": [pd.Timestamp("2024-03-01", tz="UTC")] * 4,
        }
    )
    snaps.to_parquet(base / "data.parquet", index=False)

    idx = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-02-15"), "A"),
            (pd.Timestamp("2024-02-15"), "B"),
            (pd.Timestamp("2024-02-15"), "C"),
            (pd.Timestamp("2024-03-05"), "A"),
            (pd.Timestamp("2024-03-05"), "B"),
            (pd.Timestamp("2024-03-05"), "C"),
        ],
        names=["date", "asset"],
    )
    features = pd.DataFrame({"x": [1, 2, 3, 4, 5, 6]}, index=idx)

    out = apply_pit_index_universe_mask(features, universe_root=tmp_path / "index_constituents" / "baostock")
    kept = set(out.index.tolist())
    assert (pd.Timestamp("2024-02-15"), "C") not in kept
    assert (pd.Timestamp("2024-03-05"), "A") not in kept
    assert (pd.Timestamp("2024-03-05"), "C") in kept

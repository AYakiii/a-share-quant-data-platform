from __future__ import annotations

import json

import pandas as pd

from qsys.data.factor_lake.metastore import FactorLakeMetastore
from qsys.data.factor_lake.akshare_raw_ingest import run_raw_coverage_ingest, run_raw_ingest


class _Result:
    def __init__(self, raw: pd.DataFrame):
        self.raw = raw


def _frame(**values: str) -> _Result:
    return _Result(pd.DataFrame({k: [v] for k, v in values.items()} or {"ok": [1]}))


def test_repaired_trading_attention_date_partitions_do_not_collide(tmp_path):
    calls: list[tuple[str, dict[str, str]]] = []

    def stock_dzjy_mrtj(start_date: str, end_date: str) -> _Result:
        calls.append(("stock_dzjy_mrtj", {"start_date": start_date, "end_date": end_date}))
        return _frame(start_date=start_date, end_date=end_date)

    def stock_dzjy_mrmx(start_date: str, end_date: str) -> _Result:
        calls.append(("stock_dzjy_mrmx", {"start_date": start_date, "end_date": end_date}))
        return _frame(start_date=start_date, end_date=end_date)

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["trading_attention"],
        trade_dates=["20220104", "20220105"],
        selected_api_names=["stock_dzjy_mrtj", "stock_dzjy_mrmx"],
        adapter_map={"stock_dzjy_mrtj": stock_dzjy_mrtj, "stock_dzjy_mrmx": stock_dzjy_mrmx},
        max_workers=1,
    )

    rows = out["rows"]
    assert len(rows) == 4
    assert {row["status"] for row in rows} == {"success"}
    assert {row["error_type"] for row in rows} == {""}
    assert all("trade_date=" in row["output_path"] for row in rows)
    assert {json.loads(row["partition_json"])["trade_date"] for row in rows} == {"20220104", "20220105"}
    assert calls == [
        ("stock_dzjy_mrtj", {"start_date": "20220104", "end_date": "20220104"}),
        ("stock_dzjy_mrtj", {"start_date": "20220105", "end_date": "20220105"}),
        ("stock_dzjy_mrmx", {"start_date": "20220104", "end_date": "20220104"}),
        ("stock_dzjy_mrmx", {"start_date": "20220105", "end_date": "20220105"}),
    ]


def test_snapshot_style_affected_apis_run_once_with_snapshot_partition(tmp_path):
    call_counts = {"stock_history_dividend": 0, "stock_dzjy_sctj": 0, "stock_dzjy_hyyybtj": 0}

    def stock_history_dividend() -> _Result:
        call_counts["stock_history_dividend"] += 1
        return _frame(api="stock_history_dividend")

    def stock_dzjy_sctj() -> _Result:
        call_counts["stock_dzjy_sctj"] += 1
        return _frame(api="stock_dzjy_sctj")

    def stock_dzjy_hyyybtj() -> _Result:
        call_counts["stock_dzjy_hyyybtj"] += 1
        return _frame(api="stock_dzjy_hyyybtj")

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["corporate_action", "trading_attention"],
        symbols=["000001", "000002"],
        trade_dates=["20220104", "20220105"],
        selected_api_names=["stock_history_dividend", "stock_dzjy_sctj", "stock_dzjy_hyyybtj"],
        adapter_map={
            "stock_history_dividend": stock_history_dividend,
            "stock_dzjy_sctj": stock_dzjy_sctj,
            "stock_dzjy_hyyybtj": stock_dzjy_hyyybtj,
        },
        max_workers=1,
    )

    rows = out["rows"]
    assert len(rows) == 3
    assert call_counts == {"stock_history_dividend": 1, "stock_dzjy_sctj": 1, "stock_dzjy_hyyybtj": 1}
    assert {row["status"] for row in rows} == {"success"}
    assert all(json.loads(row["partition_json"]) == {"snapshot": "latest"} for row in rows)
    assert all("snapshot=latest" in row["output_path"] for row in rows)
    assert not any("symbol=" in row["output_path"] or "trade_date=" in row["output_path"] for row in rows)


def test_existing_p0_daily_bar_raw_partition_behavior_unchanged(tmp_path):
    def stock_zh_a_hist(symbol: str, start_date: str, end_date: str) -> _Result:
        return _frame(symbol=symbol, start_date=start_date, end_date=end_date)

    metastore = FactorLakeMetastore(tmp_path / "meta.sqlite")
    result = run_raw_ingest(
        "daily_bar_raw",
        str(tmp_path),
        metastore,
        adapter_map={"stock_zh_a_hist": stock_zh_a_hist},
        daily_api_preference="stock_zh_a_hist",
        symbol="000001",
        year="2024",
    )

    assert result["status"] == "success"
    record = result["records"][0]
    assert record["partition"] == {"symbol": "000001", "year": "2024"}
    assert "market_price/stock_zh_a_hist/symbol=000001/year=2024/data.parquet" in record["data_path"]


def test_adapter_file_exists_error_is_not_converted_to_already_exists(tmp_path):
    def stock_dzjy_mrtj(start_date: str, end_date: str) -> _Result:
        raise FileExistsError("synthetic adapter failure")

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["trading_attention"],
        trade_dates=["20220104"],
        selected_api_names=["stock_dzjy_mrtj"],
        adapter_map={"stock_dzjy_mrtj": stock_dzjy_mrtj},
        max_workers=1,
    )

    [row] = out["rows"]
    assert row["status"] == "failed"
    assert row["error_type"] == "FileExistsError"
    assert "synthetic adapter failure" in row["error_message"]

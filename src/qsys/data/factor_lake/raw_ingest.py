from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

import pandas as pd

from qsys.data.factor_lake.io import write_raw_partition
from qsys.data.factor_lake.metastore import FactorLakeMetastore
from qsys.data.factor_lake.registry import DATASET_REGISTRY, get_dataset_spec, plan_partitions
from qsys.data.sources.akshare_index import fetch_stock_zh_index_hist_csindex
from qsys.data.sources.akshare_margin import fetch_stock_margin_detail_sse, fetch_stock_margin_detail_szse
from qsys.data.sources.akshare_market import fetch_stock_zh_a_daily, fetch_stock_zh_a_hist

AdapterFn = Callable[..., object]


@dataclass
class IngestRecord:
    dataset: str
    api_name: str
    partition: dict[str, str]
    status: str
    data_path: str | None = None
    metadata_path: str | None = None
    error_message: str = ""


DEFAULT_ADAPTERS: dict[str, AdapterFn] = {
    "stock_zh_a_hist": fetch_stock_zh_a_hist,
    "stock_zh_a_daily": fetch_stock_zh_a_daily,
    "stock_zh_index_hist_csindex": fetch_stock_zh_index_hist_csindex,
    "stock_margin_detail_sse": fetch_stock_margin_detail_sse,
    "stock_margin_detail_szse": fetch_stock_margin_detail_szse,
}


def _select_api(dataset: str, partition: dict[str, str]) -> str:
    if dataset != "margin_detail_raw":
        return DATASET_REGISTRY[dataset].api_names[0]
    return "stock_margin_detail_sse" if partition["exchange"].lower() == "sse" else "stock_margin_detail_szse"


def _build_adapter_kwargs(dataset: str, partition: dict[str, str]) -> dict[str, str]:
    if dataset == "daily_bar_raw":
        symbol = partition["symbol"]
        if "year" in partition:
            y = partition["year"]
            return {"symbol": symbol, "start_date": f"{y}0101", "end_date": f"{y}1231"}
        return {"symbol": symbol, "start_date": partition["start_date"].replace("-", ""), "end_date": partition["end_date"].replace("-", "")}
    if dataset == "index_bar_raw":
        symbol = partition["index_symbol"]
        if "year" in partition:
            y = partition["year"]
            return {"symbol": symbol, "start_date": f"{y}0101", "end_date": f"{y}1231"}
        return {"symbol": symbol, "start_date": partition["start_date"].replace("-", ""), "end_date": partition["end_date"].replace("-", "")}
    return {"date": partition["trade_date"].replace("-", "")}


def run_raw_ingest(dataset: str, root: str, metastore: FactorLakeMetastore, adapter_map: dict[str, AdapterFn] | None = None, timeout_seconds: float = 30.0, continue_on_error: bool = True, request_sleep: float = 0.0, daily_api_preference: str = "stock_zh_a_daily", **plan_kwargs: str) -> dict:
    get_dataset_spec(dataset)
    run_id = f"ingest_{dataset}_{uuid.uuid4().hex[:8]}"
    started_at = datetime.now(UTC)
    adapters = adapter_map or DEFAULT_ADAPTERS
    partitions = plan_partitions(dataset, **plan_kwargs)
    records: list[IngestRecord] = []

    for partition in partitions:
        api_candidates = [_select_api(dataset, partition)]
        if dataset == "daily_bar_raw":
            if daily_api_preference == "stock_zh_a_daily":
                api_candidates = ["stock_zh_a_daily", "stock_zh_a_hist"]
            elif daily_api_preference == "stock_zh_a_hist":
                api_candidates = ["stock_zh_a_hist", "stock_zh_a_daily"]

        api_name = api_candidates[0]
        t0 = time.perf_counter()
        status = "failed"
        err = ""
        data_path = meta_path = None
        for candidate in api_candidates:
            api_name = candidate
            try:
                adapter = adapters[api_name]
                result = adapter(**_build_adapter_kwargs(dataset, partition))
                raw = result.raw if hasattr(result, "raw") else result
                if not isinstance(raw, pd.DataFrame):
                    raw = pd.DataFrame(raw)
                status = "empty" if raw.empty else "success"
                metadata = {
                    "dataset": dataset,
                    "api_name": api_name,
                    "source_family": DATASET_REGISTRY[dataset].source_family,
                    "partition": partition,
                    "row_count": int(len(raw)),
                    "col_count": int(len(raw.columns)),
                    "ingested_at": datetime.now(UTC).isoformat(),
                    "status": status,
                }
                dp, mp = write_raw_partition(root, DATASET_REGISTRY[dataset].source_family, api_name, partition, raw, metadata)
                data_path, meta_path = str(dp), str(mp)
                metastore.execute(
                    "insert or replace into raw_dataset_inventory(dataset, source_family, api_name, partition_json, data_path, metadata_path, row_count, col_count) values (?, ?, ?, ?, ?, ?, ?, ?)",
                    (dataset, DATASET_REGISTRY[dataset].source_family, api_name, json.dumps(partition, sort_keys=True), data_path, meta_path, len(raw), len(raw.columns)),
                )
                err = ""
                break
            except TimeoutError:
                status = "timed_out"
                err = "timeout"
            except Exception as exc:  # noqa: BLE001
                status = "failed"
                err = str(exc)
                continue

        elapsed = time.perf_counter() - t0
        metastore.execute(
            "insert into ingest_run_log(run_id, dataset, api_name, partition_json, status, error_message, elapsed_seconds, created_at) values (?, ?, ?, ?, ?, ?, ?, ?)",
            (run_id, dataset, api_name, json.dumps(partition, sort_keys=True), status, err, elapsed, datetime.now(UTC).isoformat()),
        )
        records.append(IngestRecord(dataset, api_name, partition, status, data_path, meta_path, err))
        if status == "failed" and not continue_on_error:
            break
        if request_sleep > 0:
            time.sleep(request_sleep)

    ended_at = datetime.now(UTC)
    overall = "success" if all(r.status in {"success", "empty"} for r in records) else "failed"
    metastore.execute(
        "insert or replace into sync_meta(run_id, dataset, started_at, ended_at, status) values (?, ?, ?, ?, ?)",
        (run_id, dataset, started_at.isoformat(), ended_at.isoformat(), overall),
    )
    return {"run_id": run_id, "dataset": dataset, "status": overall, "records": [r.__dict__ for r in records], "timeout_seconds": timeout_seconds}


def run_raw_ingest_mvp(datasets: list[str], root: str, metastore_path: str, symbols: list[str], index_symbols: list[str], trade_dates: list[str], start_date: str, end_date: str, adapter_map: dict[str, AdapterFn] | None = None, continue_on_error: bool = True, request_sleep: float = 0.0, daily_api_preference: str = "stock_zh_a_daily") -> dict:
    metastore = FactorLakeMetastore(metastore_path)
    results: list[dict] = []
    for ds in datasets:
        kwargs: dict[str, object]
        if ds == "daily_bar_raw":
            kwargs = {"symbols": symbols, "start_date": start_date, "end_date": end_date}
        elif ds == "index_bar_raw":
            kwargs = {"index_symbols": index_symbols, "start_date": start_date, "end_date": end_date}
        elif ds == "margin_detail_raw":
            kwargs = {"exchanges": ["sse", "szse"], "trade_dates": trade_dates}
        else:
            continue
        res = run_raw_ingest(ds, root=root, metastore=metastore, adapter_map=adapter_map, continue_on_error=continue_on_error, request_sleep=request_sleep, daily_api_preference=daily_api_preference, **kwargs)
        results.append(res)

    catalog_rows = []
    for r in results:
        for rec in r["records"]:
            rec2 = dict(rec)
            rec2["run_id"] = r["run_id"]
            catalog_rows.append(rec2)
    catalog_df = pd.DataFrame(catalog_rows)
    out = Path(root)
    out.mkdir(parents=True, exist_ok=True)
    catalog_path = out / "raw_ingest_catalog.csv"
    summary_path = out / "raw_ingest_summary.csv"
    catalog_df.to_csv(catalog_path, index=False, encoding="utf-8-sig")
    summary = catalog_df.groupby(["dataset", "status"], as_index=False).size() if not catalog_df.empty else pd.DataFrame(columns=["dataset", "status", "size"])
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    return {"results": results, "catalog_path": str(catalog_path), "summary_path": str(summary_path)}

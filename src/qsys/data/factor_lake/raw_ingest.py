from __future__ import annotations

import csv
import json
import inspect
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


COVERAGE_API_SPECS: dict[str, list[dict[str, str]]] = {
    "market_price": [
        {"api_name": "stock_zh_a_hist", "param_mode": "daily_symbol_range_hist"},
        {"api_name": "stock_individual_info_em", "param_mode": "symbol_only"},
    ],
    "index_market": [
        {"api_name": "stock_zh_index_hist_csindex", "param_mode": "index_symbol_range"},
        {"api_name": "index_stock_cons_csindex", "param_mode": "index_symbol"},
        {"api_name": "index_stock_cons_weight_csindex", "param_mode": "index_symbol"},
    ],
    "margin_leverage": [
        {"api_name": "stock_margin_detail_sse", "param_mode": "trade_date", "exchange": "sse"},
        {"api_name": "stock_margin_detail_szse", "param_mode": "trade_date", "exchange": "szse"},
        {"api_name": "stock_margin_sse", "param_mode": "date_range"},
        {"api_name": "stock_margin_szse", "param_mode": "date_range"},
        {"api_name": "stock_margin_underlying_info_szse", "param_mode": "none"},
    ],
    "financial_fundamental": [
        {"api_name": "stock_financial_analysis_indicator", "param_mode": "symbol_only"},
        {"api_name": "stock_yjyg_em", "param_mode": "report_date"},
        {"api_name": "stock_yysj_em", "param_mode": "report_date"},
    ],
    "industry_concept": [
        {"api_name": "stock_industry_change_cninfo", "param_mode": "symbol_range"},
        {"api_name": "stock_industry_clf_hist_sw", "param_mode": "symbol_range"},
        {"api_name": "stock_industry_category_cninfo", "param_mode": "none"},
        {"api_name": "sw_index_first_info", "param_mode": "none"},
        {"api_name": "sw_index_second_info", "param_mode": "none"},
        {"api_name": "sw_index_third_info", "param_mode": "none"},
        {"api_name": "index_component_sw", "param_mode": "industry_code"},
        {"api_name": "index_hist_sw", "param_mode": "industry_code"},
        {"api_name": "stock_board_industry_index_ths", "param_mode": "industry_name_range"},
        {"api_name": "stock_board_industry_name_ths", "param_mode": "none"},
        {"api_name": "stock_board_industry_info_ths", "param_mode": "industry_name"},
        {"api_name": "stock_board_industry_summary_ths", "param_mode": "none"},
        {"api_name": "stock_board_concept_index_ths", "param_mode": "concept_name_range"},
        {"api_name": "stock_board_concept_name_ths", "param_mode": "none"},
        {"api_name": "stock_board_concept_info_ths", "param_mode": "concept_name"},
        {"api_name": "stock_board_concept_summary_ths", "param_mode": "none"},
        {"api_name": "index_realtime_sw", "param_mode": "none"},
    ],
    "event_ownership": [
        {"api_name": "stock_zh_a_gdhs", "param_mode": "none"},
        {"api_name": "stock_zh_a_gdhs_detail_em", "param_mode": "symbol_only"},
        {"api_name": "stock_gdfx_free_holding_analyse_em", "param_mode": "report_date"},
        {"api_name": "stock_gdfx_holding_analyse_em", "param_mode": "report_date"},
        {"api_name": "stock_gpzy_pledge_ratio_em", "param_mode": "none"},
        {"api_name": "stock_gpzy_pledge_ratio_detail_em", "param_mode": "report_date"},
        {"api_name": "stock_gpzy_industry_data_em", "param_mode": "none"},
        {"api_name": "stock_gpzy_profile_em", "param_mode": "none"},
    ],
    "corporate_action": [
        {"api_name": "stock_fhps_em", "param_mode": "none"},
        {"api_name": "stock_history_dividend", "param_mode": "symbol_only"},
        {"api_name": "stock_history_dividend_detail", "param_mode": "symbol_report_date"},
        {"api_name": "stock_restricted_release_detail_em", "param_mode": "report_date"},
        {"api_name": "stock_restricted_release_queue_em", "param_mode": "none"},
        {"api_name": "stock_restricted_release_summary_em", "param_mode": "none"},
    ],
    "disclosure_ir": [
        {"api_name": "stock_zh_a_disclosure_relation_cninfo", "param_mode": "symbol_range"},
        {"api_name": "stock_jgdy_tj_em", "param_mode": "report_date"},
        {"api_name": "stock_jgdy_detail_em", "param_mode": "report_date"},
    ],
    "trading_attention": [
        {"api_name": "stock_jgdy_tj_em", "param_mode": "report_date"},
        {"api_name": "stock_lhb_detail_em", "param_mode": "date_range"},
        {"api_name": "stock_lhb_jgmmtj_em", "param_mode": "date_range"},
        {"api_name": "stock_lhb_stock_statistic_em", "param_mode": "date_range"},
        {"api_name": "stock_lhb_hyyyb_em", "param_mode": "date_range"},
        {"api_name": "stock_lhb_yybph_em", "param_mode": "date_range"},
        {"api_name": "stock_dzjy_mrtj", "param_mode": "trade_date"},
        {"api_name": "stock_dzjy_mrmx", "param_mode": "trade_date"},
        {"api_name": "stock_dzjy_sctj", "param_mode": "trade_date"},
        {"api_name": "stock_dzjy_hyyybtj", "param_mode": "trade_date"},
    ],
}

PHASE_COVERAGE_FAMILIES: tuple[str, ...] = (
    "market_price",
    "index_market",
    "financial_fundamental",
    "margin_leverage",
    "industry_concept",
    "event_ownership",
    "corporate_action",
    "trading_attention",
)

TEMP_DISABLED_APIS: set[tuple[str, str]] = {
    ("market_price", "stock_zh_a_hist"),
    ("market_price", "stock_individual_info_em"),
    ("financial_fundamental", "stock_financial_analysis_indicator"),
    ("margin_leverage", "stock_margin_detail_szse"),
    ("event_ownership", "stock_gpzy_pledge_ratio_detail_em"),
    ("industry_concept", "stock_industry_clf_hist_sw"),
    ("trading_attention", "stock_jgdy_tj_em"),
    ("event_ownership", "stock_gdfx_free_holding_analyse_em"),
    ("event_ownership", "stock_gdfx_holding_analyse_em"),
}

DISABLED_API_METADATA: dict[tuple[str, str], dict[str, str | bool]] = {
    pair: {
        "enabled": False,
        "manual_review_required": True,
        "disabled_reason": "temporarily disabled for acquisition control",
    }
    for pair in TEMP_DISABLED_APIS
}
DISABLED_API_METADATA[("event_ownership", "stock_gdfx_free_holding_analyse_em")] = {
    "enabled": False,
    "manual_review_required": True,
    "disabled_reason": "expensive and unstable in 10d recovery run; Response ended prematurely",
}
DISABLED_API_METADATA[("event_ownership", "stock_gdfx_holding_analyse_em")] = {
    "enabled": False,
    "manual_review_required": True,
    "disabled_reason": "expensive and unstable in 10d recovery run; Response ended prematurely",
}

EXCLUDED_APIS: set[tuple[str, str]] = {("market_price", "stock_zh_a_daily")}


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


def _params_for_mode(mode: str, symbols: list[str], index_symbols: list[str], report_dates: list[str], trade_dates: list[str], industry_names: list[str], concept_names: list[str], start_date: str, end_date: str) -> list[dict[str, str]]:
    if mode == "none":
        return [{}]
    if mode == "symbol_only":
        return [{"symbol": symbols[0]}]
    if mode == "symbol_range":
        return [{"symbol": symbols[0], "start_date": start_date, "end_date": end_date}]
    if mode == "daily_symbol_range":
        return [{"symbol": symbols[0], "start_date": start_date, "end_date": end_date, "adjust": ""}]
    if mode == "daily_symbol_range_hist":
        return [{"symbol": symbols[0], "start_date": start_date, "end_date": end_date, "period": "daily", "adjust": "qfq"}]
    if mode == "index_symbol_range":
        return [{"symbol": index_symbols[0], "start_date": start_date, "end_date": end_date}]
    if mode == "index_symbol":
        return [{"symbol": index_symbols[0]}]
    if mode == "trade_date":
        return [{"date": trade_dates[0]}]
    if mode == "report_date":
        return [{"date": report_dates[0]}]
    if mode == "symbol_report_date":
        return [{"symbol": symbols[0], "date": report_dates[0]}]
    if mode == "industry_code":
        return [{"symbol": "801010"}]
    if mode == "industry_name_range":
        return [{"symbol": industry_names[0], "start_date": start_date, "end_date": end_date}]
    if mode == "industry_name":
        return [{"symbol": industry_names[0]}]
    if mode == "concept_name_range":
        return [{"symbol": concept_names[0], "start_date": start_date, "end_date": end_date}]
    if mode == "concept_name":
        return [{"symbol": concept_names[0]}]
    if mode == "date_range":
        return [{"start_date": start_date, "end_date": end_date}]
    return [{}]


def _fallback_csv_write(output_root: str, family: str, api_name: str, raw: pd.DataFrame) -> tuple[str, str]:
    out = Path(output_root) / "raw" / family / api_name
    out.mkdir(parents=True, exist_ok=True)
    data_path = out / "fallback.csv"
    metadata_path = out / "fallback.meta.csv"
    raw.to_csv(data_path, index=False, encoding="utf-8-sig")
    with metadata_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["source_family", "api_name", "row_count", "write_mode"])
        w.writerow([family, api_name, len(raw), "csv_fallback"])
    return str(data_path), str(metadata_path)


def _normalize_error_message(api_name: str, err: str) -> str:
    low = err.lower()
    unstable_apis = {"stock_zh_a_hist", "stock_margin_detail_szse", "stock_gpzy_pledge_ratio_detail_em", "stock_zh_a_gdhs"}
    if api_name in unstable_apis and any(k in low for k in ["timeout", "remote", "connection", "read timed out", "max retries"]):
        return f"network_unstable_retry: {err}"
    if api_name in {"stock_yjyg_em", "stock_yysj_em", "stock_industry_change_cninfo", "stock_individual_info_em"} and any(
        k in low for k in ["none", "keyerror", "indexerror", "attributeerror", "json", "expecting value"]
    ):
        return f"defensive_shape_guard: {err}"
    return err


def _should_downgrade_to_empty(api_name: str, err: str) -> bool:
    low = err.lower()
    if api_name in {"stock_yjyg_em", "stock_yysj_em"} and any(k in low for k in ["none", "not subscriptable", "expecting value"]):
        return True
    if api_name == "stock_individual_info_em" and "expecting value" in low:
        return True
    if api_name == "stock_industry_change_cninfo" and ("变更日期" in err or "keyerror" in low):
        return True
    return False


def run_raw_coverage_ingest(output_root: str, families: list[str], symbols: list[str], index_symbols: list[str], report_dates: list[str], trade_dates: list[str], industry_names: list[str], concept_names: list[str], start_date: str, end_date: str, adapter_map: dict[str, AdapterFn] | None = None, ak_module: object | None = None, request_sleep: float = 0.0, continue_on_error: bool = True, include_disabled: bool = False) -> dict:
    adapters = adapter_map or {}
    rows: list[dict] = []
    for family in families:
        for spec in COVERAGE_API_SPECS.get(family, []):
            api_name = spec["api_name"]
            params_list = _params_for_mode(spec["param_mode"], symbols, index_symbols, report_dates, trade_dates, industry_names, concept_names, start_date, end_date)
            for params in params_list:
                started_at = datetime.now(UTC)
                status = "pending_adapter"
                err = ""
                n_rows = 0
                out_path = meta_path = ""
                if (family, api_name) in TEMP_DISABLED_APIS and not include_disabled:
                    disabled_reason = str(
                        DISABLED_API_METADATA.get((family, api_name), {}).get(
                            "disabled_reason", "temporarily disabled for acquisition control"
                        )
                    )
                    finished_at = datetime.now(UTC)
                    rows.append(
                        {
                            "source_family": family,
                            "api_name": api_name,
                            "status": "skipped",
                            "rows": 0,
                            "error_message": f"disabled_reason: {disabled_reason}",
                            "output_path": "",
                            "metadata_path": "",
                            "started_at": started_at.isoformat(),
                            "finished_at": finished_at.isoformat(),
                            "elapsed_sec": max((finished_at - started_at).total_seconds(), 0.0),
                        }
                    )
                    continue
                try:
                    fn = adapters.get(api_name) or (getattr(ak_module, api_name) if ak_module is not None and hasattr(ak_module, api_name) else None)
                    if fn is None:
                        status = "pending_adapter"
                    else:
                        filtered = params
                        try:
                            sig = inspect.signature(fn)
                            accepts_kwargs = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
                            if not accepts_kwargs:
                                allowed = set(sig.parameters.keys())
                                filtered = {k: v for k, v in params.items() if k in allowed}
                        except (TypeError, ValueError):
                            filtered = params

                        ret = fn(**filtered)
                        if ret is None:
                            raise ValueError("none_result_from_api")
                        raw = ret.raw if hasattr(ret, "raw") else ret
                        if not isinstance(raw, pd.DataFrame):
                            raw = pd.DataFrame(raw)
                        n_rows = len(raw)
                        status = "empty" if raw.empty else "success"
                        partition = {"scope": "coverage", "key": api_name}
                        try:
                            dp, mp = write_raw_partition(output_root, family, api_name, partition, raw, {"source_family": family, "api_name": api_name, "params": filtered, "status": status, "row_count": n_rows})
                            out_path, meta_path = str(dp), str(mp)
                        except Exception as write_exc:  # noqa: BLE001
                            if api_name == "stock_individual_info_em":
                                out_path, meta_path = _fallback_csv_write(output_root, family, api_name, raw)
                                err = f"csv_fallback_after_write_error: {write_exc}"
                            else:
                                raise
                except Exception as exc:  # noqa: BLE001
                    err = _normalize_error_message(api_name, str(exc))
                    if _should_downgrade_to_empty(api_name, err):
                        status = "empty"
                        n_rows = 0
                    else:
                        status = "failed"
                    if not continue_on_error:
                        finished_at = datetime.now(UTC)
                        rows.append({"source_family": family, "api_name": api_name, "status": status, "rows": n_rows, "error_message": err, "output_path": out_path, "metadata_path": meta_path, "started_at": started_at.isoformat(), "finished_at": finished_at.isoformat(), "elapsed_sec": max((finished_at - started_at).total_seconds(), 0.0)})
                        break
                finished_at = datetime.now(UTC)
                rows.append({"source_family": family, "api_name": api_name, "status": status, "rows": n_rows, "error_message": err, "output_path": out_path, "metadata_path": meta_path, "started_at": started_at.isoformat(), "finished_at": finished_at.isoformat(), "elapsed_sec": max((finished_at - started_at).total_seconds(), 0.0)})
                if request_sleep > 0:
                    time.sleep(request_sleep)

    out = Path(output_root)
    out.mkdir(parents=True, exist_ok=True)
    catalog_path = out / "raw_ingest_catalog.csv"
    summary_path = out / "raw_ingest_summary.csv"
    df = pd.DataFrame(rows)
    df.to_csv(catalog_path, index=False, encoding="utf-8-sig")
    s = df.groupby(["source_family", "status"], as_index=False).size() if not df.empty else pd.DataFrame(columns=["source_family", "status", "size"])
    s.to_csv(summary_path, index=False, encoding="utf-8-sig")
    checklist_df, checklist_summary_df = build_acquisition_checklist(df)
    checklist_path = out / "raw_source_acquisition_checklist.csv"
    checklist_summary_path = out / "raw_source_acquisition_summary.csv"
    checklist_df.to_csv(checklist_path, index=False, encoding="utf-8-sig")
    checklist_summary_df.to_csv(checklist_summary_path, index=False, encoding="utf-8-sig")
    return {
        "catalog_path": str(catalog_path),
        "summary_path": str(summary_path),
        "checklist_path": str(checklist_path),
        "checklist_summary_path": str(checklist_summary_path),
        "rows": rows,
    }


def build_acquisition_checklist(catalog_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    phase_pairs = {
        (family, row["api_name"])
        for family in PHASE_COVERAGE_FAMILIES
        for row in COVERAGE_API_SPECS.get(family, [])
    }
    all_pairs = set(phase_pairs) | EXCLUDED_APIS
    status_map = {
        (str(r.get("source_family", "")), str(r.get("api_name", ""))): str(r.get("status", ""))
        for _, r in catalog_df.iterrows()
    }
    rows: list[dict[str, str]] = []
    for family, api_name in sorted(all_pairs):
        if (family, api_name) in EXCLUDED_APIS:
            acq = "排除"
        elif (family, api_name) in TEMP_DISABLED_APIS:
            acq = "暂停获取"
        else:
            st = status_map.get((family, api_name), "").lower()
            acq = "获取" if st == "success" else "暂停获取"
        rows.append({"api_name": api_name, "source_family": family, "acquisition_status": acq})
    checklist_df = pd.DataFrame(rows, columns=["api_name", "source_family", "acquisition_status"])
    summary_df = checklist_df.groupby("acquisition_status", as_index=False).size().rename(columns={"size": "count"})
    return checklist_df, summary_df

from __future__ import annotations

import csv
import json
import inspect
import time
import uuid
import multiprocessing as mp
import signal
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

import pandas as pd

from qsys.data.factor_lake.io import write_raw_partition
from qsys.data.factor_lake.metastore import FactorLakeMetastore
from qsys.data.factor_lake.registry import DATASET_REGISTRY, get_dataset_spec, plan_partitions
from qsys.data.factor_lake.acquisition_universe import build_report_dates, build_trade_dates, load_concept_names, load_index_symbols, load_industry_names, load_stock_symbols
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
    ("event_ownership", "stock_gpzy_pledge_ratio_detail_em"),
    ("disclosure_ir", "stock_jgdy_tj_em"),
    ("disclosure_ir", "stock_jgdy_detail_em"),
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
DISABLED_API_METADATA[("market_price", "stock_zh_a_hist")] = {
    "enabled": False,
    "manual_review_required": True,
    "disabled_reason": "usable in tiny-window probe; keep default paused, enable via include_disabled controlled recovery",
}
DISABLED_API_METADATA[("market_price", "stock_individual_info_em")] = {
    "enabled": False,
    "manual_review_required": True,
    "disabled_reason": "usable but mixed-type raw metadata table may need csv fallback; enable via include_disabled controlled recovery",
}
DISABLED_API_METADATA[("margin_leverage", "stock_margin_detail_szse")] = {
    "enabled": False,
    "manual_review_required": True,
    "disabled_reason": "usable in tiny-window probe; keep default paused, enable selected trade_dates via include_disabled",
}
DISABLED_API_METADATA[("financial_fundamental", "stock_financial_analysis_indicator")] = {
    "enabled": False,
    "manual_review_required": True,
    "disabled_reason": "executes but currently returns empty for tested symbols; deferred manual review",
}
DISABLED_API_METADATA[("event_ownership", "stock_gpzy_pledge_ratio_detail_em")] = {
    "enabled": False,
    "manual_review_required": True,
    "disabled_reason": "heavy_crawl detail_source; keep deferred by default to avoid blocking recovery run",
}
DISABLED_API_METADATA[("disclosure_ir", "stock_jgdy_tj_em")] = {
    "enabled": False,
    "manual_review_required": True,
    "disabled_reason": "lightweight institutional attention substitute, but paused by default for controlled recovery only",
}
DISABLED_API_METADATA[("disclosure_ir", "stock_jgdy_detail_em")] = {
    "enabled": False,
    "manual_review_required": True,
    "disabled_reason": "heavy_crawl detail_source; deferred by default",
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
        return [{"symbol": symbol} for symbol in symbols]
    if mode == "symbol_range":
        return [{"symbol": symbol, "start_date": start_date, "end_date": end_date} for symbol in symbols]
    if mode == "daily_symbol_range":
        return [{"symbol": symbol, "start_date": start_date, "end_date": end_date, "adjust": ""} for symbol in symbols]
    if mode == "daily_symbol_range_hist":
        return [{"symbol": symbol, "start_date": start_date, "end_date": end_date, "period": "daily", "adjust": "qfq"} for symbol in symbols]
    if mode == "index_symbol_range":
        return [{"symbol": symbol, "start_date": start_date, "end_date": end_date} for symbol in index_symbols]
    if mode == "index_symbol":
        return [{"symbol": symbol} for symbol in index_symbols]
    if mode == "trade_date":
        return [{"date": date} for date in trade_dates]
    if mode == "report_date":
        return [{"date": date} for date in report_dates]
    if mode == "symbol_report_date":
        return [{"symbol": symbol, "date": date} for symbol in symbols for date in report_dates]
    if mode == "industry_code":
        return [{"symbol": "801010"}]
    if mode == "industry_name_range":
        return [{"symbol": name, "start_date": start_date, "end_date": end_date} for name in industry_names]
    if mode == "industry_name":
        return [{"symbol": name} for name in industry_names]
    if mode == "concept_name_range":
        return [{"symbol": name, "start_date": start_date, "end_date": end_date} for name in concept_names]
    if mode == "concept_name":
        return [{"symbol": name} for name in concept_names]
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
        w.writerow(["source_family", "api_name", "row_count", "write_mode", "file_format"])
        w.writerow([family, api_name, len(raw), "csv_fallback", "csv"])
    return str(data_path), str(metadata_path)


def _normalize_error_message(api_name: str, err: str) -> str:
    low = err.lower()
    unstable_apis = {
        "stock_zh_a_hist",
        "stock_margin_detail_szse",
        "stock_gpzy_pledge_ratio_detail_em",
        "stock_zh_a_gdhs",
        "stock_board_industry_index_ths",
        "stock_restricted_release_summary_em",
    }
    if api_name in unstable_apis and any(k in low for k in ["timeout", "remote", "connection", "read timed out", "max retries"]):
        return f"network_unstable_retry: {err}"
    if api_name == "stock_restricted_release_summary_em" and "response ended prematurely" in low:
        return f"network_unstable_retry: {err}"
    if api_name == "sw_index_third_info" and any(k in low for k in ["find_all", "nonetype"]):
        return f"defensive_shape_guard: parser_empty_response: {err}"
    if api_name in {"stock_yjyg_em", "stock_yysj_em", "stock_industry_change_cninfo", "stock_individual_info_em", "stock_zh_a_disclosure_relation_cninfo"} and any(
        k in low for k in ["none", "keyerror", "indexerror", "attributeerror", "json", "expecting value", "are in the [columns]"]
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
    if api_name == "stock_zh_a_disclosure_relation_cninfo" and ("are in the [columns]" in low or "keyerror" in low):
        return True
    if api_name == "sw_index_third_info" and any(k in low for k in ["parser_empty_response", "find_all", "nonetype"]):
        return True
    if api_name in {"stock_board_industry_index_ths", "stock_restricted_release_summary_em"} and "network_unstable_retry" in low:
        return True
    return False


def _call_api_with_retry(
    fn: AdapterFn,
    filtered: dict[str, str],
    retries: int = 3,
    retry_wait: float = 1.0,
) -> object:
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return fn(**filtered)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < retries:
                time.sleep(retry_wait * attempt)
    if last_exc is None:
        raise RuntimeError("unknown_api_error")
    raise last_exc


def _to_akshare_daily_symbol(symbol: str | int) -> str:
    raw = str(symbol).strip().lower()
    if raw.startswith(("sz", "sh", "bj")):
        return raw
    digits = "".join(ch for ch in raw if ch.isdigit())
    digits = digits.zfill(6)
    if digits.startswith(("000", "001", "002", "003", "300")):
        return f"sz{digits}"
    if digits.startswith(("600", "601", "603", "605", "688")):
        return f"sh{digits}"
    if digits.startswith(("430", "830", "831", "832", "833", "834", "835", "836", "837", "838", "839", "870", "871", "872", "873", "874", "875", "876", "877", "920", "921", "922", "923", "924", "925", "926", "927", "928", "929")):
        return f"bj{digits}"
    return f"sz{digits}"


def _filter_daily_frame_by_range(raw: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    if raw.empty or "date" not in raw.columns:
        return raw
    df = raw.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    start_dt = pd.to_datetime(start_date, format="%Y%m%d", errors="coerce")
    end_dt = pd.to_datetime(end_date, format="%Y%m%d", errors="coerce")
    if pd.notna(start_dt):
        df = df[df["date"] >= start_dt]
    if pd.notna(end_dt):
        df = df[df["date"] <= end_dt]
    return df


def _detect_daily_date_column(raw: pd.DataFrame) -> str | None:
    for col in ("date", "日期", "trade_date"):
        if col in raw.columns:
            return col
    return None


def _split_daily_by_year_month(raw: pd.DataFrame, start_date: str, end_date: str) -> dict[tuple[str, str], pd.DataFrame]:
    if raw.empty:
        return {}
    date_col = _detect_daily_date_column(raw)
    if date_col is None:
        start_year = str(start_date)[:4] if start_date else ""
        start_month = str(start_date)[4:6] if len(str(start_date)) >= 6 else ""
        if start_year.isdigit():
            month = start_month if start_month.isdigit() else "01"
            return {(start_year, month): raw.copy()}
        return {}
    date_series = pd.to_datetime(raw[date_col], errors="coerce")
    valid_mask = date_series.notna()
    if not valid_mask.any():
        return {}
    df = raw.loc[valid_mask].copy()
    grouped = date_series.loc[valid_mask].groupby([date_series.loc[valid_mask].dt.year, date_series.loc[valid_mask].dt.month])
    out: dict[tuple[str, str], pd.DataFrame] = {}
    for (year, month), idx in grouped.groups.items():
        part = df.loc[idx]
        sort_key = pd.to_datetime(part[date_col], errors="coerce")
        out[(str(int(year)), f"{int(month):02d}")] = part.iloc[sort_key.argsort(kind="mergesort")].reset_index(drop=True)
    return out


def _write_market_price_month_partitions(
    output_root: str,
    family: str,
    used_api_name: str,
    requested_api_name: str,
    fallback_from: str,
    original_symbol: str,
    akshare_symbol: str,
    params: dict[str, str],
    raw: pd.DataFrame,
    symbol_value: str,
    adjust_label: str,
    started_at: datetime,
) -> list[dict[str, object]]:
    month_frames = _split_daily_by_year_month(raw, str(params.get("start_date", "")), str(params.get("end_date", "")))
    task_rows: list[dict[str, object]] = []
    date_col = _detect_daily_date_column(raw)
    for (year, month), month_df in month_frames.items():
        month_dates = pd.to_datetime(month_df[date_col], errors="coerce") if date_col and date_col in month_df.columns else pd.Series(dtype="datetime64[ns]")
        min_date = str(month_dates.min().date()) if not month_dates.empty and month_dates.notna().any() else ""
        max_date = str(month_dates.max().date()) if not month_dates.empty and month_dates.notna().any() else ""
        partition = {"symbol": symbol_value, "adjust": adjust_label, "year": year, "month": month}
        metadata = {
            "api_name": used_api_name,
            "source_family": family,
            "dataset_name": "raw_source_api",
            "requested_api_name": requested_api_name,
            "actual_api_name": used_api_name,
            "fallback_from": fallback_from,
            "original_symbol": original_symbol,
            "akshare_symbol": akshare_symbol,
            "start_date": str(params.get("start_date", "")),
            "end_date": str(params.get("end_date", "")),
            "year": year,
            "month": month,
            "min_date": min_date,
            "max_date": max_date,
            "rows": int(len(month_df)),
            "created_at": datetime.now(UTC).isoformat(),
            "updated_at": datetime.now(UTC).isoformat(),
        }
        try:
            dp, mp = write_raw_partition(output_root, family, used_api_name, partition, month_df, metadata)
            part_status = "success"
            part_err_type = ""
            part_err_msg = ""
        except FileExistsError as exists_exc:
            dp = Path(output_root) / "data" / "raw" / "akshare" / family / used_api_name / f"symbol={symbol_value}" / f"adjust={adjust_label}" / f"year={year}" / f"month={month}" / "data.parquet"
            mp = dp.with_name("metadata.json")
            part_status = "already_exists"
            part_err_type = "FileExistsError"
            part_err_msg = str(exists_exc)
        task_rows.append(
            {
                "dataset_name": "raw_source_api",
                "partition_json": json.dumps(partition, ensure_ascii=False, sort_keys=True),
                "params_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
                "source_family": family,
                "api_name": used_api_name,
                "requested_api_name": requested_api_name,
                "actual_api_name": used_api_name,
                "fallback_from": fallback_from,
                "original_symbol": original_symbol,
                "akshare_symbol": akshare_symbol,
                "year": year,
                "month": month,
                "min_date": min_date,
                "max_date": max_date,
                "status": part_status,
                "rows": int(len(month_df)),
                "error_type": part_err_type,
                "error_message": part_err_msg,
                "output_path": str(dp),
                "metadata_path": str(mp),
                "started_at": started_at.isoformat(),
                "finished_at": datetime.now(UTC).isoformat(),
                "elapsed_sec": max((datetime.now(UTC) - started_at).total_seconds(), 0.0),
            }
        )
    return task_rows


def _run_single_coverage_task(
    output_root: str,
    family: str,
    api_name: str,
    params: dict[str, str],
    adapters: dict[str, AdapterFn],
    ak_module: object | None,
    include_disabled: bool,
) -> list[dict[str, object]]:
    started_at = datetime.now(UTC)
    status = "pending_adapter"
    err = ""
    err_type = ""
    n_rows = 0
    out_path = meta_path = ""

    if (family, api_name) in TEMP_DISABLED_APIS and not include_disabled:
        disabled_reason = str(
            DISABLED_API_METADATA.get((family, api_name), {}).get(
                "disabled_reason", "temporarily disabled for acquisition control"
            )
        )
        finished_at = datetime.now(UTC)
        return [{
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
        }]

    used_api_name = api_name
    fallback_from = ""
    requested_api_name = api_name
    original_symbol = ""
    if "symbol" in params:
        symbol_text = str(params.get("symbol", "")).strip().lower()
        if symbol_text.startswith(("sz", "sh", "bj")):
            symbol_text = symbol_text[2:]
        digits = "".join(ch for ch in symbol_text if ch.isdigit())
        original_symbol = digits.zfill(6) if digits else str(params.get("symbol", ""))
    akshare_symbol = ""
    primary_error = ""
    fallback_error = ""
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
            if api_name == "stock_zh_a_hist":
                daily_fn = adapters.get("stock_zh_a_daily") or (getattr(ak_module, "stock_zh_a_daily") if ak_module is not None and hasattr(ak_module, "stock_zh_a_daily") else None)
                try:
                    ret = _call_api_with_retry(fn, filtered)
                except Exception as primary_exc:
                    primary_error = f"{type(primary_exc).__name__}: {primary_exc}"
                    if daily_fn is None:
                        raise
                    fallback_from = "stock_zh_a_hist"
                    used_api_name = "stock_zh_a_daily"
                    akshare_symbol = _to_akshare_daily_symbol(original_symbol)
                    daily_params = {"symbol": akshare_symbol, "adjust": ""}
                    try:
                        ret = _call_api_with_retry(daily_fn, daily_params)
                    except Exception as daily_exc:
                        fallback_error = f"{type(daily_exc).__name__}: {daily_exc}"
                        raise
            else:
                ret = fn(**filtered)
            if ret is None:
                raise ValueError("none_result_from_api")
            raw = ret.raw if hasattr(ret, "raw") else ret
            if not isinstance(raw, pd.DataFrame):
                raw = pd.DataFrame(raw)
            if used_api_name == "stock_zh_a_daily":
                raw = _filter_daily_frame_by_range(raw, str(params.get("start_date", "")), str(params.get("end_date", "")))
            n_rows = len(raw)
            status = "empty" if raw.empty else "success"
            partition = dict(filtered) if filtered else {"api_name": api_name}
            if used_api_name == "stock_zh_a_daily":
                partition = {"symbol": akshare_symbol, "adjust": ""}
            try:
                if used_api_name in {"stock_zh_a_daily", "stock_zh_a_hist"} and family == "market_price":
                    symbol_value = akshare_symbol if used_api_name == "stock_zh_a_daily" else str(filtered.get("symbol", ""))
                    adjust_label = "none" if used_api_name == "stock_zh_a_daily" else str(filtered.get("adjust", "none") or "none")
                    task_rows = _write_market_price_month_partitions(
                        output_root, family, used_api_name, requested_api_name, fallback_from, original_symbol, akshare_symbol,
                        params, raw, symbol_value, adjust_label, started_at
                    )
                    if not task_rows:
                        status = "empty"
                    else:
                        return task_rows
                else:
                    dp, mp = write_raw_partition(
                        output_root, family, used_api_name, partition, raw,
                        {
                            "source_family": family,
                            "api_name": used_api_name,
                            "requested_api_name": requested_api_name,
                            "actual_api_name": used_api_name,
                            "fallback_from": fallback_from,
                            "original_symbol": original_symbol,
                            "akshare_symbol": akshare_symbol,
                            "params": filtered,
                            "status": status,
                            "row_count": n_rows,
                        }
                    )
                    out_path, meta_path = str(dp), str(mp)
            except Exception as write_exc:  # noqa: BLE001
                if api_name == "stock_individual_info_em":
                    out_path, meta_path = _fallback_csv_write(output_root, family, api_name, raw)
                    err = f"csv_fallback_after_write_error: {write_exc}"
                elif isinstance(write_exc, FileExistsError):
                    status = "already_exists"
                    err_type = "FileExistsError"
                    err = str(write_exc)
                else:
                    raise
    except Exception as exc:  # noqa: BLE001
        err = _normalize_error_message(api_name, str(exc))
        if primary_error or fallback_error:
            err = f"primary_error={primary_error or 'n/a'}; fallback_error={fallback_error or 'n/a'}"
        err_type = type(exc).__name__
        if _should_downgrade_to_empty(api_name, err):
            status = "empty"
            n_rows = 0
            err_type = "downgraded_to_empty"
        else:
            status = "failed"

    finished_at = datetime.now(UTC)
    return [{
        "dataset_name": "raw_source_api",
        "partition_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
        "params_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
        "source_family": family,
        "api_name": used_api_name,
        "requested_api_name": requested_api_name,
        "actual_api_name": used_api_name,
        "fallback_from": fallback_from,
        "original_symbol": original_symbol,
        "akshare_symbol": akshare_symbol,
        "status": status,
        "rows": n_rows,
        "error_type": err_type,
        "error_message": err,
        "output_path": out_path,
        "metadata_path": meta_path,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "elapsed_sec": max((finished_at - started_at).total_seconds(), 0.0),
    }]






def _run_task_with_signal_timeout(
    output_root: str,
    family: str,
    api_name: str,
    params: dict[str, str],
    adapters: dict[str, AdapterFn],
    ak_module: object | None,
    include_disabled: bool,
    task_timeout_sec: float,
) -> list[dict[str, object]]:
    started_at = datetime.now(UTC)
    def _handler(signum: int, frame: object) -> None:  # noqa: ARG001
        raise TimeoutError(f"task timeout after {task_timeout_sec} sec")

    old = signal.signal(signal.SIGALRM, _handler)
    signal.setitimer(signal.ITIMER_REAL, task_timeout_sec)
    try:
        return _run_single_coverage_task(output_root, family, api_name, params, adapters, ak_module, include_disabled)
    except TimeoutError as exc:
        finished_at = datetime.now(UTC)
        symbol = str(params.get("symbol", ""))
        return [{
            "dataset_name": "raw_source_api",
            "partition_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "params_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "source_family": family,
            "api_name": api_name,
            "requested_api_name": api_name,
            "actual_api_name": api_name,
            "fallback_from": "",
            "original_symbol": symbol,
            "akshare_symbol": "",
            "status": "timeout",
            "rows": 0,
            "error_type": "TimeoutError",
            "error_message": str(exc),
            "output_path": "",
            "metadata_path": "",
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "elapsed_sec": max((finished_at - started_at).total_seconds(), 0.0),
        }]
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old)

def _run_task_in_subprocess_with_timeout(
    output_root: str,
    family: str,
    api_name: str,
    params: dict[str, str],
    adapters: dict[str, AdapterFn],
    ak_module: object | None,
    include_disabled: bool,
    task_timeout_sec: float,
) -> list[dict[str, object]]:
    started_at = datetime.now(UTC)

    def _worker(q: mp.Queue) -> None:
        try:
            rows = _run_single_coverage_task(output_root, family, api_name, params, adapters, ak_module, include_disabled)
            q.put(("ok", rows))
        except Exception as exc:  # noqa: BLE001
            q.put(("err", f"{type(exc).__name__}: {exc}"))

    ctx = mp.get_context("fork")
    q: mp.Queue = ctx.Queue()
    proc = ctx.Process(target=_worker, args=(q,))
    proc.start()
    proc.join(task_timeout_sec)
    if proc.is_alive():
        proc.terminate()
        proc.join(5)
        finished_at = datetime.now(UTC)
        symbol = str(params.get("symbol", "")).strip().lower()
        if symbol.startswith(("sz", "sh", "bj")):
            symbol = symbol[2:]
        digits = "".join(ch for ch in symbol if ch.isdigit())
        original_symbol = digits.zfill(6) if digits else str(params.get("symbol", ""))
        akshare_symbol = _to_akshare_daily_symbol(original_symbol) if original_symbol else ""
        return [{
            "dataset_name": "raw_source_api",
            "partition_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "params_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "source_family": family,
            "api_name": api_name,
            "requested_api_name": api_name,
            "actual_api_name": api_name,
            "fallback_from": "",
            "original_symbol": original_symbol,
            "akshare_symbol": akshare_symbol,
            "status": "timeout",
            "rows": 0,
            "error_type": "TimeoutError",
            "error_message": f"task timeout after {task_timeout_sec} sec",
            "output_path": "",
            "metadata_path": "",
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "elapsed_sec": max((finished_at - started_at).total_seconds(), 0.0),
        }]
    if not q.empty():
        status, payload = q.get()
        if status == "ok":
            return payload
        finished_at = datetime.now(UTC)
        return [{
            "dataset_name": "raw_source_api",
            "partition_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "params_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "source_family": family,
            "api_name": api_name,
            "requested_api_name": api_name,
            "actual_api_name": api_name,
            "fallback_from": "",
            "original_symbol": str(params.get("symbol", "")),
            "akshare_symbol": "",
            "status": "failed",
            "rows": 0,
            "error_type": "SubprocessTaskError",
            "error_message": str(payload),
            "output_path": "",
            "metadata_path": "",
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "elapsed_sec": max((finished_at - started_at).total_seconds(), 0.0),
        }]
    return []

def run_raw_coverage_ingest(output_root: str, families: list[str], symbols: list[str] | None = None, index_symbols: list[str] | None = None, report_dates: list[str] | None = None, trade_dates: list[str] | None = None, industry_names: list[str] | None = None, concept_names: list[str] | None = None, start_date: str = "20100101", end_date: str = "20101231", adapter_map: dict[str, AdapterFn] | None = None, ak_module: object | None = None, request_sleep: float = 0.0, continue_on_error: bool = True, include_disabled: bool = False, max_workers: int = 2, selected_api_names: list[str] | None = None, resume: bool = False, universe_root: str | Path = "config/factor_sources/acquisition_universe", task_timeout_sec: float | None = None) -> dict:
    selected = {x.strip() for x in (selected_api_names or []) if x and x.strip()}
    selected_specs: list[dict[str, str]] = []
    for family in families:
        for spec in COVERAGE_API_SPECS.get(family, []):
            api_name = spec["api_name"]
            if selected and api_name not in selected:
                continue
            selected_specs.append(spec)

    required_modes = {spec["param_mode"] for spec in selected_specs}
    need_symbols = bool(required_modes & {"symbol_only", "symbol_range", "daily_symbol_range", "daily_symbol_range_hist", "symbol_report_date"})
    need_index_symbols = bool(required_modes & {"index_symbol_range", "index_symbol"})
    need_trade_dates = "trade_date" in required_modes
    need_report_dates = bool(required_modes & {"report_date", "symbol_report_date"})
    need_industry_names = bool(required_modes & {"industry_name_range", "industry_name"})
    need_concept_names = bool(required_modes & {"concept_name_range", "concept_name"})

    symbols = load_stock_symbols(symbols, universe_root=universe_root) if need_symbols else (symbols or [])
    index_symbols = load_index_symbols(index_symbols, universe_root=universe_root) if need_index_symbols else (index_symbols or [])
    trade_dates = build_trade_dates(start_date, end_date, trade_dates, universe_root=universe_root) if need_trade_dates else (trade_dates or [])
    report_dates = build_report_dates(start_date, end_date, report_dates) if need_report_dates else (report_dates or [])
    industry_names = load_industry_names(industry_names, universe_root=universe_root) if need_industry_names else (industry_names or [])
    concept_names = load_concept_names(concept_names, universe_root=universe_root) if need_concept_names else (concept_names or [])

    resume_keys: set[tuple[str, str, str]] = set()
    catalog_path = Path(output_root) / "raw_ingest_catalog.csv"
    if resume and catalog_path.exists():
        existing = pd.read_csv(catalog_path)
        for _, row in existing.iterrows():
            resume_keys.add((str(row.get("source_family", "")), str(row.get("api_name", "")), str(row.get("status", ""))))
    run_id = f"raw_official_{uuid.uuid4().hex[:8]}"
    adapters = adapter_map or {}
    op_dir = Path(output_root) / "_operation_review"
    op_dir.mkdir(parents=True, exist_ok=True)
    task_events_path = op_dir / "task_events.jsonl"
    if task_events_path.exists():
        task_events_path.unlink()
    rows: list[dict] = []
    tasks: list[tuple[str, str, dict[str, str]]] = []
    for family in families:
        for spec in COVERAGE_API_SPECS.get(family, []):
            api_name = spec["api_name"]
            if selected and api_name not in selected:
                continue
            params_list = _params_for_mode(spec["param_mode"], symbols, index_symbols, report_dates, trade_dates, industry_names, concept_names, start_date, end_date)
            for params in params_list:
                if resume and (family, api_name, "success") in resume_keys:
                    continue
                tasks.append((family, api_name, params))

    def _summarize_task(task_rows: list[dict[str, object]]) -> dict[str, object]:
        if not task_rows:
            return {"status": "failed", "error_type": "NoResult", "error_message": "empty task rows", "elapsed_sec": 0.0}
        status_order = {"failed": 4, "timeout": 3, "empty": 2, "pending_adapter": 1, "success": 0}
        best = sorted(task_rows, key=lambda r: status_order.get(str(r.get("status", "")), 5), reverse=True)[0]
        return {
            "run_id": run_id,
            "source_family": best.get("source_family", ""),
            "api_name": best.get("api_name", ""),
            "requested_api_name": best.get("requested_api_name", ""),
            "fallback_from": best.get("fallback_from", ""),
            "original_symbol": best.get("original_symbol", ""),
            "akshare_symbol": best.get("akshare_symbol", ""),
            "status": best.get("status", ""),
            "error_type": best.get("error_type", ""),
            "error_message": best.get("error_message", ""),
            "started_at": best.get("started_at", ""),
            "finished_at": best.get("finished_at", ""),
            "elapsed_sec": max(float(r.get("elapsed_sec", 0.0) or 0.0) for r in task_rows),
        }

    def _record_task_rows(task_rows: list[dict[str, object]]) -> None:
        for row in task_rows:
            row["run_id"] = run_id
            rows.append(row)
        event = _summarize_task(task_rows)
        with open(task_events_path, "a", encoding="utf-8") as ef:
            ef.write(json.dumps(event, ensure_ascii=False) + "\n")
        print(f"[task] {event['status']} family={event['source_family']} api={event['api_name']} symbol={event['original_symbol']} elapsed={event['elapsed_sec']}")

    def _execute_task(family: str, api_name: str, params: dict[str, str]) -> list[dict[str, object]]:
        if task_timeout_sec and task_timeout_sec > 0:
            return _run_task_in_subprocess_with_timeout(output_root, family, api_name, params, adapters, ak_module, include_disabled, float(task_timeout_sec))
        return _run_single_coverage_task(output_root, family, api_name, params, adapters, ak_module, include_disabled)

    if max_workers <= 1:
        for family, api_name, params in tasks:
            print(f"[task] start family={family} api={api_name} symbol={params.get('symbol','')}")
            task_rows = _execute_task(family, api_name, params)
            _record_task_rows(task_rows)
            if any(row.get("status") in {"failed", "timeout"} for row in task_rows) and not continue_on_error:
                break
            if request_sleep > 0:
                time.sleep(request_sleep)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = []
            for family, api_name, params in tasks:
                print(f"[task] start family={family} api={api_name} symbol={params.get('symbol','')}")
                futures.append(ex.submit(_execute_task, family, api_name, params))
            for fut in futures:
                task_rows = fut.result()
                _record_task_rows(task_rows)
                if request_sleep > 0:
                    time.sleep(request_sleep)
    out = Path(output_root) / "raw" / family / api_name
    out.mkdir(parents=True, exist_ok=True)
    data_path = out / "fallback.csv"
    metadata_path = out / "fallback.meta.csv"
    raw.to_csv(data_path, index=False, encoding="utf-8-sig")
    with metadata_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["source_family", "api_name", "row_count", "write_mode", "file_format"])
        w.writerow([family, api_name, len(raw), "csv_fallback", "csv"])
    return str(data_path), str(metadata_path)


def _normalize_error_message(api_name: str, err: str) -> str:
    low = err.lower()
    unstable_apis = {
        "stock_zh_a_hist",
        "stock_margin_detail_szse",
        "stock_gpzy_pledge_ratio_detail_em",
        "stock_zh_a_gdhs",
        "stock_board_industry_index_ths",
        "stock_restricted_release_summary_em",
    }
    if api_name in unstable_apis and any(k in low for k in ["timeout", "remote", "connection", "read timed out", "max retries"]):
        return f"network_unstable_retry: {err}"
    if api_name == "stock_restricted_release_summary_em" and "response ended prematurely" in low:
        return f"network_unstable_retry: {err}"
    if api_name == "sw_index_third_info" and any(k in low for k in ["find_all", "nonetype"]):
        return f"defensive_shape_guard: parser_empty_response: {err}"
    if api_name in {"stock_yjyg_em", "stock_yysj_em", "stock_industry_change_cninfo", "stock_individual_info_em", "stock_zh_a_disclosure_relation_cninfo"} and any(
        k in low for k in ["none", "keyerror", "indexerror", "attributeerror", "json", "expecting value", "are in the [columns]"]
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
    if api_name == "stock_zh_a_disclosure_relation_cninfo" and ("are in the [columns]" in low or "keyerror" in low):
        return True
    if api_name == "sw_index_third_info" and any(k in low for k in ["parser_empty_response", "find_all", "nonetype"]):
        return True
    if api_name in {"stock_board_industry_index_ths", "stock_restricted_release_summary_em"} and "network_unstable_retry" in low:
        return True
    return False


def _call_api_with_retry(
    fn: AdapterFn,
    filtered: dict[str, str],
    retries: int = 3,
    retry_wait: float = 1.0,
) -> object:
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return fn(**filtered)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < retries:
                time.sleep(retry_wait * attempt)
    if last_exc is None:
        raise RuntimeError("unknown_api_error")
    raise last_exc


def _to_akshare_daily_symbol(symbol: str | int) -> str:
    raw = str(symbol).strip().lower()
    if raw.startswith(("sz", "sh", "bj")):
        return raw
    digits = "".join(ch for ch in raw if ch.isdigit())
    digits = digits.zfill(6)
    if digits.startswith(("000", "001", "002", "003", "300")):
        return f"sz{digits}"
    if digits.startswith(("600", "601", "603", "605", "688")):
        return f"sh{digits}"
    if digits.startswith(("430", "830", "831", "832", "833", "834", "835", "836", "837", "838", "839", "870", "871", "872", "873", "874", "875", "876", "877", "920", "921", "922", "923", "924", "925", "926", "927", "928", "929")):
        return f"bj{digits}"
    return f"sz{digits}"


def _filter_daily_frame_by_range(raw: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    if raw.empty or "date" not in raw.columns:
        return raw
    df = raw.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    start_dt = pd.to_datetime(start_date, format="%Y%m%d", errors="coerce")
    end_dt = pd.to_datetime(end_date, format="%Y%m%d", errors="coerce")
    if pd.notna(start_dt):
        df = df[df["date"] >= start_dt]
    if pd.notna(end_dt):
        df = df[df["date"] <= end_dt]
    return df


def _detect_daily_date_column(raw: pd.DataFrame) -> str | None:
    for col in ("date", "日期", "trade_date"):
        if col in raw.columns:
            return col
    return None


def _split_daily_by_year_month(raw: pd.DataFrame, start_date: str, end_date: str) -> dict[tuple[str, str], pd.DataFrame]:
    if raw.empty:
        return {}
    date_col = _detect_daily_date_column(raw)
    if date_col is None:
        start_year = str(start_date)[:4] if start_date else ""
        start_month = str(start_date)[4:6] if len(str(start_date)) >= 6 else ""
        if start_year.isdigit():
            month = start_month if start_month.isdigit() else "01"
            return {(start_year, month): raw.copy()}
        return {}
    date_series = pd.to_datetime(raw[date_col], errors="coerce")
    valid_mask = date_series.notna()
    if not valid_mask.any():
        return {}
    df = raw.loc[valid_mask].copy()
    grouped = date_series.loc[valid_mask].groupby([date_series.loc[valid_mask].dt.year, date_series.loc[valid_mask].dt.month])
    out: dict[tuple[str, str], pd.DataFrame] = {}
    for (year, month), idx in grouped.groups.items():
        part = df.loc[idx]
        sort_key = pd.to_datetime(part[date_col], errors="coerce")
        out[(str(int(year)), f"{int(month):02d}")] = part.iloc[sort_key.argsort(kind="mergesort")].reset_index(drop=True)
    return out


def _write_market_price_month_partitions(
    output_root: str,
    family: str,
    used_api_name: str,
    requested_api_name: str,
    fallback_from: str,
    original_symbol: str,
    akshare_symbol: str,
    params: dict[str, str],
    raw: pd.DataFrame,
    symbol_value: str,
    adjust_label: str,
    started_at: datetime,
) -> list[dict[str, object]]:
    month_frames = _split_daily_by_year_month(raw, str(params.get("start_date", "")), str(params.get("end_date", "")))
    task_rows: list[dict[str, object]] = []
    date_col = _detect_daily_date_column(raw)
    for (year, month), month_df in month_frames.items():
        month_dates = pd.to_datetime(month_df[date_col], errors="coerce") if date_col and date_col in month_df.columns else pd.Series(dtype="datetime64[ns]")
        min_date = str(month_dates.min().date()) if not month_dates.empty and month_dates.notna().any() else ""
        max_date = str(month_dates.max().date()) if not month_dates.empty and month_dates.notna().any() else ""
        partition = {"symbol": symbol_value, "adjust": adjust_label, "year": year, "month": month}
        metadata = {
            "api_name": used_api_name,
            "source_family": family,
            "dataset_name": "raw_source_api",
            "requested_api_name": requested_api_name,
            "actual_api_name": used_api_name,
            "fallback_from": fallback_from,
            "original_symbol": original_symbol,
            "akshare_symbol": akshare_symbol,
            "start_date": str(params.get("start_date", "")),
            "end_date": str(params.get("end_date", "")),
            "year": year,
            "month": month,
            "min_date": min_date,
            "max_date": max_date,
            "rows": int(len(month_df)),
            "created_at": datetime.now(UTC).isoformat(),
            "updated_at": datetime.now(UTC).isoformat(),
        }
        try:
            dp, mp = write_raw_partition(output_root, family, used_api_name, partition, month_df, metadata)
            part_status = "success"
            part_err_type = ""
            part_err_msg = ""
        except FileExistsError as exists_exc:
            dp = Path(output_root) / "data" / "raw" / "akshare" / family / used_api_name / f"symbol={symbol_value}" / f"adjust={adjust_label}" / f"year={year}" / f"month={month}" / "data.parquet"
            mp = dp.with_name("metadata.json")
            part_status = "already_exists"
            part_err_type = "FileExistsError"
            part_err_msg = str(exists_exc)
        task_rows.append(
            {
                "dataset_name": "raw_source_api",
                "partition_json": json.dumps(partition, ensure_ascii=False, sort_keys=True),
                "params_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
                "source_family": family,
                "api_name": used_api_name,
                "requested_api_name": requested_api_name,
                "actual_api_name": used_api_name,
                "fallback_from": fallback_from,
                "original_symbol": original_symbol,
                "akshare_symbol": akshare_symbol,
                "year": year,
                "month": month,
                "min_date": min_date,
                "max_date": max_date,
                "status": part_status,
                "rows": int(len(month_df)),
                "error_type": part_err_type,
                "error_message": part_err_msg,
                "output_path": str(dp),
                "metadata_path": str(mp),
                "started_at": started_at.isoformat(),
                "finished_at": datetime.now(UTC).isoformat(),
                "elapsed_sec": max((datetime.now(UTC) - started_at).total_seconds(), 0.0),
            }
        )
    return task_rows


def _run_single_coverage_task(
    output_root: str,
    family: str,
    api_name: str,
    params: dict[str, str],
    adapters: dict[str, AdapterFn],
    ak_module: object | None,
    include_disabled: bool,
) -> list[dict[str, object]]:
    started_at = datetime.now(UTC)
    status = "pending_adapter"
    err = ""
    err_type = ""
    n_rows = 0
    out_path = meta_path = ""

    if (family, api_name) in TEMP_DISABLED_APIS and not include_disabled:
        disabled_reason = str(
            DISABLED_API_METADATA.get((family, api_name), {}).get(
                "disabled_reason", "temporarily disabled for acquisition control"
            )
        )
        finished_at = datetime.now(UTC)
        return [{
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
        }]

    used_api_name = api_name
    fallback_from = ""
    requested_api_name = api_name
    original_symbol = ""
    if "symbol" in params:
        symbol_text = str(params.get("symbol", "")).strip().lower()
        if symbol_text.startswith(("sz", "sh", "bj")):
            symbol_text = symbol_text[2:]
        digits = "".join(ch for ch in symbol_text if ch.isdigit())
        original_symbol = digits.zfill(6) if digits else str(params.get("symbol", ""))
    akshare_symbol = ""
    primary_error = ""
    fallback_error = ""
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
            if api_name == "stock_zh_a_hist":
                daily_fn = adapters.get("stock_zh_a_daily") or (getattr(ak_module, "stock_zh_a_daily") if ak_module is not None and hasattr(ak_module, "stock_zh_a_daily") else None)
                try:
                    ret = _call_api_with_retry(fn, filtered)
                except Exception as primary_exc:
                    primary_error = f"{type(primary_exc).__name__}: {primary_exc}"
                    if daily_fn is None:
                        raise
                    fallback_from = "stock_zh_a_hist"
                    used_api_name = "stock_zh_a_daily"
                    akshare_symbol = _to_akshare_daily_symbol(original_symbol)
                    daily_params = {"symbol": akshare_symbol, "adjust": ""}
                    try:
                        ret = _call_api_with_retry(daily_fn, daily_params)
                    except Exception as daily_exc:
                        fallback_error = f"{type(daily_exc).__name__}: {daily_exc}"
                        raise
            else:
                ret = fn(**filtered)
            if ret is None:
                raise ValueError("none_result_from_api")
            raw = ret.raw if hasattr(ret, "raw") else ret
            if not isinstance(raw, pd.DataFrame):
                raw = pd.DataFrame(raw)
            if used_api_name == "stock_zh_a_daily":
                raw = _filter_daily_frame_by_range(raw, str(params.get("start_date", "")), str(params.get("end_date", "")))
            n_rows = len(raw)
            status = "empty" if raw.empty else "success"
            partition = dict(filtered) if filtered else {"api_name": api_name}
            if used_api_name == "stock_zh_a_daily":
                partition = {"symbol": akshare_symbol, "adjust": ""}
            try:
                if used_api_name in {"stock_zh_a_daily", "stock_zh_a_hist"} and family == "market_price":
                    symbol_value = akshare_symbol if used_api_name == "stock_zh_a_daily" else str(filtered.get("symbol", ""))
                    adjust_label = "none" if used_api_name == "stock_zh_a_daily" else str(filtered.get("adjust", "none") or "none")
                    task_rows = _write_market_price_month_partitions(
                        output_root, family, used_api_name, requested_api_name, fallback_from, original_symbol, akshare_symbol,
                        params, raw, symbol_value, adjust_label, started_at
                    )
                    if not task_rows:
                        status = "empty"
                    else:
                        return task_rows
                else:
                    dp, mp = write_raw_partition(
                        output_root, family, used_api_name, partition, raw,
                        {
                            "source_family": family,
                            "api_name": used_api_name,
                            "requested_api_name": requested_api_name,
                            "actual_api_name": used_api_name,
                            "fallback_from": fallback_from,
                            "original_symbol": original_symbol,
                            "akshare_symbol": akshare_symbol,
                            "params": filtered,
                            "status": status,
                            "row_count": n_rows,
                        }
                    )
                    out_path, meta_path = str(dp), str(mp)
            except Exception as write_exc:  # noqa: BLE001
                if api_name == "stock_individual_info_em":
                    out_path, meta_path = _fallback_csv_write(output_root, family, api_name, raw)
                    err = f"csv_fallback_after_write_error: {write_exc}"
                elif isinstance(write_exc, FileExistsError):
                    status = "already_exists"
                    err_type = "FileExistsError"
                    err = str(write_exc)
                else:
                    raise
    except Exception as exc:  # noqa: BLE001
        err = _normalize_error_message(api_name, str(exc))
        if primary_error or fallback_error:
            err = f"primary_error={primary_error or 'n/a'}; fallback_error={fallback_error or 'n/a'}"
        err_type = type(exc).__name__
        if _should_downgrade_to_empty(api_name, err):
            status = "empty"
            n_rows = 0
            err_type = "downgraded_to_empty"
        else:
            status = "failed"

    finished_at = datetime.now(UTC)
    return [{
        "dataset_name": "raw_source_api",
        "partition_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
        "params_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
        "source_family": family,
        "api_name": used_api_name,
        "requested_api_name": requested_api_name,
        "actual_api_name": used_api_name,
        "fallback_from": fallback_from,
        "original_symbol": original_symbol,
        "akshare_symbol": akshare_symbol,
        "status": status,
        "rows": n_rows,
        "error_type": err_type,
        "error_message": err,
        "output_path": out_path,
        "metadata_path": meta_path,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "elapsed_sec": max((finished_at - started_at).total_seconds(), 0.0),
    }]






def _run_task_with_signal_timeout(
    output_root: str,
    family: str,
    api_name: str,
    params: dict[str, str],
    adapters: dict[str, AdapterFn],
    ak_module: object | None,
    include_disabled: bool,
    task_timeout_sec: float,
) -> list[dict[str, object]]:
    started_at = datetime.now(UTC)
    def _handler(signum: int, frame: object) -> None:  # noqa: ARG001
        raise TimeoutError(f"task timeout after {task_timeout_sec} sec")

    old = signal.signal(signal.SIGALRM, _handler)
    signal.setitimer(signal.ITIMER_REAL, task_timeout_sec)
    try:
        return _run_single_coverage_task(output_root, family, api_name, params, adapters, ak_module, include_disabled)
    except TimeoutError as exc:
        finished_at = datetime.now(UTC)
        symbol = str(params.get("symbol", ""))
        return [{
            "dataset_name": "raw_source_api",
            "partition_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "params_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "source_family": family,
            "api_name": api_name,
            "requested_api_name": api_name,
            "actual_api_name": api_name,
            "fallback_from": "",
            "original_symbol": symbol,
            "akshare_symbol": "",
            "status": "timeout",
            "rows": 0,
            "error_type": "TimeoutError",
            "error_message": str(exc),
            "output_path": "",
            "metadata_path": "",
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "elapsed_sec": max((finished_at - started_at).total_seconds(), 0.0),
        }]
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old)

def _run_task_in_subprocess_with_timeout(
    output_root: str,
    family: str,
    api_name: str,
    params: dict[str, str],
    adapters: dict[str, AdapterFn],
    ak_module: object | None,
    include_disabled: bool,
    task_timeout_sec: float,
) -> list[dict[str, object]]:
    started_at = datetime.now(UTC)

    def _worker(q: mp.Queue) -> None:
        try:
            rows = _run_single_coverage_task(output_root, family, api_name, params, adapters, ak_module, include_disabled)
            q.put(("ok", rows))
        except Exception as exc:  # noqa: BLE001
            q.put(("err", f"{type(exc).__name__}: {exc}"))

    ctx = mp.get_context("fork")
    q: mp.Queue = ctx.Queue()
    proc = ctx.Process(target=_worker, args=(q,))
    proc.start()
    proc.join(task_timeout_sec)
    if proc.is_alive():
        proc.terminate()
        proc.join(5)
        finished_at = datetime.now(UTC)
        symbol = str(params.get("symbol", "")).strip().lower()
        if symbol.startswith(("sz", "sh", "bj")):
            symbol = symbol[2:]
        digits = "".join(ch for ch in symbol if ch.isdigit())
        original_symbol = digits.zfill(6) if digits else str(params.get("symbol", ""))
        akshare_symbol = _to_akshare_daily_symbol(original_symbol) if original_symbol else ""
        return [{
            "dataset_name": "raw_source_api",
            "partition_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "params_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "source_family": family,
            "api_name": api_name,
            "requested_api_name": api_name,
            "actual_api_name": api_name,
            "fallback_from": "",
            "original_symbol": original_symbol,
            "akshare_symbol": akshare_symbol,
            "status": "timeout",
            "rows": 0,
            "error_type": "TimeoutError",
            "error_message": f"task timeout after {task_timeout_sec} sec",
            "output_path": "",
            "metadata_path": "",
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "elapsed_sec": max((finished_at - started_at).total_seconds(), 0.0),
        }]
    if not q.empty():
        status, payload = q.get()
        if status == "ok":
            return payload
        finished_at = datetime.now(UTC)
        return [{
            "dataset_name": "raw_source_api",
            "partition_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "params_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "source_family": family,
            "api_name": api_name,
            "requested_api_name": api_name,
            "actual_api_name": api_name,
            "fallback_from": "",
            "original_symbol": str(params.get("symbol", "")),
            "akshare_symbol": "",
            "status": "failed",
            "rows": 0,
            "error_type": "SubprocessTaskError",
            "error_message": str(payload),
            "output_path": "",
            "metadata_path": "",
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "elapsed_sec": max((finished_at - started_at).total_seconds(), 0.0),
        }]
    return []

def run_raw_coverage_ingest(output_root: str, families: list[str], symbols: list[str] | None = None, index_symbols: list[str] | None = None, report_dates: list[str] | None = None, trade_dates: list[str] | None = None, industry_names: list[str] | None = None, concept_names: list[str] | None = None, start_date: str = "20100101", end_date: str = "20101231", adapter_map: dict[str, AdapterFn] | None = None, ak_module: object | None = None, request_sleep: float = 0.0, continue_on_error: bool = True, include_disabled: bool = False, max_workers: int = 2, selected_api_names: list[str] | None = None, resume: bool = False, universe_root: str | Path = "config/factor_sources/acquisition_universe", task_timeout_sec: float | None = None) -> dict:
    selected = {x.strip() for x in (selected_api_names or []) if x and x.strip()}
    selected_specs: list[dict[str, str]] = []
    for family in families:
        for spec in COVERAGE_API_SPECS.get(family, []):
            api_name = spec["api_name"]
            if selected and api_name not in selected:
                continue
            selected_specs.append(spec)

    required_modes = {spec["param_mode"] for spec in selected_specs}
    need_symbols = bool(required_modes & {"symbol_only", "symbol_range", "daily_symbol_range", "daily_symbol_range_hist", "symbol_report_date"})
    need_index_symbols = bool(required_modes & {"index_symbol_range", "index_symbol"})
    need_trade_dates = "trade_date" in required_modes
    need_report_dates = bool(required_modes & {"report_date", "symbol_report_date"})
    need_industry_names = bool(required_modes & {"industry_name_range", "industry_name"})
    need_concept_names = bool(required_modes & {"concept_name_range", "concept_name"})

    symbols = load_stock_symbols(symbols, universe_root=universe_root) if need_symbols else (symbols or [])
    index_symbols = load_index_symbols(index_symbols, universe_root=universe_root) if need_index_symbols else (index_symbols or [])
    trade_dates = build_trade_dates(start_date, end_date, trade_dates, universe_root=universe_root) if need_trade_dates else (trade_dates or [])
    report_dates = build_report_dates(start_date, end_date, report_dates) if need_report_dates else (report_dates or [])
    industry_names = load_industry_names(industry_names, universe_root=universe_root) if need_industry_names else (industry_names or [])
    concept_names = load_concept_names(concept_names, universe_root=universe_root) if need_concept_names else (concept_names or [])

    resume_keys: set[tuple[str, str, str]] = set()
    catalog_path = Path(output_root) / "raw_ingest_catalog.csv"
    if resume and catalog_path.exists():
        existing = pd.read_csv(catalog_path)
        for _, row in existing.iterrows():
            resume_keys.add((str(row.get("source_family", "")), str(row.get("api_name", "")), str(row.get("status", ""))))
    run_id = f"raw_official_{uuid.uuid4().hex[:8]}"
    adapters = adapter_map or {}
    op_dir = Path(output_root) / "_operation_review"
    op_dir.mkdir(parents=True, exist_ok=True)
    task_events_path = op_dir / "task_events.jsonl"
    if task_events_path.exists():
        task_events_path.unlink()
    rows: list[dict] = []
    tasks: list[tuple[str, str, dict[str, str]]] = []
    for family in families:
        for spec in COVERAGE_API_SPECS.get(family, []):
            api_name = spec["api_name"]
            if selected and api_name not in selected:
                continue
            params_list = _params_for_mode(spec["param_mode"], symbols, index_symbols, report_dates, trade_dates, industry_names, concept_names, start_date, end_date)
            for params in params_list:
                if resume and (family, api_name, "success") in resume_keys:
                    continue
                tasks.append((family, api_name, params))

    def _summarize_task(task_rows: list[dict[str, object]]) -> dict[str, object]:
        if not task_rows:
            return {"status": "failed", "error_type": "NoResult", "error_message": "empty task rows", "elapsed_sec": 0.0}
        status_order = {"failed": 4, "timeout": 3, "empty": 2, "pending_adapter": 1, "success": 0}
        best = sorted(task_rows, key=lambda r: status_order.get(str(r.get("status", "")), 5), reverse=True)[0]
        return {
            "run_id": run_id,
            "source_family": best.get("source_family", ""),
            "api_name": best.get("api_name", ""),
            "requested_api_name": best.get("requested_api_name", ""),
            "fallback_from": best.get("fallback_from", ""),
            "original_symbol": best.get("original_symbol", ""),
            "akshare_symbol": best.get("akshare_symbol", ""),
            "status": best.get("status", ""),
            "error_type": best.get("error_type", ""),
            "error_message": best.get("error_message", ""),
            "started_at": best.get("started_at", ""),
            "finished_at": best.get("finished_at", ""),
            "elapsed_sec": max(float(r.get("elapsed_sec", 0.0) or 0.0) for r in task_rows),
        }

    def _record_task_rows(task_rows: list[dict[str, object]]) -> None:
        for row in task_rows:
            row["run_id"] = run_id
            rows.append(row)
        event = _summarize_task(task_rows)
        with open(task_events_path, "a", encoding="utf-8") as ef:
            ef.write(json.dumps(event, ensure_ascii=False) + "\n")
        print(f"[task] {event['status']} family={event['source_family']} api={event['api_name']} symbol={event['original_symbol']} elapsed={event['elapsed_sec']}")

    def _execute_task(family: str, api_name: str, params: dict[str, str]) -> list[dict[str, object]]:
        if task_timeout_sec and task_timeout_sec > 0:
            return _run_task_in_subprocess_with_timeout(output_root, family, api_name, params, adapters, ak_module, include_disabled, float(task_timeout_sec))
        return _run_single_coverage_task(output_root, family, api_name, params, adapters, ak_module, include_disabled)

    if max_workers <= 1:
        for family, api_name, params in tasks:
            print(f"[task] start family={family} api={api_name} symbol={params.get('symbol','')}")
            task_rows = _execute_task(family, api_name, params)
            _record_task_rows(task_rows)
            if any(row.get("status") in {"failed", "timeout"} for row in task_rows) and not continue_on_error:
                break
            if request_sleep > 0:
                time.sleep(request_sleep)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = []
            for family, api_name, params in tasks:
                print(f"[task] start family={family} api={api_name} symbol={params.get('symbol','')}")
                futures.append(ex.submit(_execute_task, family, api_name, params))
            for fut in futures:
                task_rows = fut.result()
                _record_task_rows(task_rows)
                if request_sleep > 0:
                    time.sleep(request_sleep)

    out = Path(output_root)
    out.mkdir(parents=True, exist_ok=True)
    catalog_path = out / "raw_ingest_catalog.csv"
    summary_path = out / "raw_ingest_summary.csv"
    df = pd.DataFrame(rows)
    df.to_csv(catalog_path, index=False, encoding="utf-8-sig")
    timeout_df = df[df["status"] == "timeout"].copy() if not df.empty and "status" in df.columns else pd.DataFrame()
    timeout_df.to_csv(op_dir / "timeout_tasks.csv", index=False, encoding="utf-8-sig")
    recovery_df = df[df["status"].isin(["failed", "timeout"])].copy() if not df.empty and "status" in df.columns else pd.DataFrame()
    if not recovery_df.empty:
        keep_cols = [c for c in ["source_family", "api_name", "requested_api_name", "original_symbol", "akshare_symbol", "status", "error_type", "error_message"] if c in recovery_df.columns]
        recovery_df = recovery_df[keep_cols]
        recovery_df["start_date"] = start_date
        recovery_df["end_date"] = end_date
    recovery_df.to_csv(op_dir / "recovery_tasks.csv", index=False, encoding="utf-8-sig")
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


def run_raw_ingest_official(**kwargs: object) -> dict:
    """Official Stage-1 raw ingestion entrypoint (dataset-centered raw acquisition orchestration)."""
    return run_raw_coverage_ingest(**kwargs)

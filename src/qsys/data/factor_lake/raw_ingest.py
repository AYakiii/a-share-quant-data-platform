from __future__ import annotations

import csv
import json
import inspect
import time
import uuid
from collections import Counter
import multiprocessing as mp
import signal
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
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
from qsys.data.sources.akshare_jgdy_detail import JgdyDetailPageFailure, JgdyDetailSnapshotDrift, fetch_stock_jgdy_detail_em_resilient

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
        {"api_name": "stock_financial_analysis_indicator_em", "param_mode": "financial_indicator_em"},
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
    "market_sentiment": [
        {"api_name": "stock_fund_flow_concept", "param_mode": "none"},
        {"api_name": "stock_fund_flow_industry", "param_mode": "none"},
        {"api_name": "stock_hsgt_fund_flow_summary_em", "param_mode": "none"},
    ],
    "commodity_inventory": [
        {"api_name": "futures_inventory_em", "param_mode": "none"},
        {"api_name": "futures_comex_inventory", "param_mode": "none"},
        {"api_name": "futures_gfex_warehouse_receipt", "param_mode": "trade_date", "exchange": "GFEX"},
        {"api_name": "futures_shfe_warehouse_receipt", "param_mode": "trade_date", "exchange": "SHFE"},
        {"api_name": "futures_warehouse_receipt_czce", "param_mode": "trade_date", "exchange": "CZCE"},
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

P15P2_WAVE1_SOURCE_METADATA: dict[tuple[str, str], dict[str, str | bool]] = {
    ("market_sentiment", "stock_fund_flow_concept"): {
        "enabled": False,
        "default_enabled": False,
        "manual_review_required": True,
        "priority_tier": "P1.5",
        "data_theme": "concept_fund_flow",
        "acquisition_mode": "manual_selected_only",
        "disabled_category": "p15p2_wave1_recovered_source",
        "disabled_reason": "P1.5/P2 recovered candidate; disabled by default pending manual schema review",
    },
    ("market_sentiment", "stock_fund_flow_industry"): {
        "enabled": False,
        "default_enabled": False,
        "manual_review_required": True,
        "priority_tier": "P1.5",
        "data_theme": "industry_fund_flow",
        "acquisition_mode": "manual_selected_only",
        "disabled_category": "p15p2_wave1_recovered_source",
        "disabled_reason": "P1.5/P2 recovered candidate; disabled by default pending manual schema review",
    },
    ("market_sentiment", "stock_hsgt_fund_flow_summary_em"): {
        "enabled": False,
        "default_enabled": False,
        "manual_review_required": True,
        "priority_tier": "P1.5",
        "data_theme": "northbound_southbound_fund_flow",
        "acquisition_mode": "manual_selected_only",
        "disabled_category": "p15p2_wave1_recovered_source",
        "disabled_reason": "P1.5/P2 recovered candidate; disabled by default pending manual schema review",
    },
    ("commodity_inventory", "futures_inventory_em"): {
        "enabled": False,
        "default_enabled": False,
        "manual_review_required": True,
        "priority_tier": "P2",
        "data_theme": "commodity_inventory",
        "acquisition_mode": "manual_selected_only",
        "disabled_category": "p15p2_wave1_recovered_source",
        "disabled_reason": "P1.5/P2 recovered candidate; disabled by default pending manual schema review",
    },
    ("commodity_inventory", "futures_comex_inventory"): {
        "enabled": False,
        "default_enabled": False,
        "manual_review_required": True,
        "priority_tier": "P2",
        "data_theme": "global_commodity_inventory",
        "acquisition_mode": "manual_selected_only",
        "disabled_category": "p15p2_wave1_recovered_source",
        "disabled_reason": "P1.5/P2 recovered candidate; disabled by default pending manual schema review",
    },
    ("commodity_inventory", "futures_gfex_warehouse_receipt"): {
        "enabled": False,
        "default_enabled": False,
        "manual_review_required": True,
        "priority_tier": "P2",
        "data_theme": "commodity_warehouse_receipt",
        "exchange": "GFEX",
        "acquisition_mode": "manual_selected_only",
        "disabled_category": "p15p2_wave1_recovered_source",
        "disabled_reason": "P1.5/P2 recovered candidate; disabled by default pending manual schema review",
    },
    ("commodity_inventory", "futures_shfe_warehouse_receipt"): {
        "enabled": False,
        "default_enabled": False,
        "manual_review_required": True,
        "priority_tier": "P2",
        "data_theme": "commodity_warehouse_receipt",
        "exchange": "SHFE",
        "acquisition_mode": "manual_selected_only",
        "disabled_category": "p15p2_wave1_recovered_source",
        "disabled_reason": "P1.5/P2 recovered candidate; disabled by default pending manual schema review",
    },
    ("commodity_inventory", "futures_warehouse_receipt_czce"): {
        "enabled": False,
        "default_enabled": False,
        "manual_review_required": True,
        "priority_tier": "P2",
        "data_theme": "commodity_warehouse_receipt",
        "exchange": "CZCE",
        "acquisition_mode": "manual_selected_only",
        "disabled_category": "p15p2_wave1_recovered_source",
        "disabled_reason": "P1.5/P2 recovered candidate; disabled by default pending manual schema review",
    },
}

P15P2_WAVE1_APIS: set[tuple[str, str]] = set(P15P2_WAVE1_SOURCE_METADATA)
MANUAL_SELECTED_ONLY_APIS: set[tuple[str, str]] = {
    *P15P2_WAVE1_APIS,
    ("financial_fundamental", "stock_financial_analysis_indicator_em"),
    ("disclosure_ir", "stock_jgdy_detail_em"),
    ("event_ownership", "stock_gdfx_holding_analyse_em"),
}
WAREHOUSE_RECEIPT_EXCHANGES: dict[str, str] = {
    "futures_gfex_warehouse_receipt": "GFEX",
    "futures_shfe_warehouse_receipt": "SHFE",
    "futures_warehouse_receipt_czce": "CZCE",
}

TEMP_DISABLED_APIS: set[tuple[str, str]] = {
    *P15P2_WAVE1_APIS,
    ("market_price", "stock_zh_a_hist"),
    ("market_price", "stock_individual_info_em"),
    ("financial_fundamental", "stock_financial_analysis_indicator"),
    ("financial_fundamental", "stock_financial_analysis_indicator_em"),
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
    "default_enabled": False,
    "manual_review_required": True,
    "importance": "high",
    "acquisition_mode": "long_recovery_run",
    "disabled_category": "recovered_heavy_source",
    "disabled_reason": "controlled recovery run succeeded with approximately 123,880 rows in about 857 seconds; remains manual-only because of heavy source cost and elapsed time",
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
    "default_enabled": False,
    "manual_review_required": True,
    "disabled_category": "empty_review_source",
    "review_category": "parameter_schema_review",
    "legacy_policy": "legacy_start_year_required",
    "acquisition_mode": "legacy_direct_manual_only",
    "disabled_reason": "legacy Sina source preserved for controlled direct/manual calls; the official runner uses symbol_only and does not inject start_year, so default start_year=1900 may return empty when absent from the upstream year list; controlled direct/manual calls should provide a valid start_year such as 2020",
}
DISABLED_API_METADATA[("financial_fundamental", "stock_financial_analysis_indicator_em")] = {
    "enabled": False,
    "default_enabled": False,
    "manual_review_required": True,
    "priority_tier": "P1",
    "data_theme": "financial_analysis_indicator",
    "disabled_category": "recovered_financial_fundamental_source",
    "acquisition_mode": "manual_selected_only",
    "disabled_reason": "Eastmoney financial analysis indicator source is live-probed but remains manual-selected only pending schema review",
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
    "default_enabled": False,
    "manual_review_required": True,
    "importance": "high",
    "acquisition_mode": "long_detail_run",
    "disabled_category": "heavy_detail_source",
    "disabled_reason": "high-importance heavy detail source; deferred by default because real runs fan out into long detail crawls",
}

API_POLICY_METADATA: dict[tuple[str, str], dict[str, str | bool]] = {
    **DISABLED_API_METADATA,
    **P15P2_WAVE1_SOURCE_METADATA,
    ("disclosure_ir", "stock_zh_a_disclosure_relation_cninfo"): {
        "enabled": True,
        "default_enabled": True,
        "manual_review_required": True,
        "review_category": "schema_drift",
        "disabled_category": "empty_review",
        "disabled_reason": "schema/column drift can downgrade to auditable empty; keep under manual review",
    },
}


def _api_policy_metadata(family: str, api_name: str) -> dict[str, str | bool]:
    """Return acquisition policy metadata for sources with explicit review classifications."""
    return dict(API_POLICY_METADATA.get((family, api_name), {}))


def _attach_api_policy_metadata(row: dict[str, object], family: str, api_name: str) -> dict[str, object]:
    """Attach explicit acquisition policy metadata to an ingest catalog row when available."""
    for key, value in _api_policy_metadata(family, api_name).items():
        row.setdefault(key, value)
    return row

EXCLUDED_APIS: set[tuple[str, str]] = {("market_price", "stock_zh_a_daily")}


SNAPSHOT_RAW_PARTITION_APIS: set[tuple[str, str]] = {
    ("corporate_action", "stock_history_dividend"),
    ("trading_attention", "stock_dzjy_sctj"),
    ("trading_attention", "stock_dzjy_hyyybtj"),
}

TRADE_DATE_RANGE_CALL_APIS: set[tuple[str, str]] = {
    ("trading_attention", "stock_dzjy_mrtj"),
    ("trading_attention", "stock_dzjy_mrmx"),
}


def _effective_param_mode(family: str, api_name: str, param_mode: str) -> str:
    """Return the acquisition fan-out mode after raw partition collision repairs."""
    if (family, api_name) in SNAPSHOT_RAW_PARTITION_APIS:
        return "none"
    return param_mode


def _financial_indicator_em_symbol(symbol: str | int) -> str:
    """Convert A-share symbols to the suffixed Eastmoney form required by AkShare."""
    raw = str(symbol).strip().upper()
    if raw.endswith((".SZ", ".SH")):
        return raw
    if not (raw.isdigit() and len(raw) == 6):
        raise ValueError(f"unsupported EM financial indicator symbol: {symbol!r}; expected exactly six digits or .SZ/.SH suffix")
    if raw.startswith(("0", "3")):
        return f"{raw}.SZ"
    if raw.startswith("6"):
        return f"{raw}.SH"
    raise ValueError(f"unsupported EM financial indicator symbol prefix: {symbol!r}; only 0/3 => .SZ and 6 => .SH are verified")


def _financial_indicator_em_partition_label(indicator: str) -> str:
    """Return stable ASCII partition labels for Chinese EM indicator modes."""
    labels = {"按报告期": "report_period", "按单季度": "single_quarter"}
    if indicator not in labels:
        raise ValueError(f"unsupported EM financial indicator mode: {indicator!r}")
    return labels[indicator]


def _build_api_call_params(family: str, api_name: str, params: dict[str, str]) -> dict[str, str]:
    """Build adapter call params without losing logical partition keys."""
    call_params = dict(params)
    if (family, api_name) == ("financial_fundamental", "stock_financial_analysis_indicator_em") and "symbol" in params:
        call_params["symbol"] = _financial_indicator_em_symbol(params["symbol"])
    if (family, api_name) in TRADE_DATE_RANGE_CALL_APIS and "date" in params:
        trade_date = str(params["date"])
        call_params.pop("date", None)
        call_params.setdefault("start_date", trade_date)
        call_params.setdefault("end_date", trade_date)
    return call_params


def _build_raw_partition(family: str, api_name: str, params: dict[str, str], filtered: dict[str, str]) -> dict[str, str]:
    """Build a stable raw partition that preserves the logical acquisition key."""
    if (family, api_name) in SNAPSHOT_RAW_PARTITION_APIS:
        return {"snapshot": "latest"}
    if (family, api_name) == ("disclosure_ir", "stock_jgdy_detail_em"):
        since_date = "".join(ch for ch in str(params.get("date", "")) if ch.isdigit())
        return {"since_date": since_date}
    if (family, api_name) == ("financial_fundamental", "stock_financial_analysis_indicator_em"):
        symbol = str(params.get("symbol", "")).strip().upper()
        if symbol.endswith((".SZ", ".SH")):
            symbol = symbol[:-3]
        digits = "".join(ch for ch in symbol if ch.isdigit())
        logical_symbol = digits.zfill(6) if digits else symbol
        return {"symbol": logical_symbol, "indicator": _financial_indicator_em_partition_label(str(params.get("indicator", "")))}
    if "date" in params and (family, api_name) in TRADE_DATE_RANGE_CALL_APIS | {
        ("commodity_inventory", "futures_gfex_warehouse_receipt"),
        ("commodity_inventory", "futures_shfe_warehouse_receipt"),
        ("commodity_inventory", "futures_warehouse_receipt_czce"),
    }:
        partition = {"trade_date": str(params["date"])}
        if "symbol" in params:
            partition["symbol"] = str(params["symbol"])
        return partition
    if filtered:
        return dict(filtered)
    if (family, api_name) in P15P2_WAVE1_APIS:
        return {"snapshot": "latest"}
    return {"api_name": api_name}



def _canonical_json(value: object) -> str:
    """Serialize a stable JSON key for task/catalog identity fields."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _build_task_key(family: str, api_name: str, params: dict[str, str]) -> str:
    """Build the planned logical task key used for resume decisions.

    The key intentionally includes the requested parameter set in addition to the
    logical raw partition.  This keeps report-date partitions independent and
    prevents one market-price monthly output partition from proving that a wider
    multi-month request completed.
    """
    normalized_params = {str(k): str(v) for k, v in params.items()}
    return _canonical_json(
        {
            "source_family": family,
            "api_name": api_name,
            "partition": _build_raw_partition(family, api_name, normalized_params, {}),
            "params": normalized_params,
        }
    )


def _months_between(start_date: str, end_date: str) -> set[tuple[str, str]]:
    """Return inclusive (year, month) keys for compact YYYYMMDD date ranges."""
    start = pd.to_datetime(str(start_date), format="%Y%m%d", errors="coerce")
    end = pd.to_datetime(str(end_date), format="%Y%m%d", errors="coerce")
    if pd.isna(start) or pd.isna(end) or start > end:
        return set()
    months = pd.period_range(start=start.to_period("M"), end=end.to_period("M"), freq="M")
    return {(str(period.year), f"{period.month:02d}") for period in months}


def _completed_task_keys_from_catalog(existing: pd.DataFrame) -> set[str]:
    """Find exact planned-task keys that are complete in an existing catalog."""
    if existing.empty or "task_key_json" not in existing.columns or "status" not in existing.columns:
        return set()
    keyed = existing.copy()
    keyed["task_key_json"] = keyed["task_key_json"].fillna("").astype(str)
    keyed = keyed[keyed["task_key_json"] != ""]
    keyed = keyed[keyed["status"].astype(str).isin({"success", "already_exists"})]
    if keyed.empty:
        return set()

    completed: set[str] = set()
    market_api_names = {"stock_zh_a_hist", "stock_zh_a_daily"}
    for task_key, group in keyed.groupby("task_key_json", dropna=False):
        task_key_text = str(task_key)
        try:
            payload = json.loads(task_key_text)
        except json.JSONDecodeError:
            completed.add(task_key_text)
            continue
        family = str(payload.get("source_family", ""))
        api_name = str(payload.get("api_name", ""))
        params = payload.get("params", {}) if isinstance(payload.get("params", {}), dict) else {}
        if family == "market_price" and api_name in market_api_names and {"start_date", "end_date"} <= set(params):
            expected_months = _months_between(str(params.get("start_date", "")), str(params.get("end_date", "")))
            if expected_months and {"year", "month"} <= set(group.columns):
                actual_months = {
                    (str(row.get("year", "")).split(".")[0], str(row.get("month", "")).split(".")[0].zfill(2))
                    for _, row in group.iterrows()
                }
                if expected_months <= actual_months:
                    completed.add(task_key_text)
                continue
        completed.add(task_key_text)
    return completed


def _merge_catalog_rows(existing: pd.DataFrame, new_rows: list[dict[str, object]], resume: bool) -> pd.DataFrame:
    """Merge prior and new catalog rows without unbounded duplicate growth."""
    new_df = pd.DataFrame(new_rows)
    if not resume or existing.empty:
        merged = new_df
    elif new_df.empty or "task_key_json" not in new_df.columns:
        merged = existing.copy()
    else:
        rerun_keys = {str(k) for k in new_df["task_key_json"].fillna("").astype(str) if str(k)}
        prior = existing.copy()
        if "task_key_json" in prior.columns and rerun_keys:
            prior = prior[~prior["task_key_json"].fillna("").astype(str).isin(rerun_keys)]
        merged = pd.concat([prior, new_df], ignore_index=True, sort=False)
    if not merged.empty and {"task_key_json", "partition_json", "status"} <= set(merged.columns):
        merged = merged.drop_duplicates(subset=["task_key_json", "partition_json", "status"], keep="last")
    return merged.reset_index(drop=True)

def _normalize_raw_api_result(raw: object, api_name: str, params: dict[str, str]) -> pd.DataFrame:
    """Normalize an AkShare raw result while preserving source columns.

    Warehouse receipt APIs return dict[str, DataFrame].  Each non-empty child
    frame is copied, tagged with the dict key as product_key, and concatenated
    into one raw-compatible DataFrame.
    """
    if isinstance(raw, dict) and api_name in WAREHOUSE_RECEIPT_EXCHANGES:
        frames: list[pd.DataFrame] = []
        exchange = WAREHOUSE_RECEIPT_EXCHANGES[api_name]
        trade_date = str(params.get("date", ""))
        for product_key, subframe in raw.items():
            if subframe is None:
                continue
            frame = subframe.copy() if isinstance(subframe, pd.DataFrame) else pd.DataFrame(subframe)
            if frame.empty:
                continue
            frame["product_key"] = str(product_key)
            frame["source_api"] = api_name
            if exchange:
                frame["exchange"] = exchange
            if trade_date:
                frame["trade_date"] = trade_date
            frames.append(frame)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True, sort=False)
    if isinstance(raw, pd.DataFrame):
        return raw
    return pd.DataFrame(raw)


def _manual_selection_allows_disabled_source(family: str, api_name: str, manual_selected: bool) -> bool:
    """Allow only narrow manual-selected disabled sources to run by API name."""
    return manual_selected and (family, api_name) in MANUAL_SELECTED_ONLY_APIS


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
    if mode == "financial_indicator_em":
        return [
            {"symbol": symbol, "indicator": indicator}
            for symbol in symbols
            for indicator in ("按报告期", "按单季度")
        ]
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
    if api_name == "stock_jgdy_detail_em" and any(k in low for k in ["none", "not subscriptable", "keyerror", "indexerror", "attributeerror", "json", "expecting value"]):
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
    if api_name == "stock_jgdy_detail_em" and any(k in low for k in ["parser_empty_response", "none", "not subscriptable", "keyerror", "indexerror", "attributeerror"]):
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
    task_key_json = _build_task_key(family, requested_api_name, params)
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
                "task_key_json": task_key_json,
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
    manual_selected: bool = False,
) -> list[dict[str, object]]:
    started_at = datetime.now(UTC)
    task_key_json = _build_task_key(family, api_name, params)
    status = "pending_adapter"
    err = ""
    err_type = ""
    n_rows = 0
    out_path = meta_path = ""

    if (family, api_name) in TEMP_DISABLED_APIS and not include_disabled and not _manual_selection_allows_disabled_source(family, api_name, manual_selected):
        disabled_reason = str(
            DISABLED_API_METADATA.get((family, api_name), {}).get(
                "disabled_reason", "temporarily disabled for acquisition control"
            )
        )
        finished_at = datetime.now(UTC)
        return [_attach_api_policy_metadata({
            "dataset_name": "raw_source_api",
            "partition_json": json.dumps(_build_raw_partition(family, api_name, params, {}), ensure_ascii=False, sort_keys=True),
            "params_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "task_key_json": task_key_json,
            "source_family": family,
            "api_name": api_name,
            "requested_api_name": api_name,
            "actual_api_name": api_name,
            "fallback_from": "",
            "original_symbol": str(params.get("symbol", "")),
            "akshare_symbol": "",
            "status": "skipped",
            "rows": 0,
            "error_type": "default_disabled",
            "error_message": f"disabled_reason: {disabled_reason}",
            "output_path": "",
            "metadata_path": "",
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "elapsed_sec": max((finished_at - started_at).total_seconds(), 0.0),
        }, family, api_name)]

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
    partition = _build_raw_partition(family, api_name, params, {})
    try:
        fn = adapters.get(api_name) or (getattr(ak_module, api_name) if ak_module is not None and hasattr(ak_module, api_name) else None)
        if fn is None and (family, api_name) == ("disclosure_ir", "stock_jgdy_detail_em"):
            fn = fetch_stock_jgdy_detail_em_resilient
        if fn is None:
            status = "pending_adapter"
        else:
            call_params = _build_api_call_params(family, api_name, params)
            if (family, api_name) == ("financial_fundamental", "stock_financial_analysis_indicator_em"):
                akshare_symbol = str(call_params.get("symbol", ""))
            filtered = call_params
            try:
                sig = inspect.signature(fn)
                accepts_kwargs = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
                if not accepts_kwargs:
                    allowed = set(sig.parameters.keys())
                    filtered = {k: v for k, v in call_params.items() if k in allowed}
            except (TypeError, ValueError):
                filtered = call_params
            if (family, api_name) == ("disclosure_ir", "stock_jgdy_detail_em"):
                jgdy_request_get = adapters.get("__stock_jgdy_detail_em_request_get__") or adapters.get("stock_jgdy_detail_em_request_get")
                jgdy_config = adapters.get("__stock_jgdy_detail_em_config__") or {}
                if not isinstance(jgdy_config, dict):
                    jgdy_config = {}
                ret = fetch_stock_jgdy_detail_em_resilient(
                    date=str(params.get("date", "")),
                    output_root=output_root,
                    request_get=jgdy_request_get,
                    **jgdy_config,
                )
            elif api_name == "stock_zh_a_hist":
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
            raw = _normalize_raw_api_result(ret.raw if hasattr(ret, "raw") else ret, used_api_name, params)
            if used_api_name == "stock_zh_a_daily":
                raw = _filter_daily_frame_by_range(raw, str(params.get("start_date", "")), str(params.get("end_date", "")))
            n_rows = len(raw)
            status = "empty" if raw.empty else "success"
            partition = _build_raw_partition(family, api_name, params, filtered)
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
                            **_api_policy_metadata(family, requested_api_name),
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
        if isinstance(exc, (JgdyDetailPageFailure, JgdyDetailSnapshotDrift)):
            status = "failed"
        elif _should_downgrade_to_empty(api_name, err):
            status = "empty"
            n_rows = 0
            err_type = "downgraded_to_empty"
        else:
            status = "failed"

    finished_at = datetime.now(UTC)
    return [_attach_api_policy_metadata({
        "dataset_name": "raw_source_api",
        "partition_json": json.dumps(partition, ensure_ascii=False, sort_keys=True),
        "params_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
        "task_key_json": task_key_json,
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
    }, family, requested_api_name)]






def _run_task_with_signal_timeout(
    output_root: str,
    family: str,
    api_name: str,
    params: dict[str, str],
    adapters: dict[str, AdapterFn],
    ak_module: object | None,
    include_disabled: bool,
    task_timeout_sec: float,
    manual_selected: bool = False,
) -> list[dict[str, object]]:
    started_at = datetime.now(UTC)
    task_key_json = _build_task_key(family, api_name, params)
    def _handler(signum: int, frame: object) -> None:  # noqa: ARG001
        raise TimeoutError(f"task timeout after {task_timeout_sec} sec")

    old = signal.signal(signal.SIGALRM, _handler)
    signal.setitimer(signal.ITIMER_REAL, task_timeout_sec)
    try:
        return _run_single_coverage_task(output_root, family, api_name, params, adapters, ak_module, include_disabled, manual_selected)
    except TimeoutError as exc:
        finished_at = datetime.now(UTC)
        symbol = str(params.get("symbol", ""))
        return [{
            "dataset_name": "raw_source_api",
            "partition_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "params_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "task_key_json": task_key_json,
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
    manual_selected: bool = False,
    heartbeat_sec: float | None = None,
) -> list[dict[str, object]]:
    started_at = datetime.now(UTC)
    task_key_json = _build_task_key(family, api_name, params)

    def _normalize_symbols() -> tuple[str, str]:
        symbol = str(params.get("symbol", "")).strip().lower()
        if symbol.startswith(("sz", "sh", "bj")):
            symbol = symbol[2:]
        digits = "".join(ch for ch in symbol if ch.isdigit())
        original_symbol = digits.zfill(6) if digits else str(params.get("symbol", ""))
        akshare_symbol = _to_akshare_daily_symbol(original_symbol) if original_symbol else ""
        return original_symbol, akshare_symbol

    def _failed_row(error_type: str, error_message: str) -> dict[str, object]:
        finished_at = datetime.now(UTC)
        original_symbol, akshare_symbol = _normalize_symbols()
        return {
            "dataset_name": "raw_source_api",
            "partition_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "params_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "task_key_json": task_key_json,
            "source_family": family,
            "api_name": api_name,
            "requested_api_name": api_name,
            "actual_api_name": api_name,
            "fallback_from": "",
            "original_symbol": original_symbol,
            "akshare_symbol": akshare_symbol,
            "status": "timeout" if error_type == "TimeoutError" else "failed",
            "rows": 0,
            "error_type": error_type,
            "error_message": error_message,
            "output_path": "",
            "metadata_path": "",
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "elapsed_sec": max((finished_at - started_at).total_seconds(), 0.0),
        }

    def _worker(q: mp.Queue) -> None:
        try:
            rows = _run_single_coverage_task(output_root, family, api_name, params, adapters, ak_module, include_disabled, manual_selected)
            q.put(("ok", rows))
        except Exception as exc:  # noqa: BLE001
            q.put(("err", f"{type(exc).__name__}: {exc}"))

    ctx = mp.get_context("fork")
    q: mp.Queue = ctx.Queue()
    proc = ctx.Process(target=_worker, args=(q,))
    proc.start()
    heartbeat_every_sec = float(heartbeat_sec) if heartbeat_sec is not None else 0.0
    heartbeat_enabled = heartbeat_every_sec > 0
    deadline = time.monotonic() + float(task_timeout_sec)
    while proc.is_alive():
        remaining_sec = deadline - time.monotonic()
        if remaining_sec <= 0:
            break
        wait_sec = min(remaining_sec, heartbeat_every_sec if heartbeat_enabled else remaining_sec)
        proc.join(wait_sec)
        if proc.is_alive() and heartbeat_enabled:
            original_symbol, _ = _normalize_symbols()
            elapsed_sec = max((datetime.now(UTC) - started_at).total_seconds(), 0.0)
            print(
                "[heartbeat] "
                f"elapsed_sec={elapsed_sec:.1f} "
                f"source_family={family} "
                f"api_name={api_name} "
                f"original_symbol={original_symbol} "
                "pending_or_running_tasks=1"
            )
    if proc.is_alive():
        proc.terminate()
        proc.join(5)
        return [_failed_row("TimeoutError", f"task timeout after {task_timeout_sec} sec")]
    if not q.empty():
        status, payload = q.get()
        if status == "ok":
            return payload
        return [_failed_row("SubprocessTaskError", str(payload))]
    return [_failed_row("NoResult", "subprocess exited without queue payload")]

def _run_raw_coverage_ingest_duplicate_legacy(output_root: str, families: list[str], symbols: list[str] | None = None, index_symbols: list[str] | None = None, report_dates: list[str] | None = None, trade_dates: list[str] | None = None, industry_names: list[str] | None = None, concept_names: list[str] | None = None, start_date: str = "20100101", end_date: str = "20101231", adapter_map: dict[str, AdapterFn] | None = None, ak_module: object | None = None, request_sleep: float = 0.0, continue_on_error: bool = True, include_disabled: bool = False, max_workers: int = 2, selected_api_names: list[str] | None = None, resume: bool = False, universe_root: str | Path = "config/factor_sources/acquisition_universe", task_timeout_sec: float | None = None, task_retry_attempts: int = 0, task_retry_sleep_sec: float = 0.0, task_retry_backoff: float = 1.0, task_retry_jitter_sec: float = 0.0, heartbeat_sec: float | None = None) -> dict:
    selected = {x.strip() for x in (selected_api_names or []) if x and x.strip()}
    selected_specs: list[dict[str, str]] = []
    for family in families:
        for spec in COVERAGE_API_SPECS.get(family, []):
            api_name = spec["api_name"]
            if selected and api_name not in selected:
                continue
            selected_specs.append(spec)

    required_modes = {_effective_param_mode(family, spec["api_name"], spec["param_mode"]) for family in families for spec in COVERAGE_API_SPECS.get(family, []) if not selected or spec["api_name"] in selected}
    need_symbols = bool(required_modes & {"symbol_only", "symbol_range", "daily_symbol_range", "daily_symbol_range_hist", "symbol_report_date", "financial_indicator_em"})
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

    catalog_path = Path(output_root) / "raw_ingest_catalog.csv"
    existing_catalog = pd.read_csv(catalog_path) if resume and catalog_path.exists() else pd.DataFrame()
    completed_task_keys = _completed_task_keys_from_catalog(existing_catalog) if resume else set()
    run_id = f"raw_official_{uuid.uuid4().hex[:8]}"
    adapters = adapter_map or {}
    op_dir = Path(output_root) / "_operation_review"
    op_dir.mkdir(parents=True, exist_ok=True)
    task_events_path = op_dir / "task_events.jsonl"
    if task_events_path.exists() and not resume:
        task_events_path.unlink()
    rows: list[dict] = []
    tasks: list[tuple[str, str, dict[str, str]]] = []
    heartbeat_every_sec = float(heartbeat_sec) if heartbeat_sec is not None else 0.0
    heartbeat_enabled = heartbeat_every_sec > 0
    heartbeat_start = time.time()
    completed_tasks = 0
    heartbeat_every_sec = float(heartbeat_sec) if heartbeat_sec is not None else 0.0
    heartbeat_enabled = heartbeat_every_sec > 0
    heartbeat_start = time.time()
    completed_tasks = 0
    heartbeat_every_sec = float(heartbeat_sec) if heartbeat_sec is not None else 0.0
    heartbeat_enabled = heartbeat_every_sec > 0
    heartbeat_start = time.time()
    completed_tasks = 0
    heartbeat_every_sec = float(heartbeat_sec) if heartbeat_sec is not None else 0.0
    heartbeat_enabled = heartbeat_every_sec > 0
    heartbeat_start = time.time()
    completed_tasks = 0
    heartbeat_every_sec = float(heartbeat_sec) if heartbeat_sec is not None else 0.0
    heartbeat_enabled = heartbeat_every_sec > 0
    heartbeat_start = time.time()
    completed_tasks = 0
    heartbeat_every_sec = float(heartbeat_sec) if heartbeat_sec is not None else 0.0
    heartbeat_enabled = heartbeat_every_sec > 0
    heartbeat_start = time.time()
    last_heartbeat_at = heartbeat_start
    total_tasks = 0
    completed_tasks = 0
    for family in families:
        for spec in COVERAGE_API_SPECS.get(family, []):
            api_name = spec["api_name"]
            if selected and api_name not in selected:
                continue
            param_mode = _effective_param_mode(family, api_name, spec["param_mode"])
            params_list = _params_for_mode(param_mode, symbols, index_symbols, report_dates, trade_dates, industry_names, concept_names, start_date, end_date)
            for params in params_list:
                task_key_json = _build_task_key(family, api_name, params)
                if resume and task_key_json in completed_task_keys:
                    continue
                tasks.append((family, api_name, params))
    total_tasks = len(tasks)

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
            "task_key_json": best.get("task_key_json", ""),
            "status": best.get("status", ""),
            "error_type": best.get("error_type", ""),
            "error_message": best.get("error_message", ""),
            "started_at": best.get("started_at", ""),
            "finished_at": best.get("finished_at", ""),
            "elapsed_sec": max(float(r.get("elapsed_sec", 0.0) or 0.0) for r in task_rows),
        }

    def _record_task_rows(task_rows: list[dict[str, object]]) -> None:
        nonlocal completed_tasks
        for row in task_rows:
            row["run_id"] = run_id
            rows.append(row)
        event = _summarize_task(task_rows)
        with open(task_events_path, "a", encoding="utf-8") as ef:
            ef.write(json.dumps(event, ensure_ascii=False) + "\n")
        print(f"[task] {event['status']} family={event['source_family']} api={event['api_name']} symbol={event['original_symbol']} elapsed={event['elapsed_sec']}")
        completed_tasks += 1
        if heartbeat_enabled:
            now = time.time()
            if now - last_heartbeat_at >= heartbeat_every_sec:
                status_counts = Counter(str(r.get("status", "")) for r in rows)
                pending_or_running_tasks = max(total_tasks - completed_tasks, 0)
                elapsed_sec = max(now - heartbeat_start, 0.0)
                print(
                    "[heartbeat] "
                    f"elapsed_sec={elapsed_sec:.1f} "
                    f"total_tasks={total_tasks} "
                    f"completed_tasks={completed_tasks} "
                    f"pending_or_running_tasks={pending_or_running_tasks} "
                    f"status_counts={dict(status_counts)}"
                )
                last_heartbeat_at = now

    task_attempt_records: list[dict[str, object]] = []

    def _is_retryable_rows(task_rows: list[dict[str, object]]) -> bool:
        if not task_rows:
            return False
        statuses = {str(r.get("status", "")) for r in task_rows}
        if statuses & {"success", "empty", "already_exists"}:
            return False
        et = " ".join(str(r.get("error_type", "")) for r in task_rows)
        em = " ".join(str(r.get("error_message", "")) for r in task_rows).lower()
        retry_type_hits = ["timeouterror", "connectionerror", "remotedisconnected", "readtimeout", "jsondecodeerror"]
        retry_msg_hits = ["response ended prematurely", "network_unstable_retry", "timeout", "remote", "connection", "read timed out", "expecting value"]
        return any(x in et.lower() for x in retry_type_hits) or any(x in em for x in retry_msg_hits)

    def _execute_task_once(family: str, api_name: str, params: dict[str, str]) -> list[dict[str, object]]:
        if task_timeout_sec and task_timeout_sec > 0:
            if max_workers > 1 and float(task_timeout_sec) >= 5.0:
                return _run_single_coverage_task(output_root, family, api_name, params, adapters, ak_module, include_disabled, api_name in selected)
            return _run_task_in_subprocess_with_timeout(output_root, family, api_name, params, adapters, ak_module, include_disabled, float(task_timeout_sec), api_name in selected, heartbeat_sec)
        return _run_single_coverage_task(output_root, family, api_name, params, adapters, ak_module, include_disabled, api_name in selected)

    def _execute_task(family: str, api_name: str, params: dict[str, str]) -> list[dict[str, object]]:
        max_attempts = max(0, int(task_retry_attempts)) + 1
        last_rows: list[dict[str, object]] = []
        for attempt_no in range(1, max_attempts + 1):
            rows_now = _execute_task_once(family, api_name, params)
            for r in rows_now:
                task_attempt_records.append({
                    "run_id": run_id,
                    "source_family": r.get("source_family", ""),
                    "api_name": r.get("api_name", ""),
                    "requested_api_name": r.get("requested_api_name", ""),
                    "actual_api_name": r.get("actual_api_name", ""),
                    "original_symbol": r.get("original_symbol", ""),
                    "akshare_symbol": r.get("akshare_symbol", ""),
                    "task_key_json": r.get("task_key_json", ""),
                    "attempt_no": attempt_no,
                    "status": r.get("status", ""),
                    "error_type": r.get("error_type", ""),
                    "error_message": r.get("error_message", ""),
                    "started_at": r.get("started_at", ""),
                    "finished_at": r.get("finished_at", ""),
                    "elapsed_sec": r.get("elapsed_sec", 0),
                })
            last_rows = rows_now
            if not _is_retryable_rows(rows_now):
                return rows_now
            if attempt_no >= max_attempts:
                if int(task_retry_attempts) > 0:
                    for r in rows_now:
                        r["error_message"] = f"{r.get('error_message','')}; attempts_used={max_attempts}"
                return rows_now
            sleep_sec = float(task_retry_sleep_sec) * (float(task_retry_backoff) ** (attempt_no - 1))
            if task_retry_jitter_sec > 0:
                sleep_sec += float(task_retry_jitter_sec)
            if sleep_sec > 0:
                time.sleep(sleep_sec)
        return last_rows

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
            pending: set[object] = set()
            for family, api_name, params in tasks:
                print(f"[task] start family={family} api={api_name} symbol={params.get('symbol','')}")
                pending.add(ex.submit(_execute_task, family, api_name, params))
            while pending:
                wait_timeout = heartbeat_every_sec if heartbeat_enabled else None
                done, pending = wait(pending, timeout=wait_timeout, return_when=FIRST_COMPLETED)
                if not done:
                    now = time.time()
                    status_counts = Counter(str(r.get("status", "")) for r in rows)
                    pending_or_running_tasks = max(total_tasks - completed_tasks, 0)
                    elapsed_sec = max(now - heartbeat_start, 0.0)
                    print(
                        "[heartbeat] "
                        f"elapsed_sec={elapsed_sec:.1f} "
                        f"total_tasks={total_tasks} "
                        f"completed_tasks={completed_tasks} "
                        f"pending_or_running_tasks={pending_or_running_tasks} "
                        f"status_counts={dict(status_counts)}"
                    )
                    last_heartbeat_at = now
                    continue
                for fut in done:
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
    if api_name == "stock_jgdy_detail_em" and any(k in low for k in ["none", "not subscriptable", "keyerror", "indexerror", "attributeerror", "json", "expecting value"]):
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
    if api_name == "stock_jgdy_detail_em" and any(k in low for k in ["parser_empty_response", "none", "not subscriptable", "keyerror", "indexerror", "attributeerror"]):
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
    task_key_json = _build_task_key(family, requested_api_name, params)
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
                "task_key_json": task_key_json,
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
    manual_selected: bool = False,
) -> list[dict[str, object]]:
    started_at = datetime.now(UTC)
    task_key_json = _build_task_key(family, api_name, params)
    status = "pending_adapter"
    err = ""
    err_type = ""
    n_rows = 0
    out_path = meta_path = ""

    if (family, api_name) in TEMP_DISABLED_APIS and not include_disabled and not _manual_selection_allows_disabled_source(family, api_name, manual_selected):
        disabled_reason = str(
            DISABLED_API_METADATA.get((family, api_name), {}).get(
                "disabled_reason", "temporarily disabled for acquisition control"
            )
        )
        finished_at = datetime.now(UTC)
        return [_attach_api_policy_metadata({
            "dataset_name": "raw_source_api",
            "partition_json": json.dumps(_build_raw_partition(family, api_name, params, {}), ensure_ascii=False, sort_keys=True),
            "params_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "task_key_json": task_key_json,
            "source_family": family,
            "api_name": api_name,
            "requested_api_name": api_name,
            "actual_api_name": api_name,
            "fallback_from": "",
            "original_symbol": str(params.get("symbol", "")),
            "akshare_symbol": "",
            "status": "skipped",
            "rows": 0,
            "error_type": "default_disabled",
            "error_message": f"disabled_reason: {disabled_reason}",
            "output_path": "",
            "metadata_path": "",
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "elapsed_sec": max((finished_at - started_at).total_seconds(), 0.0),
        }, family, api_name)]

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
    partition = _build_raw_partition(family, api_name, params, {})
    try:
        fn = adapters.get(api_name) or (getattr(ak_module, api_name) if ak_module is not None and hasattr(ak_module, api_name) else None)
        if fn is None and (family, api_name) == ("disclosure_ir", "stock_jgdy_detail_em"):
            fn = fetch_stock_jgdy_detail_em_resilient
        if fn is None:
            status = "pending_adapter"
        else:
            call_params = _build_api_call_params(family, api_name, params)
            if (family, api_name) == ("financial_fundamental", "stock_financial_analysis_indicator_em"):
                akshare_symbol = str(call_params.get("symbol", ""))
            filtered = call_params
            try:
                sig = inspect.signature(fn)
                accepts_kwargs = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
                if not accepts_kwargs:
                    allowed = set(sig.parameters.keys())
                    filtered = {k: v for k, v in call_params.items() if k in allowed}
            except (TypeError, ValueError):
                filtered = call_params
            if (family, api_name) == ("disclosure_ir", "stock_jgdy_detail_em"):
                jgdy_request_get = adapters.get("__stock_jgdy_detail_em_request_get__") or adapters.get("stock_jgdy_detail_em_request_get")
                jgdy_config = adapters.get("__stock_jgdy_detail_em_config__") or {}
                if not isinstance(jgdy_config, dict):
                    jgdy_config = {}
                ret = fetch_stock_jgdy_detail_em_resilient(
                    date=str(params.get("date", "")),
                    output_root=output_root,
                    request_get=jgdy_request_get,
                    **jgdy_config,
                )
            elif api_name == "stock_zh_a_hist":
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
            raw = _normalize_raw_api_result(ret.raw if hasattr(ret, "raw") else ret, used_api_name, params)
            if used_api_name == "stock_zh_a_daily":
                raw = _filter_daily_frame_by_range(raw, str(params.get("start_date", "")), str(params.get("end_date", "")))
            n_rows = len(raw)
            status = "empty" if raw.empty else "success"
            partition = _build_raw_partition(family, api_name, params, filtered)
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
                            **_api_policy_metadata(family, requested_api_name),
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
        if isinstance(exc, (JgdyDetailPageFailure, JgdyDetailSnapshotDrift)):
            status = "failed"
        elif _should_downgrade_to_empty(api_name, err):
            status = "empty"
            n_rows = 0
            err_type = "downgraded_to_empty"
        else:
            status = "failed"

    finished_at = datetime.now(UTC)
    return [_attach_api_policy_metadata({
        "dataset_name": "raw_source_api",
        "partition_json": json.dumps(partition, ensure_ascii=False, sort_keys=True),
        "params_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
        "task_key_json": task_key_json,
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
    }, family, requested_api_name)]






def _run_task_with_signal_timeout(
    output_root: str,
    family: str,
    api_name: str,
    params: dict[str, str],
    adapters: dict[str, AdapterFn],
    ak_module: object | None,
    include_disabled: bool,
    task_timeout_sec: float,
    manual_selected: bool = False,
) -> list[dict[str, object]]:
    started_at = datetime.now(UTC)
    task_key_json = _build_task_key(family, api_name, params)
    def _handler(signum: int, frame: object) -> None:  # noqa: ARG001
        raise TimeoutError(f"task timeout after {task_timeout_sec} sec")

    old = signal.signal(signal.SIGALRM, _handler)
    signal.setitimer(signal.ITIMER_REAL, task_timeout_sec)
    try:
        return _run_single_coverage_task(output_root, family, api_name, params, adapters, ak_module, include_disabled, manual_selected)
    except TimeoutError as exc:
        finished_at = datetime.now(UTC)
        symbol = str(params.get("symbol", ""))
        return [{
            "dataset_name": "raw_source_api",
            "partition_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "params_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "task_key_json": task_key_json,
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
    manual_selected: bool = False,
    heartbeat_sec: float | None = None,
) -> list[dict[str, object]]:
    started_at = datetime.now(UTC)
    task_key_json = _build_task_key(family, api_name, params)

    def _normalize_symbols() -> tuple[str, str]:
        symbol = str(params.get("symbol", "")).strip().lower()
        if symbol.startswith(("sz", "sh", "bj")):
            symbol = symbol[2:]
        digits = "".join(ch for ch in symbol if ch.isdigit())
        original_symbol = digits.zfill(6) if digits else str(params.get("symbol", ""))
        akshare_symbol = _to_akshare_daily_symbol(original_symbol) if original_symbol else ""
        return original_symbol, akshare_symbol

    def _failed_row(error_type: str, error_message: str) -> dict[str, object]:
        finished_at = datetime.now(UTC)
        original_symbol, akshare_symbol = _normalize_symbols()
        return {
            "dataset_name": "raw_source_api",
            "partition_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "params_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
            "task_key_json": task_key_json,
            "source_family": family,
            "api_name": api_name,
            "requested_api_name": api_name,
            "actual_api_name": api_name,
            "fallback_from": "",
            "original_symbol": original_symbol,
            "akshare_symbol": akshare_symbol,
            "status": "timeout" if error_type == "TimeoutError" else "failed",
            "rows": 0,
            "error_type": error_type,
            "error_message": error_message,
            "output_path": "",
            "metadata_path": "",
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "elapsed_sec": max((finished_at - started_at).total_seconds(), 0.0),
        }

    def _worker(q: mp.Queue) -> None:
        try:
            rows = _run_single_coverage_task(output_root, family, api_name, params, adapters, ak_module, include_disabled, manual_selected)
            q.put(("ok", rows))
        except Exception as exc:  # noqa: BLE001
            q.put(("err", f"{type(exc).__name__}: {exc}"))

    ctx = mp.get_context("fork")
    q: mp.Queue = ctx.Queue()
    proc = ctx.Process(target=_worker, args=(q,))
    proc.start()
    heartbeat_every_sec = float(heartbeat_sec) if heartbeat_sec is not None else 0.0
    heartbeat_enabled = heartbeat_every_sec > 0
    deadline = time.monotonic() + float(task_timeout_sec)
    while proc.is_alive():
        remaining_sec = deadline - time.monotonic()
        if remaining_sec <= 0:
            break
        wait_sec = min(remaining_sec, heartbeat_every_sec if heartbeat_enabled else remaining_sec)
        proc.join(wait_sec)
        if proc.is_alive() and heartbeat_enabled:
            original_symbol, _ = _normalize_symbols()
            elapsed_sec = max((datetime.now(UTC) - started_at).total_seconds(), 0.0)
            print(
                "[heartbeat] "
                f"elapsed_sec={elapsed_sec:.1f} "
                f"source_family={family} "
                f"api_name={api_name} "
                f"original_symbol={original_symbol} "
                "pending_or_running_tasks=1"
            )
    if proc.is_alive():
        proc.terminate()
        proc.join(5)
        return [_failed_row("TimeoutError", f"task timeout after {task_timeout_sec} sec")]
    if not q.empty():
        status, payload = q.get()
        if status == "ok":
            return payload
        return [_failed_row("SubprocessTaskError", str(payload))]
    return [_failed_row("NoResult", "subprocess exited without queue payload")]

def run_raw_coverage_ingest(output_root: str, families: list[str], symbols: list[str] | None = None, index_symbols: list[str] | None = None, report_dates: list[str] | None = None, trade_dates: list[str] | None = None, industry_names: list[str] | None = None, concept_names: list[str] | None = None, start_date: str = "20100101", end_date: str = "20101231", adapter_map: dict[str, AdapterFn] | None = None, ak_module: object | None = None, request_sleep: float = 0.0, continue_on_error: bool = True, include_disabled: bool = False, max_workers: int = 2, selected_api_names: list[str] | None = None, resume: bool = False, universe_root: str | Path = "config/factor_sources/acquisition_universe", task_timeout_sec: float | None = None, task_retry_attempts: int = 0, task_retry_sleep_sec: float = 0.0, task_retry_backoff: float = 1.0, task_retry_jitter_sec: float = 0.0, heartbeat_sec: float | None = None) -> dict:
    selected = {x.strip() for x in (selected_api_names or []) if x and x.strip()}
    selected_specs: list[dict[str, str]] = []
    for family in families:
        for spec in COVERAGE_API_SPECS.get(family, []):
            api_name = spec["api_name"]
            if selected and api_name not in selected:
                continue
            selected_specs.append(spec)

    required_modes = {_effective_param_mode(family, spec["api_name"], spec["param_mode"]) for family in families for spec in COVERAGE_API_SPECS.get(family, []) if not selected or spec["api_name"] in selected}
    need_symbols = bool(required_modes & {"symbol_only", "symbol_range", "daily_symbol_range", "daily_symbol_range_hist", "symbol_report_date", "financial_indicator_em"})
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

    catalog_path = Path(output_root) / "raw_ingest_catalog.csv"
    existing_catalog = pd.read_csv(catalog_path) if resume and catalog_path.exists() else pd.DataFrame()
    completed_task_keys = _completed_task_keys_from_catalog(existing_catalog) if resume else set()
    run_id = f"raw_official_{uuid.uuid4().hex[:8]}"
    adapters = adapter_map or {}
    op_dir = Path(output_root) / "_operation_review"
    op_dir.mkdir(parents=True, exist_ok=True)
    task_events_path = op_dir / "task_events.jsonl"
    if task_events_path.exists() and not resume:
        task_events_path.unlink()
    rows: list[dict] = []
    tasks: list[tuple[str, str, dict[str, str]]] = []
    heartbeat_every_sec = float(heartbeat_sec) if heartbeat_sec is not None else 0.0
    heartbeat_enabled = heartbeat_every_sec > 0
    heartbeat_start = time.time()
    completed_tasks = 0
    for family in families:
        for spec in COVERAGE_API_SPECS.get(family, []):
            api_name = spec["api_name"]
            if selected and api_name not in selected:
                continue
            param_mode = _effective_param_mode(family, api_name, spec["param_mode"])
            params_list = _params_for_mode(param_mode, symbols, index_symbols, report_dates, trade_dates, industry_names, concept_names, start_date, end_date)
            for params in params_list:
                task_key_json = _build_task_key(family, api_name, params)
                if resume and task_key_json in completed_task_keys:
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
            "task_key_json": best.get("task_key_json", ""),
            "status": best.get("status", ""),
            "error_type": best.get("error_type", ""),
            "error_message": best.get("error_message", ""),
            "started_at": best.get("started_at", ""),
            "finished_at": best.get("finished_at", ""),
            "elapsed_sec": max(float(r.get("elapsed_sec", 0.0) or 0.0) for r in task_rows),
        }

    def _record_task_rows(task_rows: list[dict[str, object]]) -> None:
        nonlocal completed_tasks
        for row in task_rows:
            row["run_id"] = run_id
            rows.append(row)
        event = _summarize_task(task_rows)
        with open(task_events_path, "a", encoding="utf-8") as ef:
            ef.write(json.dumps(event, ensure_ascii=False) + "\n")
        print(f"[task] {event['status']} family={event['source_family']} api={event['api_name']} symbol={event['original_symbol']} elapsed={event['elapsed_sec']}")
        completed_tasks += 1

    task_attempt_records: list[dict[str, object]] = []

    def _is_retryable_rows(task_rows: list[dict[str, object]]) -> bool:
        if not task_rows:
            return False
        statuses = {str(r.get("status", "")) for r in task_rows}
        if statuses & {"success", "empty", "already_exists"}:
            return False
        et = " ".join(str(r.get("error_type", "")) for r in task_rows)
        em = " ".join(str(r.get("error_message", "")) for r in task_rows).lower()
        retry_type_hits = ["timeouterror", "connectionerror", "remotedisconnected", "readtimeout", "jsondecodeerror"]
        retry_msg_hits = ["response ended prematurely", "network_unstable_retry", "timeout", "remote", "connection", "read timed out", "expecting value"]
        return any(x in et.lower() for x in retry_type_hits) or any(x in em for x in retry_msg_hits)

    def _execute_task_once(family: str, api_name: str, params: dict[str, str]) -> list[dict[str, object]]:
        if task_timeout_sec and task_timeout_sec > 0:
            if max_workers > 1 and float(task_timeout_sec) >= 5.0:
                return _run_single_coverage_task(output_root, family, api_name, params, adapters, ak_module, include_disabled, api_name in selected)
            return _run_task_in_subprocess_with_timeout(output_root, family, api_name, params, adapters, ak_module, include_disabled, float(task_timeout_sec), api_name in selected, heartbeat_sec)
        return _run_single_coverage_task(output_root, family, api_name, params, adapters, ak_module, include_disabled, api_name in selected)

    def _execute_task(family: str, api_name: str, params: dict[str, str]) -> list[dict[str, object]]:
        max_attempts = max(0, int(task_retry_attempts)) + 1
        last_rows: list[dict[str, object]] = []
        for attempt_no in range(1, max_attempts + 1):
            rows_now = _execute_task_once(family, api_name, params)
            for r in rows_now:
                task_attempt_records.append({
                    "run_id": run_id,
                    "source_family": r.get("source_family", ""),
                    "api_name": r.get("api_name", ""),
                    "requested_api_name": r.get("requested_api_name", ""),
                    "actual_api_name": r.get("actual_api_name", ""),
                    "original_symbol": r.get("original_symbol", ""),
                    "akshare_symbol": r.get("akshare_symbol", ""),
                    "task_key_json": r.get("task_key_json", ""),
                    "attempt_no": attempt_no,
                    "status": r.get("status", ""),
                    "error_type": r.get("error_type", ""),
                    "error_message": r.get("error_message", ""),
                    "started_at": r.get("started_at", ""),
                    "finished_at": r.get("finished_at", ""),
                    "elapsed_sec": r.get("elapsed_sec", 0),
                })
            last_rows = rows_now
            if not _is_retryable_rows(rows_now):
                return rows_now
            if attempt_no >= max_attempts:
                if int(task_retry_attempts) > 0:
                    for r in rows_now:
                        r["error_message"] = f"{r.get('error_message','')}; attempts_used={max_attempts}"
                return rows_now
            sleep_sec = float(task_retry_sleep_sec) * (float(task_retry_backoff) ** (attempt_no - 1))
            if task_retry_jitter_sec > 0:
                sleep_sec += float(task_retry_jitter_sec)
            if sleep_sec > 0:
                time.sleep(sleep_sec)
        return last_rows

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
            if not heartbeat_enabled:
                for fut in futures:
                    task_rows = fut.result()
                    _record_task_rows(task_rows)
                    if request_sleep > 0:
                        time.sleep(request_sleep)
            else:
                pending = set(futures)
                while pending:
                    done, pending = wait(pending, timeout=heartbeat_every_sec, return_when=FIRST_COMPLETED)
                    if not done:
                        now = time.time()
                        status_counts = Counter(str(r.get("status", "")) for r in rows)
                        pending_or_running_tasks = max(len(tasks) - completed_tasks, 0)
                        elapsed_sec = max(now - heartbeat_start, 0.0)
                        print(
                            "[heartbeat] "
                            f"elapsed_sec={elapsed_sec:.1f} "
                            f"total_tasks={len(tasks)} "
                            f"completed_tasks={completed_tasks} "
                            f"pending_or_running_tasks={pending_or_running_tasks} "
                            f"status_counts={dict(status_counts)}"
                        )
                        continue
                    for fut in done:
                        task_rows = fut.result()
                        _record_task_rows(task_rows)
                        if request_sleep > 0:
                            time.sleep(request_sleep)

    out = Path(output_root)
    out.mkdir(parents=True, exist_ok=True)
    catalog_path = out / "raw_ingest_catalog.csv"
    summary_path = out / "raw_ingest_summary.csv"
    df = _merge_catalog_rows(existing_catalog, rows, resume)
    df.to_csv(catalog_path, index=False, encoding="utf-8-sig")
    timeout_df = df[df["status"] == "timeout"].copy() if not df.empty and "status" in df.columns else pd.DataFrame()
    timeout_df.to_csv(op_dir / "timeout_tasks.csv", index=False, encoding="utf-8-sig")
    recovery_df = df[df["status"].isin(["failed", "timeout"])].copy() if not df.empty and "status" in df.columns else pd.DataFrame()
    if not recovery_df.empty:
        keep_cols = [
            c
            for c in [
                "source_family",
                "api_name",
                "requested_api_name",
                "original_symbol",
                "akshare_symbol",
                "status",
                "error_type",
                "error_message",
                "partition_json",
                "params_json",
                "task_key_json",
            ]
            if c in recovery_df.columns
        ]
        recovery_df = recovery_df[keep_cols]
        recovery_df["start_date"] = start_date
        recovery_df["end_date"] = end_date
    recovery_df.to_csv(op_dir / "recovery_tasks.csv", index=False, encoding="utf-8-sig")
    attempts_path = op_dir / "task_attempts.csv"
    attempts_df = pd.DataFrame(task_attempt_records)
    if resume and attempts_path.exists():
        prior_attempts = pd.read_csv(attempts_path)
        attempts_df = pd.concat([prior_attempts, attempts_df], ignore_index=True, sort=False) if not attempts_df.empty else prior_attempts
    attempts_df.to_csv(attempts_path, index=False, encoding="utf-8-sig")
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
        "rows": df.to_dict("records"),
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

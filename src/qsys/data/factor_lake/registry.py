from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from qsys.data.factor_lake.schemas import SourceCase


@dataclass(frozen=True)
class DatasetSpec:
    """Executable ingest dataset spec used by raw_ingest in Phase 18A."""

    dataset: str
    source_family: str
    api_names: tuple[str, ...]
    partition_keys: tuple[str, ...]


@dataclass(frozen=True)
class SourceCapabilitySpec:
    """Coverage registry row for raw source capability planning and backfill mapping."""

    dataset_name: str
    source: str
    source_family: str
    api_name: str
    adapter_function: str
    frequency: str
    fetch_granularity: str
    partition_keys: tuple[str, ...]
    date_field: str
    symbol_field: str
    report_period_field: str
    announcement_date_field: str
    normalized_target: str
    factor_family_target: str
    lookahead_risk_fields: str
    priority: int
    notes: str


DATASET_REGISTRY: dict[str, DatasetSpec] = {
    "daily_bar_raw": DatasetSpec("daily_bar_raw", "market_price", ("stock_zh_a_hist",), ("symbol", "year")),
    "index_bar_raw": DatasetSpec("index_bar_raw", "index_market", ("stock_zh_index_hist_csindex",), ("index_symbol", "year")),
    "margin_detail_raw": DatasetSpec("margin_detail_raw", "margin_leverage", ("stock_margin_detail_sse", "stock_margin_detail_szse"), ("exchange", "trade_date")),
}


# Data-shape orientation: daily panel / index panel / event table / report-period snapshot / ownership-governance
SOURCE_CAPABILITY_REGISTRY: list[SourceCapabilitySpec] = [
    SourceCapabilitySpec("daily_bar_raw", "akshare", "market_price", "stock_zh_a_hist", "fetch_stock_zh_a_hist", "daily", "symbol+date_range", ("symbol", "year"), "日期", "股票代码", "", "", "daily_equity_panel", "technical_liquidity", "", 1, "Primary candidate. In Colab this endpoint may fail with RemoteDisconnected."),
    SourceCapabilitySpec("daily_bar_raw", "akshare", "market_price", "stock_zh_a_daily", "fetch_stock_zh_a_daily", "daily", "symbol+date_range", ("symbol", "year"), "date", "symbol(input->sz/sh code)", "", "", "daily_equity_panel", "technical_liquidity", "", 1, "Fallback candidate when stock_zh_a_hist is unstable; returns OHLCV+amount+outstanding_share+turnover."),
    SourceCapabilitySpec("index_bar_raw", "akshare", "index_market", "stock_zh_index_hist_csindex", "fetch_stock_zh_index_hist_csindex", "daily", "index_symbol+date_range", ("index_symbol", "year"), "日期", "指数代码", "", "", "daily_index_panel", "market_regime", "", 1, "Core benchmark/index panel source."),
    SourceCapabilitySpec("margin_detail_raw", "akshare", "margin_leverage", "stock_margin_detail_sse", "fetch_stock_margin_detail_sse", "daily", "trade_date", ("exchange", "trade_date"), "信用交易日期", "证券代码", "", "", "margin_trading_detail", "margin_leverage", "", 1, "Event-like daily leverage table (SSE)."),
    SourceCapabilitySpec("margin_detail_raw", "akshare", "margin_leverage", "stock_margin_detail_szse", "fetch_stock_margin_detail_szse", "daily", "trade_date", ("exchange", "trade_date"), "信用交易日期/input_date", "证券代码", "", "", "margin_trading_detail", "margin_leverage", "", 1, "Event-like daily leverage table (SZSE)."),
    SourceCapabilitySpec("financial_report_calendar_raw", "akshare", "disclosure_ir", "stock_yysj_em", "fetch_stock_yysj_em", "event", "symbol_or_market+date", ("date",), "实际披露时间", "股票代码", "", "实际披露时间", "financial_availability_calendar", "fundamental_quality", "", 2, "Report availability calendar (PIT anchor)."),
    SourceCapabilitySpec("earnings_guidance_raw", "akshare", "financial_fundamental", "stock_yjyg_em", "fetch_stock_yjyg_em", "event", "date", ("date",), "公告日期", "股票代码", "", "公告日期", "earnings_guidance_events", "earnings_expectation", "", 2, "Earnings pre-announcement event table."),
    SourceCapabilitySpec("sw_industry_hist_raw", "akshare", "industry_concept", "stock_industry_clf_hist_sw", "fetch_stock_industry_clf_hist_sw", "asof_event", "symbol+date_range", ("symbol",), "start_date", "symbol", "", "", "industry_membership_history", "industry_neutralization", "", 2, "Industry membership history (as-of)."),
    SourceCapabilitySpec("sw_index_hist_raw", "akshare", "industry_concept", "index_hist_sw", "fetch_index_hist_sw", "daily", "industry_index_symbol", ("symbol", "year"), "日期", "代码", "", "", "industry_state_panel", "industry_regime", "", 3, "Industry index OHLCV daily panel."),
    SourceCapabilitySpec("industry_change_raw", "akshare", "industry_concept", "stock_industry_change_cninfo", "fetch_stock_industry_change_cninfo", "event", "symbol+date_range", ("symbol", "date"), "变更日期", "证券代码", "", "变更日期", "industry_change_events", "classification_drift", "", 3, "Industry taxonomy change events."),
    SourceCapabilitySpec("holder_count_raw", "akshare", "event_ownership", "stock_zh_a_gdhs", "fetch_stock_zh_a_gdhs", "event", "market_snapshot", ("asof_date",), "公告日期", "股票代码", "报告期", "公告日期", "shareholder_count_events", "ownership_crowding", "", 2, "Ownership/governance family: shareholder count."),
    SourceCapabilitySpec("holder_detail_raw", "akshare", "event_ownership", "stock_zh_a_gdhs_detail_em", "fetch_stock_zh_a_gdhs_detail_em", "event", "symbol", ("symbol",), "公告日期", "股票代码", "报告期", "公告日期", "shareholder_detail_events", "ownership_structure", "", 3, "Ownership detail by symbol."),
    SourceCapabilitySpec("holder_analysis_raw", "akshare", "event_ownership", "stock_gdfx_holding_analyse_em", "fetch_stock_gdfx_holding_analyse_em", "event", "date", ("date",), "report_date/announcement_date", "股票代码", "报告期", "公告日期", "holder_concentration_events", "ownership_concentration", "", 3, "Ownership concentration analysis."),
    SourceCapabilitySpec("pledge_ratio_detail_raw", "akshare", "event_ownership", "stock_gpzy_pledge_ratio_detail_em", "fetch_stock_gpzy_pledge_ratio_detail_em", "event", "date", ("date",), "质押起始日/质押到期日", "股票代码", "", "公告日期", "equity_pledge_events", "governance_risk", "", 2, "Governance risk source."),
    SourceCapabilitySpec("dividend_plan_raw", "akshare", "corporate_action", "stock_fhps_em", "fetch_stock_fhps_em", "event", "snapshot", ("snapshot_date",), "预案公告日/除权除息日", "代码", "报告期", "最新公告日期", "dividend_action_events", "shareholder_return", "", 3, "Corporate action family."),
    SourceCapabilitySpec("dividend_history_raw", "akshare", "corporate_action", "stock_history_dividend", "fetch_stock_history_dividend", "event", "snapshot", ("snapshot_date",), "公告日期", "代码", "报告期", "公告日期", "dividend_history_events", "corporate_action", "", 3, "Dividend historical events."),
    SourceCapabilitySpec("restricted_release_detail_raw", "akshare", "corporate_action", "stock_restricted_release_detail_em", "fetch_stock_restricted_release_detail_em", "event", "snapshot", ("snapshot_date",), "解禁时间", "股票代码", "", "公告日期", "unlock_events", "supply_overhang", "上榜后1日,上榜后5日", 2, "Do not use post-event return fields as features."),
    SourceCapabilitySpec("block_trade_detail_raw", "akshare", "trading_attention", "stock_dzjy_mrmx", "fetch_stock_dzjy_mrmx", "event", "date", ("trade_date",), "交易日期", "证券代码", "", "交易日期", "block_trade_events", "flow_attention", "", 3, "Trading attention event table."),
    SourceCapabilitySpec("block_trade_summary_raw", "akshare", "trading_attention", "stock_dzjy_mrtj", "fetch_stock_dzjy_mrtj", "event", "date", ("trade_date",), "交易日期", "证券代码", "", "交易日期", "block_trade_summary", "flow_attention", "", 3, "Stock-level summary of block trades."),
    SourceCapabilitySpec("lhb_detail_raw", "akshare", "trading_attention", "stock_lhb_detail_em", "fetch_stock_lhb_detail_em", "event", "date_range", ("start_date", "end_date"), "上榜日", "代码", "", "上榜日", "lhb_abnormal_events", "abnormal_attention", "上榜后1日,上榜后2日,上榜后5日,上榜后10日", 2, "Blacklist post-event return fields."),
    SourceCapabilitySpec("institution_attention_raw", "akshare", "disclosure_ir", "stock_jgdy_tj_em", "fetch_stock_jgdy_tj_em", "event", "snapshot", ("snapshot_date",), "接待日期/公告日期", "代码", "", "公告日期", "institution_visit_events", "institution_attention", "", 3, "IR/disclosure attention signals."),
]


def list_datasets() -> list[str]:
    return list(DATASET_REGISTRY.keys())


def get_dataset_spec(dataset: str) -> DatasetSpec:
    if dataset not in DATASET_REGISTRY:
        raise KeyError(f"Unknown dataset: {dataset}")
    return DATASET_REGISTRY[dataset]


def registry_to_frame() -> pd.DataFrame:
    rows = []
    for spec in SOURCE_CAPABILITY_REGISTRY:
        row = asdict(spec)
        row["partition_keys"] = ",".join(spec.partition_keys)
        rows.append(row)
    return pd.DataFrame(rows)


def export_registry_csv(output_root: str | Path = ".") -> Path:
    out = Path(output_root) / "outputs" / "factor_lake_registry" / "source_capability_registry.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    registry_to_frame().to_csv(out, index=False, encoding="utf-8-sig")
    return out


def plan_partitions(dataset: str, **kwargs: Any) -> list[dict[str, str]]:
    if dataset in {"daily_bar_raw", "index_bar_raw"}:
        key = "symbol" if dataset == "daily_bar_raw" else "index_symbol"
        symbols = kwargs.get(key) or kwargs.get(f"{key}s")
        if isinstance(symbols, str):
            symbols = [symbols]
        if not symbols:
            raise ValueError(f"{dataset} requires {key} or {key}s")
        if "year" in kwargs:
            year = int(kwargs["year"])
            return [{key: s, "year": str(year)} for s in symbols]
        start = kwargs.get("start_date")
        end = kwargs.get("end_date")
        if start and end:
            return [{key: s, "start_date": str(start), "end_date": str(end)} for s in symbols]
        raise ValueError(f"{dataset} requires either year or start_date/end_date")
    if dataset == "margin_detail_raw":
        exchanges = kwargs.get("exchange") or kwargs.get("exchanges") or ["sse", "szse"]
        if isinstance(exchanges, str):
            exchanges = [exchanges]
        trade_dates = kwargs.get("trade_dates")
        if isinstance(trade_dates, str):
            trade_dates = [trade_dates]
        if trade_dates is None:
            td = kwargs.get("trade_date")
            trade_dates = [td] if td else []
        if not trade_dates:
            end = kwargs.get("end_date")
            start = kwargs.get("start_date", end)
            if not start:
                raise ValueError("margin_detail_raw requires trade_date(s) or start_date/end_date")
            sdt = date.fromisoformat(str(start).replace("/", "-"))
            edt = date.fromisoformat(str(end).replace("/", "-"))
            trade_dates = [(sdt + timedelta(days=i)).isoformat() for i in range((edt - sdt).days + 1)]
        return [{"exchange": ex.lower(), "trade_date": d.replace("-", "")} for ex in exchanges for d in trade_dates]
    raise KeyError(f"Unknown dataset: {dataset}")


FACTOR_SOURCE_REGISTRY: list[SourceCase] = [
    SourceCase("daily_bar_raw__hist__000001__2024q1", "market_price", "stock_zh_a_hist", {"symbol": "000001", "period": "daily", "start_date": "20240101", "end_date": "20240331", "adjust": ""}, "daily_bar_raw primary candidate"),
    SourceCase("daily_bar_raw__daily__000001__2024q1", "market_price", "stock_zh_a_daily", {"symbol": "sz000001", "start_date": "20240101", "end_date": "20240331", "adjust": ""}, "daily_bar_raw fallback candidate"),
    SourceCase("index_bar_raw__000300__2024q1", "index_market", "stock_zh_index_hist_csindex", {"symbol": "000300", "start_date": "20240101", "end_date": "20240331"}, "index bar probe"),
    SourceCase("margin_detail_raw__sse__20240329", "margin_leverage", "stock_margin_detail_sse", {"date": "20240329"}, "margin sse probe"),
    SourceCase("margin_detail_raw__szse__20240329", "margin_leverage", "stock_margin_detail_szse", {"date": "20240329"}, "margin szse probe"),
]


def filter_source_cases(cases: list[SourceCase], family: str | None = None, api_name: str | None = None, case_id: str | None = None, enabled_only: bool = False, max_cases: int | None = None) -> list[SourceCase]:
    selected: list[SourceCase] = []
    for c in cases:
        if family and c.source_family != family:
            continue
        if api_name and c.api_name != api_name:
            continue
        if case_id and c.case_id != case_id:
            continue
        if enabled_only and not c.enabled:
            continue
        selected.append(c)
    return selected[:max_cases] if max_cases else selected

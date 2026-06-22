"""Tushare local raw source registry.

The registry is the single place that maps source families to Tushare APIs.
"""
from __future__ import annotations

from qsys.data.sources.tushare_contracts import TushareSourceSpec

DAILY_FIELDS = (
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount",
)
DAILY_BASIC_FIELDS = ("ts_code", "trade_date", "turnover_rate", "turnover_rate_f", "volume_ratio", "total_share", "float_share", "free_share", "total_mv", "circ_mv")
MARGIN_DETAIL_FIELDS = (
    "ts_code",
    "trade_date",
    "rzye",
    "rzmre",
    "rzche",
    "rqyl",
    "rqmcl",
    "rqchl",
    "rqye",
    "rzrqye",
)
MONEYFLOW_FIELDS = (
    "ts_code",
    "trade_date",
    "buy_sm_vol",
    "buy_sm_amount",
    "sell_sm_vol",
    "sell_sm_amount",
    "buy_md_vol",
    "buy_md_amount",
    "sell_md_vol",
    "sell_md_amount",
    "buy_lg_vol",
    "buy_lg_amount",
    "sell_lg_vol",
    "sell_lg_amount",
    "buy_elg_vol",
    "buy_elg_amount",
    "sell_elg_vol",
    "sell_elg_amount",
    "net_mf_vol",
    "net_mf_amount",
    "trade_count",
)

TUSHARE_SOURCE_SPECS: tuple[TushareSourceSpec, ...] = (
    TushareSourceSpec(source_family="market_price", api_name="daily", fields=DAILY_FIELDS, calendar_mode="trading_days"),
    TushareSourceSpec(source_family="market_basic", api_name="daily_basic", fields=DAILY_BASIC_FIELDS, calendar_mode="trading_days"),
    TushareSourceSpec(source_family="market_flow", api_name="moneyflow", fields=MONEYFLOW_FIELDS, calendar_mode="trading_days"),
    TushareSourceSpec(source_family="margin_leverage", api_name="margin_detail", fields=MARGIN_DETAIL_FIELDS, calendar_mode="trading_days"),
    TushareSourceSpec(source_family="market_price_adjustment", api_name="adj_factor", fields=("ts_code", "trade_date", "adj_factor"), calendar_mode="trading_days"),
)


def source_specs_by_api() -> dict[str, TushareSourceSpec]:
    """Return legacy Tushare source specs keyed by API name.

    The production runner loads configs/tushare/source_registry.yaml via
    qsys.data.sources.tushare_source_registry by default. This function remains
    for compatibility with older callers and tests.
    """
    return {spec.api_name: spec for spec in TUSHARE_SOURCE_SPECS}

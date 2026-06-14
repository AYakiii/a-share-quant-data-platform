"""Tushare local raw source registry.

The registry is the single place that maps source families to Tushare APIs.
"""
from __future__ import annotations

from qsys.data.sources.tushare_contracts import TushareSourceSpec

TUSHARE_SOURCE_SPECS: tuple[TushareSourceSpec, ...] = (
    TushareSourceSpec(source_family="market_price", api_name="daily"),
    TushareSourceSpec(source_family="market_basic", api_name="daily_basic"),
    TushareSourceSpec(source_family="market_flow", api_name="moneyflow"),
    TushareSourceSpec(source_family="margin_leverage", api_name="margin_detail"),
)


def source_specs_by_api() -> dict[str, TushareSourceSpec]:
    """Return Tushare source specs keyed by API name."""
    return {spec.api_name: spec for spec in TUSHARE_SOURCE_SPECS}

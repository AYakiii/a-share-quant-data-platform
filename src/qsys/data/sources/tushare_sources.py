"""Tushare source registry skeleton.

No API is fetched in M0; the registry exists so future work can add sources
without encoding a particular universe run into module names.
"""
from __future__ import annotations

from qsys.data.sources.tushare_contracts import TushareSourceSpec

TUSHARE_SOURCE_SPECS: tuple[TushareSourceSpec, ...] = (
    TushareSourceSpec(source_family="market_price", api_name="daily"),
)

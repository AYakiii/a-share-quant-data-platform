from __future__ import annotations

import json
from pathlib import Path

import pytest

from qsys.data.sources.tushare_source_registry import load_tushare_source_specs, source_specs_by_api
from qsys.data.sources.tushare_sources import DAILY_BASIC_FIELDS, DAILY_FIELDS, MARGIN_DETAIL_FIELDS, MONEYFLOW_FIELDS


def test_current_four_apis_load_from_yaml() -> None:
    by_api = source_specs_by_api()
    assert {"daily", "daily_basic", "moneyflow", "margin_detail"}.issubset(set(by_api))
    assert all(by_api[api].production_enabled for api in ("daily", "daily_basic", "moneyflow", "margin_detail"))


def test_adj_factor_candidate_loads_from_yaml() -> None:
    by_api = source_specs_by_api()
    spec = by_api["adj_factor"]
    assert spec.source_family == "market_price_adjustment"
    assert spec.fields == ("ts_code", "trade_date", "adj_factor")
    assert spec.query_mode == "by_trade_date"
    assert spec.calendar_mode == "trading_days"
    assert spec.partition_key == "trade_date"
    assert spec.primary_key == ("ts_code", "trade_date")
    assert spec.universe_filter_mode == "ts_code"
    assert spec.compact_bucket == "year_from_trade_date"
    assert spec.status == "candidate"
    assert spec.production_enabled is False


def test_yaml_fields_match_existing_contracts_exactly() -> None:
    by_api = source_specs_by_api()
    assert by_api["daily"].fields == DAILY_FIELDS
    assert by_api["daily_basic"].fields == DAILY_BASIC_FIELDS
    assert by_api["moneyflow"].fields == MONEYFLOW_FIELDS
    assert by_api["margin_detail"].fields == MARGIN_DETAIL_FIELDS


def test_unsupported_query_mode_fails_loudly(tmp_path: Path) -> None:
    registry = tmp_path / "source_registry.yaml"
    registry.write_text(json.dumps({"sources": [{
        "source_family": "x",
        "api_name": "bad_api",
        "fields": ["ts_code", "trade_date"],
        "query_mode": "by_ts_code_range",
        "calendar_mode": "trading_days",
        "partition_key": "trade_date",
        "primary_key": ["ts_code", "trade_date"],
        "universe_filter_mode": "ts_code",
        "compact_bucket": "year_from_trade_date",
        "status": "candidate",
        "production_enabled": False,
    }]}), encoding="utf-8")
    with pytest.raises(NotImplementedError, match="unsupported Tushare query_mode"):
        load_tushare_source_specs(registry)

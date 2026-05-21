from __future__ import annotations

from qsys.data.warehouse.source_specs import (
    MARGIN_DETAIL_SPEC,
    STOCK_ZH_A_DAILY_SPEC,
    get_source_spec,
    list_source_specs,
    SOURCE_SPECS,
)


def test_get_source_spec_margin_detail_works() -> None:
    assert get_source_spec("margin_detail") is MARGIN_DETAIL_SPEC


def test_get_source_spec_stock_daily_works() -> None:
    assert get_source_spec("stock_zh_a_daily") is STOCK_ZH_A_DAILY_SPEC


def test_list_source_specs_contains_margin_and_stock_daily() -> None:
    names = list_source_specs()
    assert "margin_detail" in names
    assert "stock_zh_a_daily" in names
    assert "sw_industry_membership_rescue" in names
    assert "tradability_mask_v0" in names


def test_margin_detail_fetch_plan_stable_shape() -> None:
    parts = list(MARGIN_DETAIL_SPEC.build_fetch_plan(start_date="2024-01-01", end_date="2024-01-01", include_calendar_days=True, exchanges="sse"))
    assert len(parts) == 1
    assert set(parts[0].values.keys()) == {"exchange", "trade_date"}


def test_stock_daily_plan_and_deterministic_path(tmp_path) -> None:
    parts = list(STOCK_ZH_A_DAILY_SPEC.build_fetch_plan(symbols="000001,000002", start_date="2026-01-01", end_date="2026-01-10"))
    assert len(parts) == 2
    path = STOCK_ZH_A_DAILY_SPEC.build_raw_partition_path(tmp_path, parts[0])
    assert "stock_zh_a_daily" in str(path)
    assert "symbol=000001" in str(path)
    assert "start_date=2026-01-01_end_date=2026-01-10" in str(path)


def test_source_specs_exact_keys() -> None:
    assert {"margin_detail", "stock_zh_a_daily", "sw_industry_membership_rescue", "tradability_mask_v0"}.issubset(set(SOURCE_SPECS.keys()))

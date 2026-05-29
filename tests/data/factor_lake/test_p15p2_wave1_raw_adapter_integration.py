from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from qsys.data.factor_lake.registry import FACTOR_SOURCE_REGISTRY, SOURCE_CAPABILITY_REGISTRY
from qsys.data.factor_lake.raw_ingest import API_POLICY_METADATA, COVERAGE_API_SPECS, run_raw_coverage_ingest


WAVE1_APIS = {
    "stock_fund_flow_concept",
    "stock_fund_flow_industry",
    "stock_hsgt_fund_flow_summary_em",
    "futures_inventory_em",
    "futures_comex_inventory",
    "futures_gfex_warehouse_receipt",
    "futures_shfe_warehouse_receipt",
    "futures_warehouse_receipt_czce",
}


def test_wave1_apis_are_registered_manual_only_and_not_default_enabled() -> None:
    coverage_apis = {spec["api_name"] for specs in COVERAGE_API_SPECS.values() for spec in specs}
    capability_apis = {spec.api_name for spec in SOURCE_CAPABILITY_REGISTRY}
    source_cases = {case.api_name: case for case in FACTOR_SOURCE_REGISTRY if case.api_name in WAVE1_APIS}

    assert WAVE1_APIS <= coverage_apis
    assert WAVE1_APIS <= capability_apis
    assert WAVE1_APIS <= set(source_cases)
    assert all(not source_cases[api_name].enabled for api_name in WAVE1_APIS)
    for family, specs in COVERAGE_API_SPECS.items():
        for spec in specs:
            api_name = spec["api_name"]
            if api_name in WAVE1_APIS:
                policy = API_POLICY_METADATA[(family, api_name)]
                assert policy["default_enabled"] is False
                assert policy["manual_review_required"] is True


def test_ordinary_dataframe_wave1_source_runs_when_explicitly_selected(tmp_path: Path) -> None:
    def stock_fund_flow_concept() -> pd.DataFrame:
        return pd.DataFrame({"概念": ["人工智能"], "净流入": [100.5]})

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["market_sentiment"],
        selected_api_names=["stock_fund_flow_concept"],
        adapter_map={"stock_fund_flow_concept": stock_fund_flow_concept},
        max_workers=1,
    )

    [row] = out["rows"]
    assert row["status"] == "success"
    assert row["rows"] == 1
    assert row["source_family"] == "market_sentiment"
    assert row["api_name"] == "stock_fund_flow_concept"
    assert row["default_enabled"] is False
    assert row["manual_review_required"] is True
    assert row["priority_tier"] == "P1.5"
    assert row["data_theme"] == "concept_fund_flow"
    raw = pd.read_parquet(row["output_path"])
    assert list(raw.columns) == ["概念", "净流入"]
    metadata = json.loads(Path(row["metadata_path"]).read_text(encoding="utf-8"))
    assert metadata["source_family"] == "market_sentiment"
    assert metadata["requested_api_name"] == "stock_fund_flow_concept"
    assert metadata["status"] == "success"


def test_warehouse_receipt_dict_frames_are_flattened_with_product_exchange_and_trade_date(tmp_path: Path) -> None:
    def futures_shfe_warehouse_receipt(date: str) -> dict[str, pd.DataFrame]:
        return {
            "CU": pd.DataFrame({"仓库": ["上海", "广东"], "仓单": [1, 2]}),
            "AL": pd.DataFrame({"仓库": ["无锡"], "仓单": [3]}),
        }

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["commodity_inventory"],
        trade_dates=["20240102"],
        selected_api_names=["futures_shfe_warehouse_receipt"],
        adapter_map={"futures_shfe_warehouse_receipt": futures_shfe_warehouse_receipt},
        max_workers=1,
    )

    [row] = out["rows"]
    assert row["status"] == "success"
    assert row["rows"] == 3
    assert row["exchange"] == "SHFE"
    assert row["default_enabled"] is False
    assert json.loads(row["partition_json"]) == {"trade_date": "20240102"}
    assert "trade_date=20240102" in row["output_path"]
    raw = pd.read_parquet(row["output_path"])
    assert len(raw) == 3
    assert set(raw["product_key"]) == {"CU", "AL"}
    assert set(raw["exchange"]) == {"SHFE"}
    assert set(raw["trade_date"]) == {"20240102"}
    assert set(raw["source_api"]) == {"futures_shfe_warehouse_receipt"}
    assert {"仓库", "仓单"} <= set(raw.columns)


def test_non_warehouse_dict_result_uses_generic_dataframe_conversion(tmp_path: Path) -> None:
    def stock_fund_flow_industry() -> dict[str, list[object]]:
        return {"行业": ["银行"], "净流入": [88.0]}

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["market_sentiment"],
        selected_api_names=["stock_fund_flow_industry"],
        adapter_map={"stock_fund_flow_industry": stock_fund_flow_industry},
        max_workers=1,
    )

    [row] = out["rows"]
    assert row["status"] == "success"
    assert row["rows"] == 1
    raw = pd.read_parquet(row["output_path"])
    assert list(raw.columns) == ["行业", "净流入"]
    assert "product_key" not in raw.columns
    assert "exchange" not in raw.columns
    assert "trade_date" not in raw.columns
    assert "source_api" not in raw.columns


def test_empty_warehouse_receipt_dict_is_empty_not_success(tmp_path: Path) -> None:
    def futures_gfex_warehouse_receipt(date: str) -> dict[str, pd.DataFrame]:  # noqa: ARG001
        return {"SI": pd.DataFrame(), "LC": pd.DataFrame()}

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["commodity_inventory"],
        trade_dates=["20240102"],
        selected_api_names=["futures_gfex_warehouse_receipt"],
        adapter_map={"futures_gfex_warehouse_receipt": futures_gfex_warehouse_receipt},
        max_workers=1,
    )

    [row] = out["rows"]
    assert row["status"] == "empty"
    assert row["rows"] == 0
    assert row["output_path"]


def test_wave1_sources_are_skipped_by_default_without_explicit_selection_or_include_disabled(tmp_path: Path) -> None:
    calls: list[str] = []

    def stock_fund_flow_concept() -> pd.DataFrame:
        calls.append("stock_fund_flow_concept")
        return pd.DataFrame({"概念": ["人工智能"]})

    out = run_raw_coverage_ingest(
        output_root=str(tmp_path),
        families=["market_sentiment"],
        selected_api_names=["stock_fund_flow_concept"],
        adapter_map={"stock_fund_flow_concept": stock_fund_flow_concept},
        include_disabled=False,
        max_workers=1,
    )

    [selected_row] = out["rows"]
    assert selected_row["status"] == "success"
    assert calls == ["stock_fund_flow_concept"]

    calls.clear()
    default_out = run_raw_coverage_ingest(
        output_root=str(tmp_path / "default"),
        families=["market_sentiment"],
        adapter_map={"stock_fund_flow_concept": stock_fund_flow_concept},
        include_disabled=False,
        max_workers=1,
    )
    assert calls == []
    assert {row["status"] for row in default_out["rows"]} == {"skipped"}
    assert {row["error_type"] for row in default_out["rows"]} == {"default_disabled"}

from qsys.data.factor_lake.registry import (
    DATASET_REGISTRY,
    SOURCE_CAPABILITY_REGISTRY,
    export_registry_csv,
    plan_partitions,
    registry_to_frame,
)


def test_registry_contains_phase18a_datasets():
    assert {"daily_bar_raw", "index_bar_raw", "margin_detail_raw"}.issubset(DATASET_REGISTRY.keys())


def test_partition_planning_by_dataset():
    assert plan_partitions("daily_bar_raw", symbol="000001", year=2024) == [{"symbol": "000001", "year": "2024"}]
    assert plan_partitions("index_bar_raw", index_symbol="000300", start_date="2024-01-01", end_date="2024-01-31") == [
        {"index_symbol": "000300", "start_date": "2024-01-01", "end_date": "2024-01-31"}
    ]
    p3 = plan_partitions("margin_detail_raw", exchanges=["sse", "szse"], trade_date="2024-03-29")
    assert len(p3) == 2


def test_source_capability_registry_family_coverage_and_export(tmp_path):
    families = {x.source_family for x in SOURCE_CAPABILITY_REGISTRY}
    required = {
        "market_price",
        "index_market",
        "margin_leverage",
        "financial_fundamental",
        "industry_concept",
        "event_ownership",
        "disclosure_ir",
        "corporate_action",
        "trading_attention",
    }
    assert required.issubset(families)

    df = registry_to_frame()
    assert "dataset_name" in df.columns
    assert "adapter_function" in df.columns
    assert len(df) >= 18

    out = export_registry_csv(tmp_path)
    assert out.exists()

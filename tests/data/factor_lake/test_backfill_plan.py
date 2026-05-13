from qsys.data.factor_lake.backfill_plan import (
    backfill_plan_to_frame,
    export_backfill_plan_csv,
    generate_default_backfill_plan,
)


def test_generate_backfill_plan_and_priority_ordering():
    plan = generate_default_backfill_plan()
    assert len(plan) > 0
    priorities = [x.priority for x in plan]
    assert priorities == sorted(priorities)


def test_backfill_plan_has_required_source_families():
    plan = generate_default_backfill_plan()
    families = {x.source_family for x in plan}
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


def test_backfill_plan_export_csv(tmp_path):
    out = export_backfill_plan_csv(tmp_path / "outputs" / "factor_lake_registry")
    assert out.exists()
    df = backfill_plan_to_frame()
    assert "dataset_name" in df.columns
    assert "pit_required" in df.columns

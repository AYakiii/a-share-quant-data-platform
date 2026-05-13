from qsys.data.factor_lake.registry import FACTOR_SOURCE_REGISTRY


def test_registry_has_required_families():
    families = {c.source_family for c in FACTOR_SOURCE_REGISTRY}
    required = {
        "market_price",
        "index_market",
        "financial_fundamental",
        "margin_leverage",
        "industry_concept",
        "event_ownership",
        "disclosure_ir",
    }
    assert required.issubset(families)
    assert len(FACTOR_SOURCE_REGISTRY) > 0

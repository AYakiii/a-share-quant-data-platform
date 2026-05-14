from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd

from qsys.data.factor_lake.registry import registry_to_frame
from qsys.data.factor_lake.source_gap_audit import (
    REQUIRED_COLUMNS,
    build_source_gap_audit,
)


CANONICAL_FAMILIES = {
    "market_price",
    "index_market",
    "financial_fundamental",
    "margin_leverage",
    "industry_concept",
    "event_ownership",
    "corporate_action",
    "trading_attention",
}

GRANULAR_ONLY_SUBTYPES = {
    "block_trade",
    "lhb",
    "pledge",
    "dividend",
    "shareholder",
    "stock_basic",
    "industry_metadata",
    "concept_metadata",
    "index_membership",
    "restricted_release",
    "institution_research",
}

REQUIRED_APIS = {
    "stock_zh_a_hist","stock_individual_info_em","stock_zh_index_hist_csindex","index_stock_cons_csindex","index_stock_cons_weight_csindex",
    "stock_financial_analysis_indicator","stock_yjyg_em","stock_yysj_em","stock_margin_sse","stock_margin_detail_sse","stock_margin_szse",
    "stock_margin_detail_szse","stock_margin_underlying_info_szse","stock_industry_category_cninfo","stock_industry_change_cninfo",
    "stock_industry_clf_hist_sw","sw_index_first_info","sw_index_second_info","sw_index_third_info","index_component_sw","index_hist_sw",
    "index_realtime_sw","stock_board_industry_name_ths","stock_board_industry_index_ths","stock_board_industry_info_ths",
    "stock_board_industry_summary_ths","stock_board_concept_name_ths","stock_board_concept_index_ths","stock_board_concept_info_ths",
    "stock_board_concept_summary_ths","stock_zh_a_gdhs","stock_zh_a_gdhs_detail_em","stock_gdfx_free_holding_analyse_em",
    "stock_gdfx_holding_analyse_em","stock_gpzy_pledge_ratio_em","stock_gpzy_pledge_ratio_detail_em","stock_gpzy_industry_data_em",
    "stock_gpzy_profile_em","stock_fhps_em","stock_history_dividend","stock_history_dividend_detail","stock_restricted_release_queue_em",
    "stock_restricted_release_summary_em","stock_restricted_release_detail_em","stock_dzjy_sctj","stock_dzjy_mrmx","stock_dzjy_mrtj",
    "stock_dzjy_hyyybtj","stock_lhb_detail_em","stock_lhb_stock_statistic_em","stock_lhb_jgmmtj_em","stock_lhb_hyyyb_em",
    "stock_lhb_yybph_em","stock_jgdy_tj_em",
}


def test_viable_csv_has_exact_54_required_apis() -> None:
    df = pd.read_csv("config/factor_sources/factor_test_viable_sources_v0.csv")
    assert set(REQUIRED_COLUMNS).issubset(df.columns)
    assert "source_subtype" in df.columns
    apis = set(df["api_name"])
    assert len(df) == 54
    assert apis == REQUIRED_APIS

    families = set(df["source_family"])
    assert families == CANONICAL_FAMILIES
    assert not (set(df["source_family"]) & GRANULAR_ONLY_SUBTYPES)
    assert GRANULAR_ONLY_SUBTYPES.issubset(set(df["source_subtype"]))


def test_gap_flags_and_schema_and_preserve_ex_post(tmp_path: Path) -> None:
    viable_csv = Path("config/factor_sources/factor_test_viable_sources_v0.csv")
    reg = set(registry_to_frame()["api_name"].unique())
    nonreg = next(api for api in REQUIRED_APIS if api not in reg)
    inreg = next(api for api in REQUIRED_APIS if api in reg)

    catalog = pd.DataFrame({"api_name": [nonreg]})
    audited = pd.DataFrame({"api_name": ["stock_restricted_release_detail_em"]})
    catalog_path = tmp_path / "raw_ingest_catalog.csv"
    audit_path = tmp_path / "raw_source_health_matrix.csv"
    catalog.to_csv(catalog_path, index=False)
    audited.to_csv(audit_path, index=False)

    result = build_source_gap_audit(viable_csv, catalog_path, audit_path)
    cols = set(result.gap_matrix.columns)
    for c in [
        "already_in_registry","already_seen_in_catalog","already_audited",
        "missing_from_registry","missing_from_coverage_outputs","planned_for_expansion",
    ]:
        assert c in cols

    row_inreg = result.gap_matrix.loc[result.gap_matrix["api_name"] == inreg].iloc[0]
    assert bool(row_inreg["already_in_registry"])

    row_nonreg = result.gap_matrix.loc[result.gap_matrix["api_name"] == nonreg].iloc[0]
    assert bool(row_nonreg["already_seen_in_catalog"])
    assert not bool(row_nonreg["planned_for_expansion"])

    row_aud = result.gap_matrix.loc[
        result.gap_matrix["api_name"] == "stock_restricted_release_detail_em"
    ].iloc[0]
    assert bool(row_aud["already_audited"])
    assert bool(row_aud["contains_ex_post_fields"])

    missing_all = result.gap_matrix[
        (~result.gap_matrix["already_in_registry"])
        & (~result.gap_matrix["already_seen_in_catalog"])
        & (~result.gap_matrix["already_audited"])
    ]
    assert (missing_all["planned_for_expansion"]).all()


def test_cli_writes_outputs_without_network(tmp_path: Path) -> None:
    catalog_path = tmp_path / "raw_ingest_catalog.csv"
    audit_path = tmp_path / "raw_source_health_matrix.csv"
    pd.DataFrame({"api_name": ["stock_margin_sse"]}).to_csv(catalog_path, index=False)
    pd.DataFrame({"api_name": ["stock_zh_a_hist"]}).to_csv(audit_path, index=False)

    cmd = [
        sys.executable,
        "-m",
        "qsys.utils.audit_factor_test_source_gap",
        "--viable-sources-csv",
        "config/factor_sources/factor_test_viable_sources_v0.csv",
        "--output-root",
        str(tmp_path),
        "--raw-ingest-catalog-csv",
        str(catalog_path),
        "--raw-source-health-matrix-csv",
        str(audit_path),
    ]
    env = {"PYTHONPATH": "src"}
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    assert proc.returncode == 0, proc.stderr
    assert (tmp_path / "factor_test_source_gap_matrix.csv").exists()
    assert (tmp_path / "raw_coverage_registry_expansion_plan.csv").exists()

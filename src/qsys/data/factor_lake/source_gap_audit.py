from __future__ import annotations

from pathlib import Path

import pandas as pd

from qsys.data.factor_lake.registry import FACTOR_SOURCE_REGISTRY

VIABLE_SOURCE_COLUMNS = [
    "source_family", "api_name", "factor_test_status", "factor_test_rows", "data_type", "fetch_granularity",
    "required_params", "date_field_candidates", "symbol_field_candidates", "expected_output_shape", "pit_risk_level",
    "contains_ex_post_fields", "recommended_raw_partition_strategy", "include_in_expanded_coverage", "reason",
]


def load_viable_sources(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    missing = [c for c in VIABLE_SOURCE_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"missing required columns: {missing}")
    return df[VIABLE_SOURCE_COLUMNS].copy()


def current_coverage_frame() -> pd.DataFrame:
    return pd.DataFrame([{"source_family": c.source_family, "api_name": c.api_name, "in_registry": bool(c.enabled)} for c in FACTOR_SOURCE_REGISTRY])


def _load_optional_presence(path: str | Path | None, flag_col: str) -> pd.DataFrame:
    if not path or not Path(path).exists():
        return pd.DataFrame(columns=["source_family", "api_name", flag_col])
    df = pd.read_csv(path)
    if not {"source_family", "api_name"}.issubset(df.columns):
        return pd.DataFrame(columns=["source_family", "api_name", flag_col])
    out = df[["source_family", "api_name"]].drop_duplicates().copy()
    out[flag_col] = True
    return out


def build_gap_matrix(viable_df: pd.DataFrame, coverage_df: pd.DataFrame, catalog_df: pd.DataFrame | None = None, health_df: pd.DataFrame | None = None) -> pd.DataFrame:
    merged = viable_df.merge(coverage_df, on=["source_family", "api_name"], how="left")
    merged["in_registry"] = merged["in_registry"].fillna(False).astype(bool)

    if catalog_df is not None and not catalog_df.empty and {"source_family", "api_name"}.issubset(catalog_df.columns):
        seen = catalog_df[["source_family", "api_name"]].drop_duplicates().assign(seen_in_catalog=True)
        merged = merged.merge(seen, on=["source_family", "api_name"], how="left")
    else:
        merged["seen_in_catalog"] = False

    if health_df is not None and not health_df.empty and {"source_family", "api_name"}.issubset(health_df.columns):
        audited = health_df[["source_family", "api_name"]].drop_duplicates().assign(seen_in_health_matrix=True)
        merged = merged.merge(audited, on=["source_family", "api_name"], how="left")
    else:
        merged["seen_in_health_matrix"] = False

    merged["seen_in_catalog"] = merged["seen_in_catalog"].fillna(False).astype(bool)
    merged["seen_in_health_matrix"] = merged["seen_in_health_matrix"].fillna(False).astype(bool)

    merged["registry_status"] = merged["in_registry"].map({True: "already_in_registry", False: "missing_from_registry"})
    merged["catalog_status"] = merged["seen_in_catalog"].map({True: "already_seen_in_catalog", False: "missing_from_coverage_outputs"})
    merged["audit_status"] = merged["seen_in_health_matrix"].map({True: "already_audited", False: "missing_from_coverage_outputs"})
    merged["planned_for_expansion"] = (~merged["in_registry"] & ~merged["seen_in_catalog"] & ~merged["seen_in_health_matrix"] & merged["include_in_expanded_coverage"].astype(bool))
    merged["coverage_status"] = merged["planned_for_expansion"].map({True: "planned_for_expansion", False: "already_covered_or_observed"})
    return merged


def build_expansion_plan(gap_df: pd.DataFrame) -> pd.DataFrame:
    plan = gap_df[gap_df["planned_for_expansion"]].copy()
    plan["plan_action"] = "add_to_registry_and_coverage_runner"
    plan["ex_post_flag"] = plan["contains_ex_post_fields"].map({True: "contains_ex_post_fields", False: "none"})
    return plan[[
        "source_family", "api_name", "plan_action", "registry_status", "catalog_status", "audit_status", "coverage_status",
        "factor_test_status", "factor_test_rows", "data_type", "fetch_granularity", "required_params", "date_field_candidates",
        "symbol_field_candidates", "expected_output_shape", "pit_risk_level", "contains_ex_post_fields", "ex_post_flag",
        "recommended_raw_partition_strategy", "include_in_expanded_coverage", "reason",
    ]]


def run_source_gap_audit(viable_sources_csv: str | Path, output_root: str | Path, raw_ingest_catalog_csv: str | Path | None = None, raw_source_health_matrix_csv: str | Path | None = None) -> dict[str, str]:
    out_root = Path(output_root)
    out_root.mkdir(parents=True, exist_ok=True)

    viable = load_viable_sources(viable_sources_csv)
    coverage = current_coverage_frame()
    catalog = pd.read_csv(raw_ingest_catalog_csv) if raw_ingest_catalog_csv and Path(raw_ingest_catalog_csv).exists() else None
    health = pd.read_csv(raw_source_health_matrix_csv) if raw_source_health_matrix_csv and Path(raw_source_health_matrix_csv).exists() else None
    gap = build_gap_matrix(viable, coverage, catalog, health)
    plan = build_expansion_plan(gap)

    gap_path = out_root / "factor_test_source_gap_matrix.csv"
    plan_path = out_root / "raw_coverage_registry_expansion_plan.csv"
    gap.to_csv(gap_path, index=False, encoding="utf-8-sig")
    plan.to_csv(plan_path, index=False, encoding="utf-8-sig")
    return {"gap_matrix_path": str(gap_path), "expansion_plan_path": str(plan_path)}

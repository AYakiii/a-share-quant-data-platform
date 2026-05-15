from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from qsys.data.factor_lake.registry import registry_to_frame

REQUIRED_COLUMNS: tuple[str, ...] = (
    "source_family",
    "api_name",
    "factor_test_status",
    "factor_test_rows",
    "data_type",
    "fetch_granularity",
    "required_params",
    "date_field_candidates",
    "symbol_field_candidates",
    "expected_output_shape",
    "pit_risk_level",
    "contains_ex_post_fields",
    "recommended_raw_partition_strategy",
    "include_in_expanded_coverage",
    "reason",
)


@dataclass(frozen=True)
class SourceGapAuditResult:
    gap_matrix: pd.DataFrame
    expansion_plan: pd.DataFrame


def _load_optional_api_set(csv_path: str | Path | None) -> set[str]:
    if csv_path is None:
        return set()
    path = Path(csv_path)
    if not path.exists():
        return set()
    df = pd.read_csv(path)
    if "api_name" in df.columns:
        return set(df["api_name"].dropna().astype(str))
    if "api" in df.columns:
        return set(df["api"].dropna().astype(str))
    return set()


def load_viable_sources(csv_path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    return df.copy()


def build_source_gap_audit(
    viable_sources_csv: str | Path,
    raw_ingest_catalog_csv: str | Path | None = None,
    raw_source_health_matrix_csv: str | Path | None = None,
) -> SourceGapAuditResult:
    viable_df = load_viable_sources(viable_sources_csv)
    registry_df = registry_to_frame()

    registry_apis = set(registry_df["api_name"].dropna().astype(str))
    catalog_apis = _load_optional_api_set(raw_ingest_catalog_csv)
    audited_apis = _load_optional_api_set(raw_source_health_matrix_csv)

    gap_df = viable_df.copy()
    gap_df["already_in_registry"] = gap_df["api_name"].isin(registry_apis)
    gap_df["already_seen_in_catalog"] = gap_df["api_name"].isin(catalog_apis)
    gap_df["already_audited"] = gap_df["api_name"].isin(audited_apis)
    gap_df["missing_from_registry"] = ~gap_df["already_in_registry"]
    gap_df["missing_from_coverage_outputs"] = ~(
        gap_df["already_seen_in_catalog"] | gap_df["already_audited"]
    )
    gap_df["planned_for_expansion"] = gap_df["missing_from_registry"] & gap_df[
        "missing_from_coverage_outputs"
    ]

    expansion_plan = gap_df.loc[
        gap_df["planned_for_expansion"],
        [
            "source_family",
            "api_name",
            "data_type",
            "fetch_granularity",
            "required_params",
            "date_field_candidates",
            "symbol_field_candidates",
            "contains_ex_post_fields",
            "recommended_raw_partition_strategy",
            "reason",
        ],
    ].copy()
    expansion_plan = expansion_plan.sort_values(["source_family", "api_name"]).reset_index(
        drop=True
    )

    gap_df = gap_df.sort_values(["source_family", "api_name"]).reset_index(drop=True)
    return SourceGapAuditResult(gap_matrix=gap_df, expansion_plan=expansion_plan)


def write_source_gap_outputs(result: SourceGapAuditResult, output_root: str | Path) -> tuple[Path, Path]:
    out_root = Path(output_root)
    out_root.mkdir(parents=True, exist_ok=True)
    gap_path = out_root / "factor_test_source_gap_matrix.csv"
    plan_path = out_root / "raw_coverage_registry_expansion_plan.csv"
    result.gap_matrix.to_csv(gap_path, index=False, encoding="utf-8-sig")
    result.expansion_plan.to_csv(plan_path, index=False, encoding="utf-8-sig")
    return gap_path, plan_path

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AcquisitionProfile:
    profile_name: str
    stage: str
    description: str
    source_groups: tuple[str, ...]
    source_families: tuple[str, ...]
    api_names: tuple[str, ...]
    rescue_specs: tuple[str, ...]
    forbidden_apis: tuple[str, ...]
    accepted_statuses: tuple[str, ...]
    drive_asset_name_hint: str


def _p0_profile() -> AcquisitionProfile:
    return AcquisitionProfile(
        profile_name="p0",
        stage="U1-M5 Step 3",
        description="Proven raw acquisition wave profile that reuses P0 ingest + recovery workflow.",
        source_groups=("index_market_data", "sw_industry_data", "rescue_sources"),
        source_families=("index_market", "industry_concept"),
        api_names=(
            "stock_zh_index_hist_csindex",
            "index_stock_cons_csindex",
            "index_stock_cons_weight_csindex",
            "sw_index_first_info",
            "sw_index_second_info",
            "sw_index_third_info",
            "index_component_sw",
            "index_hist_sw",
            "sw_industry_membership_rescue",
        ),
        rescue_specs=("sw_industry_membership_rescue",),
        forbidden_apis=("tradability_mask_v0",),
        accepted_statuses=("success", "empty", "already_exists", "skipped"),
        drive_asset_name_hint="p0_{window_or_period}",
    )


PROFILES: dict[str, AcquisitionProfile] = {"p0": _p0_profile()}


def get_acquisition_profile(profile_name: str) -> AcquisitionProfile:
    key = str(profile_name or "").strip().lower()
    if key not in PROFILES:
        raise ValueError(f"Unsupported acquisition profile: {profile_name}")
    return PROFILES[key]


def list_profile_names() -> list[str]:
    return sorted(PROFILES.keys())

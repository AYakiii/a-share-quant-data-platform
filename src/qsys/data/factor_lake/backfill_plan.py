from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

from qsys.data.factor_lake.registry import SOURCE_CAPABILITY_REGISTRY


@dataclass(frozen=True)
class RawBackfillPlanItem:
    dataset_name: str
    source_family: str
    api_name: str
    priority: int
    backfill_start_date: str
    backfill_end_date: str
    fetch_granularity: str
    partition_strategy: str
    expected_partition_keys: str
    update_frequency: str
    pit_required: bool
    lookahead_risk_level: str
    normalized_target: str
    factor_family_target: str
    notes: str


def _risk_level(lookahead_fields: str) -> str:
    if not lookahead_fields:
        return "low"
    if "后" in lookahead_fields or "post" in lookahead_fields.lower():
        return "high"
    return "medium"


def generate_default_backfill_plan(start_date: str = "2010-01-01", end_date: str = "2026-12-31") -> list[RawBackfillPlanItem]:
    items: list[RawBackfillPlanItem] = []
    for spec in SOURCE_CAPABILITY_REGISTRY:
        items.append(
            RawBackfillPlanItem(
                dataset_name=spec.dataset_name,
                source_family=spec.source_family,
                api_name=spec.api_name,
                priority=spec.priority,
                backfill_start_date=start_date,
                backfill_end_date=end_date,
                fetch_granularity=spec.fetch_granularity,
                partition_strategy=spec.fetch_granularity,
                expected_partition_keys=",".join(spec.partition_keys),
                update_frequency=spec.frequency,
                pit_required=bool(spec.announcement_date_field or spec.report_period_field),
                lookahead_risk_level=_risk_level(spec.lookahead_risk_fields),
                normalized_target=spec.normalized_target,
                factor_family_target=spec.factor_family_target,
                notes=spec.notes,
            )
        )
    items.sort(key=lambda x: (x.priority, x.source_family, x.dataset_name, x.api_name))
    return items


def backfill_plan_to_frame(items: list[RawBackfillPlanItem] | None = None) -> pd.DataFrame:
    plan_items = items or generate_default_backfill_plan()
    return pd.DataFrame([asdict(x) for x in plan_items])


def export_backfill_plan_csv(output_root: str | Path = ".") -> Path:
    out = Path(output_root) / "raw_backfill_plan.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    backfill_plan_to_frame().to_csv(out, index=False, encoding="utf-8-sig")
    return out

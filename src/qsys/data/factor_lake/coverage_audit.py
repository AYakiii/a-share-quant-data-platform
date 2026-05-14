from __future__ import annotations

from pathlib import Path

import pandas as pd


def _normalize(v: object) -> str:
    return "" if v is None else str(v)


def classify_health(status: str, rows: int, error_message: str) -> tuple[str, str, str, str]:
    s = _normalize(status).lower()
    msg = _normalize(error_message)
    low = msg.lower()

    if s == "success" and rows > 0:
        return "ready", "ready_for_backfill", "Wave 1", "success_rows_gt_zero"

    if s == "empty" or (s == "success" and rows == 0):
        return "empty_check_later", "retry_with_broader_window_or_alternative_params", "Wave 2", "empty_or_zero_rows"

    if s == "missing" or any(k in low for k in ["missing adapter", "missing function", "not implemented"]):
        return "pending_adapter", "implement_or_register_adapter", "Wave 3", "adapter_not_registered"

    if any(k in msg for k in ["Response ended prematurely", "RemoteDisconnected", "Connection aborted", "Read timed out", "Max retries exceeded"]) or "timeout" in low:
        return "unstable_retry_needed", "retry_with_backoff_or_smaller_window", "Wave 2", "network_or_timeout_instability"

    if any(k in msg for k in ["'NoneType' object is not subscriptable", "KeyError", "IndexError", "AttributeError", "JSONDecodeError", "Expecting value"]):
        return "adapter_defensive_fix_needed", "add_adapter_defensive_handling", "Wave 3", "adapter_defensive_issue"

    if ("unexpected keyword argument" not in low) and any(k in low for k in ["'ai_pc'", "invalid", "not found", "参数", "keyword"]):
        return "parameter_value_review", "review_valid_parameter_values", "Wave 3", "parameter_value_issue"

    return "manual_review_needed", "manual_review", "Wave 3", "unclassified_failure"


def build_health_matrix(catalog_df: pd.DataFrame) -> pd.DataFrame:
    df = catalog_df.copy()
    if "dataset_name" not in df.columns:
        df["dataset_name"] = ""
    if "error_type" not in df.columns:
        df["error_type"] = ""
    if "metadata_path" not in df.columns:
        df["metadata_path"] = ""
    if "output_path" not in df.columns:
        df["output_path"] = ""
    if "error_message" not in df.columns:
        df["error_message"] = ""
    if "rows" not in df.columns:
        df["rows"] = 0

    health_rows = []
    for _, r in df.iterrows():
        hc, act, wave, reason = classify_health(_normalize(r.get("status")), int(r.get("rows", 0) or 0), _normalize(r.get("error_message")))
        health_rows.append(
            {
                "source_family": _normalize(r.get("source_family")),
                "api_name": _normalize(r.get("api_name")),
                "dataset_name": _normalize(r.get("dataset_name")),
                "status": _normalize(r.get("status")),
                "rows": int(r.get("rows", 0) or 0),
                "error_type": _normalize(r.get("error_type")),
                "error_message": _normalize(r.get("error_message")),
                "output_path": _normalize(r.get("output_path")),
                "metadata_path": _normalize(r.get("metadata_path")),
                "health_class": hc,
                "recommended_action": act,
                "backfill_wave": wave,
                "reason": reason,
            }
        )
    return pd.DataFrame(health_rows)


def build_backfill_wave_plan(health_df: pd.DataFrame) -> pd.DataFrame:
    df = health_df.copy()
    wave_rank = {"Wave 1": 1, "Wave 2": 2, "Wave 3": 3}
    df["_wave_rank"] = df["backfill_wave"].map(wave_rank).fillna(9)
    df = df.sort_values(["_wave_rank", "source_family", "api_name", "rows"], ascending=[True, True, True, False]).reset_index(drop=True)
    df["priority_rank"] = df.index + 1
    scope = df["backfill_wave"].map({"Wave 1": "small_historical_wave_first", "Wave 2": "retry_or_broaden_window", "Wave 3": "fix_before_backfill"}).fillna("manual_scope")
    out = df[["backfill_wave", "source_family", "api_name", "dataset_name", "priority_rank", "status", "rows", "recommended_action", "reason"]].copy()
    out["suggested_backfill_scope"] = scope
    return out


def run_coverage_audit(input_root: str | Path, output_root: str | Path) -> dict[str, str]:
    in_root = Path(input_root)
    out_root = Path(output_root)
    out_root.mkdir(parents=True, exist_ok=True)

    catalog = pd.read_csv(in_root / "raw_ingest_catalog.csv")
    _ = (in_root / "raw_ingest_summary.csv").exists()

    health = build_health_matrix(catalog)
    wave = build_backfill_wave_plan(health)

    health_path = out_root / "raw_source_health_matrix.csv"
    wave_path = out_root / "raw_backfill_wave_plan.csv"
    health.to_csv(health_path, index=False, encoding="utf-8-sig")
    wave.to_csv(wave_path, index=False, encoding="utf-8-sig")
    return {"health_matrix_path": str(health_path), "backfill_wave_plan_path": str(wave_path)}

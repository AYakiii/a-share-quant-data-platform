from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from qsys.data.factor_lake.acquisition_profiles import AcquisitionProfile


def _is_forbidden_path(path_text: str) -> bool:
    return "/nan/" in str(path_text).replace("\\", "/").lower()


def resolve_run_dir(local_root: Path, run_dir: str) -> Path:
    if run_dir != "latest":
        out = Path(run_dir)
        return out if out.is_absolute() else local_root / out
    candidates = [p for p in local_root.glob("p0_wave_*") if p.is_dir()]
    if not candidates:
        raise FileNotFoundError(f"No run directories found under {local_root}")

    def _accepted(path: Path) -> int:
        report = path / "p0_final_acceptance_report.json"
        if not report.exists():
            return 0
        payload = json.loads(report.read_text(encoding="utf-8"))
        return 1 if payload.get("final_status") == "accepted" else 0

    candidates.sort(key=lambda p: (_accepted(p), p.name), reverse=True)
    return candidates[0]


def validate_run(profile: AcquisitionProfile, run_dir: Path) -> dict[str, object]:
    catalog_path = run_dir / "p0_wave_catalog.csv"
    summary_path = run_dir / "p0_wave_summary.json"
    manifest_path = run_dir / "p0_wave_manifest.json"
    acceptance_path = run_dir / "p0_final_acceptance_report.json"

    errors: list[str] = []
    for p in (catalog_path, summary_path, manifest_path):
        if not p.exists():
            errors.append(f"missing artifact: {p.name}")

    catalog = pd.read_csv(catalog_path) if catalog_path.exists() else pd.DataFrame()
    required_cols = {"source_family", "api_name", "status", "rows", "output_path", "metadata_path"}
    if not required_cols.issubset(set(catalog.columns)):
        errors.append("missing required catalog columns")
    if catalog.empty:
        errors.append("catalog is empty")
    if not catalog.empty and int(pd.to_numeric(catalog["rows"], errors="coerce").fillna(0).sum()) <= 0:
        errors.append("catalog total rows must be > 0")

    if not catalog.empty:
        api_vals = set(catalog["api_name"].fillna("").astype(str))
        forbidden = sorted(api for api in profile.forbidden_apis if api in api_vals)
        if forbidden:
            errors.append(f"forbidden apis present: {forbidden}")
        bad_paths = catalog["output_path"].fillna("").astype(str).map(_is_forbidden_path)
        bad_meta = catalog["metadata_path"].fillna("").astype(str).map(_is_forbidden_path)
        if bool(bad_paths.any() or bad_meta.any()):
            errors.append("catalog contains /nan/ path segments")

    if acceptance_path.exists():
        acceptance = json.loads(acceptance_path.read_text(encoding="utf-8"))
        if acceptance.get("final_status") != "accepted":
            errors.append("final acceptance report status is not accepted")
        if int(acceptance.get("unresolved_failed_count", 0)) != 0:
            errors.append("unresolved_failed_count is not zero")
    elif not catalog.empty:
        statuses = set(catalog["status"].fillna("").astype(str))
        disallowed = sorted(s for s in statuses if s and s not in set(profile.accepted_statuses))
        if disallowed:
            errors.append(f"catalog has statuses not accepted by profile: {disallowed}")

    report = {"profile": profile.profile_name, "run_dir": str(run_dir), "is_valid": len(errors) == 0, "error_count": len(errors), "errors": errors}
    (run_dir / "validation_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame([{"check": "is_valid", "value": report["is_valid"]}, {"check": "error_count", "value": report["error_count"]}]).to_csv(run_dir / "validation_summary.csv", index=False)
    if errors:
        raise ValueError("; ".join(errors))
    return report

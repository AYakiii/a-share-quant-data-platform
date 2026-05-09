"""Helpers to write lightweight experiment artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


_MANIFEST_FIELDS = [
    "run_id",
    "created_at",
    "code_commit",
    "feature_root",
    "data_range",
    "universe",
    "signal_recipe",
    "portfolio_rule",
    "rebalance_rule",
    "execution_assumption",
    "cost_model",
    "benchmark",
    "diagnostics_requested",
    "known_limitations",
    "warnings",
]


def write_run_manifest(output_dir: str | Path, manifest_dict: dict[str, Any]) -> Path:
    """Write run_manifest.json in a stable, pretty JSON format."""

    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)

    manifest = dict(manifest_dict)
    for field in _MANIFEST_FIELDS:
        manifest.setdefault(field, None)

    fp = root / "run_manifest.json"
    with fp.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    return fp


def write_warnings(output_dir: str | Path, warnings: list[str]) -> Path:
    """Write warnings.md for a run directory."""

    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    fp = root / "warnings.md"

    lines = ["# Run Warnings", ""]
    if not warnings:
        lines.append("No warnings recorded.")
    else:
        for w in warnings:
            lines.append(f"- {w}")

    fp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return fp

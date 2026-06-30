"""No-delete guard and request artifacts for DWH4 Drive operations."""
from __future__ import annotations

import csv
import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

FORBIDDEN_DRIVE_DELETE_OPERATIONS = (
    "Path.unlink",
    "Path.rename",
    "Path.replace",
    "os.remove",
    "os.unlink",
    "os.rmdir",
    "shutil.rmtree",
)

DELETE_REQUEST_STATUS = "DELETE_REQUEST_REVIEW_REQUIRED"


class DriveDeleteBlocked(RuntimeError):
    """Raised when a delete-like operation targets the Drive DWH root."""


@dataclass(frozen=True)
class DriveDeleteRequest:
    """A delete request record that is written for human review only."""

    drive_dwh_root: Path
    target_path: Path
    relative_path: str
    operation: str
    reason: str
    status: str = DELETE_REQUEST_STATUS
    delete_executed: bool = False

    def as_payload(self) -> dict[str, object]:
        """Return a JSON-safe record."""
        return {
            "drive_dwh_root": str(self.drive_dwh_root),
            "relative_path": self.relative_path,
            "path": str(self.target_path),
            "operation": self.operation,
            "reason": self.reason,
            "status": self.status,
            "delete_executed": self.delete_executed,
        }

    def as_csv_row(self) -> dict[str, str]:
        """Return a CSV-safe record."""
        payload = self.as_payload()
        return {key: str(value).lower() if isinstance(value, bool) else str(value) for key, value in payload.items()}


def _resolved(path: str | Path) -> Path:
    return Path(path).expanduser().resolve()


def _relative_under_root(drive_dwh_root: str | Path, target_path: str | Path) -> tuple[Path, Path, str]:
    root = _resolved(drive_dwh_root)
    target = _resolved(target_path)
    try:
        relative = target.relative_to(root).as_posix()
    except ValueError as exc:
        raise ValueError("target_path must be under drive_dwh_root") from exc
    if not relative or relative == ".":
        raise ValueError("target_path must not be the drive_dwh_root itself")
    return root, target, relative


def is_under_drive_dwh_root(drive_dwh_root: str | Path, target_path: str | Path) -> bool:
    """Return whether target_path resolves under drive_dwh_root."""
    try:
        _relative_under_root(drive_dwh_root, target_path)
    except ValueError:
        return False
    return True


def build_drive_delete_request(
    drive_dwh_root: str | Path,
    target_path: str | Path,
    *,
    operation: str,
    reason: str = "",
) -> DriveDeleteRequest:
    """Build a human-review delete request without deleting anything."""
    if operation not in FORBIDDEN_DRIVE_DELETE_OPERATIONS:
        raise ValueError(f"operation must be one of: {', '.join(FORBIDDEN_DRIVE_DELETE_OPERATIONS)}")
    root, target, relative = _relative_under_root(drive_dwh_root, target_path)
    return DriveDeleteRequest(
        drive_dwh_root=root,
        target_path=target,
        relative_path=relative,
        operation=operation,
        reason=reason,
    )


def assert_drive_delete_allowed(
    drive_dwh_root: str | Path,
    target_path: str | Path,
    *,
    operation: str,
) -> None:
    """Raise when a forbidden delete-like operation targets the Drive DWH root."""
    if operation not in FORBIDDEN_DRIVE_DELETE_OPERATIONS:
        raise ValueError(f"operation must be one of: {', '.join(FORBIDDEN_DRIVE_DELETE_OPERATIONS)}")
    if is_under_drive_dwh_root(drive_dwh_root, target_path):
        raise DriveDeleteBlocked(f"{operation} is blocked under drive_dwh_root: {Path(target_path)}")


def guarded_unlink(path: str | Path, *, drive_dwh_root: str | Path, missing_ok: bool = False) -> None:
    """Delete a non-Drive file only after enforcing the Drive no-delete guard."""
    assert_drive_delete_allowed(drive_dwh_root, path, operation="Path.unlink")
    Path(path).unlink(missing_ok=missing_ok)


def guarded_os_remove(path: str | Path, *, drive_dwh_root: str | Path) -> None:
    """Remove a non-Drive file only after enforcing the Drive no-delete guard."""
    assert_drive_delete_allowed(drive_dwh_root, path, operation="os.remove")
    os.remove(path)


def guarded_rmdir(path: str | Path, *, drive_dwh_root: str | Path) -> None:
    """Remove a non-Drive directory only after enforcing the Drive no-delete guard."""
    assert_drive_delete_allowed(drive_dwh_root, path, operation="os.rmdir")
    os.rmdir(path)


def guarded_rmtree(path: str | Path, *, drive_dwh_root: str | Path) -> None:
    """Remove a non-Drive tree only after enforcing the Drive no-delete guard."""
    assert_drive_delete_allowed(drive_dwh_root, path, operation="shutil.rmtree")
    shutil.rmtree(path)


def guarded_replace(source: str | Path, target: str | Path, *, drive_dwh_root: str | Path) -> Path:
    """Replace a non-Drive target only after enforcing the Drive no-delete guard."""
    assert_drive_delete_allowed(drive_dwh_root, target, operation="Path.replace")
    return Path(source).replace(target)


def guarded_rename(source: str | Path, target: str | Path, *, drive_dwh_root: str | Path) -> Path:
    """Rename a non-Drive target only after enforcing the Drive no-delete guard."""
    assert_drive_delete_allowed(drive_dwh_root, source, operation="Path.rename")
    assert_drive_delete_allowed(drive_dwh_root, target, operation="Path.rename")
    return Path(source).rename(target)


def write_drive_delete_request_artifacts(
    drive_dwh_root: str | Path,
    requests: Iterable[DriveDeleteRequest],
    artifact_root: str | Path,
) -> dict[str, Path]:
    """Write human-review delete request artifacts without mutating Drive."""
    root = _resolved(drive_dwh_root)
    rows = tuple(requests)
    output_root = Path(artifact_root)
    output_root.mkdir(parents=True, exist_ok=True)
    markdown_path = output_root / "DRIVE_DELETE_REQUEST.md"
    csv_path = output_root / "drive_delete_plan.csv"
    summary_path = output_root / "drive_delete_summary.json"

    lines = [
        "# DRIVE DELETE REQUEST - NOT EXECUTED",
        "",
        "WARNING: this file is a human review request only.",
        "",
        "- No files were deleted.",
        "- Deletion is disabled by default for DWH4 Drive roots.",
        f"- drive_dwh_root: {root}",
        f"- request_count: {len(rows)}",
        "",
        "## Requested Paths",
        "",
    ]
    if rows:
        lines.append("| relative_path | operation | status | reason |")
        lines.append("| --- | --- | --- | --- |")
        for row in rows:
            lines.append(f"| {row.relative_path} | {row.operation} | {row.status} | {row.reason or ''} |")
    else:
        lines.append("- none")
    lines.append("")
    markdown_path.write_text("\n".join(lines), encoding="utf-8")

    fieldnames = ["drive_dwh_root", "relative_path", "path", "operation", "reason", "status", "delete_executed"]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(row.as_csv_row() for row in rows)

    summary = {
        "drive_dwh_root": str(root),
        "delete_request_generated": True,
        "drive_delete_executed": False,
        "drive_write_executed": False,
        "request_count": len(rows),
        "requests": [row.as_payload() for row in rows],
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "drive_delete_request": markdown_path,
        "drive_delete_plan": csv_path,
        "drive_delete_summary": summary_path,
    }

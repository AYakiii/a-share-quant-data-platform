from __future__ import annotations

import argparse
import json
from pathlib import Path

from qsys.data.factor_lake.acquisition_compact import compact_run
from qsys.data.factor_lake.acquisition_profiles import get_acquisition_profile
from qsys.data.factor_lake.acquisition_promotion import promote_compact, qa_promoted_asset
from qsys.data.factor_lake.acquisition_validation import resolve_run_dir, validate_run
from qsys.utils.run_p0_raw_acquisition_wave import parse_args as p0_parse_args
from qsys.utils.run_p0_raw_acquisition_wave import run_p0_wave


def _reject_drive_path(path_text: str) -> None:
    norm = str(path_text).lower().replace("\\", "/")
    if "drive" in norm and "mydrive" in norm:
        raise ValueError(f"Drive path is not allowed for local staging: {path_text}")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command", required=True)

    pull = sub.add_parser("pull")
    pull.add_argument("--profile", required=True)
    pull.add_argument("--start-date", required=True)
    pull.add_argument("--end-date", required=True)
    pull.add_argument("--local-root", required=True)
    pull.add_argument("--max-workers", type=int, default=2)
    pull.add_argument("--continue-on-error", action="store_true")
    pull.add_argument("--show-progress", action="store_true")
    pull.add_argument("--heartbeat-sec", type=float, default=30.0)
    pull.add_argument("--request-sleep", type=float, default=0.0)
    pull.add_argument("--task-timeout-sec", type=float, default=None)
    pull.add_argument("--auto-recover-failed", action="store_true")
    pull.add_argument("--recovery-max-workers", type=int, default=1)
    pull.add_argument("--recovery-request-sleep", type=float, default=0.5)
    pull.add_argument("--recovery-task-timeout-sec", type=float, default=120.0)
    pull.add_argument("--recovery-task-retry-attempts", type=int, default=2)

    val = sub.add_parser("validate")
    val.add_argument("--profile", required=True)
    val.add_argument("--run-dir", default="latest")
    val.add_argument("--local-root", required=True)

    compact = sub.add_parser("compact")
    compact.add_argument("--profile", required=True)
    compact.add_argument("--run-dir", default="latest")
    compact.add_argument("--local-root", required=True)
    compact.add_argument("--compact-root", required=True)

    promote = sub.add_parser("promote")
    promote.add_argument("--profile", required=True)
    promote.add_argument("--compact-root", required=True)
    promote.add_argument("--drive-root", required=True)
    promote.add_argument("--asset-name", required=True)
    promote.add_argument("--promote-to-drive", action="store_true")
    promote.add_argument("--allow-overwrite", action="store_true")

    qa = sub.add_parser("qa")
    qa.add_argument("--profile", required=True)
    qa.add_argument("--drive-root", required=True)
    qa.add_argument("--asset-name", required=True)
    qa.add_argument("--compact-root", default="")
    return p


def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    profile = get_acquisition_profile(args.profile)

    if args.command == "pull":
        _reject_drive_path(args.local_root)
        p0_args = p0_parse_args([
            "--start-date", args.start_date, "--end-date", args.end_date, "--output-root", args.local_root,
            "--max-workers", str(args.max_workers), "--heartbeat-sec", str(args.heartbeat_sec), "--request-sleep", str(args.request_sleep),
            "--recovery-max-workers", str(args.recovery_max_workers), "--recovery-request-sleep", str(args.recovery_request_sleep),
            "--recovery-task-timeout-sec", str(args.recovery_task_timeout_sec), "--recovery-task-retry-attempts", str(args.recovery_task_retry_attempts),
        ] + (["--continue-on-error"] if args.continue_on_error else []) + (["--show-progress"] if args.show_progress else []) + (["--auto-recover-failed"] if args.auto_recover_failed else []) + (["--task-timeout-sec", str(args.task_timeout_sec)] if args.task_timeout_sec else []))
        out = run_p0_wave(p0_args)
        print(json.dumps({"run_dir": str(out["run_dir"]), "profile": profile.profile_name}, ensure_ascii=False))
        return

    if args.command == "validate":
        run_dir = resolve_run_dir(Path(args.local_root), args.run_dir)
        out = validate_run(profile, run_dir)
        print(json.dumps(out, ensure_ascii=False))
        return

    if args.command == "compact":
        run_dir = resolve_run_dir(Path(args.local_root), args.run_dir)
        validate_run(profile, run_dir)
        out = compact_run(run_dir, Path(args.compact_root))
        print(json.dumps(out, ensure_ascii=False))
        return

    if args.command == "promote":
        out = promote_compact(Path(args.compact_root), Path(args.drive_root), args.asset_name, args.promote_to_drive, args.allow_overwrite)
        print(json.dumps(out, ensure_ascii=False))
        return

    if args.command == "qa":
        compact_root = Path(args.compact_root) if args.compact_root else None
        out = qa_promoted_asset(Path(args.drive_root), args.asset_name, compact_root)
        print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()

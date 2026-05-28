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
    pull.add_argument("--task-retry-attempts", type=int, default=0)
    pull.add_argument("--task-retry-sleep-sec", type=float, default=0.0)
    pull.add_argument("--task-retry-backoff", type=float, default=1.0)
    pull.add_argument("--task-retry-jitter-sec", type=float, default=0.0)
    pull.add_argument("--recovery-task-retry-sleep-sec", type=float, default=1.0)
    pull.add_argument("--recovery-task-retry-backoff", type=float, default=1.5)
    pull.add_argument("--recovery-task-retry-jitter-sec", type=float, default=0.2)
    pull.add_argument("--symbols", default="")
    pull.add_argument("--symbols-file", default="")
    pull.add_argument("--index-symbols", default="")
    pull.add_argument("--trade-dates", default="")
    pull.add_argument("--report-dates", default="")
    pull.add_argument("--industry-names", default="")
    pull.add_argument("--concept-names", default="")
    pull.add_argument("--universe-root", default="config/factor_sources/acquisition_universe")
    pull.add_argument("--include-disabled", action="store_true")
    pull.add_argument("--resume", action="store_true")

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
        pull_args = [
            "--start-date", args.start_date,
            "--end-date", args.end_date,
            "--output-root", args.local_root,
            "--max-workers", str(args.max_workers),
            "--heartbeat-sec", str(args.heartbeat_sec),
            "--request-sleep", str(args.request_sleep),
            "--recovery-max-workers", str(args.recovery_max_workers),
            "--recovery-request-sleep", str(args.recovery_request_sleep),
            "--recovery-task-timeout-sec", str(args.recovery_task_timeout_sec),
            "--recovery-task-retry-attempts", str(args.recovery_task_retry_attempts),
            "--task-retry-attempts", str(args.task_retry_attempts),
            "--task-retry-sleep-sec", str(args.task_retry_sleep_sec),
            "--task-retry-backoff", str(args.task_retry_backoff),
            "--task-retry-jitter-sec", str(args.task_retry_jitter_sec),
            "--recovery-task-retry-sleep-sec", str(args.recovery_task_retry_sleep_sec),
            "--recovery-task-retry-backoff", str(args.recovery_task_retry_backoff),
            "--recovery-task-retry-jitter-sec", str(args.recovery_task_retry_jitter_sec),
            "--universe-root", args.universe_root,
        ]
        optional_values = {
            "--symbols": args.symbols,
            "--symbols-file": args.symbols_file,
            "--index-symbols": args.index_symbols,
            "--trade-dates": args.trade_dates,
            "--report-dates": args.report_dates,
            "--industry-names": args.industry_names,
            "--concept-names": args.concept_names,
        }
        for key, value in optional_values.items():
            if str(value or "").strip():
                pull_args.extend([key, str(value)])
        if args.continue_on_error:
            pull_args.append("--continue-on-error")
        if args.show_progress:
            pull_args.append("--show-progress")
        if args.auto_recover_failed:
            pull_args.append("--auto-recover-failed")
        if args.include_disabled:
            pull_args.append("--include-disabled")
        if args.resume:
            pull_args.append("--resume")
        if args.task_timeout_sec is not None:
            pull_args.extend(["--task-timeout-sec", str(args.task_timeout_sec)])
        p0_args = p0_parse_args(pull_args)
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

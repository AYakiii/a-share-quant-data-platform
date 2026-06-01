from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Iterable

import akshare as ak
import pandas as pd

from qsys.data.factor_lake.raw_ingest import (
    API_POLICY_METADATA,
    COVERAGE_API_SPECS,
    EXCLUDED_APIS,
    PHASE_COVERAGE_FAMILIES,
    TEMP_DISABLED_APIS,
    _effective_param_mode,
    _params_for_mode,
    run_raw_ingest_official,
)
from qsys.data.factor_lake.registry import SOURCE_CAPABILITY_REGISTRY

LANES = ("main", "manual_selected", "heavy", "long_run", "deferred", "deferred_recovery")
LONG_RUN_APIS = {"stock_jgdy_detail_em", "stock_gdfx_holding_analyse_em"}
MANUAL_SELECTED_MODES = {"manual_selected_only", "manual_selected_snapshot"}
HEAVY_FAMILIES = {"index_market"}
HEAVY_PARAM_MODES = {"index_symbol_range", "index_symbol", "industry_code", "industry_name_range", "concept_name_range"}
SNAPSHOT_DIR = "universe_snapshots"
REVIEW_DIR = "_operation_review"


@dataclass(frozen=True)
class PreheatUniverse:
    """Discovered object universes for a local raw-lake preheat run."""

    stock_symbols: list[str] = field(default_factory=list)
    index_symbols: list[str] = field(default_factory=list)
    industry_names: list[str] = field(default_factory=list)
    concept_names: list[str] = field(default_factory=list)
    trading_dates: list[str] = field(default_factory=list)
    report_dates: list[str] = field(default_factory=list)
    industry_codes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class LaneConfig:
    """Execution settings for one preheat lane."""

    name: str
    max_workers: int
    request_sleep: float
    task_timeout_sec: float
    include_disabled: bool = False


def _dedupe_preserve_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        text = str(value).strip()
        if text and text not in seen:
            seen.add(text)
            out.append(text)
    return out


def _parse_csv_arg(value: str | None) -> list[str]:
    return _dedupe_preserve_order((value or "").split(","))


def load_stock_symbols_from_file(path: str | Path) -> list[str]:
    """Load six-digit logical stock symbols from a text file."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"symbols file not found: {p}")
    symbols: list[str] = []
    for line_no, raw_line in enumerate(p.read_text(encoding="utf-8").splitlines(), start=1):
        text = raw_line.strip()
        if not text or text.startswith("#"):
            continue
        if not re.fullmatch(r"\d{6}", text):
            raise ValueError(f"invalid stock symbol at {p}:{line_no}: {text!r}; expected six digits")
        symbols.append(text)
    symbols = _dedupe_preserve_order(symbols)
    if not symbols:
        raise ValueError(f"symbols file is empty after comments/blanks: {p}")
    return symbols


def parse_report_dates(value: str | None) -> list[str]:
    """Parse explicit comma-separated YYYYMMDD report dates without inventing defaults."""
    if value is None:
        raise ValueError("--report-dates is required by selected report-date APIs")
    dates = _dedupe_preserve_order(part.strip() for part in value.split(","))
    invalid = [d for d in dates if not re.fullmatch(r"\d{8}", d)]
    if invalid:
        raise ValueError(f"invalid report date(s): {invalid}; expected YYYYMMDD")
    if not dates:
        raise ValueError("--report-dates must contain at least one YYYYMMDD date")
    return dates


def _yyyymmdd(value: Any) -> str:
    if pd.isna(value):
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%Y%m%d")
    text = str(value).strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return text


def discover_trading_dates(ak_module: object, start_date: str, end_date: str) -> list[str]:
    """Discover trading dates from AkShare and filter them to the requested range."""
    frame = ak_module.tool_trade_date_hist_sina()
    if frame is None or pd.DataFrame(frame).empty:
        raise ValueError("AkShare trading calendar discovery returned no rows")
    df = pd.DataFrame(frame)
    col = "trade_date" if "trade_date" in df.columns else df.columns[0]
    dates = [_yyyymmdd(v) for v in df[col].tolist()]
    return [d for d in dates if re.fullmatch(r"\d{8}", d) and start_date <= d <= end_date]


def discover_index_symbols(ak_module: object) -> list[str]:
    """Discover the current A-share index directory via AkShare stock_zh_index_spot_em."""
    frame = ak_module.stock_zh_index_spot_em()
    if frame is None or pd.DataFrame(frame).empty:
        raise ValueError("AkShare index universe discovery returned no rows")
    df = pd.DataFrame(frame)
    col = next((c for c in ["代码", "指数代码", "symbol", "code", "指数编码"] if c in df.columns), df.columns[0])
    symbols = []
    for raw in df[col].tolist():
        text = str(raw).strip()
        digits = "".join(ch for ch in text if ch.isdigit())
        symbols.append(digits.zfill(6) if digits and len(digits) <= 6 else text)
    return _dedupe_preserve_order(symbols)


def _discover_name_column(frame: object, preferred: list[str], label: str) -> list[str]:
    df = pd.DataFrame(frame)
    if df.empty:
        raise ValueError(f"AkShare {label} universe discovery returned no rows")
    col = next((c for c in preferred if c in df.columns), df.columns[0])
    return _dedupe_preserve_order(str(v).strip() for v in df[col].tolist())


def discover_industry_names(ak_module: object) -> list[str]:
    """Discover the THS industry-name universe."""
    return _discover_name_column(ak_module.stock_board_industry_name_ths(), ["name", "板块", "行业名称", "概念名称"], "industry")


def discover_concept_names(ak_module: object) -> list[str]:
    """Discover the THS concept-name universe."""
    return _discover_name_column(ak_module.stock_board_concept_name_ths(), ["name", "板块", "概念名称"], "concept")


def discover_sw_industry_codes(ak_module: object) -> list[str]:
    """Discover SW first/second/third-level industry codes for SW fan-out APIs."""
    codes: list[str] = []
    for func_name in ("sw_index_first_info", "sw_index_second_info", "sw_index_third_info"):
        frame = getattr(ak_module, func_name)()
        df = pd.DataFrame(frame)
        if df.empty:
            continue
        col = next((c for c in ["行业代码", "指数代码", "代码", "symbol", "code"] if c in df.columns), df.columns[0])
        codes.extend(str(v).strip() for v in df[col].tolist())
    codes = _dedupe_preserve_order(codes)
    if not codes:
        raise ValueError("AkShare SW industry-code discovery returned no rows")
    return codes


def reject_drive_output_root(output_root: str | Path) -> None:
    """Reject Google Drive-like paths for local Colab preheat safety."""
    low = str(output_root).lower()
    if any(marker in low for marker in ("/content/drive", "/content/gdrive", "mydrive", "google drive", "gdrive")):
        raise ValueError(f"Google Drive output paths are not allowed for raw-lake preheat: {output_root}")


def discover_universe(symbols_file: str | Path, start_date: str, end_date: str, report_dates: str, ak_module: object | None = None) -> PreheatUniverse:
    """Eager compatibility helper for tests and direct callers."""
    if ak_module is None:
        import akshare as ak_module  # type: ignore[no-redef]
    return PreheatUniverse(
        stock_symbols=load_stock_symbols_from_file(symbols_file),
        index_symbols=discover_index_symbols(ak_module),
        industry_names=discover_industry_names(ak_module),
        concept_names=discover_concept_names(ak_module),
        trading_dates=discover_trading_dates(ak_module, start_date, end_date),
        report_dates=parse_report_dates(report_dates),
        industry_codes=discover_sw_industry_codes(ak_module),
    )


def _snapshot_path(output_root: str | Path) -> Path:
    return Path(output_root) / REVIEW_DIR / SNAPSHOT_DIR


def _read_snapshot(root: Path, filename: str, column: str) -> list[str]:
    path = root / filename
    if not path.exists():
        return []
    df = pd.read_csv(path, dtype=str).fillna("")
    if column not in df.columns:
        return []
    return _dedupe_preserve_order(df[column].astype(str).tolist())


def read_universe_snapshots(output_root: str | Path) -> PreheatUniverse:
    """Read previously written universe snapshots, preserving string codes."""
    root = _snapshot_path(output_root)
    return PreheatUniverse(
        stock_symbols=_read_snapshot(root, "stock_symbols.csv", "symbol"),
        index_symbols=_read_snapshot(root, "index_symbols.csv", "symbol"),
        industry_names=_read_snapshot(root, "industry_names.csv", "name"),
        concept_names=_read_snapshot(root, "concept_names.csv", "name"),
        trading_dates=_read_snapshot(root, "trading_calendar.csv", "trade_date"),
        report_dates=_read_snapshot(root, "report_dates.csv", "report_date"),
        industry_codes=_read_snapshot(root, "industry_codes.csv", "industry_code"),
    )


def write_universe_snapshots(output_root: str | Path, universe: PreheatUniverse) -> Path:
    """Write local lineage snapshots under the run output root."""
    root = _snapshot_path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    files = {
        "stock_symbols.csv": ("symbol", universe.stock_symbols),
        "index_symbols.csv": ("symbol", universe.index_symbols),
        "industry_names.csv": ("name", universe.industry_names),
        "concept_names.csv": ("name", universe.concept_names),
        "trading_calendar.csv": ("trade_date", universe.trading_dates),
        "report_dates.csv": ("report_date", universe.report_dates),
        "industry_codes.csv": ("industry_code", universe.industry_codes),
    }
    for filename, (column, values) in files.items():
        pd.DataFrame({column: values}).to_csv(root / filename, index=False, encoding="utf-8-sig")
    return root


def _capability_by_api() -> dict[str, Any]:
    return {spec.api_name: spec for spec in SOURCE_CAPABILITY_REGISTRY}


def _priority_tier(family: str, api_name: str) -> str:
    meta = API_POLICY_METADATA.get((family, api_name), {})
    if "priority_tier" in meta:
        return str(meta["priority_tier"])
    spec = _capability_by_api().get(api_name)
    return f"P{getattr(spec, 'priority', '')}" if spec is not None else ""


def _data_theme(api_name: str, family: str) -> str:
    meta = API_POLICY_METADATA.get((family, api_name), {})
    if "data_theme" in meta:
        return str(meta["data_theme"])
    spec = _capability_by_api().get(api_name)
    return str(getattr(spec, "factor_family_target", "") or getattr(spec, "normalized_target", "")) if spec is not None else ""


def _acquisition_mode(family: str, api_name: str, param_mode: str) -> str:
    meta = API_POLICY_METADATA.get((family, api_name), {})
    return str(meta.get("acquisition_mode") or param_mode)


def _base_lane_for(family: str, api_name: str, param_mode: str) -> str:
    if api_name in LONG_RUN_APIS:
        return "long_run"
    policy = API_POLICY_METADATA.get((family, api_name), {})
    mode = str(policy.get("acquisition_mode", ""))
    if mode in MANUAL_SELECTED_MODES:
        return "manual_selected"
    if (family, api_name) in EXCLUDED_APIS or ((family, api_name) in TEMP_DISABLED_APIS and mode not in MANUAL_SELECTED_MODES):
        return "deferred"
    if family in HEAVY_FAMILIES or param_mode in HEAVY_PARAM_MODES:
        return "heavy"
    return "main"


def _all_registered_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_api_modes: dict[str, str] = {}
    families = list(dict.fromkeys([*PHASE_COVERAGE_FAMILIES, *COVERAGE_API_SPECS.keys()]))
    for family in families:
        for spec in COVERAGE_API_SPECS.get(family, []):
            api_name = spec["api_name"]
            param_mode = _effective_param_mode(family, api_name, spec["param_mode"])
            prior_mode = seen_api_modes.get(api_name)
            if prior_mode is not None:
                if prior_mode != param_mode:
                    raise ValueError(f"Conflicting duplicate API registration for {api_name}: {prior_mode} vs {param_mode}")
                continue
            seen_api_modes[api_name] = param_mode
            rows.append(
                {
                    "source_family": family,
                    "api_name": api_name,
                    "param_mode": param_mode,
                    "priority_tier": _priority_tier(family, api_name),
                    "data_theme": _data_theme(api_name, family),
                    "acquisition_mode": _acquisition_mode(family, api_name, param_mode),
                    "lane": _base_lane_for(family, api_name, param_mode),
                }
            )
    return rows


def _selected_lanes(args: argparse.Namespace) -> set[str]:
    lanes = set(_parse_csv_arg(args.lanes))
    if getattr(args, "include_deferred_recovery", False):
        lanes.add("deferred_recovery")
    invalid = lanes - set(LANES)
    if invalid:
        raise ValueError(f"invalid lane(s): {sorted(invalid)}")
    return lanes


def _selection(row: dict[str, Any], args: argparse.Namespace) -> tuple[bool, str]:
    only_families = set(_parse_csv_arg(args.only_families))
    exclude_families = set(_parse_csv_arg(args.exclude_families))
    only_apis = set(_parse_csv_arg(args.only_apis))
    exclude_apis = set(_parse_csv_arg(args.exclude_apis))
    if row["lane"] not in _selected_lanes(args):
        return False, "lane_not_selected"
    if only_families and row["source_family"] not in only_families:
        return False, "family_not_in_only_families"
    if row["source_family"] in exclude_families:
        return False, "family_excluded"
    if only_apis and row["api_name"] not in only_apis:
        return False, "api_not_in_only_apis"
    if row["api_name"] in exclude_apis:
        return False, "api_excluded"
    if row["lane"] == "deferred":
        return False, "deferred_audit_only"
    return True, "selected"


def required_modes(plan_rows: list[dict[str, Any]]) -> set[str]:
    return {r["param_mode"] for r in plan_rows if r.get("selected")}


def discover_universe_for_plan(args: argparse.Namespace, plan_rows: list[dict[str, Any]], ak_module: object | None = None) -> PreheatUniverse:
    """Lazily discover only universe slices required by selected APIs."""
    if ak_module is None:
        import akshare as ak_module  # type: ignore[no-redef]

    existing = read_universe_snapshots(args.output_root) if args.resume and not args.refresh_universe else PreheatUniverse()
    modes = required_modes(plan_rows)
    need_symbols = bool(modes & {"symbol_only", "symbol_range", "daily_symbol_range", "daily_symbol_range_hist", "symbol_report_date", "financial_indicator_em", "financial_statement_symbol"})
    need_index = bool(modes & {"index_symbol_range", "index_symbol"})
    need_trade = "trade_date" in modes
    need_report = bool(modes & {"report_date", "symbol_report_date", "financial_statement_report_date"})
    need_industry_names = bool(modes & {"industry_name_range", "industry_name"})
    need_concept_names = bool(modes & {"concept_name_range", "concept_name"})
    need_industry_codes = "industry_code" in modes

    if need_symbols and not args.symbols_file and not existing.stock_symbols:
        raise ValueError("--symbols-file is required by selected stock-symbol APIs")
    if need_report and args.report_dates is None and not existing.report_dates:
        raise ValueError("--report-dates is required by selected report-date APIs")

    return PreheatUniverse(
        stock_symbols=existing.stock_symbols if existing.stock_symbols else (load_stock_symbols_from_file(args.symbols_file) if need_symbols else []),
        index_symbols=existing.index_symbols if existing.index_symbols else (discover_index_symbols(ak_module) if need_index else []),
        industry_names=existing.industry_names if existing.industry_names else (discover_industry_names(ak_module) if need_industry_names else []),
        concept_names=existing.concept_names if existing.concept_names else (discover_concept_names(ak_module) if need_concept_names else []),
        trading_dates=existing.trading_dates if existing.trading_dates else (discover_trading_dates(ak_module, args.start_date, args.end_date) if need_trade else []),
        report_dates=existing.report_dates if existing.report_dates else (parse_report_dates(args.report_dates) if need_report else []),
        industry_codes=existing.industry_codes if existing.industry_codes else (discover_sw_industry_codes(ak_module) if need_industry_codes else []),
    )


def _planned_task_count(param_mode: str, universe: PreheatUniverse, start_date: str, end_date: str) -> int:
    params = _params_for_mode(
        param_mode,
        universe.stock_symbols,
        universe.index_symbols,
        universe.report_dates,
        universe.trading_dates,
        universe.industry_names,
        universe.concept_names,
        start_date,
        end_date,
        industry_codes=universe.industry_codes,
    )
    return len(params)


def build_preheat_plan(args: argparse.Namespace, universe: PreheatUniverse | None = None) -> list[dict[str, Any]]:
    """Build deduplicated API lane planning rows.

    Deduplication rule: when the same AkShare api_name is registered in multiple
    families, the first registered (family, api_name) task semantics are kept.
    """
    lane_workers = {
        "main": args.max_workers,
        "manual_selected": args.max_workers,
        "heavy": args.heavy_max_workers,
        "long_run": args.long_run_max_workers,
        "deferred": 0,
        "deferred_recovery": args.deferred_max_workers,
    }
    plan_rows: list[dict[str, Any]] = []
    universe = universe or PreheatUniverse()
    selected_lanes = _selected_lanes(args)
    for row in _all_registered_rows():
        planned_row = dict(row)
        if planned_row["lane"] == "deferred" and "deferred_recovery" in selected_lanes:
            planned_row["lane"] = "deferred_recovery"
        selected, reason = _selection(planned_row, args)
        policy = API_POLICY_METADATA.get((planned_row["source_family"], planned_row["api_name"]), {})
        execution_status = "pending_execution" if selected else "not_selected"
        plan_rows.append(
            {
                **planned_row,
                "selected": selected,
                "selection_reason": reason,
                "enabled": selected,
                "execution_status": execution_status,
                "default_enabled": bool(policy.get("default_enabled", (planned_row["source_family"], planned_row["api_name"]) not in TEMP_DISABLED_APIS and (planned_row["source_family"], planned_row["api_name"]) not in EXCLUDED_APIS)),
                "manual_review_required": bool(policy.get("manual_review_required", False)),
                "planned_tasks": _planned_task_count(planned_row["param_mode"], universe, args.start_date, args.end_date) if selected and universe is not None else 0,
                "stock_symbol_count": len(universe.stock_symbols),
                "index_symbol_count": len(universe.index_symbols),
                "industry_name_count": len(universe.industry_names),
                "concept_name_count": len(universe.concept_names),
                "trading_date_count": len(universe.trading_dates),
                "report_date_count": len(universe.report_dates),
                "industry_code_count": len(universe.industry_codes),
                "lane_concurrency": lane_workers[planned_row["lane"]],
                "output_root": str(args.output_root),
            }
        )
    return plan_rows


def write_plan_artifacts(output_root: str | Path, plan_rows: list[dict[str, Any]]) -> Path:
    """Write preheat plan, deferred, and long-run review artifacts."""
    op = Path(output_root) / REVIEW_DIR
    op.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(plan_rows)
    (op / "preheat_plan.json").write_text(json.dumps(plan_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    df.to_csv(op / "preheat_plan_by_api.csv", index=False, encoding="utf-8-sig")
    df[df["lane"].isin(["deferred", "deferred_recovery"])].to_csv(op / "deferred_sources.csv", index=False, encoding="utf-8-sig")
    df[df["lane"] == "long_run"].to_csv(op / "long_run_sources.csv", index=False, encoding="utf-8-sig")
    return op


def _lane_configs(args: argparse.Namespace) -> dict[str, LaneConfig]:
    return {
        "main": LaneConfig("main", args.max_workers, args.request_sleep, args.task_timeout_sec),
        "manual_selected": LaneConfig("manual_selected", args.max_workers, args.request_sleep, args.manual_selected_task_timeout_sec, include_disabled=True),
        "heavy": LaneConfig("heavy", args.heavy_max_workers, args.heavy_request_sleep, args.heavy_task_timeout_sec),
        "long_run": LaneConfig("long_run", args.long_run_max_workers, args.long_run_request_sleep, args.long_run_task_timeout_sec, include_disabled=True),
        "deferred_recovery": LaneConfig("deferred_recovery", args.deferred_max_workers, args.heavy_request_sleep, args.deferred_task_timeout_sec, include_disabled=True),
    }


def run_lanes(args: argparse.Namespace, universe: PreheatUniverse, plan_rows: list[dict[str, Any]], runner: Callable[..., dict[str, Any]] = run_raw_ingest_official) -> list[dict[str, Any]]:
    """Execute enabled preheat lanes by delegating to the official raw-ingest runner."""
    manifests: list[dict[str, Any]] = []
    configs = _lane_configs(args)
    executed_any = False
    for lane in LANES:
        if lane == "deferred" or lane not in configs:
            continue
        cfg = configs[lane]
        lane_rows = [r for r in plan_rows if r["lane"] == lane and r.get("selected", r.get("enabled", False))]
        if not lane_rows:
            continue
        families = _dedupe_preserve_order(r["source_family"] for r in lane_rows)
        apis = _dedupe_preserve_order(r["api_name"] for r in lane_rows)
        effective_resume = bool(args.resume or executed_any)
        started = datetime.now(UTC)
        result = runner(
            output_root=str(args.output_root),
            families=families,
            symbols=universe.stock_symbols,
            index_symbols=universe.index_symbols,
            report_dates=universe.report_dates,
            trade_dates=universe.trading_dates,
            industry_names=universe.industry_names,
            concept_names=universe.concept_names,
            industry_codes=universe.industry_codes,
            start_date=args.start_date,
            end_date=args.end_date,
            max_workers=cfg.max_workers,
            selected_api_names=apis,
            include_disabled=cfg.include_disabled,
            request_sleep=cfg.request_sleep,
            resume=effective_resume,
            task_timeout_sec=cfg.task_timeout_sec,
            task_retry_attempts=args.task_retry_attempts,
            task_retry_sleep_sec=args.task_retry_sleep_sec,
            task_retry_backoff=args.task_retry_backoff,
            task_retry_jitter_sec=args.task_retry_jitter_sec,
            heartbeat_sec=args.heartbeat_sec,
            lane_name=lane,
            ak_module=ak,
        )
        rows = result.get("rows", [])
        if rows and all(
            str(row.get("status", "")) == "pending_adapter"
            for row in rows
        ):
            raise RuntimeError(
                "all selected tasks resolved to pending_adapter; "
                "AkShare module wiring may be missing"
            )
        finished = datetime.now(UTC)
        manifests.append(
            {
                "lane": lane,
                "source_families": families,
                "api_names": apis,
                "max_workers": cfg.max_workers,
                "request_sleep": cfg.request_sleep,
                "task_timeout_sec": cfg.task_timeout_sec,
                "include_disabled": cfg.include_disabled,
                "requested_resume": bool(args.resume),
                "effective_resume": effective_resume,
                "started_at": started.isoformat(),
                "finished_at": finished.isoformat(),
                "result_paths": {k: v for k, v in result.items() if k.endswith("_path")},
                "rows": rows,
            }
        )
        executed_any = True
    return manifests


def write_runtime_artifacts(output_root: str | Path, plan_rows: list[dict[str, Any]], lane_manifests: list[dict[str, Any]], *, dry_run: bool = False) -> None:
    """Write unified local review artifacts derived from lane outputs."""
    op = Path(output_root) / REVIEW_DIR
    op.mkdir(parents=True, exist_ok=True)
    (op / "lane_run_manifest.json").write_text(json.dumps(lane_manifests, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    result_rows: list[dict[str, Any]] = []
    for manifest in lane_manifests:
        for row in manifest.get("rows", []):
            item = dict(row)
            item["lane"] = manifest["lane"]
            result_rows.append(item)
    result_df = pd.DataFrame(result_rows)
    if not result_df.empty:
        dedupe_cols = [c for c in ["task_key_json", "partition_json", "status"] if c in result_df.columns]
        if dedupe_cols:
            result_df = result_df.drop_duplicates(subset=dedupe_cols, keep="last")
    checklist_rows: list[dict[str, Any]] = []
    for plan in plan_rows:
        subset = result_df[(result_df.get("source_family", pd.Series(dtype=str)) == plan["source_family"]) & (result_df.get("api_name", pd.Series(dtype=str)) == plan["api_name"])] if not result_df.empty else pd.DataFrame()
        status_counts = subset["status"].value_counts().to_dict() if not subset.empty and "status" in subset.columns else {}
        failed = int(status_counts.get("failed", 0))
        timeout = int(status_counts.get("timeout", 0))
        skipped = int(status_counts.get("skipped", 0))
        already_exists = int(status_counts.get("already_exists", 0))
        pending_adapter = int(status_counts.get("pending_adapter", 0))
        plan_selected = bool(plan.get("selected", plan.get("enabled", False)))
        if dry_run and plan_selected:
            recovery_required = False
            recommended_action = "ready_for_execution"
            execution_status = "dry_run_not_executed"
        else:
            missing_selected = plan_selected and subset.empty and plan.get("planned_tasks", 0) > 0
            recovery_required = failed > 0 or timeout > 0 or pending_adapter > 0 or skipped > 0 or missing_selected
            if not plan_selected:
                recommended_action = "not_selected"
            elif failed > 0 or timeout > 0 or missing_selected:
                recommended_action = "review_recovery_tasks"
            elif pending_adapter > 0:
                recommended_action = "review_pending_adapter"
            elif skipped > 0:
                recommended_action = "review_skipped_tasks"
            else:
                recommended_action = "ok"
            execution_status = plan.get("execution_status", "not_selected") if subset.empty else "executed"
        checklist_rows.append(
            {
                "lane": plan["lane"],
                "source_family": plan["source_family"],
                "api_name": plan["api_name"],
                "priority_tier": plan["priority_tier"],
                "data_theme": plan["data_theme"],
                "acquisition_mode": plan["acquisition_mode"],
                "planned_tasks": plan["planned_tasks"],
                "completed_tasks": int(len(subset)),
                "success_tasks": int(status_counts.get("success", 0)),
                "empty_tasks": int(status_counts.get("empty", 0)),
                "failed_tasks": failed,
                "timeout_tasks": timeout,
                "skipped_tasks": skipped,
                "already_exists_tasks": already_exists,
                "pending_adapter_tasks": pending_adapter,
                "rows": int(pd.to_numeric(subset.get("rows", pd.Series(dtype=int)), errors="coerce").fillna(0).sum()) if not subset.empty else 0,
                "elapsed_sec": float(pd.to_numeric(subset.get("elapsed_sec", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not subset.empty else 0.0,
                "recovery_required": recovery_required,
                "recommended_action": recommended_action,
                "execution_status": execution_status,
            }
        )
    checklist = pd.DataFrame(checklist_rows)
    checklist.to_csv(op / "acquisition_checklist.csv", index=False, encoding="utf-8-sig")
    recovery = checklist[checklist["recovery_required"] == True].copy() if not checklist.empty else checklist  # noqa: E712
    recovery.to_csv(op / "recovery_tasks.csv", index=False, encoding="utf-8-sig")


def print_summary(plan_rows: list[dict[str, Any]], output_root: str | Path) -> None:
    df = pd.DataFrame(plan_rows)
    summary = df.groupby("lane", as_index=False).agg(api_count=("api_name", "count"), selected_api_count=("selected", "sum"), planned_tasks=("planned_tasks", "sum"))
    print("WARNING: high-concurrency timeout is inherited from the current runner and is not a strict hard timeout when max_workers > 1.")
    print("Raw lake preheat plan summary")
    print(summary.to_string(index=False))
    print(f"Review artifacts: {Path(output_root) / REVIEW_DIR}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a local short-window Raw Data Lake preheat using the official raw ingest runner.")
    parser.add_argument("--symbols-file")
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--report-dates")
    parser.add_argument("--lanes", default="main")
    parser.add_argument("--only-families")
    parser.add_argument("--exclude-families")
    parser.add_argument("--only-apis")
    parser.add_argument("--exclude-apis")
    parser.add_argument("--max-workers", type=int, default=64)
    parser.add_argument("--heavy-max-workers", type=int, default=16)
    parser.add_argument("--long-run-max-workers", type=int, default=1)
    parser.add_argument("--deferred-max-workers", type=int, default=4)
    parser.add_argument("--heartbeat-sec", type=float, default=30)
    parser.add_argument("--task-timeout-sec", type=float, default=120)
    parser.add_argument("--manual-selected-task-timeout-sec", type=float, default=180)
    parser.add_argument("--heavy-task-timeout-sec", type=float, default=300)
    parser.add_argument("--long-run-task-timeout-sec", type=float, default=600)
    parser.add_argument("--deferred-task-timeout-sec", type=float, default=300)
    parser.add_argument("--request-sleep", type=float, default=0.10)
    parser.add_argument("--heavy-request-sleep", type=float, default=0.20)
    parser.add_argument("--long-run-request-sleep", type=float, default=0.50)
    parser.add_argument("--task-retry-attempts", type=int, default=2)
    parser.add_argument("--task-retry-sleep-sec", type=float, default=0.0)
    parser.add_argument("--task-retry-backoff", type=float, default=1.0)
    parser.add_argument("--task-retry-jitter-sec", type=float, default=0.0)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--refresh-universe", action="store_true")
    parser.add_argument("--include-deferred-recovery", action="store_true", help="Deprecated alias for --lanes deferred_recovery")
    parser.add_argument("--skip-heavy", action="store_true", help="Deprecated; prefer --lanes without heavy")
    parser.add_argument("--skip-long-run", action="store_true", help="Deprecated; prefer --lanes without long_run")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def _normalize_legacy_args(args: argparse.Namespace) -> None:
    if args.include_deferred_recovery and "deferred_recovery" not in _parse_csv_arg(args.lanes):
        args.lanes = f"{args.lanes},deferred_recovery"
    if args.skip_heavy:
        args.lanes = ",".join(l for l in _parse_csv_arg(args.lanes) if l != "heavy") or "main"
    if args.skip_long_run:
        args.lanes = ",".join(l for l in _parse_csv_arg(args.lanes) if l != "long_run") or "main"


def main(argv: list[str] | None = None, *, ak_module: object | None = None, runner: Callable[..., dict[str, Any]] = run_raw_ingest_official) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _normalize_legacy_args(args)
    reject_drive_output_root(args.output_root)
    skeleton_plan = build_preheat_plan(args, PreheatUniverse())
    universe = discover_universe_for_plan(args, skeleton_plan, ak_module=ak_module)
    write_universe_snapshots(args.output_root, universe)
    plan_rows = build_preheat_plan(args, universe)
    write_plan_artifacts(args.output_root, plan_rows)
    print_summary(plan_rows, args.output_root)
    if args.dry_run:
        write_runtime_artifacts(args.output_root, plan_rows, [], dry_run=True)
        return 0
    manifests = run_lanes(args, universe, plan_rows, runner=runner)
    write_runtime_artifacts(args.output_root, plan_rows, manifests)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import json
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(slots=True)
class MarginPanelBuildConfig:
    """Config for building normalized monthly margin panel.

    Raw layer is source-of-truth and can be small-file partitioned. Normalized panel
    is a monthly compact research-access layer. Storage roots are explicit parameters
    and can point to Google Drive, local disk, or other mounts.
    """

    raw_root: Path
    output_root: Path
    start_date: str
    end_date: str
    raw_dataset_path: Path | None = None
    output_dataset_path: Path | None = None
    artifact_dir: Path | None = None
    source_name: str = "margin_detail"
    source_version: str = "v1"
    normalized_name: str = "margin_panel"
    normalized_version: str = "v1"
    exchanges: tuple[str, ...] = ("SSE", "SZSE")
    output_partition: str = "month"
    overwrite: bool = False
    strict_schema: bool = True
    allow_empty_raw_files: bool = True
    include_name: bool = True
    sort_output: bool = True


@dataclass(slots=True)
class MarginPanelBuildResult:
    panel: pd.DataFrame
    raw_inventory: pd.DataFrame
    read_inventory: pd.DataFrame
    compact_manifest: pd.DataFrame
    summary: dict[str, Any]
    warnings: list[str]


def resolve_raw_dataset_path(config: MarginPanelBuildConfig) -> Path:
    return Path(config.raw_dataset_path) if config.raw_dataset_path else Path(config.raw_root) / config.source_name / config.source_version


def resolve_output_dataset_path(config: MarginPanelBuildConfig) -> Path:
    return Path(config.output_dataset_path) if config.output_dataset_path else Path(config.output_root) / config.normalized_name / config.normalized_version


def _validate_output_guardrails(raw_dataset_path: Path, output_dataset_path: Path) -> None:
    raw_r = raw_dataset_path.resolve()
    out_r = output_dataset_path.resolve()
    if raw_r == out_r:
        raise ValueError("output_dataset_path cannot equal raw_dataset_path")
    if raw_r in out_r.parents:
        raise ValueError("output_dataset_path cannot be inside raw_dataset_path")
    if out_r in raw_r.parents:
        raise ValueError("output_dataset_path cannot be a parent of raw_dataset_path")


def discover_margin_raw_files(config: MarginPanelBuildConfig) -> pd.DataFrame:
    raw_path = resolve_raw_dataset_path(config)
    rows: list[dict[str, Any]] = []
    for path in raw_path.glob("exchange=*/trade_date=*/data.parquet"):
        ex = path.parent.parent.name.split("=", 1)[-1].upper()
        td = path.parent.name.split("=", 1)[-1]
        selected = ex in set(config.exchanges) and (config.start_date <= td <= config.end_date)
        reason = "" if selected else "exchange/date filtered out"
        rows.append({"exchange": ex, "trade_date": td, "path": str(path), "size": path.stat().st_size, "selected": selected, "reason_if_not_selected": reason})
    inv = pd.DataFrame(rows)
    if inv.empty:
        raise FileNotFoundError(f"No raw parquet files discovered under: {raw_path}")
    if not inv["selected"].any():
        raise ValueError(f"No raw files selected for date/exchange filter under: {raw_path}")
    return inv.sort_values(["trade_date", "exchange"]).reset_index(drop=True)


def normalize_margin_raw_frame(df: pd.DataFrame, exchange: str, trade_date_from_path: str, strict_schema: bool = True, include_name: bool = True) -> tuple[pd.DataFrame, list[str]]:
    warnings: list[str] = []
    exchange = exchange.upper()
    if exchange == "SSE":
        mapping = {
            "date": "信用交易日期",
            "code": "标的证券代码",
            "name": "标的证券简称",
            "financing_balance": "融资余额",
            "financing_buy_amount": "融资买入额",
            "financing_repay_amount": "融资偿还额",
            "short_volume": "融券余量",
            "short_sell_volume": "融券卖出量",
            "short_repay_volume": "融券偿还量",
        }
        required = list(mapping.values())
        suffix = ".SH"
    elif exchange == "SZSE":
        mapping = {
            "date": "trade_date",
            "code": "证券代码",
            "name": "证券简称",
            "financing_balance": "融资余额",
            "financing_buy_amount": "融资买入额",
            "short_sell_volume": "融券卖出量",
            "short_volume": "融券余量",
            "short_balance": "融券余额",
            "margin_total_balance": "融资融券余额",
        }
        required = list(mapping.values())
        suffix = ".SZ"
    else:
        raise ValueError(f"Unsupported exchange: {exchange}")

    missing = [c for c in required if c not in df.columns]
    if missing and strict_schema:
        raise ValueError(f"Missing required columns for {exchange}: {missing}")
    if missing:
        warnings.append(f"{exchange} missing columns filled with NaN: {missing}")

    def col(key: str) -> pd.Series:
        c = mapping.get(key)
        if c and c in df.columns:
            return df[c]
        return pd.Series([pd.NA] * len(df), index=df.index)

    out = pd.DataFrame()
    raw_date = col("date")
    out["date"] = pd.to_datetime(raw_date, errors="coerce").fillna(pd.to_datetime(trade_date_from_path))
    out["asset"] = col("code").astype(str).str.zfill(6) + suffix
    out["exchange"] = exchange
    if include_name:
        out["name"] = col("name").astype(str)
    for ncol in ["financing_balance", "financing_buy_amount", "financing_repay_amount", "short_volume", "short_sell_volume", "short_repay_volume", "short_balance", "margin_total_balance"]:
        out[ncol] = pd.to_numeric(col(ncol), errors="coerce")
    if exchange == "SSE":
        out["has_short_balance"] = False
        out["has_margin_total_balance"] = False
    else:
        out["has_short_balance"] = True
        out["has_margin_total_balance"] = True
    out["source_name"] = "margin_detail"
    out["source_version"] = "v1"
    return out, warnings


def build_normalized_margin_panel(config: MarginPanelBuildConfig, show_progress: bool = False) -> MarginPanelBuildResult:
    started = time.perf_counter()
    raw_dataset_path = resolve_raw_dataset_path(config)
    output_dataset_path = resolve_output_dataset_path(config)
    artifact_dir = config.artifact_dir or (output_dataset_path / "_artifacts")
    _validate_output_guardrails(raw_dataset_path, output_dataset_path)

    if config.overwrite and output_dataset_path.exists():
        shutil.rmtree(output_dataset_path)
    output_dataset_path.mkdir(parents=True, exist_ok=True)
    artifact_dir.mkdir(parents=True, exist_ok=True)

    raw_inv = discover_margin_raw_files(config)
    selected = raw_inv[raw_inv["selected"]].copy()
    frames: list[pd.DataFrame] = []
    read_rows: list[dict[str, Any]] = []
    all_warnings: list[str] = []

    for _, r in selected.iterrows():
        p = Path(r["path"])
        rec = {"exchange": r["exchange"], "trade_date": r["trade_date"], "raw_path": str(p), "raw_exists": p.exists(), "raw_size": p.stat().st_size if p.exists() else 0, "raw_rows": 0, "normalized_rows": 0, "status": "ok", "error_type": "", "error_message": ""}
        try:
            raw_df = pd.read_parquet(p)
            rec["raw_rows"] = len(raw_df)
            if raw_df.empty and config.allow_empty_raw_files:
                rec["status"] = "empty_skipped"
                read_rows.append(rec)
                continue
            if raw_df.empty:
                raise ValueError("raw file is empty")
            ndf, ws = normalize_margin_raw_frame(raw_df, rec["exchange"], rec["trade_date"], strict_schema=config.strict_schema, include_name=config.include_name)
            all_warnings.extend(ws)
            rec["normalized_rows"] = len(ndf)
            frames.append(ndf)
        except Exception as exc:
            rec["status"] = "error"
            rec["error_type"] = type(exc).__name__
            rec["error_message"] = str(exc)
            if config.strict_schema:
                read_rows.append(rec)
                raise
        read_rows.append(rec)
        if show_progress:
            print(f"{rec['exchange']} {rec['trade_date']} -> {rec['status']}")

    panel = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if config.sort_output and not panel.empty:
        panel = panel.sort_values(["date", "asset"]).reset_index(drop=True)

    compact = []
    for (year, month), grp in panel.groupby([panel["date"].dt.year, panel["date"].dt.month], sort=True):
        y = f"year={int(year):04d}"; m = f"month={int(month):02d}"
        out_path = output_dataset_path / y / m / "data.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        grp.to_parquet(out_path, index=False)
        sz = out_path.stat().st_size
        compact.append({"year": int(year), "month": int(month), "path": str(out_path), "rows": len(grp), "n_dates": int(grp["date"].nunique()), "n_assets": int(grp["asset"].nunique()), "min_date": str(grp["date"].min().date()), "max_date": str(grp["date"].max().date()), "size": sz, "size_mb": sz / 1024 / 1024})

    read_inv = pd.DataFrame(read_rows)
    compact_manifest = pd.DataFrame(compact)
    summary = {
        "source_name": config.source_name, "source_version": config.source_version,
        "normalized_name": config.normalized_name, "normalized_version": config.normalized_version,
        "start_date": config.start_date, "end_date": config.end_date,
        "raw_root": str(config.raw_root), "raw_dataset_path": str(raw_dataset_path),
        "output_root": str(config.output_root), "output_dataset_path": str(output_dataset_path),
        "artifact_dir": str(artifact_dir), "exchanges": list(config.exchanges),
        "output_partition": config.output_partition, "overwrite": config.overwrite,
        "strict_schema": config.strict_schema, "allow_empty_raw_files": config.allow_empty_raw_files,
        "n_raw_files_discovered": int(len(raw_inv)), "n_raw_files_selected": int(len(selected)),
        "n_raw_files_read": int((read_inv["status"] != "error").sum()) if not read_inv.empty else 0,
        "n_empty_raw_files": int((read_inv["status"] == "empty_skipped").sum()) if not read_inv.empty else 0,
        "n_output_files": int(len(compact_manifest)), "n_rows": int(len(panel)),
        "n_dates": int(panel["date"].nunique()) if not panel.empty else 0,
        "n_assets": int(panel["asset"].nunique()) if not panel.empty else 0,
        "n_sse_rows": int((panel["exchange"] == "SSE").sum()) if not panel.empty else 0,
        "n_szse_rows": int((panel["exchange"] == "SZSE").sum()) if not panel.empty else 0,
        "total_output_size_mb": float(compact_manifest["size_mb"].sum()) if not compact_manifest.empty else 0.0,
        "elapsed_seconds": time.perf_counter() - started,
        "assumptions": ["Raw files follow exchange=*/trade_date=*/data.parquet.", "Overwrite only touches normalized output path."],
        "warnings_count": len(all_warnings),
    }
    (artifact_dir / "manifest.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    read_inv.to_csv(artifact_dir / "raw_to_normalized_inventory.csv", index=False)
    compact_manifest.to_csv(artifact_dir / "compact_manifest.csv", index=False)
    (artifact_dir / "warnings.md").write_text("No warnings recorded.\n" if not all_warnings else "\n".join(f"- {w}" for w in all_warnings) + "\n", encoding="utf-8")

    return MarginPanelBuildResult(panel=panel, raw_inventory=raw_inv, read_inventory=read_inv, compact_manifest=compact_manifest, summary=summary, warnings=all_warnings)


def load_margin_panel(dataset_root: Path, start_date: str, end_date: str, columns: list[str] | None = None, exchanges: list[str] | None = None, set_index: bool = True) -> pd.DataFrame:
    months = pd.period_range(pd.Period(start_date, freq="D").asfreq("M"), pd.Period(end_date, freq="D").asfreq("M"), freq="M")
    parts = []
    for p in months:
        fp = Path(dataset_root) / f"year={p.year:04d}" / f"month={p.month:02d}" / "data.parquet"
        if fp.exists():
            parts.append(pd.read_parquet(fp, columns=columns))
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"])
    df = df[(df["date"] >= pd.to_datetime(start_date)) & (df["date"] <= pd.to_datetime(end_date))]
    if exchanges:
        df = df[df["exchange"].isin([e.upper() for e in exchanges])]
    if set_index and not df.empty:
        df = df.set_index(["date", "asset"]).sort_index()
    return df

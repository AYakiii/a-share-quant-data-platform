"""YAML-backed Tushare source registry loader."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from qsys.data.sources.tushare_contracts import TushareSourceSpec
from qsys.data.sources.tushare_sources import TUSHARE_SOURCE_SPECS as FALLBACK_SOURCE_SPECS

SUPPORTED_QUERY_MODES = {"by_trade_date", "by_date_param", "by_date_range", "snapshot_by_param"}
SUPPORTED_CALENDAR_MODES = {"trading_days", "calendar_days", "range_once", "snapshot"}
SUPPORTED_UNIVERSE_FILTER_MODES = {"ts_code", "none"}
SUPPORTED_COMPACT_BUCKETS = {"year_from_trade_date", "year_from_suspend_date", "window_from_range", "snapshot", "none"}


def default_registry_path() -> Path:
    """Return the repository-local Tushare source registry path."""
    return Path(__file__).resolve().parents[4] / "configs" / "tushare" / "source_registry.yaml"


def _load_registry_payload(path: Path) -> dict[str, Any]:
    """Load the registry payload.

    The checked-in registry is JSON-compatible YAML so the loader avoids adding a
    runtime PyYAML dependency. If the file is absent, callers may use the legacy
    Python fallback through ``load_tushare_source_specs``.
    """
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid Tushare source registry YAML/JSON at {path}: {exc}") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("sources"), list):
        raise ValueError("Tushare source registry must contain a top-level sources list")
    return payload


def _as_tuple(value: Any, *, field: str, api_name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise ValueError(f"Tushare registry source {api_name!r} field {field!r} must be a non-empty string list")
    return tuple(value)


def _validate_supported(spec: TushareSourceSpec) -> None:
    if spec.query_mode not in SUPPORTED_QUERY_MODES:
        raise NotImplementedError(f"unsupported Tushare query_mode for {spec.api_name}: {spec.query_mode}")
    if spec.calendar_mode not in SUPPORTED_CALENDAR_MODES:
        raise NotImplementedError(f"unsupported Tushare calendar_mode for {spec.api_name}: {spec.calendar_mode}")
    if spec.universe_filter_mode not in SUPPORTED_UNIVERSE_FILTER_MODES:
        raise NotImplementedError(f"unsupported Tushare universe_filter_mode for {spec.api_name}: {spec.universe_filter_mode}")
    if spec.compact_bucket not in SUPPORTED_COMPACT_BUCKETS:
        raise NotImplementedError(f"unsupported Tushare compact_bucket for {spec.api_name}: {spec.compact_bucket}")
    keys = spec.partition_keys or ((spec.partition_key,) if spec.partition_key else ())
    if not keys:
        raise ValueError(f"Tushare source {spec.api_name} must declare partition_key(s)")
    if spec.query_mode == "by_date_param" and not spec.request_date_param:
        raise ValueError(f"Tushare source {spec.api_name} requires request_date_param")
    if spec.query_mode == "by_date_range" and (not spec.range_start_param or not spec.range_end_param):
        raise ValueError(f"Tushare source {spec.api_name} requires range_start_param/range_end_param")


def _spec_from_row(row: Any) -> TushareSourceSpec:
    if not isinstance(row, dict):
        raise ValueError("each Tushare source registry row must be a mapping")
    api_name = str(row.get("api_name") or "")
    source_family = str(row.get("source_family") or "")
    if not api_name or not source_family:
        raise ValueError("each Tushare source registry row requires source_family and api_name")
    spec = TushareSourceSpec(
        source_family=source_family,
        api_name=api_name,
        fields=_as_tuple(row.get("fields"), field="fields", api_name=api_name),
        query_mode=str(row.get("query_mode") or ""),
        calendar_mode=str(row.get("calendar_mode") or ""),
        partition_key=str(row.get("partition_key") or ""),
        partition_keys=tuple(row.get("partition_keys") or ()),
        request_date_param=row.get("request_date_param"),
        range_start_param=row.get("range_start_param"),
        range_end_param=row.get("range_end_param"),
        static_params=row.get("static_params"),
        param_grid=row.get("param_grid"),
        primary_key=_as_tuple(row.get("primary_key"), field="primary_key", api_name=api_name),
        universe_filter_mode=str(row.get("universe_filter_mode") or ""),
        empty_result_allowed=bool(row.get("empty_result_allowed", False)),
        compact_bucket=str(row.get("compact_bucket") or ""),
        status=str(row.get("status") or "candidate"),
        production_enabled=bool(row.get("production_enabled", False)),
    )
    _validate_supported(spec)
    return spec


def load_tushare_source_specs(path: str | Path | None = None, *, allow_python_fallback: bool = True) -> tuple[TushareSourceSpec, ...]:
    """Load Tushare source specs from the YAML registry, falling back to legacy constants only when absent."""
    registry_path = Path(path) if path is not None else default_registry_path()
    if not registry_path.exists():
        if allow_python_fallback:
            return FALLBACK_SOURCE_SPECS
        raise FileNotFoundError(f"Tushare source registry not found: {registry_path}")
    specs = tuple(_spec_from_row(row) for row in _load_registry_payload(registry_path)["sources"])
    seen: set[str] = set()
    for spec in specs:
        if spec.api_name in seen:
            raise ValueError(f"duplicate Tushare api_name in registry: {spec.api_name}")
        seen.add(spec.api_name)
    return specs


def source_specs_by_api(path: str | Path | None = None) -> dict[str, TushareSourceSpec]:
    """Return YAML-backed Tushare source specs keyed by API name."""
    return {spec.api_name: spec for spec in load_tushare_source_specs(path)}

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

RunStatus = Literal[
    "success",
    "empty",
    "failed",
    "missing",
    "timeout",
    "skipped",
    "non_dataframe",
]


@dataclass(frozen=True)
class SourceCase:
    case_id: str
    source_family: str
    api_name: str
    kwargs: dict[str, Any]
    description: str
    enabled: bool = True


@dataclass
class SourceRunResult:
    run_id: str
    case_id: str
    source_family: str
    api_name: str
    enabled: bool
    status: RunStatus
    rows: int = 0
    n_cols: int = 0
    columns: list[str] = field(default_factory=list)
    date_like_columns: list[str] = field(default_factory=list)
    symbol_like_columns: list[str] = field(default_factory=list)
    announcement_like_columns: list[str] = field(default_factory=list)
    has_date_like_column: bool = False
    has_symbol_like_column: bool = False
    has_announcement_like_column: bool = False
    kwargs_json: str = "{}"
    filtered_kwargs_json: str = "{}"
    ignored_kwargs_json: str = "{}"
    output_path: str = ""
    metadata_path: str = ""
    error_type: str = ""
    error_message: str = ""
    skipped_reason: str = ""
    timeout_seconds: float = 0.0
    elapsed_seconds: float = 0.0
    started_at: str = ""
    ended_at: str = ""

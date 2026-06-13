"""Deprecated AkShare raw ingest compatibility module.

Use :mod:`qsys.data.factor_lake.akshare_raw_ingest` for new code.
"""
from __future__ import annotations

from qsys.data.factor_lake import akshare_raw_ingest as _akshare_raw_ingest
from qsys.data.factor_lake.akshare_raw_ingest import *  # noqa: F401,F403


def __getattr__(name: str):
    """Forward legacy attribute access, including private test helpers."""
    return getattr(_akshare_raw_ingest, name)

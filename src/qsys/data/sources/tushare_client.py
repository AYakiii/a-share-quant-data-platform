"""Safe Tushare client bootstrap helpers."""
from __future__ import annotations

import getpass
import os
from typing import Any

import pandas as pd


def read_tushare_token(*, allow_prompt: bool = False) -> str:
    """Read a Tushare token without printing or persisting it."""
    token = os.environ.get("TUSHARE_TOKEN", "").strip()
    if token:
        return token
    if allow_prompt:
        token = getpass.getpass("TUSHARE_TOKEN: ").strip()
        if token:
            return token
    raise RuntimeError("TUSHARE_TOKEN is required; provide it via environment or interactive prompt.")


class TushareClient:
    """Thin wrapper around the Tushare SDK for easier mocking in tests."""

    def __init__(self, token: str | None = None) -> None:
        self._token = token or read_tushare_token(allow_prompt=False)
        import tushare as ts  # type: ignore[import-not-found]

        ts.set_token(self._token)
        self._pro = ts.pro_api()

    def query(self, api_name: str, **params: Any) -> pd.DataFrame:
        """Call a Tushare API by name and return a DataFrame."""
        fn = getattr(self._pro, api_name)
        return fn(**params)

"""Safe Tushare client bootstrap helpers."""
from __future__ import annotations

import getpass
import os


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

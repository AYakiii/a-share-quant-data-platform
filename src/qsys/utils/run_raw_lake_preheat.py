"""Deprecated compatibility entrypoint for AkShare Raw Lake preheat."""
from __future__ import annotations

import sys

from qsys.utils.run_akshare_raw_lake_preheat import main

_DEPRECATION = "qsys.utils.run_raw_lake_preheat is deprecated.\nUse qsys.utils.run_akshare_raw_lake_preheat instead."

if __name__ == "__main__":
    print(_DEPRECATION, file=sys.stderr)
    raise SystemExit(main())

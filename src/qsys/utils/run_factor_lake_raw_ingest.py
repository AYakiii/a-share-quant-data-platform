"""Deprecated compatibility entrypoint for AkShare Raw ingest."""
from __future__ import annotations

import sys

from qsys.utils.run_akshare_raw_ingest import main

_DEPRECATION = "qsys.utils.run_factor_lake_raw_ingest is deprecated.\nUse qsys.utils.run_akshare_raw_ingest instead."

if __name__ == "__main__":
    print(_DEPRECATION, file=sys.stderr)
    raise SystemExit(main())

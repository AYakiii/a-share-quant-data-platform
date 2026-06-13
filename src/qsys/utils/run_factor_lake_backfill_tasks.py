"""Deprecated compatibility entrypoint for AkShare Raw backfill tasks."""
from __future__ import annotations

import sys

from qsys.utils.run_akshare_backfill_tasks import main

_DEPRECATION = "qsys.utils.run_factor_lake_backfill_tasks is deprecated.\nUse qsys.utils.run_akshare_backfill_tasks instead."

if __name__ == "__main__":
    print(_DEPRECATION, file=sys.stderr)
    raise SystemExit(main())

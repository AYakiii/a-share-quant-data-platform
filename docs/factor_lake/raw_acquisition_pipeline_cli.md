# Raw Acquisition Pipeline CLI

This CLI productizes the proven raw acquisition workflow by orchestrating existing modules instead of replacing them.

- Reuses `run_p0_raw_acquisition_wave.py` for `pull --profile p0`.
- Reuses existing P0 artifacts (`p0_wave_catalog.csv`, `p0_wave_manifest.json`, `p0_wave_summary.json`, `p0_final_acceptance_report.json`).
- Keeps local staging before any Drive promotion.

## Commands

- `pull`: runs existing P0 runner (local-only; Drive path rejected).
- `validate`: checks artifacts, acceptance, forbidden APIs, and `/nan/` path safety.
- `compact`: copies validated outputs into compact asset with row conservation.
- `promote`: dry-run by default; copy only with `--promote-to-drive`; overwrite blocked unless `--allow-overwrite`.
- `qa`: validates promoted asset integrity and optional row parity against compact asset.

## P0 Examples

Use:

```bash
PYTHONPATH=src python -m qsys.utils.run_raw_acquisition_pipeline pull --profile p0 --start-date 2010-01-01 --end-date 2010-12-31 --local-root /tmp/raw_local
PYTHONPATH=src python -m qsys.utils.run_raw_acquisition_pipeline validate --profile p0 --run-dir latest --local-root /tmp/raw_local
PYTHONPATH=src python -m qsys.utils.run_raw_acquisition_pipeline compact --profile p0 --run-dir latest --local-root /tmp/raw_local --compact-root /tmp/raw_compact/p0_2010
PYTHONPATH=src python -m qsys.utils.run_raw_acquisition_pipeline promote --profile p0 --compact-root /tmp/raw_compact/p0_2010 --drive-root /content/gdrive/MyDrive/a_share_quant_cache --asset-name p0_2010
PYTHONPATH=src python -m qsys.utils.run_raw_acquisition_pipeline promote --profile p0 --compact-root /tmp/raw_compact/p0_2010 --drive-root /content/gdrive/MyDrive/a_share_quant_cache --asset-name p0_2010 --promote-to-drive
PYTHONPATH=src python -m qsys.utils.run_raw_acquisition_pipeline qa --profile p0 --drive-root /content/gdrive/MyDrive/a_share_quant_cache --asset-name p0_2010 --compact-root /tmp/raw_compact/p0_2010
```

## Future profiles

Add `p1` / `p2` by extending `acquisition_profiles.py`; keep existing raw ingest and runner implementations unchanged.

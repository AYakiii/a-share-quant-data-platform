# Phase 18A-20 Module Boundary Audit

| File | Classification | Action |
|---|---|---|
| src/qsys/utils/run_factor_lake_raw_ingest.py | official_production_path | keep |
| src/qsys/data/factor_lake/raw_ingest.py | official_production_path | migrate |
| src/qsys/data/factor_lake/registry.py | official_production_path | keep |
| src/qsys/data/factor_lake/io.py | production_support_module | keep |
| src/qsys/data/factor_lake/metastore.py | production_support_module | keep |
| src/qsys/data/factor_lake/acquisition_universe.py | production_support_module | migrate |
| src/qsys/data/factor_lake/local_api.py | production_support_module | keep |
| src/qsys/utils/run_factor_lake_raw_coverage_ingest.py | obsolete_test_runner | delete |
| src/qsys/utils/run_factor_lake_raw_ingest_mvp.py | obsolete_mvp_runner | delete |
| src/qsys/utils/run_factor_lake_probe.py | internal_probe_or_health_check | delete |
| src/qsys/utils/run_factor_lake_p0_smoke.py | obsolete_test_runner | delete |
| src/qsys/utils/run_factor_lake_backfill_tasks.py | production_support_module | keep |
| src/qsys/utils/build_raw_warehouse.py | legacy_warehouse_reference | migrate |
| src/qsys/data/factor_lake/probe.py | internal_probe_or_health_check | rename |
| src/qsys/data/factor_lake/backfill_execute.py | production_support_module | keep |
| tests/data/factor_lake | test_only | update tests |

Notes:
- Official entrypoint is `qsys.utils.run_factor_lake_raw_ingest`.
- Coverage/probe/smoke/mvp runners are demoted/removed from public acquisition path.
- Acquisition universe now fails loud when required production universe files are missing.

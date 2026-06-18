> Governance migration notice (2026-06-18): This file is retained in the engineering repository as a historical technical audit report. Governance-side evidence memory has been migrated to `AYakiii/a-share-quant-project-governance` in PR #29, target path `evidence_cards/2026-06-18_akshare_tushare_provider_boundary_governance_evidence.md`. Do not treat this engineering doc as the current governance source of truth without checking the governance repository and later engineering PRs.

# AkShare/Tushare Provider Boundary Governance (M0)

## 1. 修改文件清单

- AkShare entrypoints and implementation were renamed to provider-explicit modules under `src/qsys/utils/` and `src/qsys/data/factor_lake/`.
- Shared Raw Lake path helpers in `src/qsys/data/factor_lake/io.py`, `src/qsys/data/factor_lake/raw_compact.py`, and `src/qsys/utils/raw_lake_compact_cli.py` now accept `provider` and operator-defined `dataset_version` where applicable; `storage_schema_version` is retained only as a short-term compatibility alias in shared compact artifacts.
- Shared warehouse metadata now carries `source_symbol` while retaining `akshare_symbol` as an AkShare-specific compatibility field.
- Tushare dry-run skeleton modules were added under `src/qsys/data/sources/` and `src/qsys/utils/run_tushare_raw_ingest.py`.
- README and legacy acquisition pipeline docs were updated with provider-explicit active entrypoints and legacy notices.
- Tests were added/updated for provider paths, Tushare dry-run token safety, renamed AkShare entrypoints, and compact path contracts.

## 2. 文件改名映射

| Old | New | Status |
| --- | --- | --- |
| `src/qsys/utils/run_factor_lake_raw_ingest.py` | `src/qsys/utils/run_akshare_raw_ingest.py` | old path retained as deprecated forwarding shell |
| `src/qsys/data/factor_lake/raw_ingest.py` | `src/qsys/data/factor_lake/akshare_raw_ingest.py` | old path retained as deprecated import shell |
| `src/qsys/utils/run_raw_lake_preheat.py` | `src/qsys/utils/run_akshare_raw_lake_preheat.py` | old path retained as deprecated forwarding shell |
| `src/qsys/utils/raw_lake_colab_console.py` | `src/qsys/utils/akshare_raw_lake_colab_console.py` | old path retained as deprecated import shell |
| `src/qsys/utils/run_p0_raw_acquisition_wave.py` | `src/qsys/utils/run_akshare_p0_raw_acquisition_wave.py` | old path retained as deprecated import shell |
| `src/qsys/data/factor_lake/probe.py` | `src/qsys/data/factor_lake/akshare_probe.py` | ambiguous generic probe name removed |

## 3. 导出符号改名映射

| Old | New | Compatibility |
| --- | --- | --- |
| `run_raw_ingest` | `run_akshare_raw_ingest` | deprecated alias retained in AkShare module |
| `run_raw_ingest_official` | `run_akshare_raw_ingest_official` | deprecated alias retained |
| `DEFAULT_ADAPTERS` | `AKSHARE_DEFAULT_ADAPTERS` | deprecated alias retained |
| `COVERAGE_API_SPECS` | `AKSHARE_COVERAGE_API_SPECS` | deprecated alias retained |
| `API_POLICY_METADATA` | `AKSHARE_API_POLICY_METADATA` | deprecated alias retained |
| `PHASE_COVERAGE_FAMILIES` | `AKSHARE_PHASE_COVERAGE_FAMILIES` | deprecated alias retained |
| `EXCLUDED_APIS` | `AKSHARE_EXCLUDED_APIS` | deprecated alias retained |
| `TEMP_DISABLED_APIS` | `AKSHARE_TEMP_DISABLED_APIS` | deprecated alias retained |

## 4. 兼容壳清单

- `qsys.utils.run_factor_lake_raw_ingest` prints a deprecation warning and forwards to `qsys.utils.run_akshare_raw_ingest`.
- `qsys.data.factor_lake.raw_ingest` imports from `qsys.data.factor_lake.akshare_raw_ingest` only.
- `qsys.utils.run_raw_lake_preheat` prints a deprecation warning and forwards to `qsys.utils.run_akshare_raw_lake_preheat`.
- `qsys.utils.raw_lake_colab_console` imports from `qsys.utils.akshare_raw_lake_colab_console` only.
- `qsys.utils.run_p0_raw_acquisition_wave` imports from `qsys.utils.run_akshare_p0_raw_acquisition_wave` only.

## 5. Legacy 模块清单

- `src/qsys/utils/run_raw_acquisition_pipeline.py`: legacy reference, superseded by DWH3.0 Raw Lake workflow; do not extend for Tushare. Deletion condition: historical P0 pipeline callers have migrated.
- `docs/factor_lake/raw_acquisition_pipeline_cli.md`: legacy CLI documentation, retained for historical compatibility.
- `src/qsys/utils/build_raw_warehouse.py`: legacy warehouse reference; not a recommended public entrypoint and not connected to Tushare.

## 6. 新发现的污染风险点

- Symbol batching in the former generic ingest CLI recursively invoked the old module name; it now invokes the AkShare-explicit module.
- The Colab console constructed the generic preheat module name; it now constructs the AkShare-explicit preheat command.
- Shared compact roots were hard-coded to AkShare; they are now parameterized while defaulting to AkShare.
- Common warehouse artifacts required `akshare_symbol`; `source_symbol` was added as the provider-neutral field, with `akshare_symbol` retained for AkShare extension compatibility.

## 7. 处理理由

The changes isolate AkShare-specific acquisition names without building a full provider framework. Shared Raw Lake components remain provider-neutral and only gained thin `provider` and operator-defined `dataset_version` parameters required to prevent Tushare/AkShare staging, compact package, Drive target, and catalog collisions.

## 8. 未处理问题

- Historical docs still mention old phase names and AkShare-only paths where they document historical runs; these were not mechanically rewritten.
- AkShare adapter internals still emit `akshare_symbol` for provider-specific metadata compatibility.
- The Tushare skeleton does not perform API pulls, schema normalization, or Drive promotion.

## 9. 测试命令

- `PYTHONPATH=src pytest -q tests/data/sources/test_tushare_skeleton.py tests/data/factor_lake/test_provider_paths.py tests/utils/test_raw_lake_compact_cli.py tests/data/factor_lake/test_run_factor_lake_raw_ingest_cli.py tests/utils/test_run_raw_lake_preheat.py tests/utils/test_run_p0_raw_acquisition_wave.py`
- `python -m py_compile $(find src/qsys -name '*.py' -print)`
- governance scan with `rg -n 'run_factor_lake_raw_ingest|qsys\.data\.factor_lake\.raw_ingest|run_raw_lake_preheat|raw_lake_colab_console|run_p0_raw_acquisition_wave|run_raw_acquisition_pipeline|build_raw_warehouse|data/raw/akshare|raw/akshare|akshare_symbol|DEFAULT_ADAPTERS|COVERAGE_API_SPECS|API_POLICY_METADATA|run_raw_ingest_official|run_raw_ingest' .`

## 10. 测试结果

- Targeted pytest suite: 97 passed, 4 warnings. Full suite: 583 passed, 1 skipped, 20 warnings.
- Python compilation check: passed.

## 11. `rg` 扫描结果摘要

Final governance scan returned 240 matches. Remaining matches are expected categories:

- Deprecated compatibility shells and aliases.
- AkShare implementation internals and AkShare-specific tests using `akshare_symbol`.
- Legacy documentation explicitly marked as legacy.
- Historical docs not mechanically rewritten.
- README warning that Drive workflows must not create the old `<drive-dwh-root>/data/raw/akshare/...` layout.

## 12. Drive 声明

No Google Drive files were written, promoted, audited, modified, or deleted in this M0 task.

## 13. Tushare 数据拉取声明

No formal Tushare historical API pull was started. The new Tushare CLI only validates inputs and emits a token-free dry-run manifest.

## 14. 下一阶段建议

- Add one narrowly scoped Tushare source implementation behind the dry-run contract.
- Define per-source Raw storage schema contracts before enabling non-dry-run writes.
- Add integration tests using temporary local roots before any Drive prepare/promotion workflow is considered.


## PR136 Follow-up Review Fixes

- Restored AkShare legacy compact target paths so default AkShare prepare writes `raw/akshare/<family>/<api>/<bucket>/data.parquet` without inserting a schema-version layer.
- Made `dataset_version` optional for AkShare legacy paths and required for non-AkShare providers such as Tushare; Tushare prepare uses `raw/tushare/<family>/<api>/<dataset_version>/<bucket>/data.parquet`.
- Added conservative path-segment validation for `provider` and non-empty `dataset_version`, rejecting empty provider, `.`, `..`, absolute paths, separators, traversal, and non-slug input.
- Added Tushare universe lineage fields: `symbols_file`, `universe_sha256`, `symbol_row_count`, and `unique_symbol_count`; duplicate, empty, illegal-format, and expected-count mismatches are rejected.
- Fixed the Tushare CLI so it is hard-bound to `provider="tushare"`; only shared compact CLI accepts operator-supplied provider.
- Removed implicit `AKSHARE_DEFAULT_ADAPTERS` fallback from shared backfill execution; legacy AkShare wrappers now supply AkShare adapters explicitly.
- Added `provider` to RawWarehouse manifests and kept shared artifact fixed fields to `original_symbol` and `source_symbol`, with `akshare_symbol` remaining provider-specific optional metadata.
- Added `provider` to local Raw read APIs with legacy default `akshare`.
- Prepare review output now includes `provider`, `dataset_version`, and `prepared_drive_raw_root`.

### Follow-up tests

- Full suite rerun: `PYTHONPATH=src pytest -q` → 607 passed, 1 skipped, 20 warnings.
- Compile check rerun: `python -m py_compile $(find src/qsys -name '*.py' -print)` → passed.
- Tushare hard-code scan rerun for forbidden universe/count literals in Tushare modules → no matches.


## PR136 Final Backfill/Tushare Validation Follow-up

- Added `qsys.utils.run_akshare_backfill_tasks` as the explicit AkShare backfill CLI and converted `qsys.utils.run_factor_lake_backfill_tasks` into a deprecated forwarding shell with a clear migration message.
- Added explicit `provider` parameters to shared `execute_backfill_task()` and `execute_backfill_tasks()`; non-dry-run shared execution now requires both an adapter registry and provider, and passes `provider` to `write_raw_partition()`.
- AkShare backfill wrappers now explicitly pass `provider="akshare"`.
- Added tests proving shared backfill with `provider="tushare"` writes under `data/raw/tushare/...` and does not silently write AkShare paths.
- Added Tushare dry-run validation for YYYYMMDD dates, ordered date range, non-empty safe dataset version, positive expected symbol count, and non-empty universe name.
- Local API provider read coverage remains in place for `provider="tushare"`.

### Final follow-up tests

- Full suite rerun: `PYTHONPATH=src pytest -q` → 617 passed, 1 skipped, 20 warnings.
- Compile check rerun: `python -m py_compile $(find src/qsys -name '*.py' -print)` → passed.
- Tushare hard-code scan rerun for forbidden universe/count literals in Tushare modules → no matches.


## PR136 Canonical Universe / Tushare ts_code Boundary Follow-up

- Tushare dry-run Universe loading now accepts provider-neutral canonical six-digit symbols directly.
- Canonical Universe lineage (`symbols_file`, `universe_sha256`, row count, unique count) remains based on the operator's original input file.
- Tushare `ts_code` is documented and handled as a provider-specific API representation, not as the baseline Universe format.
- Added `canonical_symbol_from_ts_code()` for future API-result filtering workflows that need to derive canonical symbols from provider-specific `ts_code` values.
- No provider-specific baseline Universe file is created or required.

### Canonical Universe follow-up tests

- Full suite rerun: `PYTHONPATH=src pytest -q` → 619 passed, 1 skipped, 20 warnings.
- Compile check rerun: `python -m py_compile $(find src/qsys -name '*.py' -print)` → passed.
- Tushare hard-code scan rerun for forbidden universe/count literals in Tushare modules → no matches.


## PR136 Dataset Version Naming Follow-up

- Replaced Tushare dry-run `storage_schema_version` semantics with required `dataset_version`.
- `dataset_version` is an operator-defined dataset namespace. It can be a short name or a descriptive namespace such as `v1_csi500_2021_2025_union` or `v2_all_a_share`; code must not default to a bare version string.
- Shared compact CLI now accepts `--dataset-version`; `--storage-schema-version` remains only as a deprecated compatibility alias.
- Non-AkShare compact paths use `raw/<provider>/<family>/<api>/<dataset_version>/<bucket>/data.parquet`, while AkShare legacy compact paths still omit the version layer by default.
- Universe lineage remains anchored by `symbols_file`, `universe_sha256`, and `universe_name`. Canonical Universe files use six-digit symbols; Tushare `ts_code` remains provider-specific API representation.

### Dataset version follow-up tests

- Full suite rerun: `PYTHONPATH=src pytest -q` → 621 passed, 1 skipped, 20 warnings.
- Compile check rerun: `python -m py_compile $(find src/qsys -name '*.py' -print)` → passed.

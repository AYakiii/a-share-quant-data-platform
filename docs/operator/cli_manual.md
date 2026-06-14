# Project CLI Manual

This manual is the project-level operator entrypoint for currently active command-line workflows. Human operators, Codex runs, and Colab notebooks should consult this file before composing commands.

It is not a historical report and it is not a replacement for README. It documents the current executable CLI surface and safety boundaries.

## Safety rules

- Do not run production promotion commands without human review of the prepared package.
- Do not write Google Drive except through the explicit `raw_lake_compact_cli promote` command.
- Do not treat Tushare dry-run as real acquisition; it validates operator inputs only.
- Do not create provider-specific baseline Universe files. Canonical Universe files use six-digit symbols; Tushare `ts_code` is a provider-specific API representation.
- Keep AkShare legacy compact layout unchanged unless an operator explicitly chooses a non-AkShare provider/dataset version.

## Active CLI overview

| CLI | Purpose | Provider | Writes local staging | Writes Drive | Calls external API | Dry-run support | Status | Main inputs | Main outputs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `qsys.utils.run_akshare_raw_ingest` | AkShare Raw Lake acquisition for selected families/APIs. | `akshare` | Yes, under `output_root/data/raw/akshare/...` | No | Yes, AkShare | No formal dry-run; use narrow date/family scopes for smoke runs | Active | `output-root`, date window, families, symbols/universe inputs, worker/retry options | Local Raw parquet, metadata, ingest catalog/review artifacts |
| `qsys.utils.run_akshare_raw_lake_preheat` | Build/execute AkShare preheat lanes for Raw Lake coverage. | `akshare` | Yes, through AkShare ingest runner | No | Yes, AkShare | Planning-oriented modes/options may be available; verify `--help` before use | Active | `output-root`, date window, universe discovery inputs, lane/worker controls | Preheat plans, lane manifests, local Raw staging where executed |
| `qsys.utils.run_akshare_backfill_tasks` | Execute or dry-run AkShare backfill task CSV/registry plans with AkShare adapters explicitly supplied. | `akshare` | Yes when `--execute` | No | Yes when `--execute` | Yes; default is dry-run unless `--execute` is supplied | Active | `output-root`, `metastore-path`, filters, `max-tasks`, `--execute` | Backfill task result rows, local Raw partitions for executed tasks |
| `qsys.utils.run_tushare_raw_ingest` | Validate Tushare operator inputs and emit a token-free dry-run manifest. | `tushare` | No formal Raw write in M0; reports intended local staging root | No | No formal historical API pull in M0; only token presence is checked | Required: `--dry-run` | Active skeleton | `symbols-file`, `universe-name`, `dataset-version`, expected count, date window, `output-root`, `TUSHARE_TOKEN` | Token-free dry-run manifest printed to stdout |
| `qsys.utils.raw_lake_compact_cli prepare` | Build local compact package and Drive collision/readiness plan for review. | Operator-supplied; default `akshare` | Reads local staging | No Drive Raw writes; reads Drive target state | No provider API calls | It is a local prepare/dry-run-like planning step | Active | `provider`, optional/required `dataset-version`, `output-root`, `drive-dwh-root`, `promotion-name`, date window | Local compact package, `compact_manifest.json`, `drive_collision_plan.csv`, `READY_FOR_PROMOTION.json` |
| `qsys.utils.raw_lake_compact_cli promote` | Human-gated promotion of reviewed compact package to Drive. | From package manifest | No local staging writes | **Yes; only command that writes Drive Raw** | No provider API calls | No | Active, human-gated | `package-root`, `drive-dwh-root`, exact `confirm-promotion`, reviewed bucket opt-ins | Drive Raw files, Drive catalog artifacts, local promotion attempt report |
| `qsys.utils.raw_lake_compact_cli audit` | Read-only audit of promoted Drive Raw assets. | From promoted manifest | No | No writes; reads Drive | No provider API calls | N/A, read-only | Active | `promotion-name`, `drive-dwh-root` | Console audit summary; no mutation |

## Recommended scheduling order

### AkShare production-style Raw Lake flow

1. Run AkShare acquisition or preheat:
   - `qsys.utils.run_akshare_raw_ingest`, or
   - `qsys.utils.run_akshare_raw_lake_preheat` when using preheat lanes.
2. Confirm local staging exists under `output_root/data/raw/akshare/...`.
3. Run `qsys.utils.raw_lake_compact_cli prepare`.
4. Human-review all readiness artifacts:
   - `READY_FOR_PROMOTION.json`
   - `drive_collision_plan.csv`
   - `compact_manifest.json`
   - QA and known-gap artifacts.
5. Only after review, run `qsys.utils.raw_lake_compact_cli promote`.
6. Run `qsys.utils.raw_lake_compact_cli audit` for read-only verification.

### Tushare current M0 flow

Tushare currently stops at:

```text
qsys.utils.run_tushare_raw_ingest --dry-run
```

This validates token presence, canonical Universe lineage, `dataset_version`, expected symbol count, and date inputs. It does **not** perform real Tushare acquisition, does **not** write Drive, and does **not** save tokens.

## Command templates

### AkShare Raw ingest

```bash
PYTHONPATH=src python -m qsys.utils.run_akshare_raw_ingest \
  --output-root outputs/factor_lake_raw \
  --start-date 20220101 \
  --end-date 20241231 \
  --families market_price,index_market,margin_leverage
```

Notes:
- Writes local Raw staging only.
- Calls AkShare APIs.
- Does not write Drive.
- Use narrow date/family/symbol scopes for smoke runs.

### AkShare Raw Lake preheat

```bash
PYTHONPATH=src python -m qsys.utils.run_akshare_raw_lake_preheat \
  --output-root outputs/factor_lake_raw_preheat \
  --start-date 20220101 \
  --end-date 20241231
```

Notes:
- Intended for AkShare coverage/preheat orchestration.
- Check `--help` for lane and universe options before a large run.
- Does not write Drive.

### AkShare backfill tasks

Dry-run:

```bash
PYTHONPATH=src python -m qsys.utils.run_akshare_backfill_tasks \
  --output-root outputs/factor_lake_backfill \
  --metastore-path outputs/factor_lake_backfill/metastore.sqlite \
  --max-tasks 5 \
  --dry-run
```

Execute a bounded batch:

```bash
PYTHONPATH=src python -m qsys.utils.run_akshare_backfill_tasks \
  --output-root outputs/factor_lake_backfill \
  --metastore-path outputs/factor_lake_backfill/metastore.sqlite \
  --max-tasks 1 \
  --execute
```

Notes:
- `--execute` may call AkShare APIs and write local Raw partitions.
- `--execute` requires `--max-tasks` for safety.
- Does not write Drive.

### Tushare dry-run only

```bash
PYTHONPATH=src python -m qsys.utils.run_tushare_raw_ingest \
  --dry-run \
  --symbols-file stock_universe_v1_symbols.txt \
  --universe-name stock_universe_v1 \
  --dataset-version v1_csi500_2021_2025_union \
  --expected-symbol-count 846 \
  --start-date 20220101 \
  --end-date 20260612 \
  --output-root outputs/tushare_raw
```

Future dataset namespace example:

```bash
PYTHONPATH=src python -m qsys.utils.run_tushare_raw_ingest \
  --dry-run \
  --symbols-file stock_universe_v2_symbols.txt \
  --universe-name stock_universe_v2 \
  --dataset-version v2_all_a_share \
  --expected-symbol-count 920 \
  --start-date 20220101 \
  --end-date 20260612 \
  --output-root outputs/tushare_raw
```

Notes:
- Requires `TUSHARE_TOKEN` in the environment, but does not print or persist it.
- Does not perform formal Tushare historical API pulls in M0.
- Canonical Universe files use six-digit symbols such as `000008`; Tushare `ts_code` such as `000008.SZ` is provider-specific API representation.
- `dataset_version` is an operator-defined dataset namespace. It is not a code default and not a schema version.

### Compact prepare

AkShare legacy default path layout:

```bash
PYTHONPATH=src python -m qsys.utils.raw_lake_compact_cli prepare \
  --provider akshare \
  --output-root outputs/factor_lake_raw \
  --drive-dwh-root /content/gdrive/MyDrive/a_share_quant_data \
  --promotion-name akshare_raw_prepare_20220101_20241231
```

Tushare dataset-version-isolated layout:

```bash
PYTHONPATH=src python -m qsys.utils.raw_lake_compact_cli prepare \
  --provider tushare \
  --dataset-version v1_csi500_2021_2025_union \
  --output-root outputs/tushare_raw \
  --drive-dwh-root /content/gdrive/MyDrive/a_share_quant_data \
  --promotion-name tushare_v1_smoke_prepare
```

Notes:
- `prepare` creates a local compact package and review files.
- `prepare` may read Drive target state to build a collision plan.
- `prepare` does **not** write Drive Raw parquet.
- For non-AkShare providers, `--dataset-version` is required.

### Compact promote

```bash
PYTHONPATH=src python -m qsys.utils.raw_lake_compact_cli promote \
  --package-root outputs/raw_acquisition_compact/tushare_v1_smoke_prepare \
  --drive-dwh-root /content/gdrive/MyDrive/a_share_quant_data \
  --confirm-promotion tushare_v1_smoke_prepare
```

Notes:
- This is the **only active command that writes Drive Raw assets**.
- Only run after human review of `READY_FOR_PROMOTION.json` and `drive_collision_plan.csv`.
- Promotion blocks non-identical overwrites and changed collision plans.
- Add `--allow-reviewed-bucket-kinds scope,snapshot` only after reviewing those bucket kinds when required.

### Compact audit

```bash
PYTHONPATH=src python -m qsys.utils.raw_lake_compact_cli audit \
  --promotion-name tushare_v1_smoke_prepare \
  --drive-dwh-root /content/gdrive/MyDrive/a_share_quant_data
```

Notes:
- Audit is read-only.
- It reopens promoted parquet files from Drive and verifies manifest expectations.
- It should not create, update, or delete Drive files.

## Parameter semantics

### `provider`

Provider namespace for Raw paths and manifests.

Examples:
- `akshare`
- `tushare`

`akshare` remains the default for legacy shared compact behavior. Non-AkShare providers require explicit `dataset_version` in compact prepare.

### `dataset_version`

Operator-defined dataset namespace used to physically isolate different dataset boundaries.

It is:
- not a code default version,
- not a schema version,
- not inferred from Universe file names,
- required for Tushare dry-run and non-AkShare compact prepare.

Valid examples:
- `v1`
- `v2`
- `v1_csi500_2021_2025_union`
- `v2_all_a_share`
- `v2_custom_2027`

Different `dataset_version` values must map to different physical Raw paths, for example:

```text
raw/tushare/market_price/daily/v1_csi500_2021_2025_union/year=2024/data.parquet
raw/tushare/market_price/daily/v2_all_a_share/year=2024/data.parquet
```

### `symbols_file`

Path to the external canonical Universe file. For Tushare dry-run, the file should contain canonical six-digit symbols such as:

```text
000008
600000
```

The dry-run manifest records the original file path and SHA-256. Do not generate a provider-specific baseline Universe file just to satisfy Tushare `ts_code` format.

### `universe_name`

Operator-supplied name for the Universe lineage. It should identify the external Universe used by the run, but it does not replace `dataset_version`.

### `expected_symbol_count`

Operator-supplied row-count guard for the Universe file. If the loaded canonical symbols do not match this count, the Tushare dry-run fails loudly.

### `start_date` / `end_date`

Date window in `YYYYMMDD` format. Tushare dry-run validates format and requires `start_date <= end_date`.

### `output_root`

Local root for staging or dry-run output context. AkShare acquisition writes under this root. Tushare dry-run does not write formal Raw parquet in M0 but reports intended local staging root.

### `drive_dwh_root`

Drive DWH root used by compact prepare/promote/audit. It is reviewed during `prepare`, and `promote` refuses a different Drive root after prepare.

### `promotion_name`

Safe slug identifying a compact package/promotion. Must be human-reviewable and path-safe. The same exact value is required by `--confirm-promotion` during promotion.

### `confirm_promotion`

Human confirmation gate for promotion. It must exactly match the package `promotion_name`. A mismatch stops the write before Drive Raw mutation.

### `api_inflight_limits`

AkShare raw ingest throttling/control string for limiting per-API concurrency where supported. Use it when specific AkShare APIs need lower inflight concurrency. Check CLI `--help` and current AkShare runner behavior before large runs.

## Write boundaries

| Command | Writes local staging | Writes compact package | Reads Drive | Writes Drive Raw | Writes catalog | Reversible? |
| --- | --- | --- | --- | --- | --- | --- |
| `run_akshare_raw_ingest` | Yes | No | No | No | Local catalogs/review artifacts only | Local files can be deleted manually; API side effects are external reads only |
| `run_akshare_raw_lake_preheat` | Yes when executing lanes | No | No | No | Local preheat/review artifacts only | Local files can be deleted manually |
| `run_akshare_backfill_tasks --dry-run` | No | No | No | No | Local task-result metadata may be written | Yes, local metadata can be removed |
| `run_akshare_backfill_tasks --execute` | Yes | No | No | No | Local task-result metadata | Local files can be deleted manually; external API reads are not undone |
| `run_tushare_raw_ingest --dry-run` | No formal Raw write | No | No | No | No Drive/catalog writes | Yes; no formal Raw/Drive mutation |
| `raw_lake_compact_cli prepare` | No new staging writes | Yes | Yes, for collision planning | No | Local readiness/catalog artifacts | Yes, delete local compact package if not promoted |
| `raw_lake_compact_cli promote` | No | No | Yes | **Yes** | Yes | Not automatically reversible; requires manual governance |
| `raw_lake_compact_cli audit` | No | No | Yes | No | No | Read-only |

Key boundaries:

- `prepare` does **not** write Drive Raw.
- `promote` is the only active command that writes Drive Raw.
- `audit` is read-only.
- Tushare dry-run does not pull formal API history, does not write Drive, and does not save tokens.

## Legacy / deprecated CLI

Do not actively use these for new operations. They remain for compatibility or historical reference.

| Legacy CLI | Replacement / status |
| --- | --- |
| `qsys.utils.run_factor_lake_raw_ingest` | Deprecated forwarding shell; use `qsys.utils.run_akshare_raw_ingest`. |
| `qsys.utils.run_raw_lake_preheat` | Deprecated forwarding shell; use `qsys.utils.run_akshare_raw_lake_preheat`. |
| `qsys.utils.run_p0_raw_acquisition_wave` | Deprecated compatibility module; use `qsys.utils.run_akshare_p0_raw_acquisition_wave`. |
| `qsys.utils.run_factor_lake_backfill_tasks` | Deprecated forwarding shell; use `qsys.utils.run_akshare_backfill_tasks`. |
| `qsys.utils.run_raw_acquisition_pipeline` | Legacy reference superseded by current DWH3.0 Raw Lake workflow. Do not extend for Tushare. |
| `qsys.utils.build_raw_warehouse` | Legacy warehouse reference. Not a recommended public entrypoint and not connected to Tushare. |

## Common errors and stop conditions

| Error / stop condition | Meaning | Continue? | Correct handling |
| --- | --- | --- | --- |
| Missing `--dataset-version` | Tushare dry-run or non-AkShare compact prepare lacks operator-defined dataset namespace. | No | Choose an explicit path-safe dataset namespace and rerun. Do not rely on a default. |
| `expected_symbol_count mismatch` | Loaded canonical Universe row count differs from operator expectation. | No | Inspect the `symbols_file`, header handling, blank lines, and expected count. Rerun only after confirming lineage. |
| Illegal `dataset_version` path segment | Dataset namespace contains empty value, path separator, traversal, absolute path, or unsafe characters. | No | Use a conservative slug such as `v1_csi500_2021_2025_union` or `v2_all_a_share`. |
| Drive root changed after prepare | Promotion Drive root differs from the reviewed prepare root. | No | Rerun `prepare` for the intended Drive root and review the new artifacts. |
| Collision plan SHA mismatch | Reviewed `drive_collision_plan.csv` changed after prepare. | No | Rerun `prepare`, review the new collision plan, then promote only if accepted. |
| Non-identical overwrite blocked | A Drive target exists with different bytes. | No | Investigate the existing Drive asset and new package. Do not force overwrite. |
| `READY_FOR_PROMOTION.json` missing | Package has not completed prepare/readiness review. | No | Run `prepare` successfully and review generated readiness artifacts. |
| `TUSHARE_TOKEN` missing | Tushare dry-run cannot validate token availability. | No | Set `TUSHARE_TOKEN` in the environment or provide it through the supported secure prompt path. Do not write tokens to files/logs. |
| Tushare command without `--dry-run` | M0 Tushare entrypoint intentionally disables formal pulls. | No | Add `--dry-run`. Real Tushare acquisition requires future implementation and review. |

## Pre-flight checklist

Before acquisition or promotion:

1. Confirm you are using an active provider-explicit CLI.
2. Confirm date windows are intentional.
3. Confirm `provider` and `dataset_version` semantics.
4. Confirm `symbols_file`, `universe_name`, and `expected_symbol_count` for Tushare dry-run.
5. Confirm Drive is not written except by reviewed `promote`.
6. Confirm no token appears in command output, manifest, metadata, notebooks, or logs.
7. Run `--help` on the CLI if unsure about current options.

### M1-A: Tushare local-only Raw acquisition

M1-A formalizes the verified Tushare smoke contracts as local-only Raw acquisition:

```text
Tushare API -> local staging -> metadata / manifest / QA artifacts
```

It still does **not** write Google Drive, does **not** promote, and does **not** build normalized/factor/backtest outputs. M0 remains dry-run/plan-only validation; M1-A allows true local acquisition when `--dry-run` is omitted.

Dry-run template:

```bash
PYTHONPATH=src python -m qsys.utils.run_tushare_raw_ingest \
  --dry-run \
  --symbols-file stock_universe_v1_symbols.txt \
  --universe-name stock_universe_v1 \
  --dataset-version v1_csi500_2021_2025_union \
  --start-date 20260612 \
  --end-date 20260612 \
  --api-names daily,daily_basic \
  --output-root outputs/tushare_raw_m1a \
  --max-workers 1 \
  --request-sleep 0.3
```

Local acquisition template:

```bash
PYTHONPATH=src python -m qsys.utils.run_tushare_raw_ingest \
  --symbols-file stock_universe_v1_symbols.txt \
  --universe-name stock_universe_v1 \
  --dataset-version v1_csi500_2021_2025_union \
  --start-date 20260612 \
  --end-date 20260612 \
  --api-names daily,daily_basic,moneyflow,margin_detail \
  --output-root outputs/tushare_raw_m1a \
  --max-workers 1 \
  --request-sleep 0.3 \
  --request-jitter 0.0 \
  --retry 2 \
  --resume
```

Selection is parameter/contract driven:

- `--api-names` selects specific registered APIs such as `daily,daily_basic,moneyflow,margin_detail`.
- `--families` selects registry families such as `market_price,market_basic,market_flow,margin_leverage`.
- When both `--api-names` and `--families` are provided, the actual execution set is their intersection. Manifest field `api_names` records the actual selected APIs; `requested_api_names` / `requested_families` record operator input when present.
- `--symbols-file` must be the external canonical six-digit Universe file; no provider-specific Universe file is generated.
- `--dataset-version` is required and is not defaulted to a specific V1 namespace.

M1-A local staging layout:

```text
<output-root>/data/raw/tushare/<family>/<api>/trade_date=YYYYMMDD/data.parquet
<output-root>/data/raw/tushare/<family>/<api>/trade_date=YYYYMMDD/metadata.json
```

Token-free review artifacts are written under:

```text
<output-root>/artifacts/tushare_raw_acquisition/
```

Expected artifact files:

```text
tushare_acquisition_manifest.json
raw_ingest_catalog.csv
source_coverage_summary.csv
field_presence_summary.csv
duplicate_key_summary.csv
universe_filter_summary.csv
operation_events.jsonl
```

# Project CLI Manual

This manual is the project-level operator entrypoint for currently active command-line workflows. Human operators, Codex runs, and Colab notebooks should consult this file before composing commands.

It is not a historical report and it is not a replacement for README. It documents the current executable CLI surface and safety boundaries.

## Safety rules

- Do not run production promotion commands without human review of the prepared package.
- Do not write Google Drive except through the explicit `raw_lake_compact_cli promote` command.
- Treat Tushare dry-run as planning/validation only; real Tushare local acquisition requires omitting `--dry-run` and still writes only local staging, never Google Drive.
- Do not create provider-specific baseline Universe files. Canonical Universe files use six-digit symbols; Tushare `ts_code` is a provider-specific API representation.
- Keep AkShare legacy compact layout unchanged unless an operator explicitly chooses a non-AkShare provider/dataset version.

## Active CLI overview

| CLI | Purpose | Provider | Writes local staging | Writes Drive | Calls external API | Dry-run support | Status | Main inputs | Main outputs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `qsys.utils.run_akshare_raw_ingest` | AkShare Raw Lake acquisition for selected families/APIs. | `akshare` | Yes, under `output_root/data/raw/akshare/...` | No | Yes, AkShare | No formal dry-run; use narrow date/family scopes for smoke runs | Active | `output-root`, date window, families, symbols/universe inputs, worker/retry options | Local Raw parquet, metadata, ingest catalog/review artifacts |
| `qsys.utils.run_akshare_raw_lake_preheat` | Build/execute AkShare preheat lanes for Raw Lake coverage. | `akshare` | Yes, through AkShare ingest runner | No | Yes, AkShare | Planning-oriented modes/options may be available; verify `--help` before use | Active | `output-root`, date window, universe discovery inputs, lane/worker controls | Preheat plans, lane manifests, local Raw staging where executed |
| `qsys.utils.run_akshare_backfill_tasks` | Execute or dry-run AkShare backfill task CSV/registry plans with AkShare adapters explicitly supplied. | `akshare` | Yes when `--execute` | No | Yes when `--execute` | Yes; default is dry-run unless `--execute` is supplied | Active | `output-root`, `metastore-path`, filters, `max-tasks`, `--execute` | Backfill task result rows, local Raw partitions for executed tasks |
| `qsys.utils.run_tushare_raw_ingest` | Validate or run approved Tushare raw acquisition into local staging. | `tushare` | Yes when not `--dry-run`, under `output_root/data/raw/tushare/...` | No | Yes when not `--dry-run` | Yes | Active | `symbols-file`, `universe-name`, `dataset-version`, expected count, date window, `api-names`, `output-root`, `TUSHARE_TOKEN` | Local Raw parquet/metadata plus token-free manifest and QA artifacts |
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

### Tushare local-only flow

Tushare supports a dry-run planning pass and a real local staging pass for approved registry sources. The ingest command validates token presence, canonical Universe lineage, `dataset_version`, expected symbol count, and date inputs; when run without `--dry-run` it calls Tushare and writes local raw parquet/metadata only. It does **not** write Drive, promote data, create provider-specific universe files, or save tokens.

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

### Tushare dry-run planning

Use dry-run planning to validate local inputs, approved registry selection, date windows, and token-free artifact paths without calling Tushare data APIs or writing Raw parquet.

```bash
PYTHONPATH=src python -m qsys.utils.run_tushare_raw_ingest \
  --dry-run \
  --symbols-file stock_universe_v1_symbols.txt \
  --universe-name stock_universe_v1 \
  --dataset-version v1_csi500_2021_2025_union \
  --expected-symbol-count 846 \
  --start-date 20220101 \
  --end-date 20260612 \
  --api-names daily_basic,stk_limit,limit_list_d,suspend_d,trade_cal,stock_basic,namechange \
  --output-root outputs/tushare_c1_p0_v1_20220101_20260612
```

Dry-run output remains token-free and local. It does not write Google Drive and does not promote compact packages.

### Tushare real local acquisition

Run without `--dry-run` only after the dry-run plan is reviewed. The ingest command calls Tushare and writes local staging under `output_root/data/raw/tushare/...`; Drive writes remain manual-gated through `raw_lake_compact_cli promote`.

```bash
PYTHONPATH=src python -u -m qsys.utils.run_tushare_raw_ingest \
  --symbols-file stock_universe_v1_symbols.txt \
  --universe-name stock_universe_v1 \
  --dataset-version v1_csi500_2021_2025_union \
  --expected-symbol-count 846 \
  --start-date 20220101 \
  --end-date 20260612 \
  --api-names daily_basic,stk_limit,limit_list_d,suspend_d,trade_cal,stock_basic,namechange \
  --output-root outputs/tushare_c1_p0_v1_20220101_20260612 \
  --max-workers 2 \
  --request-sleep 0.35 \
  --request-jitter 0.15 \
  --retry 3 \
  --heartbeat-sec 30 \
  --resume
```

Notes:
- Requires `TUSHARE_TOKEN` in the environment for real acquisition, but the runner must not print or persist it.
- Canonical Universe files use six-digit symbols such as `000008`; Tushare `ts_code` such as `000008.SZ` is a provider-specific API representation.
- `dataset_version` is an operator-defined dataset namespace. It is not a code default and not a schema version.
- Snapshot APIs default `snapshot=<end_date>` unless `--snapshot-date YYYYMMDD` is supplied.

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

Local root for staging or dry-run output context. AkShare and real Tushare acquisition write local Raw parquet under this root; Tushare dry-run writes only planning/review artifacts.

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
| `run_tushare_raw_ingest --dry-run` | No Raw parquet write | No | No | No | Local planning/review artifacts only | Yes; no Raw/Drive mutation |
| `raw_lake_compact_cli prepare` | No new staging writes | Yes | Yes, for collision planning | No | Local readiness/catalog artifacts | Yes, delete local compact package if not promoted |
| `raw_lake_compact_cli promote` | No | No | Yes | **Yes** | Yes | Not automatically reversible; requires manual governance |
| `raw_lake_compact_cli audit` | No | No | Yes | No | No | Read-only |

Key boundaries:

- `prepare` does **not** write Drive Raw.
- `promote` is the only active command that writes Drive Raw.
- `audit` is read-only.
- Tushare ingest writes local staging only, does not write Drive, and does not save tokens.

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
| `TUSHARE_TOKEN` missing | Real Tushare acquisition cannot authenticate. | No | Set `TUSHARE_TOKEN` in the environment or provide it through the supported secure prompt path. Do not write tokens to files/logs. |

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

It still does **not** write Google Drive, does **not** promote, and does **not** build normalized/factor/backtest outputs. Use an independent local `output_root` for each smoke run, for example `/content/outputs/tushare_raw_m1a_20260612_smoke`, so review is not polluted by older partitions.

#### Simplified Colab console

**1) Parameters cell**

```python
from pathlib import Path

SYMBOLS_FILE = "stock_universe_v1_symbols.txt"
UNIVERSE_NAME = "stock_universe_v1"
DATASET_VERSION = "v1_csi500_2021_2025_union"
START_DATE = "20260612"
END_DATE = "20260612"
API_NAMES = "daily_basic,stk_limit,limit_list_d,suspend_d,trade_cal,stock_basic,namechange"
WORK_NAME = "tushare_c1_p0_v1_20220101_20260612"
ALLOW_DRIVE_PROMOTION = False
OUTPUT_ROOT = f"/content/outputs/{WORK_NAME}"
```

**2) Dry-run cell**

```bash
PYTHONPATH=src python -m qsys.utils.run_tushare_raw_ingest \
  --dry-run \
  --symbols-file "$SYMBOLS_FILE" \
  --universe-name "$UNIVERSE_NAME" \
  --dataset-version "$DATASET_VERSION" \
  --start-date "$START_DATE" \
  --end-date "$END_DATE" \
  --api-names "$API_NAMES" \
  --output-root "$OUTPUT_ROOT" \
  --max-workers 1 \
  --request-sleep 0.3
```

The CLI prints a compact summary by default. Add `--print-manifest` only when you intentionally need the full manifest JSON.

**3) Run cell**

```bash
PYTHONPATH=src python -m qsys.utils.run_tushare_raw_ingest \
  --symbols-file "$SYMBOLS_FILE" \
  --universe-name "$UNIVERSE_NAME" \
  --dataset-version "$DATASET_VERSION" \
  --start-date "$START_DATE" \
  --end-date "$END_DATE" \
  --api-names "$API_NAMES" \
  --output-root "$OUTPUT_ROOT" \
  --max-workers 1 \
  --request-sleep 0.3 \
  --request-jitter 0.0 \
  --retry 2 \
  --heartbeat-sec 30 \
  --resume
```

During acquisition the runner writes one-line `[heartbeat]` updates and refreshes `<output-root>/artifacts/tushare_raw_acquisition/live_progress.json`. The output root must remain local-only and must not point to Google Drive.

Progress and review have three separate operator meanings:

- CLI heartbeat is a machine/log-layer progress signal for terminal logs.
- Colab progress monitor is a human-facing, single-screen progress bar backed by `live_progress.json`.
- Operator summary is the post-run rough review backed by `operator_summary.json` and `operator_summary_by_api.csv`.

The default Colab operation sequence is parameters, dry-run, run, progress monitor, then summary review.

**4) Progress monitor cell (optional but recommended)**

Run this in a separate Colab cell while the acquisition cell is active. It refreshes one output area in place, reads only `live_progress.json`, and does not print per-task logs or inspect detailed artifacts.

```python
import json
import time
from pathlib import Path
from IPython.display import clear_output

ART = Path(OUTPUT_ROOT) / "artifacts/tushare_raw_acquisition"
LIVE = ART / "live_progress.json"

for _ in range(720):
    clear_output(wait=True)

    if not LIVE.exists():
        print("waiting for live_progress.json ...")
        time.sleep(2)
        continue

    p = json.loads(LIVE.read_text(encoding="utf-8"))

    total = int(p.get("total_tasks") or 0)
    done = int(p.get("completed_tasks") or 0)
    pending = int(p.get("pending_or_running_tasks") or 0)

    pct = 0 if total == 0 else done / total
    bar_len = 30
    filled = int(bar_len * pct)
    bar = "█" * filled + "░" * (bar_len - filled)

    print("Tushare local acquisition progress")
    print(f"[{bar}] {done}/{total} ({pct:.1%})")
    print("pending:", pending)
    print("elapsed_sec:", p.get("elapsed_sec"))
    print("current_task:", p.get("current_task"))
    print("status_counts:", p.get("status_counts"))
    print("updated_at:", p.get("updated_at"))

    if total > 0 and done >= total:
        break

    time.sleep(2)
```

The progress monitor must stay aggregate-only: it does not read `operation_events.jsonl`, display catalog or coverage rows, loop over `planned_partitions`, or display parquet samples.

**5) Compact summary review cell**

```python
import json
import pandas as pd
from pathlib import Path
from IPython.display import display

ART = Path(OUTPUT_ROOT) / "artifacts/tushare_raw_acquisition"

summary = json.loads((ART / "operator_summary.json").read_text(encoding="utf-8"))
by_api = pd.read_csv(ART / "operator_summary_by_api.csv")

print("=== RUN SUMMARY ===")
display(pd.DataFrame([summary]))

print("=== BY API SUMMARY ===")
display(by_api)

print("=== LIVE PROGRESS ===")
live_path = ART / "live_progress.json"
if live_path.exists():
    live = json.loads(live_path.read_text(encoding="utf-8"))
    display(pd.DataFrame([{
        "total_tasks": live.get("total_tasks"),
        "completed_tasks": live.get("completed_tasks"),
        "pending_or_running_tasks": live.get("pending_or_running_tasks"),
        "status_counts": live.get("status_counts"),
        "updated_at": live.get("updated_at"),
    }]))
else:
    print("live_progress.json missing")

print("ROUGH CHECK:", summary.get("rough_check"))
```

The default summary review cell reads only fixed-size operator summaries and live progress. It does not display catalog details, coverage details, abnormal rows, parquet samples, or per-partition output. Use the CSV artifacts such as `raw_ingest_catalog.csv`, `source_coverage_summary.csv`, `duplicate_key_summary.csv`, `universe_filter_summary.csv`, `field_presence_summary.csv`, and `operation_events.jsonl` only for optional diagnostics.

Raw acquisition records what the provider returns. Empty results and null values are data facts, not cleaning decisions. Their interpretation belongs to later normalization, panel construction, or feature engineering stages. Therefore `status_empty` remains a status-count fact, but it is not included in `abnormal_counts` and does not make `rough_check` fail.

Selection is parameter/contract driven:

- `--api-names` selects specific registered APIs such as `daily,daily_basic,moneyflow,margin_detail,adj_factor` and approved C1 P0 APIs `daily_basic,stk_limit,limit_list_d,suspend_d,trade_cal,stock_basic,namechange`.
- `--families` selects registry families such as `market_price,market_basic,market_flow,margin_leverage`.
- When both `--api-names` and `--families` are provided, the actual execution set is their intersection. Manifest field `api_names` records the actual selected APIs; `requested_api_names` / `requested_families` record operator input when present.
- `--symbols-file` must be the external canonical six-digit Universe file; no provider-specific Universe file is generated.
- `--dataset-version` is required and is not defaulted to a specific V1 namespace.

M1-A local staging layout:

```text
<output-root>/data/raw/tushare/<family>/<api>/<partition-key>=<value>/data.parquet
<output-root>/data/raw/tushare/market_basic/daily_basic/trade_date=YYYYMMDD/data.parquet
<output-root>/data/raw/tushare/market_calendar/trade_cal/exchange=SSE/start_date=YYYYMMDD/end_date=YYYYMMDD/data.parquet
<output-root>/data/raw/tushare/security_master/stock_basic/snapshot=YYYYMMDD/list_status=L/data.parquet
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
live_progress.json
```

### Tushare calendar-aware raw acquisition

Tushare Raw acquisition now treats the trading calendar as a system-acquired resource. Operators normally provide only the universe, dataset version, date window, API names, and a local `output-root`; the runner fetches and caches `trade_cal` under the local artifacts tree and records calendar lineage in the manifest.

Trading-day daily APIs such as `daily`, `daily_basic`, `moneyflow`, `margin_detail`, `adj_factor`, and `stk_limit` use `calendar_mode=trading_days`, so requests are planned as `api_name × trade_date` for open trading days only. The runner also supports mixed C1 P0 shapes in the same run: date-param event APIs such as `suspend_d`, range APIs such as `trade_cal` and `namechange`, and snapshot APIs such as `stock_basic`. Universe filtering is controlled per registry source; `trade_cal` uses `universe_filter_mode=none` and is ingested without `ts_code` or `canonical_symbol`, while stock-level APIs preserve canonical six-digit filtering after full-market provider requests.

`--dates-file` is reserved for debug/override use only and is not part of the normal operator flow. `max_workers` enables bounded concurrency, but it does not guarantee linear speedups because Tushare limits, network latency, and API response time still dominate. The default remains `--max-workers 1`; start experiments with `--max-workers 2` plus conservative `--request-sleep` and `--request-jitter` values.

Example smoke run:

```bash
PYTHONPATH=src python -m qsys.utils.run_tushare_raw_ingest \
  --symbols-file stock_universe_v1_symbols.txt \
  --universe-name stock_universe_v1 \
  --dataset-version v1_csi500_2021_2025_union \
  --start-date 20260608 \
  --end-date 20260612 \
  --api-names daily_basic,stk_limit,limit_list_d,suspend_d,trade_cal,stock_basic,namechange \
  --output-root /content/outputs/tushare_raw_m1c_calendar_smoke \
  --max-workers 1 \
  --request-sleep 0.3 \
  --request-jitter 0.0 \
  --retry 2 \
  --heartbeat-sec 10 \
  --resume
```

### Tushare source registry and production gate

Tushare APIs are registered in `configs/tushare/source_registry.yaml`. The runner loads this registry by default and supports task-based acquisition for approved registry rows, including by-trade-date, by-date-param, by-date-range, and snapshot-by-param sources.

Registry rows must stay within the supported source shape:

- `query_mode: by_trade_date`, `by_date_param`, `by_date_range`, or `snapshot_by_param`
- `calendar_mode: trading_days`, `calendar_days`, `range_once`, or `snapshot`
- `universe_filter_mode: ts_code` or `none`
- `compact_bucket` appropriate to the partition lineage
- `partition_key` or `partition_keys`
- `primary_key` and `fields` as explicit ordered lists
- `status` describing review state, for example `approved` or `candidate`
- `production_enabled` as the explicit production gate

`approved` sources with `production_enabled: true` are eligible for normal operator runs. A `candidate` source, or any registry source with `production_enabled: false`, is blocked by default even if the operator names it in `--api-names`. To run such a source for explicit review, the operator must add:

```bash
PYTHONPATH=src python -m qsys.utils.run_tushare_raw_ingest \
  --start-date 20260612 \
  --end-date 20260612 \
  --api-names candidate_api \
  --symbols-file symbols.csv \
  --universe-name external_universe \
  --dataset-version v1_candidate_review \
  --output-root outputs/tushare_raw_candidate \
  --allow-candidate-sources
```

To add a reviewed API, add one registry entry with the supported schema, keep `fields` in the exact provider request order expected for raw acquisition, set `status: candidate` and `production_enabled: false` while validating, then switch to `status: approved` and `production_enabled: true` only after review. Unsupported modes still fail fast in registry validation.

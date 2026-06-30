# DWH4 Dual-Entry Single-Core Workflow

Status: C0-C13 scaffold plus DWH4.1 I10 implemented.

This document defines the shared operating contract for the DWH4 Tushare
workflow. It does not replace the notebook console and does not introduce a
second Tushare downloader.

## Mission

Build one repeatable workflow with two entry routes:

- Route A: DWH4 notebook console for human-operated visual/manual work.
- Route B: PowerShell plus Codex/CLI route driven by a task JSON.
- Single core: both routes reuse the task schema, Tushare registry,
  `qsys.utils.run_tushare_raw_ingest`, and
  `qsys.utils.raw_lake_compact_cli`.

## Baseline Production API Set

The C1 P0 production-style API set is:

```text
daily_basic
stk_limit
suspend_d
trade_cal
stock_basic
namechange
```

`limit_list_d` is registered but not production-enabled. It is blocked unless a
task explicitly enables candidate sources.

## Stage Order

Legacy run-to-prepare remains available for non-incremental task sheets:

```text
validate
ingest
review-ingest
prepare
review-compact
review-promotion
```

DWH4.1 drive-aware incremental task sheets use the v2 run-to-prepare stage
plan:

```text
validate
drive-inventory
incremental-plan
ingest
review-ingest
incremental-merge
prepare
review-compact
review-promotion
final-review
```

`run-to-prepare` must stop after promotion readiness review and final review
artifact generation. It must not call `promote`. The only external commands in
run-to-prepare remain `ingest` and `prepare`.

## Token Policy

The user supplies `TUSHARE_TOKEN` in the runtime environment. Code may record
only whether the token is present.

Forbidden:

```text
writing token values to JSON
writing token values to logs
printing token values
saving token values in notebooks
saving token values in tests
adding token values to task files
```

## Task JSON

The task JSON is the shared flight plan. It authorizes:

```text
execution_repo
ops_workspace
provider
symbols_file
universe_name
expected_symbol_count
dataset_version
date range
api_names
candidate-source policy
output_root
drive_dwh_root
promotion_name
execution settings
review policy
promotion policy
human intervention policy
```

Template:

```text
configs/tushare/workflows/dwh4_tushare_c1_p0_template.json
```

## DWH4.1 Drive-Aware Incremental Contract

DWH4.1 extends the existing scaffold. It must not fork the task schema,
Tushare registry, raw ingest runner, or compact CLI.

The DWH4.1 design goal is:

```text
Drive inventory
incremental planning
open-year verified replacement
stable latest buckets
active manifest
no-delete guard
decision-first final promotion review
```

The core safety rule is:

```text
Delete is forbidden by default.
Verified same-path replacement is allowed only after strict validation and final human confirmation.
```

Normal promotion must not:

```text
delete Drive files
silently overwrite Drive files
create year=2026_part2 / year=2026_v2 / year=2026_incremental paths
replace closed-year buckets by default
accept non-identical primary-key conflicts
store or print TUSHARE_TOKEN
```

DWH4.1 task JSON may include:

```text
drive_inventory_policy
incremental_policy
drive_mutation_policy
```

The current implementation supports the DWH4.1 construction pieces through
I10:

```text
task policy validation
read-only Drive inventory
incremental planning
local verified incremental merge candidates
candidate active manifest generation
promotion action vocabulary
decision-first final review
no-delete guard and delete request artifacts
run-to-prepare v2 stage plan
verified replacement execution skeleton
notebook and PowerShell UX docs
```

These pieces are still gated. Normal plan-only and run-to-prepare modes do not
execute promotion, verified replacement, Drive delete, or active manifest Drive
write.

The active manifest path is a relative path under:

```text
catalog/active/tushare/<dataset_version>/
```

Future modeling and normalization should prefer active manifest reads instead
of recursively scanning every Drive `data.parquet` once the manifest exists.

## DWH4.1 Incremental Workflow

The DWH4.1 workflow should be operated in this order:

```text
1. Validate task JSON and token presence.
2. Read existing Drive raw/tushare inventory without mutation.
3. Build incremental fetch windows.
4. Run raw ingest into local output_root.
5. Review ingest artifacts.
6. Build local candidate replacements for mutable open-year buckets.
7. Prepare compact package and promotion plan.
8. Review compact and promotion artifacts.
9. Generate decision-first final promotion review.
10. Execute promotion or verified replacement only after explicit human confirmation.
```

Closed years are frozen by default. Open-year yearly buckets are rebuilt as
local candidates, not appended as fragment paths. Stable latest range/snapshot
buckets are represented through active manifest records and verified latest
actions; superseded legacy assets are kept unless a separate delete task is
created.

## Active Manifest Read Policy

When an active manifest exists, downstream modeling and normalization should
read it first:

```text
1. Load catalog/active/tushare/<dataset_version>/dwh4_tushare_active_manifest.json.
2. Read only active_assets listed in the manifest.
3. Ignore superseded_assets_not_deleted for normal production reads.
4. Use superseded assets only for explicit audit or recovery tasks.
```

The active manifest is a routing contract, not a delete instruction. Generating
or reading it must not delete old window/snapshot files.

## Notebook Parameter Loading

The notebook route may optionally load the same task JSON used by the agent:

```python
from qsys.workflows.tushare_dwh4_notebook import load_dwh4_tushare_notebook_globals

globals().update(load_dwh4_tushare_notebook_globals(TASK_JSON))
```

This provides notebook variable names such as:

```text
REPO_ROOT
SYMBOLS_FILE
UNIVERSE_NAME
DATASET_VERSION
START_DATE
END_DATE
API_NAMES
OUTPUT_ROOT
DRIVE_DWH_ROOT
PROMOTION_NAME
PACKAGE_ROOT
DWH41_INCREMENTAL_ENABLED
DWH41_INCREMENTAL_MODE
DWH41_TARGET_END_DATE_POLICY
DWH41_ACTIVE_MANIFEST_PATH
DWH41_ALLOW_DELETE
DWH41_ALLOW_VERIFIED_REPLACE
DWH41_INCREMENTAL_POLICY
DWH41_DRIVE_POLICY
```

`API_NAMES` remains comma-separated for existing notebook CLI cells.
`API_NAME_LIST` is also available for inspection or widgets. The loader records
only `RUNTIME_TOKEN_PRESENT`; it does not read, return, print, or persist the
token value.

The DWH4.1 values are for inspection and manual notebook branching. They do not
execute Drive inventory, replacement, promotion, or delete by themselves.

## Validation Gate

The C0-C1 validator checks only task and local metadata. It does not call
Tushare, run compact, write Drive, promote, or audit.

Validation rejects:

```text
missing execution_repo/src
provider other than tushare
missing symbols_file
expected_symbol_count mismatch
invalid date format or reversed date range
unsafe dataset_version
Drive-like output_root
empty api_names
unknown registry APIs
non-production registry APIs without explicit candidate opt-in
secret-like task key names except the explicit non-secret policy allowlist
```

The explicit non-secret allowlist is:

```text
human_intervention_policy.only_token_and_final_promotion
```

## Review Gates

Later slices should implement review gates from fixed-size artifacts:

```text
review-ingest:
  operator_summary.json
  operator_summary_by_api.csv
  tushare_acquisition_manifest.json

review-compact:
  compact_manifest.json
  compact_qa_report.csv
  raw_asset_inventory.csv
  compact_source_lineage.csv
  _LOCAL_COMPACT_READY.txt

review-promotion:
  READY_FOR_PROMOTION.json
  drive_collision_plan.csv
  compact_manifest.json
  compact_qa_report.csv
```

If a review fails, the workflow stops and writes a report. It does not ask to
continue anyway.

## PowerShell Run Pattern

Run from the repository root with `PYTHONPATH` pointed at `src`. A plan-only
agent run writes local run artifacts and does not execute subprocesses:

```powershell
$env:PYTHONPATH = "$PWD\src"
python -m qsys.utils.run_tushare_dwh4_agent `
  --task configs\tushare\workflows\dwh4_tushare_c1_p0_template.json `
  --stage run-to-prepare `
  --run-id run_001
```

To record a design-only plan without `TUSHARE_TOKEN`:

```powershell
python -m qsys.utils.run_tushare_dwh4_agent `
  --task configs\tushare\workflows\dwh4_tushare_c1_p0_template.json `
  --stage run-to-prepare `
  --run-id run_001 `
  --allow-missing-token-for-plan
```

Explicit run-to-prepare execution requires the exact run id. It may run ingest
and prepare, but it still must not promote:

```powershell
$env:TUSHARE_TOKEN = "<set outside task JSON>"
python -m qsys.utils.run_tushare_dwh4_agent `
  --task configs\tushare\workflows\dwh4_tushare_c1_p0_template.json `
  --stage run-to-prepare `
  --run-id run_001 `
  --execute-run-to-prepare `
  --confirm-execute-run-to-prepare run_001
```

Promotion is separate and requires exact `promotion_name`:

```powershell
python -m qsys.utils.run_tushare_dwh4_agent `
  --task configs\tushare\workflows\dwh4_tushare_c1_p0_template.json `
  --stage run-to-prepare `
  --run-id run_001 `
  --execute-promotion `
  --confirm-promotion <promotion_name>
```

Read-only audit is also separate:

```powershell
python -m qsys.utils.run_tushare_dwh4_agent `
  --task configs\tushare\workflows\dwh4_tushare_c1_p0_template.json `
  --stage run-to-prepare `
  --run-id run_001 `
  --execute-audit
```

Verified replacement is implemented as an explicit executor primitive for a
future controlled route. It is not part of default plan-only or
run-to-prepare execution.

## Promotion Gate

Promotion requires exact human confirmation of `promotion_name`.

When reviewed bucket kinds such as `snapshot` are required, the task must
explicitly authorize them before the promote command may include
`--allow-reviewed-bucket-kinds`.

## Safety Summary

```text
Plan-only executes subprocesses: no
Run-to-prepare external commands: ingest, prepare
Run-to-prepare promotes: no
Promotion requires exact promotion_name: yes
Verified replacement requires exact promotion_name: yes
Drive delete allowed by default: no
Delete request artifacts execute delete: no
Automatic rollback after failed replacement: no
Token values written to artifacts: no
```

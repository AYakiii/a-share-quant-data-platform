# DWH4 Tushare Agent Plan State

Status: DWH4.1 I10 implemented.

## Current Goal

Complete the DWH4.1 docs and notebook / PowerShell UX slice.
No Tushare fetch, real Drive replacement, promotion, active manifest Drive write, Drive delete, or Drive mutation is executed by this implementation/test pass.

## Files Added

```text
docs/operator/dwh4_dual_entry_single_core_workflow.md
docs/operator/dwh4_tushare_agent_plan_state.md
src/qsys/workflows/__init__.py
src/qsys/workflows/tushare_dwh4_task.py
src/qsys/workflows/tushare_dwh4_commands.py
src/qsys/workflows/tushare_dwh4_reviews.py
src/qsys/workflows/tushare_dwh4_orchestrator.py
src/qsys/workflows/tushare_dwh4_artifacts.py
src/qsys/workflows/tushare_dwh4_executor.py
src/qsys/workflows/tushare_dwh4_notebook.py
src/qsys/workflows/tushare_dwh4_incremental_plan.py
src/qsys/workflows/tushare_dwh4_incremental_merge.py
src/qsys/workflows/tushare_dwh4_active_manifest.py
src/qsys/workflows/tushare_dwh4_drive_guard.py
src/qsys/utils/run_tushare_dwh4_agent.py
tests/workflows/test_tushare_dwh4_task.py
tests/workflows/test_tushare_dwh4_commands.py
tests/workflows/test_tushare_dwh4_reviews.py
tests/workflows/test_tushare_dwh4_orchestrator.py
tests/workflows/test_tushare_dwh4_artifacts.py
tests/workflows/test_tushare_dwh4_executor.py
tests/workflows/test_tushare_dwh4_notebook.py
tests/workflows/test_tushare_dwh4_incremental_plan.py
tests/workflows/test_tushare_dwh4_incremental_merge.py
tests/workflows/test_tushare_dwh4_active_manifest.py
tests/workflows/test_tushare_dwh4_drive_guard.py
tests/utils/test_run_tushare_dwh4_agent.py
configs/tushare/workflows/dwh4_tushare_c1_p0_template.json
```

## Implemented In This Slice

```text
task JSON loader
task dataclasses
secret-like key scan with one explicit non-secret allowlist
task validation issue model
execution_repo/src checks
provider check
symbols_file and expected count checks
date checks
dataset_version path-safety check
output_root Drive-like guard
registry API existence check
production_enabled gate
optional runtime token presence warning
ingest command builder
prepare command builder
human-gated promote command builder
read-only audit command builder
run-to-prepare external command sequence that excludes promote
run artifact path helpers
token-free command execution record builder
review-ingest artifact parser
review-compact artifact parser
review-promotion artifact parser
stale READY reviewed-bucket guard using compact_manifest bucket kinds
run-to-prepare stage sequence skeleton
validation/token blocking before command planning
workflow_state payload builder
planned command payload builder
gate_decisions payload builder
plan-only artifact writer
planned_commands.json artifact path
plan-only CLI wrapper for qsys.utils.run_tushare_dwh4_agent
controlled command runner skeleton with injected runner only
run-to-prepare executable stage allowlist for ingest and prepare
token-free commands_executed.jsonl append helper
fake-runner executor pytest coverage
subprocess runner adapter behind explicit CLI execution path
--execute-run-to-prepare guarded by exact run-id confirmation
execution artifact writer for workflow_state, commands_executed, gate_decisions, and agent_report
CLI execution tests using monkeypatched runner only
mandatory review-ingest gate between ingest and prepare
prepare blocked when ingest artifacts are missing or fail review policy
workflow_state blocked_stage / blocked_reason recording
CLI execution tests for gate-pass and gate-fail paths
post-prepare review-compact gate
post-prepare review-promotion readiness gate
--package-root honored by explicit execution reviews
CLI execution tests for compact gate failure
final_promotion_review.md writer
token-free promote command preview in workflow_state
final human confirmation text with exact promotion name
promotion preview generated only after all run-to-prepare gates pass
separate --execute-promotion mode
exact --confirm-promotion gate
promotion_execution_state.json and promotion_execution_report.md
promotion execution tests using monkeypatched runner only
separate --execute-audit mode
audit_execution_state.json and audit_execution_report.md
audit execution tests using monkeypatched runner only
notebook-facing task JSON parameter adapter
load_dwh4_tushare_notebook_globals helper
token-free notebook parameter summary
DWH4.1 drive_inventory_policy task contract
DWH4.1 incremental_policy task contract
DWH4.1 drive_mutation_policy task contract
DWH4.1 active manifest path validation
DWH4.1 no-delete policy validation
focused pytest coverage
DWH4.1 decision-first final_promotion_review.md
DWH4.1 final promotion decision field in workflow_state.json
DWH4.1 delete-request final decision downgrade hook
DWH4.1 blocked collision final decision downgrade hook
table-heavy final review placeholders for I2-I9 summaries
DWH4.1 read-only Drive raw/tushare inventory reader
DWH4.1 Drive inventory path parser for raw/tushare assets
DWH4.1 parquet rows/columns/date-range extraction
DWH4.1 optional sha256 and sidecar metadata extraction
DWH4.1 drive_inventory.csv writer
DWH4.1 drive_inventory_summary.json writer
DWH4.1 read-only incremental planner from Drive inventory
DWH4.1 by-trade-date open-year fetch window computation
DWH4.1 data-lag target end-date computation
DWH4.1 overlap_trading_days start-date planning
DWH4.1 open-year overlap clipping to YYYY0101
DWH4.1 no-Drive-data fallback to task start_date
DWH4.1 closed-year freeze planning without closed-year replacement
DWH4.1 stable latest range API planning to window=latest
DWH4.1 stable latest snapshot API planning to snapshot=latest
DWH4.1 incremental_plan.csv writer
DWH4.1 incremental_plan_summary.json writer
DWH4.1 verified local incremental merge for open-year yearly buckets
DWH4.1 old Drive row preservation in local candidate output
DWH4.1 new incremental row append in local candidate output
DWH4.1 identical overlap collapse
DWH4.1 non-identical primary-key conflict blocking
DWH4.1 schema mismatch blocking
DWH4.1 duplicate candidate key blocking
DWH4.1 local candidate parquet writer under candidate_root
DWH4.1 incremental_merge_report.csv writer
DWH4.1 incremental_merge_summary.json writer
DWH4.1 candidate_active_manifest.json writer
DWH4.1 candidate active manifest generator
DWH4.1 active_assets generation from existing Drive inventory
DWH4.1 active_assets overlay from verified incremental merge candidates
DWH4.1 window=latest active path generation
DWH4.1 snapshot=latest active path generation
DWH4.1 superseded legacy window/snapshot keep records
DWH4.1 active manifest safe path validation
DWH4.1 stable_latest_report.csv writer
DWH4.1 active_manifest_summary.json writer
DWH4.1 promotion action vocabulary support
DWH4.1 promotion_action_counts metadata
DWH4.1 promotion_actions_present metadata
DWH4.1 replace_verified_incremental promotion action
DWH4.1 replace_verified_latest promotion action
DWH4.1 active_manifest_update promotion action
DWH4.1 superseded_legacy_keep promotion action
DWH4.1 delete_request_only promotion action and executor block
DWH4.1 final review promotion action summary table
DWH4.1 no-delete guard for Path.unlink/os.remove/os.unlink/os.rmdir/shutil.rmtree/Path.rename/Path.replace under drive_dwh_root
DWH4.1 DRIVE_DELETE_REQUEST.md writer
DWH4.1 drive_delete_plan.csv writer
DWH4.1 drive_delete_summary.json writer
DWH4.1 run-to-prepare v2 stage sequence for incremental tasks
DWH4.1 drive-inventory stage marker
DWH4.1 incremental-plan stage marker
DWH4.1 incremental-merge stage marker
DWH4.1 final-review stage marker
DWH4.1 run_to_prepare_v2 workflow_state flag
DWH4.1 drive_delete_executed workflow_state flag
DWH4.1 I7/I8 artifact path registration under run artifacts
DWH4.1 verified replacement execution skeleton
DWH4.1 pre-replacement local backup under run artifacts
DWH4.1 backup metadata JSON writer
DWH4.1 candidate rows/columns/sha precheck before Drive write
DWH4.1 post-write rows/columns/sha verification
DWH4.1 CRITICAL_RESTORE_REQUIRED.md writer on post-write verification failure
DWH4.1 no automatic rollback policy in replacement executor
DWH4.1 incremental workflow documented
DWH4.1 active manifest read policy documented
DWH4.1 PowerShell plan-only / run-to-prepare / promotion / audit patterns documented
DWH4.1 notebook adapter exposes incremental policy fields
DWH4.1 notebook adapter exposes Drive mutation policy fields
DWH4.1 notebook policy summaries remain token-free
```

## Not Executed Yet

```text
real Tushare API fetch
real Drive inventory against production Drive
real incremental merge against production data
real active manifest Drive write
real verified replacement Drive write
real promotion
real Drive delete
```

## Safety State

```text
Tushare API called by this implementation/test pass: no
run-to-prepare CLI execution available: explicit flag and run-id confirmation only
prepare execution requires review-ingest PASS
run-to-prepare completion requires review-compact PASS
run-to-prepare completion requires review-promotion PASS
final promotion review generated only after all gates PASS
promotion command preview executed: no
promotion execution available: explicit flag and exact promotion confirmation only
raw ingest executed: no
compact executed: no
DWH4.1 final promotion review is decision-first: yes
DWH4.1 final promotion review still executes Drive mutation: no
prepare executed: no
promote executed by this implementation/test pass: no
audit execution available: explicit flag and read-only audit stage only
audit executed by this implementation/test pass: no
Drive read executed by this implementation/test pass: no
Drive write executed by this implementation/test pass: no
DWH4.1 Drive inventory reader implemented: yes
DWH4.1 Drive inventory integrated into run-to-prepare: no
DWH4.1 Drive inventory tests used mock local Drive tree only: yes
DWH4.1 incremental planner implemented: yes
DWH4.1 incremental planner integrated into run-to-prepare: no
DWH4.1 incremental planner tests used mock inventory only: yes
DWH4.1 verified incremental merge implemented: yes
DWH4.1 verified incremental merge integrated into run-to-prepare: no
DWH4.1 verified incremental merge tests used mock parquet only: yes
DWH4.1 local candidate parquet writer implemented: yes
DWH4.1 active manifest generator implemented: yes
DWH4.1 active manifest integrated into run-to-prepare artifact tracking: yes
DWH4.1 active manifest tests used mock inventory and candidate metadata only: yes
DWH4.1 stable latest active paths implemented: yes
DWH4.1 superseded legacy assets kept and recorded: yes
DWH4.1 promotion action vocabulary implemented: yes
DWH4.1 delete_request_only blocks explicit promotion execution: yes
DWH4.1 no-delete guard implemented: yes
DWH4.1 no-delete guard tests used mock local Drive tree only: yes
DWH4.1 delete request artifacts implemented: yes
DWH4.1 run-to-prepare v2 stage sequence integrated: yes
DWH4.1 run-to-prepare v2 external commands still limited to ingest and prepare: yes
DWH4.1 verified replacement execution skeleton implemented: yes
DWH4.1 verified replacement requires exact promotion confirmation: yes
DWH4.1 verified replacement creates local backup before replacement: yes
DWH4.1 verified replacement post-write verification implemented: yes
DWH4.1 CRITICAL_RESTORE_REQUIRED.md implemented: yes
DWH4.1 automatic rollback implemented: no
notebook task JSON parameter loading available: yes
DWH4.1 notebook incremental policy fields exposed: yes
DWH4.1 notebook Drive policy fields exposed: yes
DWH4.1 PowerShell UX documented: yes
DWH4.1 active manifest read policy documented: yes
DWH4.1 policy fields added to task template: yes
DWH4.1 Drive inventory executed: no
DWH4.1 real incremental merge executed: no
DWH4.1 active manifest written to Drive: no
DWH4.1 verified replacement executed: no
DWH4.1 Drive delete executed: no
token value stored or printed: no
```



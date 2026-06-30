# DWH4 Tushare Dual-Entry Single-Core Engineering Report

Date: 2026-06-29

Target repository:

```text
<repo-root>
```

Construction workspace used for temporary patches:

```text
<worksite-root>
```

Current scaffold status:

```text
C0-C13 implemented
Remaining scaffold items: none for current scaffold plan
```

## 1. Executive Summary

This engineering pass added a DWH4 dual-entry, single-core Tushare workflow
scaffold to `a-share-quant-data-platform`.

The system now has:

```text
Route A: Notebook console can optionally load parameters from the shared task JSON.
Route B: PowerShell/Codex CLI can plan and run controlled workflow stages.
Single core: both routes reuse the same task sheet, registry, ingest runner, and compact CLI.
```

The implementation is intentionally a controlled scaffold. It builds task
validation, command planning, review gates, execution wrappers, reports, tests,
and notebook parameter loading. It does not run production ingest or write
Drive during this implementation pass.

## 2. Scope Boundary

Included:

```text
DWH4 Tushare task schema and validation
task JSON template
command builders for ingest, prepare, promote, and audit
read-only review gates for ingest, compact, and promotion readiness artifacts
run-to-prepare orchestrator
controlled execution wrappers using injected runners
plan-only and execution artifact writers
CLI entrypoint for plan, run-to-prepare, promotion, and audit modes
notebook parameter adapter using the same task JSON
operator documentation
pytest coverage for the new DWH4 scaffold
```

Excluded:

```text
real Tushare API execution
real raw ingest execution
real compact prepare execution
real promote execution
real Drive read/write audit execution
strategy, backtest, portfolio, NAV, Sharpe, drawdown, turnover, or TopN work
modification of <workspace-root> as a production system
```

Important project boundary:

```text
The implementation target is a-share-quant-data-platform.
The workspace project is not part of this system improvement.
```

## 3. Inputs Used

Primary work order files:

```text
<local-inputs>/dwh4_dual_entry_single_core_workflow_blueprint.md
<local-inputs>/DWH4_0.ipynb
```

The blueprint established the operating contract:

```text
do not replace the notebook
do not create a second Tushare downloader
reuse the same task sheet schema
reuse source_registry.yaml and TushareSourceSpec contracts
reuse qsys.utils.run_tushare_raw_ingest
reuse qsys.utils.raw_lake_compact_cli prepare/promote/audit
run-to-prepare must not promote
promotion requires exact final confirmation
token values must never be persisted
```

The notebook established the practical variable names and manual-console route,
including:

```text
UNIVERSE_NAME
DATASET_VERSION
START_DATE
END_DATE
API_NAMES
PROVIDER
WORK_NAME
OUTPUT_ROOT
DRIVE_DWH_ROOT
PROMOTION_NAME
COMPACT_PARENT
PACKAGE_ROOT
```

## 4. Stage Completion Directory

```text
C0  Foundation and task schema: completed
C1  Task validation and template: completed
C2  Command builders: completed
C3  Plan-only CLI and artifacts: completed
C4  Controlled command runner skeleton: completed
C5  Explicit run-to-prepare execution path: completed
C6  Mandatory review-ingest gate: completed
C7  Post-prepare review-compact and review-promotion gates: completed
C8  Package-root execution review wiring: completed
C9  Final promotion review artifact: completed
C10 Promotion command preview after all gates pass: completed
C11 Controlled promotion execution skeleton: completed
C12 Read-only audit execution skeleton: completed
C13 Notebook parameter loading from shared task JSON: completed
```

Current plan document:

```text
docs/operator/dwh4_tushare_agent_plan_state.md
```

It currently records:

```text
Status: C13 implemented.
Not Implemented Yet: none for current scaffold plan
```

## 5. Files Added

Documentation:

```text
docs/operator/dwh4_dual_entry_single_core_workflow.md
docs/operator/dwh4_tushare_agent_plan_state.md
docs/operator/dwh4_tushare_engineering_report.md
```

Workflow source:

```text
src/qsys/workflows/__init__.py
src/qsys/workflows/tushare_dwh4_task.py
src/qsys/workflows/tushare_dwh4_commands.py
src/qsys/workflows/tushare_dwh4_reviews.py
src/qsys/workflows/tushare_dwh4_orchestrator.py
src/qsys/workflows/tushare_dwh4_artifacts.py
src/qsys/workflows/tushare_dwh4_executor.py
src/qsys/workflows/tushare_dwh4_notebook.py
```

CLI:

```text
src/qsys/utils/run_tushare_dwh4_agent.py
```

Tests:

```text
tests/workflows/test_tushare_dwh4_task.py
tests/workflows/test_tushare_dwh4_commands.py
tests/workflows/test_tushare_dwh4_reviews.py
tests/workflows/test_tushare_dwh4_orchestrator.py
tests/workflows/test_tushare_dwh4_artifacts.py
tests/workflows/test_tushare_dwh4_executor.py
tests/workflows/test_tushare_dwh4_notebook.py
tests/utils/test_run_tushare_dwh4_agent.py
```

Configuration template:

```text
configs/tushare/workflows/dwh4_tushare_c1_p0_template.json
```

## 6. Core Design

### 6.1 Task Sheet

`tushare_dwh4_task.py` defines the shared task contract used by both routes.

It validates:

```text
workflow_mode
execution_repo and execution_repo/src
provider
symbols_file and expected symbol count
date format and date ordering
dataset_version path safety
promotion_name path safety
local output_root guard
api_names against source_registry.yaml
production_enabled policy
candidate-source opt-in policy
secret-like key rejection
optional runtime token presence warning
```

The task loader rejects secret-like keys except the explicit non-secret policy
allowlist:

```text
human_intervention_policy.only_token_and_final_promotion
```

### 6.2 Commands

`tushare_dwh4_commands.py` builds token-free command specs for:

```text
ingest
prepare
promote
audit
```

The run-to-prepare command sequence contains only:

```text
ingest
prepare
```

It never emits promote.

### 6.3 Review Gates

`tushare_dwh4_reviews.py` implements fixed-artifact review gates:

```text
review-ingest
review-compact
review-promotion
```

The gates inspect existing artifacts only. They do not call Tushare and do not
write Drive.

### 6.4 Orchestration

`tushare_dwh4_orchestrator.py` builds the run-to-prepare plan and keeps stage
status separate from execution. It supports plan-only reporting and explicit
execution routing.

### 6.5 Execution

`tushare_dwh4_executor.py` executes only through an injected runner.

The controlled execution paths are:

```text
run-to-prepare: ingest -> review-ingest -> prepare -> review-compact -> review-promotion
promotion: review-promotion -> exact confirm_promotion -> promote
audit: audit stage only
```

Promotion is separated from run-to-prepare and requires:

```text
--execute-promotion
--confirm-promotion exactly equal to task.promotion_name
review-promotion PASS
authorized reviewed bucket kinds
```

Audit is separated from promote and is modeled as read-only:

```text
--execute-audit
stage must be audit
Drive write recorded as false
```

### 6.6 Artifacts

`tushare_dwh4_artifacts.py` writes token-free run artifacts:

```text
workflow_state.json
planned_commands.json
gate_decisions.json
commands_executed.jsonl
dwh4_agent_report.md
final_promotion_review.md
promotion_execution_state.json
promotion_execution_report.md
audit_execution_state.json
audit_execution_report.md
```

Command records intentionally omit:

```text
environment values
stdout
stderr
token values
```

They record only:

```text
stage
cwd
argv
started_at
finished_at
return_code
token_present
```

### 6.7 CLI

`qsys.utils.run_tushare_dwh4_agent` supports:

```text
plan-only mode by default
--execute-run-to-prepare with exact run-id confirmation
--execute-promotion with exact promotion-name confirmation
--execute-audit as a read-only audit execution mode
```

The execution modes are mutually exclusive.

### 6.8 Notebook Adapter

`tushare_dwh4_notebook.py` lets the notebook route load the same task JSON:

```python
from qsys.workflows.tushare_dwh4_notebook import load_dwh4_tushare_notebook_globals

globals().update(load_dwh4_tushare_notebook_globals(TASK_JSON))
```

It exposes notebook-compatible names such as:

```text
REPO_ROOT
EXECUTION_REPO
OPS_WORKSPACE
SYMBOLS_FILE
UNIVERSE_NAME
EXPECTED_SYMBOL_COUNT
DATASET_VERSION
START_DATE
END_DATE
API_NAMES
API_NAME_LIST
OUTPUT_ROOT
DRIVE_DWH_ROOT
PROMOTION_NAME
COMPACT_PARENT
PACKAGE_ROOT
MAX_WORKERS
REQUEST_SLEEP
REQUEST_JITTER
RETRY
HEARTBEAT_SEC
RESUME
RUNTIME_TOKEN_PRESENT
```

`API_NAMES` remains comma-separated to match existing notebook CLI cells.
`API_NAME_LIST` is provided for inspection and widgets.

The adapter does not return, print, or persist token values.

## 7. Safety Controls

Implemented safety controls:

```text
TUSHARE_TOKEN values are not written to task JSON, logs, reports, or tests.
Task JSON secret-like keys are rejected.
Plan-only is the default CLI behavior.
run-to-prepare cannot promote.
run-to-prepare execution requires exact run-id confirmation.
prepare is blocked unless review-ingest passes.
run-to-prepare completion requires review-compact and review-promotion PASS.
promotion requires exact promotion_name confirmation.
promotion re-runs review-promotion before calling promote.
promotion reviewed bucket kinds must be authorized by task policy.
audit is a separate read-only stage.
Drive write is never performed by this implementation/test pass.
No raw data was modified by this implementation/test pass.
```

Safety state for this engineering pass:

```text
Tushare API called: no
raw ingest executed: no
compact prepare executed: no
promote executed: no
audit executed against Drive: no
Drive read executed: no
Drive write executed: no
token value stored or printed: no
```

## 8. Verification

Focused DWH4 test command:

```powershell
$env:PYTHONPATH = "$PWD\src"
python -m pytest -q `
  tests/workflows/test_tushare_dwh4_notebook.py `
  tests/workflows/test_tushare_dwh4_task.py `
  tests/workflows/test_tushare_dwh4_commands.py `
  tests/workflows/test_tushare_dwh4_reviews.py `
  tests/workflows/test_tushare_dwh4_orchestrator.py `
  tests/workflows/test_tushare_dwh4_artifacts.py `
  tests/workflows/test_tushare_dwh4_executor.py `
  tests/utils/test_run_tushare_dwh4_agent.py
```

Focused DWH4 result:

```text
88 passed, 1 warning in 3.05s
```

The warning is a `dateutil` Python 3.12 deprecation warning and is outside the
DWH4 scaffold logic.

Full repository test command:

```powershell
$env:PYTHONPATH = "$PWD\src"
python -m pytest -q
```

Full repository result observed during final verification:

```text
580 passed, 178 failed, 1 skipped, 48 warnings
```

The first sampled failures were in existing `tests/data/factor_lake/...`
modules, where raw/backfill tasks expected `success` but returned `failed`.
Those failures are outside the newly added DWH4 scaffold path.

## 9. Current Git State

At the time this report was written, the DWH4 scaffold files are still
untracked in the target repository. No `git add` or commit was performed.

Representative status:

```text
?? configs/tushare/workflows/
?? docs/operator/dwh4_dual_entry_single_core_workflow.md
?? docs/operator/dwh4_tushare_agent_plan_state.md
?? docs/operator/dwh4_tushare_engineering_report.md
?? src/qsys/utils/run_tushare_dwh4_agent.py
?? src/qsys/workflows/
?? tests/utils/test_run_tushare_dwh4_agent.py
?? tests/workflows/
```

Temporary patch files in the construction workspace were cleared after use.

## 10. Operational Handoff

Recommended next human/operator steps:

```text
1. Review the newly added docs and task template.
2. Review the DWH4 focused tests.
3. Decide whether to stage and commit the scaffold.
4. Before any real run, provide TUSHARE_TOKEN only through the runtime environment.
5. Use plan-only mode first on any real task JSON.
6. Execute run-to-prepare only with exact run-id confirmation.
7. Review final_promotion_review.md before any promotion.
8. Execute promotion only with exact promotion_name confirmation.
9. Treat full-repo pytest failures as a separate existing-suite cleanup stream.
```

The current scaffold is ready for code review and controlled dry operation. It
is not a record of any production data movement.

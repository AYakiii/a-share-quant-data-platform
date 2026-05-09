# Codebase Hygiene Audit (No-Delete Review)

Date: 2026-05-08  
Scope: repository-wide scan with emphasis on `src/qsys/utils/`, `scripts/`, `docs/notion_project_review/`, README consistency, and test collection hygiene.  
Constraint followed: **no file deletion and no behavior changes**.

---

## Audit Method

- Enumerated repository files and key directories.
- Cross-checked `README.md` claims against existing paths/files.
- Reviewed naming patterns and role clarity for utility scripts and docs.
- Checked test filenames for potential pytest module-name collisions.

---

## High-Level Findings

1. **Core research modules are generally clean and active** under `src/qsys/{features,signals,backtest,research,rebalance,risk,universe}`.
2. `src/qsys/utils/` currently mixes:
   - operational utilities (keep active),
   - examples/demos (likely archive candidates later),
   - phase-named experimental runners (keep but label clearly).
3. `docs/notion_project_review/` contains likely **duplication/staleness overlap**, especially draft/manual/bundle artifacts generated for review workflows.
4. Tests contain a likely **collection-name conflict risk**: duplicate `test_exposure.py` basenames in different folders.
5. `README.md` has **inconsistency signals** (describes `src/qsys/data/` in structure, but no tracked files currently present under that subtree in this snapshot).

---

## Classification by Requested Groups

## 1) Core active modules

- `src/qsys/features/*`
- `src/qsys/signals/*`
- `src/qsys/backtest/*`
- `src/qsys/research/*`
- `src/qsys/rebalance/*`
- `src/qsys/risk/*`
- `src/qsys/universe/*`

Assessment: These are coherent domain modules, well aligned with the current roadmap (Feature/Signal/Backtest priorities).

## 2) Active utility scripts

Likely operational and still active:
- `src/qsys/utils/build_real_feature_store.py`
- `src/qsys/utils/run_buffered_rebalance_from_feature_store.py`
- `src/qsys/utils/compare_rebalance_policies_from_feature_store.py`
- `src/qsys/utils/report_rebalance_policy_comparison.py`
- `src/qsys/utils/build_market_index_benchmarks.py`
- `scripts/sync_project_review_to_notion.py`

## 3) Experimental scripts

Likely experiment/demo entrypoints (keep, but tag clearly as experimental/demo):
- `src/qsys/utils/run_signal_sanity_grid.py`
- `src/qsys/utils/run_phase14b_risk_diagnostics.py`
- `src/qsys/utils/run_signal_quality_mvp.py`
- `src/qsys/utils/generate_synthetic_feature_store.py`

## 4) Deprecated or legacy files

- `run_demo.py` (README already flags as deprecated for real-data workflow)
- `A_share_Analytical_DWH.ipynb` (legacy notebook pipeline)

## 5) Documentation that appears stale or duplicated

Potential overlap cluster in `docs/notion_project_review/`:
- `PROJECT_SYSTEM_MANUAL_DRAFT.md`
- `PROJECT_SYSTEM_MANUAL_20260508.md`
- `GPT_REVIEW_BUNDLE.md`
- `deep_review_evidence_pack.md`
- `GPT_WHOLE_PROJECT_SYSTEM_MANUAL_PROMPT.md`

Observation: these appear to be parallel artifacts for similar review content (draft/manual/bundle/prompt/evidence), which may fragment source-of-truth.

## 6) Tests that may have collection/name conflicts

Potential pytest module-name collision risk:
- `tests/risk/test_exposure.py`
- `tests/research/test_exposure.py`

Both share basename `test_exposure.py`; depending on pytest import mode/environment, this can cause confusing collection/import behavior.

## 7) Files referenced by README but missing/inconsistent

- README “Repository Structure” includes `src/qsys/data/`, but in current snapshot no files are present under `src/qsys/data/`.
- README “Data Layer” mentions sqlite metadata/incremental update as foundation; only lightweight/partial evidence appears in current code organization (worth explicit verification).

## 8) Files that may be safe to archive later

Good archive candidates (after confirming no active dependency):
- demo/examples in `src/qsys/utils/*_example.py`
- `run_demo.py`
- legacy notebook `A_share_Analytical_DWH.ipynb`
- draft/review bundle artifacts in `docs/notion_project_review/` listed above

---

## Detailed File Recommendation Table

| File path | Current role | Recommendation | Reason |
|---|---|---|---|
| `src/qsys/features/` | Core feature layer | Keep | Active core module aligned to roadmap |
| `src/qsys/signals/` | Core signal layer | Keep | Active core module aligned to roadmap |
| `src/qsys/backtest/` | Core backtest layer | Keep | Active core module aligned to roadmap |
| `src/qsys/research/` | Diagnostics/research analytics | Keep | Active analytical support for signal validation |
| `src/qsys/rebalance/` | Rebalance policies + diagnostics | Keep | Active strategy execution research layer |
| `src/qsys/risk/` | Risk exposure helpers | Keep | Active risk-analysis support |
| `src/qsys/universe/` | Eligibility/universe logic | Keep | Core supporting module |
| `src/qsys/utils/build_real_feature_store.py` | Real data feature store builder | Keep (active utility) | Operationally relevant |
| `src/qsys/utils/run_buffered_rebalance_from_feature_store.py` | Rebalance workflow runner | Keep (active utility) | Likely used in current workflow |
| `src/qsys/utils/compare_rebalance_policies_from_feature_store.py` | Policy comparison runner | Keep (active utility) | Directly tied to rebalance research |
| `src/qsys/utils/report_rebalance_policy_comparison.py` | Rebalance report utility | Keep (active utility) | Reporting utility for active area |
| `src/qsys/utils/build_market_index_benchmarks.py` | Benchmark builder | Keep (active utility) | Supports evaluation workflows |
| `src/qsys/utils/run_signal_sanity_grid.py` | Signal experiment runner | Keep, tag experimental | Phase/experiment-style naming |
| `src/qsys/utils/run_phase14b_risk_diagnostics.py` | Phase-specific diagnostics runner | Keep, tag experimental | Phase-coupled and likely iterative |
| `src/qsys/utils/run_signal_quality_mvp.py` | MVP signal-quality runner | Keep, tag experimental | Explicit MVP/experiment script |
| `src/qsys/utils/generate_synthetic_feature_store.py` | Synthetic smoke-test data generator | Keep, possible later archive | Useful for smoke tests but non-core |
| `src/qsys/utils/*_example.py` (all) | Demo/example entrypoints | Keep, possible archive-later | Good learning artifacts; non-core runtime |
| `scripts/sync_project_review_to_notion.py` | External docs sync tool | Keep | Active tooling for docs workflow |
| `run_demo.py` | Legacy demo launcher | Keep but mark deprecated/archive-later | README already marks as deprecated |
| `A_share_Analytical_DWH.ipynb` | Legacy original notebook pipeline | Keep but archive-later | Historical reference, not core path |
| `docs/notion_project_review/PROJECT_SYSTEM_MANUAL_DRAFT.md` | Draft manual | Consolidate source-of-truth; archive-later | Overlaps with dated manual |
| `docs/notion_project_review/PROJECT_SYSTEM_MANUAL_20260508.md` | Dated consolidated manual | Keep as canonical (or rename) | Likely best candidate for canonical doc |
| `docs/notion_project_review/GPT_REVIEW_BUNDLE.md` | AI-generated review bundle | Archive-later | Intermediate artifact likely duplicated |
| `docs/notion_project_review/deep_review_evidence_pack.md` | Evidence pack | Keep if actively referenced; else archive-later | Useful but may duplicate manual content |
| `docs/notion_project_review/GPT_WHOLE_PROJECT_SYSTEM_MANUAL_PROMPT.md` | Prompt artifact | Archive-later | Prompt artifact, not product documentation |
| `tests/risk/test_exposure.py` | Risk exposure tests | Keep, rename to unique basename later | Duplicate basename conflict risk |
| `tests/research/test_exposure.py` | Research exposure tests | Keep, rename to unique basename later | Duplicate basename conflict risk |
| `README.md` | Main project guide | Keep, update for consistency later | Some structure claims appear out-of-sync |

---

## Specific Hygiene Actions Recommended (Future, not done now)

1. Add explicit tags in utility docstrings/module headers: `ACTIVE_UTILITY` vs `EXPERIMENTAL` vs `EXAMPLE`.
2. Rename one or both exposure test files to unique basenames (e.g., `test_risk_exposure.py`, `test_research_exposure.py`).
3. Consolidate Notion review docs into one canonical manual + one index, move drafts/bundles/prompts into an `archive/` subfolder.
4. Update README structure and data-layer statements to match current tracked code.
5. Add a lightweight “artifact lifecycle” policy (active/experimental/deprecated/archive).

---

## Note

Per request, this audit is classification-only: **no deletions and no behavior/code changes were made**.

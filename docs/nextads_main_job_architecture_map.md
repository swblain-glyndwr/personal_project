# NextAds Main Job Architecture Map

This document maps the current NextAds main Databricks job route to the target package and entrypoint split. It is the lower-level companion to `docs/repo_migration_map.md`, which covers the repo-wide folder migration.

The purpose is practical: if someone wants to change candidate selection, scoring, assignment logic, control-sheet parsing, or downstream delivery, they should be able to identify the right area before editing code.

## Scope

This map covers:

- `resources/jobs/mktg_next_uk_nextads.yml`, currently deployed as `mktg_next_uk_nextads_candidate_build`;
- `resources/jobs/mktg_next_uk_nextads_page_build.yml`, which is triggered after the candidate build;
- active Python entrypoints run by those two jobs;
- triggered QA and delivery jobs only as downstream boundaries.

It does not move code by itself. The migration should stay behaviour-preserving until a separate story intentionally changes output logic.

## Target Domains

Use the existing target package domains consistently:

| Domain | Owns | Does not own |
|---|---|---|
| `control` | Control sheets, ad metadata, placement/page eligibility, exclusions, attribute/theme control metadata. | Customer-level scoring, final treatment decisions, downstream payload formatting. |
| `data` | Source access contracts, data validation, table contracts, reusable dataset definitions. | Business decisioning or ranking rules. |
| `features` | Reusable feature definitions and materialisation logic. | Final ad selection. |
| `retrieval` | Candidate pool creation: which ads/items/themes could be considered before scoring or final decisioning. | Final ranking order or cell-based treatment choice. |
| `ranking` | Model outputs, score components, score normalisation, ad ordering and top-N ranked candidate tables. | Fallow/control-cell decisions, final assignment write shape. |
| `decisioning` | Customer cells, treatment rules, assignment functions, suppression rules and final ad choice. | Model training/scoring, ad metadata ingestion, downstream payload formatting. |
| `delivery` | Assignment output shaping, Bloomreach/payload contracts, PLP Google Sheet, MASID handoff. | Candidate generation or ranking logic. |
| `reporting` | QA, checks, diagnostics and result reporting. | Production selection logic. |
| `common` | Config/path/table helpers and infrastructure-neutral utilities. | Domain-specific business rules. |

## Current Runtime Flow

```text
resources/jobs/mktg_next_uk_nextads.yml
  mktg_next_uk_nextads_candidate_build
    assign_customer_cells
      -> combine_customer_cells
    load_control_sheet
    load_control_sheet_v2
    parse_attributes
      -> parse_theme_mapping
        -> score_lightweight
          -> map_theme_scores_to_ads
            -> map_theme_scores_to_ads_v2
              -> trigger_page_build_job

resources/jobs/mktg_next_uk_nextads_page_build.yml
  mktg_next_uk_nextads_page_build
    build_page_v2
      -> trigger_payload_export_job
    build_page_primary
      -> build_page_secondary
        -> trigger_qa_job
        -> trigger_masid_handoff_check_job
        -> trigger_plp_gs_delivery_job
```

The current shape is operationally valid, but it mixes domains inside broad entrypoint folders. `jobs/nextads_main/` currently contains control, cell assignment, model scoring, candidate ranking, final page assignment and orchestration. Some active v2 code remains under `scripts/` and should be treated as production-transition code, not as dead legacy code.

## Quick Routing Guide

| Desired change | Current place to inspect first | Target place after split |
|---|---|---|
| Change how eligible ads are selected for ranking | `jobs/nextads_main/map_theme_scores_to_ads.py`; `src/next_ads/ranking/theme_score_mapping.py`; `scripts/map_theme_scores_to_ads_v2.py` | `src/next_ads/retrieval/theme_ad_candidates.py`; `src/next_ads/retrieval/adsv2/page_type_candidates.py`; thin jobs in `jobs/nextads_candidates/` or `jobs/nextads_v2/`. |
| Change theme/model score generation | `jobs/nextads_main/build_markov_chain.py` | `src/next_ads/ranking/theme_affinity/`; `src/next_ads/features/theme_affinity/`; thin job in `jobs/model/theme_affinity/`. |
| Change score components, greedy weighting, ad feedback or multi-session downweighting | `src/next_ads/ranking/theme_score_mapping.py`; some helpers currently in `src/next_ads/decisioning/assignment.py` | `src/next_ads/ranking/theme_score_components.py`; `src/next_ads/ranking/ad_feedback.py`; `src/next_ads/features/session_frequency.py`. |
| Change fixed/transient customer cells or audience assignment | `jobs/nextads_main/assign_customer_cells.py`; `jobs/nextads_main/combine_customer_cells.py`; `src/next_ads/decisioning/assignment.py` | `src/next_ads/decisioning/customer_cells.py`; `src/next_ads/decisioning/audiences.py`; thin jobs in `jobs/nextads_cells/`. |
| Change Basic, Best, NextGenAds or preranked assignment | `jobs/nextads_main/build_page.py`; `scripts/build_page_v2.py`; `src/next_ads/decisioning/assignment.py` | `src/next_ads/decisioning/random_assignment.py`; `src/next_ads/decisioning/preranked_assignment.py`; `src/next_ads/decisioning/nextgenads.py`. |
| Change final cell-map treatment selection or fallow-control behaviour | `jobs/nextads_main/build_page.py`; `scripts/build_page_v2.py` | `src/next_ads/decisioning/page_selection.py`; `src/next_ads/decisioning/adsv2/page_selection.py`. |
| Change final assignment table shape, Bloomreach/payload, MASID or PLP delivery | `jobs/nextads_main/build_page.py`; `scripts/build_page_v2.py`; triggered delivery jobs | `src/next_ads/delivery/assignments.py`; `src/next_ads/delivery/adsv2/`; `jobs/nextads_delivery/`. |
| Change control sheet parsing, attributes, themes or exclusions | `jobs/nextads_main/load_control_sheet.py`; `scripts/load_control_sheet_v2.py`; `jobs/nextads_main/parse_attributes.py`; `jobs/nextads_main/parse_theme_mapping.py` | `src/next_ads/control/`; `src/next_ads/control/adsv2/`; thin jobs in `jobs/nextads_control/` or `jobs/nextads_v2/`. |
| Change QA or diagnostics | `scripts/qa.py`; triggered QA job | `src/next_ads/reporting/qa.py`; `jobs/nextads_reporting/qa.py`. |
| Change job triggering or orchestration | `jobs/nextads_main/trigger_databricks_job.py` | `src/next_ads/common/databricks_jobs.py`; `jobs/orchestration/trigger_databricks_job.py`. |

## Current Entrypoint Map

| Current task | Current Python file | Current role | Target entrypoint | Target reusable package area | Risk | Notes |
|---|---|---|---|---|---|---|
| `assign_customer_cells` | `jobs/nextads_main/assign_customer_cells.py` | Builds fixed customer cells, fallow control, staff overrides, transient cells, AlgoDivision and audience cells, then writes fixed/transient cell tables. | `jobs/nextads_cells/assign_customer_cells.py` or split into `assign_fixed_cells.py` and `assign_transient_cells.py`. | `src/next_ads/decisioning/customer_cells.py`; `src/next_ads/decisioning/audiences.py`; `src/next_ads/decisioning/algorithm_division.py`. | High | This is output-affecting and currently mixes source customer preparation, fixed-cell policy, transient-cell policy and table writes. Split only after output-equivalence checks exist. |
| `combine_customer_cells` | `jobs/nextads_main/combine_customer_cells.py` | Combines fixed and transient cells, filters to customers with `AlgoDivision`, adds premium flag and writes latest combined cells. | `jobs/nextads_cells/combine_customer_cells.py`. | `src/next_ads/decisioning/customer_cells.py`. | High | This controls the customer population that reaches assignment. Treat join/filter changes as production-affecting. |
| `load_control_sheet` | `jobs/nextads_main/load_control_sheet.py` | Reads v1 control, placements and PLX URL sheets; validates; processes active ad-location rows; writes raw/latest and processed control tables. | `jobs/nextads_control/load_control_sheet.py`. | `src/next_ads/control/load_control_sheet.py`; `src/next_ads/control/placements.py`; `src/next_ads/control/multipage_locations.py`. | High | Already has reusable package functions. Keep as thin entrypoint and avoid adding business logic back into the job file. |
| `load_control_sheet_v2` | `scripts/load_control_sheet_v2.py` | Reads v2 control and exclusions sheets, validates, active-page-type expansion, targeting criteria construction and v2 control writes. | `jobs/nextads_v2/load_control_sheet.py` or `jobs/nextads_control/load_control_sheet_v2.py`. | `src/next_ads/control/adsv2/control_sheet.py`; `src/next_ads/control/adsv2/exclusions.py`; `src/next_ads/control/adsv2/targeting.py`. | High | Active v2 route. Do not leave under `scripts/` long-term, but do not move without route tests and output checks. |
| `parse_attributes` | `jobs/nextads_main/parse_attributes.py` | Builds item attribute catalog from product metadata and basket history; refreshes attribute set and item-attribute latest table. | `jobs/nextads_features/parse_attributes.py` or `jobs/nextads_control/parse_attributes.py`. | `src/next_ads/control/item_attributes.py`; possible stable feature contract under `src/next_ads/features/item_attributes.py`. | Medium/High | This is control/feature boundary work. If outputs are used by theme mapping, treat changes as ranking-affecting. |
| `parse_theme_mapping` | `jobs/nextads_main/parse_theme_mapping.py` | Reads theme mapping sheet, validates theme ranks, builds theme-to-attribute and item-to-theme mappings. | `jobs/nextads_features/parse_theme_mapping.py` or `jobs/nextads_control/parse_theme_mapping.py`. | `src/next_ads/control/theme_mapping.py`; possible feature contract under `src/next_ads/features/item_themes.py`. | High | Changes can alter every downstream theme score and ad ranking. |
| `score_lightweight` | `jobs/nextads_main/build_markov_chain.py` | Builds lightweight theme transition/scoring route from purchases and views; optionally refreshes transition probabilities; writes customer next-theme scores. | `jobs/model/theme_affinity/score_lightweight.py` or `jobs/nextads_scoring/build_theme_scores.py`. | `src/next_ads/ranking/theme_affinity/transition_model.py`; `src/next_ads/ranking/theme_affinity/customer_scoring.py`; `src/next_ads/features/theme_affinity/events.py`. | High | Despite the name, this is model/scoring logic, not page assignment. It should not live beside final assignment entrypoints. |
| `map_theme_scores_to_ads` | `jobs/nextads_main/map_theme_scores_to_ads.py` -> `src/next_ads/ranking/theme_score_mapping.py` | Maps customer theme scores to eligible ads, applies greedy/ad-feedback/multi-session scoring, ranks top ads per location and writes preranked candidates. | `jobs/nextads_candidates/build_theme_ad_candidates.py`. | `src/next_ads/retrieval/theme_ad_candidates.py`; `src/next_ads/ranking/theme_score_components.py`; `src/next_ads/ranking/theme_ad_ranking.py`. | High | This is the main candidate-selection area. It should be the obvious place to change candidate construction or ranking, not `build_page.py`. |
| `map_theme_scores_to_ads_v2` | `scripts/map_theme_scores_to_ads_v2.py` | Converts slot/location-level preranked candidates into v2 page-type-ranked candidates. | `jobs/nextads_v2/build_page_type_candidates.py`. | `src/next_ads/retrieval/adsv2/page_type_candidates.py`; `src/next_ads/ranking/adsv2/page_type_ranking.py`. | High | Active v2 candidate route. It should move out of `scripts/` with a compatibility wrapper. |
| `trigger_page_build_job` | `jobs/nextads_main/trigger_databricks_job.py` | Submits the page-build job without waiting. | `jobs/orchestration/trigger_databricks_job.py` or keep as generic job utility. | `src/next_ads/common/databricks_jobs.py`. | Medium | Thin utility. Safe to move once resource paths/tests are updated. |
| `build_page_primary` / `build_page_secondary` | `jobs/nextads_main/build_page.py` | Final v1 page-location assignment: reads control/cells/preranked candidates, assigns Basic/Best/NextGenAds, applies treatment map, fallow, page isolation, MASID, incremental suppression and writes assignments. | `jobs/nextads_assignment/build_page.py`. | `src/next_ads/decisioning/page_assignment.py`; `src/next_ads/decisioning/page_selection.py`; `src/next_ads/delivery/assignments.py`; `src/next_ads/control/ad_eligibility.py`. | High | This file is currently the final decisioning hotspot. Candidate ranking logic should not be added here; only final selection rules should live here. |
| `build_page_v2` | `scripts/build_page_v2.py` | Final v2 page-type assignment with rank spine, Basic/Best candidates, treatment selection, trigger-score carry-through and v2 assignment writes. | `jobs/nextads_v2/build_page.py` or `jobs/nextads_assignment/build_page_v2.py`. | `src/next_ads/decisioning/adsv2/page_assignment.py`; `src/next_ads/decisioning/adsv2/page_selection.py`; `src/next_ads/delivery/adsv2/assignments.py`. | High | Active v2 route. Keep separate from v1 final assignment until output contracts converge. |
| `trigger_qa_job` | `jobs/nextads_main/trigger_databricks_job.py` -> `scripts/qa.py` | Starts post-assignment QA. | `jobs/nextads_reporting/qa.py`. | `src/next_ads/reporting/qa.py`. | Medium | QA failure is deliberately decoupled from the main generation route; keep this operational boundary explicit. |
| `trigger_masid_handoff_check_job` | `jobs/nextads_main/trigger_databricks_job.py` -> `jobs/nextads_delivery/masid_handoff_check.py` | Starts MASID handoff validation/delivery check. | Keep in `jobs/nextads_delivery/masid_handoff_check.py`. | `src/next_ads/delivery/masid_handoff.py`. | Medium | Already belongs in delivery. Package reusable checks if they are not already there. |
| `trigger_payload_export_job` | `jobs/nextads_main/trigger_databricks_job.py` -> `scripts/build_v2_payload.py` | Starts v2 payload/Bloomreach export after v2 page build. | `jobs/nextads_delivery/build_v2_payload.py`. | `src/next_ads/delivery/adsv2/payload.py`; `src/next_ads/delivery/bloomreach.py`. | High | Active downstream contract. Move from `scripts/` only with payload contract checks. |
| `trigger_plp_gs_delivery_job` | `jobs/nextads_main/trigger_databricks_job.py` -> `jobs/nextads_delivery/plp_gs.py` | Starts PLP Google Sheet delivery. | Keep in `jobs/nextads_delivery/plp_gs.py`. | `src/next_ads/delivery/plp_google_sheets.py`. | Medium | Already on a delivery entrypoint path. Keep external integration logic out of candidate/assignment code. |

## Package Split Map For Large Current Modules

### `src/next_ads/ranking/theme_score_mapping.py`

This module currently spans multiple concerns. Split it by responsibility before making behavioural changes.

| Current concern | Target package | Reason |
|---|---|---|
| Read control sheet ads, filter `AudienceOnly`, remove underperforming ads | `src/next_ads/control/ad_eligibility.py` | Ad eligibility is control metadata, not scoring. |
| Theme-to-ad candidate mapping | `src/next_ads/retrieval/theme_ad_candidates.py` | This determines which ads can be considered for a customer/theme. |
| Theme score normalisation and greedy theme scoring | `src/next_ads/ranking/theme_score_components.py` | This changes ordering, but not final treatment selection. |
| Ad feedback score application | `src/next_ads/ranking/ad_feedback.py` | Feedback adjusts ranking score and is reused outside one entrypoint. |
| Multi-session downweighting | `src/next_ads/features/session_frequency.py` or `src/next_ads/ranking/session_downweighting.py` | The source signal is a feature; the multiplier is ranking policy. Keep the boundary clear. |
| Distinct ad-set construction by location | `src/next_ads/retrieval/location_ad_sets.py` | Candidate efficiency logic belongs with retrieval. |
| Top-N ad ranking per customer/ad set/location | `src/next_ads/ranking/theme_ad_ranking.py` | This is the core ranked-candidate output. |
| Writes to `preranked_ads_from_themes_latest` and score-component tables | Thin job layer or `src/next_ads/delivery/ranked_candidates.py` | Writes are operational IO and should stay easy to test/trace. |

### `src/next_ads/decisioning/assignment.py`

This file is useful but broad. It currently contains reusable assignment functions plus support functions that cross ranking/retrieval boundaries.

| Current concern | Target package |
|---|---|
| Random Basic assignment, including v2 cyclic assignment | `src/next_ads/decisioning/random_assignment.py` |
| Preranked Best assignment | `src/next_ads/decisioning/preranked_assignment.py` |
| NextGenAds cluster assignment | `src/next_ads/decisioning/nextgenads.py` |
| Greedy/quota assignment used by theme ranking | `src/next_ads/ranking/greedy_quota.py` unless it is only used for final allocation. |
| Ad feedback scoring | `src/next_ads/ranking/ad_feedback.py` |
| Audience and transient-cell helpers | `src/next_ads/decisioning/audiences.py` and `src/next_ads/decisioning/customer_cells.py` |
| AlgoDivision helper logic | `src/next_ads/decisioning/algorithm_division.py` or `src/next_ads/ranking/algorithm_division.py`, depending on whether it is a cell policy or score-derived segment. |

Keep `src/next_ads/decisioning/assignment.py` as a compatibility module during transition. It can re-export the moved functions until all callers are updated.

### `jobs/nextads_main/build_page.py`

This entrypoint should become a thin wrapper. Its internal logic should move into package modules.

| Current concern | Target package |
|---|---|
| Load ad metadata for one location | `src/next_ads/control/ad_eligibility.py` |
| Load customer cells and preranked candidates | `src/next_ads/retrieval/assignment_inputs.py` |
| Assign Basic, Best, BestChallenger and NextGenAds candidates | `src/next_ads/decisioning/page_assignment.py` |
| Apply configured cell map to select treatment | `src/next_ads/decisioning/page_selection.py` |
| Apply fallow control, premium substitution, page-type isolation and incremental suppression | `src/next_ads/decisioning/suppression.py` and `src/next_ads/decisioning/page_selection.py` |
| MASID construction and default rows | `src/next_ads/delivery/masid.py` |
| Write assignment tables | `src/next_ads/delivery/assignments.py` or the thin entrypoint layer. |

### `jobs/nextads_main/build_markov_chain.py`

This file is model/scoring work and should move away from `nextads_main`.

| Current concern | Target package |
|---|---|
| Build purchase/view scoring events | `src/next_ads/features/theme_affinity/events.py` |
| Build item-title and item-theme inputs | `src/next_ads/features/theme_affinity/item_inputs.py` |
| Train/refresh transition probabilities | `src/next_ads/ranking/theme_affinity/transition_model.py` |
| Score customers using purchase/view history | `src/next_ads/ranking/theme_affinity/customer_scoring.py` |
| Global top-theme backfill | `src/next_ads/ranking/theme_affinity/backfill.py` |
| Write next-theme score tables | `jobs/model/theme_affinity/score_lightweight.py` or a delivery helper for score outputs. |

## Proposed Target Job Folders

```text
jobs/
  nextads_control/
    load_control_sheet.py
    parse_attributes.py
    parse_theme_mapping.py

  nextads_cells/
    assign_customer_cells.py
    combine_customer_cells.py

  nextads_candidates/
    build_theme_scores.py              # or jobs/model/theme_affinity/score_lightweight.py
    build_theme_ad_candidates.py

  nextads_assignment/
    build_page.py

  nextads_v2/
    load_control_sheet.py
    build_page_type_candidates.py
    build_page.py

  nextads_delivery/
    build_v2_payload.py
    plp_gs.py
    masid_handoff_check.py

  nextads_reporting/
    qa.py

  orchestration/
    trigger_databricks_job.py
```

`resources/jobs/` can continue to hold the Databricks Asset Bundle YAML until the wider `resources/` to `pipelines/databricks/` migration is agreed. The important improvement is that each `python_file` points to an entrypoint path whose domain is obvious.

## Proposed Target Package Additions

```text
src/next_ads/
  control/
    ad_eligibility.py
    placements.py
    multipage_locations.py
    adsv2/
      control_sheet.py
      exclusions.py
      targeting.py

  features/
    item_attributes.py
    item_themes.py
    session_frequency.py
    theme_affinity/
      events.py
      item_inputs.py

  retrieval/
    theme_ad_candidates.py
    location_ad_sets.py
    assignment_inputs.py
    adsv2/
      page_type_candidates.py

  ranking/
    ad_feedback.py
    greedy_quota.py
    session_downweighting.py
    theme_score_components.py
    theme_ad_ranking.py
    theme_affinity/
      transition_model.py
      customer_scoring.py
      backfill.py
    adsv2/
      page_type_ranking.py

  decisioning/
    customer_cells.py
    audiences.py
    algorithm_division.py
    random_assignment.py
    preranked_assignment.py
    nextgenads.py
    page_assignment.py
    page_selection.py
    suppression.py
    adsv2/
      page_assignment.py
      page_selection.py

  delivery/
    assignments.py
    masid.py
    plp_google_sheets.py
    bloomreach.py
    adsv2/
      assignments.py
      payload.py

  reporting/
    qa.py
```

These names are intentionally explicit. A future maintainer should not need to know the old script history to find the code path.

## Migration Order

1. Add or update route tests before moving more code. Mirror `tests/unit/test_main_job_entrypoint_move.py` for each new entrypoint folder.
2. Convert top-level script logic into `main(...)` or `run(...)` functions where it is still executed at import time. This makes wrappers and tests safer.
3. Move v2 active scripts out of `scripts/` with compatibility wrappers first: `load_control_sheet_v2.py`, `map_theme_scores_to_ads_v2.py`, `build_page_v2.py`, and `build_v2_payload.py`.
4. Extract pure/reusable logic from `build_page.py` into `decisioning` and `delivery` without changing outputs.
5. Split `theme_score_mapping.py` into retrieval/ranking modules. Keep a compatibility function `run_theme_score_mapping(...)` until the job entrypoint is updated.
6. Move `build_markov_chain.py` into `jobs/model/theme_affinity/` or `jobs/nextads_candidates/` after score-output contracts are documented.
7. Update Databricks resource YAML paths in small PRs. Each path move should include DAB validation and import/path tests.
8. Remove old `scripts/` compatibility wrappers only after no Databricks resource, test or import path references them.

## Validation Expectations

| Move type | Minimum validation |
|---|---|
| Entrypoint path-only move | Import/path tests, DAB validate for DEV Integration/PREPROD/PROD targets, compatibility wrapper test. |
| Candidate retrieval split | Row count, primary-key check and eligibility comparison for old vs new candidate tables. |
| Ranking split | Score-component comparison, top-N comparison by `AccountNumber`/`Location` or `PageType`, deterministic tie-break check. |
| Customer-cell split | Fixed/transient/combined cell output comparison and account-count reconciliation. |
| Final assignment split | Assignment table equivalence by `AccountNumber`, `Location`/`PageType`, `Treatment`, `UniqueAdIDAssigned`, `MASID` and `TriggerScore` where relevant. |
| Delivery split | Contract/schema check for payload, PLP sheet output or MASID handoff table. |
| Control-sheet split | Raw/latest/processed table schema and primary-key checks; active-ad count comparison. |

## Practical Boundary Rules

- Candidate selection changes should start in `retrieval` or `ranking`, not in `build_page.py`.
- Final treatment selection changes should start in `decisioning`, not in `theme_score_mapping.py`.
- Control sheet and eligibility changes should start in `control`, then flow into candidate generation.
- Output schema/export changes should start in `delivery`.
- QA/reporting checks should not be embedded into production selection code.
- Active v2 files under `scripts/` are production-transition code and should be moved deliberately, not deleted as legacy scripts.

# NextAds V1/V2 Parallel Route DAG

Status: Target route for `feature/SWB/nextads-v1-v2-route-split`

This route keeps Theme Affinity as one shared upstream model output. The model writes customer-theme scores. V1 and v2 then split at the loaded control-sheet join layer: each route reads its own control sheet, uses that sheet's `Themes` column as the ad-theme mapping, joins customer `NextTheme` to ad `Themes`, and ranks the resulting customer-ad candidates at the route grain.

The v2 workbook is the source of truth for the product `Theme Mapping` tab. A Google Sheets Apps Script copies that tab into the v1 workbook, and the v1 tab should be locked to prevent manual edits. The Databricks job validates the copied v1 tab against the v2 source before `parse_theme_mapping` runs. If they differ, the candidate build stops so the Trade team can correct the source/copy before shared product theme scoring is refreshed.

The `score_lightweight` arrows in the candidate-build job are current Databricks task dependencies retained from the existing evening graph. They are not the customer-theme data source for `run_theme_score_mapping`; the mapper reads `config.theme_affinity_assignment_sources.champion` or `challenger`, which currently resolve to `theme_affinity_model_latest`.

## Diagram Legend

| Colour | Meaning |
| --- | --- |
| Blue | Shared Theme Affinity model route and shared customer-theme output. |
| Grey/teal | Shared candidate-build inputs and shared candidate-build tasks. |
| Green | V1 location-based route. |
| Purple | V2 page-type route. |
| Amber | Validation/guardrail task that can stop the candidate build. |
| Yellow | External Google Sheets/App Script dependency. |

## Databricks Job DAG

This view separates Databricks jobs from tasks. The candidate-build job is the scheduled evening orchestration point; page-build and delivery jobs are separate Databricks jobs triggered after their route-specific candidate tables are ready.

```mermaid
flowchart TD
  classDef sharedModel fill:#dbeafe,stroke:#2563eb,color:#111827
  classDef sharedTask fill:#e0f2f1,stroke:#0f766e,color:#111827
  classDef v1 fill:#dcfce7,stroke:#16a34a,color:#111827
  classDef v2 fill:#ede9fe,stroke:#7c3aed,color:#111827
  classDef guardrail fill:#fef3c7,stroke:#d97706,color:#111827
  classDef external fill:#fef9c3,stroke:#ca8a04,color:#111827

  subgraph EXTERNAL["External workbook dependency"]
    V2TM["V2 Theme Mapping<br/>source of truth"]:::external
    COPY["Apps Script<br/>copy"]:::external
    V1TM["Locked V1 Theme Mapping<br/>copied tab"]:::external
    V2TM --> COPY --> V1TM
  end

  subgraph TA_JOB["Job: theme_affinity"]
    TA_START["publish, predict,<br/>clean, sense-check"]:::sharedModel
    TA_OUT["theme_affinity_model_latest<br/>AccountNumber, NextTheme"]:::sharedModel
    TA_START --> TA_OUT
  end

  subgraph CAND_JOB["Job: candidate_build"]
    CAND_SHARED["shared cells,<br/>attributes, scoring"]:::sharedTask
    CAND_GUARD["sync and coverage<br/>guardrails"]:::guardrail
    CAND_V1["v1 candidates<br/>location grain"]:::v1
    CAND_V2["v2 candidates<br/>page-type grain"]:::v2
    TR1["trigger v1<br/>page build"]:::v1
    TR2["trigger v2<br/>page build"]:::v2
    CAND_SHARED --> CAND_GUARD
    CAND_GUARD --> CAND_V1 --> TR1
    CAND_GUARD --> CAND_V2 --> TR2
  end

  subgraph V1_PAGE_JOB["Job: page_build_v1"]
    PB1["build page<br/>Location"]:::v1
  end

  subgraph V2_PAGE_JOB["Job: page_build_v2"]
    PB2["build page<br/>PageType"]:::v2
  end

  subgraph V1_DELIVERY["Triggered v1 jobs"]
    QA["assignment_validation"]:::v1
    MASID["masid_handoff"]:::v1
    PLP["plp_gs_delivery"]:::v1
  end

  subgraph V2_DELIVERY["Triggered v2 job"]
    PAYLOAD["payload_export"]:::v2
  end

  V1TM --> CAND_GUARD
  TA_OUT --> CAND_GUARD
  TA_OUT --> CAND_V1
  TA_OUT --> CAND_V2
  TR1 --> PB1
  TR2 --> PB2
  PB1 --> QA
  PB1 --> MASID
  PB1 --> PLP
  PB2 --> PAYLOAD
```

## Candidate-Build Task DAG

This view expands the tasks inside `mktg_next_uk_nextads_candidate_build`. V1 and v2 are separate only where the route input or output contract differs.

```mermaid
flowchart TD
  classDef sharedModel fill:#dbeafe,stroke:#2563eb,color:#111827
  classDef sharedTask fill:#e0f2f1,stroke:#0f766e,color:#111827
  classDef v1 fill:#dcfce7,stroke:#16a34a,color:#111827
  classDef v2 fill:#ede9fe,stroke:#7c3aed,color:#111827
  classDef guardrail fill:#fef3c7,stroke:#d97706,color:#111827
  classDef external fill:#fef9c3,stroke:#ca8a04,color:#111827

  V2TM2["v2 Theme Mapping<br/>source"]:::external
  COPY2["Apps Script<br/>copy"]:::external
  V1TM2["locked v1 Theme Mapping<br/>copy"]:::external
  V2TM2 --> COPY2 --> V1TM2

  TA_LATEST["theme_affinity_model_latest<br/>AccountNumber, NextTheme"]:::sharedModel

  ASSIGN["assign_customer_cells"]:::sharedTask
  COMBINE["combine_customer_cells<br/>customer_cells_latest"]:::sharedTask
  ASSIGN --> COMBINE

  LOAD_V1["load_control_sheet_v1<br/>control_sheet_latest"]:::v1
  LOAD_V2["load_control_sheet_v2<br/>control_sheet_latest_v2"]:::v2

  ATTR["parse_attributes"]:::sharedTask
  SYNC["validate_theme_mapping_sync<br/>stops on drift"]:::guardrail
  THEME_MAP["parse_theme_mapping<br/>item_themes_latest"]:::sharedTask
  SCORE["score_lightweight"]:::sharedTask

  V1TM2 --> SYNC
  ATTR --> THEME_MAP
  SYNC --> THEME_MAP
  THEME_MAP --> SCORE

  COVER["validate_theme_affinity<br/>theme coverage"]:::guardrail
  LOAD_V1 --> COVER
  LOAD_V2 --> COVER
  TA_LATEST --> COVER

  MAP_V1["map_theme_scores_to_ads_v1<br/>Location grain"]:::v1
  MAP_V2["map_theme_scores_to_ads_v2<br/>PageType grain"]:::v2

  TA_LATEST --> MAP_V1
  TA_LATEST --> MAP_V2
  SCORE --> MAP_V1
  SCORE --> MAP_V2
  LOAD_V1 --> MAP_V1
  LOAD_V2 --> MAP_V2
  COVER --> MAP_V1
  COVER --> MAP_V2

  TRIGGER_V1["trigger_page_build_v1_job"]:::v1
  TRIGGER_V2["trigger_page_build_v2_job"]:::v2
  COMBINE --> TRIGGER_V1
  MAP_V1 --> TRIGGER_V1
  COMBINE --> TRIGGER_V2
  MAP_V2 --> TRIGGER_V2
```

## Rationale

The previous migration assumption was that v2 would fully replace v1 after a short parallel run. That is no longer true: Home Page remains on v1, while new page types need v2. The safe split is therefore at the route-specific control-sheet join and output-grain layer.

| Boundary | Recommendation | Reason |
| --- | --- | --- |
| Theme Affinity | Keep one shared scheduled job. | It writes customer-theme scores and does not depend on either control sheet. |
| Product Theme Mapping and lightweight scoring | Keep shared, with v2 as the workbook source of truth and copied into v1. | Shared product theme scoring remains valid only if both workbooks have identical Theme Mapping tabs; `validate_theme_mapping_sync` stops the job if the copy drifts. |
| Control sheets | Keep separate tasks. | V1 is location-based and v2 is page-type based. Each loaded table carries its own ad `Themes` values. |
| Theme coverage | Validate before both mappers. | A shared Theme Affinity model only works if route ad `Themes` exist in the shared `NextTheme` output. |
| Candidate mapping | Split v1 and v2 tasks. | This is where customer-theme scores are joined to route-specific ad themes and ranked by `Location` or `PageType`. |
| Page build | Keep separate triggered jobs. | V1 builds by `Location`; v2 builds by `PageType` and feeds payload export. |

## Databricks Job Granularity

The current YAMLs do not create a separate Databricks job for every node in the candidate-build DAG. They create one scheduled candidate-build job with multiple tasks, then trigger separate page-build and delivery jobs after candidate mapping completes.

| Databricks job | YAML | Runnable independently? | Contains / runs |
| --- | --- | --- | --- |
| `mktg_next_uk_nextads_theme_affinity` | `pipelines/databricks/jobs/mktg_next_uk_nextads_theme_affinity.yml` | Yes | Scheduled upstream model route; writes `theme_affinity_model_latest`. |
| `mktg_next_uk_nextads_candidate_build` | `pipelines/databricks/jobs/mktg_next_uk_nextads.yml` | Yes, as one multi-task job | Customer cells, v1/v2 control-sheet loads, shared product theme mapping/scoring, v1/v2 candidate mapping, and page-build triggers. |
| `mktg_next_uk_nextads_page_build` | `pipelines/databricks/jobs/mktg_next_uk_nextads_page_build.yml` | Yes | V1 location-based page build, then triggers validation, MASID handoff, and PLP delivery. |
| `mktg_next_uk_nextads_page_build_v2` | `pipelines/databricks/jobs/mktg_next_uk_nextads_page_build_v2.yml` | Yes | V2 page-type page build, then triggers payload export. |
| `mktg_next_uk_nextads_assignment_validation` | `pipelines/databricks/jobs/mktg_next_uk_nextads_assignment_validation.yml` | Yes | V1 assignment validation. |
| `mktg_next_uk_nextads_masid_handoff` | `pipelines/databricks/jobs/mktg_next_uk_nextads_masid_handoff.yml` | Yes | V1 MASID handoff check. |
| `mktg_next_uk_nextads_plp_gs_delivery` | `pipelines/databricks/jobs/mktg_next_uk_nextads_plp_gs_delivery.yml` | Yes | V1 PLP Google Sheets delivery. |
| `mktg_next_uk_nextads_payload_export` | `pipelines/databricks/jobs/mktg_next_uk_nextads_payload_export.yml` | Yes | V2 Bloomreach payload export. |

Inside `mktg_next_uk_nextads_candidate_build`, these are tasks, not standalone Databricks jobs:

| Candidate-build task | Script | Upstream task dependencies |
| --- | --- | --- |
| `assign_customer_cells` | `jobs/nextads_cells/assign_customer_cells.py` | None |
| `combine_customer_cells` | `jobs/nextads_cells/combine_customer_cells.py` | `assign_customer_cells` |
| `load_control_sheet_v1` | `jobs/nextads_control/load_control_sheet.py` | None |
| `load_control_sheet_v2` | `jobs/nextads_control/load_control_sheet_v2.py` | None |
| `parse_attributes` | `jobs/nextads_control/parse_attributes.py` | None |
| `validate_theme_mapping_sync` | `jobs/nextads_control/validate_theme_mapping_sync.py` | None |
| `parse_theme_mapping` | `jobs/nextads_control/parse_theme_mapping.py` | `parse_attributes`, `validate_theme_mapping_sync` |
| `score_lightweight` | `jobs/nextads_candidates/build_theme_scores.py` | `parse_theme_mapping` |
| `validate_theme_affinity_theme_coverage` | `jobs/nextads_candidates/validate_theme_affinity_theme_coverage.py` | `load_control_sheet_v1`, `load_control_sheet_v2` |
| `map_theme_scores_to_ads_v1` | `jobs/nextads_candidates/build_theme_ad_candidates.py` | `score_lightweight`, `load_control_sheet_v1`, `validate_theme_affinity_theme_coverage` |
| `map_theme_scores_to_ads_v2` | `jobs/nextads_candidates/build_page_type_candidates_v2.py` | `score_lightweight`, `load_control_sheet_v2`, `validate_theme_affinity_theme_coverage` |
| `trigger_page_build_v1_job` | `jobs/orchestration/trigger_databricks_job.py` | `combine_customer_cells`, `map_theme_scores_to_ads_v1` |
| `trigger_page_build_v2_job` | `jobs/orchestration/trigger_databricks_job.py` | `combine_customer_cells`, `map_theme_scores_to_ads_v2` |

If each node needs to be run as an independently addressable Databricks job, the YAML structure would need another split: separate job YAMLs for the control, scoring, and candidate-mapping nodes, plus an orchestration job using `run_job_task` or trigger tasks. That would improve manual rerun control, but it would add more deployed jobs, job ids, alert surfaces, and ordering contracts to maintain.

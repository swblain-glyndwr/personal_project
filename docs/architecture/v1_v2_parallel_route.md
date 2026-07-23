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
    V2TM["V2 Theme Mapping tab<br/>Workbook: 1UuqCDDvjrGIDPLIdc4Sq09KMHv8zy9VL0zehb0EJXp4<br/>Source of truth"]:::external
    COPY["Google Sheets Apps Script copy"]:::external
    V1TM["Copied/locked V1 Theme Mapping tab<br/>Workbook used by current parser"]:::external
    V2TM --> COPY --> V1TM
  end

  subgraph TA_JOB["Databricks job: mktg_next_uk_nextads_theme_affinity<br/>YAML: pipelines/databricks/jobs/mktg_next_uk_nextads_theme_affinity.yml<br/>Schedule: 09:00 Europe/London"]
    TA_START["publish, predict, clean, sense-check"]:::sharedModel
    TA_OUT["Writes: next_uk_nextads_theme_affinity_model_latest<br/>Grain: AccountNumber, NextTheme"]:::sharedModel
    TA_START --> TA_OUT
  end

  subgraph CAND_JOB["Databricks job: mktg_next_uk_nextads_candidate_build<br/>YAML: pipelines/databricks/jobs/mktg_next_uk_nextads.yml<br/>Schedule: 18:00 Europe/London"]
    CAND_SHARED["Shared inputs and scoring tasks<br/>Customer cells, attributes, copied Theme Mapping, lightweight scoring"]:::sharedTask
    CAND_GUARD["Guardrails<br/>Theme Mapping sync and Theme Affinity theme coverage"]:::guardrail
    CAND_V1["V1 candidate mapping<br/>Writes: preranked_ads_from_themes_latest"]:::v1
    CAND_V2["V2 candidate mapping<br/>Writes: preranked_ads_from_themes_v2_latest"]:::v2
    TR1["trigger_page_build_v1_job<br/>Script: jobs/orchestration/trigger_databricks_job.py"]:::v1
    TR2["trigger_page_build_v2_job<br/>Script: jobs/orchestration/trigger_databricks_job.py"]:::v2
    CAND_SHARED --> CAND_GUARD
    CAND_GUARD --> CAND_V1 --> TR1
    CAND_GUARD --> CAND_V2 --> TR2
  end

  subgraph V1_PAGE_JOB["Databricks job: mktg_next_uk_nextads_page_build<br/>YAML: pipelines/databricks/jobs/mktg_next_uk_nextads_page_build.yml"]
    PB1["build_page_primary/build_page_secondary<br/>Script: jobs/nextads_assignment/build_page.py<br/>Route grain: Location"]:::v1
  end

  subgraph V2_PAGE_JOB["Databricks job: mktg_next_uk_nextads_page_build_v2<br/>YAML: pipelines/databricks/jobs/mktg_next_uk_nextads_page_build_v2.yml"]
    PB2["build_page_v2<br/>Script: jobs/nextads_v2/build_page.py<br/>Route grain: PageType"]:::v2
  end

  subgraph V1_DELIVERY["Triggered v1 downstream jobs"]
    QA["mktg_next_uk_nextads_assignment_validation<br/>Script: jobs/nextads_reporting/assignment_validation.py"]:::v1
    MASID["mktg_next_uk_nextads_masid_handoff<br/>Script: jobs/nextads_delivery/masid_handoff_check.py"]:::v1
    PLP["mktg_next_uk_nextads_plp_gs_delivery<br/>Script: jobs/nextads_delivery/plp_gs.py"]:::v1
  end

  subgraph V2_DELIVERY["Triggered v2 downstream job"]
    PAYLOAD["mktg_next_uk_nextads_payload_export<br/>Script: jobs/nextads_delivery/build_v2_payload.py"]:::v2
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

  V2TM2["External: v2 Theme Mapping tab<br/>Source of truth"]:::external
  COPY2["External: Apps Script copy"]:::external
  V1TM2["External: copied/locked v1 Theme Mapping tab"]:::external
  V2TM2 --> COPY2 --> V1TM2

  TA_LATEST["Shared upstream table<br/>next_uk_nextads_theme_affinity_model_latest<br/>Columns include: AccountNumber, NextTheme"]:::sharedModel

  ASSIGN["assign_customer_cells<br/>Script: jobs/nextads_cells/assign_customer_cells.py"]:::sharedTask
  COMBINE["combine_customer_cells<br/>Script: jobs/nextads_cells/combine_customer_cells.py<br/>Writes: customer_cells_latest"]:::sharedTask
  ASSIGN --> COMBINE

  LOAD_V1["load_control_sheet_v1<br/>Script: jobs/nextads_control/load_control_sheet.py<br/>Writes: control_sheet_latest<br/>Route fields: Location, Themes"]:::v1
  LOAD_V2["load_control_sheet_v2<br/>Script: jobs/nextads_control/load_control_sheet_v2.py<br/>Writes: control_sheet_latest_v2<br/>Route fields: PageType, Themes"]:::v2

  ATTR["parse_attributes<br/>Script: jobs/nextads_control/parse_attributes.py"]:::sharedTask
  SYNC["validate_theme_mapping_sync<br/>Script: jobs/nextads_control/validate_theme_mapping_sync.py<br/>Compares v2 source tab to copied v1 tab<br/>Stops on differences"]:::guardrail
  THEME_MAP["parse_theme_mapping<br/>Script: jobs/nextads_control/parse_theme_mapping.py<br/>Reads copied v1 Theme Mapping<br/>Writes: theme_mapping_latest, item_themes_latest"]:::sharedTask
  SCORE["score_lightweight<br/>Script: jobs/nextads_candidates/build_theme_scores.py"]:::sharedTask

  V1TM2 --> SYNC
  ATTR --> THEME_MAP
  SYNC --> THEME_MAP
  THEME_MAP --> SCORE

  COVER["validate_theme_affinity_theme_coverage<br/>Script: jobs/nextads_candidates/validate_theme_affinity_theme_coverage.py<br/>Checks v1/v2 ad Themes exist in shared NextTheme output<br/>Stops on missing coverage"]:::guardrail
  LOAD_V1 --> COVER
  LOAD_V2 --> COVER
  TA_LATEST --> COVER

  MAP_V1["map_theme_scores_to_ads_v1<br/>Script: jobs/nextads_candidates/build_theme_ad_candidates.py<br/>Join: theme_affinity_model_latest.NextTheme = control_sheet_latest.Themes<br/>Writes: preranked_ads_from_themes_latest<br/>Output grain: Location"]:::v1
  MAP_V2["map_theme_scores_to_ads_v2<br/>Script: jobs/nextads_candidates/build_page_type_candidates_v2.py<br/>Join: theme_affinity_model_latest.NextTheme = control_sheet_latest_v2.Themes<br/>Writes: preranked_ads_from_themes_v2_latest<br/>Output grain: PageType"]:::v2

  TA_LATEST --> MAP_V1
  TA_LATEST --> MAP_V2
  SCORE --> MAP_V1
  SCORE --> MAP_V2
  LOAD_V1 --> MAP_V1
  LOAD_V2 --> MAP_V2
  COVER --> MAP_V1
  COVER --> MAP_V2

  TRIGGER_V1["trigger_page_build_v1_job<br/>Script: jobs/orchestration/trigger_databricks_job.py<br/>Submits: mktg_next_uk_nextads_page_build"]:::v1
  TRIGGER_V2["trigger_page_build_v2_job<br/>Script: jobs/orchestration/trigger_databricks_job.py<br/>Submits: mktg_next_uk_nextads_page_build_v2"]:::v2
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

## Alternatives

| Option | Shape | Why not chosen |
| --- | --- | --- |
| Keep v2 dependent on v1 mapping | V2 reads `preranked_ads_from_themes_latest` and reshapes `Location` to `PageType`. | Couples v2 to v1 location eligibility and ranking, which is wrong for a long-lived page-type route. |
| Split Theme Affinity into v1 and v2 jobs | Two scheduled model-scoring jobs feed two candidate routes. | Adds model scheduling, monitoring, and table risk without a control-sheet dependency to justify it. |
| Split product Theme Mapping and lightweight scoring now | Separate `item_themes` and `next_theme_scores` tables are built from each workbook before candidate mapping. | Avoided because v2 is now the Theme Mapping source of truth and the v1 tab is a locked copy. The validation task catches copy drift without introducing duplicate scoring routes. |
| Fully separate v1 and v2 candidate-build jobs now | Duplicate customer-cell and attribute tasks too. | More operational surface than needed while those inputs are still common. |
| Fully switch off v1 | V2 becomes the only route. | Not valid because Home Page continues on v1 beyond the initial parallel-run period. |

# NextAds V1/V2 Parallel Route DAG

Status: Current bundle route in this checkout

For a plain explanation of scoring, score selection, customer information, advert options and assignments, read [NextAds job and table flow](nextads_job_table_flow.md) first. This page owns the exact V1/V2 task dependencies and failure boundaries.

Theme Affinity is the account-theme score source currently selected for serving. Markov is an independently runnable shadow score source. Both write the same standard table shape, so a later model can join the route by producing that shape and being selected through configuration rather than by adding model-specific logic to advert-option or assignment code.

The 16:00 shared-customer-input job—named Candidate Foundation internally—prepares customer cells, repeat-ad exposure and advert feedback in one accepted record. At 18:00, both routes select that record: customer cells and repeat exposure affect V1 and V2, while advert feedback is applied only by V1. The routes then independently load their controls, select exact accepted score outputs, map those scores to active adverts and publish versioned batches of advert options. A control, score-selection or mapping failure blocks only the affected route. The quality measurements are a deliberate exception: V2 quality waits for V1 quality so the two tasks cannot write their shared tables concurrently, although neither quality task gates advert mapping.

The v1 and v2 page jobs are separate synchronous children. V1 calculates all 79 location scopes in one Spark graph; v2 calculates all five page types in one Spark graph. Each route writes its dated history before replacing live latest. Delivery starts only after the relevant live table has advanced successfully.

## Route Ownership And Colour Legend

| Colour | Meaning |
| --- | --- |
| Blue | Accepted score-provider output. |
| Teal | Shared input or candidate-foundation work. |
| Green | V1 location-based route. |
| Purple | V2 page-type route. |
| Amber | Validation or compatibility work. |
| Yellow | External CMS dependency. |

## Scheduled And Child-Job DAG

```mermaid
flowchart TD
  classDef provider fill:#dbeafe,stroke:#2563eb,color:#111827
  classDef shared fill:#e0f2f1,stroke:#0f766e,color:#111827
  classDef v1 fill:#dcfce7,stroke:#16a34a,color:#111827
  classDef v2 fill:#ede9fe,stroke:#7c3aed,color:#111827
  classDef monitor fill:#fef3c7,stroke:#d97706,color:#111827
  classDef external fill:#fef9c3,stroke:#ca8a04,color:#111827

  MODEL_REQUEST["12:15 model_scoring<br/>validate model_name=theme_affinity"]:::provider
  THEME_INPUTS["main NextAds child operation<br/>PREPARE_SCORING_INPUTS<br/>accepted mapping and attributes"]:::shared
  THEME_AFFINITY["model_scoring theme_affinity path<br/>accepted provider + compatibility"]:::provider
  MARKOV["13:00 Markov<br/>optional shadow output"]:::provider
  CANDIDATE_FOUNDATION["16:00 Candidate Foundation<br/>cells, exposure and feedback"]:::shared
  CMS["CMS data pull"]:::external

  MODEL_REQUEST --> THEME_INPUTS --> THEME_AFFINITY
  THEME_INPUTS -. "independent 13:00 schedule; waits for accepted input" .-> MARKOV

  subgraph CANDIDATE_JOB["18:00 candidate_build"]
    SELECT_FOUNDATION["select accepted Candidate Foundation"]:::shared
    V1_CONTROL["load and audit v1 control"]:::v1
    V2_CONTROL_RAW["land raw v2 control"]:::v2
    V2_CONTROL["process and audit v2 control<br/>against refreshed CMS"]:::v2
    V1_PROVIDER["select exact v1 provider build<br/>and validate theme coverage"]:::v1
    V2_PROVIDER["select exact v2 provider build<br/>and validate theme coverage"]:::v2
    V1_CANDIDATES["publish accepted v1 candidates"]:::v1
    V2_CANDIDATES["publish accepted v2 candidates"]:::v2
    RUN_V1["run v1 page build and wait"]:::v1
    RUN_V2["run v2 page build and wait"]:::v2

    SELECT_FOUNDATION --> V1_CANDIDATES
    SELECT_FOUNDATION --> V2_CANDIDATES
    V1_CONTROL --> V1_PROVIDER --> V1_CANDIDATES --> RUN_V1
    V2_CONTROL --> V2_PROVIDER --> V2_CANDIDATES --> RUN_V2
  end

  subgraph V1_PAGE_JOB["page_build_v1"]
    V1_BUILD["build_and_publish_v1<br/>79 scopes in one Spark graph<br/>history then live latest"]:::v1
    MASID["MASID handoff"]:::v1
    PLP["PLP delivery"]:::v1
    V1_BUILD --> MASID
    V1_BUILD --> PLP
  end

  subgraph V2_PAGE_JOB["page_build_v2"]
    V2_BUILD["build_and_publish_v2<br/>five page types in one Spark graph<br/>history then live latest"]:::v2
    PAYLOAD["Bloomreach payload export"]:::v2
    V2_BUILD --> PAYLOAD
  end

  COMPAT["21:00 candidate compatibility<br/>and assignment validation"]:::monitor

  CANDIDATE_FOUNDATION --> SELECT_FOUNDATION
  V2_CONTROL_RAW --> CMS --> V2_CONTROL
  THEME_AFFINITY --> V1_PROVIDER
  THEME_AFFINITY --> V2_PROVIDER
  MARKOV -. shadow; does not block .-> V1_PROVIDER
  MARKOV -. shadow; does not block .-> V2_PROVIDER
  RUN_V1 --> V1_BUILD
  RUN_V2 --> V2_BUILD
  V1_CANDIDATES -. exact accepted attempt .-> COMPAT
  V2_CANDIDATES -. exact accepted attempt .-> COMPAT
```

## Candidate-Build Task DAG

The main NextAds job has two validated operations. The generic model-scoring job calls `PREPARE_SCORING_INPUTS` for the same logical date; that branch creates the accepted theme-input snapshot and stops. The scheduled 18:00 run defaults to `CANDIDATE_BUILD`; that branch contains no customer-cell or model calculation, selects already accepted inputs, handles the two control routes independently and waits for both page-build child jobs.

```mermaid
flowchart TD
  classDef shared fill:#e0f2f1,stroke:#0f766e,color:#111827
  classDef v1 fill:#dcfce7,stroke:#16a34a,color:#111827
  classDef v2 fill:#ede9fe,stroke:#7c3aed,color:#111827
  classDef guard fill:#fef3c7,stroke:#d97706,color:#111827
  classDef external fill:#fef9c3,stroke:#ca8a04,color:#111827

  FOUNDATION["select_candidate_foundation"]:::shared
  LOAD_V1["load_control_sheet_v1"]:::v1
  AUDIT_V1["audit_control_sheet_v1"]:::guard
  QUALITY_AUDIT_ADS_V1["quality_audit_ads_v1"]:::guard
  LOAD_V2["load_control_sheet_v2<br/>land raw inputs"]:::v2
  WRITE_EXCLUSIONS["write_exclusions<br/>publish Cosmos exclusions"]:::external
  CMS["trigger_data_pull_for_CMS_pull<br/>use landed advert IDs"]:::external
  VALIDATE["validate_operation"]:::guard
  OPERATION["prepare_scoring_inputs_operation"]:::guard
  PREP_INPUTS["PREPARE_SCORING_INPUTS branch<br/>mapping + attributes + READY snapshot"]:::shared
  PROCESS_V2["process_control_sheet_v2<br/>check refreshed CMS"]:::v2
  AUDIT_V2["audit_control_sheet_v2"]:::guard
  QUALITY_AUDIT_ADS_V2["quality_audit_ads_v2"]:::guard

  SELECT_V1["resolve_scoring_portfolio_v1"]:::v1
  SELECT_V2["resolve_scoring_portfolio_v2"]:::v2
  COVER_V1["validate_score_provider_theme_coverage_v1"]:::guard
  COVER_V2["validate_score_provider_theme_coverage_v2"]:::guard
  MAP_V1["map_theme_scores_to_ads_v1"]:::v1
  MAP_V2["map_theme_scores_to_ads_v2"]:::v2
  PAGE_V1["run_page_build_v1 and wait"]:::v1
  PAGE_V2["run_page_build_v2 and wait"]:::v2

  VALIDATE --> OPERATION
  OPERATION -- true --> PREP_INPUTS
  OPERATION -- false --> LOAD_V1
  OPERATION -- false --> LOAD_V2
  OPERATION -- false --> FOUNDATION
  OPERATION -- false --> SELECT_V1
  OPERATION -- false --> SELECT_V2
  LOAD_V1 --> AUDIT_V1 --> COVER_V1
  LOAD_V1 --> QUALITY_AUDIT_ADS_V1
  SELECT_V1 --> COVER_V1
  LOAD_V2 --> WRITE_EXCLUSIONS
  LOAD_V2 --> CMS --> PROCESS_V2 --> AUDIT_V2 --> COVER_V2
  PROCESS_V2 --> QUALITY_AUDIT_ADS_V2
  QUALITY_AUDIT_ADS_V1 --> QUALITY_AUDIT_ADS_V2
  SELECT_V2 --> COVER_V2
  FOUNDATION --> MAP_V1
  FOUNDATION --> MAP_V2
  COVER_V1 --> MAP_V1 --> PAGE_V1
  COVER_V2 --> MAP_V2 --> PAGE_V2
```

## V1/V2 Separation Boundaries

The earlier migration assumption was that v2 would replace v1 after a short parallel run. Home Page remains on v1 while new page types use v2, so the routes now split at control loading, provider selection, candidate mapping and assignment publication.

| Boundary | Current behaviour | Reason |
| --- | --- | --- |
| Scoring provider | Models run independently and publish the same provider contract. | A new model can be evaluated or selected without changing candidate or assignment algorithms. |
| Candidate Foundation | Customer cells, repeat-ad exposure and advert feedback are prepared once in one accepted record. | Both routes use the accepted customer cells and repeat exposure without recalculating them. Only V1 applies the advert-feedback input. |
| Control data | V1 and v2 load and audit separate tables. | V1 uses locations and v2 uses page types. |
| Provider selection | V1 and v2 resolve separate configured selections and exact provider versions. | A failure or future configuration change in one route does not have to block the other. |
| Candidate mapping | V1 and v2 publish separate accepted candidate attempts through the same internal contract. | Each route keeps its own output grain while sharing the same readiness and repair rules. |
| Page build | V1 and v2 run separate bulk Spark jobs. | Each route replaces its complete live result independently and cannot leave a mixture of old and new scopes. |
| Compatibility and validation | At 21:00, compatibility derives the legacy advert-option shapes from exact accepted results, then starts assignment validation over the latest operational tables. | Validation or compatibility failures do not invalidate the accepted advert-option or serving assignment result. |

## Job And Candidate-Task Responsibilities

| Databricks job | YAML | Current responsibility |
| --- | --- | --- |
| `mktg_next_uk_nextads_candidate_foundation` | `pipelines/databricks/jobs/mktg_next_uk_nextads_candidate_foundation.yml` | Builds customer cells, repeat-ad exposure and advert feedback in parallel, then records one accepted foundation. |
| `mktg_next_uk_nextads_model_scoring` | `pipelines/databricks/jobs/mktg_next_uk_nextads_model_scoring.yml` | Validates `model_name`, calls the main NextAds `PREPARE_SCORING_INPUTS` operation for the same date, runs the supported scoring implementation and records the accepted provider build as READY before publishing compatibility outputs. Theme Affinity is the current supported implementation. |
| `mktg_next_uk_nextads_markov_scoring` | `pipelines/databricks/jobs/mktg_next_uk_nextads_markov_scoring.yml` | Builds and publishes the optional shadow provider, then writes its legacy compatibility outputs. |
| `mktg_next_uk_nextads_candidate_build` | `pipelines/databricks/jobs/mktg_next_uk_nextads.yml` | With `PREPARE_SCORING_INPUTS`, builds the accepted theme-input snapshot for the caller; with the scheduled default `CANDIDATE_BUILD`, selects the foundation, loads/audits both controls, selects provider builds, publishes candidate attempts and waits for both page jobs. |
| `mktg_next_uk_nextads_page_build` | `pipelines/databricks/jobs/mktg_next_uk_nextads_page_build.yml` | Builds and publishes all v1 assignments in one task, then runs the read-only MASID handoff check and PLP delivery. |
| `mktg_next_uk_nextads_page_build_v2` | `pipelines/databricks/jobs/mktg_next_uk_nextads_page_build_v2.yml` | Builds and publishes all v2 assignments in one task, then runs payload export. |
| `mktg_next_uk_nextads_candidate_compatibility` | `pipelines/databricks/jobs/mktg_next_uk_nextads_candidate_compatibility.yml` | Publishes the legacy v1/v2 candidate table shapes and triggers `mktg_next_uk_nextads_assignment_validation` independently. |

| Candidate-build task | Script or task type | Upstream task dependencies |
| --- | --- | --- |
| `validate_operation` | `jobs/orchestration/validate_nextads_operation.py` | None; accepts only `CANDIDATE_BUILD` or `PREPARE_SCORING_INPUTS` |
| `prepare_scoring_inputs_operation` | Native condition task | `validate_operation`; selects exactly one operation branch |
| `land_authoritative_theme_mapping`, `refresh_item_attributes`, `build_authoritative_item_themes`, `accept_scoring_inputs` | Main NextAds scoring-input entrypoints | `prepare_scoring_inputs_operation=true`; writes the exact accepted input snapshot and does not enter candidate build |
| `select_candidate_foundation` | `jobs/orchestration/select_candidate_foundation.py` | `prepare_scoring_inputs_operation=false` |
| `load_control_sheet_v1` | `jobs/nextads_control/load_control_sheet.py` | `prepare_scoring_inputs_operation=false` |
| `audit_control_sheet_v1` | `jobs/nextads_control/audit_control_sheet.py` | `load_control_sheet_v1` |
| `quality_audit_ads_v1` | `jobs/nextads_control/quality_audit_adverts.py` | `load_control_sheet_v1` |
| `load_control_sheet_v2` | `jobs/nextads_control/load_control_sheet_v2.py --phase land` | `prepare_scoring_inputs_operation=false` |
| `write_exclusions` | `jobs/nextads_control/exclusions_export.py` | `load_control_sheet_v2`; publishes the landed exclusions to the configured Cosmos container without gating V2 mapping |
| `trigger_data_pull_for_CMS_pull` | Native `run_job_task` | `load_control_sheet_v2` |
| `process_control_sheet_v2` | `jobs/nextads_control/load_control_sheet_v2.py --phase process` | `trigger_data_pull_for_CMS_pull` |
| `audit_control_sheet_v2` | `jobs/nextads_control/audit_control_sheet.py` | `process_control_sheet_v2` |
| `quality_audit_ads_v2` | `jobs/nextads_control/quality_audit_adverts.py` | `process_control_sheet_v2`, `quality_audit_ads_v1` |
| `resolve_scoring_portfolio_v1` | `jobs/orchestration/resolve_scoring_portfolio.py` | `prepare_scoring_inputs_operation=false` |
| `resolve_scoring_portfolio_v2` | `jobs/orchestration/resolve_scoring_portfolio.py` | `prepare_scoring_inputs_operation=false` |
| `validate_score_provider_theme_coverage_v1` | `jobs/nextads_candidates/validate_theme_affinity_theme_coverage.py` | `audit_control_sheet_v1`, `resolve_scoring_portfolio_v1` |
| `validate_score_provider_theme_coverage_v2` | `jobs/nextads_candidates/validate_theme_affinity_theme_coverage.py` | `audit_control_sheet_v2`, `resolve_scoring_portfolio_v2` |
| `map_theme_scores_to_ads_v1` | `jobs/nextads_candidates/build_theme_ad_candidates.py` | `select_candidate_foundation`, `validate_score_provider_theme_coverage_v1` |
| `map_theme_scores_to_ads_v2` | `jobs/nextads_candidates/build_page_type_candidates_v2.py` | `select_candidate_foundation`, `validate_score_provider_theme_coverage_v2` |
| `run_page_build_v1` | Native `run_job_task` | `map_theme_scores_to_ads_v1` |
| `run_page_build_v2` | Native `run_job_task` | `map_theme_scores_to_ads_v2` |

The internal candidate boundary consists of `candidate_ad_sets`, `candidate_scores` and the manifest-last `candidate_builds` table. Only rows belonging to an accepted candidate attempt can be selected by page build. Public v1/v2 assignment schemas remain unchanged.

The full time-based job and table hand-off is maintained in [`nextads_databricks_runtime_map.md`](../CICD/nextads_databricks_runtime_map.md).

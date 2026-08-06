# NextAds Databricks Runtime Map

Status: Working note

Architecture view refreshed: 2026-08-06. Observed runtime evidence last
refreshed from Databricks: 2026-07-03.

This page describes the NextAds Databricks bundle shape from a data-science perspective: what runs, when it runs, which tasks it calls, which jobs wait for child jobs, and where reusable model-building routes sit alongside the operational delivery routes.

This is a runtime map, not a deployment policy. For target availability rules, see [nextads_databricks_job_environment_matrix.md](nextads_databricks_job_environment_matrix.md). For the wider model, Feature Store and MLflow architecture view, see [../architecture/nextads_model_feature_overview.md](../architecture/nextads_model_feature_overview.md).

## How to Read This

The diagrams below show the Databricks Asset Bundle job structure currently defined under `pipelines/databricks/jobs`. Schedules and recent runtimes were pulled from Databricks job runs, using PROD jobs unless the row explicitly says DEV. Child jobs do not have their own fixed schedule; the parent waits for their result.

Durations are recent observed successful run durations, not SLAs. They should be treated as a guide for debugging, planning model refreshes, and understanding where a new model, feature-store table, or challenger route would attach.

## Modular, Deterministic And Atomic Route Introduced By This Change

This is the single-page view of the operational changes. The coloured
backgrounds are system responsibilities; each box names the job or boundary
that owns the work. Solid arrows are required dependencies. Dotted arrows are
optional provider participation or independent maintenance, not blocking
dependencies.

```mermaid
flowchart LR
  classDef input fill:#ecfeff,stroke:#0891b2,color:#111827
  classDef scoring fill:#dbeafe,stroke:#2563eb,color:#111827
  classDef ranking fill:#f3e8ff,stroke:#9333ea,color:#111827
  classDef decision fill:#dcfce7,stroke:#16a34a,color:#111827
  classDef delivery fill:#fff7ed,stroke:#ea580c,color:#111827
  classDef state fill:#f8fafc,stroke:#475569,color:#111827
  classDef failure fill:#fef2f2,stroke:#dc2626,color:#111827
  classDef guarantee fill:#111827,stroke:#111827,color:#ffffff
  classDef operations fill:#f1f5f9,stroke:#64748b,color:#111827

  subgraph INPUTS["INPUTS AND ACCEPTED FOUNDATIONS"]
    direction TB
    theme_sources["Theme mapping<br/>and item attributes"]:::input
    theme_inputs["Job: theme_inputs<br/>land, build and validate inputs"]:::input
    input_snapshot[("Accepted scoring-input snapshot<br/>RunDate, source versions and checksum<br/>ready manifest written last")]:::state
    foundation_sources["Customer cells, sessions/actions<br/>and advert results"]:::input
    candidate_foundation["Job: candidate_foundation<br/>cells + repeat exposure + raw feedback"]:::input
    foundation_snapshot[("Accepted candidate foundation<br/>exact source Delta versions<br/>ready manifest written last")]:::state
    control_v1["Candidate Build input branch<br/>load and audit v1 control sheet"]:::input
    control_v2["Candidate Build input branch<br/>data pull, then load and audit v2 control sheet"]:::input
    other_model_inputs["Any model-owned<br/>input or foundation"]:::input

    theme_sources --> theme_inputs --> input_snapshot
    foundation_sources --> candidate_foundation --> foundation_snapshot
  end

  subgraph SCORING["SCORING PROVIDERS"]
    direction TB
    theme_affinity["Job: theme_affinity<br/>build foundation, predict and validate"]:::scoring
    theme_provider[("Theme Affinity canonical provider<br/>model URI, exact input and output version<br/>ready manifest written last")]:::state
    markov["Job: markov_scoring<br/>independent shadow calculation"]:::scoring
    markov_provider[("Markov canonical provider<br/>optional EVALUATE entry<br/>ready manifest written last")]:::state
    challenger["Any themed or non-themed challenger<br/>model output adapted to the<br/>canonical provider contract"]:::scoring
    challenger_provider[("Canonical challenger provider<br/>validated output and ready manifest")]:::state

    theme_affinity --> theme_provider
    markov --> markov_provider
    challenger --> challenger_provider
  end

  subgraph RANKING["PORTFOLIO AND CANDIDATE RANKING"]
    direction TB
    portfolio["Candidate Build: resolve v1/v2 portfolios<br/>bind exact provider attempts and versions<br/>required serving slots must be present"]:::ranking
    candidate_v1["Candidate Build: one bulk v1 candidate task<br/>all serving entries and advert sets<br/>validate rows, then write ready manifest"]:::ranking
    candidate_v2["Candidate Build: one bulk v2 candidate task<br/>all serving entries and page types<br/>validate rows, then write ready manifest"]:::ranking
    candidate_attempt_v1[("Accepted v1 candidate attempt<br/>top 20 retained for assignment")]:::state
    candidate_attempt_v2[("Accepted v2 candidate attempt<br/>top 20 retained for assignment")]:::state

    portfolio --> candidate_v1 --> candidate_attempt_v1
    portfolio --> candidate_v2 --> candidate_attempt_v2
  end

  subgraph DECISIONING["DECISIONING AND COMPLETE-SNAPSHOT PUBLICATION"]
    direction TB
    build_v1["Job: page_build_v1<br/>bulk-stage 77 primary scopes, then<br/>SB2 and OC2 as one isolated attempt"]:::decision
    gate_v1{"All 79 v1 scopes<br/>complete and valid?"}:::decision
    publish_v1["Idempotently publish the complete v1 date slice<br/>then atomically replace the live latest snapshot"]:::decision
    keep_v1["No public v1 update<br/>previous accepted snapshot stays active"]:::failure
    build_v2["Job: page_build_v2<br/>bulk-stage all five page types<br/>as one isolated attempt"]:::decision
    gate_v2{"All five v2 page types<br/>complete and valid?"}:::decision
    publish_v2["Idempotently publish the complete v2 date slice<br/>then atomically replace the live latest snapshot"]:::decision
    keep_v2["No public v2 update<br/>previous accepted snapshot stays active"]:::failure
    retry_rule["Retry stability<br/>each repair keeps its attempt identity;<br/>rows from different attempts cannot mix"]:::state

    build_v1 --> gate_v1
    gate_v1 -- Yes --> publish_v1
    gate_v1 -- No --> keep_v1
    publish_v1 -. latest replacement fails .-> keep_v1
    build_v2 --> gate_v2
    gate_v2 -- Yes --> publish_v2
    gate_v2 -- No --> keep_v2
    publish_v2 -. latest replacement fails .-> keep_v2
    retry_rule --> build_v1
    retry_rule --> build_v2
  end

  subgraph DELIVERY["DELIVERY"]
    direction TB
    v1_delivery["V1 fan-out after publication<br/>assignment validation + MASID + PLP"]:::delivery
    v2_delivery["V2 fan-out after publication<br/>payload export"]:::delivery
  end

  subgraph GUARANTEES["SYSTEM GUARANTEES ADDED BY THIS CHANGE"]
    direction TB
    deterministic["DETERMINISM<br/>Pinned dates, snapshots and Delta versions<br/>stable ordering, checksums and attempt IDs"]:::guarantee
    atomic["ATOMICITY<br/>Rows before ready manifests<br/>all scopes before public publication<br/>failure leaves the previous snapshot active"]:::guarantee
    modular["MODULARITY<br/>Any canonical provider can join a portfolio<br/>and reuse candidate, decisioning and delivery"]:::guarantee
  end

  subgraph OPERATIONS["INDEPENDENT OPERATIONS"]
    maintenance["Job: table_maintenance at 05:00<br/>remove expired attempts and staging<br/>never blocks the nightly route"]:::operations
  end

  style INPUTS fill:#f0fdff,stroke:#0891b2,stroke-width:2px
  style SCORING fill:#eff6ff,stroke:#2563eb,stroke-width:2px
  style RANKING fill:#faf5ff,stroke:#9333ea,stroke-width:2px
  style DECISIONING fill:#f0fdf4,stroke:#16a34a,stroke-width:2px
  style DELIVERY fill:#fffaf5,stroke:#ea580c,stroke-width:2px
  style GUARANTEES fill:#f8fafc,stroke:#111827,stroke-width:2px
  style OPERATIONS fill:#f8fafc,stroke:#64748b,stroke-width:2px

  input_snapshot --> theme_affinity
  input_snapshot --> markov
  other_model_inputs --> challenger
  theme_provider --> portfolio
  markov_provider -. optional shadow .-> portfolio
  challenger_provider -. configured challenger .-> portfolio
  foundation_snapshot --> candidate_v1
  foundation_snapshot --> candidate_v2
  control_v1 --> candidate_v1
  control_v2 --> candidate_v2
  candidate_attempt_v1 --> build_v1
  candidate_attempt_v2 --> build_v2
  publish_v1 --> v1_delivery
  publish_v2 --> v2_delivery
  deterministic -. governs .-> input_snapshot
  deterministic -. governs .-> portfolio
  atomic -. governs .-> candidate_attempt_v1
  atomic -. governs .-> gate_v1
  atomic -. governs .-> gate_v2
  modular -. enables .-> challenger_provider
  modular -. enables .-> portfolio
  maintenance -. retention only .-> retry_rule
```

The important live-state rule is shown by the two decision gates. Candidate
rows and assignment scopes are private to one attempt until their ready or
complete marker is published. A failed task or repair therefore cannot combine
part of one attempt with part of another. If any required v1 scope or v2 page
type is missing, the public latest table is not advanced and the previous
accepted assignment snapshot remains active. V1 and v2 make that decision
independently, so one route can succeed without publishing partial state from
the other. This is the protection against the partial-retry failure mode behind
duplicate live Shopping Bag assignments: a repaired scope cannot be combined
with rows from another attempt in the public latest snapshot.

Theme Affinity is the required provider in the current default portfolios.
Markov is an independently runnable, optional evaluation entry and cannot block
candidate publication. A new challenger follows the same model-output,
canonical-provider, portfolio, candidate and assignment path; it does not need
a separate assignment or delivery implementation.

## Daily Runtime Shape

```mermaid
flowchart TD
  classDef sharedModel fill:#dbeafe,stroke:#2563eb,color:#111827
  classDef sharedTask fill:#e0f2f1,stroke:#0f766e,color:#111827
  classDef v1 fill:#dcfce7,stroke:#16a34a,color:#111827
  classDef v2 fill:#ede9fe,stroke:#7c3aed,color:#111827
  classDef reporting fill:#f1f5f9,stroke:#64748b,color:#111827

  results["07:15 results"]:::reporting
  realtime_results["07:30 realtime results"]:::reporting
  theme_inputs["12:15 theme inputs"]:::sharedModel
  theme_affinity["13:00 theme_affinity"]:::sharedModel
  markov["13:00 markov_scoring<br/>optional shadow"]:::sharedModel
  candidate_foundation["16:00 candidate_foundation"]:::sharedTask
  candidate["18:00 candidate_build"]:::sharedTask
  realtime_inputs["18:00 realtime inputs"]:::reporting
  page_build["synchronous page_build_v1"]:::v1
  page_build_v2["synchronous page_build_v2"]:::v2
  qa["assignment_validation"]:::v1
  masid["masid_handoff"]:::v1
  payload["payload_export"]:::v2
  plp["plp_gs_delivery"]:::v1
  feature_store["21:00 feature_store"]:::sharedModel

  theme_inputs --> theme_affinity
  theme_inputs --> markov
  theme_affinity --> candidate
  candidate_foundation --> candidate
  markov -. optional evaluation .-> candidate
  candidate --> page_build
  candidate --> page_build_v2
  page_build --> qa
  page_build --> masid
  page_build --> plp
  page_build_v2 --> payload
```

## Recent Observed Runtimes

| Route | Workspace/profile | Databricks job id | Schedule or trigger | Latest successful run id | Last three successful durations |
| --- | --- | --- | --- | --- | --- |
| Candidate build | PROD | `539075297323897` | 18:00 daily | `101421282112344` | 3h 50m 34s; 3h 40m 30s; 3h 21m 55s |
| Page build | PROD | `269306885845144` | Triggered by candidate build | `724497366216494` | 45m 46s; 50m 52s; 41m 29s |
| Assignment validation | PROD | `499074472587770` | Triggered by page build | `1001390743121428` | 12m 43s; 11m 55s; 12m 41s |
| MASID handoff | PROD | `61892626872113` | Triggered by page build | `493812673009118` | 9m 20s; 16m 1s; 8m 21s |
| Payload export | PROD | `1085437962619214` | Triggered by page build | `313359766451448` | 26m 16s; 24m 17s; 23m 54s |
| PLP Google Sheets delivery | PROD | `258720571842239` | Triggered by page build | `293773116584228` | 10m 27s; 10m 42s; 11m 41s |
| Results | PROD | `326879697801368` | 07:15 daily | `1016879679282989` | 2h 1m 6s; 2h 12m 17s; 2h 1m 37s |
| Realtime inputs | PROD | `510370009427574` | 18:00 daily | `384313899991554` | 20m 23s; 18m 26s; 19m 15s |
| Realtime results | PROD | `36753739041122` | 07:30 daily | `276035162295688` | 9m 21s; 11m 23s; 9m 2s |
| Theme Affinity | PROD | `27892907532455` | 09:00 daily | `11890698402594` | 3h 14m 33s; 3h 41m 37s; 4h 2m 16s |
| Feature store | DEV | `643939878851484` | 21:00 daily in `DEV_FEATURE_STORE` | None found in recent successful runs | No recent successful durations found |

## Candidate Build Task Graph

This is the main evening operational route. It selects one accepted candidate
foundation shared by v1 and v2, while each route independently captures its
control input, resolves a declared scoring portfolio and publishes an accepted
candidate attempt from its serving entries. Theme Inputs, Candidate Foundation,
Theme Affinity and Markov scoring are upstream jobs rather than candidate-task
implementations.

Each route audit and coverage task reports business findings without hiding technical failures. Missing themes are surfaced for follow-up and naturally cannot produce theme-matched candidates; an unreadable control or pinned provider snapshot stops only the affected route before mapping.

Colour key: blue = accepted score-provider output; teal = shared candidate tasks; green = v1 route; purple = v2 route; amber = guardrail; yellow = external CMS dependency.

```mermaid
flowchart TD
  classDef sharedModel fill:#dbeafe,stroke:#2563eb,color:#111827
  classDef sharedTask fill:#e0f2f1,stroke:#0f766e,color:#111827
  classDef v1 fill:#dcfce7,stroke:#16a34a,color:#111827
  classDef v2 fill:#ede9fe,stroke:#7c3aed,color:#111827
  classDef guardrail fill:#fef3c7,stroke:#d97706,color:#111827
  classDef external fill:#fef9c3,stroke:#ca8a04,color:#111827

  provider_build["accepted canonical<br/>score-provider build"]:::sharedModel
  foundation_build["accepted candidate<br/>foundation snapshot"]:::sharedTask
  cms_pull["CMS data pull"]:::external

  subgraph CANDIDATE_JOB["Job: candidate_build"]
    select_candidate_foundation["select_candidate_foundation"]:::sharedTask
    load_control_sheet_v1["load_control_sheet_v1<br/>control_sheet_latest"]:::v1
    audit_control_sheet_v1["audit_control_sheet_v1"]:::guardrail
    load_control_sheet_v2["load_control_sheet_v2<br/>control_sheet_latest_v2"]:::v2
    audit_control_sheet_v2["audit_control_sheet_v2"]:::guardrail
    select_provider_v1["resolve_scoring_portfolio_v1"]:::v1
    select_provider_v2["resolve_scoring_portfolio_v2"]:::v2
    validate_provider_coverage_v1["validate provider coverage v1"]:::guardrail
    validate_provider_coverage_v2["validate provider coverage v2"]:::guardrail
    map_theme_scores_to_ads_v1["map_theme_scores_to_ads_v1<br/>accepted Location candidates"]:::v1
    map_theme_scores_to_ads_v2["map_theme_scores_to_ads_v2<br/>accepted PageType candidates"]:::v2
    run_page_build_v1["run_page_build_v1<br/>waits"]:::v1
    run_page_build_v2["run_page_build_v2<br/>waits"]:::v2
  end

  foundation_build --> select_candidate_foundation
  load_control_sheet_v1 --> audit_control_sheet_v1
  cms_pull --> load_control_sheet_v2 --> audit_control_sheet_v2
  provider_build --> select_provider_v1
  provider_build --> select_provider_v2
  audit_control_sheet_v1 --> validate_provider_coverage_v1
  select_provider_v1 --> validate_provider_coverage_v1
  audit_control_sheet_v2 --> validate_provider_coverage_v2
  select_provider_v2 --> validate_provider_coverage_v2
  select_candidate_foundation --> map_theme_scores_to_ads_v1
  select_candidate_foundation --> map_theme_scores_to_ads_v2
  validate_provider_coverage_v1 --> map_theme_scores_to_ads_v1
  validate_provider_coverage_v2 --> map_theme_scores_to_ads_v2
  map_theme_scores_to_ads_v1 --> run_page_build_v1
  map_theme_scores_to_ads_v2 --> run_page_build_v2
  run_page_build_v1 --> page_build["child job<br/>page_build_v1"]:::v1
  run_page_build_v2 --> page_build_v2["child job<br/>page_build_v2"]:::v2
```

Observed latest successful candidate-build task timing, from run `101421282112344`, with task names normalised to the target route. New guardrail tasks have no observed PROD baseline yet, so their durations are listed as new rather than historical measurements:

| Task | Starts after run start | Duration | Depends on |
| --- | ---: | ---: | --- |
| `select_candidate_foundation` | 0m | Ready immediately or waits up to 30 minutes for the accepted same-day foundation | None |
| `load_control_sheet_v1` | 0m | 12m 43s historical baseline | None |
| `audit_control_sheet_v1` | After v1 control | New route guard | `load_control_sheet_v1` |
| `trigger_data_pull_for_CMS_pull` | 0m | Child-job runtime | None |
| `load_control_sheet_v2` | After CMS acquisition | 11m 52s historical loader baseline | `trigger_data_pull_for_CMS_pull` |
| `audit_control_sheet_v2` | After v2 control | New route guard | `load_control_sheet_v2` |
| `resolve_scoring_portfolio_v1` | 0m | Ready immediately or required serving entry waits to 18:30 | None |
| `resolve_scoring_portfolio_v2` | 0m | Ready immediately or required serving entry waits to 18:30 | None |
| `validate_score_provider_theme_coverage_v1` | After v1 audit and portfolio resolution | New route guard | `audit_control_sheet_v1`, `resolve_scoring_portfolio_v1` |
| `validate_score_provider_theme_coverage_v2` | After v2 audit and portfolio resolution | New route guard | `audit_control_sheet_v2`, `resolve_scoring_portfolio_v2` |
| `map_theme_scores_to_ads_v1` | After the accepted foundation and v1 checks | 1h 22m 55s historical mapping baseline | Builds every serving entry, writes ad sets and top-20 candidate rows, then publishes the accepted attempt. |
| `map_theme_scores_to_ads_v2` | After the accepted foundation and v2 checks | 43m 36s historical mapping baseline | Applies the same manifest-last candidate publication at page-type grain. |
| `run_page_build_v1` | After v1 mapping | Full child-job runtime | `map_theme_scores_to_ads_v1` |
| `run_page_build_v2` | After v2 mapping | Full child-job runtime | `map_theme_scores_to_ads_v2` |

The prior trigger-task durations are no longer comparable: `run_page_build_v1`
and `run_page_build_v2` now remain active until their child jobs finish. Capture a
new three-run DEV baseline before using this table for end-to-end timing targets.

The candidate tasks retain the current preranked tables as compatibility
outputs, but page-build consumers no longer read them. Each page-build child
loads its exact ready `candidate_build_attempt_id`, resolves separate `best` and
`best_challenger` portfolio entries, and carries the accepted portfolio and
foundation provenance into internal staging and completion events.

## Bulk Page Build And Delivery Fan-Out

The v1 and v2 page-build jobs are not normally scheduled by themselves in PROD.
They are run as synchronous child jobs after their respective mapping tables and
shared customer cells are ready. V1 builds its 77 primary locations in one task
and its two inherited secondary locations in one following task. V2 builds all
five page types in one task. This removes per-scope cluster starts while keeping
the public assignment grain unchanged. After publication, v1 still fans out to
QA, MASID handoff and PLP delivery, while v2 fans out to payload export.

```mermaid
flowchart TD
  classDef v1 fill:#dcfce7,stroke:#16a34a,color:#111827
  classDef v2 fill:#ede9fe,stroke:#7c3aed,color:#111827
  classDef trigger fill:#f8fafc,stroke:#64748b,color:#111827

  subgraph V1_PAGE_BUILD["Job: page_build_v1"]
    build_page_primary["build_page_primary"]:::v1
    build_page_secondary["build_page_secondary"]:::v1
    publish_assignment_build_v1["publish_assignment_build_v1"]:::v1
    run_assignment_validation["run_assignment_validation"]:::trigger
    run_masid_handoff["run_masid_handoff"]:::trigger
    run_plp_gs_delivery["run_plp_gs_delivery"]:::trigger
    build_page_primary --> build_page_secondary
    build_page_secondary --> publish_assignment_build_v1
    publish_assignment_build_v1 --> run_assignment_validation
    publish_assignment_build_v1 --> run_masid_handoff
    publish_assignment_build_v1 --> run_plp_gs_delivery
  end
  subgraph V2_PAGE_BUILD["Job: page_build_v2"]
    build_page_v2["build_page_v2"]:::v2
    publish_assignment_build_v2["publish_assignment_build_v2"]:::v2
    run_payload_export["run_payload_export"]:::trigger
    build_page_v2 --> publish_assignment_build_v2 --> run_payload_export
  end
  run_assignment_validation --> qa["assignment_validation"]:::v1
  run_masid_handoff --> masid["masid_handoff"]:::v1
  run_payload_export --> payload["payload_export"]:::v2
  run_plp_gs_delivery --> plp["plp_gs_delivery"]:::v1
```

The following timings are the pre-bulk baseline from successful run
`724497366216494`; capture new DEV evidence before treating them as current:

| Task | Starts after run start | Duration | Depends on |
| --- | ---: | ---: | --- |
| `build_page_primary` | 0m | 31m 50s | None |
| `build_page_v2` | 0m | 22m 30s | None |
| `build_page_secondary` | 31m | 14m 58s | `build_page_primary` |
| `publish_assignment_build_v1` | After secondary scopes | New complete-build publisher | `build_page_secondary` |
| `publish_assignment_build_v2` | After v2 scopes | New complete-build publisher | `build_page_v2` |
| `run_plp_gs_delivery` | After v1 publication | Full child-job runtime | `publish_assignment_build_v1` |
| `run_assignment_validation` | After v1 publication | Full child-job runtime | `publish_assignment_build_v1` |
| `run_masid_handoff` | After v1 publication | Full child-job runtime | `publish_assignment_build_v1` |
| `run_payload_export` | After v2 publication | Full child-job runtime | `publish_assignment_build_v2` |

The delivery jobs remain single-purpose and independently runnable, but the
nightly page route waits for their result: `assignment_validation`,
`masid_handoff_check`, `Export_for_Bloomreach`, and `nextads_plp_gs`.

## Theme Affinity Route

Theme Affinity is its own scheduled production model route. It is not part of the feature-store refresh and it is not triggered by candidate build. Its outputs can become inputs to later operational or model-building work, but the current job remains a standalone scheduled route.

```mermaid
flowchart TD
  predict_data_prep --> publish_foundation
  predict_data_prep --> sense_check_dlt_data
  publish_foundation --> model_predict
  model_predict --> publish_provider_build
  publish_provider_build --> sense_check_model_outputs
```

Observed latest successful Theme Affinity task timing, from run `11890698402594`:

| Task | Starts after run start | Duration | Depends on |
| --- | ---: | ---: | --- |
| `predict_data_prep` | 0m | 2h 16m 28s | None |
| `sense_check_dlt_data` | 2h 16m | 34m 24s | `predict_data_prep` |
| `publish_foundation` | 2h 16m | 36m 12s | `predict_data_prep` |
| `model_predict` | 2h 53m | 9m 17s | `publish_foundation` |
| `publish_provider_build` | Not yet measured | Not yet measured | `model_predict` |
| `sense_check_model_outputs` | 3h 9m | 6m 38s | `publish_provider_build` |

## Results Route

The results job is a scheduled reporting and labelling route. It is where production outcome data, performance checks, BigQuery output, top-ad reporting, and Theme Affinity inference-log enrichment are assembled after delivery has happened.

```mermaid
flowchart TD
  results_1 --> results_2 --> results_3 --> results_agg --> results_performance_check --> results_top_ads
  results_3 --> enrich_theme_affinity_inference_log
  results_agg --> results_to_bigquery
```

## Realtime Routes

Realtime inputs and realtime results are scheduled jobs, not part of the candidate-build fan-out.

```mermaid
flowchart TD
  viewed_bought["18:00 realtime_inputs<br/>viewed_bought"]
  realtime_results["07:30 realtime_results_cicd<br/>realtime_results"]
```

## Feature Store Route

The feature-store job is deliberately separate from the operational PROD delivery routes. It currently exists in the `DEV_FEATURE_STORE` target only, writing reusable model-building tables in `marketingdata_dev.nextads_feature_store`. Its purpose is to publish reusable features and model inputs, not final assignment, ranking, delivery, or production scoring output.

```mermaid
flowchart TD
  create_feature_store_tables --> preflight_feature_store_sources
  create_feature_store_tables --> build_theme_affinity_training_input
  preflight_feature_store_sources --> build_account_features
  preflight_feature_store_sources --> build_advert_features
  build_account_features --> build_theme_affinity_features
  build_advert_features --> build_theme_affinity_features
  build_account_features --> build_pctr_affinity_features
  build_advert_features --> build_pctr_affinity_features
  build_theme_affinity_features --> build_model_inputs
  build_pctr_affinity_features --> build_model_inputs
  build_model_inputs --> quality_checks
```

For future challenger work, this means the feature store should be treated as a reusable input layer. A challenger model can consume feature-store tables, Theme Affinity outputs, operational candidate tables, or experiment-specific joins, but its final scores, rankings, assignment decisions, and delivery outputs should remain in model output or decisioning tables rather than being hidden inside feature creation.

## Where New Model Work Fits

New NextAds model work should decide which layer it belongs to before adding a job or table:

| Layer | Use it for | Do not use it for |
| --- | --- | --- |
| Source or operational preparation | Stable inputs and existing operational pipeline steps. | Reusable model feature contracts unless they are intentionally feature-store owned. |
| Feature store | Reusable account, advert, candidate, label, and model-input features that can be rebuilt point-in-time and shared across models. | Final scores, rankings, assignment decisions, delivery payloads, or one-off experiment outputs. |
| Model scoring/challenger output | Model-specific scores, probabilities, candidate rankings, and challenger evidence. | General reusable features unless they are promoted into a feature-store contract. |
| Decisioning/assignment adapter | Selection between champion/challenger outputs and conversion into the current delivery shape. | Feature engineering or training-set assembly. |
| Delivery/reporting | Page build, exports, QA, handoff checks, results, and external/reporting outputs. | Model-training features or hidden scoring logic. |


# NextAds retry-stability source audit

This audit covers every Python and SQL file under `jobs/` and `src/`, including
all repository entrypoints referenced by the deployed bundle. It records the
remaining occurrences of partition-sensitive operations, unordered
aggregations, deduplication, destructive writers, direct table overwrites and
per-write housekeeping.

This is source-review evidence. It is not production runtime proof and it does
not claim that an owner has approved an exclusion. `Out of scope` means the
code is outside the incident-bearing candidate-to-delivery route and must be
reviewed with the named owner before a later migration changes its behaviour.

The machine guard reads the marked tables below. A new occurrence, a removed
occurrence without an audit update, or an unrecorded window definition fails
the unit gate. The lexical `max_bytes` matches from the broad command are not
`max_by(...)` calls and are therefore not findings.

## Resolved active-route findings

| Path | Change |
| --- | --- |
| `jobs/realtime/viewed_bought.py` | Replaced truncate/reload with a validated, explicit-column, atomic snapshot while preserving the required `rundate`. |
| `jobs/nextads_data/archive_sort_order_data.py` | Replaced both delete/insert archives with validated atomic logical-date scope replacements. |
| `src/next_ads/delivery/google_sheets.py` | Removed arbitrary PLP deduplication and per-run history housekeeping; the logical-date, realm and territory history scope is replaced atomically before latest. |
| `jobs/nextads_control/load_control_sheet_v2.py` | Canonically sorts the page-type membership array. |
| `src/next_ads/control/load_control_sheet.py` | Canonically sorts the location membership array. |
| `src/next_ads/control/attributes.py` | Canonically sorts collected attribute values. |
| `src/next_ads/control/item_attributes.py` | Canonically sorts collected item-attribute values before the order-insensitive explode. |

No `F.rand(...)` or `sampleBy(...)` calls remain under `jobs/` or `src/`.

## Bundle entrypoints outside the source audit roots

All job and pipeline entrypoints under `jobs/` and `src/` are covered by the
source scan. The deployed analytics PCTR experiment is outside those roots and
outside the incident-bearing route; its direct bundle references are recorded
individually so a new experiment entrypoint cannot be added without review.

<!-- bundle-exclusions:start -->
| Path | Reachability | Disposition | Rationale and review boundary |
| --- | --- | --- | --- |
| `experiments/analytics_pctr/SQL/00_base_sessions.sql` | Deployed analytics experiment | Out of scope experiment | Analytics PCTR does not feed candidate or assignment publication; analytics-owner review is required. |
| `experiments/analytics_pctr/SQL/01_core_datasets.sql` | Deployed analytics experiment | Out of scope experiment | Analytics PCTR does not feed candidate or assignment publication; analytics-owner review is required. |
| `experiments/analytics_pctr/SQL/02_customer_advert_base-predictions.sql` | Deployed analytics experiment | Out of scope experiment | Analytics PCTR does not feed candidate or assignment publication; analytics-owner review is required. |
| `experiments/analytics_pctr/SQL/03_session_aggregation.sql` | Deployed analytics experiment | Out of scope experiment | Analytics PCTR does not feed candidate or assignment publication; analytics-owner review is required. |
| `experiments/analytics_pctr/SQL/04_ctr_metrics.sql` | Deployed analytics experiment | Out of scope experiment | Analytics PCTR does not feed candidate or assignment publication; analytics-owner review is required. |
| `experiments/analytics_pctr/SQL/05_page_views.sql` | Deployed analytics experiment | Out of scope experiment | Analytics PCTR does not feed candidate or assignment publication; analytics-owner review is required. |
| `experiments/analytics_pctr/SQL/06_purchases.sql` | Deployed analytics experiment | Out of scope experiment | Analytics PCTR does not feed candidate or assignment publication; analytics-owner review is required. |
| `experiments/analytics_pctr/SQL/07_customer_advert_exposure.sql` | Deployed analytics experiment | Out of scope experiment | Analytics PCTR does not feed candidate or assignment publication; analytics-owner review is required. |
| `experiments/analytics_pctr/SQL/08_view_advert_affinity.sql` | Deployed analytics experiment | Out of scope experiment | Analytics PCTR does not feed candidate or assignment publication; analytics-owner review is required. |
| `experiments/analytics_pctr/SQL/09_combining_data.sql` | Deployed analytics experiment | Out of scope experiment | Analytics PCTR does not feed candidate or assignment publication; analytics-owner review is required. |
| `experiments/analytics_pctr/run_predictions.py` | Deployed analytics experiment | Out of scope experiment | Analytics PCTR does not feed candidate or assignment publication; analytics-owner review is required. |
<!-- bundle-exclusions:end -->

## Remaining source findings

<!-- source-findings:start -->
| Path | Pattern | Count | Reachability | Disposition | Rationale and review boundary |
| --- | --- | ---: | --- | --- | --- |
| `jobs/features/nextads/preflight_checks.py` | `deduplicate` | 1 | Deployed feature foundation | Out of scope feature foundation | The distinct source-date check is outside the incident route; feature-platform owner review is required. |
| `jobs/nextads_assignment/build_page.py` | `deduplicate` | 1 | Active assignment route | Deterministic | The call projects and retains exact distinct assignment rows; conflicting assignment keys fail staging validation. |
| `jobs/nextads_candidates/build_targeting_scores.py` | `deduplicate` | 1 | Unbundled legacy | Out of scope | No deployed bundle resource references this older scorer; candidate-route owner review is required before reuse. |
| `jobs/nextads_candidates/build_targeting_scores.py` | `truncate_and_load` | 1 | Unbundled legacy | Out of scope | No deployed bundle resource references this older scorer; candidate-route owner review is required before reuse. |
| `jobs/nextads_candidates/build_theme_scores.py` | `collect_set` | 1 | Active Markov route | Deterministic | Used only to display a test account and never contributes to a persisted scoring result. |
| `jobs/nextads_candidates/build_theme_scores.py` | `deduplicate` | 5 | Active Markov route | Deterministic | Calls retain exact distinct event or business-key projections; total-order windows handle later single-row selection. |
| `jobs/nextads_candidates/build_theme_scores.py` | `saveAsTable` | 1 | Active Markov route | Deterministic | Run-unique temporary scorer materialisation uses `errorifexists` and is removed in `finally`; it is not a serving-table overwrite. |
| `jobs/nextads_candidates/conditional_probability_recs.py` | `collect_list` | 3 | Unbundled legacy | Out of scope | The legacy conditional-probability entrypoint is not referenced by a deployed bundle resource; candidate-route owner review is required before reuse. |
| `jobs/nextads_candidates/conditional_probability_recs.py` | `collect_set` | 3 | Unbundled legacy | Out of scope | The legacy conditional-probability entrypoint is not referenced by a deployed bundle resource; candidate-route owner review is required before reuse. |
| `jobs/nextads_candidates/conditional_probability_recs.py` | `deduplicate` | 7 | Unbundled legacy | Out of scope | The legacy conditional-probability entrypoint is not referenced by a deployed bundle resource; candidate-route owner review is required before reuse. |
| `jobs/nextads_candidates/conditional_probability_recs.py` | `delete_from_and_load` | 1 | Unbundled legacy | Out of scope | The legacy conditional-probability entrypoint is not referenced by a deployed bundle resource; candidate-route owner review is required before reuse. |
| `jobs/nextads_candidates/conditional_probability_recs.py` | `truncate_and_load` | 5 | Unbundled legacy | Out of scope | The legacy conditional-probability entrypoint is not referenced by a deployed bundle resource; candidate-route owner review is required before reuse. |
| `jobs/nextads_candidates/get_ad_items.py` | `deduplicate` | 1 | Unbundled legacy | Out of scope | No deployed bundle resource references this older item-retrieval entrypoint; candidate-route owner review is required before reuse. |
| `jobs/nextads_candidates/get_ad_items.py` | `delete_from_and_load` | 1 | Unbundled legacy | Out of scope | No deployed bundle resource references this older item-retrieval entrypoint; candidate-route owner review is required before reuse. |
| `jobs/nextads_cells/assign_customer_cells.py` | `deduplicate` | 3 | Active candidate route | Deterministic | Calls project exact control dates or full customer rows; stable hashing performs the subsequent allocation. |
| `jobs/nextads_control/load_control_sheet.py` | `deduplicate` | 1 | Active candidate route | Deterministic | The entrypoint retains exact distinct ad identifiers for warning logic and does not select between conflicting rows. |
| `jobs/nextads_control/load_control_sheet_v2.py` | `collect_set` | 1 | Active candidate route | Deterministic | The membership array is wrapped in `sort_array`; consumers use membership rather than input order. |
| `jobs/nextads_control/load_control_sheet_v2.py` | `deduplicate` | 7 | Active candidate route | Deterministic | All calls remove exact full-row duplicates or project distinct keys; conflicting rows remain and fail downstream key validation. |
| `jobs/nextads_control/parse_theme_mapping.py` | `deduplicate` | 1 | Active Markov route | Deterministic | The call projects exact distinct item-theme rows before validated publication. |
| `jobs/nextads_control/parse_attributes.py` | `overwrite_mode` | 1 | Active Markov route | External full snapshot | This is the optional BigQuery dashboard export after atomic Delta publication; dashboard-delivery owner review is required before changing its snapshot contract. |
| `jobs/nextads_delivery/build_v2_payload.py` | `collect_list` | 4 | Active delivery route | Deterministic | Fragment, trigger and page-type collections are canonically sorted before JSON and hash construction; dedicated payload tests enforce this. |
| `jobs/nextads_delivery/build_v2_payload.py` | `deduplicate` | 2 | Active delivery route | Deterministic | Both calls retain exact distinct split values or account-to-profile rows; they do not choose between conflicting payload records. |
| `jobs/nextads_delivery/build_v2_payload.py` | `overwrite_mode` | 1 | Active delivery route | External full snapshot | The post-publication CSV feed is intentionally a complete path replacement and does not mutate a Delta serving table. |
| `jobs/nextads_delivery/exclusions_export.py` | `collect_list` | 2 | Unbundled delivery | Out of scope | No deployed bundle resource references this Cosmos exclusions entrypoint; delivery-owner review is required before reuse. |
| `jobs/nextads_reporting/assignment_validation.py` | `collect_set` | 1 | Deployed read-only validation | Out of scope reporting | The set is used only for diagnostic validation after publication and is never written; reporting-owner review is required for output changes. |
| `jobs/nextads_reporting/assignment_validation.py` | `deduplicate` | 6 | Deployed read-only validation | Out of scope reporting | Exact distinct projections support post-publication diagnostics only; reporting-owner review is required before changing their semantics. |
| `jobs/nextads_reporting/realtime_results.py` | `deduplicate` | 2 | Deployed reporting | Out of scope reporting | Exact distinct reporting projections cannot affect candidate or assignment publication; reporting-owner review is required before semantic changes. |
| `jobs/nextads_reporting/realtime_results.py` | `delete_from_and_load` | 1 | Deployed reporting | Out of scope reporting | This independent reporting job is outside candidate and page publication; reporting-owner review is required before writer migration. |
| `jobs/nextads_reporting/realtime_results.py` | `truncate_and_load` | 1 | Deployed reporting | Out of scope reporting | This independent reporting job is outside candidate and page publication; reporting-owner review is required before writer migration. |
| `jobs/nextads_reporting/results_1.py` | `collect_set` | 1 | Deployed reporting | Out of scope reporting | The collection feeds reporting metrics rather than assignment selection; reporting-owner review is required before semantic changes. |
| `jobs/nextads_reporting/results_1.py` | `deduplicate` | 18 | Deployed reporting | Out of scope reporting | The deduplication is in reporting enrichment and cannot affect candidate or assignment publication. |
| `jobs/nextads_reporting/results_1.py` | `overwrite_mode` | 8 | Deployed reporting | Out of scope reporting | These are reporting and external-export snapshots; reporting-owner review is required before writer migration. |
| `jobs/nextads_reporting/results_2.py` | `deduplicate` | 3 | Deployed reporting | Out of scope reporting | Exact distinct reporting projections cannot affect the production-build route; reporting-owner review is required before semantic changes. |
| `jobs/nextads_reporting/results_2.py` | `overwrite_mode` | 4 | Deployed reporting | Out of scope reporting | These are reporting and external-export snapshots; reporting-owner review is required before writer migration. |
| `jobs/nextads_reporting/results_3.py` | `deduplicate` | 1 | Deployed reporting | Out of scope reporting | The exact distinct reporting projection cannot affect candidate or assignment publication; reporting-owner review is required before changes. |
| `jobs/nextads_reporting/results_3.py` | `overwrite_mode` | 1 | Deployed reporting | Out of scope reporting | This is a reporting snapshot outside candidate and assignment publication. |
| `jobs/nextads_reporting/results_agg.py` | `collect_set` | 6 | Deployed reporting | Out of scope reporting | Collections feed reporting aggregates rather than decisioning; reporting-owner review is required before canonicalisation. |
| `jobs/nextads_reporting/results_agg.py` | `deduplicate` | 4 | Deployed reporting | Out of scope reporting | Exact distinct reporting projections are outside the publication failure domain; reporting-owner review is required before semantic changes. |
| `jobs/nextads_reporting/results_agg.py` | `delete_from_and_load` | 11 | Deployed reporting | Out of scope reporting | The count includes active and commented legacy reporting references; the reporting job is outside the production-build failure domain. |
| `jobs/nextads_reporting/results_performance_checks.py` | `deduplicate` | 5 | Deployed reporting | Out of scope reporting | Exact distinct performance-check projections cannot gate publication; reporting-owner review is required before their semantics are changed. |
| `jobs/nextads_reporting/results_performance_checks.py` | `delete_from_and_load` | 1 | Deployed reporting | Out of scope reporting | This independent performance-reporting output cannot gate candidate or page publication. |
| `jobs/nextads_reporting/results_to_bigquery.py` | `overwrite_mode` | 1 | Deployed reporting | Out of scope reporting | The BigQuery reporting export is an intentional external snapshot. |
| `jobs/nextads_reporting/results_top_ads_by_location.py` | `truncate_and_load` | 1 | Deployed reporting | Out of scope reporting | The realtime top-ad reporting snapshot is outside the nightly publication failure domain. |
| `jobs/nextads_v2/build_page.py` | `deduplicate` | 1 | Active assignment route | Deterministic | The call retains exact distinct account-and-rank rows after union; complete-build staging subsequently validates assignment keys. |
| `jobs/realtime/viewed_bought.py` | `deduplicate` | 4 | Active realtime input | Deterministic | Exact distinct visit, item and account projections precede total-order ranking; the final atomic snapshot validates its business key. |
| `jobs/table_operations/calculate_table_sizes.py` | `delete_from_and_load` | 1 | Deployed monitoring | Out of scope operations | Table-size monitoring is operational reporting; operations-owner review is required before its writer is changed. |
| `jobs/table_operations/init_starting_tables.py` | `TRUNCATE TABLE` | 4 | Unbundled setup | Development only | This explicit setup utility is not a nightly bundle entrypoint and requires manual setup authority. |
| `jobs/table_operations/truncate_tables_in_dev.py` | `TRUNCATE TABLE` | 1 | Unbundled setup | Development only | This explicit DEV-only utility is not a production route. |
| `src/next_ads/control/attributes.py` | `collect_set` | 1 | Active Markov dependency | Deterministic | `sort_array` canonicalises the collected attribute set before publication. |
| `src/next_ads/control/control_sheet_audit.py` | `collect_list` | 1 | Active warning-only control audit | Deterministic | The diagnostic examples are total-ordered, capped and sorted before they are collected into the warning report. |
| `src/next_ads/control/control_sheet_audit.py` | `collect_set` | 3 | Active warning-only control audit | Deterministic | `sort_array` canonicalises raw-row signatures, processed scope membership and CMS target URLs before comparison. |
| `src/next_ads/control/item_attributes.py` | `collect_list` | 1 | Active Markov dependency | Deterministic | `sort_array` canonicalises values before an order-insensitive explode. |
| `src/next_ads/control/item_attributes.py` | `deduplicate` | 12 | Active Markov dependency | Deterministic | Calls retain exact distinct item, basket, attribute or full-row projections; none arbitrarily selects among conflicting values. |
| `src/next_ads/control/load_control_sheet.py` | `collect_set` | 1 | Active candidate dependency | Deterministic | `sort_array` canonicalises the membership array; consumers use only `array_contains`. |
| `src/next_ads/control/load_control_sheet.py` | `deduplicate` | 5 | Active candidate dependency | Deterministic | Calls remove exact full-row duplicates or project distinct keys; conflicting source rows remain visible to key validation. |
| `src/next_ads/control/theme_mapping.py` | `deduplicate` | 2 | Active Markov dependency | Deterministic | Exact distinct theme projections support validation and parsing; they do not choose one of multiple conflicting mappings. |
| `src/next_ads/control/theme_mapping_sync.py` | `deduplicate` | 1 | Active warning-only validation | Deterministic | The normalised comparison uses exact distinct rows so duplicates do not alter warning-only set differences. |
| `src/next_ads/data/sort_order/data_pull.py` | `deduplicate` | 2 | Active data-pull route | Deterministic | Calls project exact distinct CMS page IDs and ad delivery keys before external reads and validated archive publication. |
| `src/next_ads/data/validation/custom_checks.py` | `deduplicate` | 1 | Active data validation | Deterministic | Exact distinct invalid values are displayed for diagnostics only and cannot select or mutate output rows. |
| `src/next_ads/decisioning/assignment.py` | `deduplicate` | 8 | Active assignment dependency | Deterministic | Calls retain exact distinct customer, advert and allocation projections; total-order ranking and final key validation govern selection. |
| `src/next_ads/decisioning/assignment_publication.py` | `collect_set` | 1 | Active assignment publisher | Deterministic | `sort_array` canonicalises teaser tokens before the correction predicate. |
| `src/next_ads/decisioning/table_maintenance.py` | `OPTIMIZE` | 2 | Deployed maintenance | Maintenance only | This allowlisted noon job is outside the build failure domain and runs optimisation only on its weekly branch. |
| `src/next_ads/decisioning/table_maintenance.py` | `VACUUM` | 3 | Deployed maintenance | Maintenance only | This allowlisted noon job retains 168 hours and runs vacuum only on its weekly branch. |
| `src/next_ads/decisioning/table_maintenance.py` | `DELETE FROM` | 1 | Deployed maintenance | Maintenance only | Date retention is isolated in the maintenance job and does not execute during candidate or page publication. |
| `src/next_ads/delivery/google_sheets.py` | `overwrite_mode` | 1 | Active delivery dependency | External full snapshot | The ABFS feed is intentionally replaced after successful atomic history and latest table publication. |
| `src/next_ads/delivery/masid_handoff.py` | `deduplicate` | 2 | Active delivery handoff | Deterministic | Exact distinct run-date and location projections validate a complete published assignment snapshot before submission. |
| `src/next_ads/features/materialization.py` | `DELETE FROM` | 1 | Deployed feature foundation | Out of scope feature foundation | Point-in-time feature replacement is outside this incident route; feature-platform owner review is required before migration. |
| `src/next_ads/features/nextads_core.py` | `collect_list` | 1 | Deployed feature foundation | Out of scope feature foundation | Feature-map construction is outside the candidate-to-delivery route covered by this change. |
| `src/next_ads/features/nextads_core.py` | `deduplicate` | 15 | Deployed feature foundation | Out of scope feature foundation | Feature-contract deduplication is explicitly excluded from this incident fix; feature-platform owner review is required. |
| `src/next_ads/features/theme_affinity.py` | `deduplicate` | 7 | Deployed feature foundation | Out of scope feature foundation | Feature-contract deduplication is explicitly excluded from this incident fix; feature-platform owner review is required. |
| `src/next_ads/ranking/scoring.py` | `deduplicate` | 1 | Active Markov dependency | Deterministic | Exact distinct model names constrain the model lookup and never select one of multiple score records. |
| `src/next_ads/ranking/theme_affinity/clean_output.py` | `deduplicate` | 2 | Active future inference | Deterministic | Exact distinct penalty themes form set membership only; total-order windows govern all subsequent single-row selections. |
| `src/next_ads/ranking/theme_affinity/data_prep.py` | `deduplicate` | 4 | Deployed scoring foundation | Deterministic | Exact distinct projections establish the reusable account and theme spine once; validated business keys and total-order ranking govern later selection. |
| `src/next_ads/ranking/theme_affinity/data_prep.py` | `saveAsTable` | 2 | Deployed model pipeline | Out of scope model lifecycle | Intermediate model-pipeline table replacement is outside model retraining and promotion boundaries for this change. |
| `src/next_ads/ranking/theme_affinity/data_prep.py` | `overwrite_mode` | 1 | Deployed model pipeline | Out of scope model lifecycle | The complete intermediate model dataset is replaced as one Delta snapshot; model-owner review is required before migration. |
| `src/next_ads/ranking/theme_affinity/dlt_pipeline.py` | `deduplicate` | 1 | Deployed model pipeline | Out of scope model lifecycle | Exact full-row deduplication is inside the managed model pipeline; model-owner review is required before semantic changes. |
| `src/next_ads/ranking/theme_affinity/sense_check.py` | `saveAsTable` | 1 | Deployed model checks | Out of scope model lifecycle | This is a model sense-check summary snapshot and cannot affect candidate publication. |
| `src/next_ads/ranking/theme_affinity/sense_check.py` | `overwrite_mode` | 1 | Deployed model checks | Out of scope model lifecycle | This is a model sense-check summary snapshot and cannot affect candidate publication. |
| `src/next_ads/ranking/theme_affinity/sql/2_atbs_bythemes.sql` | `max_by` | 1 | Deployed model pipeline | Deterministic | The ordering struct contains date and timestamp; equal ordering structs produce the same timestamp value. |
| `src/next_ads/ranking/theme_affinity/sql/2_views_bythemes.sql` | `max_by` | 1 | Deployed model pipeline | Deterministic | The ordering struct contains date and timestamp; equal ordering structs produce the same timestamp value. |
| `src/next_ads/ranking/theme_affinity/training_data.py` | `deduplicate` | 1 | Future model training | Deterministic | The call removes exact full-row duplicates from a two-column stratum lookup; candidate sampling already uses stable hashes. |
| `src/next_ads/ranking/theme_coverage.py` | `deduplicate` | 1 | Active warning-only validation | Deterministic | Exact distinct theme values are compared only for diagnostic coverage and do not gate candidate publication. |
| `src/next_ads/ranking/theme_score_eligibility.py` | `deduplicate` | 2 | Active candidate dependency | Deterministic | Exact distinct advert identifiers are counted before and after eligibility filtering; no assignment row is selected. |
| `src/next_ads/ranking/theme_score_generation.py` | `deduplicate` | 3 | Active Markov dependency | Deterministic | Each dataframe is projected to the required key columns, so only identical rows or distinct business keys are retained. |
| `src/next_ads/ranking/theme_score_retrieval.py` | `deduplicate` | 2 | Active candidate dependency | Deterministic | Exact distinct theme-ad and audience-group projections define eligibility sets; later validation rejects conflicting assignment keys. |
| `src/next_ads/realtime/decisioning/advert_affinity_data_build.py` | `collect_set` | 1 | Active realtime dependency | Deterministic | `sort_array` canonicalises the item membership array. |
| `src/next_ads/realtime/decisioning/advert_affinity_data_build.py` | `deduplicate` | 16 | Active realtime dependency | Deterministic | Calls use exact distinct products, visits, adverts or validation keys; total-order windows govern persisted single-row selections. |
| `src/next_ads/realtime/decisioning/advert_affinity_data_build.py` | `saveAsTable` | 4 | Active realtime dependency | Atomic Delta snapshot | Each whole-table overwrite is one Delta transaction after deterministic selection and quality checks; changing schemas or adding validation actions would increase this separate job's runtime. |
| `src/next_ads/realtime/decisioning/advert_affinity_data_build.py` | `overwrite_mode` | 4 | Active realtime dependency | Atomic Delta snapshot | Each whole-table overwrite is one Delta transaction after deterministic selection and quality checks; changing schemas or adding validation actions would increase this separate job's runtime. |
| `src/next_ads/realtime/unknown.py` | `collect_list` | 1 | Active realtime consumer | Deterministic | `sort_array` canonicalises location and MASID structs before `map_from_entries`. |
| `src/next_ads/realtime/unknown.py` | `deduplicate` | 1 | Active realtime consumer | Deterministic | Exact distinct locations define a membership loop; canonical map construction makes the delivered lookup order stable. |
| `src/next_ads/reporting/autotrading.py` | `deduplicate` | 3 | Deployed reporting | Out of scope reporting | Exact distinct campaign and advert projections cannot affect candidate or assignment publication; reporting-owner review is required. |
| `src/next_ads/reporting/results.py` | `deduplicate` | 5 | Deployed reporting | Out of scope reporting | Exact distinct reporting projections cannot affect the production-build route; reporting-owner review is required before semantic changes. |
<!-- source-findings:end -->

## Source-context fingerprints

These fingerprints bind the reviewed findings above to their normalised
five-line source contexts without relying on line numbers. A finding moved or
replaced inside the same file must therefore be reviewed and the fingerprint
updated, even when the per-file pattern count is unchanged.

<!-- source-context-fingerprints:start -->
| Path | Fingerprint |
| --- | --- |
| `jobs/features/nextads/preflight_checks.py` | `10f99e2382863fd4` |
| `jobs/nextads_assignment/build_page.py` | `e0c727e1591e9a37` |
| `jobs/nextads_candidates/build_targeting_scores.py` | `8c7c6abaf90cdf22` |
| `jobs/nextads_candidates/build_theme_scores.py` | `13a659d3a45793db` |
| `jobs/nextads_candidates/conditional_probability_recs.py` | `1345f38a460ac2bc` |
| `jobs/nextads_candidates/get_ad_items.py` | `25a2eeb377ed8ddb` |
| `jobs/nextads_cells/assign_customer_cells.py` | `84cb7f3eb5dcee6b` |
| `jobs/nextads_control/load_control_sheet.py` | `5b9fbaef2d99f803` |
| `jobs/nextads_control/load_control_sheet_v2.py` | `b1f449c3ef9bddb1` |
| `jobs/nextads_control/parse_attributes.py` | `b4bc8088f77e8504` |
| `jobs/nextads_control/parse_theme_mapping.py` | `5334b45a2f79bdf5` |
| `jobs/nextads_delivery/build_v2_payload.py` | `b1791f7d919529a3` |
| `jobs/nextads_delivery/exclusions_export.py` | `d44e9807ec8d1818` |
| `jobs/nextads_reporting/assignment_validation.py` | `056cab90367c7e4e` |
| `jobs/nextads_reporting/realtime_results.py` | `a9b649c5327f6422` |
| `jobs/nextads_reporting/results_1.py` | `1ed2887c6a78df2e` |
| `jobs/nextads_reporting/results_2.py` | `2f2f2094a3cd6bea` |
| `jobs/nextads_reporting/results_3.py` | `26162d1c18b6ecb8` |
| `jobs/nextads_reporting/results_agg.py` | `c9ce78ce0ecd4782` |
| `jobs/nextads_reporting/results_performance_checks.py` | `10a34a8c3387c140` |
| `jobs/nextads_reporting/results_to_bigquery.py` | `d8af87610032b987` |
| `jobs/nextads_reporting/results_top_ads_by_location.py` | `3c8094e80fa32b15` |
| `jobs/nextads_v2/build_page.py` | `937fde5c184cab74` |
| `jobs/realtime/viewed_bought.py` | `d05110b43f9419ec` |
| `jobs/table_operations/calculate_table_sizes.py` | `d84fa6085af0ddb1` |
| `jobs/table_operations/init_starting_tables.py` | `6b9905a406c99e49` |
| `jobs/table_operations/truncate_tables_in_dev.py` | `555f325c2b6cbca4` |
| `src/next_ads/control/attributes.py` | `088a006e8929497f` |
| `src/next_ads/control/control_sheet_audit.py` | `9d0a0dfcce07fc4f` |
| `src/next_ads/control/item_attributes.py` | `26f5078a2a341633` |
| `src/next_ads/control/load_control_sheet.py` | `3a83ff5435b18af5` |
| `src/next_ads/control/theme_mapping.py` | `fcbd620481b89b30` |
| `src/next_ads/control/theme_mapping_sync.py` | `f3de2421f3365b69` |
| `src/next_ads/data/sort_order/data_pull.py` | `94a56daae56851ca` |
| `src/next_ads/data/validation/custom_checks.py` | `d29043e49e31db92` |
| `src/next_ads/decisioning/assignment.py` | `d258bdb77fc54dc3` |
| `src/next_ads/decisioning/assignment_publication.py` | `bee35828dd6d7c61` |
| `src/next_ads/decisioning/table_maintenance.py` | `295aed008767ece0` |
| `src/next_ads/delivery/google_sheets.py` | `3b8f3f9bd142852a` |
| `src/next_ads/delivery/masid_handoff.py` | `0e5737e9b5a0eece` |
| `src/next_ads/features/materialization.py` | `3a1b04a869a8f0d6` |
| `src/next_ads/features/nextads_core.py` | `c0c506a3d561cf00` |
| `src/next_ads/features/theme_affinity.py` | `29132ead649f42f5` |
| `src/next_ads/ranking/scoring.py` | `e366917953536f92` |
| `src/next_ads/ranking/theme_affinity/clean_output.py` | `bb70272c46cf2860` |
| `src/next_ads/ranking/theme_affinity/data_prep.py` | `e6d4a8cd497e8285` |
| `src/next_ads/ranking/theme_affinity/dlt_pipeline.py` | `5d25f3006ed9e7e3` |
| `src/next_ads/ranking/theme_affinity/sense_check.py` | `17c5bc37cd36e7b1` |
| `src/next_ads/ranking/theme_affinity/sql/2_atbs_bythemes.sql` | `68cad891323c39a3` |
| `src/next_ads/ranking/theme_affinity/sql/2_views_bythemes.sql` | `1ef162775f5feac5` |
| `src/next_ads/ranking/theme_affinity/training_data.py` | `3ef68e2984dea861` |
| `src/next_ads/ranking/theme_coverage.py` | `ab47912564724448` |
| `src/next_ads/ranking/theme_score_eligibility.py` | `0b602bd2413b78bb` |
| `src/next_ads/ranking/theme_score_generation.py` | `d98d95ac568f999a` |
| `src/next_ads/ranking/theme_score_retrieval.py` | `1047e3c155495bd3` |
| `src/next_ads/realtime/decisioning/advert_affinity_data_build.py` | `b480d63a3d27364a` |
| `src/next_ads/realtime/unknown.py` | `561ac1a518a6d385` |
| `src/next_ads/reporting/autotrading.py` | `39a4095c9eab3cd5` |
| `src/next_ads/reporting/results.py` | `47421bb65775c1e8` |
<!-- source-context-fingerprints:end -->

## Ordered-window review

The table records every direct `Window...orderBy(...)` construction under
`jobs/` and `src/`. Windows without ordering are aggregation-only and are not
selection windows. Exact single-row selections on the active route are also
covered by the focused determinism tests.

<!-- window-findings:start -->
| Path | Count | Reachability | Disposition | Rationale and review boundary |
| --- | ---: | --- | --- | --- |
| `jobs/nextads_candidates/build_theme_scores.py` | 2 | Active Markov route | Total order | Latest item and customer ranking windows finish with stable business keys. |
| `jobs/nextads_candidates/conditional_probability_recs.py` | 2 | Unbundled legacy | Out of scope | The entrypoint is not deployed; candidate-route owner review is required before reuse. |
| `jobs/nextads_reporting/realtime_results.py` | 2 | Deployed reporting | Total order | Latest update and action windows finish with stable hash-backed business keys. |
| `jobs/nextads_reporting/results_top_ads_by_location.py` | 1 | Deployed reporting | Total order | The single-row selection finishes with `UniqueAdID`. |
| `jobs/orchestration/prepare_score_provider_context.py` | 1 | Active scoring-provider route | Total order | Accepted input attempts finish with execution count, completion timestamp and task run ID before the stable snapshot identifier. |
| `jobs/orchestration/prepare_scoring_foundation_context.py` | 1 | Active scoring-foundation route | Total order | Accepted input attempts finish with execution count, completion timestamp and task run ID before the stable snapshot identifier. |
| `jobs/realtime/viewed_bought.py` | 2 | Active realtime input | Total order | Revenue and association ranks finish with item identifiers. |
| `src/next_ads/control/control_sheet_audit.py` | 1 | Active warning-only control audit | Total order | Capped diagnostic examples are ordered by their unique rendered example key before aggregation. |
| `src/next_ads/control/theme_mapping.py` | 2 | Active Markov dependency | Tie preserving | Both use `dense_rank`; equal scoring inputs intentionally share a rank and no arbitrary single row is selected. |
| `src/next_ads/decisioning/assignment.py` | 12 | Active assignment dependency | Total order | Exact assignment selections finish with stable hash ordering and `UniqueAdID`; focused allocation tests cover repartitioning. |
| `src/next_ads/features/nextads_core.py` | 2 | Deployed feature foundation | Out of scope | Feature-foundation ranking is outside this incident route; feature-platform owner review is required. |
| `src/next_ads/features/theme_affinity.py` | 2 | Deployed feature foundation | Total order | Feature ranks finish with the theme key, but feature-foundation output validation remains separately owned. |
| `src/next_ads/ranking/provider_signals.py` | 1 | Active scoring-provider route | Total order | Provider ranking finishes with the entity identifier, giving identical scores a stable and replay-safe order. |
| `src/next_ads/ranking/theme_affinity/clean_output.py` | 2 | Active future inference | Total order | Penalty and final ranks finish with theme identifiers and stable hashes. |
| `src/next_ads/ranking/theme_affinity/sense_check.py` | 1 | Deployed model checks | Total order | Top-theme comparison finishes with `NextTheme`. |
| `src/next_ads/ranking/theme_affinity/spark_model.py` | 3 | Future model training | Total order | Ranking and score-bin windows use stable business-key ordering; model promotion is outside this change. |
| `src/next_ads/ranking/theme_affinity/training_data.py` | 4 | Future model training | Total order | Sampling and cap windows use stable hashes followed by candidate keys. |
| `src/next_ads/ranking/theme_score_eligibility.py` | 4 | Active Markov dependency | Total order | Theme and customer fallback ranks finish with theme or account keys. |
| `src/next_ads/ranking/theme_score_generation.py` | 2 | Active Markov dependency | Total order | Latest and top-theme ranks finish with theme identifiers. |
| `src/next_ads/ranking/theme_score_ranking.py` | 3 | Active candidate dependency | Total order | FM and per-ad selections finish with stable business keys and `UniqueAdID`. |
| `src/next_ads/realtime/decisioning/advert_affinity_data_build.py` | 3 | Active realtime dependency | Total order | Product and advert-affinity selections finish with product or advert identifiers. |
<!-- window-findings:end -->

## SQL-window review

SQL files and dynamic SQL strings are scanned separately from PySpark window
objects. Ranking windows finish with stable business keys. Aggregate and
percentile windows either preserve ties deliberately or have downstream
results that are invariant to equal ordering values.

<!-- sql-window-findings:start -->
| Path | Count | Reachability | Disposition | Rationale and review boundary |
| --- | ---: | --- | --- | --- |
| `src/next_ads/ranking/theme_affinity/data_prep.py` | 1 | Deployed model pipeline | Total order | The simple-rules `ROW_NUMBER` finishes with `theme_clean`. |
| `src/next_ads/ranking/theme_affinity/sql/1a_vatb.sql` | 1 | Active future inference | Total order | The VATB rank finishes with `theme_clean2`. |
| `src/next_ads/ranking/theme_affinity/sql/2_atbs_bythemes.sql` | 2 | Active future inference | Total order | Both recency ranks finish with `theme_clean` after date, timestamp and frequency. |
| `src/next_ads/ranking/theme_affinity/sql/2_baskets_bythemes.sql` | 1 | Active future inference | Total order | The basket recency rank finishes with `theme_clean`. |
| `src/next_ads/ranking/theme_affinity/sql/2_repurchased.sql` | 1 | Active future inference | Tie invariant | Equal order dates are identical ordering values; only gaps over 60 days survive, so equal-date row order cannot change the aggregate gap result. |
| `src/next_ads/ranking/theme_affinity/sql/2_views_bythemes.sql` | 2 | Active future inference | Total order | Both recency ranks finish with `theme_clean` after date, timestamp and frequency. |
| `src/next_ads/ranking/theme_affinity/sql/4_customer_segments.sql` | 3 | Active future inference | Tie preserving | `percent_rank` intentionally assigns equal spend values the same percentile and bucket; adding an account tie-breaker would change that contract. |
<!-- sql-window-findings:end -->

## Release boundary

The out-of-scope reporting, feature-foundation, model-lifecycle, setup and
unbundled legacy rows are recorded so they cannot silently gain new findings.
They are not candidates for opportunistic cleanup in this release. Any later
activation or migration requires the named route owner, focused equivalence
evidence and a new audit disposition.

# Lean, fast and atomic NextAds DEV acceptance

## Runtime contract

The critical route follows one rule:

> Validate cheap metadata first, calculate once in Spark, commit once to Delta, record the exact commit, then publish READY last.

Large outputs are not single-partition writes. Foundation, provider, candidate and assignment frames remain distributed across the Databricks workers. “One write” means one atomic Delta transaction for a logical output, with no `coalesce(1)`, driver collection or repeated full-table checksum scan.

Theme Affinity physically publishes the ranked foundation once. Prediction reads that exact Delta version and publishes provider signals once. Candidate generation computes a distinct provider once, writes one ad-set slice and one score slice, then publishes its READY row. V1 and v2 assignment each use one bulk Spark graph, one grouped final-key check, one dated-history transaction and one live-latest transaction. The live-latest transaction is the final mandatory operation.

The public assignment and payload schemas and all existing eligibility, suppression, feedback, exposure, allocation and delivery behaviour remain unchanged. Markov remains shadow-only. Compatibility tables and heavyweight sense checks run asynchronously from exact READY versions.

## Existing cluster envelope

No larger clusters are required by this change.

- Theme Affinity publication and prediction use the existing D32 autoscaling cluster with one to four workers. Prediction is spread by account across 128 partitions so work can occupy all 128 worker vCPUs at maximum scale, while Arrow batches are capped at 10,000 rows to bound Python-worker memory. The ranked frame feeds prediction directly; there is no second complete-table copy or reread.
- Markov uses the existing D32 one-to-four-worker cluster in one task. The scorer lineage is passed directly to the provider writer without a temporary Delta table.
- V1 and v2 assignment use the existing fixed four-worker D32 Photon cluster. Shared cells, control and candidate inputs are persisted with spill support. The final v1 frame is spread across 2,048 scope/account partitions and v2 across 512. Adaptive execution may coalesce small shuffle partitions, while `maxRecordsPerFile=1000000` prevents oversized output files.
- Only grouped scope/key summaries and one-row typed manifests return to the driver. The full customer, score, candidate and assignment frames never do.

DEV acceptance must capture peak executor memory, spill, skewed tasks, output file sizes and autoscaling events. A run fails this acceptance if it succeeds only after increasing any cluster beyond this envelope.

## Personal-schema table operations

Use the deployed `mktg_next_uk_nextads_table_operations` job against the named DEV schema. Do not create or alter the schema itself, copy PROD tables, or ask `table_operations` to rebuild a large canonical table.

First run `recreate_tables` only for these small PR-owned manifest tables:

- `scoring_input_snapshots`
- `scoring_input_snapshot_sources`
- `scoring_foundation_outputs`
- `scoring_foundation_builds`
- `score_provider_builds`
- `candidate_foundation_builds`
- `candidate_builds`
- `assignments_build_staging`
- `assignments_v2_build_staging`
- `assignment_build_events`

Set `client=next_uk`, `job_env=dev`, the repository-configured DEV catalog and named schema, `tables` to the comma-separated list above, `confirm_destructive=true`, and `dry_run=false`. These tables are deliberately recreated without backup copies because they are small internal manifests that the acceptance run repopulates.

Then run `alter_tables` for the existing large canonical tables below:

- `score_provider_signals`
- `candidate_scores`
- `assignments_v2`
- `assignments_v2_latest`

Set `confirm_mutating=true` and `dry_run=false`. This adds only the repo-owned finite-score/rank checks in place. It must not create a backup or copy the table. Use `create_missing_tables` with the same narrow `tables` list only when one of these PR-owned tables does not yet exist.

Review the planned SQL in the task log before allowing each mutating run. The log must contain no `__backup_` table and no `CREATE SCHEMA`, and large tables must not be recreated.

## Repository gates

The local suite guards:

- typed schemas for every nullable manifest/context row, including the `CANNOT_DETERMINE_TYPE` regression;
- exact Delta receipt lookup by build and repair-attempt identity;
- repair after a data commit without another data action or data write;
- one ranked-foundation write, one provider-signal write, and one write per canonical candidate table;
- one provider calculation when champion and challenger bind the same build;
- the absence of scope scripts, per-scope replacement writes and intermediate Markov `saveAsTable` materialisation;
- stable identities, tie-breaking and partition/retry determinism;
- no critical-path whole-row JSON hash, `countDistinct`, post-write full scan, `coalesce(1)` or driver-side materialisation.

Spark-backed parity tests run in Databricks. They compare the bulk output with the previous route for all 79 v1 locations and all five v2 page types using bidirectional `EXCEPT ALL`.

## Three clean DEV cycles

Run three complete DEV cycles with the same pinned input Delta versions, configuration, model version and Git commit. Retain job/run/task IDs, repair attempts, Delta versions, receipt IDs, cluster metrics and DBU.

For every cycle:

1. Run Theme Inputs and Candidate Foundation in parallel.
2. After Theme Inputs is READY, run Theme Affinity. Markov may run in parallel or later; it is shadow-only and does not block candidates.
   Markov reads an exact version of the existing production transition model and writes its results only to the named DEV schema, so the personal DEV transition table does not need to be seeded before this acceptance run.
3. After Candidate Foundation, the accepted provider and the audited route control version are ready, run v1 and v2 candidate generation in parallel.
4. Run the v1 and v2 bulk page jobs in parallel from their exact accepted candidate attempts.
5. Let v1 continue to MASID and PLP delivery and v2 continue to payload export.
6. Run provider compatibility at 17:00 and candidate compatibility/quality at 21:00, or trigger those independent jobs manually against the exact READY versions when testing the route earlier in the day.

Compare provider signals, candidate scores/ad sets, v1 assignments, v2 assignments and payloads across all three cycles with bidirectional `EXCEPT ALL`. Every comparison must return zero rows. Confirm all 79 v1 scopes and all five v2 page types are present and public schemas contain no internal receipt or provenance fields.

## Performance acceptance

All of the following must hold:

- Theme Affinity critical-path median is at most 120 minutes and no run exceeds 135 minutes.
- Ranked foundation publication is at most 15 minutes.
- Receipt and manifest overhead is below two minutes per build.
- Repair from an already committed physical output to READY is below five minutes and performs no second data write.
- Candidate plus assignment median is at least 50% below the verified pre-PR per-scope baseline.
- Total DBU is no higher than the verified pre-PR median.
- No stage has one output partition, a driver-side full-frame action, sustained executor OOM, pathological spill, or a single skewed task dominating runtime.

## Failure and repair acceptance

Inject failures after foundation, provider and candidate data commits but before their READY manifest. Repair must reuse the exact receipt/version and write only the typed READY metadata.

For a Theme Affinity repair, select `prepare_foundation_context` and `publish_and_score`, and leave `predict_data_prep` unselected. Preparation reactivates the same failed foundation attempt after checking the Lakeflow build marker. Publication then reuses the ranked-foundation receipt. If the provider signals transaction had also committed, the stable provider attempt finds that receipt before model loading, prediction, penalty calculation or any large input-table action, and writes only the typed READY manifest. Markov uses the same receipt-first repair rule inside its single task.

Inject assignment failures before history and before live latest. A failure must not advance live latest; yesterday’s complete snapshot remains active. If history committed, repair must publish live latest from that exact history version without rebuilding assignments. No mandatory validation, metadata write or logging transaction may occur after live latest advances.

Compatibility and monitoring failures must alert independently without revoking a canonical READY build. V1 and v2 failure injection must also prove that the healthy sibling route can complete.

DEV acceptance is complete only when the three-cycle equality, runtime, DBU, cluster-envelope and injected-failure evidence is attached for review.

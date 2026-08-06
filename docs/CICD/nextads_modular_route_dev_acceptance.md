# NextAds modular route acceptance

## What this foundation enables

The nightly route now has interchangeable provider, portfolio, candidate and assignment boundaries. A new challenger does not need its own assignment process. The practical integration path is:

1. Build the model output and adapt it to the canonical score-provider columns.
2. Validate the output keys, values, ranks and checksum, then publish the exact provider attempt and Delta version with the ready manifest written last.
3. Declare the provider in a portfolio policy as a serving, challenger or evaluation entry. The portfolio binds an exact provider attempt; undeclared runtime overrides are rejected.
4. Build the standard top-20 candidates for that portfolio entry and validate the candidate and ad-set outputs. The candidate ready manifest is written last.
5. Pass the accepted candidate attempt into the ordinary v1 or v2 assignment route. The page jobs resolve each public serving slot independently, validate every required scope and publish the complete assignment snapshot before delivery starts.

The default policy still maps both `best` and `best_challenger` to Theme Affinity, so the new boundaries do not themselves change customer traffic. Markov remains a shadow provider. Multiple simultaneous challenger traffic allocation is not active; enabling that needs a separately reviewed allocation policy and experiment design.

## Runtime and transaction contract

Provider, portfolio and candidate data rows are written before their accepted manifest. Assignment scopes are staged before the complete build is published. A partial or failed attempt therefore remains unselectable and cannot advance a live assignment or delivery table.

Atomicity means one Delta transaction per table, followed by a manifest-last logical commit across related tables. It does not mean `BEGIN ATOMIC`. On DBR 15.4, the write helper orders source columns to the target schema before issuing target-ordered `REPLACE WHERE`; it does not use unsupported combined `BY NAME` syntax.

The independent maintenance job runs at 05:00 Europe/London and is not on the nightly build dependency path. No public-preview Lakeflow metadata is required: supported job and pipeline identifiers are recorded, while the accepted physical Delta publication and version form the downstream data binding. Feature compatibility remains separate from the nightly route.

## Repository acceptance gates

The unit suite protects the following behaviours:

- deterministic provider, candidate and assignment output at one, four and eight Spark partitions;
- provider, portfolio and candidate ready manifests are written last;
- a missing Markov shadow build does not block serving;
- a missing required Theme Affinity build prevents the affected route from reaching candidate or assignment publication;
- provider attempts, table versions, input snapshots, portfolio attempts, candidate attempts, customer-cell versions and assignment build IDs cross job boundaries explicitly;
- repaired attempts resolve deterministically and cannot mix their staging rows with another attempt;
- v1 and v2 failures block only the affected route;
- business control-sheet findings remain warning-only, while malformed inputs or audit execution failures stop the affected route;
- assignment configuration cannot request a candidate rank above 20;
- page and delivery code cannot reintroduce mutable latest-model or latest-candidate reads;
- the active candidate-to-delivery route cannot reintroduce random sampling, arbitrary `dropDuplicates`, or delete/truncate publication writers.

The broader stability audit records any remaining exact-distinct operations and other sensitive constructs with their reachability and rationale. Adding or moving one changes the guarded audit and requires an explicit review.

## DEV evidence to collect before merge

Run this only in DEV after completing the clean personal-schema table setup in
`nextads_databricks_job_settings.md`. That setup recreates every feature-owned
modular table, then creates any other missing configured table without running
a broad alter or copying large tables into backups.

1. Run three complete cycles with identical pinned inputs and retain the job run IDs, task attempts and table Delta versions.
2. Compare provider signals, candidate scores/ad sets, v1 assignments, v2 assignments and payload output between the three cycles with bidirectional `EXCEPT ALL`. Every comparison must return zero rows.
3. Compare the default Theme Affinity compatibility outputs with the accepted modular outputs. Record any deliberately excluded metadata columns; all customer-facing rows must match.
4. Confirm all 79 v1 scopes and all five v2 page types completed, and confirm the public assignment schemas did not gain internal provenance columns.
5. Confirm stable customer-cell rows and versions are identical across the cycles.
6. Confirm the selected portfolio names Theme Affinity for `best` and `best_challenger`, while Markov is present only as an evaluation entry and has no serving allocation.
7. Inject one failure at each provider, portfolio, candidate and assignment-scope boundary. After each failure, verify that no downstream ready manifest or live table advanced. Repeat one failed task as a repair and verify that only the repaired attempt is selected.
8. Fail v1 and v2 independently and prove the healthy sibling route still completes.
9. Record candidate and page-task durations and DBU consumption. Compare the median with the pre-bulk baseline: no major stage may regress by more than 5%, total DBU use may not regress by more than 10%, and the run must show that the former 77+2+5 per-scope cluster-start pattern is absent.

DEV acceptance is complete only when the three-cycle comparisons, injected-failure checks and runtime/DBU evidence have been attached to the change review. PREPROD and PROD validation are outside this feature checkpoint.

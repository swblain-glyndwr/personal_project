# NextAds Repo Structure

This page describes the target repo structure introduced by story 5111656.
The first implementation creates the target package home without moving
production job entry points or changing production behaviour.

## Target Layout

```text
next-ads/
  src/           # reusable production package code
  pipelines/     # process-flow definitions, if introduced later
  jobs/          # Databricks entry points, if introduced later
  configs/       # settings and policies
  sql/           # table, view, and reporting SQL
  experiments/   # safe exploration
  docs/          # team and AI context
  tests/         # confidence checks
  deployment/    # release setup, if introduced later
```

## Package Layout

Reusable production code should move toward:

```text
src/
  next_ads/
    common/       # shared utilities used across the repo
    data/         # data contracts, features, labels, and datasets
    control/      # control sheet, ad metadata, and eligibility
    retrieval/    # creates the pool of ads that could be considered
    ranking/      # scores or orders candidate ads
    decisioning/  # applies rules and selects final ads
    delivery/     # prepares outputs for downstream systems
    reporting/    # reusable reporting and diagnostics logic
    realtime/     # real-time adjustment logic and contracts
```

## Current Transition Rules

- `src/next_ads` is the future home for reusable production package code.
- Existing Databricks job entry points remain in `scripts/` for now.
- Existing Databricks job definitions remain in `resources/jobs/` for now.
- The current `config/` folder is not renamed in this first slice.
- Existing imports from the top-level `next_ads` package must keep working.
- Decision-affecting logic should move only in follow-up stories with output
  equivalence checks.
- Databricks job entry-point changes should be handled separately from this
  foundation story.

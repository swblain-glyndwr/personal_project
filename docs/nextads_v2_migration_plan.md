# NextAds v2 Migration Plan

This plan documents the current TL v2 branch direction. The route files have
been moved out of `scripts/` into route-oriented job folders, but the v2 runtime
logic and output contracts remain unchanged.

## Current Rule

Current v2 runtime paths:

- `jobs/nextads_control/load_control_sheet_v2.py`
- `jobs/nextads_candidates/build_page_type_candidates_v2.py`
- `jobs/nextads_v2/build_page.py`
- `jobs/nextads_delivery/build_v2_payload.py`
- existing v2 DAB `python_file` references now point at these route folders

Do not rewrite v2 behaviour until the active TL branches are reconciled.

## TL Branch Findings

Latest inspected TL branches:

| Branch | Latest observed commit | Direction |
| --- | --- | --- |
| `origin/feature/TL/v2exclusions` | `9af4d25` on 2026-06-30 | Adds exclusions/Cosmos export work and linting on top of an older mixed layout. |
| `origin/feature/TL/v2_exclusions` | `de1a0dc` on 2026-06-25 | Similar exclusions route; includes older config/path assumptions. |
| `origin/feature/TL/datapullv2` | `22fb8a1` on 2026-06-25 | Data-pull v2 with Dynaconf work in progress. |
| `origin/feature/TL/datapull_v2` | `255ea4d` on 2026-06-24 | Earlier data-pull v2 settings work. |
| `origin/feature/TL/cms_data_load` | `d3f23dd` on 2026-06-30 | Adds CMS data-load script direction. |
| `origin/feature/TL/control_app` | `a0f65ce` on 2026-06-24 | Control app planning/work-in-progress branch. |
| `origin/feature/TL/control_sheet_v2` | `1da37c1` on 2026-05-15 | Adds v2 control-sheet validation schema direction. |
| `origin/feature/TL/nextadsv2_deploy_control_sheet_v2_task` | `350e707` on 2026-05-19 | Adds/deploys v2 control-sheet task direction. |
| `origin/feature/TL/e2e_pipeline` | `2c5c716` on 2026-06-02 | Adds ability to serve NextGenAds end to end. |
| `origin/feature/TL/real_time_v2` | `8cc2c62` on 2026-06-05 | Adds MASID/Bloomreach real-time v2 client direction. |

Several branches pre-date the current `src/`, `jobs/`, `configs/`, and grouped
`sql/` layout. Do not merge any branch wholesale into the restructure branch.
Cherry-pick or port the v2 changes after the restructure lands.

## Deferred Target Shape

Once non-v2 restructure is complete, move v2 as a dedicated PR:

```text
jobs/nextads_v2/
src/next_ads/control/adsv2/
src/next_ads/ranking/adsv2/
src/next_ads/decisioning/adsv2/
src/next_ads/delivery/adsv2/
configs/adsv2/
sql/adsv2/
```

Keep v2 output table names, config keys, and downstream payload contracts stable
until a separate compatibility/cutover plan is approved.

## Suggested v2 Move Order

1. Re-check active TL branches and open PRs.
2. Port v2 control-sheet schema/load changes into `src/next_ads/control/adsv2`
   and `jobs/nextads_v2/load_control_sheet.py`.
3. Port v2 score/ad mapping into `src/next_ads/ranking/adsv2` and
   `jobs/nextads_v2/map_theme_scores_to_ads.py`.
4. Port v2 page/payload work into `src/next_ads/decisioning/adsv2`,
   `src/next_ads/delivery/adsv2`, and matching `jobs/nextads_v2` entrypoints.
5. Add output contract checks comparing v1/v2 where relevant.
6. Update DAB paths only after DEV Integration validation is ready.

## Validation Expected

- v2 config load tests for DEV and DEV Integration.
- v2 control-sheet schema tests.
- v2 output table and payload contract checks.
- DAB validate for DEV and DEV Integration.
- PREPROD validation only when the v2 route is release-candidate.

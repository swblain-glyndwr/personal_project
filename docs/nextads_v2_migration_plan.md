# NextAds v2 Migration Record

> Historical design record. This page records the route-migration decisions and the TL branches reviewed during that work. For the current running design, start with [NextAds job and table flow](architecture/nextads_job_table_flow.md) and use [V1/V2 parallel route](architecture/v1_v2_parallel_route.md) for exact task dependencies.

The long-lived decision remains that V1 and V2 run beside one another. The route files have moved out of `scripts/` into route-oriented job folders, and candidate mapping splits at the control-sheet join because the two routes have different placement grains.

## Current Decision

V2 is not a full replacement for V1. Home Page remains on the V1 location-based route, while newer page-type assignments use V2. Both routes use the same accepted customer-state inputs and selected score outputs. Candidate mapping splits by route because V1 reads `control_sheet_latest` with `Location`, while V2 reads `control_sheet_latest_v2` with `PageType`.

The V2 workbook is the operational source for the `Theme Mapping` tab. During the earlier scoring-input operation, the job lands that V2 mapping directly and uses it to build the accepted item-to-theme input. It also compares the copied V1 tab and reports differences, but the comparison is warning-only and does not replace the V2 source or stop candidate building.

## Current Rule

Current v2 runtime paths are already route-oriented and should remain stable unless a specific file is clearly misplaced:

- `jobs/nextads_control/load_control_sheet_v2.py`
- `jobs/nextads_candidates/build_page_type_candidates_v2.py`
- `jobs/nextads_v2/build_page.py`
- `jobs/nextads_delivery/build_v2_payload.py`
- existing v2 DAB `python_file` references now point at these route folders

Do not move all V2 files into `jobs/nextads_v2/` by default; `jobs/nextads_control`, `jobs/nextads_candidates`, and `jobs/nextads_delivery` are valid homes when the route role is clearer. The V2 candidate task maps the exact selected score output to adverts from the captured `control_sheet_latest_v2` version and publishes accepted `candidate_*` tables without reading V1 output. The separate 21:00 candidate-compatibility job derives `preranked_ads_from_themes_v2_latest` from that exact accepted V2 attempt.

## Route Boundaries

| Layer | V1 route | V2 route | Notes |
| --- | --- | --- | --- |
| Control sheet | `load_control_sheet_v1` writes `control_sheet_latest` | `load_control_sheet_v2` writes `control_sheet_latest_v2` | Separate inputs and table contracts. |
| Product Theme Mapping | The scoring-input operation lands the V2 mapping and builds accepted item-theme inputs | Shared input used by both routes | The copied V1 tab is compared for visibility only; a mismatch warns rather than replacing or blocking the V2 source. |
| Score sources | Theme Affinity supplies both current serving positions; Markov publishes a shadow comparison output | Both routes bind exact accepted score outputs through reviewed configuration | Markov remains non-serving, so its failure does not block advert-candidate publication. |
| Theme Mapping copy check | Compares the copied V1 Theme Mapping with the V2 source | Warning-only check during scoring-input preparation | Reports a stale copy without preventing use of the authoritative V2 mapping. |
| Score coverage check | Compares active V1/V2 advert themes with the exact selected score-output version | Warning-only check in the candidate build | Reports business coverage gaps; inability to read or validate the selected data still fails the affected route. |
| Candidate mapping | `map_theme_scores_to_ads_v1` reads `control_sheet_latest` and writes accepted V1 `candidate_*` records | `map_theme_scores_to_ads_v2` reads `control_sheet_latest_v2` and writes accepted V2 `candidate_*` records | The separate 21:00 compatibility job converts those accepted records into the two legacy preranked table shapes. |
| Output grain | `Location` | `PageType` | The shared mapper is parameterised by output grain. |
| Page build | `mktg_next_uk_nextads_page_build` runs `jobs/nextads_assignment/build_page.py` | `mktg_next_uk_nextads_page_build_v2` runs `jobs/nextads_v2/build_page.py` | Separate triggered jobs keep route contracts clear. |
| Downstream fan-out | V1 page building, MASID handoff and PLP/Google Sheets delivery | V2 page building and Bloomreach payload export | The independent 21:00 compatibility job publishes legacy candidate shapes, then starts assignment validation. |

## Historical TL Branch Findings

The following branch observations were captured during the migration work. They are not current runtime evidence:

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

Several branches pre-date the current `src/`, `jobs/`, `configs/`, and grouped `sql/` layout. Do not merge any branch wholesale into the restructure branch. Cherry-pick or port the v2 changes after the restructure lands.

## Historical Deferred Target Shape

Once non-v2 restructure is complete, move reusable v2 logic as a dedicated PR:

```text
src/next_ads/control/adsv2/
src/next_ads/ranking/adsv2/
src/next_ads/decisioning/adsv2/
src/next_ads/delivery/adsv2/
configs/adsv2/
sql/adsv2/
```

Keep v2 output table names, config keys, and downstream payload contracts stable until a separate compatibility/cutover plan is approved.

CMS/data-pull work was reconciled through completed PR `249403` (`feature/TL/cmsdata`). Treat the current data-pull job/pipeline route as part of the post-v2 baseline, not as outstanding Ads v2 structural cleanup.

## Historical Suggested V2 Move Order

1. Re-check active TL branches and open PRs.
2. Port v2 control-sheet schema/load changes into `src/next_ads/control/adsv2` while keeping the current `jobs/nextads_control/load_control_sheet_v2.py` route entrypoint unless the job boundary changes.
3. Port v2 score/ad mapping into `src/next_ads/ranking/adsv2` and keep `jobs/nextads_candidates/build_page_type_candidates_v2.py` as the route entrypoint unless the job boundary changes.
4. Port v2 page/payload work into `src/next_ads/decisioning/adsv2`, `src/next_ads/delivery/adsv2`, and the matching current route entrypoints.
5. Add output contract checks comparing v1/v2 where relevant.
6. Update DAB paths only after DEV Integration validation is ready.

## Historical Validation Expectations

- v2 config load tests for DEV and DEV Integration.
- v2 control-sheet schema tests.
- Theme Mapping sync tests proving the v2 workbook tab is the source of truth and the v1 tab is only the copied parser input.
- Theme Affinity coverage tests proving v1/v2 ad `Themes` can join to shared customer `NextTheme` output.
- v2 output table and payload contract checks.
- DAB validate for DEV and DEV Integration.
- PREPROD validation only when the v2 route is release-candidate.

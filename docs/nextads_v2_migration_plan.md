# NextAds v2 Migration Plan

This plan documents the current TL v2 branch direction and the long-lived v1/v2
route split. The route files have been moved out of `scripts/` into
route-oriented job folders. V2 candidate mapping now changes at the
control-sheet join layer because v2 must remain active beside v1 rather than
being a short cutover route.

## Current Decision

V2 is not a full replacement for v1. Home Page remains on the v1 location-based route, while new page-type assignments use v2. Customer cells, item attributes, product Theme Mapping, and lightweight theme scoring remain shared in this PR. Candidate mapping splits by route because v1 and v2 read different loaded control sheets: v1 uses `control_sheet_latest` with `Location`, and v2 uses workbook `1UuqCDDvjrGIDPLIdc4Sq09KMHv8zy9VL0zehb0EJXp4` through `control_sheet_latest_v2` with `PageType`.

The v2 workbook is the operational source of truth for the `Theme Mapping` tab. A Google Sheets Apps Script copies that tab into the v1 workbook, and the v1 tab should be locked so Trade cannot edit the copied version directly. The candidate build runs `validate_theme_mapping_sync` before `parse_theme_mapping`; differences stop the job and should be raised to Trade because shared product theme scoring would otherwise be built from a stale copy.

## Current Rule

Current v2 runtime paths are already route-oriented and should remain stable
unless a specific file is clearly misplaced:

- `jobs/nextads_control/load_control_sheet_v2.py`
- `jobs/nextads_candidates/build_page_type_candidates_v2.py`
- `jobs/nextads_v2/build_page.py`
- `jobs/nextads_delivery/build_v2_payload.py`
- existing v2 DAB `python_file` references now point at these route folders

Do not move all v2 files into `jobs/nextads_v2/` by default;
`jobs/nextads_control`, `jobs/nextads_candidates`, and `jobs/nextads_delivery`
are valid homes when the route role is clearer. The accepted behavioural change
in this route-split PR is limited to candidate mapping: v2 maps shared Theme Affinity customer-theme scores directly to ads from `control_sheet_latest_v2` and writes `preranked_ads_from_themes_v2_latest` without reading v1 preranked output.

## Route Boundaries

| Layer | V1 route | V2 route | Notes |
| --- | --- | --- | --- |
| Control sheet | `load_control_sheet_v1` writes `control_sheet_latest` | `load_control_sheet_v2` writes `control_sheet_latest_v2` | Separate inputs and table contracts. |
| Product Theme Mapping | Shared `parse_theme_mapping` writes `theme_mapping_latest` and `item_themes_latest` from the copied v1 tab | Shared upstream task, validated against the v2 source tab first | V2 is the source of truth; the v1 workbook copy preserves the current parser/table contract. |
| Lightweight scoring | Shared `score_lightweight` writes `next_theme_scores_latest` | Shared upstream task dependency | Kept as-is to avoid a wider scheduling change; the v2 candidate mapper reads Theme Affinity model latest for customer-theme scores. |
| Theme Mapping sync validation | `validate_theme_mapping_sync` compares copied v1 Theme Mapping to v2 source | Same hard-stop validation | Stops the candidate build if the Apps Script copy has not kept the workbooks aligned. |
| Theme Affinity coverage validation | `validate_theme_affinity_theme_coverage` checks v1/v2 ad `Themes` against shared `theme_affinity_model_latest.NextTheme` | Warning-only validation in the candidate build | Calls out route themes that cannot currently be scored by the single customer-theme model output, without blocking the rest of the build. |
| Candidate mapping | `map_theme_scores_to_ads_v1` reads `control_sheet_latest` plus shared Theme Affinity scores and writes `preranked_ads_from_themes_latest` | `map_theme_scores_to_ads_v2` reads `control_sheet_latest_v2` plus shared Theme Affinity scores and writes `preranked_ads_from_themes_v2_latest` | Both routes join customer `NextTheme` to their control sheet's ad `Themes`; v2 does not read or reshape v1 preranked output. |
| Output grain | `Location` | `PageType` | The shared mapper is parameterised by output grain. |
| Page build | `mktg_next_uk_nextads_page_build` runs `jobs/nextads_assignment/build_page.py` | `mktg_next_uk_nextads_page_build_v2` runs `jobs/nextads_v2/build_page.py` | Separate triggered jobs keep route contracts clear. |
| Downstream fan-out | Assignment validation, MASID handoff, PLP Google Sheets delivery | V2 payload export | Delivery remains route-specific. |

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

Once non-v2 restructure is complete, move reusable v2 logic as a dedicated PR:

```text
src/next_ads/control/adsv2/
src/next_ads/ranking/adsv2/
src/next_ads/decisioning/adsv2/
src/next_ads/delivery/adsv2/
configs/adsv2/
sql/adsv2/
```

Keep v2 output table names, config keys, and downstream payload contracts stable
until a separate compatibility/cutover plan is approved.

CMS/data-pull work was reconciled through completed PR `249403`
(`feature/TL/cmsdata`). Treat the current data-pull job/pipeline route as part
of the post-v2 baseline, not as outstanding Ads v2 structural cleanup.

## Suggested v2 Move Order

1. Re-check active TL branches and open PRs.
2. Port v2 control-sheet schema/load changes into `src/next_ads/control/adsv2`
   while keeping the current `jobs/nextads_control/load_control_sheet_v2.py`
   route entrypoint unless the job boundary changes.
3. Port v2 score/ad mapping into `src/next_ads/ranking/adsv2` and
   keep `jobs/nextads_candidates/build_page_type_candidates_v2.py` as the
   route entrypoint unless the job boundary changes.
4. Port v2 page/payload work into `src/next_ads/decisioning/adsv2`,
   `src/next_ads/delivery/adsv2`, and the matching current route entrypoints.
5. Add output contract checks comparing v1/v2 where relevant.
6. Update DAB paths only after DEV Integration validation is ready.

## Validation Expected

- v2 config load tests for DEV and DEV Integration.
- v2 control-sheet schema tests.
- Theme Mapping sync tests proving the v2 workbook tab is the source of truth and the v1 tab is only the copied parser input.
- Theme Affinity coverage tests proving v1/v2 ad `Themes` can join to shared customer `NextTheme` output.
- v2 output table and payload contract checks.
- DAB validate for DEV and DEV Integration.
- PREPROD validation only when the v2 route is release-candidate.

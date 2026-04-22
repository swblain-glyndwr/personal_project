
# NoAdFound in Hackathon Model Assignments (BestChallenger)

## Summary
We observed a large imbalance in `UniqueAdIDAssigned == 'NoAdFound'` for the `BestChallenger` treatment in `marketingdata_prod.warehouse.next_uk_nextads_assignments_latest`.

Key conclusion: there are **two distinct contributors** to `NoAdFound` for `BestChallenger`.

1. **Acute incident (2026-03-25):** a **race condition** where the hackathon preranked table was **TRUNCATED** while `build_page.py` was running, causing some locations to read an *empty* table and produce **100% NULL** `UniqueAdIDBestChallenger`.
2. **Chronic baseline (most days):** the hackathon model/prerank pipeline covers **fewer customers** than the customer-cell base, causing a stable ~**23%** NULL `UniqueAdIDBestChallenger` rate (and therefore ~21% `NoAdFound` after accounting for the 10% fallow control).

## Context / What the pipeline does
- Customer/treatment assignment is created in `scripts/assign_customer_cells.py` and stored in `{client}_nextads_customer_cells_latest`.
- `scripts/build_page.py` reads eligible ads for a location from `{client}_nextads_control_sheet_latest` and computes:
	- `UniqueAdIDBasic` via `assign_random_ads()`
	- `UniqueAdIDBest` via `assign_preranked_ads(..., preranked_ads_from_themes_latest)`
	- `UniqueAdIDBestChallenger` via `assign_preranked_ads(..., preranked_ads_from_themes_hackathon_latest)`
- The config `config/next_uk.json` maps customers to the treatment (Basic/Best/BestChallenger) via `chain_when_thens()`.
- Any NULL `UniqueAdIDMeasurement` is replaced with `'NoAdFound'` before writing assignments.

## What we observed
### The symptom
Counts from `assignments_latest` showed far higher `NoAdFound` under `BestChallenger` than `Best`.

### Location-level pattern
On 2026-03-25, location-level results showed two modes:

- For many primary locations (e.g. `SB1`, `OC1`, `PL1`–`PL32`):
	- `UniqueAdIDBestChallenger` was NULL for **100%** of customers
	- `NoAdFound` was ~**90%** (consistent with ~10% fallow control receiving `NoAd`)

- For a later subset (e.g. `SB2`, `OC2`, `PL33`–`PL39`, `PLX`):
	- `UniqueAdIDBestChallenger` was NULL for ~**23%** of customers
	- `NoAdFound` was ~**20.8%** (≈ 23% × 90% non-fallow)

### Day-to-day pattern
For `SB1` (and similarly for many locations):
- From 2026-03-19 to 2026-03-24: `NoAdFound` ≈ **20.8–20.9%** (baseline)
- On 2026-03-25: `NoAdFound` jumped to **~90%** (incident)

## What we checked (evidence)
### 1) Hackathon preranked table exists, is fresh, and has expected locations
- Hackathon preranked table contained expected locations and had a `rundate` of 2026-03-25.
- `DESCRIBE DETAIL` showed `lastModified` ~21:17:41 on 2026-03-25.

### 2) Eligible ad coverage is not the driver
For `SB1`:
- Eligible Best ads: 213
- Champion preranked ads: 211
- Hackathon preranked ads: 211
- Only 2 eligible ads missing from hackathon for SB1

This is not large enough to explain the observed `NoAdFound` spike.

### 3) Customer coverage differs materially
For `SB1`:
- Customers in cells: 11,818,901
- Customers present in champion preranked for SB1: 9,772,303 (82.7%)
- Customers present in hackathon preranked for SB1: 9,080,033 (76.8%)

The missing ~23.2% explains the baseline NULL/NoAdFound rate on normal days.

### 4) Hackathon table Delta history confirms a truncate window
Delta history of `...preranked_ads_from_themes_hackathon_latest` on 2026-03-25:
- `TRUNCATE` at **20:44:31**
- `WRITE` at **21:17:19**

This creates a ~33 minute period where the table is empty.

### 5) Primary/secondary task ordering explains which locations were affected
The main job runs `build_page_primary` as a `for_each_task` with high concurrency.
- Early-starting locations that read during the truncate window produced **100% NULL** `UniqueAdIDBestChallenger`.
- Later locations and secondary tasks (`SB2`, `OC2`) read after the `WRITE` completed and showed the baseline ~23% NULL.

## Likely causes
### Cause A — Race condition (incident, 2026-03-25)
The hackathon job refreshes `..._hackathon_latest` using a truncate-then-load write pattern (via `truncate_and_load`).
If `build_page.py` reads the table after truncate but before the write completes, `assign_preranked_ads()` finds no rows and returns NULL for all customers.

This exactly matches:
- 100% NULL `UniqueAdIDBestChallenger` for many locations on 2026-03-25
- the truncate/write timestamps on the hackathon table
- the fact that later locations revert to the baseline

### Cause B — Customer coverage gap (baseline)
Even when there is no race condition, the hackathon model/prerank pipeline covers only ~76.8% of the customer base. Customers missing from the hackathon preranked table cannot receive a `BestChallenger` ad and end up as NULL → `NoAdFound`.

This explains the stable ~20.8–20.9% `NoAdFound` on 2026-03-19 to 2026-03-24.

### Minor code issue (not root cause)
In `scripts/build_page.py`, the `.drop()` call before `.withColumns()` is missing commas and therefore drops an unintended concatenated column name.
This is unlikely to be driving `NoAdFound` (because `.withColumns()` overwrites those columns anyway), but it should be corrected.

## Fix and next steps
### 1) Eliminate the truncate window (recommended)
Update the hackathon refresh job (the one that runs `--algo challenger`) to avoid `TRUNCATE` + long-running `WRITE` against the same live table read by `build_page`.

Options (ordered by robustness):
1. **Atomic swap pattern**: write to a new table (or temp table) and then swap/rename so readers never see an empty table.
2. **Overwrite/replace** in a single operation (if supported in your environment and table management): avoid explicit truncate; prefer an atomic replace semantics.
3. **Scheduling coordination**: ensure the challenger refresh finishes before `build_page_primary` starts (this is operationally simple but less robust).

### 2) Address baseline missing-customer coverage
Decide desired behavior for customers not present in the challenger model:
- Preferred: expand the challenger model input so it covers the same customer base as champion.
- Alternative: implement a fallback in assignment selection so `BestChallenger` falls back to `Best` when `UniqueAdIDBestChallenger` is NULL (avoids `NoAdFound` but changes experimental semantics).

### 3) Add monitoring / alerting
Add a daily QA check after assignments build:
- NULL rate for `UniqueAdIDBestChallenger` by location
- `NoAdFound` rate by treatment
- A threshold alert for sudden spikes (e.g. >30% for SB1)

### 4) Code hygiene
Fix the missing commas in the `.drop(...)` call in `scripts/build_page.py`.


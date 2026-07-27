# Feature Documentation: Redefining AlgoDivision

##Overview
Because the existing legacy propensity models are being switched off, this change redefines how **AlgoDivision** (assigning customers to their preferred shopping division) is calculated. Instead of calling out to external propensity model tables, the core calculation has been moved entirely into an in-repo SQL script within the NextAds code base.
- **Linked Work Item:** 5098494 - Redefining AlgoDivision
- **Dev/Preprod Verification Job:** [Databricks Run Link](https://adb-6188831950334199.19.azuredatabricks.net/jobs/874880684505409/runs/162002461679448?focus_task=assign_customer_cells&o=6188831950334199)

___

##Core Algorithm Mechanics
The new engine relies on historical customer interaction logs over a lookback window, combining weighted purchases and views to calculate division specific scores.
- **Purchases (Lookback: 2 Years):** Extracted from `warehouse.baskets_uk_3y`. Recency matters here; purchase impacts are linearly decayed using a calculated weight formula:
  $$\text{weight} = 1.0 - \left(\frac{\text{Days Since Purchase}}{730.0}\right)$$
- **Views (Lookback: 60 Days):** Extracted via a union of web and app page views (`bq_views_next_uk` and `bq_views_next_uk_app`). Views are aggregated as unique item counts and explicitly down-weighted by a factor of `0.5`.
Both activities are pivoted by product department attributes ('womenswear', 'menswear', etc.), combined per account, and divided by the max division value to form final affinity scores.
___
## Technical Architecture & File Changes
### 1. Configuration (`next_uk.yaml`)
Added a dictionary block to allow the framework to map and locate internal SQL logic payloads dynamically:
- **Added Key:**
```json
"sql_files": {
    "account_department_scores": "next_uk_nextads_account_department_scores.sql"
}
```
### 2. Orchestration Layer (`assign_customer_cells.py`)
Updates to look up the configured internal SQL path, establish data-science alerts, and pass them down into the division assignment function.
- **Imports Added:** `SQL_FILES`, `ACCOUNT_DEPARTMENT_SCORE_SQL`, and `WEBHOOK_URL_DS` (pointing to `cfg['webhooks']['DS Warnings']`).
- **Signature Change:**
   - *Old*: `df_divs = get_algo_divisions(MODEL_SCORES_LATEST)`
   - *New*: `df_divs = get_algo_divisions(ACCOUNT_DEPARTMENT_SCORE_SQL, TRANSIENT_CELLLS_TABLE_LATEST, WEBHOOK_URL_DS, JOB_ENV)`

### 3. Core Logic (`Assignment.py`)
The function `get_algo_divisions` has been rewritten to execute the in-repo SQL query via PySpark (`get_spark().sql(sql_query)`).

#### Tiebreaker & Fallback Logic
If a customer has completely missing record or equal tier ranks, strict sorting rules prevent dropouts. It defaults to the "**Womens**" division as the absolute tiebreaker, which historically only affects arpund 0.9% of the customer footprint:
```python
Window.partitionBy("AccountNumber").orderBy(
    F.desc(F.col("ScoreScaled")),
    (F.col("AlgoDivision") == "Womens").cast("int").desc(),
    F.col("AlgoDivision").asc()
)
```
## Automated Production Monitoring & Guardrails
To raise code bugs or bad upstream data, two automated sanity test hooks run at the end of the execution block.
If any threshold breached, the job throws warnings to logging and triggers an immediate Google Chat alert via webhook (on prod env runs only)
```
+------------------------------------------------------------+
   |             get_algo_divisions Pipeline Execution          |
   +------------------------------------------------------------+
                                 |
                                 v
   +------------------------------------------------------------+
   |            Execute Core SQL Score Generation Pipeline      |
   +------------------------------------------------------------+
                                 |
                                 v
                 [ Run Automated Monitoring Sanity Checks ]
                                 |
        +------------------------+------------------------+
        |                                                 |
        v                                                 v
+-------------------------------+                 +-------------------------------+
| Check 1: Account Coverage     |                 | Check 2: Distribution Delta   |
| Evaluates account drop rate  |                 | Compares current group % vs   |
| against transient tables.     |                 | historical distribution data.  |
+-------------------------------+                 +-------------------------------+
        |                                                 |
        v                                                 v
  Is Drop > 5%?                                     Is Delta > 5%?
   (Yes) / (No)                                      (Yes) / (No)
     /      \                                          /      \
    v        v                                        v        v
[Alert]   [Pass]                                   [Alert]   [Pass]
```
### Check 1: Account Coverage Drop-off
Compares the output unique customer counts directly against what exists in `TRANSIENT_CELLS_TABLE_LATEST` (filtered on `Cell === 'AlgoDivision'`).
- **Condition**: If the percentage of missing accounts from the historical baseline exceeds **5%**, an error alert is flagged.

### Check 2: Division Distribution Deviation
Calculates the macro population share distribution across all divisions (e.g., % of total user base assigned to Menswear) and shifts it against historical benchmarks.
- **Condition:** If any single division's population shares swing up or down by more than *5%* (|pct_diff|>5), a diistribution wariance arning is flagged

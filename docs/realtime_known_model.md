# Realtime Known Customer Reranking Model

## Overview & Purpose
The purpose of the  **Realtime Known Customer Reranking Model** is to dynamically adjust advert recommendations for recognized customers in real time. By using the most recent (live) of recent user interactions (e.g., product views, add-to-bag actions), the model can enable optimisation of adverts to the userscurrent intent, to drive incremental revenue.

Critical to an effective real-time setup is establishing **when** and **how** to trigger updates. To manage this effectively, the system is designed to be a modular 3-component architecture.

---

## High-Level Architecture

[ Triggering Mechanism ] ──> [ Decisioning Logic ] ──> [ Execution Layer ]
(Bloomreach / SDK)          (MLflow / Databricks)       (Direct API / Fallback)

1. **Triggering Mechanism:** Initial event signalling the need for a real-time advert adjustment.
2. **Decisioning Logic:** Activated by the trigger; evaluates live inputs and identifies the optimal advert set.
3. **Execution Layer:** Final delivery layer that updates the adverts shown to the customer.


### Component Details

#### 1. Trigger Architecture
* **Initial Mechanism:** Bloomreach event integration.
* **Proposed Alternative:** Custom SDK build mechanism (led by Architecture team).
* **Payload:** Forwards the customer's `RPID` alongside the most recent item interaction details to the decisioning engine.

#### 2. Model Architecture
* Implemented as a **custom MLflow PyFunc model class**.
* Registered within MLflow and deployed via **Databricks Model Serving**.

* Rationale for  **Custom Requirements:**
    * Requires custom post-processing logic alongside a core model inference to format standard responses expected by downstream consumers.
    * Enables flexibility in the model build & application of post-processing if required

#### 3. Execution Architecture
Employs a dual-path response strategy:
* **Primary Path:** Direct, low-latency API return from the model serving endpoint.
* **Fallback Path:** Secondary call via Bloomreach API if primary response latency exceeds target thresholds. <i>(Please note secondary path integration is still pending)</i>
* Rationale for  **Dual-Path Requirements:**
    * Low-Latency Preferred: Serves real-time ads instantly, dropping the request if it risks slowing down page rendering.
    * Guaranteed Fallback: Pushes updates via Bloomreach API so intent-based ads still render on subsequent interactions if the primary path times out.

---

## Model Implementation (`RealtimeKnownRerankingModel`)

A custom MLflow Python model class designed for real-time reranking of batch-scored adverts.

### Core Workflow
1. Receives input containing a customer `RPID` and a payload of recent item interactions.
2. Looks up live features against respective item and customer advert data stores.
3. Reranks target adverts based on recent item features and interaction context.
4. Identifies the highest-associated advert (based on view/add to basket history) for each target placement location.
5. Returns the associated adverts as the final recommendations.

### Input Schema
The `predict()` method expects a JSON-like payload matching the following structure:

```json
{
  "rpid": "string (Customer RPID)",
  "items": {
    "1": {
      "item": "string (Item PID)",
      "action": "view | atb"
    },
    "2": {
      "item": "string (Item PID)",
      "action": "view | atb"
    }
  }
}
```
### Example Usage

``` python
from nextads.realtime.decisioning.realtime_known_reranking_model import RealtimeKnownRerankingModel

# Initialize model
rt_reranking_model = RealtimeKnownRerankingModel()

# Run inference
results = rt_reranking_model.predict({
    "rpid": "8192127685",
    "items": {
        "1": {"item": "v12037", "action": "view"},
        "2": {"item": "w87234", "action": "view"},
        "3": {"item": "w03942", "action": "view"}
    }
})
```
Saving to Model Registry

<i>To add once completed</i>

Served Model Usage

<i>To add once completed</i>

## Data Pipelines & Feature Store

To ensure minimal latency during live serving, heavy data transformations are pre-calculated during daily batch processing.

### Data Flow & Architecture
1. **Daily Batch Processing:** Heavy feature extraction jobs compute the primary component tables daily.
2. **Delta Lake Integration:** Results are written to primary Delta tables within Databricks.
3. **Online Sync:** Delta tables synccontinuously/on-completion to **Lakebase Online Feature Store** tables via automated Databricks pipelines <i> (Please note this is pending addition to the batch job to trigger the updates)</i>

[ Daily Batch Job ] ──> [ Databricks Delta Tables ] ──> [ Lakebase Online Feature Store ] ──> [ Low-Latency Model Serving ]

### Critical Operational Constraints
> **Important Note on Schema Sync:**
> Lakebase online feature store tables map directly to underlying Delta table schemas. **Do not modify the upstream schema directly** without re-creating the online table sync, as schema mismatches will immediately break synchronization.

### Data Sync Pipeline Status

| Table Name / Target | Source Table | Batch Job |Sync Trigger | Target Status | Notes |
| ------------------- | ------------------- | ----------- |----------- | --------- |--------------|
| `next_uk_nextads_realtime_reranking_preranked_ads_online` | `next_uk_nextads_realtime_reranking_preranked_ads` | mktg_next_uk_nextads_realtime_data | Post-Batch Complete | Pending Schedule | This is 900M records - need to optimise sync |
| `next_uk_nextads_realtime_reranking_item_weighting_rules_online` | `next_uk_nextads_realtime_reranking_item_weighting_rules` | mktg_next_uk_nextads_realtime_data | Post-Batch Complete | Pending Schedule | |
| `next_uk_nextads_advert_advert_association_online` | `next_uk_nextads_advert_advert_association` | mktg_next_uk_nextads_realtime_data | Post-Batch Complete | Pending Schedule |  |
---

## Technical Challenges & Next Steps

### Current Technical Challenges
* **High-Volume Upsert Performance:** Overcoming performance bottlenecks when efficiently syncing and updating large-scale datasets (~900M record tables) to the online Lakebase feature store without causing latency spikes.

### Implementation Next Steps
1. **Finalize Batch Pipelines:** Complete development and testing of daily batch data ingestion and scoring jobs.
2. **Automate Feature Store Sync:** Configure robust automated refresh schedules for Online Feature Store pipelines triggered immediately upon batch job completion.
3. **Model Registration:** Finalize packaging and deploy the trained custom MLflow PyFunc model to the Feature Store registry.
4. **Endpoint Provisioning:** Provision, test, and benchmark the Databricks Model Serving endpoint under simulated peak loads.
5. **Bloomreach API Integration:** Implement and test the secondary Bloomreach API update call within the model pipeline to support fallback delivery.

### Current Testing limitations
* Please be aware that due to the issue with syncing the customer records table to the online store- the data table currently being used for Advert details is a a sample table with only a few RPIDs in!

---
## Model Overview

``` mermaid
graph TD
    %% 1. TRIGGER
    subgraph Trigger ["1. Triggering Mechanism"]
        Action[Customer Behavior Action: View / ATB] --> Event{Event Trigger}
        Event --> Bloomreach[Bloomreach Event or Custom SDK]
        Bloomreach --> Payload[Payload: Customer RPID & Item Details]
    end

    %% DATA PIPELINE
    subgraph Data ["Data Store"]
        Batch[Daily Batch Processing] --> Delta[(Databricks Delta Tables)]
        Delta --> Online[(Lakebase Online Feature Store)]
    end

    %% 2. DECISIONING / MODEL
    subgraph Model ["2. Decisioning Logic (MLflow Model)"]
        Payload --> Endpoint[Databricks Model Serving]
        Endpoint --> Lookup[Lookup Item & Customer Data]
        Online --> Lookup
        Lookup --> Rerank[Rerank Adverts by Intent]
        Rerank --> Incrementality[Identify 'Next Best Advert' for Incrementality]
        Incrementality --> Select[Select Top Next Best Advert' per Location]
        Online --> Incrementality
    end

    %% 3. EXECUTION
    subgraph Execution ["3. Execution Layer"]
        Select --> DualPath{Execution Strategy}

        %% Primary Path
        DualPath -->|Primary Path: Low Latency| Direct[Immediate API Return]
        Direct --> Display[Updated Adverts Served]

        %% Fallback Path
        DualPath -->|Secondary Path: Fallback| BR_API[Bloomreach API Update]
        BR_API --> Display
    end

    %% Apply custom background (fill) and border (stroke) colors
    style Trigger fill:#F5EBED,stroke:#F5EBED,stroke-width:2px
    style Model fill:#FAFCE1,stroke:#FAFCE1,stroke-width:2px
    style Execution fill:#E1FCE7,stroke:#E1FCE7,stroke-width:2px
    style Data fill:#D1F0FF,stroke:#D1F0FF,stroke-width:2px

```

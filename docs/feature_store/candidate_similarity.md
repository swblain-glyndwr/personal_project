# Next Ads Candidate-Level Similarity Calculation

Azure Boards story: 5111876  
Feature: 5111595 - Reusable feature layer (Databricks Feature Store)

## Purpose

This document defines how customer-to-ad similarity features should be calculated for ranked candidate pairs using stored vectors. The feature store must support pCTR, LTR/theme affinity and future two-tower retrieval without ever creating a full customer-by-ad cross join.

## Candidate-Pair Grain

Similarity is calculated only after candidate creation. The input grain is:

```text
one account
+ one candidate advert, product, item or theme
+ one placement/location where relevant
+ one reference date
-> one candidate similarity row
```

For pCTR, the model-ready candidate grain is:

```text
account_number
advert_id
location
session_date
reference_date
```

For two-tower/retrieval training, the generic pair grain is:

```text
anchor_entity_type
anchor_entity_id
candidate_entity_type
candidate_entity_id
label_name
reference_date
```

The candidate set is produced by existing Next Ads eligibility, theme-to-ad mapping, assignment, retrieval or experiment logic. The similarity job enriches those candidates; it does not decide global eligibility from a full cross join.

## Vector Dependencies

| Side | Required feature table | Required fields |
| --- | --- | --- |
| Customer/product interest | `next_uk_nextads_fs_seasonal_product_demand_daily` or future customer vector table | account/entity key, `feature_date`/`reference_date`, vector array or scalar vector dims, coverage metrics |
| Advert product profile | `next_uk_nextads_fs_advert_product_profile_daily` | `advert_id`, `feature_date`, product vector, product embedding coverage |
| Product/item embedding lookup | `next_uk_nextads_fs_product_embeddings_latest` | `item_id`, embedding vector, `embedding_model_name`, `embedding_model_version` |
| Advert semantic profile | `next_uk_nextads_fs_advert_semantic_profile_daily` | `advert_id`, `feature_date`, semantic vector or scalar semantic dims, model metadata |
| Candidate rows | `next_uk_nextads_fs_pctr_model_input` or `next_uk_nextads_fs_two_tower_training_pairs` | candidate entity keys and reference date |

The model/version metadata on both sides must match before cosine similarity is calculated. If model versions differ, the row should either be skipped for that similarity feature or flagged with zero/unknown coverage rather than mixing incompatible vector spaces.

## Cosine Similarity Route

For each candidate row:

1. Join the candidate to the customer/account-side vector using the account key and reference date.
2. Join the candidate to the advert/product/theme-side vector using the candidate key and feature/reference date.
3. Check both vectors are present, non-empty and from compatible embedding model versions.
4. Calculate cosine similarity:

```text
cosine_similarity = dot(customer_vector, candidate_vector)
                    / (norm(customer_vector) * norm(candidate_vector))
```

5. Write both the similarity score and coverage fields to the candidate/model-input table.

The calculation can be implemented with Spark array functions, scalar dimension columns, or a small UDF where vector arrays are the stable storage format. The implementation should preserve null/coverage semantics so models can learn the difference between "low similarity" and "not enough vector data".

## Candidate-Level Output Fields

The initial pCTR/model-input output fields should include:

- `customer_ad_product_cosine_similarity`
- `customer_ad_product_embedding_coverage`
- `customer_ad_seasonal_product_cosine_similarity`
- `customer_ad_seasonal_product_embedding_coverage`
- `customer_vector_model_name`
- `customer_vector_model_version`
- `advert_vector_model_name`
- `advert_vector_model_version`
- `similarity_calculated_at`

For future two-tower/retrieval work, the generic pair output can include:

- `anchor_embedding_model_name`
- `anchor_embedding_model_version`
- `candidate_embedding_model_name`
- `candidate_embedding_model_version`
- `candidate_cosine_similarity`
- `candidate_similarity_rank`
- `similarity_feature_coverage`

## Integration Points

For current pCTR, similarity fields should land in `next_uk_nextads_fs_pctr_model_input` after the candidate rows exist. Existing score output tables stay unchanged until a separate migration proves output equivalence.

For Theme Affinity/LTR, the first route is through compatibility view `next_uk_nextads_theme_affinity_features_latest`; native feature-store reads can follow once the model input equivalence is proven.

For challenger testing, similarity features should be switched on as candidate/model features behind a model experiment or challenger configuration. They should not alter production rank assignment directly in this feature-store setup slice.

## Acceptance Criteria Mapping

| Acceptance criterion | Evidence in this document |
| --- | --- |
| Candidate-pair input grain documented | Candidate-pair grain section. |
| Cosine similarity calculation route documented | Cosine similarity route section. |
| Customer and advert vector dependencies identified | Vector dependencies section. |
| Candidate-level output fields agreed | Candidate-level output fields section. |
| Integration into challenger testing captured as follow-on work | Integration points section. |

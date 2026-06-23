# Shopping Bag pCTR Feature Build

## Purpose

This branch builds the first Shopping Bag pCTR feature pipeline for NextAds.
The goal is to create model-ready account/ad exposure rows that estimate:

> Which actual advert is this customer likely to click in this placement?

`pCTR` means predicted click-through rate. In this branch it is the model's estimated probability that a known customer clicks a specific advert after seeing it on the Shopping Bag page.

The model row is deliberately at account/ad/placement level:

```text
one customer
+ one advert
+ one page placement
+ one point in time
-> predicted probability of click
```

This is deliberately separate from the existing Hackathon/LTR theme model:

- Hackathon model: which themes does this customer like?
- pCTR model: which of these actual ads will this customer click here?
- NextAds: given ranked ads, cells, rules, fallow control, premium logic, and MASID mapping, what do we assign?

The intended future flow is:

```text
Hackathon model
-> AccountNumber x Theme score
-> map themes to ads
-> join pCTR scores
-> combined Score
-> Rank
-> build_page
-> assignments_latest
```

The simplest integration is to keep the existing NextAds assignment process intact and use pCTR as an additional ad-level reranking signal. A first combined score could be:

```text
combined_score = hackathon_theme_score * predicted_pctr
```

or a calibrated weighted blend after model testing.

The initial integration can treat the Hackathon score as the theme-level preference signal and pCTR as the ad-level click-propensity signal. The weighting does not have to stay equal or fixed. If testing shows pCTR is more predictive, the combined score can upweight pCTR and downweight the Hackathon theme score.

For example:

```text
combined_score = (theme_score_weight * hackathon_theme_score) + (pctr_weight * predicted_pctr)
```

or:

```text
combined_score = hackathon_theme_score ^ theme_weight * predicted_pctr ^ pctr_weight
```

That gives a migration path:

```text
theme-led ranking
-> theme score plus pCTR reranking
-> pCTR-led ranking with theme as a supporting feature
-> direct ad-level ranking if theme mapping is no longer needed
```

## Key Terms Used In This Document

- `reference_date`: the date the feature build pretends it is running on. For example, if `reference_date = 2026-04-15`, all recency features are calculated as if 15 April 2026 is "today". This helps avoid accidentally using future data when building training rows.
- `feature`: a model input column. It can be a simple number like `online_orders_n`, a category like `gender`, or one numeric dimension from an embedding.
- `label` or `target`: the answer the model is trained to predict. In this branch, the main target is whether the customer clicked the advert/campaign within 7 days of seeing it.
- `coverage`: a data quality/completeness measure. For example, `advert_product_embedding_coverage` says how much of an advert's linked product set had usable product embeddings.
- `candidate row`: an account/ad row that could be scored by the pCTR model. It is a possible advert assignment, not necessarily the advert that will finally be shown.
- `snapshot`: a point-in-time copy of the feature/training data for a specific `reference_date`. Snapshots let the model train on historic months and test on later months in a realistic way.
- `feature contract`: the saved list of columns the trained model expects at scoring time. It stops training and scoring drifting apart.
- `cosine similarity`: a number that measures how close two numeric fingerprints are. In this branch it is used to compare customer product interest with advert product content. Higher means more similar.
- `attribute profile`: a summary of the products linked to an advert. For example, if an advert links mostly to dresses from one brand, the profile should show that brand/category strongly.
- `weighted`: not every linked product contributes equally. Products that are more representative of the advert can be given more influence when creating the advert summary.

## Embedding Cache Change

The 18-month snapshot build can encounter the same advert text, item text, and product text many times. Without a cache, each monthly run sends those same texts back through the SentenceTransformer model, even when the text and embedding model have not changed. That is slow and unnecessarily expensive.

This branch now adds a shared cache table:

```text
marketingdata_dev.{dev_schema}.next_uk_pctr_text_embedding_cache
```

The cache stores the expensive part of the process:

```text
cleaned text
+ embedding model name
+ embedding model version
-> text embedding
```

The cache key is built from the embedding model, model version, and cleaned text. This means:

- if the same advert text appears in multiple monthly snapshots, it is embedded once and reused;
- if the same product text appears in multiple adverts, customer histories, or monthly snapshots, it is embedded once and reused;
- if the text changes, the cache key changes and a new embedding is created;
- if the embedding model version changes, the cache key changes and new embeddings are created for that model version.

The cache table contains:

- `embedding_cache_key`: hashed key for model/version/text.
- `embedding_model_name`: the Hugging Face model used.
- `embedding_model_uri`: the registered MLflow/Unity Catalog model URI.
- `embedding_model_version`: the model version used when the embedding was created.
- `embedding_text`: the cleaned text that was embedded.
- `text_embedding`: the numeric embedding array.
- `created_at`: when the cache row was created.

What changed in the notebooks:

- `pctr_advert_semantic_embeddings.py` now checks the cache before embedding `advert_text_corpus`.
- `pctr_advert_semantic_embeddings.py` also checks the same cache before embedding linked item text.
- `pctr_product_embedding_features.py` now checks the same cache before embedding `product_text`.
- Only new, unseen text is sent to the SentenceTransformer.
- Monthly snapshot output tables are still written as before, so the point-in-time feature tables keep their expected shape.

What is deliberately not cached:

- Customer product vectors are not cached, because they depend on each customer's recent views and purchases for the current `reference_date`.
- Advert product vectors are not cached, because active linked products and item weights can change by snapshot date.
- Seasonal vectors are not cached, because same-month-last-year purchases, 7-day demand, 30-day demand, and trend windows change by `reference_date`.
- Labelled training rows are not cached, because exposure windows and click labels are point-in-time training data.

So the cache does not freeze the feature build. It only avoids repeating the stable text-to-number conversion when the same text has already been embedded with the same model version.

During an 18-month backfill, the first month will populate the cache with many new texts. Later months should increasingly show cache hits and only embed genuinely new advert/item/product text.

## Notebooks In This Branch

### `customer_behaviour_features.py`

Builds account-grain customer features anchored to a `reference_date`.
This is the "who is the customer and how active are they?" layer.

Main columns/features created:

- Customer identifiers: `account_number`, `accountnumberkey`, `roamingprofileid`.
- Customer descriptors: `postcode_area`, `gender`, `uk_region`, `account_type`, `svoc_credit_type`, `creditcustomer`, `creditactive`, `cashactive`, `emailoptin`, `smsoptin`, `lapsingstatus`, `specialaccount`, `customerprofile`.
- Account age and recency: `account_age_days`, `days_since_last_site_visit`, `days_since_last_online_order`, `days_since_last_store_purchase`.
- Commercial history: `online_orders_n`, `online_spend_n`, `online_returns_n`, `retail_orders_n`, `retail_spend_n`, `retail_returns_n`.
- Browse activity: `browse_sessions_90d`, `browse_active_days_90d`, `days_since_last_browse_session`, `page_events_90d`, `avg_pages_per_session_90d`, `shopping_bag_page_events_90d`.
- Action activity: `action_events_90d`, `action_active_days_90d`, `add_to_bag_actions_90d`, `pdp_action_rows_90d`, `days_since_last_action`.

Why the model needs it:

- pCTR is not only about the advert. Two customers shown the same advert can have very different click probability because one is recently active, one is lapsing, one browses heavily, one rarely interacts, one has a stronger online buying history, and so on.
- These columns give the model a baseline customer propensity before it considers the advert itself.

Output:

- `next_uk_pctr_customer_behaviour_features`

### `pctr_advert_metadata_attribute_profile.py`

Builds the "what is this advert?" layer, i.e. the structured fields that describe the advert in the control sheet and linked product feeds.

Main columns/features created:

- Advert identifiers and placement context: `feature_date`, `advert_id`, `placement_id`, `campaign_id`, `advert_url`, `page_path`, `screen`, `page_group`.
- Creative/copy descriptors: `advert_title`, `headline`, `subtext`, `cta`, `template_name`.
- Trade/category descriptors: `advert_theme`, `advert_category`, `advert_brand_name`, `advert_campaign`, `advert_mission`, `algo_division`, `trade_division`, `control_sheet_brand`.
- Visual/config descriptors: `header_colour`, `text_colour`, `background_colour`, `button_text_colour`, `button_colour`, `background_image`, `mobile_image`, `flat_jpg`.
- Control-sheet targeting descriptors: `tags`, `targeting_attributes`, `themes`.
- Linked products: `advert_item_count`, `advert_item_weight_sum`, `advert_item_sources`, `advert_item_text_corpus`.
- Attribute profile counts: `attribute_profile_attribute_count`, `attribute_profile_value_count`.
- Top weighted product attributes: `top_brand`, `top_use`, `top_colour`, `top_style`, `top_category`, `top_department`, `top_gender`.
- Attribute strength columns: `top_brand_weight`, `top_use_weight`, `top_colour_weight`, `top_style_weight`, `top_category_weight`, `top_department_weight`, `top_gender_weight`.
- Coverage/quality flags: `has_item_attribute_profile`, `advert_active_placement_count`, distinct-value counts such as `brand_profile_distinct_values`, `category_profile_distinct_values`, `gender_profile_distinct_values`.

Why the model needs it:

- This turns an advert from an opaque ID like `P123_C456...` into model features describing what it contains.
- It lets the model learn, for example, that a customer is more likely to click adverts for certain brands, departments, product uses, categories, or creative styles.
- It also gives coverage indicators so the model can handle adverts with weak or missing item links differently from adverts with rich product profiles.

Attribute profile example:

If an advert links to five products and four of them are women's dresses, the advert profile should make `top_category = dresses` and give that category a strong weight. If another advert links to a mixed set of shoes, bags, and dresses, the profile will look less concentrated. That helps the model understand whether the advert has a clear product identity or a broad/mixed one.

Notes:

- Some of these columns are blank, as they're built from the control sheet, but they are kept in so that in future builds you can flow in this information from other sources

Outputs:

- `next_uk_pctr_advert_daily_core_90d`
- `next_uk_pctr_item_attribute_lookup_latest`
- `next_uk_pctr_advert_attribute_profile_90d`

### `pctr_advert_semantic_embeddings.py`

Builds advert text and semantic embedding features from destination metadata, creative copy, image fields, and linked item text.
The registered SentenceTransformer is staged to a UC Volume and loaded from that local volume path inside Spark workers. This avoids every executor repeatedly downloading the Unity Catalog MLflow model and timing out when temporary credentials are requested.

The embedding inputs are materialised to Delta before repartitioned Spark inference. This removes Spark's indeterminate shuffle retry failure mode.

The expensive text-to-embedding step is cached in `next_uk_pctr_text_embedding_cache`. The cache key is built from the embedding model, model version, and cleaned text. If the same advert text or item text appears in several monthly snapshots, it is only embedded once; later snapshots reuse the cached numeric embedding and still write their own point-in-time feature rows.

Databricks fix:

- Before this change, each Spark worker could try to fetch the registered embedding model from Unity Catalog/MLflow during the distributed job. That created repeated network calls and temporary-credential requests, which timed out.
- Now the notebook downloads/stages the model once to the UC Volume, then workers load it from that stable path.
- Before repartitioned embedding inference, the input rows are written to a Delta table and read back. This gives Spark a stable checkpoint so if one task fails and is retried, Spark can rerun from a deterministic input instead of failing the whole job.
- Before embedding inference, the notebook checks the shared text embedding cache and only sends new, unseen text to the SentenceTransformer. This avoids paying to embed the same advert/item text repeatedly during an 18-month backfill.

Main columns/features created:

- `advert_text_corpus`: the plain text that represents the advert. It is built by combining the advert headline, subtext, call to action, destination page fields, and linked product text. This is the sentence/paragraph that is passed to the embedding model.
- Text size/quality checks:
  - `advert_semantic_char_count`: how many characters are in the advert text.
  - `advert_semantic_token_count`: roughly how many words/tokens are in the advert text.
  - `advert_semantic_unique_token_count`: how many distinct words/tokens are in the advert text.
  - `advert_has_destination_image`: whether the advert has usable destination image information.
- Model traceability:
  - `embedding_model_name`: the Hugging Face model name used to convert text into numbers.
  - `embedding_model_uri`: the registered MLflow/Unity Catalog model URI used for the run.
- `advert_semantic_embedding`: the full numeric representation of the advert text. This is an array of numbers. Each advert gets one array. Two adverts with similar wording/meaning should have arrays that are close together; two unrelated adverts should have arrays that are further apart.
- `advert_semantic_dim_000` through `advert_semantic_dim_031`: the first 32 numbers from that advert embedding, split into separate columns so normal Spark ML models can use them like any other numeric feature. These columns do not have simple human labels like "brand" or "colour"; together they act as a compressed meaning fingerprint for the advert.
- Similar-ad features:
  - `advert_embedding_neighbour_count`: how many other adverts are semantically similar to this advert.
  - `advert_embedding_top_similarity`: the strongest similarity score to another advert.
  - `advert_embedding_avg_similarity`: the average similarity score across the closest similar adverts.
- Item semantic layer: the same text-to-numbers process is also applied to linked product/item text. This helps the advert features understand the products behind the advert, not only the advert copy.

Why the model needs it:

- Structured fields cannot fully capture what an advert is saying. A headline, CTA, product description, and destination text may imply style, occasion, offer type, or intent.
- The embedding columns give the model a way to use text meaning without manually creating hundreds of keyword flags.
- For example, two adverts might not share the same exact category label but may both be about "summer dresses for holidays". Their embedding numbers should be close enough for the model to learn that they behave similarly.
- The 32 scalar embedding dimensions let Spark ML models use this meaning signal without needing to understand arrays directly.
- Neighbour features tell the model whether an advert sits in a dense group of similar adverts or is unusual.

Embedding explanation:

An embedding is a list of numbers that represents meaning. In a logistic regression style model, we are used to columns like `online_orders_n = 4` or `gender = F`. Text is harder because the words are messy and sparse. An embedding model reads the text and converts it into a fixed-size numeric fingerprint.

For example:

```text
"linen holiday dresses summer edit"
-> [0.12, -0.08, 0.44, ..., 0.03]
```

The individual numbers are not meant to be read one by one. The useful property is distance: adverts with similar meaning get similar number patterns. Once the text has been turned into numeric columns, the pCTR model can learn whether certain kinds of wording/product meaning are more likely to be clicked by certain customers.

Outputs:

- `next_uk_pctr_advert_destination_content_90d`
- `next_uk_pctr_advert_semantic_embeddings_90d`
- `next_uk_pctr_item_semantic_embeddings_latest`
- `next_uk_pctr_advert_embedding_neighbours_90d`

### `pctr_product_embedding_features.py`

Builds reusable product embeddings and rolls them up to advert-side and customer-side product interest features.
This supports model features such as customer/ad product similarity, advert product coverage, and customer product-interest vectors.

Like the advert semantic notebook, it stages the SentenceTransformer to the UC Volume and materialises embedding input rows to Delta before distributed inference.

It also uses the shared `next_uk_pctr_text_embedding_cache`. Product text is especially worth caching because the same product can appear across many adverts, many customers' browse/purchase histories, and many monthly snapshots.

After product embeddings are built, the notebook writes that product embedding snapshot to Delta and reads it back before building advert and customer product rollups. This breaks the distributed embedding inference lineage before the later aggregations run.

The customer interaction input is also materialised to Delta after purchases/views have been joined to product embeddings and before the customer-level aggregation. Without these materialisation boundaries, a later customer-feature shuffle retry can fail with Spark's indeterminate output error even after the product embedding and advert product tables have already written successfully.

The final advert product and customer product rollups are also locally checkpointed before the output writes. This cuts the remaining shuffle-heavy aggregation lineage immediately before Delta persistence, which makes intermittent Spark task retries less likely to abort the whole snapshot.

Main columns/features created:

- Product text and embedding:
  - `itemno`: the product/item identifier.
  - `product_text`: the plain text description of the product built from available product fields.
  - `product_embedding`: the numeric meaning fingerprint for the product text, using the same idea as the advert text embedding.
  - Embedding norm checks: simple QA columns that check whether the product embedding vector has a sensible size.
- Advert product coverage: `advert_product_item_count`, `advert_product_embedded_item_count`, `advert_product_embedding_coverage`.
- Advert product vector: `advert_product_embedding` and scalar columns `advert_product_dim_000` through `advert_product_dim_031`.
- Customer product interaction counts: `customer_product_interaction_count`, `customer_product_purchase_interaction_count`, `customer_product_view_interaction_count`.
- Customer product breadth/coverage: `customer_product_distinct_item_count`, `customer_product_embedded_item_count`, `customer_product_embedding_coverage`.
- Customer product vector: `customer_product_embedding` and scalar columns `customer_product_dim_000` through `customer_product_dim_031`.

Why the model needs it:

- This is the core customer-to-ad product affinity signal.
- The advert vector says "what products does this advert represent?" It is created by taking the embeddings for the products linked to the advert and averaging them with weights.
- The customer vector says "what products has this customer recently viewed or bought?" It is created from the embeddings of products the customer interacted with, with purchases weighted more heavily than views.
- The training and scoring notebooks compare these vectors using `customer_ad_product_cosine_similarity` and `customer_ad_product_embedding_coverage`, so the model can learn whether a closer product match increases click probability.

Product matching example:

If a customer has recently viewed or bought items whose product text is close to "black wide-leg trousers" and an advert links to products with similar text, the customer and advert product embeddings should be close together. That closeness becomes a numeric feature. The model can then learn whether close product matches make clicks more likely.

Caching note:

The product embedding cache does not freeze the customer or advert product features. Those still rebuild for each `reference_date`, because recent views, purchases, active advert items, and demand windows change over time. The cache only avoids repeating the stable text-to-number conversion for product descriptions that have already been embedded with the same model version.

Outputs:

- `next_uk_pctr_product_embeddings_latest`
- `next_uk_pctr_advert_product_features_90d`
- `next_uk_pctr_customer_product_features`

### `pctr_seasonal_product_features.py`

Adds seasonal product signals on top of product embeddings.
It builds same-month-last-year customer purchase embeddings, recent advert linked-product demand, same-month-last-year advert demand, and seasonal customer/ad similarity features.

Main columns/features created:

- Customer seasonal history: `customer_same_month_ly_purchase_count`, `customer_same_month_ly_distinct_item_count`, `customer_same_month_ly_embedded_item_count`.
- Customer seasonal vector:
  - `customer_seasonal_product_embedding`: a numeric meaning fingerprint of the products this customer bought in the same month last year.
  - `customer_seasonal_product_embedding_coverage`: how much of that seasonal purchase history had usable product embeddings.
  - `customer_seasonal_product_dim_000` through `customer_seasonal_product_dim_031`: the first 32 numbers from that seasonal customer fingerprint split into normal numeric columns.
- Advert recent demand: `advert_product_views_7d`, `advert_product_views_30d`, `advert_product_purchases_7d`, `advert_product_purchases_30d`.
- Advert same-season demand: `advert_product_views_ly_same_month`, `advert_product_purchases_ly_same_month`.
- Advert trend signal: `advert_product_trending_7x30`.
- Advert seasonal vector:
  - `seasonal_advert_product_embedding`: a numeric meaning fingerprint for the products linked to the advert, weighted by recent and seasonal product demand.
  - `seasonal_advert_product_embedding_coverage`: how much of the advert's linked product set had usable product embeddings.
  - `seasonal_advert_product_dim_000` through `seasonal_advert_product_dim_031`: the first 32 numbers from that advert seasonal fingerprint split into normal numeric columns.

Why the model needs it:

- Some adverts are more clickable because the linked products are in season now, or because the customer has bought similar seasonal products in the same period last year.
- This layer lets the model distinguish general product affinity from timely/seasonal affinity.
- The training and scoring notebooks compare customer and advert seasonal vectors using `customer_ad_seasonal_product_cosine_similarity`.

Seasonal example:

A customer might generally like dresses, but their same-month-last-year purchases may show they buy partywear in November and swimwear in May. The seasonal features let the model learn that the timing of the product match matters, not just the product match itself.

Outputs:

- `next_uk_pctr_customer_seasonal_product_features`
- `next_uk_pctr_advert_seasonal_product_features`

### `pctr_tagged_click_training.py`

Builds the Shopping Bag training table.
It creates observed Shopping Bag exposure rows from BQ page visits and NextAds assignment outputs, then labels those exposures using tagged BQ click actions.

The clicked advert is taken from `bq_actions.Level2` for `Action = "Banner Click - Next Ads"` and `PagePath = "/shoppingbag"`.
Labels are created for same-session, 24-hour, and 7-day attribution windows. The current modelling target is `label_7d`.

Label explanation:

The model needs examples with an answer attached. For each observed Shopping Bag advert exposure, this notebook asks:

```text
after this customer saw this advert,
did they click the same advert/campaign?
```

The answer becomes a label:

- `label_same_session = 1` means the click happened in the same web session.
- `label_24h = 1` means the click happened within 24 hours.
- `label_7d = 1` means the click happened within 7 days.
- A value of `0` means no matching click was found in that window.

The model currently trains on `label_7d` because tagged advert clicks are sparse. The 7-day window gives more positive examples for the first model experiment.

Main columns/features created:

- Training grain: one row per observed account/ad Shopping Bag exposure.
- Exposure keys: `reference_date`, `account_number`, `unique_ad_id`, `assigned_unique_ad_id`, `campaign_key`, `assigned_campaign_key`, `placement_id`, `unique_visit_id`, `session_date`, `exposure_ts`.
- Advert context kept for audit: `advert_url`, `campaign_id`, `advert_theme`, `advert_category`, `page_path`.
- Assignment/session context: `device`, `treatment`, `fallow_control`, `exposure_source`, `exposure_confidence`.
- Time context: `exposure_hour`, `exposure_dayofweek`, `exposure_month`, `exposure_weekofyear`, `exposure_quarter`, `exposure_is_weekend`, `exposure_month_sin`, `exposure_month_cos`, `exposure_week_sin`, `exposure_week_cos`.
- Click labels: `first_click_ts_7d`, `hours_to_first_click`, `label_same_session`, `label_24h`, `label_7d`.
- Joined advert features: top product attributes, advert semantic dimensions, advert product dimensions, advert seasonal demand and seasonal dimensions.
- Joined customer features: customer descriptors, behaviour features, customer product dimensions, customer seasonal product dimensions.
- Customer/ad match features: `customer_ad_product_cosine_similarity`, `customer_ad_product_embedding_coverage`, `customer_ad_seasonal_product_cosine_similarity`, `customer_ad_seasonal_product_embedding_coverage`.

Why the model needs it:

- This notebook is where feature tables become supervised training data.
- It answers: "The customer was shown this advert here at this time. Did they click this advert/campaign afterwards?"
- The model target is currently `label_7d`, because same-session and 24-hour clicks are expected to be sparse for the first experiment.
- The click join uses advert/campaign keys so the model is learning ad-level click propensity rather than a generic "clicked any advert" target.

Campaign-key explanation:

The BQ click action tells us which advert was clicked through `Level2`. The notebook extracts a campaign-style key such as `P123_C456` from both the shown advert and the clicked advert. This is slightly broader than matching the full creative ID, but it is more robust when variants of the same advert/campaign have different suffixes. It means the label is best read as "clicked this advert/campaign family" rather than "clicked this exact full creative string".

Outputs:

- `next_uk_pctr_sb_observed_exposures`
- `next_uk_pctr_sb_tagged_click_training`
- `next_uk_pctr_sb_tagged_click_training_sampled`

### `pctr_build_training_snapshots.py`

Runs the feature and training notebooks for one or more monthly `reference_date` values using `write_mode=append_snapshot`.
This is the backfill/smoke-test orchestrator for point-in-time training data.

For a one-month smoke test, use `snapshot_months=1` and `snapshot_table_suffix=smoke`.

Why snapshots matter:

- A single latest table only tells the model what the world looked like on one date.
- A pCTR model needs many historic examples so it can learn from different campaigns, customers, seasons, and advert mixes.
- Snapshot tables preserve those historic "as of this date" views. That means a model can train on older months and be tested on newer months, which is closer to how it would behave in production.

Snapshot example:

```text
reference_date = 2026-01-15 -> January snapshot
reference_date = 2026-02-15 -> February snapshot
reference_date = 2026-03-15 -> March snapshot
```

Each snapshot rebuilds the customer, advert, product, seasonality, and labelled click rows as they would have looked at that point in time.

### `pctr_spark_model_training.py`

Trains candidate Spark ML pCTR models from the snapshot training table.
It compares logistic regression, random forest, gradient boosted trees, and Spark XGBoost candidates using time-based train/validation/test splits.

This notebook has not been run as part of the current feature smoke test. It is included so the branch has a clear model-testing path once enough monthly snapshots exist.

How it links the columns to the model:

- Uses `label_7d` as the default target.
- Reads the snapshot training table, so model evaluation can train on older `reference_date` partitions and test on newer partitions.
- Splits feature columns into numeric and categorical sets.
- Encodes categorical columns with `StringIndexer` and `OneHotEncoder`. This means values like `gender = F` or `top_brand = Nike` are converted into numeric columns that Spark ML can use.
- Assembles numeric, embedding, similarity, count, recency, time, and encoded categorical columns into a single Spark ML `features` vector. This is just Spark's required format: all model inputs are packed into one vector column before training.
- Logs `feature_columns.json`, `numeric_feature_columns.json`, and `categorical_feature_columns.json` with the model. The scoring notebook uses those artifacts as the feature contract.
- Compares models using metrics such as AUCPR, AUC, log loss, lift/precision at top scored rows, and calibration by score bucket.

Metric explanation:

- AUCPR: useful when clicks are rare. It rewards models that find clicked rows near the top of the scored list.
- AUC: checks whether clicked rows generally score higher than non-clicked rows.
- Log loss: checks whether the predicted probabilities are sensible, not just whether the ordering is good.
- Lift/precision at top scored rows: checks whether the highest pCTR rows contain more real clicks than average.
- Calibration: checks whether a score like `0.05` behaves like roughly a 5% click probability.

Output:

- registered UC model `nextads_pctr_spark_model` with alias `dev_candidate`

### `pctr_score_ad_candidates.py`

Loads the registered pCTR model and scores advert options for each account from `next_uk_nextads_assignments_latest`.
It joins the same feature families used in training, aligns to the saved model feature contract, and writes `predicted_pctr`.

This notebook has not been run as part of the current feature smoke test because it depends on a registered pCTR model from `pctr_spark_model_training.py`.

What scoring candidates means:

At scoring time, the pCTR model needs a table of possible advert rows to score. A row means:

```text
this account
+ this possible advert
+ this page/placement
+ this scoring date
```

Those rows are the scoring candidates. They are the adverts that are available to be considered for an account/page after the existing NextAds process has already applied the business setup around active adverts, page/location, assignment rows, measurement adverts, cells, treatment, and MASID context.

So the pCTR scoring notebook is not starting from the full advert catalogue and deciding which adverts are eligible. Eligibility and candidate creation still sit with the existing NextAds flow. The pCTR notebook takes those possible account/ad rows and asks:

```text
if this account is shown this candidate advert here,
what is the predicted probability they click it?
```

For each candidate row, the notebook rebuilds the same feature groups used during training:

- customer behaviour for the account
- advert attributes and creative/content fields for the advert
- advert semantic features
- advert/product match features
- customer/product interest features
- seasonal product features
- placement and scoring-date context

The output is one `predicted_pctr` probability per account/ad/placement row.

Main columns/features created:

- Candidate keys: `account_number`, `reference_date`, `placement_id`, `unique_ad_id`, `assigned_unique_ad_id`, `treatment`, `masid`.
- Scoring-time exposure context: deterministic time features for the scoring `reference_date`.
- Joined feature families: customer behaviour, advert attributes, advert semantic dimensions, advert product dimensions, customer product features, seasonal product features.
- Match features rebuilt at scoring time: `customer_ad_product_cosine_similarity`, `customer_ad_product_embedding_coverage`, `customer_ad_seasonal_product_cosine_similarity`, `customer_ad_seasonal_product_embedding_coverage`.
- Final score output: `predicted_pctr`, `model_name`, `model_version`, `rundate`.

What the match features mean:

- `customer_ad_product_cosine_similarity`: how similar the customer's recent product interests are to the products linked to the advert.
- `customer_ad_product_embedding_coverage`: whether both sides had enough product embedding data to make that comparison trustworthy.
- `customer_ad_seasonal_product_cosine_similarity`: the same idea, but focused on seasonal product behaviour.
- `customer_ad_seasonal_product_embedding_coverage`: whether the seasonal comparison had enough usable data.

Why the model needs it:

- This is the bridge from model training to NextAds ranking.
- A trained model cannot score "an account" or "an advert" in isolation. It needs the exact account/ad/page rows that could be ranked.
- It recreates the same feature shape used during training for current candidate adverts.
- The output can be joined into the existing theme-to-ad mapped table and used to adjust the final ad `Score` before `Rank` is calculated.

Scoring example:

```text
Account A + Advert 1 + Shopping Bag -> predicted_pctr = 0.012
Account A + Advert 2 + Shopping Bag -> predicted_pctr = 0.031
Account A + Advert 3 + Shopping Bag -> predicted_pctr = 0.006
```

Those probabilities can then be combined with the existing theme/LTR score before NextAds decides the final ranked advert order.

Outputs:

- `next_uk_pctr_ad_candidate_scores_latest`
- `next_uk_pctr_ad_candidate_scores`

## Dev Smoke Test

A one-month dev smoke test has been run for the feature build path:

1. Customer behaviour features
2. Advert metadata and attribute profiles
3. Advert semantic embeddings
4. Product embeddings and product-match features
5. Seasonal product features

The smoke test exposed two Databricks/Spark issues that this branch fixes:

- Spark workers were loading the registered MLflow/UC SentenceTransformer directly, causing repeated calls to `generate-temporary-credentials` and executor timeouts.
- Product/semantic embedding inference could fail after retry because Spark could not roll back an indeterminate shuffle stage.

The fix is to stage the model once to:

```text
/Volumes/marketingdata_dev/ds_sandbox/ds_volume/next_ads/embedding_models
```

and to materialise embedding input rows to Delta before repartitioned batch inference.

The tagged-click training, model training, and scoring notebooks still need full validation after the feature tables are confirmed for the smoke month.

## Recommended Branch Plan

This branch should finish as the Shopping Bag feature-build branch.
It should prove that the source data joins, feature tables, embedding generation, seasonality, and tagged click labels are structurally sound for SB.

Next branch:

```text
feature/pctr-productionisation
```

That branch should create the production-shaped daily job, stable UC feature tables, target table, model-ready scoring table, QA checks, and config structure.

Separate modelling/placement branch:

```text
experiment/pctr-placement-model-comparison
```

That branch should test SB vs OC vs homepage/page type, label windows, model performance, ad placement impact, calibration, and whether separate page-type models are genuinely needed.

### Placement As A Model Signal

The existing NextAds flow already knows where an advert is being considered before the final assignment is written; `Location`.

The current flow is:

```text
map_theme_scores_to_ads.py
-> AccountNumber, UniqueAdID, Location, Score, Rank
-> build_page.py
-> AccountNumber, Location, UniqueAdIDMeasurement, UniqueAdIDAssigned, Treatment, MASID
-> next_uk_nextads_assignments_latest
```

The training data can include the location where the advert was shown, and the scoring data can include the location where the advert is being considered.

The tagged-click training notebook already carries this through as `placement_id`, derived from `Location` currently, but this can be swapped to `PageType`. It is part of the observed exposure row alongside the account, advert, session, device, treatment, and click labels.

However, the current model-training notebook treats `placement_id` as an excluded identifier rather than as a predictive feature due to wanting to test this only on the one location.

The recommended follow-up is to test `placement_id` as a categorical model feature:

```text
account + advert + Location + context -> probability of click
```

If pCTR is later moved earlier into the ranking process, the join point should be around `map_theme_scores_to_ads.py`, before the final `Rank` is calculated. The pCTR score can then adjust the ad-level `Score` for each `AccountNumber`, `UniqueAdID`, and `Location`, while `build_page.py` continues to handle cells, fallow control, premium substitutions, MASID mapping, and assignment writes.

## Productionisation Direction

The production version should move from notebook-first feature exploration to a scheduled Databricks job that writes stable UC tables. The model should be able to read one model-ready DataFrame rather than rebuilding every dependency inline.

The purpose of productionisation is to turn this exploratory feature build into a repeatable daily data product. Instead of a person running notebooks in order, a Databricks job should refresh the required feature tables, run QA checks, and produce a stable scoring table that the NextAds ranking step can consume.

Recommended production tables:

- account/customer feature table
- advert feature table
- product embedding feature table
- seasonal feature table
- target/label table by account, advert, page type, and attribution window
- model-ready training table
- model-ready scoring candidate table
- scored pCTR table with `AccountNumber`, `UniqueAdID`, `Location`, `predicted_pctr`, `model_name`, `model_version`, and `reference_date`

The NextAds serving path should not be rewritten for this stage. pCTR should join into the existing rank-writing stage, update the ad-level `Score`, produce `Rank`, and let `build_page` continue to handle assignment logic.

### Advert-Item Bridge Improvement

The next feature-source improvement should be to use the Next Ads sort-order tables as the preferred source of advert-to-item mappings.

Relevant tables:

- `marketingdata_prod.warehouse.next_ads_sort_order`: history table.
- `marketingdata_prod.warehouse.next_ads_sort_order_latest`: latest/current table.

Why this matters:

- The current feature build uses linked items from the control sheet and existing representative-item outputs.
- The sort-order tables should be closer to the actual items that were used for an advert.
- Better advert-item mapping improves every feature that tries to explain what an advert contains.

Where it would feed in:

- `pctr_advert_metadata_attribute_profile.py`: create better advert attribute profiles, such as top brand/category/colour/use.
- `pctr_advert_semantic_embeddings.py`: improve linked item text inside `advert_text_corpus`.
- `pctr_product_embedding_features.py`: improve `advert_product_embedding` and customer/ad product similarity.
- `pctr_seasonal_product_features.py`: improve advert-side product demand and seasonal product vectors.

Recommended production pattern:

```text
next_ads_sort_order history
-> top 10 items per advert as of reference_date
-> reusable advert-item bridge
-> advert attribute, semantic, product, and seasonal features
```

The bridge should have a stable shape:

```text
feature_date
advert_id
itemno
item_position
item_weight
item_source
```

For snapshot training, use the history table and select the top 10 items as of the snapshot `reference_date`. This keeps the feature build point-in-time correct and avoids leaking today's item mapping into older training months.

For current scoring, `next_ads_sort_order_latest` can be used because scoring is meant to represent the current advert setup.

Suggested source priority:

1. `next_ads_sort_order` history for snapshot/backfill features.
2. `next_ads_sort_order_latest` for current/latest scoring features.
3. Existing representative-item table as fallback.
4. Control sheet `Items` as final fallback.

This is not included in the current branch because it changes the underlying advert product definition across several feature layers. It should be brought in as a focused productionisation change, with before/after QA on advert item coverage, top attributes, product embedding coverage, and training label coverage.

Production flow:

```text
daily feature job runs
-> stable feature tables refreshed
-> model-ready scoring candidates built
-> pCTR model scores account/ad candidates
-> pCTR scores joined to existing ad ranking table
-> combined Score and Rank written
-> existing build_page process assigns ads as it does today
```

## Model Feature Map

At a high level, the final model-ready row contains these groups:

- Customer propensity: account status, lifecycle, shopping activity, web activity, action activity.
- Advert identity and content: campaign, placement, theme/category, creative copy, linked product attributes.
- Product affinity: similarity between the customer's recent product interactions and the products represented by the advert.
- Semantic affinity: dense text dimensions that describe what the advert and linked products mean, beyond exact category labels.
- Seasonal affinity: whether the advert's products and the customer's historic purchases are relevant to this time of year.
- Context: page/placement, device, treatment, exposure time, day, week, month, and weekend flags.
- Target: `label_7d`, with same-session and 24-hour labels retained for comparison.

This keeps the feature build interpretable:

```text
customer features
+ advert features
+ customer/ad product match
+ seasonality
+ exposure context
-> label from tagged clicks
-> train pCTR
-> score candidate account/ad rows
-> combine with Hackathon theme score
```

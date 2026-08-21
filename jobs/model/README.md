# Model Jobs

This folder contains the shared entry points for model development, lifecycle
movement, operational scoring, research and monitoring. New model work should
use the existing declared operations instead of creating a saved Databricks job
for each model or experiment.

Theme Affinity, pCTR, and future model work should be split into feature
generation, training or model loading, scoring/ranking, decisioning, and
delivery rather than productised as one large custom package.

- `development/` dispatches the declared `BUILD`, `RESEARCH`, `REVIEW_SELECT` and `EVALUATE` operations.
- `research/` contains the model-comparison and reviewed-selection entry points.
- `scoring/` owns supported operational score publication.
- `lifecycle/` owns controlled model movement between environments.
- `monitoring/` owns model-monitoring entry points.

Read [Model research: data scientist guide](../../docs/model_research_walkthrough.md) before running research or evaluation. Read [NextAds job and table flow](../../docs/architecture/nextads_job_table_flow.md) to understand how operational scores later become advert options, assignments and delivery outputs.

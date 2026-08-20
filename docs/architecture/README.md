# NextAds Architecture And Data-Flow Guides

These pages map the complete NextAds job and table flow, then provide detailed
views of the assignment, Feature Store, Theme Affinity and model-promotion
routes. Operating settings and evidence remain in the linked reference pages.

| Page | Use it for |
| --- | --- |
| [nextads_job_table_flow.md](nextads_job_table_flow.md) | Inclusive guide to the operational assignment/delivery route and the in-flight Feature Store/model jobs, including what they consume and produce. |
| [theme_affinity_operational_flow.md](theme_affinity_operational_flow.md) | Theme Affinity DLT/Lakeflow and prediction route, including where its outputs feed Feature Store. |
| [v1_v2_parallel_route.md](v1_v2_parallel_route.md) | Long-lived v1/v2 candidate mapping and page-build DAG, including the shared Theme Affinity boundary. |
| [feature_store_flow.md](feature_store_flow.md) | Detailed DEV Feature Store task order and parallel build branches; use the inclusive job-and-table page for cross-route inputs and outputs. |
| [../model_research_walkthrough.md](../model_research_walkthrough.md) | Complete DS-facing declared research, AutoML, reviewed selection and isolated evaluation walkthrough, including every option and the Shopping Bag proof. |
| [mlflow_model_lifecycle.md](mlflow_model_lifecycle.md) | DEV-to-PROD model movement through MLflow and Unity Catalog. |
| [future_model_adoption.md](future_model_adoption.md) | How pCTR, LTR and direct-ad challengers should reuse feature contracts and lifecycle jobs. |

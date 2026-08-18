# NextAds Architecture Diagrams

These pages give a visual map of the current NextAds model, feature-store and
model-lifecycle work. They are intentionally diagram-led; the detailed operating
steps remain in the feature-store and model-lifecycle runbooks.

| Page | Use it for |
| --- | --- |
| [nextads_model_feature_overview.md](nextads_model_feature_overview.md) | Inclusive guide to the current and in-flight jobs, the tables or model artifacts they consume and produce, their boundaries, and the detailed docs that own each topic. |
| [theme_affinity_operational_flow.md](theme_affinity_operational_flow.md) | Theme Affinity DLT/Lakeflow and prediction route, including where its outputs feed Feature Store. |
| [v1_v2_parallel_route.md](v1_v2_parallel_route.md) | Long-lived v1/v2 candidate mapping and page-build DAG, including the shared Theme Affinity boundary. |
| [feature_store_flow.md](feature_store_flow.md) | Detailed DEV Feature Store task order and parallel build branches; use the overview page for the job-to-table guide. |
| [mlflow_model_lifecycle.md](mlflow_model_lifecycle.md) | DEV-to-PROD model movement through MLflow and Unity Catalog. |
| [future_model_adoption.md](future_model_adoption.md) | How pCTR, LTR and direct-ad challengers should reuse feature contracts and lifecycle jobs. |

# NextAds Architecture Diagrams

These pages give a visual map of the current NextAds model, feature-store and
model-lifecycle work. They are intentionally diagram-led; the detailed operating
steps remain in the feature-store and model-lifecycle runbooks.

| Page | Use it for |
| --- | --- |
| [nextads_model_feature_overview.md](nextads_model_feature_overview.md) | One-page view of how production delivery, Theme Affinity, Feature Store, MLflow and future challengers fit together. |
| [theme_affinity_operational_flow.md](theme_affinity_operational_flow.md) | Theme Affinity DLT/Lakeflow and prediction route, including where its outputs feed Feature Store. |
| [feature_store_flow.md](feature_store_flow.md) | DEV Feature Store inputs, feature tables, training inputs, compatibility views and quality checks. |
| [mlflow_model_lifecycle.md](mlflow_model_lifecycle.md) | DEV-to-PROD model movement through MLflow and Unity Catalog. |
| [future_model_adoption.md](future_model_adoption.md) | How pCTR, LTR and direct-ad challengers should reuse feature contracts and lifecycle jobs. |

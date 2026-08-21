# NextAds V2 Jobs

This folder contains V2 page-building entry points. The long-lived V1/V2 route
split is active; V2 is not a temporary replacement path.

V2 is production-transition work, not an experiment. Keep it isolated from the
V1 route at the control-sheet join, advert-candidate mapping and page-build
layers. Shared upstream inputs such as customer cells, theme mapping and score
outputs remain outside this folder.

See [V1/V2 parallel route](../../docs/architecture/v1_v2_parallel_route.md) for exact dependencies and failure boundaries, and [NextAds job and table flow](../../docs/architecture/nextads_job_table_flow.md) for the plain-language daily sequence.

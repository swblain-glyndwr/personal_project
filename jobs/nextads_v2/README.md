# NextAds V2 Jobs

Ads v2 job entrypoints live here while the long-lived v1/v2 route split is in
flight.

V2 is production-transition work, not an experiment. Keep it isolated from the
current v1 route at the control-sheet join, candidate mapping, and page-build
layers. Shared upstream inputs such as customer cells, theme mapping, and Theme
Affinity remain outside this folder.

CREATE TABLE {catalog}.{schema}.{client}_nextads_results_ads_top_by_location (
    Location STRING NOT NULL,
    UniqueAdID STRING NOT NULL,
    MASIDToken STRING,
    rundate date not null,
    CONSTRAINT {client}_nextads_results_ads_top_by_location_pk_location PRIMARY KEY (Location, rundate)
)
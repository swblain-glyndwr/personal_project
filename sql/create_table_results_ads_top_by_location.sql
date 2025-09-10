CREATE TABLE marketingdata_prod.{schema}.{client}_nextads_results_ads_top_by_location (
    Location STRING NOT NULL,
    UniqueAdID STRING NOT NULL,
    MASIDToken STRING,
    CONSTRAINT {client}_nextads_results_ads_top_by_location_pk_location PRIMARY KEY (Location)
)
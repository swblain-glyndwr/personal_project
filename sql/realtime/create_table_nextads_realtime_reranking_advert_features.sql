CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{client}_nextads_realtime_reranking_advert_features (
    UniqueAdID STRING NOT NULL,
    CMSPageID STRING NOT NULL,
    brand STRING,
    brand_perc_coverage double, 
    department STRING,
    department_perc_coverage double, 
    next_category STRING, 
    next_category_perc_coverage double,
    prem_level_brand STRING, 
    prem_level_brand_perc_coverage double,
    rundate date not null,
    constraint pk_{client}_nextads_realtime_reranking_advert_features primary key (
        UniqueAdID,
        rundate)
)

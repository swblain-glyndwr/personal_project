CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{client}_nextads_realtime_reranking_product_features (
    pid  STRING NOT NULL,
    brand STRING,
    department STRING, 
    next_category STRING,
    prem_level_brand BOOL, 
    rundate date not null,
    constraint pk_{client}_nextads_realtime_reranking_product_features primary key (
        pid,
        rundate)
)

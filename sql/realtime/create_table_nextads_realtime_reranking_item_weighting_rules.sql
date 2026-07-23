CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{client}_nextads_realtime_reranking_item_weighting_rules (
 pid STRING NOT NULL ,
 brand STRING ,
 next_category STRING ,
 department STRING,
 prem_level_brand BOOLEAN,
 action STRING NOT NULL,
 weighting_brand DOUBLE NOT NULL,
 weighting_department DOUBLE NOT NULL,
 weighting_next_category DOUBLE NOT NULL,
 weighting_prem_level_brand DOUBLE NOT NULL,
rundate date not null,
    constraint pk_{client}_nextads_realtime_reranking_item_weighting_rules primary key (
        pid,
        action,
        rundate)
)
USING DELTA
PARTITIONED BY (pid)
TBLPROPERTIES('delta.enableChangeDataFeed'= 'true' )
;
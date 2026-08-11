CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{client}_nextads_realtime_reranking_rules_weighting (
    ruleID STRING NOT NULL,
    action STRING NOT NULL,
    feature STRING NOT NULL, 
    weight double,
    rundate date not null,
    constraint pk_{client}_nextads_realtime_reranking_rules_weighting primary key (
        action,
        feature,
        rundate)
)

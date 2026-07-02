CREATE TABLE {catalog}.{schema}.{client}_nextads_results_underperforming_ads (
    UniqueAdID STRING NOT NULL,
    min_session_date DATE,
    max_session_date DATE,
    ApportionedRevenue DOUBLE,
    Sessions BIGINT,
    C_ApportionedRevenue DOUBLE,
    C_Sessions BIGINT,
    SessionOverlapRatio DOUBLE,
    ARPS DOUBLE,
    C_ARPS DOUBLE,
    IncARPS DOUBLE,
    IncARPSAdj DOUBLE,
    EstContribution DOUBLE,
    IncPct DOUBLE,
    rundate DATE NOT NULL,
CONSTRAINT pk_{client}_nextads_results_underperforming_ads PRIMARY KEY (
    UniqueAdID, 
    rundate)
)
PARTITIONED BY (rundate);
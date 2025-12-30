CREATE TABLE IF NOT EXISTS marketingdata_prod.{schema}.{client}_nextads_viewed_bought_latest (
    itemno1 STRING NOT NULL,
    itemno2 STRING NOT NULL,
    freq12 BIGINT NOT NULL,
    freq1 BIGINT,
    freq2 BIGINT,
    all_customers INT NOT NULL,
    support12 DOUBLE,
    support1 DOUBLE,
    support2 DOUBLE,
    lift DOUBLE,
    lift_adjusted DOUBLE,
    cosine_similarity DOUBLE,
    conversion_rate DOUBLE,
    rank INT NOT NULL,
    rundate date not null,
    constraint pk_{client}_nextads_viewed_bought_latest primary key (
        itemno1,
        itemno2)
)
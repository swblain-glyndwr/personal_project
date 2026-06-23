CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{client}_theme_score_components_hackathon_latest (
    AccountNumber STRING NOT NULL,
    Theme STRING NOT NULL,
    UniqueAdID STRING NOT NULL,
    RelevanceScore FLOAT NOT NULL,
    IncrementalScore FLOAT NOT NULL,
    Score FLOAT NOT NULL,
    CONSTRAINT pk_{client}_theme_score_components_hackathon_latest
    PRIMARY KEY (
        AccountNumber,
        Theme,
        UniqueAdID
    )
)
PARTITIONED BY (Theme)

CREATE TABLE {catalog}.{schema}.{client}_nextads_preranked_ads_from_themes_hackathon_latest (
    AccountNumber STRING not null,
    UniqueAdID STRING not null,
    Location STRING not null,
    Score STRING not null,
    Rank INT not null,
    rundate DATE not null,
    CONSTRAINT pk_{client}_nextads_preranked_ads_from_themes_hackathon_latest PRIMARY KEY (
        AccountNumber,
        UniqueAdID,
        Location
        )
)
PARTITIONED BY (Location)


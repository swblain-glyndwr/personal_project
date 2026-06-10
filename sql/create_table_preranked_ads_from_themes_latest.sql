create table {catalog}.{schema}.{client}_nextads_preranked_ads_from_themes_latest (
    AccountNumber string not null,
    UniqueAdID string not null,
    Location string not null,
    Score float not null,
    TriggerScore float,
    Rank int not null,
    rundate date not null,
  constraint pk_{client}_nextads_preranked_ads_from_themes_latest primary key (
    AccountNumber,
    UniqueAdID,
    Location
    )
)
partitioned by (Location)

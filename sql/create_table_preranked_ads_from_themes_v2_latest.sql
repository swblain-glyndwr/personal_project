create table {catalog}.{schema}.{client}_nextads_preranked_ads_from_themes_v2_latest (
    AccountNumber string not null,
    UniqueAdID string not null,
    PageFamily string not null,
    Score string not null,
    Rank int not null,
    rundate date not null,
  constraint pk_{client}_nextads_preranked_ads_from_themes_v2_latest primary key (
    AccountNumber,
    UniqueAdID,
    PageFamily
    )
)
partitioned by (PageFamily)

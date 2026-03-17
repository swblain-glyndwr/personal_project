create table {catalog}.{schema}.{client}_nextads_theme_score_components_latest (
    AccountNumber string not null,
    Theme string not null,
    UniqueAdID string not null,
    RelevanceScore float not null,
    IncrementalScore float not null,
    Score float not null,
    rundate date not null,
  constraint pk_{client}_nextads_theme_score_components_latest primary key (
    AccountNumber,
    Theme,
    UniqueAdID
    )
)
partitioned by (Theme)
create table {catalog}.{schema}.{client}_nextads_theme_score_components (
    AccountNumber string not null,
    Theme string not null,
    UniqueAdID string not null,
    RelevanceScore float not null,
    IncrementalScore float not null,
    Score float not null,
    MultiSessionDownweightScore double,
    rundate date not null,
  constraint pk_{client}_nextads_theme_score_components primary key (
    AccountNumber,
    Theme,
    UniqueAdID,
    rundate
    )
)
partitioned by (rundate)

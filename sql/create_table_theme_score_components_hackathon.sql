create table IF NOT EXISTS {catalog}.{schema}.{client}_theme_score_components_hackathon (
AccountNumber STRING not null,
Theme STRING not null,
UniqueAdID STRING not null,
RelevanceScore float not null,
IncrementalScore float not null,
Score float not null,
rundate date not null,
constraint pk_{client}_theme_score_components_hackathon primary key (
    AccountNumber,
    Theme,
    UniqueAdID,
    rundate
    )
)
partitioned by (rundate)
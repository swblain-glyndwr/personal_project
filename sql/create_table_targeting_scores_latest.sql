create table marketingdata_prod.{schema}.{domain}_nextads_targeting_scores_latest (
    AccountNumber string not null,
    TargetingCriteria string not null,
    TargetingScore double,
    rundate date not null,
  constraint pk_{domain}_nextads_targeting_scores_latest primary key (
    AccountNumber,
    TargetingCriteria
    )
)
partitioned by (rundate)
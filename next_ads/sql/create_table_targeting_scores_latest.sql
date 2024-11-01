create table marketingdata_prod.ds_sandbox.next_uk_nextads_targeting_scores_latest (
    AccountNumber string not null,
    TargetingCriteria string not null,
    TargetingScore double,
    rundate date not null,
  constraint pk_next_uk_nextads_targeting_scores_latest primary key (
    AccountNumber,
    TargetingCriteria,
    rundate
    )
)
partitioned by (rundate)
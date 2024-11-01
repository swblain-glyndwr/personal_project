create table {table} (
    AccountNumber string not null,
    TargetingCriteria string not null,
    TargetingScore double,
    rundate date not null,
  constraint pk_{table_name} primary key (
    AccountNumber,
    Location,
    rundate
    )
)
partitioned by (rundate)